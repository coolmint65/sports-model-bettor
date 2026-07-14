"""HR odds fetcher for motorsports series.

Bypasses ``scrapers.hardrock_odds._fetch_events_for_comp`` because that
helper hardcodes ``outright=false`` in the filter — motorsports IS
outright (one market per race, N selections per market). We use the
underlying ``_graphql_post`` directly with the outright filter dropped.

Public:
    fetch_series_odds(series) -> dict[race_id, race_odds]

Where ``race_odds`` is::

    {
      "hr_event_id":     "...",
      "race_name":       "Canadian GP 2026",
      "start_time":      "2026-05-24T20:00:00Z",
      "winner": {                                   # optional
          driver_id_1: {"odds": 165, "name": "...", "hr_ext_id": "..."},
          ...
      },
      "podium": {                                   # optional, top-3 market
          driver_id_1: {"odds": 130, "name": "...", "hr_ext_id": "..."},
          ...
      },
    }

Both markets keyed by *our* driver_id once we resolve the HR name → DB
match. Selections HR ships with ``odds: null`` (no current line) are
dropped silently.
"""
from __future__ import annotations

import json
import logging
import time
import unicodedata
from typing import Any

from ._config import get_series_config
from ._db import get_conn

logger = logging.getLogger(__name__)


_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 120  # short TTL — line moves matter for outrights


def fetch_series_odds(series: str, force: bool = False) -> dict[str, dict]:
    """Fetch HR's race-winner + podium markets for ``series``. Returns a
    dict keyed by *our* race_id ('{season}-{round:02d}'). Empty dict on
    any failure — callers fall through to no-odds-no-picks rather than
    crashing the slate."""
    cfg = get_series_config(series)
    comp_id = cfg.get("hr_comp_id")
    if not comp_id:
        return {}

    now = time.time()
    cached = _CACHE.get(series)
    if cached and not force and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]

    events = _fetch_events_outright(str(comp_id))
    if not events:
        _CACHE[series] = (now, {})
        return {}

    # Pre-build name → driver_id lookup once per call (DB hit count: 1).
    name_to_driver = _build_driver_lookup(series)

    out: dict[str, dict] = {}
    for ev in events:
        parsed = _parse_event(series, ev, name_to_driver, cfg)
        if parsed:
            out[parsed["race_id"]] = parsed
    _CACHE[series] = (now, out)
    logger.info("[%s] HR fetched odds for %d races", series, len(out))
    return out


# ── HR fetch (without the outright=false filter) ────────────

def _fetch_events_outright(comp_id: str) -> list[dict]:
    """Hit HR GraphQL for ``comp_id`` *without* the outright filter so
    motorsports race-winner + podium markets come back. Also merges in
    the sports-tree-based fetch used elsewhere in the codebase — HR
    occasionally moves individual race events to the non-outright side
    of its comp tree pre-race (observed 2026-07-03 when NASCAR eero
    400 was live at ~14:00 ET but gone from the outright query by
    ~19:00). Two-path union keeps the pipeline resilient to HR's
    market-lifecycle quirks."""
    from scrapers.hardrock_odds import (
        _load_query, _load_headers, _graphql_post, GRAPHQL_URL,
        _fetch_events_for_comp,
    )
    seen_ids: set = set()
    out: list[dict] = []

    body = _load_query()
    body.setdefault("variables", {})["filters"] = [
        {"field": "compId", "values": comp_id},
    ]
    status, raw, err = _graphql_post(GRAPHQL_URL, body, _load_headers())
    if status == 200 and raw:
        try:
            data = json.loads(raw)
            for ev in (data.get("data", {}).get("betSync", {})
                            .get("events", {}).get("data") or []):
                eid = ev.get("id")
                if eid and eid not in seen_ids:
                    seen_ids.add(eid)
                    out.append(ev)
        except json.JSONDecodeError as e:
            logger.warning("HR motorsports JSON parse: %s", e)
    else:
        logger.warning("HR motorsports comp=%s outright fetch: %s",
                       comp_id, err or f"HTTP {status}")

    # Secondary path — same comp id, sports-tree-embedded events. Some
    # race markets sit here even when the outright query returns just
    # the championship futures.
    try:
        tree_evs, tree_err = _fetch_events_for_comp(str(comp_id))
        if not tree_err:
            for ev in tree_evs or []:
                eid = ev.get("id")
                if eid and eid not in seen_ids:
                    seen_ids.add(eid)
                    out.append(ev)
    except Exception as e:
        logger.debug("HR motorsports comp=%s tree-fetch: %s", comp_id, e)

    return out


# ── Driver-name resolution ──────────────────────────────────

def _strip_accents(s: str) -> str:
    """Strip diacritics so HR's 'Nico Hülkenberg' resolves to our DB
    'Nico Hulkenberg' without an exact match."""
    nfkd = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _build_driver_lookup(series: str) -> dict[str, dict]:
    """Map every plausible driver-name spelling to its driver row.
    Indexes: full name, abbreviation, last-name, accent-stripped variants
    of each."""
    conn = get_conn(series)
    rows = conn.execute(
        "SELECT id, name, abbreviation, ergast_id, hr_ext_id FROM drivers"
    ).fetchall()
    out: dict[str, dict] = {}

    def _add(key: str, row: dict) -> None:
        if not key:
            return
        ks = _strip_accents(key).strip().lower()
        if ks:
            out.setdefault(ks, row)

    for r in rows:
        d = dict(r)
        name = (d["name"] or "").strip()
        abbr = (d["abbreviation"] or "").strip()
        if abbr:
            _add(abbr, d)
        if name:
            _add(name, d)
            tokens = name.split()
            # Last name is the usual short-form ("Verstappen", "Norris")
            if tokens and len(tokens[-1]) >= 3:
                _add(tokens[-1], d)
            # First-last (handles "Lewis Hamilton" without middle name)
            if len(tokens) >= 2:
                _add(f"{tokens[0]} {tokens[-1]}", d)
    return out


def _resolve_driver(name: str, lookup: dict[str, dict]) -> dict | None:
    if not name:
        return None
    n = _strip_accents(name).strip().lower()
    if n in lookup:
        return lookup[n]
    # Last-token fallback
    parts = n.split()
    if parts and parts[-1] in lookup:
        return lookup[parts[-1]]
    # First+last
    if len(parts) >= 2 and f"{parts[0]} {parts[-1]}" in lookup:
        return lookup[f"{parts[0]} {parts[-1]}"]
    return None


# ── Decimal → American odds conversion ──────────────────────

def _decimal_to_american(dec: Any) -> int | None:
    """Standard decimal → American odds. Returns None for null/invalid."""
    if dec is None:
        return None
    try:
        d = float(dec)
    except (TypeError, ValueError):
        return None
    if d <= 1.0:
        return None
    if d >= 2.0:
        return int(round((d - 1.0) * 100))
    return int(round(-100.0 / (d - 1.0)))


# ── Event parsing ───────────────────────────────────────────

def _parse_event(series: str, ev: dict,
                 name_to_driver: dict[str, dict],
                 cfg: dict) -> dict | None:
    """Convert one HR race event to our odds dict. Returns None when
    the event isn't a race (futures, championship, etc.) or has no
    parsable winner/podium markets."""
    name = (ev.get("eventNames") or {}).get("name") or ""
    # Skip season-long futures — they're per-driver/constructor titles
    # rather than per-race outrights. Heuristic on event name.
    n_lo = name.lower()
    if any(k in n_lo for k in ("championship", "constructors", "drivers ")):
        return None

    # Resolve which DB race this HR event corresponds to. Match by
    # (event_time → race_date) within ±1 day so HR's "GP weekend
    # Sunday" timestamps line up with our calendar.
    et = ev.get("eventTime")
    if not isinstance(et, (int, float)) or et <= 0:
        return None
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(et / 1000.0, tz=timezone.utc)
    race_id = _race_id_for_date(series, dt.strftime("%Y-%m-%d"))
    if not race_id:
        # Calendar may be out of date — log so we notice and refresh.
        logger.debug("HR motorsports: no race in DB for %s on %s",
                     series, dt.strftime("%Y-%m-%d"))
        return None

    out: dict[str, Any] = {
        "race_id": race_id,
        "hr_event_id": ev.get("id"),
        "race_name": name,
        "start_time": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    win_market = cfg.get("hr_market_winner")
    pod_market = cfg.get("hr_market_podium")
    for m in ev.get("markets") or []:
        mt = (m.get("type") or "").upper()
        if mt == win_market:
            out["winner"] = _selections_to_dict(m, name_to_driver)
        elif mt == pod_market:
            out["podium"] = _selections_to_dict(m, name_to_driver)

    if not (out.get("winner") or out.get("podium")):
        return None
    return out


def _selections_to_dict(market: dict,
                         name_to_driver: dict[str, dict]) -> dict[int, dict]:
    """Per-driver odds for one outright market. Drops null-odds entries
    and any selection we can't resolve to a DB driver — the latter is
    logged at DEBUG so a roster-mismatch surfaces during cold-start."""
    sels = market.get("selection") or market.get("selections") or []
    out: dict[int, dict] = {}
    for s in sels:
        odds_dec = s.get("odds") or s.get("price")
        american = _decimal_to_american(odds_dec)
        if american is None:
            continue
        nm = (s.get("name") or "").strip()
        d = _resolve_driver(nm, name_to_driver)
        if not d:
            logger.debug("HR motorsports: could not resolve driver %r", nm)
            continue
        out[d["id"]] = {
            "odds": american,
            "name": d["name"],
            "hr_ext_id": str(s.get("type") or ""),
        }
    return out


def _race_id_for_date(series: str, date: str) -> str | None:
    """Look up the race scheduled on ``date`` (YYYY-MM-DD). Returns
    the matching race_id or None."""
    conn = get_conn(series)
    row = conn.execute(
        "SELECT race_id FROM races WHERE race_date = ? LIMIT 1",
        (date,),
    ).fetchone()
    return row["race_id"] if row else None
