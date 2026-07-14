"""HR outright odds fetcher for the golf framework.

Golf is outright structure (N players per market, one winner). Bypasses
``scrapers.hardrock_odds._fetch_events_for_comp`` because that helper
hardcodes ``outright=false`` — same dance motorsports does for race
winners. We use ``_graphql_post`` directly with the outright filter
dropped.

Public:
    fetch_tournament_odds(tour, tournament_id) ->
        {market_type: {player_id: int_odds, ...}}

Market types we parse:
    WINNER       — outright winner
    TOP_5        — finish in top 5 (yes-side only)
    TOP_10       — finish in top 10
    TOP_20       — finish in top 20
    MAKE_CUT     — make the cut

HR's golf event ids rotate per tournament (PGA Championship 2026 ≠
PGA Championship 2025), so the caller passes the ESPN tournament id
and we resolve the HR comp via the in-DB hr_event_id mapping (set by
the discovery hit + filled on each /odds fetch).
"""
from __future__ import annotations

import json
import logging
import time
import unicodedata
from typing import Any

from ._db import get_conn

logger = logging.getLogger(__name__)


# HR's golf hierarchy: sport → tour category → competition (one per
# tournament) → event (the tournament itself). Competition ids rotate
# per tournament so we auto-discover at fetch time by name-matching the
# tournament against the active competitions under the tour's category
# (and, for PGA, the Majors category which hosts the four majors
# separately from the regular PGA Tour calendar).
#
# Cache + refresh: discovery is one extra GraphQL probe per cache miss
# (60s TTL) — cheap, and lets new tournaments (announced 2-4 weeks
# pre-event) pick up odds without a code change.


# Market-type → bet_type mapping. Codes verified live against HR's
# 2026 PGA Championship event (321 markets total). The country-specific
# winner sub-markets (e.g. USWINNER, ENGLANDWINNER) and round-specific
# variants stay out of the priced set for v1 — we'd need a separate
# pool of nationality / round predictions to evaluate them.
_HR_MARKET_TO_BET_TYPE = {
    "GOLF:FT:WIN":     "WINNER",
    "GOLF:FT:TOP5":    "TOP_5",
    "GOLF:FT:TOP10":   "TOP_10",
    "GOLF:FT:TOP20":   "TOP_20",
    "GOLF:FT:MAKECUT": "MAKE_CUT",
}

# Competition-id discovery cache. Keyed by (tour, espn_tournament_id);
# value is the HR competition id once resolved. Set by
# ``_resolve_hr_competition_id`` and inspected before any fresh probe.
_COMP_ID_CACHE: dict[tuple[str, str], str | None] = {}
_COMP_ID_CACHE_TS: dict[tuple[str, str], float] = {}
_COMP_ID_TTL_S = 600     # 10 min — tournaments don't appear that often


def _resolve_hr_competition_id(tour: str, tournament_id: str) -> str | None:
    """Probe HR's sports tree for the competition id matching our
    tournament. Walks the tour's category (and Majors for PGA) and
    name-matches the active tournament's display name.

    Cached for ``_COMP_ID_TTL_S`` so repeat slate fetches don't pound
    the sports-tree endpoint. Returns None when no match (e.g. event
    not yet on HR's board, or HR closed the market post-event)."""
    from ._config import get_tour_config
    key = (tour, tournament_id)
    now = time.time()
    cached_ts = _COMP_ID_CACHE_TS.get(key, 0.0)
    if cached_ts and (now - cached_ts) < _COMP_ID_TTL_S:
        return _COMP_ID_CACHE.get(key)
    cfg = get_tour_config(tour)
    # Pull tournament name from our DB.
    conn = get_conn(tour)
    tinfo = conn.execute(
        "SELECT name, short_name FROM tournaments WHERE id = ?",
        (tournament_id,),
    ).fetchone()
    if not tinfo:
        _COMP_ID_CACHE[key] = None
        _COMP_ID_CACHE_TS[key] = now
        return None
    candidates_names: set[str] = set()
    for nm in (tinfo["name"], tinfo["short_name"]):
        if nm:
            candidates_names.add(_strip_accents(nm).lower())

    # Walk the sports tree.
    from scrapers.hardrock_odds import _fetch_event_tree
    data, err = _fetch_event_tree()
    if err or not isinstance(data, dict):
        _COMP_ID_CACHE[key] = None
        _COMP_ID_CACHE_TS[key] = now
        return None
    data = data.get("data", data)
    sports = (data.get("betSync") or {}).get("sports") or []
    golf = None
    for s in sports:
        if (s.get("code") or "").lower() == "golf" \
                or "golf" in (s.get("name") or "").lower():
            golf = s
            break
    if not golf:
        _COMP_ID_CACHE[key] = None
        _COMP_ID_CACHE_TS[key] = now
        return None
    cat_ids_to_probe = [
        cfg.get("hr_category_id"), cfg.get("hr_majors_category_id"),
    ]
    cat_ids_to_probe = [c for c in cat_ids_to_probe if c]
    best_comp_id: str | None = None
    for cat in golf.get("categories") or []:
        if str(cat.get("id")) not in cat_ids_to_probe:
            continue
        for comp in cat.get("competitions") or []:
            comp_name = _strip_accents(comp.get("name") or "").lower()
            # Strip year suffixes HR appends (" 2026"), normalize spaces.
            for yy in ("2024", "2025", "2026", "2027", "2028"):
                comp_name = comp_name.replace(yy, "")
            comp_name = comp_name.strip()
            for tnm in candidates_names:
                if not tnm:
                    continue
                if tnm == comp_name:
                    best_comp_id = str(comp["id"])
                    break
                # Substring match either direction — HR sometimes uses
                # "The British Open" while ESPN ships "The Open
                # Championship".
                if (tnm in comp_name or comp_name in tnm) and len(tnm) > 4:
                    best_comp_id = str(comp["id"])
                    break
            if best_comp_id:
                break
        if best_comp_id:
            break
    _COMP_ID_CACHE[key] = best_comp_id
    _COMP_ID_CACHE_TS[key] = now
    if best_comp_id:
        logger.info("[golf:%s] resolved HR comp id for %r -> %s",
                    tour, tinfo["name"], best_comp_id)
    return best_comp_id

_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 120


def _strip_accents(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _fetch_events_outright(comp_id: str) -> list[dict]:
    """HR GraphQL fetch without the outright filter."""
    from scrapers.hardrock_odds import (
        _load_query, _load_headers, _graphql_post, GRAPHQL_URL,
    )
    body = _load_query()
    body.setdefault("variables", {})["filters"] = [
        {"field": "compId", "values": comp_id},
    ]
    status, raw, err = _graphql_post(GRAPHQL_URL, body, _load_headers())
    if status != 200 or not raw:
        logger.warning("HR golf comp=%s fetch failed: %s",
                       comp_id, err or f"HTTP {status}")
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning("HR golf JSON parse: %s", e)
        return []
    return (data.get("data", {}).get("betSync", {})
                .get("events", {}).get("data") or [])


def _build_player_lookup(tour: str) -> dict[str, int]:
    """Map player names (and accent-stripped / last-name variants) to
    our DB player_id. HR ships full names like "Scottie Scheffler"; we
    normalize to handle accent differences ("Ludvig Aberg" vs
    "Ludvig Åberg")."""
    conn = get_conn(tour)
    rows = conn.execute("SELECT id, name, short_name FROM players").fetchall()
    out: dict[str, int] = {}

    def _add(key: str, pid: int) -> None:
        if not key:
            return
        ks = _strip_accents(key).strip().lower()
        if ks:
            out.setdefault(ks, pid)

    for r in rows:
        pid = int(r["id"])
        name = (r["name"] or "").strip()
        short = (r["short_name"] or "").strip()
        if name:
            _add(name, pid)
            # Last name only — HR sometimes drops the first
            tokens = name.split()
            if len(tokens) >= 2:
                _add(tokens[-1], pid)
        if short:
            _add(short, pid)
    return out


# ── Market classifier ───────────────────────────────────────

def _classify_market(market: dict) -> str:
    """HR market type → our internal bet_type. Returns empty string
    when the market isn't priced by the v1 picker (country sub-winners,
    H2H, round-specific markets all skip until those layers land)."""
    mtype = (market.get("type") or "").upper()
    return _HR_MARKET_TO_BET_TYPE.get(mtype, "")


def _selection_to_int_odds(odds_val) -> int | None:
    """HR ships golf outright odds as decimals (1.4 / 2.25 / 11.0) —
    convert to American. Other HR markets sometimes ship odds as
    pre-formatted strings ("+150"); the parse fallback handles both."""
    if odds_val is None or odds_val == "":
        return None
    # Decimal path
    try:
        d = float(odds_val)
        if d <= 1.0:
            return None
        if d >= 2.0:
            return int(round((d - 1.0) * 100))
        return int(round(-100.0 / (d - 1.0)))
    except (TypeError, ValueError):
        pass
    # String / American fallback
    try:
        from scrapers.hardrock_odds import _parse_odds_string
        res = _parse_odds_string(str(odds_val))
        if res is None:
            return None
        return int(res[0] if isinstance(res, tuple) else res)
    except Exception:
        return None


def _extract_player_name_from_selection(sel: dict) -> str:
    """Outright selection names are typically just the player's full
    name. Some books prefix with the team / nationality flag — strip
    those if present."""
    raw = (sel.get("name") or "").strip()
    return raw


# ── Public ────────────────────────────────────────────────────

def fetch_tournament_odds(tour: str, tournament_id: str) -> dict[str, dict]:
    """Pull HR's outrights for ``tournament_id`` (ESPN id) and return
    a per-market dict keyed by our internal player_id.

    Shape:
        {
          "WINNER":   {player_id: int_odds, ...},
          "TOP_5":    {...},
          "TOP_10":   {...},
          "TOP_20":   {...},
          "MAKE_CUT": {...},
          "_event_id":   "<HR event id>",
          "_event_name": "<HR display name>",
        }
    """
    comp_id = _resolve_hr_competition_id(tour, tournament_id)
    if not comp_id:
        logger.debug("[golf:%s] no HR competition id for %s — skip",
                     tour, tournament_id)
        return {}

    cache_key = f"{tour}:{tournament_id}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]

    events = _fetch_events_outright(comp_id)
    if not events:
        return {}

    # One competition = one tournament for golf, so pick the first event.
    hr_event = events[0]
    lookup = _build_player_lookup(tour)
    markets = hr_event.get("markets") or []
    out: dict[str, Any] = {
        "_event_id": str(hr_event.get("id") or ""),
        "_event_name": hr_event.get("name") or "",
    }
    skipped_player = 0
    for m in markets:
        # HR's golf events ship state=None (no per-market open/closed
        # flag — entire event lifecycle gates whether markets exist).
        # Don't filter on state here; whole-event closure is upstream.
        bet_type = _classify_market(m)
        if not bet_type:
            continue
        market_dict = out.setdefault(bet_type, {})
        for sel in (m.get("selection") or []):
            name = _extract_player_name_from_selection(sel)
            if not name:
                continue
            key = _strip_accents(name).lower()
            pid = lookup.get(key)
            if pid is None:
                # Last-name fallback
                last = name.split()[-1] if name.split() else ""
                pid = lookup.get(_strip_accents(last).lower())
            if pid is None:
                skipped_player += 1
                continue
            odds = _selection_to_int_odds(sel.get("odds"))
            if odds is None:
                continue
            market_dict[int(pid)] = int(odds)
    logger.info("[golf:%s] HR %r: %d markets, players %s, skipped %d",
                tour, hr_event.get("name"),
                sum(1 for k in out if not k.startswith("_")),
                {k: len(v) for k, v in out.items() if not k.startswith("_")},
                skipped_player)
    _CACHE[cache_key] = (now, out)
    return out
