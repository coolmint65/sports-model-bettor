"""Hockey HR odds fetcher — mirrors ``engine.basketball._odds``.

Queries Hard Rock for a single hockey league by ``hr_comp_id`` from
``HOCKEY_LEAGUE_REGISTRY`` and returns a dict keyed by
``"<away_abbr>@<home_abbr>"`` matching what the slate route expects.
"""
from __future__ import annotations

import logging
import time

from . import HOCKEY_LEAGUE_REGISTRY, get_league_config

logger = logging.getLogger(__name__)

_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 120


def fetch_league_odds(league: str, force: bool = False) -> dict[str, dict]:
    """Pull HR's hockey events for ``league``. Returns dict keyed by
    ``away@home`` (abbreviations). Empty dict on any failure — callers
    just see "no odds, no picks" rather than crash."""
    cfg = get_league_config(league)
    comp_id = cfg.get("hr_comp_id")
    if not comp_id:
        return {}
    now = time.time()
    cached = _CACHE.get(league)
    if cached and not force and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]

    try:
        from scrapers.hardrock_odds import _fetch_events_for_comp
    except Exception as e:
        logger.warning("[%s] HR scraper import failed: %s", league, e)
        return {}

    events, err = _fetch_events_for_comp(str(comp_id))
    if err or not events:
        if err:
            logger.warning("[%s] HR comp_id=%s fetch failed: %s",
                           league, comp_id, err)
        _CACHE[league] = (now, {})
        return {}

    name_to_abbr = _build_name_lookup(league)
    out: dict[str, dict] = {}
    for ev in events:
        parsed = _parse_event(ev, name_to_abbr)
        if parsed:
            out[parsed["_key"]] = {
                k: v for k, v in parsed.items() if not k.startswith("_")
            }
    _CACHE[league] = (now, out)
    logger.info("[%s] HR fetched %d games (comp_id=%s)",
                league, len(out), comp_id)
    return out


# ── Team-name resolution ────────────────────────────────────

def _strip_accents(s: str) -> str:
    """Drop combining diacritics so HR's accent-free 'Montreal Pwhl'
    matches our DB's 'Montréal Victoire'. Mirror of the same helper in
    engine.basketball._odds."""
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _build_name_lookup(league: str) -> dict[str, str]:
    """Build {hr_team_name_lower: db_abbr_or_short} for the league.

    NHL uses ``engine.nhl_db.nhl_teams``; AHL/PWHL use the theScore-
    backed ``engine.sports.{league}.db``.

    Keys are lower-cased AND accent-stripped so HR's ASCII team names
    (Montreal Pwhl) resolve against DB rows that carry diacritics
    (Montréal Victoire). City is added as a fallback key because HR
    uses generic "<City> Pwhl" labels for women's hockey instead of the
    actual team mascot.
    """
    out: dict[str, str] = {}
    if league == "nhl":
        try:
            from engine.nhl_db import get_conn
        except Exception:
            return out
        rows = get_conn().execute(
            "SELECT abbreviation, name, NULL AS city FROM nhl_teams"
        ).fetchall()
    else:
        try:
            mod = __import__(f"engine.sports.{league}.db",
                              fromlist=["get_conn"])
        except Exception as e:
            logger.debug("[%s] team lookup import failed: %s", league, e)
            return out
        # ``short_name`` is empty for SofaScore-sourced leagues
        # (AIHL/NZIHL) — only theScore ingest populates it. Read the
        # real ``abbreviation`` column instead so both data sources
        # work without per-source forks. ``city`` is included for the
        # HR-uses-city-only edge case (PWHL).
        try:
            rows = mod.get_conn().execute(
                "SELECT abbreviation, full_name AS name, city FROM teams"
            ).fetchall()
        except Exception:
            # Fallback for older schemas without a city column.
            rows = mod.get_conn().execute(
                "SELECT abbreviation, full_name AS name, NULL AS city "
                "FROM teams"
            ).fetchall()

    def _add(key: str, abbr: str) -> None:
        if not key or not abbr:
            return
        k = _strip_accents(key.strip()).lower()
        if k:
            out.setdefault(k, abbr)

    for r in rows:
        abbr = (r["abbreviation"] or "").strip()
        if not abbr:
            continue
        _add(abbr, abbr)
        _add(r["name"], abbr)
        # Last-word fallback so "Vegas Golden Knights" → VGK
        if r["name"]:
            last = r["name"].split()[-1]
            if len(last) >= 3:
                _add(last, abbr)
        # City fallback so HR's "Montreal Pwhl" / "Ottawa Pwhl" resolve
        # for women's hockey where the mascot is omitted.
        _add(r["city"], abbr)
    return out


def _resolve_team(name: str, name_to_abbr: dict[str, str]) -> str | None:
    if not name:
        return None
    n = _strip_accents(name.strip()).lower()
    if n in name_to_abbr:
        return name_to_abbr[n]
    last = n.split()[-1] if n else ""
    if last and last in name_to_abbr:
        return name_to_abbr[last]
    # Try each token — "Montreal Pwhl" against "montreal" / "pwhl".
    for tok in n.split():
        if tok in name_to_abbr:
            return name_to_abbr[tok]
    for k, abbr in name_to_abbr.items():
        if k and k in n:
            return abbr
    return None


# ── Event parsing ────────────────────────────────────────────

def _parse_event(ev: dict, name_to_abbr: dict[str, str]) -> dict | None:
    """Parse one HR hockey event into our standard odds dict shape.
    Returns None if home/away can't be resolved or the event is live."""
    from scrapers.hardrock_odds import _pick, _event_already_started, _extract_teams
    from scrapers.hardrock_discover import _extract_event_meta, _extract_market_meta
    # Note: don't drop already-started events here. HR pre-flips inplay
    # ~10 minutes before puck drop on AHL games, which would hide the
    # odds from a card whose theScore status is still 'pre_game'. The
    # downstream picker (routes_hockey) only generates picks when our
    # status is 'pre_game', so we never bet on a live game regardless.
    started = _event_already_started(ev)
    meta = _extract_event_meta(ev)
    away_name, home_name = _extract_teams(ev)
    home_abbr = _resolve_team(home_name, name_to_abbr)
    away_abbr = _resolve_team(away_name, name_to_abbr)
    if not (home_abbr and away_abbr):
        return None
    # HR ships eventTime as epoch milliseconds — use it as an
    # authoritative puck-drop time when our theScore ingest doesn't
    # have one (HR-only stub path in routes_hockey).
    et_ms = ev.get("eventTime")
    start_iso = None
    if isinstance(et_ms, (int, float)) and et_ms > 0:
        from datetime import datetime as _dt, timezone as _tz
        try:
            start_iso = _dt.fromtimestamp(et_ms / 1000.0,
                                            tz=_tz.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, OSError):
            pass
    out: dict[str, object] = {
        "_key": f"{away_abbr}@{home_abbr}",
        "home_abbr": home_abbr, "away_abbr": away_abbr,
        "matchup": meta.get("matchup", ""),
        "start_time": start_iso or meta.get("start_time") or None,
        "hr_inplay": started,
        "_spread_lines": [],
    }
    # HR ships markets at the event-top-level under `markets`. Each
    # market has `type` (e.g. ICE_HOCKEY:FTOT:ML), `selection` (list),
    # and selections carry `type` (A/B/AH/BH/Over/Under), `odds`
    # (decimal string), and `name` (with line embedded for spread/OU).
    markets = ev.get("markets") or []
    if not isinstance(markets, list):
        return out
    for mkt in markets:
        if not isinstance(mkt, dict):
            continue
        mtype = (mkt.get("type") or "").upper()
        sels = mkt.get("selection") or []
        if not isinstance(sels, list):
            continue
        if mtype == "ICE_HOCKEY:FTOT:ML":
            for s in sels:
                t = (s.get("type") or "").upper()
                am = _decimal_to_american(s.get("odds"))
                if am is None:
                    continue
                if t == "A":
                    out["home_ml"] = am
                elif t == "B":
                    out["away_ml"] = am
        elif mtype == "ICE_HOCKEY:FTOT:SPRD":
            # HR returns multiple SPRD markets per event — main +/-1.5
            # AND alt lines (2.5, 0.5). The "main" puck line is +/- 1.5;
            # collect every line and pick that one in the canonical
            # direction (favorite at -1.5) as the headline. Don't
            # overwrite blindly — the last alt-line write was a real bug
            # (PROVB @ SPRT showed -2.5 instead of the main -1.5 puck).
            for s in sels:
                t = (s.get("type") or "").upper()
                am = _decimal_to_american(s.get("odds"))
                line = _line_from_name(s.get("name"))
                if am is None or line is None:
                    continue
                # Stash every line we see; pick the canonical one after.
                if t == "AH":
                    out.setdefault("_spread_lines", []).append(
                        ("home", line, am))
                elif t == "BH":
                    out.setdefault("_spread_lines", []).append(
                        ("away", line, am))
        elif mtype == "ICE_HOCKEY:FTOT:OU":
            # HR ships multiple OU markets per event — main line plus
            # alt lines (e.g. 4.5 / 5.5 / 6.5). Stash every selection
            # we see into `_ou_lines`; the canonical main is picked
            # after the full markets loop. Same pattern as SPRD below.
            for s in sels:
                t = (s.get("type") or "").lower()
                am = _decimal_to_american(s.get("odds"))
                line = _line_from_name(s.get("name"))
                if am is None or line is None:
                    continue
                try:
                    dec = float(s.get("odds"))
                except (TypeError, ValueError):
                    dec = None
                slot = out.setdefault("_ou_lines", {}).setdefault(line, {})
                if t == "over":
                    slot["over_am"] = am
                    slot["over_dec"] = dec
                elif t == "under":
                    slot["under_am"] = am
                    slot["under_dec"] = dec

    # Canonical OU line: pick the line where over and under decimal
    # odds are closest (the "main"); alt lines come with skewed odds
    # (over_dec << under_dec or vice versa). Previously a single
    # over_under field was overwritten per-market and the last OU
    # block in HR's payload won, silently picking an alt line.
    ou_lines = out.pop("_ou_lines", {}) or {}
    complete = [
        (line, info) for line, info in ou_lines.items()
        if info.get("over_dec") is not None
        and info.get("under_dec") is not None
    ]
    if complete:
        main_line, main_info = min(
            complete,
            key=lambda kv: abs(kv[1]["over_dec"] - kv[1]["under_dec"]),
        )
        out["over_under"] = main_line
        out["over_odds"] = main_info.get("over_am")
        out["under_odds"] = main_info.get("under_am")

    # Canonical puck line: hockey's main spread is always +/-1.5 with
    # the ML favorite at -1.5. HR returns multiple SPRD markets at
    # different lines (1.5, 2.5, 0.5) and both directions; we pick the
    # 1.5 pair in the canonical favorite-laying direction. If HR didn't
    # ship a 1.5 market, the card shows no spread — alt lines are not a
    # substitute for the headline puck line.
    lines = out.pop("_spread_lines", []) or []
    home_15 = [(l, o) for (s, l, o) in lines if s == "home" and abs(l) == 1.5]
    away_15 = [(l, o) for (s, l, o) in lines if s == "away" and abs(l) == 1.5]
    home_ml = out.get("home_ml")
    away_ml = out.get("away_ml")
    home_is_fav = (home_ml is not None and away_ml is not None
                    and home_ml < away_ml)
    if home_15 and away_15:
        if home_is_fav:
            picked_h = next((p for p in home_15 if p[0] == -1.5), home_15[0])
            picked_a = next((p for p in away_15 if p[0] == 1.5), away_15[0])
        else:
            picked_h = next((p for p in home_15 if p[0] == 1.5), home_15[0])
            picked_a = next((p for p in away_15 if p[0] == -1.5), away_15[0])
        out["home_spread_point"] = picked_h[0]
        out["home_spread_odds"] = picked_h[1]
        out["away_spread_point"] = picked_a[0]
        out["away_spread_odds"] = picked_a[1]
    return out


def _decimal_to_american(v) -> int | None:
    """HR ships odds in European decimal format. Convert to American
    so picks_core + the factor pickers see the same shape they do for
    NHL (which already gets American from the live odds path)."""
    if v is None:
        return None
    try:
        d = float(v)
    except (TypeError, ValueError):
        return None
    if d <= 1.0:
        return None
    if d >= 2.0:
        return round((d - 1.0) * 100)
    return round(-100.0 / (d - 1.0))


def _line_from_name(name: str | None) -> float | None:
    """Spread/OU lines live inside the selection name (e.g. 'Over 4.5',
    'MTL Victoire (W) -1.5'). Pull the last numeric token with a sign."""
    if not name:
        return None
    import re
    # Try signed (e.g. -1.5, +1.5) first, then plain (e.g. 4.5)
    m = re.search(r"([+-]?\d+(?:\.\d+)?)\s*$", name.strip())
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None
