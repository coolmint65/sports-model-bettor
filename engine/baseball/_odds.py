"""Hard Rock odds fetcher for baseball framework leagues. HR ships
college baseball under BASEBALL:FTEI:* (Full Time including Extra
Innings) — separate from MLB's BASEBALL:FTOT:* shape used by the
older MLB-specific scraper. Three markets per event: ML, SPRD, OU.
"""
from __future__ import annotations

import logging
import time
import unicodedata

from . import get_league_config
from ._db import get_conn


logger = logging.getLogger(__name__)


_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 120


def fetch_league_odds(league: str, force: bool = False
                       ) -> dict[str, dict]:
    now = time.time()
    cached = _CACHE.get(league)
    if cached and not force and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]
    cfg = get_league_config(league)
    comp_id = cfg.get("hr_comp_id")
    if not comp_id:
        return {}
    from scrapers.hardrock_odds import _fetch_events_for_comp
    events, err = _fetch_events_for_comp(str(comp_id))
    if err or not events:
        logger.debug("[baseball:%s] HR fetch err=%s n=%d",
                      league, err, 0 if not events else len(events))
        _CACHE[league] = (now, {})
        return {}
    name_to_abbr = _build_name_lookup(league)
    out: dict[str, dict] = {}
    for ev in events:
        parsed = _parse_event(ev, name_to_abbr)
        if not parsed:
            continue
        key = f"{parsed['away_abbr']}@{parsed['home_abbr']}"
        out[key] = parsed
    _CACHE[league] = (now, out)
    logger.info("[baseball:%s] HR fetched %d games (comp=%s)",
                league, len(out), comp_id)
    return out


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s or "")
                   if not unicodedata.combining(c))


def _normalize(s: str) -> str:
    """Lower + strip accents + flatten parens. ``Miami (FL)`` becomes
    ``miami fl`` so the state qualifier survives as a normal token instead
    of becoming a noise token like ``(fl)`` that would pollute the lookup
    (St. Thomas (FL) Bobcats was poisoning the ``(fl)`` token, making
    HR's ``Miami (FL)`` resolve to St. Thomas)."""
    n = _strip_accents(s or "").lower()
    n = n.replace("(", " ").replace(")", " ")
    return " ".join(n.split())


def _build_name_lookup(league: str) -> dict[str, str]:
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT id, name, abbreviation, short_name, location FROM teams"
    ).fetchall()
    out: dict[str, str] = {}
    ambiguous: set[str] = set()

    def add(key: str, abbr: str) -> None:
        if not key or not abbr:
            return
        norm = _normalize(key)
        if not norm:
            return
        existing = out.get(norm)
        if existing is None:
            out[norm] = abbr
        elif existing != abbr:
            ambiguous.add(norm)

    for r in rows:
        abbr = (r["abbreviation"] or "").strip()
        if not abbr:
            continue
        add(abbr, abbr)
        for field in (r["name"], r["short_name"], r["location"]):
            if field:
                add(field, abbr)
                # Tokens get added too so single-word HR names like
                # "Pittsburgh" resolve against multi-word DB names like
                # "Pittsburgh Panthers". Skip 2-char state codes that
                # are now valid tokens after the paren-flatten (FL, OH,
                # TX, etc) — those are too generic.
                for tok in _normalize(field).split():
                    if len(tok) >= 4:
                        add(tok, abbr)
    for k in ambiguous:
        out.pop(k, None)
    return out


# HR ships some teams with state qualifiers (e.g. "Miami (FL)") that
# don't match the DB's bare team name ("Miami Hurricanes"). Without an
# override, the first-token loop would either miss entirely or hit the
# wrong sibling (Miami (OH) RedHawks). Per-league hand-picked map of
# HR's normalized name → DB abbreviation. Add entries as ambiguities
# surface during the season.
_HR_NAME_OVERRIDES: dict[str, str] = {
    # college baseball
    "miami fl": "MIA",
}


def _resolve_team(name: str, name_to_abbr: dict[str, str]) -> str | None:
    if not name:
        return None
    n = _normalize(name)
    if n in _HR_NAME_OVERRIDES:
        return _HR_NAME_OVERRIDES[n]
    if n in name_to_abbr:
        return name_to_abbr[n]
    # First-word match — common in college baseball where HR ships
    # "Pittsburgh" while DB has "Pittsburgh Panthers" with abbr PITT.
    first = n.split()[0] if n else ""
    if first and len(first) >= 4 and first in name_to_abbr:
        return name_to_abbr[first]
    for tok in n.split():
        if len(tok) >= 4 and tok in name_to_abbr:
            return name_to_abbr[tok]
    return None


def _decimal_to_american(v) -> int | None:
    if v is None:
        return None
    try:
        d = float(v)
    except (TypeError, ValueError):
        return None
    if d <= 1.0:
        return None
    if d >= 2.0:
        return int(round((d - 1.0) * 100))
    return int(round(-100 / (d - 1.0)))


def _line_from_name(name: str | None) -> float | None:
    if not name:
        return None
    for tok in name.replace("(", " ").replace(")", " ").split():
        try:
            return float(tok)
        except ValueError:
            continue
    return None


def _parse_event(ev: dict, name_to_abbr: dict[str, str]) -> dict | None:
    parts = ev.get("participants") or []
    if len(parts) < 2:
        return None
    home_name = next((p.get("name") for p in parts if p.get("position") == 0), None)
    away_name = next((p.get("name") for p in parts if p.get("position") == 1), None)
    if not (home_name and away_name):
        return None
    home_abbr = _resolve_team(home_name, name_to_abbr)
    away_abbr = _resolve_team(away_name, name_to_abbr)
    if not (home_abbr and away_abbr):
        logger.debug("[baseball] unresolved teams: %s / %s",
                      home_name, away_name)
        return None

    out: dict = {
        "hr_event_id":    ev.get("id"),
        "start_time":     ev.get("eventTime"),
        "home_abbr":      home_abbr,
        "away_abbr":      away_abbr,
        "home_full_name": home_name,
        "away_full_name": away_name,
    }
    spread_candidates: list[tuple[str, float, int]] = []
    ou_lines: dict[float, dict] = {}
    for mkt in (ev.get("markets") or []):
        mtype = (mkt.get("type") or "").upper()
        sels = mkt.get("selection") or []
        if mtype == "BASEBALL:FTEI:ML":
            for s in sels:
                t = (s.get("type") or "").upper()
                am = _decimal_to_american(s.get("odds"))
                if am is None:
                    continue
                if t == "A":
                    out["home_ml"] = am
                elif t == "B":
                    out["away_ml"] = am
        elif mtype == "BASEBALL:FTEI:SPRD":
            for s in sels:
                t = (s.get("type") or "").upper()
                am = _decimal_to_american(s.get("odds"))
                line = _line_from_name(s.get("name"))
                if am is None or line is None:
                    continue
                if t == "AH":
                    spread_candidates.append(("home", line, am))
                elif t == "BH":
                    spread_candidates.append(("away", line, am))
        elif mtype == "BASEBALL:FTEI:OU":
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
                slot = ou_lines.setdefault(line, {})
                if t == "over":
                    slot["over_am"] = am
                    slot["over_dec"] = dec
                elif t == "under":
                    slot["under_am"] = am
                    slot["under_dec"] = dec

    if spread_candidates:
        lines: dict[float, dict] = {}
        for side, line, am in spread_candidates:
            slot = lines.setdefault(abs(line), {})
            if side == "home":
                slot["home_line"] = line
                slot["home_odds"] = am
            else:
                slot["away_line"] = line
                slot["away_odds"] = am
        complete = [(k, v) for k, v in lines.items()
                     if "home_line" in v and "away_line" in v]
        if complete:
            # College baseball spreads vary widely (-1.5 typical, but
            # -3.5 / -5.5 for power-vs-cupcake mismatches). Pick the
            # smallest-magnitude line as the "main" (closest to even-
            # vig) — matches the hockey / football pattern.
            _, info = min(complete, key=lambda kv: kv[0])
            out["home_spread_point"] = info["home_line"]
            out["home_spread_odds"]  = info["home_odds"]
            out["away_spread_point"] = info["away_line"]
            out["away_spread_odds"]  = info["away_odds"]

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
        out["over_odds"]  = main_info.get("over_am")
        out["under_odds"] = main_info.get("under_am")

    return out
