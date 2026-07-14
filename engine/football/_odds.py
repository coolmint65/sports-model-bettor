"""Hard Rock odds fetcher for football leagues.

Wraps ``scrapers.hardrock_odds._fetch_events_for_comp`` and translates
the per-event payload into the (home_abbr, away_abbr) → odds dict
shape the picks pipeline expects. Mirrors ``engine.hockey._odds``.

Market types we extract for each event:
    AMERICAN_FOOTBALL:FTOT:ML    home_ml / away_ml
    AMERICAN_FOOTBALL:FTOT:SPRD  spread + odds (canonical line ±N)
    AMERICAN_FOOTBALL:FTOT:OU    total + over/under odds
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
    """Return ``{ "AWAY@HOME": odds_dict, ... }`` for the league's
    active HR events. Empty dict on any failure."""
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
        logger.debug("[football:%s] HR fetch err=%s n=%d",
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
    logger.info("[football:%s] HR fetched %d games (comp=%s)",
                league, len(out), comp_id)
    return out


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s or "")
                   if not unicodedata.combining(c))


def _build_name_lookup(league: str) -> dict[str, str]:
    """Map normalized team names (and sub-tokens) to our DB
    abbreviation. UFL team names are short ("Orlando Storm", "DC
    Defenders") and don't collide much, but the lookup respects the
    same ambiguity-detection pattern soccer uses."""
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT id, name, abbreviation, short_name, location FROM teams"
    ).fetchall()
    out: dict[str, str] = {}
    ambiguous: set[str] = set()

    def add(key: str, abbr: str) -> None:
        if not key or not abbr:
            return
        norm = _strip_accents(key).strip().lower()
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
                for tok in field.split():
                    if len(tok) >= 4:
                        add(tok, abbr)
    for k in ambiguous:
        out.pop(k, None)
    return out


def _resolve_team(name: str, name_to_abbr: dict[str, str]) -> str | None:
    if not name:
        return None
    n = _strip_accents(name).strip().lower()
    if n in name_to_abbr:
        return name_to_abbr[n]
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
        logger.debug("[football] unresolved teams: %s / %s",
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
        if mtype == "AMERICAN_FOOTBALL:FTOT:ML":
            for s in sels:
                t = (s.get("type") or "").upper()
                am = _decimal_to_american(s.get("odds"))
                if am is None:
                    continue
                if t == "A":
                    out["home_ml"] = am
                elif t == "B":
                    out["away_ml"] = am
        elif mtype == "AMERICAN_FOOTBALL:FTOT:SPRD":
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
        elif mtype == "AMERICAN_FOOTBALL:FTOT:OU":
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

    # Canonical spread: pick the favorite's negative-line pair. If
    # multiple lines are present (rare for football), prefer the
    # smallest absolute spread — the "main" line is always the
    # closest-to-even one.
    if spread_candidates:
        # Pair (home_line, home_odds, away_line, away_odds) per line.
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
            # Smallest-magnitude spread is the canonical main; alt
            # lines (less common in football than hockey) get dropped.
            _, info = min(complete, key=lambda kv: kv[0])
            out["home_spread_point"] = info["home_line"]
            out["home_spread_odds"]  = info["home_odds"]
            out["away_spread_point"] = info["away_line"]
            out["away_spread_odds"]  = info["away_odds"]

    # Canonical OU: most-balanced (over/under decimal odds closest to
    # each other). UFL typically only ships one OU line, but the
    # pattern matches engine.hockey._odds for safety.
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
