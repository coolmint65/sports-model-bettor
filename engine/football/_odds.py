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
        # WARNING, not debug (2026-10-25): three restarts in a row booted into an empty
        # board (CF breaker) and nothing above DEBUG said why — the slate then computed
        # every locked game pick-less. Surface the actual HR error text.
        logger.warning("[football:%s] HR fetch returned no events (err=%s) — board %s",
                       league, err, "kept last-good" if (_CACHE.get(league) or (0, {}))[1] else "EMPTY")
        # Transient-failure resilience (Austin 2026-10-14): HR intermittently
        # 403s (CloudFlare; the shared CF breaker backs off ~300s). Caching an
        # EMPTY odds dict on failure blanked the whole slate until the next
        # success (the CFB midweek "no odds" bug). Keep the LAST-GOOD odds so a
        # transient block never wipes the board; a later success refreshes them.
        prev = _CACHE.get(league)
        if prev and prev[1]:
            _CACHE[league] = (now, prev[1])
            return prev[1]
        _CACHE[league] = (now, {})
        return {}

    name_to_abbr = _build_name_lookup(league)
    out: dict[str, dict] = {}
    for ev in events:
        parsed = _parse_event(ev, name_to_abbr, league)
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


_EXACT: dict[str, dict[str, str]] = {}   # league -> exact-field lookup (never token-popped)
# HR spelling -> ESPN exact key (lowercase). Extend as unresolved names show up in the log.
_HR_ALIASES = {
    "albany": "ualbany",
    "umass": "umass",
    "hawaii": "hawai'i",
    "ole miss": "ole miss",
    "miami (fl)": "miami",
    "miami (oh)": "miami (oh)",
    "app state": "appalachian state",
    "usf": "south florida",
    "utsa": "utsa",
    "utep": "utep",
    "ul monroe": "ul monroe",
    "sam houston": "sam houston",
    # 2026-10-31: HR spellings that left today's cards unpriced (Austin: UConn/AF, USM/ULM)
    "connecticut": "uconn",
    "north carolina state": "nc state",
    "st. thomas (mn)": "st. thomas",
    "southeastern louisiana": "se louisiana",
}

# Direct HR-name -> our abbr fallback (Austin 2026-09-19): a handful of college
# names either clash in the exact lookup (ambiguous "Charlotte"/"Troy") or use a
# form our teams table doesn't carry ("Louisiana Monroe" vs "UL Monroe",
# "Appalachian State" vs "App State"). Applied only AFTER the normal resolution
# fails, so it never overrides a correct match. Extend as new misses surface.
_HR_DIRECT = {
    "louisiana monroe": "ULM",
    "appalachian state": "APP",
    "charlotte": "CLT",
    "troy": "TROY",
    "sam houston state": "SHSU",
    "gardner webb": "GWEB",
    "south dakota st": "SDST",
    "north dakota st": "NDSU",
}


def _build_exact_lookup(league: str) -> dict[str, str]:
    """2026-09-03: college namespaces (755 teams) make single tokens ambiguous
    ('colorado' = Colorado + Colorado State, 'utah', 'illinois', 'delaware' …), and the
    token-popping in _build_name_lookup then deleted the EXACT location key too, so HR's
    'Colorado' / 'Utah' / 'Delaware' never resolved (8 of 11 games unpriced on 2026-09-03).
    Exact matches on abbreviation / name / short_name / location win before any token."""
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT id, name, abbreviation, short_name, location FROM teams"
    ).fetchall()
    out: dict[str, str] = {}
    clash: set[str] = set()
    for r in rows:
        abbr = (r["abbreviation"] or "").strip()
        if not abbr:
            continue
        for field in (abbr, r["name"], r["short_name"], r["location"]):
            k = _strip_accents(field or "").strip().lower()
            if not k:
                continue
            if k in out and out[k] != abbr:
                clash.add(k)
            else:
                out[k] = abbr
    for k in clash:
        out.pop(k, None)
    return out


def _resolve_team(name: str, name_to_abbr: dict[str, str],
                  league: str | None = None) -> str | None:
    if not name:
        return None
    n = _strip_accents(name).strip().lower()
    # HR prefixes ranked teams: '#25 Missouri' -> 'missouri'
    if n.startswith('#'):
        n = n.split(' ', 1)[1] if ' ' in n else n
    n = _HR_ALIASES.get(n, n)
    if league:
        ex = _EXACT.get(league)
        if ex is None:
            ex = _EXACT[league] = _build_exact_lookup(league)
        if n in ex:
            return ex[n]
        # HR 'Georgia Tech' vs ESPN location 'Georgia Tech' handled above; also try
        # common HR spellings: 'St.' <-> 'State', 'Pine Bluff' etc. via a loose exact pass
        n2 = n.replace(' st.', ' state').replace(' st ', ' state ')
        if n2 in ex:
            return ex[n2]
    if n in name_to_abbr:
        return name_to_abbr[n]
    for tok in n.split():
        if len(tok) >= 4 and tok in name_to_abbr:
            return name_to_abbr[tok]
    # Direct-alias fallback for known ambiguous/mismatched college names.
    if n in _HR_DIRECT:
        return _HR_DIRECT[n]
    # HR hyphenates some schools ('Louisiana-Monroe'); retry once with a space
    if '-' in n:
        return _resolve_team(n.replace('-', ' '), name_to_abbr, league)
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


_PERIODS = ("H1", "H2", "Q1", "Q2", "Q3", "Q4")   # periods price_period can price (see _derivatives._PERIOD_PTS_FRAC)


def _yes_no(sels):
    """(yes_am, no_am) for a Yes/No market, or None unless BOTH legs are priced."""
    y = n = None
    for s in sels:
        t = (s.get("type") or "").lower()
        am = _decimal_to_american(s.get("odds"))
        if am is None:
            continue
        if t == "yes":
            y = am
        elif t == "no":
            n = am
    return (y, n) if (y is not None and n is not None) else None


def _parse_event(ev: dict, name_to_abbr: dict[str, str],
                 league: str | None = None) -> dict | None:
    from scrapers.hardrock_odds import _extract_teams
    # HR dropped the participants[] field in an Oct-2026 event-shape change
    # (now ships a single `name` like "Giants vs Saints"); read teams via the
    # shared _extract_teams helper (same path hockey uses), then fall back to
    # the legacy participants[] order. (MintyBot 2026-10-13)
    away_name, home_name = _extract_teams(ev)
    if not (home_name and away_name):
        parts = ev.get("participants") or []
        if len(parts) >= 2:
            home_name = next((p.get("name") for p in parts if p.get("position") == 0), None)
            away_name = next((p.get("name") for p in parts if p.get("position") == 1), None)
    if not (home_name and away_name):
        return None
    home_abbr = _resolve_team(home_name, name_to_abbr, league)
    away_abbr = _resolve_team(away_name, name_to_abbr, league)
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
    home_tt: dict[float, dict] = {}
    away_tt: dict[float, dict] = {}
    _period: dict = {}          # {period: {spreads, ou, dnb, axb, home_tt, away_tt, btts}} for price_period
    # Game-script markets priceable from the score sim (2026-10-25, Austin "work on 3 to 5"):
    # both-teams-to-score (+ "N+ points" variants) and lead-after-every-quarter. Odd/even,
    # exact points and overtime need DISCRETE scores -> the #4 scoring-composition layer.
    btts_pts = None; btts_x: dict = {}; leq_home = leq_away = None
    for mkt in (ev.get("markets") or []):
        mtype = (mkt.get("type") or "").upper()
        sels = mkt.get("selection") or []
        # ── Period markets → collected for price_period (MintyBot 2026-10-16; all six periods +
        # period TEAM totals 2026-10-25, Austin "I like 1 and 2": H2/Q2-Q4 lines (~1,300/Sunday)
        # and P:A:OU/P:B:OU team totals (~650) were parsed away here and never priced) ──
        mper = (mkt.get("period") or "").upper()
        if mper in _PERIODS and ":P:" in mtype:
            sub = mtype.split(":P:", 1)[1]
            pdd = _period.setdefault(mper, {"spreads": [], "ou": {}, "dnb": {}, "axb": {},
                                            "home_tt": {}, "away_tt": {}, "btts": None})
            if sub == "BTTSPTS":                 # "1st Half Both Teams To Score"
                _yn = _yes_no(sels)
                if _yn:
                    pdd["btts"] = _yn
                continue
            if sub == "SPRD":
                for s in sels:
                    t = (s.get("type") or "").upper()
                    am = _decimal_to_american(s.get("odds"))
                    line = _line_from_name(s.get("name"))
                    if am is None or line is None:
                        continue
                    if t == "AH":
                        pdd["spreads"].append(("home", line, am))
                    elif t == "BH":
                        pdd["spreads"].append(("away", line, am))
            elif sub == "OU":
                for s in sels:
                    t = (s.get("type") or "").lower()
                    am = _decimal_to_american(s.get("odds"))
                    line = _line_from_name(s.get("name"))
                    if am is None or line is None:
                        continue
                    slot = pdd["ou"].setdefault(line, {})
                    if t == "over":
                        slot["over_am"] = am
                    elif t == "under":
                        slot["under_am"] = am
            elif sub in ("A:OU", "B:OU"):      # period TEAM totals (A = home, B = away, as FTOT:A/B:OU)
                tt = pdd["home_tt"] if sub == "A:OU" else pdd["away_tt"]
                for s in sels:
                    t = (s.get("type") or "").lower()
                    am = _decimal_to_american(s.get("odds"))
                    line = _line_from_name(s.get("name"))
                    if am is None or line is None:
                        continue
                    slot = tt.setdefault(line, {})
                    if t == "over":
                        slot["over_am"] = am
                    elif t == "under":
                        slot["under_am"] = am
            elif sub == "DNB":
                for s in sels:
                    t = (s.get("type") or "").upper()
                    am = _decimal_to_american(s.get("odds"))
                    if am is None:
                        continue
                    if t == "A":
                        pdd["dnb"]["home"] = am
                    elif t == "B":
                        pdd["dnb"]["away"] = am
            elif sub == "AXB":
                for s in sels:
                    t = (s.get("type") or "").upper()
                    am = _decimal_to_american(s.get("odds"))
                    if am is None:
                        continue
                    if t == "A":
                        pdd["axb"]["home"] = am
                    elif t in ("X", "DRAW"):        # HR ships the tie leg as type 'Draw' (verified live 2026-10-25) — 'X' never matched, so every period 3-way winner (57 mkts/Sunday) parsed to nothing
                        pdd["axb"]["draw"] = am
                    elif t == "B":
                        pdd["axb"]["away"] = am
            continue
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
                try:
                    sdec = float(s.get("odds"))
                except (TypeError, ValueError):
                    sdec = None
                if t == "AH":
                    spread_candidates.append(("home", line, am, sdec))
                elif t == "BH":
                    spread_candidates.append(("away", line, am, sdec))
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
        elif mtype in ("AMERICAN_FOOTBALL:FTOT:A:OU", "AMERICAN_FOOTBALL:FTOT:B:OU"):
            tt = home_tt if mtype.endswith("A:OU") else away_tt
            for s in sels:
                t = (s.get("type") or "").lower()
                am = _decimal_to_american(s.get("odds"))
                line = _line_from_name(s.get("name"))
                if am is None or line is None:
                    continue
                slot = tt.setdefault(line, {})
                if t == "over":
                    slot["over_am"] = am
                elif t == "under":
                    slot["under_am"] = am
        elif mtype == "AMERICAN_FOOTBALL:FT:BTTSPTS":        # Both Teams To Score (>= 1 point)
            btts_pts = _yes_no(sels) or btts_pts
        elif mtype == "AMERICAN_FOOTBALL:FT:BTTSXPTS":       # "Both Teams To Score 20+ Points"
            _m = __import__("re").search(r"(\d+)\+", str(mkt.get("name") or ""))
            _yn = _yes_no(sels)
            if _m and _yn:
                btts_x[int(_m.group(1))] = _yn
        elif mtype in ("AMERICAN_FOOTBALL:FT:A:LEADEVERYQUARTER", "AMERICAN_FOOTBALL:FT:B:LEADEVERYQUARTER"):
            _yn = _yes_no(sels)
            if _yn:
                if mtype.endswith("A:LEADEVERYQUARTER"):
                    leq_home = _yn
                else:
                    leq_away = _yn

    # Canonical spread: pick the favorite's negative-line pair. If
    # multiple lines are present (rare for football), prefer the
    # smallest absolute spread — the "main" line is always the
    # closest-to-even one.
    if spread_candidates:
        # Pair (home_line, home_odds, away_line, away_odds) per line.
        lines: dict[float, dict] = {}
        for side, line, am, sdec in spread_candidates:
            # Keyed by the HOME team's signed point (2026-10-26). abs(line) merged "DEL -1.5 / WKU +1.5"
            # with "WKU -1.5 / DEL +1.5" into one slot -- the later market overwrote the earlier one, so
            # HR's main line (DEL -1.5 -105) became the alternate WKU -1.5 +100 on the card (Austin).
            slot = lines.setdefault(line if side == "home" else -line, {})
            if side == "home":
                slot["home_line"] = line
                slot["home_odds"] = am
                slot["home_dec"] = sdec
            else:
                slot["away_line"] = line
                slot["away_odds"] = am
                slot["away_dec"] = sdec
        complete = [(k, v) for k, v in lines.items()
                     if "home_line" in v and "away_line" in v]
        if complete:
            # 2026-09-03: HR ships a full ALT-SPREAD ladder for CFB (e.g. WAKE -15.5 @ -400
            # … -27.5 @ -110 … -40.5 @ +300). "Smallest |spread|" grabbed the -400/+300
            # rung and every dog looked like a +30pp edge. The main line is the most
            # BALANCED pair (same rule the O/U block below already uses).
            def _bal(kv):
                v = kv[1]
                if v.get("home_dec") is None or v.get("away_dec") is None:
                    return 99.0
                return abs(v["home_dec"] - v["away_dec"])
            _, info = min(complete, key=_bal)
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

    # Full alt ladders + team totals for the derivative pricer (Austin 2026-10-13).
    _sl: dict = {}
    for _side, _line, _am, _sdec in spread_candidates:
        _slot = _sl.setdefault(_line if _side == "home" else -_line, {})   # signed home point (2026-10-26)
        if _side == "home":
            _slot["home_line"], _slot["home_odds"] = _line, _am
        else:
            _slot["away_line"], _slot["away_odds"] = _line, _am
    out["alt_spreads"] = [(v["home_line"], v["home_odds"], v.get("away_odds"))
                          for v in _sl.values() if "home_line" in v and "away_line" in v]
    out["alt_totals"] = [(ln, i.get("over_am"), i.get("under_am")) for ln, i in ou_lines.items()
                         if i.get("over_am") is not None and i.get("under_am") is not None]
    out["home_team_totals"] = [(ln, i.get("over_am"), i.get("under_am")) for ln, i in home_tt.items()
                               if i.get("over_am") is not None and i.get("under_am") is not None]
    out["away_team_totals"] = [(ln, i.get("over_am"), i.get("under_am")) for ln, i in away_tt.items()
                               if i.get("over_am") is not None and i.get("under_am") is not None]
    out["btts"] = btts_pts                                   # (yes_am, no_am) or None
    out["btts_x"] = sorted(btts_x.items())                   # [(N, (yes_am, no_am)), ...]
    out["leq_home"], out["leq_away"] = leq_home, leq_away    # (yes_am, no_am) or None

    # Period (1H/1Q) ladders + winners for price_period (MintyBot 2026-10-16).
    for _per, _pd in _period.items():
        _ls: dict = {}
        for _side, _line, _am in _pd["spreads"]:
            _slot = _ls.setdefault(_line if _side == "home" else -_line, {})   # signed home point (2026-10-26)
            if _side == "home":
                _slot["home_line"], _slot["home_odds"] = _line, _am
            else:
                _slot["away_line"], _slot["away_odds"] = _line, _am
        _spreads = [(v["home_line"], v["home_odds"], v.get("away_odds"))
                    for v in _ls.values() if "home_line" in v and "away_line" in v]
        _totals = [(ln, i.get("over_am"), i.get("under_am")) for ln, i in _pd["ou"].items()
                   if i.get("over_am") is not None and i.get("under_am") is not None]
        _ml2 = ((_pd["dnb"].get("home"), _pd["dnb"].get("away"))
                if _pd["dnb"].get("home") is not None and _pd["dnb"].get("away") is not None else None)
        _ml3 = ((_pd["axb"].get("home"), _pd["axb"].get("draw"), _pd["axb"].get("away"))
                if all(_pd["axb"].get(k) is not None for k in ("home", "draw", "away")) else None)
        _htt = [(ln, i.get("over_am"), i.get("under_am")) for ln, i in _pd["home_tt"].items()
                if i.get("over_am") is not None and i.get("under_am") is not None]
        _att = [(ln, i.get("over_am"), i.get("under_am")) for ln, i in _pd["away_tt"].items()
                if i.get("over_am") is not None and i.get("under_am") is not None]
        out[_per.lower()] = {          # "h1" / "h2" / "q1" .. "q4"
            "spreads": _spreads, "totals": _totals, "ml2": _ml2, "ml3": _ml3,
            "home_team_totals": _htt, "away_team_totals": _att,
            "btts": _pd.get("btts"),
        }
    return out
