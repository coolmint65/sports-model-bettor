"""HR odds fetcher for framework-managed basketball leagues.

Bypasses the sport-keyed scraper in ``scrapers.hardrock_odds`` because
that module's ``_parse_response`` hardcodes US team-name maps. Here we
go straight to ``_fetch_events_for_comp(comp_id)`` and parse the events
ourselves using the league's own teams table for name → abbreviation
resolution.

Public:
    fetch_league_odds(league) -> dict[game_key, odds_dict]

odds_dict shape matches what ``basketball._picks.generate_picks``
expects::

    {
      "ml":     {"home": int, "away": int},
      "spread": {"home_pt": float, "home_odds": int,
                 "away_pt": float, "away_odds": int},
      "total":  {"line": float, "over_odds": int, "under_odds": int},
      "comp":   "WNBA",       # for debug
      "hr_event_id": "...",   # for joining back to HR
    }

Game keys are ``f"{away_abbr}@{home_abbr}"`` so the slate route can
match HR rows to the framework's games table without depending on
HR's event id (which sometimes drifts).
"""
from __future__ import annotations

import logging
import time
from typing import Any

from ._config import get_league_config
from ._db import get_conn, teams_table

logger = logging.getLogger(__name__)


# Per-process cache so a slate refresh doesn't re-hit HR for every
# league call. 2-min TTL — short enough that line moves get caught,
# long enough that a tab refresh feels free.
_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 120


def fetch_league_odds(league: str, force: bool = False) -> dict[str, dict]:
    """Fetch HR's basketball odds for ``league``. Returns a dict keyed
    by ``"<away_abbr>@<home_abbr>"`` so the slate route can match
    games-table rows by the same key.

    Empty dict on any failure — callers fall through to "no odds, no
    picks" rather than crashing the slate."""
    cfg = get_league_config(league)
    comp_id = cfg.get("hr_comp_id")
    if not comp_id:
        logger.debug("[%s] no hr_comp_id in registry — skipping odds fetch", league)
        return {}

    now = time.time()
    cached = _CACHE.get(league)
    if cached and not force and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]

    try:
        from scrapers.hardrock_odds import (
            _fetch_events_for_comp,
            _fetch_sports_tree,
            _resolve_comp_ids,
        )
    except Exception as e:
        logger.warning("[%s] HR scraper import failed: %s", league, e)
        return {}

    # Comp ID set: registry's anchor first, plus any sibling comps the
    # central resolver finds in the sports tree (e.g. WNBA Commissioner's
    # Cup, NCAAM conference tournaments). Mid-season cup games live in a
    # separate comp that disappears once the tournament ends, so pinning
    # to a hardcoded ID would silently drop coverage; resolving from the
    # tree picks them up while they're live without registry churn.
    comp_ids: list[str] = [str(comp_id)]
    try:
        tree, tree_err = _fetch_sports_tree()
        if not tree_err and tree:
            for cid in _resolve_comp_ids(league, tree):
                if cid and cid not in comp_ids:
                    comp_ids.append(cid)
    except Exception as e:
        logger.debug("[%s] sibling-comp discovery failed: %s", league, e)

    events: list[dict] = []
    last_err: str | None = None
    for cid in comp_ids:
        ev_list, err = _fetch_events_for_comp(cid)
        if err:
            last_err = err
            logger.warning("[%s] HR comp_id=%s fetch failed: %s",
                           league, cid, err)
            continue
        if ev_list:
            events.extend(ev_list)
    if not events:
        if last_err:
            logger.warning("[%s] HR all comps failed; last err: %s",
                           league, last_err)
        _CACHE[league] = (now, {})
        return {}

    name_to_abbr = _build_name_lookup(league)
    out: dict[str, dict] = {}
    for ev in events:
        parsed = _parse_event(ev, name_to_abbr,
                               league_name=cfg.get("hr_comp_name") or league,
                               league_key=league)
        if not parsed:
            continue
        key = parsed["_key"]
        # Dedup collision: HR sometimes ships the same matchup under
        # two events (a "real" one and a phantom/duplicate stale one).
        # Toronto Tempo @ LA on 2026-05-15 had a Fri 22:00 ET event
        # with 108 markets AND a Sun 19:00 ET event with 2 markets;
        # last-write-wins left only the 2-market sparse row. Prefer
        # the richer event when both exist.
        existing = out.get(key)
        clean = {k: v for k, v in parsed.items() if not k.startswith("_")}
        if existing and _event_richness(existing) > _event_richness(clean):
            continue
        out[key] = clean
    _CACHE[league] = (now, out)
    logger.info("[%s] HR fetched %d games (comp_id=%s)",
                league, len(out), comp_id)
    return out


def _event_richness(parsed: dict) -> int:
    """Score an HR-parsed odds dict by how many core markets it carries.
    Used to break ties when the same matchup key appears twice in HR's
    event list — prefer the richer event."""
    score = 0
    for k in ("ml", "spread", "total", "q1"):
        v = parsed.get(k)
        if isinstance(v, dict) and v:
            score += 1
    # Each alt + period market adds a smaller bump so a rich main bundle
    # always wins over an alt-only ghost row.
    if isinstance(parsed.get("alt_spreads"), list):
        score += len(parsed["alt_spreads"]) // 4
    if isinstance(parsed.get("alt_totals"), list):
        score += len(parsed["alt_totals"]) // 4
    return score


# ── Team-name resolution ──────────────────────────────────────

def _strip_accents(s: str) -> str:
    """Strip diacritics so HR's 'Fenerbahçe' / 'Žalgiris' / 'Crvena
    Zvezda' resolve against accent-stripped DB rows. Mirrors the
    same normalization tennis already uses."""
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _build_name_lookup(league: str) -> dict[str, str]:
    """Map every plausible HR team-name spelling to our abbreviation.

    HR ships team names like 'Las Vegas Aces' / 'New York Liberty';
    our DB has 'Las Vegas Aces' / 'New York Liberty' (full) and 'LV' /
    'NY' (abbr). Build both directions so the parser can match either.
    Also strips obvious noise (city prefixes, sponsor suffixes) so
    'KOSNER BASKONIA VITORIA-GASTEIZ' resolves to 'BAS'."""
    conn = get_conn(league)
    tbl = teams_table(league)
    rows = conn.execute(
        f"SELECT abbreviation, name FROM {tbl}"
    ).fetchall()
    out: dict[str, str] = {}

    def _add(key: str, abbr: str) -> None:
        if not key:
            return
        ks = _strip_accents(key).strip().lower()
        if ks:
            out.setdefault(ks, abbr)

    for r in rows:
        abbr = (r["abbreviation"] or "").strip()
        name = (r["name"] or "").strip()
        if not abbr:
            continue
        _add(abbr, abbr)
        if name:
            _add(name, abbr)
            tokens = name.split()
            # Last word fallback: "Las Vegas Aces" → "aces"
            if tokens and len(tokens[-1]) >= 3:
                _add(tokens[-1], abbr)
            # First word fallback: critical for Euroleague where HR
            # ships short club names ("Fenerbahce", "Panathinaikos")
            # but our DB stores the full sponsor-laden official name.
            if tokens and len(tokens[0]) >= 3:
                _add(tokens[0], abbr)
            # Index every meaningful token (≥4 chars) for cross-match
            # against varied HR spellings.
            for tok in tokens:
                if len(tok) >= 4:
                    _add(tok, abbr)
    return out


def _resolve_team(name: str, name_to_abbr: dict[str, str]) -> str | None:
    """Resolve an HR team name to our abbreviation. Tries: exact,
    case-folded + accent-stripped, first/last word, then substring
    inclusion (both directions) as fallback."""
    if not name:
        return None
    n = _strip_accents(name).strip().lower()
    if n in name_to_abbr:
        return name_to_abbr[n]
    tokens = n.split()
    # Last word
    if tokens and tokens[-1] in name_to_abbr:
        return name_to_abbr[tokens[-1]]
    # First word
    if tokens and tokens[0] in name_to_abbr:
        return name_to_abbr[tokens[0]]
    # Bidirectional substring — bounded by team count (~20)
    for k, abbr in name_to_abbr.items():
        if not k:
            continue
        if k in n or n in k:
            return abbr
    return None


def _synth_abbr(team_name: str) -> str | None:
    """Derive a usable abbreviation from a raw HR team / city name when
    the per-league teams table can't resolve it. Strategy: use the
    first word uppercase; collapse to a 4-letter cap so it fits the
    OddsGrid + GameCard cells without wrapping."""
    if not team_name:
        return None
    first = team_name.strip().split()[0]
    return first.upper()[:4] if first else None


def _start_time_iso(ev: dict) -> str | None:
    """HR ships ``eventTime`` as epoch milliseconds. Convert to UTC ISO
    so the stub-backfill route + GameCard get a real puck-drop / tip-off
    timestamp instead of synthesizing one (see standard_layout #7)."""
    et = ev.get("eventTime")
    if not isinstance(et, (int, float)) or et <= 0:
        return None
    from datetime import datetime as _dt, timezone as _tz
    try:
        return _dt.fromtimestamp(et / 1000.0, tz=_tz.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, OSError):
        return None


def _disambiguate(home: str, away: str) -> tuple[str, str]:
    """Both first-word prefixes collided (Shandong vs Shanghai → SHAN
    vs SHAN). Walk one char at a time until the prefixes diverge, then
    return upper-cased prefixes of equal length."""
    h = (home or "").strip().split()[0] if home else ""
    a = (away or "").strip().split()[0] if away else ""
    n = min(len(h), len(a))
    diverge = n  # default: full length
    for i in range(n):
        if h[i].lower() != a[i].lower():
            diverge = i + 1
            break
    # Need at least 4 chars for the cell, but also need to include the
    # divergence position.
    width = max(4, diverge)
    return h.upper()[:width], a.upper()[:width]


# ── Event parser ──────────────────────────────────────────────

def _parse_event(ev: dict, name_to_abbr: dict[str, str],
                  league_name: str, league_key: str | None = None
                  ) -> dict | None:
    """Convert one HR event dict to our standard odds dict. Returns None
    when the event lacks competitors or no markets we care about.

    HR shape (verified live 2026-05-04):
      - eventNames is a single dict: ``{name, shortName, delimiter}``
        with ``name`` like ``"<Away> @ <Home>"`` (US convention).
      - Each market carries a ``type`` like ``BASKETBALL:FTOT:ML``,
        ``BASKETBALL:FTOT:SPRD``, ``BASKETBALL:FTOT:OU``.
      - Markets array uses key ``selection`` (singular, not the
        ``selections`` plural some other sports use).
      - For ML: selection type ``A`` = HOME, ``B`` = AWAY (HR's A/B is
        inverted vs the US away/home convention; A is participants[0]
        which corresponds to the team on the *right* of the @ sign,
        i.e. the home team).
      - For SPRD: selection types ``AH`` / ``BH`` (A's handicap, B's
        handicap). Same A=home convention.
      - For OU: selection types ``Over`` / ``Under``. Line lives in the
        selection name (``"Over 162.5"``) — no separate handicap field.
    """
    parts = ev.get("participants") or []
    if len(parts) < 2:
        return None
    en = ev.get("eventNames") or {}
    canonical_name = en.get("name") if isinstance(en, dict) else ""
    away_name, home_name = _split_canonical(canonical_name or "")
    if not (away_name and home_name):
        # Fallback to participants — but HR's order isn't guaranteed
        # so this is best-effort.
        away_name = (parts[0].get("name") or "").strip()
        home_name = (parts[1].get("name") or "").strip()
    home_abbr = _resolve_team(home_name, name_to_abbr)
    away_abbr = _resolve_team(away_name, name_to_abbr)
    # Pre-launch leagues (china_cba, brazil_nbb, … any tier without a
    # canonical schedule scraper) ship empty teams tables — every HR
    # event would be dropped here even though we can render the card
    # from the matchup name alone. Fall back to a synthetic abbrev
    # derived from the short event name ("Guangdong @ Beijing" →
    # GUANGDONG / BEIJING). Stub-id stays stable because the key is
    # built from these abbrevs deterministically.
    if not home_abbr:
        home_abbr = _synth_abbr(home_name)
    if not away_abbr:
        away_abbr = _synth_abbr(away_name)
    # Disambiguate collision (CBA: Shandong + Shanghai both -> "SHAN").
    # Walk both abbrevs out one extra char at a time until they diverge.
    if home_abbr and away_abbr and home_abbr == away_abbr:
        home_abbr, away_abbr = _disambiguate(home_name, away_name)
    if not (home_abbr and away_abbr):
        return None

    # Pull full participant names so the stub backfill route can show
    # "Beijing Ducks" instead of "BEIJ" when the per-league teams table
    # is empty. participants list is unordered but HR's eventNames.name
    # gives us the AWAY @ HOME convention; we cross-match by short-name
    # substring.
    home_full = home_name
    away_full = away_name
    for p in parts:
        pname = (p.get("name") or "").strip()
        if not pname:
            continue
        if home_name and home_name.lower() in pname.lower():
            home_full = pname
        elif away_name and away_name.lower() in pname.lower():
            away_full = pname

    odds: dict[str, Any] = {
        "_key": f"{away_abbr}@{home_abbr}",
        "comp": league_name,
        "hr_event_id": ev.get("id") or ev.get("eventId"),
        "home_abbr": home_abbr,
        "away_abbr": away_abbr,
        "home_full_name": home_full,
        "away_full_name": away_full,
        "start_time": _start_time_iso(ev),
    }
    # Strict full-game market filter. HR ships ~70 markets per game
    # including Q1/Q2/Q3/Q4/H1/H2 splits, team totals (A:/B:), odd-vs-
    # even, etc. We only want the canonical full-game ML / SPREAD /
    # TOTAL — partial-period lines were leaking onto the card with
    # tiny totals like "Over 43.5" (a Q1 total) or wrong spreads.
    #
    # Also: HR ships *multiple* full-game O/U markets (alt lines).
    # We pick the line with juice closest to -110 (the "main" line)
    # so the card shows the headline number.
    markets = ev.get("markets") or []
    full_total_candidates: list[dict] = []
    full_spread_candidates: list[dict] = []
    q1_total_candidates: list[dict] = []
    q1_spread_candidates: list[dict] = []
    q1_ml: dict | None = None
    # Accept both BASKETBALL:FTOT:* and AUSSIE_RULES:FT:* full-game
    # market codes — AFL is hosted under the basketball framework
    # because the structural math + selection schema match, but HR
    # uses a sport-specific prefix. Easier than forking the parser.
    FULLGAME_ML = ("BASKETBALL:FTOT:ML", "AUSSIE_RULES:FT:ML")
    FULLGAME_SPRD = ("BASKETBALL:FTOT:SPRD", "AUSSIE_RULES:FT:SPRD")
    FULLGAME_OU = ("BASKETBALL:FTOT:OU", "AUSSIE_RULES:FT:OU")
    # First-period markets — HR groups all per-period markets under
    # BASKETBALL:P:* (or AUSSIE_RULES:P:* for AFL) with the ``period``
    # field carrying 'Q1', 'Q2', 'Q3', 'Q4', 'H1', 'H2'. We accept Q1
    # for quarter leagues (q1_share≈0.25) and H1 for halves leagues
    # (NCAAM, q1_share≈0.47 — the home_q1 column for NCAAM actually
    # stores first-half scores because ESPN doesn't ship per-quarter
    # splits for college basketball). The picker doesn't currently
    # ship H1 picks for quarter leagues — when HR posts H1-only (no
    # Q1) for a quarter league like Hungary, no Q1 pick fires.
    Q1_ML   = ("BASKETBALL:P:DNB", "AUSSIE_RULES:P:DNB")
    Q1_SPRD = ("BASKETBALL:P:SPRD", "AUSSIE_RULES:P:SPRD")
    Q1_OU   = ("BASKETBALL:P:OU", "AUSSIE_RULES:P:OU")
    # League-aware period selection. Determined from constants —
    # q1_share>0.40 means home_q1 stores H1 data (halves league) so we
    # accept H1 lines. Otherwise quarter league — accept Q1 only.
    league_is_halves = False
    if league_key:
        try:
            from ._calibrate import load as _load_constants
            _cons = _load_constants(league_key) or {}
            _share = (_cons.get("q1_avg_total") /
                      (_cons.get("league_avg_total") or 1)) \
                     if _cons.get("league_avg_total") else 0.0
            league_is_halves = _share > 0.40
        except Exception as _e:
            logger.debug("[basketball:%s] q1_share probe failed: %s",
                          league_key, _e)
            league_is_halves = False
    _Q1_PERIODS = ("H1",) if league_is_halves else ("Q1",)
    for m in markets:
        mtype = (m.get("type") or "").upper()
        period = (m.get("period") or "").upper()
        sels = m.get("selection") or m.get("selections") or []
        # Full game (period='M' or empty)
        if period in ("", "M"):
            if mtype in FULLGAME_ML:
                ml = _ml_from_selections(sels)
                if ml and "ml" not in odds:
                    odds["ml"] = ml
            elif mtype in FULLGAME_SPRD:
                sp = _spread_from_selections(sels)
                if sp:
                    full_spread_candidates.append(sp)
            elif mtype in FULLGAME_OU:
                tot = _total_from_selections(sels)
                if tot:
                    full_total_candidates.append(tot)
            continue
        # Q1 markets — opt-in via league check at the call site
        # (downstream picker enforces it). Parser collects them so the
        # data is available; picker decides whether to fire on them.
        if period in _Q1_PERIODS:
            if mtype in Q1_ML:
                ml = _ml_from_selections(sels)
                if ml and q1_ml is None:
                    q1_ml = ml
            elif mtype in Q1_SPRD:
                sp = _spread_from_selections(sels)
                if sp:
                    q1_spread_candidates.append(sp)
            elif mtype in Q1_OU:
                tot = _total_from_selections(sels)
                if tot:
                    # Sanity check on the line: HR occasionally mis-
                    # labels a 1st-half OU as period='Q1' on certain
                    # leagues (China CBA shipped a 97.5-line "Q1" on
                    # 2026-05-18 that was clearly the H1 market). For
                    # quarter leagues a Q1 total > 70 is implausible
                    # (Q1 averages 35-55 in the highest-scoring
                    # quarter basketball). Reject the candidate so the
                    # picker doesn't fire a phantom Under.
                    line_val = tot.get("line")
                    if (not league_is_halves
                            and line_val is not None
                            and line_val > 70):
                        logger.debug(
                            "[basketball:%s] dropping Q1 OU candidate "
                            "with implausible line=%s (looks like H1 "
                            "mislabel)", league_name, line_val,
                        )
                        continue
                    q1_total_candidates.append(tot)
    # Pick the main spread/total — the one with juice closest to -110.
    if full_spread_candidates:
        odds["spread"] = _pick_main_spread(full_spread_candidates)
    if full_total_candidates:
        odds["total"] = _pick_main_total(full_total_candidates)
    # Q1 markets — only emit when present so leagues without Q1 coverage
    # (RealGM tier basketball) pass through cleanly. Picker reads the
    # ``q1`` sub-dict and skips when absent.
    if q1_ml or q1_spread_candidates or q1_total_candidates:
        q1: dict[str, Any] = {}
        if q1_ml:
            q1["ml"] = q1_ml
        if q1_spread_candidates:
            q1["spread"] = _pick_main_spread(q1_spread_candidates)
        if q1_total_candidates:
            q1["total"] = _pick_main_total(q1_total_candidates)
        odds["q1"] = q1
    if not any(k in odds for k in ("ml", "spread", "total")):
        return None
    return odds


def _juice_distance(odds_val: int | None) -> int:
    """Distance from -110 (the conventional 'main' line juice). Lower
    means closer to even-vig — that's the line books treat as primary."""
    if odds_val is None:
        return 9999
    return abs(int(odds_val) - (-110))


def _pick_main_total(candidates: list[dict]) -> dict:
    """From multiple full-game O/U candidates (alt lines), pick the
    one whose juice is closest to -110 on either side. Tie → smallest
    absolute juice difference between the two sides (true main line)."""
    def score(c):
        return _juice_distance(c.get("over_odds")) + _juice_distance(c.get("under_odds"))
    return min(candidates, key=score)


def _pick_main_spread(candidates: list[dict]) -> dict:
    def score(c):
        return _juice_distance(c.get("home_odds")) + _juice_distance(c.get("away_odds"))
    return min(candidates, key=score)


def _split_canonical(s: str) -> tuple[str, str]:
    if " @ " in s:
        a, b = s.split(" @ ", 1)
        return a.strip(), b.strip()
    if " v " in s:
        a, b = s.split(" v ", 1)
        return a.strip(), b.strip()
    return ("", "")


def _decimal_to_american(dec: float | str | None) -> int | None:
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


def _selection_odds(sel: dict) -> int | None:
    return _decimal_to_american(sel.get("odds") or sel.get("price"))


def _ml_from_selections(sels: list[dict]) -> dict | None:
    """HR ML: selection.type 'A' = HOME, 'B' = AWAY (verified live —
    eventNames.name is "<Away> @ <Home>" but participants[0] / type=A is
    the team on the right of @, i.e. home)."""
    a = next((s for s in sels if (s.get("type") or "").upper() == "A"), None)
    b = next((s for s in sels if (s.get("type") or "").upper() == "B"), None)
    if not (a and b):
        return None
    home = _selection_odds(a)
    away = _selection_odds(b)
    if home is None or away is None:
        return None
    return {"home": home, "away": away}


def _spread_from_selections(sels: list[dict]) -> dict | None:
    """HR spread: types AH (home handicap) / BH (away handicap). Line
    embedded in selection name like 'New York Liberty -17.5'."""
    ah = next((s for s in sels if (s.get("type") or "").upper() == "AH"), None)
    bh = next((s for s in sels if (s.get("type") or "").upper() == "BH"), None)
    if not (ah and bh):
        return None
    h_odds = _selection_odds(ah)
    a_odds = _selection_odds(bh)
    h_pt = _line_value(ah)
    a_pt = _line_value(bh)
    if None in (h_odds, a_odds, h_pt, a_pt):
        return None
    return {
        "home_pt": h_pt, "home_odds": h_odds,
        "away_pt": a_pt, "away_odds": a_odds,
    }


def _total_from_selections(sels: list[dict]) -> dict | None:
    """HR total: types 'Over' / 'Under'. Line in selection name."""
    over = next(
        (s for s in sels
         if (s.get("type") or "").lower() == "over"
            or "over" in (s.get("name") or "").lower()), None
    )
    under = next(
        (s for s in sels
         if (s.get("type") or "").lower() == "under"
            or "under" in (s.get("name") or "").lower()), None
    )
    if not (over and under):
        return None
    line = _line_value(over) or _line_value(under)
    if line is None:
        return None
    o_odds = _selection_odds(over)
    u_odds = _selection_odds(under)
    if o_odds is None or u_odds is None:
        return None
    return {"line": float(line), "over_odds": o_odds, "under_odds": u_odds}


def _line_value(sel: dict) -> float | None:
    """Pull the points line from a selection. HR ships it under
    ``handicap`` / ``points`` / parsed from name like 'Over 158.5'."""
    for key in ("handicap", "points", "line", "value"):
        v = sel.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    name = sel.get("name") or ""
    import re
    m = re.search(r"([+-]?\d+(?:\.\d+)?)", name)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None
