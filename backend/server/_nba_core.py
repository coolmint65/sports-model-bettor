"""NBA scoreboard/odds helpers.

Extracted 2026-07-27 from backend/server/__init__.py (de-god-file, audit T2-E).
Helpers-only (no routes); re-exported from backend.server so routes_nba.py's
lazy proxies keep resolving.
"""
from __future__ import annotations

import logging
from ._espn import ESPN_BASE
from ._espn import _fetch_espn_json
from ._tz import et_now
from ._tz import et_today_str
import time

logger = logging.getLogger(__name__)
CACHE_TTL = 120
MAX_CACHE_ENTRIES = 50

# ══════════════════════════════════════════════════════════════
#  NBA Q1 ENDPOINTS
# ══════════════════════════════════════════════════════════════

_nba_scoreboard_cache: dict[str, tuple[float, list]] = {}


_nba_sb_refreshing: set = set()


def _get_nba_scoreboard(date: str = "", lookahead: bool = False) -> list[dict]:
    """Stale-while-revalidate front (speed pass 2026-10-28): once a slate has been built, a
    request past CACHE_TTL returns it immediately and rebuilds in the background. The build is
    3+ serial ESPN fetches whose TLS handshakes regularly time out from the Beelink, which made
    /api/nba/scoreboard take 17-25 s every two minutes."""
    import threading
    target_date = date or et_today_str()
    cache_key = f"nba:{target_date.replace('-', '')}:{'la' if lookahead else 'd'}"
    hit = _nba_scoreboard_cache.get(cache_key)
    if hit is not None:
        if time.time() - hit[0] >= CACHE_TTL and cache_key not in _nba_sb_refreshing:
            _nba_sb_refreshing.add(cache_key)

            def _bg():
                try:
                    _build_nba_scoreboard(date, lookahead)
                except Exception as e:
                    logger.warning("NBA scoreboard bg refresh failed: %s", e)
                finally:
                    _nba_sb_refreshing.discard(cache_key)

            threading.Thread(target=_bg, daemon=True).start()
        return hit[1]
    return _build_nba_scoreboard(date, lookahead)


def _build_nba_scoreboard(date: str = "", lookahead: bool = False) -> list[dict]:
    """Fetch NBA scoreboard from ESPN, enriched with Q1 scores and odds.

    When called with no date (default-today path), also pulls
    yesterday's scoreboard and keeps any live games. Late-night games
    that cross midnight UTC otherwise drop off the dashboard while
    they're still in progress — surfaces as "DEN @ MIN disappeared
    in the 4th quarter at midnight" on the user side.
    """
    target_date = date or et_today_str()
    espn_date = target_date.replace("-", "")

    cache_key = f"nba:{espn_date}:{'la' if lookahead else 'd'}"
    now = time.time()

    url = f"{ESPN_BASE}/basketball/nba/scoreboard?dates={espn_date}"
    logger.info("Fetching NBA scoreboard: %s", url)
    espn_data = _fetch_espn_json(url)

    games = []
    if espn_data:
        events = espn_data.get("events", [])
        logger.info("ESPN NBA returned %d events", len(events))
        games = _parse_nba_scoreboard(espn_data)

    # Yesterday-still-live carryover. Only on the default-today path —
    # explicit ?date=YYYY-MM-DD lookups stay strict.
    if date == "":
        from datetime import timedelta as _td
        yest = (et_now() - _td(days=1)).strftime("%Y%m%d")
        try:
            yest_data = _fetch_espn_json(
                f"{ESPN_BASE}/basketball/nba/scoreboard?dates={yest}")
            if yest_data:
                yest_games = _parse_nba_scoreboard(yest_data)
                seen_ids = {g.get("id") for g in games}
                for g in yest_games:
                    state = (g.get("status") or {}).get("state", "")
                    if state == "in" and g.get("id") not in seen_ids:
                        games.append(g)
                        logger.info("NBA: carried over live game from "
                                    "yesterday: %s", g.get("id"))
        except Exception as e:
            logger.debug("NBA yesterday-live carryover failed: %s", e)

    # Lookahead (Austin 2026-10-03): append the next N days' games to the
    # default slate so the card shows upcoming games (preseason etc.) the way
    # every other sport already does (NHL _get_nhl_scoreboard, 2026-09-03).
    # N from NBA_LOOKAHEAD_DAYS (default 7). Future games are TAGGED
    # lookahead=True so best-bets' recording piggybacks can stay TODAY-only.
    if date == "" and lookahead:
        import os as _os
        from datetime import timedelta as _td
        try:
            _n = int(_os.environ.get("NBA_LOOKAHEAD_DAYS", "1"))
        except ValueError:
            _n = 7
        seen_ids = {g.get("id") for g in games}
        for _fwd in range(1, max(0, _n) + 1):
            d8 = (et_now() + _td(days=_fwd)).strftime("%Y%m%d")
            try:
                fd = _fetch_espn_json(
                    f"{ESPN_BASE}/basketball/nba/scoreboard?dates={d8}")
                if not fd:
                    continue
                for g in _parse_nba_scoreboard(fd):
                    if g.get("id") not in seen_ids:
                        g["lookahead"] = True
                        seen_ids.add(g.get("id"))
                        games.append(g)
            except Exception as e:
                logger.debug("NBA lookahead %s failed: %s", d8, e)

    # If no games today, try tomorrow
    if not games and date == "":
        from datetime import timedelta as _td
        tomorrow = (et_now() + _td(days=1)).strftime("%Y%m%d")
        logger.info("No NBA games today, checking tomorrow (%s)", tomorrow)
        espn_data = _fetch_espn_json(f"{ESPN_BASE}/basketball/nba/scoreboard?dates={tomorrow}")
        if espn_data:
            games = _parse_nba_scoreboard(espn_data)
            if games:
                logger.info("Found %d NBA games for tomorrow", len(games))

    # ── Hard Rock fallback (only when ESPN returned zero events) ──
    if not games and date == "":
        try:
            from scrapers.hardrock_odds import fetch_nba as _hr_nba
            hr_odds = _hr_nba()
            if hr_odds:
                logger.info("NBA: ESPN empty, building %d games from Hard Rock",
                            len(hr_odds))
                for matchup_key, odds_data in hr_odds.items():
                    parts = matchup_key.split("@")
                    if len(parts) != 2:
                        continue
                    a_abbr, h_abbr = parts
                    existing = any(
                        g["home"]["abbreviation"] == h_abbr and
                        g["away"]["abbreviation"] == a_abbr
                        for g in games
                    )
                    if existing:
                        continue
                    games.append({
                        "id": f"hr_{a_abbr}_{h_abbr}",
                        "date": target_date,
                        "home": {
                            "abbreviation": h_abbr,
                            "name": h_abbr,
                            "score": None,
                        },
                        "away": {
                            "abbreviation": a_abbr,
                            "name": a_abbr,
                            "score": None,
                        },
                        "status": {"state": "pre", "detail": "Scheduled"},
                        "odds": odds_data,
                        "source": "hardrock",
                    })
        except Exception as e:
            logger.debug("NBA Hard Rock fallback failed: %s", e)

    # Fetch NBA odds via the unified chain (Hard Rock → ESPN).
    try:
        nba_odds = _fetch_nba_odds()
        if nba_odds:
            matched = 0
            for game in games:
                h = game["home"]["abbreviation"]
                a = game["away"]["abbreviation"]
                key = f"{a}@{h}"
                from engine.picks import match_odds as _match
                matched_odds = _match(h, a, nba_odds)
                if matched_odds:
                    game["odds"] = matched_odds
                    matched += 1
            logger.info("NBA odds: matched %d/%d games", matched, len(games))
    except Exception as e:
        logger.warning("NBA odds failed: %s", e)

    # Cache
    if len(_nba_scoreboard_cache) >= MAX_CACHE_ENTRIES:
        oldest = min(_nba_scoreboard_cache, key=lambda k: _nba_scoreboard_cache[k][0])
        del _nba_scoreboard_cache[oldest]
    # ── Enrich with playoff series context ──
    try:
        from engine.series_context import infer_series
        from engine.nba_q1_predict import _is_nba_playoffs
        if _is_nba_playoffs():
            for game in games:
                h = game["home"]["abbreviation"]
                a = game["away"]["abbreviation"]
                series = infer_series("nba", h, a)
                if series.get("in_series"):
                    game["series"] = series
    except Exception as e:
        logger.debug("NBA series enrichment failed: %s", e)

    _nba_scoreboard_cache[cache_key] = (now, games)
    return games


def _parse_nba_scoreboard(espn_data: dict) -> list[dict]:
    """Parse ESPN NBA scoreboard response into our standard format with Q1 data."""
    games = []
    for ev in espn_data.get("events", []):
      try:
        comp = ev.get("competitions", [{}])[0]
        teams = comp.get("competitors", [])
        if len(teams) < 2:
            continue

        home_raw = next((t for t in teams if t.get("homeAway") == "home"), teams[0])
        away_raw = next((t for t in teams if t.get("homeAway") == "away"), teams[1])

        def parse_team(raw):
            t = raw.get("team", {})
            if isinstance(t, str):
                t = {"name": t}
            record = ""
            for r in raw.get("records", []):
                if not isinstance(r, dict):
                    continue
                if r.get("type") == "total":
                    record = r.get("summary", "")
                    break
            logo = t.get("logo", "")
            if isinstance(logo, dict):
                logo = logo.get("href", "")
            elif isinstance(logo, list) and logo:
                logo = logo[0].get("href", "") if isinstance(logo[0], dict) else str(logo[0])
            return {
                "name": t.get("displayName", t.get("name", "")),
                "abbreviation": t.get("abbreviation", ""),
                "logo": logo if isinstance(logo, str) else "",
                "record": record,
                "score": str(raw.get("score", "")),
                "winner": raw.get("winner", False),
            }

        home = parse_team(home_raw)
        away = parse_team(away_raw)

        # Parse Q1 scores from linescores
        home_q1 = None
        away_q1 = None
        home_ls = home_raw.get("linescores", [])
        away_ls = away_raw.get("linescores", [])
        if home_ls and len(home_ls) >= 1:
            v = home_ls[0]
            home_q1 = int(v.get("value", 0)) if isinstance(v, dict) else int(v) if str(v).isdigit() else None
        if away_ls and len(away_ls) >= 1:
            v = away_ls[0]
            away_q1 = int(v.get("value", 0)) if isinstance(v, dict) else int(v) if str(v).isdigit() else None

        # Quarter scores for display
        quarters = []
        for i in range(max(len(home_ls), len(away_ls))):
            hval = home_ls[i] if i < len(home_ls) else 0
            aval = away_ls[i] if i < len(away_ls) else 0
            hv = int(hval.get("value", 0)) if isinstance(hval, dict) else int(hval) if str(hval).isdigit() else 0
            av = int(aval.get("value", 0)) if isinstance(aval, dict) else int(aval) if str(aval).isdigit() else 0
            quarters.append({"quarter": i + 1, "home": hv, "away": av})

        status_raw = comp.get("status", {})
        status_type = status_raw.get("type", {})
        state = status_type.get("state", "pre")
        period = status_raw.get("period", 0)

        game = {
            "id": ev.get("id", ""),
            "date": ev.get("date", ""),
            "venue": comp.get("venue", {}).get("fullName", ""),
            "broadcast": "",
            "home": home,
            "away": away,
            "q1": {
                "home": home_q1,
                "away": away_q1,
            },
            "quarters": quarters,
            "status": {
                "state": state,
                "detail": status_type.get("detail", ""),
                "completed": status_type.get("completed", False),
                "period": period,
            },
        }

        # Extract broadcast - names can be strings or dicts
        for bc in comp.get("broadcasts", []):
            if not isinstance(bc, dict):
                continue
            raw_names = bc.get("names", [])
            names = [n.get("shortName", str(n)) if isinstance(n, dict) else str(n) for n in raw_names]
            if names:
                game["broadcast"] = ", ".join(names)
                break

        games.append(game)
      except Exception as e:
        import traceback
        logger.warning("NBA parse event failed: %s\n%s", e, traceback.format_exc())
        continue

    return games


# NBA abbreviation aliases (ESPN vs odds providers)
_NBA_ABBR_MAP = {
    "GS": "GSW", "GSW": "GS",
    "NY": "NYK", "NYK": "NY",
    "SA": "SAS", "SAS": "SA",
    "NO": "NOP", "NOP": "NO",
    "PHX": "PHO", "PHO": "PHX",
    "WSH": "WAS", "WAS": "WSH",
    "BKN": "BRK", "BRK": "BKN",
    "CHA": "CHO", "CHO": "CHA",
}


def _nba_alt_abbr(abbr: str) -> str:
    return _NBA_ABBR_MAP.get(abbr, abbr)


# NBA team name -> abbreviation for odds matching
_NBA_TEAM_ABBRS = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GS", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "LA Clippers": "LAC", "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL", "LA Lakers": "LAL",
    "Memphis Grizzlies": "MEM", "Miami Heat": "MIA", "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL", "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX", "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC", "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR", "Utah Jazz": "UTA", "Washington Wizards": "WAS",
}


def _fetch_nba_odds() -> dict:
    """Fetch NBA odds (full-game + Q1 markets) via the unified fallback
    chain in scrapers.nba_odds (Hard Rock → ESPN).

    Delegates to fetch_all_nba_odds so the UI scoreboard endpoint picks
    up the same data the tracker uses - including h2h_q1, spreads_q1,
    and totals_q1 markets via the per-event endpoint. Previously this
    was a duplicated bulk-only fetcher that silently dropped Q1 markets,
    which is why the Q1 model picks card kept showing (-110) defaults
    no matter how many times the scrapers were fixed.
    """
    try:
        from scrapers.nba_odds import fetch_all_nba_odds
        return fetch_all_nba_odds() or {}
    except Exception as e:
        logger.warning("NBA odds unified fetch failed: %s", e)
        return {}




