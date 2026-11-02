"""NHL scoreboard/odds helpers + line-movement route.

Extracted 2026-07-27 from backend/server/__init__.py (de-god-file, audit T2-E).
The helpers are re-exported from backend.server so routes_nhl.py's lazy
proxies keep resolving; the line-movement endpoint mounts via include_router.
"""
from __future__ import annotations

import logging
from fastapi import APIRouter
from ._espn import ESPN_BASE
from ._espn import _fetch_espn_json
from ._tz import et_now
from ._tz import et_today_str
import time

logger = logging.getLogger(__name__)
router = APIRouter()
CACHE_TTL = 120           # mirror backend.server cache-freshness window
MAX_CACHE_ENTRIES = 50    # mirror backend.server cache cap

# ══════════════════════════════════════════════════════════════
#  NHL ENDPOINTS
# ══════════════════════════════════════════════════════════════

# NHL team name → abbreviation (for Odds API matching)
_NHL_TEAM_ABBR = {
    "Anaheim Ducks": "ANA", "Arizona Coyotes": "ARI", "Utah Hockey Club": "UTA", "Utah Mammoth": "UTA",
    "Boston Bruins": "BOS", "Buffalo Sabres": "BUF",
    "Calgary Flames": "CGY", "Carolina Hurricanes": "CAR",
    "Chicago Blackhawks": "CHI", "Colorado Avalanche": "COL",
    "Columbus Blue Jackets": "CBJ", "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET", "Edmonton Oilers": "EDM",
    "Florida Panthers": "FLA", "Los Angeles Kings": "LAK",
    "Minnesota Wild": "MIN", "Montreal Canadiens": "MTL",
    "Nashville Predators": "NSH", "New Jersey Devils": "NJD",
    "New York Islanders": "NYI", "New York Rangers": "NYR",
    "Ottawa Senators": "OTT", "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT", "San Jose Sharks": "SJS",
    "Seattle Kraken": "SEA", "St. Louis Blues": "STL",
    "Tampa Bay Lightning": "TBL", "Toronto Maple Leafs": "TOR",
    "Vancouver Canucks": "VAN", "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH", "Winnipeg Jets": "WPG",
}

_NHL_ABBR_ALTS = {
    "TB": "TBL", "TBL": "TB",
    "NJ": "NJD", "NJD": "NJ",
    "SJ": "SJS", "SJS": "SJ",
    "LA": "LAK", "LAK": "LA",
    "WAS": "WSH", "WSH": "WAS",
    "CLB": "CBJ", "CBJ": "CLB",
    "MON": "MTL", "MTL": "MON",
    "NAS": "NSH", "NSH": "NAS",
    "AZ": "UTA", "UTA": "AZ",
    "UTAH": "UTA", "UTH": "UTA",
}

# Map ESPN abbreviation to team JSON key
_NHL_ESPN_TO_KEY = {}  # populated lazily

def _nhl_espn_to_key() -> dict:
    """Build mapping from ESPN abbreviation to JSON file key."""
    if _NHL_ESPN_TO_KEY:
        return _NHL_ESPN_TO_KEY
    from engine.data import list_teams, load_team
    for t in list_teams("NHL"):
        team = load_team("NHL", t["key"])
        if team:
            abbr = team.get("abbreviation", "")
            if abbr:
                _NHL_ESPN_TO_KEY[abbr] = t["key"]
                # Add alternate abbreviations
                alt = _NHL_ABBR_ALTS.get(abbr)
                if alt:
                    _NHL_ESPN_TO_KEY[alt] = t["key"]
            _NHL_ESPN_TO_KEY[t["key"]] = t["key"]
            # Also map by short name (e.g. "Bruins" -> "bruins")
            short = team.get("short_name", "")
            if short:
                _NHL_ESPN_TO_KEY[short.lower()] = t["key"]
            # Also map by last word of name (e.g. "Mammoth" for Utah Mammoth)
            name = team.get("name", "")
            if name:
                last_word = name.split()[-1].lower()
                if last_word not in _NHL_ESPN_TO_KEY:
                    _NHL_ESPN_TO_KEY[last_word] = t["key"]
    return _NHL_ESPN_TO_KEY


def _nhl_alt_abbr(abbr: str) -> str:
    return _NHL_ABBR_ALTS.get(abbr, abbr)


_scoreboard_cache: dict[str, tuple[float, list]] = {}


_nhl_sb_refreshing: set = set()


def _get_nhl_scoreboard(date: str = "", lookahead: bool = True) -> list[dict]:
    """Stale-while-revalidate front, same as NBA (speed pass 2026-10-28): past CACHE_TTL a
    built slate is returned immediately and rebuilt in the background, so a slow ESPN TLS
    handshake never sits on a request (cold /api/nhl/scoreboard took ~18 s)."""
    import threading
    cache_key = f"nhl:{(date or et_today_str()).replace('-', '')}"
    hit = _scoreboard_cache.get(cache_key)
    if hit is not None:
        if time.time() - hit[0] >= CACHE_TTL and cache_key not in _nhl_sb_refreshing:
            _nhl_sb_refreshing.add(cache_key)

            def _bg():
                try:
                    _build_nhl_scoreboard(date, lookahead)
                except Exception as e:
                    logger.warning("NHL scoreboard bg refresh failed: %s", e)
                finally:
                    _nhl_sb_refreshing.discard(cache_key)

            threading.Thread(target=_bg, daemon=True).start()
        return hit[1]
    return _build_nhl_scoreboard(date, lookahead)


def _build_nhl_scoreboard(date: str = "", lookahead: bool = True) -> list[dict]:
    """Fetch NHL scoreboard from ESPN."""
    target_date = date or et_today_str()
    espn_date = target_date.replace("-", "")

    cache_key = f"nhl:{espn_date}"
    now = time.time()

    url = f"{ESPN_BASE}/hockey/nhl/scoreboard?dates={espn_date}"
    logger.info("Fetching NHL scoreboard: %s", url)
    espn_data = _fetch_espn_json(url)

    games = []
    if espn_data:
        events = espn_data.get("events", [])
        logger.info("ESPN NHL returned %d events", len(events))
        games = _parse_nhl_scoreboard(espn_data)

    # Yesterday-still-live carryover. Same rationale as NBA: late
    # games that cross midnight UTC drop off today's slate while
    # they're still in progress.
    if date == "":
        from datetime import timedelta as _td
        yest = (et_now() - _td(days=1)).strftime("%Y%m%d")
        try:
            yest_data = _fetch_espn_json(
                f"{ESPN_BASE}/hockey/nhl/scoreboard?dates={yest}")
            if yest_data:
                yest_games = _parse_nhl_scoreboard(yest_data)
                seen_ids = {g.get("id") for g in games}
                for g in yest_games:
                    state = (g.get("status") or {}).get("state", "")
                    if state == "in" and g.get("id") not in seen_ids:
                        games.append(g)
                        logger.info("NHL: carried over live game from "
                                    "yesterday: %s", g.get("id"))
        except Exception as e:
            logger.debug("NHL yesterday-live carryover failed: %s", e)

    # 2026-09-03 (Austin: lookahead) — append the next N days' games to the default
    # slate so the card shows what HR already prices (GameCard renders the weekday
    # for non-today dates). N from NHL_LOOKAHEAD_DAYS (default 7). Only on the
    # default call (date == "") so explicit-date lookups stay single-day. best-bets
    # passes lookahead=False so picks/recording stay TODAY-only.
    if date == "" and lookahead:
        import os as _os
        from datetime import timedelta as _td
        try:
            _n = int(_os.environ.get("NHL_LOOKAHEAD_DAYS", "7"))
        except ValueError:
            _n = 7
        seen_ids = {g.get("id") for g in games}
        for _fwd in range(1, max(0, _n) + 1):
            d8 = (et_now() + _td(days=_fwd)).strftime("%Y%m%d")
            try:
                fd = _fetch_espn_json(f"{ESPN_BASE}/hockey/nhl/scoreboard?dates={d8}")
                if not fd:
                    continue
                for g in _parse_nhl_scoreboard(fd):
                    if g.get("id") not in seen_ids:
                        seen_ids.add(g.get("id")); games.append(g)
            except Exception as e:
                logger.debug("NHL lookahead %s failed: %s", d8, e)

    # Fallback without date
    if not games and date == "":
        espn_data = _fetch_espn_json(f"{ESPN_BASE}/hockey/nhl/scoreboard")
        if espn_data:
            games = _parse_nhl_scoreboard(espn_data)

    # If still no games, try tomorrow
    if not games and date == "":
        from datetime import timedelta as _td
        tomorrow = (et_now() + _td(days=1)).strftime("%Y%m%d")
        espn_data = _fetch_espn_json(f"{ESPN_BASE}/hockey/nhl/scoreboard?dates={tomorrow}")
        if espn_data:
            games = _parse_nhl_scoreboard(espn_data)

    # ── Hard Rock fallback ──────────────────────────────────────
    # When ESPN hasn't posted today's schedule at all (zero events),
    # build minimal scoreboard entries from Hard Rock. Only triggers
    # when ESPN returned nothing — NOT when all games are live/done
    # (that would pull in tomorrow's games from HR).
    if not games and date == "":
        try:
            from scrapers.hardrock_odds import fetch_nhl as _hr_nhl
            hr_odds = _hr_nhl()
            if hr_odds:
                logger.info("NHL: ESPN empty, building %d games from Hard Rock",
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
            logger.debug("NHL Hard Rock fallback failed: %s", e)

    # Fetch NHL odds from The Odds API
    try:
        nhl_odds = _fetch_nhl_odds()
        if nhl_odds:
            matched = 0
            for game in games:
                h = game["home"]["abbreviation"]
                a = game["away"]["abbreviation"]
                key = f"{a}@{h}"
                from engine.picks import match_odds as _match
                matched_odds = _match(h, a, nhl_odds)
                if matched_odds:
                    game["odds"] = matched_odds
                    matched += 1
            logger.info("NHL odds: matched %d/%d games", matched, len(games))

            # Store odds snapshots for historical backtesting
            try:
                from engine.odds_history import store_nhl_odds
                odds_rows = []
                for game in games:
                    if game.get("odds"):
                        odds_rows.append({
                            "game_date": target_date,
                            "home_abbr": game["home"]["abbreviation"],
                            "away_abbr": game["away"]["abbreviation"],
                            "odds": game["odds"],
                        })
                if odds_rows:
                    store_nhl_odds(odds_rows)
            except Exception as e:
                logger.debug("Odds history storage failed: %s", e)

            # Compare current odds against tracked opening odds for line movement.
            # If we haven't seen this matchup yet, store the opening snapshot.
            try:
                from engine.line_movement import get_line_movement, track_opening_odds
                for game in games:
                    if not game.get("odds"):
                        continue
                    h_abbr = game["home"]["abbreviation"]
                    a_abbr = game["away"]["abbreviation"]
                    game_date = (game.get("date", "") or "")[:10] or target_date
                    key = f"{game_date}_{a_abbr}@{h_abbr}"
                    movement = get_line_movement("nhl", key, game["odds"])
                    if movement:
                        game["line_movement"] = movement
                    else:
                        track_opening_odds("nhl", key, game["odds"])
            except Exception as e:
                logger.debug("NHL line movement failed: %s", e)
    except Exception as e:
        logger.warning("NHL odds failed: %s", e)

    # Preseason / Odds-API gaps: The Odds API doesn't carry preseason lines, so any
    # game still without odds gets filled from Hard Rock (which DOES price preseason).
    # Austin 2026-09-22: missing odds for tonight's preseason games.
    _missing = [g for g in games if not g.get("odds")]
    if _missing:
        try:
            from scrapers.hardrock_odds import fetch_nhl as _hr_nhl2
            _hr = _hr_nhl2()
            if _hr:
                _filled = 0
                for g in _missing:
                    _key = f'{g["away"]["abbreviation"]}@{g["home"]["abbreviation"]}'
                    _od = _hr.get(_key)
                    if _od:
                        g["odds"] = _od
                        g["odds_source"] = "hardrock"
                        _filled += 1
                logger.info("NHL: filled %d/%d missing-odds games from Hard Rock (preseason gap)", _filled, len(_missing))
        except Exception as _e:
            logger.debug("NHL Hard Rock odds fill failed: %s", _e)

    # Enrich with starting goalies - try DailyFaceoff first, then NHL API
    goalie_count = 0
    try:
        from scrapers.dailyfaceoff import get_starting_goalies
        df_goalies = get_starting_goalies()
        if df_goalies:
            for game in games:
                h = game["home"]["abbreviation"]
                a = game["away"]["abbreviation"]
                for abbr_try, side in [(h, "home_goalie"), (a, "away_goalie")]:
                    for try_abbr in [abbr_try, _nhl_alt_abbr(abbr_try)]:
                        if try_abbr in df_goalies:
                            game[side] = df_goalies[try_abbr]
                            goalie_count += 1
                            break
    except Exception as e:
        logger.debug("DailyFaceoff failed: %s", e)

    # Fallback: NHL API for goalies if DailyFaceoff didn't work
    if goalie_count == 0:
        try:
            nhl_schedule = _fetch_espn_json("https://api-web.nhle.com/v1/score/now")
            if nhl_schedule and nhl_schedule.get("games"):
                for nhl_game in nhl_schedule["games"]:
                    # Extract team abbreviations
                    def _gs(obj):
                        return obj.get("default", "") if isinstance(obj, dict) else str(obj) if obj else ""

                    h_abbr = _gs(nhl_game.get("homeTeam", {}).get("abbrev", ""))
                    a_abbr = _gs(nhl_game.get("awayTeam", {}).get("abbrev", ""))

                    # Match to our scoreboard games
                    for game in games:
                        gh = game["home"]["abbreviation"]
                        ga = game["away"]["abbreviation"]
                        if (gh == h_abbr or _nhl_alt_abbr(gh) == h_abbr) and \
                           (ga == a_abbr or _nhl_alt_abbr(ga) == a_abbr):
                            # Home goalie
                            hg = nhl_game.get("homeTeam", {}).get("goalie", {})
                            if not hg:
                                # Try alternate field names
                                hg = nhl_game.get("homeTeam", {}).get("startingGoalie", {})
                            if hg and hg.get("id"):
                                first = _gs(hg.get("firstName", ""))
                                last = _gs(hg.get("lastName", ""))
                                game["home_goalie"] = {
                                    "name": f"{first} {last}".strip(),
                                    "status": "expected",
                                    "id": hg.get("id"),
                                }

                            # Away goalie
                            ag = nhl_game.get("awayTeam", {}).get("goalie", {})
                            if not ag:
                                ag = nhl_game.get("awayTeam", {}).get("startingGoalie", {})
                            if ag and ag.get("id"):
                                first = _gs(ag.get("firstName", ""))
                                last = _gs(ag.get("lastName", ""))
                                game["away_goalie"] = {
                                    "name": f"{first} {last}".strip(),
                                    "status": "expected",
                                    "id": ag.get("id"),
                                }
                            break
        except Exception as e:
            logger.debug("NHL API goalie fallback failed: %s", e)

    # ── Enrich with playoff series context ──
    try:
        from engine.series_context import infer_series
        from engine.nhl_predict import _is_playoff_window
        if _is_playoff_window():
            for game in games:
                h = game["home"]["abbreviation"]
                a = game["away"]["abbreviation"]
                series = infer_series("nhl", h, a)
                if series.get("in_series"):
                    game["series"] = series
    except Exception as e:
        logger.debug("NHL series enrichment failed: %s", e)

    _scoreboard_cache[cache_key] = (now, games)
    # Evict oldest entries if cache grows too large
    if len(_scoreboard_cache) > MAX_CACHE_ENTRIES:
        oldest = min(_scoreboard_cache, key=lambda k: _scoreboard_cache[k][0])
        del _scoreboard_cache[oldest]
    return games


def _parse_nhl_scoreboard(data: dict) -> list[dict]:
    """Parse ESPN NHL scoreboard into game objects."""
    events = data.get("events", [])
    games = []

    for event in events:
        comps = event.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        status = comp.get("status", {})
        status_type = status.get("type", {})

        home_team = None
        away_team = None
        for c in competitors:
            team = c.get("team", {})
            raw_score = c.get("score", "0")
            if isinstance(raw_score, dict):
                score = raw_score.get("displayValue", raw_score.get("value", "0"))
            else:
                score = str(raw_score)

            # Parse record - NHL has W-L-OTL format
            record = ""
            if c.get("records"):
                record = c["records"][0].get("summary", "")

            entry = {
                "espn_id": team.get("id", ""),
                "name": team.get("displayName", team.get("name", "")),
                "abbreviation": team.get("abbreviation", ""),
                "score": score,
                "record": record,
                "logo": "",
                "winner": c.get("winner", False),
            }

            logo = team.get("logo", "")
            if isinstance(logo, str) and logo:
                entry["logo"] = logo
            elif isinstance(logo, dict):
                entry["logo"] = logo.get("href", "")
            else:
                logos = team.get("logos", [])
                if logos and isinstance(logos, list):
                    first = logos[0]
                    entry["logo"] = first.get("href", "") if isinstance(first, dict) else (first if isinstance(first, str) else "")

            if c.get("homeAway") == "home":
                home_team = entry
            else:
                away_team = entry

        if not home_team or not away_team:
            continue

        game = {
            "id": event.get("id", ""),
            "date": event.get("date", ""),
            "name": event.get("name", ""),
            "short_name": event.get("shortName", ""),
            "home": home_team,
            "away": away_team,
            "status": {
                "state": status_type.get("state", "pre"),
                "detail": status_type.get("shortDetail",
                          status_type.get("detail", "")),
                "description": status_type.get("description", ""),
                "completed": status_type.get("completed", False),
                "period": status.get("period", 0),
            },
            "venue": comp.get("venue", {}).get("fullName", ""),
            "broadcast": "",
            "odds": None,
        }

        # Broadcast
        broadcasts = comp.get("broadcasts", [])
        if broadcasts:
            names = []
            for b in broadcasts:
                for n in b.get("names", []):
                    names.append(n)
            game["broadcast"] = ", ".join(names[:2])

        # ESPN inline odds
        odds = comp.get("odds", [])
        if odds:
            o = odds[0]
            home_odds = o.get("homeTeamOdds", {}) or {}
            away_odds = o.get("awayTeamOdds", {}) or {}
            game["odds"] = {
                "spread": o.get("details", ""),
                "over_under": o.get("overUnder"),
                "home_ml": home_odds.get("moneyLine"),
                "away_ml": away_odds.get("moneyLine"),
                "home_spread_odds": home_odds.get("spreadOdds"),
                "away_spread_odds": away_odds.get("spreadOdds"),
                "home_spread_point": home_odds.get("spreadLine") or home_odds.get("line"),
                "away_spread_point": away_odds.get("spreadLine") or away_odds.get("line"),
                "over_odds": o.get("overOdds"),
                "under_odds": o.get("underOdds"),
                "provider": "ESPN",
            }

        games.append(game)

    return games


# NHL odds cache (separate from MLB)
_nhl_odds_cache: dict | None = None
_nhl_odds_cache_time: float = 0

# Negative-result TTL: when upstream returns empty, DON'T retry on every
# best-bets tick. Cache the empty dict for NHL_EMPTY_CACHE_TTL seconds so
# transient upstream failures don't hammer the source.
NHL_EMPTY_CACHE_TTL = 120


def _fetch_nhl_odds() -> dict:
    """Fetch NHL odds from Hard Rock Bet (FL operator). Cached for
    10 min; 2 min for empty results so a transient upstream failure
    doesn't hammer it."""
    global _nhl_odds_cache, _nhl_odds_cache_time

    if _nhl_odds_cache is not None:
        age = time.time() - _nhl_odds_cache_time
        ttl = 600 if _nhl_odds_cache else NHL_EMPTY_CACHE_TTL
        if age < ttl:
            return _nhl_odds_cache

    merged: dict = {}

    # 1. Hard Rock primary
    try:
        from scrapers.hardrock_odds import fetch_nhl as _hr_nhl
        hr = _hr_nhl()
        if hr:
            merged.update(hr)
            logger.info("NHL odds: %d games from Hard Rock", len(hr))
    except Exception as e:
        logger.debug("Hard Rock NHL odds failed: %s", e)

    _nhl_odds_cache = merged
    _nhl_odds_cache_time = time.time()
    return merged


# _fetch_nhl_odds_oddsapi removed 2026-05-11 along with the rest of
# the The-Odds-API integration.


# /api/accuracy migrated to routes_mlb.py (#317).


@router.get("/api/line-movement/{sport}/{matchup_key}")
def api_line_movement(sport: str, matchup_key: str):
    """Get line movement for a specific game."""
    try:
        from engine.line_movement import get_line_movement
        current_odds = {}
        if sport == "mlb":
            from engine.picks import fetch_real_odds_for_games
            all_odds = fetch_real_odds_for_games()
            current_odds = all_odds.get(matchup_key, {})
        return get_line_movement(sport, matchup_key, current_odds) or {"movement": "none"}
    except Exception as e:
        return {"error": str(e)}





