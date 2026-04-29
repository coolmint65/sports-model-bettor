"""NHL pick settlement: full-game ML / O/U / PL plus period-specific
derivative bet types (Period Total, Period BTS, Period DNB, Total O/E,
Overtime, BTS, Team Total).
"""

from __future__ import annotations
import json
import logging
from datetime import datetime, timedelta

from ._helpers import _get_nhl_db
from ._scoreboard import _fetch_nhl_scoreboard

logger = logging.getLogger(__name__)


def settle_picks() -> dict:
    """Settle all pending NHL picks against final game results."""
    conn = _get_nhl_db()

    # Self-heal: auto-push picks pending >7 days. Same rationale as
    # MLB / NBA settlers.
    stale_cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    stale = conn.execute(
        "SELECT id, date, matchup, bet_type, pick FROM nhl_picks "
        "WHERE result IS NULL AND date < ?", (stale_cutoff,),
    ).fetchall()
    auto_pushed = 0
    for row in stale:
        conn.execute(
            "UPDATE nhl_picks SET result='P', profit=0, settled_at=datetime('now') "
            "WHERE id=?", (row["id"],),
        )
        logger.warning(
            "NHL settle: auto-pushed stale pending id=%s date=%s "
            "%s %s/%s — older than 7 days",
            row["id"], row["date"], row["matchup"], row["bet_type"], row["pick"],
        )
        auto_pushed += 1
    if auto_pushed:
        conn.commit()

    pending = conn.execute(
        "SELECT * FROM nhl_picks WHERE result IS NULL"
    ).fetchall()

    if not pending:
        return {"settled": 0, "auto_pushed": auto_pushed,
                "message": "No pending NHL picks"}

    # Group by date to fetch scoreboards
    dates = set()
    for p in pending:
        dates.add(p["date"])

    # Fetch scoreboard for each date. Include in-progress games so
    # period-specific picks (Period Total P1, Period BTS P1, Period
    # DNB P1) can settle as soon as their period ends — no waiting
    # for the full game. Full-game bet types (ML, O/U, PL, Team Total,
    # Total O/E, Overtime, BTS) still gate on `is_completed`.
    final_scores = {}
    for d in dates:
        events = _fetch_nhl_scoreboard(d)
        for event in events:
            eid = event.get("id", "")
            comp = event.get("competitions", [{}])[0]
            status_info = comp.get("status", {})
            status = status_info.get("type", {})
            state = status.get("state", "pre")
            if state == "pre":
                continue
            is_completed = status.get("completed", False)

            cur_period = status_info.get("period") or 0
            detail = (status.get("detail") or status.get("shortDetail")
                      or "").lower()
            went_to_ot = (is_completed and (cur_period > 3
                          or "ot" in detail or "shootout" in detail))

            home_score = 0
            away_score = 0
            h_abbr = ""
            a_abbr = ""
            home_periods: list[int] = []
            away_periods: list[int] = []
            for c in comp.get("competitors", []):
                team = c.get("team", {})
                raw = c.get("score", "0")
                score = int(raw) if isinstance(raw, (int, str)) and str(raw).isdigit() else 0
                ls_raw = c.get("linescores") or []
                periods = []
                for ls in ls_raw:
                    val = ls.get("value")
                    try:
                        periods.append(int(val))
                    except (TypeError, ValueError):
                        periods.append(0)
                if c.get("homeAway") == "home":
                    home_score = score
                    h_abbr = team.get("abbreviation", "")
                    home_periods = periods
                else:
                    away_score = score
                    a_abbr = team.get("abbreviation", "")
                    away_periods = periods

            linescore_depth = min(len(home_periods), len(away_periods))
            if is_completed:
                periods_locked = linescore_depth
            else:
                periods_locked = max(0, min(linescore_depth, cur_period - 1))

            final_scores[eid] = {
                "home_abbr": h_abbr, "away_abbr": a_abbr,
                "home_score": home_score, "away_score": away_score,
                "total": home_score + away_score,
                "home_periods": home_periods,
                "away_periods": away_periods,
                "went_to_ot": went_to_ot,
                "is_completed": is_completed,
                "periods_locked": periods_locked,
            }

    # Fetch current NHL odds for closing line capture
    closing_odds_map = _fetch_closing_map_for_settle()

    settled = 0
    wins = 0
    losses = 0

    for pick in pending:
        pick = dict(pick)
        game_id = pick["game_id"]

        game = final_scores.get(game_id)
        if not game:
            continue

        hs = game["home_score"]
        as_ = game["away_score"]
        total = game["total"]
        h = game["home_abbr"]
        a = game["away_abbr"]

        # Capture closing odds if not already stored
        if not pick.get("closing_odds") and h and a:
            _ALT_CL = {
                "TB": "TBL", "TBL": "TB", "NJ": "NJD", "NJD": "NJ",
                "SJ": "SJS", "SJS": "SJ", "LA": "LAK", "LAK": "LA",
                "WAS": "WSH", "WSH": "WAS", "CLB": "CBJ", "CBJ": "CLB",
                "MON": "MTL", "MTL": "MON", "NAS": "NSH", "NSH": "NAS",
            }
            game_cl_odds = None
            for a_try in [a, _ALT_CL.get(a, "")]:
                for h_try in [h, _ALT_CL.get(h, "")]:
                    if a_try and h_try:
                        game_cl_odds = closing_odds_map.get(f"{a_try}@{h_try}")
                        if game_cl_odds:
                            break
                if game_cl_odds:
                    break
            if game_cl_odds:
                bt_tmp = pick["bet_type"]
                pk_tmp = pick["pick"]
                closing = None
                if bt_tmp == "ML":
                    closing = game_cl_odds.get("home_ml") if pk_tmp == h else game_cl_odds.get("away_ml")
                elif bt_tmp == "O/U":
                    closing = game_cl_odds.get("over_odds") if "Over" in pk_tmp else game_cl_odds.get("under_odds")
                elif bt_tmp == "PL":
                    pick_team = pk_tmp.split()[0] if pk_tmp.split() else ""
                    closing = game_cl_odds.get("home_spread_odds") if pick_team == h else game_cl_odds.get("away_spread_odds")
                if closing is not None:
                    conn.execute("UPDATE nhl_picks SET closing_odds = ? WHERE id = ?",
                                 (int(closing), pick["id"]))
                    pick["closing_odds"] = int(closing)

        bt = pick["bet_type"]
        pk = pick["pick"]
        odds = pick["odds"] or -110
        result = None

        # Per-bet-type readiness check.
        _FULL_GAME_TYPES = {
            "ML", "O/U", "ALT O/U", "PL", "ALT PL",
            "Team Total", "Total O/E", "Overtime", "BTS",
        }
        if bt in _FULL_GAME_TYPES and not game.get("is_completed", False):
            continue

        if bt == "ML":
            home_won = hs > as_
            if pk == h:
                won = home_won
            else:
                won = not home_won
            result = "W" if won else "L"

        elif bt in ("O/U", "ALT O/U"):
            if "Over" in pk:
                line = float(pk.split()[-1])
                if total > line:
                    result = "W"
                elif total < line:
                    result = "L"
                else:
                    result = "P"
            else:
                line = float(pk.split()[-1])
                if total < line:
                    result = "W"
                elif total > line:
                    result = "L"
                else:
                    result = "P"

        elif bt in ("PL", "ALT PL"):
            parts = pk.split()
            pick_team = parts[0] if parts else ""
            spread = float(parts[1]) if len(parts) > 1 else 1.5

            if pick_team == h:
                team_margin = hs - as_
            else:
                team_margin = as_ - hs

            if team_margin + spread > 0:
                result = "W"
            elif team_margin + spread == 0:
                result = "P"
            else:
                result = "L"

        # ── Phase 1 derivative markets ──
        elif bt == "Team Total":
            parts = pk.split()
            if len(parts) >= 3:
                pick_team = parts[0]
                direction = parts[1].lower()
                try:
                    line = float(parts[2])
                except ValueError:
                    continue
                team_goals = hs if pick_team == h else as_
                if team_goals > line:
                    result = "W" if direction == "over" else "L"
                elif team_goals < line:
                    result = "L" if direction == "over" else "W"
                else:
                    result = "P"

        elif bt == "Period Total":
            home_periods = game.get("home_periods") or []
            away_periods = game.get("away_periods") or []
            parts = pk.split()
            if len(parts) >= 3 and parts[0].startswith("P"):
                try:
                    period_n = int(parts[0][1:])
                    line = float(parts[2])
                except ValueError:
                    continue
                direction = parts[1].lower()
                if period_n < 1 or period_n > game.get("periods_locked", 0):
                    continue
                period_total = (home_periods[period_n - 1]
                                + away_periods[period_n - 1])
                if period_total > line:
                    result = "W" if direction == "over" else "L"
                elif period_total < line:
                    result = "L" if direction == "over" else "W"
                else:
                    result = "P"

        elif bt == "Period BTS":
            home_periods = game.get("home_periods") or []
            away_periods = game.get("away_periods") or []
            parts = pk.split()
            if len(parts) >= 3 and parts[0].startswith("P"):
                try:
                    period_n = int(parts[0][1:])
                except ValueError:
                    continue
                direction = parts[2].lower()
                if period_n < 1 or period_n > game.get("periods_locked", 0):
                    continue
                bts_yes = (home_periods[period_n - 1] > 0
                           and away_periods[period_n - 1] > 0)
                if direction == "yes":
                    result = "W" if bts_yes else "L"
                else:
                    result = "W" if not bts_yes else "L"

        elif bt == "Period DNB":
            home_periods = game.get("home_periods") or []
            away_periods = game.get("away_periods") or []
            parts = pk.split()
            # Accept both old "P{n} DNB {team}" (3 tokens) and new
            # "P{n} {team}" (2 tokens) formats. The "DNB" label was
            # confusing the user; new picks drop it. Settler walks
            # tokens to find the team rather than hard-indexing.
            if len(parts) >= 2 and parts[0].startswith("P"):
                try:
                    period_n = int(parts[0][1:])
                except ValueError:
                    continue
                # Team is the last non-"DNB" token
                pick_team = next((t for t in reversed(parts[1:]) if t != "DNB"), "")
                if not pick_team:
                    continue
                if period_n < 1 or period_n > game.get("periods_locked", 0):
                    continue
                hp = home_periods[period_n - 1]
                ap = away_periods[period_n - 1]
                if hp == ap:
                    result = "P"  # tied period pushes
                elif pick_team == h:
                    result = "W" if hp > ap else "L"
                else:
                    result = "W" if ap > hp else "L"

        elif bt == "Total O/E":
            parts = pk.split()
            if len(parts) >= 2:
                direction = parts[1].lower()
                is_odd = (total % 2 == 1)
                if direction == "odd":
                    result = "W" if is_odd else "L"
                else:
                    result = "W" if not is_odd else "L"

        elif bt == "Overtime":
            went_to_ot = game.get("went_to_ot", False)
            parts = pk.split()
            if len(parts) >= 2:
                direction = parts[1].lower()
                if direction == "yes":
                    result = "W" if went_to_ot else "L"
                else:
                    result = "W" if not went_to_ot else "L"

        elif bt == "BTS":
            bts_yes = (hs > 0 and as_ > 0)
            parts = pk.split()
            if len(parts) >= 2:
                direction = parts[1].lower()
                if direction == "yes":
                    result = "W" if bts_yes else "L"
                else:
                    result = "W" if not bts_yes else "L"

        if result is None:
            continue

        if result == "W":
            profit = (odds if odds > 0 else 100 / abs(odds) * 100)
            wins += 1
        elif result == "L":
            profit = -100
            losses += 1
        else:
            profit = 0

        conn.execute("""
            UPDATE nhl_picks SET result = ?, profit = ?, settled_at = datetime('now')
            WHERE id = ?
        """, (result, round(profit, 2), pick["id"]))
        settled += 1

    conn.commit()

    return {
        "settled": settled,
        "wins": wins,
        "losses": losses,
        "pending_remaining": conn.execute(
            "SELECT COUNT(*) as c FROM nhl_picks WHERE result IS NULL"
        ).fetchone()["c"],
    }


def _fetch_closing_map_for_settle() -> dict:
    """Pull the-odds-api NHL prices for closing-odds capture during
    settle. Returns {f"{away}@{home}": {ml/spread/total dict}}."""
    closing_odds_map: dict = {}
    try:
        import os
        import urllib.request as _urlreq
        from pathlib import Path as _Path
        key_file = _Path(__file__).resolve().parent.parent.parent / "data" / "odds_api_key.txt"
        api_key = os.environ.get("ODDS_API_KEY") or (key_file.read_text().strip() if key_file.exists() else None)
        if not api_key:
            return closing_odds_map
        _url = (f"https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds/"
                f"?apiKey={api_key}&regions=us&markets=h2h,spreads,totals"
                f"&oddsFormat=american&bookmakers=draftkings")
        _req = _urlreq.Request(_url, headers={"User-Agent": "NHLTracker/1.0"})
        with _urlreq.urlopen(_req, timeout=15) as _resp:
            _odds_data = json.loads(_resp.read().decode())

        _NHL_ABBR = {
            "Anaheim Ducks": "ANA", "Utah Hockey Club": "UTA",
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
        for _g in (_odds_data or []):
            _home = _g.get("home_team", "")
            _away = _g.get("away_team", "")
            _h_ab = _NHL_ABBR.get(_home, _home[:3].upper())
            _a_ab = _NHL_ABBR.get(_away, _away[:3].upper())
            _key = f"{_a_ab}@{_h_ab}"
            _res = {}
            for _bk in _g.get("bookmakers", [])[:1]:
                for _mkt in _bk.get("markets", []):
                    _mk = _mkt.get("key", "")
                    for _o in _mkt.get("outcomes", []):
                        if _mk == "h2h":
                            if _o.get("name") == _home:
                                _res["home_ml"] = _o.get("price")
                            elif _o.get("name") == _away:
                                _res["away_ml"] = _o.get("price")
                        elif _mk == "spreads":
                            if _o.get("name") == _home:
                                _res["home_spread_odds"] = _o.get("price")
                            elif _o.get("name") == _away:
                                _res["away_spread_odds"] = _o.get("price")
                        elif _mk == "totals":
                            _nm = _o.get("name", "").lower()
                            if "over" in _nm:
                                _res["over_odds"] = _o.get("price")
                            elif "under" in _nm:
                                _res["under_odds"] = _o.get("price")
            if _res:
                closing_odds_map[_key] = _res
    except Exception as e:
        logger.debug("Could not fetch NHL closing odds: %s", e)
    return closing_odds_map
