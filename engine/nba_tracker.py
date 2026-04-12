"""
NBA Q1 Pick Tracker -- records model picks and settles them against results.

Settles Q1 bets using the home_q1 and away_q1 columns from nba_games.
Bet types: Q1_SPREAD, Q1_TOTAL, Q1_ML

Usage:
    python -m engine.nba_tracker --record     # Record today's picks
    python -m engine.nba_tracker --settle     # Settle completed picks
    python -m engine.nba_tracker --summary    # Print running totals
"""

import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def _compute_clv(bet_odds, closing_odds):
    """Compute closing line value.
    Positive CLV = got better price than closing line = sharp.
    """
    if not bet_odds or not closing_odds:
        return None
    bet_implied = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 else 100 / (bet_odds + 100)
    close_implied = abs(closing_odds) / (abs(closing_odds) + 100) if closing_odds < 0 else 100 / (closing_odds + 100)
    return round((close_implied - bet_implied) * 100, 2)  # positive = we got a better price


# ESPN alternate abbreviation map (ESPN sometimes uses different abbrs)
_ALT_ABBRS = {
    "GS": "GSW", "GSW": "GS",
    "SA": "SAS", "SAS": "SA",
    "NO": "NOP", "NOP": "NO",
    "NY": "NYK", "NYK": "NY",
    "PHO": "PHX", "PHX": "PHO",
    "UTAH": "UTA", "UTA": "UTAH",
    "WSH": "WAS", "WAS": "WSH",
    "BKN": "BK", "BK": "BKN",
    "CHA": "CHO", "CHO": "CHA",
}


def _fetch_nba_scoreboard(date: str) -> list[dict]:
    """Fetch NBA scoreboard from ESPN for a given date (YYYY-MM-DD)."""
    espn_date = date.replace("-", "")
    url = (
        "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
        f"/scoreboard?dates={espn_date}"
    )
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("events", [])
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to fetch NBA scoreboard for %s: %s", date, e)
        return []


def _parse_q1_scores(event: dict) -> dict | None:
    """Parse Q1 scores and metadata from an ESPN event.

    Returns dict with home_abbr, away_abbr, home_q1, away_q1, etc.
    Returns None if Q1 scores are not available.
    """
    comp = event.get("competitions", [{}])[0]
    status_type = comp.get("status", {}).get("type", {})
    completed = status_type.get("completed", False)

    if not completed:
        return None

    result = {"game_id": event.get("id", "")}

    for team_entry in comp.get("competitors", []):
        team = team_entry.get("team", {})
        abbr = team.get("abbreviation", "")
        is_home = team_entry.get("homeAway") == "home"
        score = 0
        raw_score = team_entry.get("score", "0")
        if isinstance(raw_score, (int, str)) and str(raw_score).isdigit():
            score = int(raw_score)

        # Parse linescores for Q1
        linescores = team_entry.get("linescores", [])
        q1 = None
        if linescores:
            val = linescores[0].get("value")
            if val is not None:
                q1 = int(val)

        if is_home:
            result["home_abbr"] = abbr
            result["home_score"] = score
            result["home_q1"] = q1
        else:
            result["away_abbr"] = abbr
            result["away_score"] = score
            result["away_q1"] = q1

    if result.get("home_q1") is None or result.get("away_q1") is None:
        return None

    result["q1_total"] = result["home_q1"] + result["away_q1"]
    result["q1_margin"] = result["home_q1"] - result["away_q1"]  # positive = home won Q1

    return result


# ESPN scoreboard abbreviations that don't match the Odds API / internal
# abbrs used elsewhere. Extend this when a new mismatch shows up.
_ESPN_TO_INTERNAL_ABBR = {
    "GS": "GSW",     # Golden State Warriors
    # ESPN sometimes uses these too:
    "NOP": "NO", "NYK": "NY", "SAS": "SA", "UTA": "UTAH", "WAS": "WSH",
}


def _normalize_espn_abbr(abbr: str) -> str:
    """Map an ESPN scoreboard team abbreviation to the internal form used
    by nba_odds / nba_db / nba_picks."""
    return _ESPN_TO_INTERNAL_ABBR.get(abbr, abbr)


def record_picks(date: str | None = None, min_edge: float = 1.5,
                 force: bool = False) -> list[dict]:
    """Run NBA Q1 model on today's games and record the best pick per game.

    Args:
        date: Target date (YYYY-MM-DD). Defaults to today.
        min_edge: Minimum edge percentage to record a pick.
        force: If True, delete any existing pick for each game before
            recording so the latest model/odds take precedence. Use this
            when the model or odds have materially changed during the day
            (e.g. starter-rest news lands after the first sync).

    Returns:
        List of recorded pick dicts.
    """
    from .nba_db import get_conn

    conn = get_conn()
    target_date = date or datetime.now().strftime("%Y-%m-%d")

    from .nba_q1_predict import generate_q1_picks

    # Fetch today's games from ESPN
    events = _fetch_nba_scoreboard(target_date)
    if not events:
        logger.info("No NBA games found for %s", target_date)
        return []

    # Pull Q1 odds using the full fallback chain (Odds API → DK → ESPN).
    # When all three fail, picks fall back to -110 defaults and Q1 ML only.
    q1_odds_map = {}
    try:
        from scrapers.nba_odds import fetch_all_nba_odds
        q1_odds_map = fetch_all_nba_odds()
    except Exception as e:
        logger.debug("NBA Q1 odds fetch failed: %s", e)

    recorded = []

    for event in events:
        game_id = event.get("id", "")
        comp = event.get("competitions", [{}])[0]

        # Skip completed games
        status = comp.get("status", {}).get("type", {})
        if status.get("completed", False):
            continue

        competitors = comp.get("competitors", [])
        h_abbr = ""
        a_abbr = ""
        for c in competitors:
            team = c.get("team", {})
            abbr = team.get("abbreviation", "")
            # Normalize ESPN scoreboard abbrs to internal form so lookups
            # in the odds map (keyed by internal abbrs like GSW/NO/NY) hit.
            abbr = _normalize_espn_abbr(abbr)
            if c.get("homeAway") == "home":
                h_abbr = abbr
            else:
                a_abbr = abbr

        if not h_abbr or not a_abbr:
            continue

        matchup = f"{a_abbr} @ {h_abbr}"

        # Duplicate handling:
        #   force=False: skip games that already have a pick recorded
        #   force=True:  delete existing picks for this game so the new
        #                model/odds take effect.
        if force:
            conn.execute("DELETE FROM nba_picks WHERE game_id = ? "
                         "AND result IS NULL", (game_id,))
        else:
            existing = conn.execute(
                "SELECT COUNT(*) as c FROM nba_picks WHERE game_id = ?",
                (game_id,)
            ).fetchone()["c"]
            if existing > 0:
                continue

        # Merge real Odds API Q1 markets (when available) with -110 defaults
        market_odds = q1_odds_map.get(f"{a_abbr}@{h_abbr}") or {}
        odds_dict = {
            "q1_spread": market_odds.get("q1_spread"),
            "q1_total": market_odds.get("q1_total"),
            "q1_spread_home_odds": market_odds.get("q1_spread_home_odds", -110),
            "q1_spread_away_odds": market_odds.get("q1_spread_away_odds", -110),
            "q1_over_odds": market_odds.get("q1_over_odds", -110),
            "q1_under_odds": market_odds.get("q1_under_odds", -110),
            "home_ml": market_odds.get("q1_home_ml") or market_odds.get("home_ml"),
            "away_ml": market_odds.get("q1_away_ml") or market_odds.get("away_ml"),
        }

        # Generate Q1 ML picks (always available without specific Q1 odds)
        picks = generate_q1_picks(h_abbr, a_abbr, odds_dict)
        if not picks:
            continue

        # Record the best pick (highest edge)
        best = picks[0]
        if best["edge"] < min_edge:
            continue

        conn.execute("""
            INSERT INTO nba_picks (game_id, date, matchup, bet_type, pick,
                                   model_prob, edge, odds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (game_id, target_date, matchup, best["type"], best["pick"],
              best["prob"], best["edge"], best["odds"]))

        recorded.append({
            "matchup": matchup, "type": best["type"],
            "pick": best["pick"], "prob": round(best["prob"], 3),
            "edge": round(best["edge"], 1), "odds": best["odds"],
        })

    conn.commit()
    return recorded


def settle_picks() -> dict:
    """Settle all pending NBA Q1 picks against final game results.

    Uses Q1 scores from ESPN scoreboard to determine outcomes.
    Handles Q1_SPREAD, Q1_TOTAL, and Q1_ML bet types.
    """
    from .nba_db import get_conn

    conn = get_conn()

    pending = conn.execute(
        "SELECT * FROM nba_picks WHERE result IS NULL"
    ).fetchall()

    if not pending:
        return {"settled": 0, "message": "No pending NBA picks"}

    # Group by date to fetch scoreboards efficiently
    dates = set()
    for p in pending:
        dates.add(p["date"])

    # Fetch final Q1 scores for each date
    final_q1: dict[str, dict] = {}  # game_id -> q1 scores dict
    for d in dates:
        events = _fetch_nba_scoreboard(d)
        for event in events:
            q1_data = _parse_q1_scores(event)
            if q1_data:
                final_q1[q1_data["game_id"]] = q1_data

    # Fetch current NBA odds for closing line capture
    closing_odds_map = {}
    try:
        import os
        from pathlib import Path as _Path
        key_file = _Path(__file__).resolve().parent.parent / "data" / "odds_api_key.txt"
        api_key = os.environ.get("ODDS_API_KEY") or (key_file.read_text().strip() if key_file.exists() else None)
        if api_key:
            _url = (f"https://api.the-odds-api.com/v4/sports/basketball_nba/odds/"
                    f"?apiKey={api_key}&regions=us&markets=h2h"
                    f"&oddsFormat=american&bookmakers=draftkings")
            req = urllib.request.Request(_url, headers={"User-Agent": "NBATracker/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                _odds_data = json.loads(resp.read().decode())

            _NBA_ABBR = {
                "Atlanta Hawks": "ATL", "Boston Celtics": "BOS",
                "Brooklyn Nets": "BKN", "Charlotte Hornets": "CHA",
                "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
                "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN",
                "Detroit Pistons": "DET", "Golden State Warriors": "GSW",
                "Houston Rockets": "HOU", "Indiana Pacers": "IND",
                "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL",
                "Memphis Grizzlies": "MEM", "Miami Heat": "MIA",
                "Milwaukee Bucks": "MIL", "Minnesota Timberwolves": "MIN",
                "New Orleans Pelicans": "NOP", "New York Knicks": "NYK",
                "Oklahoma City Thunder": "OKC", "Orlando Magic": "ORL",
                "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX",
                "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC",
                "San Antonio Spurs": "SAS", "Toronto Raptors": "TOR",
                "Utah Jazz": "UTA", "Washington Wizards": "WAS",
            }
            for _g in (_odds_data or []):
                _home = _g.get("home_team", "")
                _away = _g.get("away_team", "")
                _h_ab = _NBA_ABBR.get(_home, _home[:3].upper())
                _a_ab = _NBA_ABBR.get(_away, _away[:3].upper())
                _key = f"{_a_ab}@{_h_ab}"
                _res = {}
                for _bk in _g.get("bookmakers", [])[:1]:
                    for _mkt in _bk.get("markets", []):
                        if _mkt.get("key") == "h2h":
                            for _o in _mkt.get("outcomes", []):
                                if _o.get("name") == _home:
                                    _res["home_ml"] = _o.get("price")
                                elif _o.get("name") == _away:
                                    _res["away_ml"] = _o.get("price")
                if _res:
                    closing_odds_map[_key] = _res
    except Exception as e:
        logger.debug("Could not fetch NBA closing odds: %s", e)

    settled = 0
    wins = 0
    losses = 0

    for pick in pending:
        pick = dict(pick)
        game_id = pick["game_id"]

        game = final_q1.get(game_id)
        if not game:
            continue

        h_q1 = game["home_q1"]
        a_q1 = game["away_q1"]
        q1_total = game["q1_total"]
        q1_margin = game["q1_margin"]  # positive = home won Q1
        h = game["home_abbr"]
        a = game["away_abbr"]

        # Capture closing odds if not already stored
        if not pick.get("closing_odds") and h and a:
            # Try direct and alternate abbreviations
            game_cl_odds = None
            for a_try in [a, _ALT_ABBRS.get(a, "")]:
                for h_try in [h, _ALT_ABBRS.get(h, "")]:
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
                if bt_tmp == "Q1_ML":
                    pick_team = pk_tmp.split()[0]
                    is_home = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))
                    closing = game_cl_odds.get("home_ml") if is_home else game_cl_odds.get("away_ml")
                elif bt_tmp in ("Q1_SPREAD", "Q1_TOTAL"):
                    # Q1-specific lines aren't typically in the odds API h2h market;
                    # use full-game ML as a proxy if Q1_ML, skip for spread/total
                    closing = None
                if closing is not None:
                    conn.execute("UPDATE nba_picks SET closing_odds = ? WHERE id = ?",
                                 (int(closing), pick["id"]))
                    pick["closing_odds"] = int(closing)

        bt = pick["bet_type"]
        pk = pick["pick"]
        odds = pick["odds"] or -110
        result = None

        if bt == "Q1_ML":
            # Pick format: "LAL Q1 ML" -- first token is team abbreviation
            pick_team = pk.split()[0]
            home_won_q1 = q1_margin > 0

            # Check both direct and alternate abbreviations
            is_home_pick = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))
            is_away_pick = (pick_team == a or pick_team == _ALT_ABBRS.get(a, ""))

            if is_home_pick:
                if home_won_q1:
                    result = "W"
                elif q1_margin == 0:
                    result = "P"
                else:
                    result = "L"
            elif is_away_pick:
                if not home_won_q1 and q1_margin != 0:
                    result = "W"
                elif q1_margin == 0:
                    result = "P"
                else:
                    result = "L"

        elif bt == "Q1_SPREAD":
            # Pick format: "LAL -2.5 Q1" or "BOS +2.5 Q1"
            parts = pk.split()
            if len(parts) >= 2:
                pick_team = parts[0]
                spread = float(parts[1])

                is_home_pick = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))

                if is_home_pick:
                    actual_margin = h_q1 - a_q1
                else:
                    actual_margin = a_q1 - h_q1

                covered = actual_margin + spread
                if covered > 0:
                    result = "W"
                elif covered == 0:
                    result = "P"
                else:
                    result = "L"

        elif bt == "Q1_TOTAL":
            # Pick format: "Over 55.5 Q1" or "Under 55.5 Q1"
            parts = pk.split()
            if len(parts) >= 2:
                direction = parts[0].lower()
                line = float(parts[1])

                if direction == "over":
                    if q1_total > line:
                        result = "W"
                    elif q1_total < line:
                        result = "L"
                    else:
                        result = "P"
                else:  # under
                    if q1_total < line:
                        result = "W"
                    elif q1_total > line:
                        result = "L"
                    else:
                        result = "P"

        if result is None:
            continue

        # Calculate profit (based on $100 unit)
        if result == "W":
            if odds > 0:
                profit = float(odds)
            else:
                profit = 100 / abs(odds) * 100
            wins += 1
        elif result == "L":
            profit = -100.0
            losses += 1
        else:  # Push
            profit = 0.0

        conn.execute("""
            UPDATE nba_picks SET result = ?, profit = ?, settled_at = datetime('now')
            WHERE id = ?
        """, (result, round(profit, 2), pick["id"]))
        settled += 1

    conn.commit()

    return {
        "settled": settled,
        "wins": wins,
        "losses": losses,
        "pending_remaining": conn.execute(
            "SELECT COUNT(*) as c FROM nba_picks WHERE result IS NULL"
        ).fetchone()["c"],
    }


def get_pick_summary() -> dict:
    """Get running totals across all NBA Q1 picks."""
    from .nba_db import get_conn

    conn = get_conn()

    summary = {}
    for bt in ["Q1_SPREAD", "Q1_TOTAL", "Q1_ML"]:
        row = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
                COALESCE(SUM(profit), 0) as profit
            FROM nba_picks WHERE bet_type = ?
        """, (bt,)).fetchone()

        total = row["total"] or 0
        w = row["wins"] or 0
        l = row["losses"] or 0
        settled_count = w + l
        summary[bt] = {
            "total": total,
            "wins": w,
            "losses": l,
            "pushes": row["pushes"] or 0,
            "pending": row["pending"] or 0,
            "profit": round(row["profit"], 2),
            "win_pct": round(w / settled_count * 100, 1) if settled_count > 0 else 0,
            "roi": round(row["profit"] / settled_count, 1) if settled_count > 0 else 0,
        }

    recent = conn.execute("""
        SELECT * FROM nba_picks ORDER BY created_at DESC LIMIT 30
    """).fetchall()

    totals = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
            COALESCE(SUM(profit), 0) as profit
        FROM nba_picks
    """).fetchone()

    tw = totals["wins"] or 0
    tl = totals["losses"] or 0

    # Compute CLV across all settled picks that have closing odds
    clv_rows = conn.execute("""
        SELECT odds, closing_odds FROM nba_picks
        WHERE result IS NOT NULL AND odds IS NOT NULL AND closing_odds IS NOT NULL
    """).fetchall()
    clv_values = []
    for r in clv_rows:
        clv = _compute_clv(r["odds"], r["closing_odds"])
        if clv is not None:
            clv_values.append(clv)
    avg_clv = round(sum(clv_values) / len(clv_values), 2) if clv_values else None

    return {
        "by_type": summary,
        "overall": {
            "total": totals["total"] or 0,
            "wins": tw,
            "losses": tl,
            "pending": totals["pending"] or 0,
            "profit": round(totals["profit"] or 0, 2),
            "win_pct": round(tw / (tw + tl) * 100, 1) if (tw + tl) > 0 else 0,
            "avg_clv": avg_clv,
            "clv_sample": len(clv_values),
        },
        "recent": [dict(r) for r in recent],
    }


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = set(sys.argv[1:])

    if "--record" in args:
        force = "--force" in args
        print(f"Recording today's NBA Q1 picks{' (force refresh)' if force else ''}...",
              flush=True)
        picks = record_picks(force=force)
        print(f"Recorded {len(picks)} NBA Q1 picks:")
        for p in picks:
            print(f"  {p['matchup']} | {p['type']:12s} | {p['pick']:20s} | "
                  f"{p['prob']:.1%} | edge: {p['edge']:+.1f}%")

    elif "--settle" in args:
        print("Settling completed NBA Q1 picks...", flush=True)
        result = settle_picks()
        print(f"Settled: {result.get('settled', 0)} "
              f"({result.get('wins', 0)}W-{result.get('losses', 0)}L)")
        print(f"Pending: {result.get('pending_remaining', 0)}")
        if result.get("message"):
            print(f"  {result['message']}")

    elif "--summary" in args:
        summary = get_pick_summary()
        overall = summary["overall"]
        print(f"\n{'='*55}")
        print(f"  NBA Q1 PICK TRACKER -- Running Totals")
        print(f"{'='*55}")
        print(f"  Total picks: {overall['total']}")
        print(f"  Record: {overall['wins']}-{overall['losses']} ({overall['win_pct']}%)")
        print(f"  Profit: ${overall['profit']:+.2f}")
        print(f"  Pending: {overall['pending']}")
        if overall.get("avg_clv") is not None:
            print(f"  Avg CLV: {overall['avg_clv']:+.2f}% ({overall['clv_sample']} picks)")
        print()
        for bt, label in [("Q1_SPREAD", "Q1 Spread"), ("Q1_TOTAL", "Q1 Total"),
                          ("Q1_ML", "Q1 Moneyline")]:
            s = summary["by_type"][bt]
            if s["total"] == 0:
                continue
            print(f"  {label:14s}: {s['wins']}-{s['losses']} "
                  f"({s['win_pct']}%) ${s['profit']:+.2f} "
                  f"[ROI: {s['roi']:+.1f}]")
        print(f"{'='*55}")

        # Show recent picks
        recent = summary.get("recent", [])
        if recent:
            print(f"\n  Recent picks (last {len(recent)}):")
            for p in recent[:10]:
                result_str = p.get("result") or "PEND"
                profit_str = f"${p['profit']:+.0f}" if p.get("profit") is not None else ""
                print(f"    {p['date']} | {p['matchup']:12s} | {p['bet_type']:12s} | "
                      f"{p['pick']:20s} | {result_str:4s} {profit_str}")

    else:
        print("Usage: python -m engine.nba_tracker --record | --settle | --summary")
