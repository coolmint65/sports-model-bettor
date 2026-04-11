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


def record_picks(date: str | None = None, min_edge: float = 1.5) -> list[dict]:
    """Run NBA Q1 model on today's games and record the best pick per game.

    Args:
        date: Target date (YYYY-MM-DD). Defaults to today.
        min_edge: Minimum edge percentage to record a pick.

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
            if c.get("homeAway") == "home":
                h_abbr = abbr
            else:
                a_abbr = abbr

        if not h_abbr or not a_abbr:
            continue

        matchup = f"{a_abbr} @ {h_abbr}"

        # Skip if already recorded
        existing = conn.execute(
            "SELECT COUNT(*) as c FROM nba_picks WHERE game_id = ?", (game_id,)
        ).fetchone()["c"]
        if existing > 0:
            continue

        # Generate picks (use default -110 odds if no real odds available)
        odds_dict = {
            "q1_spread": None,  # No spread available without odds feed
            "q1_total": None,
            "q1_spread_home_odds": -110,
            "q1_spread_away_odds": -110,
            "q1_over_odds": -110,
            "q1_under_odds": -110,
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

    return {
        "by_type": summary,
        "overall": {
            "total": totals["total"] or 0,
            "wins": tw,
            "losses": tl,
            "pending": totals["pending"] or 0,
            "profit": round(totals["profit"] or 0, 2),
            "win_pct": round(tw / (tw + tl) * 100, 1) if (tw + tl) > 0 else 0,
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
        print("Recording today's NBA Q1 picks...", flush=True)
        picks = record_picks()
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
