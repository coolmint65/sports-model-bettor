"""
Pick tracker — records model picks and settles them against results.

Call record_picks() before games start to log today's picks.
Call settle_picks() after games finish to mark W/L and calculate profit.

Usage:
    python -m engine.tracker --record     # Record today's picks
    python -m engine.tracker --settle     # Settle completed picks
    python -m engine.tracker --summary    # Print running totals
"""

import logging
from datetime import datetime

from .db import get_conn, get_team_by_id
from .mlb_predict import predict_matchup
from .bankroll import ml_to_implied_prob

logger = logging.getLogger(__name__)

SEASON = datetime.now().year


def record_picks(date: str | None = None, min_edge: float = 1.5) -> list[dict]:
    """
    Run model on today's games and record picks with edge >= min_edge.
    Only records picks that haven't already been recorded for this date.
    """
    conn = get_conn()
    target_date = date or datetime.now().strftime("%Y-%m-%d")

    # Get today's scheduled/live games
    games = conn.execute("""
        SELECT * FROM games WHERE date = ? AND status IN ('scheduled', 'live')
    """, (target_date,)).fetchall()

    if not games:
        # Try from games that are final today (late recording)
        games = conn.execute("""
            SELECT * FROM games WHERE date = ?
        """, (target_date,)).fetchall()

    recorded = []
    for game in games:
        game = dict(game)
        game_id = game.get("mlb_game_id")

        # Skip if already recorded
        existing = conn.execute(
            "SELECT COUNT(*) as c FROM picks WHERE game_id = ?", (game_id,)
        ).fetchone()["c"]
        if existing > 0:
            continue

        home_id = game.get("home_team_id")
        away_id = game.get("away_team_id")
        if not home_id or not away_id:
            continue

        pred = predict_matchup(
            home_team_id=home_id,
            away_team_id=away_id,
            home_pitcher_id=game.get("home_pitcher_id"),
            away_pitcher_id=game.get("away_pitcher_id"),
            venue=game.get("venue"),
        )

        if "error" in pred:
            continue

        home_team = get_team_by_id(home_id)
        away_team = get_team_by_id(away_id)
        h = home_team["abbreviation"] if home_team else "?"
        a = away_team["abbreviation"] if away_team else "?"
        matchup = f"{a} @ {h}"

        wp = pred.get("win_prob", {})
        fi = pred.get("first_inning", {})
        rl = pred.get("run_line", {})
        total = pred.get("total", 0)

        picks = []

        # Moneyline
        if wp.get("home", 0) > wp.get("away", 0):
            ml_pick, ml_prob = h, wp["home"]
            ml_implied = 0.60  # ~-150
            ml_odds = -150
        else:
            ml_pick, ml_prob = a, wp["away"]
            ml_implied = 0.435  # ~+130
            ml_odds = 130
        ml_edge = (ml_prob - ml_implied) * 100
        if ml_edge >= min_edge:
            picks.append(("ml", ml_pick, ml_prob, ml_edge, ml_odds))

        # Over/Under (use 8.5 as default line)
        ou_line = 8.5
        ou_probs = pred.get("over_under", {})
        p_over = 0.5
        for lk, probs in ou_probs.items():
            if abs(float(lk) - ou_line) < 0.5:
                p_over = probs.get("over", 0.5)
                ou_line = float(lk)
                break
        ou_pick = f"Over {ou_line}" if p_over > 0.5 else f"Under {ou_line}"
        ou_prob = p_over if p_over > 0.5 else 1 - p_over
        ou_edge = (ou_prob - 0.524) * 100
        if ou_edge >= min_edge:
            picks.append(("ou", ou_pick, ou_prob, ou_edge, -110))

        # NRFI
        nrfi_prob = fi.get("nrfi", 0.5)
        if nrfi_prob > 0.5:
            nrfi_pick, nrfi_p = "NRFI", nrfi_prob
        else:
            nrfi_pick, nrfi_p = "YRFI", fi.get("yrfi", 0.5)
        nrfi_edge = (nrfi_p - 0.524) * 100
        if nrfi_edge >= min_edge:
            picks.append(("nrfi", nrfi_pick, nrfi_p, nrfi_edge, -120))

        # Run Line
        rl_h = rl.get("home_minus_1_5", 0.5)
        rl_a = rl.get("away_plus_1_5", 0.5)
        if rl_h > 0.5:
            rl_pick, rl_prob = f"{h} -1.5", rl_h
        else:
            rl_pick, rl_prob = f"{a} +1.5", rl_a
        rl_edge = (rl_prob - 0.524) * 100
        if rl_edge >= min_edge:
            picks.append(("rl", rl_pick, rl_prob, rl_edge, -110))

        # Take only the highest-edge pick per game
        if picks:
            picks.sort(key=lambda p: p[3], reverse=True)  # Sort by edge
            bet_type, pick, prob, edge, odds = picks[0]
            conn.execute("""
                INSERT INTO picks (game_id, date, matchup, bet_type, pick, model_prob, edge, odds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (game_id, target_date, matchup, bet_type, pick, prob, edge, odds))
            recorded.append({
                "matchup": matchup, "type": bet_type,
                "pick": pick, "prob": round(prob, 3), "edge": round(edge, 1),
            })

    conn.commit()
    return recorded


def settle_picks() -> dict:
    """
    Settle all pending picks against final game results.
    Returns summary of settled picks.
    """
    conn = get_conn()

    pending = conn.execute(
        "SELECT * FROM picks WHERE result IS NULL"
    ).fetchall()

    if not pending:
        return {"settled": 0, "message": "No pending picks"}

    settled = 0
    wins = 0
    losses = 0

    for pick in pending:
        pick = dict(pick)
        game_id = pick["game_id"]

        game = conn.execute(
            "SELECT * FROM games WHERE mlb_game_id = ? AND status = 'final'",
            (game_id,)
        ).fetchone()

        if not game:
            continue  # Game not finished yet

        game = dict(game)
        hs = game.get("home_score", 0) or 0
        as_ = game.get("away_score", 0) or 0
        total_runs = hs + as_
        margin = hs - as_

        home_team = get_team_by_id(game.get("home_team_id"))
        away_team = get_team_by_id(game.get("away_team_id"))
        h = home_team["abbreviation"] if home_team else ""
        a = away_team["abbreviation"] if away_team else ""

        result = None
        profit = 0
        bt = pick["bet_type"]
        pk = pick["pick"]
        odds = pick["odds"] or -110

        if bt == "ml":
            home_won = hs > as_
            if pk == h:
                won = home_won
            else:
                won = not home_won
            result = "W" if won else "L"

        elif bt == "ou":
            if "Over" in pk:
                line = float(pk.split()[-1])
                if total_runs > line:
                    result = "W"
                elif total_runs < line:
                    result = "L"
                else:
                    result = "P"
            else:
                line = float(pk.split()[-1])
                if total_runs < line:
                    result = "W"
                elif total_runs > line:
                    result = "L"
                else:
                    result = "P"

        elif bt == "nrfi":
            # Rough approximation — proper tracking needs inning data
            scoreless_1st = total_runs <= 6
            if pk == "NRFI":
                result = "W" if scoreless_1st else "L"
            else:
                result = "W" if not scoreless_1st else "L"

        elif bt == "rl":
            if "-1.5" in pk:
                result = "W" if margin >= 2 else "L"
            else:
                result = "W" if margin <= 1 else "L"

        if result == "W":
            profit = (odds if odds > 0 else 100 / abs(odds) * 100)
            wins += 1
        elif result == "L":
            profit = -100
            losses += 1
        else:
            profit = 0

        conn.execute("""
            UPDATE picks SET result = ?, profit = ?, settled_at = datetime('now')
            WHERE id = ?
        """, (result, round(profit, 2), pick["id"]))
        settled += 1

    conn.commit()

    return {
        "settled": settled,
        "wins": wins,
        "losses": losses,
        "pending_remaining": conn.execute(
            "SELECT COUNT(*) as c FROM picks WHERE result IS NULL"
        ).fetchone()["c"],
    }


def get_pick_summary() -> dict:
    """Get running totals across all recorded picks."""
    conn = get_conn()

    summary = {}
    for bt in ["ml", "ou", "nrfi", "rl"]:
        row = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
                COALESCE(SUM(profit), 0) as profit
            FROM picks WHERE bet_type = ?
        """, (bt,)).fetchone()

        total = row["total"] or 0
        w = row["wins"] or 0
        l = row["losses"] or 0
        settled = w + l
        summary[bt] = {
            "total": total,
            "wins": w,
            "losses": l,
            "pushes": row["pushes"],
            "pending": row["pending"],
            "profit": round(row["profit"], 2),
            "win_pct": round(w / settled * 100, 1) if settled > 0 else 0,
            "roi": round(row["profit"] / settled, 1) if settled > 0 else 0,
        }

    # Recent picks
    recent = conn.execute("""
        SELECT * FROM picks ORDER BY created_at DESC LIMIT 20
    """).fetchall()

    # Overall
    totals = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
            COALESCE(SUM(profit), 0) as profit
        FROM picks
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
        print("Recording today's picks...", flush=True)
        picks = record_picks()
        print(f"Recorded {len(picks)} picks:")
        for p in picks:
            print(f"  {p['matchup']} | {p['type']:5s} | {p['pick']:15s} | {p['prob']:.1%} | edge: {p['edge']:+.1f}%")

    elif "--settle" in args:
        print("Settling completed picks...", flush=True)
        result = settle_picks()
        print(f"Settled: {result['settled']} ({result['wins']}W-{result['losses']}L)")
        print(f"Pending: {result['pending_remaining']}")

    elif "--summary" in args:
        summary = get_pick_summary()
        overall = summary["overall"]
        print(f"\n{'='*50}")
        print(f"  PICK TRACKER — Running Totals")
        print(f"{'='*50}")
        print(f"  Total picks: {overall['total']}")
        print(f"  Record: {overall['wins']}-{overall['losses']} ({overall['win_pct']}%)")
        print(f"  Profit: ${overall['profit']:+.2f}")
        print(f"  Pending: {overall['pending']}")
        print()
        for bt, label in [("ml", "ML"), ("ou", "O/U"), ("nrfi", "NRFI"), ("rl", "RL")]:
            s = summary["by_type"][bt]
            if s["total"] == 0:
                continue
            print(f"  {label}: {s['wins']}-{s['losses']} ({s['win_pct']}%) ${s['profit']:+.2f}")
        print(f"{'='*50}")

    else:
        print("Usage: python -m engine.tracker --record | --settle | --summary")
