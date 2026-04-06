"""
Pick tracker — records model picks and settles them against results.

Call record_picks() before games start to log today's picks.
Call settle_picks() after games finish to mark W/L and calculate profit.

Usage:
    python -m engine.tracker --record     # Record today's picks
    python -m engine.tracker --settle     # Settle completed picks
    python -m engine.tracker --summary    # Print running totals
"""

import json
import logging
import urllib.request
from datetime import datetime

from .db import get_conn, get_team_by_id
from .mlb_predict import predict_matchup
from .bankroll import ml_to_implied_prob

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"


def _fetch_espn_scoreboard(date: str) -> list[dict]:
    """Fetch MLB scoreboard from ESPN for a given date."""
    espn_date = date.replace("-", "")
    url = f"{ESPN_BASE}/baseball/mlb/scoreboard?dates={espn_date}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        logger.warning("ESPN scoreboard fetch failed: %s", e)
        return []

    games = []
    for event in data.get("events", []):
        comps = event.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        home_team, away_team = None, None
        for c in competitors:
            team = c.get("team", {})
            entry = {
                "abbreviation": team.get("abbreviation", ""),
                "name": team.get("displayName", ""),
                "team_id": None,
            }
            # Try to resolve team_id from DB
            db_team = get_team_by_id(None)  # won't match
            from .db import get_conn as _gc
            row = _gc().execute(
                "SELECT mlb_id FROM teams WHERE abbreviation = ?",
                (entry["abbreviation"],)
            ).fetchone()
            if row:
                entry["team_id"] = row["mlb_id"]

            if c.get("homeAway") == "home":
                home_team = entry
            else:
                away_team = entry

        if not home_team or not away_team:
            continue

        status = comp.get("status", {}).get("type", {})

        # Probable pitchers
        home_pid, away_pid = None, None
        for c in competitors:
            pp = c.get("probables", [])
            if pp:
                pid = pp[0].get("athlete", {}).get("id")
                if c.get("homeAway") == "home":
                    home_pid = pid
                else:
                    away_pid = pid

        games.append({
            "id": event.get("id", ""),
            "home": home_team,
            "away": away_team,
            "home_pitcher": {"id": home_pid} if home_pid else {},
            "away_pitcher": {"id": away_pid} if away_pid else {},
            "venue": comp.get("venue", {}).get("fullName", ""),
            "status": {
                "state": status.get("state", "pre"),
                "completed": status.get("completed", False),
            },
        })

    return games

logger = logging.getLogger(__name__)

SEASON = datetime.now().year


def record_picks(date: str | None = None, min_edge: float = 1.5) -> list[dict]:
    """
    Run model on today's games and record the best pick per game.
    Uses the unified picks engine for consistent edge calculations.
    Falls back to ESPN scoreboard if games aren't in the DB.
    """
    conn = get_conn()
    target_date = date or datetime.now().strftime("%Y-%m-%d")

    games = conn.execute("""
        SELECT * FROM games WHERE date = ?
    """, (target_date,)).fetchall()

    # If no games in DB, try syncing today's schedule first
    if not games:
        logger.info("No games in DB for %s — fetching from MLB API", target_date)
        try:
            from scrapers.mlb_stats import fetch_schedule
            fetch_schedule(target_date, target_date)
            games = conn.execute("""
                SELECT * FROM games WHERE date = ?
            """, (target_date,)).fetchall()
        except Exception as e:
            logger.warning("Could not fetch today's schedule: %s", e)

    # Still no games? Try ESPN scoreboard as last resort
    if not games:
        try:
            scoreboard = _fetch_espn_scoreboard(target_date)
            if scoreboard:
                logger.info("Using ESPN scoreboard (%d games)", len(scoreboard))
                return _record_from_scoreboard(conn, scoreboard, target_date, min_edge)
        except Exception as e:
            logger.warning("Scoreboard fallback failed: %s", e)
        return []

    # Fetch real odds once for all games
    from .picks import generate_picks, get_best_pick, fetch_real_odds_for_games, match_odds

    all_odds = fetch_real_odds_for_games()

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

        home_team = get_team_by_id(home_id)
        away_team = get_team_by_id(away_id)
        h = home_team["abbreviation"] if home_team else "?"
        a = away_team["abbreviation"] if away_team else "?"
        matchup = f"{a} @ {h}"

        # Get real odds for this game
        game_odds = match_odds(h, a, all_odds)

        # Generate picks using unified engine
        picks = generate_picks(
            home_team_id=home_id,
            away_team_id=away_id,
            home_pitcher_id=game.get("home_pitcher_id"),
            away_pitcher_id=game.get("away_pitcher_id"),
            venue=game.get("venue"),
            odds=game_odds,
        )

        # Take the best pick
        best = get_best_pick(picks)
        if not best or best["edge"] < min_edge:
            continue

        conn.execute("""
            INSERT INTO picks (game_id, date, matchup, bet_type, pick,
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


def _record_from_scoreboard(conn, scoreboard: list, target_date: str,
                            min_edge: float) -> list[dict]:
    """Record picks using live scoreboard data when DB has no games."""
    from .picks import generate_picks, get_best_pick, match_odds, fetch_real_odds_for_games

    all_odds = fetch_real_odds_for_games()
    recorded = []

    for game in scoreboard:
        # Skip completed games
        if game.get("status", {}).get("completed") or game.get("status", {}).get("state") == "post":
            continue

        game_id = game.get("id") or game.get("game_pk")
        if not game_id:
            continue

        # Skip if already recorded
        existing = conn.execute(
            "SELECT COUNT(*) as c FROM picks WHERE game_id = ?", (game_id,)
        ).fetchone()["c"]
        if existing > 0:
            continue

        home_id = game.get("home", {}).get("team_id")
        away_id = game.get("away", {}).get("team_id")
        if not home_id or not away_id:
            continue

        h = game.get("home", {}).get("abbreviation", "?")
        a = game.get("away", {}).get("abbreviation", "?")
        matchup = f"{a} @ {h}"

        # Get odds
        game_odds = game.get("odds") or match_odds(h, a, all_odds)

        # Get pitcher IDs
        hp = game.get("home_pitcher") or {}
        ap = game.get("away_pitcher") or {}
        try:
            h_pid = int(hp["id"]) if hp.get("id") else None
            a_pid = int(ap["id"]) if ap.get("id") else None
        except (ValueError, TypeError):
            h_pid, a_pid = None, None

        try:
            picks = generate_picks(
                home_team_id=home_id, away_team_id=away_id,
                home_pitcher_id=h_pid, away_pitcher_id=a_pid,
                venue=game.get("venue"), odds=game_odds,
            )
        except Exception as e:
            logger.warning("Prediction failed for %s: %s", matchup, e)
            continue

        best = get_best_pick(picks)
        if not best or best["edge"] < min_edge:
            continue

        conn.execute("""
            INSERT INTO picks (game_id, date, matchup, bet_type, pick,
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
    logger.info("Recorded %d picks from scoreboard", len(recorded))
    return recorded


def settle_picks() -> dict:
    """
    Settle all pending picks against final game results.
    First refreshes recent game scores from MLB API, then settles.
    """
    conn = get_conn()

    # Re-fetch recent game results so completed games are marked 'final'
    try:
        from scrapers.mlb_stats import fetch_schedule
        from datetime import timedelta
        today = datetime.now().strftime("%Y-%m-%d")
        three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        fetch_schedule(three_days_ago, today)
    except Exception as e:
        logger.warning("Could not refresh recent games: %s", e)

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

        if bt in ("ml", "ML"):
            home_won = hs > as_
            if pk == h:
                won = home_won
            else:
                won = not home_won
            result = "W" if won else "L"

        elif bt in ("ou", "O/U"):
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

        elif bt in ("nrfi", "1st INN"):
            # Use real linescore data when available
            import json as _json
            home_ls = game.get("home_linescore")
            away_ls = game.get("away_linescore")
            if home_ls and away_ls:
                try:
                    h_inn = _json.loads(home_ls)
                    a_inn = _json.loads(away_ls)
                    if len(h_inn) > 0 and len(a_inn) > 0:
                        scoreless_1st = (h_inn[0] == 0 and a_inn[0] == 0)
                    else:
                        scoreless_1st = total_runs <= 6
                except Exception:
                    scoreless_1st = total_runs <= 6
            else:
                scoreless_1st = total_runs <= 6

            if pk == "NRFI":
                result = "W" if scoreless_1st else "L"
            else:
                result = "W" if not scoreless_1st else "L"

        elif bt in ("rl", "RL"):
            # Extract team and spread from pick (e.g. "DET -1.5", "COL +1.5")
            parts = pk.split()
            pick_team = parts[0] if parts else ""
            spread = float(parts[1]) if len(parts) > 1 else 1.5

            # Calculate margin from the picked team's perspective
            if pick_team == h:
                team_margin = hs - as_
            else:
                team_margin = as_ - hs

            # Team covers if their margin + spread > 0
            if team_margin + spread > 0:
                result = "W"
            elif team_margin + spread == 0:
                result = "P"
            else:
                result = "L"

        if result is None:
            continue  # Could not determine result — skip

        if result == "W":
            profit = (odds if odds > 0 else 100 / abs(odds) * 100)
            wins += 1
        elif result == "L":
            profit = -100
            losses += 1
        else:
            profit = 0  # Push

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
    # Map canonical keys to all possible bet_type values (old lowercase + new uppercase)
    bt_aliases = {
        "ML": ("ML", "ml"),
        "O/U": ("O/U", "ou"),
        "1st INN": ("1st INN", "nrfi"),
        "RL": ("RL", "rl"),
    }
    for bt, aliases in bt_aliases.items():
        placeholders = ",".join("?" for _ in aliases)
        row = conn.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
                COALESCE(SUM(profit), 0) as profit
            FROM picks WHERE bet_type IN ({placeholders})
        """, aliases).fetchone()

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
        for bt, label in [("ML", "Moneyline"), ("O/U", "Over/Under"), ("1st INN", "1st Inning"), ("RL", "Run Line")]:
            s = summary["by_type"][bt]
            if s["total"] == 0:
                continue
            print(f"  {label}: {s['wins']}-{s['losses']} ({s['win_pct']}%) ${s['profit']:+.2f}")
        print(f"{'='*50}")

    else:
        print("Usage: python -m engine.tracker --record | --settle | --summary")
