"""
Per-team model calibration.

Instead of one global model, this learns team-specific tendencies:
- How much does each team over/under-perform their stats?
- How do they perform at home vs away?
- How do they perform vs lefties vs righties?
- First-inning scoring tendencies per team
- Bullpen reliability per team

Updated daily from game results. Stored in DB per team per season.
"""

import json
import logging
import math
from datetime import datetime, timedelta

from .db import get_conn
from .pit_stats import compute_team_stats_at_date

logger = logging.getLogger(__name__)


def calibrate_teams(season: int | None = None, days: int = 60) -> dict:
    """
    Compute per-team adjustment factors from historical performance.

    For each team, compares their actual results to what the base model
    predicted, and generates a correction factor.
    """
    conn = get_conn()
    yr = season or datetime.now().year
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # Ensure table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS team_adjustments (
            team_id     INTEGER NOT NULL,
            season      INTEGER NOT NULL,
            -- Performance vs expectations
            offense_factor  REAL DEFAULT 1.0,   -- >1 = scores more than expected
            defense_factor  REAL DEFAULT 1.0,   -- >1 = allows more than expected
            home_factor     REAL DEFAULT 1.0,   -- >1 = better at home than avg
            away_factor     REAL DEFAULT 1.0,   -- >1 = better on road than avg
            first_inn_factor REAL DEFAULT 1.0,  -- >1 = scores more in 1st than avg
            bullpen_factor  REAL DEFAULT 1.0,   -- >1 = bullpen allows more than avg
            -- Confidence
            games_analyzed  INTEGER DEFAULT 0,
            updated_at      TEXT DEFAULT (datetime('now')),
            UNIQUE(team_id, season)
        )
    """)
    conn.commit()

    teams = conn.execute("SELECT mlb_id, abbreviation FROM teams").fetchall()

    results = {}

    for team in teams:
        team_id = team["mlb_id"]
        abbr = team["abbreviation"]

        games = conn.execute("""
            SELECT * FROM games
            WHERE (home_team_id = ? OR away_team_id = ?)
              AND date >= ? AND season = ? AND status = 'final'
              AND home_score IS NOT NULL AND away_score IS NOT NULL
        """, (team_id, team_id, start_date, yr)).fetchall()

        if len(games) < 3:
            continue

        # Track actual vs expected performance
        total_runs_scored = 0
        total_runs_allowed = 0
        expected_runs_scored = 0
        expected_runs_allowed = 0
        home_runs_scored = 0
        home_expected = 0
        home_games = 0
        away_runs_scored = 0
        away_expected = 0
        away_games = 0
        first_inn_runs = 0
        first_inn_games = 0
        late_runs_allowed = 0  # Innings 7-9
        late_games = 0

        for g in games:
            g = dict(g)
            is_home = g["home_team_id"] == team_id
            team_score = g["home_score"] if is_home else g["away_score"]
            opp_score = g["away_score"] if is_home else g["home_score"]

            total_runs_scored += team_score
            total_runs_allowed += opp_score

            # Expected based on league average
            expected_runs_scored += 4.5
            expected_runs_allowed += 4.5

            if is_home:
                home_runs_scored += team_score
                home_expected += 4.5
                home_games += 1
            else:
                away_runs_scored += team_score
                away_expected += 4.5
                away_games += 1

            # First inning from linescore
            ls_key = "home_linescore" if is_home else "away_linescore"
            ls = g.get(ls_key)
            if ls:
                try:
                    innings = json.loads(ls)
                    if len(innings) > 0:
                        first_inn_runs += innings[0]
                        first_inn_games += 1
                    # Late innings (7-9) for bullpen
                    if len(innings) >= 9:
                        opp_ls_key = "away_linescore" if is_home else "home_linescore"
                        opp_ls = g.get(opp_ls_key)
                        if opp_ls:
                            opp_innings = json.loads(opp_ls)
                            if len(opp_innings) >= 9:
                                late_runs_allowed += sum(opp_innings[6:9])
                                late_games += 1
                except (json.JSONDecodeError, IndexError):
                    pass

        n = len(games)

        # Offense factor: actual R/G vs expected
        offense_factor = (total_runs_scored / n) / 4.5 if n > 0 else 1.0

        # Defense factor: actual RA/G vs expected (>1 = worse defense)
        defense_factor = (total_runs_allowed / n) / 4.5 if n > 0 else 1.0

        # Home factor
        home_factor = 1.0
        if home_games >= 2:
            home_rpg = home_runs_scored / home_games
            overall_rpg = total_runs_scored / n
            if overall_rpg > 0:
                home_factor = home_rpg / overall_rpg

        # Away factor
        away_factor = 1.0
        if away_games >= 2:
            away_rpg = away_runs_scored / away_games
            overall_rpg = total_runs_scored / n
            if overall_rpg > 0:
                away_factor = away_rpg / overall_rpg

        # First inning factor
        first_inn_factor = 1.0
        if first_inn_games >= 3:
            # Compare team's 1st inning R/G to league avg (~0.5)
            fi_rpg = first_inn_runs / first_inn_games
            first_inn_factor = fi_rpg / 0.5 if fi_rpg > 0 else 0.5

        # Bullpen factor
        bullpen_factor = 1.0
        if late_games >= 3:
            late_rpg = late_runs_allowed / late_games
            # League avg late innings ~1.5 runs (innings 7-9)
            bullpen_factor = late_rpg / 1.5 if late_rpg > 0 else 0.5

        # Clamp factors to reasonable range
        offense_factor = max(0.70, min(1.40, offense_factor))
        defense_factor = max(0.70, min(1.40, defense_factor))
        home_factor = max(0.80, min(1.25, home_factor))
        away_factor = max(0.80, min(1.25, away_factor))
        first_inn_factor = max(0.30, min(2.50, first_inn_factor))
        bullpen_factor = max(0.50, min(2.00, bullpen_factor))

        # Save to DB
        conn.execute("""
            INSERT INTO team_adjustments (team_id, season,
                offense_factor, defense_factor, home_factor, away_factor,
                first_inn_factor, bullpen_factor, games_analyzed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_id, season) DO UPDATE SET
                offense_factor=excluded.offense_factor,
                defense_factor=excluded.defense_factor,
                home_factor=excluded.home_factor,
                away_factor=excluded.away_factor,
                first_inn_factor=excluded.first_inn_factor,
                bullpen_factor=excluded.bullpen_factor,
                games_analyzed=excluded.games_analyzed,
                updated_at=datetime('now')
        """, (team_id, yr,
              round(offense_factor, 4), round(defense_factor, 4),
              round(home_factor, 4), round(away_factor, 4),
              round(first_inn_factor, 4), round(bullpen_factor, 4),
              n))

        results[abbr] = {
            "games": n,
            "offense": round(offense_factor, 3),
            "defense": round(defense_factor, 3),
            "home": round(home_factor, 3),
            "away": round(away_factor, 3),
            "first_inn": round(first_inn_factor, 3),
            "bullpen": round(bullpen_factor, 3),
        }

    conn.commit()
    logger.info("Calibrated %d teams", len(results))
    return results


def get_team_adjustment(team_id: int, season: int | None = None) -> dict:
    """Get per-team adjustment factors. Returns defaults if not calibrated."""
    conn = get_conn()
    yr = season or datetime.now().year

    defaults = {
        "offense_factor": 1.0, "defense_factor": 1.0,
        "home_factor": 1.0, "away_factor": 1.0,
        "first_inn_factor": 1.0, "bullpen_factor": 1.0,
        "games_analyzed": 0,
    }

    try:
        row = conn.execute("""
            SELECT * FROM team_adjustments WHERE team_id = ? AND season = ?
        """, (team_id, yr)).fetchone()
    except Exception:
        # Table doesn't exist yet — return defaults
        return defaults

    if row:
        return {
            "offense_factor": row["offense_factor"],
            "defense_factor": row["defense_factor"],
            "home_factor": row["home_factor"],
            "away_factor": row["away_factor"],
            "first_inn_factor": row["first_inn_factor"],
            "bullpen_factor": row["bullpen_factor"],
            "games_analyzed": row["games_analyzed"],
        }

    return defaults


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    print("Calibrating per-team factors...", flush=True)
    results = calibrate_teams()

    if results:
        print(f"\n{'Team':5s} {'G':>3s} {'Off':>6s} {'Def':>6s} {'Home':>6s} {'Away':>6s} {'1st':>6s} {'BP':>6s}")
        print("-" * 48)
        for abbr, data in sorted(results.items()):
            print(f"{abbr:5s} {data['games']:3d} {data['offense']:6.3f} {data['defense']:6.3f} "
                  f"{data['home']:6.3f} {data['away']:6.3f} {data['first_inn']:6.3f} {data['bullpen']:6.3f}")
    else:
        print("No teams calibrated (not enough data)")
