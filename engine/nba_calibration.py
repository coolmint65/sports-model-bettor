"""
NBA Q1 Model Calibration -- learns from prediction errors to improve accuracy.

Analyzes completed games with Q1 scores, compares model predictions against
actual outcomes, and computes calibration adjustments for:
    - Home court Q1 boost (is 1.5 pts accurate?)
    - Q1 total bias (systematic over/under-prediction?)
    - Pace factor accuracy
    - Per-team Q1 bias (some teams consistently outperform in Q1)

Usage:
    python -m engine.nba_calibration                 # Full season
    python -m engine.nba_calibration --days 30       # Last 30 days
    python -m engine.nba_calibration --team LAL      # Specific team
"""

import logging
import math
from datetime import datetime, timedelta

from .nba_db import get_conn

logger = logging.getLogger(__name__)


def calibrate(days: int = 0) -> dict:
    """Analyze NBA Q1 prediction errors and compute calibration adjustments.

    Args:
        days: Number of days to look back. 0 = full season (all data).

    Returns:
        Calibration results dict with bias measurements and recommendations.
    """
    conn = get_conn()

    if days > 0:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        games = conn.execute("""
            SELECT g.*,
                   ht.abbreviation as home_abbr, at.abbreviation as away_abbr
            FROM nba_games g
            LEFT JOIN nba_teams ht ON g.home_team_id = ht.id
            LEFT JOIN nba_teams at ON g.away_team_id = at.id
            WHERE g.status = 'final'
              AND g.home_q1 IS NOT NULL AND g.away_q1 IS NOT NULL
              AND g.date >= ?
            ORDER BY g.date
        """, (cutoff,)).fetchall()
    else:
        games = conn.execute("""
            SELECT g.*,
                   ht.abbreviation as home_abbr, at.abbreviation as away_abbr
            FROM nba_games g
            LEFT JOIN nba_teams ht ON g.home_team_id = ht.id
            LEFT JOIN nba_teams at ON g.away_team_id = at.id
            WHERE g.status = 'final'
              AND g.home_q1 IS NOT NULL AND g.away_q1 IS NOT NULL
            ORDER BY g.date
        """).fetchall()

    if len(games) < 20:
        return {
            "message": f"Not enough games with Q1 data ({len(games)}) for calibration",
            "games": len(games),
        }

    # ── Accumulators ──
    game_count = 0
    home_q1_margins = []
    q1_totals = []
    home_q1_scored = []
    away_q1_scored = []
    home_wins_q1 = 0

    for game in games:
        game = dict(game)
        h_q1 = game.get("home_q1")
        a_q1 = game.get("away_q1")

        if h_q1 is None or a_q1 is None:
            continue

        game_count += 1
        margin = h_q1 - a_q1
        total = h_q1 + a_q1

        home_q1_margins.append(margin)
        q1_totals.append(total)
        home_q1_scored.append(h_q1)
        away_q1_scored.append(a_q1)

        if margin > 0:
            home_wins_q1 += 1

    if game_count == 0:
        return {"message": "No completed games with Q1 data found", "games": 0}

    # ── Compute calibration metrics ──

    # Reference values come from the live model constants so the report
    # always compares actual outcomes to what the model currently assumes.
    from .nba_q1_predict import HOME_Q1_BOOST, LEAGUE_AVG_Q1_TOTAL, Q1_STD_DEV

    # 1. Home court Q1 advantage
    avg_home_q1_margin = sum(home_q1_margins) / game_count
    home_q1_win_rate = home_wins_q1 / game_count
    home_boost_error = avg_home_q1_margin - HOME_Q1_BOOST

    # 2. Q1 total bias
    avg_q1_total = sum(q1_totals) / game_count
    avg_home_q1 = sum(home_q1_scored) / game_count
    avg_away_q1 = sum(away_q1_scored) / game_count
    total_bias = avg_q1_total - LEAGUE_AVG_Q1_TOTAL

    # 3. Q1 margin standard deviation (for spread probability calibration)
    margin_variance = sum((m - avg_home_q1_margin) ** 2 for m in home_q1_margins) / game_count
    actual_std_dev = math.sqrt(margin_variance)
    std_dev_error = actual_std_dev - Q1_STD_DEV

    # 4. Q1 total standard deviation
    total_variance = sum((t - avg_q1_total) ** 2 for t in q1_totals) / game_count
    total_std_dev = math.sqrt(total_variance)

    # ── Save calibration values to DB ──
    _save_config("q1_home_boost", avg_home_q1_margin, conn)
    _save_config("q1_avg_total", avg_q1_total, conn)
    _save_config("q1_margin_std_dev", actual_std_dev, conn)
    _save_config("q1_total_std_dev", total_std_dev, conn)
    _save_config("q1_avg_home_ppg", avg_home_q1, conn)
    _save_config("q1_avg_away_ppg", avg_away_q1, conn)
    _save_config("q1_home_win_rate", home_q1_win_rate, conn)
    _save_config("calibration_games", float(game_count), conn)

    # ── Build recommendations ──
    recommendations = []

    if abs(home_boost_error) > 0.3:
        direction = "increase" if home_boost_error > 0 else "decrease"
        recommendations.append(
            f"Home court Q1 boost should {direction}: "
            f"actual avg margin is {avg_home_q1_margin:+.2f} "
            f"(model assumes {HOME_Q1_BOOST:+.2f}, off by {home_boost_error:+.2f})"
        )

    if abs(total_bias) > 1.0:
        direction = "higher" if total_bias > 0 else "lower"
        recommendations.append(
            f"Q1 totals trending {direction}: avg {avg_q1_total:.1f} "
            f"(model baseline {LEAGUE_AVG_Q1_TOTAL:.1f}, off by {total_bias:+.1f})"
        )

    if abs(std_dev_error) > 0.3:
        recommendations.append(
            f"Q1 margin std dev is {actual_std_dev:.2f} "
            f"(model assumes {Q1_STD_DEV:.2f}, off by {std_dev_error:+.2f}). "
            f"Update Q1_STD_DEV for better spread probabilities."
        )

    if not recommendations:
        recommendations.append("Model calibration looks good -- no significant biases detected.")

    return {
        "games": game_count,
        "days": days,
        "home_q1_advantage": {
            "actual_avg_margin": round(avg_home_q1_margin, 2),
            "model_assumes": HOME_Q1_BOOST,
            "error": round(home_boost_error, 2),
            "home_q1_win_rate": round(home_q1_win_rate, 3),
        },
        "q1_totals": {
            "actual_avg_total": round(avg_q1_total, 1),
            "model_baseline": LEAGUE_AVG_Q1_TOTAL,
            "bias": round(total_bias, 1),
            "avg_home_q1": round(avg_home_q1, 1),
            "avg_away_q1": round(avg_away_q1, 1),
        },
        "q1_variance": {
            "actual_margin_std_dev": round(actual_std_dev, 2),
            "model_assumes": Q1_STD_DEV,
            "error": round(std_dev_error, 2),
            "total_std_dev": round(total_std_dev, 2),
        },
        "recommendations": recommendations,
    }


def calibrate_team(team_abbr: str, days: int = 0) -> dict:
    """Analyze Q1 prediction accuracy for a specific team.

    Returns per-team Q1 bias data: how much a team over/under-performs
    in Q1 relative to expectations.
    """
    from .nba_db import get_nba_team_by_abbr

    team = get_nba_team_by_abbr(team_abbr)
    if not team:
        return {"error": f"Team not found: {team_abbr}"}

    team_id = team["id"]
    conn = get_conn()

    where_clause = """
        WHERE (g.home_team_id = ? OR g.away_team_id = ?)
          AND g.status = 'final'
          AND g.home_q1 IS NOT NULL AND g.away_q1 IS NOT NULL
    """
    params = [team_id, team_id]

    if days > 0:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        where_clause += " AND g.date >= ?"
        params.append(cutoff)

    games = conn.execute(f"""
        SELECT g.*,
               ht.abbreviation as home_abbr, at.abbreviation as away_abbr
        FROM nba_games g
        LEFT JOIN nba_teams ht ON g.home_team_id = ht.id
        LEFT JOIN nba_teams at ON g.away_team_id = at.id
        {where_clause}
        ORDER BY g.date
    """, params).fetchall()

    if len(games) < 5:
        return {
            "team": team_abbr,
            "message": f"Not enough games ({len(games)}) for team calibration",
        }

    # Accumulate team-specific Q1 data
    q1_scored = []
    q1_allowed = []
    q1_home_scored = []
    q1_home_allowed = []
    q1_away_scored = []
    q1_away_allowed = []
    wins_q1 = 0

    for game in games:
        game = dict(game)
        is_home = game["home_team_id"] == team_id

        if is_home:
            scored = game["home_q1"]
            allowed = game["away_q1"]
            q1_home_scored.append(scored)
            q1_home_allowed.append(allowed)
        else:
            scored = game["away_q1"]
            allowed = game["home_q1"]
            q1_away_scored.append(scored)
            q1_away_allowed.append(allowed)

        q1_scored.append(scored)
        q1_allowed.append(allowed)

        if scored > allowed:
            wins_q1 += 1

    total = len(q1_scored)
    avg = lambda vals: round(sum(vals) / len(vals), 2) if vals else None

    # Streaks: look at last 10 games for trend
    recent_margins = [s - a for s, a in zip(q1_scored[-10:], q1_allowed[-10:])]
    season_margins = [s - a for s, a in zip(q1_scored, q1_allowed)]

    return {
        "team": team_abbr,
        "team_name": team["name"],
        "games": total,
        "q1_ppg": avg(q1_scored),
        "q1_opp_ppg": avg(q1_allowed),
        "q1_avg_margin": avg(season_margins),
        "q1_win_rate": round(wins_q1 / total, 3) if total > 0 else None,
        "home_q1_ppg": avg(q1_home_scored),
        "home_q1_opp_ppg": avg(q1_home_allowed),
        "away_q1_ppg": avg(q1_away_scored),
        "away_q1_opp_ppg": avg(q1_away_allowed),
        "recent_10_avg_margin": avg(recent_margins) if recent_margins else None,
        "home_games": len(q1_home_scored),
        "away_games": len(q1_away_scored),
    }


def get_calibrated_home_boost() -> float | None:
    """Get calibrated Q1 home court boost from model config.

    Returns the calibrated value or None if not yet computed.
    """
    conn = get_conn()
    row = conn.execute(
        "SELECT value FROM nba_model_config WHERE key = 'q1_home_boost'"
    ).fetchone()
    return row["value"] if row else None


def get_calibrated_std_dev() -> float | None:
    """Get calibrated Q1 margin standard deviation from model config."""
    conn = get_conn()
    row = conn.execute(
        "SELECT value FROM nba_model_config WHERE key = 'q1_margin_std_dev'"
    ).fetchone()
    return row["value"] if row else None


def get_total_adjustment() -> float | None:
    """Get Q1 total bias adjustment from calibration.

    Returns the bias (positive = actual totals are higher than 55.0 baseline).
    """
    conn = get_conn()
    row = conn.execute(
        "SELECT value FROM nba_model_config WHERE key = 'q1_avg_total'"
    ).fetchone()
    if row:
        return round(row["value"] - 55.0, 1)
    return None


def _save_config(key: str, value: float, conn=None) -> None:
    """Save a calibration value to nba_model_config."""
    if conn is None:
        conn = get_conn()
    conn.execute("""
        INSERT INTO nba_model_config (key, value, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
    """, (key, value))
    conn.commit()


def full_calibration_report(days: int = 0) -> str:
    """Generate a human-readable calibration report."""
    result = calibrate(days)

    lines = []
    lines.append(f"{'='*60}")
    lines.append(f"  NBA Q1 Calibration Report")
    lines.append(f"  Games analyzed: {result.get('games', 0)}")

    # Early return if not enough data
    if "message" in result:
        lines.append(f"  {result['message']}")
        lines.append(f"{'='*60}")
        return "\n".join(lines)
    if days > 0:
        lines.append(f"  Period: last {days} days")
    lines.append(f"{'='*60}")

    ha = result.get("home_q1_advantage", {})
    lines.append(f"\n  Home Court Q1 Advantage:")
    margin_val = ha.get("actual_avg_margin")
    lines.append(f"    Actual avg margin: {margin_val:+.2f}" if margin_val is not None else "    Actual avg margin: N/A")
    lines.append(f"    Model assumes: {ha.get('model_assumes', 0):+.2f}")
    err_val = ha.get("error")
    lines.append(f"    Error: {err_val:+.2f}" if err_val is not None else "    Error: N/A")
    wr_val = ha.get("home_q1_win_rate", 0)
    lines.append(f"    Home Q1 win rate: {wr_val:.1%}" if wr_val is not None else "    Home Q1 win rate: N/A")

    qt = result.get("q1_totals", {})
    lines.append(f"\n  Q1 Totals:")
    lines.append(f"    Actual avg total: {qt.get('actual_avg_total', 'N/A')}")
    lines.append(f"    Model baseline: {qt.get('model_baseline', 'N/A')}")
    bias_val = qt.get("bias")
    lines.append(f"    Bias: {bias_val:+.1f}" if bias_val is not None else "    Bias: N/A")
    lines.append(f"    Avg home Q1: {qt.get('avg_home_q1', 'N/A')}")
    lines.append(f"    Avg away Q1: {qt.get('avg_away_q1', 'N/A')}")

    qv = result.get("q1_variance", {})
    lines.append(f"\n  Q1 Variance:")
    lines.append(f"    Actual margin std dev: {qv.get('actual_margin_std_dev', 'N/A')}")
    lines.append(f"    Model assumes: {qv.get('model_assumes', 'N/A')}")
    qv_err = qv.get("error")
    lines.append(f"    Error: {qv_err:+.2f}" if qv_err is not None else "    Error: N/A")
    lines.append(f"    Total std dev: {qv.get('total_std_dev', 'N/A')}")

    lines.append(f"\n  Recommendations:")
    for rec in result.get("recommendations", []):
        lines.append(f"    - {rec}")

    lines.append(f"{'='*60}")

    return "\n".join(lines)


# ── CLI entry point ───────��────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = sys.argv[1:]

    # Parse --days N
    days_val = 0
    for i, a in enumerate(args):
        if a == "--days" and i + 1 < len(args):
            days_val = int(args[i + 1])

    # Parse --team ABBR
    team_abbr = None
    for i, a in enumerate(args):
        if a == "--team" and i + 1 < len(args):
            team_abbr = args[i + 1].upper()

    if team_abbr:
        result = calibrate_team(team_abbr, days=days_val)
        if "error" in result:
            print(f"Error: {result['error']}")
            sys.exit(1)
        print(f"\n{'='*50}")
        print(f"  {result.get('team_name', team_abbr)} Q1 Profile")
        print(f"{'='*50}")
        print(f"  Games: {result.get('games', 0)}")
        print(f"  Q1 PPG: {result.get('q1_ppg', 'N/A')}")
        print(f"  Q1 Opp PPG: {result.get('q1_opp_ppg', 'N/A')}")
        print(f"  Q1 Avg Margin: {result.get('q1_avg_margin', 'N/A')}")
        print(f"  Q1 Win Rate: {result.get('q1_win_rate', 'N/A')}")
        print(f"  Home Q1: {result.get('home_q1_ppg', 'N/A')} / {result.get('home_q1_opp_ppg', 'N/A')}")
        print(f"  Away Q1: {result.get('away_q1_ppg', 'N/A')} / {result.get('away_q1_opp_ppg', 'N/A')}")
        print(f"  Recent L10 Margin: {result.get('recent_10_avg_margin', 'N/A')}")
        print(f"{'='*50}")
    else:
        report = full_calibration_report(days=days_val)
        print(report)
