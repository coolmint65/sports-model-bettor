"""
MLB Prediction Engine.

Generates matchup predictions using:
  1. Starting pitcher quality (ERA, FIP, xFIP, WHIP, K/9, recent form)
  2. Team offense strength (OPS, wRC+, ISO, K%, BB%)
  3. Bullpen strength & fatigue
  4. Batter-vs-pitcher H2H matchups
  5. Park factors (run environment)
  6. Home/away splits & recent form
  7. Platoon advantages (L/R matchups)

Output: expected runs, win probability, run line, O/U, F5, inning
breakdown, and edge analysis vs Vegas.
"""

import logging
import math
from datetime import datetime

from .db import (
    get_conn, get_team_by_id, get_team_record,
    get_pitcher_season, get_bullpen, get_park_factor,
    get_recent_games, get_team_h2h_vs_pitcher,
)

logger = logging.getLogger(__name__)

SEASON = datetime.now().year

# ── League-wide baselines ────────────────────────────────────

MLB_AVG_RPG = 4.5          # Average runs per game per team
MLB_AVG_ERA = 4.10
MLB_AVG_OPS = .720
MLB_AVG_FIP = 4.10
MLB_AVG_WHIP = 1.28
MLB_AVG_K9 = 8.5
MLB_AVG_BB9 = 3.2
MLB_AVG_WRC_PLUS = 100     # By definition
MLB_HOME_EDGE = 0.28       # ~0.28 runs home advantage


# ── Core prediction ──────────────────────────────────────────

def predict_matchup(home_team_id: int, away_team_id: int,
                    home_pitcher_id: int | None = None,
                    away_pitcher_id: int | None = None,
                    venue: str | None = None) -> dict:
    """
    Full MLB matchup prediction.

    Returns dict with expected_score, win_prob, spread, total,
    over_under, f5, inning breakdown, pitcher detail, and reasoning.
    """
    # ── Load data ──
    home_team = get_team_by_id(home_team_id)
    away_team = get_team_by_id(away_team_id)
    if not home_team or not away_team:
        return {"error": "Team not found"}

    home_stats = get_team_record(home_team_id, SEASON) or {}
    away_stats = get_team_record(away_team_id, SEASON) or {}

    home_sp = get_pitcher_season(home_pitcher_id, SEASON) if home_pitcher_id else None
    away_sp = get_pitcher_season(away_pitcher_id, SEASON) if away_pitcher_id else None

    home_bullpen = get_bullpen(home_team_id, SEASON) or {}
    away_bullpen = get_bullpen(away_team_id, SEASON) or {}

    park = get_park_factor(venue, SEASON) if venue else None

    # ── Step 1: Baseline expected runs ──
    home_off = _team_offense_rating(home_stats)
    away_off = _team_offense_rating(away_stats)

    # ── Step 2: Starting pitcher adjustment ──
    # Pitcher factor: <1 = good pitcher (suppresses runs), >1 = bad
    home_sp_factor = _pitcher_factor(home_sp)
    away_sp_factor = _pitcher_factor(away_sp)

    # Home offense scores against away SP, away offense against home SP
    home_xr = home_off * away_sp_factor
    away_xr = away_off * home_sp_factor

    # ── Step 3: Bullpen adjustment ──
    home_bp_factor = _bullpen_factor(home_bullpen)
    away_bp_factor = _bullpen_factor(away_bullpen)

    # Bullpen covers ~35% of the game (last 3-4 innings)
    home_xr *= (1 + 0.35 * (away_bp_factor - 1))
    away_xr *= (1 + 0.35 * (home_bp_factor - 1))

    # ── Step 4: Park factor ──
    park_run_factor = 1.0
    if park:
        park_run_factor = park.get("run_factor", 1.0) or 1.0
    home_xr *= park_run_factor
    away_xr *= park_run_factor

    # ── Step 5: Home advantage ──
    # Note: Umpire zone impact removed — MLB ABS (automated balls/strikes)
    # fully rolled out in 2026, eliminating umpire zone variance.
    home_xr += MLB_HOME_EDGE / 2
    away_xr -= MLB_HOME_EDGE / 2

    # ── Step 7: H2H adjustments ──
    h2h_adj_home, h2h_adj_away = 0.0, 0.0
    h2h_data = {}
    if away_pitcher_id:
        home_h2h = get_team_h2h_vs_pitcher(home_team_id, away_pitcher_id)
        h2h_adj_home = _h2h_adjustment(home_h2h)
        if home_h2h:
            h2h_data["home_vs_sp"] = _summarize_h2h(home_h2h)
    if home_pitcher_id:
        away_h2h = get_team_h2h_vs_pitcher(away_team_id, home_pitcher_id)
        h2h_adj_away = _h2h_adjustment(away_h2h)
        if away_h2h:
            h2h_data["away_vs_sp"] = _summarize_h2h(away_h2h)

    home_xr += h2h_adj_home
    away_xr += h2h_adj_away

    # ── Step 8: Recent form ──
    home_form = _form_adjustment(home_team_id)
    away_form = _form_adjustment(away_team_id)
    home_xr *= (1 + home_form)
    away_xr *= (1 + away_form)

    # ── Floor ──
    home_xr = max(home_xr, 1.5)
    away_xr = max(away_xr, 1.5)

    total = home_xr + away_xr
    spread = away_xr - home_xr  # Negative = home favored

    # ── Win probability (Poisson-based) ──
    matrix = _build_score_matrix(home_xr, away_xr, max_runs=15)
    p_home, p_away = _win_probs_from_matrix(matrix)

    # ── Over/Under lines ──
    ou_lines = _generate_ou_lines(total, matrix)

    # ── Run line probabilities ──
    run_line = _run_line_probs(matrix)

    # ── F5 (First 5 innings) ──
    f5 = _compute_f5(home_xr, away_xr, home_sp_factor, away_sp_factor)

    # ── Inning breakdown ──
    innings = _inning_breakdown(home_xr, away_xr)

    # ── NRFI / First Inning ──
    first_inning = _compute_first_inning(
        home_xr, away_xr, home_sp_factor, away_sp_factor,
        home_pitcher_id, away_pitcher_id, home_team_id, away_team_id)

    # ── Correct scores ──
    correct_scores = _top_correct_scores(matrix, n=8)

    # ── Build reasoning ──
    reasoning = _build_reasoning(
        home_team, away_team, home_stats, away_stats,
        home_sp, away_sp, home_xr, away_xr,
        park_run_factor, home_form, away_form, h2h_data,
    )

    # ── Pitcher detail for frontend ──
    home_pitcher_detail = _pitcher_detail(home_sp, home_pitcher_id)
    away_pitcher_detail = _pitcher_detail(away_sp, away_pitcher_id)

    # ── Assemble result ──
    home_record = f"{home_stats.get('wins', 0)}-{home_stats.get('losses', 0)}"
    away_record = f"{away_stats.get('wins', 0)}-{away_stats.get('losses', 0)}"

    return {
        "home": {
            "team_id": home_team_id,
            "name": home_team["name"],
            "abbreviation": home_team["abbreviation"],
            "record": home_record,
            "streak": home_stats.get("streak", ""),
            "pitcher": home_pitcher_detail,
        },
        "away": {
            "team_id": away_team_id,
            "name": away_team["name"],
            "abbreviation": away_team["abbreviation"],
            "record": away_record,
            "streak": away_stats.get("streak", ""),
            "pitcher": away_pitcher_detail,
        },
        "expected_score": {
            "home": round(home_xr, 1),
            "away": round(away_xr, 1),
        },
        "total": round(total, 1),
        "spread": round(spread, 1),
        "win_prob": {
            "home": round(p_home, 4),
            "away": round(p_away, 4),
        },
        "park_factor": round(park_run_factor, 3),
        "over_under": ou_lines,
        "run_line": run_line,
        "f5": f5,
        "first_inning": first_inning,
        "innings": innings,
        "correct_scores": correct_scores,
        "h2h": h2h_data,
        "reasoning": reasoning,
    }


# ── Rating components ────────────────────────────────────────

def _team_offense_rating(stats: dict) -> float:
    """
    Estimate runs/game from team stats.
    Weighted blend of actual R/G + advanced metrics.
    """
    runs_pg = stats.get("runs_pg")
    if runs_pg and runs_pg > 0:
        base = runs_pg
    else:
        base = MLB_AVG_RPG

    wrc_plus = stats.get("wrc_plus")
    ops = stats.get("ops")

    if wrc_plus and wrc_plus > 0:
        wrc_factor = wrc_plus / 100
        base = MLB_AVG_RPG * wrc_factor * 0.6 + base * 0.4
    elif ops and ops > 0:
        ops_factor = ops / MLB_AVG_OPS
        base = MLB_AVG_RPG * ops_factor * 0.5 + base * 0.5

    return base


def _pitcher_factor(sp: dict | None) -> float:
    """
    Starting pitcher quality as run multiplier.
    <1 = suppresses runs (ace), >1 = inflates runs (bad starter).
    Uses FIP > xFIP > ERA hierarchy for predictive accuracy.
    """
    if not sp:
        return 1.0

    innings = sp.get("innings") or 0
    if innings < 10:
        return 1.0

    # Primary: best available run prevention metric
    fip = sp.get("fip")
    x_fip = sp.get("x_fip")
    era = sp.get("era")
    whip = sp.get("whip")
    k_per_9 = sp.get("k_per_9")
    bb_per_9 = sp.get("bb_per_9")

    if fip is not None and fip > 0:
        primary = fip
        baseline = MLB_AVG_FIP
    elif era is not None and era > 0:
        primary = era
        baseline = MLB_AVG_ERA
    else:
        return 1.0

    run_factor = primary / baseline

    # Secondary adjustments
    adj = 0.0
    if whip and whip > 0:
        adj += ((whip - MLB_AVG_WHIP) / MLB_AVG_WHIP) * 0.10
    if k_per_9 and k_per_9 > 0:
        adj -= ((k_per_9 - MLB_AVG_K9) / MLB_AVG_K9) * 0.08
    if bb_per_9 and bb_per_9 > 0:
        adj += ((bb_per_9 - MLB_AVG_BB9) / MLB_AVG_BB9) * 0.06
    if x_fip and fip:
        adj += ((x_fip - fip) / baseline) * 0.05

    run_factor += adj
    return max(0.60, min(1.50, run_factor))


def _bullpen_factor(bp: dict) -> float:
    """
    Bullpen quality factor.
    <1 = good pen, >1 = bad pen. Includes fatigue.
    """
    if not bp:
        return 1.0

    era = bp.get("era")
    if not era or era <= 0:
        return 1.0

    factor = era / MLB_AVG_ERA

    innings_3d = bp.get("innings_last_3d", 0) or 0
    if innings_3d > 10:
        factor *= 1.08
    elif innings_3d > 7:
        factor *= 1.04

    return max(0.70, min(1.40, factor))


def _h2h_adjustment(h2h_matchups: list[dict]) -> float:
    """Runs adjustment from H2H batter-vs-pitcher history (-0.5 to +0.5)."""
    if not h2h_matchups:
        return 0.0

    total_ab = sum(m.get("at_bats", 0) or 0 for m in h2h_matchups)
    total_hits = sum(m.get("hits", 0) or 0 for m in h2h_matchups)
    total_hr = sum(m.get("home_runs", 0) or 0 for m in h2h_matchups)

    if total_ab < 20:
        return 0.0

    h2h_avg = total_hits / total_ab
    avg_diff = h2h_avg - 0.250
    confidence = min(1.0, total_ab / 100)
    hr_rate = total_hr / total_ab
    hr_bonus = (hr_rate - 0.03) * 5

    adjustment = (avg_diff * 3.0 + hr_bonus * 0.5) * confidence
    return max(-0.5, min(0.5, adjustment))


def _summarize_h2h(matchups: list[dict]) -> dict:
    """Summarize H2H data for display."""
    total_ab = sum(m.get("at_bats", 0) or 0 for m in matchups)
    total_hits = sum(m.get("hits", 0) or 0 for m in matchups)
    total_hr = sum(m.get("home_runs", 0) or 0 for m in matchups)
    total_k = sum(m.get("strikeouts", 0) or 0 for m in matchups)
    avg = round(total_hits / total_ab, 3) if total_ab > 0 else 0

    return {
        "at_bats": total_ab, "hits": total_hits,
        "home_runs": total_hr, "strikeouts": total_k,
        "avg": avg, "batters": len(matchups),
    }


def _form_adjustment(team_id: int) -> float:
    """Recent form factor from last 10 games. Returns -0.10 to +0.10."""
    recent = get_recent_games(team_id, 10)
    if len(recent) < 5:
        return 0.0

    wins = 0
    total_margin = 0
    for g in recent:
        is_home = g.get("home_team_id") == team_id
        ts = g.get("home_score", 0) if is_home else g.get("away_score", 0)
        os = g.get("away_score", 0) if is_home else g.get("home_score", 0)
        if ts is not None and os is not None:
            if ts > os:
                wins += 1
            total_margin += (ts - os)

    n = len(recent)
    win_adj = (wins / n - 0.5) * 0.15
    margin_adj = max(-0.05, min(0.05, (total_margin / n) * 0.005))
    return max(-0.10, min(0.10, win_adj + margin_adj))


# ── Poisson & probability ────────────────────────────────────

def _poisson_prob(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def _build_score_matrix(home_xr: float, away_xr: float,
                        max_runs: int = 15) -> list[list[float]]:
    matrix = []
    for h in range(max_runs + 1):
        row = []
        for a in range(max_runs + 1):
            row.append(_poisson_prob(home_xr, h) * _poisson_prob(away_xr, a))
        matrix.append(row)
    return matrix


def _win_probs_from_matrix(matrix: list[list[float]]) -> tuple[float, float]:
    p_home = p_away = p_tie = 0.0
    for h in range(len(matrix)):
        for a in range(len(matrix[0])):
            if h > a:
                p_home += matrix[h][a]
            elif a > h:
                p_away += matrix[h][a]
            else:
                p_tie += matrix[h][a]

    # Distribute ties proportionally (extra innings)
    if p_tie > 0:
        total = p_home + p_away
        if total > 0:
            p_home += p_tie * (p_home / total)
            p_away += p_tie * (p_away / total)
        else:
            p_home += p_tie / 2
            p_away += p_tie / 2

    return p_home, p_away


# ── Over/Under ───────────────────────────────────────────────

def _generate_ou_lines(total: float, matrix: list[list[float]]) -> dict:
    base = round(total * 2) / 2
    lines = [base - 2, base - 1, base - 0.5, base, base + 0.5, base + 1, base + 2]
    lines = [l for l in lines if 4.5 <= l <= 16.5]

    result = {}
    for line in lines:
        p_over = sum(matrix[h][a] for h in range(len(matrix))
                     for a in range(len(matrix[0])) if (h + a) > line)
        result[str(line)] = {
            "over": round(p_over, 4),
            "under": round(1 - p_over, 4),
        }
    return result


# ── Run Line ─────────────────────────────────────────────────

def _run_line_probs(matrix: list[list[float]]) -> dict:
    p_home_cover = 0.0  # Home -1.5 (wins by 2+)
    p_away_cover = 0.0  # Away +1.5 (loses by 1 or less, or wins)

    for h in range(len(matrix)):
        for a in range(len(matrix[0])):
            margin = h - a
            if margin >= 2:
                p_home_cover += matrix[h][a]
            if margin <= 1:
                p_away_cover += matrix[h][a]

    return {
        "home_minus_1_5": round(p_home_cover, 4),
        "away_plus_1_5": round(p_away_cover, 4),
    }


# ── First 5 Innings ──────────────────────────────────────────

def _compute_f5(home_xr: float, away_xr: float,
                home_sp_factor: float, away_sp_factor: float) -> dict:
    """F5 prediction. Starters account for ~58-62% of runs."""
    sp_depth_home = 0.62 if home_sp_factor < 0.90 else 0.58
    sp_depth_away = 0.62 if away_sp_factor < 0.90 else 0.58

    f5_home = round(home_xr * sp_depth_away, 1)
    f5_away = round(away_xr * sp_depth_home, 1)
    f5_total = round(f5_home + f5_away, 1)

    f5_matrix = _build_score_matrix(f5_home, f5_away, max_runs=10)
    f5_p_home, f5_p_away = _win_probs_from_matrix(f5_matrix)

    return {
        "home": f5_home, "away": f5_away, "total": f5_total,
        "win_prob": {"home": round(f5_p_home, 4), "away": round(f5_p_away, 4)},
    }


# ── Inning Breakdown ─────────────────────────────────────────

def _compute_first_inning(home_xr: float, away_xr: float,
                           home_sp_factor: float, away_sp_factor: float,
                           home_pitcher_id: int | None = None,
                           away_pitcher_id: int | None = None,
                           home_team_id: int | None = None,
                           away_team_id: int | None = None) -> dict:
    """
    First inning analysis for NRFI/YRFI betting.

    Uses pitcher-specific first-inning history when available,
    falls back to Poisson estimates from expected runs.
    """
    first_inning_weight = 0.105

    # Try to get pitcher first-inning scoreless rates from PIT data
    from .pit_stats import compute_pitcher_stats_at_date, compute_team_stats_at_date
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    season = datetime.now().year

    # P(away scores 0) — driven by home pitcher's first-inning dominance
    p_away_zero = None
    if home_pitcher_id:
        sp_pit = compute_pitcher_stats_at_date(home_pitcher_id, today, season)
        if sp_pit and sp_pit.get("first_inning_scoreless_pct") is not None and sp_pit.get("first_inning_starts", 0) >= 3:
            p_away_zero = sp_pit["first_inning_scoreless_pct"]

    # P(home scores 0) — driven by away pitcher's first-inning dominance
    p_home_zero = None
    if away_pitcher_id:
        sp_pit = compute_pitcher_stats_at_date(away_pitcher_id, today, season)
        if sp_pit and sp_pit.get("first_inning_scoreless_pct") is not None and sp_pit.get("first_inning_starts", 0) >= 3:
            p_home_zero = sp_pit["first_inning_scoreless_pct"]

    # Fallback to Poisson if no pitcher data
    if p_away_zero is None:
        away_1st_xr = away_xr * first_inning_weight * (0.85 + 0.15 * home_sp_factor)
        p_away_zero = _poisson_prob(away_1st_xr, 0)
    if p_home_zero is None:
        home_1st_xr = home_xr * first_inning_weight * (0.85 + 0.15 * away_sp_factor)
        p_home_zero = _poisson_prob(home_1st_xr, 0)

    # NRFI = both teams score 0 in the first
    nrfi = p_home_zero * p_away_zero
    yrfi = 1 - nrfi

    # P(exactly 1 run total in 1st)
    p_home_one = _poisson_prob(home_1st_xr, 1)
    p_away_one = _poisson_prob(away_1st_xr, 1)
    p_exactly_one = (p_home_one * p_away_zero) + (p_home_zero * p_away_one)

    # Away team bats first — P(away scores in top 1st)
    p_away_scores_1st = 1 - p_away_zero
    p_home_scores_1st = 1 - p_home_zero

    return {
        "nrfi": round(nrfi, 4),
        "yrfi": round(yrfi, 4),
        "home_scores_1st": round(p_home_scores_1st, 4),
        "away_scores_1st": round(p_away_scores_1st, 4),
        "home_xr_1st": round(home_1st_xr, 3),
        "away_xr_1st": round(away_1st_xr, 3),
        "exactly_one_run": round(p_exactly_one, 4),
    }


def _inning_breakdown(home_xr: float, away_xr: float) -> list[dict]:
    """Expected runs by inning with typical MLB scoring distribution."""
    weights = [0.105, 0.100, 0.105, 0.110, 0.100, 0.105, 0.115, 0.120, 0.140]
    return [{
        "inning": i + 1,
        "home": round(home_xr * w, 2),
        "away": round(away_xr * w, 2),
        "total": round((home_xr + away_xr) * w, 2),
    } for i, w in enumerate(weights)]


# ── Correct Scores ───────────────────────────────────────────

def _top_correct_scores(matrix: list[list[float]], n: int = 8) -> list[dict]:
    scores = []
    for h in range(min(len(matrix), 12)):
        for a in range(min(len(matrix[0]), 12)):
            if h == a:
                continue
            scores.append({"home": h, "away": a, "prob": round(matrix[h][a], 4)})
    scores.sort(key=lambda x: x["prob"], reverse=True)
    return scores[:n]


# ── Pitcher detail for frontend ──────────────────────────────

def _pitcher_detail(sp: dict | None, pitcher_id: int | None) -> dict | None:
    if not sp and not pitcher_id:
        return None

    conn = get_conn()
    if not sp:
        row = conn.execute("SELECT name, throws FROM players WHERE mlb_id = ?",
                          (pitcher_id,)).fetchone()
        if row:
            return {"name": row["name"], "throws": row["throws"], "id": pitcher_id}
        return {"name": "TBD", "id": pitcher_id}

    row = conn.execute("SELECT name, throws FROM players WHERE mlb_id = ?",
                      (sp["player_id"],)).fetchone()
    name = row["name"] if row else "Unknown"
    throws = row["throws"] if row else ""
    w = sp.get("wins", 0) or 0
    l = sp.get("losses", 0) or 0

    return {
        "id": sp["player_id"], "name": name, "throws": throws,
        "record": f"{w}-{l}",
        "era": sp.get("era"), "fip": sp.get("fip"), "x_fip": sp.get("x_fip"),
        "whip": sp.get("whip"), "k_per_9": sp.get("k_per_9"),
        "bb_per_9": sp.get("bb_per_9"), "innings": sp.get("innings"),
        "k_pct": sp.get("k_pct"), "babip": sp.get("babip"),
        "hr_per_9": sp.get("hr_per_9"), "avg_velocity": sp.get("avg_velocity"),
        "barrel_pct_against": sp.get("barrel_pct_against"),
    }


# ── Reasoning ────────────────────────────────────────────────

def _build_reasoning(home_team, away_team, home_stats, away_stats,
                     home_sp, away_sp, home_xr, away_xr,
                     park_factor, home_form, away_form, h2h_data) -> list[str]:
    hn = home_team.get("abbreviation", "HOME")
    an = away_team.get("abbreviation", "AWAY")
    reasons = []

    reasons.append(f"Model projects {hn} {round(home_xr)} - {round(away_xr)} {an} (expected runs: {home_xr:.1f} - {away_xr:.1f})")

    if home_sp:
        era = home_sp.get("era") or home_sp.get("fip")
        if era:
            reasons.append(f"{hn} SP: {era:.2f} ERA, "
                          f"{home_sp.get('k_per_9', 0):.1f} K/9, "
                          f"{home_sp.get('whip', 0):.2f} WHIP")
    if away_sp:
        era = away_sp.get("era") or away_sp.get("fip")
        if era:
            reasons.append(f"{an} SP: {era:.2f} ERA, "
                          f"{away_sp.get('k_per_9', 0):.1f} K/9, "
                          f"{away_sp.get('whip', 0):.2f} WHIP")

    h_ops = home_stats.get("ops")
    a_ops = away_stats.get("ops")
    if h_ops and a_ops:
        reasons.append(f"Team OPS: {hn} {h_ops:.3f} | {an} {a_ops:.3f}")

    h_wrc = home_stats.get("wrc_plus")
    a_wrc = away_stats.get("wrc_plus")
    if h_wrc and a_wrc:
        reasons.append(f"wRC+: {hn} {h_wrc:.0f} | {an} {a_wrc:.0f}")

    if park_factor and park_factor != 1.0:
        if park_factor > 1.03:
            reasons.append(f"Park factor {park_factor:.2f} — hitter-friendly venue")
        elif park_factor < 0.97:
            reasons.append(f"Park factor {park_factor:.2f} — pitcher-friendly venue")

    if abs(home_form) > 0.03:
        reasons.append(f"{hn} running {'hot' if home_form > 0 else 'cold'} (form {home_form:+.1%})")
    if abs(away_form) > 0.03:
        reasons.append(f"{an} running {'hot' if away_form > 0 else 'cold'} (form {away_form:+.1%})")

    if "home_vs_sp" in h2h_data:
        h2h = h2h_data["home_vs_sp"]
        if h2h["at_bats"] >= 20:
            reasons.append(
                f"{hn} batters vs {an} SP: {h2h['avg']:.3f} AVG "
                f"({h2h['hits']}/{h2h['at_bats']}, {h2h['home_runs']} HR)")

    hw = home_stats.get("wins", 0)
    hl = home_stats.get("losses", 0)
    aw = away_stats.get("wins", 0)
    al = away_stats.get("losses", 0)
    if hw + hl > 0 and aw + al > 0:
        reasons.append(f"Records: {hn} {hw}-{hl} | {an} {aw}-{al}")

    return reasons


# ── Utility: odds conversion ─────────────────────────────────

def ml_to_implied_prob(ml: int) -> float:
    """Convert American moneyline to implied probability."""
    if ml > 0:
        return 100 / (ml + 100)
    else:
        return abs(ml) / (abs(ml) + 100)


def find_edge(model_prob: float, ml: int) -> float:
    """Model prob minus implied prob. Positive = value bet."""
    return (model_prob - ml_to_implied_prob(ml)) * 100
