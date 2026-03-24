"""
Prediction engine.

Takes two teams + league config, outputs a full matchup prediction:
- Expected final score
- Win probabilities (home / away / draw for soccer)
- Period/quarter/half/inning breakdowns
- Spread and total projections
- Key edges and reasoning
"""

import math
from .leagues import get_league
from .data import load_team, get_league_averages


def poisson_prob(lam: float, k: int) -> float:
    """Probability of exactly k events given rate lam."""
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def build_score_matrix(home_xg: float, away_xg: float, max_goals: int = 10) -> list[list[float]]:
    """Build probability matrix[home_goals][away_goals] via independent Poisson."""
    matrix = []
    for h in range(max_goals + 1):
        row = []
        for a in range(max_goals + 1):
            row.append(poisson_prob(home_xg, h) * poisson_prob(away_xg, a))
        matrix.append(row)
    return matrix


def _expected_goals(team_off: float, opp_def: float, league_avg: float) -> float:
    """Calculate expected goals/points using attack*defense/league_avg formula."""
    if league_avg <= 0:
        return team_off
    return (team_off * opp_def) / league_avg


def predict_matchup(league_key: str, home_key: str, away_key: str) -> dict:
    """
    Run a full matchup prediction.

    Returns a dict with:
      - league, home, away (team info)
      - expected_score (home, away)
      - win_prob (home, away, draw for soccer)
      - spread, total
      - periods[] with expected scores per period
      - halves[] with expected scores per half
      - reasoning[]
    """
    league = get_league(league_key)
    home = load_team(league_key, home_key)
    away = load_team(league_key, away_key)

    if not home or not away:
        missing = []
        if not home:
            missing.append(f"Home team '{home_key}' not found in {league_key}")
        if not away:
            missing.append(f"Away team '{away_key}' not found in {league_key}")
        return {"error": missing}

    home_stats = home.get("stats", {})
    away_stats = away.get("stats", {})
    league_avgs = get_league_averages(league_key)

    sport = league["sport"]

    # ── Compute expected scores ──
    if sport == "soccer":
        result = _predict_soccer(league, home, away, home_stats, away_stats, league_avgs)
    elif sport == "hockey":
        result = _predict_hockey(league, home, away, home_stats, away_stats, league_avgs)
    elif sport == "baseball":
        result = _predict_baseball(league, home, away, home_stats, away_stats, league_avgs)
    elif sport == "basketball":
        result = _predict_basketball(league, home, away, home_stats, away_stats, league_avgs)
    elif sport == "football":
        result = _predict_football(league, home, away, home_stats, away_stats, league_avgs)
    else:
        return {"error": f"Unknown sport: {sport}"}

    # ── Common fields ──
    result["league"] = league_key.upper()
    result["league_name"] = league["name"]
    result["home"] = {"key": home_key, "name": home.get("name", home_key), "record": home.get("record", "")}
    result["away"] = {"key": away_key, "name": away.get("name", away_key), "record": away.get("record", "")}

    return result


# ─────────────────────────────────────────────
# Sport-specific prediction logic
# ─────────────────────────────────────────────

def _predict_soccer(league, home, away, hs, as_, la):
    avg_scored = la.get("goals_for_avg", league["avg_total"] / 2)
    avg_conceded = la.get("goals_against_avg", league["avg_total"] / 2)
    home_edge = league["avg_home_edge"]

    home_att = hs.get("goals_for_avg", avg_scored)
    home_def = hs.get("goals_against_avg", avg_conceded)
    away_att = as_.get("goals_for_avg", avg_scored)
    away_def = as_.get("goals_against_avg", avg_conceded)

    home_xg = _expected_goals(home_att, away_def, avg_conceded) + home_edge / 2
    away_xg = _expected_goals(away_att, home_def, avg_conceded) - home_edge / 2
    home_xg = max(home_xg, 0.3)
    away_xg = max(away_xg, 0.3)

    matrix = build_score_matrix(home_xg, away_xg, max_goals=8)
    max_g = len(matrix)

    p_home = sum(matrix[h][a] for h in range(max_g) for a in range(max_g) if h > a)
    p_away = sum(matrix[h][a] for h in range(max_g) for a in range(max_g) if a > h)
    p_draw = sum(matrix[h][a] for h in range(max_g) for a in range(max_g) if h == a)

    # BTTS
    p_btts = sum(matrix[h][a] for h in range(1, max_g) for a in range(1, max_g))

    # Over/Under common lines
    ou_lines = _compute_ou(matrix, [1.5, 2.5, 3.5, 4.5])

    # Half breakdown
    hw = league["half_weights"]
    halves = []
    if hw:
        for i, label in enumerate(league["halves"]):
            halves.append({
                "period": label,
                "home": round(home_xg * hw[i], 2),
                "away": round(away_xg * hw[i], 2),
                "total": round((home_xg + away_xg) * hw[i], 2),
            })

    # Correct scores (top 5 most likely)
    correct_scores = _top_correct_scores(matrix, 5)

    reasoning = _build_reasoning_soccer(home, away, hs, as_, home_xg, away_xg, p_home, p_draw, p_away)

    return {
        "expected_score": {"home": round(home_xg, 2), "away": round(away_xg, 2)},
        "total": round(home_xg + away_xg, 2),
        "spread": round(away_xg - home_xg, 1),
        "win_prob": {"home": round(p_home, 4), "draw": round(p_draw, 4), "away": round(p_away, 4)},
        "btts": round(p_btts, 4),
        "over_under": ou_lines,
        "halves": halves,
        "periods": [],
        "correct_scores": correct_scores,
        "reasoning": reasoning,
    }


def _predict_hockey(league, home, away, hs, as_, la):
    avg_scored = la.get("goals_for_avg", league["avg_total"] / 2)
    avg_conceded = la.get("goals_against_avg", league["avg_total"] / 2)
    home_edge = league["avg_home_edge"]

    home_att = hs.get("goals_for_avg", avg_scored)
    home_def = hs.get("goals_against_avg", avg_conceded)
    away_att = as_.get("goals_for_avg", avg_scored)
    away_def = as_.get("goals_against_avg", avg_conceded)

    home_xg = _expected_goals(home_att, away_def, avg_conceded) + home_edge / 2
    away_xg = _expected_goals(away_att, home_def, avg_conceded) - home_edge / 2
    home_xg = max(home_xg, 0.5)
    away_xg = max(away_xg, 0.5)

    matrix = build_score_matrix(home_xg, away_xg, max_goals=10)
    max_g = len(matrix)

    p_home = sum(matrix[h][a] for h in range(max_g) for a in range(max_g) if h > a)
    p_away = sum(matrix[h][a] for h in range(max_g) for a in range(max_g) if a > h)
    p_draw_reg = sum(matrix[i][i] for i in range(max_g))

    # In hockey, ties go to OT — adjust ML to include OT resolution
    p_home_ml = p_home + p_draw_reg * 0.5
    p_away_ml = p_away + p_draw_reg * 0.5

    ou_lines = _compute_ou(matrix, [4.5, 5.5, 6.5, 7.5])

    # Period breakdown
    pw = league["period_weights"]
    periods = []
    for i, label in enumerate(league["periods"]):
        periods.append({
            "period": label,
            "home": round(home_xg * pw[i], 2),
            "away": round(away_xg * pw[i], 2),
            "total": round((home_xg + away_xg) * pw[i], 2),
        })

    correct_scores = _top_correct_scores(matrix, 5)
    reasoning = _build_reasoning_default(home, away, hs, as_, home_xg, away_xg, "goals")

    return {
        "expected_score": {"home": round(home_xg, 2), "away": round(away_xg, 2)},
        "total": round(home_xg + away_xg, 2),
        "spread": round(away_xg - home_xg, 1),
        "win_prob": {
            "home": round(p_home_ml, 4),
            "away": round(p_away_ml, 4),
        },
        "regulation_draw_prob": round(p_draw_reg, 4),
        "over_under": ou_lines,
        "halves": [],
        "periods": periods,
        "correct_scores": correct_scores,
        "reasoning": reasoning,
    }


def _predict_baseball(league, home, away, hs, as_, la):
    avg_scored = la.get("runs_per_game", league["avg_total"] / 2)

    home_off = hs.get("runs_per_game", avg_scored)
    home_era = hs.get("era", la.get("era", 4.2))
    away_off = as_.get("runs_per_game", avg_scored)
    away_era = as_.get("era", la.get("era", 4.2))

    league_era = la.get("era", 4.2)
    home_edge = league["avg_home_edge"]

    # Runs = offense_rate * (opp_era / league_era)
    home_xr = home_off * (away_era / league_era) + home_edge / 2
    away_xr = away_off * (home_era / league_era) - home_edge / 2
    home_xr = max(home_xr, 1.0)
    away_xr = max(away_xr, 1.0)

    matrix = build_score_matrix(home_xr, away_xr, max_goals=15)
    max_g = len(matrix)

    p_home = sum(matrix[h][a] for h in range(max_g) for a in range(max_g) if h > a)
    p_away = sum(matrix[h][a] for h in range(max_g) for a in range(max_g) if a > h)

    ou_lines = _compute_ou(matrix, [6.5, 7.5, 8.5, 9.5, 10.5])

    # Inning breakdown
    pw = league["period_weights"]
    periods = []
    for i, label in enumerate(league["periods"]):
        periods.append({
            "period": f"Inn {label}",
            "home": round(home_xr * pw[i], 2),
            "away": round(away_xr * pw[i], 2),
            "total": round((home_xr + away_xr) * pw[i], 2),
        })

    # F5/L4 breakdown
    hw = league["half_weights"]
    halves = [
        {"period": "F5", "home": round(home_xr * hw[0], 2), "away": round(away_xr * hw[0], 2),
         "total": round((home_xr + away_xr) * hw[0], 2)},
        {"period": "L4", "home": round(home_xr * hw[1], 2), "away": round(away_xr * hw[1], 2),
         "total": round((home_xr + away_xr) * hw[1], 2)},
    ]

    reasoning = _build_reasoning_baseball(home, away, hs, as_, home_xr, away_xr)

    return {
        "expected_score": {"home": round(home_xr, 2), "away": round(away_xr, 2)},
        "total": round(home_xr + away_xr, 2),
        "spread": round(away_xr - home_xr, 1),
        "win_prob": {"home": round(p_home, 4), "away": round(p_away, 4)},
        "over_under": ou_lines,
        "halves": halves,
        "periods": periods,
        "correct_scores": [],
        "reasoning": reasoning,
    }


def _predict_basketball(league, home, away, hs, as_, la):
    avg_ppg = la.get("ppg", league["avg_total"] / 2)
    avg_opp = la.get("opp_ppg", league["avg_total"] / 2)
    home_edge = league["avg_home_edge"]

    home_off = hs.get("ppg", avg_ppg)
    home_def = hs.get("opp_ppg", avg_opp)
    away_off = as_.get("ppg", avg_ppg)
    away_def = as_.get("opp_ppg", avg_opp)

    # Points projection: (off / league_avg) * (opp_def / league_avg) * league_avg
    if avg_ppg > 0:
        home_xp = (home_off / avg_ppg) * (away_def / avg_opp) * avg_ppg + home_edge / 2
        away_xp = (away_off / avg_ppg) * (home_def / avg_opp) * avg_ppg - home_edge / 2
    else:
        home_xp = home_off + home_edge / 2
        away_xp = away_off - home_edge / 2

    total = home_xp + away_xp
    spread = away_xp - home_xp

    # Win probability from spread (basketball ~4.5 pts per std dev)
    std_dev = 11.0 if "NCAA" not in league["name"] else 9.5
    z = -spread / std_dev
    p_home = _norm_cdf(z)
    p_away = 1 - p_home

    # OU lines
    ou_lines = {}
    for line in _basketball_ou_lines(total):
        p_over = 0.5 + 0.5 * math.erf((total - line) / (std_dev * math.sqrt(2)))
        ou_lines[str(line)] = {"over": round(p_over, 4), "under": round(1 - p_over, 4)}

    # Period / half breakdown
    pw = league.get("period_weights", [])
    periods = []
    for i, label in enumerate(league["periods"]):
        periods.append({
            "period": label,
            "home": round(home_xp * pw[i], 1),
            "away": round(away_xp * pw[i], 1),
            "total": round(total * pw[i], 1),
        })

    hw = league.get("half_weights", [])
    halves = []
    for i, label in enumerate(league["halves"]):
        halves.append({
            "period": label,
            "home": round(home_xp * hw[i], 1),
            "away": round(away_xp * hw[i], 1),
            "total": round(total * hw[i], 1),
        })

    reasoning = _build_reasoning_basketball(home, away, hs, as_, home_xp, away_xp, league)

    return {
        "expected_score": {"home": round(home_xp, 1), "away": round(away_xp, 1)},
        "total": round(total, 1),
        "spread": round(spread, 1),
        "win_prob": {"home": round(p_home, 4), "away": round(p_away, 4)},
        "over_under": ou_lines,
        "halves": halves,
        "periods": periods,
        "correct_scores": [],
        "reasoning": reasoning,
    }


def _predict_football(league, home, away, hs, as_, la):
    avg_ppg = la.get("ppg", league["avg_total"] / 2)
    avg_opp = la.get("opp_ppg", league["avg_total"] / 2)
    home_edge = league["avg_home_edge"]

    home_off = hs.get("ppg", avg_ppg)
    home_def = hs.get("opp_ppg", avg_opp)
    away_off = as_.get("ppg", avg_ppg)
    away_def = as_.get("opp_ppg", avg_opp)

    if avg_ppg > 0:
        home_xp = (home_off / avg_ppg) * (away_def / avg_opp) * avg_ppg + home_edge / 2
        away_xp = (away_off / avg_ppg) * (home_def / avg_opp) * avg_ppg - home_edge / 2
    else:
        home_xp = home_off + home_edge / 2
        away_xp = away_off - home_edge / 2

    total = home_xp + away_xp
    spread = away_xp - home_xp

    # Win probability from spread (football ~13.5 pts std dev)
    std_dev = 13.5
    z = -spread / std_dev
    p_home = _norm_cdf(z)
    p_away = 1 - p_home

    # OU lines
    ou_lines = {}
    for line in _football_ou_lines(total):
        p_over = 0.5 + 0.5 * math.erf((total - line) / (std_dev * math.sqrt(2)))
        ou_lines[str(line)] = {"over": round(p_over, 4), "under": round(1 - p_over, 4)}

    # Period / half breakdown
    pw = league.get("period_weights", [])
    periods = []
    for i, label in enumerate(league["periods"]):
        periods.append({
            "period": label,
            "home": round(home_xp * pw[i], 1),
            "away": round(away_xp * pw[i], 1),
            "total": round(total * pw[i], 1),
        })

    hw = league.get("half_weights", [])
    halves = []
    for i, label in enumerate(league["halves"]):
        halves.append({
            "period": label,
            "home": round(home_xp * hw[i], 1),
            "away": round(away_xp * hw[i], 1),
            "total": round(total * hw[i], 1),
        })

    reasoning = _build_reasoning_football(home, away, hs, as_, home_xp, away_xp, league)

    return {
        "expected_score": {"home": round(home_xp, 1), "away": round(away_xp, 1)},
        "total": round(total, 1),
        "spread": round(spread, 1),
        "win_prob": {"home": round(p_home, 4), "away": round(p_away, 4)},
        "over_under": ou_lines,
        "halves": halves,
        "periods": periods,
        "correct_scores": [],
        "reasoning": reasoning,
    }


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _norm_cdf(z: float) -> float:
    """Standard normal CDF approximation."""
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def _compute_ou(matrix: list[list[float]], lines: list[float]) -> dict:
    """Compute over/under probabilities for given lines from score matrix."""
    max_g = len(matrix)
    ou = {}
    for line in lines:
        p_over = sum(
            matrix[h][a]
            for h in range(max_g) for a in range(max_g)
            if (h + a) > line
        )
        p_under = sum(
            matrix[h][a]
            for h in range(max_g) for a in range(max_g)
            if (h + a) < line
        )
        ou[str(line)] = {"over": round(p_over, 4), "under": round(p_under, 4)}
    return ou


def _top_correct_scores(matrix: list[list[float]], n: int) -> list[dict]:
    """Return top N most likely correct scores."""
    scores = []
    for h in range(len(matrix)):
        for a in range(len(matrix[0])):
            scores.append({"home": h, "away": a, "prob": round(matrix[h][a], 4)})
    scores.sort(key=lambda x: x["prob"], reverse=True)
    return scores[:n]


def _basketball_ou_lines(total: float) -> list[float]:
    """Generate sensible O/U lines around projected total."""
    base = round(total * 2) / 2  # Round to nearest 0.5
    return [base - 5, base - 2.5, base, base + 2.5, base + 5]


def _football_ou_lines(total: float) -> list[float]:
    base = round(total * 2) / 2
    return [base - 7, base - 3.5, base, base + 3.5, base + 7]


# ─────────────────────────────────────────────
# Reasoning builders
# ─────────────────────────────────────────────

def _build_reasoning_soccer(home, away, hs, as_, hxg, axg, ph, pd, pa):
    reasons = []
    hn, an = home.get("name", "Home"), away.get("name", "Away")

    reasons.append(f"Model projects {hn} {hxg:.2f} - {axg:.2f} {an}")

    if ph > pa and ph > pd:
        reasons.append(f"{hn} favored at {ph:.0%} win probability")
    elif pa > ph and pa > pd:
        reasons.append(f"{an} favored at {pa:.0%} win probability")
    else:
        reasons.append(f"Draw is the most likely outcome at {pd:.0%}")

    if hs.get("goals_for_avg") and as_.get("goals_against_avg"):
        reasons.append(
            f"{hn} scoring {hs['goals_for_avg']:.2f}/gm vs {an} conceding {as_['goals_against_avg']:.2f}/gm"
        )
    if as_.get("goals_for_avg") and hs.get("goals_against_avg"):
        reasons.append(
            f"{an} scoring {as_['goals_for_avg']:.2f}/gm vs {hn} conceding {hs['goals_against_avg']:.2f}/gm"
        )

    return reasons


def _build_reasoning_default(home, away, hs, as_, hx, ax, unit):
    hn, an = home.get("name", "Home"), away.get("name", "Away")
    return [
        f"Model projects {hn} {hx:.2f} - {ax:.2f} {an} ({unit})",
        f"Projected total: {hx + ax:.2f} {unit}",
        f"Spread: {hn} {hx - ax:+.1f}",
    ]


def _build_reasoning_baseball(home, away, hs, as_, hx, ax):
    hn, an = home.get("name", "Home"), away.get("name", "Away")
    reasons = [f"Model projects {hn} {hx:.1f} - {ax:.1f} {an} (runs)"]

    if hs.get("era") and as_.get("era"):
        reasons.append(f"Team ERA: {hn} {hs['era']:.2f} | {an} {as_['era']:.2f}")
    if hs.get("runs_per_game") and as_.get("runs_per_game"):
        reasons.append(f"Runs/game: {hn} {hs['runs_per_game']:.1f} | {an} {as_['runs_per_game']:.1f}")

    return reasons


def _build_reasoning_basketball(home, away, hs, as_, hx, ax, league):
    hn, an = home.get("name", "Home"), away.get("name", "Away")
    reasons = [f"Model projects {hn} {hx:.0f} - {ax:.0f} {an}"]

    if hs.get("ppg") and as_.get("ppg"):
        reasons.append(f"PPG: {hn} {hs['ppg']:.1f} | {an} {as_['ppg']:.1f}")
    if hs.get("opp_ppg") and as_.get("opp_ppg"):
        reasons.append(f"Opp PPG: {hn} {hs['opp_ppg']:.1f} | {an} {as_['opp_ppg']:.1f}")

    pace_h = hs.get("pace")
    pace_a = as_.get("pace")
    if pace_h and pace_a:
        reasons.append(f"Pace: {hn} {pace_h:.1f} | {an} {pace_a:.1f}")

    return reasons


def _build_reasoning_football(home, away, hs, as_, hx, ax, league):
    hn, an = home.get("name", "Home"), away.get("name", "Away")
    reasons = [f"Model projects {hn} {hx:.0f} - {ax:.0f} {an}"]

    if hs.get("ppg") and as_.get("ppg"):
        reasons.append(f"PPG: {hn} {hs['ppg']:.1f} | {an} {as_['ppg']:.1f}")
    if hs.get("opp_ppg") and as_.get("opp_ppg"):
        reasons.append(f"Opp PPG allowed: {hn} {hs['opp_ppg']:.1f} | {an} {as_['opp_ppg']:.1f}")
    if hs.get("yards_per_game") and as_.get("yards_per_game"):
        reasons.append(f"YPG: {hn} {hs['yards_per_game']:.0f} | {an} {as_['yards_per_game']:.0f}")

    return reasons
