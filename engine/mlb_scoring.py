"""
MLB Scoring — Score matrix, Poisson math, and probability utilities.

Pure math functions with no external data dependencies.
Extracted from mlb_predict.py for cleaner separation of concerns.
"""

import math

# ── League-wide baselines ────────────────────────────────────

MLB_AVG_RPG = 4.5          # Average runs per game per team
MLB_AVG_ERA = 4.10
MLB_AVG_OPS = .720
MLB_AVG_FIP = 4.10
MLB_AVG_WHIP = 1.28
MLB_AVG_K9 = 8.5
MLB_AVG_BB9 = 3.2
MLB_AVG_WRC_PLUS = 100     # By definition

# Home-field advantage. Pulled from config so it's tunable in one place.
# Fallback preserves the historical 0.28 default if config is missing.
try:
    from .config import MLB_HOME_EDGE  # noqa: F401
except Exception:
    MLB_HOME_EDGE = 0.28


# ── Poisson & probability ────────────────────────────────────

def _poisson_prob(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def _build_uncertain_matrix(home_xr: float, away_xr: float,
                            confidence: int, max_runs: int = 15) -> list[list[float]]:
    """
    Build score matrix with uncertainty baked in.

    Instead of a single Poisson(lambda), we average over multiple
    lambdas drawn from a range around the point estimate. The range
    is wider when confidence is low.

    At 100% confidence: single Poisson (standard).
    At 0% confidence: average over lambda ± 2.0 runs (very uncertain).

    This naturally produces probabilities closer to 50% when we
    don't have good data, preventing fake 84% edges.
    """
    if confidence >= 90:
        # High confidence — use standard single Poisson
        return _build_score_matrix(home_xr, away_xr, max_runs)

    # Uncertainty: at low confidence, each team's true scoring rate could
    # be significantly different from our estimate. We model this by
    # averaging over a range of possible lambdas.
    # 0% conf = ±3.0 runs uncertainty, 50% = ±1.5, 90% = ±0.0
    uncertainty = 3.0 * (1 - confidence / 100) ** 0.7

    # Generate 9 scenarios across the uncertainty range
    n_scenarios = 9
    combined = [[0.0] * (max_runs + 1) for _ in range(max_runs + 1)]

    for i in range(n_scenarios):
        frac = (i / (n_scenarios - 1)) - 0.5  # -0.5 to +0.5
        h_off = frac * 2 * uncertainty
        a_off = frac * 2 * uncertainty
        h_lambda = max(1.5, home_xr + h_off)
        a_lambda = max(1.5, away_xr + a_off)
        m = _build_score_matrix(h_lambda, a_lambda, max_runs)
        for h in range(max_runs + 1):
            for a in range(max_runs + 1):
                combined[h][a] += m[h][a]

    for h in range(max_runs + 1):
        for a in range(max_runs + 1):
            combined[h][a] /= n_scenarios

    return combined


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

def _run_line_probs(matrix: list[list[float]], home_xr: float = 0,
                     away_xr: float = 0) -> dict:
    """
    Compute run line probabilities for multiple spreads.
    Includes standard -1.5 plus the model's projected spread.
    """
    # Calculate probability for each possible margin
    margin_probs = {}
    for h in range(len(matrix)):
        for a in range(len(matrix[0])):
            margin = h - a  # Positive = home wins by N
            margin_probs[margin] = margin_probs.get(margin, 0) + matrix[h][a]

    # Standard -1.5 run line — all four sides
    p_home_minus_15 = sum(p for m, p in margin_probs.items() if m >= 2)   # Home wins by 2+
    p_home_plus_15 = sum(p for m, p in margin_probs.items() if m >= -1)   # Home loses by 0-1 or wins
    p_away_minus_15 = sum(p for m, p in margin_probs.items() if m <= -2)  # Away wins by 2+
    p_away_plus_15 = sum(p for m, p in margin_probs.items() if m <= 1)    # Away loses by 0-1 or wins

    # Model's projected spread (rounded to 0.5)
    model_spread = round((home_xr - away_xr) * 2) / 2
    if model_spread == 0:
        model_spread = 0.5 if home_xr > away_xr else -0.5

    # Generate lines: -1.5, model spread, and a couple around it
    lines = sorted(set([-1.5, 1.5, model_spread]))
    # Add +/- 0.5 around model spread
    for offset in [-1, -0.5, 0.5, 1]:
        lines.append(model_spread + offset)
    lines = sorted(set(l for l in lines if -6 <= l <= 6))

    spreads = {}
    for line in lines:
        # P(home covers line): home margin > line
        if line > 0:
            # Home -line: home must win by more than line
            p_cover = sum(p for m, p in margin_probs.items() if m > line)
            label = f"home_{line:+.1f}".replace("+", "minus_").replace("-", "plus_").replace(".", "_")
        else:
            # Home +line (underdog): home can lose by less than |line|
            p_cover = sum(p for m, p in margin_probs.items() if m > line)
            label = f"home_{line:+.1f}".replace("+", "minus_").replace("-", "plus_").replace(".", "_")

        spreads[str(line)] = {
            "home_cover": round(p_cover, 4),
            "away_cover": round(1 - p_cover, 4),
        }

    # Cap RL probabilities to realistic ranges.
    # No team covers -1.5 more than ~75% of the time in MLB.
    # The +1.5 side covers at most ~85%.
    # Raw Poisson matrix with extreme xR gaps (6.5 vs 2.0) produces
    # 90%+ cover probabilities which are miscalibrated.
    # Cap all four sides to realistic MLB ranges.
    p_home_minus_15 = max(0.15, min(0.65, p_home_minus_15))
    p_home_plus_15 = max(0.30, min(0.75, p_home_plus_15))
    p_away_minus_15 = max(0.15, min(0.65, p_away_minus_15))
    p_away_plus_15 = max(0.30, min(0.75, p_away_plus_15))

    return {
        "home_minus_1_5": round(p_home_minus_15, 4),
        "away_plus_1_5": round(p_away_plus_15, 4),
        "home_plus_1_5": round(p_home_plus_15, 4),
        "away_minus_1_5": round(p_away_minus_15, 4),
        "model_spread": model_spread,
        "spreads": spreads,
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
