"""
MLB Prediction Engine.

Combines starting pitcher quality, team offense, bullpen strength,
park factors, batter-vs-pitcher H2H matchups, and situational factors
to produce a comprehensive game prediction.

The model outputs:
  - Expected runs for each team
  - Moneyline win probability
  - Run line (spread) probability
  - Over/Under probabilities at multiple totals
  - First 5 innings (F5) prediction
  - Inning-by-inning breakdown
  - Key edges and reasoning
"""

import math
from .db import (
    get_team_by_id, get_team_record, get_pitcher_season,
    get_bullpen, get_park_factor, get_team_h2h_vs_pitcher,
    get_recent_games, get_pitcher_recent_starts,
)

# ── League-wide constants ───────────────────────────────────

MLB_AVG_RUNS = 4.5        # League average runs per team per game
MLB_AVG_ERA = 4.20         # League average ERA
MLB_AVG_OPS = 0.720        # League average OPS
MLB_AVG_FIP = 4.10         # League average FIP
MLB_AVG_WRC_PLUS = 100     # By definition
MLB_HOME_EDGE = 0.035      # ~3.5% home win probability boost
PITCHER_WEIGHT = 0.38      # How much starting pitcher matters
OFFENSE_WEIGHT = 0.30      # Team offense weight
BULLPEN_WEIGHT = 0.18      # Bullpen weight
PARK_WEIGHT = 0.08         # Park factor weight
H2H_WEIGHT = 0.06          # Batter vs pitcher H2H weight

# Run line standard deviation (~4.1 runs historically)
RUN_STD = 4.1


def predict_game(home_team_id: int, away_team_id: int, season: int,
                  home_pitcher_id: int | None = None,
                  away_pitcher_id: int | None = None,
                  venue: str | None = None) -> dict:
    """
    Run a full MLB game prediction.

    Returns a comprehensive prediction dict with expected scores,
    win probabilities, O/U lines, F5 prediction, and reasoning.
    """
    # Load all data
    home_team = get_team_by_id(home_team_id)
    away_team = get_team_by_id(away_team_id)
    if not home_team or not away_team:
        return {"error": "Team not found"}

    home_record = get_team_record(home_team_id, season) or {}
    away_record = get_team_record(away_team_id, season) or {}

    home_sp = get_pitcher_season(home_pitcher_id, season) if home_pitcher_id else None
    away_sp = get_pitcher_season(away_pitcher_id, season) if away_pitcher_id else None

    home_bp = get_bullpen(home_team_id, season)
    away_bp = get_bullpen(away_team_id, season)

    park = get_park_factor(venue or home_team.get("venue", ""), season)

    # ── Starting Pitcher Component ──────────────────────────

    home_sp_factor = _pitcher_factor(home_sp)
    away_sp_factor = _pitcher_factor(away_sp)

    # ── Team Offense Component ──────────────────────────────

    home_off_factor = _offense_factor(home_record)
    away_off_factor = _offense_factor(away_record)

    # ── Bullpen Component ───────────────────────────────────

    home_bp_factor = _bullpen_factor(home_bp)
    away_bp_factor = _bullpen_factor(away_bp)

    # ── Park Factor ─────────────────────────────────────────

    park_run = park.get("run_factor", 1.0) if park else 1.0

    # ── H2H Matchup Adjustment ──────────────────────────────

    home_h2h_adj = 0.0
    away_h2h_adj = 0.0
    h2h_insights = []

    if away_pitcher_id:
        home_h2h = get_team_h2h_vs_pitcher(home_team_id, away_pitcher_id)
        home_h2h_adj, insights = _h2h_adjustment(home_h2h)
        h2h_insights.extend(insights)

    if home_pitcher_id:
        away_h2h = get_team_h2h_vs_pitcher(away_team_id, home_pitcher_id)
        away_h2h_adj, insights = _h2h_adjustment(away_h2h)
        h2h_insights.extend(insights)

    # ── Compute Expected Runs ───────────────────────────────

    # Base expected runs = league_avg * offense_factor / opposing_pitcher_factor
    home_xr = MLB_AVG_RUNS * home_off_factor / away_sp_factor
    away_xr = MLB_AVG_RUNS * away_off_factor / home_sp_factor

    # Apply bullpen adjustment (opponent's bullpen reduces your runs)
    home_xr *= (1 + (1 - away_bp_factor) * BULLPEN_WEIGHT)
    away_xr *= (1 + (1 - home_bp_factor) * BULLPEN_WEIGHT)

    # Apply park factor
    home_xr *= park_run
    away_xr *= park_run

    # Apply H2H adjustment
    home_xr *= (1 + home_h2h_adj * H2H_WEIGHT)
    away_xr *= (1 + away_h2h_adj * H2H_WEIGHT)

    # Apply home field advantage
    home_xr *= (1 + MLB_HOME_EDGE)
    away_xr *= (1 - MLB_HOME_EDGE * 0.5)

    # Apply recent form adjustment
    home_form = _form_adjustment(home_team_id)
    away_form = _form_adjustment(away_team_id)
    home_xr *= (1 + home_form)
    away_xr *= (1 + away_form)

    # Ensure minimum
    home_xr = max(home_xr, 1.5)
    away_xr = max(away_xr, 1.5)

    total = home_xr + away_xr
    spread = away_xr - home_xr  # negative = home favored

    # ── Win Probability ─────────────────────────────────────

    z = -spread / RUN_STD
    p_home = _norm_cdf(z)
    p_away = 1 - p_home

    # ── F5 (First 5 Innings) Prediction ─────────────────────

    # Starting pitchers dominate F5; weight pitcher more heavily
    f5_home = home_xr * 0.58  # F5 typically ~58% of total
    f5_away = away_xr * 0.58
    # Adjust F5 more toward pitcher quality
    if home_sp:
        f5_away *= (1 + (home_sp_factor - 1) * 0.15)
    if away_sp:
        f5_home *= (1 + (away_sp_factor - 1) * 0.15)

    f5_total = f5_home + f5_away
    f5_spread = f5_away - f5_home
    f5_z = -f5_spread / (RUN_STD * 0.7)  # Lower variance in F5
    f5_p_home = _norm_cdf(f5_z)

    # ── Over/Under Lines ────────────────────────────────────

    ou_lines = _compute_ou_lines(total, RUN_STD)

    # ── Run Line (spread) Probabilities ─────────────────────

    rl_lines = {}
    for rl in [-2.5, -1.5, -0.5, 0.5, 1.5, 2.5]:
        # Home covers rl means home wins by more than |rl|
        p_cover = _norm_cdf(-(spread + rl) / RUN_STD)
        rl_lines[str(rl)] = round(p_cover, 4)

    # ── Inning Breakdown ────────────────────────────────────

    # MLB scoring distribution by inning (approximate)
    inning_weights = [0.112, 0.108, 0.115, 0.110, 0.108,
                      0.106, 0.112, 0.115, 0.114]
    innings = []
    for i in range(9):
        innings.append({
            "inning": i + 1,
            "home": round(home_xr * inning_weights[i], 2),
            "away": round(away_xr * inning_weights[i], 2),
            "total": round(total * inning_weights[i], 2),
        })

    # ── Poisson-based Correct Score Probabilities ───────────

    correct_scores = _top_scores(home_xr, away_xr, n=10)

    # ── Build Reasoning ─────────────────────────────────────

    reasoning = _build_reasoning(
        home_team, away_team, home_record, away_record,
        home_sp, away_sp, home_bp, away_bp, park,
        home_xr, away_xr, home_sp_factor, away_sp_factor,
        home_off_factor, away_off_factor, h2h_insights,
        home_form, away_form,
    )

    # ── Pitcher summaries for display ───────────────────────

    home_sp_summary = _pitcher_summary(home_sp, home_pitcher_id)
    away_sp_summary = _pitcher_summary(away_sp, away_pitcher_id)

    return {
        "home": {
            "team_id": home_team_id,
            "name": home_team["name"],
            "abbreviation": home_team["abbreviation"],
            "record": f"{home_record.get('wins', 0)}-{home_record.get('losses', 0)}",
            "pitcher": home_sp_summary,
        },
        "away": {
            "team_id": away_team_id,
            "name": away_team["name"],
            "abbreviation": away_team["abbreviation"],
            "record": f"{away_record.get('wins', 0)}-{away_record.get('losses', 0)}",
            "pitcher": away_sp_summary,
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
        "f5": {
            "home": round(f5_home, 1),
            "away": round(f5_away, 1),
            "total": round(f5_total, 1),
            "spread": round(f5_spread, 1),
            "win_prob": {
                "home": round(f5_p_home, 4),
                "away": round(1 - f5_p_home, 4),
            },
        },
        "over_under": ou_lines,
        "run_line": rl_lines,
        "innings": innings,
        "correct_scores": correct_scores,
        "park_factor": park_run,
        "reasoning": reasoning,
        "h2h_insights": h2h_insights,
        "venue": venue or home_team.get("venue", ""),
    }


# ── Component factor functions ──────────────────────────────

def _pitcher_factor(sp: dict | None) -> float:
    """
    Rate a starting pitcher relative to league average.
    Returns multiplier: <1.0 = better than avg (fewer runs), >1.0 = worse.
    """
    if not sp:
        return 1.0  # Unknown pitcher = league average

    # Primary: FIP or ERA (FIP is more predictive)
    fip = sp.get("fip") or sp.get("era") or MLB_AVG_FIP
    era = sp.get("era") or fip

    # Blend FIP (60%) and ERA (40%) — FIP better predicts future, ERA captures now
    blended = fip * 0.6 + era * 0.4

    # Normalize to league average
    factor = blended / MLB_AVG_ERA

    # xFIP adjustment (if available, blend in)
    xfip = sp.get("x_fip")
    if xfip:
        xfip_factor = xfip / MLB_AVG_FIP
        factor = factor * 0.75 + xfip_factor * 0.25

    # WHIP secondary signal
    whip = sp.get("whip")
    if whip:
        whip_adj = (whip - 1.28) * 0.08  # 1.28 = ~league avg WHIP
        factor += whip_adj

    # K rate bonus/penalty
    k_per_9 = sp.get("k_per_9")
    if k_per_9:
        k_adj = (8.5 - k_per_9) * 0.015  # 8.5 K/9 = ~avg
        factor += k_adj

    # Statcast adjustments
    barrel_pct = sp.get("barrel_pct_against")
    if barrel_pct:
        barrel_adj = (barrel_pct - 7.5) * 0.01  # 7.5% = ~avg
        factor += barrel_adj

    # Innings sample size — regress toward average with fewer innings
    innings = sp.get("innings", 0) or 0
    if innings < 40:
        regression = max(0.3, 1 - innings / 40)
        factor = factor * (1 - regression) + 1.0 * regression

    return max(0.5, min(2.0, factor))


def _offense_factor(team_stats: dict) -> float:
    """
    Rate team offense relative to league average.
    Returns multiplier: >1.0 = better offense, <1.0 = worse.
    """
    if not team_stats:
        return 1.0

    # Primary: wRC+ (already indexed to 100)
    wrc_plus = team_stats.get("wrc_plus")
    if wrc_plus:
        factor = wrc_plus / MLB_AVG_WRC_PLUS
    else:
        # Fallback to OPS
        ops = team_stats.get("ops", MLB_AVG_OPS)
        factor = ops / MLB_AVG_OPS

    # Runs per game as reality check
    rpg = team_stats.get("runs_pg")
    if rpg:
        rpg_factor = rpg / MLB_AVG_RUNS
        factor = factor * 0.7 + rpg_factor * 0.3

    return max(0.5, min(1.8, factor))


def _bullpen_factor(bp: dict | None) -> float:
    """
    Rate bullpen quality. Returns multiplier like pitcher_factor:
    <1.0 = better than avg, >1.0 = worse.
    """
    if not bp:
        return 1.0

    era = bp.get("era", MLB_AVG_ERA)
    factor = era / MLB_AVG_ERA

    # Fatigue adjustment: recent heavy usage = worse performance
    innings_3d = bp.get("innings_last_3d", 0) or 0
    if innings_3d > 10:  # Heavy recent usage
        factor *= 1.05 + (innings_3d - 10) * 0.02

    return max(0.5, min(2.0, factor))


def _h2h_adjustment(matchups: list[dict]) -> tuple[float, list[str]]:
    """
    Calculate adjustment from batter-vs-pitcher H2H data.
    Returns (adjustment_multiplier, insight_strings).
    """
    if not matchups:
        return 0.0, []

    total_ab = sum(m.get("at_bats", 0) for m in matchups)
    total_hits = sum(m.get("hits", 0) for m in matchups)
    total_hrs = sum(m.get("home_runs", 0) for m in matchups)
    total_ks = sum(m.get("strikeouts", 0) for m in matchups)

    if total_ab < 10:
        return 0.0, []

    h2h_avg = total_hits / total_ab if total_ab > 0 else 0.250
    league_avg = 0.250

    # Adjustment based on how much better/worse the lineup hits vs this pitcher
    adj = (h2h_avg - league_avg) / league_avg
    # Cap it
    adj = max(-0.15, min(0.15, adj))

    insights = []
    if total_ab >= 20:
        insights.append(
            f"Lineup is {total_hits}-for-{total_ab} (.{int(h2h_avg*1000):03d}) "
            f"with {total_hrs} HR, {total_ks} K in H2H matchups"
        )

    # Individual standout matchups
    for m in matchups:
        if m.get("at_bats", 0) >= 8:
            avg = m.get("avg", 0) or 0
            if avg >= 0.350:
                insights.append(
                    f"{m.get('batter_name', '?')} owns this pitcher: "
                    f".{int(avg*1000):03d} in {m['at_bats']} AB"
                )
            elif avg <= 0.120:
                insights.append(
                    f"{m.get('batter_name', '?')} struggles: "
                    f".{int(avg*1000):03d} in {m['at_bats']} AB"
                )

    return adj, insights


def _form_adjustment(team_id: int) -> float:
    """Adjust based on recent performance (last 10 games)."""
    recent = get_recent_games(team_id, 10)
    if len(recent) < 5:
        return 0.0

    wins = 0
    total_margin = 0
    for g in recent:
        is_home = g.get("home_team_id") == team_id
        team_score = g.get("home_score", 0) if is_home else g.get("away_score", 0)
        opp_score = g.get("away_score", 0) if is_home else g.get("home_score", 0)
        if team_score is not None and opp_score is not None:
            if team_score > opp_score:
                wins += 1
            total_margin += team_score - opp_score

    n = len(recent)
    win_pct = wins / n
    avg_margin = total_margin / n

    # Win rate component (0.5 = neutral)
    win_adj = (win_pct - 0.5) * 0.06

    # Margin component
    margin_adj = max(-0.03, min(0.03, avg_margin * 0.003))

    return max(-0.08, min(0.08, win_adj + margin_adj))


# ── Math helpers ────────────────────────────────────────────

def _norm_cdf(z: float) -> float:
    """Standard normal CDF approximation."""
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def _poisson(lam: float, k: int) -> float:
    """Poisson probability P(X=k)."""
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def _compute_ou_lines(total: float, std: float) -> dict:
    """Generate O/U probabilities at standard lines around the projected total."""
    base = round(total * 2) / 2  # Nearest 0.5
    lines = [base - 2, base - 1, base - 0.5, base, base + 0.5, base + 1, base + 2]
    # Also include common MLB totals
    for common in [7.0, 7.5, 8.0, 8.5, 9.0, 9.5, 10.0, 10.5, 11.0]:
        if common not in lines:
            lines.append(common)
    lines = sorted(set(lines))

    ou = {}
    for line in lines:
        p_over = 0.5 + 0.5 * math.erf((total - line) / (std * math.sqrt(2)))
        ou[str(line)] = {
            "over": round(p_over, 4),
            "under": round(1 - p_over, 4),
        }
    return ou


def _top_scores(home_xr: float, away_xr: float, n: int = 10) -> list[dict]:
    """Most likely final scores via independent Poisson."""
    scores = []
    for h in range(15):
        for a in range(15):
            prob = _poisson(home_xr, h) * _poisson(away_xr, a)
            scores.append({"home": h, "away": a, "prob": round(prob, 4)})
    scores.sort(key=lambda x: x["prob"], reverse=True)
    return scores[:n]


# ── Pitcher summary for display ─────────────────────────────

def _pitcher_summary(sp: dict | None, pitcher_id: int | None) -> dict | None:
    """Build a display-friendly pitcher summary."""
    if not sp and not pitcher_id:
        return None

    from .db import get_conn
    conn = get_conn()
    player = conn.execute(
        "SELECT name, throws FROM players WHERE mlb_id = ?", (pitcher_id,)
    ).fetchone() if pitcher_id else None

    summary = {
        "id": pitcher_id,
        "name": player["name"] if player else "TBD",
        "throws": player["throws"] if player else "",
    }

    if sp:
        summary.update({
            "record": f"{sp.get('wins', 0)}-{sp.get('losses', 0)}",
            "era": sp.get("era"),
            "whip": sp.get("whip"),
            "k_per_9": sp.get("k_per_9"),
            "bb_per_9": sp.get("bb_per_9"),
            "fip": sp.get("fip"),
            "innings": sp.get("innings"),
            "games_started": sp.get("games_started"),
        })

    return summary


# ── Reasoning builder ──────────────────────────────────────

def _build_reasoning(home_team, away_team, home_rec, away_rec,
                      home_sp, away_sp, home_bp, away_bp, park,
                      home_xr, away_xr, home_sp_f, away_sp_f,
                      home_off_f, away_off_f, h2h_insights,
                      home_form, away_form) -> list[str]:
    """Build human-readable analysis bullets."""
    hn = home_team["name"]
    an = away_team["name"]
    reasons = []

    # Projected score
    reasons.append(
        f"Model projects {hn} {home_xr:.1f} - {away_xr:.1f} {an} "
        f"(total: {home_xr + away_xr:.1f} runs)"
    )

    # Pitching matchup
    if home_sp and away_sp:
        home_era = home_sp.get("era", "?")
        away_era = away_sp.get("era", "?")
        home_fip = home_sp.get("fip")
        away_fip = away_sp.get("fip")
        line = f"Pitching: {hn} SP ERA {home_era}"
        if home_fip:
            line += f" (FIP {home_fip:.2f})"
        line += f" vs {an} SP ERA {away_era}"
        if away_fip:
            line += f" (FIP {away_fip:.2f})"
        reasons.append(line)

    # Pitcher edge
    if home_sp_f != 1.0 or away_sp_f != 1.0:
        if home_sp_f < away_sp_f:
            edge = (away_sp_f - home_sp_f) / away_sp_f * 100
            reasons.append(f"{hn} has a {edge:.0f}% pitching edge")
        elif away_sp_f < home_sp_f:
            edge = (home_sp_f - away_sp_f) / home_sp_f * 100
            reasons.append(f"{an} has a {edge:.0f}% pitching edge")

    # Offense comparison
    if home_off_f != 1.0 or away_off_f != 1.0:
        h_wrc = home_rec.get("wrc_plus")
        a_wrc = away_rec.get("wrc_plus")
        if h_wrc and a_wrc:
            reasons.append(f"Offense: {hn} {h_wrc:.0f} wRC+ vs {an} {a_wrc:.0f} wRC+")
        h_ops = home_rec.get("ops")
        a_ops = away_rec.get("ops")
        if h_ops and a_ops:
            reasons.append(f"OPS: {hn} .{int(h_ops*1000):03d} vs {an} .{int(a_ops*1000):03d}")

    # Bullpen
    if home_bp and away_bp:
        h_bp_era = home_bp.get("era", "?")
        a_bp_era = away_bp.get("era", "?")
        reasons.append(f"Bullpen ERA: {hn} {h_bp_era} vs {an} {a_bp_era}")

    # Park factor
    if park:
        pf = park.get("run_factor", 1.0)
        if pf > 1.03:
            reasons.append(f"Hitter-friendly park ({park.get('venue', 'venue')}, {pf:.2f}x run factor)")
        elif pf < 0.97:
            reasons.append(f"Pitcher-friendly park ({park.get('venue', 'venue')}, {pf:.2f}x run factor)")

    # Form
    if abs(home_form) > 0.02 or abs(away_form) > 0.02:
        if home_form > 0.02:
            reasons.append(f"{hn} is in good form (recent surge)")
        elif home_form < -0.02:
            reasons.append(f"{hn} is struggling recently")
        if away_form > 0.02:
            reasons.append(f"{an} is in good form (recent surge)")
        elif away_form < -0.02:
            reasons.append(f"{an} is struggling recently")

    # H2H insights
    reasons.extend(h2h_insights)

    return reasons
