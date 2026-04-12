"""
MLB Factors — Feature engineering and factor computation.

Computes individual prediction factors (offense ratings, pitcher
adjustments, bullpen factors, H2H, form, confidence, etc.).
Extracted from mlb_predict.py for cleaner separation of concerns.

────────────────────────────────────────────────────────────────────────
STACKED-MULTIPLIER AUDIT (MLB)
────────────────────────────────────────────────────────────────────────
The current mlb_predict.predict_matchup() stacks the following
multiplicative adjustments on home_xr / away_xr, in order. NHL had 12
compounding factors and that broke the model; MLB has 16+ which is
why RL barely works and ML/OU/1st INN lose money.

"Validated" below means: has an empirical backtest tying the factor's
output to actual outcomes with a documented WR lift. Most of these are
plausible-sounding priors that have NOT been validated end-to-end.

Multiplier name                     Range         Validated?  Notes
────────────────────────────────────────────────────────────────────
 1. blended_pitcher (SP factor)     0.60 – 1.50   partial     Core signal; FIP/ERA-based.
 2. lineup_strength                 0.90 – 1.12   NO          wRC+/OPS proxy, assumed independent of team offense baseline but overlaps.
 3. team_cal offense_factor         learned       partial     From team_calibration.py; learned off past games.
 4. team_cal defense_factor         learned       partial     Cross-applied (opp scoring); compounds with #1.
 5. team_cal home_factor            learned       partial     Compounds #3/#4 conditioned on venue — double-dips home advantage.
 6. team_cal away_factor            learned       partial     Same as #5 for away side.
 7. (1 + 0.35*(bullpen-1))          ~0.89 – 1.14  partial     Opponent bullpen softens/raises scoring.
 8. bullpen_fatigue_penalty         1.00 – 1.05   NO          Compounds directly on top of #7 — no independence argument.   [SITUATIONAL]
 9. park_run_factor                 ~0.92 – 1.08  YES (known) Standard MLB research backs this.
10. coors_boost (if Coors)          1.08          NO          Extra +8% on top of park factor — double-count risk at Coors.
11. situational aggregate           ~0.92 – 1.08  NO          From situational.py: weather + rest + pitcher rest + lineup + platoon, already compounded internally.                                                [SITUATIONAL]
12. umpire_factor                   ~0.95 – 1.05  NO          Applied to BOTH home and away (not differential).             [SITUATIONAL]
13. weather_adj                     ~0.90 – 1.10  NO          Duplicates weather inside situational aggregate #11.          [SITUATIONAL]
14. travel_fatigue (per side)       ~0.95 – 1.05  NO          Compounds with situational rest factor #11.                   [SITUATIONAL]
15. platoon_home_adj / platoon_away 0.97 / 1.00   NO          Duplicates the platoon factor inside situational #11.
16. matchup interaction             ~0.92 – 1.10  NO          Compound-on-compound; reads team_cal already-applied factors. [SITUATIONAL]
17. (1 + form) (additive)           0.90 – 1.10   partial     Recent form; small range.
18. injury impact                   ~0.92 – 1.00  partial     ESPN injury list impact.

Count of multiplicative compounding layers: 16–18 (depending on how you
count learned team-cal and form). NHL had 12; MLB has more.

Flagged for independence violations (compound-on-compound):
  • #8 bullpen_fatigue stacks on #7 bullpen_factor
  • #10 coors stacks on #9 park factor
  • #11 situational aggregate duplicates #13 weather and #15 platoon
  • #12 umpire is applied symmetrically — does not differentiate sides
  • #14 travel duplicates situational "rest" component in #11
  • #16 matchup interaction reads already-adjusted team_cal factors (#3-#6) and multiplies again

The MLB_ENABLE_SITUATIONAL_FACTORS toggle in config.py gates the group
marked [SITUATIONAL] above (#8 bullpen fatigue, #11 situational
aggregate, #12 umpire, #13 weather, #14 travel, #16 matchup
interaction) so we can ablate and see if ML / O/U / 1st INN WR lifts.
────────────────────────────────────────────────────────────────────────
"""

import logging
from datetime import datetime

from .db import get_conn, get_recent_games

from .mlb_scoring import (
    MLB_AVG_RPG, MLB_AVG_ERA, MLB_AVG_OPS, MLB_AVG_FIP,
    MLB_AVG_WHIP, MLB_AVG_K9, MLB_AVG_BB9, MLB_AVG_WRC_PLUS,
    _poisson_prob, _build_score_matrix, _win_probs_from_matrix,
)

logger = logging.getLogger(__name__)


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


def _blended_offense(pit: dict | None, stats: dict) -> float:
    """
    Blend PIT runs/game with league average based on sample size.
    Small sample: mostly league avg. Large sample: mostly PIT data.
    """
    base = _team_offense_rating(stats)  # From team_stats table or league avg

    if not pit or not pit.get("runs_pg"):
        return base

    gp = pit.get("games_played", 0)
    pit_rpg = pit["runs_pg"]

    if gp == 0:
        return base
    elif gp < 5:
        # Very small sample — 20% PIT, 80% baseline
        return pit_rpg * 0.20 + base * 0.80
    elif gp < 15:
        # Small sample — 50% PIT, 50% baseline
        return pit_rpg * 0.50 + base * 0.50
    elif gp < 30:
        # Moderate — 75% PIT, 25% baseline
        return pit_rpg * 0.75 + base * 0.25
    else:
        # Full confidence in PIT data
        return pit_rpg


def _blended_pitcher(sp_pit: dict | None, sp_db: dict | None) -> float:
    """
    Blend PIT pitcher ERA with DB stats or league average.
    Small sample: mostly league avg. More starts: mostly PIT.
    """
    db_factor = _pitcher_factor(sp_db)

    if not sp_pit or not sp_pit.get("era"):
        return db_factor

    starts = sp_pit.get("games_started", 0)
    pit_factor = sp_pit["era"] / MLB_AVG_ERA
    pit_factor = max(0.60, min(1.50, pit_factor))

    if starts == 0:
        return db_factor
    elif starts == 1:
        # One start — 20% PIT, 80% baseline
        return pit_factor * 0.20 + db_factor * 0.80
    elif starts <= 3:
        # 50/50 blend
        return pit_factor * 0.50 + db_factor * 0.50
    elif starts <= 8:
        # 75% PIT
        return pit_factor * 0.75 + db_factor * 0.25
    else:
        return pit_factor


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


def _bullpen_fatigue_penalty(bp: dict, recent_games: list[dict]) -> float:
    """
    Additional bullpen fatigue penalty based on heavy recent usage.

    Returns a multiplier >= 1.0 (higher = opponent scores more runs).
    Compounds with the base bullpen factor.

    Checks two conditions:
    1. Used 4+ relievers yesterday AND played the day before that: +3%
    2. Bullpen ERA in last 7 days > 5.00: +2%
    """
    penalty = 1.0

    if not recent_games or len(recent_games) < 2:
        return penalty

    # Condition 1: Heavy reliever usage in back-to-back games.
    # Detect if they played yesterday AND the day before by checking
    # the dates of the two most recent games.
    from datetime import timedelta
    today_dt = datetime.now().date()
    try:
        game1_date = datetime.strptime(recent_games[0].get("date", ""), "%Y-%m-%d").date()
        game2_date = datetime.strptime(recent_games[1].get("date", ""), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        game1_date = None
        game2_date = None

    if game1_date and game2_date:
        played_yesterday = (today_dt - game1_date).days == 1
        played_day_before = (today_dt - game2_date).days == 2

        if played_yesterday and played_day_before:
            # Check if bullpen was heavily used yesterday.
            # games_last_3d >= 4 implies 4+ relievers appeared in the window;
            # with back-to-back games that signals heavy usage yesterday.
            games_3d = (bp or {}).get("games_last_3d", 0) or 0
            if games_3d >= 4:
                penalty *= 1.03  # -3% runs penalty (tired bullpen)

    # Condition 2: Recent bullpen ERA is terrible.
    # innings_last_7d is tracked; estimate a 7-day ERA from it
    # by looking at the bullpen's recent innings and overall ERA trend.
    # The bullpen_stats table doesn't have a separate era_7d field,
    # so we approximate: if the bullpen has high innings in 7 days AND
    # the season ERA is already elevated, that compounds.
    bp_era = (bp or {}).get("era", 0) or 0
    innings_7d = (bp or {}).get("innings_last_7d", 0) or 0

    # If heavy recent innings and bad ERA, it's likely worse in that window
    if bp_era > 5.00 and innings_7d > 5:
        penalty *= 1.02  # -2% additional penalty

    return penalty


def _compute_lineup_strength(team_id: int, season: int) -> float:
    """Compute lineup quality multiplier from individual batter stats.

    Queries the batter_stats table for the team's active batters,
    computes average wRC+ or OPS, and compares to league average.

    Returns multiplier (1.0 = average, 1.05 = strong, 0.95 = weak).
    """
    conn = get_conn()

    # Get batters for this team/season with meaningful plate appearances
    rows = conn.execute("""
        SELECT wrc_plus, ops, plate_appearances
        FROM batter_stats
        WHERE team_id = ? AND season = ? AND plate_appearances >= 30
        ORDER BY plate_appearances DESC
        LIMIT 13
    """, (team_id, season)).fetchall()

    if not rows:
        return 1.0

    # Prefer wRC+ (directly comparable to league avg of 100)
    wrc_values = [(r["wrc_plus"], r["plate_appearances"]) for r in rows
                  if r["wrc_plus"] is not None and r["wrc_plus"] > 0]

    if wrc_values:
        total_pa = sum(pa for _, pa in wrc_values)
        if total_pa == 0:
            return 1.0
        weighted_wrc = sum(wrc * pa for wrc, pa in wrc_values) / total_pa
        # Convert to multiplier: 100 = 1.0, 110 = ~1.04, 90 = ~0.96
        multiplier = 1.0 + (weighted_wrc - MLB_AVG_WRC_PLUS) / MLB_AVG_WRC_PLUS * 0.40
        return max(0.90, min(1.12, multiplier))

    # Fallback to OPS
    ops_values = [(r["ops"], r["plate_appearances"]) for r in rows
                  if r["ops"] is not None and r["ops"] > 0]

    if ops_values:
        total_pa = sum(pa for _, pa in ops_values)
        if total_pa == 0:
            return 1.0
        weighted_ops = sum(ops * pa for ops, pa in ops_values) / total_pa
        multiplier = 1.0 + (weighted_ops - MLB_AVG_OPS) / MLB_AVG_OPS * 0.35
        return max(0.90, min(1.12, multiplier))

    return 1.0


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
    today = datetime.now().strftime("%Y-%m-%d")
    season = datetime.now().year

    # Always compute expected 1st inning runs (needed for Poisson baseline)
    away_1st_xr = away_xr * first_inning_weight * (0.85 + 0.15 * home_sp_factor)
    home_1st_xr = home_xr * first_inning_weight * (0.85 + 0.15 * away_sp_factor)

    # Poisson baseline (always computed)
    p_away_zero_poisson = _poisson_prob(away_1st_xr, 0)
    p_home_zero_poisson = _poisson_prob(home_1st_xr, 0)

    # Start with Poisson
    p_away_zero = p_away_zero_poisson
    p_home_zero = p_home_zero_poisson

    # Blend with pitcher first-inning data when available
    # Weight by sample size: more starts = more trust in pitcher data
    if home_pitcher_id:
        sp_pit = compute_pitcher_stats_at_date(home_pitcher_id, today, season)
        if sp_pit and sp_pit.get("first_inning_scoreless_pct") is not None and sp_pit.get("first_inning_starts", 0) >= 3:
            starts = sp_pit["first_inning_starts"]
            pit_weight = min(0.7, starts / 30)  # Max 70% pitcher data, even with 30+ starts
            p_away_zero = (pit_weight * sp_pit["first_inning_scoreless_pct"]
                          + (1 - pit_weight) * p_away_zero_poisson)

    if away_pitcher_id:
        sp_pit = compute_pitcher_stats_at_date(away_pitcher_id, today, season)
        if sp_pit and sp_pit.get("first_inning_scoreless_pct") is not None and sp_pit.get("first_inning_starts", 0) >= 3:
            starts = sp_pit["first_inning_starts"]
            pit_weight = min(0.7, starts / 30)
            p_home_zero = (pit_weight * sp_pit["first_inning_scoreless_pct"]
                          + (1 - pit_weight) * p_home_zero_poisson)

    # Cap at realistic bounds. Previous bounds (0.40-0.92) produced NRFI
    # probabilities as high as 85% which are wildly miscalibrated — the
    # backtest showed 1st INN picks at "80%+ confidence" were actually
    # winning 46% of the time. Real per-team P(0 runs in 1st) is 65-80%,
    # rarely outside that range.
    p_away_zero = max(0.55, min(0.80, p_away_zero))
    p_home_zero = max(0.55, min(0.80, p_home_zero))

    # NRFI = both teams score 0 in the first
    nrfi = p_home_zero * p_away_zero

    # Regress hard toward MLB baseline (~56% NRFI) — individual matchups
    # rarely deviate more than ±10% from the league average.
    MLB_NRFI_BASELINE = 0.56
    nrfi = MLB_NRFI_BASELINE * 0.65 + nrfi * 0.35  # 65% baseline weight
    nrfi = max(0.45, min(0.68, nrfi))  # Hard cap to realistic range
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


def _compute_confidence(home_pit, away_pit, home_sp_pit, away_sp_pit) -> dict:
    """
    Compute prediction confidence based on data quality.
    Returns 0-100 score and a label.
    """
    score = 0
    max_score = 0

    # Team games played (0-25 pts each)
    for pit in [home_pit, away_pit]:
        max_score += 25
        gp = pit.get("games_played", 0) if pit else 0
        if gp >= 50:
            score += 25
        elif gp >= 30:
            score += 20
        elif gp >= 15:
            score += 15
        elif gp >= 5:
            score += 8
        elif gp > 0:
            score += 3

    # Pitcher starts (0-25 pts each)
    for sp in [home_sp_pit, away_sp_pit]:
        max_score += 25
        starts = sp.get("games_started", 0) if sp else 0
        if starts >= 10:
            score += 25
        elif starts >= 5:
            score += 18
        elif starts >= 3:
            score += 12
        elif starts >= 1:
            score += 5

    pct = round(score / max_score * 100) if max_score > 0 else 0

    if pct >= 80:
        label = "high"
    elif pct >= 50:
        label = "medium"
    elif pct >= 25:
        label = "low"
    else:
        label = "very_low"

    return {"score": pct, "label": label}


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
