"""
Conditional live predictor — Phase 3b stage 1.

Given a live game state (current score + clock + period), project the
remaining-game scoring distribution and emit:

  - `home_win_prob`: probability home team finishes ahead
  - `total_dist`:    {mean, std} of final combined score
  - `margin_dist`:   {mean, std} of final home-minus-away margin
  - `regulation_end_in_s`: seconds of game-clock left in regulation

Stage 1 (this file): NBA full-game ML + TOTAL only.
Stage 2 (next): NBA quarters/halves, NHL periods.

Approach
========

We treat remaining scoring as a Brownian-motion increment with variance
that scales linearly in remaining time. Empirically this is close
enough to truth for live ML/total markets at NBA's pace; the heavier
tail at extreme score states (garbage time, intentional fouling) is
handled by garbage-time detection (deferred to stage 2).

For NBA:

    σ²_margin_full = MARGIN_STD_DEV² = 16.06²        (calibrated n=4130)
    σ²_total_full  = TOTAL_STD_DEV²  = 21.25²

    remaining_fraction f = remaining_seconds / 2880  (48 min × 60)
    σ_margin_remaining   = MARGIN_STD_DEV × √f
    σ_total_remaining    = TOTAL_STD_DEV × √f

The remaining-margin mean is held at zero (stage 1 — no prematch
prediction dependency). Remaining-total mean blends current observed
pace with league average, weighted by elapsed fraction:

    blended_pace = α × observed_pace + (1-α) × league_avg_pace
    α = clip(elapsed_fraction × 2, 0, 1)

This avoids early-game pace noise (one fast Q1 doesn't project to
260+ point game) while letting late-game observed scoring dominate.

Final-score predictions:

    final_margin   ~ Normal(current_margin, σ_margin_remaining)
    final_total    ~ Normal(current_total + blended_pace × rem_min,
                            σ_total_remaining)

P(home_win) = Φ(current_margin / σ_margin_remaining)
P(over X)   = 1 - Φ((X - μ_total_final) / σ_total_remaining)

OT handling: if the game reaches OT (margin = 0 at end of regulation),
we follow ESPN's score forward — overtime periods append to the
linescore array. The predictor doesn't need to model OT explicitly
for live picks because once OT begins we restart the clock with
period=5 and shrink σ_remaining accordingly. The bet settles the
moment OT ends regardless.
"""

from __future__ import annotations
import logging
import math

logger = logging.getLogger(__name__)


# ── NBA calibration (mirrors engine.nba_predict) ──
NBA_REGULATION_SECONDS = 48 * 60      # 2880s; OT periods are 5min each
NBA_QUARTER_SECONDS    = 12 * 60      # 720s per quarter
NBA_HALF_SECONDS       = 24 * 60      # 1440s per half (Q1+Q2 or Q3+Q4)
NBA_MARGIN_STD_FULL    = 16.06
NBA_TOTAL_STD_FULL     = 21.25
NBA_LEAGUE_AVG_TOTAL   = 227.89        # 113.95 ppg per team × 2
NBA_LEAGUE_AVG_PACE_PPS = NBA_LEAGUE_AVG_TOTAL / NBA_REGULATION_SECONDS

# Per-period league averages. Empirically Q3 scores more than Q1/Q2/Q4
# (post-halftime adjustment + foul-trouble rotation) but the split is
# small at slate level. Treat all quarters as equal until tracker
# data justifies a per-quarter constant.
NBA_LEAGUE_AVG_QUARTER_TOTAL = NBA_LEAGUE_AVG_TOTAL / 4.0   # ~56.97
NBA_LEAGUE_AVG_HALF_TOTAL    = NBA_LEAGUE_AVG_TOTAL / 2.0   # ~113.95

# Per-period stds. Quarters aren't independent (pace correlates across
# periods within one game), so σ_quarter > σ_full / √4. Empirical hold:
# σ_quarter ≈ 11 pts, σ_half ≈ 15 pts. Tunable via flags as we
# accumulate live tracker history; don't refit until we have ~200
# settled per-period picks.
# σ values tightened from live_backtest n=4129 quarter-break replays
# (2026-04-29). Backtest measured Q4-from-end-of-Q3 RMSE = 9.27 and
# H2-from-end-of-Q2 RMSE = 13.55. Pre-tightening we used 11.0 and
# 15.0 — picks were firing at lower confidence than the predictor
# could justify. Margin σ tightened proportionally.
NBA_QUARTER_TOTAL_STD  = 9.5
NBA_HALF_TOTAL_STD     = 13.5
NBA_QUARTER_MARGIN_STD = 7.5
NBA_HALF_MARGIN_STD    = 10.5

# Garbage-time damper. When the leading team is up 20+ in Q4 (or
# 25+ at end of Q3), scoring decelerates: starters benched, teams
# walk the ball up, intentional fouling fragments possessions. The
# raw pace-projection over-states remaining scoring by ~1-2 pts on
# average (live_backtest end-of-Q3 -> Q4 bias = +1.95). Damping the
# projected remaining pace by 0.70 in this regime is the basketball-
# analytics-literature default — used without re-tuning to avoid
# over-fitting the same backtest dataset that surfaced the bias.
NBA_GARBAGE_MARGIN_Q4 = 20      # Q4: |margin| >= 20 triggers damper
NBA_GARBAGE_MARGIN_Q3 = 25      # Late Q3: tighter threshold
NBA_GARBAGE_PACE_FACTOR = 0.85


# ── Math helpers ──

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


# ── State extraction ──

def _nba_remaining_seconds(state: dict) -> int:
    """Seconds of regulation game-clock remaining.

    Periods 1-4 are 12 min each in regulation; OT periods are 5 min.
    State carries `period` (1-4 reg, 5+ OT) and `clock_secs` (parsed
    from the ESPN displayClock).

    For OT we return 0 — by definition regulation has ended, so
    margin_sigma collapses and live picks settle on regulation result.
    Stage 2 will model OT explicitly (margin distribution over a
    5-minute period instead of regulation-only).
    """
    status = state.get("status") or {}
    period = int(status.get("period") or 0)
    clock_secs = int(status.get("clock_secs") or 0)

    if period <= 0:
        return NBA_REGULATION_SECONDS
    if period >= 5:
        # In OT — for stage 1 we ignore further scoring
        return 0
    # Periods 1–4: each is 12 min long
    completed_periods = period - 1
    remaining = (4 - completed_periods) * 12 * 60 - (12 * 60 - clock_secs)
    return max(0, remaining)


def _current_score(state: dict) -> tuple[int, int]:
    """Return (home_score, away_score). Defaults to 0/0 on missing
    fields so a half-formed payload doesn't crash the predictor."""
    home = state.get("home") or {}
    away = state.get("away") or {}
    try:
        return int(home.get("score") or 0), int(away.get("score") or 0)
    except (ValueError, TypeError):
        return 0, 0


def _is_garbage_time(period: int, abs_margin: float) -> bool:
    """True when the game's competitive phase is effectively over
    and remaining pace should be damped. Two thresholds:

      - Q4 (period == 4): |margin| >= 20
      - Late Q3 (period == 3, applies only to predictions reaching
        into Q4 like H2/full-game): |margin| >= 25

    Period 5+ (overtime) is excluded — OT margins are by definition
    small and competitive.
    """
    if period == 4 and abs_margin >= NBA_GARBAGE_MARGIN_Q4:
        return True
    if period == 3 and abs_margin >= NBA_GARBAGE_MARGIN_Q3:
        return True
    return False


# ── Predictor ──

def predict_live_nba_full(state: dict,
                          prematch_total: float | None = None,
                          prematch_margin: float | None = None) -> dict:
    """Project the final-score distribution for a live NBA game.

    ``prematch_total`` / ``prematch_margin`` are the team-strength prior
    from the prematch model (engine.nba_predict.predict_full). When
    supplied, the live predictor blends remaining-game scoring toward
    that prior instead of league average — it knows that ORL/DET were
    expected to score ~215 prematch, not 228 (league avg), and that
    CLE/TOR were expected at ~225. Mean reversion lands on the
    sport-+-team-specific expectation, not a generic league number.

    Without the prior, falls back to the league-average blend (legacy
    behaviour). The picks generator passes the prior through whenever
    ``predict_full`` is callable for the matchup; backtest paths that
    only have a state dict (no DB access) get the legacy fallback.

    Returns ``{}`` when the game isn't actually in-play (e.g. a
    pre-game payload slipped through) so callers can short-circuit.

    Result keys::

        home_win_prob   — P(final home margin > 0)  [0, 1]
        push_prob       — P(final margin == 0 at regulation end)
        margin_mean     — μ of final home-minus-away margin
        margin_std      — σ of final margin
        total_mean      — μ of final combined score
        total_std       — σ of final total
        remaining_s     — regulation-clock seconds left
        elapsed_frac    — 1 - (remaining_s / regulation_total)
        current_margin  — home_score - away_score right now
        current_total   — home_score + away_score right now
    """
    status = state.get("status") or {}
    if status.get("state") != "in":
        return {}

    home_score, away_score = _current_score(state)
    cur_margin = home_score - away_score
    cur_total = home_score + away_score

    remaining_s = _nba_remaining_seconds(state)
    elapsed_s = NBA_REGULATION_SECONDS - remaining_s
    elapsed_frac = _clip(elapsed_s / NBA_REGULATION_SECONDS, 0.0, 1.0)
    remaining_frac = 1.0 - elapsed_frac

    # σ scales with sqrt of remaining time.
    sigma_margin = NBA_MARGIN_STD_FULL * math.sqrt(remaining_frac)
    sigma_total = NBA_TOTAL_STD_FULL * math.sqrt(remaining_frac)

    # ── Pace blending ──
    # When prematch_total is provided, the prior is the team-specific
    # expected pace (predicted_total / regulation_seconds). Otherwise
    # fall back to league average. Same shape either way: as the game
    # progresses, observed pace gets more weight, but the prior never
    # fully washes out — we cap the observed weight below 1.0 so games
    # always retain some pull toward the team-strength expectation.
    if prematch_total is not None and prematch_total > 0:
        prior_pace = float(prematch_total) / NBA_REGULATION_SECONDS
    else:
        prior_pace = NBA_LEAGUE_AVG_PACE_PPS

    if elapsed_s > 0:
        observed_pace = cur_total / elapsed_s
    else:
        observed_pace = prior_pace

    # α-cap: even at end of game the prior gets some weight. Without
    # this cap, ORL/DET running hot through Q2 produced a model
    # forecast of 251 final (vs market 225, prematch 215, actual ~210)
    # because α hit 1.0 at halftime and league/prematch washed out.
    # Cap at 0.7 leaves 30% prior weight forever, dampens overshoot.
    NBA_LIVE_OBSERVED_WEIGHT_CAP = 0.7
    alpha = _clip(elapsed_frac * 2.0, 0.0, NBA_LIVE_OBSERVED_WEIGHT_CAP)
    blended_pace = alpha * observed_pace + (1 - alpha) * prior_pace

    # Garbage-time damper: when the game is decided, remaining pace
    # decelerates. See module docstring + _is_garbage_time.
    period = int((status or {}).get("period") or 0)
    if _is_garbage_time(period, abs(cur_margin)):
        blended_pace *= NBA_GARBAGE_PACE_FACTOR

    # ── Margin blending ──
    # Live margin starts at the current scoreline. Without a prior,
    # remaining-game margin is centered on zero (no team-strength
    # signal). With prematch_margin, the remaining margin is centered
    # on the prematch expectation scaled to remaining time — so a
    # game where the home team was favored by 8 over 48 min carries a
    # 2-pt remaining margin advantage at end of Q3 (8 × 0.25).
    if prematch_margin is not None:
        remaining_margin_prior = float(prematch_margin) * remaining_frac
    else:
        remaining_margin_prior = 0.0
    margin_mean = float(cur_margin) + remaining_margin_prior

    total_mean = cur_total + blended_pace * remaining_s

    # Probabilities (handle σ→0 at regulation end / in OT)
    if sigma_margin <= 1e-6:
        home_win_prob = 1.0 if margin_mean > 0 else (0.5 if margin_mean == 0 else 0.0)
    else:
        home_win_prob = 1.0 - _norm_cdf(-margin_mean / sigma_margin)

    return {
        "home_win_prob":   _clip(home_win_prob, 0.0, 1.0),
        "margin_mean":     margin_mean,
        "margin_std":      sigma_margin,
        "total_mean":      total_mean,
        "total_std":       sigma_total,
        "remaining_s":     remaining_s,
        "elapsed_frac":    elapsed_frac,
        "current_margin":  cur_margin,
        "current_total":   cur_total,
    }


def _period_endpoints(code: str) -> tuple[int, int]:
    """Return (start_period, end_period) for a NBA period market code.

    Q1=(1,1), Q2=(2,2), Q3=(3,3), Q4=(4,4),
    H1=(1,2),  H2=(3,4)

    Periods are 1-indexed to match ESPN's `status.period` field.
    """
    return {
        "Q1": (1, 1), "Q2": (2, 2), "Q3": (3, 3), "Q4": (4, 4),
        "H1": (1, 2), "H2": (3, 4),
    }[code]


def _period_score_so_far(state: dict, start_p: int, end_p: int) -> tuple[int, int, bool]:
    """Sum the home/away points scored in periods [start_p, end_p]
    inclusive based on ESPN linescores. Returns (home_pts, away_pts,
    period_closed) — period_closed is True if every period in the
    range has been completed (current ESPN period is past end_p, or
    the game is final).
    """
    status = state.get("status") or {}
    cur_period = int(status.get("period") or 0)
    completed = bool(status.get("completed"))
    linescores = state.get("linescores") or {}
    home_ls = linescores.get("home") or []
    away_ls = linescores.get("away") or []

    home_pts = 0
    away_pts = 0
    for p in range(start_p, end_p + 1):
        idx = p - 1
        if idx < len(home_ls):
            try:
                home_pts += int(home_ls[idx])
            except (TypeError, ValueError):
                pass
        if idx < len(away_ls):
            try:
                away_pts += int(away_ls[idx])
            except (TypeError, ValueError):
                pass

    period_closed = completed or cur_period > end_p
    return home_pts, away_pts, period_closed


def predict_live_nba_period(state: dict, code: str) -> dict:
    """Project the final-score distribution for a single NBA period
    market (Q1/Q2/Q3/Q4 or H1/H2).

    Three regimes per period:
      - **closed**: the period has already ended; returns the actual
        scores and ``status='closed'`` so callers can skip pick gen.
      - **current**: ESPN's clock is inside this period range; project
        remaining time using observed-pace blending (same logic as
        full-game, but scoped to the period's time budget).
      - **future**: the period hasn't started; project the full window
        using league averages (no live conditioning available).

    Result keys (when status != 'closed')::

        status         "current" | "future"
        total_mean     μ of period total points
        total_std      σ of period total points
        margin_mean    μ of home-minus-away margin within the period
        margin_std     σ of period margin
        remaining_s    seconds left in the period range
        period_total   actual home+away pts already scored in-period
        period_margin  actual (home-away) pts already scored in-period
    """
    if code not in ("Q1", "Q2", "Q3", "Q4", "H1", "H2"):
        return {}

    status = state.get("status") or {}
    if status.get("state") != "in":
        return {}

    start_p, end_p = _period_endpoints(code)
    cur_period = int(status.get("period") or 0)
    clock_secs = int(status.get("clock_secs") or 0)

    home_in, away_in, closed = _period_score_so_far(state, start_p, end_p)
    period_total = home_in + away_in
    period_margin = home_in - away_in

    if closed:
        return {
            "status": "closed",
            "period_total":  period_total,
            "period_margin": period_margin,
        }

    # Period budgets — quarter = 12min, half = 24min.
    period_seconds = (NBA_HALF_SECONDS if code in ("H1", "H2")
                      else NBA_QUARTER_SECONDS)
    is_half = code in ("H1", "H2")

    # Compute remaining_s within the period range.
    # Cases: cur_period < start_p → full period ahead (future)
    #        cur_period > end_p   → impossible (caught by `closed`)
    #        start_p <= cur_period <= end_p → partial period
    if cur_period < start_p:
        regime = "future"
        remaining_s = period_seconds
        elapsed_s = 0
    else:
        regime = "current"
        # Time left in the current quarter is clock_secs.
        # Plus full quarters remaining within the period range.
        full_quarters_left = end_p - cur_period
        remaining_s = clock_secs + full_quarters_left * NBA_QUARTER_SECONDS
        elapsed_s = period_seconds - remaining_s

    elapsed_frac = max(0.0, min(1.0, elapsed_s / period_seconds))

    # σ scales with √(remaining time / period time). For a future
    # period this is √1 = 1 (full uncertainty); for a current period
    # we shrink as elapsed time accumulates.
    if is_half:
        sigma_total_full = NBA_HALF_TOTAL_STD
        sigma_margin_full = NBA_HALF_MARGIN_STD
        league_avg_total = NBA_LEAGUE_AVG_HALF_TOTAL
    else:
        sigma_total_full = NBA_QUARTER_TOTAL_STD
        sigma_margin_full = NBA_QUARTER_MARGIN_STD
        league_avg_total = NBA_LEAGUE_AVG_QUARTER_TOTAL

    rem_frac = remaining_s / period_seconds
    sigma_total = sigma_total_full * math.sqrt(rem_frac)
    sigma_margin = sigma_margin_full * math.sqrt(rem_frac)

    league_avg_pps = league_avg_total / period_seconds

    # Pace blending — same shape as full-game predictor. Early in the
    # period we trust league average; mid/late we trust observed pace.
    if elapsed_s > 0 and regime == "current":
        observed_pps = period_total / elapsed_s
        alpha = _clip(elapsed_frac * 2.0, 0.0, 1.0)
        blended_pps = alpha * observed_pps + (1 - alpha) * league_avg_pps
    else:
        blended_pps = league_avg_pps

    # Garbage-time damper. Use whole-game margin (home_score-away_score
    # at this moment), not period margin — the damper kicks in when the
    # GAME is decided, regardless of which period the bet is on.
    cur_period = int(status.get("period") or 0)
    home_now, away_now = _current_score(state)
    full_game_margin = home_now - away_now
    if _is_garbage_time(cur_period, abs(full_game_margin)):
        blended_pps *= NBA_GARBAGE_PACE_FACTOR

    total_mean = period_total + blended_pps * remaining_s
    margin_mean = float(period_margin)  # remaining margin centered on 0

    return {
        "status":         regime,
        "total_mean":     total_mean,
        "total_std":      sigma_total,
        "margin_mean":    margin_mean,
        "margin_std":     sigma_margin,
        "remaining_s":    remaining_s,
        "period_total":   period_total,
        "period_margin": period_margin,
    }


# ── NHL calibration ──
#
# League averages from full-season ingest (rolling 3-season window
# 2023-2025). Empirical from nhl_games table: ~6.0 goals/game, ~2.0
# per period. Goals are Poisson-distributed within a period; the
# predictor uses closed-form Poisson PMF/CDF and a Skellam-based
# winner-prob derivation. No per-team xG model in stage 3 — that
# lives in Phase 4 alongside player-level shot-quality data.
#
# Per-period rate is the same across P1/P2/P3 in our dataset (the
# 1st-period dip + 3rd-period push largely cancel at slate level).
# When league data shifts we can split, but no signal yet.
NHL_PERIOD_SECONDS         = 20 * 60       # 1200s per regulation period
NHL_REGULATION_SECONDS     = 60 * 60       # 3600s
NHL_LEAGUE_AVG_TOTAL_GAME  = 6.00          # goals per game (both teams)
NHL_LEAGUE_AVG_TOTAL_PERIOD = NHL_LEAGUE_AVG_TOTAL_GAME / 3.0   # 2.00
NHL_LEAGUE_AVG_PER_TEAM_PERIOD = NHL_LEAGUE_AVG_TOTAL_PERIOD / 2.0  # 1.00


def _poisson_cdf(k: int, lam: float) -> float:
    """Return P(X <= k) for X ~ Poisson(lam). Computed by summing the
    PMF in float space — fine for the small λ values NHL produces
    (per-period λ caps at ~6 even in extreme garbage-time scenarios).
    """
    if lam <= 0:
        return 1.0 if k >= 0 else 0.0
    if k < 0:
        return 0.0
    s = 0.0
    term = math.exp(-lam)
    s += term
    for i in range(1, k + 1):
        term *= lam / i
        s += term
    return min(1.0, s)


def _poisson_sf(k: int, lam: float) -> float:
    """P(X > k) — survival function. Used for over-line probabilities.

    The over-line for "Over N.5 goals in period" is P(remaining > N -
    current_period_total - 0.5). Half-point lines avoid the integer
    push case, so this is just 1 - CDF(floor(line)).
    """
    return max(0.0, 1.0 - _poisson_cdf(k, lam))


def _skellam_home_win_prob(home_lam: float, away_lam: float,
                           cap: int = 12) -> tuple[float, float, float]:
    """Return (P_home_wins, P_tie, P_away_wins) for the Skellam
    distribution Z = X_home - X_away where X_i ~ Poisson(λ_i).

    Computed by summing the joint Poisson grid up to `cap` in each
    direction (covers >99.9% of probability for typical NHL period
    rates). Period winner / DNB markets settle on the Skellam tails.
    """
    if home_lam <= 0 and away_lam <= 0:
        return 0.0, 1.0, 0.0
    p_home = p_tie = p_away = 0.0
    # Pre-compute Poisson PMFs for both teams.
    def _pmf(lam: float) -> list[float]:
        if lam <= 0:
            return [1.0] + [0.0] * cap
        out = [0.0] * (cap + 1)
        out[0] = math.exp(-lam)
        for k in range(1, cap + 1):
            out[k] = out[k - 1] * lam / k
        return out
    h = _pmf(home_lam)
    a = _pmf(away_lam)
    for hi in range(cap + 1):
        for ai in range(cap + 1):
            p = h[hi] * a[ai]
            if hi > ai:
                p_home += p
            elif hi < ai:
                p_away += p
            else:
                p_tie += p
    return p_home, p_tie, p_away


def _period_endpoints_nhl(period_num: int) -> tuple[int, int]:
    """Return (start_period, end_period) for an NHL period market.
    NHL is single-period so start == end == period_num."""
    return (period_num, period_num)


def _nhl_period_score_so_far(state: dict, period_num: int) -> tuple[int, int, bool]:
    """Sum home/away goals scored in `period_num` from the linescore.
    Returns (home, away, period_closed)."""
    status = state.get("status") or {}
    cur_period = int(status.get("period") or 0)
    completed = bool(status.get("completed"))
    linescores = state.get("linescores") or {}
    home_ls = linescores.get("home") or []
    away_ls = linescores.get("away") or []
    idx = period_num - 1
    home = away = 0
    if 0 <= idx < len(home_ls):
        try:
            home = int(home_ls[idx])
        except (TypeError, ValueError):
            pass
    if 0 <= idx < len(away_ls):
        try:
            away = int(away_ls[idx])
        except (TypeError, ValueError):
            pass
    period_closed = completed or cur_period > period_num
    return home, away, period_closed


def predict_live_nhl_period(state: dict, period_num: int) -> dict:
    """Project the goal distribution for one NHL period (P1/P2/P3).

    Three regimes per period:
      - **closed**: period over; returns actual scores + status='closed'
      - **current**: clock is inside this period; project remaining
        time using observed pace blended with league average
      - **future**: period hasn't started; project full window using
        league average rate

    Result keys (when not closed)::

        status              "current" | "future"
        home_lambda         remaining-period λ for home team goals
        away_lambda         remaining-period λ for away team goals
        total_lambda        home_lambda + away_lambda (full remaining)
        home_period_total   actual home goals in this period so far
        away_period_total   actual away goals in this period so far
        period_total        sum of the two
        remaining_s         seconds left in the period
    """
    if period_num not in (1, 2, 3):
        return {}

    status = state.get("status") or {}
    if status.get("state") != "in":
        return {}

    cur_period = int(status.get("period") or 0)
    clock_secs = int(status.get("clock_secs") or 0)
    home_now, away_now, closed = _nhl_period_score_so_far(state, period_num)
    period_total = home_now + away_now

    if closed:
        return {
            "status": "closed",
            "home_period_total": home_now,
            "away_period_total": away_now,
            "period_total": period_total,
            "period_margin": home_now - away_now,
        }

    if cur_period < period_num:
        regime = "future"
        remaining_s = NHL_PERIOD_SECONDS
        elapsed_s = 0
    else:  # cur_period == period_num (closed already returned above)
        regime = "current"
        remaining_s = clock_secs
        elapsed_s = NHL_PERIOD_SECONDS - remaining_s

    elapsed_frac = _clip(elapsed_s / NHL_PERIOD_SECONDS, 0.0, 1.0)
    rem_frac = remaining_s / NHL_PERIOD_SECONDS

    # League-average per-team rate per second.
    league_per_team_rate = NHL_LEAGUE_AVG_PER_TEAM_PERIOD / NHL_PERIOD_SECONDS

    # Bayesian blending — conjugate Gamma prior, Poisson likelihood.
    # Goals are rare events (1/team/period league avg); a linear
    # interpolation like NBA uses produces wild extremes when 0 goals
    # are scored in the first 10 min of a period (observed_rate=0
    # forces λ_remaining=0 and yields fictional 100% confidence on
    # "no more goals" picks). Conjugate Gamma updates smooth this.
    #
    # Prior: λ_per_period_per_team ~ Gamma(α=1, β=1), mean=1.0
    # Posterior: Gamma(α + goals, β + elapsed_frac)
    # Posterior mean per period: (NHL_PRIOR_STRENGTH + goals) /
    #                            (NHL_PRIOR_STRENGTH + elapsed_frac)
    NHL_PRIOR_STRENGTH = 1.0   # Equivalent to 1 period of "league-avg" belief
    if regime == "current":
        post_home_per_period = (NHL_PRIOR_STRENGTH + home_now) / (
            NHL_PRIOR_STRENGTH + elapsed_frac)
        post_away_per_period = (NHL_PRIOR_STRENGTH + away_now) / (
            NHL_PRIOR_STRENGTH + elapsed_frac)
    else:
        post_home_per_period = NHL_LEAGUE_AVG_PER_TEAM_PERIOD
        post_away_per_period = NHL_LEAGUE_AVG_PER_TEAM_PERIOD

    home_lambda = post_home_per_period * rem_frac
    away_lambda = post_away_per_period * rem_frac
    total_lambda = home_lambda + away_lambda

    return {
        "status":             regime,
        "home_lambda":        home_lambda,
        "away_lambda":        away_lambda,
        "total_lambda":       total_lambda,
        "home_period_total":  home_now,
        "away_period_total":  away_now,
        "period_total":       period_total,
        "period_margin":      home_now - away_now,
        "remaining_s":        remaining_s,
        "elapsed_frac":       elapsed_frac,
    }


def nhl_period_total_over_prob(line: float, pred: dict) -> float:
    """P(period total > line). Caller passes the half-point line; we
    drop the 0.5 and ask Poisson for P(remaining > floor(line - cur))."""
    cur = pred.get("period_total", 0)
    lam = pred.get("total_lambda", 0.0)
    # Need remaining > line - cur. With half-point line, line - cur is
    # always non-integer, so floor it for Poisson SF.
    threshold = line - cur
    if threshold <= 0:
        # Line already cleared
        return 1.0
    # P(remaining > threshold) = P(remaining >= ceil(threshold))
    k_floor = int(math.floor(threshold))
    return _poisson_sf(k_floor, lam)


def nhl_period_bts_yes_prob(pred: dict) -> float:
    """P(both teams score at least 1 goal in this period).

    Treats home + away as independent Poissons (close enough for
    period-level signal — score-state correlations matter on the
    margins but the BTS market at our edge floor doesn't fire often).
    P(home >= 1) × P(away >= 1) accounts for the goals already scored.
    """
    home_now = pred.get("home_period_total", 0)
    away_now = pred.get("away_period_total", 0)
    h_lam = pred.get("home_lambda", 0.0)
    a_lam = pred.get("away_lambda", 0.0)
    p_home_scored = 1.0 if home_now >= 1 else _poisson_sf(0, h_lam)
    p_away_scored = 1.0 if away_now >= 1 else _poisson_sf(0, a_lam)
    return p_home_scored * p_away_scored


def nhl_period_winner_probs(pred: dict) -> tuple[float, float, float]:
    """Return (P_home_wins_period, P_tie, P_away_wins_period).

    Adds remaining-period goal distributions on top of the period
    margin already on the board. Computes via Skellam grid sum.
    """
    h_lam = pred.get("home_lambda", 0.0)
    a_lam = pred.get("away_lambda", 0.0)
    cur_h = pred.get("home_period_total", 0)
    cur_a = pred.get("away_period_total", 0)

    # P(home_remaining - away_remaining = d) for each integer d, then
    # shift by the current margin. Easier: enumerate joint final
    # period totals via the Poisson grids the Skellam helper builds.
    if h_lam <= 0 and a_lam <= 0:
        margin = cur_h - cur_a
        if margin > 0:
            return 1.0, 0.0, 0.0
        if margin < 0:
            return 0.0, 0.0, 1.0
        return 0.0, 1.0, 0.0

    cap = 12
    def _pmf(lam: float) -> list[float]:
        if lam <= 0:
            return [1.0] + [0.0] * cap
        out = [0.0] * (cap + 1)
        out[0] = math.exp(-lam)
        for k in range(1, cap + 1):
            out[k] = out[k - 1] * lam / k
        return out
    h_pmf = _pmf(h_lam)
    a_pmf = _pmf(a_lam)
    p_home = p_tie = p_away = 0.0
    for hi in range(cap + 1):
        for ai in range(cap + 1):
            p = h_pmf[hi] * a_pmf[ai]
            final_margin = (cur_h + hi) - (cur_a + ai)
            if final_margin > 0:
                p_home += p
            elif final_margin < 0:
                p_away += p
            else:
                p_tie += p
    return p_home, p_tie, p_away


def total_over_prob(line: float, total_mean: float, total_std: float) -> float:
    """P(final total > line). Half-point lines avoid push; integer
    lines have a small push mass we ignore (dwarfed by σ for any
    realistic NBA scoreline)."""
    if total_std <= 1e-6:
        return 1.0 if total_mean > line else 0.0
    return 1.0 - _norm_cdf((line - total_mean) / total_std)


def margin_cover_prob(line: float, margin_mean: float, margin_std: float) -> float:
    """P(home final margin > line). For live spread markets:
    - Home -3.5 favorite: line = +3.5 (home must beat by >3.5 → margin > +3.5)
    - Home +3.5 underdog: line = -3.5 (home margin must be > -3.5)
    Caller passes the *signed line as it applies to the home team*."""
    if margin_std <= 1e-6:
        return 1.0 if margin_mean > line else 0.0
    return 1.0 - _norm_cdf((line - margin_mean) / margin_std)
