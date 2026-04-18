"""
MLB Monte Carlo simulator.

Simulates a game at-bat by at-bat across N independent runs, then
aggregates into market probabilities via engine.mc_sim.aggregate_mc_outcomes.

Design notes:
  - Outcome model per at-bat uses Bill James' log5 formula to blend
    per-batter and per-pitcher rates against the league average.
  - Handled outcomes: K, BB, HBP, HR, 3B, 2B, 1B, OUT (non-K out).
  - Batter-pitcher platoon via OPS-vs-L / OPS-vs-R -- scales contact
    outcomes (HR + base hits) without touching K/BB which are already
    platoon-adjusted by the batter's overall rates.
  - Pitcher transitions: starter pitches until a drawn inning ceiling
    (normal around the SP's IP/GS), then bullpen takes over with team
    average rates.
  - Lineup discipline: lineup index carries across half-innings within
    a sim (just like a real game).

The simulator does NOT call the database. Callers (engine.mc_mlb_run)
load hitter / pitcher / bullpen profiles from the DB and hand them in
as HitterProfile / PitcherProfile dataclasses. Keeping the simulator
pure makes it unit-testable with synthetic inputs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Sequence

import numpy as np

from .mc_sim import log5

logger = logging.getLogger(__name__)


# ── League baselines (2024-2025 MLB) ───────────────────────────
LEAGUE_K_PCT  = 0.225
LEAGUE_BB_PCT = 0.085
LEAGUE_HBP_PCT = 0.011
LEAGUE_HR_RATE = 0.034   # HR per PA
LEAGUE_BABIP   = 0.295
LEAGUE_ISO     = 0.165   # slg - avg; measures extra-base power

# Distribution of extra-base hits given contact + ISO.
# These are conditional on "non-K, non-BB, non-HBP, non-HR hit" (i.e. a
# base hit on contact). Based on 2024 league splits.
_HIT_SHARES_BASELINE = {
    "1B": 0.66,
    "2B": 0.25,
    "3B": 0.02,
    # HR is handled separately via HR rate
}


# ── Outcome enum ───────────────────────────────────────────────
OUTCOMES = ("K", "BB", "HBP", "HR", "3B", "2B", "1B", "OUT")
_OUT_INDEX = {o: i for i, o in enumerate(OUTCOMES)}


# ── Hitter / pitcher profiles ──────────────────────────────────
@dataclass
class HitterProfile:
    """Per-batter inputs to the sim.

    Rates are all PA-based. If a batter's vs-L / vs-R OPS are unknown
    we use `None` and the sim falls back to the overall rates.
    """
    k_pct: float = LEAGUE_K_PCT
    bb_pct: float = LEAGUE_BB_PCT
    iso: float = LEAGUE_ISO
    babip: float = LEAGUE_BABIP
    ops_vs_l: float | None = None
    ops_vs_r: float | None = None
    ops: float = 0.720
    handedness: str = "R"   # "L", "R", "S"
    name: str = "?"


@dataclass
class PitcherProfile:
    """Per-pitcher inputs to the sim."""
    k_pct: float = LEAGUE_K_PCT
    bb_pct: float = LEAGUE_BB_PCT
    hr_per_pa: float = LEAGUE_HR_RATE
    babip_against: float = LEAGUE_BABIP
    handedness: str = "R"
    expected_innings: float = 5.5   # SP stamina anchor
    name: str = "?"


@dataclass
class TeamProfile:
    """Container for a team's lineup + SP + bullpen + handedness-of-SP.

    The sim asks the TeamProfile for "the pitcher on the mound at inning I"
    so the starter-to-bullpen transition can be drawn per-sim.
    """
    lineup: Sequence[HitterProfile]
    starter: PitcherProfile
    bullpen: PitcherProfile
    park_hr_factor: float = 1.0   # >1 means park inflates HR


# ── Outcome-probability computation ─────────────────────────────
def outcome_probs(batter: HitterProfile, pitcher: PitcherProfile,
                   park_hr_factor: float) -> np.ndarray:
    """Produce an 8-vector of probabilities over OUTCOMES for one at-bat.

    Uses log5 to blend batter + pitcher rates against league averages,
    applies a platoon multiplier via the batter's OPS splits, then
    splits residual contact into hit categories using batter ISO.
    """
    # K / BB / HBP / HR from log5
    k  = log5(batter.k_pct, pitcher.k_pct, LEAGUE_K_PCT)
    bb = log5(batter.bb_pct, pitcher.bb_pct, LEAGUE_BB_PCT)
    hbp = LEAGUE_HBP_PCT   # no meaningful split data; use league rate
    # Batter HR-per-PA correlates with ISO at ~0.20:1 in MLB data
    # (league avg ISO 0.165 -> ~0.033 HR/PA). Anything else over-predicts
    # home runs -- earlier version used iso*0.6 + 0.03 which put the
    # league-average hitter at 10% HR per PA vs. real 3.4%.
    batter_hr_rate = max(0.003, min(0.10, batter.iso * 0.20))
    hr = log5(batter_hr_rate, pitcher.hr_per_pa, LEAGUE_HR_RATE) * park_hr_factor
    # platoon: scale HR and base hits by OPS-vs-handedness ratio
    platoon_mult = _platoon_multiplier(batter, pitcher)
    hr *= platoon_mult

    # Keep rates sane after scaling
    k = max(0.02, min(0.55, k))
    bb = max(0.01, min(0.25, bb))
    hr = max(0.002, min(0.09, hr))
    hbp = max(0.0, min(0.03, hbp))

    non_event = max(0.01, 1.0 - k - bb - hbp - hr)

    # On contact (no K/BB/HBP/HR), split into base hits vs. out via BABIP.
    # BABIP applies only to balls in play -- scale it by the batter-pitcher
    # blended rate so good hitters against weak pitchers land more hits.
    babip_blend = log5(batter.babip, pitcher.babip_against, LEAGUE_BABIP)
    babip_blend *= platoon_mult
    babip_blend = max(0.15, min(0.45, babip_blend))

    hit_prob = non_event * babip_blend
    out_prob = non_event - hit_prob

    # Split hits into 1B / 2B / 3B using batter ISO to scale the extra-base share
    iso_gap = batter.iso - LEAGUE_ISO
    xbh_share = min(0.55, max(0.20, 0.34 + iso_gap * 0.5))
    single_share = 1.0 - xbh_share
    # Within extra-base hits: 90% 2B, 10% 3B (league is roughly 12:1)
    double_share = xbh_share * 0.92
    triple_share = xbh_share * 0.08

    h1b = hit_prob * single_share
    h2b = hit_prob * double_share
    h3b = hit_prob * triple_share

    probs = np.array([k, bb, hbp, hr, h3b, h2b, h1b, out_prob], dtype=float)
    # Renormalize to exactly 1.0 -- small numerical drift is expected.
    s = probs.sum()
    if s > 0:
        probs /= s
    return probs


def _platoon_multiplier(batter: HitterProfile, pitcher: PitcherProfile) -> float:
    """Scale contact outcomes by the batter's OPS-vs-handedness advantage.

    Returns 1.0 when we have no platoon data or when the two sides cancel.
    Clamped to [0.80, 1.25] so a single extreme split doesn't dominate.
    """
    if pitcher.handedness not in ("L", "R"):
        return 1.0
    split_ops = batter.ops_vs_l if pitcher.handedness == "L" else batter.ops_vs_r
    base_ops = batter.ops
    if not split_ops or not base_ops:
        return 1.0
    ratio = split_ops / base_ops
    return float(max(0.80, min(1.25, ratio)))


# ── Sim kernel ──────────────────────────────────────────────────

_BASES_EMPTY = (False, False, False)  # (1B, 2B, 3B)


def _advance_runners(outcome: str, bases: list[bool]) -> tuple[int, list[bool]]:
    """Resolve a single at-bat's effect on runners. Returns (runs, new_bases).

    Conservative run-advancement model:
      K, OUT  -> no advance
      BB, HBP -> force: batter to 1B, runners advance iff forced
      1B      -> 3B runner scores; 2B runner scores 50% else 3B; 1B to 2B; batter to 1B
      2B      -> 2B and 3B score; 1B to 3B; batter to 2B
      3B      -> all score; batter to 3B
      HR      -> all score incl batter
    """
    r1, r2, r3 = bases
    runs = 0
    if outcome == "HR":
        runs = 1 + int(r1) + int(r2) + int(r3)
        return runs, [False, False, False]
    if outcome == "3B":
        runs = int(r1) + int(r2) + int(r3)
        return runs, [False, False, True]
    if outcome == "2B":
        runs = int(r2) + int(r3)
        new_r3 = r1
        return runs, [False, True, new_r3]
    if outcome == "1B":
        if r3:
            runs += 1
        # 2B runner scores 50% of the time on a single, else to 3B
        send_2b_home = bool(np.random.random() < 0.5)
        new_r3 = (r2 and not send_2b_home) or False
        if r2 and send_2b_home:
            runs += 1
        new_r2 = r1
        return runs, [True, new_r2, new_r3]
    if outcome in ("BB", "HBP"):
        # Force only the runners directly forced
        if r1 and r2 and r3:
            runs += 1
            return runs, [True, True, True]
        if r1 and r2:
            return runs, [True, True, True]
        if r1:
            return runs, [True, True, r3]
        return runs, [True, r2, r3]
    # K / OUT: no advancement (simplified)
    return 0, bases


def _sim_half_inning(lineup_len: int,
                     lineup_idx: int,
                     pitcher_role: int,
                     prob_table: np.ndarray,
                     cum_table: np.ndarray,
                     outs_per_outcome: np.ndarray,
                     rng: np.random.Generator) -> tuple[int, int]:
    """Simulate one half-inning against a precomputed prob table.

    prob_table[batter_idx, pitcher_role] is the 8-vector of outcome
    probabilities for that matchup; cum_table is its cumulative sum
    along the outcome axis so a single rng.random() + np.searchsorted
    replaces rng.choice (which has per-call p-array overhead).

    Returns (runs, new_lineup_idx).
    """
    outs = 0
    bases = [False, False, False]
    runs = 0
    while outs < 3:
        idx_in_lineup = lineup_idx % lineup_len
        cum = cum_table[idx_in_lineup, pitcher_role]
        u = rng.random()
        idx = int(np.searchsorted(cum, u))
        if idx >= 8:
            idx = 7
        if outs_per_outcome[idx]:  # K or OUT
            outs += 1
        else:
            outcome = OUTCOMES[idx]
            got_runs, bases = _advance_runners(outcome, bases)
            runs += got_runs
        lineup_idx += 1
    return runs, lineup_idx


def _precompute_prob_table(lineup: Sequence[HitterProfile],
                            starter: PitcherProfile,
                            bullpen: PitcherProfile,
                            park_hr_factor: float) -> tuple[np.ndarray, np.ndarray]:
    """Build (n_batters, 2, 8) probability + cumulative tables, indexed by
    pitcher role (0=starter, 1=bullpen). Called twice per game (one per
    side) -- eliminates the 4.25M per-PA recompute that was the bottleneck
    at 50k sims (~97s/game before this change)."""
    n = len(lineup)
    probs = np.zeros((n, 2, 8), dtype=np.float64)
    pitchers = (starter, bullpen)
    for i, batter in enumerate(lineup):
        for r, pitcher in enumerate(pitchers):
            probs[i, r] = outcome_probs(batter, pitcher, park_hr_factor)
    cum = np.cumsum(probs, axis=-1)
    return probs, cum


def _draw_sp_innings_ceiling(pitcher: PitcherProfile, rng: np.random.Generator) -> int:
    """Draw the inning after which the starter is pulled.

    Mean = pitcher.expected_innings, stddev = 1.0. Clamped to [3, 9].
    Returns an int inning index (0-8) so the comparison becomes:
        if inning_0based <= ceiling: use SP, else bullpen.
    """
    mean = max(2.0, min(8.0, pitcher.expected_innings))
    draw = rng.normal(mean, 1.0)
    pulled_after_inning = int(max(3, min(9, round(draw))))
    return pulled_after_inning - 1   # 0-based: 5th inning -> index 4


# ── Public API ──────────────────────────────────────────────────
def simulate_games(home: TeamProfile, away: TeamProfile,
                    n_sims: int = 50_000,
                    seed: int | None = None) -> dict:
    """Run N Monte Carlo simulations of a game between home and away.

    Returns a dict containing:
        home_runs     -- (n_sims,) array of final totals
        away_runs     -- (n_sims,) array of final totals
        home_inning_runs -- (n_sims, 9) array, home runs per inning
        away_inning_runs -- (n_sims, 9) array

    This is the raw output. Use engine.mc_sim.aggregate_mc_outcomes to
    collapse into market probabilities.
    """
    rng = np.random.default_rng(seed)
    home_runs_final = np.zeros(n_sims, dtype=np.int16)
    away_runs_final = np.zeros(n_sims, dtype=np.int16)
    home_inn = np.zeros((n_sims, 9), dtype=np.int16)
    away_inn = np.zeros((n_sims, 9), dtype=np.int16)

    # Precompute the batter × pitcher-role outcome tables once per game.
    # Inside the hot loop we only do an array lookup + searchsorted sample
    # instead of rebuilding log5 / platoon math for every plate appearance.
    away_probs, away_cum = _precompute_prob_table(
        away.lineup, home.starter, home.bullpen, home.park_hr_factor,
    )
    home_probs, home_cum = _precompute_prob_table(
        home.lineup, away.starter, away.bullpen, home.park_hr_factor,
    )
    # 1 where the outcome increments `outs` (K or OUT), 0 otherwise.
    outs_per_outcome = np.array(
        [1 if o in ("K", "OUT") else 0 for o in OUTCOMES], dtype=np.int8,
    )
    home_lineup_len = len(home.lineup)
    away_lineup_len = len(away.lineup)

    for s in range(n_sims):
        home_idx = 0
        away_idx = 0
        home_sp_ceiling = _draw_sp_innings_ceiling(home.starter, rng)
        away_sp_ceiling = _draw_sp_innings_ceiling(away.starter, rng)
        game_home = 0
        game_away = 0

        for inning in range(9):
            # Top of inning: away bats, home pitches
            home_role = 0 if inning <= home_sp_ceiling else 1
            runs_top, away_idx = _sim_half_inning(
                away_lineup_len, away_idx, home_role,
                away_probs, away_cum, outs_per_outcome, rng,
            )
            away_inn[s, inning] = runs_top
            game_away += runs_top

            # Walk-off: skip bottom of 9th if home is leading
            if inning == 8 and game_home > game_away:
                break

            # Bottom: home bats, away pitches
            away_role = 0 if inning <= away_sp_ceiling else 1
            runs_bot, home_idx = _sim_half_inning(
                home_lineup_len, home_idx, away_role,
                home_probs, home_cum, outs_per_outcome, rng,
            )
            home_inn[s, inning] = runs_bot
            game_home += runs_bot

            # Home wins with walk-off; skip rest (no real extras handling yet)
            if inning == 8 and game_home > game_away:
                break

        home_runs_final[s] = game_home
        away_runs_final[s] = game_away

    return {
        "home_runs": home_runs_final,
        "away_runs": away_runs_final,
        "home_inning_runs": home_inn,
        "away_inning_runs": away_inn,
    }
