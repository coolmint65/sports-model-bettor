"""
NHL Monte Carlo simulator.

NHL games don't have at-bats; the natural sim unit is a shift (~45s
of 5v5 hockey, 60-70 shifts per team per game). Each shift produces
a Poisson-distributed number of shots; each shot has a save-rate-dependent
probability of becoming a goal.

Design notes:
  - Per-team shots-for rate scales with offensive quality; shots-against
    rate scales with defensive quality. We use the team's adjusted
    goals-for / goals-against averages as the signal.
  - Goalies matter a lot in hockey -- the simulator takes a goalie
    save percentage per side and uses it to convert shots to goals.
    Backup goalies dragging down team stats is baked into the team
    GA signal, but the confirmed starter's save% overrides for
    tonight's game when available.
  - Power plays are modeled as rate bumps: home_pp_goals_per_game and
    away_pp_goals_per_game add directly to the Poisson lambda.
  - Regulation ends at 60 min; if tied, a sudden-death 5-min OT with
    3v3 scoring rates and a 50/50 shootout coin flip if still tied.

Outputs match MLB shape so engine.mc_sim.aggregate_mc_outcomes produces
compatible market probability dicts (swap per-inning for per-period).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Sequence

import numpy as np

logger = logging.getLogger(__name__)


# ── League baselines (2023-24 + 2024-25 NHL) ───────────────────
LEAGUE_GF_PER_GAME = 3.10          # goals FOR per team per game
LEAGUE_GA_PER_GAME = 3.10          # equal at league level
LEAGUE_PP_GOALS_PER_GAME = 0.65    # PP goals added per team per game
LEAGUE_SAVE_PCT = 0.905            # NHL save% regressed to mean
LEAGUE_HOME_EDGE_GOALS = 0.20      # ~0.2 goals/game home advantage
OT_MIN = 5
OT_3V3_GF_RATE = 2.0               # 3v3 OT is more wide open


@dataclass
class NHLTeamProfile:
    """Per-team sim inputs."""
    gf_per_game: float = LEAGUE_GF_PER_GAME
    ga_per_game: float = LEAGUE_GA_PER_GAME
    pp_goals_per_game: float = LEAGUE_PP_GOALS_PER_GAME
    save_pct: float = LEAGUE_SAVE_PCT
    # Optional adjustments already applied upstream (injury / travel / rest)
    offense_mult: float = 1.0
    defense_mult: float = 1.0
    name: str = "?"


def expected_goals(home: NHLTeamProfile, away: NHLTeamProfile,
                    is_playoff: bool = False) -> tuple[float, float]:
    """Combine the two teams' offense and defense into per-team xG.

    The standard sabermetric blend: home_xG = mean(home_off, away_def)
    normalized so that two average teams produce LEAGUE_GF_PER_GAME.
    """
    lg = LEAGUE_GF_PER_GAME
    home_off = home.gf_per_game * home.offense_mult
    away_off = away.gf_per_game * away.offense_mult
    home_def = home.ga_per_game * home.defense_mult
    away_def = away.ga_per_game * away.defense_mult
    # Harmonic-mean-like blend that preserves league total when both teams
    # are at average (home_off * away_def / lg).
    home_xg = (home_off * away_def) / lg
    away_xg = (away_off * home_def) / lg
    # Home ice advantage (goals, split symmetrically)
    home_xg += LEAGUE_HOME_EDGE_GOALS / 2
    away_xg -= LEAGUE_HOME_EDGE_GOALS / 2
    # Playoff scoring shrinkage -- matches engine.nhl_predict.
    # NHL_PLAYOFF_SCORING_FACTOR (0.93) applied symmetrically.
    if is_playoff:
        home_xg *= 0.93
        away_xg *= 0.93
    # Special-teams signal: PP goals are ALREADY baked into gf_per_game.
    # We don't re-add them; instead we use the PP/PK gap between the
    # two teams as a small differential nudge. A team with +0.3 PP
    # goals/game above the opponent gets +10% of that nudge.
    pp_gap = (home.pp_goals_per_game - away.pp_goals_per_game) * 0.1
    home_xg += pp_gap
    away_xg -= pp_gap
    # Apply the opposing goalie's save-pct differential as a multiplier.
    # LEAGUE save% of 0.905 -> 0.095 save-miss rate. A 0.920 goalie has
    # 0.080 save-miss rate, so shots-become-goals is scaled by 0.080/0.095.
    home_xg *= (1.0 - away.save_pct) / (1.0 - LEAGUE_SAVE_PCT)
    away_xg *= (1.0 - home.save_pct) / (1.0 - LEAGUE_SAVE_PCT)
    return max(0.5, home_xg), max(0.5, away_xg)


def simulate_games(home: NHLTeamProfile, away: NHLTeamProfile,
                    n_sims: int = 50_000,
                    is_playoff: bool = False,
                    seed: int | None = None) -> dict:
    """Run N Poisson-by-period simulations.

    Per-period goals are drawn from Poisson(xG / 3). Regulation ties
    go to a 3v3 OT (one Poisson draw per team at the higher OT rate);
    if still tied, a 50/50 coin flip simulates the shootout. Shootout
    goals are attributed to the winner's last period (standard NHL
    record-keeping style) but totals always advance by 1.
    """
    rng = np.random.default_rng(seed)
    home_xg, away_xg = expected_goals(home, away, is_playoff=is_playoff)

    # Vectorize per-period draws across all sims at once.
    home_per_period = rng.poisson(home_xg / 3, size=(n_sims, 3)).astype(np.int16)
    away_per_period = rng.poisson(away_xg / 3, size=(n_sims, 3)).astype(np.int16)

    home_reg = home_per_period.sum(axis=1)
    away_reg = away_per_period.sum(axis=1)
    reg_tie = home_reg == away_reg

    # OT: scaled Poisson, 5/60 of an hour at 3v3 rate
    ot_lambda = OT_3V3_GF_RATE * (OT_MIN / 60.0)
    ot_home = rng.poisson(ot_lambda, size=n_sims).astype(np.int16)
    ot_away = rng.poisson(ot_lambda, size=n_sims).astype(np.int16)
    # Sudden death: the first team to score more wins. Simplification:
    # compare totals; if equal, go to shootout.
    ot_home_mask = reg_tie & (ot_home > ot_away)
    ot_away_mask = reg_tie & (ot_away > ot_home)
    ot_still_tied = reg_tie & (ot_home == ot_away)

    # Shootout: 50/50 coin flip among the tied sims
    so_home_win = rng.random(n_sims) < 0.5

    final_home = home_reg.copy()
    final_away = away_reg.copy()
    final_home[ot_home_mask] += 1   # OT win awards 1 goal
    final_away[ot_away_mask] += 1
    # Shootout: credit the winner with a goal in the record.
    so_winners_home = ot_still_tied & so_home_win
    so_winners_away = ot_still_tied & ~so_home_win
    final_home[so_winners_home] += 1
    final_away[so_winners_away] += 1

    # Build a 3-period array for downstream aggregation (P1, P2, P3).
    # OT/SO goals are folded into period 3 so totals reconcile.
    # Splitting OT/SO out requires a separate field -- keep simple for now.
    home_per_period = home_per_period.copy()
    away_per_period = away_per_period.copy()
    home_per_period[:, 2] += (final_home - home_reg).astype(np.int16)
    away_per_period[:, 2] += (final_away - away_reg).astype(np.int16)

    return {
        "home_goals": final_home,
        "away_goals": final_away,
        "home_period_goals": home_per_period,
        "away_period_goals": away_per_period,
        "reg_ties": reg_tie,
    }


def aggregate_nhl(raw: dict) -> dict:
    """NHL-specific aggregation -- shares most of its shape with MLB
    but uses 3 periods instead of 9 innings and adds regulation-tie
    probability and first-period markets.
    """
    home = raw["home_goals"]
    away = raw["away_goals"]
    home_p = raw["home_period_goals"]
    away_p = raw["away_period_goals"]
    n = len(home)

    home_wins = home > away
    away_wins = away > home

    totals = home + away
    margins = home.astype(int) - away.astype(int)

    mean_total = float(totals.mean())
    anchor = round(mean_total * 2) / 2
    ou: dict = {}
    for offset in (-2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2):
        line = anchor + offset
        if line < 0.5:
            continue
        over = float((totals > line).sum()) / n
        under = float((totals < line).sum()) / n
        ou[f"{line:.1f}"] = {
            "over": round(over, 4),
            "under": round(under, 4),
            "push": round(1.0 - over - under, 4),
        }

    # Puck line (typically +/- 1.5)
    pl: dict = {}
    for point in (-2.5, -1.5, -0.5, 0.5, 1.5, 2.5):
        home_cov = float((margins > -point).sum()) / n
        pl[f"home_{point}"] = round(home_cov, 4)

    # Correct scores
    score_counts: dict[tuple[int, int], int] = {}
    for h, a in zip(home.astype(int), away.astype(int)):
        key = (int(h), int(a))
        score_counts[key] = score_counts.get(key, 0) + 1
    top_scores = sorted(score_counts.items(), key=lambda kv: -kv[1])[:10]
    correct_scores = [
        {"home": h, "away": a, "prob": round(c / n, 4)}
        for (h, a), c in top_scores
    ]

    # 1st period markets: regulation-style P1 totals, home P1 wins
    p1_home = home_p[:, 0]
    p1_away = away_p[:, 0]
    p1_total = p1_home + p1_away
    p1 = {
        "expected_total": round(float(p1_total.mean()), 3),
        "over_0_5": round(float((p1_total > 0.5).sum()) / n, 4),
        "over_1_5": round(float((p1_total > 1.5).sum()) / n, 4),
        "over_2_5": round(float((p1_total > 2.5).sum()) / n, 4),
        "home_win": round(float((p1_home > p1_away).sum()) / n, 4),
        "away_win": round(float((p1_away > p1_home).sum()) / n, 4),
    }

    return {
        "n_sims": n,
        "win_prob": {
            "home": round(float(home_wins.sum()) / n, 4),
            "away": round(float(away_wins.sum()) / n, 4),
        },
        "expected_goals": {
            "home": round(float(home.mean()), 3),
            "away": round(float(away.mean()), 3),
            "total": round(mean_total, 3),
        },
        "margin_std": round(float(margins.std()), 3),
        "total_std": round(float(totals.std()), 3),
        "regulation_tie_prob": round(float(raw["reg_ties"].sum()) / n, 4),
        "over_under": ou,
        "puck_line": pl,
        "correct_scores": correct_scores,
        "first_period": p1,
    }
