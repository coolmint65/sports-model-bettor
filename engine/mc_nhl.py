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
from dataclasses import dataclass, field
from typing import Sequence

import numpy as np

from . import mc_constants as _mc

logger = logging.getLogger(__name__)


# ── League baselines ───────────────────────────────────────────
# Backed by engine.mc_constants. NHL's calibration only persists
# ``home_edge`` (goals) today; the others fall through to defaults
# defined in mc_constants. Wrapping the names as callables keeps
# downstream callers working when fitted values eventually land.
LEAGUE_GF_PER_GAME = _mc.nhl_gf_per_game
LEAGUE_GA_PER_GAME = _mc.nhl_ga_per_game
LEAGUE_PP_GOALS_PER_GAME = _mc.nhl_pp_goals_per_game
LEAGUE_SAVE_PCT = _mc.nhl_save_pct
LEAGUE_HOME_EDGE_GOALS = _mc.nhl_home_edge_goals
OT_MIN = 5
OT_3V3_GF_RATE = _mc.nhl_ot_3v3_gf_rate


@dataclass
class NHLTeamProfile:
    """Per-team sim inputs."""
    gf_per_game: float = field(default_factory=_mc.nhl_gf_per_game)
    ga_per_game: float = field(default_factory=_mc.nhl_ga_per_game)
    pp_goals_per_game: float = field(default_factory=_mc.nhl_pp_goals_per_game)
    save_pct: float = field(default_factory=_mc.nhl_save_pct)
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
    lg = _mc.nhl_gf_per_game()
    home_edge_goals = _mc.nhl_home_edge_goals()
    league_save_pct = _mc.nhl_save_pct()
    home_off = home.gf_per_game * home.offense_mult
    away_off = away.gf_per_game * away.offense_mult
    home_def = home.ga_per_game * home.defense_mult
    away_def = away.ga_per_game * away.defense_mult
    # Harmonic-mean-like blend that preserves league total when both teams
    # are at average (home_off * away_def / lg).
    home_xg = (home_off * away_def) / lg
    away_xg = (away_off * home_def) / lg
    # Home ice advantage (goals, split symmetrically)
    home_xg += home_edge_goals / 2
    away_xg -= home_edge_goals / 2
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
    # League save% ~0.905 -> ~0.095 save-miss rate. A 0.920 goalie has
    # 0.080 save-miss rate, so shots-become-goals is scaled by 0.080/0.095.
    home_xg *= (1.0 - away.save_pct) / (1.0 - league_save_pct)
    away_xg *= (1.0 - home.save_pct) / (1.0 - league_save_pct)
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
    ot_lambda = _mc.nhl_ot_3v3_gf_rate() * (OT_MIN / 60.0)
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

    # ── Derivative market aggregations ──
    def _team_total_dist(goals_arr) -> dict:
        mean_g = float(goals_arr.mean())
        anchor_g = round(mean_g * 2) / 2
        out = {}
        for off in (-1.5, -1, -0.5, 0, 0.5, 1, 1.5):
            line = anchor_g + off
            if line < 0.5:
                continue
            out[f"{line:.1f}"] = {
                "over": round(float((goals_arr > line).sum()) / n, 4),
                "under": round(float((goals_arr < line).sum()) / n, 4),
            }
        return out

    team_totals = {
        "home": {"expected": round(float(home.mean()), 3),
                 "lines": _team_total_dist(home)},
        "away": {"expected": round(float(away.mean()), 3),
                 "lines": _team_total_dist(away)},
    }

    # Per-period markets — Period Total, Period BTS, Period DNB.
    # home_p / away_p shape (N, 3) for P1/P2/P3 (regulation only).
    per_period: dict[str, dict] = {}
    for i in range(home_p.shape[1]):
        ph = home_p[:, i]
        pa = away_p[:, i]
        pt = ph + pa
        bts = (ph > 0) & (pa > 0)
        home_wp = ph > pa
        away_wp = pa > ph
        tied = ph == pa
        per_period[str(i + 1)] = {
            "expected_total": round(float(pt.mean()), 3),
            "over_0_5": round(float((pt > 0.5).sum()) / n, 4),
            "over_1_5": round(float((pt > 1.5).sum()) / n, 4),
            "over_2_5": round(float((pt > 2.5).sum()) / n, 4),
            "bts_yes": round(float(bts.sum()) / n, 4),
            "winner": {
                "home": round(float(home_wp.sum()) / n, 4),
                "away": round(float(away_wp.sum()) / n, 4),
                "tie":  round(float(tied.sum()) / n, 4),
            },
        }

    # Total Odd/Even — using FULL game totals (incl. OT goal if any).
    odd_count = int((totals.astype(int) % 2 == 1).sum())
    total_oe = {
        "odd":  round(odd_count / n, 4),
        "even": round((n - odd_count) / n, 4),
    }

    # Both teams to score full game — distinct from period BTS.
    full_bts = (home > 0) & (away > 0)
    bts_full = {
        "yes": round(float(full_bts.sum()) / n, 4),
        "no":  round(1.0 - float(full_bts.sum()) / n, 4),
    }

    # Overtime probability = regulation_tie_prob already computed.
    # Expose as a derivative-friendly key for symmetry with MLB.
    overtime = {
        "yes": round(float(raw["reg_ties"].sum()) / n, 4),
        "no":  round(1.0 - float(raw["reg_ties"].sum()) / n, 4),
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
        "team_totals": team_totals,
        "per_period": per_period,
        "total_oe": total_oe,
        "bts_full": bts_full,
        "overtime": overtime,
    }
