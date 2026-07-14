"""
NBA intermission predictor (Phase 5i + 5j).

Fires at end of any quarter (5j) or halftime (5i, the headline case).
Reads observed score + PBP, blends with the prematch full-game
projection, and emits markets for the remaining quarters / halves /
F48.

MVP shrinkage approach
----------------------
Rather than re-run the full factor model with adjusted features, we
blend prematch projections with observed-so-far via a shrinkage
factor. The intuition: a team that's gone 60-50 in H1 isn't going to
go 60-50 again in H2 just because they did the first time, but they
ARE more likely to outperform their prematch projection in H2 than
to revert all the way back.

For each team:

    expected_so_far  = full_predicted_score * (elapsed / 48)
    actual_so_far    = state.score
    multiplier       = 1 + (actual / expected - 1) * shrink_weight
    remaining_points = full_predicted_score * (remaining / 48) * multiplier
    final_score      = actual_so_far + remaining_points

``shrink_weight`` rises with elapsed time — a team's H1 carries more
signal than their Q1, so halftime predictions get a heavier weight
on observed than quarter-break predictions do.

What's not in MVP
-----------------
Per-player lineup state and foul trouble are NOT factored in (those
require 5c, deferred). Without them, the predictor doesn't know that
the leading team's star is sitting on 4 fouls and likely won't see
the floor for the first 6 min of Q3 — so it'll over-project that
team's H2 scoring slightly. Effect on totals: minor (the back-up
takes the minutes). Effect on tight late-game spread/ML: real but
the volume of late-game spread bets we'd take at intermission is
low enough that the MVP error is acceptable.
"""

from __future__ import annotations

import logging
import math
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


# Calibration: how much of the observed/expected gap to carry forward.
# Halftime gets 0.50 (24 minutes elapsed = bigger sample = trust more).
# Quarter breaks get 0.30. Buckets are intentionally simple — this is
# MVP, room for per-team-per-stat refinement later.
_SHRINK_WEIGHT = {
    1: 0.20,  # End of Q1
    2: 0.50,  # Halftime
    3: 0.40,  # End of Q3
}

# Default elapsed minutes per quarter (NBA). Sport-specific values
# come from _SPORT_CONSTANTS below — kept at module level for back-
# compat with callers that still import _QUARTER_MIN.
_QUARTER_MIN = 12.0
_GAME_MIN = 48.0


# Per-sport timing + variance constants. WNBA is the 4×10-min cousin
# of NBA's 4×12, so the analytical layer reuses the same shrinkage +
# Gaussian per-quarter logic — only the constants change. Per-quarter
# team sigma is fitted from prematch sigma shrunk for elapsed time.
#
# WNBA team_sigma_full sized smaller than NBA's 13.0 because WNBA
# games run ~40 min vs 48, and league-wide score variance is tighter
# (per the basketball framework calibration). 11.0 ≈ 13.0 × sqrt(40/48).
_SPORT_CONSTANTS = {
    "nba":   {"quarter_min": 12.0, "game_min": 48.0, "team_sigma_full": 13.0},
    "wnba":  {"quarter_min": 10.0, "game_min": 40.0, "team_sigma_full": 11.0},
    # NCAAM = 2 × 20-min halves. "quarter_min" is the period length;
    # team_sigma_full fitted 2026-05-04 from 6125 games (calibrate
    # cycle), stored in basketball/_config.py.margin_std ÷ √2 (team
    # variance is half of total-margin variance under independence).
    "ncaam": {"quarter_min": 20.0, "game_min": 40.0, "team_sigma_full": 12.7},
    # AFL = 4 × 20-min quarters. team_sigma_full = 25.9 (home-score
    # stddev fitted from 1371 games 2026-05-15). Quarters_min counts
    # active-clock minutes; AFL also has stoppage time but the live
    # state at period-end is what matters here.
    "afl":   {"quarter_min": 20.0, "game_min": 80.0, "team_sigma_full": 25.9},
}

def _ensemble_blend(values: list) -> float | None:
    """Average non-None layer outputs. Returns None when every layer
    is None (degenerates to analytical-only on the caller's side)."""
    valid = [v for v in values if v is not None]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _gbm_layer_nba(period_ended: int, home_score: int, away_score: int,
                    pbp_summary: dict | None, prematch: dict | None,
                    sport: str = "nba") -> dict | None:
    """Call engine.gbm.predict.predict_live with a state dict. Returns
    {home_win_prob, total_mean, margin_mean} or None when artifacts
    aren't available.

    Sport-aware artifact lookup: NBA loads ``nba_live_gbm_*``, WNBA
    loads ``wnba_live_gbm_*``. The state dict + feature shape is
    identical (same ESPN PBP, same 4×Q structure)."""
    try:
        from ..gbm.predict import predict_live, is_available
        artifact_sport = f"{sport}_live"
        if not is_available(artifact_sport):
            return None
        # Build state dict matching what features_live expects.
        # PBP summary uses 'home'/'away' subdicts when present; flatten.
        h_stats = (pbp_summary or {}).get("home", {})
        a_stats = (pbp_summary or {}).get("away", {})
        state = {
            "period_ended": period_ended,
            "home_score_so_far": home_score,
            "away_score_so_far": away_score,
            "made_fg_home": h_stats.get("made_fg") or 0,
            "missed_fg_home": h_stats.get("missed_fg") or 0,
            "made_fg_away": a_stats.get("made_fg") or 0,
            "missed_fg_away": a_stats.get("missed_fg") or 0,
            "fouls_home": h_stats.get("fouls") or 0,
            "fouls_away": a_stats.get("fouls") or 0,
            "turnovers_home": h_stats.get("turnovers") or 0,
            "turnovers_away": a_stats.get("turnovers") or 0,
        }
        gbm = predict_live(sport, state)
        if not gbm or "home_final_win" not in gbm:
            return None
        win = gbm.get("home_final_win")
        total = gbm.get("final_total_points")
        margin = gbm.get("final_margin")
        if isinstance(win, dict) or isinstance(total, dict):
            return None
        return {
            "home_win_prob": float(win),
            "total_mean":    float(total) if total is not None else None,
            "margin_mean":   float(margin) if margin is not None else None,
        }
    except Exception as e:
        logger.debug("GBM layer NBA failed: %s", e)
        return None


def _state_mc_layer(sport: str, period_ended: int, margin_so_far: int,
                     home_score: int, away_score: int) -> dict | None:
    """Sport-agnostic state-MC layer. Loads the empirical sampler for
    ``sport`` and returns {home_win_prob, total_mean}. WNBA uses the
    same NBA-shaped grid (quarter splits drive both fits)."""
    try:
        from ._state_mc import sample_remaining
        samples = sample_remaining(sport, period_ended, margin_so_far,
                                     n=10_000)
        if not samples:
            return None
        import numpy as _np
        arr = _np.array(samples)
        if arr.size == 0:
            return None
        final_home = home_score + arr[:, 0]
        final_away = away_score + arr[:, 1]
        return {
            "home_win_prob": float((final_home > final_away).mean()),
            "total_mean":    float((final_home + final_away).mean()),
        }
    except Exception as e:
        logger.debug("state_mc layer %s failed: %s", sport, e)
        return None


def _state_mc_layer_nba(period_ended: int, margin_so_far: int,
                         home_score: int, away_score: int) -> dict | None:
    """Pull empirical samples from state_mc and compute home_win_prob
    + total_mean."""
    try:
        from ._state_mc import sample_remaining
        samples = sample_remaining("nba", period_ended, margin_so_far,
                                     n=5000)
        if not samples or len(samples) < 50:
            return None
        import numpy as np
        h_rem = np.array([s[0] for s in samples], dtype=float)
        a_rem = np.array([s[1] for s in samples], dtype=float)
        final_h = home_score + h_rem
        final_a = away_score + a_rem
        final_total = final_h + final_a
        final_margin = final_h - final_a
        return {
            "home_win_prob": float((final_margin > 0).mean()),
            "total_mean":    float(final_total.mean()),
            "margin_mean":   float(final_margin.mean()),
        }
    except Exception as e:
        logger.debug("state_mc layer NBA failed: %s", e)
        return None


# Per-player-in-foul-trouble penalty applied to a team's remaining
# offense. A starter at 4 fouls leaving H1 typically loses 4-6
# minutes early in Q3 to bench coverage. With 5 starters splitting
# scoring, that's roughly a 4% drag on team output for the half.
# Conservative because foul trouble is heuristic — sometimes a coach
# rides the limit, sometimes they pull immediately.
_FOUL_TROUBLE_DRAG_PER_PLAYER = 0.04


# ── PBP feature extraction ────────────────────────────────────

_FOUL_TYPES = {"shooting foul", "personal foul", "offensive foul",
                "loose ball foul", "technical foul"}
_TURNOVER_TYPES = {"bad pass turnover", "lost ball turnover",
                    "out of bounds - bad pass turnover",
                    "offensive foul turnover", "traveling",
                    "double dribble", "3-second violation",
                    "5-second violation", "8-second violation",
                    "shot clock turnover", "kicked ball violation"}


def summarize_pbp(plays: list[dict],
                  home_team_id: str | None,
                  away_team_id: str | None,
                  through_period: int) -> dict:
    """Aggregate per-team stats through ``through_period``. Used to
    compute observed pace + foul state at intermission.

    Returns ``{home: {fouls, turnovers, made_fg, missed_fg},
                away: {...},
                periods_complete: int}``"""
    bucket_template = {
        "fouls": 0, "turnovers": 0, "made_fg": 0, "missed_fg": 0,
    }
    home = dict(bucket_template)
    away = dict(bucket_template)
    periods_complete = 0
    for p in plays or []:
        try:
            period = int(p.get("period") or 0)
        except (TypeError, ValueError):
            period = 0
        if period <= 0 or period > through_period:
            continue
        type_text = (p.get("type_text") or "").strip().lower()
        team_id = str(p.get("team_id") or "")
        if type_text == "end period":
            periods_complete = max(periods_complete, period)
            continue
        if not team_id:
            continue
        if team_id == str(home_team_id):
            bucket = home
        elif team_id == str(away_team_id):
            bucket = away
        else:
            continue
        if type_text in _FOUL_TYPES:
            bucket["fouls"] += 1
        elif type_text in _TURNOVER_TYPES:
            bucket["turnovers"] += 1
        elif p.get("shooting_play"):
            if p.get("scoring_play"):
                bucket["made_fg"] += 1
            else:
                bucket["missed_fg"] += 1
    return {
        "home": home,
        "away": away,
        "periods_complete": periods_complete,
    }


# ── Predictor ─────────────────────────────────────────────────

def predict_intermission(sport: str, game_id: str, period_ended: int,
                          n_sims: int = 30_000) -> dict | None:
    """Project final-game and remaining-period markets at an
    intermission boundary. Supports NBA (full ensemble: analytical +
    GBM + state-MC) and WNBA (analytical-only — WNBA has no trained
    GBM or per-period state-MC sampler yet).

    Args:
      sport: 'nba' or 'wnba'
      game_id: ESPN event id
      period_ended: 1, 2, or 3 (end of Q1 / halftime / end of Q3)

    Returns a dict suitable for downstream pick generation, or None
    when prereqs (state, prematch projection) aren't available."""
    if sport not in ("nba", "wnba", "ncaam", "afl"):
        return None
    # NCAAM has a single intermission (halftime = end of period 1).
    # NBA/WNBA/AFL carry 3 (end of Q1/Q2/Q3).
    if sport == "ncaam":
        if period_ended != 1:
            return None
    elif period_ended not in (1, 2, 3):
        return None
    constants = _SPORT_CONSTANTS[sport]
    _q_min = constants["quarter_min"]
    _g_min = constants["game_min"]
    team_sigma_full = constants["team_sigma_full"]

    from ._store import get_state, get_pbp
    # Per-sport prematch + team lookup. NBA uses the legacy
    # engine.nba_predict.predict_full(home, away). Basketball framework
    # leagues (WNBA, NCAAM, Euroleague, …) go through
    # engine.basketball._predict.predict_full(league, home, away).
    if sport == "nba":
        from ..nba_predict import predict_full as _predict_full
        from ..nba_db import get_nba_team_by_abbr as _get_team
        def _call_prematch(h, a):
            return _predict_full(h, a)
    else:
        # Basketball framework path (wnba/ncaam/euroleague/etc).
        from ..basketball._predict import predict_full as _bb_predict_full
        from ..basketball._db import get_conn as _bb_get_conn
        _league = sport  # league key matches sport for these
        def _call_prematch(h, a):
            return _bb_predict_full(_league, h, a)
        def _get_team(abbr):
            row = _bb_get_conn(_league).execute(
                "SELECT id, abbreviation, name FROM teams "
                "WHERE abbreviation = ?", (abbr,)
            ).fetchone()
            return dict(row) if row else None

    state = get_state(sport, game_id)
    if not state:
        return None
    home_abbr = (state.get("home") or {}).get("abbr")
    away_abbr = (state.get("away") or {}).get("abbr")
    if not home_abbr or not away_abbr:
        return None
    home_score = (state.get("home") or {}).get("score") or 0
    away_score = (state.get("away") or {}).get("score") or 0

    # Prematch projection — re-call without odds since we just need the
    # baseline expected scores
    try:
        prematch = _call_prematch(home_abbr, away_abbr)
    except Exception as e:
        logger.warning("%s predict_full failed for %s/%s: %s",
                       sport, home_abbr, away_abbr, e)
        return None
    if not prematch:
        return None

    home_predicted_full = float(prematch.get("home_q1_expected") or 0)
    away_predicted_full = float(prematch.get("away_q1_expected") or 0)
    # predict_full returns home_q1_expected / away_q1_expected as the
    # FULL game projection in this codebase (legacy naming — q1_predict
    # was the original entry point and predict_full reuses the same
    # field names). The actual full-game projection is the
    # ``predicted_total`` / ``predicted_margin`` block.
    home_predicted_full = float(prematch.get("home_expected")
                                or prematch.get("home_q1_expected") or 0)
    away_predicted_full = float(prematch.get("away_expected")
                                or prematch.get("away_q1_expected") or 0)
    total_predicted = float(prematch.get("predicted_total")
                            or (home_predicted_full + away_predicted_full))
    if home_predicted_full <= 0 or away_predicted_full <= 0:
        return None

    # Time elapsed / remaining
    elapsed_min = period_ended * _q_min
    remaining_min = _g_min - elapsed_min
    if remaining_min <= 0:
        return None

    # Score-based shrinkage multiplier per team
    shrink = _SHRINK_WEIGHT.get(period_ended, 0.30)
    home_expected_so_far = home_predicted_full * (elapsed_min / _g_min)
    away_expected_so_far = away_predicted_full * (elapsed_min / _g_min)
    home_mult = 1.0 + ((home_score / max(0.1, home_expected_so_far)) - 1.0) * shrink
    away_mult = 1.0 + ((away_score / max(0.1, away_expected_so_far)) - 1.0) * shrink
    # Cap multipliers to prevent runaway extrapolation on a freak quarter
    home_mult = max(0.5, min(1.6, home_mult))
    away_mult = max(0.5, min(1.6, away_mult))

    # ── Foul-trouble drag (5c integration) ─────────────────────
    # NBA-only: relies on play-by-play lineup snapshots which the WNBA
    # path doesn't populate. For WNBA we set both teams' drag to 1.0
    # (no adjustment) — captures the same final-score variance as NBA
    # at the cost of mis-projecting tight late-game splits where a star
    # is in foul trouble. A future WNBA Phase 5c can add this.
    home_team = _get_team(home_abbr) or {}
    away_team = _get_team(away_abbr) or {}
    home_team_id = str(home_team.get("id") or "")
    away_team_id = str(away_team.get("id") or "")

    # Foul-trouble drag is sport-agnostic at the code level — WNBA
    # shares the same 6-foul disqualification rule as NBA, and ESPN
    # ships identical PBP shape for both. As long as the live worker
    # has populated PBP for the in-progress game we can compute it for
    # either sport. When PBP is empty (cold start before any plays
    # land) we fall through to a no-drag default.
    from ._lineup_state import (
        snapshot_at_period_end as _lineup_snap,
        in_foul_trouble as _foul_trouble,
    )
    plays_for_lineup = get_pbp(sport, game_id) or []
    if plays_for_lineup:
        snap = _lineup_snap(plays_for_lineup, home_team_id, away_team_id,
                             period=period_ended)
        home_in_trouble = sum(
            1 for pid in snap["home"]["on_floor"]
            if _foul_trouble(snap["home"]["fouls"].get(pid, 0), period_ended)
        )
        away_in_trouble = sum(
            1 for pid in snap["away"]["on_floor"]
            if _foul_trouble(snap["away"]["fouls"].get(pid, 0), period_ended)
        )
        home_foul_drag = 1.0 - (_FOUL_TROUBLE_DRAG_PER_PLAYER * home_in_trouble)
        away_foul_drag = 1.0 - (_FOUL_TROUBLE_DRAG_PER_PLAYER * away_in_trouble)
    else:
        snap = {"home": {"on_floor": [], "fouls": {}},
                 "away": {"on_floor": [], "fouls": {}}}
        home_in_trouble = away_in_trouble = 0
        home_foul_drag = away_foul_drag = 1.0

    # Project remaining
    home_remaining_mean = (home_predicted_full
                            * (remaining_min / _g_min) * home_mult
                            * home_foul_drag)
    away_remaining_mean = (away_predicted_full
                            * (remaining_min / _g_min) * away_mult
                            * away_foul_drag)

    # Run a Gaussian MC per upcoming quarter independently. Each quarter's
    # sample = Normal(quarter_mean, team_sigma_quarter). Summed across
    # quarters = total_remaining with the right variance because of
    # independence: var(total) = N × var(quarter). Per-quarter samples
    # let us price Q-specific markets later in this function.
    rng = np.random.default_rng(
        abs(hash((game_id, period_ended))) % (2**32),
    )
    # ``team_sigma_full`` already set from _SPORT_CONSTANTS above —
    # NBA = 13.0, WNBA = 11.0.
    quarters_remaining = max(1, int(round(remaining_min / _q_min)))
    home_q_mean = home_remaining_mean / quarters_remaining
    away_q_mean = away_remaining_mean / quarters_remaining
    # Per-quarter sigma so that variance sums to full-game sigma^2 across
    # quarters_remaining quarters. The 0.85 damper compresses live
    # variance — once the game state is known (score, who has the
    # ball, foul situation), the remaining outcome is a tighter
    # distribution than a cold pre-game prior would suggest. Damper
    # value validated against 4129-game NBA backtest (literature
    # default 0.70 over-corrected and produced over-confident picks).
    rem_frac = remaining_min / _g_min
    _LIVE_SIGMA_DAMPER = 0.85
    sigma_remaining = team_sigma_full * math.sqrt(rem_frac) * _LIVE_SIGMA_DAMPER
    sigma_per_q = sigma_remaining / math.sqrt(quarters_remaining)
    home_per_q = rng.normal(home_q_mean, sigma_per_q,
                             size=(quarters_remaining, n_sims))
    away_per_q = rng.normal(away_q_mean, sigma_per_q,
                             size=(quarters_remaining, n_sims))
    home_per_q = np.clip(home_per_q, 0, None)
    away_per_q = np.clip(away_per_q, 0, None)
    home_rem = home_per_q.sum(axis=0)
    away_rem = away_per_q.sum(axis=0)

    final_home = home_score + home_rem
    final_away = away_score + away_rem
    final_total = final_home + final_away
    final_margin = final_home - final_away
    rog_total = home_rem + away_rem

    # ── Triangulation layer 2: Live GBM ─────────────────────────
    # Sport-aware artifact lookup. NBA loads nba_live_gbm_*; WNBA
    # loads wnba_live_gbm_*. is_available() inside _gbm_layer_nba
    # short-circuits to None when the league's artifact hasn't been
    # trained yet, so the analytical+state-MC layers carry the load.
    pbp_summary = summarize_pbp(plays_for_lineup,
                                 home_team_id=home_team_id,
                                 away_team_id=away_team_id,
                                 through_period=period_ended) if plays_for_lineup else None
    gbm_pred = _gbm_layer_nba(period_ended, home_score, away_score,
                                pbp_summary=pbp_summary,
                                prematch={"home_ppg": home_predicted_full,
                                           "away_ppg": away_predicted_full,
                                           "home_opp_ppg": home_predicted_full,
                                           "away_opp_ppg": away_predicted_full},
                                sport=sport)
    # ── Triangulation layer 3: State-MC empirical sampler ──
    # Sport-agnostic — WNBA uses a quarter-splits-fitted sampler with
    # the same NBA-shape (5-pt margin bins). NBA path unchanged.
    state_mc = _state_mc_layer(sport, period_ended,
                                  home_score - away_score,
                                  home_score, away_score)

    # ── Ensemble blend (33/33/33) ──────────────────────────────
    # Each layer contributes a P(home wins) and a final-total mean.
    # Weights start equal; once 30+ days of settled rows accumulate,
    # auto-tune via engine.ensemble_auto_tune on the layer probs we
    # log per pick.
    layer_probs = {
        "analytical": float((final_margin > 0).mean()),
        "gbm":        gbm_pred.get("home_win_prob") if gbm_pred else None,
        "state_mc":   state_mc.get("home_win_prob") if state_mc else None,
    }
    layer_totals = {
        "analytical": float(final_total.mean()),
        "gbm":        gbm_pred.get("total_mean") if gbm_pred else None,
        "state_mc":   state_mc.get("total_mean") if state_mc else None,
    }
    blended_p_home = _ensemble_blend([layer_probs["analytical"],
                                        layer_probs["gbm"],
                                        layer_probs["state_mc"]])
    blended_total = _ensemble_blend([layer_totals["analytical"],
                                       layer_totals["gbm"],
                                       layer_totals["state_mc"]])

    return {
        "sport": sport,
        "game_id": str(game_id),
        "period_ended": period_ended,
        "kind": "halftime" if period_ended == 2 else "period_end",
        "observed_score": {"home": home_score, "away": away_score},
        "prematch_predicted": {
            "home": home_predicted_full,
            "away": away_predicted_full,
            "total": total_predicted,
        },
        "multipliers": {
            "home": round(home_mult, 3),
            "away": round(away_mult, 3),
            "shrink": shrink,
        },
        "foul_state": {
            "home_in_trouble": home_in_trouble,
            "away_in_trouble": away_in_trouble,
            "home_drag": round(home_foul_drag, 3),
            "away_drag": round(away_foul_drag, 3),
        },
        "lineup_state": {
            "home_on_floor": sorted(snap["home"]["on_floor"]),
            "away_on_floor": sorted(snap["away"]["on_floor"]),
        },
        "remaining_mean": {
            "home": float(home_remaining_mean),
            "away": float(away_remaining_mean),
            "total": float(home_remaining_mean + away_remaining_mean),
        },
        "final_total_mean": float(blended_total
                                    if blended_total is not None
                                    else final_total.mean()),
        "final_total_std": float(final_total.std()),
        "final_margin_mean": float(final_margin.mean()),
        "final_margin_std": float(final_margin.std()),
        "rog_mean_total": float(rog_total.mean()),
        "rog_total_std": float(rog_total.std()),
        "home_final_win_prob": float(blended_p_home
                                       if blended_p_home is not None
                                       else (final_margin > 0).mean()),
        "away_final_win_prob": float(1.0 - (blended_p_home
                                              if blended_p_home is not None
                                              else (final_margin > 0).mean())),
        "layer_probs": layer_probs,
        "layer_totals": layer_totals,
        "pbp_summary": pbp_summary,
        "samples": {
            "final_total": final_total,
            "final_margin": final_margin,
            "final_home": final_home,
            "final_away": final_away,
            "rog_total": rog_total,
            "home_rem": home_rem,
            "away_rem": away_rem,
            # Per-upcoming-quarter samples — index 0 is the next quarter,
            # 1 is the one after, etc. Used to price Q-specific markets.
            "home_per_q": home_per_q,
            "away_per_q": away_per_q,
            "quarters_remaining": quarters_remaining,
        },
    }


# ── Worker entry point ────────────────────────────────────────

def run_for_event(event: dict) -> dict | None:
    """Worker hands off here when an NBA intermission fires. Halftime
    (period_ended == 2) is the headline case (5i). Q1/Q3 ends are the
    stretch case (5j) — same code path, tighter shrink weight."""
    from ._intermissions import mark_consumed

    sport = event.get("sport")
    game_id = event.get("game_id")
    period_ended = int(event.get("period") or 0)
    if (sport not in ("nba", "wnba", "ncaam", "afl")
            or not game_id or period_ended <= 0):
        return None

    # Final-period boundary is regulation done — no upcoming-period picks
    # to make. The full-game live tracker handles OT once it kicks off.
    # NCAAM = 2 halves, everything else = 4 quarters.
    final_period = 2 if sport == "ncaam" else 4
    if period_ended >= final_period:
        return None

    try:
        pred = predict_intermission(sport, game_id, period_ended)
    except Exception as e:
        logger.warning("NBA intermission predict %s P%s failed: %s",
                       game_id, period_ended, e, exc_info=True)
        return None
    if not pred:
        return None

    logger.info(
        "NBA intermission %s end-of-Q%d (%s): final total mean=%.1f, "
        "final margin mean=%+.1f, home WP=%.3f",
        game_id, period_ended, pred["kind"],
        pred["final_total_mean"], pred["final_margin_mean"],
        pred["home_final_win_prob"],
    )

    # Emit BEFORE marking consumed. The original order (consume first,
    # emit second) lost picks for live games whose HR odds weren't yet
    # populated at the exact period-end tick — emit silently produced
    # 0 picks, intermission stayed consumed, no retry on the next tick
    # when odds had arrived. Now we only mark consumed when at least
    # one pick lands, so a transient odds gap retries cleanly.
    emitted = 0
    try:
        emitted = _emit_intermission_picks(pred) or 0
    except Exception as e:
        logger.warning("NBA intermission pick emit failed: %s", e)
        emitted = 0
    if emitted > 0:
        try:
            mark_consumed(sport, str(game_id), period_ended)
        except Exception:
            pass
    else:
        logger.info(
            "NBA intermission %s end-Q%d emitted 0 picks (odds or "
            "edges missing); leaving unconsumed for retry next tick.",
            game_id, period_ended,
        )
    return pred


def _emit_intermission_picks(pred: dict) -> int:
    """Compare F48 markets against HR live odds and persist edges that
    clear the standard live floors via the existing live_tracker.

    Bet types use the canonical names already understood by
    engine.live._picks + engine.live_tracker._record so the picks
    surface in the same UI as continuous live picks. Edge floors:
    4% for SPREAD/ML, 12% for TOTAL — matches live_picks defaults.

    Returns the number of picks persisted so the caller can decide
    whether to mark the intermission consumed (>=1 emitted) or leave
    it open for retry on the next tick when odds/state may have
    populated.
    """
    from ._store import get_state
    from ..live_tracker import record_live_pick

    state = get_state(pred["sport"], pred["game_id"]) or {}
    odds = state.get("odds") or {}
    matchup = state.get("matchup") or ""
    home_abbr = (state.get("home") or {}).get("abbr") or ""
    away_abbr = (state.get("away") or {}).get("abbr") or ""
    status = state.get("status") or {}
    snapshot = {
        "period": status.get("period"),
        "clock": status.get("clock"),
        "clock_secs": status.get("clock_secs"),
        "home_score": (state.get("home") or {}).get("score"),
        "away_score": (state.get("away") or {}).get("score"),
        "remaining_s": _remaining_seconds_nba(status),
    }

    samples_total = pred["samples"]["final_total"]
    samples_margin = pred["samples"]["final_margin"]
    candidates: list[dict] = []

    # Full-game total
    line = odds.get("over_under")
    over_odds = odds.get("over_odds")
    under_odds = odds.get("under_odds")
    if isinstance(line, (int, float)) and over_odds and under_odds:
        line_f = float(line)
        p_over = float((samples_total > line_f).mean())
        p_under = float((samples_total < line_f).mean())
        over_imp = _market_implied(over_odds)
        under_imp = _market_implied(under_odds)
        if over_imp is not None:
            candidates.append({
                "bet_type": "TOTAL",
                "pick": f"Over {line_f}",
                "odds": int(over_odds),
                "model_prob": p_over,
                "edge_pct": (p_over - over_imp) * 100,
            })
        if under_imp is not None:
            candidates.append({
                "bet_type": "TOTAL",
                "pick": f"Under {line_f}",
                "odds": int(under_odds),
                "model_prob": p_under,
                "edge_pct": (p_under - under_imp) * 100,
            })

    # Full-game spread
    home_sp = odds.get("home_spread_point")
    home_sp_odds = odds.get("home_spread_odds")
    away_sp = odds.get("away_spread_point")
    away_sp_odds = odds.get("away_spread_odds")
    if (isinstance(home_sp, (int, float)) and home_sp_odds
            and isinstance(away_sp, (int, float)) and away_sp_odds):
        p_home_cover = float((samples_margin > -float(home_sp)).mean())
        p_away_cover = float((samples_margin < -float(home_sp)).mean())
        h_imp = _market_implied(home_sp_odds)
        a_imp = _market_implied(away_sp_odds)
        if h_imp is not None:
            candidates.append({
                "bet_type": "SPREAD",
                "pick": f"{home_abbr} {home_sp:+g}",
                "odds": int(home_sp_odds),
                "model_prob": p_home_cover,
                "edge_pct": (p_home_cover - h_imp) * 100,
            })
        if a_imp is not None:
            candidates.append({
                "bet_type": "SPREAD",
                "pick": f"{away_abbr} {away_sp:+g}",
                "odds": int(away_sp_odds),
                "model_prob": p_away_cover,
                "edge_pct": (p_away_cover - a_imp) * 100,
            })

    # Full-game ML
    home_ml = odds.get("home_ml") or odds.get("home_moneyline_odds")
    away_ml = odds.get("away_ml") or odds.get("away_moneyline_odds")
    if home_ml and away_ml:
        p_home = pred["home_final_win_prob"]
        p_away = pred["away_final_win_prob"]
        h_imp = _market_implied(home_ml)
        a_imp = _market_implied(away_ml)
        if h_imp is not None:
            candidates.append({
                "bet_type": "ML",
                "pick": home_abbr,
                "odds": int(home_ml),
                "model_prob": p_home,
                "edge_pct": (p_home - h_imp) * 100,
            })
        if a_imp is not None:
            candidates.append({
                "bet_type": "ML",
                "pick": away_abbr,
                "odds": int(away_ml),
                "model_prob": p_away,
                "edge_pct": (p_away - a_imp) * 100,
            })

    # ── Per-upcoming-quarter markets ─────────────────────────────
    # HR's NBA per-period odds land at odds["periods"][code] with the
    # same shape as full-game (home_ml, away_ml, home_spread_point,
    # over_under, ...). At end of Q1 we price Q2; at halftime we price
    # Q3 + H2; at end of Q3 we price Q4. The H2 case combines the
    # two upcoming quarters; everything else is single-quarter.
    periods_block = odds.get("periods") or {}
    samples_per_q = pred["samples"]["home_per_q"], pred["samples"]["away_per_q"]
    quarters_remaining = pred["samples"]["quarters_remaining"]

    def _q_samples(q_index_offset: int) -> tuple[np.ndarray, np.ndarray] | None:
        """Return (home_q_samples, away_q_samples) for the upcoming
        quarter at offset 0 (next), 1 (after that), etc. None when
        the offset is past the remaining quarters."""
        if q_index_offset >= quarters_remaining:
            return None
        h_arr, a_arr = samples_per_q
        return h_arr[q_index_offset], a_arr[q_index_offset]

    # Map period_ended → list of (period_code, sample_offset_or_aggregate)
    # period codes match HR's NBA `periods` dict keys.
    # ``code`` is the HR `periods` dict key (Q2/Q3/Q4/H2). ``bt_prefix``
    # is what we use in the live_tracker bet_type so its market_scope
    # mapper recognizes it (Q-prefixed = quarter, "2H " = H2).
    period_ended_n = pred.get("period_ended") or 0
    period_targets: list[tuple[str, str, str]] = []  # (hr_code, bt_prefix, mode)
    if period_ended_n == 1:
        period_targets.append(("Q2", "Q2", "single"))
    elif period_ended_n == 2:
        period_targets.append(("Q3", "Q3", "single"))
        period_targets.append(("H2", "2H", "pair"))
    elif period_ended_n == 3:
        period_targets.append(("Q4", "Q4", "single"))

    for hr_code, bt_prefix, mode in period_targets:
        block = periods_block.get(hr_code)
        if not isinstance(block, dict):
            continue
        # Build samples for THIS upcoming period
        if mode == "single":
            qs = _q_samples(0)
            if qs is None:
                continue
            home_p, away_p = qs
        else:  # 'pair' = sum of next 2 quarters (H2 = Q3 + Q4)
            q0 = _q_samples(0)
            q1 = _q_samples(1)
            if q0 is None or q1 is None:
                continue
            home_p = q0[0] + q1[0]
            away_p = q0[1] + q1[1]
        period_total_samples = home_p + away_p
        period_margin_samples = home_p - away_p

        # Period total
        p_line = block.get("over_under")
        p_over = block.get("over_odds")
        p_under = block.get("under_odds")
        if isinstance(p_line, (int, float)) and p_over and p_under:
            line_f = float(p_line)
            prob_o = float((period_total_samples > line_f).mean())
            prob_u = float((period_total_samples < line_f).mean())
            o_imp = _market_implied(p_over)
            u_imp = _market_implied(p_under)
            if o_imp is not None:
                candidates.append({
                    "bet_type": f"{bt_prefix} TOTAL",
                    "pick": f"Over {line_f}",
                    "odds": int(p_over),
                    "model_prob": prob_o,
                    "edge_pct": (prob_o - o_imp) * 100,
                })
            if u_imp is not None:
                candidates.append({
                    "bet_type": f"{bt_prefix} TOTAL",
                    "pick": f"Under {line_f}",
                    "odds": int(p_under),
                    "model_prob": prob_u,
                    "edge_pct": (prob_u - u_imp) * 100,
                })

        # Period spread
        p_h_sp = block.get("home_spread_point")
        p_h_sp_o = block.get("home_spread_odds")
        p_a_sp = block.get("away_spread_point")
        p_a_sp_o = block.get("away_spread_odds")
        if (isinstance(p_h_sp, (int, float)) and p_h_sp_o
                and isinstance(p_a_sp, (int, float)) and p_a_sp_o):
            phc = float((period_margin_samples > -float(p_h_sp)).mean())
            pac = float((period_margin_samples < -float(p_h_sp)).mean())
            h_imp = _market_implied(p_h_sp_o)
            a_imp = _market_implied(p_a_sp_o)
            if h_imp is not None:
                candidates.append({
                    "bet_type": f"{bt_prefix} SPREAD",
                    "pick": f"{home_abbr} {p_h_sp:+g}",
                    "odds": int(p_h_sp_o),
                    "model_prob": phc,
                    "edge_pct": (phc - h_imp) * 100,
                })
            if a_imp is not None:
                candidates.append({
                    "bet_type": f"{bt_prefix} SPREAD",
                    "pick": f"{away_abbr} {p_a_sp:+g}",
                    "odds": int(p_a_sp_o),
                    "model_prob": pac,
                    "edge_pct": (pac - a_imp) * 100,
                })

        # Period ML
        p_h_ml = block.get("home_ml")
        p_a_ml = block.get("away_ml")
        if p_h_ml and p_a_ml:
            p_home_win = float((period_margin_samples > 0).mean())
            p_away_win = float((period_margin_samples < 0).mean())
            h_imp = _market_implied(p_h_ml)
            a_imp = _market_implied(p_a_ml)
            if h_imp is not None:
                candidates.append({
                    "bet_type": f"{bt_prefix} ML",
                    "pick": home_abbr,
                    "odds": int(p_h_ml),
                    "model_prob": p_home_win,
                    "edge_pct": (p_home_win - h_imp) * 100,
                })
            if a_imp is not None:
                candidates.append({
                    "bet_type": f"{bt_prefix} ML",
                    "pick": away_abbr,
                    "odds": int(p_a_ml),
                    "model_prob": p_away_win,
                    "edge_pct": (p_away_win - a_imp) * 100,
                })

    if not candidates:
        return 0

    candidates.sort(key=lambda c: -c["edge_pct"])
    # Apply edge floors per market — same convention as live._picks.
    # Period markets reuse the same floors (their bet_type strips the
    # prefix to look up — "Q3 TOTAL" matches "TOTAL").
    floor_for = {"ML": 4.0, "SPREAD": 4.0, "TOTAL": 12.0}

    def _floor_for_bet_type(bt: str) -> float:
        # Strip period prefix for floor lookup ("Q3 TOTAL" -> "TOTAL").
        for prefix in ("Q1 ", "Q2 ", "Q3 ", "Q4 ", "1H ", "2H "):
            if bt.startswith(prefix):
                return floor_for.get(bt[len(prefix):], 4.0)
        return floor_for.get(bt, 4.0)
    # Cap the picks emitted per intermission so the UI doesn't get
    # flooded with 8 picks the moment the buzzer hits. Top 3 by edge
    # mirrors MAX_PICKS_PER_GAME from continuous live.
    # Inherit the same -200 juice wall the continuous live picker
    # uses (engine.live._picks.LIVE_ODDS_FLOOR). Without it, intermission
    # picks could fire on heavy chalk like ML VGK @ -475 where one
    # variance event wipes multiple wins. Live testing 2026-04-30:
    # VGK end-of-P2 surfaced ML @ -475 with +6.1% edge — exactly the
    # case this gate refuses.
    from ._picks import LIVE_ODDS_FLOOR

    emitted = 0
    for c in candidates:
        floor = _floor_for_bet_type(c["bet_type"])
        if c["edge_pct"] < floor:
            continue
        if c["odds"] < 0 and c["odds"] < LIVE_ODDS_FLOOR:
            continue
        # Belief gate: never pick a side the model thinks loses. Same
        # rule picks_core enforces for prematch picks (feedback_be_right
        # _first.md, and the underdog-ML directive in CLAUDE.md). Was
        # silently bypassed by the live predictor 2026-05-13 — MIN ML
        # fired at +325 with model_prob=0.31 (model says MIN loses) just
        # because the EV math was positive. Apply to every bet_type, not
        # just ML: a Q3 ML pick on a side we expect to lose has the same
        # shape problem as a full-game ML pick on that side.
        if c["model_prob"] < 0.50:
            logger.info(
                "NBA intermission pick REJECTED by belief gate: "
                "%s '%s' @ %+d (model_prob=%.3f edge=%+.1f%%) "
                "— model says this side loses",
                c["bet_type"], c["pick"], c["odds"],
                c["model_prob"], c["edge_pct"],
            )
            continue
        try:
            pick_payload = {
                "sport": pred["sport"],
                "game_id": pred["game_id"],
                "matchup": matchup,
                "bet_type": c["bet_type"],
                "pick": c["pick"],
                "odds": c["odds"],
                "model_prob": c["model_prob"],
                "edge_pct": c["edge_pct"],
                "snapshot": snapshot,
                # Triangulation log so future ensemble auto-tune has
                # per-layer ground truth to fit on.
                "layer_probs": pred.get("layer_probs") or {},
            }
            new_id = record_live_pick(pred["sport"], pick_payload)
            logger.info(
                "NBA intermission pick recorded id=%s end-Q%d %s '%s' @ %+d "
                "(edge %+.1f%% prob %.3f)",
                new_id, pred["period_ended"], c["bet_type"], c["pick"],
                c["odds"], c["edge_pct"], c["model_prob"],
            )
        except Exception as e:
            logger.warning("NBA intermission record failed: %s", e)
            continue
        emitted += 1
        if emitted >= 3:
            break
    return emitted


def _remaining_seconds_nba(status: dict) -> int | None:
    """Approximate remaining-seconds at intermission.
    Halftime: 24 minutes left = 1440s
    End of Q1: 36 min left = 2160s
    End of Q3: 12 min left = 720s
    """
    period = status.get("period") or 0
    if period == 1:
        return 36 * 60
    if period == 2:
        return 24 * 60
    if period == 3:
        return 12 * 60
    return None


def _market_implied(odds: int | None) -> float | None:
    if odds is None:
        return None
    n = float(odds)
    if n == 0:
        return None
    if n < 0:
        return abs(n) / (abs(n) + 100.0)
    return 100.0 / (n + 100.0)


__all__ = ["summarize_pbp", "predict_intermission", "run_for_event"]
