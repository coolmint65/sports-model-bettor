"""
NHL period-end predictor (Phase 5h, MVP).

Fires once per intermission via the worker's intermission detector.
Reads PBP + team profiles, adjusts each team's xG profile with what
the period(s) so far actually showed, simulates remaining-game time,
and writes picks against HR's intermission markets.

MVP scope
---------
The predictor leverages existing season-level NHL profiles (from
nhl_team_stats) and adjusts them with observed period-N stats:

  - shots-for / shots-against pace vs prematch baseline
  - live goalie save% blended with prematch save%
  - goals scored already (added to MC output for final-game markets)

What's NOT in MVP (deferred to a later iteration once historical PBP
is backfilled):

  - Special teams state — PP / PK time remaining when a period ends
    (rare but consequential)
  - High-danger / low-danger shot quality split (needs xG-by-shot,
    ESPN doesn't expose shot location consistently)
  - Score-state shooting% adjustment (well-documented effect: trailing
    teams shoot more aggressively, leading teams sit on the lead)

Output markets
--------------
For period N just ended (N in {1, 2}):

  - Rest-of-game total goals (over/under at HR's posted line)
  - Period N+1 winner (home / away / draw)
  - Period N+1 total goals
  - Game total goals (observed + ROG mean)
  - Final ML home win probability
  - Regulation tie probability (relevant when period 3 isn't yet
    played; helps price OT yes/no)

Picks land in the same live_picks table the existing live predictor
writes to, so the existing UI / tracker pick them up automatically.
"""

from __future__ import annotations

import logging
import math
from collections import Counter
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


# ── PBP feature extraction ─────────────────────────────────────

# ESPN NHL play type_text values seen in 5b live data
_SHOT_ON_GOAL = {"shot", "goal"}
_MISSED = {"missed"}
_BLOCKED = {"blocked"}
_PENALTY = {"tripping", "interference", "holding", "boarding",
             "hooking", "slashing", "high-sticking", "roughing",
             "cross checking", "elbowing", "delaying game",
             "delay of game", "too many men", "unsportsmanlike",
             "fighting", "instigator", "misconduct"}


def _team_id_for_play(play: dict) -> str | None:
    return (play.get("team_id") or None)


def summarize_pbp(plays: list[dict],
                  home_team_id: str | None,
                  away_team_id: str | None) -> dict:
    """Walk a game's plays once and emit per-team aggregate stats.

    Returns ``{periods_complete, home: {...}, away: {...}}`` where each
    team block has ``shots_on_goal``, ``shot_attempts``, ``goals``,
    ``penalties_taken``. ``periods_complete`` is the count of fully-
    finished periods (matches highest-numbered Period End play).
    """
    home = {"shots_on_goal": 0, "shot_attempts": 0, "goals": 0,
            "penalties_taken": 0}
    away = dict(home)
    periods_complete = 0
    for p in plays or []:
        type_text = (p.get("type_text") or "").strip().lower()
        team_id = _team_id_for_play(p)
        if type_text == "period end":
            try:
                periods_complete = max(periods_complete,
                                       int(p.get("period") or 0))
            except (TypeError, ValueError):
                pass
            continue
        # Map team_id to home / away bucket
        if team_id and home_team_id and str(team_id) == str(home_team_id):
            bucket = home
        elif team_id and away_team_id and str(team_id) == str(away_team_id):
            bucket = away
        else:
            continue
        if type_text in _SHOT_ON_GOAL:
            bucket["shots_on_goal"] += 1
            bucket["shot_attempts"] += 1
            if type_text == "goal":
                bucket["goals"] += 1
        elif type_text in _MISSED:
            bucket["shot_attempts"] += 1
        elif type_text in _BLOCKED:
            # Blocked shots are CREDITED to the shooter's team for
            # Corsi (shot attempts) — the team_id on a blocked play
            # is the SHOOTER, not the blocker, in ESPN's PBP.
            bucket["shot_attempts"] += 1
        elif type_text in _PENALTY:
            bucket["penalties_taken"] += 1
    return {
        "periods_complete": periods_complete,
        "home": home,
        "away": away,
    }


# ── xG / save% adjustment ──────────────────────────────────────

# How heavily to weight live observations vs the prematch profile.
# 0.5 means a team that's outperformed prematch by 20% over one period
# gets +10% of that gap baked in. Conservative to avoid over-fitting
# to one period of variance — a hot first 20 minutes doesn't make a
# bottom-tier team a top-tier team going forward.
_LIVE_WEIGHT_PER_PERIOD = 0.20

LEAGUE_SHOTS_PER_GAME = 30.0  # NHL average roughly; used for SOG normalization


def _blend_save_pct(prematch: float, live_save_pct: float | None,
                    shots_against: int) -> float:
    """Bayesian-style blend of prematch goalie save% with observed
    live save%. Sample size from shots_against ranges 0..40+; prior
    weight is 30 shots so a clean P1 (10 shots, 1 GA = 0.900 live)
    only nudges season 0.910 by ~0.003."""
    if live_save_pct is None or shots_against <= 0:
        return prematch
    prior_n = 30.0
    w_live = shots_against / (shots_against + prior_n)
    return prematch * (1 - w_live) + live_save_pct * w_live


def _adjusted_profile(prematch_profile, observed: dict,
                      opponent_observed: dict,
                      periods_complete: int):
    """Return a new NHLTeamProfile with offense/defense/save_pct nudged
    by what the live game showed. ``periods_complete`` weights the
    adjustment — more completed periods means we trust the live data
    more.

    offense_mult drift: shots-on-goal pace vs prematch's shot rate
    proxy (gf_per_game scaled to per-period). Above-prematch SOG ⇒
    bump offense, below ⇒ shrink.

    defense_mult drift: opponent's shots-on-goal pace against THIS team
    relative to expectation. Lots of shots conceded ⇒ defense weakened.

    save_pct drift: observed live SV% on shots faced.
    """
    from ..mc_nhl import NHLTeamProfile, LEAGUE_SAVE_PCT

    # Per-period expected SOG. Use 30 shots/game / 3 periods = 10 SOG/period
    # as a league baseline; teams' specific shot rates aren't on the
    # season-stats table, so we use the league number as the prior and
    # just compare observed vs expected.
    expected_sog = LEAGUE_SHOTS_PER_GAME / 3.0 * max(1, periods_complete)

    # Offense adjustment: live SOG / expected. Damped per period.
    sog_obs = observed.get("shots_on_goal") or 0
    if expected_sog > 0:
        sog_ratio = sog_obs / expected_sog
        # Pull toward 1.0 unless the gap is sustained
        live_weight = min(1.0, _LIVE_WEIGHT_PER_PERIOD * periods_complete)
        offense_mult = 1.0 + (sog_ratio - 1.0) * live_weight
    else:
        offense_mult = 1.0

    # Defense adjustment: opponent SOG vs expected (more = defense down)
    opp_sog = opponent_observed.get("shots_on_goal") or 0
    if expected_sog > 0:
        opp_ratio = opp_sog / expected_sog
        live_weight = min(1.0, _LIVE_WEIGHT_PER_PERIOD * periods_complete)
        # Opp shooting more = our defense weakened = defense_mult goes UP
        # (defense_mult > 1 means GA bumps up in the simulator's blend).
        defense_mult = 1.0 + (opp_ratio - 1.0) * live_weight
    else:
        defense_mult = 1.0

    # Save% adjustment: live save rate on shots faced. opponent's SOG
    # is shots-against from THIS team's goalie's perspective.
    live_save_pct = None
    opp_goals = opponent_observed.get("goals") or 0
    shots_against = opp_sog
    if shots_against > 0:
        live_save_pct = max(0.0, 1.0 - (opp_goals / shots_against))
    blended_sv = _blend_save_pct(
        prematch_profile.save_pct, live_save_pct, shots_against,
    )

    return NHLTeamProfile(
        gf_per_game=prematch_profile.gf_per_game,
        ga_per_game=prematch_profile.ga_per_game,
        pp_goals_per_game=prematch_profile.pp_goals_per_game,
        save_pct=blended_sv,
        offense_mult=prematch_profile.offense_mult * offense_mult,
        defense_mult=prematch_profile.defense_mult * defense_mult,
        name=prematch_profile.name,
    )


# ── Remaining-game simulator ───────────────────────────────────

def simulate_remaining(home_xg_per_period: float,
                        away_xg_per_period: float,
                        periods_remaining: int,
                        n_sims: int = 50_000,
                        seed: int | None = None) -> dict:
    """Poisson-by-period sim for the remaining periods. Returns the
    rest-of-game goal distributions per team plus per-period samples
    so the caller can extract period-specific markets.

    Mirrors mc_nhl.simulate_games but for partial-game horizon — no
    OT / SO logic baked in (caller adds that for final-game markets
    by combining with observed score)."""
    rng = np.random.default_rng(seed)
    if periods_remaining <= 0:
        return {"home_goals": np.zeros(n_sims, dtype=int),
                "away_goals": np.zeros(n_sims, dtype=int),
                "home_per_period": np.zeros((0, n_sims), dtype=int),
                "away_per_period": np.zeros((0, n_sims), dtype=int)}
    home_lambda = max(0.05, home_xg_per_period)
    away_lambda = max(0.05, away_xg_per_period)
    home_per_period = rng.poisson(home_lambda, size=(periods_remaining, n_sims))
    away_per_period = rng.poisson(away_lambda, size=(periods_remaining, n_sims))
    return {
        "home_goals": home_per_period.sum(axis=0),
        "away_goals": away_per_period.sum(axis=0),
        "home_per_period": home_per_period,
        "away_per_period": away_per_period,
    }


# ── Picks generation ───────────────────────────────────────────

def _market_implied(odds: int | None) -> float | None:
    if odds is None:
        return None
    n = float(odds)
    if n == 0:
        return None
    if n < 0:
        return abs(n) / (abs(n) + 100.0)
    return 100.0 / (n + 100.0)


def _to_american(prob: float) -> int:
    """Convert a model probability to American odds (no juice)."""
    p = min(0.99, max(0.01, prob))
    if p >= 0.5:
        return -int(round((p / (1 - p)) * 100))
    return int(round(((1 - p) / p) * 100))


def _ensemble_blend(values: list) -> float | None:
    """Average non-None layer outputs."""
    valid = [v for v in values if v is not None]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _gbm_layer_nhl(period_ended: int, home_score: int, away_score: int,
                    summary: dict | None) -> dict | None:
    """Predict via Live GBM. ``summary`` is the per-team aggregate
    from summarize_pbp."""
    try:
        from ..gbm.predict import predict_live, is_available
        if not is_available("nhl_live"):
            return None
        h_stats = (summary or {}).get("home", {})
        a_stats = (summary or {}).get("away", {})
        state = {
            "period_ended": period_ended,
            "home_score_so_far": home_score,
            "away_score_so_far": away_score,
            "shots_home": h_stats.get("shots_on_goal") or 0,
            "shots_away": a_stats.get("shots_on_goal") or 0,
            "shot_attempts_home": h_stats.get("shot_attempts") or 0,
            "shot_attempts_away": a_stats.get("shot_attempts") or 0,
            "penalties_home": h_stats.get("penalties_taken") or 0,
            "penalties_away": a_stats.get("penalties_taken") or 0,
        }
        gbm = predict_live("nhl", state)
        if not gbm or "home_final_win" not in gbm:
            return None
        win = gbm.get("home_final_win")
        total = gbm.get("final_total_points")
        if isinstance(win, dict) or isinstance(total, dict):
            return None
        return {"home_win_prob": float(win),
                "total_mean":    float(total) if total is not None else None}
    except Exception as e:
        logger.debug("GBM layer NHL failed: %s", e)
        return None


def _state_mc_layer_nhl(period_ended: int, margin_so_far: int,
                         home_score: int, away_score: int) -> dict | None:
    """Empirical sampler from state_mc."""
    try:
        from ._state_mc import sample_remaining
        samples = sample_remaining("nhl", period_ended, margin_so_far,
                                     n=5000)
        if not samples or len(samples) < 50:
            return None
        import numpy as np
        h_rem = np.array([s[0] for s in samples], dtype=float)
        a_rem = np.array([s[1] for s in samples], dtype=float)
        final_h = home_score + h_rem
        final_a = away_score + a_rem
        return {"home_win_prob": float((final_h - final_a > 0).mean()),
                "total_mean":    float((final_h + final_a).mean())}
    except Exception as e:
        logger.debug("state_mc layer NHL failed: %s", e)
        return None


def predict_intermission(sport: str, game_id: str, period_ended: int,
                          n_sims: int = 50_000) -> dict | None:
    """Run the period-end predictor for a single game. Returns a
    summary dict with keys ``periods_complete``, ``observed_score``,
    ``rog_mean_total``, ``final_total_mean``, ``home_final_win_prob``,
    ``period_next``, ``regulation_tie_prob``.

    Returns None when prerequisites are missing (no team profiles, no
    PBP, etc.).
    """
    if sport != "nhl":
        return None
    from ..mc_nhl_run import _load_team_profile
    from ..nhl_db import get_nhl_team_by_abbr
    from ._store import get_state, get_pbp

    state = get_state(sport, game_id)
    if not state:
        logger.debug("predict_intermission: no live_state for %s/%s",
                     sport, game_id)
        return None

    home_abbr = (state.get("home") or {}).get("abbr")
    away_abbr = (state.get("away") or {}).get("abbr")
    if not home_abbr or not away_abbr:
        return None

    # Resolve team ids for PBP team-id matching. nhl_teams.id is what
    # ESPN's team.id field points at via abbr lookup.
    home_team = get_nhl_team_by_abbr(home_abbr) or {}
    away_team = get_nhl_team_by_abbr(away_abbr) or {}
    home_team_id = str(home_team.get("id") or "")
    away_team_id = str(away_team.get("id") or "")

    plays = get_pbp(sport, game_id)
    if not plays:
        logger.debug("predict_intermission: no PBP for %s/%s",
                     sport, game_id)
        return None

    summary = summarize_pbp(plays, home_team_id, away_team_id)
    periods_complete = summary["periods_complete"] or period_ended

    # Build prematch profiles
    home_profile = _load_team_profile(home_abbr, None)
    away_profile = _load_team_profile(away_abbr, None)

    # Live-adjusted profiles
    home_adj = _adjusted_profile(
        home_profile, summary["home"], summary["away"], periods_complete,
    )
    away_adj = _adjusted_profile(
        away_profile, summary["away"], summary["home"], periods_complete,
    )

    # Compute live xG per period using mc_nhl.expected_goals on adjusted
    # profiles, then divide by 3.
    from ..mc_nhl import expected_goals
    from ..nhl_predict import _is_playoff_window
    is_playoff = False
    try:
        is_playoff = _is_playoff_window()
    except Exception:
        pass
    home_xg, away_xg = expected_goals(home_adj, away_adj, is_playoff=is_playoff)
    home_xg_period = home_xg / 3.0
    away_xg_period = away_xg / 3.0

    periods_remaining = max(0, 3 - periods_complete)
    sim = simulate_remaining(home_xg_period, away_xg_period,
                              periods_remaining, n_sims=n_sims)

    # Observed score so far
    home_observed = (state.get("home") or {}).get("score") or 0
    away_observed = (state.get("away") or {}).get("score") or 0

    # Rest-of-game (regulation only) totals + final reg-time totals
    rog_total = sim["home_goals"] + sim["away_goals"]
    final_home = home_observed + sim["home_goals"]
    final_away = away_observed + sim["away_goals"]
    final_total = final_home + final_away

    # Regulation tie prob — informative for OT yes/no markets even if
    # we don't price them directly here.
    reg_tie = (final_home == final_away)

    # Final ML — assume regulation winners win, ties resolve 50/50 to
    # crudely approximate OT/SO. This isn't great but it's MVP; a fuller
    # version would re-roll a Bernoulli OT-goal at the league OT rate.
    home_wins = final_home > final_away
    away_wins = final_away > final_home
    home_win_prob = (home_wins.sum() + 0.5 * reg_tie.sum()) / float(len(rog_total))

    # Period-next markets — just the next period's distribution
    period_next = None
    if sim["home_per_period"].shape[0] > 0:
        next_home = sim["home_per_period"][0]
        next_away = sim["away_per_period"][0]
        next_total = next_home + next_away
        period_next = {
            "period": periods_complete + 1,
            "home_goals_mean": float(next_home.mean()),
            "away_goals_mean": float(next_away.mean()),
            "total_goals_mean": float(next_total.mean()),
            "home_win_prob": float((next_home > next_away).mean()),
            "away_win_prob": float((next_away > next_home).mean()),
            "draw_prob": float((next_home == next_away).mean()),
            "over_05_prob": float((next_total > 0.5).mean()),
            "over_15_prob": float((next_total > 1.5).mean()),
            "over_25_prob": float((next_total > 2.5).mean()),
        }

    # ── Triangulation: Live GBM + state-MC ──────────────────────
    gbm_pred = _gbm_layer_nhl(periods_complete, home_observed,
                                away_observed, summary)
    state_mc = _state_mc_layer_nhl(periods_complete,
                                     home_observed - away_observed,
                                     home_observed, away_observed)
    layer_probs = {
        "analytical": float(home_win_prob),
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
        "periods_complete": periods_complete,
        "observed_score": {"home": home_observed, "away": away_observed},
        "live_xg_per_period": {"home": home_xg_period, "away": away_xg_period},
        "rog_mean_total": float(rog_total.mean()),
        "rog_total_std": float(rog_total.std()),
        "final_total_mean": float(blended_total
                                    if blended_total is not None
                                    else final_total.mean()),
        "final_total_std": float(final_total.std()),
        "home_final_win_prob": float(blended_p_home
                                       if blended_p_home is not None
                                       else home_win_prob),
        "away_final_win_prob": float(1.0 - (blended_p_home
                                              if blended_p_home is not None
                                              else home_win_prob)),
        "regulation_tie_prob": float(reg_tie.mean()),
        "period_next": period_next,
        "layer_probs": layer_probs,
        "layer_totals": layer_totals,
        "samples": {
            "rog_total": rog_total,
            "final_total": final_total,
            "final_home": final_home,
            "final_away": final_away,
            # Per-upcoming-period samples — index 0 is the next period,
            # 1 is the one after that. Used for per-period market pricing.
            "home_per_period": sim["home_per_period"],
            "away_per_period": sim["away_per_period"],
            "periods_complete": periods_complete,
            "periods_remaining": periods_remaining,
        },
    }


# ── Entry point for the worker ─────────────────────────────────

def run_for_event(event: dict) -> dict | None:
    """Worker calls this when an intermission fires. Runs the predictor,
    surfaces picks, marks the intermission row consumed.

    Returns the prediction dict on success, None on no-op (predictor
    failed prerequisites, no picks land regardless)."""
    from ._intermissions import mark_consumed

    sport = event.get("sport")
    game_id = event.get("game_id")
    period_ended = int(event.get("period") or 0)
    if sport != "nhl" or not game_id or period_ended <= 0:
        return None
    # NHL has 3 regulation periods. End of P3 is regulation done; OT/SO
    # markets price differently and live_predict + live_picks already
    # handle them. Skip P3+ here to avoid muddying that boundary.
    if period_ended >= 3:
        return None

    try:
        pred = predict_intermission(sport, game_id, period_ended)
    except Exception as e:
        logger.warning("NHL intermission predict %s P%s failed: %s",
                       game_id, period_ended, e, exc_info=True)
        return None
    if not pred:
        return None

    logger.info(
        "NHL intermission %s P%s: ROG total mean=%.2f, final ML home=%.3f",
        game_id, period_ended,
        pred["rog_mean_total"], pred["home_final_win_prob"],
    )

    # Mark consumed BEFORE writing picks so a pick-write failure
    # doesn't loop forever — picks are best-effort.
    try:
        mark_consumed(sport, str(game_id), period_ended)
    except Exception:
        pass

    # Pick-writing is gated on having HR live odds for the markets.
    # Implementation: see _emit_intermission_picks below; until 5h/5i
    # finalize the live tracker write path, the prediction itself is
    # logged + persisted in live_intermissions for downstream UI.
    try:
        _emit_intermission_picks(pred)
    except Exception as e:
        logger.warning("NHL intermission pick emit failed: %s", e)

    return pred


def _emit_intermission_picks(pred: dict) -> None:
    """Compare predicted F60 + per-period markets against HR live odds
    and persist edges via the existing live tracker.

    Edge floors: 4% ML/PL/period Winner/BTS, 12% TOTAL/period total.
    Per-period markets read from HR's `periods` (NBA-shape) and
    `period_totals` / `period_dnb` / `period_bts` dicts. At end of P1
    we price P2 + P3; at end of P2 we price P3.

    Period winner label is "Winner" not "DNB" — the user reads "DNB"
    as "doesn't score" and questioned a correct W, so we keep the
    bet_type human-readable while the settler resolves it via the
    existing endswith("WINNER") branch.
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
        "remaining_s": _remaining_seconds_nhl(pred["periods_complete"]),
    }

    samples_total = pred["samples"]["final_total"]
    samples_home = pred["samples"]["final_home"]
    samples_away = pred["samples"]["final_away"]
    candidates: list[dict] = []

    # F60 total
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
                "bet_type": "TOTAL", "pick": f"Over {line_f}",
                "odds": int(over_odds), "model_prob": p_over,
                "edge_pct": (p_over - over_imp) * 100,
            })
        if under_imp is not None:
            candidates.append({
                "bet_type": "TOTAL", "pick": f"Under {line_f}",
                "odds": int(under_odds), "model_prob": p_under,
                "edge_pct": (p_under - under_imp) * 100,
            })

    # F60 ML
    home_ml = odds.get("home_ml") or odds.get("home_moneyline_odds")
    away_ml = odds.get("away_ml") or odds.get("away_moneyline_odds")
    if home_ml and away_ml:
        p_home = pred["home_final_win_prob"]
        p_away = pred["away_final_win_prob"]
        h_imp = _market_implied(home_ml)
        a_imp = _market_implied(away_ml)
        if h_imp is not None:
            candidates.append({
                "bet_type": "ML", "pick": home_abbr, "odds": int(home_ml),
                "model_prob": p_home,
                "edge_pct": (p_home - h_imp) * 100,
            })
        if a_imp is not None:
            candidates.append({
                "bet_type": "ML", "pick": away_abbr, "odds": int(away_ml),
                "model_prob": p_away,
                "edge_pct": (p_away - a_imp) * 100,
            })

    # F60 puck line — home/away ±1.5
    home_pl = odds.get("home_puck_line_point")
    home_pl_odds = odds.get("home_puck_line_odds")
    away_pl = odds.get("away_puck_line_point")
    away_pl_odds = odds.get("away_puck_line_odds")
    if (isinstance(home_pl, (int, float)) and home_pl_odds
            and isinstance(away_pl, (int, float)) and away_pl_odds):
        margin_samples = samples_home - samples_away
        # home covers iff margin > -home_pl (e.g. home -1.5 means margin > 1.5)
        p_home_cover = float((margin_samples > -float(home_pl)).mean())
        p_away_cover = float((margin_samples < -float(home_pl)).mean())
        h_imp = _market_implied(home_pl_odds)
        a_imp = _market_implied(away_pl_odds)
        if h_imp is not None:
            candidates.append({
                "bet_type": "PL",
                "pick": f"{home_abbr} {home_pl:+g}",
                "odds": int(home_pl_odds),
                "model_prob": p_home_cover,
                "edge_pct": (p_home_cover - h_imp) * 100,
            })
        if a_imp is not None:
            candidates.append({
                "bet_type": "PL",
                "pick": f"{away_abbr} {away_pl:+g}",
                "odds": int(away_pl_odds),
                "model_prob": p_away_cover,
                "edge_pct": (p_away_cover - a_imp) * 100,
            })

    # ── Per-upcoming-period markets ─────────────────────────────
    # HR's NHL period markets land in two possible locations depending
    # on how the scraper categorized them:
    #   1. ``odds["periods"]["Q1"]`` etc. — BASKETBALL-style storage
    #      (HR re-uses Q-codes across sports; for NHL Q2 means P2).
    #      This holds the period total + spread + ML in NBA shape.
    #   2. ``odds["period_totals"]`` / ``period_dnb`` / ``period_bts``
    #      keyed on stringified period numbers — explicit NHL
    #      per-period dicts when HR exposes the dedicated codes.
    # We check both so a market in either location is priced.
    periods_block = odds.get("periods") or {}
    period_totals = odds.get("period_totals") or {}
    period_dnbs = odds.get("period_dnb") or {}
    period_btss = odds.get("period_bts") or {}
    home_per_period = pred["samples"]["home_per_period"]
    away_per_period = pred["samples"]["away_per_period"]
    periods_complete = pred["samples"]["periods_complete"]
    periods_remaining = pred["samples"]["periods_remaining"]

    for i in range(periods_remaining):
        upcoming_period = periods_complete + 1 + i
        if upcoming_period > 3:  # regulation only
            break
        h_p = home_per_period[i]
        a_p = away_per_period[i]
        period_total_samples = h_p + a_p
        period_margin_samples = h_p - a_p
        pkey = str(upcoming_period)
        # HR's NBA-style key for this period (used in odds["periods"])
        q_key = f"Q{upcoming_period}"
        bt_prefix = f"P{upcoming_period}"
        # Pull the periods-dict block (NBA-shape: home_ml, over_under, etc.)
        nba_block = periods_block.get(q_key) if isinstance(periods_block, dict) else None

        # Period total — try explicit period_totals first, fall back to
        # the periods[Qn] block.
        ptot = period_totals.get(pkey)
        p_line = p_over = p_under = None
        if isinstance(ptot, dict):
            p_line = ptot.get("line")
            p_over = ptot.get("over_odds")
            p_under = ptot.get("under_odds")
        elif isinstance(nba_block, dict):
            p_line = nba_block.get("over_under")
            p_over = nba_block.get("over_odds")
            p_under = nba_block.get("under_odds")
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

        # Period winner — DNB (ties push) preferred when present.
        # Falls back to plain ML in the periods[Qn] block (which doesn't
        # push on tie — assigns the tie to the away side via NBA
        # convention; we still price using the unconditional win prob).
        pdnb = period_dnbs.get(pkey)
        p_h_ml = p_a_ml = None
        ml_is_dnb = False
        if isinstance(pdnb, dict):
            p_h_ml = pdnb.get("home_ml")
            p_a_ml = pdnb.get("away_ml")
            ml_is_dnb = bool(p_h_ml and p_a_ml)
        if not (p_h_ml and p_a_ml) and isinstance(nba_block, dict):
            p_h_ml = nba_block.get("home_ml")
            p_a_ml = nba_block.get("away_ml")
        if p_h_ml and p_a_ml:
            # Period winner. The DNB variant pushes on tie → conditional
            # P(home wins | not tied); the plain ML variant can't push
            # in HR's NBA-shaped block, so we price raw P(home wins).
            #
            # Bet_type label dodges "DNB" intentionally — the prior call
            # (engine.nhl_derivative_picks line 299) was that "DNB" reads
            # like "doesn't score" to non-bettor eyes and a correct W
            # got questioned. Use "Winner" for live picks and let the
            # settler's existing endswith("WINNER") branch resolve it.
            home_wins_period = float((period_margin_samples > 0).mean())
            away_wins_period = float((period_margin_samples < 0).mean())
            if ml_is_dnb:
                non_tied = home_wins_period + away_wins_period
                if non_tied > 0.0:
                    h_prob = home_wins_period / non_tied
                    a_prob = away_wins_period / non_tied
                else:
                    h_prob = a_prob = 0.5
            else:
                h_prob = home_wins_period
                a_prob = away_wins_period
            bt_kind = "Winner"
            h_imp = _market_implied(p_h_ml)
            a_imp = _market_implied(p_a_ml)
            if h_imp is not None:
                candidates.append({
                    "bet_type": f"{bt_prefix} {bt_kind}",
                    "pick": home_abbr,
                    "odds": int(p_h_ml),
                    "model_prob": h_prob,
                    "edge_pct": (h_prob - h_imp) * 100,
                })
            if a_imp is not None:
                candidates.append({
                    "bet_type": f"{bt_prefix} {bt_kind}",
                    "pick": away_abbr,
                    "odds": int(p_a_ml),
                    "model_prob": a_prob,
                    "edge_pct": (a_prob - a_imp) * 100,
                })

        # Period BTS (both teams to score in the period)
        pbts = period_btss.get(pkey)
        if isinstance(pbts, dict):
            yes_odds = pbts.get("yes_odds")
            no_odds = pbts.get("no_odds")
            if yes_odds and no_odds:
                p_yes = float(((h_p > 0) & (a_p > 0)).mean())
                p_no = 1.0 - p_yes
                y_imp = _market_implied(yes_odds)
                n_imp = _market_implied(no_odds)
                if y_imp is not None:
                    candidates.append({
                        "bet_type": f"{bt_prefix} BTS",
                        "pick": "Yes",
                        "odds": int(yes_odds),
                        "model_prob": p_yes,
                        "edge_pct": (p_yes - y_imp) * 100,
                    })
                if n_imp is not None:
                    candidates.append({
                        "bet_type": f"{bt_prefix} BTS",
                        "pick": "No",
                        "odds": int(no_odds),
                        "model_prob": p_no,
                        "edge_pct": (p_no - n_imp) * 100,
                    })

    if not candidates:
        return

    candidates.sort(key=lambda c: -c["edge_pct"])
    # Edge floors: 4% for ML/PL/Winner/BTS, 12% for TOTAL. Period markets
    # reuse the same floors after stripping the period prefix.
    floor_for = {"ML": 4.0, "PL": 4.0, "Winner": 4.0, "BTS": 4.0,
                  "TOTAL": 12.0}

    def _floor_for_bet_type(bt: str) -> float:
        for prefix in ("P1 ", "P2 ", "P3 "):
            if bt.startswith(prefix):
                return floor_for.get(bt[len(prefix):], 4.0)
        return floor_for.get(bt, 4.0)

    # -200 juice wall — same as continuous live picks
    # (engine.live._picks.LIVE_ODDS_FLOOR). Refuses heavy chalk where
    # one variance event wipes multiple wins. VGK @ -475 with +6.1%
    # edge on 2026-04-30 was the live-fire case that proved this gate
    # was missing from the original intermission emit.
    from ._picks import LIVE_ODDS_FLOOR

    emitted = 0
    for c in candidates:
        floor = _floor_for_bet_type(c["bet_type"])
        if c["edge_pct"] < floor:
            continue
        if c["odds"] < 0 and c["odds"] < LIVE_ODDS_FLOOR:
            continue
        # Belief gate — same fix as NBA intermission predictor (see
        # _nba_intermission_predict). Live continuous picker already
        # enforces this via engine.live._picks; the period predictor
        # was the bypass.
        if c["model_prob"] < 0.50:
            logger.info(
                "NHL intermission pick REJECTED by belief gate: "
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
                "NHL intermission pick recorded id=%s P%s %s '%s' @ %+d "
                "(edge %+.1f%% prob %.3f)",
                new_id, pred["period_ended"], c["bet_type"], c["pick"],
                c["odds"], c["edge_pct"], c["model_prob"],
            )
        except Exception as e:
            logger.warning("NHL intermission record failed: %s", e)
            continue
        emitted += 1
        if emitted >= 3:
            break


def _remaining_seconds_nhl(periods_complete: int) -> int | None:
    """20-min periods. Returns the regulation seconds remaining at the
    intermission boundary."""
    if periods_complete >= 3:
        return 0
    return (3 - periods_complete) * 20 * 60


__all__ = [
    "summarize_pbp",
    "predict_intermission",
    "run_for_event",
]
