"""Shared pick scoring engine.

The four sport-specific pickers (engine/picks.py for MLB,
engine/nhl_picks.py, engine/nba_picks.py, engine/tennis_picks.py) all
ran the same defensive pipeline against each candidate pick:

  1. Juice wall (American odds floor; e.g. -200)
  2. Plus-odds cap (American odds ceiling; e.g. +400)
  3. Belief gate (model_prob >= 0.50)
  4. Empirical calibration (Bayesian posterior shrinkage)
  5. Blend calibration (sport-wide isotonic / Platt / passthrough)
  6. Edge calculation against implied
  7. Edge ceiling (per-bet-type — anomaly guard)
  8. Edge floor (data-driven minimum from historical ROI)

Each pipeline step was implemented separately in 4 files. Net result
of that duplication: the spread belief gate (#146) was missing in 4
places and we shipped picks where the model said the side LOSES.
Same shape bug eventually for every new gate we add.

This module hosts the universal pipeline. Per-sport pickers parse
their sport's market structure into ``PickCandidate`` dicts and call
``score_pick`` to run the gates in lockstep. Adding a new gate now
means one edit here, applied to every sport.

Public API:
   PickCandidate (dict shape — see _PICK_CANDIDATE_KEYS docstring)
   score_pick(candidate, sport, juice_wall) -> dict | None
   apply_post_filters(picks, sport) -> list[dict]
   rank(picks, sport) -> list[dict]
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ── Implied prob ──────────────────────────────────────────────

def _implied(odds: int | float) -> float:
    o = int(odds)
    if o < 0:
        return abs(o) / (abs(o) + 100)
    return 100.0 / (o + 100)


# ── Per-sport gate lookups ────────────────────────────────────

def _belief_gate_passes(raw_prob: float) -> bool:
    """Universal across sports + markets: don't pick a side the model
    expects to lose. See feedback_be_right_first.md principle #6."""
    return raw_prob >= 0.50


def _juice_wall_passes(odds: int, juice_wall: int) -> bool:
    """American-odds floor (most-negative). Same default (-200) across
    all sports — see config.{MLB,NHL,NBA,TENNIS}_JUICE_WALL."""
    return odds >= juice_wall


def _odds_cap_passes(sport: str, bet_type: str, odds: int) -> bool:
    """Plus-odds ceiling per (sport, bet_type). Tennis caps every
    bet_type via ``_default``; team sports cap ML at +400.

    Returns True for negative odds (cap is plus-side only)."""
    if odds <= 0:
        return True
    try:
        from .config import MAIN_ODDS_CAP
        sport_caps = MAIN_ODDS_CAP.get(sport) or {}
        cap = sport_caps.get(bet_type)
        if cap is None:
            cap = sport_caps.get("_default")
    except Exception:
        cap = None
    return cap is None or odds <= int(cap)


def _edge_ceiling_passes(sport: str, bet_type: str, edge_pct: float) -> bool:
    """Reject suspiciously huge claimed edges. For tennis ML at 15pp,
    NBA / NHL / MLB main markets at MAIN_EDGE_CEILING. The model has
    no business claiming +40pp edge on a Top-10 player vs market.

    When no per-(sport, bet_type) entry exists, falls through to
    DEFAULT_EDGE_CEILING_PCT instead of being uncapped — a fresh
    framework league with no calibration data could otherwise emit
    a +120% edge from a cold-start prob and pass every gate."""
    try:
        if sport == "tennis":
            from .config import TENNIS_EDGE_CEILING
            ceiling = TENNIS_EDGE_CEILING.get(bet_type)
        else:
            from .config import MAIN_EDGE_CEILING
            ceiling = (MAIN_EDGE_CEILING.get(sport) or {}).get(bet_type)
        if ceiling is None:
            from .config import DEFAULT_EDGE_CEILING_PCT
            ceiling = DEFAULT_EDGE_CEILING_PCT
    except Exception:
        ceiling = None
    return ceiling is None or float(edge_pct) < float(ceiling)


def _edge_floor_passes(sport: str, bet_type: str, edge_pct: float,
                        pick_text: str | None = None) -> bool:
    """Data-driven minimum edge per (sport, bet_type, direction).

    NOPLAY check moved to score_pick itself so banned cells emit as
    SHADOW picks (stake=0, recorded, tracked) instead of being
    discarded. The hard ban broke the learning loop — banned cells
    got no new picks → no settled data → empirical_calibration never
    saw evidence the cell improved → permanent dead zone. 2026-05-26
    user push: "is banning really the right call?" No — it isn't.
    """
    try:
        from .edge_floors import required_edge
        floor = required_edge(sport, bet_type, pick_text=pick_text)
        return edge_pct >= float(floor) and floor < 99.0
    except Exception:
        try:
            from .config import MIN_EDGE_PCT
            return edge_pct >= float(MIN_EDGE_PCT)
        except Exception:
            return edge_pct > 0


# ── The pipeline ──────────────────────────────────────────────

# Required keys on every PickCandidate. Optional keys (direction,
# extra) carry through but don't gate the pipeline.
_PICK_CANDIDATE_KEYS = (
    "type",       # bet_type, e.g. "ML", "SPREAD", "TOTAL"
    "pick",       # display text, e.g. "BOS -3.5", "Over 47.5"
    "raw_prob",   # pre-calibration probability the picked side wins
    "odds",       # American
)


def score_pick(candidate: dict[str, Any], *,
                sport: str, juice_wall: int) -> dict | None:
    """Run the universal pipeline on one candidate. Returns a complete
    pick dict, or None if any gate rejects.

    `candidate` shape:
        {type, pick, raw_prob, odds,                   # required
         direction (str|None), extra (dict, optional)} # optional

    Output dict mirrors the historical per-sport pick shape:
        {type, pick, prob, prob_raw, odds, edge,       # always
         + any keys from candidate.extra}              # passthrough

    Pipeline gates (any FAIL → return None):
      1. juice wall
      2. plus-odds cap
      3. belief gate (raw_prob >= 0.50)
      4. calibration (empirical → blend)
      5. edge > 0 against calibrated prob
      6. edge ceiling
      7. edge floor

    Gates run in cheap-first order so we short-circuit on the easy
    rejections.
    """
    bet_type = str(candidate["type"])
    pick_text = str(candidate["pick"])
    raw_prob = float(candidate["raw_prob"])
    odds = int(candidate["odds"])
    direction = candidate.get("direction")
    game_id = candidate.get("game_id")

    # Double-calibration guard. Earlier the basketball framework
    # pre-calibrated raw_prob via _calibration_apply, then picks_core
    # re-calibrated via framework_calibration — two-stage shrinkage
    # that pushed every high-conviction pick to the bucket-midpoint
    # cap. The pre-calibration step was removed, but if it ever comes
    # back this assertion fires loudly instead of silently double-shrinking.
    if candidate.get("_pre_calibrated"):
        raise ValueError(
            f"score_pick received a pre-calibrated candidate "
            f"({sport}/{bet_type}/{pick_text}). picks_core IS the "
            f"calibration step — don't pre-process the raw_prob."
        )

    # Provenance trace — built up at each stage so a rejected pick
    # carries full context (which gate failed, what intermediate
    # values were). Persisted at exit (accept or reject) so debugging
    # losing streaks is a query, not detective work.
    trace: dict[str, Any] = {
        "raw_prob": raw_prob,
        "odds": odds,
        "implied": _implied(odds),
        "juice_wall": juice_wall,
    }

    def _record(accepted: bool, reason: str | None,
                final_prob: float | None, final_edge: float | None) -> None:
        try:
            from .pick_provenance import record as _pp_record
            if reason:
                trace["reject_reason"] = reason
            _pp_record(
                sport=sport,
                bet_type=bet_type,
                pick_text=pick_text,
                trace=trace,
                accepted=accepted,
                final_prob=final_prob,
                final_edge=final_edge,
                game_id=str(game_id) if game_id is not None else None,
            )
        except Exception:
            pass

    if not _juice_wall_passes(odds, juice_wall):
        trace["juice_wall_passed"] = False
        _record(False, "juice_wall", None, None)
        return None
    trace["juice_wall_passed"] = True

    if not _odds_cap_passes(sport, bet_type, odds):
        trace["odds_cap_passed"] = False
        _record(False, "odds_cap", None, None)
        return None
    trace["odds_cap_passed"] = True

    if not _belief_gate_passes(raw_prob):
        trace["belief_gate_passed"] = False
        _record(False, "belief_gate", None, None)
        return None
    trace["belief_gate_passed"] = True

    # Calibration. Two stages — empirical (Bayesian beta-binomial,
    # see project_bayesian_calibration.md) then blend (sport-wide
    # isotonic / Platt). Both shrink toward truth as sample grows.
    from .empirical_calibration import (
        calibrate as _empirical, has_isotonic as _empirical_has_iso,
    )
    from .blend_calibration import calibrate as _blend
    from . import framework_calibration as _framework
    # Precedence:
    #   1. Empirical isotonic — if the live-picks table has n>=20 for
    #      this (sport, bet_type, sub-key) cell we have direct evidence
    #      and it's monotonic + continuous (no bucket collapse).
    #   2. Framework walk-forward bucket — historical replay; useful
    #      before enough live samples land.
    #   3. Empirical Beta-Binomial bucket (via _empirical fallback).
    cal_emp = float(raw_prob)
    if _empirical_has_iso(sport, bet_type, edge=None, odds=odds,
                            pick_text=pick_text):
        cal_emp = float(_empirical(bet_type, raw_prob, sport=sport,
                                    edge=None,
                                    odds=odds, pick_text=pick_text))
        trace["empirical_calibrated"] = round(cal_emp, 4)
        trace["calibrator"] = "empirical_isotonic"
    elif _framework.is_available(sport):
        cal_emp = float(_framework.calibrate(sport, raw_prob,
                                              bet_type=bet_type))
        trace["framework_calibrated"] = round(cal_emp, 4)
        trace["calibrator"] = "framework"
        # Framework only ships ML buckets today for most leagues —
        # when bet_type's bucket is missing it returns raw passthrough,
        # so let empirical (live picks-table beta-binomial) take over
        # rather than letting raw probabilities push edges to 30%+.
        if abs(cal_emp - float(raw_prob)) < 1e-9:
            cal_emp = float(_empirical(bet_type, raw_prob, sport=sport,
                                        edge=None,
                                        odds=odds, pick_text=pick_text))
            trace["empirical_calibrated"] = round(cal_emp, 4)
            trace["calibrator"] = "empirical_fallback"
    else:
        cal_emp = float(_empirical(bet_type, raw_prob, sport=sport,
                                    edge=None,
                                    odds=odds, pick_text=pick_text))
        trace["empirical_calibrated"] = round(cal_emp, 4)
        trace["calibrator"] = "empirical"
    cal = float(_blend(sport, cal_emp))
    trace["blend_calibrated"] = round(cal, 4)

    # Closing-line Bayesian prior — final stage. Treats market implied
    # as informed prior, model output as likelihood. Shrinks calibrated
    # prob toward market by an amount learned per (sport, bet_type)
    # from historical Brier on settled picks. Gated by config flag
    # ENABLE_CLOSING_LINE_PRIOR — defaults dormant until weights are
    # fitted (otherwise shrinks toward market with cold-start w=1.0
    # which is a no-op anyway). See engine/closing_line_prior.py.
    try:
        from .config import get_flag as _gf
        if _gf("ENABLE_CLOSING_LINE_PRIOR", False, sport=sport):
            from .closing_line_prior import (
                learned_weight as _clp_w,
                posterior_prob as _clp_post,
            )
            w = _clp_w(sport, bet_type)
            cal_post = float(_clp_post(cal, _implied(odds), w))
            trace["closing_line_weight"] = w
            trace["closing_line_posterior"] = round(cal_post, 4)
            cal = cal_post
    except Exception as exc:
        logger.debug("closing_line_prior skipped (%s)", exc)

    edge = (cal - _implied(odds)) * 100.0
    trace["edge_pct"] = round(edge, 2)
    if edge <= 0:
        _record(False, "edge_negative", cal, edge)
        return None
    if not _edge_ceiling_passes(sport, bet_type, edge):
        trace["edge_ceiling_passed"] = False
        _record(False, "edge_ceiling", cal, edge)
        return None
    trace["edge_ceiling_passed"] = True
    # Edge floor gate. 2026-07-03: NOPLAY tier removed — bleeding
    # cells stay live at a scaled stake instead of shadow=0. The gate
    # still enforces a minimum edge per cell (BLEED cells demand fat
    # edges before firing) which does the containment work NOPLAY used
    # to; the stake multiplier below sizes down cell exposure without
    # cutting the model off from new evidence.
    if not _edge_floor_passes(sport, bet_type, edge,
                               pick_text=pick_text):
        trace["edge_floor_passed"] = False
        _record(False, "edge_floor", cal, edge)
        return None
    trace["edge_floor_passed"] = True
    trace["shadow"] = False

    out = {
        "type": bet_type,
        "pick": pick_text,
        "prob": round(cal, 4),
        "prob_raw": round(raw_prob, 4),
        "odds": odds,
        "edge": round(edge, 1),
    }
    # Universal stake-units recommendation. Computed here so every pick
    # path that uses score_pick — props, derivatives, outrights, ML,
    # spread, total, AH, BTTS, period, live, every sport — carries the
    # same ladder. Pickers that don't route through score_pick attach
    # it explicitly via _pick_helpers.stake_units_for. The edge_floors
    # stake_multiplier (0.10-1.00 based on cell's realized ROI) is
    # applied on top so bleed cells bet smaller without going to zero.
    try:
        from ._pick_helpers import stake_units_for
        kelly_stake = stake_units_for(cal, edge, odds=odds)
        try:
            from .edge_floors import stake_multiplier as _stk_mult
            mult = _stk_mult(sport, bet_type, pick_text=pick_text)
        except Exception:
            mult = 1.0
        out["stake_units"] = round(kelly_stake * mult, 3)
    except Exception:
        pass
    if direction is not None:
        out["direction"] = direction
    extra = candidate.get("extra") or {}
    for k, v in extra.items():
        if k not in out:
            out[k] = v
    _record(True, None, cal, edge)
    return out


# ── Post-filters + ranking ────────────────────────────────────

def apply_post_filters(picks: list[dict], sport: str) -> list[dict]:
    """Filters that need the WHOLE candidate set, not per-pick.
    Currently: data-driven floor wholesale (already inside score_pick
    today, but keeping the hook for batch logic that may need it
    later, e.g. cross-pick deduplication)."""
    return picks


def rank(picks: list[dict], sport: str) -> list[dict]:
    """Rank picks by accuracy-first conviction.

    Per ``feedback_be_right_first`` (locked principle): the model's
    purpose is accurate prediction, not edge-chasing. Among picks that
    already cleared the edge floor (so all have positive EV), the
    headline goes to the most-likely-to-win, not the largest-edge.

    Primary sort key: ``conviction = prob × dynamic_reliability``.
    A 0.80-prob pick at +5% edge ranks above a 0.65-prob pick at
    +12% edge — full stop. Edge stays as a TIEBREAKER for picks
    with effectively-equal conviction.

    Backward-compat: ``adjusted_ev = edge × reliability`` is still
    written to each pick so any frontend that reads it doesn't break.
    Just no longer the sort key.
    """
    if not picks:
        return picks
    try:
        from .dynamic_reliability import get_reliability
        for p in picks:
            rel = get_reliability(sport, p["type"])
            prob = float(p.get("prob") or 0)
            edge = float(p.get("edge") or 0)
            p["conviction_score"] = round(prob * rel, 4)
            # adjusted_ev kept for backward compat (frontend / older
            # consumers may display it); not the sort key anymore.
            p["adjusted_ev"] = round(edge * rel, 2)
        # Primary: conviction descending. Tiebreaker: edge descending.
        picks.sort(key=lambda p: (-p["conviction_score"], -(p.get("edge") or 0)))
    except Exception as e:
        logger.warning("rank: reliability weighting failed (%s) "
                        "— sorting by raw prob", e)
        picks.sort(key=lambda p: (-(p.get("prob") or 0),
                                   -(p.get("edge") or 0)))
    try:
        from ._pick_helpers import tag_confidence
        tag_confidence(picks)
    except Exception as e:
        logger.warning("rank: tag_confidence failed: %s", e)
    return picks


__all__ = [
    "score_pick", "apply_post_filters", "rank",
]
