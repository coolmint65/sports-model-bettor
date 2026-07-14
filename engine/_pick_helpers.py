"""Shared pick-shaping helpers.

Three idioms that previously appeared verbatim in engine/picks.py,
engine/nhl_picks.py, engine/nba_picks.py, engine/nba_q1_predict.py and
twice inside backend/server.py — extracted so a future tweak (e.g.
shifting EDGE_LEAN from 4.0 to 3.5) lands in one place instead of four.

The audit on 2026-04-28 flagged confidence-tagging as duplicated 4×,
JUICE_WALL gating as ~72×, and floor/cap gating as scattered. This
module covers the first two cleanly. JUICE_WALL is per-call — keep
the inline `if odds >= JUICE_WALL` checks because they're tied to
local odds variables; centralizing would require passing odds through
a helper for marginal benefit.
"""

from __future__ import annotations
from typing import Iterable


# Markets exempt from the global EDGE_SKIP cutoff. They run their own
# per-market floor inside the picker (MLB_NRFI_MIN_EDGE etc.) and the
# user wants 0.5-1% picks to surface as coin-flip data collection.
_EDGE_SKIP_EXEMPT = frozenset({"1st INN"})


def tag_confidence(picks: Iterable[dict]) -> None:
    """Mutate each pick in-place to add a ``confidence`` field — now
    prob-led under "be right, not edge-first" (#176, 2026-05-03).

    Tier mapping by calibrated ``prob`` (with edge as a small kicker
    for picks that are confident AND have a thick edge)::

        strong:   prob >= 0.65 OR (prob >= 0.58 AND edge >= EDGE_STRONG)
        moderate: prob >= 0.58 OR (prob >= 0.54 AND edge >= EDGE_MODERATE)
        lean:     prob >= 0.52
        skip:     otherwise

    Picks below the lean threshold are tagged "skip" except for
    markets in ``_EDGE_SKIP_EXEMPT`` (currently 1st INN), which floor
    at "lean" so the user-facing surface keeps them visible.

    Why this changed: previous implementation used pure edge thresholds
    so an 80%-prob 4%-edge pick was tagged "lean" while a 55%-prob
    12%-edge pick was tagged "strong" — which inverted the conviction
    signal the user actually wanted. Edge stays as a tiebreaker for
    same-prob picks; primary differentiation is "how likely am I right".

    No-op when ``picks`` is empty. Safe to call multiple times — each
    call recomputes from ``prob`` + ``edge`` so re-calibration upstream
    is reflected.
    """
    from .config import EDGE_STRONG, EDGE_MODERATE
    for p in picks:
        prob = float(p.get("prob") or 0)
        edge = float(p.get("edge") or 0)
        if prob >= 0.65 or (prob >= 0.58 and edge >= EDGE_STRONG):
            p["confidence"] = "strong"
        elif prob >= 0.58 or (prob >= 0.54 and edge >= EDGE_MODERATE):
            p["confidence"] = "moderate"
        elif prob >= 0.52:
            p["confidence"] = "lean"
        else:
            p["confidence"] = "skip"
        if p.get("type") in _EDGE_SKIP_EXEMPT and p["confidence"] == "skip":
            p["confidence"] = "lean"
        # Stake recommendation via Quarter-Kelly (calibration-aware).
        # The ``odds`` field carries the American line — required for
        # the Kelly fraction calculation. Standardized units across
        # every sport so tracker P/L numbers (which assume $100 = 1u)
        # remain coherent.
        p["stake_units"] = stake_units_for(prob, edge, p.get("confidence"),
                                             odds=p.get("odds"))


def stake_units_for(prob: float, edge: float,
                     confidence: str | None = None,
                     odds: int | None = None) -> float:
    """Map calibrated (prob, edge, odds) to a unit stake.

    Quarter-Kelly with a 1.0u hard cap. Derived from the tracker audit
    on 2026-05-22: the old ladder (which went up to 1.5u for prob≥0.65
    + edge≥10) shipped 452 "premium" picks at -1.13% ROI while the
    smallest 0.25u tier returned +10.10%. The model is systematically
    overconfident at the high end (predicted 77% → realized 52%), so
    sizing UP into those picks turned every winning small play into a
    losing big play.

    Sizing now goes through Quarter-Kelly off the CALIBRATED prob:
      f* = (b·p − q) / b   (full Kelly fraction)
      stake = clamp(0.25 · f*, 0, max_units_per_pick)

    Quarter-Kelly is the textbook risk-controlled variant (Thorp et al)
    — full Kelly assumes perfect calibration, which our 14-25pp gap at
    the high end shows we don't have. Quarter trades expected growth
    for variance protection.

    Mapping fraction → unit-tier (1u = 1% bankroll):
      f*·0.25 < 0.0025 → 0u    (don't bet, edge too thin for Kelly)
      0.0025-0.005     → 0.25u (lean)
      0.005-0.0125     → 0.5u  (moderate)
      0.0125-0.025     → 0.75u (strong)
      0.025+           → 1.0u  (max — premium tier removed 2026-05-22)

    The ``confidence`` arg is kept for API compatibility but ignored.
    """
    try:
        p = float(prob or 0)
        e = float(edge or 0)
    except (TypeError, ValueError):
        return 0.0
    if p < 0.52:
        return 0.0
    if e < 1.5:  # below break-even after juice, don't size up regardless of prob
        return 0.0
    # Stage-2 shrinkage REMOVED 2026-05-26 after Phase A calibration
    # refresh. The hard-coded shrinkage was added on 2026-05-22 because
    # the OLD calibrated probs still showed predicted 77% → realized 52%
    # gaps. After Phase A.1 (empirical_calibration refit on 11k-69k rows
    # per sport), the calibrated `prob` arriving here is already a
    # Bayesian-shrunk posterior — no need for a second hard-coded layer.
    # Double-shrinking was producing 0u on every chalky pick the user
    # would reasonably bet (ALT TOTAL Under at -200 with +13% raw edge
    # was getting shrunk to 0u even when the source calibration agreed
    # the edge was real). Defer to the empirical layer; if it's wrong,
    # the next walk-forward refit catches it.
    p_size = p
    # American → decimal odds. Default -110 when odds missing.
    o = odds if odds is not None else -110
    try:
        o = int(o)
    except (TypeError, ValueError):
        o = -110
    if o == 0:
        return 0.0
    decimal = (1 + 100.0/abs(o)) if o < 0 else (1 + o/100.0)
    b = decimal - 1
    if b <= 0:
        return 0.0
    q = 1.0 - p_size
    full_kelly = (b * p_size - q) / b
    if full_kelly <= 0:
        return 0.0
    quarter = full_kelly * 0.25
    # Map to discrete tier. Re-tiered 2026-05-26 after removing the
    # stage-2 shrinkage that was masking the natural Kelly distribution.
    # Without shrinkage, almost everything hit the top tier — user
    # wanted gradation back (0.25u / 0.5u / 0.75u / 1.0u). New
    # thresholds put 1u at ≥ 7% quarter-Kelly (≈ 7% bankroll), which
    # only fat-edge +money or large-favorite-flat-juice picks reach.
    if quarter < 0.01:
        return 0.0
    if quarter < 0.02:
        return 0.25
    if quarter < 0.04:
        return 0.5
    if quarter < 0.07:
        return 0.75
    return 1.0


def filter_by_data_driven_floors(sport: str, picks: list) -> list:
    """Drop picks that fail the data-driven floor for their (bet_type,
    direction). Used by MLB / NHL pickers after their per-bet-type
    gates already passed — applies the historical-ROI override on
    top so cells that consistently lose ROI even at "above-floor"
    edges (MLB Unders, etc.) get filtered out before they ship.

    Static MAIN_EDGE_FLOOR enforcement should already be done by the
    picker's per-bet-type logic; this is the data-derived layer
    that NOPLAYs cells outright or raises the floor based on
    realized ROI per (bet_type, direction).
    """
    try:
        from .edge_floors import required_edge, NOPLAY_FLOOR
    except Exception:
        return picks
    out = []
    for p in picks:
        bt = p.get("type") or ""
        edge = float(p.get("edge") or 0.0)
        pick_text = p.get("pick")
        floor = required_edge(sport, bt, pick_text=pick_text, default=0.0)
        if floor >= NOPLAY_FLOOR:
            continue  # cell is banned
        if edge < floor:
            continue
        out.append(p)
    return out


def passes_floor(sport: str, bet_type: str, edge: float,
                 floors: dict | None = None,
                 pick_text: str | None = None) -> bool:
    """Return True when `edge` clears the per-market floor for
    (sport, bet_type, direction).

    Two-tier floor: the static ``MAIN_EDGE_FLOOR`` (engine.config) is
    the textbook 4% ML / 12% TOTAL baseline; the data-derived
    ``engine.edge_floors`` may raise it (or mark NOPLAY) for cells
    where realized ROI shows the static floor isn't conservative
    enough — e.g. MLB Unders historically bleed at -40% ROI even at
    "above floor" edges and now correctly fail every gate.

    ``pick_text`` lets the data-driven layer derive direction
    (over/under, nrfi/yrfi); when omitted, only the bet_type-level
    floor applies.
    """
    # Static floor first
    if floors is None:
        from .config import MAIN_EDGE_FLOOR
        floors = MAIN_EDGE_FLOOR.get(sport, {})
    static_floor = floors.get(bet_type)
    static_pass = static_floor is None or float(edge) >= float(static_floor)

    # Data-driven override on top
    try:
        from .edge_floors import required_edge, NOPLAY_FLOOR
        data_floor = required_edge(sport, bet_type, pick_text=pick_text,
                                     default=float(static_floor or 0.0))
        if data_floor >= NOPLAY_FLOOR:
            return False  # cell is banned — never pass regardless of edge
        return static_pass and float(edge) >= float(data_floor)
    except Exception:
        # If the edge_floors module isn't available / loaded, fall back
        # to static-only behavior — never silently fail open by raising.
        return static_pass


def passes_odds_cap(sport: str, bet_type: str, american_odds,
                    caps: dict | None = None) -> bool:
    """Return True when `american_odds` is within the per-market cap.
    Used to suppress longshot ML / SPREAD picks where calibration
    spikes correlate with mispriced +money lines (the trap that
    motivated MAIN_ODDS_CAP['ML']=400 across all three sports)."""
    if american_odds is None:
        return True
    if caps is None:
        from .config import MAIN_ODDS_CAP
        caps = MAIN_ODDS_CAP.get(sport, {})
    cap = caps.get(bet_type)
    if cap is None:
        return True
    try:
        return int(american_odds) <= int(cap)
    except (TypeError, ValueError):
        return True


def passes_edge_ceiling(sport: str, bet_type: str, edge: float,
                        ceilings: dict | None = None) -> bool:
    """Return True when `edge` is BELOW the per-market max-edge ceiling.

    Counter-intuitive guard for calibration-overshoot tails: certain
    (sport, bet_type) cells have shown that the higher the model's
    edge, the worse the actual ROI — the model gets more wrong as it
    gets more confident. Surfacing those picks costs money. Tracker
    autopsies populate `MAIN_EDGE_CEILING` with the breakpoints; picks
    above the ceiling get marked as failing this gate so the picker
    drops them.

    Markets without an explicit ceiling pass freely.
    """
    if ceilings is None:
        from .config import MAIN_EDGE_CEILING
        ceilings = MAIN_EDGE_CEILING.get(sport, {})
    ceiling = ceilings.get(bet_type)
    return ceiling is None or float(edge) < float(ceiling)
