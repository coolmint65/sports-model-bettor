"""Soccer Pick Authority market anchor (MintyBot 2026-10-28, Austin "EPL up to standard") -- the
NHL / NBA twin (engine.sports.nhl._market_anchor).

For a PA league (PA_LEAGUES, EPL only) every candidate's model probability is pulled toward the
de-vigged market:

    p_final = W * p_model + (1 - W) * p_market_devig

and that anchor is the ONLY calibration (engine.calibration_gate.anchor_only skips score_pick's
empirical / blend / final stages for the league key). W is the Pick Authority's per-cell weight,
edge_proven.market_weight("soccer", league, bet_type, pick, odds): 0.50 once that market side has
PROVEN an edge, 0.15 until then. Inside shadow_mode() the anchor is skipped (W=1) so the shadow lane
records the ratings model's own number and unproven markets can earn their way onto the card.
Three-way markets (1X2, double chance) de-vig all three prices together.

Walk-forward check of the EPL ridge model (Pi scratchpad epl/bt): blended with the opening 1X2 at
W .20-.50 it beats the open on log-loss (0.9986 at .35 vs 1.0009), so the .15 / .50 range brackets
where the model earns its say.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager

PA_LEAGUES = frozenset({"eng_premier"})
FALLBACK_W = 0.15
_TL = threading.local()


def is_pa(league) -> bool:
    return str(league or "") in PA_LEAGUES


@contextmanager
def shadow_mode():
    """Price every candidate on the model's own probability (W=1). Thread-local."""
    prev = getattr(_TL, "shadow", False)
    _TL.shadow = True
    try:
        yield
    finally:
        _TL.shadow = prev


def in_shadow() -> bool:
    return bool(getattr(_TL, "shadow", False))


def weight(league, bet_type=None, pick=None, odds=None) -> float:
    if in_shadow():
        return 1.0
    if bet_type:
        try:
            from ..selection.edge_proven import market_weight
            return float(market_weight("soccer", league, bet_type, pick or "", odds))
        except Exception:
            pass
    return FALLBACK_W


def anchor(league, prob, side_odds, opp_odds, bet_type=None, pick=None):
    """Two-way market. Unresolvable market -> prob unchanged (no anchor beats a wrong one)."""
    try:
        from ..market_blend import anchor_to_market
        return anchor_to_market(prob, side_odds, opp_odds, weight(league, bet_type, pick, side_odds))
    except Exception:
        return prob


def anchor_multi(league, probs, odds_list, bet_type=None, picks=None, target_sum=1.0):
    """N-way market (1X2: target_sum 1; double chance: 2). De-vig all prices together, then blend
    each outcome with its own weight. Unresolvable market -> probs unchanged."""
    try:
        from ..market_blend import devig_multiway, anchor_prob
        dv = devig_multiway(list(odds_list), target_sum)
        if dv is None:
            return list(probs)
        picks = picks or [None] * len(probs)
        return [anchor_prob(p, d, weight(league, bet_type, pk, o))
                for p, d, pk, o in zip(probs, dv, picks, odds_list)]
    except Exception:
        return list(probs)
