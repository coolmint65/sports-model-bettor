"""NHL Pick Authority market anchor (MintyBot 2026-10-26, Austin "Let's make sure NHL is up to
snuff") -- the NBA twin (engine.sports.nba._market_anchor).

Every NHL candidate's model probability is pulled toward the de-vigged market:

    p_final = W * p_model + (1 - W) * p_market_devig

and that anchor is the ONLY calibration (engine.calibration_gate.anchor_only skips score_pick's
empirical / blend / final / derivative stages for "nhl" -- they had stacked four maps on the
old model, e.g. a puck line .595 -> .661 -> .627 -> .54). W is the Pick Authority's per-cell
weight, edge_proven.market_weight("nhl", "nhl", bet_type, pick, odds): 0.50 once that market
side has PROVEN an edge, 0.15 until then. Inside shadow_mode() the anchor is skipped (W=1): the
shadow lane records the ratings model's own number for every candidate so unproven markets can
earn their way onto the card. Three-way markets (regulation / period result) de-vig all three
prices together.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager

FALLBACK_W = 0.15
_TL = threading.local()


@contextmanager
def shadow_mode():
    """Price every candidate on the model's own probability (W=1). Thread-local, so the
    parallel best-bets workers don't leak into each other."""
    prev = getattr(_TL, "shadow", False)
    _TL.shadow = True
    try:
        yield
    finally:
        _TL.shadow = prev


def in_shadow() -> bool:
    return bool(getattr(_TL, "shadow", False))


def weight(bet_type=None, pick=None, odds=None) -> float:
    """The model's blend weight for this candidate (1.0 in shadow mode)."""
    if in_shadow():
        return 1.0
    if bet_type:
        try:
            from ...selection.edge_proven import market_weight
            return float(market_weight("nhl", "nhl", bet_type, pick or "", odds))
        except Exception:
            pass
    return FALLBACK_W


def anchor(prob, side_odds, opp_odds, bet_type=None, pick=None):
    """Blend toward the de-vigged two-way market. Returns ``prob`` unchanged when the market
    can't be resolved (no anchor beats a wrong anchor)."""
    try:
        from ...market_blend import anchor_to_market
        return anchor_to_market(prob, side_odds, opp_odds, weight(bet_type, pick, side_odds))
    except Exception:
        return prob


def anchor_multi(probs, odds_list, bet_type=None, picks=None):
    """N-way market (e.g. home / tie / away): de-vig all prices together, then blend each
    outcome with its own weight. Unresolvable market -> probs unchanged."""
    try:
        from ...market_blend import devig_multiway, anchor_prob
        dv = devig_multiway(list(odds_list))
        if dv is None:
            return list(probs)
        picks = picks or [None] * len(probs)
        return [anchor_prob(p, d, weight(bet_type, pk, o))
                for p, d, pk, o in zip(probs, dv, picks, odds_list)]
    except Exception:
        return list(probs)
