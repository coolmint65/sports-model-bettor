"""NBA market-anchoring weight + helper (Austin 2026-10-20; Pick Authority 2026-10-26).

Pulls every NBA pick's model probability toward the de-vigged market
(engine.market_blend.anchor_to_market):

    p_final = W * p_model + (1 - W) * p_market_devig

2026-10-26 (Austin "lets get to work on NBA for PA"): NBA now calibrates ONCE, like
football -- this anchor is the only correction (engine.calibration_gate.anchor_only skips
score_pick's empirical / final / derivative stages for "nba", which were stacking
non-monotonic bucket maps on top of it). W is the Pick Authority's per-cell weight,
edge_proven.market_weight(sport, league, bet_type, pick, odds): a market that has PROVEN
an edge keeps half its say (0.50), everything else defers hard to the market (0.15).
Inside shadow_mode() the anchor is skipped (W=1): the shadow lane records the MODEL's own
number for every candidate so unproven markets can earn their way onto the card.
NBA_MARKET_W is only the fallback when a call site can't name its market.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager

NBA_MARKET_W = 0.3
_TL = threading.local()


@contextmanager
def shadow_mode():
    """Price every candidate on the model's own probability (W=1) -- the Pick Authority
    shadow lane. Thread-local, so concurrent best-bets workers don't leak into each other."""
    prev = getattr(_TL, "shadow", False)
    _TL.shadow = True
    try:
        yield
    finally:
        _TL.shadow = prev


def weight(bet_type=None, pick=None, odds=None) -> float:
    """The model's blend weight for this candidate (1.0 in shadow mode)."""
    if getattr(_TL, "shadow", False):
        return 1.0
    if bet_type:
        try:
            from ...selection.edge_proven import market_weight
            return float(market_weight("nba", "nba", bet_type, pick or "", odds))
        except Exception:
            pass
    return NBA_MARKET_W


def anchor(prob, side_odds, opp_odds, w: float | None = None,
           bet_type: str | None = None, pick: str | None = None):
    """Blend a model prob toward the de-vigged two-way market. ``bet_type``/``pick``
    name the candidate so the Pick Authority weight applies. Returns ``prob``
    unchanged if the market can't be resolved (no anchor beats a wrong anchor)."""
    try:
        from ...market_blend import anchor_to_market
        if w is None:
            w = weight(bet_type, pick, side_odds)
        return anchor_to_market(prob, side_odds, opp_odds, w)
    except Exception:
        return prob
