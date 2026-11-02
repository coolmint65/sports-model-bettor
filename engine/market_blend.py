"""
engine/market_blend.py -- market de-vig anchoring (Workstream D, Austin 2026-10-14).

Anchors a model probability toward the vig-free (de-vigged) market probability so
an over-confident model that disagrees with the sharp line gets pulled back toward
it. The models are currently LOSING to the close (see the NHL audit: every bucket
~15pp over-confident, big dogs 6% win), so they must DEFER to the market and earn
the right to disagree -- the blend weight W is the MODEL's say (0..1); the rest is
the market. Fixed W for now; later, weight by the model's PROVEN edge (CLV) once
the closing-odds capture is fixed.

    p_final = W * p_model + (1 - W) * p_market_devig

Edge is then measured as p_final vs the bet's (vigged) implied, so a model that
only marginally disagrees with the line can no longer mint a fat edge.
"""
from __future__ import annotations

# Default model weight. 0.5 = equal say; lower leans harder on the market. Tunable
# as we watch results; the long-term target is a per-bucket CLV-proven weight.
DEFAULT_W = 0.5


def _implied(odds) -> float | None:
    """American odds -> implied probability (incl. the vig). None if unusable."""
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0 or abs(o) < 100:
        return None
    return abs(o) / (abs(o) + 100.0) if o < 0 else 100.0 / (o + 100.0)


def devig_prob(side_odds, opp_odds) -> float | None:
    """Vig-free probability of the ``side_odds`` outcome from the two-way market
    (side vs its opposite). Normalises the two implied probs to sum to 1 so the
    bookmaker's hold is removed. None if either price is unusable."""
    a = _implied(side_odds)
    b = _implied(opp_odds)
    if a is None or b is None:
        return None
    tot = a + b
    return (a / tot) if tot > 0 else None


def anchor_to_market(p_model, side_odds, opp_odds, w: float = DEFAULT_W):
    """Blend ``p_model`` toward the de-vigged market prob for its side. ``w`` is
    the model's weight. Returns ``p_model`` unchanged when the two-way market
    can't be resolved (no anchoring is better than a wrong anchor)."""
    try:
        p = float(p_model)
    except (TypeError, ValueError):
        return p_model
    dv = devig_prob(side_odds, opp_odds)
    if dv is None:
        return p
    w = max(0.0, min(1.0, float(w)))
    return w * p + (1.0 - w) * dv


def devig_multiway(odds_list, target_sum=1.0):
    """Vig-free probs for an N-way market (list of American odds). Normalizes the
    implied probs to ``target_sum`` -- 1.0 for mutually-exclusive outcomes (1X2),
    2.0 for double-chance (each of the 3 bets covers 2 of 3 outcomes). Returns a
    list of de-vigged probs, or None if any price is unusable. (MintyBot 2026-10-16)"""
    imps = [_implied(o) for o in odds_list]
    if any(i is None for i in imps):
        return None
    s = sum(imps)
    if s <= 0:
        return None
    return [i / s * target_sum for i in imps]


def anchor_prob(p_model, devig, w=DEFAULT_W):
    """Blend p_model toward a pre-computed de-vigged prob (e.g. one side of a 3-way
    from devig_multiway). No-op when devig is None."""
    if devig is None:
        return p_model
    try:
        p = float(p_model)
    except (TypeError, ValueError):
        return p_model
    w = max(0.0, min(1.0, float(w)))
    return w * p + (1.0 - w) * devig
