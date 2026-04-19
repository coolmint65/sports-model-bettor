"""Win-probability tail compression shared across sports.

The raw factor models (MLB Poisson on expected runs, NHL Poisson on
expected goals) are systematically over-confident at the extremes —
live-tracker data shows predicted 70%+ buckets hit 40-55% in reality.

Two shaping strategies we've tried:

1. Hard clamp: ``p = max(floor, min(cap, raw))`` -- blunt. Every raw
   prob above ``cap`` gets flattened to exactly ``cap``, so a 0.62
   matchup and a 0.88 matchup both read as ``cap`` downstream. The
   picks engine then sees identical edges against different prices
   and treats them the same. Worse, it inflates underdog win prob
   above its true talent level, which creates false +EV picks on
   mispriced dogs (e.g. a +225 dog whose real prob is 20% but whose
   post-clamp prob is ``1 - cap = 0.45`` -> fake 14% edge).

2. Soft compression (this module): below ``floor`` or above ``cap``
   the tail is squashed toward the boundary by a fixed factor, so
   ordering is preserved but the tail is pulled toward the empirical
   real-WR zone. With defaults (cap=0.58, compress=0.35):
       raw 0.50 -> 0.500 (unchanged)
       raw 0.58 -> 0.580 (at boundary)
       raw 0.65 -> 0.605 (0.58 + 0.07 * 0.35)
       raw 0.75 -> 0.640 (0.58 + 0.17 * 0.35)
       raw 0.90 -> 0.692 (0.58 + 0.32 * 0.35)
   A 0.85 favorite still reads as a stronger favorite than a 0.70
   favorite, but both end up clustered above ``cap`` where the
   empirical win rates actually live.

Used by engine.mlb_predict and engine.nhl_predict; NBA Q1 uses a
Normal CDF on margin / std so doesn't need tail compression.
"""


def compress_win_prob(raw: float, floor: float, cap: float,
                      compress: float = 0.35) -> float:
    """Soft-compress an over-confident win probability toward the
    empirical sweet spot.

    Below ``floor``: ``return max(0, floor - (floor - raw) * compress)``
    Above ``cap``:   ``return min(1, cap + (raw - cap) * compress)``
    Inside [floor, cap]: raw passes through unchanged.

    Args:
        raw: Poisson- or margin-derived win probability before shaping.
        floor: Lower boundary of the "trusted" band.
        cap:   Upper boundary of the "trusted" band.
        compress: Factor applied to the distance outside the band.
            0.0 = hard clamp, 1.0 = no compression, 0.35 = default
            (empirically chosen for MLB, seems to hold for NHL too).
    """
    if raw < floor:
        deficit = floor - raw
        return max(0.0, floor - deficit * compress)
    if raw > cap:
        excess = raw - cap
        return min(1.0, cap + excess * compress)
    return raw
