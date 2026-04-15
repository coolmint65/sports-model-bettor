"""
Shared confidence-interval helper.

The MLB model already computes a CI half-width on win probability
driven by pitcher-start + team-game sample sizes (engine.mlb_factors.
_prob_ci_half_width). NHL and NBA don't have per-matchup "expert
samples" like pitcher starts, but they do have team-game samples that
drive the same data-quality uncertainty. This module exposes a single
helper both sports can call so the three prediction surfaces render
compatible histogram bands.

The half-width represents our uncertainty in the POINT estimate of
the win probability -- not the game's inherent variance (which is
dominated by the score distribution). Small N -> wide band, asymptote
at ~2pp once the team has played roughly a season's worth of games.
"""

from math import sqrt


# Minimum half-width so the rendered band always has some visible
# thickness. The smallest reasonable claim for a single-game prediction.
_MIN_HALF_WIDTH = 0.02

# Maximum half-width. Early-season / no-data cases should still return
# a sane number -- 20pp is wide enough to signal "we barely know".
_MAX_HALF_WIDTH = 0.20


def ci_half_width(n_samples: int) -> float:
    """Estimate half-width of a 95% CI on the win-probability point estimate.

    Uses a sqrt(N) curve calibrated so 82 games (a full NHL regular
    season) lands near the floor of 2pp and 0 games returns the 20pp
    ceiling. Between those endpoints the drop is concave, matching
    the intuition that the first 20 games of data are worth more than
    the 60th through 80th.
    """
    n = max(int(n_samples or 0), 0)
    if n <= 0:
        return _MAX_HALF_WIDTH
    # 0.30 / sqrt(N) matches the MLB helper's shape. At N=82 -> ~0.033;
    # clamped down to 0.02 by the _MIN floor. At N=10 -> ~0.095.
    raw = 0.30 / sqrt(n)
    return round(max(_MIN_HALF_WIDTH, min(_MAX_HALF_WIDTH, raw)), 4)


def ci_band(prob: float, half_width: float) -> tuple[float, float]:
    """Return (low, high) clamped to [0, 1] for a given point + half-width."""
    if prob is None:
        return (0.0, 1.0)
    return (round(max(0.0, prob - half_width), 4),
            round(min(1.0, prob + half_width), 4))


def parse_record_games(record: str | None) -> int:
    """Pull total games from a record string.

    Handles NHL ``"32-15-4"`` (W-L-OTL), MLB ``"48-52"`` (W-L), and
    NBA ``"41-41"`` (W-L) formats. Returns 0 on unrecognised input.
    """
    if not record:
        return 0
    try:
        parts = str(record).split("-")
        return sum(int(p) for p in parts if p.strip().lstrip("+-").isdigit())
    except Exception:
        return 0
