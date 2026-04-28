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
    """Mutate each pick in-place to add a `confidence` field based on
    edge thresholds in engine.config (EDGE_STRONG / MODERATE / LEAN /
    SKIP).

    Picks below EDGE_SKIP are tagged "skip" except for markets in
    `_EDGE_SKIP_EXEMPT` (currently 1st INN), which floor at "lean" so
    the user-facing surface keeps them visible. The dashboard filter
    drops "skip"-tier picks; engine consumers (settler, calibration)
    don't care about the label.

    No-op when `picks` is empty. Safe to call multiple times — each
    call recomputes from `edge` so re-calibration upstream is reflected.
    """
    from .config import EDGE_STRONG, EDGE_MODERATE, EDGE_LEAN, EDGE_SKIP
    for p in picks:
        e = p.get("edge") or 0
        if e >= EDGE_STRONG:
            p["confidence"] = "strong"
        elif e >= EDGE_MODERATE:
            p["confidence"] = "moderate"
        elif e >= EDGE_LEAN:
            p["confidence"] = "lean"
        else:
            p["confidence"] = "skip"
        if e < EDGE_SKIP and p.get("type") not in _EDGE_SKIP_EXEMPT:
            p["confidence"] = "skip"
        if p.get("type") in _EDGE_SKIP_EXEMPT and p["confidence"] == "skip":
            p["confidence"] = "lean"


def passes_floor(sport: str, bet_type: str, edge: float,
                 floors: dict | None = None) -> bool:
    """Return True when `edge` clears the per-market floor for
    (sport, bet_type). When `floors` is omitted, reads MAIN_EDGE_FLOOR
    from engine.config. Markets without an explicit floor pass freely
    — the global EDGE_LEAN gate in tag_confidence() catches them."""
    if floors is None:
        from .config import MAIN_EDGE_FLOOR
        floors = MAIN_EDGE_FLOOR.get(sport, {})
    floor = floors.get(bet_type)
    return floor is None or float(edge) >= float(floor)


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
