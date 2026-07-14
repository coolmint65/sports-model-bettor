"""Auto-eligible bet queue route.

Surfaces the ``engine.bet_queue.build_queue`` output to the frontend.
Read-only — the queue never places bets, it only recommends what to
place manually. The one-tap-to-HR flow lives on the frontend.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Query

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/bet-queue")
def api_bet_queue(min_n: int = Query(50, ge=1),
                    min_roi: float = Query(5.0),
                    max_units: float = Query(5.0, ge=0.1),
                    unit_dollars: float = Query(100.0, ge=1.0)) -> dict:
    """Return the auto-eligible queue for right now.

    Query params let the operator experiment with tighter / looser
    thresholds without a server restart. Defaults match the
    conservative production settings in ``engine.bet_queue``.
    """
    from engine.bet_queue import build_queue
    return build_queue(
        min_n=min_n,
        min_roi=min_roi,
        max_daily_units=max_units,
        unit_dollars=unit_dollars,
    )


@router.get("/api/bet-queue/tracker")
def api_bet_queue_tracker(min_n: int = Query(50, ge=1),
                            min_roi: float = Query(5.0),
                            unit_dollars: float = Query(100.0, ge=1.0),
                            limit_recent: int = Query(25, ge=1, le=200)
                            ) -> dict:
    """Historical performance for picks in currently-queue-eligible
    cells. Same threshold vocabulary as ``/api/bet-queue`` so the two
    endpoints agree on which cells qualify."""
    from engine.bet_queue import build_tracker
    return build_tracker(
        min_n=min_n,
        min_roi=min_roi,
        unit_dollars=unit_dollars,
        limit_recent=limit_recent,
    )
