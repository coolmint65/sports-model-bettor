"""Grade placed queue-placer rows after the underlying event resolves.

Walks placements with status='placed' AND result IS NULL AND mode='live',
computes W/L/P/V via per-sport graders, and writes result +
profit_dollars + settled_at back to the placements table. Only live-fire
rows are considered — dry_run and rejected rows are terminal and don't
carry a stake to grade.

Tennis is currently the only sport with a grader. Add MLB/NBA/NHL/etc.
to ``_SPORT_GRADERS`` as those markets start firing through the placer.

TODO: switch to HR ticket-status lookup once PiBot's relay exposes
``GET /ticket-status?hr_ticket_id=...``. HR's own settlement handles
voids, cash-outs, and partial fills correctly; match-result grading can
miss those edge cases (e.g. HR voiding a bet after the player retires
mid-first-set — WIN_AT_LEAST_ONE_SET would grade the retiring player's
Yes as W here even though HR paid it as V).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable

from ._schema import get_conn

logger = logging.getLogger(__name__)


def _american_profit(result: str, stake_dollars: float,
                      american_odds: int) -> float:
    if result == "W":
        if american_odds > 0:
            return stake_dollars * (american_odds / 100.0)
        return stake_dollars * (100.0 / abs(american_odds))
    if result == "L":
        return -stake_dollars
    return 0.0


def _refresh_tennis_te_window() -> None:
    """Pull the last 5 days of TennisExplorer results before grading.

    Mirrors settle_tennis_picks()'s pre-flight so our settler doesn't
    depend on the tennis-picks cadence having already run.
    """
    try:
        from datetime import date, timedelta
        from scrapers.tennis_results import fetch_results_for_date, store_results
        today = date.today()
        for back in range(0, 5):
            d = (today - timedelta(days=back)).isoformat()
            try:
                rows = fetch_results_for_date(d)
                if rows:
                    store_results(rows)
            except Exception as e:
                logger.warning("TE results fetch %s failed: %s", d, e)
    except Exception as e:
        logger.warning("TE results fetch chain crashed: %s", e)


def _grade_tennis(row: dict) -> tuple[str, dict] | None:
    """Match a tennis placement to a tennis_match_results row and grade
    it via the tennis tracker's existing resolver.

    Returns (result, match_meta) on hit, None if the match hasn't
    resolved yet (row stays pending for the next sweep).
    """
    from ..tennis_tracker import _match_from_results_table, _resolve_pick
    from ..tennis_db import get_conn as get_tennis_conn

    matchup = row.get("matchup") or ""
    if " vs " not in matchup:
        return None

    tennis_conn = get_tennis_conn()
    adapted_pick = {
        "matchup": matchup,
        "date": (row.get("queued_at") or "")[:10],
        "tour": None,
        "p1_id": 0,
        "p2_id": 0,
    }
    match = _match_from_results_table(tennis_conn, adapted_pick)
    if not match:
        return None

    pick_row = {
        "bet_type": row.get("bet_type"),
        "pick": row.get("pick"),
        "p1_id": 0,
        "p2_id": 0,
    }
    result = _resolve_pick(pick_row, match)
    if result is None:
        return None
    return result, {"score": match.get("score"),
                     "winner": match.get("winner_name")}


_SPORT_GRADERS: dict[str, Callable[[dict], tuple[str, dict] | None]] = {
    "tennis": _grade_tennis,
}


def settle_placements(sport: str | None = None,
                       max_rows: int = 500) -> dict:
    """Grade pending placer placements.

    Returns per-sport summary dict.
    """
    conn = get_conn()
    where = ("status = 'placed' AND mode = 'live' AND result IS NULL "
             "AND hr_ticket_id IS NOT NULL")
    args: list[Any] = []
    if sport:
        where += " AND sport = ?"
        args.append(sport)
    pending = conn.execute(
        f"SELECT * FROM placements WHERE {where} "
        "ORDER BY queued_at ASC LIMIT ?",
        (*args, max_rows),
    ).fetchall()

    summary: dict[str, dict] = {}
    if not pending:
        return summary

    tennis_pending = any((dict(r).get("sport") == "tennis") for r in pending)
    if tennis_pending:
        _refresh_tennis_te_window()

    now_iso = datetime.now(timezone.utc).isoformat()
    for row in pending:
        d = dict(row)
        s = d.get("sport") or ""
        bucket = summary.setdefault(s, {
            "settled": 0, "wins": 0, "losses": 0, "pushes": 0,
            "voids": 0, "pending_remaining": 0, "unsupported": 0,
        })
        grader = _SPORT_GRADERS.get(s)
        if grader is None:
            bucket["unsupported"] += 1
            continue
        try:
            graded = grader(d)
        except Exception as e:
            logger.warning(
                "queue placer settle: grader crash for id=%s sport=%s: %s",
                d.get("id"), s, e,
            )
            bucket["pending_remaining"] += 1
            continue
        if graded is None:
            bucket["pending_remaining"] += 1
            continue
        result, meta = graded
        stake = float(d.get("placed_stake_d")
                       or d.get("requested_stake_d") or 0.0)
        odds = int(d.get("placed_odds") or d.get("requested_odds") or 0)
        profit = _american_profit(result, stake, odds) if odds else 0.0
        conn.execute(
            "UPDATE placements SET result = ?, profit_dollars = ?, "
            "settled_at = ? WHERE id = ?",
            (result, profit, now_iso, d.get("id")),
        )
        bucket["settled"] += 1
        if result == "W":
            bucket["wins"] += 1
        elif result == "L":
            bucket["losses"] += 1
        elif result == "P":
            bucket["pushes"] += 1
        elif result == "V":
            bucket["voids"] += 1
        logger.info(
            "queue placer settled id=%s %s %s %s @ %+d → %s (profit=$%.2f)",
            d.get("id"), s, d.get("bet_type"), d.get("pick"),
            odds, result, profit,
        )
    return summary
