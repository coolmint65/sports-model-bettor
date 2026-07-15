"""Grade placed queue-placer rows after the underlying event resolves.

Primary path: HR's own settlement via the relay's POST /history
(→ /mybets/closed on api.hardrocksportsbook.com). HR is authoritative
for won/lost/void/cashout — no ESPN score re-derivation, no missing
retirement-void edge cases.

Fallback: per-sport match-result graders (tennis wired). Used when a
placed ticket hasn't yet appeared in HR's /mybets/closed (HR's history
can lag placement by minutes to hours) OR when the relay is unreachable.
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


# HR splits settle state across two fields:
#   betStatus       — coarse state: SETTLED (win or loss), VOID, PENDING.
#   displayStatus   — fine-grained outcome for settled bets: WIN, LOSE,
#                     PUSH, HALF_WIN, HALF_LOSE.
#
# Grading rule (from live payload inspection 2026-07-14):
#   betStatus=VOID     → V   (ignore displayStatus; refund is on the bet)
#   betStatus=SETTLED  → look at displayStatus → W/L/P
#   anything else      → None (pending / unknown → row stays open)
_HR_DISPLAY_MAP: dict[str, str] = {
    "WIN":         "W",
    "WON":         "W",
    "HALF_WIN":    "W",
    "LOSE":        "L",
    "LOST":        "L",
    "LOSS":        "L",
    "HALF_LOSE":   "L",
    "PUSH":        "P",
    "PUSHED":      "P",
    "CASHED_OUT":  "W",
    "CASHOUT":     "W",
}


def _read_status(bet: dict, *keys: str) -> str:
    """Case-insensitive fetch — returns UPPER + '_'-normalized string."""
    for k in keys:
        v = bet.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().upper().replace(" ", "_")
    return ""


def _read_float(bet: dict, *keys: str) -> float:
    """Fetch the first numeric-coercible field; supports both top-level
    numbers and {amount: N} nested shapes."""
    for k in keys:
        v = bet.get(k)
        if v is None:
            continue
        if isinstance(v, dict):
            v = v.get("amount")
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def _hr_extract_settle(bet: dict) -> tuple[str, float] | None:
    """From an HR bet object, extract (result, profit_dollars).

    Live payload shape observed 2026-07-14:
      WIN     — betStatus=SETTLED, displayStatus=WIN,   payout=<total>
      LOSS    — betStatus=SETTLED, displayStatus=LOSE
      VOID    — betStatus=VOID,    refund=<stake>
    """
    if not isinstance(bet, dict):
        return None

    bet_status = _read_status(bet, "betStatus", "status")
    if bet_status == "VOID":
        result = "V"
    elif bet_status == "SETTLED":
        display = _read_status(bet, "displayStatus", "outcome", "result")
        result = _HR_DISPLAY_MAP.get(display)
        if result is None:
            return None  # SETTLED with unrecognized displayStatus
    else:
        return None  # PENDING / unknown → leave row open

    stake = _read_float(bet, "stake", "stakeAmount", "risk", "wagerAmount")
    # HR ships payout as a top-level number (payout / totalPayout), not
    # a {amount} dict — _read_float handles both.
    payout = _read_float(bet, "payout", "totalPayout",
                          "winnings", "returnAmount", "netPayout",
                          "payoutAmount")
    refund = _read_float(bet, "refund", "refundAmount")

    if result == "W":
        # payout = stake + profit for winners. Fall through to 0 if HR
        # didn't ship a payout; the caller then uses the odds fallback.
        profit = (payout - stake) if payout > 0 else 0.0
    elif result == "L":
        profit = -stake
    elif result == "V":
        # HR refunds stake on voids — net profit is 0. If refund<stake
        # (partial void), signal by returning the actual delta.
        profit = (refund - stake) if refund > 0 else 0.0
    else:  # P (push)
        profit = 0.0
    return result, profit


def _hr_history_snapshot() -> dict[str, tuple[str, float]]:
    """Pull HR's settled-bets history and index by hr_ticket_id.

    Empty dict on any error — settler falls back to per-sport graders.
    """
    from . import _relay
    try:
        bets = _relay.history_closed(days_back=30)
    except Exception as e:
        logger.warning("HR history fetch crashed: %s", e)
        return {}
    snapshot: dict[str, tuple[str, float]] = {}
    for bet in bets:
        # Ticket id — HR field name observed variations: id, betId,
        # ticketId. Placer stores whatever HR returned in placeBets, so
        # we probe the same names on the readback side.
        ticket_id = None
        for k in ("id", "betId", "ticketId"):
            v = bet.get(k)
            if v is not None:
                ticket_id = str(v)
                break
        if not ticket_id:
            continue
        settled = _hr_extract_settle(bet)
        if settled is not None:
            snapshot[ticket_id] = settled
    return snapshot


def _refresh_tennis_te_window() -> None:
    """Pull the last 5 days of TennisExplorer results before fallback
    grading. Mirrors settle_tennis_picks()'s pre-flight so the fallback
    path works even if the tennis-picks cadence hasn't run recently.
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
    """Fallback grader: match a tennis placement to tennis_match_results
    and grade via the tennis tracker's existing resolver. Used only
    when HR's /mybets/closed doesn't yet carry the ticket.

    Returns (result, meta) on hit, None to leave the row pending.
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

    Tries HR's authoritative settlement first (via relay POST /history →
    /mybets/closed). Rows not yet in HR's history fall back to per-sport
    match-result graders where available.

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

    # 1) HR-native snapshot — one relay round trip covers every open
    #    ticket regardless of sport.
    hr_snapshot = _hr_history_snapshot()

    # 2) Fallback pre-flight (only if any tennis row is pending AND its
    #    ticket isn't in the HR snapshot).
    tennis_pending_fallback = any(
        (dict(r).get("sport") == "tennis"
         and str(dict(r).get("hr_ticket_id") or "") not in hr_snapshot)
        for r in pending
    )
    if tennis_pending_fallback:
        _refresh_tennis_te_window()

    now_iso = datetime.now(timezone.utc).isoformat()
    for row in pending:
        d = dict(row)
        s = d.get("sport") or ""
        bucket = summary.setdefault(s, {
            "settled_hr": 0, "settled_fallback": 0,
            "wins": 0, "losses": 0, "pushes": 0, "voids": 0,
            "pending_remaining": 0, "unsupported": 0,
        })
        stake = float(d.get("placed_stake_d")
                       or d.get("requested_stake_d") or 0.0)
        odds = int(d.get("placed_odds") or d.get("requested_odds") or 0)

        result: str | None = None
        profit: float | None = None
        source: str = ""

        # Prefer HR-native settlement.
        ticket_id = str(d.get("hr_ticket_id") or "")
        if ticket_id and ticket_id in hr_snapshot:
            result, hr_profit = hr_snapshot[ticket_id]
            # If HR gave us a payout, use it; else compute from odds.
            profit = hr_profit if hr_profit else (
                _american_profit(result, stake, odds) if odds else 0.0
            )
            source = "hr"

        # Fallback to per-sport grader.
        if result is None:
            grader = _SPORT_GRADERS.get(s)
            if grader is None:
                bucket["unsupported"] += 1
                continue
            try:
                graded = grader(d)
            except Exception as e:
                logger.warning(
                    "queue placer settle: grader crash id=%s sport=%s: %s",
                    d.get("id"), s, e,
                )
                bucket["pending_remaining"] += 1
                continue
            if graded is None:
                bucket["pending_remaining"] += 1
                continue
            result, _meta = graded
            profit = _american_profit(result, stake, odds) if odds else 0.0
            source = "fallback"

        conn.execute(
            "UPDATE placements SET result = ?, profit_dollars = ?, "
            "settled_at = ? WHERE id = ?",
            (result, profit, now_iso, d.get("id")),
        )
        if source == "hr":
            bucket["settled_hr"] += 1
        else:
            bucket["settled_fallback"] += 1
        if result == "W":
            bucket["wins"] += 1
        elif result == "L":
            bucket["losses"] += 1
        elif result == "P":
            bucket["pushes"] += 1
        elif result == "V":
            bucket["voids"] += 1
        logger.info(
            "queue placer settled id=%s %s %s %s @ %+d → %s "
            "(profit=$%.2f, source=%s)",
            d.get("id"), s, d.get("bet_type"), d.get("pick"),
            odds, result, profit or 0.0, source,
        )
    return summary
