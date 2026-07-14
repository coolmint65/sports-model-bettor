"""Circuit breakers + cap enforcement for the queue placer.

Every decision the placer makes flows through `check_can_place(...)` —
one entry point, one place to audit. Returns `(ok, reason)` where
`reason` is a short slug the placement log persists.

Breakers implemented:
  - kill_switch          Env-var OR on-disk flag is off.
  - offline              HR_RELAY_URL not configured.
  - per_bet_cap          Requested stake > MAX_STAKE_PER_BET_DOLLARS.
  - daily_cap            Today's staked total would breach cap.
  - weekly_cap           This week's staked total would breach cap.
  - consecutive_loss     Last N placed bets were all L → halt.
  - daily_drawdown       Today's realized P/L is below the floor.
  - dedup                Same (game, market, selection) already placed
                         today.

The queue's own filter (n≥50, ROI≥5%) is the primary allowlist;
"cell drops out of queue → no bet appears here" is the primary drift
guard. This module handles execution-time safety.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any

from . import _config
from ._schema import get_conn
from ._dedup import dedup_key


def _today_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _week_start_iso() -> str:
    now = datetime.now()
    # Monday-based week
    return (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")


def _sum_staked_since(cutoff_iso: str) -> float:
    """Sum of stake_dollars actually placed since cutoff (excluding
    dry-run rows). Rejected placements don't count against the cap —
    only bets that actually left our account."""
    conn = get_conn()
    r = conn.execute(
        "SELECT COALESCE(SUM(placed_stake_d), 0) FROM placements "
        "WHERE mode = 'live' AND status IN ('placed', 'submitted') "
        "  AND DATE(queued_at) >= ?",
        (cutoff_iso,),
    ).fetchone()
    return float(r[0] or 0.0)


def _daily_realized_pnl() -> float:
    """Realized P/L for today's placed bets. Sum of profit_dollars for
    settled placements; pending placements count as unrealized zero."""
    conn = get_conn()
    r = conn.execute(
        "SELECT COALESCE(SUM(profit_dollars), 0) FROM placements "
        "WHERE mode = 'live' AND DATE(queued_at) = ? "
        "  AND result IN ('W', 'L', 'P')",
        (_today_iso(),),
    ).fetchone()
    return float(r[0] or 0.0)


def _consecutive_recent_losses() -> int:
    """Count how many of the most-recent settled placements are L in
    a row, walking backward from the newest. Stops at first non-L."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT result FROM placements "
        "WHERE mode = 'live' AND result IN ('W', 'L', 'P') "
        "ORDER BY settled_at DESC LIMIT ?",
        (_config.CONSECUTIVE_LOSS_HALT + 5,),
    ).fetchall()
    n = 0
    for r in rows:
        if r["result"] == "L":
            n += 1
        else:
            break
    return n


def _dedup_hit_today(key: str) -> bool:
    # Only "terminal" rejections and successful LIVE placements burn
    # the dedup slot for the day. Transient / on-our-side rejections
    # (offline, halt, malformed payload, other) stay retryable so a
    # single relay hiccup or config bug doesn't lock a pick out
    # until midnight. Dry-run rows never burn dedup — flipping from
    # dry-run to live during the same day must be allowed to actually
    # place, otherwise dry-run mode would silently blackhole future
    # live attempts.
    conn = get_conn()
    r = conn.execute(
        "SELECT 1 FROM placements "
        "WHERE dedup_key = ? AND DATE(queued_at) = ? "
        "  AND mode = 'live' "
        "  AND status NOT IN ('rejected_dedup', 'rejected_offline', "
        "                     'rejected_halt', 'rejected_other', "
        "                     'rejected_market') "
        "LIMIT 1",
        (key, _today_iso()),
    ).fetchone()
    return r is not None


def check_can_place(pick: dict, *, mode: str) -> tuple[bool, str]:
    """Gate every placement decision. Returns (ok, reason_slug).

    `mode` is 'dry_run' or 'live'. Dry-run skips the kill-switch check
    (that's the point of dry-run) but respects every other breaker so
    we can validate the safety infrastructure without live-fire.
    """
    # Kill switch — only enforced in live mode.
    if mode == "live" and not _config.is_live_fire_enabled():
        return False, "kill_switch"

    # Relay must be reachable.
    if mode == "live" and not _config.relay_url():
        return False, "offline"

    # Per-bet cap.
    stake_d = float(pick.get("stake_dollars") or 0.0)
    if stake_d > _config.MAX_STAKE_PER_BET_DOLLARS + 1e-9:
        return False, "per_bet_cap"

    # Daily / weekly caps — count only what's live-placed, and only
    # what would push us past the cap AFTER this bet.
    if mode == "live":
        today_staked = _sum_staked_since(_today_iso())
        # Sports-app's own envelope — DO NOT subtract TT's daily stake
        # from it. That's a separate concern (see the total-account
        # ceiling below). Subtracting a co-tenant's stake here would
        # let a chatty TT session starve us to zero.
        if today_staked + stake_d > _config.MAX_STAKED_PER_DAY_DOLLARS + 1e-9:
            return False, "daily_cap"
        week_staked = _sum_staked_since(_week_start_iso())
        if week_staked + stake_d > _config.MAX_STAKED_PER_WEEK_DOLLARS + 1e-9:
            return False, "weekly_cap"

        # Optional shared-account ceiling (opt-in via config). This
        # protects the SHARED HR balance from combined draw across
        # sports-app + TT. Only enforced when the config sets a
        # positive value AND the relay exposes /daily-stake with
        # TT's number. Moot at flat $1 units.
        ceiling = _config.TOTAL_ACCOUNT_DAILY_CEILING_DOLLARS
        if ceiling and ceiling > 0:
            try:
                from . import _relay as _r
                tt_today = float((_r.daily_stake() or {}).get("tt") or 0.0)
            except Exception:
                tt_today = 0.0
            combined = today_staked + tt_today + stake_d
            if combined > ceiling + 1e-9:
                return False, "shared_account_ceiling"

        # Daily drawdown.
        pnl = _daily_realized_pnl()
        if pnl <= _config.DAILY_DRAWDOWN_HALT_DOLLARS:
            return False, "daily_drawdown"

        # Consecutive-loss halt.
        losses = _consecutive_recent_losses()
        if losses >= _config.CONSECUTIVE_LOSS_HALT:
            return False, "consecutive_loss"

    # Dedup — always checked regardless of mode so the log stays clean.
    key = dedup_key(pick)
    if _dedup_hit_today(key):
        return False, "dedup"

    return True, "ok"
