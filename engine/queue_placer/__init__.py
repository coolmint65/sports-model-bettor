"""Queue auto-placement package.

Splits the "brain" (this codebase — decides what to bet) from the
"hands" (the Beelink relay — physically executes on HR). Communication
is a small HTTP contract over Tailscale so the placement layer is
swappable per book.

Safety posture:
  - Default mode is dry-run.
  - Live-fire needs env AUTO_BET_LIVE=1 AND the on-disk flag from
    /api/bet-queue/live-fire/on — two-key launch.
  - Every attempt (dry-run and live) writes to the placements log.
  - Circuit breakers: per-bet cap, daily cap, weekly cap, consecutive
    losses, daily drawdown. Cell allowlist inherits directly from the
    queue's cell filter — if the queue doesn't surface it, we don't
    place it.

Public API — everything else is private.
"""
import logging as _logging
from ._placer import sweep_and_place, poll_pending_placements
from ._config import (
    is_live_fire_enabled, set_live_fire, kill_flag_path,
    relay_url, relay_secret, relay_token,
    UNIT_DOLLARS, MAX_STAKE_PER_BET_DOLLARS,
    MAX_STAKED_PER_DAY_DOLLARS, MAX_STAKED_PER_WEEK_DOLLARS,
    CONSECUTIVE_LOSS_HALT, DAILY_DRAWDOWN_HALT_DOLLARS,
    LINE_DRIFT_TOLERANCE_PP, PLACER_INTERVAL_S,
)
from ._schema import get_conn as _get_placements_conn, STATUS_VALUES


# Import-time diagnostic — surface the current safety posture in the
# backend log so a bug never silently switches us into live-fire without
# a paper trail. Prints one line and a warning when live is armed.
_log = _logging.getLogger(__name__)
_log.info(
    "queue placer boot: live=%s (env_flag=%s, on_disk_flag=%s), "
    "relay_url=%s, token=%s, caps=$%.2f/bet $%.2f/day $%.2f/week",
    is_live_fire_enabled(),
    "1" if _get_placements_conn is not None else "?",
    kill_flag_path().exists(),
    relay_url() or "<unset>",
    "set" if relay_token() else "<unset>",
    MAX_STAKE_PER_BET_DOLLARS, MAX_STAKED_PER_DAY_DOLLARS,
    MAX_STAKED_PER_WEEK_DOLLARS,
)
if is_live_fire_enabled():
    _log.warning(
        "queue placer boot: LIVE-FIRE ARMED — real money will place. "
        "POST /api/bet-queue/live-fire/off to halt.")
del _log, _logging

__all__ = [
    "sweep_and_place",
    "poll_pending_placements",
    "is_live_fire_enabled",
    "set_live_fire",
    "kill_flag_path",
    "relay_url",
    "relay_secret",
    "UNIT_DOLLARS",
    "MAX_STAKE_PER_BET_DOLLARS",
    "MAX_STAKED_PER_DAY_DOLLARS",
    "MAX_STAKED_PER_WEEK_DOLLARS",
    "CONSECUTIVE_LOSS_HALT",
    "DAILY_DRAWDOWN_HALT_DOLLARS",
    "LINE_DRIFT_TOLERANCE_PP",
    "PLACER_INTERVAL_S",
    "STATUS_VALUES",
]
