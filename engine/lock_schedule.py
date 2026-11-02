"""
engine/lock_schedule.py -- the ONE time-based pick-lock schedule.

Replaces the per-sport lock bandaids (the blanket 7am POTD lock in
pick_of_day/_select.py, football's game-day POTD lock, MLB's unlocked
paper-POTD, and each sport's separate card "lock on first lean"
recorder). Every card and POTD lock decision routes through here so the
behaviour is identical across sports.

The rule (Austin 2026-10-14) -- two daily lock points, in ET:
  * game starts BEFORE 5pm ET   -> lock 7:00am ET that morning   (day game)
  * game starts 5pm ET or later -> lock 4:30pm ET that afternoon (night game)
  * game starts overnight (before 7am ET, e.g. Asian-slate tennis) -> 7am is
    already after first ball, so lock 9:00pm ET the PRIOR evening.

Before its lock time a pick is PROVISIONAL (free to re-pick as the odds
mature); at/after its lock time it is FROZEN -- the side + market never
change again, only the closing line keeps updating for CLV.

Times are compared in ET wall-clock. ``game_start`` may be tz-aware (it
is converted to ET) or naive (it is ASSUMED to already be ET).
"""
from __future__ import annotations

from datetime import datetime, time, timedelta

from ._tz import et_now

try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:                       # pragma: no cover - zoneinfo/tzdata missing
    _ET = None

LOCK_MORNING = time(7, 0)               # day games (start < 5pm ET)
LOCK_AFTERNOON = time(16, 30)           # night games (start >= 5pm ET)
LOCK_OVERNIGHT = time(21, 0)            # prior evening, for overnight (pre-7am) starts
DAY_CUTOFF = time(17, 0)                # 5pm ET -- day vs night boundary


def _et_wall(dt: datetime) -> datetime:
    """Return ``dt`` as a naive ET wall-clock datetime. Aware inputs are
    converted to ET; naive inputs are assumed to already be ET."""
    if dt.tzinfo is not None and _ET is not None:
        dt = dt.astimezone(_ET)
    return dt.replace(tzinfo=None)


def lock_time(game_start: datetime) -> datetime:
    """Naive ET wall-clock datetime at which a pick for a game starting at
    ``game_start`` freezes."""
    gs = _et_wall(game_start)
    d = gs.date()
    t = gs.time()
    if t < LOCK_MORNING:                               # overnight / pre-7am
        return datetime.combine(d - timedelta(days=1), LOCK_OVERNIGHT)
    if t < DAY_CUTOFF:                                 # day game (7am-5pm)
        return datetime.combine(d, LOCK_MORNING)
    return datetime.combine(d, LOCK_AFTERNOON)         # night game (>=5pm)


def is_locked(game_start: datetime, now: datetime | None = None) -> bool:
    """True if a pick for ``game_start`` is frozen as of ``now`` (ET now by
    default)."""
    now_wall = _et_wall(now) if now is not None else et_now().replace(tzinfo=None)
    return now_wall >= lock_time(game_start)


def lock_label(game_start: datetime) -> str:
    """Human label for which bucket a game falls in (debug/display)."""
    t = _et_wall(game_start).time()
    if t < LOCK_MORNING:
        return "overnight->prior-9pm"
    if t < DAY_CUTOFF:
        return "day->7am"
    return "night->4:30pm"
