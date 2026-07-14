"""ET timezone helpers for backend routes.

Every "today" slate filter, season-month check, and pick-date stamp in
the routes layer needs to be in US Eastern, not the server's local
timezone. Earlier code used ``datetime.now().strftime("%Y-%m-%d")``
which silently breaks the moment the server is moved off ET (PT, UTC,
or a cloud region). Centralizing the conversion here means there's one
place to inspect, one place to test, and a clear seam for tests to
freeze "today" via monkeypatch.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    _US_EASTERN = ZoneInfo("America/New_York")
except Exception:
    _US_EASTERN = None


def et_now() -> datetime:
    """Return the current time as an aware datetime in US Eastern.

    When zoneinfo is unavailable (some stripped-down runtimes), falls
    back to UTC offset by month -- EDT roughly mid-March through early
    November, EST otherwise. The fallback is precise enough for
    date-stamping and `month` lookups; only the ~hour around DST
    transitions can disagree with the canonical conversion.
    """
    if _US_EASTERN is not None:
        return datetime.now(_US_EASTERN)
    now_utc = datetime.now(timezone.utc)
    offset_hours = -4 if 3 <= now_utc.month <= 10 else -5
    return now_utc.astimezone(timezone(timedelta(hours=offset_hours)))


def et_today_str(fmt: str = "%Y-%m-%d") -> str:
    """Today's date as a string in US Eastern.

    Drop-in replacement for ``datetime.now().strftime("%Y-%m-%d")`` in
    routes that filter the per-sport games table or stamp the date on
    a pick.
    """
    return et_now().strftime(fmt)


def et_month() -> int:
    """Current month in US Eastern. Drop-in replacement for
    ``datetime.now().month`` when the value is consumed by season-month
    membership checks (which are always ET-keyed)."""
    return et_now().month
