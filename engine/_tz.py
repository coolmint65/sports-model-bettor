"""ET timezone helpers for engine modules.

Mirror of backend/server/_tz.py but lives at the engine level so
non-route code (trackers, ingest, calibration, picks) can pull "today
in ET" without importing from the backend layer.

All date-stamping in trackers / picks / calibration should go through
``et_today_str()`` so the same timestamp is produced regardless of the
machine's local timezone. Earlier each module called
``datetime.now().strftime("%Y-%m-%d")`` directly, which silently broke
when the worker ran in UTC or PT.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    _US_EASTERN = ZoneInfo("America/New_York")
except Exception:
    _US_EASTERN = None


def et_now() -> datetime:
    """Aware datetime in US Eastern. Falls back to a month-keyed UTC
    offset when zoneinfo is unavailable (off by an hour around DST
    transitions only)."""
    if _US_EASTERN is not None:
        return datetime.now(_US_EASTERN)
    now_utc = datetime.now(timezone.utc)
    offset_hours = -4 if 3 <= now_utc.month <= 10 else -5
    return now_utc.astimezone(timezone(timedelta(hours=offset_hours)))


def et_today_str(fmt: str = "%Y-%m-%d") -> str:
    """Today's date as a string in US Eastern. Drop-in replacement
    for ``datetime.now().strftime("%Y-%m-%d")``."""
    return et_now().strftime(fmt)


def et_month() -> int:
    """Current month in US Eastern. For season-month membership checks."""
    return et_now().month
