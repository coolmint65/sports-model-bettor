"""Shared utility functions."""

from datetime import datetime, timezone
from typing import Optional


def serialize_utc_datetime(dt: Optional[datetime]) -> Optional[str]:
    """Serialize a datetime as an ISO-8601 string with explicit UTC offset.

    SQLite strips timezone info on round-trip, so naive datetimes read back
    from the DB need the +00:00 suffix re-attached. Without it, JavaScript's
    ``new Date()`` interprets the string as local time.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()
