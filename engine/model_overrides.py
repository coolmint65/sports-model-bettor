"""
Runtime model-flag overrides backed by a per-sport DB table.

Source-code values in ``engine/config.py`` are the human's intended
defaults. The data may suggest something different — for example, after
40 picks at 32% WR the data implies a flag should be flipped off. We
don't want to silently rewrite source code; we also don't want to keep
placing losing bets while the human catches up to the report.

This module provides a runtime layer that ``engine/config.get_flag()``
consults before returning the source-code value. Overrides are written
ONLY by ``engine.train --apply`` and ONLY when guard rails pass:

  - N >= MIN_SAMPLES (default 30) bets in the bucket
  - p-value < MAX_P_VALUE (default 0.01) against implied break-even
  - Direction = disable only (never auto-enable -- prevents flapping)
  - Override expires after EXPIRY_DAYS (default 14) so a one-off cold
    streak doesn't lock in forever

Audit trail: every write also appends to model_override_log so we can
reconstruct WHY a flag was flipped (sample size, p-value, ROI bucket).

Concurrency: callers should rely on the SQLite connection's WAL mode;
no application-level locking. Reads are wrapped in a 30-second cache
to keep get_flag() cheap when called per-pick.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# Guard-rail defaults. Centralized so train.py and any future caller
# read the same numbers.
MIN_SAMPLES = 30
MAX_P_VALUE = 0.01
EXPIRY_DAYS = 14
MAX_OVERRIDES_PER_RUN = 5  # safety brake -- a bug can't disable everything


def _conn(sport: str):
    """Sport-specific DB handle."""
    if sport == "mlb":
        from .db import get_conn
    elif sport == "nhl":
        from .nhl_db import get_conn
    elif sport == "nba":
        from .nba_db import get_conn
    else:
        raise ValueError(f"unknown sport: {sport}")
    return get_conn()


def _ensure_tables(sport: str) -> None:
    """Create model_overrides + model_override_log tables if missing."""
    c = _conn(sport)
    c.execute("""
        CREATE TABLE IF NOT EXISTS model_overrides (
            key         TEXT PRIMARY KEY,
            value       TEXT NOT NULL,
            reason      TEXT,
            n_samples   INTEGER,
            p_value     REAL,
            applied_at  TEXT NOT NULL DEFAULT (datetime('now')),
            expires_at  TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS model_override_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            key         TEXT NOT NULL,
            value       TEXT NOT NULL,
            action      TEXT NOT NULL,   -- 'apply' / 'expire' / 'revert'
            reason      TEXT,
            n_samples   INTEGER,
            p_value     REAL,
            recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    c.commit()


# ── Read path (hot) ────────────────────────────────────────────

# Cache: (sport, key) -> (timestamp, value)
_OVERRIDE_CACHE: dict[tuple[str, str], tuple[float, Any]] = {}
_OVERRIDE_CACHE_TTL = 30  # seconds; balances staleness vs DB hit per pick


def _coerce(s: str) -> Any:
    """Decode a stored override value back to its Python form.

    Stored as TEXT for portability; we recognize bool / int / float and
    fall back to the raw string (covers the future case of overriding a
    string config like a juice-wall preset name).
    """
    if s is None:
        return None
    if s in ("True", "true", "1"):
        return True
    if s in ("False", "false", "0"):
        return False
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return s


def get_override(sport: str, key: str) -> Any | None:
    """Return the active override value for ``key``, or None.

    Returns None if no override exists OR if the override has expired.
    Expired overrides are NOT proactively deleted from the read path;
    cleanup happens in expire_stale().
    """
    cached = _OVERRIDE_CACHE.get((sport, key))
    now = time.time()
    if cached and (now - cached[0]) < _OVERRIDE_CACHE_TTL:
        return cached[1]

    try:
        _ensure_tables(sport)
        c = _conn(sport)
        row = c.execute(
            "SELECT value, expires_at FROM model_overrides WHERE key = ?",
            (key,),
        ).fetchone()
    except Exception as e:
        logger.debug("get_override(%s, %s) failed: %s", sport, key, e)
        _OVERRIDE_CACHE[(sport, key)] = (now, None)
        return None

    if not row:
        _OVERRIDE_CACHE[(sport, key)] = (now, None)
        return None

    try:
        expires = datetime.fromisoformat(row["expires_at"].replace(" ", "T"))
        if expires < datetime.now():
            _OVERRIDE_CACHE[(sport, key)] = (now, None)
            return None
    except Exception:
        pass

    value = _coerce(row["value"])
    _OVERRIDE_CACHE[(sport, key)] = (now, value)
    return value


def list_overrides(sport: str, include_expired: bool = False) -> list[dict]:
    """Return all overrides for the sport, with computed expiry status."""
    _ensure_tables(sport)
    c = _conn(sport)
    rows = c.execute("SELECT * FROM model_overrides ORDER BY applied_at DESC").fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            expires = datetime.fromisoformat(d["expires_at"].replace(" ", "T"))
            d["expired"] = expires < datetime.now()
            d["days_remaining"] = round(
                (expires - datetime.now()).total_seconds() / 86400, 1
            )
        except Exception:
            d["expired"] = False
            d["days_remaining"] = None
        if d["expired"] and not include_expired:
            continue
        out.append(d)
    return out


# ── Write path (cold; called only by train --apply / API /revert) ──

def apply_override(sport: str, key: str, value: Any, *,
                   reason: str = "", n_samples: int | None = None,
                   p_value: float | None = None,
                   expiry_days: int = EXPIRY_DAYS) -> None:
    """Write or refresh an override. Logged to model_override_log."""
    _ensure_tables(sport)
    c = _conn(sport)
    expires = (datetime.now() + timedelta(days=expiry_days)).isoformat(timespec="seconds")
    val_str = "True" if value is True else "False" if value is False else str(value)
    c.execute(
        "INSERT INTO model_overrides (key, value, reason, n_samples, p_value, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
        "    reason = excluded.reason, n_samples = excluded.n_samples, "
        "    p_value = excluded.p_value, applied_at = datetime('now'), "
        "    expires_at = excluded.expires_at",
        (key, val_str, reason, n_samples, p_value, expires),
    )
    c.execute(
        "INSERT INTO model_override_log (key, value, action, reason, n_samples, p_value) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (key, val_str, "apply", reason, n_samples, p_value),
    )
    c.commit()
    _OVERRIDE_CACHE.pop((sport, key), None)
    logger.info("Override applied: %s.%s = %s (n=%s, p=%s, expires=%s)",
                sport, key, val_str, n_samples, p_value, expires)


def revert_override(sport: str, key: str) -> bool:
    """Manually clear an override. Returns True if a row was deleted."""
    _ensure_tables(sport)
    c = _conn(sport)
    cur = c.execute("DELETE FROM model_overrides WHERE key = ?", (key,))
    c.execute(
        "INSERT INTO model_override_log (key, value, action) VALUES (?, ?, ?)",
        (key, "", "revert"),
    )
    c.commit()
    _OVERRIDE_CACHE.pop((sport, key), None)
    deleted = cur.rowcount > 0
    if deleted:
        logger.info("Override reverted: %s.%s", sport, key)
    return deleted


def expire_stale(sport: str) -> int:
    """Drop overrides past their expires_at timestamp. Logged."""
    _ensure_tables(sport)
    c = _conn(sport)
    rows = c.execute(
        "SELECT key, value FROM model_overrides WHERE expires_at < datetime('now')"
    ).fetchall()
    for r in rows:
        c.execute(
            "INSERT INTO model_override_log (key, value, action) VALUES (?, ?, ?)",
            (r["key"], r["value"], "expire"),
        )
    cur = c.execute(
        "DELETE FROM model_overrides WHERE expires_at < datetime('now')"
    )
    c.commit()
    n = cur.rowcount or 0
    if n:
        logger.info("Expired %d %s overrides", n, sport.upper())
        for r in rows:
            _OVERRIDE_CACHE.pop((sport, r["key"]), None)
    return n


# ── Statistical guard rail ─────────────────────────────────────

def binomial_p_value(wins: int, n: int, p_null: float = 0.524) -> float:
    """Two-sided binomial p-value for observed WR vs the null break-even rate.

    Default null is 52.4% (the implied probability of a -110 line). This
    is the win rate you need to break even at standard juice; if our WR
    is significantly below that, the bucket is losing money in
    expectation, not just on this run of variance.

    Uses a normal approximation -- exact binomial is overkill given the
    other approximations in the pipeline and our N >= 30 floor.
    """
    if n <= 0:
        return 1.0
    p_obs = wins / n
    se = (p_null * (1 - p_null) / n) ** 0.5
    if se == 0:
        return 0.0 if p_obs != p_null else 1.0
    z = abs(p_obs - p_null) / se
    # Two-sided normal-approx p-value
    from math import erfc, sqrt
    return erfc(z / sqrt(2))
