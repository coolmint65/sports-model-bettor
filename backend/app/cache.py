"""
Two-layer local caching system.

Layer 1 — HTTP Response Cache (dedicated SQLite file)
    Caches raw API responses by URL + params so that repeated external
    calls serve from local storage until their TTL expires.  Survives
    process restarts.  Uses its own SQLite database to avoid write-lock
    contention with the main application database.

Layer 2 — Feature Cache (in-memory)
    Caches computed features (goalie stats, team form, H2H, etc.) by
    a composite key so they aren't rebuilt from scratch every prediction
    cycle.  Cleared on restart (features are cheap to recompute once).

Both layers use a TTL-based invalidation strategy.  The background
scheduler refreshes data at its own cadence; the caches simply prevent
redundant work within the TTL window.
"""

import hashlib
import json
import logging
import time
from typing import Any, Dict, Optional

import aiosqlite

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Layer 1: HTTP Response Cache (dedicated SQLite file)
# ---------------------------------------------------------------------------

_cache_db: Optional[aiosqlite.Connection] = None


async def _get_cache_db() -> aiosqlite.Connection:
    """Return the shared cache database connection, creating it on first use."""
    global _cache_db
    if _cache_db is not None:
        return _cache_db

    from app.config import settings

    cache_path = settings.db_dir / "http_cache.db"
    settings.db_dir.mkdir(parents=True, exist_ok=True)

    _cache_db = await aiosqlite.connect(str(cache_path))
    # WAL mode for concurrent reads while writing
    await _cache_db.execute("PRAGMA journal_mode=WAL")
    await _cache_db.execute("""
        CREATE TABLE IF NOT EXISTS http_response_cache (
            cache_key   TEXT PRIMARY KEY,
            url         TEXT NOT NULL,
            response    TEXT NOT NULL,
            fetched_at  REAL NOT NULL,
            ttl         REAL NOT NULL
        )
    """)
    await _cache_db.commit()
    logger.info("HTTP response cache ready (dedicated SQLite)")
    return _cache_db


async def close_cache_db() -> None:
    """Close the cache database connection.  Called on app shutdown."""
    global _cache_db
    if _cache_db is not None:
        await _cache_db.close()
        _cache_db = None


def _make_cache_key(url: str, params: Optional[Dict] = None) -> str:
    """Deterministic cache key from URL + sorted params."""
    raw = url
    if params:
        sorted_params = sorted(params.items())
        raw += "?" + "&".join(f"{k}={v}" for k, v in sorted_params)
    return hashlib.sha256(raw.encode()).hexdigest()


async def get_cached_response(
    url: str,
    params: Optional[Dict] = None,
) -> Optional[Any]:
    """Return cached JSON response if still within TTL, else None."""
    db = await _get_cache_db()
    key = _make_cache_key(url, params)
    now = time.time()

    async with db.execute(
        "SELECT response, fetched_at, ttl "
        "FROM http_response_cache WHERE cache_key = ?",
        (key,),
    ) as cursor:
        row = await cursor.fetchone()

    if row is None:
        return None

    response_json, fetched_at, ttl = row
    if now - fetched_at > ttl:
        return None  # expired

    try:
        return json.loads(response_json)
    except (json.JSONDecodeError, TypeError):
        return None


async def get_stale_response(
    url: str,
    params: Optional[Dict] = None,
) -> Optional[Any]:
    """Return cached JSON response even if expired (stale-while-error).

    Used as a fallback when the API returns a 429 or other transient error.
    Better to serve slightly stale data than no data at all.
    """
    db = await _get_cache_db()
    key = _make_cache_key(url, params)

    async with db.execute(
        "SELECT response FROM http_response_cache WHERE cache_key = ?",
        (key,),
    ) as cursor:
        row = await cursor.fetchone()

    if row is None:
        return None

    try:
        return json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return None


async def set_cached_response(
    url: str,
    params: Optional[Dict],
    response: Any,
    ttl: float,
) -> None:
    """Store an API response in the cache with the given TTL (seconds)."""
    db = await _get_cache_db()
    key = _make_cache_key(url, params)
    now = time.time()

    try:
        response_json = json.dumps(response)
    except (TypeError, ValueError) as exc:
        logger.warning("Cannot cache response for %s: %s", url, exc)
        return

    await db.execute(
        "INSERT OR REPLACE INTO http_response_cache "
        "(cache_key, url, response, fetched_at, ttl) "
        "VALUES (?, ?, ?, ?, ?)",
        (key, url, response_json, now, ttl),
    )
    await db.commit()


async def clear_expired_responses() -> int:
    """Remove expired entries.  Call periodically from the scheduler."""
    db = await _get_cache_db()
    now = time.time()
    cursor = await db.execute(
        "DELETE FROM http_response_cache WHERE (? - fetched_at) > ttl",
        (now,),
    )
    await db.commit()
    deleted = cursor.rowcount
    if deleted:
        logger.debug("Cleared %d expired HTTP cache entries", deleted)
    return deleted


# ---------------------------------------------------------------------------
# Layer 2: Feature Cache (in-memory, TTL-based)
# ---------------------------------------------------------------------------

_feature_store: Dict[str, Dict[str, Any]] = {}
# Each entry: {"data": <computed dict>, "expires_at": <float>}

# Default TTLs per feature type (seconds).
FEATURE_TTLS: Dict[str, float] = {
    "team_form": 300,           # 5 min — changes after each game
    "home_away_splits": 600,    # 10 min — slow to change
    "head_to_head": 900,        # 15 min — rarely changes mid-day
    "goalie_features": 300,     # 5 min — may change with starter swap
    "goalie_venue": 900,        # 15 min
    "goalie_workload": 300,     # 5 min
    "goalie_vs_team": 900,      # 15 min
    "period_stats": 600,        # 10 min
    "overtime_tendency": 900,   # 15 min
    "skater_impact": 600,       # 10 min
    "lineup_status": 300,       # 5 min — injuries can change
    "injury_impact": 300,       # 5 min
    "ev_possession": 900,       # 15 min — external data, slow refresh
    "pace_metrics": 600,        # 10 min
    "score_effects": 600,       # 10 min
    "close_game": 600,          # 10 min
    "venue_splits": 900,        # 15 min
}


def _feature_key(feature_type: str, *identifiers) -> str:
    """Build a composite cache key from feature type + identifiers."""
    parts = [feature_type] + [str(i) for i in identifiers]
    return ":".join(parts)


def get_cached_feature(
    feature_type: str,
    *identifiers,
) -> Optional[Dict[str, Any]]:
    """Return cached feature dict if still within TTL, else None."""
    key = _feature_key(feature_type, *identifiers)
    entry = _feature_store.get(key)
    if entry is None:
        return None
    if time.time() > entry["expires_at"]:
        del _feature_store[key]
        return None
    return entry["data"]


def set_cached_feature(
    feature_type: str,
    *identifiers,
    data: Dict[str, Any],
    ttl: Optional[float] = None,
) -> None:
    """Store a computed feature dict with TTL."""
    if ttl is None:
        ttl = FEATURE_TTLS.get(feature_type, 300)
    key = _feature_key(feature_type, *identifiers)
    _feature_store[key] = {
        "data": data,
        "expires_at": time.time() + ttl,
    }


def invalidate_feature(feature_type: str, *identifiers) -> None:
    """Explicitly invalidate a cached feature."""
    key = _feature_key(feature_type, *identifiers)
    _feature_store.pop(key, None)


def invalidate_all_features() -> None:
    """Clear the entire feature cache (e.g., after model retrain)."""
    _feature_store.clear()
    logger.info("Feature cache cleared")


def feature_cache_stats() -> Dict[str, Any]:
    """Return cache statistics for monitoring."""
    now = time.time()
    total = len(_feature_store)
    expired = sum(1 for e in _feature_store.values() if now > e["expires_at"])
    return {
        "total_entries": total,
        "active_entries": total - expired,
        "expired_entries": expired,
    }
