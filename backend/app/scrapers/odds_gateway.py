"""
Centralized Odds API gateway.

All Odds API requests across all sports go through this single module.
This ensures:
- Shared rate limiting (one budget, not per-sport)
- Credit tracking via x-requests-remaining headers
- Consistent retry/backoff logic
- Sport-aware market and endpoint configuration
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Credit tracking (shared across all sports)
# ---------------------------------------------------------------------------

_credit_lock = asyncio.Lock()
_credits_remaining: Optional[int] = None
_credits_used: Optional[int] = None
_credits_updated_at: float = 0.0

# Low-credit threshold — when remaining drops below this, non-essential
# requests (alt lines, props) should be skipped.
CREDIT_LOW_THRESHOLD = 50


def get_credit_status() -> Dict[str, Any]:
    """Return current credit tracking state (non-blocking snapshot)."""
    return {
        "remaining": _credits_remaining,
        "used": _credits_used,
        "updated_at": _credits_updated_at,
        "low": _credits_remaining is not None and _credits_remaining < CREDIT_LOW_THRESHOLD,
    }


def credits_are_low() -> bool:
    """Return True if we know credits are below the safety threshold."""
    return _credits_remaining is not None and _credits_remaining < CREDIT_LOW_THRESHOLD


# ---------------------------------------------------------------------------
# Rate limiter — shared across all sports hitting the Odds API
# ---------------------------------------------------------------------------

_rate_lock = asyncio.Lock()
_last_request_at: float = 0.0
_MIN_REQUEST_GAP = 1.0  # seconds between Odds API requests


async def _wait_for_rate_limit() -> None:
    """Enforce minimum gap between Odds API requests."""
    global _last_request_at
    async with _rate_lock:
        now = time.monotonic()
        elapsed = now - _last_request_at
        if elapsed < _MIN_REQUEST_GAP:
            await asyncio.sleep(_MIN_REQUEST_GAP - elapsed)
        _last_request_at = time.monotonic()


# ---------------------------------------------------------------------------
# Shared HTTP client
# ---------------------------------------------------------------------------

_client: Optional[httpx.AsyncClient] = None
_client_lock = asyncio.Lock()


async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        async with _client_lock:
            if _client is None or _client.is_closed:
                _client = httpx.AsyncClient(
                    timeout=httpx.Timeout(15.0),
                    follow_redirects=True,
                    limits=httpx.Limits(
                        max_keepalive_connections=10,
                        max_connections=20,
                    ),
                )
    return _client


async def close() -> None:
    """Shut down the shared client (call on app shutdown)."""
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None


# ---------------------------------------------------------------------------
# Core request function
# ---------------------------------------------------------------------------

async def _request(
    url: str,
    params: Dict[str, Any],
    max_retries: int = 2,
    timeout: float = 15.0,
) -> Optional[Any]:
    """Make a rate-limited, retried GET request to the Odds API.

    Updates global credit tracking from response headers.
    Returns parsed JSON or None on failure.
    """
    global _credits_remaining, _credits_used, _credits_updated_at

    api_key = settings.odds_api_key
    if not api_key:
        logger.warning("ODDS_API_KEY not set — skipping Odds API request")
        return None

    params = {**params, "apiKey": api_key}
    _log_url = url.split("?")[0]

    for attempt in range(1 + max_retries):
        await _wait_for_rate_limit()

        try:
            client = await _get_client()
            resp = await client.get(url, params=params, timeout=timeout)

            # Update credit tracking from response headers
            used = resp.headers.get("x-requests-used")
            remaining = resp.headers.get("x-requests-remaining")
            if used or remaining:
                async with _credit_lock:
                    if remaining is not None:
                        try:
                            _credits_remaining = int(remaining)
                        except (ValueError, TypeError):
                            pass
                    if used is not None:
                        try:
                            _credits_used = int(used)
                        except (ValueError, TypeError):
                            pass
                    _credits_updated_at = time.time()
                logger.info(
                    "Odds API credits: used=%s remaining=%s (%s)",
                    used or "?", remaining or "?", _log_url,
                )
                if credits_are_low():
                    logger.warning(
                        "Odds API credits LOW: %d remaining — "
                        "non-essential requests will be skipped",
                        _credits_remaining,
                    )

            if resp.status_code == 200:
                return resp.json()

            # 422 — invalid request (bad markets, bad sport key, etc.)
            if resp.status_code == 422:
                body = ""
                try:
                    body = resp.text[:500]
                except Exception:
                    pass
                logger.warning(
                    "Odds API 422 (invalid request) from %s -- %s",
                    _log_url, body,
                )
                return None  # Don't retry — request is malformed

            # 429 — rate limited
            if resp.status_code == 429 and attempt < max_retries:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait = min(int(retry_after), 60)
                    except (ValueError, TypeError):
                        wait = 2 ** (attempt + 1)
                else:
                    wait = 2 ** (attempt + 1)
                logger.warning(
                    "Odds API 429 from %s -- retrying in %ds (attempt %d/%d)",
                    _log_url, wait, attempt + 1, max_retries,
                )
                await asyncio.sleep(wait)
                continue

            # 5xx — server error, retry
            if resp.status_code >= 500 and attempt < max_retries:
                wait = 2 ** (attempt + 1)
                logger.warning(
                    "Odds API %d from %s -- retrying in %ds (attempt %d/%d)",
                    resp.status_code, _log_url, wait, attempt + 1, max_retries,
                )
                await asyncio.sleep(wait)
                continue

            # Other error — log and return None
            body_snippet = ""
            if 400 <= resp.status_code < 500:
                try:
                    body_snippet = resp.text[:300]
                except Exception:
                    body_snippet = "(could not read body)"
            logger.warning(
                "Odds API HTTP %d from %s%s",
                resp.status_code, _log_url,
                f" -- {body_snippet}" if body_snippet else "",
            )
            return None

        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            if attempt < max_retries:
                wait = 2 ** (attempt + 1)
                logger.warning(
                    "Odds API %s for %s -- retrying in %ds (attempt %d/%d)",
                    type(exc).__name__, _log_url, wait,
                    attempt + 1, max_retries,
                )
                await asyncio.sleep(wait)
                continue
            logger.warning(
                "Odds API %s for %s (exhausted %d retries)",
                type(exc).__name__, _log_url, max_retries,
            )
            return None
        except Exception as exc:
            logger.warning("Odds API request failed for %s: %s", _log_url, exc)
            return None

    return None


# ---------------------------------------------------------------------------
# Public API — sport-aware endpoints
# ---------------------------------------------------------------------------

# Per-sport bulk cache (prevents duplicate calls within a sync cycle)
_bulk_cache: Dict[str, Dict[str, Any]] = {}  # sport -> {"data": ..., "ts": ...}
_bulk_cache_lock = asyncio.Lock()
_BULK_CACHE_TTL = 10.0  # seconds


async def fetch_bulk_odds(
    sport: str,
    markets_override: Optional[str] = None,
) -> Optional[List[Dict[str, Any]]]:
    """Fetch bulk odds for a sport from the /odds endpoint.

    Uses SportConfig to determine sport key, markets, and regions.
    Results are cached for _BULK_CACHE_TTL seconds to prevent duplicate
    calls from multiple consumers in the same sync cycle.
    """
    sport_cfg = settings.get_sport_config(sport)
    if not sport_cfg.odds_api_sport_key:
        logger.warning("No odds_api_sport_key configured for sport=%s", sport)
        return None

    # Fast-path cache check
    now = time.monotonic()
    cached = _bulk_cache.get(sport)
    if cached and (now - cached["ts"]) < _BULK_CACHE_TTL:
        return cached["data"]

    async with _bulk_cache_lock:
        # Re-check after acquiring lock
        cached = _bulk_cache.get(sport)
        if cached and (time.monotonic() - cached["ts"]) < _BULK_CACHE_TTL:
            return cached["data"]

        url = f"https://api.the-odds-api.com/v4/sports/{sport_cfg.odds_api_sport_key}/odds"
        markets = markets_override or sport_cfg.odds_api_bulk_markets
        params = {
            "regions": sport_cfg.odds_api_regions,
            "markets": markets,
            "oddsFormat": "american",
        }

        data = await _request(url, params)

        if data is None:
            logger.warning(
                "Odds API: bulk fetch failed for sport=%s markets=%r",
                sport, markets,
            )
            return None

        if isinstance(data, dict) and data.get("message"):
            logger.warning("Odds API error for sport=%s: %s", sport, data.get("message"))
            return None

        if isinstance(data, list) and len(data) > 0:
            has_bookmakers = any(
                len(ev.get("bookmakers", [])) > 0 for ev in data
            )
            if has_bookmakers:
                total_books = set()
                for ev in data:
                    for bm in ev.get("bookmakers", []):
                        total_books.add(bm.get("key", "unknown"))
                logger.info(
                    "Odds API: sport=%s got %d events, markets=%r, %d bookmakers: %s",
                    sport, len(data), markets, len(total_books),
                    ", ".join(sorted(total_books)),
                )
                _bulk_cache[sport] = {"data": data, "ts": time.monotonic()}
                return data
            else:
                logger.info(
                    "Odds API: sport=%s returned %d events but no bookmakers",
                    sport, len(data),
                )

        logger.warning("Odds API: no data returned for sport=%s", sport)
        return None


async def fetch_event_odds(
    sport: str,
    event_id: str,
    markets: str,
    regions: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Fetch odds for a single event (per-event endpoint).

    Used for alternate lines and player props. Each call costs
    additional API credits.
    """
    if credits_are_low():
        logger.info(
            "Skipping per-event fetch (credits low): sport=%s event=%s",
            sport, event_id,
        )
        return None

    sport_cfg = settings.get_sport_config(sport)
    if not sport_cfg.odds_api_sport_key:
        return None

    url = (
        f"https://api.the-odds-api.com/v4/sports/"
        f"{sport_cfg.odds_api_sport_key}/events/{event_id}/odds"
    )
    params = {
        "regions": regions or sport_cfg.odds_api_regions,
        "markets": markets,
        "oddsFormat": "american",
    }

    return await _request(url, params)


def invalidate_bulk_cache(sport: Optional[str] = None) -> None:
    """Clear the bulk cache for a sport (or all sports)."""
    if sport:
        _bulk_cache.pop(sport, None)
    else:
        _bulk_cache.clear()
