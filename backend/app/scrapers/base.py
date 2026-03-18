"""
Base scraper with async HTTP client, retry logic, rate limiting, and error handling.

All sport-specific scrapers should inherit from BaseScraper and implement
their own fetch/sync methods using the shared HTTP infrastructure.
"""

import asyncio
import collections
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger(__name__)


class ScraperError(Exception):
    """Base exception for scraper errors."""

    pass


class RateLimitError(ScraperError):
    """Raised when the API rate limit is hit."""

    pass


class APIResponseError(ScraperError):
    """Raised when the API returns an unexpected response."""

    def __init__(self, message: str, status_code: int = 0, url: str = ""):
        self.status_code = status_code
        self.url = url
        super().__init__(message)


class _SharedRateLimiter:
    """Per-host sliding-window rate limiter shared across scraper instances.

    Multiple scraper instances hitting the same base_url (e.g. three
    NBAScraper() objects) must share a single request window — otherwise
    each starts with a fresh counter and collectively exceeds the API limit.
    """

    _instances: Dict[str, "_SharedRateLimiter"] = {}

    def __init__(self, rpm_limit: int, min_delay: float):
        self.rpm_limit = rpm_limit
        self.min_delay = min_delay
        self._lock = asyncio.Lock()
        self._timestamps: collections.deque = collections.deque()

    @classmethod
    def get(cls, base_url: str, rpm_limit: int, min_delay: float) -> "_SharedRateLimiter":
        """Return (or create) the singleton limiter for *base_url*."""
        if base_url not in cls._instances:
            cls._instances[base_url] = cls(rpm_limit, min_delay)
        limiter = cls._instances[base_url]
        # Update limits if a later instance requests tighter values
        limiter.rpm_limit = min(limiter.rpm_limit, rpm_limit)
        limiter.min_delay = max(limiter.min_delay, min_delay)
        return limiter

    async def wait(self) -> None:
        """Block until both the min-delay and RPM budget allow a request."""
        async with self._lock:
            now = time.monotonic()

            # --- Per-request minimum delay ---
            if self._timestamps:
                elapsed = now - self._timestamps[-1]
                if elapsed < self.min_delay:
                    wait_time = self.min_delay - elapsed
                    logger.debug("Rate limit: waiting %.2fs (min delay)", wait_time)
                    await asyncio.sleep(wait_time)
                    now = time.monotonic()

            # --- Sliding window: prune entries older than 60s ---
            window = 60.0
            while self._timestamps and (now - self._timestamps[0]) > window:
                self._timestamps.popleft()

            # --- If at the RPM ceiling, wait until oldest entry expires ---
            if len(self._timestamps) >= self.rpm_limit:
                wait_until = self._timestamps[0] + window
                wait_time = wait_until - now
                if wait_time > 0:
                    logger.info(
                        "Rate limit: %d/%d requests in last 60s, waiting %.1fs",
                        len(self._timestamps), self.rpm_limit, wait_time,
                    )
                    await asyncio.sleep(wait_time)
                    now = time.monotonic()
                    while self._timestamps and (now - self._timestamps[0]) > window:
                        self._timestamps.popleft()

            self._timestamps.append(now)


class BaseScraper(ABC):
    """
    Abstract base class for all data scrapers.

    Provides:
    - Async HTTP client (httpx) with connection pooling
    - Automatic retries with exponential backoff
    - Rate limiting to respect API constraints (shared per host)
    - Consistent error handling and logging
    - Graceful resource cleanup

    Subclasses must implement `sync_all` at minimum.
    """

    # Default configuration; subclasses can override
    DEFAULT_TIMEOUT = 30.0
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_BACKOFF = 1.0  # seconds, multiplied by attempt number
    DEFAULT_RATE_LIMIT = 1.0  # minimum seconds between requests
    DEFAULT_REQUESTS_PER_MINUTE = 0  # 0 = derive from rate_limit
    DEFAULT_USER_AGENT = (
        "SportsModelBettor/1.0 (async data scraper; contact: dev@example.com)"
    )

    def __init__(
        self,
        base_url: str,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_backoff: float = DEFAULT_RETRY_BACKOFF,
        rate_limit: float = DEFAULT_RATE_LIMIT,
        requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.rate_limit = rate_limit

        # Derive RPM from per-request delay if not explicitly set
        if requests_per_minute > 0:
            rpm = requests_per_minute
        else:
            rpm = max(1, int(60.0 / rate_limit))

        # Shared per-host — all scraper instances hitting the same
        # base_url coordinate through a single rate limiter.
        self._limiter = _SharedRateLimiter.get(self.base_url, rpm, rate_limit)

        # Build default headers
        self._headers = {
            "User-Agent": self.DEFAULT_USER_AGENT,
            "Accept": "application/json",
        }
        if headers:
            self._headers.update(headers)

        # Lazily-created client (created on first use)
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # HTTP client lifecycle
    # ------------------------------------------------------------------

    def _get_client(self) -> httpx.AsyncClient:
        """Return the shared httpx client, creating it if needed."""
        if self._client is None or self._client.is_closed:
            transport = httpx.AsyncHTTPTransport(
                retries=0,  # We handle retries ourselves
                limits=httpx.Limits(
                    max_keepalive_connections=10,
                    max_connections=20,
                    keepalive_expiry=30,
                ),
            )
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=self._headers,
                timeout=httpx.Timeout(self.timeout),
                transport=transport,
                follow_redirects=True,
            )
        return self._client

    async def close(self) -> None:
        """Close the underlying HTTP client and release resources."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
            logger.debug("HTTP client closed for %s", self.__class__.__name__)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    async def _wait_for_rate_limit(self) -> None:
        """Delegate to the shared per-host rate limiter."""
        await self._limiter.wait()

    # ------------------------------------------------------------------
    # Core HTTP methods
    # ------------------------------------------------------------------

    # Default cache TTL; subclasses can override per-scraper.
    # Set to 0 to disable caching for a scraper.
    DEFAULT_CACHE_TTL = 0.0

    async def fetch_json(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        cache_ttl: Optional[float] = None,
    ) -> Any:
        """
        Fetch a URL and return the parsed JSON response.

        Handles rate limiting, retries with exponential backoff,
        and structured error reporting.  When ``cache_ttl`` (or the
        scraper's ``DEFAULT_CACHE_TTL``) is > 0, GET responses are
        served from the local HTTP response cache if still fresh.

        Args:
            path: URL path relative to `base_url` (e.g., "/standings/now").
            params: Optional query parameters.
            method: HTTP method (default GET).
            cache_ttl: Override cache TTL for this request (seconds).
                       0 = skip cache.  None = use DEFAULT_CACHE_TTL.

        Returns:
            Parsed JSON response (dict or list).

        Raises:
            RateLimitError: If the API returns 429 after all retries.
            APIResponseError: If the API returns a non-success status code.
            ScraperError: If the request fails after all retries.
        """
        url = path if path.startswith("http") else path
        full_url = f"{self.base_url}/{url.lstrip('/')}" if not url.startswith("http") else url

        # --- Local cache check (GET only) ---
        ttl = cache_ttl if cache_ttl is not None else self.DEFAULT_CACHE_TTL
        if method.upper() == "GET" and ttl > 0:
            try:
                from app.cache import get_cached_response, set_cached_response
                cached = await get_cached_response(full_url, params)
                if cached is not None:
                    logger.debug("Cache HIT: %s", full_url)
                    return cached
            except Exception as exc:
                logger.debug("Cache read error (non-fatal): %s", exc)

        client = self._get_client()
        last_exception: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            await self._wait_for_rate_limit()

            try:
                logger.debug(
                    "Request %s %s (attempt %d/%d)",
                    method,
                    url,
                    attempt,
                    self.max_retries,
                )
                response = await client.request(method, url, params=params)

                if response.status_code == 200:
                    data = response.json()
                    # Store in local cache for future requests
                    if method.upper() == "GET" and ttl > 0:
                        try:
                            await set_cached_response(
                                full_url, params, data, ttl,
                            )
                        except Exception as exc:
                            logger.debug("Cache write error (non-fatal): %s", exc)
                    return data

                if response.status_code == 429:
                    retry_after = int(
                        response.headers.get("Retry-After", self.retry_backoff * attempt * 2)
                    )
                    # Clamp to a sane maximum (2 min) but always respect
                    # the server's Retry-After to avoid repeated 429s.
                    retry_after = min(retry_after, 120)
                    logger.warning(
                        "Rate limited (429) on %s. Retrying in %ds (attempt %d/%d).",
                        url, retry_after, attempt, self.max_retries,
                    )
                    await asyncio.sleep(retry_after)
                    last_exception = RateLimitError(
                        f"Rate limited on {url}"
                    )
                    continue

                if response.status_code == 404:
                    logger.warning("Resource not found (404): %s", url)
                    raise APIResponseError(
                        f"Resource not found: {url}",
                        status_code=404,
                        url=url,
                    )

                if response.status_code >= 500:
                    logger.warning(
                        "Server error (%d) on %s, attempt %d/%d",
                        response.status_code,
                        url,
                        attempt,
                        self.max_retries,
                    )
                    last_exception = APIResponseError(
                        f"Server error {response.status_code} on {url}",
                        status_code=response.status_code,
                        url=url,
                    )
                    if attempt < self.max_retries:
                        backoff = self.retry_backoff * attempt
                        await asyncio.sleep(backoff)
                    continue

                # Other 4xx errors are not retried
                raise APIResponseError(
                    f"HTTP {response.status_code} on {url}: {response.text[:500]}",
                    status_code=response.status_code,
                    url=url,
                )

            except httpx.TimeoutException as exc:
                logger.warning(
                    "Timeout on %s, attempt %d/%d: %s",
                    url,
                    attempt,
                    self.max_retries,
                    exc,
                )
                last_exception = exc
                if attempt < self.max_retries:
                    backoff = self.retry_backoff * attempt
                    await asyncio.sleep(backoff)

            except httpx.RequestError as exc:
                logger.warning(
                    "Request error on %s, attempt %d/%d: %s",
                    url,
                    attempt,
                    self.max_retries,
                    exc,
                )
                last_exception = exc
                if attempt < self.max_retries:
                    backoff = self.retry_backoff * attempt
                    await asyncio.sleep(backoff)

            except (APIResponseError, RateLimitError):
                raise

            except Exception as exc:
                logger.error("Unexpected error fetching %s: %s", url, exc)
                raise ScraperError(f"Unexpected error: {exc}") from exc

        raise ScraperError(
            f"Failed to fetch {url} after {self.max_retries} attempts"
        ) from last_exception

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    @staticmethod
    def safe_get(data: dict, *keys, default=None) -> Any:
        """
        Safely traverse nested dicts.

        Example:
            safe_get(resp, "teamAbbrev", "default")
            # equivalent to resp.get("teamAbbrev", {}).get("default")
        """
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return default
            if current is None:
                return default
        return current

    @staticmethod
    def parse_toi(toi_str: str) -> Optional[float]:
        """
        Parse a time-on-ice string like '18:32' into total seconds as a float.

        Returns None if the input is invalid.
        """
        if not toi_str or not isinstance(toi_str, str):
            return None
        try:
            parts = toi_str.split(":")
            if len(parts) == 2:
                minutes, seconds = int(parts[0]), int(parts[1])
                return float(minutes * 60 + seconds)
            return None
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def parse_toi_minutes(toi_str: str) -> Optional[float]:
        """
        Parse a time-on-ice string like '18:32' into total minutes as a float.

        Returns None if the input is invalid.
        """
        if not toi_str or not isinstance(toi_str, str):
            return None
        try:
            parts = toi_str.split(":")
            if len(parts) == 2:
                minutes, seconds = int(parts[0]), int(parts[1])
                return round(minutes + seconds / 60.0, 2)
            return None
        except (ValueError, AttributeError):
            return None

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def sync_all(self, db_session) -> None:
        """
        Run all sync operations in sequence.

        Subclasses must implement this to orchestrate their full
        data synchronisation workflow.
        """
        ...
