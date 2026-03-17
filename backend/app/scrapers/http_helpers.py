"""
Shared HTTP request helpers for scrapers.

Consolidates the duplicated _make_request pattern used by odds_multi.py
and player_props.py into a single reusable function.
"""

import asyncio
import logging
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger(__name__)


async def make_request(
    client: httpx.AsyncClient,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 10.0,
    max_retries: int = 2,
    log_credit_headers: bool = True,
) -> Optional[Any]:
    """Make a GET request with retry on 429.  Returns parsed JSON or None.

    Args:
        client: httpx async client to use.
        url: Full URL to request.
        headers: Optional request headers.
        params: Optional query parameters.
        timeout: Request timeout in seconds.
        max_retries: Number of retries on 429 rate-limit responses.
        log_credit_headers: If True, log Odds API credit usage headers.
    """
    # Strip API key from log output
    _log_url = url.split("?")[0]
    for attempt in range(1 + max_retries):
        try:
            resp = await client.get(
                url,
                headers=headers or {},
                params=params,
                timeout=timeout,
            )
            if resp.status_code == 200:
                # Log Odds API credit usage from response headers
                if log_credit_headers:
                    used = resp.headers.get("x-requests-used")
                    remaining = resp.headers.get("x-requests-remaining")
                    if used or remaining:
                        logger.info(
                            "Odds API credits: used=%s remaining=%s (%s)",
                            used or "?", remaining or "?", _log_url,
                        )
                return resp.json()
            # Retry on 429 with exponential backoff
            if resp.status_code == 429 and attempt < max_retries:
                wait = 2 ** attempt
                logger.info(
                    "429 from %s — retrying in %ds (attempt %d/%d)",
                    _log_url, wait, attempt + 1, max_retries,
                )
                await asyncio.sleep(wait)
                continue
            # Log response body on client errors to aid debugging
            body_snippet = ""
            if 400 <= resp.status_code < 500:
                try:
                    body_snippet = resp.text[:300]
                except Exception:
                    body_snippet = "(could not read body)"
            logger.warning(
                "HTTP %d from %s%s",
                resp.status_code,
                _log_url,
                f" — {body_snippet}" if body_snippet else "",
            )
            return None
        except httpx.TimeoutException:
            logger.warning("Timeout (%.0fs) for %s", timeout, _log_url)
            return None
        except httpx.ConnectError as exc:
            logger.warning("Connection failed for %s: %s", _log_url, exc)
            return None
        except Exception as exc:
            logger.warning("Request failed for %s: %s", _log_url, exc)
            return None
    return None
