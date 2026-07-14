"""HR price-ladder cache.

Every selection HR ships in a market carries a `rootIdx` — an integer
index into the shared price ladder. The ladder maps that index to the
canonical decimal odds HR's `placeBets` will accept.

Why we need this: HR's GraphQL `selection.odds` is a *display* price
that can drift from the exact ladder decimal at the same `rootIdx`.
`placeBets` insists on `odds.decimal` matching `ladder[rootIdx].decimal`
EXACTLY (down to the seven-decimal repeating value). A near-miss
throws an opaque INTERNAL_ERROR instead of a clean NEW_ODDS, so we
have no way to recover if we send anything but the ladder value.

The ladder itself is static per HR release — no need for TTL. Fetched
once per process boot and cached in-process. A miss (rootIdx not in
ladder) treats the payload as un-placeable and bubbles up.
"""
from __future__ import annotations
import logging
from typing import Any

logger = logging.getLogger(__name__)


_LADDER_URL = ("https://api.hardrocksportsbook.com/sportsbook/v1/api/"
                "getRootLadder")

# Values are the EXACT decimal-odds string HR returned in the ladder
# response — never re-serialized through float(). HR's `placeBets`
# compares byte-for-byte, so `2.4` (JSON number) is not the same as
# `"2.4"` (string); a mismatch throws INTERNAL_ERROR instead of a
# clean NEW_ODDS. Keep strings; convert to float only for drift math.
_ladder: dict[int, str] | None = None


def get_decimal(root_idx: Any) -> str | None:
    """Return the exact ladder-decimal STRING for a rootIdx. Callers
    must send this string verbatim in `odds.decimal` — don't cast to
    float first. Returns None on any failure."""
    if root_idx is None:
        return None
    try:
        idx = int(root_idx)
    except (TypeError, ValueError):
        return None
    ladder = _get_ladder()
    if not ladder:
        return None
    return ladder.get(idx)


def get_decimal_float(root_idx: Any) -> float | None:
    """Numeric variant for drift comparisons only. Do NOT use the
    return value to build a placement payload — always use get_decimal()
    to preserve HR's exact string format."""
    s = get_decimal(root_idx)
    if s is None:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _get_ladder() -> dict[int, str]:
    global _ladder
    if _ladder is not None:
        return _ladder
    _ladder = _fetch_ladder()
    return _ladder


def _fetch_ladder() -> dict[int, str]:
    """One-shot ladder fetch. Uses curl_cffi for TLS parity with the
    HR odds scraper (Cloudflare fingerprints TLS ClientHello). Falls
    back to urllib on hosts without curl_cffi — most calls will 403 in
    that path, but the empty ladder just short-circuits placements to
    a clean rejection rather than a crash."""
    import json
    try:
        raw = _http_get(_LADDER_URL)
    except Exception as e:
        logger.warning("HR ladder fetch failed: %s", e)
        return {}
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception as e:
        logger.warning("HR ladder JSON parse failed: %s", e)
        return {}
    # Shape (per TT): {PriceAdjustmentDetailsResponse: {rootLadder: [
    #   {rootIndex, decimal, moneyline, fractional}, ...
    # ]}}
    root = (data.get("PriceAdjustmentDetailsResponse") or data)
    entries = root.get("rootLadder") or root.get("root_ladder") or []
    out: dict[int, str] = {}
    for e in entries:
        try:
            idx = int(e.get("rootIndex"))
        except (TypeError, ValueError):
            continue
        dec = e.get("decimal")
        if not isinstance(dec, str) or not dec.strip():
            continue
        out[idx] = dec.strip()
    logger.info("HR ladder loaded: %d entries", len(out))
    return out


def _http_get(url: str, timeout: float = 15.0) -> bytes | None:
    # Mirror the hardrock_odds headers so Cloudflare accepts our fetch
    # from the same browser origin.
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:150.0) "
            "Gecko/20100101 Firefox/150.0"
        ),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin":  "https://app.hardrock.bet",
        "Referer": "https://app.hardrock.bet/",
    }
    try:
        from curl_cffi import requests as _cc  # type: ignore
        # Reuse the profile the scraper's already validated for HR
        # TLS. Scoped import so a missing package doesn't kill this
        # module at import time.
        from scrapers.hardrock_odds import _active_profile, _IMPERSONATE_PROFILES
        order = (
            (_active_profile,) + tuple(p for p in _IMPERSONATE_PROFILES
                                         if p != _active_profile)
        ) if _active_profile else _IMPERSONATE_PROFILES
        for profile in order:
            if profile is None:
                continue
            try:
                r = _cc.get(url, headers=headers,
                             impersonate=profile, timeout=timeout)
                if r.status_code == 200:
                    return r.content
            except Exception:
                continue
        return None
    except ImportError:
        pass
    import urllib.request
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def refresh() -> int:
    """Force a re-fetch. Returns the number of ladder entries loaded.
    Exposed for the operator when HR ships a new ladder version and
    a running process needs to pick it up without a restart."""
    global _ladder
    _ladder = _fetch_ladder()
    return len(_ladder or {})
