"""HTTP client for the Pi DK placement service (dk-place-server.js).

Unlike the HR relay, DK resolution + placement happen server-side on the
Pi (it owns dk.sh + the DK board fetch). We just POST the pick and map the
response into the placer's status vocabulary — the same shape _relay
returns, so _place_one treats DK and HR uniformly. Reuses _relay's
exception classes so the placer's except-clauses catch both books.
"""
from __future__ import annotations
import json
import logging
from urllib import request, error

from . import _config
from ._relay import RelayError, RelayTransient, RelayFatal, RelayOffline  # noqa: F401 (re-export for callers)

logger = logging.getLogger(__name__)

# dk.sh placebet can take a while (login + navurl + accordion-expand + fire),
# and the Pi resolves the board on a cold cache. Generous ceiling; the Pi
# side has its own 240s dk.sh timeout inside this.
_TIMEOUT_S = 275.0


def place_bet(pick: dict, *, dedup_key: str) -> dict:
    """POST the pick to the Pi DK service. Returns the placer's response
    shape. Raises RelayOffline/RelayTransient/RelayFatal on failure so
    _place_one classifies it exactly like an HR relay failure."""
    base = _config.dk_place_url()
    if not base:
        raise RelayOffline("DK_PLACE_URL not set")
    body = {
        "token": _config.dk_place_token(),
        "sport": pick.get("sport"),
        "matchup": pick.get("matchup"),
        "pick": pick.get("pick"),
        "bet_type": pick.get("bet_type"),
        "stake_dollars": pick.get("stake_dollars"),
        "dedup_key": dedup_key,
    }
    data = json.dumps(body).encode("utf-8")
    req = request.Request(base.rstrip("/") + "/place", data=data, method="POST",
                          headers={"Content-Type": "application/json", "Accept": "application/json"})
    try:
        with request.urlopen(req, timeout=_TIMEOUT_S) as resp:
            payload = resp.read().decode("utf-8")
            out = json.loads(payload) if payload else {}
    except error.HTTPError as e:
        if e.code == 403:
            raise RelayFatal("DK place service rejected token (403)") from e
        if e.code >= 500 or e.code == 429:
            raise RelayTransient(f"dk place {e.code}") from e
        raise RelayFatal(f"dk place {e.code}") from e
    except (error.URLError, TimeoutError, OSError) as e:
        raise RelayTransient(f"dk place unreachable: {e}") from e

    status = (out.get("status") or "").lower()
    # Board/login problems on the Pi side are retryable — surface as
    # transient so the pick stays eligible for the next sweep.
    if status == "rejected_offline":
        raise RelayTransient(f"dk service offline: {out.get('reject_reason')}")

    # The service already speaks our status vocab (placed / rejected_market
    # / rejected_other). Reuse the hr_ticket_id column for DK's event id.
    return {
        "status": out.get("status"),
        "reject_reason": out.get("reject_reason"),
        "placed_stake_d": out.get("placed_stake_d"),
        "placed_odds": out.get("placed_odds"),   # American int from dk-place-server
                                                 # (real DK board odds); None for tennis/legacy
        "line_drift_pp": None,
        "hr_ticket_id": out.get("event_id"),
    }


def poll_result(dedup_key: str) -> dict:
    """DK places synchronously (no queued/submitted state), so there's
    nothing to poll. Present only to satisfy the relay interface."""
    raise RelayError("dk placement is synchronous; no poll")


def session_pull() -> str:
    raise RelayError("dk service manages its own session")
