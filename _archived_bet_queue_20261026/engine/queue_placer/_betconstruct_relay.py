"""HTTP client for the Pi BetConstruct placement service
(books/_shared/betconstruct-place-server.js on :9100). Serves BOTH Zotabet
and Betandplay — the pick's `book` selects which. Like DK, resolution +
placement happen server-side on the Pi (it owns the netns + cookie session);
we POST the pick and map the response into the placer's status vocabulary,
the same shape _relay/_dk_relay return so _place_one treats every book
uniformly. Mirrors _dk_relay. WNBA only (all 6 markets). Built 2026-07-31.
"""
from __future__ import annotations
import json
import logging
from urllib import request, error

from . import _config
from ._relay import RelayError, RelayTransient, RelayFatal, RelayOffline  # noqa: F401 (re-export)

logger = logging.getLogger(__name__)

# BetConstruct placement is a single netns-routed POST; fast relative to DK.
_TIMEOUT_S = 60.0


def place_bet(pick: dict, *, dedup_key: str) -> dict:
    """POST the pick to the Pi BetConstruct service for pick['book']
    (zotabet|betandplay). Raises RelayOffline/RelayTransient/RelayFatal on
    failure so _place_one classifies it exactly like an HR/DK relay failure."""
    base = _config.betconstruct_place_url()
    if not base:
        raise RelayOffline("BETCONSTRUCT_PLACE_URL not set")
    book = (pick.get("book") or "").lower()
    if book not in ("zotabet", "betandplay"):
        raise RelayFatal(f"betconstruct relay: bad book {book!r}")
    body = {
        "token": _config.betconstruct_place_token(),
        "book": book,
        "matchup": pick.get("matchup"),
        "pick": pick.get("pick"),
        "bet_type": pick.get("bet_type"),
        "line": pick.get("line"),
        "odds": pick.get("odds"),
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
            raise RelayFatal("betconstruct place service rejected token (403)") from e
        if e.code >= 500 or e.code == 429:
            raise RelayTransient(f"betconstruct place {e.code}") from e
        raise RelayFatal(f"betconstruct place {e.code}") from e
    except (error.URLError, TimeoutError, OSError) as e:
        raise RelayTransient(f"betconstruct place unreachable: {e}") from e

    status = (out.get("status") or "").lower()
    # VPN/session problems on the Pi side are retryable — surface transient
    # so the pick stays eligible for the next sweep.
    if status == "rejected_offline":
        raise RelayTransient(f"betconstruct service offline: {out.get('reject_reason')}")

    # The service already speaks our status vocab (placed / rejected_market /
    # rejected_other). Reuse hr_ticket_id for the book's bet id (settlement key).
    return {
        "status": out.get("status"),
        "reject_reason": out.get("reject_reason"),
        "placed_stake_d": out.get("placed_stake_d"),
        "placed_odds": out.get("odds"),
        "line_drift_pp": None,
        "hr_ticket_id": out.get("bet_id") or out.get("event_id"),
    }


def poll_result(dedup_key: str) -> dict:
    """BetConstruct places synchronously (bet id returned inline), so there's
    nothing to poll. Present only to satisfy the relay interface."""
    raise RelayError("betconstruct placement is synchronous; no poll")


def session_pull() -> str:
    raise RelayError("betconstruct service manages its own session")
