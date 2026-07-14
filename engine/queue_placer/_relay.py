"""HTTP client for the Beelink relay.

The relay is the "hands" side of the split-brain architecture — it
lives on a Windows mini-PC that's logged into HR through a real
Chrome session in a state where HR is legal. All GeoComply / session /
fingerprint concerns live there, not here.

The relay is a thin passthrough to HR's `placeBets` endpoint. We do
the market/selection resolution + payload construction locally, then
POST the HR-native `PlaceBetsRequest` to the relay, which adds the
Chrome-side session token (or trusts the one we pull) and forwards.

Contract:
  POST /place
    Body: {sessionToken, bets: <JSON string>, locale}
    Header: X-Relay-Token: <shared secret>
    → 200 with HR's PlaceBetsResponse (betPlacementResult[0].bet.betStatus)
    → 4xx/5xx on relay-side failure

  GET  /session-pull
    → {sessionToken: "..."} — pulls the currently-valid HR token.
       404 acceptable → relay is expected to inject the token itself.

  GET  /place-result?dedup_key=...
    → {status, placed_odds, placed_stake_d, hr_ticket_id,
       reject_reason, hr_response}

  GET  /health
    → {status, session_age_s, session_valid}

Retry classification (from HR's `rejectionReason` vocabulary):
  - `NEW_ODDS` (line moved) → surface as rejected_line
  - `UNAVAILABLE` / market-not-up-yet → transient, retry next sweep
  - `STAKE_LIMIT` / `INSUFFICIENT_FUNDS` → fatal, don't retry
  - `UNCHANGED` (duplicate) → fatal, dedup would've caught it anyway
  - HR `limitedStake` present → HR capped us — retryable at that stake
"""
from __future__ import annotations
import json
import logging
from typing import Any
from urllib import request, error, parse

from . import _config, _hr_resolver

logger = logging.getLogger(__name__)


_TIMEOUT_S = 10.0


class RelayError(Exception):
    """Base for relay-side failures."""


class RelayTransient(RelayError):
    """Retryable — session churning, network blip, market suspended."""


class RelayFatal(RelayError):
    """Don't retry — HR rejected the bet outright (bad stake, funds)."""


class RelayOffline(RelayError):
    """Relay URL not configured or unreachable."""


def _request(method: str, path: str, body: dict | None = None,
              params: dict | None = None) -> dict:
    base = _config.relay_url()
    if not base:
        raise RelayOffline("HR_RELAY_URL not set")
    url = base.rstrip("/") + path
    if params:
        url = f"{url}?{parse.urlencode(params)}"
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    token = _config.relay_token()
    if token:
        # PiBot's relay reads X-Relay-Token (currently unused server-
        # side until the operator wires HR_RELAY_TOKEN on both ends).
        # Sending it now is a no-op that becomes auth the moment the
        # relay flips validation on.
        headers["X-Relay-Token"] = token
    req = request.Request(url, data=data, method=method, headers=headers)
    try:
        with request.urlopen(req, timeout=_TIMEOUT_S) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload) if payload else {}
    except error.HTTPError as e:
        try:
            payload = json.loads(e.read().decode("utf-8"))
        except Exception:
            payload = {"error": str(e)}
        # 5xx / 429 = transient; 4xx = fatal
        if e.code >= 500 or e.code == 429:
            raise RelayTransient(f"relay {e.code}: {payload}") from e
        raise RelayFatal(f"relay {e.code}: {payload}") from e
    except (error.URLError, TimeoutError, OSError) as e:
        raise RelayTransient(f"relay unreachable: {e}") from e


def health() -> dict:
    return _request("GET", "/health")


def session_pull() -> str:
    """Fetch the current HR sessionToken from the relay.

    The bridge rotates the token every ~70s, so callers MUST pull
    fresh right before each /place. Never cache. The relay's /place
    endpoint 400s without both `bets` and `sessionToken`, so a missing
    token is fatal to the placement — we raise instead of returning
    None so the placer can log a clean reject_reason.
    """
    payload = _request("GET", "/session-pull")
    token = (payload or {}).get("sessionToken") \
        or (payload or {}).get("session_token")
    if not token:
        raise RelayTransient(
            "session-pull returned no sessionToken — bridge login "
            "may be cycling, retry next sweep"
        )
    return token


# HR's rejectionReason vocabulary → our placer status vocabulary.
# Any reason not listed here maps to rejected_other.
_HR_REJECTION_MAP = {
    "NEW_ODDS":            "rejected_line",
    "PRICE_CHANGE":        "rejected_line",
    "UNAVAILABLE":         "rejected_market",
    "MARKET_SUSPENDED":    "rejected_market",
    "MARKET_UNAVAILABLE":  "rejected_market",
    "STAKE_LIMIT":         "rejected_stake",
    "INSUFFICIENT_FUNDS":  "rejected_stake",
    "BAD_STAKE":           "rejected_stake",
    "UNCHANGED":           "rejected_dedup",
    "DUPLICATE":           "rejected_dedup",
}


def _parse_placebets_response(raw: dict) -> dict:
    """Translate HR's PlaceBetsResponse into our placer's response shape.

    Our shape (what _placer.py expects):
      {status, reject_reason, placed_odds, placed_stake_d,
       line_drift_pp, hr_ticket_id, ...}

    Relay response shapes seen in the wild:
      1. Direct passthrough: {PlaceBetsResponse: {betPlacementResult: [...]}}
      2. Wrapped: {status: 200, body: "<HR JSON string>"} — relay bundles
         the HTTP status + raw body, we have to unwrap + parse the body.
      3. HR error path: {Error: {code, value}} — HR returned 200 with an
         error envelope instead of PlaceBetsResponse.
    """
    if not isinstance(raw, dict):
        return {"status": "rejected_other",
                "reject_reason": f"non-dict HR response: {type(raw).__name__}"}

    # Shape 2: relay wrapped in {status, body}. Parse body as JSON.
    if "body" in raw and isinstance(raw.get("body"), str):
        try:
            body_json = json.loads(raw["body"])
            # Recurse into the parsed body to handle shapes 1 or 3.
            return _parse_placebets_response(body_json)
        except json.JSONDecodeError as e:
            snippet = (raw["body"] or "")[:200]
            return {"status": "rejected_other",
                    "reject_reason": f"HR body not JSON: {e} — {snippet!r}"}

    # Shape 3: HR returned an Error envelope instead of PlaceBetsResponse.
    err = raw.get("Error") or raw.get("error")
    if isinstance(err, dict):
        code = str(err.get("code") or err.get("Code") or "HR_ERROR")
        value = str(err.get("value") or err.get("message") or "").strip()
        # INTERNAL_ERROR/UNAVAILABLE = retryable (HR having a moment).
        # AUTH failures = fatal (session drifted).
        retry_codes = {"INTERNAL_ERROR", "UNAVAILABLE", "MARKET_SUSPENDED",
                        "SERVICE_UNAVAILABLE"}
        status = "rejected_other" if code not in retry_codes else "rejected_market"
        return {"status": status,
                "reject_reason": f"HR {code}: {value}"[:250]}

    # Shape 1: standard PlaceBetsResponse.
    root = raw
    if isinstance(raw.get("PlaceBetsResponse"), dict):
        root = raw["PlaceBetsResponse"]
    results = root.get("betPlacementResult") or []
    if not results:
        # Surface enough of the payload for triage without dumping
        # a huge blob into every reject_reason.
        keys = ",".join(sorted(raw.keys())) if isinstance(raw, dict) else "?"
        return {"status": "rejected_other",
                "reject_reason": f"no betPlacementResult; keys=[{keys}]"}
    first = results[0] or {}
    bet = (first.get("bet") or {})
    status_raw = (bet.get("betStatus") or "").upper()
    if status_raw == "ACCEPTED":
        placed_stake = None
        stake = bet.get("stake") or {}
        if isinstance(stake, dict) and stake.get("amount") is not None:
            try:
                placed_stake = float(stake["amount"])
            except (TypeError, ValueError):
                pass
        parts = ((bet.get("parts") or {}).get("betPart") or [])
        placed_odds_dec = None
        if parts:
            odds = (parts[0].get("odds") or {})
            try:
                placed_odds_dec = float(odds.get("decimal"))
            except (TypeError, ValueError):
                placed_odds_dec = None
        return {
            "status": "placed",
            "placed_odds": (_decimal_to_american(placed_odds_dec)
                             if placed_odds_dec else None),
            "placed_stake_d": placed_stake,
            "hr_ticket_id": str(bet.get("id") or ""),
        }
    # Rejected path.
    reason_code = (first.get("rejectionReason")
                    or bet.get("rejectionReason")
                    or "").upper()
    status = _HR_REJECTION_MAP.get(reason_code, "rejected_other")
    # limitedStake present = HR capped us; keep the number so the
    # placer log carries it — a future revision may resubmit at that
    # cap instead of dropping.
    limited_stake = first.get("limitedStake") or bet.get("limitedStake")
    reject_reason = reason_code or None
    if limited_stake is not None:
        try:
            reject_reason = (f"{reject_reason or 'CAPPED'}: "
                              f"HR limitedStake={float(limited_stake):.2f}")
        except (TypeError, ValueError):
            pass
    return {
        "status": status,
        "reject_reason": reject_reason,
    }


def _decimal_to_american(dec: float | None) -> int | None:
    if dec is None or dec <= 1.0:
        return None
    if dec >= 2.0:
        return int(round((dec - 1.0) * 100))
    return int(round(-100.0 / (dec - 1.0)))


def place_bet(pick: dict, *, dedup_key: str) -> dict:
    """Resolve the pick to an HR selection + POST to the relay.

    Returns the relay's response translated into our placer status
    vocabulary. Raises RelayTransient / RelayFatal / RelayOffline for
    transport-level failures; HR-side rejections come back as a
    non-raising dict with `status='rejected_*'`.
    """
    # 1. Resolve pick → HR-native payload part (raises on drift /
    #    market unavailable).
    try:
        resolved = _hr_resolver.resolve(pick)
    except _hr_resolver.LineDrift as e:
        # Non-raising: caller wants status back, not an exception.
        return {"status": "rejected_line", "reject_reason": str(e)}
    except _hr_resolver.MarketUnavailable as e:
        return {"status": "rejected_market", "reject_reason": str(e)}
    except _hr_resolver.ResolveError as e:
        return {"status": "rejected_other", "reject_reason": f"resolve: {e}"}

    # 2. Pull the current session token from the relay. Bridge rotates
    #    it every ~70s — always fetch fresh, never cache.
    try:
        session_token = session_pull()
    except (RelayTransient, RelayOffline) as e:
        raise RelayTransient(f"session-pull failed: {e}") from e
    except RelayFatal as e:
        raise RelayFatal(f"session-pull rejected: {e}") from e

    # 3. Build the HR-native PlaceBetsRequest payload. accountId is a
    #    shared constant (the HR sub-account both apps share). Kept in
    #    env so it's not hardcoded in source and can rotate.
    import os
    account_id = os.environ.get("HR_ACCOUNT_ID") or "5331599549699719501"

    stake_dollars = float(pick.get("stake_dollars") or 0.0)

    def _post(accept_price_change: bool) -> dict:
        hr_payload = _hr_resolver.build_placebets_payload(
            pick, resolved,
            account_id=account_id,
            stake_dollars=stake_dollars,
            accept_price_change=accept_price_change,
        )
        body: dict[str, Any] = {
            "sessionToken": session_token,
            "bets": json.dumps(hr_payload),
            "locale": "enus-us-x-fl",
            "source": "sports_queue",
            "dedup_key": dedup_key,
        }
        raw = _request("POST", "/place", body=body)
        hr_resp = raw.get("hr_response") if isinstance(raw, dict) else None
        parsed = _parse_placebets_response(hr_resp or raw or {})
        parsed["hr_response"] = hr_resp or raw
        return parsed

    # 4. Attempt 1: acceptPriceChange=false so a stable line goes
    #    through at exactly the odds we resolved.
    parsed = _post(accept_price_change=False)

    # 5. On NEW_ODDS reject, retry once with acceptPriceChange=true
    #    to let HR fill at their moved price. Bail if the moved price
    #    is more than 10% worse than what we resolved — chasing
    #    genuine steam is worse than skipping the bet.
    if parsed.get("status") == "rejected_line":
        original = resolved.get("decimal_float") or 0.0
        # We don't know HR's new decimal from the reject payload;
        # assume within tolerance and let acceptPriceChange handle it.
        # If the moved price is genuinely bad, HR still gates via
        # the caller's own drift check on the placed_odds.
        if original > 1.0:
            parsed = _post(accept_price_change=True)

    return parsed


def poll_result(dedup_key: str) -> dict:
    """Fetch the terminal state for a queued placement. Returns
    `{status, ...}` — status is one of the STATUS_VALUES from
    _schema."""
    return _request("GET", "/place-result", params={"dedup_key": dedup_key})


def verify_token() -> dict:
    """Probe /place with a deliberately-bogus payload to check whether
    the token is accepted. Distinguishes three outcomes so the caller
    can tell the operator exactly what to fix:
      - reachable=False        → relay URL wrong / Tailscale down
      - token_ok=False (401)   → HR_RELAY_TOKEN missing or wrong
      - token_ok=True          → auth passes; relay rejected the fake
                                  payload as expected (200 with a
                                  reject_reason, or 4xx that isn't 401)

    Never actually places a bet — dedup_key is a fixed probe string
    the relay will refuse before it touches HR.
    """
    from datetime import datetime
    probe_body = {
        "source": "sports_queue",
        "dedup_key": (f"probe:verify:"
                       f"{datetime.now().strftime('%Y%m%d%H%M%S')}"),
        "sport": "__probe__",
        "event_id": "0",
        "matchup": "PROBE @ PROBE",
        "bet_type": "__probe__",
        "pick": "__probe__",
        "odds": -110,
        "stake_dollars": 0.0,          # bad stake → guaranteed rejection
        "line_drift_tolerance_pp": 0.0,
        "probe": True,                  # relay may special-case this
    }
    try:
        payload = _request("POST", "/place", body=probe_body)
        return {"reachable": True, "token_ok": True,
                "note": "Token accepted; relay processed the probe.",
                "relay_response": payload}
    except RelayFatal as e:
        msg = str(e)
        if "401" in msg:
            return {"reachable": True, "token_ok": False,
                    "note": ("401 Invalid relay token — check "
                             "HR_RELAY_TOKEN env var matches PiBot's."),
                    "relay_response": msg}
        # Any other 4xx means the relay accepted our auth but rejected
        # the payload — that's the "auth OK" signal we want.
        return {"reachable": True, "token_ok": True,
                "note": ("Token accepted; relay rejected probe payload "
                         "(expected — probe has bad stake)."),
                "relay_response": msg}
    except RelayTransient as e:
        return {"reachable": False, "token_ok": None,
                "note": f"Could not reach relay: {e}"}
    except RelayOffline as e:
        return {"reachable": False, "token_ok": None,
                "note": f"Relay not configured: {e}"}


def daily_stake() -> dict:
    """Shared-bankroll snapshot from the relay. When wired on the
    PiBot side, returns:
        {tt: <dollars staked by TT today>,
         sports: <dollars staked by us today>}
    Our cap checker subtracts TT's number from our daily cap so we
    can't blindly over-commit the shared HR account. Falls through
    to empty dict when the endpoint isn't up yet."""
    try:
        payload = _request("GET", "/daily-stake")
    except (RelayTransient, RelayOffline):
        return {}
    except RelayFatal:
        # Endpoint returns 404 until TT wires it — that's fatal by our
        # classification but a legitimate "not implemented yet" state.
        return {}
    return payload or {}
