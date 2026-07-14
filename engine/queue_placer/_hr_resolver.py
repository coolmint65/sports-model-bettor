"""Turn a queue pick into an HR-native `placeBets` payload part.

The relay is a thin passthrough — it takes the HR-native payload we
build here, adds the session token from its Chrome context, and POSTs
to HR's placement endpoint. So this module owns:

  1. Querying HR's GraphQL live for the pick's market + selection.
  2. Mapping our (bet_type, pick-text) vocabulary to HR's market-type
     code + player slot (A/B).
  3. Picking the selection whose price is inside the line-drift
     tolerance versus what the queue captured.
  4. Emitting the payload shape HR's `placeBets` expects, minus the
     session token / accountId (those live on the relay).

Tennis is the only sport plumbed today. Adding others is a matter of
extending `_MARKET_MAP` + one dispatcher entry.

Design notes:
  - GraphQL `selection` already carries `id` (selectionId), `odds`
    (decimal), and `rootIdx` — so we skip the WS subscribe step for
    prematch resolution. WS is only needed when GraphQL odds are stale
    versus the live line, which for prematch tennis they aren't.
  - We match matches by player-pair signature (order-independent) so
    the queue's `matchup` string can be "A vs B" or "B vs A".
"""
from __future__ import annotations
import logging
import re
import unicodedata
from typing import Any

from . import _config, _hr_ladder

logger = logging.getLogger(__name__)


# ── Public entry ─────────────────────────────────────────────

# Sports the resolver knows how to translate into HR-native payloads.
# Adding a new sport = extend `resolve()` dispatch + market map. Any
# sport not in this set is filtered upstream by the placer so we don't
# fill the log with pointless rejections.
SUPPORTED_SPORTS: frozenset[str] = frozenset({"tennis"})


class ResolveError(Exception):
    """Resolver couldn't turn the pick into an HR payload part."""


class LineDrift(ResolveError):
    """HR's current price moved past our tolerance versus the captured
    line. Caller should log status='rejected_line' and NOT retry until
    the pick reprices."""


class MarketUnavailable(ResolveError):
    """The market or selection isn't on HR's board right now (suspended,
    match not up yet, etc). Retryable next sweep."""


def resolve(pick: dict) -> dict:
    """Turn a pick into `{selection_id, decimal, period, line,
    market_id, market_type, side}`.

    Raises ResolveError subclasses on failure. Every failure classifies
    into a placer status (line drift = rejected_line, missing market =
    rejected_market/retryable).
    """
    sport = (pick.get("sport") or "").lower()
    if sport != "tennis":
        raise ResolveError(f"resolver only supports tennis, got {sport!r}")
    return _resolve_tennis(pick)


# ── Tennis resolver ─────────────────────────────────────────

# Bet-types that need the player side (A=position 0, B=position 1) to
# pick the HR market-type code.
_PER_PLAYER_TYPES = {
    "WIN_AT_LEAST_ONE_SET": {"A": "TENNIS:FT:A:WAL1S",
                              "B": "TENNIS:FT:B:WAL1S"},
    "PLAYER_TOTAL_GAMES":   {"A": "TENNIS:FT:A:OU",
                              "B": "TENNIS:FT:B:OU"},
}

# Bet-types with a single HR market-type regardless of side.
_SHARED_TYPES = {
    "ML":          "TENNIS:FT:ML",
    "TOTAL_GAMES": "TENNIS:FT:OU",
    "TOTAL_SETS":  "TENNIS:FT:STOT",
    "SET_SPREAD":  "TENNIS:FT:SHCP",
    "GAME_SPREAD": "TENNIS:FT:SPRD",
    "SET_BETTING": "TENNIS:FT:CSFLEX",
    "MOST_GAMES":  "TENNIS:FT:MOSTGAMES",
    "SET_WINNER":  "TENNIS:P:ML",
}


def _resolve_tennis(pick: dict) -> dict:
    matchup = pick.get("matchup") or ""
    bet_type = (pick.get("bet_type") or "").upper()
    pick_text = (pick.get("pick") or "").strip()

    p1, p2 = _split_matchup(matchup)
    if not p1 or not p2:
        raise ResolveError(f"can't split matchup {matchup!r}")

    event = _find_event(p1, p2)
    if event is None:
        raise MarketUnavailable(f"HR has no live event for {p1} vs {p2}")

    # HR labels position 0 as A and position 1 as B. Our matchup
    # doesn't know which player HR calls A, so infer from participants.
    a_name = _participant_by_position(event, 0)
    b_name = _participant_by_position(event, 1)
    if not a_name or not b_name:
        raise ResolveError("HR event missing A/B participants")

    picked_side = _side_for_pick(pick_text, a_name, b_name)

    market_type = _market_type(bet_type, picked_side)
    if not market_type:
        raise ResolveError(f"no HR market-type for {bet_type} side={picked_side}")

    market, selection = _find_market_and_selection(
        event, market_type, bet_type, pick_text, picked_side,
    )
    if not selection:
        raise MarketUnavailable(
            f"selection not on HR board (market={market_type}, "
            f"pick={pick_text!r})"
        )
    if market.get("suspended"):
        raise MarketUnavailable(f"HR market {market_type} suspended")

    # HR's placeBets compares odds.decimal byte-for-byte against the
    # ladder value at the selection's rootIdx. GraphQL's `selection.odds`
    # is a *display* field that can drift from the ladder; sending it
    # produces an opaque INTERNAL_ERROR (not a clean NEW_ODDS reject).
    # So we ignore selection.odds and read the exact ladder string.
    root_idx = selection.get("rootIdx")
    ladder_decimal = _hr_ladder.get_decimal(root_idx)
    if not ladder_decimal:
        raise MarketUnavailable(
            f"HR ladder has no decimal for rootIdx={root_idx!r} "
            f"(selection {selection.get('id')})"
        )
    try:
        ladder_decimal_f = float(ladder_decimal)
    except ValueError:
        raise MarketUnavailable(
            f"HR ladder decimal not numeric: {ladder_decimal!r}"
        ) from None
    if ladder_decimal_f <= 1.0:
        raise MarketUnavailable("HR ladder decimal <= 1.0")

    _check_line_drift(pick, ladder_decimal_f)

    return {
        "selection_id": str(selection.get("id") or ""),
        # STRING — must go to the payload verbatim. `decimal_float`
        # is provided for drift/log math only.
        "decimal":        ladder_decimal,
        "decimal_float":  ladder_decimal_f,
        "period":       str(market.get("period") or "M"),
        "line":         _to_float(market.get("line")),
        "market_id":    str(market.get("id") or ""),
        "market_type":  market_type,
        "side":         picked_side,
        "event_id":     str(event.get("id") or ""),
        "root_idx":     root_idx,
    }


# ── Live GraphQL event lookup ───────────────────────────────

_EVENT_CACHE: dict[str, Any] = {"index": None, "ts": 0.0}
_EVENT_CACHE_TTL_S = 20   # short — placer sweeps every 60s and we
                          # want fresh odds for drift checks


def _find_event(p1: str, p2: str) -> dict | None:
    """Return the raw HR event dict with markets+selections intact.
    Uses a 20s in-process cache so a single sweep's back-to-back
    picks share one HR trip."""
    import time
    now = time.time()
    idx = _EVENT_CACHE.get("index")
    if idx is None or (now - _EVENT_CACHE["ts"]) > _EVENT_CACHE_TTL_S:
        idx = _build_tennis_event_index()
        _EVENT_CACHE["index"] = idx
        _EVENT_CACHE["ts"] = now
    key = frozenset({_norm(p1), _norm(p2)})
    return idx.get(key)


def _build_tennis_event_index() -> dict[frozenset[str], dict]:
    """Enumerate every tennis compId → fetch events → index by
    normalized player pair."""
    # Deferred import — scrapers pkg pulls in curl_cffi which is heavy
    # relative to a placer sweep.
    from scrapers.hardrock_odds import (
        _fetch_sports_tree, _resolve_comp_ids, _fetch_events_for_comp,
    )
    tree, err = _fetch_sports_tree()
    if err:
        logger.warning("HR sports tree fetch failed: %s", err)
        return {}
    comp_ids = _resolve_comp_ids("tennis", tree)
    if not comp_ids:
        return {}
    out: dict[frozenset[str], dict] = {}
    for cid in comp_ids:
        events, e_err = _fetch_events_for_comp(cid)
        if e_err:
            logger.debug("HR tennis comp=%s failed: %s", cid, e_err)
            continue
        for ev in events:
            a = _participant_by_position(ev, 0)
            b = _participant_by_position(ev, 1)
            if not a or not b:
                continue
            out[frozenset({_norm(a), _norm(b)})] = ev
    return out


# ── Market + selection matchers ─────────────────────────────

def _market_type(bet_type: str, side: str | None) -> str | None:
    if bet_type in _SHARED_TYPES:
        return _SHARED_TYPES[bet_type]
    per_player = _PER_PLAYER_TYPES.get(bet_type)
    if per_player and side in per_player:
        return per_player[side]
    return None


def _find_market_and_selection(
    event: dict, market_type: str, bet_type: str,
    pick_text: str, picked_side: str | None,
) -> tuple[dict, dict | None]:
    """Locate the specific market + selection that matches the pick.

    For per-player OU-shape markets there's one market per player, so
    finding the right market is enough — the market's `line` and
    over/under selection.type disambiguate.

    For SHCP / SPRD we match on both the line value and the handicap
    side (AH/BH). For CSFLEX (set betting) the selection.type IS the
    score string, so we match on the pick text directly.
    """
    line_from_pick = _line_from_pick_text(pick_text)
    over_under_from_pick = _over_under_from_pick_text(pick_text)

    candidates = [m for m in (event.get("markets") or [])
                   if (m.get("type") or "").upper() == market_type.upper()]

    for market in candidates:
        selections = market.get("selection") or []

        if bet_type == "ML":
            for sel in selections:
                sel_type = (sel.get("type") or "").upper()
                if sel_type == picked_side:
                    return market, sel

        elif bet_type == "SET_WINNER":
            for sel in selections:
                sel_type = (sel.get("type") or "").upper()
                if sel_type == picked_side:
                    return market, sel

        elif bet_type in ("TOTAL_GAMES", "TOTAL_SETS", "PLAYER_TOTAL_GAMES"):
            # Line must match (within 0.01). Selection.type is Over/Under.
            market_line = _to_float(market.get("line"))
            if line_from_pick is not None and market_line is not None \
                    and abs(market_line - line_from_pick) > 0.01:
                continue
            for sel in selections:
                sel_type = (sel.get("type") or "").lower()
                if sel_type == over_under_from_pick:
                    return market, sel

        elif bet_type in ("SET_SPREAD", "GAME_SPREAD"):
            # HR ships one market per handicap point. Selection.type
            # AH = player A handicap; BH = player B handicap. Line
            # sign is the handicap the player carries (e.g. "Kim +1.5"
            # → we want AH selection at line=+1.5 when Kim is A).
            wanted_side = "AH" if picked_side == "A" else "BH"
            for sel in selections:
                sel_type = (sel.get("type") or "").upper()
                if sel_type != wanted_side:
                    continue
                sel_line = _line_from_selection_name(sel.get("name") or "")
                if line_from_pick is None or sel_line is None:
                    return market, sel  # can't compare lines — take it
                if abs(sel_line - line_from_pick) < 0.01:
                    return market, sel

        elif bet_type == "SET_BETTING":
            # pick_text is "2-0 p1" / "2-1 p2" etc. Selection.type is
            # the score string ("2-0", "2-1", ...). HR's shape doesn't
            # bake the player-slot into the type, so the market's own
            # A/B interpretation is fixed — trust HR to have one
            # market per score+winner.
            score = pick_text.split()[0] if " " in pick_text else pick_text
            for sel in selections:
                if (sel.get("type") or "").strip() == score:
                    return market, sel

        elif bet_type == "MOST_GAMES":
            for sel in selections:
                sel_type = (sel.get("type") or "").upper()
                if sel_type == picked_side:
                    return market, sel

        elif bet_type == "WIN_AT_LEAST_ONE_SET":
            # Market is already player-specific (A:WAL1S vs B:WAL1S).
            # Selection.type is Yes / No.
            yes_no = "yes" if pick_text.lower().endswith("yes") else "no"
            for sel in selections:
                if (sel.get("type") or "").lower() == yes_no:
                    return market, sel

    return {}, None


# ── Line-drift check ────────────────────────────────────────

def _check_line_drift(pick: dict, live_decimal: float) -> None:
    """Compare HR's current implied prob vs the captured US odds. Bail
    if the price moved past `_config.LINE_DRIFT_TOLERANCE_PP`."""
    captured_us = pick.get("odds")
    if captured_us in (None, 0):
        return  # can't compare — trust the live price
    try:
        captured_us = int(captured_us)
    except (TypeError, ValueError):
        return
    live_imp = 1.0 / live_decimal
    captured_imp = _american_to_implied(captured_us)
    if captured_imp is None:
        return
    drift_pp = abs(live_imp - captured_imp) * 100.0
    tol = _config.LINE_DRIFT_TOLERANCE_PP
    if drift_pp > tol + 1e-6:
        raise LineDrift(
            f"line moved {drift_pp:.2f}pp > {tol:.2f}pp "
            f"(captured {captured_us:+d}, HR now {live_decimal:.3f})"
        )


def _american_to_implied(us: int) -> float | None:
    if not us:
        return None
    if us > 0:
        return 100.0 / (us + 100.0)
    return (-us) / ((-us) + 100.0)


# ── Payload builder ─────────────────────────────────────────

def build_placebets_payload(
    pick: dict, resolved: dict, *, account_id: str,
    stake_dollars: float, req_id: int | None = None,
    accept_price_change: bool = False,
) -> dict:
    """Compose the exact `PlaceBetsRequest` shape HR expects.

    Structural details HR is strict about (confirmed 2026-07-14 by
    diffing our bounced bytes vs PiBot's working payload):
      - `bets` is an OBJECT `{bet: [...]}`, NOT an array
        `[{bet: [...]}]`. The extra array wrapper was the primary
        INTERNAL_ERROR trigger.
      - `odds.decimal` is a JSON NUMBER (float). Strings bounce.
      - `acceptPriceChange` is a JSON BOOLEAN (false/true), not
        the strings "false"/"true".
      - `stake.amount` is a JSON NUMBER (whole amounts as int, not 1.0).

    Line is only sent for markets that have one (totals / spreads).
    WAL1S / ML omit it.
    """
    from time import time as _time
    bet_type = (pick.get("bet_type") or "").upper()
    include_line = bet_type in {
        "TOTAL_GAMES", "TOTAL_SETS", "PLAYER_TOTAL_GAMES",
        "SET_SPREAD", "GAME_SPREAD",
    }

    # decimal is a number in the payload. We stored the exact ladder
    # STRING in `resolved["decimal"]` so we could send it byte-for-byte
    # earlier — but HR actually wants a number, and `decimal_float`
    # preserves the ladder value as a float exactly. Fall back to
    # parseFloat of the string when the resolver didn't stamp the
    # numeric variant (old callers).
    decimal_num = resolved.get("decimal_float")
    if decimal_num is None:
        try:
            decimal_num = float(resolved["decimal"])
        except (TypeError, ValueError, KeyError) as e:
            raise ResolveError(f"bad ladder decimal: {e}") from e

    part: dict[str, Any] = {
        "partNo": 1,
        "selectionId": resolved["selection_id"],
        "oddsType": "MONEYLINE",
        "odds": {"decimal": decimal_num},
        "period": resolved.get("period") or "M",
    }
    if include_line and resolved.get("line") is not None:
        part["line"] = resolved["line"]

    # Whole-dollar stakes → int (matches PiBot's shape); fractional
    # cents stay as float so partial-dollar amounts still serialize.
    stake = float(stake_dollars)
    stake_out = int(stake) if stake == int(stake) else stake

    return {
        "PlaceBetsRequest": {
            "accountId": str(account_id),
            "bets": {
                "bet": [{
                    "type": "SINGLE",
                    "winType": "WIN",
                    "stake": {"amount": stake_out, "currency": "USD"},
                    "parts": {"betPart": [part]},
                }],
            },
            "reqId": int(req_id if req_id is not None
                          else _time() * 1000),
            # Boolean, not string. False on attempt 1 so a stable
            # line goes through at the price we're expecting; the
            # caller retries with True on a NEW_ODDS reject to
            # accept HR's moved price.
            "acceptPriceChange": bool(accept_price_change),
            "unityAccountId": "",
        }
    }


# ── Small helpers ───────────────────────────────────────────

def _split_matchup(matchup: str) -> tuple[str, str] | tuple[None, None]:
    """'Alice Smith vs Bob Jones' → ('Alice Smith', 'Bob Jones').
    Also handles ' @ ' and ' - ' delimiters."""
    for delim in (" vs ", " @ ", " - "):
        if delim in matchup:
            a, _, b = matchup.partition(delim)
            return a.strip(), b.strip()
    return None, None


def _norm(name: str) -> str:
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(ascii_only.lower().split())


def _participant_by_position(event: dict, pos: int) -> str | None:
    for p in event.get("participants") or []:
        raw = p.get("position")
        if raw is None:
            continue
        try:
            if int(raw) == pos:
                return p.get("name")
        except (TypeError, ValueError):
            continue
    return None


def _side_for_pick(pick_text: str, a_name: str, b_name: str) -> str | None:
    """Return 'A' if pick_text starts with (or contains) player A's
    name, 'B' for player B, or None for direction-only picks (Over /
    Under)."""
    n = _norm(pick_text)
    na = _norm(a_name)
    nb = _norm(b_name)
    if na and (n.startswith(na) or na in n):
        return "A"
    if nb and (n.startswith(nb) or nb in n):
        return "B"
    return None


def _line_from_pick_text(text: str) -> float | None:
    """Extract a numeric line from '<player> +1.5' / 'Over 24.5' /
    'Under 2.5'. Returns None if no numeric tail present."""
    m = re.search(r"([+-]?\d+(?:\.\d+)?)\s*$", (text or "").strip())
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _over_under_from_pick_text(text: str) -> str | None:
    t = (text or "").strip().lower()
    if t.startswith("over"):
        return "over"
    if t.startswith("under"):
        return "under"
    return None


def _line_from_selection_name(name: str) -> float | None:
    m = re.search(r"([+-]?\d+(?:\.\d+)?)\s*$", (name or "").strip())
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
