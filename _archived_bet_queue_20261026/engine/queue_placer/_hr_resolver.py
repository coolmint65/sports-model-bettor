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
SUPPORTED_SPORTS: frozenset[str] = frozenset({"wnba", "mlb"})


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
    if sport == "tennis":
        return _resolve_tennis(pick)
    if sport == "wnba":
        return _resolve_wnba(pick)
    if sport == "mlb":
        return _resolve_mlb(pick)
    raise ResolveError(f"resolver only supports tennis/wnba/mlb, got {sport!r}")


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


_OU_LINE_CAP_PTS = 1.0  # basketball O/U: max points HR's line may differ from our pick's


def _find_market_and_selection(
    event: dict, market_type: str, bet_type: str,
    pick_text: str, picked_side: str | None,
    period_q1: bool = False,
    line_override: float | None = None,
) -> tuple[dict, dict | None]:
    """Locate the specific market + selection that matches the pick.

    For per-player OU-shape markets there's one market per player, so
    finding the right market is enough — the market's `line` and
    over/under selection.type disambiguate.

    For SHCP / SPRD we match on both the line value and the handicap
    side (AH/BH). For CSFLEX (set betting) the selection.type IS the
    score string, so we match on the pick text directly.
    """
    line_from_pick = (line_override if line_override is not None
                      else _line_from_pick_text(pick_text))
    over_under_from_pick = _over_under_from_pick_text(pick_text)

    candidates = [m for m in (event.get("markets") or [])
                   if (m.get("type") or "").upper() == market_type.upper()]
    if period_q1:
        candidates = [m for m in candidates if _is_q1_market(m)]

    # basketball O/U ships one market per line; collect every direction match
    # across them and take the nearest line (capped) after the loop.
    _ou_nearest = []  # (line_dist, market, sel)

    for market in candidates:
        selections = market.get("selection") or []

        if bet_type in ("ML", "Q1_ML"):
            for sel in selections:
                sel_type = (sel.get("type") or "").upper()
                if sel_type == picked_side:
                    return market, sel

        elif bet_type == "SET_WINNER":
            for sel in selections:
                sel_type = (sel.get("type") or "").upper()
                if sel_type == picked_side:
                    return market, sel

        elif bet_type in ("TOTAL_GAMES", "TOTAL_SETS", "PLAYER_TOTAL_GAMES", "TOTAL", "Q1_TOTAL"):
            # Line must match (within 0.01). Selection.type is Over/Under.
            market_line = _to_float(market.get("line"))
            if line_from_pick is not None and market_line is not None \
                    and abs(market_line - line_from_pick) > 0.01:
                continue
            for sel in selections:
                sel_type = (sel.get("type") or "").lower()
                if sel_type != over_under_from_pick:
                    continue
                # Basketball O/U ships one market per line with the line in
                # the selection NAME (market.line is None); match it there.
                if line_from_pick is not None and market_line is None:
                    sel_line = _line_from_selection_name(sel.get("name") or "")
                    if sel_line is not None:
                        # no-line-guard: keep it, pick nearest after the loop
                        _ou_nearest.append((abs(sel_line - line_from_pick), market, sel))
                        continue
                return market, sel

        elif bet_type in ("SET_SPREAD", "GAME_SPREAD", "SPREAD", "Q1_SPREAD"):
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

    if _ou_nearest:
        # basketball O/U had no exact line: take HR's nearest within the cap;
        # beyond it we refuse (the placed bet must stay close to the model line).
        dist, mkt, sel = min(_ou_nearest, key=lambda t: t[0])
        if dist <= _OU_LINE_CAP_PTS:
            return mkt, sel

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
        "TOTAL", "Q1_TOTAL", "SPREAD", "Q1_SPREAD",
        "O/U", "ALT O/U", "F5 O/U", "RL", "ALT RL", "F5 RL",
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
    _t = re.sub(r"\s+(?:Q[1-4]|H[12]|OT)\s*$", "", (text or "").strip(), flags=re.I)  # strip period suffix (Q1/H1/OT) so line parses
    m = re.search(r"([+-]?\d+(?:\.\d+)?)\s*$", _t)
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


# -- WNBA resolver (basketball) --------------------------------------------
# Mirrors the tennis resolver: team-matchup event index via
# scrapers.hardrock_odds + bet_type -> HR market-key maps. HR basketball
# market shape is identical to tennis (verified vs Atlanta Dream v Seattle
# Storm 2026-07-31). Full-game FTOT:* live now; Q1 P:* posts closer to tip.
_WNBA_TYPES = {
    "ML":        "BASKETBALL:FTOT:ML",
    "SPREAD":    "BASKETBALL:FTOT:SPRD",
    "TOTAL":     "BASKETBALL:FTOT:OU",
    "Q1_ML":     "BASKETBALL:P:DNB",
    "Q1_SPREAD": "BASKETBALL:P:SPRD",
    "Q1_TOTAL":  "BASKETBALL:P:OU",
}
_WNBA_Q1_TYPES = frozenset({"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"})

_WNBA_TEAM_TOKENS = {
    "ATL": ("atlanta", "dream"),
    "CHI": ("chicago", "sky"),
    "CON": ("connecticut", "sun"), "CONN": ("connecticut", "sun"),
    "DAL": ("dallas", "wings"),
    "GS": ("golden state", "valkyries"), "GSV": ("golden state", "valkyries"),
    "IND": ("indiana", "fever"),
    "LV": ("las vegas", "aces"), "LVA": ("las vegas", "aces"),
    "LA": ("los angeles", "sparks"), "LAS": ("los angeles", "sparks"),
    "MIN": ("minnesota", "lynx"),
    "NY": ("new york", "liberty"), "NYL": ("new york", "liberty"),
    "PHX": ("phoenix", "mercury"), "PHO": ("phoenix", "mercury"),
    "POR": ("portland", "fire"),
    "SEA": ("seattle", "storm"),
    "WAS": ("washington", "mystics"), "WSH": ("washington", "mystics"),
}


def _is_q1_market(m: dict) -> bool:
    nm = (m.get("name") or "").lower()
    pd = (m.get("period") or "")
    return ("1st quarter" in nm) or ("q1" in nm) or pd in ("M1", "Q1", "1")


def _wnba_team_match(abbr: str, hr_name: str) -> bool:
    toks = _WNBA_TEAM_TOKENS.get((abbr or "").strip().upper())
    n = _norm(hr_name)
    if toks:
        return any(t in n for t in toks)
    a = (abbr or "").strip().lower()
    return bool(a) and (n.startswith(a) or a in n.replace(" ", ""))


_WNBA_EVENT_CACHE: dict[str, Any] = {"events": None, "ts": 0.0}


def _fetch_wnba_events() -> list[dict]:
    from scrapers.hardrock_odds import (
        _fetch_sports_tree, _resolve_comp_ids, _fetch_events_for_comp,
    )
    tree, err = _fetch_sports_tree()
    if err:
        logger.warning("HR sports tree fetch failed: %s", err)
        return []
    comp_ids = _resolve_comp_ids("wnba", tree)
    out: list[dict] = []
    for cid in comp_ids:
        evs, e_err = _fetch_events_for_comp(cid)
        if e_err:
            logger.debug("HR wnba comp=%s failed: %s", cid, e_err)
            continue
        out.extend(evs or [])
    return out


def _find_wnba_event(away_abbr: str, home_abbr: str) -> dict | None:
    import time
    now = time.time()
    if (_WNBA_EVENT_CACHE["events"] is None
            or now - _WNBA_EVENT_CACHE["ts"] > _EVENT_CACHE_TTL_S):
        _WNBA_EVENT_CACHE["events"] = _fetch_wnba_events()
        _WNBA_EVENT_CACHE["ts"] = now
    for ev in _WNBA_EVENT_CACHE["events"] or []:
        a = _participant_by_position(ev, 0)
        b = _participant_by_position(ev, 1)
        if not a or not b:
            continue
        if ((_wnba_team_match(away_abbr, a) and _wnba_team_match(home_abbr, b))
                or (_wnba_team_match(away_abbr, b) and _wnba_team_match(home_abbr, a))):
            return ev
    return None


def _resolve_wnba(pick: dict) -> dict:
    matchup = pick.get("matchup") or ""
    bet_type = (pick.get("bet_type") or "").upper()
    pick_text = (pick.get("pick") or "").strip()

    away, home = _split_matchup(matchup)
    if not away or not home:
        raise ResolveError(f"can't split WNBA matchup {matchup!r}")

    event = _find_wnba_event(away, home)
    if event is None:
        raise MarketUnavailable(f"HR has no WNBA event for {away} @ {home}")

    market_type = _WNBA_TYPES.get(bet_type)
    if not market_type:
        raise ResolveError(f"no HR WNBA market-type for {bet_type!r}")

    a_name = _participant_by_position(event, 0)
    b_name = _participant_by_position(event, 1)
    picked_side = _side_for_pick(pick_text, a_name or "", b_name or "")

    market, selection = _find_market_and_selection(
        event, market_type, bet_type, pick_text, picked_side,
        period_q1=(bet_type in _WNBA_Q1_TYPES),
        line_override=_to_float(pick.get("line")),
    )
    if not selection:
        raise MarketUnavailable(
            f"WNBA selection not on HR board (market={market_type}, pick={pick_text!r})")
    if market.get("suspended"):
        raise MarketUnavailable(f"HR market {market_type} suspended")

    root_idx = selection.get("rootIdx")
    ladder_decimal = _hr_ladder.get_decimal(root_idx)
    if not ladder_decimal:
        raise MarketUnavailable(f"HR ladder has no decimal for rootIdx={root_idx!r}")
    try:
        ladder_decimal_f = float(ladder_decimal)
    except ValueError:
        raise MarketUnavailable(f"HR ladder decimal not numeric: {ladder_decimal!r}") from None
    if ladder_decimal_f <= 1.0:
        raise MarketUnavailable("HR ladder decimal <= 1.0")

    _check_line_drift(pick, ladder_decimal_f)

    return {
        "selection_id": str(selection.get("id") or ""),
        "decimal":       ladder_decimal,
        "decimal_float": ladder_decimal_f,
        "period":      str(market.get("period") or "M"),
        "line":        (_to_float(market.get("line"))
                        if market.get("line") is not None
                        else _line_from_selection_name(selection.get("name") or "")),
        "market_id":   str(market.get("id") or ""),
        "market_type": market_type,
        "side":        picked_side,
        "event_id":    str(event.get("id") or ""),
        "root_idx":    root_idx,
    }


# -- MLB resolver (baseball) ------------------------------------------------
# HR market shape verified 2026-08-12 (Tigers v Guardians, Dodgers v Royals):
# every market carries a `selection` list of {id, name, type, odds, rootIdx};
# the LINE lives in the selection name ("Over 9.5" / "Guardians +1.5"), not on
# the market (market.line is None for baseball). Pick shape (picks table):
# matchup "AWAY @ HOME" abbrs; pick_text is a team abbr for ML/F5 ML ("HOU"),
# "ABBR +/-N.N" for run-lines, "Over/Under N.N" (optional "F5 " prefix) for
# totals. Prematch only -> on doubleheaders prefer the non-inplay event.
# Game-level markets wired; player props / team totals / 1st-inning raise
# ResolveError so they are never silently misplaced.
_MLB_TYPES = {
    "ML":      "BASEBALL:FTEI:ML",
    "O/U":     "BASEBALL:FTEI:OU",
    "ALT O/U": "BASEBALL:FTEI:OU",
    "RL":      "BASEBALL:FTEI:SPRD",
    "ALT RL":  "BASEBALL:FTEI:SPRD",
    "F5 ML":   "BASEBALL:P:ML5",
    "F5 O/U":  "BASEBALL:P:OU5",
    "F5 RL":   "BASEBALL:P:SPRD5",
}
_MLB_TOTAL_TYPES = frozenset({"O/U", "ALT O/U", "F5 O/U"})
_MLB_SPREAD_TYPES = frozenset({"RL", "ALT RL", "F5 RL"})
_MLB_TEAM_TOTAL_TYPES = frozenset({"TEAM TOTAL", "F5 TEAM TOTAL"})

# our-abbr -> HR nickname token(s). HR selection names use the nickname
# ("Dodgers", "Red Sox"); a substring match on the normalised name is enough.
_MLB_TEAM_TOKENS = {
    "ARI": ("diamondbacks",), "AZ": ("diamondbacks",), "ATL": ("braves",),
    "BAL": ("orioles",), "BOS": ("red sox",), "CHC": ("cubs",),
    "CWS": ("white sox",), "CHW": ("white sox",), "CIN": ("reds",),
    "CLE": ("guardians",), "COL": ("rockies",), "DET": ("tigers",),
    "HOU": ("astros",), "KC": ("royals",), "KCR": ("royals",),
    "LAA": ("angels",), "LAD": ("dodgers",), "MIA": ("marlins",),
    "MIL": ("brewers",), "MIN": ("twins",), "NYM": ("mets",),
    "NYY": ("yankees",), "OAK": ("athletics",), "ATH": ("athletics",),
    "PHI": ("phillies",), "PIT": ("pirates",), "SD": ("padres",),
    "SDP": ("padres",), "SF": ("giants",), "SFG": ("giants",),
    "SEA": ("mariners",), "STL": ("cardinals",), "TB": ("rays",),
    "TBR": ("rays",), "TEX": ("rangers",), "TOR": ("blue jays",),
    "WSH": ("nationals",), "WAS": ("nationals",),
}

_MLB_EVENT_CACHE: dict = {"events": None, "ts": 0.0}


def _mlb_team_match(abbr: str, hr_name: str) -> bool:
    toks = _MLB_TEAM_TOKENS.get((abbr or "").upper())
    if not toks:
        return False
    n = _norm(hr_name)
    return any(t in n for t in toks)


def _fetch_mlb_events() -> list[dict]:
    from scrapers.hardrock_odds import (
        _fetch_sports_tree, _resolve_comp_ids, _fetch_events_for_comp,
    )
    tree, err = _fetch_sports_tree()
    if err:
        logger.warning("HR sports tree fetch failed (mlb): %s", err)
        return []
    out: list[dict] = []
    for cid in _resolve_comp_ids("mlb", tree):
        evs, e_err = _fetch_events_for_comp(cid)
        if e_err:
            logger.debug("HR mlb comp=%s failed: %s", cid, e_err)
            continue
        out.extend(evs or [])
    return out


def _find_mlb_event(away_abbr: str, home_abbr: str) -> dict | None:
    import time
    now = time.time()
    if (_MLB_EVENT_CACHE["events"] is None
            or now - _MLB_EVENT_CACHE["ts"] > _EVENT_CACHE_TTL_S):
        _MLB_EVENT_CACHE["events"] = _fetch_mlb_events()
        _MLB_EVENT_CACHE["ts"] = now
    matches: list[dict] = []
    for ev in _MLB_EVENT_CACHE["events"] or []:
        a = _participant_by_position(ev, 0)
        b = _participant_by_position(ev, 1)
        if not a or not b:
            continue
        if ((_mlb_team_match(away_abbr, a) and _mlb_team_match(home_abbr, b))
                or (_mlb_team_match(away_abbr, b) and _mlb_team_match(home_abbr, a))):
            matches.append(ev)
    if not matches:
        return None
    for ev in matches:            # build_queue is prematch-only: prefer the non-live event
        if not ev.get("inplay"):
            return ev
    return matches[0]


def _resolve_mlb(pick: dict) -> dict:
    matchup = pick.get("matchup") or ""
    bet_type = (pick.get("bet_type") or "").upper()
    pick_text = (pick.get("pick_text") or pick.get("pick") or "").strip()

    if bet_type in _MLB_PROP_TYPES:
        return _resolve_mlb_prop(pick)

    away, home = _split_matchup(matchup)
    if not away or not home:
        raise ResolveError(f"can't split MLB matchup {matchup!r}")
    event = _find_mlb_event(away, home)
    if event is None:
        raise MarketUnavailable(f"HR has no MLB event for {away} @ {home}")

    if bet_type in _MLB_TEAM_TOTAL_TYPES:
        return _resolve_mlb_team_total(pick, event, bet_type, pick_text)

    market_type = _MLB_TYPES.get(bet_type)
    if not market_type:
        raise ResolveError(f"MLB bet_type {bet_type!r} not wired for HR")
    cands = [m for m in (event.get("markets") or [])
             if m.get("type") == market_type]
    if not cands:
        raise MarketUnavailable(f"HR MLB market {market_type} not on board")

    want_line = _to_float(pick.get("line"))
    if want_line is None:
        want_line = _line_from_pick_text(pick_text)
    core = re.sub(r"^F5\s+", "", pick_text, flags=re.I).strip()
    market = selection = None

    if bet_type in _MLB_TOTAL_TYPES:
        ou = _over_under_from_pick_text(core)
        if not ou:
            raise ResolveError(f"can't read over/under from {pick_text!r}")
        for m in cands:
            for s in (m.get("selection") or []):
                if (s.get("type") or "").lower() == ou:
                    sline = _line_from_selection_name(s.get("name") or "")
                    if (want_line is not None and sline is not None
                            and abs(sline - want_line) < 0.01):
                        market, selection = m, s
                        break
            if selection:
                break
    elif bet_type in _MLB_SPREAD_TYPES:
        abbr = re.sub(r"\s*[+-]?\d+(?:\.\d+)?\s*$", "", core).strip()
        for m in cands:
            for s in (m.get("selection") or []):
                if _mlb_team_match(abbr, s.get("name") or ""):
                    sline = _line_from_selection_name(s.get("name") or "")
                    if want_line is None or (sline is not None
                                             and abs(sline - want_line) < 0.01):
                        market, selection = m, s
                        break
            if selection:
                break
    else:  # ML / F5 ML -- team match
        for m in cands:
            for s in (m.get("selection") or []):
                if _mlb_team_match(core, s.get("name") or ""):
                    market, selection = m, s
                    break
            if selection:
                break

    if not selection:
        raise MarketUnavailable(
            f"MLB selection not on HR board "
            f"(market={market_type}, pick={pick_text!r}, line={want_line})")

    root_idx = selection.get("rootIdx")
    ladder_decimal = _hr_ladder.get_decimal(root_idx)
    ladder_f = _to_float(ladder_decimal)
    if not ladder_f or ladder_f <= 1.0:
        # ladder miss -- fall back to the selection's own odds snapshot
        ladder_decimal = selection.get("odds")
        ladder_f = _to_float(ladder_decimal)
    if not ladder_f or ladder_f <= 1.0:
        raise MarketUnavailable(f"HR has no usable decimal for rootIdx={root_idx!r}")

    _check_line_drift(pick, ladder_f)

    return {
        "selection_id": str(selection.get("id") or ""),
        "decimal":       ladder_decimal,
        "decimal_float": ladder_f,
        "period":      str(market.get("period") or "M"),
        "line":        _line_from_selection_name(selection.get("name") or ""),
        "market_id":   str(market.get("id") or ""),
        "market_type": market_type,
        "side":        pick_text,
        "event_id":    str(event.get("id") or ""),
        "root_idx":    root_idx,
    }


def _resolve_mlb_team_total(pick, event, bet_type, pick_text):
    """Per-team totals. Full-game -> BASEBALL:FTOT:A|B:OUEXSO ("<Team> Total
    Runs"); F5 -> BASEBALL:P:A|B:OU5 ("First 5 Innings <Team> Total Runs").
    A = away (participant 0), B = home (participant 1). pick_text e.g.
    "COL Under 5.5" / "MIL F5 Over 1.5". HR shape verified 2026-09-02."""
    core = re.sub(r"\bF5\b", "", pick_text, flags=re.I).strip()
    mm = re.match(r"^(.+?)\s+(Over|Under)\s+([0-9.]+)\s*$", core, re.I)
    if not mm:
        raise ResolveError(f"can't parse MLB team-total pick {pick_text!r}")
    team, side, line = mm.group(1).strip(), mm.group(2).lower(), _to_float(mm.group(3))
    p0 = _participant_by_position(event, 0)   # away = A
    p1 = _participant_by_position(event, 1)   # home = B
    if _mlb_team_match(team, p0):
        pos = "A"
    elif _mlb_team_match(team, p1):
        pos = "B"
    else:
        raise ResolveError(f"MLB team-total team {team!r} not in {p0} @ {p1}")
    is_f5 = bet_type.upper() == "F5 TEAM TOTAL"
    market_type = f"BASEBALL:P:{pos}:OU5" if is_f5 else f"BASEBALL:FTOT:{pos}:OUEXSO"
    cands = [m for m in (event.get("markets") or []) if m.get("type") == market_type]
    if not cands:
        raise MarketUnavailable(f"HR MLB market {market_type} not on board")
    market = selection = None
    for m in cands:
        for sel in (m.get("selection") or []):
            if (sel.get("type") or "").lower() == side:
                sl = _line_from_selection_name(sel.get("name") or "")
                if sl is not None and line is not None and abs(sl - line) < 0.01:
                    market, selection = m, sel
                    break
        if selection:
            break
    if not selection:
        raise MarketUnavailable(
            f"MLB team-total selection not on HR board "
            f"(market={market_type}, pick={pick_text!r}, line={line})")
    root_idx = selection.get("rootIdx")
    ladder_decimal = _hr_ladder.get_decimal(root_idx)
    ladder_f = _to_float(ladder_decimal)
    if not ladder_f or ladder_f <= 1.0:
        ladder_decimal = selection.get("odds")
        ladder_f = _to_float(ladder_decimal)
    if not ladder_f or ladder_f <= 1.0:
        raise MarketUnavailable(f"HR has no usable decimal for rootIdx={root_idx!r}")
    _check_line_drift(pick, ladder_f)
    return {
        "selection_id": str(selection.get("id") or ""),
        "decimal":       ladder_decimal,
        "decimal_float": ladder_f,
        "period":      str(market.get("period") or "M"),
        "line":        _line_from_selection_name(selection.get("name") or ""),
        "market_id":   str(market.get("id") or ""),
        "market_type": market_type,
        "side":        pick_text,
        "event_id":    str(event.get("id") or ""),
        "root_idx":    root_idx,
    }


# -- MLB player-prop resolver (pitcher Ks; selective) -----------------------
# player_props_picks (mlb.db) carries player_name + side + line. Only the
# Pitcher Ks Under cell qualifies (+12.9% n=75; the rest are -EV so the cell
# gate drops them). HR shape verified 2026-08-13: one PROPSOMULTILINE market
# per pitcher (market.name "<Pitcher> - Strikeouts"), selections are the O/U
# lines ("Under 4.5"); PROPALTSTRIKEOUTS<N> add per-line granularity. Match =
# pitcher name (all tokens in market.name) + side + exact line. Missing line
# -> MarketUnavailable (never bet a different line).
_MLB_PROP_TYPES = {
    "PITCHER KS O/U": "BASEBALL:FT:PROPSOMULTILINE",
}
_MLB_PROP_ALT = {
    "PITCHER KS O/U": "BASEBALL:FT:PROPALTSTRIKEOUTS",
}


def _resolve_mlb_prop(pick: dict) -> dict:
    matchup = pick.get("matchup") or ""
    bet_type = (pick.get("bet_type") or "").upper()
    player = (pick.get("player_name") or "").strip()
    side = (pick.get("side") or "").strip().lower()
    line = _to_float(pick.get("line"))
    pick_text = (pick.get("pick") or "").strip()

    if not player:
        raise ResolveError("MLB prop pick missing player_name")
    if side not in ("over", "under"):
        side = _over_under_from_pick_text(pick_text) or side
    if line is None:
        line = _line_from_pick_text(pick_text)
    if side not in ("over", "under") or line is None:
        raise ResolveError(f"can't read side/line from prop pick {pick_text!r}")

    away, home = _split_matchup(matchup)
    if not away or not home:
        raise ResolveError(f"can't split MLB matchup {matchup!r}")
    event = _find_mlb_event(away, home)
    if event is None:
        raise MarketUnavailable(f"HR has no MLB event for {away} @ {home}")

    main_type = _MLB_PROP_TYPES.get(bet_type)
    if not main_type:
        raise ResolveError(f"MLB prop {bet_type!r} not wired for HR")
    alt_prefix = _MLB_PROP_ALT.get(bet_type, "\0")
    ptoks = [t for t in _norm(player).split() if t]
    if not ptoks:
        raise ResolveError(f"empty player name {player!r}")

    market = selection = None
    for m in (event.get("markets") or []):
        # HR only accepts placement on the MAIN PROPSOMULTILINE market; the
        # ALT "To Record N+" markets reject UNAVAILABLE_SELECTION (verified
        # 2026-08-13: main placed @ -450, alt rejected). Match main only ->
        # ALT-only (low) lines skip as MarketUnavailable instead of erroring.
        if (m.get("type") or "") != main_type:
            continue
        if not all(tok in _norm(m.get("name") or "") for tok in ptoks):
            continue
        for s in (m.get("selection") or []):
            sn = (s.get("name") or "")
            snl = sn.lower()
            s_side = ("over" if snl.startswith("over")
                      else "under" if snl.startswith("under") else None)
            s_line = _line_from_selection_name(sn)
            if (s_side == side and s_line is not None
                    and abs(s_line - line) < 0.01):
                market, selection = m, s
                break
        if selection:
            break

    if not selection:
        raise MarketUnavailable(f"HR has no {bet_type} {side} {line} for {player}")

    root_idx = selection.get("rootIdx")
    ladder_decimal = _hr_ladder.get_decimal(root_idx)
    ladder_f = _to_float(ladder_decimal)
    if not ladder_f or ladder_f <= 1.0:
        ladder_decimal = selection.get("odds")
        ladder_f = _to_float(ladder_decimal)
    if not ladder_f or ladder_f <= 1.0:
        raise MarketUnavailable(f"HR has no usable decimal for rootIdx={root_idx!r}")

    _check_line_drift(pick, ladder_f)

    return {
        "selection_id": str(selection.get("id") or ""),
        "decimal":       ladder_decimal,
        "decimal_float": ladder_f,
        "period":      str(market.get("period") or "M"),
        "line":        line,
        "market_id":   str(market.get("id") or ""),
        "market_type": market.get("type") or main_type,
        "side":        side,
        "player":      player,
        "event_id":    str(event.get("id") or ""),
        "root_idx":    root_idx,
    }
