"""
Tennis odds extractor (Hard Rock).

Bypasses the team-sport parser in ``scrapers.hardrock_odds`` because
tennis events differ structurally — no home/away, set-level markets
in addition to match-level, per-player game totals. We use the same
GraphQL transport (``_fetch_with_markets("tennis")``) but apply a
tennis-specific market mapper.

Output shape per match (keyed on a normalised player-pair signature
so caller can match independent of name order)::

    {
      "p1_name":  "Casper Ruud",
      "p2_name":  "Alexander Blockx",
      "match_id": "<HR event id>",
      "comp":     "ATP Madrid",
      "markets": {
        "ml":            {"p1_odds": 130, "p2_odds": -165},
        "set_spread":    [{"player": "p1", "point": +1.5, "odds": -110},
                           {"player": "p2", "point": -1.5, "odds": -130}],
        "total_games":   {"line": 24.5, "over_odds": -110, "under_odds": -110},
        "total_sets":    {"line": 2.5, "over_odds": -125, "under_odds": +105},
        "set_betting":   {"2-0": +200, "2-1": +250, "0-2": +260, "1-2": +400},
        "p1_total_games": {"line": 14.5, "over_odds": -120, "under_odds": -110},
        "p2_total_games": {"line": 13.5, "over_odds": -110, "under_odds": -120},
        "p1_win_at_least_one_set": {"yes_odds": -130, "no_odds": +110},
        "p2_win_at_least_one_set": {"yes_odds": -130, "no_odds": +110}
      }
    }

The picker reads this, matches each scheduled match by player names
to the HR market dict, and emits picks against every market with a
calibrated edge.

Why ``A`` and ``B`` mapping
---------------------------
HR's selections use a stable A/B split per event: A is participant
position 0, B is position 1. We surface those as ``p1`` (A) and
``p2`` (B) so downstream consumers don't have to know HR's internal
labels.
"""

from __future__ import annotations

import logging
import math
from typing import Any

logger = logging.getLogger(__name__)


# ── Decimal → American conversion ─────────────────────────────

def _decimal_to_american(dec: float | str | None) -> int | None:
    """Convert HR's decimal odds (string or float) to American.

    Decimal 2.0 → +100; 1.5 → -200; 3.0 → +200. Round to nearest int.
    """
    if dec is None:
        return None
    try:
        d = float(dec)
    except (TypeError, ValueError):
        return None
    if d <= 1.0:
        return None
    if d >= 2.0:
        return int(round((d - 1.0) * 100))
    return int(round(-100.0 / (d - 1.0)))


def _selection_odds(sel: dict) -> int | None:
    return _decimal_to_american(sel.get("odds"))


def _line_from_selection(sel: dict) -> float | None:
    """Extract the over/under or spread line from a selection name like
    'Over 24.5' or 'Casper Ruud +1.5'. Falls back to None."""
    name = sel.get("name") or ""
    import re
    m = re.search(r"([+-]?\d+(?:\.\d+)?)\s*$", name.strip())
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _participant_name_for(event: dict, side: str) -> str | None:
    """side = 'A' or 'B'. Returns the player's full name from the
    event's participants list.

    NOTE: HR uses position 0 for the A side. ``0 or -1`` evaluates to
    -1 in Python because 0 is falsy — so we check explicit None
    before defaulting.
    """
    pos = 0 if side == "A" else 1
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


# ── Per-market extractors ─────────────────────────────────────

def _extract_ml(market: dict) -> dict | None:
    """TENNIS:FT:ML. selection.type ∈ {'A', 'B'}."""
    out = {"p1_odds": None, "p2_odds": None}
    for sel in market.get("selection") or []:
        side = (sel.get("type") or "").upper()
        odds = _selection_odds(sel)
        if odds is None:
            continue
        if side == "A":
            out["p1_odds"] = odds
        elif side == "B":
            out["p2_odds"] = odds
    return out if (out["p1_odds"] or out["p2_odds"]) else None


def _extract_total_games(market: dict) -> dict | None:
    """TENNIS:FT:OU."""
    out = {"line": None, "over_odds": None, "under_odds": None}
    for sel in market.get("selection") or []:
        side = (sel.get("type") or "").lower()
        odds = _selection_odds(sel)
        line = _line_from_selection(sel)
        if line is not None:
            out["line"] = line
        if side == "over":
            out["over_odds"] = odds
        elif side == "under":
            out["under_odds"] = odds
    return out if (out["line"] is not None
                   and out["over_odds"] and out["under_odds"]) else None


def _extract_total_sets(market: dict) -> dict | None:
    """TENNIS:FT:STOT."""
    return _extract_total_games(market)  # same selection shape


def _extract_set_spread(market: dict) -> list[dict] | None:
    """TENNIS:FT:SHCP. selection.type ∈ {'AH', 'BH'} for home/away
    handicap points."""
    out: list[dict] = []
    for sel in market.get("selection") or []:
        side = (sel.get("type") or "").upper()
        odds = _selection_odds(sel)
        line = _line_from_selection(sel)
        if odds is None or line is None:
            continue
        if side == "AH":
            out.append({"player": "p1", "point": line, "odds": odds})
        elif side == "BH":
            out.append({"player": "p2", "point": line, "odds": odds})
    return out or None


def _extract_game_spread(market: dict) -> list[dict] | None:
    """TENNIS:FT:SPRD — Total Games Spread."""
    return _extract_set_spread(market)  # same A/B handicap structure


def _extract_player_total_games(market: dict, player: str) -> dict | None:
    """TENNIS:FT:A:OU or TENNIS:FT:B:OU. ``player`` ∈ {'p1', 'p2'}."""
    return _extract_total_games(market)


def _extract_at_least_one_set(market: dict) -> dict | None:
    """TENNIS:FT:A:WAL1S or B:WAL1S. selection.type ∈ {'Yes', 'No'}."""
    out = {"yes_odds": None, "no_odds": None}
    for sel in market.get("selection") or []:
        side = (sel.get("type") or "").lower()
        odds = _selection_odds(sel)
        if odds is None:
            continue
        if side in ("yes",):
            out["yes_odds"] = odds
        elif side in ("no",):
            out["no_odds"] = odds
    return out if (out["yes_odds"] or out["no_odds"]) else None


def _extract_set_betting(market: dict) -> dict | None:
    """TENNIS:FT:CSFLEX. selection.type is the score string '2-0' /
    '2-1' / '1-2' / '0-2' (BO3) or '3-0' / '3-1' / '3-2' / '0-3' /
    '1-3' / '2-3' (BO5)."""
    out: dict[str, int] = {}
    for sel in market.get("selection") or []:
        score = (sel.get("type") or "").strip()
        odds = _selection_odds(sel)
        if score and odds is not None and "-" in score:
            out[score] = odds
    return out or None


def _extract_most_games(market: dict) -> dict | None:
    """TENNIS:FT:MOSTGAMES. 3-way: A / B / Tie."""
    out = {"p1_odds": None, "p2_odds": None, "tie_odds": None}
    for sel in market.get("selection") or []:
        side = (sel.get("type") or "").upper()
        odds = _selection_odds(sel)
        if odds is None:
            continue
        if side == "A":
            out["p1_odds"] = odds
        elif side == "B":
            out["p2_odds"] = odds
        elif side == "X":
            out["tie_odds"] = odds
    return out if any(v for v in out.values()) else None


# ── Per-event composer ─────────────────────────────────────────

def _normalize_event(event: dict) -> dict | None:
    """Convert an HR tennis event into the unified output shape."""
    p1 = _participant_name_for(event, "A")
    p2 = _participant_name_for(event, "B")
    if not p1 or not p2:
        return None
    # HR ships startTime / eventTime as either ISO-8601 strings or
    # epoch-millis. Normalize to a UTC ISO-8601 string here so the
    # tennis routes can hand HR's authoritative wall-clock straight
    # to the frontend (TE and ESPN both ship placeholder times for
    # tournaments where the exact court time isn't set at ingest;
    # HR has the current bookmaker time and updates it live).
    hr_start = ""
    from datetime import datetime as _dt, timezone as _tz
    for field in ("startTime", "eventTime"):
        v = event.get(field)
        if isinstance(v, str) and v:
            hr_start = v
            break
        if isinstance(v, (int, float)) and v > 0:
            try:
                hr_start = _dt.fromtimestamp(
                    float(v) / 1000.0, tz=_tz.utc,
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
                break
            except (ValueError, TypeError, OSError):
                continue
    out = {
        "p1_name": p1,
        "p2_name": p2,
        "match_id": str(event.get("id") or ""),
        "comp": event.get("compName") or "",
        "start_time": hr_start,
        "markets": {},
    }
    for m in event.get("markets") or []:
        mtu = (m.get("type") or "").upper()
        if mtu == "TENNIS:FT:ML":
            r = _extract_ml(m)
            if r:
                out["markets"]["ml"] = r
        elif mtu == "TENNIS:FT:OU":
            r = _extract_total_games(m)
            if r:
                out["markets"]["total_games"] = r
        elif mtu == "TENNIS:FT:STOT":
            r = _extract_total_sets(m)
            if r:
                out["markets"]["total_sets"] = r
        elif mtu == "TENNIS:FT:SHCP":
            r = _extract_set_spread(m)
            if r:
                out["markets"]["set_spread"] = r
        elif mtu == "TENNIS:FT:SPRD":
            r = _extract_game_spread(m)
            if r:
                out["markets"]["game_spread"] = r
        elif mtu == "TENNIS:FT:A:OU":
            r = _extract_total_games(m)
            if r:
                out["markets"]["p1_total_games"] = r
        elif mtu == "TENNIS:FT:B:OU":
            r = _extract_total_games(m)
            if r:
                out["markets"]["p2_total_games"] = r
        elif mtu == "TENNIS:FT:A:WAL1S":
            r = _extract_at_least_one_set(m)
            if r:
                out["markets"]["p1_win_at_least_one_set"] = r
        elif mtu == "TENNIS:FT:B:WAL1S":
            r = _extract_at_least_one_set(m)
            if r:
                out["markets"]["p2_win_at_least_one_set"] = r
        elif mtu == "TENNIS:FT:CSFLEX":
            r = _extract_set_betting(m)
            if r:
                out["markets"]["set_betting"] = r
        elif mtu == "TENNIS:FT:MOSTGAMES":
            r = _extract_most_games(m)
            if r:
                out["markets"]["most_games"] = r
    return out


# ── Public API ────────────────────────────────────────────────

# Shared cache + circuit breaker. HR aggressively rate-limits the
# tennis GraphQL endpoint and the previous "fetch fresh per call"
# pattern was getting our IP throttled for hours at a time. Every
# tennis path (API, picker, sync, schedule supplement) now goes
# through fetch_all() so they share one cache and one circuit.
_HR_CACHE: dict = {"events": None, "ts": 0.0}
_HR_TTL_S = 900.0  # 15 minutes — odds don't move fast pre-match
_HR_BLOCK_UNTIL: float = 0.0  # epoch; live fetches skipped until here
_HR_BLOCK_DURATION_S = 1800.0  # 30-min cooldown after a 429
import os as _os
import json as _json
from pathlib import Path as _Path
_HR_DISK_CACHE = (_Path(__file__).resolve().parent.parent
                    / "data" / "tennis_hr_cache.json")


def _normalize_payload(data: dict) -> list[dict]:
    out: list[dict] = []
    sports = (data or {}).get("data", {}).get("betSync", {}).get("sports") or []
    for s in sports:
        for comp in s.get("competitions") or []:
            comp_name = comp.get("name") or ""
            for ev in comp.get("events", {}).get("data", []):
                normalized = _normalize_event(ev)
                if normalized is None:
                    continue
                normalized["comp"] = comp_name
                out.append(normalized)
    return out


def _disk_load() -> tuple[list[dict] | None, float]:
    try:
        blob = _json.loads(_HR_DISK_CACHE.read_text(encoding="utf-8"))
        return blob.get("events") or None, float(blob.get("ts") or 0.0)
    except Exception:
        return None, 0.0


def _disk_save(events: list[dict], ts: float) -> None:
    try:
        _HR_DISK_CACHE.parent.mkdir(parents=True, exist_ok=True)
        _HR_DISK_CACHE.write_text(
            _json.dumps({"events": events, "ts": ts}),
            encoding="utf-8",
        )
    except Exception as e:
        logger.debug("HR tennis disk cache write failed: %s", e)


def fetch_all(*, force: bool = False) -> list[dict]:
    """Fetch HR tennis events with shared in-process + disk cache and
    a 30-min circuit breaker on 429.

    Resolution order each call:
      1. Fresh in-process cache (within 15 min)         → instant
      2. Circuit breaker open (recent 429)              → cached/disk only
      3. Live HR fetch                                  → refresh both caches
      4. On any failure: disk cache → stale in-proc → []

    ``force=True`` bypasses the in-process freshness check but still
    respects the circuit breaker — we never hammer HR while we're
    in cooldown, even on explicit refresh.
    """
    import time as _time
    now = _time.time()
    cached = _HR_CACHE["events"]

    # Fresh in-process cache wins
    if not force and cached is not None \
            and now - _HR_CACHE["ts"] < _HR_TTL_S:
        return cached

    # Circuit breaker — if we got a 429 recently, don't call HR.
    # Serve cached/disk data instead so the rest of the slate keeps
    # working off the last known odds.
    global _HR_BLOCK_UNTIL
    if now < _HR_BLOCK_UNTIL:
        if cached is not None:
            return cached
        disk_events, _ = _disk_load()
        if disk_events:
            _HR_CACHE["events"] = disk_events
            return disk_events
        return []

    # Live fetch
    try:
        from scrapers.hardrock_odds import _fetch_with_markets
        data, err = _fetch_with_markets("tennis")
        if err and "429" in str(err):
            # Trip the breaker
            _HR_BLOCK_UNTIL = now + _HR_BLOCK_DURATION_S
            logger.warning("HR tennis: 429 — circuit open for %d min",
                           int(_HR_BLOCK_DURATION_S / 60))
            disk_events, _ = _disk_load()
            return cached or disk_events or []
        if err or not data:
            logger.warning("HR tennis fetch failed: %s", err)
            return cached or []
        events = _normalize_payload(data)
        if events:
            _HR_CACHE["events"] = events
            _HR_CACHE["ts"] = now
            _disk_save(events, now)
        return events
    except Exception as e:
        msg = str(e)
        if "429" in msg:
            _HR_BLOCK_UNTIL = now + _HR_BLOCK_DURATION_S
            logger.warning("HR tennis: 429 — circuit open")
        logger.debug("HR tennis live fetch raised: %s", e)
        return cached or []


def _normalize_name_key(name: str) -> str:
    """Normalize for fuzzy lookup. Same spirit as the player resolver
    in tennis_db: strip accents + lowercase, collapse whitespace."""
    if not name:
        return ""
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(ascii_only.lower().split())


def build_lookup(events: list[dict]) -> dict[frozenset[str], dict]:
    """Index events by normalized player-pair signature so callers
    can lookup independent of name spelling / order."""
    out: dict[frozenset[str], dict] = {}
    for ev in events:
        n1 = _normalize_name_key(ev.get("p1_name") or "")
        n2 = _normalize_name_key(ev.get("p2_name") or "")
        if not n1 or not n2:
            continue
        key = frozenset({n1, n2})
        out[key] = ev
    return out


def find_match(lookup: dict, p1_name: str, p2_name: str) -> dict | None:
    """Find an HR match by player names. The pair signature is
    order-independent so caller doesn't need to know which player
    HR labeled A vs B."""
    n1 = _normalize_name_key(p1_name)
    n2 = _normalize_name_key(p2_name)
    if not n1 or not n2:
        return None
    return lookup.get(frozenset({n1, n2}))


def align_to_caller(match: dict, p1_name: str, p2_name: str) -> dict:
    """Return a copy of ``match`` with p1/p2 ordered to match the
    caller's expectation. HR labels A=p1 and B=p2 by event creation
    order; if our p1 is HR's B, we swap.

    Swaps every market that has a p1/p2 dimension: ml, set_spread,
    game_spread, per-player totals, at-least-one-set, most_games.
    Set betting and total_games / total_sets are symmetric — no swap.
    """
    if not match:
        return match
    hr_n1 = _normalize_name_key(match.get("p1_name") or "")
    caller_n1 = _normalize_name_key(p1_name)
    if hr_n1 == caller_n1:
        return match
    # Swap p1 ↔ p2 across all asymmetric markets
    out = {
        "p1_name": match.get("p2_name"),
        "p2_name": match.get("p1_name"),
        "match_id": match.get("match_id"),
        "comp": match.get("comp"),
        "start_time": match.get("start_time"),
        "markets": {},
    }
    src_markets = match.get("markets") or {}
    for k, v in src_markets.items():
        if k == "ml":
            out["markets"]["ml"] = {
                "p1_odds": v.get("p2_odds"),
                "p2_odds": v.get("p1_odds"),
            }
        elif k in ("set_spread", "game_spread"):
            swapped = []
            for entry in v:
                player = "p2" if entry.get("player") == "p1" else "p1"
                swapped.append({"player": player,
                                "point": entry.get("point"),
                                "odds": entry.get("odds")})
            out["markets"][k] = swapped
        elif k == "p1_total_games":
            out["markets"]["p2_total_games"] = v
        elif k == "p2_total_games":
            out["markets"]["p1_total_games"] = v
        elif k == "p1_win_at_least_one_set":
            out["markets"]["p2_win_at_least_one_set"] = v
        elif k == "p2_win_at_least_one_set":
            out["markets"]["p1_win_at_least_one_set"] = v
        elif k == "most_games":
            out["markets"]["most_games"] = {
                "p1_odds": v.get("p2_odds"),
                "p2_odds": v.get("p1_odds"),
                "tie_odds": v.get("tie_odds"),
            }
        elif k == "set_betting":
            # Set betting swap: 2-1 ↔ 1-2, 2-0 ↔ 0-2, 3-0 ↔ 0-3 etc.
            swapped: dict[str, int] = {}
            for score, odds in v.items():
                a, _, b = score.partition("-")
                swapped[f"{b}-{a}"] = odds
            out["markets"]["set_betting"] = swapped
        else:
            # Symmetric markets (total_games, total_sets, total_tiebreaks)
            out["markets"][k] = v
    return out


__all__ = ["fetch_all", "build_lookup", "find_match", "align_to_caller"]
