"""Dedup key generation for placements.

Prevents double-staking on the same (game, market, selection) within
the same calendar day. Handles the queue's cross-sport variety —
tennis picks have match_id, team sports have
game_id. Falls back to a normalized matchup+pick hash when no event_id
is present.
"""
from __future__ import annotations
import hashlib
import re


def _norm(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip().lower()


def dedup_key(pick: dict) -> str:
    """Stable placement identity. Two picks that hash to the same key
    are considered the same bet — the placer refuses to fire the
    second one on the same calendar day.

    Uses `event_id` when present (queue backend already normalizes
    sport-specific id columns into this field). Falls back to a
    matchup+pick hash for legacy rows.
    """
    sport = _norm(pick.get("sport"))
    bt = _norm(pick.get("bet_type"))
    pk = _norm(pick.get("pick"))
    # Strip the betting LINE (e.g. "over 40.5 q1" -> "over q1") so a line move
    # does NOT open a second dedup slot and double-bet the same game+market+side.
    # 2026-08-05: DAL@WSH Q1 total bet at 40.5 AND 41.5 (HR+DK) on a line move.
    pk = re.sub(r"[-+]?\d+\.\d+", "", pk)
    pk = re.sub(r"\s+", " ", pk).strip()
    # Prefer the matchup hash: it is STABLE across the queue dual event_id (the SAME game can carry
    # both an ESPN id like 401857111 and an HR id like "hr-863718089", which split one bet into two
    # keys and let it double-fire). matchup+bt+pk is the true bet identity. 2026-08-03.
    _mu = _norm(pick.get("matchup"))
    if _mu:
        _h = hashlib.md5(f"{sport}|{_mu}|{bt}|{pk}".encode()).hexdigest()[:12]
        return f"{sport}::{_h}:{bt}:{pk}"
    event_id = pick.get("event_id")
    if event_id:
        return f"{sport}:{event_id}:{bt}:{pk}"
    # Legacy fallback — a stable hash of the matchup text.
    matchup = _norm(pick.get("matchup"))
    h = hashlib.md5(f"{sport}|{matchup}|{bt}|{pk}".encode()).hexdigest()[:12]
    return f"{sport}::{h}:{bt}:{pk}"
