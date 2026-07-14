"""Dedup key generation for placements.

Prevents double-staking on the same (game, market, selection) within
the same calendar day. Handles the queue's cross-sport variety —
tennis picks have match_id, motorsports have race_id, team sports have
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
    event_id = pick.get("event_id")
    if event_id:
        return f"{sport}:{event_id}:{bt}:{pk}"
    # Legacy fallback — a stable hash of the matchup text.
    matchup = _norm(pick.get("matchup"))
    h = hashlib.md5(f"{sport}|{matchup}|{bt}|{pk}".encode()).hexdigest()[:12]
    return f"{sport}::{h}:{bt}:{pk}"
