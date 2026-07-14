"""Universal record-time dedup for pick trackers.

The recurring duplicate-pick pattern across leagues — WNBA POR @ NY
3 SPREAD rows, college baseball CAM @ NE ML + SPREAD, NBA Q1_SPREAD
× 4 — comes from each per-sport tracker reinventing dedup with
slightly different rules. This module gives every tracker the same
two-pass guard:

  1. **Void stale pending picks in the same family** for this game
     that aren't in today's incoming set. Catches the line-move /
     name-format / model-changed-mind cases.

  2. **Block insert when a settled pick exists in the same family**
     for this game. Catches the re-record-after-settle case.

Callers wrap their INSERT with ``record_with_family_dedup`` and pass
a ``bet_type_family`` function that maps each pick's bet_type to a
family key (e.g. ``"full"`` for ML/SPREAD/TOTAL/ALT, ``"q1"`` for
Q1_*, ``"h1"`` for H1_*, etc).

Usage::

    from engine._dedup_helpers import (
        enforce_one_per_game_per_family, default_family_for,
    )

    enforce_one_per_game_per_family(
        conn, table='picks', game_id=str(game_id),
        date=today, picks=incoming_picks,
        family_func=default_family_for,
    )
    # then loop incoming picks and INSERT as before — the helper
    # has already cleared stale pending rows.
"""
from __future__ import annotations

from datetime import datetime
from typing import Callable, Iterable


# Default family mapping — collapses sibling bet_types into one
# "headline per game" tier. Sports that want a different shape pass
# their own ``family_func`` (see basketball's Q1 separation).
_FAMILY = {
    # Full-game team markets all collapse — "one card pick per game".
    "ML": "full", "SPREAD": "full", "TOTAL": "full", "O/U": "full",
    "ALT SPREAD": "full", "ALT TOTAL": "full", "ALT O/U": "full",
    "RL": "full", "ALT RL": "full", "PL": "full", "ALT PL": "full",
    # Q1 family (basketball)
    "Q1_ML": "q1", "Q1_SPREAD": "q1", "Q1_TOTAL": "q1",
    "Q1 ALT SPREAD": "q1", "Q1 ALT TOTAL": "q1",
    # H1 family (soccer)
    "H1_ML": "h1", "H1_DC": "h1", "H1_DNB": "h1",
    "H1_BTTS": "h1", "H1_TOTAL": "h1",
    # Period family (hockey)
    "Period Total": "period", "Period DNB": "period", "Period BTS": "period",
    # First-inning side market (MLB)
    "1st INN": "1st_inn",
    # Team total / total OE / overtime — each is its own derivative
    "Team Total": "team_total",
    "Total O/E": "total_oe",
    "Overtime": "overtime",
    "BTS": "bts",
    "Extra Innings": "extra_innings",
    "F5 O/U": "f5", "F5 ML": "f5", "F5 RL": "f5", "F5 Team Total": "f5",
    # Soccer markets — each its own family within full-game
    "DC": "full", "DNB": "full", "BTTS": "full", "AH": "full",
    "OU": "full",
}


def default_family_for(bet_type: str | None) -> str:
    """Return the dedup family for a bet_type. Unknown types fall
    back to the bet_type itself (so each unrecognized type acts as
    its own family — safe default that doesn't collapse unrelated
    markets together)."""
    if not bet_type:
        return ""
    return _FAMILY.get(bet_type, bet_type)


def enforce_one_per_game_per_family(
    conn,
    *,
    table: str,
    game_id: str,
    date: str,
    picks: Iterable[dict],
    family_func: Callable[[str | None], str] = default_family_for,
    game_id_col: str = "game_id",
    bet_type_key: str = "type",
    pick_key: str = "pick",
) -> dict:
    """Run the two-pass dedup guard BEFORE the caller inserts. Returns
    ``{"voided_stale": N, "blocked_dup_settled": N}``.

    Picks not in the incoming set (per family) get voided. After this
    call, the caller can safely loop and INSERT each incoming pick —
    only one per family will survive in the pending state, and any
    already-settled family member will block its sibling from being
    re-inserted.
    """
    out = {"voided_stale": 0, "blocked_dup_settled": 0}
    incoming = list(picks or [])
    if not incoming:
        return out

    # Build {family: [(bet_type, pick)]} from incoming so we know
    # which existing rows to keep vs void.
    incoming_by_family: dict[str, list[tuple[str, str]]] = {}
    for p in incoming:
        bt = p.get(bet_type_key) or ""
        fam = family_func(bt)
        incoming_by_family.setdefault(fam, []).append(
            (bt, p.get(pick_key) or "")
        )
    incoming_families = set(incoming_by_family.keys())

    # Void any pending pick in an incoming-family for this game that
    # isn't an exact (bet_type, pick) match in the incoming set.
    # Picks in OTHER families (e.g. yesterday's Period Total pick on
    # the same game) aren't touched.
    pending_rows = conn.execute(
        f"SELECT id, bet_type, pick FROM {table} "
        f"WHERE {game_id_col} = ? AND result IS NULL",
        (str(game_id),),
    ).fetchall()
    now = datetime.utcnow().isoformat()
    for r in pending_rows:
        bt = r["bet_type"] if "bet_type" in r.keys() else r[1]
        pk = r["pick"]     if "pick"     in r.keys() else r[2]
        rid = r["id"]      if "id"       in r.keys() else r[0]
        fam = family_func(bt)
        if fam not in incoming_families:
            continue
        if (bt, pk) in incoming_by_family[fam]:
            continue
        conn.execute(
            f"UPDATE {table} SET result='V', profit=0, settled_at=? "
            f"WHERE id=?",
            (now, rid),
        )
        out["voided_stale"] += 1
    conn.commit()
    return out


def is_settled_dup(
    conn,
    *,
    table: str,
    game_id: str,
    bet_type: str,
    family_func: Callable[[str | None], str] = default_family_for,
    game_id_col: str = "game_id",
) -> int | None:
    """Return the id of a SETTLED pick in the same family for this
    game, if one exists. Caller uses this to skip inserting a new
    pending pick that would duplicate already-graded content
    regardless of pick-text variants (line moves, name format)."""
    fam = family_func(bet_type)
    if not fam:
        return None
    # Walk pending+settled — only block on settled (W/L/P). Pending
    # is handled by enforce_one_per_game_per_family above.
    rows = conn.execute(
        f"SELECT id, bet_type, result FROM {table} "
        f"WHERE {game_id_col} = ? AND result IN ('W','L','P')",
        (str(game_id),),
    ).fetchall()
    for r in rows:
        bt = r["bet_type"] if "bet_type" in r.keys() else r[1]
        if family_func(bt) == fam:
            return r["id"] if "id" in r.keys() else r[0]
    return None


__all__ = [
    "default_family_for",
    "enforce_one_per_game_per_family",
    "is_settled_dup",
]
