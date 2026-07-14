"""Canonical pick recorder.

Single write path for every sport. INSERT OR IGNORE on the unique key
``(game_key, scope, bet_type, pick_text)`` so re-submitting the same
pick is a silent no-op. Returns the pick's ``id`` whether it was newly
inserted or already present.

If a re-submission comes in with a *different* (bet_type, pick_text)
for the same family — i.e. the model changed its mind about the
headline pick — call ``void_superseded`` first to mark the old row
``V``, then record the new one. Family is defined per-sport in
``engine._dedup_helpers``; we route through that module rather than
re-implementing.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from ._schema import get_conn
from ._types import Pick


_COLUMNS = (
    "sport", "league", "game_key", "pick_date", "matchup",
    "scope", "bet_type", "variant", "pick_text", "side", "line",
    "odds", "closing_odds", "closing_captured_at",
    "prob", "prob_raw",
    "edge_pct", "stake_units", "confidence", "is_shadow",
    "created_at", "settled_at", "result", "profit",
    "pick_at_period", "pick_at_clock_secs",
    "pick_at_home_score", "pick_at_away_score",
    "model_version", "pipeline_version",
)


def record_pick(pick: Pick) -> int:
    """Insert a pick. Returns the row id whether newly inserted or
    matched against the existing unique-key row.

    Idempotent: callers can re-record the same pick freely. The unique
    index on ``(game_key, scope, bet_type, pick_text)`` enforces the
    "one canonical row per family per game" rule we swept all sports
    to on 2026-06-10.
    """
    if pick.created_at is None:
        pick.created_at = datetime.now(timezone.utc).isoformat()
    row = pick.to_row()
    conn = get_conn()
    placeholders = ",".join("?" for _ in _COLUMNS)
    cur = conn.execute(
        f"INSERT OR IGNORE INTO picks ({','.join(_COLUMNS)}) "
        f"VALUES ({placeholders})",
        tuple(row[col] for col in _COLUMNS),
    )
    if cur.lastrowid and cur.rowcount > 0:
        return int(cur.lastrowid)
    # Pre-existing row — fetch its id.
    existing = conn.execute(
        "SELECT id FROM picks "
        "WHERE game_key=? AND scope=? AND bet_type=? AND pick_text=?",
        (pick.game_key, row["scope"], pick.bet_type, pick.pick_text),
    ).fetchone()
    if not existing:
        raise RuntimeError(
            f"INSERT OR IGNORE skipped row but no existing row found "
            f"for {pick.game_key}/{pick.scope}/{pick.bet_type}/{pick.pick_text}"
        )
    return int(existing["id"])


def record_picks(picks: Iterable[Pick]) -> dict:
    """Bulk variant. Returns ``{recorded, duplicate, errors}``."""
    out = {"recorded": 0, "duplicate": 0, "errors": 0, "ids": []}
    for p in picks:
        try:
            before_count = _count_picks()
            pid = record_pick(p)
            after_count = _count_picks()
            if after_count > before_count:
                out["recorded"] += 1
            else:
                out["duplicate"] += 1
            out["ids"].append(pid)
        except Exception:
            out["errors"] += 1
    return out


def void_superseded(game_key: str, scope: str, bet_type_family: set[str],
                     keep: tuple[str, str]) -> int:
    """Void pending picks in the given family that aren't the one we're
    keeping. Mirrors the per-sport ``enforce_one_per_game_per_family``
    helpers but operates on the unified table.

    ``bet_type_family`` is the set of bet_type values that count as the
    same family (e.g. ``{"ML"}`` or ``{"SPREAD", "ALT SPREAD"}``).
    ``keep`` is ``(bet_type, pick_text)`` — these stay.

    Returns the number of rows voided.
    """
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    placeholders = ",".join("?" for _ in bet_type_family)
    rows = conn.execute(
        f"SELECT id, bet_type, pick_text FROM picks "
        f"WHERE game_key=? AND scope=? AND result IS NULL "
        f"  AND bet_type IN ({placeholders})",
        (game_key, scope, *bet_type_family),
    ).fetchall()
    voided = 0
    for r in rows:
        if (r["bet_type"], r["pick_text"]) == keep:
            continue
        conn.execute(
            "UPDATE picks SET result='V', profit=0, settled_at=? "
            "WHERE id=?",
            (now, r["id"]),
        )
        voided += 1
    return voided


def _count_picks() -> int:
    return int(get_conn().execute("SELECT COUNT(*) FROM picks").fetchone()[0])
