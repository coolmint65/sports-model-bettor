"""Canonical settler.

The settler asks ONE question per pick:

    "Given this game's current state, what's the outcome of this market?"

Sport-specific code is reduced to two things:
  1. Resolve game state from the game_key (`_resolve_game_state`)
  2. Compute outcome from state + market (`_resolve_outcome`)

Both dispatch on the GameKey's sport field. Everything else — the
"is this pick pending?", "is the game finished?", stale-push, mirror
healing, ROI accounting — is universal and lives here.

Adapters are registered by the per-sport modules at import time. The
dispatch table is empty by default; importing a sport's adapter populates
its entry. Allows the settler to ship without depending on every sport
being importable (golf-only deploys still work).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Callable, Protocol

from ._schema import get_conn
from ._game_key import GameKey
from ._types import Pick, Result


logger = logging.getLogger(__name__)


# ── Adapter registry ──────────────────────────────────────────


class GameStateResolver(Protocol):
    """Per-sport: parse a game_key, return a state dict describing
    where the game is (final score, period scores, status). Return
    None when the game can't be resolved (typically: not yet played,
    or stub-id not yet linked to a canonical row)."""

    def __call__(self, key: GameKey) -> dict | None: ...


class OutcomeResolver(Protocol):
    """Per-sport: given a pending pick and the game state dict the
    GameStateResolver returned, decide the outcome.

    Returns ``(Result, profit_per_unit_basis)`` where profit is in
    $100-per-unit terms (the recorder/reader handle stake-weighting).
    Returns ``None`` when the pick's scope hasn't resolved yet
    (e.g. game still in Q2, pick is Q3)."""

    def __call__(self, pick: Pick, state: dict) -> tuple[Result, float] | None: ...


_STATE_RESOLVERS: dict[str, GameStateResolver] = {}
_OUTCOME_RESOLVERS: dict[str, OutcomeResolver] = {}


def register_sport(sport: str, *,
                    state_resolver: GameStateResolver,
                    outcome_resolver: OutcomeResolver) -> None:
    """Per-sport adapter modules call this at import time."""
    _STATE_RESOLVERS[sport] = state_resolver
    _OUTCOME_RESOLVERS[sport] = outcome_resolver


# ── Settle one ────────────────────────────────────────────────


def settle_pick(pick_id: int) -> Result | None:
    """Settle a single pending pick. Returns the new Result, or None
    when the pick isn't resolvable yet."""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM picks WHERE id=? AND result IS NULL",
        (pick_id,),
    ).fetchone()
    if not row:
        return None
    return _settle_one(dict(row))


def _settle_one(row: dict) -> Result | None:
    """Internal: settle a row already fetched. Used by the loop in
    ``settle_pending``."""
    pick = Pick.from_row(row)
    try:
        key = GameKey.parse(pick.game_key)
    except ValueError as e:
        logger.warning("settler: bad game_key on pick id=%s: %s", pick.id, e)
        return None

    state_fn = _STATE_RESOLVERS.get(key.sport)
    outcome_fn = _OUTCOME_RESOLVERS.get(key.sport)
    if not state_fn or not outcome_fn:
        logger.debug("settler: no adapter registered for sport=%s", key.sport)
        return None

    state = state_fn(key)
    if state is None:
        return None

    outcome = outcome_fn(pick, state)
    if outcome is None:
        return None
    result, profit = outcome

    now = datetime.now(timezone.utc).isoformat()
    get_conn().execute(
        "UPDATE picks SET result=?, profit=?, settled_at=? WHERE id=?",
        (result.value, profit, now, pick.id),
    )
    return result


# ── Settle many ───────────────────────────────────────────────


def settle_pending(sport: str | None = None, *,
                    stale_days: int = 7) -> dict:
    """Walk pending picks and settle whatever's resolvable.

    Universal stale-push: picks pending more than ``stale_days`` days
    auto-flip to PUSH so they don't sit forever. Same threshold every
    per-sport settler used to enforce independently.

    With ``sport=None``, runs against every pending pick.
    """
    conn = get_conn()

    stale_cutoff = (datetime.now(timezone.utc) - timedelta(days=stale_days)
                    ).date().isoformat()
    if sport:
        stale_rows = conn.execute(
            "SELECT id, sport, pick_date, matchup, bet_type, pick_text "
            "FROM picks WHERE result IS NULL AND sport=? AND pick_date < ?",
            (sport, stale_cutoff),
        ).fetchall()
    else:
        stale_rows = conn.execute(
            "SELECT id, sport, pick_date, matchup, bet_type, pick_text "
            "FROM picks WHERE result IS NULL AND pick_date < ?",
            (stale_cutoff,),
        ).fetchall()
    now = datetime.now(timezone.utc).isoformat()
    auto_pushed = 0
    for r in stale_rows:
        conn.execute(
            "UPDATE picks SET result='P', profit=0, settled_at=? WHERE id=?",
            (now, r["id"]),
        )
        logger.warning(
            "settle: auto-pushed stale pick id=%s sport=%s date=%s "
            "%s/%s — older than %d days",
            r["id"], r["sport"], r["pick_date"], r["bet_type"], r["pick_text"],
            stale_days,
        )
        auto_pushed += 1

    if sport:
        pending = conn.execute(
            "SELECT * FROM picks WHERE result IS NULL AND sport=?",
            (sport,),
        ).fetchall()
    else:
        pending = conn.execute(
            "SELECT * FROM picks WHERE result IS NULL"
        ).fetchall()

    settled = wins = losses = pushes = 0
    for r in pending:
        try:
            result = _settle_one(dict(r))
        except Exception as e:
            logger.warning("settler: id=%s crash: %s", r["id"], e)
            continue
        if result is None:
            continue
        settled += 1
        if result == Result.WIN:
            wins += 1
        elif result == Result.LOSS:
            losses += 1
        elif result == Result.PUSH:
            pushes += 1

    return {
        "settled": settled,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "auto_pushed": auto_pushed,
        "pending_remaining": int(
            conn.execute(
                "SELECT COUNT(*) FROM picks WHERE result IS NULL"
                + (" AND sport=?" if sport else ""),
                (sport,) if sport else (),
            ).fetchone()[0]
        ),
    }
