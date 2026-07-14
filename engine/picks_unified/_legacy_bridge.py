"""Legacy → unified bridge.

One function: ``mirror_to_unified(sport, league, legacy_dict, ...)``.
Each per-sport recorder calls it after its INSERT to the legacy table.
Failures are swallowed (logged debug) — the legacy write is the source
of truth during cutover; unified is mirror-only.

After parity is validated and reads move to unified, we flip the
relationship: unified becomes the primary, legacy becomes optional.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ._game_key import GameKey
from ._recorder import record_pick
from ._types import Pick, Result, Scope, Variant


logger = logging.getLogger(__name__)


# ── Scope inference ──────────────────────────────────────────


def _infer_scope(bet_type: str | None, sport: str) -> Scope:
    bt = (bet_type or "").upper().strip()
    # Period-prefixed
    if bt.startswith("Q1") or bt.endswith("Q1") or "Q1_" in bt:
        return Scope.Q1
    if bt.startswith("Q2") or bt.endswith("Q2") or "Q2_" in bt:
        return Scope.Q2
    if bt.startswith("Q3") or bt.endswith("Q3") or "Q3_" in bt:
        return Scope.Q3
    if bt.startswith("Q4") or bt.endswith("Q4") or "Q4_" in bt:
        return Scope.Q4
    if bt.startswith("H1") or "H1_" in bt:
        return Scope.H1
    if bt.startswith("H2") or "H2_" in bt:
        return Scope.H2
    if bt.startswith("P1 ") or bt == "P1":
        return Scope.P1
    if bt.startswith("P2 ") or bt == "P2":
        return Scope.P2
    if bt.startswith("P3 ") or bt == "P3":
        return Scope.P3
    return Scope.FULL


def _infer_variant(bet_type: str | None) -> Variant:
    bt = (bet_type or "").upper().strip()
    if bt.startswith("ALT "):
        return Variant.ALT
    if "PROP" in bt or "BATTER" in bt or "PITCHER" in bt or "SKATER" in bt \
            or "PLAYER" in bt or "TOP_" in bt:
        return Variant.PROP
    if bt in ("1ST INN", "NRFI", "YRFI", "BTS"):
        return Variant.DERIVATIVE
    return Variant.MAIN


def _game_key_for(sport: str, league: str, native_id: Any) -> GameKey:
    """Build the canonical game_key. Sport-specific because the
    league string and native id format differ."""
    nid = str(native_id)
    if sport == "mlb":
        return GameKey.for_mlb(nid)
    if sport == "nhl":
        return GameKey.for_nhl(nid)
    if sport == "nba":
        return GameKey.for_nba(nid)
    if sport == "basketball":
        return GameKey.for_basketball(league, nid)
    if sport == "soccer":
        return GameKey.for_soccer(league, nid)
    if sport == "football":
        return GameKey.for_football(league, nid)
    if sport == "baseball":
        return GameKey.for_baseball(league, nid)
    if sport == "hockey":
        return GameKey.for_hockey(league, nid)
    if sport == "tennis":
        return GameKey.for_tennis(league, nid)
    if sport == "golf":
        return GameKey.for_golf(league, nid)
    if sport == "motorsports":
        return GameKey.for_motorsports(league, nid)
    # Fallback — generic format
    return GameKey(sport=sport, league=league, native_id=nid)


# ── Public bridge ────────────────────────────────────────────


def mirror_to_unified(
    *,
    sport: str,
    league: str,
    native_game_id: Any,
    pick_date: str,
    matchup: str,
    bet_type: str,
    pick_text: str,
    odds: int,
    prob: float,
    edge_pct: float,
    stake_units: float = 0.0,
    side: str | None = None,
    line: float | None = None,
    closing_odds: int | None = None,
    confidence: str | None = None,
    is_shadow: bool = False,
    result: str | None = None,
    profit: float | None = None,
    created_at: str | None = None,
    settled_at: str | None = None,
    pick_at_period: int | None = None,
    pick_at_clock_secs: int | None = None,
) -> int | None:
    """Mirror a legacy pick into the unified table. Returns the unified
    row id on success, None on failure. Never raises — the legacy
    write is the source of truth during cutover."""
    try:
        scope = _infer_scope(bet_type, sport)
        variant = _infer_variant(bet_type)
        gk = str(_game_key_for(sport, league, native_game_id))
        res_enum = (Result(result)
                    if result in ("W", "L", "P", "V") else None)
        pick = Pick(
            sport=sport,
            league=league,
            game_key=gk,
            pick_date=pick_date or "",
            matchup=matchup or "",
            scope=scope,
            bet_type=bet_type or "",
            variant=variant,
            pick_text=pick_text or "",
            side=side,
            line=line,
            odds=int(odds or 0),
            closing_odds=closing_odds,
            prob=float(prob or 0.0),
            edge_pct=float(edge_pct or 0.0),
            stake_units=float(stake_units or 0.0),
            confidence=confidence,
            is_shadow=bool(is_shadow),
            created_at=created_at or datetime.now(timezone.utc).isoformat(),
            settled_at=settled_at,
            result=res_enum,
            profit=profit,
            pick_at_period=pick_at_period,
            pick_at_clock_secs=pick_at_clock_secs,
        )
        return record_pick(pick)
    except Exception as e:
        logger.debug("mirror_to_unified failed for %s/%s/%s: %s",
                     sport, league, bet_type, e)
        return None


def mirror_settlement(
    *,
    sport: str,
    league: str,
    native_game_id: Any,
    bet_type: str,
    pick_text: str,
    result: str,
    profit: float,
    settled_at: str | None = None,
) -> bool:
    """Push a settlement from legacy → unified. Called by per-sport
    settlers after they UPDATE result/profit on their legacy table.
    Idempotent: if the unified row is already graded with the same
    result it's a no-op."""
    try:
        from ._schema import get_conn
        conn = get_conn()
        gk = str(_game_key_for(sport, league, native_game_id))
        scope = _infer_scope(bet_type, sport)
        row = conn.execute(
            "SELECT id, result FROM picks "
            "WHERE game_key=? AND scope=? AND bet_type=? AND pick_text=?",
            (gk, scope.value, bet_type, pick_text),
        ).fetchone()
        if not row:
            return False
        if row["result"] == result:
            return True  # already graded the same way
        ts = settled_at or datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE picks SET result=?, profit=?, settled_at=? WHERE id=?",
            (result, profit, ts, row["id"]),
        )
        return True
    except Exception as e:
        logger.debug("mirror_settlement failed for %s/%s/%s/%s: %s",
                     sport, league, bet_type, pick_text, e)
        return False


def ensure_adapters_registered() -> None:
    """Idempotent import-trigger for every sport adapter so the
    unified settler dispatch knows about them. Called by route handlers
    and any module that needs picks_unified ready to settle."""
    # Each adapter module auto-registers at import time.
    from . import (
        _mlb_adapter, _nhl_adapter, _nba_adapter,
        _basketball_adapter, _soccer_adapter,
        _football_adapter, _baseball_adapter, _hockey_adapter,
        _tennis_adapter, _golf_adapter, _motorsports_adapter,
    )
    # Touch them so static analyzers don't flag unused imports.
    _ = (_mlb_adapter, _nhl_adapter, _nba_adapter,
         _basketball_adapter, _soccer_adapter,
         _football_adapter, _baseball_adapter, _hockey_adapter,
         _tennis_adapter, _golf_adapter, _motorsports_adapter)
