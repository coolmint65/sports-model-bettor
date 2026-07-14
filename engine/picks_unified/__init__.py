"""Unified picks layer.

Single canonical pick shape used end-to-end across every sport. Replaces
the per-sport picks tables (mlb.picks, nba_picks, nhl_picks, basketball/
{league}/picks, soccer/{league}/picks, etc.) with one schema, one
recorder, one settler dispatch, and one reader.

Design decisions logged in project_picks_unified.md:

  Q1 scope semantics — flat enum scope + variant axis (main/alt/prop)
  Q2 storage shape   — single canonical table at data/picks_unified.db
  Q3 game identity   — universal game_key string "{sport}:{league}:{native}"
  Q4 migration       — big bang per-sport port

Public API:

    from engine.picks_unified import (
        Pick, Scope, Variant, GameKey,
        record_pick, settle_pending, list_picks, get_summary,
    )
"""
from __future__ import annotations

from ._types import Pick, Scope, Variant, Result
from ._game_key import GameKey
from ._recorder import record_pick, record_picks
from ._settler import settle_pending, settle_pick
from ._reader import list_picks, get_summary, get_pick_by_id

__all__ = [
    "Pick", "Scope", "Variant", "Result",
    "GameKey",
    "record_pick", "record_picks",
    "settle_pending", "settle_pick",
    "list_picks", "get_summary", "get_pick_by_id",
]
