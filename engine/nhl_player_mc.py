"""
NHL per-player MC sampler (Phase 2i-ii).

Mirrors ``engine.mlb_player_mc`` with role-aware filtering for
skaters vs goalies (NHL game-log rows carry both groups in the
same table; skater stats are missing on goalie rows and vice
versa). Distribution choices come from the locked
``_NHL_STAT_DISTRIBUTIONS``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import numpy as np

from .player_props_db import _conn_for
from .distribution_fit import get_distribution
from .mlb_player_mc import _sample, prob_over, prob_under, player_summary

logger = logging.getLogger(__name__)

_SKATER_STATS = {"g", "a", "sog", "hits", "blocks"}
_GOALIE_STATS = {"saves", "shots_against", "ga"}


def _is_present(stats: dict, stat_key: str) -> bool:
    """Require the player actually played this game (toi_min > 0).
    Stat key must also exist in this row's blob — skater rows don't
    carry ``saves`` and goalie rows don't carry ``g``/``a``."""
    if stat_key not in stats:
        return False
    return stats.get("toi_min", 0) > 0


def _player_observations(player_id: int, stat_key: str,
                          since_date: str) -> np.ndarray:
    conn = _conn_for("nhl")
    rows = conn.execute(
        "SELECT stats_json FROM player_game_logs "
        "WHERE player_id = ? AND date >= ? "
        "ORDER BY date",
        (int(player_id), since_date),
    ).fetchall()
    out = []
    for r in rows:
        try:
            stats = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        if not _is_present(stats, stat_key):
            continue
        try:
            out.append(float(stats[stat_key]))
        except (TypeError, ValueError):
            continue
    return np.asarray(out, dtype=float)


def build_player_mc(player_id: int,
                    stats: list[str] | None = None,
                    *,
                    n_sims: int = 10_000,
                    lookback_days: int = 60,
                    min_games: int = 5,
                    seed: int | None = None) -> dict[str, np.ndarray]:
    """Build sorted sample arrays for every NHL stat we have enough
    history on. Returns ``{stat_key: sorted_samples}``."""
    target_stats = stats or sorted(_SKATER_STATS | _GOALIE_STATS)
    since = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rng = np.random.default_rng(seed if seed is not None else
                                hash((player_id, since)) & 0xFFFFFFFF)
    out: dict[str, np.ndarray] = {}
    raw: dict[str, np.ndarray] = {}
    for stat_key in target_stats:
        dist = get_distribution("nhl", stat_key)
        if dist is None:
            continue
        obs = _player_observations(player_id, stat_key, since)
        if len(obs) < min_games:
            continue
        raw[stat_key] = _sample(obs, dist, n_sims, rng)

    out: dict[str, np.ndarray] = {k: np.sort(v) for k, v in raw.items()}
    # Skater Points = G + A composite.
    if {"g", "a"} <= set(raw):
        out["p"] = np.sort(raw["g"] + raw["a"])
    return out


__all__ = ["build_player_mc", "prob_over", "prob_under", "player_summary"]
