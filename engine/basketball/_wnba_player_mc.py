"""WNBA per-player Monte Carlo sampler.

Direct port of ``engine.sports.nba.player_mc``. Same distribution
families (NegBin for counting stats, Geom for blocks) — see
``distribution_fit.get_distribution('wnba', ...)`` which aliases to
the locked NBA shapes pending a WNBA-specific dispersion refit.

Reads from ``player_game_logs`` via ``_conn_for('wnba')`` which routes
to the basketball-framework wnba.db.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import numpy as np

from ..player_props_db import _conn_for
from ..distribution_fit import get_distribution
from ..mlb_player_mc import _sample, prob_over, prob_under, player_summary

logger = logging.getLogger(__name__)

_WNBA_STATS = {"pts", "reb", "ast", "tpm", "ftm", "to", "stl", "blk"}


def _is_present(stats: dict, stat_key: str) -> bool:
    if stat_key not in stats:
        return False
    return stats.get("min", 0) > 0


def _player_observations(player_id: int, stat_key: str,
                          since_date: str) -> np.ndarray:
    conn = _conn_for("wnba")
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
                    game_id: str | None = None,
                    n_sims: int = 10_000,
                    lookback_days: int = 60,
                    min_games: int = 5,
                    seed: int | None = None) -> dict[str, np.ndarray]:
    """Build sorted sample arrays for every WNBA stat we have enough
    history on. Returns ``{stat_key: sorted_samples}``.

    No GBM hook yet — NBA's prop_gbm relies on per-stat features trained
    against many seasons of NBA data; WNBA gets it after we accumulate
    a few thousand prop settles. For now the rolling-mean μ drives every
    sample; the locked NegBin dispersions still capture overdispersion."""
    target_stats = stats or sorted(_WNBA_STATS)
    since = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rng = np.random.default_rng(seed if seed is not None else
                                hash((player_id, since)) & 0xFFFFFFFF)
    raw: dict[str, np.ndarray] = {}
    for stat_key in target_stats:
        dist = get_distribution("wnba", stat_key)
        if dist is None:
            continue
        obs = _player_observations(player_id, stat_key, since)
        if len(obs) < min_games:
            continue
        raw[stat_key] = _sample(obs, dist, n_sims, rng)

    out: dict[str, np.ndarray] = {k: np.sort(v) for k, v in raw.items()}
    if {"pts", "reb", "ast"} <= set(raw):
        out["pra"] = np.sort(raw["pts"] + raw["reb"] + raw["ast"])
    return out


__all__ = ["build_player_mc", "prob_over", "prob_under", "player_summary"]
