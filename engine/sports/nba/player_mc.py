"""
NBA per-player MC sampler (Phase 2h-ii).

Mirrors ``engine.mlb_player_mc`` — the only sport-specific concern
is the role filter (NBA: every player on the floor counts; just
require ``min > 0`` so DNPs and 0-min appearances don't drag the
rolling mean). Distribution choices come from the locked
``_NBA_STAT_DISTRIBUTIONS``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import numpy as np

from ...player_props_db import _conn_for
from ...distribution_fit import get_distribution
from ...mlb_player_mc import _sample, prob_over, prob_under, player_summary

logger = logging.getLogger(__name__)

_NBA_STATS = {"pts", "reb", "ast", "tpm", "ftm", "to", "stl", "blk"}


def _is_present(stats: dict, stat_key: str) -> bool:
    """Require the player actually played this game (min > 0). NBA
    boxscores include DNPs in the players list with stat fields all
    0 — counting them tanks the rolling mean."""
    if stat_key not in stats:
        return False
    return stats.get("min", 0) > 0


def _player_observations(player_id: int, stat_key: str,
                          since_date: str) -> np.ndarray:
    conn = _conn_for("nba")
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
    """Build sorted sample arrays for every NBA stat we have enough
    history on. Returns ``{stat_key: sorted_samples}``.

    When ``game_id`` is provided AND a shipped GBM exists for the
    stat, the GBM-predicted μ replaces the rolling-mean μ. Player's
    own variance still drives NegBin dispersion."""
    target_stats = stats or sorted(_NBA_STATS)
    since = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rng = np.random.default_rng(seed if seed is not None else
                                hash((player_id, since)) & 0xFFFFFFFF)
    use_gbm = game_id is not None
    if use_gbm:
        try:
            from .prop_gbm import has_model as _gbm_has, predict_mu as _gbm_predict
        except ImportError:
            use_gbm = False
    out: dict[str, np.ndarray] = {}
    # Cache UNSORTED samples so composites (pra) can sum element-wise
    # before sorting.
    raw: dict[str, np.ndarray] = {}
    for stat_key in target_stats:
        dist = get_distribution("nba", stat_key)
        if dist is None:
            continue
        obs = _player_observations(player_id, stat_key, since)
        if len(obs) < min_games:
            continue
        dist_use = dist
        if use_gbm and _gbm_has(stat_key):
            gbm_mu = _gbm_predict(stat_key, player_id, str(game_id))
            if gbm_mu is not None:
                dist_use = {**dist, "_override_mu": gbm_mu}
        raw[stat_key] = _sample(obs, dist_use, n_sims, rng)

    out: dict[str, np.ndarray] = {k: np.sort(v) for k, v in raw.items()}
    # PRA composite: only when all three components have samples.
    if {"pts", "reb", "ast"} <= set(raw):
        out["pra"] = np.sort(raw["pts"] + raw["reb"] + raw["ast"])
    return out


__all__ = ["build_player_mc", "prob_over", "prob_under", "player_summary"]
