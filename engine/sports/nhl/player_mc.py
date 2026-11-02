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
import zlib
from datetime import datetime, timedelta

import numpy as np

from ...player_props_db import _conn_for
from ...distribution_fit import get_distribution
from ...mlb_player_mc import _sample, prob_over, prob_under, player_summary

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


def _stable_seed(player_id: int, since: str) -> int:
    """Deterministic per-(player, window) RNG seed.

    NOT ``hash((player_id, since)) & 0xFFFFFFFF`` — Python salts the
    builtin ``hash()`` of str/tuple objects per-process (PYTHONHASHSEED
    randomization, on by default since 3.3), so that expression looked
    stable within one long-lived process but drew a fresh salt -- and
    therefore a fresh seed, fresh MC draws, and a reshuffled top-N pick
    list -- every time a NEW process called it (every scheduled-task
    run, every backend restart). ``since`` is already day-granular
    (``%Y-%m-%d``), so this is stable all day regardless of process
    restarts, and still changes if lookback_days moves the window.
    zlib.crc32 is a plain deterministic checksum, not salted.
    """
    key = f"{player_id}|{since}".encode("utf-8")
    return zlib.crc32(key) & 0xFFFFFFFF


def build_player_mc(player_id: int,
                    stats: list[str] | None = None,
                    *,
                    game_id: str | None = None,
                    n_sims: int = 10_000,
                    lookback_days: int = 60,
                    min_games: int = 5,
                    seed: int | None = None) -> dict[str, np.ndarray]:
    """Build sorted sample arrays for every NHL stat we have enough
    history on. Returns ``{stat_key: sorted_samples}``.

    GBM-injected μ for shipped stats (g, a) when ``game_id`` is
    provided. Other stats fall back to rolling-mean μ."""
    target_stats = stats or sorted(_SKATER_STATS | _GOALIE_STATS)
    since = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rng = np.random.default_rng(seed if seed is not None else
                                _stable_seed(player_id, since))
    use_gbm = game_id is not None
    if use_gbm:
        try:
            from .prop_gbm import has_model as _gbm_has, predict_mu as _gbm_predict
        except ImportError:
            use_gbm = False
    out: dict[str, np.ndarray] = {}
    raw: dict[str, np.ndarray] = {}
    for stat_key in target_stats:
        dist = get_distribution("nhl", stat_key)
        if dist is None:
            continue
        # SOG v2 (MintyBot 2026-11-01, Austin "Yes" to the SOG rebuild): an as-of GLM -- decayed
        # SOG/60 shrunk to the F/D prior x decayed TOI x opponent shots allowed x home/D/playoff --
        # with NegBin(k fit on residuals). Replaces the flat window mean x matchup mult below, which
        # had no resolution vs the market (backtest + numbers in engine/sports/nhl/sog_v2.py). The v1
        # draw is kept as "sog_v1" so prop_eval keeps logging both. Revert: model override
        # ENABLE_NHL_SOG_V2 = False.
        if stat_key == "sog" and game_id is not None:
            v2 = None
            try:
                from . import sog_v2 as _sog_v2
                if _sog_v2.enabled():
                    v2 = _sog_v2.predict(player_id, str(game_id))
            except Exception as e:
                logger.debug("sog_v2 failed for %s/%s: %s", player_id, game_id, e)
            if v2 is not None:
                from ...mlb_player_mc import _sample_negbin
                raw["sog"] = _sample_negbin(v2[0], v2[1], n_sims, rng)
                obs_v1 = _player_observations(player_id, stat_key, since)
                if len(obs_v1) >= min_games:
                    _mu_v1 = None
                    try:
                        from .prop_matchup import matchup_mu_mult
                        _m = matchup_mu_mult(stat_key, player_id, str(game_id))
                        if _m != 1.0:
                            _mu_v1 = float(np.mean(obs_v1)) * _m
                    except Exception:
                        pass
                    _d1 = dist if _mu_v1 is None else {**dist, "_override_mu": _mu_v1}
                    raw["sog_v1"] = _sample(obs_v1, _d1, n_sims, rng)
                else:
                    raw["sog_v1"] = np.zeros(0)   # v1 can't price him; the key still marks "v2 priced this"
                continue
        obs = _player_observations(player_id, stat_key, since)
        if len(obs) < min_games:
            continue
        dist_use = dist
        override_mu = None
        if use_gbm and _gbm_has(stat_key):
            gbm_mu = _gbm_predict(stat_key, player_id, str(game_id))
            if gbm_mu is not None:
                override_mu = gbm_mu
        if (override_mu is None and game_id is not None
                and stat_key in ("g", "a", "sog", "saves", "ga", "blocks")):
            # The GBM yields no mu pre-game (its opponent features need a
            # game-log row that does not exist until the game is played),
            # and sog/saves have no trained model at all -- so these
            # otherwise price BLIND on the player's flat mean. Apply the
            # direct opponent-vs-league matchup multiplier to mu (same
            # _override_mu hook, nhl_games-derived rates). The GBM still
            # wins whenever it produces a mu. Austin 2026-10-22.
            try:
                from .prop_matchup import matchup_mu_mult
                _m = matchup_mu_mult(stat_key, player_id, str(game_id))
                if _m != 1.0:
                    override_mu = float(np.mean(obs)) * _m
            except Exception:
                pass
        if override_mu is not None:
            dist_use = {**dist, "_override_mu": override_mu}
        raw[stat_key] = _sample(obs, dist_use, n_sims, rng)

    out: dict[str, np.ndarray] = {k: np.sort(v) for k, v in raw.items()}
    # Skater Points = G + A composite.
    if {"g", "a"} <= set(raw):
        out["p"] = np.sort(raw["g"] + raw["a"])
    return out


__all__ = ["build_player_mc", "prob_over", "prob_under", "player_summary"]
