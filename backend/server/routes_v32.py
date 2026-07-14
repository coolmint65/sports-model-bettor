"""V3.2 signal-explanation routes.

Surfaces the "why did this pick fire?" breakdown for every sport with
a `prediction_signals` table. The table logs the three sub-model
probabilities per (sport, game, market) at prediction time — factor
model, Monte Carlo, GBM. The ensemble layer blends them with per-
sport weights from `*_model_config`; the calibration layer shrinks
the blended raw probability to a realized-hit-rate via per-bucket
historical data; the picks_core layer compares calibrated prob to
implied prob from odds to produce an edge.

This route serializes that full chain so the frontend can render a
"Why did the model fire on this?" popover without re-running any of
the math.
"""
from __future__ import annotations

import logging
import sqlite3

from fastapi import APIRouter

router = APIRouter()
logger = logging.getLogger(__name__)


# Per-sport routing of the prediction_signals table. The sport name is
# the same prefix used elsewhere (mlb/nhl/nba); framework leagues with
# their own DBs (UFL today; future football/soccer leagues with
# prediction_signals) take a (league_module, league_key) tuple so the
# resolver knows to call get_conn(league_key).
_SPORT_DB_FACTORY: dict[str, tuple] = {
    "mlb": ("engine.db", "get_conn", None),
    "nhl": ("engine.nhl_db", "get_conn", None),
    "nba": ("engine.nba_db", "get_conn", None),
    "ufl": ("engine.football._db", "get_conn", "ufl"),
}


def _conn_for(sport: str) -> sqlite3.Connection | None:
    spec = _SPORT_DB_FACTORY.get(sport)
    if not spec:
        return None
    mod_name, fn_name, arg = spec
    try:
        mod = __import__(mod_name, fromlist=[fn_name])
        fn = getattr(mod, fn_name)
        return fn(arg) if arg is not None else fn()
    except Exception as e:
        logger.warning("[v3.2:%s] db open failed: %s", sport, e)
        return None


def _ensemble_weights(sport: str, conn: sqlite3.Connection,
                       market: str | None = None) -> dict:
    """Pull the active ensemble weights for ``(sport, market)``.

    Source-of-truth chain (matches what the picker actually applies):
      1. ``engine.ensemble.weights_for(sport, market)`` — handles
         the per-market overrides, dynamic weights from recent ROI, and
         the static defaults for MLB/NHL/NBA. Stays in sync with what
         the picker uses to blend factor + MC + GBM.
      2. NBA basketball-framework JSON fallback for leagues that
         persist weights via ``_ensemble_fit`` to
         ``data/basketball/{league}_gbm/ensemble_weights.json``.
      3. Conservative 0.34/0.33/0.33 default.

    When ``market`` is None (e.g. signal_summary aggregating across
    many markets), return the default-weights dict for the sport's
    headline market so the response can still surface "what would
    blend a typical pick".
    """
    # 1. Live engine.ensemble lookup — covers mlb/nhl/nba with the
    #    same code the picker uses.
    if market:
        try:
            from engine.ensemble import weights_for
            w = weights_for(sport, market)
            if w and isinstance(w, dict):
                return {k: float(v) for k, v in w.items()}
        except Exception as e:
            logger.debug("[v3.2:%s] ensemble.weights_for failed: %s",
                          sport, e)

    # 2. NBA basketball-framework JSON.
    if sport == "nba":
        try:
            from pathlib import Path
            import json
            p = (Path(__file__).resolve().parent.parent.parent
                 / "data" / "basketball" / "nba_gbm"
                 / "ensemble_weights.json")
            if p.exists():
                data = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    target = market or "home_win"
                    for k in (target, "home_win", "margin",
                              "total", "default"):
                        v = data.get(k)
                        if isinstance(v, dict):
                            return v
                    if all(isinstance(v, (int, float))
                           for v in data.values()):
                        return data
        except Exception:
            pass

    # 3. Default — first hit in DEFAULTS for this sport, else uniform.
    try:
        from engine.ensemble import _DEFAULT_WEIGHTS
        for (s, mk), w in _DEFAULT_WEIGHTS.items():
            if s == sport:
                return dict(w)
    except Exception:
        pass
    return {"factor": 0.34, "gbm": 0.33, "mc": 0.33}


@router.get("/api/v3/signal_explain/{sport}/{game_id}/{market}")
def api_signal_explain(sport: str, game_id: str, market: str) -> dict:
    """Return the factor/MC/GBM breakdown + ensemble weights + final
    blended prob for one (sport, game, market) prediction.

    Response shape::

        {
          "sport": "nba",
          "game_id": "401873341",
          "market": "q1_home_win",
          "signals": [
            {"name": "factor", "prob": 0.68, "weight": 0.40},
            {"name": "mc",     "prob": 0.33, "weight": 0.30},
            {"name": "gbm",    "prob": null, "weight": 0.30}
          ],
          "blended_prob": 0.572,
          "captured_at": "2026-05-20T00:02:33Z"
        }
    """
    sport = sport.lower()
    conn = _conn_for(sport)
    if conn is None:
        return {"error": f"unsupported sport {sport!r}"}

    try:
        row = conn.execute(
            "SELECT factor_prob, mc_prob, gbm_prob, captured_at "
            "FROM prediction_signals "
            "WHERE game_id = ? AND market = ? "
            "ORDER BY captured_at DESC LIMIT 1",
            (str(game_id), market),
        ).fetchone()
    except Exception as e:
        return {"error": f"prediction_signals query failed: {e}"}
    if not row:
        return {
            "sport": sport, "game_id": game_id, "market": market,
            "signals": [], "blended_prob": None,
            "note": "no signal data captured for this prediction",
        }

    weights = _ensemble_weights(sport, conn, market=market)
    signals = []
    weighted_sum = 0.0
    weight_total = 0.0
    for name in ("factor", "mc", "gbm"):
        prob = row[f"{name}_prob"]
        weight = weights.get(name, 1 / 3)
        signals.append({"name": name, "prob": prob, "weight": weight})
        if prob is not None:
            weighted_sum += prob * weight
            weight_total += weight
    blended = (weighted_sum / weight_total) if weight_total > 0 else None

    return {
        "sport":        sport,
        "game_id":      game_id,
        "market":       market,
        "signals":      signals,
        "blended_prob": round(blended, 4) if blended is not None else None,
        "captured_at":  row["captured_at"],
    }


def _team_id_for(sport: str, conn: sqlite3.Connection,
                   abbr: str | None) -> int | None:
    """Resolve a team abbreviation to its sport-DB team id. Returns
    None when the abbreviation is missing or doesn't match. Each sport
    keeps a teams table on its own DB; the column names are the same
    across NHL/NBA/MLB framework DBs."""
    if not abbr:
        return None
    table = {
        "nhl": "nhl_teams",
        "nba": "nba_teams",
        "mlb": "teams",
    }.get(sport)
    if not table:
        return None
    try:
        row = conn.execute(
            f"SELECT id FROM {table} WHERE abbreviation = ? LIMIT 1",
            (abbr.upper(),),
        ).fetchone()
        return int(row["id"]) if row else None
    except Exception:
        return None


@router.get("/api/v3/signal_explain_by_matchup/{sport}/{market}")
def api_signal_explain_by_matchup(sport: str, market: str,
                                    date: str, home: str,
                                    away: str) -> dict:
    """Matchup-keyed counterpart of ``signal_explain``. Used when the
    caller has the slate's (date, home_abbr, away_abbr) but not a
    canonical game_id — common on NHL/NBA where the live signal
    logger writes NULL game_id when the games table hasn't been
    refreshed for tonight's slate yet.

    Falls through to the same breakdown shape ``signal_explain`` returns,
    so frontends can switch endpoints without reshaping their parser.
    """
    sport = sport.lower()
    conn = _conn_for(sport)
    if conn is None:
        return {"error": f"unsupported sport {sport!r}"}

    home_tid = _team_id_for(sport, conn, home)
    away_tid = _team_id_for(sport, conn, away)
    if not home_tid or not away_tid:
        return {
            "sport": sport, "market": market, "signals": [],
            "blended_prob": None,
            "note": f"could not resolve teams home={home!r} away={away!r}",
        }

    try:
        row = conn.execute(
            "SELECT factor_prob, mc_prob, gbm_prob, captured_at, game_id "
            "FROM prediction_signals "
            "WHERE game_date = ? AND home_team_id = ? "
            "  AND away_team_id = ? AND market = ? "
            "ORDER BY captured_at DESC LIMIT 1",
            (date, home_tid, away_tid, market),
        ).fetchone()
    except Exception as e:
        return {"error": f"prediction_signals query failed: {e}"}
    if not row:
        return {
            "sport": sport, "market": market, "signals": [],
            "blended_prob": None,
            "note": (f"no signal data captured for "
                      f"{away}@{home} on {date}"),
        }

    weights = _ensemble_weights(sport, conn, market=market)
    signals = []
    weighted_sum = 0.0
    weight_total = 0.0
    for name in ("factor", "mc", "gbm"):
        prob = row[f"{name}_prob"]
        weight = weights.get(name, 1 / 3)
        signals.append({"name": name, "prob": prob, "weight": weight})
        if prob is not None:
            weighted_sum += prob * weight
            weight_total += weight
    blended = (weighted_sum / weight_total) if weight_total > 0 else None

    return {
        "sport":        sport,
        "game_id":      row["game_id"],
        "matchup":      f"{away.upper()} @ {home.upper()}",
        "date":         date,
        "market":       market,
        "signals":      signals,
        "blended_prob": round(blended, 4) if blended is not None else None,
        "captured_at":  row["captured_at"],
    }


@router.get("/api/v3/signal_summary/{sport}")
def api_signal_summary(sport: str, limit: int = 50) -> dict:
    """Aggregate breakdown across recent predictions for a sport.

    Useful for the calibration tab — tells the operator which signal
    has been most divergent from the consensus on the latest slate,
    which is the cheapest signal that "earned the difference" on
    settled picks, etc.
    """
    sport = sport.lower()
    conn = _conn_for(sport)
    if conn is None:
        return {"error": f"unsupported sport {sport!r}"}

    try:
        rows = conn.execute(
            "SELECT market, factor_prob, mc_prob, gbm_prob, captured_at "
            "FROM prediction_signals "
            "ORDER BY captured_at DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    except Exception as e:
        return {"error": f"prediction_signals query failed: {e}"}

    per_market: dict[str, dict] = {}
    for r in rows:
        m = r["market"] or "unknown"
        d = per_market.setdefault(m, {
            "n": 0, "factor_n": 0, "mc_n": 0, "gbm_n": 0,
            "factor_avg": 0.0, "mc_avg": 0.0, "gbm_avg": 0.0,
        })
        d["n"] += 1
        for k in ("factor", "mc", "gbm"):
            p = r[f"{k}_prob"]
            if p is not None:
                d[f"{k}_n"] += 1
                d[f"{k}_avg"] = (
                    (d[f"{k}_avg"] * (d[f"{k}_n"] - 1) + p) / d[f"{k}_n"]
                )
    # Round averages.
    for d in per_market.values():
        for k in ("factor_avg", "mc_avg", "gbm_avg"):
            d[k] = round(d[k], 4)

    weights = _ensemble_weights(sport, conn)
    return {
        "sport":           sport,
        "limit":           int(limit),
        "ensemble_weights": weights,
        "per_market":      per_market,
    }
