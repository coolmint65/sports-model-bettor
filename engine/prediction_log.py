"""
Prediction signal logging + ensemble tuning dataset assembly.

Every time /api/predict runs we want to remember WHAT each component
model thought, per market, so that later -- once the game settles --
we can compare those probabilities to the actual outcome and decide
how much weight each signal deserves in the ensemble.

Three functions:

  log_signals(sport, game_id, date, teams, signals)
      Write per-market factor/MC/GBM probabilities for one game.
      Called from the prediction endpoint. UNIQUE(sport, game_id,
      market) ensures a re-prediction of the same game overwrites
      its own row.

  count_settled_per_market(sport, days=365)
      Count how many signal rows are joinable against a completed
      game row. Drives the "do we have enough data to tune?" check.

  build_tuning_dataset(sport, days=365, markets=None)
      Join signals + games to produce the {market: {components,
      actuals}} dict that engine.ensemble_tune consumes.
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


# Per-market threshold: minimum settled samples before the auto-tuner
# will write a new ensemble weight override. 200 is the floor where
# "A beats B by 0.02 Brier" becomes statistically meaningful; below
# that we're just fitting noise.
DEFAULT_MIN_SAMPLES = 200


# Markets we track per sport. Keep in sync with engine.ensemble; adding
# a market here without a matching entry there just wastes storage.
MARKETS = {
    "mlb": ("home_win", "total", "nrfi", "f5_home_win", "f5_total"),
    "nhl": ("home_win", "total"),
    "nba": ("q1_home_win", "q1_total"),
}


# ── Logging ────────────────────────────────────────────────────

def log_signals(sport: str, game_id: int | None, game_date: str,
                home_team_id: int | None, away_team_id: int | None,
                signals: dict[str, dict]) -> int:
    """Persist per-market component probabilities for a prediction.

    signals shape:
        {"home_win": {"factor": 0.58, "mc": 0.61, "gbm": 0.60},
         "total":    {"factor": 8.3,  "mc": 8.1,  "gbm": 8.4},
         ...}
    Missing components (None / absent) are stored as NULL so the
    tuner can still count rows even when one model didn't run.
    Returns rows written.
    """
    if not sport or not game_date:
        return 0
    conn = _get_conn(sport)
    if conn is None:
        return 0
    written = 0
    for market, comps in (signals or {}).items():
        if not isinstance(comps, dict):
            continue
        try:
            conn.execute(
                "INSERT OR REPLACE INTO prediction_signals "
                "  (sport, game_id, game_date, home_team_id, away_team_id, "
                "   market, factor_prob, mc_prob, gbm_prob) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (sport, game_id, game_date,
                 home_team_id, away_team_id, market,
                 _num(comps.get("factor")),
                 _num(comps.get("mc")),
                 _num(comps.get("gbm"))),
            )
            written += 1
        except Exception as e:
            logger.debug("log_signals %s/%s/%s failed: %s",
                         sport, game_id, market, e)
    if written:
        try:
            conn.commit()
        except Exception:
            pass
    return written


# ── Count settled ──────────────────────────────────────────────

def count_settled_per_market(sport: str, days: int = 365) -> dict[str, int]:
    """Return {market: n_settled} for each market tracked for this sport.

    "Settled" means the signal row's game has a final outcome row we
    can compute the actual value from.
    """
    ds = build_tuning_dataset(sport, days=days)
    return {market: len(v.get("actuals") or []) for market, v in ds.items()}


# ── Tuning dataset ─────────────────────────────────────────────

def build_tuning_dataset(sport: str, days: int = 365,
                         markets: list[str] | None = None) -> dict:
    """Assemble the preds-file dict engine.ensemble_tune consumes.

    Shape:
        {"home_win": {"components": {"factor": [...], "mc": [...],
                                     "gbm":    [...]},
                      "actuals":    [0, 1, 0, ...]},
         ...}
    Components that ran for < 50% of samples for a market are dropped.
    """
    target_markets = list(markets or MARKETS.get(sport, ()))
    if not target_markets:
        return {}

    joined = _load_joined_rows(sport, days)
    if not joined:
        return {}

    out: dict[str, dict] = {}
    for market in target_markets:
        rows = []
        for game_row, sig in joined:
            actual = _actual_for_market(sport, market, game_row)
            if actual is None:
                continue
            if sig["market"] != market:
                continue
            rows.append((sig, actual))

        if not rows:
            continue

        comps: dict[str, list] = {"factor": [], "mc": [], "gbm": []}
        actuals: list = []
        for sig, actual in rows:
            actuals.append(actual)
            comps["factor"].append(sig.get("factor_prob"))
            comps["mc"].append(sig.get("mc_prob"))
            comps["gbm"].append(sig.get("gbm_prob"))

        clean: dict[str, list] = {}
        n = len(actuals)
        for name, series in comps.items():
            non_null = [v for v in series if v is not None]
            if len(non_null) < n * 0.5:
                continue
            mean_v = sum(non_null) / len(non_null) if non_null else 0.0
            clean[name] = [(mean_v if v is None else float(v))
                           for v in series]
        if clean and actuals:
            out[market] = {"components": clean, "actuals": actuals}
    return out


# ── DB glue ────────────────────────────────────────────────────

def _get_conn(sport: str):
    if sport == "mlb":
        from .db import get_conn
    elif sport == "nhl":
        from .nhl_db import get_conn
    elif sport == "nba":
        from .nba_db import get_conn
    else:
        return None
    try:
        return get_conn()
    except Exception as e:
        logger.debug("get_conn failed for %s: %s", sport, e)
        return None


def _load_joined_rows(sport: str, days: int) -> list[tuple[dict, dict]]:
    """Return (game_row, signal_row) pairs joined by game_id.

    Does the join in Python so the market-specific outcome extraction
    can live in one place (below) without per-sport SQL gymnastics.
    """
    conn = _get_conn(sport)
    if conn is None:
        return []

    try:
        signals = conn.execute(
            "SELECT game_id, game_date, market, factor_prob, mc_prob, gbm_prob "
            "FROM prediction_signals "
            "WHERE sport = ? AND game_date >= date('now', ?)",
            (sport, f"-{int(days)} days"),
        ).fetchall()
    except Exception as e:
        logger.debug("signals query failed: %s", e)
        return []
    if not signals:
        return []

    # Load the relevant game rows in one query, indexed by game_id.
    game_ids = sorted({int(s["game_id"]) for s in signals if s["game_id"]})
    if not game_ids:
        return []
    placeholders = ",".join("?" for _ in game_ids)

    if sport == "mlb":
        table = "games"
        id_col = "mlb_game_id"
        sel = ("mlb_game_id, status, home_score, away_score, "
               "home_linescore, away_linescore")
    elif sport == "nhl":
        table = "nhl_games"
        id_col = "game_id"
        sel = "game_id, status, home_score, away_score"
    elif sport == "nba":
        table = "nba_games"
        id_col = "game_id"
        sel = ("game_id, status, home_score, away_score, "
               "home_q1, away_q1")
    else:
        return []

    try:
        games = conn.execute(
            f"SELECT {sel} FROM {table} "
            f"WHERE status = 'final' AND {id_col} IN ({placeholders})",
            game_ids,
        ).fetchall()
    except Exception as e:
        logger.debug("games query failed: %s", e)
        return []

    games_by_id = {int(dict(g)[id_col]): dict(g) for g in games}
    out = []
    for s in signals:
        gid = s["game_id"]
        if gid is None:
            continue
        game_row = games_by_id.get(int(gid))
        if not game_row:
            continue
        out.append((game_row, dict(s)))
    return out


# ── Per-market outcome extraction ──────────────────────────────

def _actual_for_market(sport: str, market: str, g: dict):
    """Extract the ground-truth outcome from a games-table row.

    Returns a numeric actual value, or None if the row can't answer
    this market (e.g. MLB game with no linescore for inning markets).
    """
    hs = g.get("home_score")
    as_ = g.get("away_score")

    if sport == "mlb":
        if market == "home_win":
            if hs is None or as_ is None:
                return None
            return 1 if hs > as_ else 0
        if market == "total":
            if hs is None or as_ is None:
                return None
            return hs + as_
        if market in ("nrfi", "f5_home_win", "f5_total"):
            h_inn = _parse_linescore(g.get("home_linescore"))
            a_inn = _parse_linescore(g.get("away_linescore"))
            if h_inn is None or a_inn is None:
                return None
            if market == "nrfi":
                if len(h_inn) < 1 or len(a_inn) < 1:
                    return None
                return 1 if (h_inn[0] == 0 and a_inn[0] == 0) else 0
            if len(h_inn) < 5 or len(a_inn) < 5:
                return None
            h5 = sum(h_inn[:5])
            a5 = sum(a_inn[:5])
            if market == "f5_home_win":
                if h5 == a5:
                    return None  # push -- skip
                return 1 if h5 > a5 else 0
            if market == "f5_total":
                return h5 + a5

    elif sport == "nhl":
        if hs is None or as_ is None:
            return None
        if market == "home_win":
            return 1 if hs > as_ else 0
        if market == "total":
            return hs + as_

    elif sport == "nba":
        hq1 = g.get("home_q1")
        aq1 = g.get("away_q1")
        if hq1 is None or aq1 is None:
            return None
        if market == "q1_home_win":
            if hq1 == aq1:
                return None
            return 1 if hq1 > aq1 else 0
        if market == "q1_total":
            return hq1 + aq1

    return None


def _parse_linescore(raw) -> list[int] | None:
    """Linescores are JSON arrays in the games table."""
    if not raw:
        return None
    if isinstance(raw, list):
        return raw
    try:
        out = json.loads(raw)
        if isinstance(out, list):
            return [int(x or 0) for x in out]
    except Exception:
        pass
    return None


def _num(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None
