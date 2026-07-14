"""Soccer per-league constants calibrator.

Mirrors engine.basketball._calibrate's shape — pull every completed
match for the league, fit the soccer-specific constants the Dixon-Coles
predictor reads from the registry, persist to JSON at
``data/soccer/{league}_constants.json``. The registry's
``_apply_fitted_overrides()`` overlay loads them on next import so the
predictor sees real values without a code edit.

Constants fit:
    home_advantage     mean(home_score - away_score)
    avg_home_goals     mean(home_score)
    avg_away_goals     mean(away_score)
    dc_rho             Dixon-Coles low-score correction parameter

For dc_rho we keep the literature-blessed default unless the empirical
fit clearly diverges from it; soccer ROI is dominated by lambda
calibration, not the tau correction.

Public:
    fit(league, min_matches=80) -> dict
    load(league) -> dict | None
"""
from __future__ import annotations

import json
import logging
import statistics
from datetime import datetime
from pathlib import Path

from ._config import LEAGUE_REGISTRY
from ._db import get_conn

logger = logging.getLogger(__name__)

_CONSTANTS_DIR = (Path(__file__).resolve().parent.parent.parent
                  / "data" / "soccer")


def _constants_path(league: str) -> Path:
    return _CONSTANTS_DIR / f"{league}_constants.json"


def _today_iso() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def fit(league: str, min_matches: int = 80) -> dict:
    """Fit + persist constants for ``league``. Raises ValueError when
    the matches table doesn't have enough finished games."""
    if league not in LEAGUE_REGISTRY:
        raise KeyError(f"unknown league {league!r}")
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT home_score, away_score, home_score_ht, away_score_ht "
        "FROM matches "
        "WHERE home_score IS NOT NULL AND away_score IS NOT NULL"
    ).fetchall()
    if len(rows) < min_matches:
        raise ValueError(
            f"only {len(rows)} finished matches for {league!r} "
            f"(need >= {min_matches}); ingest more history first"
        )

    home_goals = [r["home_score"] for r in rows]
    away_goals = [r["away_score"] for r in rows]

    avg_home = statistics.mean(home_goals)
    avg_away = statistics.mean(away_goals)
    # home_advantage is the goal-differential cushion; predictor uses
    # avg_home_goals/avg_away_goals as the lambda scaffolding so this is
    # mainly an audit number.
    home_adv = avg_home - avg_away

    # H1 (first-half) constants. h1_share is the fraction of full-game
    # goals scored before halftime — averaged at ~0.42-0.45 across
    # club football (less goals in H1 due to tactical opening + fewer
    # fatigue mistakes). Used by the predictor to derive H1 lambdas
    # from full-game lambdas without re-fitting a separate model.
    h1_rows = [r for r in rows
                if r["home_score_ht"] is not None
                and r["away_score_ht"] is not None]
    h1_avg_home = h1_avg_away = h1_share = None
    if h1_rows:
        h1_home_goals = [r["home_score_ht"] for r in h1_rows]
        h1_away_goals = [r["away_score_ht"] for r in h1_rows]
        h1_avg_home = statistics.mean(h1_home_goals)
        h1_avg_away = statistics.mean(h1_away_goals)
        full_avg_total = avg_home + avg_away
        if full_avg_total > 0:
            h1_share = (h1_avg_home + h1_avg_away) / full_avg_total

    constants = {
        "avg_home_goals": round(avg_home, 3),
        "avg_away_goals": round(avg_away, 3),
        "home_advantage": round(home_adv, 3),
        # Keep dc_rho on the literature default unless we add a proper
        # MLE later. The fitted lambdas above are the high-leverage piece.
        "dc_rho": None,
        # H1-period fitted means + share. None when HT scores aren't
        # backfilled yet — the predictor skips H1 markets in that case.
        "h1_avg_home_goals": round(h1_avg_home, 3) if h1_avg_home is not None else None,
        "h1_avg_away_goals": round(h1_avg_away, 3) if h1_avg_away is not None else None,
        "h1_share": round(h1_share, 4) if h1_share is not None else None,
        "h1_fitted_n": len(h1_rows),
        "fitted_n": len(rows),
        "fitted_at": _today_iso(),
    }
    _CONSTANTS_DIR.mkdir(parents=True, exist_ok=True)
    _constants_path(league).write_text(
        json.dumps(constants, indent=2), encoding="utf-8",
    )
    logger.info(
        "[soccer:%s] fitted n=%d avg_home=%.2f avg_away=%.2f adv=%.2f",
        league, len(rows), avg_home, avg_away, home_adv,
    )
    return constants


def load(league: str) -> dict | None:
    p = _constants_path(league)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("[soccer:%s] constants load failed: %s", league, e)
        return None


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.soccer._calibrate")
    ap.add_argument("league")
    ap.add_argument("--min-matches", type=int, default=80)
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    try:
        c = fit(args.league, min_matches=args.min_matches)
    except ValueError as e:
        print(f"ERROR: {e}")
        return 1
    print(json.dumps(c, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
