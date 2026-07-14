"""Per-league constant calibration.

Reads a league's finalized games from its DB and fits the constants the
generic predictor needs (home_boost, margin_std, total_std,
league_avg_total, league_avg_ppg). Persists the values back into the
LEAGUE_REGISTRY by writing a small JSON override file at
``data/basketball/<league>_constants.json``; ``_config.py`` loads
overrides at import time so this is the canonical source of truth once
fit.

Run:
    python -m engine.basketball._calibrate wnba
    python -m engine.basketball._calibrate ncaam --min-games 100
"""
from __future__ import annotations

import json
import logging
import statistics
from pathlib import Path

from ._config import LEAGUE_REGISTRY
from ._db import get_conn, games_table

logger = logging.getLogger(__name__)


CONSTANTS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "basketball"


def _constants_path(league: str) -> Path:
    return CONSTANTS_DIR / f"{league}_constants.json"


def fit(league: str, min_games: int = 50) -> dict:
    """Fit per-league constants from the finalized games table. Writes
    them to JSON + returns the dict. Raises ValueError if too few games.

    Constants fit:
        home_boost          mean(home_score - away_score)
        margin_std          stdev(home - away)
        total_std           stdev(home + away)
        league_avg_total    mean(home + away)
        league_avg_ppg      mean of either side's per-game scoring
        league_avg_pace     placeholder; needs possession data, defaults
                            to the basketball-wide prior

    b2b_penalty stays a prior — fitting it requires schedule density
    analysis the framework doesn't yet do per-league.
    """
    if league not in LEAGUE_REGISTRY:
        raise KeyError(f"Unknown league {league!r}")
    conn = get_conn(league)
    g_tbl = games_table(league)
    rows = conn.execute(
        f"SELECT home_score, away_score FROM {g_tbl} "
        f"WHERE status = 'final' "
        f"  AND home_score IS NOT NULL AND away_score IS NOT NULL"
    ).fetchall()
    if len(rows) < min_games:
        raise ValueError(
            f"only {len(rows)} finalized games for {league!r} "
            f"(need >= {min_games}); backfill first"
        )

    margins = [r["home_score"] - r["away_score"] for r in rows]
    totals = [r["home_score"] + r["away_score"] for r in rows]
    ppgs = []
    for r in rows:
        ppgs.append(r["home_score"])
        ppgs.append(r["away_score"])

    constants = {
        "home_boost": round(statistics.mean(margins), 2),
        "margin_std": round(statistics.pstdev(margins), 2),
        "total_std": round(statistics.pstdev(totals), 2),
        "league_avg_total": round(statistics.mean(totals), 2),
        "league_avg_ppg": round(statistics.mean(ppgs), 2),
        # b2b stays a prior; pace defaults to basketball-wide prior on read
        "fitted_n": len(rows),
        "fitted_at": _today_iso(),
    }

    # Q1 constants — only fit when the games table carries home_q1/away_q1.
    # The framework's quarter-splits columns are populated for ESPN-sourced
    # leagues (WNBA/NCAAM/AFL) and the SofaScore-backfilled RealGM leagues
    # (nz_nbl/brazil_nbb/argentina_lnb/australia_nbl/china_cba).
    try:
        q1_rows = conn.execute(
            f"SELECT home_q1, away_q1 FROM {g_tbl} "
            f"WHERE status = 'final' "
            f"  AND home_q1 IS NOT NULL AND away_q1 IS NOT NULL"
        ).fetchall()
    except Exception:
        q1_rows = []
    if len(q1_rows) >= max(min_games, 50):
        q1_margins = [r["home_q1"] - r["away_q1"] for r in q1_rows]
        q1_totals = [r["home_q1"] + r["away_q1"] for r in q1_rows]
        constants.update({
            "q1_home_boost": round(statistics.mean(q1_margins), 2),
            "q1_margin_std": round(statistics.pstdev(q1_margins), 2),
            "q1_total_std": round(statistics.pstdev(q1_totals), 2),
            "q1_avg_total": round(statistics.mean(q1_totals), 2),
            "q1_fitted_n": len(q1_rows),
            "q1_fitted_at": _today_iso(),
        })
    CONSTANTS_DIR.mkdir(parents=True, exist_ok=True)
    _constants_path(league).write_text(
        json.dumps(constants, indent=2), encoding="utf-8",
    )
    logger.info(
        "[%s] fitted constants from n=%d: home_boost=%.2f margin_std=%.2f "
        "total_std=%.2f league_avg_total=%.2f",
        league, len(rows),
        constants["home_boost"], constants["margin_std"],
        constants["total_std"], constants["league_avg_total"],
    )
    return constants


def load(league: str) -> dict | None:
    """Return persisted constants for ``league`` or None if not yet fit."""
    p = _constants_path(league)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("[%s] constants load failed: %s", league, e)
        return None


def _today_iso() -> str:
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.basketball._calibrate")
    ap.add_argument("league")
    ap.add_argument("--min-games", type=int, default=50)
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    try:
        c = fit(args.league, min_games=args.min_games)
    except ValueError as e:
        print(f"ERROR: {e}")
        return 1
    print(json.dumps(c, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
