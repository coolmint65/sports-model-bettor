"""Per-league constants fitter for the football framework.

Fits four numbers from the league's finalized-game history:
  - home_advantage   — mean(home_score - away_score), in points
  - league_avg_total — mean(home_score + away_score)
  - margin_sigma     — stdev of (home_score - away_score) residuals
  - total_sigma      — stdev of (home_score + away_score) residuals

Persists to ``data/football/{league}_constants.json``. Imported back
into ``LEAGUE_REGISTRY`` at next-process boot via the registry's
overlay loader (when wired).
"""
from __future__ import annotations

import json
import logging
import statistics
from datetime import datetime
from pathlib import Path

from . import LEAGUE_REGISTRY
from ._db import get_conn


logger = logging.getLogger(__name__)
_CONSTANTS_DIR = (Path(__file__).resolve().parent.parent.parent
                  / "data" / "football")


def fit(league: str, *, min_games: int = 30) -> dict:
    if league not in LEAGUE_REGISTRY:
        raise KeyError(f"unknown league {league!r}")
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT home_score, away_score FROM games "
        "WHERE status='final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL"
    ).fetchall()
    n = len(rows)
    if n < min_games:
        raise ValueError(
            f"only {n} finalized games for {league!r}; need >= {min_games}"
        )
    margins = [int(r["home_score"]) - int(r["away_score"]) for r in rows]
    totals  = [int(r["home_score"]) + int(r["away_score"]) for r in rows]
    out = {
        "home_advantage":   round(statistics.mean(margins), 2),
        "league_avg_total": round(statistics.mean(totals), 2),
        "margin_sigma":     round(statistics.pstdev(margins), 2),
        "total_sigma":      round(statistics.pstdev(totals), 2),
        "fitted_n":         n,
        "fitted_at":        datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    _CONSTANTS_DIR.mkdir(parents=True, exist_ok=True)
    path = _CONSTANTS_DIR / f"{league}_constants.json"
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    logger.info("[football:%s] constants fitted n=%d %s", league, n, out)
    return out


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.football._calibrate")
    ap.add_argument("league")
    ap.add_argument("--min-games", type=int, default=30)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    res = fit(args.league, min_games=args.min_games)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
