"""V3.1 soccer market-blend Brier backtest.

Per-league sanity check: does blending the Dixon-Coles 1X2 output
with Pinnacle closing-implied probabilities reduce Brier on
finalized matches?

The DC baseline runs against current Elo state (mild leakage in DC's
favor), so the absolute deltas are conservative — a walk-forward DC
would lose more ground to the market. The point of this script is
**per-league go/no-go signal**, not the final ROI number. If the
50/50 blend doesn't beat raw DC by ≥1% Brier, V3.1 is not worth
shipping for that league.

Reads ``data/soccer/{league}/historical_odds.db`` (built by
``scrapers.football_data_soccer``) joined to the per-league matches
table via ``engine.soccer._market_join``. Prints a one-line summary
per league + persists per-league JSON to
``data/soccer/{league}/v31_brier.json``.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

from ._db import get_conn
from ._market_join import (
    _historical_db_path, _resolve, historical_db_exists
)
from ._predict import predict_match

logger = logging.getLogger(__name__)


def _brier_3way(p_h: float, p_d: float, p_a: float,
                 won: tuple[int, int, int]) -> float:
    """3-way Brier — average squared error across the H/D/A legs."""
    h_w, d_w, a_w = won
    return ((p_h - h_w) ** 2 + (p_d - d_w) ** 2 + (p_a - a_w) ** 2) / 3.0


def _outcome_from_score(h: int, a: int) -> tuple[int, int, int]:
    if h > a:
        return (1, 0, 0)
    if h < a:
        return (0, 0, 1)
    return (0, 1, 0)


def _market_implied(psch: float, pscd: float, psca: float
                     ) -> tuple[float, float, float]:
    raw = (1.0 / psch, 1.0 / pscd, 1.0 / psca)
    total = sum(raw)
    return (raw[0] / total, raw[1] / total, raw[2] / total)


def run(league: str, *, cutoff_date: str = "2024-07-01") -> dict:
    """Walk every finalized match in the league with both DC + market
    probs available. Report Brier for DC, market, blend.
    """
    if not historical_db_exists(league):
        return {"league": league, "error": "no historical_odds.db"}

    h = sqlite3.connect(str(_historical_db_path(league)))
    h.row_factory = sqlite3.Row
    odds_rows = h.execute(
        "SELECT match_date, home_name, away_name, psch, pscd, psca "
        "FROM historical_odds "
        "WHERE match_date >= ? AND psch IS NOT NULL "
        "  AND pscd IS NOT NULL AND psca IS NOT NULL",
        (cutoff_date,),
    ).fetchall()
    h.close()

    soc = get_conn(league)
    n_dc = n_market = n_blend = 0
    sum_brier_dc = sum_brier_market = sum_brier_blend = 0.0

    for o in odds_rows:
        h_abbr = _resolve(league, o["home_name"])
        a_abbr = _resolve(league, o["away_name"])
        if not (h_abbr and a_abbr):
            continue
        # Find the finalized match in our DB. UTC↔ET fuzz applied.
        m = soc.execute(
            "SELECT m.id, m.home_team_id, m.away_team_id, "
            "       m.home_score, m.away_score, m.neutral_site, "
            "       m.home_side, m.status "
            "FROM matches m "
            "JOIN teams ht ON ht.id = m.home_team_id "
            "JOIN teams at ON at.id = m.away_team_id "
            "WHERE m.date BETWEEN date(?, '-1 day') AND date(?, '+1 day') "
            "  AND ht.abbreviation = ? AND at.abbreviation = ? "
            "  AND m.status = 'final' "
            "  AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL "
            "LIMIT 1",
            (o["match_date"], o["match_date"], h_abbr, a_abbr),
        ).fetchone()
        if not m:
            continue

        won = _outcome_from_score(int(m["home_score"]), int(m["away_score"]))
        m_h, m_d, m_a = _market_implied(o["psch"], o["pscd"], o["psca"])

        try:
            dc = predict_match(
                league, int(m["home_team_id"]), int(m["away_team_id"]),
                neutral_site=bool(m["neutral_site"]),
                home_side=m["home_side"],
            )
            p_dc_h = dc["p_home"]
            p_dc_d = dc["p_draw"]
            p_dc_a = dc["p_away"]
        except Exception:
            continue

        sum_brier_dc     += _brier_3way(p_dc_h, p_dc_d, p_dc_a, won)
        n_dc += 1

        sum_brier_market += _brier_3way(m_h, m_d, m_a, won)
        n_market += 1

        # 50/50 blend — baseline V3.1 weight. Per-league tuning of `w`
        # is the next step once the signal is confirmed; for now the
        # universal weight is a fair go/no-go test.
        b_h = 0.5 * p_dc_h + 0.5 * m_h
        b_d = 0.5 * p_dc_d + 0.5 * m_d
        b_a = 0.5 * p_dc_a + 0.5 * m_a
        sum_brier_blend  += _brier_3way(b_h, b_d, b_a, won)
        n_blend += 1

    if not n_dc:
        return {"league": league, "n": 0, "error": "no joined matches"}

    brier_dc     = sum_brier_dc     / n_dc
    brier_market = sum_brier_market / n_market
    brier_blend  = sum_brier_blend  / n_blend
    delta_blend  = (brier_blend - brier_dc) / brier_dc * 100.0
    delta_market = (brier_market - brier_dc) / brier_dc * 100.0

    out = {
        "league":         league,
        "n":              n_dc,
        "brier_dc":       round(brier_dc, 5),
        "brier_market":   round(brier_market, 5),
        "brier_blend":    round(brier_blend, 5),
        "delta_blend_pct":  round(delta_blend, 2),
        "delta_market_pct": round(delta_market, 2),
        "verdict":        "ship" if delta_blend <= -1.0 else "skip",
    }
    out_path = (Path(__file__).resolve().parent.parent.parent
                / "data" / "soccer" / league / "v31_brier.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2))
    return out


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.soccer._market_backtest")
    ap.add_argument("leagues", nargs="*",
                    help="league keys (default: every league with "
                         "a historical_odds.db file)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")

    if not args.leagues:
        # Auto-discover every league that has a historical_odds.db.
        from . import LEAGUE_REGISTRY
        leagues = [lg for lg in LEAGUE_REGISTRY
                    if historical_db_exists(lg)]
    else:
        leagues = args.leagues

    rows = []
    for lg in leagues:
        try:
            r = run(lg)
            rows.append(r)
        except Exception as e:
            rows.append({"league": lg, "error": str(e)})
    print(f"{'league':18s} {'n':>5s} {'dc':>8s} {'market':>8s} "
          f"{'blend':>8s} {'d_blend%':>8s} verdict")
    for r in rows:
        if r.get("error"):
            print(f"{r['league']:18s} ERR {r['error']}")
            continue
        print(f"{r['league']:18s} {r['n']:>5d} "
              f"{r['brier_dc']:>8.4f} {r['brier_market']:>8.4f} "
              f"{r['brier_blend']:>8.4f} {r['delta_blend_pct']:>8.2f}  "
              f"{r['verdict']}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
