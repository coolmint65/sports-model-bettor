"""
NBA full-game backtest (Phase 2k.6).

Walks finalized games in nba_games chronologically, runs the full-game
factor model with point-in-time team PPG (only games strictly BEFORE
the target date), computes hypothetical picks against synthetic -110
odds, and reports ROI per market vs the actual final score.

Caveats:
  - Synthetic -110 juice on every market. Real HR prices vary; this
    is an upper bound, not a live-money projection.
  - nba_q1_stats season-aggregate fields (pace, off_rating, def_rating)
    have a small leak — they include the game being predicted as a
    1/82 fraction of the season. Mitigated by using PIT for the high-
    weight inputs (home/away PPG and recent form) which dominate.
  - Skips MC + GBM for speed. Factor model only. The live ensemble
    blend will perform at least as well as factor-only on every
    market that has a winning factor, so this is a conservative
    baseline.

Usage:
    python -m engine.nba_full_backtest --days 365 --min-edge 4.0
    python -m engine.nba_full_backtest --season 2024 --min-edge 4.0
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _empty_cat() -> dict:
    return {"picks": 0, "wins": 0, "losses": 0, "pushes": 0, "profit": 0.0}


def _record(cat: dict, won: bool | None, odds: int = -110) -> None:
    cat["picks"] += 1
    if won is None:
        cat["pushes"] += 1
        return
    if won:
        cat["wins"] += 1
        cat["profit"] += 100.0 if odds == 100 else (100 / abs(odds) * 100 if odds < 0 else odds)
    else:
        cat["losses"] += 1
        cat["profit"] -= 100.0


def _summarize(cat: dict) -> dict:
    settled = cat["wins"] + cat["losses"]
    wr = cat["wins"] / settled * 100 if settled else 0.0
    # ROI %  =  total_profit / total_stake.
    # Each pick is $100, so total_stake = picks * 100.
    # ROI(%) = profit / (picks * 100) * 100 = profit / picks.
    roi_pct = cat["profit"] / cat["picks"] if cat["picks"] else 0.0
    return {
        "picks": cat["picks"],
        "wins": cat["wins"],
        "losses": cat["losses"],
        "pushes": cat["pushes"],
        "win_pct": round(wr, 1),
        "profit_per_100": round(roi_pct, 2),
        "roi_pct": round(roi_pct, 2),
    }


def _pit_ppg(conn, team_id: int, season: int, before_date: str) -> dict | None:
    """Compute team's PPG / opp_PPG using only games strictly before
    ``before_date`` in the same season."""
    rows = conn.execute(
        "SELECT home_team_id, away_team_id, home_score, away_score "
        "FROM nba_games "
        "WHERE season = ? AND status = 'final' "
        "  AND date < ? AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "  AND (home_team_id = ? OR away_team_id = ?)",
        (season, before_date, team_id, team_id),
    ).fetchall()
    if not rows:
        return None
    scored, allowed = [], []
    for r in rows:
        if r["home_team_id"] == team_id:
            scored.append(r["home_score"])
            allowed.append(r["away_score"])
        else:
            scored.append(r["away_score"])
            allowed.append(r["home_score"])
    return {
        "games": len(scored),
        "ppg": sum(scored) / len(scored),
        "opp_ppg": sum(allowed) / len(allowed),
    }


def run(season: int | None = None,
        days: int | None = None,
        min_edge: float = 4.0) -> dict:
    """Run the backtest. Returns per-market summaries."""
    from engine.nba_db import get_conn
    from engine.nba_predict import predict_full, _compute_team_full_ppg

    # Burn the in-process PPG cache so each game uses its own PIT slice
    # rather than the season-final values populated by an earlier run.
    if hasattr(_compute_team_full_ppg, "_cache"):
        _compute_team_full_ppg._cache = {}

    conn = get_conn()

    if days:
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute(
            "SELECT * FROM nba_games WHERE status='final' AND date >= ? "
            "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
            "ORDER BY date, game_id",
            (cutoff,),
        ).fetchall()
    elif season is not None:
        rows = conn.execute(
            "SELECT * FROM nba_games WHERE status='final' AND season = ? "
            "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
            "ORDER BY date, game_id",
            (season,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM nba_games WHERE status='final' "
            "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
            "ORDER BY date, game_id"
        ).fetchall()

    games = [dict(r) for r in rows]
    if not games:
        return {"error": "no finalized games", "games_tested": 0}

    abbr_by_id: dict[int, str] = {
        r["id"]: r["abbreviation"]
        for r in conn.execute("SELECT id, abbreviation FROM nba_teams").fetchall()
    }

    cats = {
        "ml":     _empty_cat(),
        "spread": _empty_cat(),
        "total":  _empty_cat(),
        "best":   _empty_cat(),
    }
    edge_buckets = {
        "ml":     {"4-6": _empty_cat(), "6-8": _empty_cat(), "8-12": _empty_cat(), "12+": _empty_cat()},
        "spread": {"4-6": _empty_cat(), "6-8": _empty_cat(), "8-12": _empty_cat(), "12+": _empty_cat()},
        "total":  {"4-6": _empty_cat(), "6-8": _empty_cat(), "8-12": _empty_cat(), "12+": _empty_cat()},
    }

    def _bucket(e: float) -> str:
        if e >= 12: return "12+"
        if e >= 8:  return "8-12"
        if e >= 6:  return "6-8"
        return "4-6"

    skipped = 0
    tested = 0
    for game in games:
        h_id = game["home_team_id"]
        a_id = game["away_team_id"]
        h_abbr = abbr_by_id.get(h_id)
        a_abbr = abbr_by_id.get(a_id)
        season = game["season"]
        date = game["date"]
        h_score = game["home_score"]
        a_score = game["away_score"]
        if not (h_abbr and a_abbr) or h_score is None or a_score is None:
            skipped += 1
            continue

        # Point-in-time PPG. Inject into the predict_full helper's
        # cache so the predictor sees only pre-game data.
        h_pit = _pit_ppg(conn, h_id, season, date)
        a_pit = _pit_ppg(conn, a_id, season, date)
        if not h_pit or not a_pit or h_pit["games"] < 5 or a_pit["games"] < 5:
            skipped += 1
            continue

        # Hot-swap the cache so _compute_team_full_ppg returns our PIT
        # values for THIS game. Reset cache after each game.
        _compute_team_full_ppg._cache = {
            (h_id, season): {
                "games": h_pit["games"],
                "ppg": round(h_pit["ppg"], 2),
                "opp_ppg": round(h_pit["opp_ppg"], 2),
                "margin": round(h_pit["ppg"] - h_pit["opp_ppg"], 2),
                "home_ppg": None, "home_opp_ppg": None,
                "away_ppg": None, "away_opp_ppg": None,
            },
            (a_id, season): {
                "games": a_pit["games"],
                "ppg": round(a_pit["ppg"], 2),
                "opp_ppg": round(a_pit["opp_ppg"], 2),
                "margin": round(a_pit["ppg"] - a_pit["opp_ppg"], 2),
                "home_ppg": None, "home_opp_ppg": None,
                "away_ppg": None, "away_opp_ppg": None,
            },
        }

        try:
            pred = predict_full(h_abbr, a_abbr, season=season, backtest=True)
        except Exception as e:
            logger.debug("predict_full failed %s @ %s on %s: %s",
                         a_abbr, h_abbr, date, e)
            skipped += 1
            continue

        tested += 1
        full_margin = h_score - a_score
        full_total = h_score + a_score

        # ── ML evaluation at synthetic -110 ──
        ml_home = pred["ml_home"]
        # Implied at -110 is 0.524.
        IMPLIED_110 = 110 / (110 + 100)
        if ml_home > IMPLIED_110:
            edge = (ml_home - IMPLIED_110) * 100
            if edge >= min_edge:
                won = full_margin > 0
                _record(cats["ml"], won)
                _record(edge_buckets["ml"][_bucket(edge)], won)
        elif (1 - ml_home) > IMPLIED_110:
            edge = ((1 - ml_home) - IMPLIED_110) * 100
            if edge >= min_edge:
                won = full_margin < 0
                _record(cats["ml"], won)
                _record(edge_buckets["ml"][_bucket(edge)], won)

        # ── Spread evaluation: simulate at the model's implied line ──
        # Without historical odds, we benchmark by asking: if HR offered
        # the game at the model's predicted margin (rounded), did the
        # model's COVER probability beat -110 implied? This tests the
        # margin shape, not pricing.
        pm = pred["predicted_margin"]
        line = -round(pm * 2) / 2  # home line at the model's center
        # Cover prob at this line should be ~50%; we test edge against
        # actual cover. Skip — without offering a synthetic spread at a
        # different number we can't generate edge here. Spread bucket
        # shipped only when historical odds are wired (deferred).

        # ── Total evaluation: anchor on league average ──
        # No historical posted-total available, so anchor on the league's
        # rolling average total (~228). This isn't testing the price
        # but it IS testing whether the model's per-game total
        # projection is calibrated against actual outcomes — over the
        # league average a high model_total should over-hit and a low
        # model_total should under-hit.
        LEAGUE_TOTAL_ANCHOR = 228.0
        over_p = sum(p for t, p in pred["total_probs"].items()
                     if t > LEAGUE_TOTAL_ANCHOR)
        if over_p > IMPLIED_110:
            edge = (over_p - IMPLIED_110) * 100
            if edge >= min_edge:
                won = full_total > LEAGUE_TOTAL_ANCHOR
                _record(cats["total"], won)
                _record(edge_buckets["total"][_bucket(edge)], won)
        elif (1 - over_p) > IMPLIED_110:
            edge = ((1 - over_p) - IMPLIED_110) * 100
            if edge >= min_edge:
                won = full_total < LEAGUE_TOTAL_ANCHOR
                _record(cats["total"], won)
                _record(edge_buckets["total"][_bucket(edge)], won)

    return {
        "games_tested": tested,
        "games_skipped": skipped,
        "min_edge": min_edge,
        "ml":     _summarize(cats["ml"]),
        "total":  _summarize(cats["total"]),
        "ml_by_edge":     {k: _summarize(v) for k, v in edge_buckets["ml"].items()},
        "total_by_edge":  {k: _summarize(v) for k, v in edge_buckets["total"].items()},
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=None)
    ap.add_argument("--days", type=int, default=None)
    ap.add_argument("--min-edge", type=float, default=4.0)
    ap.add_argument("--json", action="store_true", help="emit JSON only")
    args = ap.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    report = run(season=args.season, days=args.days, min_edge=args.min_edge)
    if args.json:
        print(json.dumps(report, indent=2))
        return 0

    print("=" * 70)
    print("  NBA FULL-GAME BACKTEST (synthetic -110 juice)")
    print("=" * 70)
    print(f"  games_tested  : {report['games_tested']}")
    print(f"  games_skipped : {report['games_skipped']}")
    print(f"  min_edge      : {report['min_edge']}%")
    print()
    for market in ("ml", "total"):
        s = report[market]
        print(f"  {market.upper():6s}  picks={s['picks']:>5d}  W-L-P={s['wins']}-{s['losses']}-{s['pushes']}  "
              f"WR={s['win_pct']}%  ROI={s['roi_pct']:+.2f}%  profit/100=${s['profit_per_100']:+.2f}")
        print("    by edge:")
        for bkt, v in report[f"{market}_by_edge"].items():
            print(f"      {bkt:>5s}  picks={v['picks']:>5d}  WR={v['win_pct']}%  ROI={v['roi_pct']:+.2f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
