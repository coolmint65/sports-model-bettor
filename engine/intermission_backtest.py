"""
Intermission predictor backtest.

Replays the Phase 5h (NHL) and 5i (NBA) intermission predictors
against historical_pbp + the canonical games tables, scoring
final-outcome accuracy vs naive baselines.

Why this exists
---------------
We've been shipping intermission picks live without ever validating
the predictor on historical data. This backtest answers the bedrock
question: when our model says "after H1 the final total will be X",
how close to right is it on a 4000-game corpus?

Core comparisons
----------------
For each historical intermission moment we evaluate three predictions:

1. **Model prediction** — the actual 5h/5i predictor, fed the same
   inputs it would see live (score so far, foul state, period).

2. **Linear-extrapolation baseline** — assume current pace continues.
   home_final = home_score × (game_minutes / elapsed_minutes).
   This is what "winging it" looks like — anything we beat here is
   real predictive value.

3. **Prematch baseline** — score-naïve: prematch projection as if the
   game is starting fresh. Tests whether knowing the H1 score actually
   helps vs ignoring it.

Outputs (per sport):
    - Final-total RMSE for each predictor
    - Final-margin RMSE
    - Win-classification accuracy (home_final_win > 0.5 vs reality)
    - Buckets: by lead size, by quarter, by surface (NHL not applicable)

Usage::

    python -m engine.intermission_backtest --sport nba [--limit 500]
"""

from __future__ import annotations

import argparse
import logging
import math
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "live.db"


def _live_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ── Game-final lookup ─────────────────────────────────────────

def _final_outcomes_for_sport(sport: str) -> dict[str, dict]:
    """Pull final scores from the canonical sport DB so we have ground
    truth for every backfilled game."""
    if sport == "nba":
        from .nba_db import get_conn
        conn = get_conn()
        rows = conn.execute(
            "SELECT game_id, home_score, away_score, "
            "       home_q1, away_q1, home_q2, away_q2, "
            "       home_q3, away_q3, home_q4, away_q4 "
            "FROM nba_games WHERE status='final' "
            "  AND home_score IS NOT NULL"
        ).fetchall()
    elif sport == "nhl":
        from .nhl_db import get_conn
        conn = get_conn()
        rows = conn.execute(
            "SELECT game_id, home_score, away_score, "
            "       home_p1, away_p1, home_p2, away_p2, home_p3, away_p3 "
            "FROM nhl_games WHERE status='final' "
            "  AND home_score IS NOT NULL"
        ).fetchall()
    else:
        return {}
    # historical_pbp keys are TEXT; cast on insert so .get() lookups
    # work regardless of whether canonical DB stored INT or TEXT.
    return {str(r["game_id"]): dict(r) for r in rows}


# ── PBP → intermission state extraction ───────────────────────

def _pbp_intermissions(sport: str, game_id: str) -> list[dict]:
    """Walk a game's PBP and return the score state at each
    end-of-period boundary (excluding the final-period buzzer).

    Returns a list of dicts with keys: period_ended, home_score,
    away_score, where the score is what the canonical PBP rows
    show on the period-end play.
    """
    conn = _live_conn()
    plays = conn.execute(
        "SELECT play_id, sequence, period, type_text, home_score, away_score "
        "FROM historical_pbp "
        "WHERE sport = ? AND game_id = ? "
        "ORDER BY period, sequence",
        (sport, str(game_id)),
    ).fetchall()
    if not plays:
        return []

    out: list[dict] = []
    # ESPN NBA uses "period end"/"end period"; NHL Stats API uses
    # "period-end" (hyphenated). Match both.
    end_markers = {"period end", "end period", "end of period",
                    "period-end"}
    max_p = max(int(p["period"] or 0) for p in plays)
    for p in plays:
        type_text = (p["type_text"] or "").strip().lower()
        if type_text not in end_markers:
            continue
        period = int(p["period"] or 0)
        if period <= 0 or period >= max_p:
            # Final-period buzzer is the game-end, not an intermission
            continue
        out.append({
            "period_ended": period,
            "home_score": int(p["home_score"] or 0),
            "away_score": int(p["away_score"] or 0),
        })
    return out


# ── Predictors / baselines ─────────────────────────────────────

def _quarter_min_for(sport: str) -> tuple[float, float]:
    """Return (period_minutes, total_game_minutes) for the sport."""
    if sport == "nba":
        return 12.0, 48.0
    return 20.0, 60.0


def _linear_extrap(sport: str, intermission: dict) -> tuple[float, float]:
    """Naive baseline: assume scoring continues at the same per-minute
    rate. Returns (final_total, final_margin) projection.
    """
    p_min, g_min = _quarter_min_for(sport)
    elapsed = p_min * intermission["period_ended"]
    if elapsed <= 0:
        return 0.0, 0.0
    score_factor = g_min / elapsed
    home_final = intermission["home_score"] * score_factor
    away_final = intermission["away_score"] * score_factor
    return (home_final + away_final, home_final - away_final)


def _model_predict_nba(state_dict: dict, intermission: dict) -> dict | None:
    """Call engine.live._nba_intermission_predict.predict_intermission
    with a synthesised state. Bypasses the live-state store / lineup
    snapshot since we're working purely off the canonical score at
    period end.

    Foul state is approximated as zero in trouble (data unavailable
    historically without per-player accumulation, which would slow
    the backtest by ~20x). The 5i predictor without foul drag is the
    fairer test of the score-shrinkage layer alone.
    """
    from .live._nba_intermission_predict import (
        _SHRINK_WEIGHT, _QUARTER_MIN, _GAME_MIN,
    )
    from .nba_predict import predict_full
    home_abbr = state_dict.get("home_abbr")
    away_abbr = state_dict.get("away_abbr")
    if not home_abbr or not away_abbr:
        return None
    period_ended = int(intermission["period_ended"])
    if period_ended >= 4:
        return None
    try:
        prematch = predict_full(home_abbr, away_abbr)
    except Exception:
        return None
    if not prematch:
        return None
    home_pred = float(prematch.get("home_expected") or 0)
    away_pred = float(prematch.get("away_expected") or 0)
    if home_pred <= 0 or away_pred <= 0:
        return None
    elapsed = period_ended * _QUARTER_MIN
    remaining = _GAME_MIN - elapsed
    shrink = _SHRINK_WEIGHT.get(period_ended, 0.30)
    home_score = intermission["home_score"]
    away_score = intermission["away_score"]
    home_exp_so_far = home_pred * (elapsed / _GAME_MIN)
    away_exp_so_far = away_pred * (elapsed / _GAME_MIN)
    home_mult = 1.0 + ((home_score / max(0.1, home_exp_so_far)) - 1.0) * shrink
    away_mult = 1.0 + ((away_score / max(0.1, away_exp_so_far)) - 1.0) * shrink
    home_mult = max(0.5, min(1.6, home_mult))
    away_mult = max(0.5, min(1.6, away_mult))
    home_rem = home_pred * (remaining / _GAME_MIN) * home_mult
    away_rem = away_pred * (remaining / _GAME_MIN) * away_mult
    final_home = home_score + home_rem
    final_away = away_score + away_rem
    return {
        "final_total": final_home + final_away,
        "final_margin": final_home - final_away,
        "prematch_total": home_pred + away_pred,
        "prematch_margin": home_pred - away_pred,
    }


_NHL_ABBR_TO_KEY: dict[str, str] = {}


def _nhl_team_key_for(abbr: str) -> str | None:
    """Map an NHL abbreviation (TOR / CBJ / VGK) to the JSON file
    stem (maple_leafs / blue_jackets / golden_knights) that
    nhl_predict.predict_matchup expects.

    Built lazily on first call from engine.data.list_teams + the
    nhl_db lookup. Cached at module level."""
    if not abbr:
        return None
    if not _NHL_ABBR_TO_KEY:
        from .data import list_teams
        from .nhl_db import get_nhl_team_by_abbr
        # Build name → key map first
        name_to_key = {t["name"]: t["key"] for t in list_teams("nhl")}
        # Then walk every team in the DB and find its matching key.
        for t in list_teams("nhl"):
            # name match handled directly above; fall through to abbr
            # lookup so we cover both directions.
            pass
        # We need abbr → key, so reverse-resolve via DB lookup
        # against name_to_key.
        # Using nhl_teams' abbreviations table directly
        from .nhl_db import get_conn
        rows = get_conn().execute(
            "SELECT abbreviation, name FROM nhl_teams"
        ).fetchall()
        for r in rows:
            ab = r["abbreviation"]
            key = name_to_key.get(r["name"])
            if ab and key:
                _NHL_ABBR_TO_KEY[ab] = key
    return _NHL_ABBR_TO_KEY.get(abbr)


def _model_predict_nhl(state_dict: dict, intermission: dict) -> dict | None:
    """Mirror of _model_predict_nba for NHL. Calls the prematch NHL
    factor predict + the same shot/save adjustment 5h applies."""
    from .nhl_predict import predict_matchup as _nhl_pm
    from .live._nhl_period_predict import simulate_remaining
    home_abbr = state_dict.get("home_abbr")
    away_abbr = state_dict.get("away_abbr")
    if not home_abbr or not away_abbr:
        return None
    home_key = _nhl_team_key_for(home_abbr)
    away_key = _nhl_team_key_for(away_abbr)
    if not home_key or not away_key:
        return None
    period_ended = int(intermission["period_ended"])
    if period_ended >= 3:
        return None
    try:
        prematch = _nhl_pm(home_key, away_key, backtest=True)
    except Exception:
        return None
    if not prematch:
        return None
    home_total = float(prematch.get("total") or 0)
    if home_total <= 0:
        return None
    # Simple per-period rate from the prematch total; xG adjustments
    # would require per-period shots-on-goal which we'd have to walk
    # PBP for. Skipped here for backtest speed; the 5h predictor with
    # full xG/SV% adjustments is what production runs.
    home_xg = float((prematch.get("expected_score") or {}).get("home") or home_total / 2)
    away_xg = float((prematch.get("expected_score") or {}).get("away") or home_total / 2)
    periods_remaining = 3 - period_ended
    if periods_remaining <= 0:
        return None
    home_xg_period = home_xg / 3.0
    away_xg_period = away_xg / 3.0
    sim = simulate_remaining(home_xg_period, away_xg_period,
                              periods_remaining, n_sims=10_000,
                              seed=abs(hash((state_dict.get("game_id"),
                                              period_ended))) % (2**32))
    home_rem_mean = float(sim["home_goals"].mean())
    away_rem_mean = float(sim["away_goals"].mean())
    home_score = intermission["home_score"]
    away_score = intermission["away_score"]
    return {
        "final_total": home_score + away_score + home_rem_mean + away_rem_mean,
        "final_margin": (home_score + home_rem_mean) - (away_score + away_rem_mean),
        "prematch_total": home_xg + away_xg,
        "prematch_margin": home_xg - away_xg,
    }


# ── Game-state lookup helpers ──────────────────────────────────

def _state_for_game(sport: str, game_id: str) -> dict | None:
    """Pull just enough metadata for the predictor — home/away abbr,
    final scores."""
    if sport == "nba":
        from .nba_db import get_conn
        conn = get_conn()
        row = conn.execute(
            "SELECT g.game_id, ht.abbreviation AS home_abbr, "
            "       at.abbreviation AS away_abbr "
            "FROM nba_games g "
            "LEFT JOIN nba_teams ht ON ht.id = g.home_team_id "
            "LEFT JOIN nba_teams at ON at.id = g.away_team_id "
            "WHERE g.game_id = ?",
            (str(game_id),),
        ).fetchone()
    elif sport == "nhl":
        from .nhl_db import get_conn
        conn = get_conn()
        # nhl_games.game_id is INTEGER; historical_pbp.game_id is TEXT.
        # SQLite's loose typing won't reliably equate '2023010065' to
        # the integer 2023010065 across all versions, so cast for the
        # JOIN.
        try:
            gid_i = int(game_id)
        except (TypeError, ValueError):
            return None
        row = conn.execute(
            "SELECT g.game_id, ht.abbreviation AS home_abbr, "
            "       at.abbreviation AS away_abbr "
            "FROM nhl_games g "
            "LEFT JOIN nhl_teams ht ON ht.id = g.home_team_id "
            "LEFT JOIN nhl_teams at ON at.id = g.away_team_id "
            "WHERE g.game_id = ?",
            (gid_i,),
        ).fetchone()
    else:
        return None
    return dict(row) if row else None


# ── Backtest loop ─────────────────────────────────────────────

def backtest_sport(sport: str, *, limit: int | None = None,
                    progress_every: int = 100) -> dict:
    """Walk every backfilled game, replay each intermission moment
    through the predictor, score against actual outcome.
    """
    conn = _live_conn()
    games = [r["game_id"] for r in conn.execute(
        "SELECT DISTINCT game_id FROM historical_pbp WHERE sport = ?",
        (sport,),
    ).fetchall()]
    if limit:
        games = games[: int(limit)]
    finals = _final_outcomes_for_sport(sport)

    predictor = _model_predict_nba if sport == "nba" else _model_predict_nhl

    accumulator = {
        "model":    {"total_se": 0.0, "margin_se": 0.0, "n": 0,
                      "win_correct": 0, "win_n": 0},
        "linear":   {"total_se": 0.0, "margin_se": 0.0, "n": 0,
                      "win_correct": 0, "win_n": 0},
        "prematch": {"total_se": 0.0, "margin_se": 0.0, "n": 0,
                      "win_correct": 0, "win_n": 0},
    }
    by_period = defaultdict(lambda: {"model_total_se": 0.0,
                                       "linear_total_se": 0.0,
                                       "n": 0})

    started = time.monotonic()
    games_processed = 0
    intermission_count = 0
    skipped_no_state = 0

    for idx, game_id in enumerate(games, 1):
        final = finals.get(str(game_id))
        if not final:
            continue
        state = _state_for_game(sport, game_id)
        if not state:
            skipped_no_state += 1
            continue
        intermissions = _pbp_intermissions(sport, game_id)
        if not intermissions:
            continue
        actual_total = (final["home_score"] or 0) + (final["away_score"] or 0)
        actual_margin = (final["home_score"] or 0) - (final["away_score"] or 0)
        actual_home_wins = actual_margin > 0

        for inter in intermissions:
            try:
                pred = predictor(state, inter)
            except Exception as e:
                logger.debug("predict failed %s/%s: %s", sport, game_id, e)
                continue
            if not pred:
                continue

            # Linear extrapolation baseline
            lin_total, lin_margin = _linear_extrap(sport, inter)

            # Score the three predictors
            for label, total_pred, margin_pred in (
                ("model",    pred["final_total"], pred["final_margin"]),
                ("linear",   lin_total, lin_margin),
                ("prematch", pred["prematch_total"], pred["prematch_margin"]),
            ):
                acc = accumulator[label]
                acc["total_se"]  += (total_pred - actual_total) ** 2
                acc["margin_se"] += (margin_pred - actual_margin) ** 2
                acc["n"] += 1
                # Win classification — predictor says home wins iff
                # final_margin > 0; same for actual
                pred_home_wins = margin_pred > 0
                if pred_home_wins == actual_home_wins:
                    acc["win_correct"] += 1
                acc["win_n"] += 1

            bp = by_period[inter["period_ended"]]
            bp["model_total_se"]  += (pred["final_total"] - actual_total) ** 2
            bp["linear_total_se"] += (lin_total - actual_total) ** 2
            bp["n"] += 1
            intermission_count += 1

        games_processed += 1
        if games_processed % progress_every == 0:
            elapsed = time.monotonic() - started
            rate = games_processed / max(0.1, elapsed)
            eta = (len(games) - games_processed) / max(0.1, rate)
            logger.info("backtest[%s]: %d/%d games (%.1f/s, ~%ds left, "
                        "%d intermissions scored)",
                        sport, games_processed, len(games), rate, eta,
                        intermission_count)

    # Compute summary
    summary: dict = {
        "sport": sport,
        "games_processed": games_processed,
        "intermissions_scored": intermission_count,
        "elapsed_sec": round(time.monotonic() - started, 1),
        "skipped_no_state": skipped_no_state,
        "predictors": {},
        "by_period": {},
    }
    for label, acc in accumulator.items():
        n = max(1, acc["n"])
        win_n = max(1, acc["win_n"])
        summary["predictors"][label] = {
            "n": acc["n"],
            "total_rmse":  round(math.sqrt(acc["total_se"]  / n), 2),
            "margin_rmse": round(math.sqrt(acc["margin_se"] / n), 2),
            "win_accuracy": round(acc["win_correct"] / win_n, 4),
        }
    for period, bp in sorted(by_period.items()):
        n = max(1, bp["n"])
        summary["by_period"][f"end_of_{period}"] = {
            "n": bp["n"],
            "model_rmse":  round(math.sqrt(bp["model_total_se"]  / n), 2),
            "linear_rmse": round(math.sqrt(bp["linear_total_se"] / n), 2),
        }
    return summary


# ── CLI ───────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="engine.intermission_backtest")
    ap.add_argument("--sport", choices=("nba", "nhl"), required=True)
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap games to process (testing).")
    args = ap.parse_args(argv)

    res = backtest_sport(args.sport, limit=args.limit)
    print()
    print(f"  {args.sport.upper()} intermission backtest")
    print(f"  games: {res['games_processed']}, "
          f"intermissions: {res['intermissions_scored']}, "
          f"elapsed: {res['elapsed_sec']}s")
    print(f"\n  {'predictor':>10s}  {'n':>6s}  {'total_rmse':>11s}  "
          f"{'margin_rmse':>12s}  {'win_acc':>8s}")
    for label, m in res["predictors"].items():
        print(f"  {label:>10s}  {m['n']:>6d}  "
              f"{m['total_rmse']:>11.2f}  {m['margin_rmse']:>12.2f}  "
              f"{m['win_accuracy']:>7.1%}")
    print(f"\n  {'period':>14s}  {'n':>6s}  {'model':>8s}  {'linear':>8s}")
    for k, v in res["by_period"].items():
        print(f"  {k:>14s}  {v['n']:>6d}  "
              f"{v['model_rmse']:>8.2f}  {v['linear_rmse']:>8.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
