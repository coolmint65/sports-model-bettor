"""Backfill calibration_samples for MLB by re-running predict_matchup
on every historical final game.

Why: the live picks tracker only has ~200 settled rows -- not enough
to fill the calibration buckets (>=10 samples per bucket). Each
historical game can produce 3-4 synthetic samples (ML, OU, NRFI, F5_ML)
once the model has been re-evaluated on it. The MLB games table holds
~7500 finals back to 2023, so a full backfill bumps the calibration
sample size by ~30k.

The synthetic samples are point-in-time correct: predict_matchup uses
PIT stats computed as of the game's date, so predictions for an Apr
2024 game only see data from before that game. No look-ahead leak.

Usage:
    python -m scripts.backfill_calibration                # all sport=mlb finals
    python -m scripts.backfill_calibration --season 2025  # one season
    python -m scripts.backfill_calibration --limit 500    # cap rows for testing

Idempotent: UNIQUE(game_id, bet_type) on calibration_samples means
re-running skips already-processed (game, market) pairs unless
`--force` is passed.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime

from engine.db import get_conn
from engine.mlb_predict import predict_matchup


def _result(actual: bool) -> str:
    return "W" if actual else "L"


def _samples_for_game(game: dict, pred: dict) -> list[dict]:
    """Pull every (bet_type, model_prob, actual_outcome) row this game
    contributes. Returns one dict per market the game can grade."""
    out: list[dict] = []
    if not pred or "error" in pred:
        return out

    home_score = game.get("home_score") or 0
    away_score = game.get("away_score") or 0
    total = home_score + away_score

    # ── ML ─────────────────────────────────────────────────────────
    wp = (pred.get("win_prob") or {})
    home_wp = wp.get("home")
    away_wp = wp.get("away")
    if home_wp is not None and away_wp is not None:
        # Pick the side the model favored. Tie-breaker: home.
        if home_wp >= away_wp:
            picked = "home"
            prob = float(home_wp)
            won = home_score > away_score
        else:
            picked = "away"
            prob = float(away_wp)
            won = away_score > home_score
        out.append({"bet_type": "ML", "pick": picked,
                    "model_prob": prob, "result": _result(won)})

    # ── O/U ────────────────────────────────────────────────────────
    # Use the modeled line nearest 8.5 (typical MLB total). Pick the
    # side the model leans on; grade against actual total.
    ou_lines = pred.get("over_under") or {}
    if ou_lines:
        try:
            anchor = min(ou_lines.keys(), key=lambda k: abs(float(k) - 8.5))
            line_val = float(anchor)
            data = ou_lines[anchor]
            over_p = data.get("over")
            under_p = data.get("under")
            if over_p is not None and under_p is not None:
                if over_p >= under_p:
                    prob = float(over_p)
                    won = total > line_val
                    pick = f"Over {anchor}"
                else:
                    prob = float(under_p)
                    won = total < line_val
                    pick = f"Under {anchor}"
                if total != line_val:  # exclude pushes
                    out.append({"bet_type": "O/U", "pick": pick,
                                "model_prob": prob,
                                "result": _result(won)})
        except (ValueError, TypeError, AttributeError):
            pass

    # ── NRFI / YRFI ────────────────────────────────────────────────
    fi = pred.get("first_inning") or {}
    nrfi_p = fi.get("nrfi")
    yrfi_p = fi.get("yrfi")
    home_ls = game.get("home_linescore")
    away_ls = game.get("away_linescore")
    if nrfi_p is not None and yrfi_p is not None and home_ls and away_ls:
        try:
            h_inn = json.loads(home_ls)
            a_inn = json.loads(away_ls)
            if h_inn and a_inn:
                scoreless_1st = (h_inn[0] == 0 and a_inn[0] == 0)
                if nrfi_p >= yrfi_p:
                    prob = float(nrfi_p)
                    won = scoreless_1st
                    pick = "NRFI"
                else:
                    prob = float(yrfi_p)
                    won = not scoreless_1st
                    pick = "YRFI"
                out.append({"bet_type": "1st INN", "pick": pick,
                            "model_prob": prob, "result": _result(won)})
        except (json.JSONDecodeError, IndexError):
            pass

    # ── F5 ML ──────────────────────────────────────────────────────
    f5 = pred.get("f5") or {}
    f5_wp = (f5.get("win_prob") or {})
    f5_h = f5_wp.get("home")
    f5_a = f5_wp.get("away")
    if f5_h is not None and f5_a is not None and home_ls and away_ls:
        try:
            h_inn = json.loads(home_ls)
            a_inn = json.loads(away_ls)
            if len(h_inn) >= 5 and len(a_inn) >= 5:
                f5_home_total = sum(h_inn[:5])
                f5_away_total = sum(a_inn[:5])
                if f5_home_total != f5_away_total:  # exclude ties
                    if f5_h >= f5_a:
                        prob = float(f5_h)
                        won = f5_home_total > f5_away_total
                        pick = "home"
                    else:
                        prob = float(f5_a)
                        won = f5_away_total > f5_home_total
                        pick = "away"
                    out.append({"bet_type": "F5 ML", "pick": pick,
                                "model_prob": prob,
                                "result": _result(won)})
        except (json.JSONDecodeError, IndexError):
            pass

    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=None,
                        help="Limit to a single MLB season (default: all)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap rows processed (for testing)")
    parser.add_argument("--force", action="store_true",
                        help="Reprocess games already in calibration_samples")
    args = parser.parse_args()

    conn = get_conn()

    where = ["status = 'final'",
             "home_score IS NOT NULL", "away_score IS NOT NULL",
             "home_team_id IS NOT NULL", "away_team_id IS NOT NULL"]
    params: list = []
    if args.season:
        where.append("season = ?")
        params.append(args.season)

    sql = (
        "SELECT mlb_game_id, date, home_team_id, away_team_id, "
        "       home_pitcher_id, away_pitcher_id, venue, "
        "       home_score, away_score, home_linescore, away_linescore "
        "FROM games WHERE " + " AND ".join(where) +
        " ORDER BY date ASC"
    )
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"

    games = [dict(r) for r in conn.execute(sql, params).fetchall()]
    if not games:
        print("No matching games. Exiting.", flush=True)
        return 0

    # Skip games that already have samples (unless --force).
    if not args.force:
        existing = {r["game_id"] for r in conn.execute(
            "SELECT DISTINCT game_id FROM calibration_samples"
        ).fetchall()}
        games = [g for g in games if g["mlb_game_id"] not in existing]
        print(f"Skipping {len(existing)} already-processed games", flush=True)

    print(f"Backfilling {len(games)} games", flush=True)
    t0 = time.time()
    inserted = 0
    failures = 0
    last_report = t0

    for i, g in enumerate(games):
        try:
            pred = predict_matchup(
                home_team_id=g["home_team_id"],
                away_team_id=g["away_team_id"],
                home_pitcher_id=g["home_pitcher_id"],
                away_pitcher_id=g["away_pitcher_id"],
                venue=g.get("venue"),
            )
        except Exception as e:
            failures += 1
            if failures < 10:
                print(f"  predict failed for game {g['mlb_game_id']}: {e}",
                      flush=True)
            continue

        samples = _samples_for_game(g, pred)
        for s in samples:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO calibration_samples "
                    "(game_id, date, bet_type, pick, model_prob, result) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (g["mlb_game_id"], g["date"], s["bet_type"],
                     s["pick"], s["model_prob"], s["result"]),
                )
                inserted += 1
            except Exception as e:
                failures += 1
                if failures < 10:
                    print(f"  insert failed for {g['mlb_game_id']} "
                          f"{s['bet_type']}: {e}", flush=True)

        if (i + 1) % 50 == 0:
            conn.commit()

        now = time.time()
        if now - last_report > 5:
            elapsed = now - t0
            rate = (i + 1) / elapsed
            eta = (len(games) - i - 1) / rate if rate > 0 else 0
            print(f"  [{i + 1}/{len(games)}] {inserted} samples written "
                  f"({rate:.1f} games/s, ETA {int(eta)}s)", flush=True)
            last_report = now

    conn.commit()
    elapsed = round(time.time() - t0, 1)
    print(f"Done in {elapsed}s -- inserted {inserted} samples "
          f"across {len(games)} games ({failures} failures)",
          flush=True)

    # Trigger a calibration refresh so the new samples take effect immediately.
    try:
        from engine.empirical_calibration import refresh_calibration
        s = refresh_calibration("mlb")
        print(f"Calibration refreshed: {s}", flush=True)
    except Exception as e:
        print(f"  WARN: refresh_calibration failed: {e}", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
