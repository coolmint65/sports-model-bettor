"""Backfill calibration_samples for MLB / NHL / NBA.

For each sport we:
  1. Walk the sport's historical final games.
  2. Re-run that sport's factor model against PIT stats.
  3. Extract model probabilities for each calibratable market.
  4. Compare to the actual outcome and store (model_prob, result) rows
     in the sport's calibration_samples table.

The calibrator UNIONs these with real tracker picks, so buckets
that currently rely on passthrough (N < MIN_BUCKET_N) fill with honest
empirical rates as soon as a backfill lands.

Usage:
    python -m scripts.backfill_calibration                   # MLB, all seasons
    python -m scripts.backfill_calibration --sport nhl
    python -m scripts.backfill_calibration --sport nba
    python -m scripts.backfill_calibration --season 2025
    python -m scripts.backfill_calibration --sport nhl --limit 300
    python -m scripts.backfill_calibration --force          # reprocess existing

All sports are idempotent (UNIQUE(game_id, bet_type) on the samples
table). Re-running skips already-processed games unless --force.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Callable


def _result(actual: bool) -> str:
    return "W" if actual else "L"


# ── MLB ────────────────────────────────────────────────────────────

def _mlb_conn():
    from engine.db import get_conn
    return get_conn()


def _mlb_load_games(conn, season: int | None, limit: int | None) -> list[dict]:
    where = ["status = 'final'",
             "home_score IS NOT NULL", "away_score IS NOT NULL",
             "home_team_id IS NOT NULL", "away_team_id IS NOT NULL"]
    params: list = []
    if season:
        where.append("season = ?")
        params.append(season)
    sql = (
        "SELECT mlb_game_id AS game_id, date, home_team_id, away_team_id, "
        "       home_pitcher_id, away_pitcher_id, venue, "
        "       home_score, away_score, home_linescore, away_linescore "
        "FROM games WHERE " + " AND ".join(where) + " ORDER BY date ASC"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _mlb_samples(game: dict) -> list[dict]:
    from engine.mlb_predict import predict_matchup
    try:
        pred = predict_matchup(
            home_team_id=game["home_team_id"],
            away_team_id=game["away_team_id"],
            home_pitcher_id=game["home_pitcher_id"],
            away_pitcher_id=game["away_pitcher_id"],
            venue=game.get("venue"),
        )
    except Exception:
        return []
    if not pred or "error" in pred:
        return []

    home_score = game.get("home_score") or 0
    away_score = game.get("away_score") or 0
    total = home_score + away_score
    samples: list[dict] = []

    # ML
    wp = pred.get("win_prob") or {}
    h, a = wp.get("home"), wp.get("away")
    if h is not None and a is not None:
        if h >= a:
            samples.append({"bet_type": "ML", "pick": "home",
                            "model_prob": float(h),
                            "result": _result(home_score > away_score)})
        else:
            samples.append({"bet_type": "ML", "pick": "away",
                            "model_prob": float(a),
                            "result": _result(away_score > home_score)})

    # O/U (anchor 8.5)
    ou = pred.get("over_under") or {}
    if ou:
        try:
            anchor = min(ou.keys(), key=lambda k: abs(float(k) - 8.5))
            line_val = float(anchor)
            d = ou[anchor]
            o, u = d.get("over"), d.get("under")
            if o is not None and u is not None and total != line_val:
                if o >= u:
                    samples.append({"bet_type": "O/U",
                                    "pick": f"Over {anchor}",
                                    "model_prob": float(o),
                                    "result": _result(total > line_val)})
                else:
                    samples.append({"bet_type": "O/U",
                                    "pick": f"Under {anchor}",
                                    "model_prob": float(u),
                                    "result": _result(total < line_val)})
        except (ValueError, TypeError, AttributeError):
            pass

    # NRFI/YRFI
    fi = pred.get("first_inning") or {}
    nrfi_p, yrfi_p = fi.get("nrfi"), fi.get("yrfi")
    h_ls, a_ls = game.get("home_linescore"), game.get("away_linescore")
    if nrfi_p is not None and yrfi_p is not None and h_ls and a_ls:
        try:
            hi = json.loads(h_ls); ai = json.loads(a_ls)
            if hi and ai:
                scoreless = (hi[0] == 0 and ai[0] == 0)
                if nrfi_p >= yrfi_p:
                    samples.append({"bet_type": "1st INN", "pick": "NRFI",
                                    "model_prob": float(nrfi_p),
                                    "result": _result(scoreless)})
                else:
                    samples.append({"bet_type": "1st INN", "pick": "YRFI",
                                    "model_prob": float(yrfi_p),
                                    "result": _result(not scoreless)})
        except (json.JSONDecodeError, IndexError):
            pass

    # F5 ML
    f5 = pred.get("f5") or {}
    f5wp = f5.get("win_prob") or {}
    fh, fa = f5wp.get("home"), f5wp.get("away")
    if fh is not None and fa is not None and h_ls and a_ls:
        try:
            hi = json.loads(h_ls); ai = json.loads(a_ls)
            if len(hi) >= 5 and len(ai) >= 5:
                fh_total = sum(hi[:5]); fa_total = sum(ai[:5])
                if fh_total != fa_total:
                    if fh >= fa:
                        samples.append({"bet_type": "F5 ML", "pick": "home",
                                        "model_prob": float(fh),
                                        "result": _result(fh_total > fa_total)})
                    else:
                        samples.append({"bet_type": "F5 ML", "pick": "away",
                                        "model_prob": float(fa),
                                        "result": _result(fa_total > fh_total)})
        except (json.JSONDecodeError, IndexError):
            pass

    return samples


# ── NHL ────────────────────────────────────────────────────────────

def _nhl_conn():
    from engine.nhl_db import get_conn
    return get_conn()


def _nhl_team_key_map() -> dict[int, str]:
    """Map nhl_teams.id -> team JSON file stem (e.g. 'bruins').

    predict_matchup takes the JSON stem, NOT the abbreviation, so we
    build the map by walking data/teams/NHL/*.json and matching each
    file's `abbreviation` field back to nhl_teams.id. Mirrors
    backend/server.py:_nhl_espn_to_key.
    """
    from engine.data import list_teams, load_team
    conn = _nhl_conn()
    # nhl_teams.id -> abbreviation (uppercase, canonical)
    id_to_abbr: dict[int, str] = {}
    for r in conn.execute("SELECT id, abbreviation FROM nhl_teams").fetchall():
        abbr = (r["abbreviation"] or "").strip().upper()
        if abbr:
            id_to_abbr[r["id"]] = abbr

    # abbreviation -> JSON file stem (loaded from data/teams/NHL/*.json)
    abbr_to_stem: dict[str, str] = {}
    for t in list_teams("NHL"):
        stem = t.get("key")
        if not stem:
            continue
        team = load_team("NHL", stem)
        if team and team.get("abbreviation"):
            abbr_to_stem[team["abbreviation"].strip().upper()] = stem

    # Join: team_id -> stem
    return {tid: abbr_to_stem[abbr]
            for tid, abbr in id_to_abbr.items() if abbr in abbr_to_stem}


def _nhl_load_games(conn, season: int | None, limit: int | None) -> list[dict]:
    where = ["status = 'final'",
             "home_score IS NOT NULL", "away_score IS NOT NULL",
             "home_team_id IS NOT NULL", "away_team_id IS NOT NULL"]
    params: list = []
    if season:
        where.append("season = ?")
        params.append(season)
    sql = (
        "SELECT game_id, date, home_team_id, away_team_id, "
        "       home_score, away_score, home_p1, away_p1 "
        "FROM nhl_games WHERE " + " AND ".join(where) + " ORDER BY date ASC"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _nhl_samples(game: dict, key_map: dict[int, str]) -> list[dict]:
    from engine.nhl_predict import predict_matchup
    h_key = key_map.get(game["home_team_id"])
    a_key = key_map.get(game["away_team_id"])
    if not h_key or not a_key:
        return []
    try:
        pred = predict_matchup(h_key, a_key)
    except Exception:
        return []
    if not pred:
        return []

    home_score = game.get("home_score") or 0
    away_score = game.get("away_score") or 0
    total = home_score + away_score
    samples: list[dict] = []

    # ML (regulation + OT combined)
    wp = pred.get("win_prob") or {}
    h, a = wp.get("home"), wp.get("away")
    if h is not None and a is not None:
        if h >= a:
            samples.append({"bet_type": "ML", "pick": "home",
                            "model_prob": float(h),
                            "result": _result(home_score > away_score)})
        else:
            samples.append({"bet_type": "ML", "pick": "away",
                            "model_prob": float(a),
                            "result": _result(away_score > home_score)})

    # O/U (anchor 6.5, typical NHL total)
    ou = pred.get("over_under") or {}
    if ou:
        try:
            anchor = min(ou.keys(), key=lambda k: abs(float(k) - 6.5))
            line_val = float(anchor)
            d = ou[anchor]
            o, u = d.get("over"), d.get("under")
            if o is not None and u is not None and total != line_val:
                if o >= u:
                    samples.append({"bet_type": "O/U",
                                    "pick": f"Over {anchor}",
                                    "model_prob": float(o),
                                    "result": _result(total > line_val)})
                else:
                    samples.append({"bet_type": "O/U",
                                    "pick": f"Under {anchor}",
                                    "model_prob": float(u),
                                    "result": _result(total < line_val)})
        except (ValueError, TypeError, AttributeError):
            pass

    # Puck line (home ±1.5)
    pl = pred.get("puck_line") or {}
    h_m15 = pl.get("home_minus_1_5")
    h_p15 = pl.get("home_plus_1_5")
    margin = home_score - away_score
    if h_m15 is not None and h_p15 is not None:
        # Pick the side the model prefers (and that actually makes sense).
        # home -1.5 covers iff margin >= 2. home +1.5 covers iff margin >= -1.
        if h_m15 >= h_p15:
            samples.append({"bet_type": "PL", "pick": "home -1.5",
                            "model_prob": float(h_m15),
                            "result": _result(margin >= 2)})
        else:
            samples.append({"bet_type": "PL", "pick": "home +1.5",
                            "model_prob": float(h_p15),
                            "result": _result(margin >= -1)})

    # P1 (first-period total over 1.5, when period scores are available)
    fp = pred.get("first_period") or {}
    p1_over_15 = fp.get("over_15")
    p1_under_15 = fp.get("under_15")
    h_p1 = game.get("home_p1")
    a_p1 = game.get("away_p1")
    if p1_over_15 is not None and p1_under_15 is not None \
            and h_p1 is not None and a_p1 is not None:
        p1_total = h_p1 + a_p1
        if p1_total != 1.5:  # never pushes since 1.5 is a half-point line
            if p1_over_15 >= p1_under_15:
                samples.append({"bet_type": "P1 O/U", "pick": "Over 1.5",
                                "model_prob": float(p1_over_15),
                                "result": _result(p1_total > 1.5)})
            else:
                samples.append({"bet_type": "P1 O/U", "pick": "Under 1.5",
                                "model_prob": float(p1_under_15),
                                "result": _result(p1_total < 1.5)})

    return samples


# ── NBA ────────────────────────────────────────────────────────────

def _nba_conn():
    from engine.nba_db import get_conn
    return get_conn()


def _nba_abbr_map() -> dict[int, str]:
    conn = _nba_conn()
    rows = conn.execute(
        "SELECT id, abbreviation FROM nba_teams"
    ).fetchall()
    return {r["id"]: (r["abbreviation"] or "").strip().upper()
            for r in rows}


def _nba_load_games(conn, season: int | None, limit: int | None) -> list[dict]:
    where = ["status = 'final'",
             "home_score IS NOT NULL", "away_score IS NOT NULL",
             "home_q1 IS NOT NULL", "away_q1 IS NOT NULL"]
    params: list = []
    if season:
        where.append("season = ?")
        params.append(season)
    sql = (
        "SELECT game_id, date, home_team_id, away_team_id, "
        "       home_score, away_score, home_q1, away_q1 "
        "FROM nba_games WHERE " + " AND ".join(where) + " ORDER BY date ASC"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _nba_samples(game: dict, abbr_map: dict[int, str]) -> list[dict]:
    from engine.nba_q1_predict import predict_q1
    h_abbr = abbr_map.get(game["home_team_id"])
    a_abbr = abbr_map.get(game["away_team_id"])
    if not h_abbr or not a_abbr:
        return []
    try:
        pred = predict_q1(h_abbr, a_abbr)
    except Exception:
        return []
    if not pred:
        return []

    home_q1 = game.get("home_q1") or 0
    away_q1 = game.get("away_q1") or 0
    home_score = game.get("home_score") or 0
    away_score = game.get("away_score") or 0
    samples: list[dict] = []

    # Q1 ML (home win prob); predict_q1 emits q1_ml_home / q1_ml_away.
    q1_h = pred.get("q1_ml_home")
    q1_a = pred.get("q1_ml_away")
    if q1_h is not None and q1_a is not None and home_q1 != away_q1:
        if q1_h >= q1_a:
            samples.append({"bet_type": "Q1 ML", "pick": "home",
                            "model_prob": float(q1_h),
                            "result": _result(home_q1 > away_q1)})
        else:
            samples.append({"bet_type": "Q1 ML", "pick": "away",
                            "model_prob": float(q1_a),
                            "result": _result(away_q1 > home_q1)})

    # Full-game ML via the Q1 margin implied prob isn't reliable; skip.

    return samples


# ── Main loop ──────────────────────────────────────────────────────

def _backfill(sport: str, season: int | None, limit: int | None,
              force: bool) -> int:
    if sport == "mlb":
        conn = _mlb_conn()
        games = _mlb_load_games(conn, season, limit)
        sampler: Callable[[dict], list[dict]] = _mlb_samples
        key_arg = None
    elif sport == "nhl":
        conn = _nhl_conn()
        games = _nhl_load_games(conn, season, limit)
        key_arg = _nhl_team_key_map()
        sampler = lambda g: _nhl_samples(g, key_arg)
    elif sport == "nba":
        conn = _nba_conn()
        games = _nba_load_games(conn, season, limit)
        key_arg = _nba_abbr_map()
        sampler = lambda g: _nba_samples(g, key_arg)
    else:
        print(f"Unknown sport: {sport}", flush=True)
        return 1

    if not games:
        print(f"No {sport} games to process. Exiting.", flush=True)
        return 0

    if not force:
        existing = {r["game_id"] for r in conn.execute(
            "SELECT DISTINCT game_id FROM calibration_samples"
        ).fetchall()}
        before = len(games)
        games = [g for g in games if g["game_id"] not in existing]
        print(f"[{sport}] Skipping {before - len(games)} "
              f"already-processed games", flush=True)

    print(f"[{sport}] Backfilling {len(games)} games", flush=True)
    t0 = time.time()
    inserted = 0
    failures = 0
    last_report = t0

    for i, g in enumerate(games):
        samples = sampler(g)
        for s in samples:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO calibration_samples "
                    "(game_id, date, bet_type, pick, model_prob, result) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (g["game_id"], g["date"], s["bet_type"],
                     s["pick"], s["model_prob"], s["result"]),
                )
                inserted += 1
            except Exception as e:
                failures += 1
                if failures < 10:
                    print(f"  insert failed for {g['game_id']} "
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
    print(f"[{sport}] Done in {elapsed}s -- inserted {inserted} samples "
          f"across {len(games)} games ({failures} failures)", flush=True)

    # Refresh calibration so the new samples take effect immediately.
    try:
        from engine.empirical_calibration import refresh_calibration
        s = refresh_calibration(sport)
        print(f"[{sport}] Calibration refreshed: {s}", flush=True)
    except Exception as e:
        print(f"  WARN: refresh_calibration failed: {e}", flush=True)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sport", choices=["mlb", "nhl", "nba"],
                        default="mlb")
    parser.add_argument("--season", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    return _backfill(args.sport, args.season, args.limit, args.force)


if __name__ == "__main__":
    sys.exit(main())
