"""F1 race-winner + podium predictor.

Approach (v1, deliberately small to ship a calibrated baseline before
adding complexity):

  1. Driver form score from last N races: per-race performance =
     (field - finish_pos + 1) / field, treating DNF/DSQ as 0. Average
     over the last ``FORM_WINDOW`` races, weighted exponentially toward
     recent.
  2. Team form score: same calculation, averaged over both team drivers.
     Captures car pace independent of driver.
  3. Qualifying-grid effect: pole sitter wins ~40% historically, P15
     wins <2%. Encoded as ``GRID_FACTOR ** (grid_pos - 1)`` so pole=1.0,
     P2=0.85, P10≈0.20.
  4. Combined latent strength s_i = α·driver + β·team + γ·log(grid_factor).
  5. P(win_i) = softmax(s) — guarantees sum-to-1 across the field.
  6. P(podium_i) by Monte Carlo: ``MC_SIMS`` Plackett-Luce samples,
     count fraction of sims where driver finishes top-3.

Why softmax vs Bradley-Terry: BT is pairwise; softmax is the natural
multi-class extension and matches the Plackett-Luce ranking model that
generates the podium estimates.

Calibration: the raw probs are then mapped through an isotonic /
Beta-Binomial calibrator (same plumbing as team sports) once we have
enough motorsports settled picks. Until then, predictor outputs are
fed to the picks engine as-is and gated only by edge floors.
"""
from __future__ import annotations

import logging
import math
import random
from typing import Any

from ._db import get_conn

logger = logging.getLogger(__name__)


# Hyperparameters — kept as module constants until we have enough live
# results to fit them via walk-forward CV (planned once 2-3 races' worth
# of picks have settled).
FORM_WINDOW = 10            # last N races for driver form
FORM_DECAY = 0.85           # exponential decay (0.85^9 ≈ 23% weight on
                            # the oldest race in the window)
DRIVER_WEIGHT = 0.45        # α
TEAM_WEIGHT = 0.30          # β
GRID_WEIGHT = 0.25          # γ (log-grid contribution)
GRID_FACTOR = 0.92          # 0.92^9 ≈ 0.47 — P10 has roughly half the
                            # grid-bonus of pole. Calibrated against
                            # Ergast 2015-2024 grid→winner rates.
SOFTMAX_TEMP = 9.0          # higher = more concentrated on top contender,
                            # lower = flatter. 4.0 was clearly too flat
                            # — model gave bottom-half drivers ~7%
                            # podium probability when market priced them
                            # at <1%, producing huge spurious "edges"
                            # that would have backed losers at +50000.
                            # 9.0 puts the field-leader around 30-35%
                            # win prob and bottom-half near 0% podium.

MC_SIMS = 10000             # Monte Carlo sims for podium probabilities


def predict_race(series: str, race_id: str,
                 grid_overrides: dict[int, int] | None = None,
                 apply_calibration: bool = True) -> dict:
    """Compute per-driver win + podium probabilities for ``race_id``.

    ``grid_overrides`` lets the caller inject qualifying positions when
    they're known but not yet in the DB (qualifying happens 24h before
    the race; results land in race_results only after the race itself).
    Maps driver_id → grid_pos (1 = pole). When None, uses the most
    recent qualifying-pos from the driver's last completed race as a
    placeholder (reasonable since car pace correlates strongly week-to-
    week, but explicit overrides are always preferred).

    Returns::

        {
          "race_id": "2026-05",
          "drivers": [
              {"driver_id": 1, "name": "Max Verstappen", "abbrev": "VER",
               "team": "Red Bull Racing", "grid_pos": 1,
               "p_win": 0.34, "p_podium": 0.71},
              ...
          ],
        }
    """
    conn = get_conn(series)
    race = conn.execute(
        "SELECT * FROM races WHERE race_id = ?", (race_id,)
    ).fetchone()
    if not race:
        raise ValueError(f"unknown race {race_id}")

    # Active driver list = drivers with a result in the *current* season.
    # Avoids predicting a 2024-only driver who's no longer on the grid.
    season = race["season"]
    drivers = conn.execute("""
        SELECT DISTINCT d.id, d.name, d.abbreviation, d.team_id,
               d.nationality,
               t.name AS team_name, t.id AS team_id_join
        FROM drivers d
        JOIN race_results rr ON rr.driver_id = d.id
        JOIN races r ON r.race_id = rr.race_id
        LEFT JOIN teams t ON t.id = rr.team_id
        WHERE r.season = ?
          AND r.race_date < ?
        GROUP BY d.id
    """, (season, race["race_date"])).fetchall()

    if not drivers:
        # First race of season — no current-season data. Fall back to
        # last-season rosters so the panel still returns something.
        # Schema parity with the primary query (nationality column was
        # missing here and caused IndexError on 2025-01 / 2026-01 calls).
        drivers = conn.execute("""
            SELECT DISTINCT d.id, d.name, d.abbreviation, d.team_id,
                   d.nationality,
                   t.name AS team_name, t.id AS team_id_join
            FROM drivers d
            JOIN race_results rr ON rr.driver_id = d.id
            JOIN races r ON r.race_id = rr.race_id
            LEFT JOIN teams t ON t.id = rr.team_id
            WHERE r.season = ?
            GROUP BY d.id
        """, (season - 1,)).fetchall()
    if not drivers:
        # Final fallback for the very first season in the DB (no prior
        # season to lean on): use every driver in the drivers table
        # with their currently-known team. Cold-start neutral weights —
        # not great predictions, but unblocks the calibration backfill
        # so we don't drop the first race of the corpus.
        drivers = conn.execute("""
            SELECT d.id, d.name, d.abbreviation, d.team_id,
                   d.nationality,
                   t.name AS team_name, t.id AS team_id_join
            FROM drivers d
            LEFT JOIN teams t ON t.id = d.team_id
        """).fetchall()

    # Driver form scores
    driver_scores = {}
    for d in drivers:
        driver_scores[d["id"]] = _driver_form(
            conn, d["id"], season, race["race_date"]
        )

    # Team form scores (drivers share team avg)
    team_scores: dict[int, float] = {}
    for d in drivers:
        tid = d["team_id_join"]
        if tid is None:
            continue
        if tid not in team_scores:
            team_scores[tid] = _team_form(
                conn, tid, season, race["race_date"]
            )

    # Combined latent strength s_i = α·driver + β·team + γ·log(grid_factor)
    strengths: list[tuple[dict, float]] = []
    for d in drivers:
        d_score = driver_scores.get(d["id"], 0.10)
        t_score = team_scores.get(d["team_id_join"], 0.10)
        grid = (grid_overrides or {}).get(d["id"]) or _last_grid_pos(
            conn, d["id"], race["race_date"]
        ) or 11  # mid-grid default if completely unknown
        grid_term = math.log(max(GRID_FACTOR ** (grid - 1), 1e-6))
        s = (DRIVER_WEIGHT * d_score
             + TEAM_WEIGHT * t_score
             + GRID_WEIGHT * grid_term)
        strengths.append((d, s, grid))

    # Softmax → P(win)
    raw = [SOFTMAX_TEMP * s for _, s, _ in strengths]
    m = max(raw)
    exps = [math.exp(x - m) for x in raw]
    z = sum(exps)
    p_wins = [e / z for e in exps]

    # Podium via Monte Carlo Plackett-Luce
    weights = exps  # PL strengths = unnormalized softmax weights
    p_podiums = _mc_podium(weights)

    # Apply calibration (no-op if not fitted). Calibrator maps per-
    # driver independently so the field-sum invariant (winners sum to
    # 1.0, podium sums to 3.0) breaks. Re-normalize after calibration
    # to restore it — picks engine compares model vs market on a
    # de-vigged field so internal consistency matters more than the
    # absolute scale.
    cal_w = cal_p = None
    raw_w_list = list(p_wins)
    raw_p_list = list(p_podiums)
    if apply_calibration:
        from . import _calibration
        cal_w_vec = [_calibration.calibrate(series, "WINNER", x) for x in p_wins]
        cal_p_vec = [_calibration.calibrate(series, "PODIUM", x) for x in p_podiums]
        sum_w = sum(cal_w_vec) or 1.0
        sum_p = sum(cal_p_vec) or 1.0
        p_wins = [v / sum_w for v in cal_w_vec]
        p_podiums = [v * 3.0 / sum_p for v in cal_p_vec]

    out = []
    for (d, _s, grid), pw, pp, raw_w, raw_p in zip(
        strengths, p_wins, p_podiums, raw_w_list, raw_p_list,
    ):
        out.append({
            "driver_id": d["id"],
            "name": d["name"],
            "abbrev": d["abbreviation"],
            "team": d["team_name"] or "",
            "nationality": d["nationality"] or "",
            "grid_pos": grid,
            "p_win": pw,
            "p_podium": pp,
            "raw_p_win": raw_w,
            "raw_p_podium": raw_p,
        })
    out.sort(key=lambda r: -r["p_win"])
    return {"race_id": race_id, "drivers": out}


# ── Form-score helpers ─────────────────────────────────────

def _driver_form(conn, driver_id: int, season: int, before_date: str) -> float:
    """Exp-decay weighted average of (field-rank-1)/field over the last
    FORM_WINDOW races. DNF/DSQ scores 0."""
    rows = conn.execute("""
        SELECT rr.finish_pos, rr.status,
               (SELECT COUNT(*) FROM race_results WHERE race_id = rr.race_id) AS field
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WHERE rr.driver_id = ? AND r.race_date < ? AND r.status = 'complete'
        ORDER BY r.race_date DESC LIMIT ?
    """, (driver_id, before_date, FORM_WINDOW)).fetchall()
    if not rows:
        return 0.10  # rookie / no-data prior
    total = 0.0
    weight_sum = 0.0
    for i, row in enumerate(rows):
        w = FORM_DECAY ** i
        finish = row["finish_pos"]
        field = row["field"] or 20
        if finish is None:
            score = 0.0
        else:
            score = max(0.0, (field - finish + 1) / field)
        total += w * score
        weight_sum += w
    return total / weight_sum if weight_sum else 0.10


def _team_form(conn, team_id: int, season: int, before_date: str) -> float:
    """Same as driver form but averaged across the team's drivers'
    last FORM_WINDOW races. Captures car pace as a separate factor."""
    rows = conn.execute("""
        SELECT rr.finish_pos,
               (SELECT COUNT(*) FROM race_results WHERE race_id = rr.race_id) AS field
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WHERE rr.team_id = ? AND r.race_date < ? AND r.status = 'complete'
        ORDER BY r.race_date DESC LIMIT ?
    """, (team_id, before_date, FORM_WINDOW * 2)).fetchall()
    if not rows:
        return 0.10
    scores = []
    for row in rows:
        finish = row["finish_pos"]
        field = row["field"] or 20
        if finish is None:
            scores.append(0.0)
        else:
            scores.append(max(0.0, (field - finish + 1) / field))
    return sum(scores) / len(scores)


def _last_grid_pos(conn, driver_id: int, before_date: str) -> int | None:
    row = conn.execute("""
        SELECT rr.qualifying_pos
        FROM race_results rr JOIN races r ON r.race_id = rr.race_id
        WHERE rr.driver_id = ? AND r.race_date < ?
              AND rr.qualifying_pos IS NOT NULL
        ORDER BY r.race_date DESC LIMIT 1
    """, (driver_id, before_date)).fetchone()
    return row["qualifying_pos"] if row else None


# ── Plackett-Luce Monte Carlo for podium ──────────────────

def _mc_podium(weights: list[float], sims: int = MC_SIMS) -> list[float]:
    """Return P(driver_i finishes in top 3) for each driver, computed
    by ``sims`` Plackett-Luce samples. Each sample draws drivers without
    replacement weighted by ``weights`` until 3 are picked.

    PL is the right model because it matches our softmax assumption: if
    P(win | field) ∝ exp(s_i), then P(2nd | field minus winner) is the
    same softmax over the remaining drivers. So MC gives us the joint
    podium distribution implied by the win model — no extra parameters
    needed.
    """
    n = len(weights)
    if n == 0:
        return []
    podium_counts = [0] * n
    rng = random.Random(42)  # deterministic for testability
    indices = list(range(n))
    for _ in range(sims):
        remaining_w = list(weights)
        # Sample 3 without replacement
        for _slot in range(min(3, n)):
            tot = sum(remaining_w)
            if tot <= 0:
                break
            r = rng.random() * tot
            acc = 0.0
            for i in indices:
                if remaining_w[i] <= 0:
                    continue
                acc += remaining_w[i]
                if r <= acc:
                    podium_counts[i] += 1
                    remaining_w[i] = 0
                    break
    return [c / sims for c in podium_counts]


def predict_next_race(series: str) -> dict:
    """Convenience: predict the next scheduled race in the calendar."""
    from datetime import datetime
    conn = get_conn(series)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    row = conn.execute("""
        SELECT race_id FROM races
        WHERE race_date >= ? AND status = 'scheduled'
        ORDER BY race_date LIMIT 1
    """, (today,)).fetchone()
    if not row:
        raise ValueError(f"no upcoming race for {series}")
    return predict_race(series, row["race_id"])
