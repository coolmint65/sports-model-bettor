"""Walk-forward calibration seeding for hockey-framework leagues.

Replays the factor predictor against every historical final game in
date order, using only data available BEFORE that game. Aggregates
predicted-bucket → realized hit rate so the empirical calibration
layer can map raw probabilities to realised win rates without waiting
for live picks to accumulate.

Mirrors `engine.basketball._walkforward.seed_calibration` in spirit
but doesn't depend on a trained GBM — the hockey-framework Poisson
predictor is the only signal source today, so the calibration uses
its raw output directly.

Markets seeded (all leakage-clean from historical scores):
  ML     — Poisson grid P(home wins) vs realized winner
  SPREAD — Skellam tail P(margin > L) at puck-line offsets, vs
           realized margin. Same approach AFL/golf use: probe the
           model's distribution at several offsets per game and bucket
           by max(p, 1-p) so the realized-cover rate per conviction
           bucket emerges from real outcome data, no synthesized lines.
  TOTAL  — Poisson sum tail P(total > L) at line offsets around the
           predicted total, vs realized total.

Output: ``data/hockey/<league>_calibration.json`` in Format A
(`buckets[bet_type]` keyed) so ``picks_core`` reads bet-type-aware
shrinkage. Backward-compatible `ml` dict is also written for any
caller still on the legacy format.

Usage::

    python -m engine.hockey._calibrate ahl
    python -m engine.hockey._calibrate pwhl
"""
from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)

_OUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "hockey"

# Same bucket grid empirical_calibration uses — keeps format compatible.
_BUCKETS = [
    (0.30, 0.40),
    (0.40, 0.50),
    (0.50, 0.55),
    (0.55, 0.60),
    (0.60, 0.65),
    (0.65, 0.70),
    (0.70, 0.80),
    (0.80, 1.01),
]


def _bucket_for(p: float) -> tuple[float, float] | None:
    for lo, hi in _BUCKETS:
        if lo <= p < hi:
            return lo, hi
    return None


# ── Point-in-time team rates ─────────────────────────────────

def _team_rates_as_of(conn, team_id: int, as_of_date: str,
                       season: int, league_avg_gpg: float,
                       min_games: int = 5) -> tuple[float, float, int]:
    """Compute (off_rate, def_rate, n_games) using only games this team
    played in ``season`` BEFORE ``as_of_date``. Returns league averages
    when the sample is too thin (< min_games)."""
    rows = conn.execute(
        "SELECT home_team_id, away_team_id, home_score, away_score "
        "FROM games WHERE season = ? AND status = 'final' "
        "  AND date < ? "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "  AND (home_team_id = ? OR away_team_id = ?)",
        (season, as_of_date, team_id, team_id),
    ).fetchall()
    if len(rows) < min_games:
        return league_avg_gpg, league_avg_gpg, len(rows)
    scored = []; allowed = []
    for r in rows:
        if r["home_team_id"] == team_id:
            scored.append(r["home_score"]); allowed.append(r["away_score"])
        else:
            scored.append(r["away_score"]); allowed.append(r["home_score"])
    return (sum(scored) / len(scored),
             sum(allowed) / len(allowed),
             len(rows))


# ── Win-prob from xG ────────────────────────────────────────

def _poisson_pmf(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def _ml_home_prob(home_xg: float, away_xg: float, max_goals: int = 12) -> float:
    """Sum P(home_score > away_score) + ½·P(tie) over a Poisson grid.
    Hockey ties resolve via OT/SO; splitting tie mass 50/50 is the
    cleanest assumption when we don't have a per-team tiebreaker model."""
    p_home = p_away = p_tie = 0.0
    for h in range(max_goals + 1):
        for a in range(max_goals + 1):
            joint = _poisson_pmf(home_xg, h) * _poisson_pmf(away_xg, a)
            if h > a:
                p_home += joint
            elif a > h:
                p_away += joint
            else:
                p_tie += joint
    if p_tie > 0:
        total = p_home + p_away
        if total > 0:
            p_home += p_tie * (p_home / total)
        else:
            p_home += p_tie / 2
    return p_home


def _spread_home_cover_prob(home_xg: float, away_xg: float,
                              line: float, max_goals: int = 12) -> float:
    """P(home_margin > line) where line is the home-side spread
    (e.g. line=-1.5 means home wins by 2+). Skellam tail via Poisson
    grid convolution — exact, not Gaussian-approximated. Hockey scores
    are integers so the strict-inequality > vs >= matters; we use
    fractional puck-line lines (±1.5) so this is unambiguous."""
    p = 0.0
    for h in range(max_goals + 1):
        ph = _poisson_pmf(home_xg, h)
        for a in range(max_goals + 1):
            if (h - a) > line:
                p += ph * _poisson_pmf(away_xg, a)
    return p


def _total_over_prob(home_xg: float, away_xg: float,
                      line: float, max_goals: int = 18) -> float:
    """P(home_score + away_score > line). Sum of independent Poissons
    is Poisson(home_xg + away_xg), so we just need the upper tail of
    a single Poisson at fractional `line`."""
    total_lam = home_xg + away_xg
    if total_lam <= 0:
        return 0.0
    p = 0.0
    for k in range(int(line) + 1, max_goals + 1):
        p += _poisson_pmf(total_lam, k)
    return p


# ── Walk-forward seeding ────────────────────────────────────

def seed_calibration(league: str) -> dict:
    """Walk every final game for ``league`` in date order. For each:
    compute predicted ml_home using only prior games, compare to actual,
    bucket the prediction. Persist + return the calibration dict."""
    from . import HOCKEY_LEAGUE_REGISTRY
    if league not in HOCKEY_LEAGUE_REGISTRY or league == "nhl":
        raise KeyError(f"Unknown hockey-framework league {league!r}")

    cfg = HOCKEY_LEAGUE_REGISTRY[league]
    league_avg_gpg = cfg.get("league_avg_gpg") or 3.0
    home_boost = cfg.get("home_boost") or 0.15

    mod = __import__(f"engine.sports.{league}.db",
                      fromlist=["get_conn"])
    conn = mod.get_conn()

    # Walk in chronological order so each prediction only uses prior data.
    games = conn.execute(
        "SELECT id, date, season, home_team_id, away_team_id, "
        "       home_score, away_score "
        "FROM games WHERE status = 'final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date ASC, id ASC"
    ).fetchall()

    buckets: dict[tuple[float, float], dict] = {b: {"n": 0, "wins": 0,
                                                       "sum_p": 0.0,
                                                       "sum_brier": 0.0}
                                                    for b in _BUCKETS}
    # SPREAD/TOTAL accumulators — same shape as ML buckets but
    # populated via distribution-reliability probes per game (not one
    # observation per game). Each probe is one (predicted_prob,
    # realized_won) pair the bucketer eats.
    spread_buckets = {b: {"n": 0, "wins": 0, "sum_p": 0.0} for b in _BUCKETS}
    total_buckets = {b: {"n": 0, "wins": 0, "sum_p": 0.0} for b in _BUCKETS}

    # Spread probes at half-point puck-line offsets around 0. Hockey
    # spreads are integer-margin events so half-point lines avoid push
    # ambiguity. Range [-2.5, +2.5] gives bucket coverage from ~0.55
    # (close to even) up through 0.85 (heavy favorite).
    _SPREAD_LINES = (-2.5, -1.5, -0.5, 0.5, 1.5, 2.5)
    # Total probes at offsets around the predicted total. Hockey totals
    # cluster at 5.5-6.5 typically; ±2.5 covers the full conviction
    # range.
    _TOTAL_OFFSETS = (-2.5, -1.5, -0.5, 0.5, 1.5, 2.5)

    skipped_thin = 0
    total = 0
    n_games_each_team_seen: dict[int, int] = defaultdict(int)
    for g in games:
        # We want at least N prior games per team; helper enforces this.
        h_off, h_def, h_n = _team_rates_as_of(
            conn, g["home_team_id"], g["date"], g["season"],
            league_avg_gpg,
        )
        a_off, a_def, a_n = _team_rates_as_of(
            conn, g["away_team_id"], g["date"], g["season"],
            league_avg_gpg,
        )
        if h_n < 5 or a_n < 5:
            skipped_thin += 1
            continue
        # Same Poisson model the live predictor uses.
        home_xg = (h_off * (a_def / league_avg_gpg)) + home_boost
        away_xg = a_off * (h_def / league_avg_gpg)
        actual_margin = int(g["home_score"]) - int(g["away_score"])
        actual_total = int(g["home_score"]) + int(g["away_score"])

        # ── ML ──
        p_home = _ml_home_prob(home_xg, away_xg)
        outcome = 1 if actual_margin > 0 else 0
        prob = p_home if p_home >= 0.5 else (1 - p_home)
        side_outcome = outcome if p_home >= 0.5 else (1 - outcome)
        b = _bucket_for(prob)
        if b is not None:
            buckets[b]["n"] += 1
            buckets[b]["wins"] += side_outcome
            buckets[b]["sum_p"] += prob
            buckets[b]["sum_brier"] += (prob - side_outcome) ** 2

        # ── SPREAD ── Skellam-tail probe at canonical puck-line offsets.
        # For each line L the model gives P(home covers L). Bucket by
        # the favored side's prob; record whether favored side won.
        for line in _SPREAD_LINES:
            p_home_cover = _spread_home_cover_prob(home_xg, away_xg, line)
            home_won_cover = 1 if actual_margin > line else 0
            if p_home_cover >= 0.5:
                sp, sw = p_home_cover, home_won_cover
            else:
                sp, sw = 1.0 - p_home_cover, 1 - home_won_cover
            bk = _bucket_for(sp)
            if bk is not None:
                spread_buckets[bk]["n"] += 1
                spread_buckets[bk]["wins"] += sw
                spread_buckets[bk]["sum_p"] += sp

        # ── TOTAL ── Poisson sum tail at fractional line offsets
        # around the predicted total. Center on the integer floor of
        # the predicted total then probe ±2.5.
        predicted_total = home_xg + away_xg
        center = round(predicted_total)
        for off in _TOTAL_OFFSETS:
            line = center + off
            if line < 0.5:
                continue
            p_over = _total_over_prob(home_xg, away_xg, line)
            over_won = 1 if actual_total > line else 0
            if p_over >= 0.5:
                sp, sw = p_over, over_won
            else:
                sp, sw = 1.0 - p_over, 1 - over_won
            bk = _bucket_for(sp)
            if bk is not None:
                total_buckets[bk]["n"] += 1
                total_buckets[bk]["wins"] += sw
                total_buckets[bk]["sum_p"] += sp
        total += 1

    def _fmt_buckets(buckets_dict, include_brier: bool = False) -> list[dict]:
        rows = []
        for lo, hi in _BUCKETS:
            v = buckets_dict[(lo, hi)]
            n = v["n"]
            entry = {
                "bucket": [lo, hi],
                "n": n,
                "avg_pred": round(v["sum_p"] / n, 4) if n else None,
                "realized_wr": round(v["wins"] / n, 4) if n else None,
            }
            if include_brier:
                entry["brier"] = (round(v["sum_brier"] / n, 4)
                                  if n else None)
            rows.append(entry)
        return rows

    out: dict = {
        "league": league,
        "n_games": total,
        "skipped_thin": skipped_thin,
        "fitted_at": _now_iso(),
        # Format A: bet-type-keyed buckets — read by framework_calibration
        # with the bet_type kwarg from picks_core. SPREAD/TOTAL keys
        # match what picks_core's BT_MAP normalizes hockey PL/OU to.
        "buckets": {
            "ML":     _fmt_buckets(buckets, include_brier=True),
            "SPREAD": _fmt_buckets(spread_buckets),
            "TOTAL":  _fmt_buckets(total_buckets),
        },
        # Format B kept for backward compat (any caller still reading
        # the old shape; we can drop this once every consumer has moved
        # to picks_core with bet_type-aware lookups).
        "ml": {
            f"{lo:.2f}-{hi:.2f}": {
                "n": v["n"],
                "wins": v["wins"],
                "hit_rate": round(v["wins"] / v["n"], 4) if v["n"] else None,
                "avg_pred": round(v["sum_p"] / v["n"], 4) if v["n"] else None,
                "brier": round(v["sum_brier"] / v["n"], 4) if v["n"] else None,
            }
            for (lo, hi), v in buckets.items()
        },
        "deferred": [],
    }

    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _OUT_DIR / f"{league}_calibration.json"
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    logger.info(
        "[%s] walk-forward calibration: %d games scored, %d skipped (thin), "
        "wrote %s", league, total, skipped_thin, out_path,
    )
    return out


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.hockey._calibrate")
    ap.add_argument("league", choices=("ahl", "pwhl", "aihl", "nzihl"))
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    res = seed_calibration(args.league)
    # Print a compact summary
    print(f"\n=== {args.league.upper()} walk-forward calibration ===")
    print(f"Games scored: {res['n_games']}, skipped (thin): {res['skipped_thin']}")
    print(f"\nBucket  |  n  | hit_rate | avg_pred | Brier")
    print("-" * 50)
    for bk, v in res["ml"].items():
        if v["n"] == 0: continue
        print(f"{bk:<10} {v['n']:>4}  {v['hit_rate']!s:<8}  "
              f"{v['avg_pred']!s:<8}  {v['brier']}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
