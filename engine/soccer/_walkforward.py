"""Walk-forward calibration seeding for the soccer framework.

Walks the league's holdout window in chronological order, replays Elo
in-memory at each match (no leakage from future matches), predicts
markets via the Dixon-Coles + Elo predictor, records (predicted_prob,
realized_outcome) per (bet_type, prob_bucket), and persists the
realized hit rate per bucket.

Output: ``data/soccer/<league>_calibration.json`` consumed by
``engine.framework_calibration.calibrate('soccer', raw_prob, bet_type)``
to map the raw Dixon-Coles probability into a Bayesian-shrunk realized
hit rate.

Bet types seeded:
    ML        — 1X2 (home / draw / away each as their own observation)
    DC        — double chance (1X / X2 / 12)
    DNB       — draw-no-bet (home or away with draws filtered out)
    BTTS      — both teams to score yes/no
    TOTAL     — over/under 2.5 (and probe lines via Poisson grid)
    AH        — asian handicap (probe offsets around the predicted margin)

Markets unique to soccer don't fit basketball/hockey's BT_MAP today;
the calibrator writes them under their natural keys and
``framework_calibration.py`` learns the mapping (DC → DC, DNB → DNB,
BTTS → BTTS, AH → SPREAD-equivalent).

CLI::

    python -m engine.soccer._walkforward mls
    python -m engine.soccer._walkforward --all
    python -m engine.soccer._walkforward mls --holdout-days 240
"""
from __future__ import annotations

import argparse
import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ._config import LEAGUE_REGISTRY, get_league_config
from ._db import get_conn
from ._elo import (
    HOME_ADVANTAGE_ELO,
    INIT_ELO,
    NEUTRAL_HOME_ADVANTAGE,
    _k_for_competition,
    _outcome,
    expected_score,
    goal_margin_multiplier,
)
from ._predict import predict_match

logger = logging.getLogger(__name__)


_OUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "soccer"


_ABBR_CACHE: dict[tuple[str, int], str] = {}


def _abbr_for(league: str, team_id: int) -> str:
    """Cached teams.abbreviation lookup. Used by the V3.1 blend hook so
    the per-match market-feature join can find the team."""
    key = (league, int(team_id))
    if key in _ABBR_CACHE:
        return _ABBR_CACHE[key]
    from ._db import get_conn as _gc
    conn = _gc(league)
    row = conn.execute(
        "SELECT abbreviation FROM teams WHERE id = ? LIMIT 1",
        (int(team_id),),
    ).fetchone()
    abbr = row["abbreviation"] if row else ""
    _ABBR_CACHE[key] = abbr
    return abbr

# Same bucket grid as engine.basketball._walkforward — keeps the
# on-disk format compatible with framework_calibration's reader.
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


def _default_holdout_start(rows: list[dict], holdout_days: int) -> str:
    """Pick the date of the first holdout match: ``holdout_days`` back
    from the most recent finalized match. Falls back to the median date
    when the league has less than ``holdout_days`` of data."""
    if not rows:
        return datetime.utcnow().strftime("%Y-%m-%d")
    last = max(r["date"] for r in rows if r.get("date"))
    last_dt = datetime.strptime(last, "%Y-%m-%d")
    cutoff = last_dt - timedelta(days=holdout_days)
    return cutoff.strftime("%Y-%m-%d")


def _replay_elo_to(rows: list[dict], cutoff_date: str,
                    competition_type: str | None,
                    confederation: str | None) -> dict[int, float]:
    """Replay every pre-holdout match in chronological order against an
    in-memory Elo dict, returning the rating snapshot at the cutoff.

    No DB writes — the team_elo table is left untouched so live picks
    keep reading authoritative current ratings while we calibrate."""
    ratings: dict[int, float] = defaultdict(lambda: INIT_ELO)
    for r in rows:
        if r["date"] >= cutoff_date:
            continue
        _update_ratings(ratings, r, competition_type, confederation)
    return dict(ratings)


def _update_ratings(ratings: dict[int, float], match: dict,
                     competition_type: str | None,
                     confederation: str | None) -> None:
    """In-memory mirror of ``_elo.update_for_match`` — no DB writes."""
    home_id = int(match["home_team_id"])
    away_id = int(match["away_team_id"])
    home_score = int(match.get("home_score") or 0)
    away_score = int(match.get("away_score") or 0)
    neutral = bool(match.get("neutral_site"))

    ra = ratings.get(home_id, INIT_ELO)
    rb = ratings.get(away_id, INIT_ELO)
    home_adv = NEUTRAL_HOME_ADVANTAGE if neutral else HOME_ADVANTAGE_ELO
    e_home = expected_score(ra, rb, home_advantage=home_adv)
    s_home, _ = _outcome(home_score, away_score)
    k = _k_for_competition(competition_type, confederation)
    g = goal_margin_multiplier(home_score, away_score)
    delta = k * g * (s_home - e_home)
    ratings[home_id] = ra + delta
    ratings[away_id] = rb - delta


def _ah_probe_offsets() -> tuple[float, ...]:
    """Asian-handicap probe lines (in goals) relative to the predicted
    margin. Spans the live picker's ±2 goal candidate window so every
    conviction bucket gets samples without needing a book to have
    offered that exact line."""
    return (-2.0, -1.5, -1.0, -0.5, 0.5, 1.0, 1.5, 2.0)


def _total_probe_offsets() -> tuple[float, ...]:
    """Over/under probe lines around the predicted goal total. Half-goal
    granularity matches the lines books actually offer."""
    return (-1.5, -1.0, -0.5, 0.5, 1.0, 1.5)


def _grid_p_margin(grid: list[list[float]]) -> dict[int, float]:
    """Marginal distribution of (home_goals - away_goals)."""
    out: dict[int, float] = defaultdict(float)
    for h in range(len(grid)):
        for a in range(len(grid[h])):
            out[h - a] += grid[h][a]
    return out


def _grid_p_total(grid: list[list[float]]) -> dict[int, float]:
    """Marginal distribution of (home_goals + away_goals)."""
    out: dict[int, float] = defaultdict(float)
    for h in range(len(grid)):
        for a in range(len(grid[h])):
            out[h + a] += grid[h][a]
    return out


def _score_grid_for(league: str, pred: dict) -> list[list[float]]:
    """Recompute the score grid from the predictor's lambdas. Walks
    integer goal counts up to a safe ceiling (8x8) — gives <0.5%
    truncation error at top-flight goal rates."""
    from ._predict import _score_grid
    cfg = get_league_config(league)
    rho = cfg.get("dc_rho")
    from ._predict import DEFAULT_DC_RHO
    if rho is None:
        rho = DEFAULT_DC_RHO
    return _score_grid(pred["lambda_home"], pred["lambda_away"], rho)


def _p_margin_over(p_margin: dict[int, float], line: float) -> float:
    """P(home_margin > line). For half-line `line=0.5`, sums P(margin>=1)."""
    s = 0.0
    for m, p in p_margin.items():
        if m > line:
            s += p
    return s


def _p_total_over(p_total: dict[int, float], line: float) -> float:
    s = 0.0
    for t, p in p_total.items():
        if t > line:
            s += p
    return s


def _record(buckets: dict[tuple[float, float], list[tuple[float, int]]],
             prob: float, realized: int) -> None:
    """Append (prob, realized) to whichever bucket prob lands in."""
    bk = _bucket_for(prob)
    if bk is not None:
        buckets[bk].append((float(prob), int(realized)))


def _favored_side(prob_a: float, won_a: int) -> tuple[float, int]:
    """Always bucket the favored side. Picker only bets the favored
    side of a binary market; calibration must mirror that selection
    bias or the bucket means won't match what picks_core sees."""
    if prob_a >= 0.5:
        return prob_a, won_a
    return 1.0 - prob_a, 1 - won_a


def seed_calibration(league: str, *, holdout_days: int = 240) -> dict:
    """Walk the holdout matches, generate Dixon-Coles predictions with
    in-memory Elo, aggregate bucket → realized hit rate per bet_type.
    Persists ``data/soccer/{league}_calibration.json`` and returns it.
    """
    if league not in LEAGUE_REGISTRY:
        raise KeyError(f"Unknown soccer league {league!r}")

    cfg = get_league_config(league)
    conn = get_conn(league)
    rows = [dict(r) for r in conn.execute(
        "SELECT id, date, start_time, home_team_id, away_team_id, "
        "       home_score, away_score, neutral_site, home_side, "
        "       home_score_ht, away_score_ht "
        "FROM matches WHERE status = 'final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY start_time ASC, id ASC"
    ).fetchall()]
    if not rows:
        raise ValueError(f"no finalized matches for soccer/{league}")

    cutoff = _default_holdout_start(rows, holdout_days)
    pre_count = sum(1 for r in rows if r["date"] < cutoff)
    holdout_rows = [r for r in rows if r["date"] >= cutoff]
    if not holdout_rows:
        raise ValueError(
            f"no holdout matches for soccer/{league} after cutoff {cutoff}")

    competition_type = cfg.get("competition_type")
    confederation = cfg.get("confederation")

    ratings = _replay_elo_to(rows, cutoff, competition_type, confederation)
    logger.info("[%s] elo replay: pre=%d holdout=%d cutoff=%s",
                league, pre_count, len(holdout_rows), cutoff)

    # Bucket tables per bet_type — full game + H1 (when HT scores
    # are backfilled for the league).
    buckets: dict[str, dict[tuple[float, float], list[tuple[float, int]]]] = {
        "ML": defaultdict(list),
        "DC": defaultdict(list),
        "DNB": defaultdict(list),
        "BTTS": defaultdict(list),
        "TOTAL": defaultdict(list),
        "AH": defaultdict(list),
        "H1_ML": defaultdict(list),
        "H1_DC": defaultdict(list),
        "H1_DNB": defaultdict(list),
        "H1_BTTS": defaultdict(list),
        "H1_TOTAL": defaultdict(list),
    }

    blend_w = cfg.get("v31_market_blend")

    n_processed = 0
    n_skipped = 0
    for r in holdout_rows:
        home_id = int(r["home_team_id"])
        away_id = int(r["away_team_id"])
        r_home = ratings.get(home_id, INIT_ELO)
        r_away = ratings.get(away_id, INIT_ELO)
        try:
            pred = predict_match(
                league, home_id, away_id,
                neutral_site=bool(r["neutral_site"]),
                home_side=r.get("home_side"),
                r_home_override=r_home,
                r_away_override=r_away,
            )
        except Exception as e:
            logger.debug("predict failed match=%s: %s", r["id"], e)
            n_skipped += 1
            continue
        # V3.1 blend: when the league is configured for market-blend,
        # mutate `pred` with Pinnacle closing odds (when present) so
        # bucket calibration trains against the blended distribution
        # the live picker will actually use.
        if blend_w is not None:
            from ._predict import _apply_v31_blend
            _apply_v31_blend(
                pred, league, r.get("date") or "",
                _abbr_for(league, home_id), _abbr_for(league, away_id),
                float(blend_w),
            )

        home_score = int(r["home_score"])
        away_score = int(r["away_score"])
        margin = home_score - away_score
        total = home_score + away_score

        # ── ML (1X2) — each of the three outcomes is its own observation.
        # The picker fires only the highest-edge side per match, but a
        # calibrated bucket needs to be evaluated against every match
        # the model would have classified into that bucket regardless
        # of whether we'd have bet it.
        for prob, won in (
            (pred["p_home"], int(margin > 0)),
            (pred["p_draw"], int(margin == 0)),
            (pred["p_away"], int(margin < 0)),
        ):
            sp, sw = _favored_side(prob, won)
            _record(buckets["ML"], sp, sw)

        # ── DC (double-chance)
        for prob, won in (
            (pred["p_dc_home"], int(margin >= 0)),    # 1X
            (pred["p_dc_away"], int(margin <= 0)),    # X2
            (pred["p_dc_draw"], int(margin != 0)),    # 12
        ):
            sp, sw = _favored_side(prob, won)
            _record(buckets["DC"], sp, sw)

        # ── DNB — only counts non-draws; pushes don't seed buckets.
        if margin != 0:
            for prob, won in (
                (pred["p_dnb_home"], int(margin > 0)),
                (pred["p_dnb_away"], int(margin < 0)),
            ):
                sp, sw = _favored_side(prob, won)
                _record(buckets["DNB"], sp, sw)

        # ── BTTS
        btts_yes = int(home_score > 0 and away_score > 0)
        for prob, won in (
            (pred["p_btts_yes"], btts_yes),
            (pred["p_btts_no"], 1 - btts_yes),
        ):
            sp, sw = _favored_side(prob, won)
            _record(buckets["BTTS"], sp, sw)

        # ── TOTAL — probe lines around the predicted total. Walking
        # the integer-total marginal gives exact P(total > L) without
        # the normal-approx the basketball walkforward uses (Poisson
        # is discrete so we can do this directly).
        grid = _score_grid_for(league, pred)
        p_total = _grid_p_total(grid)
        predicted_total = float(pred["lambda_home"] + pred["lambda_away"])
        for off in _total_probe_offsets():
            line = predicted_total + off
            p_over = _p_total_over(p_total, line)
            over_won = int(total > line)
            sp, sw = _favored_side(p_over, over_won)
            _record(buckets["TOTAL"], sp, sw)

        # ── AH — probe handicap lines around the predicted margin.
        p_margin = _grid_p_margin(grid)
        predicted_margin = float(pred["lambda_home"] - pred["lambda_away"])
        for off in _ah_probe_offsets():
            # Asian handicap line H means "home -H" pays out when
            # actual_margin > H. Symmetric probes give both
            # favorite-side and underdog-side bucket coverage.
            line = predicted_margin + off
            p_home_cover = _p_margin_over(p_margin, line)
            home_won_cover = int(margin > line)
            sp, sw = _favored_side(p_home_cover, home_won_cover)
            _record(buckets["AH"], sp, sw)

        # ── H1 markets — only when the league has backfilled HT scores
        # for this match. Skip silently otherwise so leagues without HT
        # coverage still seed the full-game buckets.
        if (r.get("home_score_ht") is not None
                and r.get("away_score_ht") is not None):
            h1_home = int(r["home_score_ht"])
            h1_away = int(r["away_score_ht"])
            h1_margin = h1_home - h1_away
            h1_total = h1_home + h1_away

            for prob, won in (
                (pred["p_h1_home"], int(h1_margin > 0)),
                (pred["p_h1_draw"], int(h1_margin == 0)),
                (pred["p_h1_away"], int(h1_margin < 0)),
            ):
                sp, sw = _favored_side(prob, won)
                _record(buckets["H1_ML"], sp, sw)

            for prob, won in (
                (pred["p_h1_dc_home"], int(h1_margin >= 0)),
                (pred["p_h1_dc_away"], int(h1_margin <= 0)),
                (pred["p_h1_dc_draw"], int(h1_margin != 0)),
            ):
                sp, sw = _favored_side(prob, won)
                _record(buckets["H1_DC"], sp, sw)

            if h1_margin != 0:
                for prob, won in (
                    (pred["p_h1_dnb_home"], int(h1_margin > 0)),
                    (pred["p_h1_dnb_away"], int(h1_margin < 0)),
                ):
                    sp, sw = _favored_side(prob, won)
                    _record(buckets["H1_DNB"], sp, sw)

            h1_btts_yes = int(h1_home > 0 and h1_away > 0)
            for prob, won in (
                (pred["p_h1_btts_yes"], h1_btts_yes),
                (pred["p_h1_btts_no"], 1 - h1_btts_yes),
            ):
                sp, sw = _favored_side(prob, won)
                _record(buckets["H1_BTTS"], sp, sw)

            # H1 totals — Poisson grid is built on H1 lambdas inside
            # predict_match; re-derive here to probe lines around the
            # H1 predicted total.
            lam_h1_h = float(pred.get("lambda_h1_home", 0.0))
            lam_h1_a = float(pred.get("lambda_h1_away", 0.0))
            from ._predict import _score_grid as _sg
            cfg_local = get_league_config(league)
            rho_local = cfg_local.get("dc_rho")
            from ._predict import DEFAULT_DC_RHO as _DEF_RHO
            if rho_local is None:
                rho_local = _DEF_RHO
            h1_grid = _sg(lam_h1_h, lam_h1_a, rho_local)
            p_h1_total = _grid_p_total(h1_grid)
            predicted_h1_total = lam_h1_h + lam_h1_a
            # H1 typically lands 0.5/1.5/2.5 — narrower probe offsets.
            for off in (-1.0, -0.5, 0.5, 1.0):
                line = predicted_h1_total + off
                p_over = _p_total_over(p_h1_total, line)
                over_won = int(h1_total > line)
                sp, sw = _favored_side(p_over, over_won)
                _record(buckets["H1_TOTAL"], sp, sw)

        # Update Elo for the next iteration's prediction
        _update_ratings(ratings, r, competition_type, confederation)
        n_processed += 1

    out = _serialize(buckets)
    out["league"] = league
    out["test_start_date"] = cutoff
    out["holdout_n"] = len(holdout_rows)
    out["n_processed"] = n_processed
    out["n_skipped"] = n_skipped
    out["fitted_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    out_path = _OUT_DIR / f"{league}_calibration.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    logger.info("[%s] calibration persisted → %s (n=%d, %d skipped)",
                league, out_path.name, n_processed, n_skipped)
    return out


def _serialize(buckets: dict[str, dict[tuple[float, float],
                                       list[tuple[float, int]]]]) -> dict:
    """Convert the bucket dicts into the on-disk format
    framework_calibration.calibrate() reads (Format A — bet_type-keyed
    list of {bucket, n, avg_pred, realized_wr})."""
    out: dict[str, Any] = {"buckets": {}}
    for bt, table in buckets.items():
        rows = []
        for bk, samples in sorted(table.items()):
            if not samples:
                continue
            n = len(samples)
            avg_pred = sum(p for p, _ in samples) / n
            wins = sum(w for _, w in samples)
            rows.append({
                "bucket": [float(bk[0]), float(bk[1])],
                "n": n,
                "avg_pred": round(avg_pred, 4),
                "realized_wr": round(wins / n, 4),
            })
        out["buckets"][bt] = rows
    return out


def _cli() -> int:
    ap = argparse.ArgumentParser(prog="engine.soccer._walkforward")
    ap.add_argument("league", nargs="?")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--holdout-days", type=int, default=240,
                     help="days back from latest final to start the "
                          "holdout window (default 240 ≈ 8 months)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.all:
        leagues = list(LEAGUE_REGISTRY.keys())
    elif args.league:
        leagues = [args.league]
    else:
        ap.error("specify --all or a league name")
        return 1
    for lg in leagues:
        try:
            out = seed_calibration(lg, holdout_days=args.holdout_days)
            for bt, rows in out["buckets"].items():
                if not rows:
                    continue
                print(f"[{lg}] {bt}:")
                for row in rows:
                    bk = row["bucket"]
                    print(f"  [{bk[0]:.2f}, {bk[1]:.2f}) n={row['n']:>4}  "
                          f"avg={row['avg_pred']:.3f}  wr={row['realized_wr']:.3f}")
        except Exception as e:
            print(f"[{lg}] FAILED: {e}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
