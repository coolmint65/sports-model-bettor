"""Walk-forward calibration seeding (B4).

Walks the league's holdout games (the same chronological 20% split the
GBM uses for testing), generates ensemble predictions per game using
only prior data, then aggregates predicted-bucket → realized hit rate
into a calibration table that ``picks_core`` can consume.

Only the ML target is seeded directly — historical games carry win/loss
ground truth without needing a market line. SPREAD and TOTAL calibration
require forward-collected odds (we don't have an odds-history feed yet);
those are noted as ``deferred`` in the output so future iterations can
fill them in once the tracker has accumulated real picks against real
lines.

Output: ``data/basketball/<league>_calibration.json`` containing the
per-bucket realized hit rate per (bet_type, prob_bucket).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from ._config import LEAGUE_REGISTRY
from ._db import get_conn
from ._features import extract_features, extract_targets
from ._ensemble import blend
from ._gbm import _model_path

logger = logging.getLogger(__name__)


_OUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "basketball"


# Same bucket grid empirical_calibration uses — keeps the calibration
# format compatible with the existing reader.
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


def seed_calibration(league: str) -> dict:
    """Walk the GBM holdout games, blend ensemble predictions, aggregate
    realized hit rate per ML bucket. Returns the calibration dict and
    persists to disk.

    Requires GBM models to be trained (B2 must have run). The walk-
    forward window is read from ``<league>_gbm/summary.json`` so the
    train/test split matches what the GBM saw — no leakage."""
    if league not in LEAGUE_REGISTRY:
        raise KeyError(f"Unknown league {league!r}")
    summary_path = _OUT_DIR / f"{league}_gbm" / "summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(
            f"GBM summary missing — run engine.basketball._gbm {league} first"
        )
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    test_start = summary.get("test_start_date")
    if not test_start:
        raise ValueError("GBM summary missing test_start_date")

    # Verify all GBM models exist
    for target in ("home_win", "margin", "total_points"):
        if not _model_path(league, target).exists():
            raise FileNotFoundError(
                f"GBM model missing for {league}/{target}"
            )

    conn = get_conn(league)
    rows = conn.execute(
        "SELECT * FROM games WHERE status = 'final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "  AND date >= ? "
        "ORDER BY date",
        (test_start,),
    ).fetchall()

    # Bucket → list of (predicted, realized)
    from collections import defaultdict
    ml_buckets: dict[tuple[float, float], list[tuple[float, int]]] = defaultdict(list)
    # SPREAD / TOTAL / Q1_SPREAD / Q1_TOTAL buckets — calibrated via
    # distribution-reliability scoring against the real outcome margin/
    # total from each holdout game.
    #
    # For SPREAD: the model gives us N(predicted_margin, margin_std).
    # At any candidate line L the model's prob = P(margin > L). The
    # question "is the model's probability calibrated?" is the same
    # one we ask live — only the line input changes. We evaluate at
    # several offsets per game so every conviction bucket gets samples
    # without needing a book to have offered that exact line. The
    # OUTCOME data (did home actually cover that point margin?) is
    # 100% real from the games table — no synthesis on either the
    # model side or the realized side.
    #
    # The deferred-until-line-history concern was about CLV (did we
    # beat the closing line?); that's a market-shopping question
    # answered by the separate CLV pipeline. Probability calibration
    # is answered here, today, from the score history we already have.
    spread_buckets: dict[tuple[float, float], list[tuple[float, int]]] = defaultdict(list)
    total_buckets: dict[tuple[float, float], list[tuple[float, int]]] = defaultdict(list)
    # Q1_* buckets — populated only when the league has fitted Q1
    # constants AND the games table carries q1 scores. Both conditions
    # hold for ESPN-sourced leagues (WNBA/NCAAM/AFL) and the SofaScore-
    # backfilled RealGM leagues. Leagues missing either skip silently.
    from ._calibrate import load as _load_constants
    constants_full = _load_constants(league) or {}
    q1_ready = all(constants_full.get(k) is not None for k in (
        "q1_margin_std", "q1_total_std", "q1_avg_total", "q1_home_boost",
    )) and constants_full.get("league_avg_total")
    q1_ml_buckets: dict[tuple[float, float], list[tuple[float, int]]] = defaultdict(list)
    q1_spread_buckets: dict[tuple[float, float], list[tuple[float, int]]] = defaultdict(list)
    q1_total_buckets: dict[tuple[float, float], list[tuple[float, int]]] = defaultdict(list)
    q1_share = (constants_full.get("q1_avg_total") /
                constants_full["league_avg_total"]) if q1_ready else None
    q1_margin_std = constants_full.get("q1_margin_std") or 0.0
    q1_total_std = constants_full.get("q1_total_std") or 0.0
    q1_avg_total = constants_full.get("q1_avg_total") or 0.0
    q1_home_boost = constants_full.get("q1_home_boost") or 0.0

    margin_std = float(constants_full.get("margin_std") or 0.0)
    total_std = float(constants_full.get("total_std") or 0.0)

    # Probe offsets expressed in units of σ. Spans the full conviction
    # range each bucket cares about:
    #   z=0.13 → p≈0.55, z=0.25 → 0.60, z=0.39 → 0.65, z=0.52 → 0.70,
    #   z=0.84 → 0.80, z=1.04 → 0.85. Mirrored for the away/under side
    #   so coverage is symmetric. Picking offsets in σ-units (not raw
    #   points) means the same probe set works across SPREAD/TOTAL and
    #   across leagues with very different std deviations.
    _Z_OFFSETS = (-1.04, -0.84, -0.52, -0.39, -0.25, -0.13,
                  0.13, 0.25, 0.39, 0.52, 0.84, 1.04)

    import math

    def _normal_cdf(z: float) -> float:
        return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))

    n_processed = 0
    n_skipped = 0
    for r in rows:
        g = dict(r)
        feats = extract_features(league, g)
        if not feats:
            n_skipped += 1
            continue
        # Build a minimal factor view from saved game scores so the MC
        # has expected_scores to centre samples on. For the walk-forward
        # we approximate factor's expected scores from the game's own
        # ratings (slight shortcut — full factor would re-run predict_full
        # but that re-uses ALL prior data and we want to match what was
        # available the day of the game).
        from ._predict import _team_full_ppg, _resolve_constants
        constants, _src = _resolve_constants(league)
        season = g["season"]
        league_avg_ppg = constants["league_avg_ppg"]
        home_full = _team_full_ppg(league, g["home_team_id"], season, league_avg_ppg)
        away_full = _team_full_ppg(league, g["away_team_id"], season, league_avg_ppg)
        # Synthetic factor view — the ensemble gets the GBM signal which
        # is what we're really trying to calibrate; factor here is just
        # a centring point for MC.
        factor_view = {
            "predicted_margin": home_full["margin"] - away_full["margin"],
            "predicted_total": home_full["ppg"] + away_full["ppg"],
            "ml_home": 0.5,
            "home_expected": home_full["ppg"],
            "away_expected": away_full["ppg"],
            "factors": {"constants": constants},
        }
        try:
            ens = blend(
                league, factor_view, feats,
                home_id=g["home_team_id"], away_id=g["away_team_id"],
                n_sims=2000,  # smaller N for speed during walk-forward
            )
        except Exception as e:
            logger.debug("blend failed game=%s: %s", g.get("game_id"), e)
            n_skipped += 1
            continue
        ml_pred = (ens["ensemble"] or {}).get("ml_home")
        if ml_pred is None:
            n_skipped += 1
            continue
        targets = extract_targets(g)
        if not targets:
            n_skipped += 1
            continue
        realized = int(targets["home_win"])
        bucket = _bucket_for(ml_pred)
        if bucket:
            ml_buckets[bucket].append((float(ml_pred), realized))

        # SPREAD calibration — distribution reliability of the model's
        # N(predicted_margin, margin_std). For each probe offset around
        # the predicted margin, compute P(margin > L) and check whether
        # home actually beat L. Symmetric: if model favors away (p<0.5)
        # we flip to track the side the picker would have backed.
        actual_margin = float(g["home_score"]) - float(g["away_score"])
        margin_pred = (ens["ensemble"] or {}).get("margin")
        if margin_pred is not None and margin_std > 0:
            for z_off in _Z_OFFSETS:
                line = float(margin_pred) + z_off * margin_std
                p_home_cover = 1.0 - _normal_cdf(z_off)
                home_won_cover = int(actual_margin > line)
                if p_home_cover >= 0.5:
                    side_prob, side_won = p_home_cover, home_won_cover
                else:
                    side_prob, side_won = 1.0 - p_home_cover, 1 - home_won_cover
                bk = _bucket_for(side_prob)
                if bk:
                    spread_buckets[bk].append((side_prob, side_won))

        # TOTAL calibration — same shape against N(predicted_total, total_std)
        actual_total = float(g["home_score"]) + float(g["away_score"])
        total_pred = (ens["ensemble"] or {}).get("total")
        if total_pred is not None and total_std > 0:
            for z_off in _Z_OFFSETS:
                line = float(total_pred) + z_off * total_std
                p_over = 1.0 - _normal_cdf(z_off)
                over_won = int(actual_total > line)
                if p_over >= 0.5:
                    side_prob, side_won = p_over, over_won
                else:
                    side_prob, side_won = 1.0 - p_over, 1 - over_won
                bk = _bucket_for(side_prob)
                if bk:
                    total_buckets[bk].append((side_prob, side_won))

        # Q1_ML / Q1_SPREAD / Q1_TOTAL — uses real Q1 scores from the
        # games table. Q1 quantities derive from full-game predictions
        # via the same q1_share + q1_home_boost transform the live
        # picker (_picks.py) uses, so calibration matches deployment.
        if q1_ready and g.get("home_q1") is not None and g.get("away_q1") is not None:
            if margin_pred is not None and q1_margin_std > 0:
                q1_margin_pred = (float(margin_pred) * float(q1_share)
                                  + q1_home_boost)
                z = q1_margin_pred / q1_margin_std
                p_home_q1 = _normal_cdf(z)
                actual_q1_margin = int(g["home_q1"]) - int(g["away_q1"])
                home_q1_win = int(actual_q1_margin > 0)
                # Q1_ML
                if p_home_q1 >= 0.5:
                    side_prob, side_won = p_home_q1, home_q1_win
                else:
                    side_prob, side_won = 1.0 - p_home_q1, 1 - home_q1_win
                bk = _bucket_for(side_prob)
                if bk:
                    q1_ml_buckets[bk].append((side_prob, side_won))
                # Q1_SPREAD — distribution reliability vs actual q1 margin
                for z_off in _Z_OFFSETS:
                    line = q1_margin_pred + z_off * q1_margin_std
                    p_home_cover = 1.0 - _normal_cdf(z_off)
                    home_won_cover = int(actual_q1_margin > line)
                    if p_home_cover >= 0.5:
                        side_prob, side_won = p_home_cover, home_won_cover
                    else:
                        side_prob, side_won = (
                            1.0 - p_home_cover, 1 - home_won_cover
                        )
                    bk = _bucket_for(side_prob)
                    if bk:
                        q1_spread_buckets[bk].append((side_prob, side_won))
            # Q1_TOTAL — center on q1_avg_total scaled by full-game total
            # ratio so we anchor on the model's view of the game pace
            # rather than the long-run league average.
            if total_pred is not None and q1_total_std > 0:
                q1_total_pred = (float(total_pred) * float(q1_share)
                                 if q1_share else q1_avg_total)
                actual_q1_total = int(g["home_q1"]) + int(g["away_q1"])
                for z_off in _Z_OFFSETS:
                    line = q1_total_pred + z_off * q1_total_std
                    p_over = 1.0 - _normal_cdf(z_off)
                    over_won = int(actual_q1_total > line)
                    if p_over >= 0.5:
                        side_prob, side_won = p_over, over_won
                    else:
                        side_prob, side_won = 1.0 - p_over, 1 - over_won
                    bk = _bucket_for(side_prob)
                    if bk:
                        q1_total_buckets[bk].append((side_prob, side_won))
        n_processed += 1

    # Aggregate → realized WR per bucket. Generic helper because we
    # build the same shape for ML and Q1_ML.
    def _agg(buckets: dict) -> list[dict]:
        rows = []
        for lo, hi in _BUCKETS:
            samples = buckets.get((lo, hi), [])
            n = len(samples)
            wins = sum(s[1] for s in samples)
            avg_pred = (sum(s[0] for s in samples) / n) if n else None
            realized = (wins / n) if n else None
            rows.append({
                "bucket": [lo, hi],
                "n": n,
                "avg_pred": round(avg_pred, 4) if avg_pred is not None else None,
                "realized_wr": round(realized, 4) if realized is not None else None,
            })
        return rows

    cal_table: dict[str, list] = {
        "ML":     _agg(ml_buckets),
        "SPREAD": _agg(spread_buckets),
        "TOTAL":  _agg(total_buckets),
    }
    if q1_ready:
        cal_table["Q1_ML"] = _agg(q1_ml_buckets)
        cal_table["Q1_SPREAD"] = _agg(q1_spread_buckets)
        cal_table["Q1_TOTAL"] = _agg(q1_total_buckets)
    bucket_summary: list[dict] = []
    for market in cal_table:
        bucket_summary += [
            {"market": market,
             **{k: row[k] for k in ("bucket", "n", "avg_pred", "realized_wr")}}
            for row in cal_table[market]
        ]
    out = {
        "league": league,
        "method": "walkforward_holdout",
        "test_start_date": test_start,
        "n_processed": n_processed,
        "n_skipped": n_skipped,
        "buckets": cal_table,
        "deferred": [],
        "fitted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    (_OUT_DIR / f"{league}_calibration.json").write_text(
        json.dumps(out, indent=2), encoding="utf-8",
    )
    logger.info("[%s] calibration seeded: n_processed=%d", league, n_processed)
    return {"summary": out, "bucket_view": bucket_summary}


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.basketball._walkforward")
    ap.add_argument("league")
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    res = seed_calibration(args.league)
    print(json.dumps(res["bucket_view"], indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
