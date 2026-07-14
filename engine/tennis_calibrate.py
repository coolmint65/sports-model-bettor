"""Tennis walk-forward calibration via chronological Elo replay.

Generates leakage-free per-bucket realized hit rates for the tennis
picker's bet_types by replaying Elo updates match-by-match through the
entire tennis_matches corpus and capturing the model's prediction
*before* each holdout match's outcome updates the state.

Why a full replay and not just calling ``tennis_predict.predict_match``:
that function reads ``tennis_elo.rating_for()``, which returns the
CURRENT (post-all-matches) Elo book. Predicting an April 25 match
using May 16 Elo means the model has implicitly "seen" the match's
own outcome plus three weeks of subsequent results. The first attempt
at this calibrator did exactly that and produced absurdly inflated
realized rates (0.62 predicted → 0.84 realized) that contradicted the
43% live ML hit rate.

The fix: maintain an in-memory ``_RatingState`` book (same data
structure ``engine.tennis_elo.train`` uses internally) and walk every
match in tennis_matches in chronological order. Before each match in
the holdout window, snapshot the current state into a prediction; after
the prediction, apply the match's outcome to the state.

Markets seeded (BO3 and BO5 handled separately):
    ML                    — Glicko win prob vs realized winner
    SET_SPREAD            — straight-sets sweep probability vs realized
    TOTAL_SETS            — went-distance prob vs realized
    WIN_AT_LEAST_ONE_SET  — derived from sweep prob; vs realized
    TOTAL_GAMES           — distribution-reliability bucketing of
                             tennis_dist_gbm's predicted_total against
                             N(predicted, val_rmse) at σ-scaled offsets,
                             realized = actual total games from score.
                             GBM features are serve-rates + structural
                             (its Elo query is a broken SELECT that
                             defaults to 1500 for everyone), so it's
                             leakage-clean by accident — the model only
                             ever saw the val-rmse residuals.

Output: ``data/tennis_calibration.json`` in the shape framework_calibration
expects (buckets keyed by bet_type, same as basketball framework leagues).

Run:
    python -m engine.tennis_calibrate                  # both tours, all-time
    python -m engine.tennis_calibrate --since-year 2020 # speed-up
    python -m engine.tennis_calibrate --holdout-months 6
"""
from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

_OUT = Path(__file__).resolve().parent.parent / "data" / "tennis_calibration.json"

# σ-scaled probe offsets for distribution-reliability bucketing of
# TOTAL_GAMES. Same shape as engine.basketball._walkforward — covers
# every conviction bucket from 0.55 through 0.85.
_Z_OFFSETS = (-1.04, -0.84, -0.52, -0.39, -0.25, -0.13,
              0.13, 0.25, 0.39, 0.52, 0.84, 1.04)


def _normal_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


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


def _per_set_prob_from_match_bo3(p_match: float) -> float:
    """Invert the BO3 cubic match→set formula via bisection. Mirror of
    engine.tennis_predict._per_set_prob_from_match_bo3."""
    if p_match <= 0.0:
        return 0.0
    if p_match >= 1.0:
        return 1.0
    lo, hi = 0.0, 1.0
    for _ in range(40):
        mid = (lo + hi) / 2.0
        if mid * mid * (1.0 + 2.0 * (1.0 - mid)) < p_match:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def _set_dist_bo3(p_set: float) -> dict[tuple[int, int], float]:
    q = 1.0 - p_set
    return {
        (2, 0): p_set * p_set,
        (2, 1): 2.0 * p_set * p_set * q,
        (1, 2): 2.0 * p_set * q * q,
        (0, 2): q * q,
    }


def _set_dist_bo5(p_set: float) -> dict[tuple[int, int], float]:
    q = 1.0 - p_set
    return {
        (3, 0): p_set ** 3,
        (3, 1): 3 * p_set ** 3 * q,
        (3, 2): 6 * p_set ** 3 * q ** 2,
        (2, 3): 6 * p_set ** 2 * q ** 3,
        (1, 3): 3 * p_set * q ** 3,
        (0, 3): q ** 3,
    }


def _parse_score(score: str | None, best_of: int) -> dict | None:
    """Parse a Sackmann-style score into (winner_sets, loser_sets,
    total_games). Returns None on malformed input or when winner_sets
    is below the best-of threshold."""
    if not score:
        return None
    p1_sets = p2_sets = total_games = 0
    for s in score.split():
        if "(" in s:
            s = s.split("(", 1)[0]
        if "-" not in s:
            return None
        try:
            a, b = s.split("-", 1)
            a_g, b_g = int(a), int(b)
        except (ValueError, TypeError):
            return None
        if a_g > b_g:
            p1_sets += 1
        elif b_g > a_g:
            p2_sets += 1
        total_games += a_g + b_g
    need = 2 if best_of == 3 else 3
    if p1_sets < need:
        return None
    return {"winner_sets": p1_sets, "loser_sets": p2_sets,
            "total_games": total_games}


# ── Chronological Elo replay ──────────────────────────────────────

def _glicko_win_prob(r_a: float, r_b: float, rd_a: float, rd_b: float) -> float:
    """Mirror of engine.tennis_predict._glicko_win_prob — RD-weighted
    Elo win prob so high-uncertainty matchups shrink toward 0.5."""
    g_rd_sq = (rd_a * rd_a + rd_b * rd_b)
    g_factor = 1.0 / math.sqrt(1.0 + 3.0 * g_rd_sq / (math.pi ** 2 * 400.0 * 400.0))
    exponent = -g_factor * (r_a - r_b) / 400.0
    return 1.0 / (1.0 + 10.0 ** exponent)


def seed(*, tours: list[str] | None = None,
         since_year: int | None = 2020,
         holdout_months: int = 6) -> dict:
    """Walk tennis_matches chronologically, calibrating predictions on
    the trailing ``holdout_months`` window via leakage-clean replay.

    Args:
        tours: ['atp'] | ['wta'] | ['atp','wta'] (default both)
        since_year: only replay matches from this year forward — older
                    matches don't change post-2020 Elo much, and this
                    speeds the replay 5x
        holdout_months: window at the end of the corpus to calibrate on

    Returns: {summary, bucket_view}; persists data/tennis_calibration.json.
    """
    from .tennis_db import get_conn
    from .tennis_elo import (
        _normalize_surface, _k_for, _decay_rd, _rd_after_match,
        _expected, INIT_RATING, INIT_RD, UPSET_BONUS,
    )

    conn = get_conn()
    tours = tours or ["atp", "wta"]

    # In-memory rating book keyed by (tour, player_id, surface).
    # Surface 'all' is tracked alongside the surface-specific entry so
    # predict_match's surface-fallback Bayesian blend can resolve.
    book: dict[tuple, dict] = {}

    def _get(tour: str, pid: int, surface: str, match_date: str) -> dict:
        key = (tour, int(pid), surface)
        entry = book.get(key)
        if entry is None:
            entry = {"rating": INIT_RATING, "rd": INIT_RD,
                     "matches": 0, "last_match": None}
            book[key] = entry
        entry["rd"] = _decay_rd(entry["rd"], entry["last_match"], match_date)
        return entry

    def _update(tour: str, pid: int, surface: str,
                rating: float, rd: float, match_date: str) -> None:
        key = (tour, int(pid), surface)
        entry = book.setdefault(key, {
            "rating": INIT_RATING, "rd": INIT_RD,
            "matches": 0, "last_match": None,
        })
        entry["rating"] = rating
        entry["rd"] = rd
        entry["matches"] += 1
        entry["last_match"] = match_date

    # Surface-shrinkage parameters mirror engine.tennis_predict
    MIN_SURFACE_MATCHES = 5
    SURFACE_SHRINK_K = 100

    def _eff_rating(tour: str, pid: int, surface: str,
                     match_date: str) -> tuple[float, float]:
        """Return (effective_rating, effective_rd) blending surface +
        all-surface ratings, matching tennis_predict._rating_block.
        Falls back to 'all' when surface volume < MIN_SURFACE_MATCHES."""
        surf_entry = _get(tour, pid, surface, match_date)
        all_entry = _get(tour, pid, "all", match_date)
        if surf_entry["matches"] >= MIN_SURFACE_MATCHES:
            w = surf_entry["matches"] / (surf_entry["matches"] + SURFACE_SHRINK_K)
            r = w * surf_entry["rating"] + (1.0 - w) * all_entry["rating"]
            rd = w * surf_entry["rd"] + (1.0 - w) * all_entry["rd"]
            return r, rd
        return all_entry["rating"], all_entry["rd"]

    # Determine holdout cutoff
    cutoff_date = (datetime.now() - timedelta(days=holdout_months * 30)
                   ).strftime("%Y-%m-%d")
    logger.info("holdout cutoff: %s (last %d months)",
                cutoff_date, holdout_months)

    # Walk chronologically. SELECT order matters — chronological replay
    # is the whole point. tourney_date is YYYY-MM-DD for 2026 rows.
    where = ["winner_id IS NOT NULL", "loser_id IS NOT NULL",
             "tourney_date IS NOT NULL", "score IS NOT NULL"]
    params: list = []
    if since_year is not None:
        where.append("substr(tourney_date, 1, 4) >= ?")
        params.append(str(since_year))
    in_clause = ",".join("?" for _ in tours)
    where.append(f"tour IN ({in_clause})")
    params.extend(tours)
    sql = ("SELECT tour, tourney_date, tourney_level, surface, "
           "       best_of, winner_id, loser_id, score, "
           "       w_svpt, w_1stWon, w_2ndWon, "
           "       l_svpt, l_1stWon, l_2ndWon "
           f"FROM tennis_matches WHERE {' AND '.join(where)} "
           "ORDER BY tourney_date, match_id")
    rows = conn.execute(sql, params).fetchall()
    logger.info("replay corpus: %d matches", len(rows))

    accum: dict[str, dict[tuple[float, float], list[tuple[float, int]]]] = {
        bt: defaultdict(list) for bt in (
            "ML", "SET_SPREAD", "TOTAL_SETS", "WIN_AT_LEAST_ONE_SET",
            "TOTAL_GAMES",
        )
    }
    # Load TOTAL_GAMES GBM directly + its val_rmse for σ. We bypass
    # tennis_dist_gbm.predict_total_games and build features inline
    # against an in-memory serve-rate aggregator — the published predict
    # function does 4 DB scans of tennis_matches per call (~470ms),
    # which makes a 20k-match walkforward take 2.5 hours. Inline state
    # cuts it to seconds.
    _tg_model = None
    _tg_sigma = 6.0
    import json as _json
    meta_path = (Path(__file__).resolve().parent.parent
                 / "data" / "models"
                 / "tennis_dist_gbm_total_games_latest.meta.json")
    model_path = (Path(__file__).resolve().parent.parent
                  / "data" / "models"
                  / "tennis_dist_gbm_total_games_latest.json")
    try:
        if model_path.exists() and meta_path.exists():
            import xgboost as xgb
            _tg_model = xgb.XGBRegressor()
            _tg_model.load_model(str(model_path))
            _tg_sigma = float(_json.loads(meta_path.read_text())
                              .get("val_rmse") or 6.0)
            logger.info("TOTAL_GAMES GBM loaded (σ=%.3f)", _tg_sigma)
    except Exception as _exc:
        logger.warning("TOTAL_GAMES GBM not loadable — skipping: %s", _exc)
        _tg_model = None

    # In-memory serve-rate state — per (tour, player_id, surface) and
    # per (tour, player_id, 'all'). Accumulates svpt/1st+2nd-won totals
    # per match so the running rate is queryable in O(1).
    _SURFACE_PRIORS = {"Hard": 0.608, "Clay": 0.591,
                       "Grass": 0.631, "Carpet": 0.634}
    serve_state: dict[tuple, dict] = {}

    def _serve_get(tour: str, pid: int, key_surf: str,
                    prior: float) -> dict:
        k = (tour, int(pid), key_surf)
        entry = serve_state.get(k)
        if entry is None:
            entry = {"svpt_total": 0, "won_total": 0, "prior": prior}
            serve_state[k] = entry
        return entry

    def _serve_rate(tour: str, pid: int, key_surf: str,
                     prior: float) -> float:
        entry = _serve_get(tour, pid, key_surf, prior)
        if entry["svpt_total"] >= 50:
            return entry["won_total"] / entry["svpt_total"]
        return prior
    n_predicted = 0
    n_skipped = 0
    n_pre = 0  # pre-holdout matches used only to warm the state

    for i, r in enumerate(rows):
        if i and i % 5000 == 0:
            logger.info("replay %d/%d (predicted=%d)",
                        i, len(rows), n_predicted)
        tour = r["tour"]
        date = r["tourney_date"]
        winner = int(r["winner_id"])
        loser = int(r["loser_id"])
        surface = _normalize_surface(r["surface"])
        best_of = int(r["best_of"] or 3)
        level = r["tourney_level"]

        in_holdout = date >= cutoff_date

        # ── Prediction phase (only for holdout matches) ──────────
        if in_holdout:
            # Canonicalize player order by ID so realized outcome
            # doesn't leak into which side is "p1".
            if winner < loser:
                p1_id, p2_id, p1_is_winner = winner, loser, True
            else:
                p1_id, p2_id, p1_is_winner = loser, winner, False

            r1, rd1 = _eff_rating(tour, p1_id, surface, date)
            r2, rd2 = _eff_rating(tour, p2_id, surface, date)
            # Same Glicko win prob the live predict_match uses
            p1_match_bo3 = _glicko_win_prob(r1, r2, rd1, rd2)
            # Convert BO3 → match prob for the actual best_of. BO5
            # extends BO3 by repeating the per-set prob in a 3-of-5.
            if best_of == 5:
                p_set = _per_set_prob_from_match_bo3(p1_match_bo3)
                p1_match = (p_set ** 3
                            + 3 * p_set ** 3 * (1 - p_set)
                            + 6 * p_set ** 3 * (1 - p_set) ** 2)
            else:
                p1_match = p1_match_bo3

            if not (0.0 < p1_match < 1.0):
                n_skipped += 1
            else:
                parsed = _parse_score(r["score"], best_of)
                if not parsed:
                    n_skipped += 1
                else:
                    # Remap parsed sides into model perspective
                    if p1_is_winner:
                        mp1_sets, mp2_sets = (parsed["winner_sets"],
                                              parsed["loser_sets"])
                    else:
                        mp1_sets, mp2_sets = (parsed["loser_sets"],
                                              parsed["winner_sets"])
                    p1_won_match = int(p1_is_winner)

                    # ── ML ──
                    if p1_match >= 0.5:
                        sp, sw = p1_match, p1_won_match
                    else:
                        sp, sw = 1.0 - p1_match, 1 - p1_won_match
                    bk = _bucket_for(sp)
                    if bk:
                        accum["ML"][bk].append((sp, sw))

                    # ── Per-set derived markets ──
                    p_set = _per_set_prob_from_match_bo3(p1_match_bo3)
                    if best_of == 3:
                        dist = _set_dist_bo3(p_set)
                        p_p1_sweep = dist[(2, 0)]
                        p_p2_sweep = dist[(0, 2)]
                        p1_swept = int(mp1_sets == 2 and mp2_sets == 0)
                        p2_swept = int(mp2_sets == 2 and mp1_sets == 0)
                        # WIN_AT_LEAST_ONE_SET (Yes side)
                        for side_prob, side_won in (
                            (1.0 - p_p2_sweep, int(mp1_sets >= 1)),
                            (1.0 - p_p1_sweep, int(mp2_sets >= 1)),
                        ):
                            if side_prob >= 0.5:
                                ssp, ssw = side_prob, side_won
                            else:
                                ssp, ssw = 1.0 - side_prob, 1 - side_won
                            bk = _bucket_for(ssp)
                            if bk:
                                accum["WIN_AT_LEAST_ONE_SET"][bk].append((ssp, ssw))
                        # SET_SPREAD ±1.5 ↔ wins 2-0
                        for side_prob, side_won in (
                            (p_p1_sweep, p1_swept),
                            (p_p2_sweep, p2_swept),
                        ):
                            if side_prob >= 0.5:
                                ssp, ssw = side_prob, side_won
                            else:
                                ssp, ssw = 1.0 - side_prob, 1 - side_won
                            bk = _bucket_for(ssp)
                            if bk:
                                accum["SET_SPREAD"][bk].append((ssp, ssw))
                        # TOTAL_SETS 2.5
                        p_over = dist[(2, 1)] + dist[(1, 2)]
                        over_won = int((mp1_sets + mp2_sets) > 2)
                        if p_over >= 0.5:
                            ssp, ssw = p_over, over_won
                        else:
                            ssp, ssw = 1.0 - p_over, 1 - over_won
                        bk = _bucket_for(ssp)
                        if bk:
                            accum["TOTAL_SETS"][bk].append((ssp, ssw))
                    else:  # BO5
                        dist = _set_dist_bo5(p_set)
                        p_p1_sweep = dist[(3, 0)]
                        p_p2_sweep = dist[(0, 3)]
                        p1_swept = int(mp1_sets == 3 and mp2_sets == 0)
                        p2_swept = int(mp2_sets == 3 and mp1_sets == 0)
                        for side_prob, side_won in (
                            (1.0 - p_p2_sweep, int(mp1_sets >= 1)),
                            (1.0 - p_p1_sweep, int(mp2_sets >= 1)),
                        ):
                            if side_prob >= 0.5:
                                ssp, ssw = side_prob, side_won
                            else:
                                ssp, ssw = 1.0 - side_prob, 1 - side_won
                            bk = _bucket_for(ssp)
                            if bk:
                                accum["WIN_AT_LEAST_ONE_SET"][bk].append((ssp, ssw))
                        for side_prob, side_won in (
                            (p_p1_sweep, p1_swept),
                            (p_p2_sweep, p2_swept),
                        ):
                            if side_prob >= 0.5:
                                ssp, ssw = side_prob, side_won
                            else:
                                ssp, ssw = 1.0 - side_prob, 1 - side_won
                            bk = _bucket_for(ssp)
                            if bk:
                                accum["SET_SPREAD"][bk].append((ssp, ssw))
                        # TOTAL_SETS 3.5
                        p_over = (dist[(3, 1)] + dist[(3, 2)]
                                  + dist[(1, 3)] + dist[(2, 3)])
                        over_won = int((mp1_sets + mp2_sets) > 3)
                        if p_over >= 0.5:
                            ssp, ssw = p_over, over_won
                        else:
                            ssp, ssw = 1.0 - p_over, 1 - over_won
                        bk = _bucket_for(ssp)
                        if bk:
                            accum["TOTAL_SETS"][bk].append((ssp, ssw))

                    # ── TOTAL_GAMES (distribution reliability) ──
                    # Build the GBM's 13-feature vector inline from the
                    # in-memory serve-rate state + the snapshot Elo we
                    # already have. Matches what tennis_dist_gbm's
                    # _build_feature_row produces (the broken Elo query
                    # in that function defaults to 1500 — we feed the
                    # actual snapshot Elo here, which is strictly more
                    # informative).
                    if _tg_model is not None:
                        prior = _SURFACE_PRIORS.get(surface, 0.60)
                        p1_serve_raw = _serve_rate(tour, p1_id, "all", prior)
                        p2_serve_raw = _serve_rate(tour, p2_id, "all", prior)
                        p1_serve_surf = _serve_rate(tour, p1_id, surface, prior)
                        p2_serve_surf = _serve_rate(tour, p2_id, surface, prior)
                        feat = [
                            float(best_of),
                            1.0 if surface == "Clay" else 0.0,
                            1.0 if surface == "Grass" else 0.0,
                            1.0 if surface == "Hard" else 0.0,
                            1.0 if tour == "atp" else 0.0,
                            abs(r1 - r2),
                            max(r1, r2),
                            p1_serve_raw, p2_serve_raw,
                            p1_serve_surf, p2_serve_surf,
                            (p1_serve_surf + p2_serve_surf) / 2.0,
                            abs(p1_serve_surf - p2_serve_surf),
                        ]
                        try:
                            mu = float(_tg_model.predict([feat])[0])
                        except Exception:
                            mu = None
                        actual_total = parsed["total_games"]
                        if mu is not None and _tg_sigma > 0:
                            for z_off in _Z_OFFSETS:
                                line = mu + z_off * _tg_sigma
                                p_over = 1.0 - _normal_cdf(z_off)
                                over_won = int(actual_total > line)
                                if p_over >= 0.5:
                                    ssp, ssw = p_over, over_won
                                else:
                                    ssp, ssw = 1.0 - p_over, 1 - over_won
                                bk = _bucket_for(ssp)
                                if bk:
                                    accum["TOTAL_GAMES"][bk].append((ssp, ssw))
                    n_predicted += 1
        else:
            n_pre += 1

        # ── State update phase (for every match, holdout or not) ──
        k = _k_for(level)
        for surf in (surface, "all"):
            w_state = _get(tour, winner, surf, date)
            l_state = _get(tour, loser, surf, date)
            r_w = w_state["rating"]
            r_l = l_state["rating"]
            exp_w = _expected(r_w, r_l)
            delta_w = k * (1.0 - exp_w)
            if r_w < r_l:
                delta_w *= UPSET_BONUS
            new_rw = r_w + delta_w
            new_rl = r_l - delta_w
            _update(tour, winner, surf, new_rw,
                    _rd_after_match(w_state["rd"]), date)
            _update(tour, loser, surf, new_rl,
                    _rd_after_match(l_state["rd"]), date)

        # Serve-rate state update — accumulate svpt and won counts per
        # player per (surface, 'all'). When stats columns are NULL (lots
        # of historical Sackmann rows are), skip — running rate falls
        # back to the surface prior at query time.
        w_svpt = r["w_svpt"]
        l_svpt = r["l_svpt"]
        if w_svpt and l_svpt:
            prior = _SURFACE_PRIORS.get(surface, 0.60)
            w_won = (r["w_1stWon"] or 0) + (r["w_2ndWon"] or 0)
            l_won = (r["l_1stWon"] or 0) + (r["l_2ndWon"] or 0)
            for surf_key in (surface, "all"):
                we = _serve_get(tour, winner, surf_key, prior)
                we["svpt_total"] += int(w_svpt)
                we["won_total"] += int(w_won)
                le = _serve_get(tour, loser, surf_key, prior)
                le["svpt_total"] += int(l_svpt)
                le["won_total"] += int(l_won)

    # ── Aggregate ──
    def _agg(buckets) -> list[dict]:
        rows_out = []
        for lo, hi in _BUCKETS:
            samples = buckets.get((lo, hi), [])
            n = len(samples)
            wins = sum(s[1] for s in samples)
            avg_pred = (sum(s[0] for s in samples) / n) if n else None
            realized = (wins / n) if n else None
            rows_out.append({
                "bucket": [lo, hi],
                "n": n,
                "avg_pred": round(avg_pred, 4) if avg_pred is not None else None,
                "realized_wr": round(realized, 4) if realized is not None else None,
            })
        return rows_out

    cal_table = {bt: _agg(accum[bt]) for bt in accum}
    summary_view = []
    for market, rows_out in cal_table.items():
        for row in rows_out:
            if row["n"] > 0:
                summary_view.append({
                    "market": market,
                    "bucket": row["bucket"],
                    "n": row["n"],
                    "avg_pred": row["avg_pred"],
                    "realized_wr": row["realized_wr"],
                })

    out = {
        "sport": "tennis",
        "method": "chronological_elo_replay",
        "holdout_cutoff": cutoff_date,
        "since_year": since_year,
        "tours_seeded": tours,
        "n_pre_holdout": n_pre,
        "n_predicted": n_predicted,
        "n_skipped": n_skipped,
        "buckets": cal_table,
        "deferred": [],
        "fitted_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    _OUT.parent.mkdir(parents=True, exist_ok=True)
    _OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    logger.info("tennis calibration seeded: n_predicted=%d, n_pre=%d, "
                "n_skipped=%d", n_predicted, n_pre, n_skipped)
    return {"summary": out, "bucket_view": summary_view}


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.tennis_calibrate")
    ap.add_argument("--tour", choices=("atp", "wta"), default=None)
    ap.add_argument("--since-year", type=int, default=2020,
                    help="lower-bound the replay corpus by year")
    ap.add_argument("--holdout-months", type=int, default=6,
                    help="trailing window to calibrate on")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    tours = [args.tour] if args.tour else None
    res = seed(tours=tours,
               since_year=args.since_year,
               holdout_months=args.holdout_months)
    print(json.dumps(res["bucket_view"], indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
