"""Field-level predictor for golf tournaments.

Architecture mirrors ``engine.motorsports._predict``:
  1. Per-player skill rating from historical finishes.
  2. Softmax across the active field → P(winner).
  3. Monte Carlo over per-player score distributions → P(top-N).

Skill rating is the mean score-to-par the player posted over their
last ``LOOKBACK_TOURNAMENTS`` events, regressed toward field-mean to
shrink small-sample players. Lower (more negative) score-to-par means
stronger.

A position-finish probability for "top N" is computed by simulating
each player's tournament score as a Normal(skill, sigma) where sigma
is the residual std across their finishes (or a field-wide default for
players with <5 historical finishes). N simulations rank the field;
P(top_K) for a player = fraction of sims in which they ranked ≤ K.
"""
from __future__ import annotations

import json
import logging
import math
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np

from ._db import get_conn

logger = logging.getLogger(__name__)


# ── Tunables ─────────────────────────────────────────────────

# Historical window per player. PGA Tour ~25 events/year; 20 covers
# ~10 months of form. Older finishes are noise on a player whose game
# has evolved (swing change, injury, etc) but golf form turns over more
# slowly than I initially thought — bumped from 15 → 20 to capture more
# major-cycle data points.
LOOKBACK_TOURNAMENTS = 20

# Minimum historical events before we trust a player's mean. Below
# this, regress hard toward field-mean.
MIN_TOURNAMENTS_TRUSTED = 8

# Recency half-life in days — exponential decay weight on older results.
# Tightened from 180 → 90 earlier on the intuition that "golf form
# shifts on a ~3-month cycle"; backtest on 20 recent PGA tournaments
# 2026-05-18 disproved this: tightening hurt Brier on every Top-N
# market and MAKE_CUT. Half-life 365 was Brier-optimal; 180 is the
# pragmatic compromise that still gives weeks-old form a chance to
# fade.
#
# Why longer is better here: SG_FORM_WEIGHT (DataGolf SG snapshot)
# already captures recent form at weight 0.3. The historical-mean
# component should be the long-term stability anchor; double-counting
# recency by tightening this hurts calibration on Top-N markets where
# consistency over many starts matters more than week-to-week noise.
RECENCY_HALF_LIFE_DAYS = 180

# Default per-player residual std when historical sample is too thin
# to fit one. PGA Tour score-to-par residuals empirically sit around
# 5-7 strokes per tournament; 6 is a reasonable prior.
DEFAULT_SIGMA = 6.0

# Field-mean prior — pulled in for shrinkage. Recomputed per call from
# the historical pool, but if the pool is empty we fall back to 0 (par).
DEFAULT_FIELD_MEAN = 0.0

# Softmax temperature for the winner probability. PGA Tour outrights
# rarely price the favorite higher than ~10-12% so the softmax has to
# be flat. Tightened from 4.0 → 3.0 to widen the gap between the
# top-skill and field-mean players (current 3.35% favorite price is
# too compressed vs HR's ~14% Scheffler).
SOFTMAX_TEMP = 3.0

# Reference (typical) PGA field size for softmax-temperature scaling.
# When the actual field is smaller (e.g., opposite-field events ~70
# players), the softmax concentrates more mass on the top names than
# the empirical winners support; we widen temperature by
# sqrt(typical / actual) to flatten the head of the distribution.
# Calibrated to the PGA average of 144 finishers per event.
FIELD_SIZE_TYPICAL = 144

# Minimum field size we'll apply the scaling to. Below ~30 the model
# is in pure noise territory and we cap the temperature scaler.
FIELD_SIZE_FLOOR = 30

# Monte Carlo sim count for top-N derivations. 5000 sims gives ~1pp
# precision on top-10 probabilities which is more than enough at the
# current cold-start data depth.
N_SIMS = 5000

# Cut-line model. PGA Tour rule: low 65 + ties make the weekend.
# We approximate via top-N where N varies by field size. Made-cut
# probability = P(player rank after 2 sims ≤ MADE_CUT_RANK).
MADE_CUT_RANK = 70

# OWGR prior strength — when a player has thin history, we anchor their
# skill on the OWGR-derived prior instead of plain field-mean. Mapped
# via -OWGR_SCALE * log(1 + points_avg) + OWGR_OFFSET so that:
#   rank 1   (~14 points) → ~-2.7 strokes (elite skill)
#   rank 100 (~3 points)  → ~-1.2 strokes
#   rank 300 (~0.5 points)→ ~+0.0 strokes
# Calibrated to match the empirical mean course-adjusted score-to-par
# of pre-existing players at those ranks. Floor enforced so we never
# attribute superhuman skill (≤ -3.5).
OWGR_SCALE = 1.2
OWGR_OFFSET = 0.5
OWGR_FLOOR = -3.5

# DataGolf SG rolling-form adjustment. SG-per-round (event_cum /
# rounds_completed) multiplied by this weight is *subtracted* from
# skill_mean (positive SG = better = lower score-to-par). Damped to
# 0.3 because a single event's SG is high variance — half-cycle of
# the event_cum is from one good round.
SG_FORM_WEIGHT = 0.3

# ── Course history at venue ──
# Players have venue-specific edges (Russell Henley at Mayakoba, certain
# guys at Phoenix). The skill mean is venue-agnostic by default; this
# adds a course-fit delta = weighted-average of player's prior
# course-adjusted scores at THIS tournament_id, blended in proportional
# to sample depth.
#
# Weight applied AS A FRACTION of the total skill estimate. A player
# with 5 prior starts at this venue gets ~0.3 of their venue mean +
# 0.7 of their general skill; a player with no prior starts here gets
# 1.0 general skill (no venue signal).
#
# Backtest 2026-05-18 (20 PGA holdout tournaments) showed
# COURSE_FIT_MAX_WEIGHT = 0.30 made T20 + Top-10 + MAKE_CUT worse;
# 0.10 hit the sweet spot — small positive on T20 (-0.15%) and
# MAKE_CUT (-0.36%), essentially flat elsewhere. Course-fit is a
# weak signal at this data depth; cap is conservative.
COURSE_FIT_MAX_WEIGHT = 0.10
COURSE_FIT_TAU = 3.0    # Bayesian shrinkage — n_venue_starts / (n + tau)
COURSE_FIT_RECENCY_HALF_LIFE_DAYS = 730   # Course fit decays slower (2y half-life)
                                            # than general form — venue-fit
                                            # is more stable than week-to-week.

# Require at least this many rounds in the SG snapshot before we trust
# the rolling form. 1-round samples are too noisy to nudge skill.
SG_FORM_MIN_ROUNDS = 2


# ── Skill rating ─────────────────────────────────────────────

def _player_history(conn: sqlite3.Connection, player_id: int,
                    before_date: str | None = None
                    ) -> list[tuple[str, float, bool, str]]:
    """Per-player history: ``[(tournament_end_date, adjusted_score,
    made_cut, tournament_id), ...]`` sorted oldest → newest.

    Score is **course-adjusted**: raw score_to_par minus the field's
    mean score_to_par at that tournament. This corrects for course
    difficulty — a player who shot -10 at a low-scoring U.S. Open
    rates higher than one who shot -10 at the easy-setup Sentry. The
    relative metric also approximates strokes-gained-total in lieu of
    a paid feed.

    Caller can clamp to events ending strictly before ``before_date``
    so the predictor doesn't peek at the current tournament's score."""
    sql = (
        "SELECT t.id AS tid, t.end_date, f.score_to_par, f.made_cut, "
        "       f.withdrew, f.disqualified, t.status "
        "FROM field_entries f "
        "JOIN tournaments t ON t.id = f.tournament_id "
        "WHERE f.player_id = ? "
        "  AND t.status IN ('final', 'in') "
    )
    params: list = [int(player_id)]
    if before_date:
        sql += "  AND t.end_date < ? "
        params.append(before_date)
    sql += "ORDER BY t.end_date ASC"
    out = []
    for r in conn.execute(sql, params).fetchall():
        if r["withdrew"] or r["disqualified"]:
            continue
        if r["score_to_par"] is None:
            continue
        # Course-adjust: subtract this tournament's field-mean score
        # from the player's score. Cached so we don't re-query 156
        # field rows per player per tournament.
        field_mean = _tournament_field_mean(conn, r["tid"])
        adj = float(r["score_to_par"]) - field_mean
        out.append((r["end_date"], adj,
                     bool(r["made_cut"]) if r["made_cut"] is not None
                     else True,
                     r["tid"]))
    return out


# Cache the field-mean per tournament — recomputed once, reused across
# every player history pull for the same predictor run. Cleared on
# every fit_skill call by re-instantiating; module-level is fine because
# tournaments don't change once final.
_TOURNAMENT_FIELD_MEAN_CACHE: dict[str, float] = {}


def _tournament_field_mean(conn: sqlite3.Connection,
                            tournament_id: str) -> float:
    """Mean score-to-par across all players who finished a tournament.
    Excludes WD/DQ. Cached per-tournament (tournaments are immutable
    once final)."""
    if tournament_id in _TOURNAMENT_FIELD_MEAN_CACHE:
        return _TOURNAMENT_FIELD_MEAN_CACHE[tournament_id]
    row = conn.execute(
        "SELECT AVG(score_to_par) AS m FROM field_entries "
        "WHERE tournament_id = ? "
        "  AND score_to_par IS NOT NULL "
        "  AND withdrew = 0 AND disqualified = 0",
        (tournament_id,),
    ).fetchone()
    val = float(row["m"]) if row and row["m"] is not None else 0.0
    _TOURNAMENT_FIELD_MEAN_CACHE[tournament_id] = val
    return val


def _recency_weight(end_date: str, reference_date: datetime) -> float:
    """Exponential decay vs reference date."""
    try:
        dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            from datetime import timezone as _tz
            dt = dt.replace(tzinfo=_tz.utc)
        days_old = max(0.0, (reference_date - dt).total_seconds() / 86400.0)
    except (TypeError, ValueError):
        return 0.5
    return math.pow(0.5, days_old / RECENCY_HALF_LIFE_DAYS)


def _owgr_priors(conn: sqlite3.Connection) -> dict[int, float]:
    """Map ``{player_id: skill_anchor_strokes}`` from OWGR points_average.

    OWGR is the closest thing we have to a cross-tour ground-truth skill
    prior — it aggregates results across PGA, DP World, Asian, etc. into
    one weighted points average, refreshed weekly. We use a log mapping
    because points are heavily skewed (Scheffler ~14, rank-300 ~0.5)."""
    out: dict[int, float] = {}
    for r in conn.execute(
        "SELECT player_id, points_average FROM owgr_rankings"
    ).fetchall():
        pts = r["points_average"]
        if pts is None or pts < 0:
            continue
        anchor = -OWGR_SCALE * math.log(1.0 + float(pts)) + OWGR_OFFSET
        out[int(r["player_id"])] = max(OWGR_FLOOR, anchor)
    return out


def _course_fit(conn: sqlite3.Connection, tournament_id: str,
                 before_date: str | None,
                 reference_date: datetime) -> dict[int, tuple[float, int]]:
    """Per-player venue-fit at THIS tournament. Returns
    ``{player_id: (mean_adj_score, n_starts)}`` from prior runnings of
    the same tournament_id only (not the same course — the
    tournament_id captures the venue + setup pair).

    Uses recency-decay with a longer half-life than form because
    course-fit is a more stable trait than week-to-week swing form."""
    if not tournament_id:
        return {}
    # Find prior tournaments with the same NAME or same course as this
    # tournament. ESPN ships a new tournament_id per year so we can't
    # join on raw id — match by ``name`` (trimming the year suffix
    # if present).
    target = conn.execute(
        "SELECT id, name, course FROM tournaments WHERE id = ?",
        (str(tournament_id),),
    ).fetchone()
    if not target:
        return {}
    name = (target["name"] or "").strip()
    course = (target["course"] or "").strip()
    # Find all prior runnings of this tournament. Prefer course match
    # (more specific) when available, fall back to name match.
    prior_ids: list[str] = []
    if course:
        for r in conn.execute(
            "SELECT id FROM tournaments "
            "WHERE course = ? AND id != ? "
            "  AND status IN ('final', 'in')"
            "  AND end_date < COALESCE(?, '9999')",
            (course, str(tournament_id), before_date),
        ):
            prior_ids.append(r["id"])
    if not prior_ids and name:
        for r in conn.execute(
            "SELECT id FROM tournaments "
            "WHERE name = ? AND id != ? "
            "  AND status IN ('final', 'in')"
            "  AND end_date < COALESCE(?, '9999')",
            (name, str(tournament_id), before_date),
        ):
            prior_ids.append(r["id"])
    if not prior_ids:
        return {}
    placeholders = ",".join("?" * len(prior_ids))
    rows = conn.execute(
        f"SELECT f.player_id, f.score_to_par, t.id AS tid, t.end_date "
        f"FROM field_entries f "
        f"JOIN tournaments t ON t.id = f.tournament_id "
        f"WHERE f.tournament_id IN ({placeholders}) "
        f"  AND f.withdrew = 0 AND f.disqualified = 0 "
        f"  AND f.score_to_par IS NOT NULL",
        prior_ids,
    ).fetchall()
    # Accumulate weighted course-adjusted score per player
    sums: dict[int, list[float]] = {}
    weights: dict[int, list[float]] = {}
    for r in rows:
        pid = int(r["player_id"])
        tid = r["tid"]
        field_mean = _tournament_field_mean(conn, tid)
        adj = float(r["score_to_par"]) - field_mean
        # Longer half-life — venue-fit decays slower than rolling form.
        try:
            dt = datetime.fromisoformat(
                (r["end_date"] or "").replace("Z", "+00:00"))
            if dt.tzinfo is None:
                from datetime import timezone as _tz
                dt = dt.replace(tzinfo=_tz.utc)
            days_old = max(0.0, (reference_date - dt).total_seconds() / 86400.0)
        except (TypeError, ValueError):
            continue
        w = math.pow(0.5, days_old / COURSE_FIT_RECENCY_HALF_LIFE_DAYS)
        sums.setdefault(pid, []).append(w * adj)
        weights.setdefault(pid, []).append(w)
    out: dict[int, tuple[float, int]] = {}
    for pid in sums:
        ws = sum(weights[pid])
        if ws <= 0:
            continue
        mean_adj = sum(sums[pid]) / ws
        out[pid] = (mean_adj, len(sums[pid]))
    return out


def _sg_form(conn: sqlite3.Connection,
              course_name: str | None = None) -> dict[int, float]:
    """Map ``{player_id: sg_per_round}`` from the most-recent DataGolf
    SG snapshot. SG values are field- + course-adjusted strokes-gained,
    so subtracting them from skill_mean is dimensionally correct: each
    +1 SG/round = ~1 stroke better than field-mean over a round.

    When ``course_name`` is provided, we recompute the per-round SG via
    a course-demand-weighted sum of the four SG categories (OTT/APP/
    ARG/PUTT) instead of using ``sg_total``. This lets a course that
    rewards approach play (e.g. Augusta) weight a player's SG_APP
    higher than a course that rewards driving (e.g. Torrey South).
    Weights come from ``_course_profiles.get_demand_weights`` —
    archetype-based today, data-fit in a future iteration.

    Only returns rows with rounds_completed ≥ SG_FORM_MIN_ROUNDS — a
    single round is too noisy to anchor on. Players with NULL category
    breakdowns fall through to ``sg_total`` (legacy ingest rows)."""
    from ._course_profiles import get_demand_weights, weighted_sg
    weights = get_demand_weights(course_name) if course_name else None
    out: dict[int, float] = {}
    for r in conn.execute(
        "SELECT player_id, sg_total, sg_ott, sg_app, sg_arg, sg_putt, "
        "       rounds_completed "
        "FROM player_sg WHERE rounds_completed IS NOT NULL "
        "  AND rounds_completed >= ?",
        (SG_FORM_MIN_ROUNDS,),
    ).fetchall():
        rounds = int(r["rounds_completed"])
        if rounds <= 0:
            continue
        # Prefer the course-weighted sum when all four categories are
        # present + we have a course profile to weight by.
        chosen = None
        if weights is not None and r["sg_ott"] is not None and \
                r["sg_app"] is not None and r["sg_arg"] is not None and \
                r["sg_putt"] is not None:
            chosen = weighted_sg(
                float(r["sg_ott"]), float(r["sg_app"]),
                float(r["sg_arg"]), float(r["sg_putt"]),
                weights,
            )
        if chosen is None and r["sg_total"] is not None:
            chosen = float(r["sg_total"])
        if chosen is None:
            continue
        out[int(r["player_id"])] = chosen / rounds
    return out


def fit_skill(tour: str, tournament_id: str,
               reference_date: datetime | None = None) -> dict:
    """Build per-player ``{player_id: {mean, sigma, n}}`` for the active
    field of ``tournament_id``. Historical events filtered to those that
    ended before the tournament starts (no look-ahead).

    Field-mean shrinkage is applied so players with <8 historical events
    don't dominate the softmax via one good week.

    Major-vs-regular weighting: when the target tournament is a major,
    rows from prior majors get a 2x weight multiplier on top of the
    recency-decay. Captures the empirical "Scheffler / Schauffele
    elevate at majors, some weekly-grinders fold" effect."""
    conn = get_conn(tour)
    tourney = conn.execute(
        "SELECT id, start_date, is_major, course "
        "FROM tournaments WHERE id = ?",
        (tournament_id,),
    ).fetchone()
    if not tourney:
        return {}
    cutoff_date = tourney["start_date"]
    target_is_major = bool(tourney["is_major"])
    if reference_date is None:
        try:
            reference_date = datetime.fromisoformat(
                cutoff_date.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            reference_date = datetime.utcnow().replace(
                tzinfo=__import__("datetime").timezone.utc)

    # Pull every player in this event's field.
    field_ids = [r["player_id"] for r in conn.execute(
        "SELECT player_id FROM field_entries WHERE tournament_id = ?",
        (tournament_id,),
    ).fetchall()]
    if not field_ids:
        return {}

    # Pre-load is_major flag per tournament_id we'll look up while
    # walking each player's history. One bulk query beats N+1 per
    # history row.
    is_major_lookup = {
        r["id"]: bool(r["is_major"])
        for r in conn.execute(
            "SELECT id, is_major FROM tournaments"
        ).fetchall()
    }

    # Field-mean prior — weighted average of course-adjusted scores
    # across every player's history. Since scores are already
    # course-adjusted (player minus field-mean per tournament), the
    # pool's center sits near 0 by construction; this prior captures
    # cohort drift (current generation's overall skill vs the rolling
    # field).
    #
    # Major-context weighting: when target is a major, rows from past
    # majors get 2x weight. Multiplier applied to recency-weight so
    # the decay still applies (a 5-year-old major isn't worth more
    # than a recent one — both get the multiplier × their recency).
    weighted_sum = 0.0
    weight_total = 0.0
    raw_history: dict[int, list[tuple[str, float, float]]] = {}  # (date, score, weight)
    for pid in field_ids:
        hist = _player_history(conn, pid, before_date=cutoff_date)
        hist = hist[-LOOKBACK_TOURNAMENTS:]
        hist_w: list[tuple[str, float, float]] = []
        for d, s, _mc, tid in hist:
            recency = _recency_weight(d, reference_date)
            # Major context: 2x weight for major rows when predicting
            # a major; 1x otherwise. Players who elevate at majors
            # (or fold) get their major track record amplified.
            major_mult = 2.0 if (target_is_major
                                   and is_major_lookup.get(tid, False)) else 1.0
            w = recency * major_mult
            hist_w.append((d, s, w))
            weighted_sum += w * s
            weight_total += w
        raw_history[pid] = hist_w
    field_mean = weighted_sum / weight_total if weight_total > 0 else DEFAULT_FIELD_MEAN

    # External skill priors: OWGR ranking → per-player anchor, DataGolf
    # SG → per-player rolling-form delta. Both keyed on our internal
    # player_id (the ingest layer handled name-resolution). When a
    # player has neither external signal we fall back to field_mean.
    owgr_priors = _owgr_priors(conn)
    # Pass the target course so per-category SG gets weighted by the
    # course's demand profile (archetype-based today, data-fit later
    # via ``_course_profiles``). Approach-heavy courses lean on SG_APP,
    # putting-heavy courses lean on SG_PUTT, etc.
    sg_form = _sg_form(conn, course_name=tourney["course"])

    # Course-fit: per-player past performance at THIS tournament/course.
    # ``{player_id: (mean_adj_score, n_starts)}``. Recency-decayed with
    # a 2-year half-life since venue-fit is more stable than week-to-week
    # form.
    course_fit = _course_fit(conn, tournament_id, cutoff_date, reference_date)

    out: dict[int, dict] = {}
    for pid, hist in raw_history.items():
        # Per-player prior: OWGR anchor when available, else field_mean.
        # This is what we shrink the score-to-par mean toward for thin-
        # history players — the right answer for a Korn Ferry crossover
        # is "OWGR thinks they're 220th, regress them to that skill", not
        # "regress them to field-mean (0)".
        player_prior = owgr_priors.get(pid, field_mean)
        if not hist:
            # No history at all → live entirely on the external prior.
            sg_adj = -SG_FORM_WEIGHT * sg_form.get(pid, 0.0)
            out[pid] = {
                "mean": player_prior + sg_adj,
                "sigma": DEFAULT_SIGMA,
                "n": 0,
                "raw_mean": player_prior,
                "weight_total": 0.0,
                "owgr_prior": owgr_priors.get(pid),
                "sg_form": sg_form.get(pid),
            }
            continue
        # Weighted mean + std using the pre-computed (recency × major)
        # weights from the field-mean pass above.
        ws = 0.0
        ws_x = 0.0
        ws_x2 = 0.0
        for d, s, w in hist:
            ws += w
            ws_x += w * s
            ws_x2 += w * s * s
        raw_mean = ws_x / ws if ws > 0 else field_mean
        raw_var = max(0.0, (ws_x2 / ws) - (raw_mean ** 2)) if ws > 0 else DEFAULT_SIGMA ** 2
        # Shrink mean toward the player's prior. Weight = n / (n + tau).
        # Prior is OWGR-derived when available, else field_mean. Thin-
        # history players get pulled hard toward their OWGR anchor —
        # so an OWGR top-50 player with 2 PGA starts doesn't get
        # mistakenly priced at field_mean.
        n = len(hist)
        tau = MIN_TOURNAMENTS_TRUSTED
        shrink_w = n / (n + tau)
        mean = shrink_w * raw_mean + (1 - shrink_w) * player_prior
        # DataGolf SG rolling-form: subtract (positive SG = better).
        # Layered on top of the shrunk mean so it shifts both deep- and
        # thin-history players' projections symmetrically.
        sg_per_round = sg_form.get(pid)
        if sg_per_round is not None:
            mean = mean - SG_FORM_WEIGHT * sg_per_round
        # Course-fit at this venue. Blend mean toward course-fit-adjusted
        # mean proportional to sqrt(n_venue_starts) / (sqrt(n) + tau). A
        # player with 5 prior starts at this event gets ~50% weight on
        # venue-fit; thin samples stay close to the venue-agnostic mean.
        course_fit_entry = course_fit.get(pid)
        course_fit_weight = 0.0
        if course_fit_entry:
            cf_mean, cf_n = course_fit_entry
            cf_n_eff = math.sqrt(cf_n)
            course_fit_weight = min(
                COURSE_FIT_MAX_WEIGHT,
                cf_n_eff / (cf_n_eff + COURSE_FIT_TAU),
            )
            # course-fit delta is the difference between venue-only mean
            # and the player's general mean. Subtract a fraction of that
            # delta to nudge the prediction toward venue-fit while keeping
            # general skill the anchor.
            cf_delta = cf_mean - raw_mean
            mean = mean + course_fit_weight * cf_delta
        # Sigma: use the player's own residual when history is deep
        # enough (n ≥ 12), blend toward DEFAULT_SIGMA for thinner
        # samples. Mirrors the shrinkage on the mean but with a higher
        # threshold (variance needs more samples to stabilize than the
        # mean does).
        if n >= 12:
            sigma = math.sqrt(raw_var)
        else:
            sigma_shrink = n / (n + 2 * tau)   # heavier shrink than mean
            sigma = math.sqrt(sigma_shrink * raw_var
                              + (1 - sigma_shrink) * DEFAULT_SIGMA ** 2)
        sigma = max(sigma, 2.0)   # Floor so the softmax stays differentiable
        out[pid] = {
            "mean": mean,
            "sigma": sigma,
            "n": n,
            "raw_mean": raw_mean,
            "weight_total": ws,
            "field_mean": field_mean,
            "owgr_prior": owgr_priors.get(pid),
            "sg_form": sg_per_round,
            "course_fit_mean": course_fit_entry[0] if course_fit_entry else None,
            "course_fit_n": course_fit_entry[1] if course_fit_entry else 0,
            "course_fit_weight": course_fit_weight,
        }
    return out


# ── Field probabilities ──────────────────────────────────────

def predict_field(tour: str, tournament_id: str,
                   *, n_sims: int = N_SIMS,
                   seed: int | None = None) -> dict:
    """Run the softmax + Monte Carlo and return per-player probabilities.

    Returns ``{player_id: {p_winner, p_top_5, p_top_10, p_top_20,
                            p_made_cut, mean_proj}}``."""
    skills = fit_skill(tour, tournament_id)
    if not skills:
        return {}
    player_ids = list(skills.keys())
    means = np.array([skills[pid]["mean"] for pid in player_ids])
    sigmas = np.array([skills[pid]["sigma"] for pid in player_ids])

    # Softmax winner prob — uses *negative* mean (lower is better)
    # divided by temperature. Temperature absorbs the rough scale of
    # PGA Tour score-to-par spreads (~10-15 strokes top-to-bottom).
    #
    # Bayesian field-size shrinkage: small fields (opposite events,
    # smaller LPGA/Korn Ferry slates) over-concentrate softmax mass on
    # the top names because there's less competition. Widen temperature
    # by sqrt(typical / actual_field) so a 70-player field gets
    # ~1.43× flatter softmax than a 144-player major. Capped at field
    # floor so the scaler doesn't explode on tiny fields.
    field_n = max(FIELD_SIZE_FLOOR, len(player_ids))
    temp_scale = math.sqrt(FIELD_SIZE_TYPICAL / field_n)
    eff_temp = SOFTMAX_TEMP * temp_scale
    z = -means / eff_temp
    z -= z.max()       # numerical stability
    exp_z = np.exp(z)
    p_winner = exp_z / exp_z.sum()

    # Monte Carlo for top-K — each sim draws score ~ N(mean, sigma)
    # per player; rank ascending; count top-K appearances.
    rng = np.random.default_rng(seed if seed is not None else
                                 hash(tournament_id) & 0xFFFFFFFF)
    samples = rng.normal(means[:, None], sigmas[:, None], size=(len(player_ids), n_sims))
    # Rank within each column (sim). argsort ascending → best=0.
    ranks = samples.argsort(axis=0).argsort(axis=0)  # rank[player,sim]
    p_top_5 = (ranks < 5).mean(axis=1)
    p_top_10 = (ranks < 10).mean(axis=1)
    p_top_20 = (ranks < 20).mean(axis=1)
    # Made-cut roughly = finish in top ~70 + ties (T-65 rule). We
    # approximate via top-70.
    p_made_cut = (ranks < 70).mean(axis=1)
    mean_proj = samples.mean(axis=1)

    out: dict[int, dict] = {}
    for i, pid in enumerate(player_ids):
        out[pid] = {
            "p_winner": float(p_winner[i]),
            "p_top_5": float(p_top_5[i]),
            "p_top_10": float(p_top_10[i]),
            "p_top_20": float(p_top_20[i]),
            "p_made_cut": float(p_made_cut[i]),
            "mean_proj": float(mean_proj[i]),
            "skill_mean": skills[pid]["mean"],
            "skill_sigma": skills[pid]["sigma"],
            "skill_n": skills[pid]["n"],
        }
    return out
