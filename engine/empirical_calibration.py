"""Probability calibration from settled tracker picks.

The factor + MC + GBM ensemble is systemically over-confident at the
upper tail. The 285-pick MLB tracker (May 2026) showed:
   ML  60-70% predicted bucket -> 47.7% real (-17pp)
   ML  70-80% predicted bucket -> 57.1% real (-18pp)
   ML  80-90% predicted bucket -> 51.5% real (-33pp)
i.e. the model claims it's 80%-confident but reality is a coin flip.

This module Bayesian-calibrates raw model probabilities against
observed outcomes. Each (sport, bet_type, bucket, ...) cell holds a
Beta posterior over the true win rate. Posterior mean replaces the
raw prob. Always returns a value — never passthrough — because the
prior is centered on the bucket midpoint with effective sample size
PRIOR_N₀, so even an empty cell returns a sensible estimate.

Hierarchical priors: each finer cell (granular > fav-split / dir-split
> coarse > root) uses the next-coarser cell's posterior mean as ITS
prior center. So a 0.6-bucket fav-split with thin data borrows
strength from the (bet_type, bucket) coarse posterior, which in turn
borrows from the bucket midpoint root.

Why this replaces the old empirical_calibration.MIN_BUCKET_N approach:
- Old: bucket with n<10 → passthrough (raw prob unchanged). Created
  cliffs where picks at edge of bucket boundary swung wildly.
- New: every cell has a posterior. Small samples shrink toward the
  parent prior automatically. As n grows, posterior approaches
  empirical. Smooth handoff, no cliffs.

Public API (unchanged):
   calibrate(bet_type, raw_prob, sport, ...) -> calibrated_prob
   refresh_calibration(sport)               -> dict (called nightly)
   refresh_all_sports()                     -> dict
   snapshot(sport)                          -> dict (diag)

Removed:
   MIN_BUCKET_N, SHRINKAGE_TARGET_N — superseded by PRIOR_N0
"""

from __future__ import annotations

import logging
import threading
from collections import defaultdict
from typing import Iterable

logger = logging.getLogger(__name__)

# Effective prior sample size for each Bayesian cell. With PRIOR_N0=10:
#   n = 0   → posterior is pure prior (bucket-midpoint estimate)
#   n = 10  → posterior is 50/50 prior+empirical
#   n = 100 → posterior is ~91% empirical
#   n = 1000 → posterior is ~99% empirical
# Smaller PRIOR_N0 = quicker shift toward empirical. 10 is the
# textbook weakly-informative choice for binomials. Tuneable later
# per-sport if backtest evidence supports it.
PRIOR_N0 = 10

# Isotonic hybrid — 2026-07-03. Cells with at least this many samples
# get a monotonic isotonic-regression mapping in addition to the
# Bayesian posterior; thinner cells stay on the bucket-midpoint
# fallback. The dry-run at scripts/dryrun_isotonic_calibration
# confirmed the bucketed pipeline was non-monotonic (raw 0.65 → 0.53,
# raw 0.70 → 0.81 on tennis ML) which let overconfident picks slip
# through the belief gate.
#
# 2026-07-07: bumped from 20 → 100 after WNBA regression. Isotonic on
# 20-75 samples produced degenerate step functions clamped to
# ISOTONIC_Y_MIN=0.05 for entire raw-prob swaths (WNBA TOTAL Over 175.5
# raw=0.66 → cal=0.05, killing every full-game pick). 100 gives the
# pool-adjacent-violators algorithm enough evidence per knee to
# produce a smooth curve. Sports below the threshold fall back to the
# Bayesian Beta-Binomial bucket or framework walk-forward — both stay
# monotonic even on thin data.
ISOTONIC_MIN_N = 100
# Guard bands prevent isotonic from saturating to 0.0/1.0 at the tails
# (thin sample + streak → boundary lock-in). Matches the 5%-95% floor/
# ceiling used in Platt scaling literature.
ISOTONIC_Y_MIN = 0.05
ISOTONIC_Y_MAX = 0.95
# Degenerate-curve guard: refuse to persist a fitted isotonic whose
# output range collapses to a single boundary. Fit still runs; the
# check reads sklearn's fitted y_thresholds_ and rejects when the
# span is smaller than this. 0.15 = "must move at least 15pp between
# min and max prediction to be useful". Smaller means the isotonic is
# just returning a constant floor/ceiling for the whole raw-prob range
# and framework/Bayesian buckets will do better.
ISOTONIC_MIN_Y_SPAN = 0.15

# Legacy constants — kept as 0 / 0 sentinel for any external caller
# that still imports them. The new Bayesian path doesn't use them.
MIN_BUCKET_N = 0
SHRINKAGE_TARGET_N = 0

# Buckets are open-on-the-right intervals: [lo, hi). Shared with
# framework_calibration via engine.calibration_buckets so adding a
# finer bucket only requires one edit.
from .calibration_buckets import BUCKETS as _BUCKETS

# Secondary dimensions for granular calibration. The coarse (bet_type,
# prob_bucket) bucket handled every pick the same way regardless of
# whether it was a tight-juice favorite or a plus-money dog — which
# masked systematic per-quadrant biases (e.g. -190 favorites hit
# markedly worse than -110 favorites at the same raw prob). Granular
# buckets split by (edge_tier, fav/dog, juice_tier) AND fall back to
# the coarse key when the narrower bucket hasn't accumulated enough
# samples yet.
_EDGE_TIERS = (
    ("low",   0.0, 3.0),
    ("mid",   3.0, 7.0),
    ("high",  7.0, 15.0),
    ("xhigh", 15.0, 999.0),
)
# Juice tiers apply to favorites only. Dogs collapse to a single bucket
# since plus-money prices span a much wider range with thinner volume.
_JUICE_TIERS_FAV = (
    ("cheap",  -110),  # odds in [-110, 0)
    ("mid",    -150),  # odds in [-150, -110)
    ("heavy",  -200),  # odds in [-200, -150)
)


def _edge_tier(edge: float | None) -> str | None:
    if edge is None:
        return None
    e = float(edge)
    for label, lo, hi in _EDGE_TIERS:
        if lo <= e < hi:
            return label
    return None


def _fav_flag(odds: int | float | None) -> str | None:
    if odds is None:
        return None
    try:
        o = int(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    return "fav" if o < 0 else "dog"


def _juice_tier(odds: int | float | None) -> str | None:
    """Return the juice bucket label. Dogs (odds > 0) collapse to 'plus'."""
    if odds is None:
        return None
    try:
        o = int(odds)
    except (TypeError, ValueError):
        return None
    if o >= 0:
        return "plus"
    # Favorite: walk the tiers from cheapest to heaviest
    prev = 0
    for label, ceiling in _JUICE_TIERS_FAV:
        if ceiling <= o < prev:
            return label
        prev = ceiling
    # Below -200 — lumped with the existing heaviest tier rather than
    # opening a fourth bucket that would rarely accumulate samples.
    return "heavy"


def _direction_label(bet_type: str, pick_text: str | None) -> str | None:
    """Direction key for sub-bucket calibration. Delegates to
    edge_floors._direction_label so over/under/nrfi/yrfi/fav/dog
    extraction stays in one place — the May-2026 MLB tracker showed
    Unders 3-8 / -$523 at avg 24% edge while Overs went 5-2 / +$249,
    a directional bias the coarse (bet_type, prob_bucket) view masks.
    Returns None (not "") for empty so the existing == None checks
    in the granular-bucket lookup keep working."""
    from .edge_floors import _direction_label as _ef
    label = _ef(bet_type or "", pick_text)
    return label if label else None

# Per-sport calibration tables: {sport: {bet_type: [(lo, hi, real_wr)]}}
_TABLE: dict[str, dict[str, list[tuple[float, float, float]]]] = {}
_TABLE_LOCK = threading.Lock()
_TABLE_LOADED: dict[str, bool] = {}
# Isotonic curves per sport, keyed by (bt, ...) without the bucket.
# Populated during refresh_calibration; consulted by calibrate() before
# the bucket-based Beta-Binomial fallback. Empty dict when sklearn
# isn't installed or when refresh hasn't run yet — calibrate falls
# through cleanly.
_ISO_TABLE: dict[str, dict[tuple, dict]] = {}

# Per-sport pick-table location. Each sport has its own SQLite DB and
# its own picks-table name. Adding a sport = one entry here.
_SPORT_SOURCES = {
    "mlb": ("engine.db",     "picks"),
    "nhl": ("engine.nhl_db", "nhl_picks"),
    "nba": ("engine.nba_db", "nba_picks"),
    # Tennis added 2026-05-01. Same per-(bet_type, prob bucket)
    # shrinkage applies — once tennis_picks has enough settled rows
    # the table will populate and calibrate.
    "tennis": ("engine.tennis_db", "tennis_picks"),
    # Live calibration (#175, 2026-05-03) — separate buckets from
    # prematch because in-game probabilities have very different
    # variance characteristics. live_picks_{nba,nhl} tables live in
    # the same per-sport DB as prematch picks.
    "nba_live": ("engine.nba_db", "live_picks_nba"),
    "nhl_live": ("engine.nhl_db", "live_picks_nhl"),
    # Basketball framework leagues — each lives in its own per-league
    # SQLite under data/basketball/. Same picks-table schema across
    # the framework so the empirical calibrator can train on every
    # framework league once it accrues settled rows. Wnba/ncaam/afl
    # called out explicitly; the rest pick up the same path when they
    # need it (predictor reads the registry-overlaid constants).
    "wnba":  ("engine.basketball._db", "picks", "wnba"),
    "ncaam": ("engine.basketball._db", "picks", "ncaam"),
    "afl":   ("engine.basketball._db", "picks", "afl"),
}


def _normalize_bet_type(bt: str) -> str:
    """Collapse capitalization variants the tracker has accumulated."""
    if not bt:
        return ""
    bt = bt.strip().lower()
    # Keep "1st inn" -> "nrfi" so NRFI / 1st INN bucket together
    if bt in ("1st inn", "1stinn"):
        return "nrfi"
    return bt


def has_isotonic(sport: str, bet_type: str,
                  edge: float | None = None,
                  odds: int | float | None = None,
                  pick_text: str | None = None) -> bool:
    """True iff the empirical calibrator has an isotonic curve for the
    most specific cell reachable by the given inputs. Used by
    picks_core to decide whether to prefer the live-data isotonic
    over the framework walk-forward bucket calibrator: once we have
    n>=ISOTONIC_MIN_N live samples the tracker signal is fresher +
    more honest than an offline replay."""
    if not _TABLE_LOADED.get(sport):
        try:
            refresh_calibration(sport)
        except Exception:
            return False
    iso_table = _ISO_TABLE.get(sport) or {}
    if not iso_table:
        return False
    bt = _normalize_bet_type(bet_type)
    etier = _edge_tier(edge)
    fav = _fav_flag(odds)
    jtier = _juice_tier(odds)
    direction = _direction_label(bet_type, pick_text)
    ladder: list[tuple] = []
    if etier and fav and jtier:
        ladder.append((bt, etier, fav, jtier))
    if direction:
        ladder.append((bt, f"dir:{direction}"))
    if fav:
        ladder.append((bt, fav))
    ladder.append((bt,))
    for k in ladder:
        entry = iso_table.get(k)
        if entry and entry.get("x") and entry.get("y"):
            return True
    return False


def _iso_interpolate(xs: list[float], ys: list[float],
                       raw: float) -> float:
    """Piecewise-linear interpolation over the isotonic step points.

    sklearn's IsotonicRegression stores its solution as
    (X_thresholds_, y_thresholds_) — a set of breakpoints where the
    step function jumps. We linearly interpolate between adjacent
    breakpoints (smoother than a pure step function, still monotonic
    given the isotonic constraint). Values outside the fitted range
    clip to the boundary y-values, matching sklearn's
    ``out_of_bounds='clip'`` behavior at fit time.
    """
    if not xs or not ys:
        return raw
    if raw <= xs[0]:
        return ys[0]
    if raw >= xs[-1]:
        return ys[-1]
    # Binary search would be tidier but the arrays are small (~20
    # breakpoints max). Linear walk is clearer and equally fast at
    # this scale.
    for i in range(1, len(xs)):
        if raw <= xs[i]:
            x_lo, x_hi = xs[i - 1], xs[i]
            if x_hi == x_lo:
                return ys[i]
            frac = (raw - x_lo) / (x_hi - x_lo)
            return ys[i - 1] + frac * (ys[i] - ys[i - 1])
    return ys[-1]


def calibrate(bet_type: str, raw_prob: float, sport: str = "mlb",
              edge: float | None = None, odds: int | float | None = None,
              pick_text: str | None = None) -> float:
    """Map a raw model probability to the Bayesian-posterior estimate
    for the appropriate (sport, bet_type, ..., bucket) cell.

    Walks the same fallback ladder as before — granular > direction
    > fav/dog > coarse — but now every cell carries a Beta posterior
    with hierarchical priors (each finer cell uses the next-coarser
    cell's posterior as ITS prior center). So the most-specific cell
    with data wins, and "data" is no longer all-or-nothing — even a
    cell with n=2 returns a posterior that's mostly the parent prior
    plus a small empirical nudge.

    Returns:
      - posterior mean of most-specific cell with non-zero data
      - bucket-midpoint root prior if no cell has any data
      - raw_prob ONLY if the sport table hasn't loaded at all yet
        (cold-start safety)
    """
    if raw_prob is None:
        return raw_prob
    if not _TABLE_LOADED.get(sport):
        try:
            refresh_calibration(sport)
        except Exception as e:
            logger.debug("calibration load failed (%s): passthrough: %s",
                         sport, e)
            return raw_prob

    bt = _normalize_bet_type(bet_type)
    sport_table = _TABLE.get(sport, {})

    b = _bucket_for(raw_prob)
    if b is None:
        # Outside the bucket range — return raw. Don't clamp.
        return raw_prob

    # Build the fallback ladder from most to least specific.
    keys: list[tuple] = []
    etier = _edge_tier(edge)
    fav = _fav_flag(odds)
    jtier = _juice_tier(odds)
    direction = _direction_label(bet_type, pick_text)
    if etier and fav and jtier:
        keys.append((bt, etier, fav, jtier, b))
    if direction:
        keys.append((bt, f"dir:{direction}", b))
    if fav:
        keys.append((bt, fav, b))
    keys.append((bt, b))

    # Isotonic pre-pass — same ladder but keyed without the bucket
    # (isotonic operates on the full raw-prob range for the cell).
    # First cell with a fitted curve wins. The isotonic curve gives a
    # continuous, monotonic mapping so we don't collapse raw 0.71 and
    # 0.79 to the same output the way the bucket layer does.
    iso_table = _ISO_TABLE.get(sport) or {}
    iso_keys: list[tuple] = []
    if etier and fav and jtier:
        iso_keys.append((bt, etier, fav, jtier))
    if direction:
        iso_keys.append((bt, f"dir:{direction}"))
    if fav:
        iso_keys.append((bt, fav))
    iso_keys.append((bt,))
    for ik in iso_keys:
        iso = iso_table.get(ik)
        if iso and iso.get("x") and iso.get("y"):
            return _iso_interpolate(iso["x"], iso["y"], float(raw_prob))

    # Walk the bucket ladder top-down (granular → coarse). First cell
    # with a posterior wins — even a cell with n=0 has one (the prior).
    # This is the fallback for bet_types / sub-keys that haven't
    # accumulated ISOTONIC_MIN_N samples yet.
    for key in keys:
        cell = sport_table.get(key)
        if cell:
            return float(cell["mean"])

    # Coarse cell missing means the sport+bet_type+bucket has zero
    # samples even at the coarsest level. Return the bucket midpoint
    # as the absolute root prior. Log so an empty calibration table
    # doesn't masquerade as a real shrinkage value.
    logger.debug(
        "empirical_calibration %s/%s/[%.2f,%.2f): coarse cell empty, "
        "returning bucket midpoint as cold-start prior",
        sport, bet_type, b[0], b[1],
    )
    return (b[0] + b[1]) / 2.0


def calibrated_edge(bet_type: str, raw_prob: float, odds: int,
                    sport: str = "mlb",
                    pick_text: str | None = None) -> float:
    """Edge = calibrated_prob - implied_prob, in percentage points.

    Use this everywhere the picks pipeline currently does
    `(prob - implied(odds)) * 100`. The edge value computed against
    the raw prob is also used to route the granular calibration lookup,
    so the per-quadrant buckets apply automatically. When `pick_text`
    is supplied for a totals/first-inning market, the direction-specific
    bucket is consulted too."""
    if odds is None or raw_prob is None:
        return 0.0
    if odds < 0:
        implied = abs(odds) / (abs(odds) + 100)
    else:
        implied = 100.0 / (odds + 100)
    raw_edge = (raw_prob - implied) * 100.0
    cal = calibrate(bet_type, raw_prob, sport=sport,
                    edge=raw_edge, odds=odds, pick_text=pick_text)
    return (cal - implied) * 100.0


# ── Signal-log integration ─────────────────────────────────────
#
# Maps engine.prediction_log market names to the bet_types used in
# the per-sport picks tables. Only PROBABILITY markets survive
# this mapping — total-goal expected values (mlb total, mlb f5_total,
# nhl total, nba q1_total) are numeric expectations, not probabilities,
# so they don't feed the calibration buckets here.
_SIGNAL_MARKET_TO_BET_TYPE = {
    "mlb": {
        "home_win": "ML",
        "nrfi": "1st INN",
        "f5_home_win": "F5 ML",
    },
    "nhl": {
        "home_win": "ML",
    },
    "nba": {
        "q1_home_win": "Q1_ML",
    },
}


def _signal_log_calibration_rows(sport: str) -> list[dict]:
    """Pull (model_prob, outcome) pairs from prediction_signals for the
    sport. These represent EVERY prediction (not just fired picks),
    closing the selection-bias gap in the live picks training set.

    Returns a list of row dicts compatible with the _add() ingestor:
    {"bet_type", "model_prob", "won": bool, "pick": str}.
    """
    market_map = _SIGNAL_MARKET_TO_BET_TYPE.get(sport)
    if not market_map:
        return []
    try:
        from .prediction_log import _load_joined_rows, _actual_for_market
    except Exception as e:
        logger.debug("calibration: prediction_log import failed: %s", e)
        return []
    try:
        # 180-day window — long enough to cover a full season of picks,
        # short enough that schema/model changes >6 months old don't
        # pollute current-model calibration.
        joined = _load_joined_rows(sport, days=180)
    except Exception as e:
        logger.debug("calibration: signal-log query failed: %s", e)
        return []
    if not joined:
        return []
    out: list[dict] = []
    for game_row, sig in joined:
        market = sig.get("market")
        bet_type = market_map.get(market)
        if not bet_type:
            continue  # numeric markets (total, f5_total) skipped
        # Ensemble probability — simple mean of the components that ran.
        # If any single component is missing, the others still inform
        # the ensemble; this matches what picks_core consumes downstream.
        comps = [sig.get(k) for k in ("factor_prob", "mc_prob", "gbm_prob")]
        comps = [float(c) for c in comps if c is not None]
        if not comps:
            continue
        ensemble = sum(comps) / len(comps)
        if not (0.0 <= ensemble <= 1.0):
            continue  # safety — skip rows where the value isn't a prob
        actual = _actual_for_market(sport, market, game_row)
        if actual is None:
            continue
        # _actual_for_market returns 1/0 for win markets (and a number
        # for total markets, which we already filtered out above).
        won = bool(actual)
        out.append({
            "bet_type": bet_type,
            "model_prob": ensemble,
            "won": won,
            "pick": "signal_log",  # no side info; falls through coarse view
        })
    return out


def refresh_calibration(sport: str = "mlb") -> dict:
    """Rebuild the calibration table for `sport` from its picks table.
    Populates three keyed views from the same rows — fully granular,
    fav/dog-only, and legacy bet-type-only — so calibrate() can walk
    the ladder and pick the first one with enough samples."""
    summary = {"sport": sport, "granular_filled": 0, "fav_filled": 0,
               "direction_filled": 0, "coarse_filled": 0, "total_rows": 0}
    src = _SPORT_SOURCES.get(sport)
    if not src:
        # Demoted to debug: unknown sports here just fall through to
        # identity passthrough, which is the documented behavior. The
        # warning was firing on every basketball framework league lookup
        # (wnba / euroleague / etc.) — those sports calibrate upstream
        # via engine.basketball._calibration_apply, not through this
        # module, so the caller already has a working calibration path.
        logger.debug("calibration: unknown sport %r — passthrough", sport)
        return summary
    # 3-tuple shape for basketball-framework leagues carries the
    # registry-key as the third element (get_conn(league) instead of
    # get_conn()). Big-3 sports stay on the 2-tuple shape.
    bb_league = None
    if len(src) == 3:
        db_module_name, table_name, bb_league = src
    else:
        db_module_name, table_name = src

    try:
        import importlib
        db_module = importlib.import_module(db_module_name)
        get_conn = db_module.get_conn
    except Exception as e:
        logger.warning("calibration: cannot import %s: %s",
                       db_module_name, e)
        return summary
    # Bind the league arg when this is a framework league.
    if bb_league is not None:
        _orig_get_conn = get_conn
        get_conn = lambda _l=bb_league: _orig_get_conn(_l)

    # Optional date cutoff — see engine.config.CALIBRATION_CUTOFF.
    # Pre-cutoff picks reflect a *different* model and would poison
    # the current model's calibration map (memory: 2026-04-28 NegBin
    # fix flipped MLB ALT O/U from -37% to +31% ROI; pre-fix picks
    # shrank raw 70% to 49% and killed every pick at the edge gate).
    try:
        from .config import CALIBRATION_CUTOFF as _CC
        cutoff = _CC.get(sport)
    except Exception:
        cutoff = None
    cutoff_clause = ""
    cutoff_params: tuple = ()
    if cutoff:
        cutoff_clause = " AND date >= ?"
        cutoff_params = (cutoff,)

    try:
        # Pull every settled live pick with its edge + odds so we can
        # route each sample into the granular buckets. The synthetic
        # samples table only carries (bet_type, model_prob, result) so
        # those rows land in the coarse (bet_type, bucket) view only.
        conn = get_conn()
        live = conn.execute(
            f"SELECT bet_type, model_prob, result, edge, odds, pick "
            f"FROM {table_name} "
            f"WHERE result IN ('W', 'L') AND model_prob IS NOT NULL"
            f"{cutoff_clause}",
            cutoff_params,
        ).fetchall()
        synth = []
        try:
            synth = conn.execute(
                f"SELECT bet_type, model_prob, result, pick "
                f"FROM calibration_samples "
                f"WHERE result IN ('W', 'L') AND model_prob IS NOT NULL"
                f"{cutoff_clause}",
                cutoff_params,
            ).fetchall()
        except Exception:
            # Table not present in this DB yet -- fine, just use real picks.
            try:
                synth = conn.execute(
                    f"SELECT bet_type, model_prob, result "
                    f"FROM calibration_samples "
                    f"WHERE result IN ('W', 'L') AND model_prob IS NOT NULL"
                    f"{cutoff_clause}",
                    cutoff_params,
                ).fetchall()
                synth = [{**dict(r), "pick": None} for r in synth]
            except Exception:
                synth = []
    except Exception as e:
        logger.warning("calibration: query on %s failed: %s",
                       table_name, e)
        return summary

    # Third source: prediction_signals — log of EVERY prediction made,
    # not just fired picks. Closes the selection-bias gap that biased
    # calibration data toward cases where model > market (since picks
    # only fire when edge > floor). With unfired-prediction samples,
    # buckets see the true distribution. See task #402 / #403 + memory
    # project_phase4_diagnostics.
    signal_rows = _signal_log_calibration_rows(sport)

    total_rows = len(live) + len(synth) + len(signal_rows)
    summary["total_rows"] = total_rows
    summary["signal_log_rows"] = len(signal_rows)
    if total_rows == 0:
        with _TABLE_LOCK:
            _TABLE[sport] = {}
            _TABLE_LOADED[sport] = True
        return summary

    # Accumulate counts into every applicable key so one sample
    # contributes to coarse, fav-split, and granular simultaneously.
    counts: dict[tuple, dict] = defaultdict(lambda: {"n": 0, "w": 0})
    # Raw samples per bet_type × non-bucket key. Used by the isotonic
    # pass below — isotonic operates on continuous raw_prob → won
    # pairs across the whole bet_type (not per bucket) so we sidestep
    # the resolution-loss the bucket layer introduces.
    samples: dict[tuple, list[tuple[float, float]]] = defaultdict(list)

    def _add(sample_bt: str, prob: float, won: bool,
             sample_edge: float | None, sample_odds: int | float | None,
             sample_pick: str | None) -> None:
        bt = _normalize_bet_type(sample_bt)
        b = _bucket_for(prob or 0.0)
        if not b:
            return
        won_val = 1.0 if won else 0.0
        # Coarse: (bet_type, bucket)
        c = counts[(bt, b)]
        c["n"] += 1
        if won:
            c["w"] += 1
        # Isotonic sample buckets — no bucket component, isotonic
        # spans the whole raw-prob range for the bet_type.
        samples[(bt,)].append((float(prob or 0.0), won_val))
        # Direction-split: (bet_type, dir:over|under|nrfi|yrfi, bucket).
        # Captures the OU direction bias that the coarse view smears.
        direction = _direction_label(sample_bt, sample_pick)
        if direction:
            cd = counts[(bt, f"dir:{direction}", b)]
            cd["n"] += 1
            if won:
                cd["w"] += 1
            samples[(bt, f"dir:{direction}")].append(
                (float(prob or 0.0), won_val))
        # Fav-split: (bet_type, fav/dog, bucket)
        fav = _fav_flag(sample_odds)
        if fav:
            c2 = counts[(bt, fav, b)]
            c2["n"] += 1
            if won:
                c2["w"] += 1
            samples[(bt, fav)].append((float(prob or 0.0), won_val))
        # Granular: (bet_type, edge_tier, fav/dog, juice_tier, bucket)
        et = _edge_tier(sample_edge)
        jt = _juice_tier(sample_odds)
        if fav and et and jt:
            c3 = counts[(bt, et, fav, jt, b)]
            c3["n"] += 1
            if won:
                c3["w"] += 1
            samples[(bt, et, fav, jt)].append(
                (float(prob or 0.0), won_val))

    for r in live:
        r = dict(r)
        _add(r.get("bet_type"), r.get("model_prob") or 0.0,
             r["result"] == "W",
             r.get("edge"), r.get("odds"), r.get("pick"))
    for r in synth:
        r = dict(r) if not isinstance(r, dict) else r
        _add(r.get("bet_type"), r.get("model_prob") or 0.0,
             r["result"] == "W",
             None, None, r.get("pick"))
    # Signal-log rows: every prediction made, not just fired picks.
    # No edge/odds context — these populate the coarse (bet_type, bucket)
    # and direction-split views only. The granular (edge_tier, jtier)
    # view stays selection-biased because granular keys require odds.
    for r in signal_rows:
        _add(r["bet_type"], r["model_prob"], r["won"],
             None, None, r.get("pick"))

    # Hierarchical Bayesian posteriors. Every cell stores
    # {n, w, alpha, beta, mean} where:
    #   alpha + beta = PRIOR_N0 + n  (effective sample size)
    #   mean = (alpha + 0) / (alpha + beta) — the posterior expectation
    #
    # The hierarchy is bottom-up: a cell's prior center is the
    # posterior mean of its parent (next-coarser cell). Order of
    # computation matters — coarse first, then fav/dir, then granular.
    #
    # Parent map (which key is whose prior):
    #   ROOT       (bucket midpoint)
    #     ↓
    #   (bt, b)                        — coarse
    #     ↓                            ↓
    #   (bt, fav/dog, b)         (bt, dir:X, b)        — split layers
    #     ↓
    #   (bt, etier, fav, jtier, b)     — granular
    new_table: dict[tuple, dict] = {}

    def _posterior(prior_mean: float, n: int, w: int) -> dict:
        alpha = PRIOR_N0 * prior_mean + w
        beta = PRIOR_N0 * (1.0 - prior_mean) + (n - w)
        mean = alpha / (alpha + beta)
        return {"n": n, "w": w, "alpha": alpha, "beta": beta, "mean": mean}

    # Pass 1: COARSE cells (bt, b) — prior is bucket midpoint.
    for key, c in counts.items():
        if len(key) != 2:
            continue
        bt, b = key
        midpoint = (b[0] + b[1]) / 2.0
        new_table[key] = _posterior(midpoint, c["n"], c["w"])
        summary["coarse_filled"] += 1

    # Pass 2: FAV-split (bt, fav/dog, b) and DIRECTION-split (bt, dir:X, b)
    # — prior is the coarse posterior mean.
    for key, c in counts.items():
        if len(key) != 3:
            continue
        bt, mid_label, b = key
        coarse = new_table.get((bt, b))
        if coarse:
            prior_mean = coarse["mean"]
        else:
            prior_mean = (b[0] + b[1]) / 2.0
        new_table[key] = _posterior(prior_mean, c["n"], c["w"])
        if isinstance(mid_label, str) and mid_label.startswith("dir:"):
            summary["direction_filled"] += 1
        else:
            summary["fav_filled"] += 1

    # Pass 3: GRANULAR cells (bt, etier, fav, jtier, b) — prior is the
    # fav-split posterior mean.
    for key, c in counts.items():
        if len(key) != 5:
            continue
        bt, etier, fav, jtier, b = key
        parent = new_table.get((bt, fav, b))
        if parent:
            prior_mean = parent["mean"]
        else:
            coarse = new_table.get((bt, b))
            prior_mean = (coarse["mean"] if coarse
                          else (b[0] + b[1]) / 2.0)
        new_table[key] = _posterior(prior_mean, c["n"], c["w"])
        summary["granular_filled"] += 1

    # Pass 4: Isotonic curves per (bet_type, sub-key) cell — where
    # sub-key is one of {(), (dir:X,), (fav|dog,), granular}. Fitted
    # over raw_prob → won pairs regardless of bucket. Cells with
    # < ISOTONIC_MIN_N samples skip so we don't overfit thin data.
    # Stored as (iso_x, iso_y) breakpoint arrays alongside the
    # Beta-Binomial posterior for the same bucket cells; calibrate()
    # picks isotonic when present, falls back to the bucket mean.
    try:
        from sklearn.isotonic import IsotonicRegression
        _iso_available = True
    except ImportError:
        _iso_available = False
    summary["isotonic_fitted"] = 0
    summary["isotonic_skipped_thin"] = 0
    if _iso_available:
        # Store isotonic on a separate table indexed by the non-bucket
        # key so lookups can find it without needing a bucket first.
        iso_table: dict[tuple, dict] = {}
        for key, pts in samples.items():
            if len(pts) < ISOTONIC_MIN_N:
                summary["isotonic_skipped_thin"] += 1
                continue
            xs = [p for p, _ in pts]
            ys = [w for _, w in pts]
            try:
                iso = IsotonicRegression(
                    out_of_bounds="clip",
                    y_min=ISOTONIC_Y_MIN,
                    y_max=ISOTONIC_Y_MAX,
                )
                iso.fit(xs, ys)
                y_vals = [float(v) for v in iso.y_thresholds_.tolist()]
                # Degenerate-curve guard. A fit whose entire y-range
                # collapses to y_min (or y_max) is worthless — every
                # input maps to the same clamped output. Refuse to
                # persist those; the caller falls back to framework /
                # Beta-Binomial which stay sensible on thin data.
                y_span = max(y_vals) - min(y_vals)
                if y_span < ISOTONIC_MIN_Y_SPAN:
                    summary["isotonic_skipped_thin"] += 1
                    continue
                # Serialize as endpoint arrays. sklearn exposes
                # X_thresholds_ + y_thresholds_ after fit; those are
                # the step-function knees we need to reconstruct
                # interpolation without keeping sklearn in the loop.
                iso_table[key] = {
                    "n": len(pts),
                    "x": [float(v) for v in iso.X_thresholds_.tolist()],
                    "y": y_vals,
                }
                summary["isotonic_fitted"] += 1
            except Exception as e:
                logger.debug("isotonic fit failed for %s/%s: %s",
                              sport, key, e)
        with _TABLE_LOCK:
            _ISO_TABLE[sport] = iso_table
    else:
        with _TABLE_LOCK:
            _ISO_TABLE[sport] = {}

    with _TABLE_LOCK:
        _TABLE[sport] = new_table
        _TABLE_LOADED[sport] = True

    logger.info("calibration[%s]: built %d posteriors + %d isotonic "
                "curves from %d rows (%d granular / %d dir / %d fav / "
                "%d coarse)",
                sport, len(new_table), summary["isotonic_fitted"],
                total_rows, summary["granular_filled"],
                summary["direction_filled"], summary["fav_filled"],
                summary["coarse_filled"])
    return summary


def refresh_all_sports() -> dict:
    """Refresh every sport in _SPORT_SOURCES. Returns per-sport summaries."""
    return {sport: refresh_calibration(sport) for sport in _SPORT_SOURCES}


def snapshot(sport: str | None = None) -> dict:
    """Return the calibration table for `sport`, or all sports when None.
    Keys are the composite bucket tuples; values are {n, w, real_wr}."""
    with _TABLE_LOCK:
        if sport is None:
            return {s: {k: dict(v) for k, v in tbl.items()}
                    for s, tbl in _TABLE.items()}
        return {k: dict(v) for k, v in _TABLE.get(sport, {}).items()}


from .calibration_buckets import bucket_for as _bucket_for  # noqa: E402


def buckets() -> Iterable[tuple[float, float]]:
    """Bucket boundaries used internally; useful for the diag report."""
    return list(_BUCKETS)
