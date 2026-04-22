"""Empirical probability calibration from settled tracker picks.

The factor + MC + GBM ensemble is systemically over-confident at the
upper tail. The 191-pick tracker (Apr 2026) showed:
   ML  80%+ predicted bucket -> 60.0% real (-30pp)
   OU  70%+ predicted bucket -> 37.5% real (-37pp)
   RL  80%+ predicted bucket -> 47.1% real (-43pp)
i.e. the model claims it's 80%-confident but reality is a coin flip.

Rather than guess at a soft-compression factor, we calibrate from the
actual outcomes. Each bet type has a piecewise-linear map from raw
predicted probability -> empirical win rate. Outside the table we
clamp to the nearest bucket. The map is rebuilt nightly by
`refresh_calibration()` from the picks table; until enough rows
accumulate per bucket (>= MIN_BUCKET_N) the bucket falls back to its
midpoint passthrough (no recalibration).

This is a Platt/isotonic-flavored calibration, not a perfect fit, but
it's grounded in observed data and updates automatically as more picks
settle. When a bucket has too few samples we keep the model honest
(passthrough); when we have data we use it.

Public API:
   calibrate(bet_type, raw_prob) -> calibrated_prob
   refresh_calibration() -> dict (called by sync scheduler)
"""

from __future__ import annotations

import logging
import threading
from collections import defaultdict
from typing import Iterable

logger = logging.getLogger(__name__)

# Minimum samples per (bet_type, bucket) before we trust the empirical
# real_wr enough to override the model's raw prediction. Below this we
# pass the raw prob through unchanged so we don't over-fit small samples.
# Set to 10 because at N=5-9 a single bad streak can make a bucket look
# like 15% real WR when underlying truth is closer to 50%; that level of
# noise actively misleads the picks engine. 10 trades cold-start coverage
# for fewer false-positive recalibrations.
MIN_BUCKET_N = 10

# Sample count at which the empirical WR fully replaces the raw model
# prob. Below it, we linearly blend empirical with raw: at n=10 (the
# MIN_BUCKET_N floor) we keep 80% raw + 20% empirical; at n=50 it's
# fully empirical. Prevents small-sample noise from rewriting
# probabilities by ±15pp and creating phantom +EV picks at the tails.
SHRINKAGE_TARGET_N = 50

# Buckets are open-on-the-right intervals: [lo, hi). The "raw" bucket
# label is also used as the lookup key.
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

# Per-sport calibration tables: {sport: {bet_type: [(lo, hi, real_wr)]}}
_TABLE: dict[str, dict[str, list[tuple[float, float, float]]]] = {}
_TABLE_LOCK = threading.Lock()
_TABLE_LOADED: dict[str, bool] = {}

# Per-sport pick-table location. Each sport has its own SQLite DB and
# its own picks-table name. Adding a sport = one entry here.
_SPORT_SOURCES = {
    "mlb": ("engine.db",     "picks"),
    "nhl": ("engine.nhl_db", "nhl_picks"),
    "nba": ("engine.nba_db", "nba_picks"),
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


def calibrate(bet_type: str, raw_prob: float, sport: str = "mlb",
              edge: float | None = None, odds: int | float | None = None) -> float:
    """Map a raw model probability to the empirical win-rate bucket
    for `sport`.

    Consults progressively coarser keys: the fully granular
    (bet_type, edge_tier, fav/dog, juice_tier, prob_bucket) key first,
    then (bet_type, fav/dog, prob_bucket), finally the legacy
    (bet_type, prob_bucket). A bucket is "used" only when it has at
    least MIN_BUCKET_N samples; below that the next coarser key is
    tried. This lets granular buckets surface real per-quadrant biases
    once volume accumulates without starving cold-start picks of any
    calibration signal.

    edge / odds are optional — when omitted (old callers), the function
    skips the granular keys and behaves exactly like the previous
    bet_type-only version.

    Falls back to raw passthrough when:
      - The sport's calibration table hasn't been built yet (cold start)
      - This bet_type has no data for the sport
      - No fallback bucket cleared MIN_BUCKET_N samples
    """
    if raw_prob is None:
        return raw_prob
    if not _TABLE_LOADED.get(sport):
        # Lazy-load on first call so callers don't need to know about it.
        try:
            refresh_calibration(sport)
        except Exception as e:
            logger.debug("calibration load failed (%s): passthrough: %s",
                         sport, e)
            return raw_prob

    bt = _normalize_bet_type(bet_type)
    sport_table = _TABLE.get(sport, {})
    if not sport_table:
        return raw_prob

    b = _bucket_for(raw_prob)
    if b is None:
        # Outside the bucket range (below lowest, above highest) —
        # passthrough rather than clamp, matching legacy behavior.
        return raw_prob

    # Build the fallback ladder from most to least specific.
    keys: list[tuple] = []
    etier = _edge_tier(edge)
    fav = _fav_flag(odds)
    jtier = _juice_tier(odds)
    if etier and fav and jtier:
        keys.append((bt, etier, fav, jtier, b))
    if fav:
        keys.append((bt, fav, b))
    keys.append((bt, b))

    for key in keys:
        bucket = sport_table.get(key)
        if not bucket:
            continue
        real_wr, n = bucket["real_wr"], bucket["n"]
        if n < MIN_BUCKET_N:
            continue
        # Shrink the empirical toward the raw model prob based on
        # sample count. With n=10 the bucket has too little data to
        # trust wholesale (a single hot streak moves the bar by 10pp);
        # with n>=SHRINKAGE_TARGET_N it's reliable enough to apply
        # fully. Without this, a synthetic ~12-sample bucket that hit
        # 45% was lifting every heavy underdog from raw 32% to
        # "calibrated" 45%, producing phantom +EV edges against
        # heavy market prices.
        w = min(1.0, n / SHRINKAGE_TARGET_N)
        return w * real_wr + (1.0 - w) * raw_prob

    return raw_prob


def calibrated_edge(bet_type: str, raw_prob: float, odds: int,
                    sport: str = "mlb") -> float:
    """Edge = calibrated_prob - implied_prob, in percentage points.

    Use this everywhere the picks pipeline currently does
    `(prob - implied(odds)) * 100`. The edge value computed against
    the raw prob is also used to route the granular calibration lookup,
    so the per-quadrant buckets apply automatically."""
    if odds is None or raw_prob is None:
        return 0.0
    if odds < 0:
        implied = abs(odds) / (abs(odds) + 100)
    else:
        implied = 100.0 / (odds + 100)
    raw_edge = (raw_prob - implied) * 100.0
    cal = calibrate(bet_type, raw_prob, sport=sport,
                    edge=raw_edge, odds=odds)
    return (cal - implied) * 100.0


def refresh_calibration(sport: str = "mlb") -> dict:
    """Rebuild the calibration table for `sport` from its picks table.
    Populates three keyed views from the same rows — fully granular,
    fav/dog-only, and legacy bet-type-only — so calibrate() can walk
    the ladder and pick the first one with enough samples."""
    summary = {"sport": sport, "granular_filled": 0, "fav_filled": 0,
               "coarse_filled": 0, "total_rows": 0}
    src = _SPORT_SOURCES.get(sport)
    if not src:
        logger.warning("calibration: unknown sport %r", sport)
        return summary
    db_module_name, table_name = src

    try:
        import importlib
        db_module = importlib.import_module(db_module_name)
        get_conn = db_module.get_conn
    except Exception as e:
        logger.warning("calibration: cannot import %s: %s",
                       db_module_name, e)
        return summary

    try:
        # Pull every settled live pick with its edge + odds so we can
        # route each sample into the granular buckets. The synthetic
        # samples table only carries (bet_type, model_prob, result) so
        # those rows land in the coarse (bet_type, bucket) view only.
        conn = get_conn()
        live = conn.execute(
            f"SELECT bet_type, model_prob, result, edge, odds "
            f"FROM {table_name} "
            "WHERE result IN ('W', 'L') AND model_prob IS NOT NULL"
        ).fetchall()
        synth = []
        try:
            synth = conn.execute(
                "SELECT bet_type, model_prob, result "
                "FROM calibration_samples "
                "WHERE result IN ('W', 'L') AND model_prob IS NOT NULL"
            ).fetchall()
        except Exception:
            # Table not present in this DB yet -- fine, just use real picks.
            pass
    except Exception as e:
        logger.warning("calibration: query on %s failed: %s",
                       table_name, e)
        return summary

    total_rows = len(live) + len(synth)
    summary["total_rows"] = total_rows
    if total_rows == 0:
        with _TABLE_LOCK:
            _TABLE[sport] = {}
            _TABLE_LOADED[sport] = True
        return summary

    # Accumulate counts into every applicable key so one sample
    # contributes to coarse, fav-split, and granular simultaneously.
    counts: dict[tuple, dict] = defaultdict(lambda: {"n": 0, "w": 0})

    def _add(sample_bt: str, prob: float, won: bool,
             sample_edge: float | None, sample_odds: int | float | None) -> None:
        bt = _normalize_bet_type(sample_bt)
        b = _bucket_for(prob or 0.0)
        if not b:
            return
        # Coarse: (bet_type, bucket)
        c = counts[(bt, b)]
        c["n"] += 1
        if won:
            c["w"] += 1
        # Fav-split: (bet_type, fav/dog, bucket)
        fav = _fav_flag(sample_odds)
        if fav:
            c2 = counts[(bt, fav, b)]
            c2["n"] += 1
            if won:
                c2["w"] += 1
        # Granular: (bet_type, edge_tier, fav/dog, juice_tier, bucket)
        et = _edge_tier(sample_edge)
        jt = _juice_tier(sample_odds)
        if fav and et and jt:
            c3 = counts[(bt, et, fav, jt, b)]
            c3["n"] += 1
            if won:
                c3["w"] += 1

    for r in live:
        r = dict(r)
        _add(r.get("bet_type"), r.get("model_prob") or 0.0,
             r["result"] == "W",
             r.get("edge"), r.get("odds"))
    for r in synth:
        r = dict(r)
        _add(r.get("bet_type"), r.get("model_prob") or 0.0,
             r["result"] == "W",
             None, None)

    # Flatten counts into the lookup table. calibrate() only consults
    # buckets that cleared MIN_BUCKET_N, but we still surface thinner
    # buckets in the snapshot for diagnostics.
    new_table: dict[tuple, dict] = {}
    for key, c in counts.items():
        if c["n"] == 0:
            continue
        real_wr = c["w"] / c["n"]
        new_table[key] = {"n": c["n"], "w": c["w"], "real_wr": real_wr}
        if c["n"] >= MIN_BUCKET_N:
            # Categorize by key length for the summary.
            klen = len(key)
            if klen == 5:
                summary["granular_filled"] += 1
            elif klen == 3:
                summary["fav_filled"] += 1
            else:
                summary["coarse_filled"] += 1

    with _TABLE_LOCK:
        _TABLE[sport] = new_table
        _TABLE_LOADED[sport] = True

    logger.info("calibration[%s]: built from %d rows — %d granular / "
                "%d fav-split / %d coarse buckets cleared MIN_BUCKET_N",
                sport, total_rows, summary["granular_filled"],
                summary["fav_filled"], summary["coarse_filled"])
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


def _bucket_for(p: float) -> tuple[float, float] | None:
    for lo, hi in _BUCKETS:
        if lo <= p < hi:
            return (lo, hi)
    return None


def buckets() -> Iterable[tuple[float, float]]:
    """Bucket boundaries used internally; useful for the diag report."""
    return list(_BUCKETS)
