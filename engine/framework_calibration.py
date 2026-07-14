"""Cross-framework calibration applicator.

Reads the per-league walk-forward calibration JSONs produced by
``engine.basketball._walkforward_factor`` and ``engine.hockey._calibrate``,
exposes a single ``calibrate(sport, raw_prob)`` entry point that
``picks_core.score_pick`` can consult for sports the legacy
``empirical_calibration`` doesn't know about.

For each (sport, raw_prob), looks up the bucket, returns the realized
hit rate from the walk-forward sample. Falls back to the bucket
midpoint, then to raw_prob if even that's missing.

Wired in by ``engine.picks_core.score_pick`` for any sport key it sees
that isn't in the legacy ``empirical_calibration._SPORT_SOURCES`` —
basketball framework leagues (wnba, euroleague, ncaam, ...) and
hockey framework leagues (ahl, pwhl) all flow through here.

Cache: per-league JSON read once per process and held in memory until
``invalidate(sport)`` is called.
"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

_BASE = Path(__file__).resolve().parent.parent / "data"

# Per-sport file lookup. Hockey framework leagues live under data/hockey;
# basketball framework leagues under data/basketball; soccer framework
# leagues under data/soccer; baseball + football frameworks under their
# own directories. picks_core passes a sport key like
# ``baseball_college`` for baseball-framework leagues (see
# engine/baseball/_picks.py), so we accept both the bare league key
# and the framework-prefixed form.
_HOCKEY = {"ahl", "pwhl", "aihl", "nzihl"}
_SOCCER = {
    "arg_lpf", "bra_seriea", "conmebol_libertadores",
    "eng_premier", "esp_laliga", "fifa_internationals",
    "fifa_world_cup", "fra_ligue1", "ger_bundesliga",
    "ita_seriea", "mls", "uefa_champions",
    "uefa_conference", "uefa_europa", "usl_championship",
    "us_open_cup", "us_nwsl",
}
_BASEBALL = {"college"}
_FOOTBALL = {"ufl"}


def _calibration_path(sport: str) -> Path | None:
    # picks_core passes a sport key like "baseball_college" or
    # "football_ufl" for the new frameworks. Strip the prefix to find
    # the bare league key the calibration JSON is named under.
    league = sport
    if sport.startswith("baseball_"):
        league = sport[len("baseball_"):]
    elif sport.startswith("football_"):
        league = sport[len("football_"):]
    if league in _HOCKEY:
        return _BASE / "hockey" / f"{league}_calibration.json"
    if league in _SOCCER:
        candidate = _BASE / "soccer" / f"{league}_calibration.json"
        if candidate.exists():
            return candidate
        return None
    if league in _BASEBALL:
        candidate = _BASE / "baseball" / f"{league}_calibration.json"
        if candidate.exists():
            return candidate
        return None
    if league in _FOOTBALL:
        candidate = _BASE / "football" / f"{league}_calibration.json"
        if candidate.exists():
            return candidate
        return None
    # Tennis ships a single combined JSON (ATP+WTA share the same
    # bet_type bucket math; per-tour split is a follow-on if the
    # aggregate shows systematic divergence).
    if sport == "tennis":
        candidate = _BASE / "tennis_calibration.json"
        if candidate.exists():
            return candidate
        return None
    # NHL ships its own top-level walk-forward JSON. Lives outside
    # data/hockey/ because the NHL framework predates the hockey-
    # framework layout (AHL/PWHL/AIHL/NZIHL) and has its own DB +
    # ingest. Same Format-A schema as the rest.
    if sport == "nhl":
        candidate = _BASE / "nhl_calibration.json"
        if candidate.exists():
            return candidate
        return None
    # basketball framework leagues — anything else maps here.
    candidate = _BASE / "basketball" / f"{sport}_calibration.json"
    if candidate.exists():
        return candidate
    return None


# Bucket grid lives in engine.calibration_buckets — shared with
# engine.empirical_calibration so adding a finer bucket only requires
# one edit.
from .calibration_buckets import BUCKETS as _BUCKETS, bucket_for as _bucket_for


_CACHE: dict[str, dict] = {}
_CACHE_MTIMES: dict[str, float] = {}
_LOCK = threading.Lock()


def _load(sport: str) -> dict | None:
    """Load + memoize calibration JSON, refreshing automatically when
    the file's mtime advances (i.e. walk-forward refit just wrote a
    new version). Without this, a long-running process keeps using
    the stale buckets it loaded at startup until ``invalidate()`` is
    called by hand."""
    with _LOCK:
        path = _calibration_path(sport)
        if not path or not path.exists():
            _CACHE[sport] = None
            _CACHE_MTIMES[sport] = 0.0
            return None
        cur_mtime = path.stat().st_mtime
        if sport in _CACHE and _CACHE_MTIMES.get(sport) == cur_mtime:
            return _CACHE[sport]
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("framework calibration load failed for %s: %s",
                           sport, e)
            _CACHE[sport] = None
            _CACHE_MTIMES[sport] = cur_mtime
            return None
        # Validate the file matches one of the two known shapes:
        #   A. Walk-forward output: top-level "buckets" with an "ML" list.
        #   B. Walk-forward-factor output: top-level "ml" dict keyed by
        #      bucket strings ("0.30-0.40").
        # Anything else means a producer wrote the wrong format — log
        # loudly and treat as cold-start so calibrate() returns raw
        # passthrough rather than silently producing wrong probs.
        has_format_a = bool((data.get("buckets") or {}).get("ML"))
        has_format_b = bool(data.get("ml"))
        if not (has_format_a or has_format_b):
            logger.warning(
                "framework calibration %s: unrecognized schema (no "
                "'buckets.ML' nor 'ml' top-level key) at %s — treating "
                "as cold-start",
                sport, path,
            )
            _CACHE[sport] = None
            _CACHE_MTIMES[sport] = cur_mtime
            return None
        _CACHE[sport] = data
        _CACHE_MTIMES[sport] = cur_mtime
        return data


def invalidate(sport: str | None = None) -> None:
    """Drop cached table for ``sport`` (or all when None). Mostly
    redundant now that ``_load`` checks file mtime on every call —
    kept for explicit-flush callers."""
    with _LOCK:
        if sport is None:
            _CACHE.clear()
            _CACHE_MTIMES.clear()
        else:
            _CACHE.pop(sport, None)
            _CACHE_MTIMES.pop(sport, None)


def is_available(sport: str) -> bool:
    """True iff a walk-forward calibration JSON exists for ``sport``."""
    return _load(sport) is not None


# Empirical-Bayes prior strength — same value as engine.empirical_calibration.
# A bucket of n=6 going 6/6 at avg_pred=0.53 was previously returning 1.0,
# which then got mapped to the 0.905 belief-gate cap. With n0=10 the
# returned prob is ~0.71 — a meaningful, non-saturating calibration.
_PRIOR_N0 = 10.0


def calibrate(sport: str, raw_prob: float, bet_type: str | None = None) -> float:
    """Map raw ``raw_prob`` to a Bayesian-shrunk realized hit rate.

    Reads either of two on-disk shapes:
      - ``buckets[bet_type][i] = {bucket, n, avg_pred, realized_wr}``
        (current ``_walkforward.py`` output — ML-only until SPREAD/TOTAL
        odds-history backfill lands)
      - ``ml["lo-hi"] = {n, hit_rate, avg_pred}``
        (legacy ``_walkforward_factor.py`` output)

    When ``bet_type`` is provided, only that market's bucket is read.
    When bet_type's bucket isn't in the on-disk table (e.g. AFL has
    ML data but SPREAD/TOTAL are "deferred"), the function returns
    raw_prob unchanged — caller falls through to engine.empirical_calibration
    which trains on the live picks table per (sport, bet_type, bucket).

    Returns raw_prob when no calibration JSON exists for the sport
    (cold-start passthrough).
    """
    if raw_prob is None:
        return raw_prob
    data = _load(sport)
    if not data:
        return raw_prob

    # Bucket the favored-side probability.
    favored = raw_prob >= 0.5
    p = raw_prob if favored else (1 - raw_prob)
    bucket = _bucket_for(p)
    if bucket is None:
        return raw_prob

    # Resolve which bet_type's bucket to read. ML is the legacy default
    # (callers that didn't pass bet_type still get the same behaviour
    # for ML picks). Anything else looks up its own bucket and falls
    # through to raw when missing — prevents ML-bucket shrinkage from
    # being applied to SPREAD/TOTAL picks.
    bt_norm = (bet_type or "ML").upper().replace(" ", "_")
    # Map common picks-engine bet_types to walkforward bucket keys.
    # Hockey emits PL (puck line) / OU (over-under); basketball uses
    # SPREAD / TOTAL — bucket keys normalize to the latter shape.
    BT_MAP = {
        "ML": "ML", "MONEYLINE": "ML",
        "SPREAD": "SPREAD", "ALT_SPREAD": "SPREAD",
        "PL": "SPREAD", "PUCK_LINE": "SPREAD",
        "Q1_ML": "Q1_ML",
        "Q1_SPREAD": "Q1_SPREAD",
        "TOTAL": "TOTAL", "ALT_TOTAL": "TOTAL", "ALT_O/U": "TOTAL",
        "OU": "TOTAL", "O/U": "TOTAL",
        "Q1_TOTAL": "Q1_TOTAL",
        # Soccer markets — DC/DNB/BTTS are first-class types from
        # engine.soccer._picks. AH (asian handicap) follows SPREAD's
        # bucket shape but the realized rates can differ from
        # basketball/hockey SPREAD (soccer scoring is much lower
        # variance) so it keeps its own bucket key when the calibration
        # JSON has an AH bucket and otherwise falls through to SPREAD.
        "DC": "DC", "DOUBLE_CHANCE": "DC",
        "DNB": "DNB", "DRAW_NO_BET": "DNB",
        "BTTS": "BTTS", "BOTH_TEAMS_TO_SCORE": "BTTS",
        "AH": "AH", "ASIAN_HANDICAP": "AH",
        # Soccer H1 (first-half) variants — bucketed separately
        # because the predictor's H1 lambdas have different accuracy
        # characteristics than full-game (fewer goals = higher variance
        # on tail outcomes).
        "H1_ML": "H1_ML",
        "H1_DC": "H1_DC",
        "H1_DNB": "H1_DNB",
        "H1_BTTS": "H1_BTTS",
        "H1_TOTAL": "H1_TOTAL",
    }
    bucket_key = BT_MAP.get(bt_norm, bt_norm)
    n = 0
    realized: float | None = None
    avg_pred: float | None = None
    # Format A — new walkforward output (bet_type-keyed buckets)
    for entry in (data.get("buckets") or {}).get(bucket_key, []):
        bk = entry.get("bucket") or [None, None]
        if bk[0] is None or bk[1] is None:
            continue
        if abs(bk[0] - bucket[0]) < 1e-9 and abs(bk[1] - bucket[1]) < 1e-9:
            n = int(entry.get("n") or 0)
            realized = entry.get("realized_wr")
            avg_pred = entry.get("avg_pred")
            break
    # Format B — legacy walkforward_factor output (ML-only). Only
    # consulted when caller asked for ML to preserve historical behaviour.
    if realized is None and bucket_key == "ML":
        key = f"{bucket[0]:.2f}-{bucket[1]:.2f}"
        cell = (data.get("ml") or {}).get(key) or {}
        n = int(cell.get("n") or 0)
        realized = cell.get("hit_rate")
        avg_pred = cell.get("avg_pred")

    if realized is None or n <= 0:
        # No data for this (bet_type, bucket) cell — pass raw through.
        # Caller (picks_core) falls back to empirical_calibration which
        # trains on settled picks per bet_type.
        return raw_prob

    if avg_pred is None:
        # Malformed cell — bucket has n>0 and a realized rate but no
        # mean prediction logged. Bucket midpoint is a poor center for
        # shrinkage; warn so a re-seed is triggered.
        logger.warning(
            "framework calibration %s: bucket [%.2f, %.2f) has n=%d "
            "realized=%.3f but avg_pred missing; falling back to "
            "midpoint center. Re-run walk-forward calibration to fix.",
            sport, bucket[0], bucket[1], n, float(realized),
        )
        center = (bucket[0] + bucket[1]) / 2.0
    else:
        center = avg_pred
    calibrated = (n * float(realized) + _PRIOR_N0 * float(center)) / (n + _PRIOR_N0)

    # Flip back if we calibrated the complement.
    return calibrated if favored else (1 - calibrated)


# ── Edge calc helper that mirrors empirical_calibration.calibrated_edge ──

def calibrated_edge(sport: str, raw_prob: float, odds: int) -> dict:
    """Score a pick like empirical_calibration.calibrated_edge does, but
    using the framework calibration table.

    Returns: ``{"raw_prob", "calibrated_prob", "implied_prob", "edge_pct"}``.
    """
    cal = calibrate(sport, raw_prob)
    if odds < 0:
        implied = abs(odds) / (abs(odds) + 100)
    else:
        implied = 100 / (odds + 100)
    edge = round((cal - implied) * 100, 2)
    return {
        "raw_prob": raw_prob,
        "calibrated_prob": cal,
        "implied_prob": implied,
        "edge_pct": edge,
    }
