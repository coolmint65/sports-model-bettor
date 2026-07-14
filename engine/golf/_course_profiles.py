"""Per-course SG demand profiles.

A course's "SG demand profile" weights the four strokes-gained
categories (off-the-tee / approach / around-the-green / putting) by
how much each predicts success at THAT specific course. Augusta
rewards driving accuracy + approach, Pebble rewards putting on poa
greens, etc.

Two-stage rollout
-----------------

**Stage 1 (today)**: archetype lookup. Courses with known character
get hand-mapped to one of five archetypes (driving / approach /
short_game / putting / balanced). Anything unmapped falls through to
``balanced``. This gives us a useful, defensible weighting today
without pretending we've fit per-course coefficients we don't have.

**Stage 2 (future)**: data-fit profiles. Once ``player_sg`` accumulates
~5+ historical runnings of a course (PGA Tour rotates ~50 courses/year
on a single-event-per-course cadence, so this is a multi-year lift),
regress finish_position against per-category SG within that course's
field. Replace the archetype with the fitted weights via the same
``get_demand_weights`` API — no caller changes.

The DataGolf SG ingest captures per-category breakdowns already; the
predictor reads only ``sg_total`` today because differential weighting
needed a course profile. This module is the missing piece.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


# Archetype → weight vector. Each tuple is (ott, app, arg, putt) and
# sums to 1.0. Weights derived from the published Mark Broadie /
# DataGolf framework: ~25% OTT + 30% APP + 20% ARG + 25% PUTT for
# tour-average season-long predictiveness. Archetypes shift that
# baseline based on the course's known character.
_ARCHETYPES: dict[str, tuple[float, float, float, float]] = {
    # Long, narrow, penal rough → driving distance + accuracy decides
    "driving":     (0.40, 0.30, 0.15, 0.15),
    # Firm greens, awkward angles → approach play decides
    "approach":    (0.20, 0.45, 0.20, 0.15),
    # Small greens, lots of scrambling → short game decides
    "short_game":  (0.20, 0.30, 0.30, 0.20),
    # Tricky/firm greens (poa, slope, speed) → putting decides
    "putting":     (0.20, 0.25, 0.20, 0.35),
    # Default — equal-ish weighting tuned to PGA Tour season-long
    "balanced":    (0.25, 0.30, 0.20, 0.25),
}


# Per-course archetype mapping. Conservative — only courses with
# strong public consensus on character are listed. Add more as ops
# learns each venue's identity (or as data-fit Stage 2 lands).
# Keys are normalized (lowercased, stripped) for fuzzy matching.
_COURSE_ARCHETYPES: dict[str, str] = {
    # Majors + signature courses with established character
    "augusta national golf club":      "approach",   # Masters — approach + putting
    "the country club":                "short_game", # US Open Brookline
    "pebble beach golf links":         "putting",    # poa greens
    "torrey pines south course":       "driving",    # long, penal
    "torrey pines golf course":        "driving",
    "tpc sawgrass":                    "approach",   # Players — island greens
    "muirfield village":               "approach",   # Memorial
    "bay hill club & lodge":           "driving",    # API
    "riviera country club":            "approach",   # Genesis — kikuyu rough
    "harbour town golf links":         "short_game", # RBC Heritage — tight, small greens
    "tpc scottsdale":                  "putting",    # WMP — firm greens
    "kapalua plantation course":       "driving",    # SOTY — long
    "waialae country club":            "putting",    # Sony — short, putting course
    "tpc summerlin":                   "balanced",   # Shriners — generic
    "tpc craig ranch":                 "driving",    # Byron Nelson — long bomber friendly
    "colonial country club":           "approach",   # Schwab — accuracy course
    "muirfield":                       "approach",   # The Open rotation
    "royal portrush":                  "driving",    # The Open
    "royal liverpool":                 "approach",   # The Open
    "valhalla golf club":              "driving",    # PGA Championship long
    "oak hill country club":           "approach",   # PGA Championship
    "southern hills country club":     "approach",   # PGA Championship
    "aronimink golf club":             "approach",   # PGA Championship 2026
}


_DEFAULT_PROFILE = "balanced"


def _norm_course(course: str | None) -> str:
    if not course:
        return ""
    return course.strip().lower()


def get_archetype(course: str | None) -> str:
    """Lookup archetype for ``course``. Falls back to 'balanced' when
    unknown."""
    key = _norm_course(course)
    if not key:
        return _DEFAULT_PROFILE
    return _COURSE_ARCHETYPES.get(key, _DEFAULT_PROFILE)


def get_demand_weights(course: str | None) -> tuple[float, float, float, float]:
    """Return (w_ott, w_app, w_arg, w_putt) for ``course``. Sums to 1.0.

    Stage 1 path: archetype lookup. Stage 2 hook (data-fit) checks for a
    persisted per-course profile in ``data/golf/course_profiles.json``
    first; if present, those weights override the archetype default.
    """
    # Stage 2 hook — per-course fit overrides archetype when available.
    persisted = _load_persisted_profiles()
    key = _norm_course(course)
    if key and key in persisted:
        w = persisted[key]
        return (w["ott"], w["app"], w["arg"], w["putt"])
    # Stage 1 — archetype default.
    archetype = get_archetype(course)
    return _ARCHETYPES.get(archetype, _ARCHETYPES[_DEFAULT_PROFILE])


def weighted_sg(sg_ott: float | None, sg_app: float | None,
                 sg_arg: float | None, sg_putt: float | None,
                 weights: tuple[float, float, float, float]
                 ) -> float | None:
    """Course-demand-weighted SG on the same per-round-stroke scale as
    ``sg_total``. Each category w_i scales its contribution; the
    normalizer (×4) keeps the output dimensionally identical to the
    flat sum (sg_ott + sg_app + sg_arg + sg_putt) when weights are
    balanced (0.25 each).

    Math: ``out = N * sum(w_i * sg_i) / sum(w_i)``, where N is the
    number of categories supplied. With balanced weights and 4
    categories present, this reduces to the textbook sg_total = sum
    of components. With a driving-heavy course (0.40/0.30/0.15/0.15),
    a player whose strength is OTT gets a higher number than they
    would under sg_total alone — and vice versa for a putter on a
    driving course.

    Missing categories drop out; the remaining weights renormalize so
    a 3-category sample still produces a comparable scale (instead of
    silently shrinking to 75% of expected magnitude)."""
    parts: list[tuple[float, float]] = []
    for v, w in zip((sg_ott, sg_app, sg_arg, sg_putt), weights):
        if v is None:
            continue
        parts.append((float(v), float(w)))
    if not parts:
        return None
    total_w = sum(w for _, w in parts)
    if total_w <= 0:
        return None
    # Scale by N (number of categories) so the result is on the same
    # additive scale as the legacy sg_total = sum-of-components.
    n = len(parts)
    return n * sum(v * w for v, w in parts) / total_w


# ── Persisted fits (Stage 2) ───────────────────────────────────

_PROFILES_PATH = (Path(__file__).resolve().parent.parent.parent
                   / "data" / "golf" / "course_profiles.json")
_persisted_cache: dict[str, dict] | None = None


def _load_persisted_profiles() -> dict[str, dict]:
    """Read fitted per-course profiles. Returns {} when the file
    doesn't exist (Stage 1 hasn't been augmented yet)."""
    global _persisted_cache
    if _persisted_cache is not None:
        return _persisted_cache
    try:
        if _PROFILES_PATH.exists():
            _persisted_cache = json.loads(
                _PROFILES_PATH.read_text(encoding="utf-8"))
        else:
            _persisted_cache = {}
    except Exception as e:
        logger.warning("course_profiles: load failed: %s", e)
        _persisted_cache = {}
    return _persisted_cache


def reload() -> None:
    """Drop the persisted-profiles cache. Call after a refit run."""
    global _persisted_cache
    _persisted_cache = None


__all__ = [
    "get_archetype",
    "get_demand_weights",
    "weighted_sg",
    "reload",
]
