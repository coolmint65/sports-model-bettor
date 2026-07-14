"""
Tennis match predictor.

Reads per-surface Elo ratings (engine.tennis_elo) and emits match
probabilities for any (p1, p2, surface, best_of) request.

Factors (per ``factors=noise`` directive — see same-named memory):

1. **Surface Elo gap** — primary signal. Use surface-specific rating
   when both players have ≥ 5 matches on that surface; fall back to
   'all' rating otherwise.

2. **Recent form** — small adjustment from last-N-matches win
   percentage. Capped at ±25 rating points so a hot streak doesn't
   override the underlying skill prior.

3. **Fatigue** — penalty for back-to-back matches and accumulated
   minutes-played in the last 7 days. Modest effect (±20 rating
   points) but documented in tennis Elo literature.

NOT in:

- Head-to-head priors (separate module if needed; small sample on
  most pairs)
- Hand matchup (R-vs-L) — Elo absorbs over time
- Age curve — drift in the rating absorbs it
- Tournament prestige — already in the K-factor at training time
- Weather — outdoor/indoor surface split is enough granularity

Public API::

    predict_match(tour, p1_id, p2_id, *, surface, best_of=3, date=None)
        -> dict

Returns ``{p1_win_prob, p2_win_prob, p1_rating, p2_rating, surface_used,
factors}`` where the factors block lists the rating points contributed
by each lever (for explainability in the UI).
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
from typing import Any
from ._tz import et_today_str

logger = logging.getLogger(__name__)


# ── Constants ──────────────────────────────────────────────────

# Surface-specific rating used only when player has at least this
# many matches on that surface; otherwise fall back to 'all'.
MIN_SURFACE_MATCHES = 5

# Recent-form adjustment caps: how many rating points a hot or cold
# streak can move the prediction. Conservative because Elo already
# absorbs form over time — recency is just a smoothing layer.
RECENT_FORM_LOOKBACK = 10
RECENT_FORM_CAP_POINTS = 25.0

# Fatigue inputs
FATIGUE_LOOKBACK_DAYS = 7
FATIGUE_CAP_POINTS = 20.0
# Minutes per match scales: a typical BO3 is 90-120 min, BO5 is
# 150-200. We penalize 1 rating point per ~5 min played in the last
# week, capped at FATIGUE_CAP_POINTS.
FATIGUE_PER_MIN = 0.20

# Health / load signals that Elo doesn't capture
# ────────────────────────────────────────────────────────────────
# Sackmann's score field encodes outcomes Elo throws away: walkovers
# given (W/O), mid-match retirements (RET), defaults (DEF). These are
# the kinds of signals HR has and we don't — a player who walked over
# yesterday is hurt today; a player with three RETs in their last 10
# is probably battling something chronic.
#
# We treat each as a rating-point penalty. Caps stay conservative —
# Elo already absorbs persistent issues over time, so this is a
# recency layer for things that haven't yet bled into the rating.

# Walkovers GIVEN (player withdrew before the match — strong injury signal)
HEALTH_WO_LOOKBACK = 10
HEALTH_WO_CAP_POINTS = 35.0
HEALTH_WO_PER_EVENT = 18.0   # one W/O in last 10 = -18, two = -35 (capped)

# Mid-match retirements (player retired during play — also injury signal,
# but slightly weaker than W/O since they at least started)
HEALTH_RET_LOOKBACK = 10
HEALTH_RET_CAP_POINTS = 25.0
HEALTH_RET_PER_EVENT = 12.0

# Match-density penalty — total matches in last 14 days. Already-counted
# in FATIGUE (minutes), but density catches the case where a player is
# playing every other day at sub-tour level (where minutes/match are
# shorter so the minute-based fatigue under-counts).
DENSITY_LOOKBACK_DAYS = 14
DENSITY_THRESHOLD = 6        # matches above this trigger penalty
DENSITY_PER_OVER = 4.0
DENSITY_CAP_POINTS = 20.0


# BO5 carries a meaningful rating-gap amplification because more sets
# reduces variance. Computed on the fly via per-set win-prob inversion
# rather than a hardcoded multiplier.


# ── Rating retrieval ───────────────────────────────────────────

SURFACE_SHRINK_K = 150


def _rating_block(tour: str, player_id: int, surface: str) -> dict:
    """Return ``{rating, rd, surface_used, matches}`` after surface
    fallback + Bayesian shrinkage toward the all-surface rating.

    Pure surface Elo over-specializes when a player's surface sample
    is small relative to their cross-surface body of work. The
    canonical case: Sinner Clay 1786 (79 matches) vs Zverev Clay
    1898 (208) — pure surface says Zverev is the clay favorite, but
    Sinner's all-rating is 2141 (vs Zverev 1970) reflecting much
    stronger overall current skill. Market priced Sinner -600 / Zverev
    +400 for their Madrid 2026 final; our pure-surface model said
    Zverev 62% and emitted +41pp Zverev ML edge.

    Shrinkage formula::

        w = matches / (matches + SURFACE_SHRINK_K)
        eff_rating = w * surface_rating + (1 - w) * all_rating
        eff_rd     = w * surface_rd     + (1 - w) * all_rd

    With SURFACE_SHRINK_K=100, a player with 100 surface matches
    gets a 50/50 blend; 200 matches → 67/33 toward surface; 25
    matches → 20/80 toward all. This preserves clay-specialist
    signal at high volume while preventing surface drift from
    overriding overall skill at low volume.
    """
    from .tennis_elo import rating_for, _normalize_surface
    surf = _normalize_surface(surface)
    r_surf = rating_for(tour, player_id, surface=surf, fallback_to_all=False)
    r_all = rating_for(tour, player_id, surface="all", fallback_to_all=False)

    if r_surf and (r_surf.get("matches") or 0) >= MIN_SURFACE_MATCHES \
            and r_all:
        # Both available — blend.
        m_surf = float(r_surf.get("matches") or 0)
        w = m_surf / (m_surf + SURFACE_SHRINK_K)
        eff_rating = w * r_surf["rating"] + (1.0 - w) * r_all["rating"]
        eff_rd     = w * r_surf["rd"]     + (1.0 - w) * r_all["rd"]
        return {
            "rating": eff_rating,
            "rd": eff_rd,
            "surface_used": surf,
            "matches": int(r_surf.get("matches") or 0),
        }
    # Fall back to 'all'
    if r_all:
        return {"rating": r_all["rating"], "rd": r_all["rd"],
                "surface_used": "all (cold-start surface)",
                "matches": r_all["matches"]}
    # Unknown player — use init values
    from .tennis_elo import INIT_RATING, INIT_RD
    return {"rating": INIT_RATING, "rd": INIT_RD,
            "surface_used": "unrated", "matches": 0}


# ── Form + fatigue ─────────────────────────────────────────────

def _recent_form_adjustment(tour: str, player_id: int,
                              cutoff_date: str) -> float:
    """Last RECENT_FORM_LOOKBACK matches for the player before
    ``cutoff_date``. Returns rating-points delta (negative when
    underperforming, positive when overperforming).

    A hot 8-of-10 player gets +20 pts; a 2-of-10 cold streak gets -20.
    Anything mid-range (4-7 wins) stays close to 0.
    """
    from .tennis_db import get_conn
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT winner_id, loser_id FROM tennis_matches
         WHERE tour = ? AND tourney_date < ?
           AND (winner_id = ? OR loser_id = ?)
         ORDER BY tourney_date DESC, match_id DESC
         LIMIT ?
        """,
        (tour, cutoff_date, int(player_id), int(player_id),
         RECENT_FORM_LOOKBACK),
    ).fetchall()
    if not rows:
        return 0.0
    wins = sum(1 for r in rows if int(r["winner_id"]) == int(player_id))
    n = len(rows)
    if n < 3:  # too small to mean anything
        return 0.0
    win_rate = wins / n
    # Map [0.0, 1.0] win-rate → [-CAP, +CAP] linearly around 0.5
    delta = (win_rate - 0.5) * 2 * RECENT_FORM_CAP_POINTS
    return max(-RECENT_FORM_CAP_POINTS, min(RECENT_FORM_CAP_POINTS, delta))


def _health_adjustment(tour: str, player_id: int,
                        cutoff_date: str) -> tuple[float, dict]:
    """Penalty for recent walkovers GIVEN + mid-match retirements.
    Returns ``(rating_delta, breakdown)`` so the predictor can
    surface why a player got marked down.

    Sackmann convention:
      - W/O: ``loser_id`` is the player who withdrew → score == 'W/O'
      - RET: ``loser_id`` is the player who retired during the match
             → score contains 'ret' or 'RET'
      - DEF: same as W/O (administrative variant) → score == 'DEF'
    """
    from .tennis_db import get_conn
    conn = get_conn()
    lookback = max(HEALTH_WO_LOOKBACK, HEALTH_RET_LOOKBACK)
    rows = conn.execute(
        "SELECT winner_id, loser_id, score FROM tennis_matches "
        "WHERE tour = ? AND tourney_date < ? "
        "  AND (winner_id = ? OR loser_id = ?) "
        "ORDER BY tourney_date DESC, match_id DESC LIMIT ?",
        (tour, cutoff_date, int(player_id), int(player_id), lookback),
    ).fetchall()
    n_wo = n_ret = 0
    for r in rows:
        if int(r["loser_id"]) != int(player_id):
            continue  # only count when this player was the one who didn't finish
        score = (r["score"] or "").strip().upper()
        if score in ("W/O", "DEF") or score.startswith("W/O") or score.startswith("DEF"):
            n_wo += 1
        elif "RET" in score:
            n_ret += 1
    wo_penalty = min(HEALTH_WO_CAP_POINTS, n_wo * HEALTH_WO_PER_EVENT)
    ret_penalty = min(HEALTH_RET_CAP_POINTS, n_ret * HEALTH_RET_PER_EVENT)
    total = -(wo_penalty + ret_penalty)
    return total, {"walkovers": n_wo, "retirements": n_ret}


def _density_adjustment(tour: str, player_id: int,
                         cutoff_date: str) -> tuple[float, int]:
    """Match density penalty. Counts how many matches the player has
    played in the last DENSITY_LOOKBACK_DAYS. Above DENSITY_THRESHOLD,
    each extra match docks DENSITY_PER_OVER points (capped).

    Catches sub-tour cases where a player wins multiple short matches
    per day at a Futures qualifying — minutes-based fatigue
    under-counts because each match is short."""
    from .tennis_db import get_conn
    try:
        cur_dt = datetime.strptime(cutoff_date, "%Y-%m-%d")
    except ValueError:
        return 0.0, 0
    since = (cur_dt - timedelta(days=DENSITY_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    conn = get_conn()
    n = conn.execute(
        "SELECT COUNT(*) FROM tennis_matches "
        "WHERE tour = ? AND tourney_date >= ? AND tourney_date < ? "
        "  AND (winner_id = ? OR loser_id = ?)",
        (tour, since, cutoff_date, int(player_id), int(player_id)),
    ).fetchone()[0]
    over = max(0, int(n) - DENSITY_THRESHOLD)
    penalty = min(DENSITY_CAP_POINTS, over * DENSITY_PER_OVER)
    return -penalty, int(n)


def _fatigue_adjustment(tour: str, player_id: int,
                         cutoff_date: str) -> float:
    """Sum of minutes played in the last FATIGUE_LOOKBACK_DAYS days
    converted to a rating penalty. Never bonuses (a rested player
    isn't 'positively rated'; they're at baseline)."""
    from .tennis_db import get_conn
    try:
        cur_dt = datetime.strptime(cutoff_date, "%Y-%m-%d")
    except ValueError:
        return 0.0
    since = (cur_dt - timedelta(days=FATIGUE_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT minutes FROM tennis_matches
         WHERE tour = ? AND tourney_date >= ? AND tourney_date < ?
           AND (winner_id = ? OR loser_id = ?)
        """,
        (tour, since, cutoff_date, int(player_id), int(player_id)),
    ).fetchall()
    total_min = sum((r["minutes"] or 0) for r in rows)
    if total_min <= 0:
        return 0.0
    penalty = min(FATIGUE_CAP_POINTS, total_min * FATIGUE_PER_MIN)
    return -penalty


# ── Match probability ─────────────────────────────────────────

def _elo_win_prob(r_a: float, r_b: float) -> float:
    """Standard Elo win probability. Match-level (calibrated against
    match outcomes during training). Used as the BO3-equivalent base
    when both ratings are reliable.
    """
    return 1.0 / (1.0 + 10.0 ** ((r_b - r_a) / 400.0))


def _glicko_win_prob(r_a: float, r_b: float,
                     rd_a: float, rd_b: float) -> float:
    """RD-weighted win probability (Glicko-style g(RD) shrinkage).

    Standard Elo treats both ratings as exact. When one player's
    rating is uncertain (high RD — typical for new / sub-tour /
    low-volume players), the rating gap should regress toward 0.

    The Zverev-vs-Blockx anomaly was the canonical case: Zverev rated
    1898 (RD 30, 208 matches) vs Blockx rated 1538 (RD ~150, 13
    matches) → standard Elo gives Zverev 88% from the 360-point gap.
    Glicko shrinkage drops it to ~85% at this RD spread, and gets
    much more aggressive (~70%) when the cold-start side has RD 250+.

    Formula (Glickman 1995): p = 1 / (1 + 10^(-g(RD_combined) * (r_a - r_b) / 400))
    where g(RD) = 1 / sqrt(1 + 3 * q² * RD² / π²) and q = ln(10)/400.
    """
    import math
    q = math.log(10) / 400.0
    rd_combined = math.sqrt(rd_a * rd_a + rd_b * rd_b)
    g = 1.0 / math.sqrt(1.0 + 3.0 * (q * rd_combined / math.pi) ** 2)
    exponent = -g * (r_a - r_b) / 400.0
    return 1.0 / (1.0 + 10.0 ** exponent)


def _per_set_prob_from_match_bo3(p_match: float) -> float:
    """Invert ``p_match = p_set² × (3 - 2*p_set)`` (probability of
    winning best-of-3 given per-set win prob) to get p_set.

    No closed form; bisect over [0.5, 1] when p_match >= 0.5 and
    [0, 0.5] when p_match < 0.5. p_match=0.5 iff p_set=0.5.
    """
    if p_match <= 0.0:
        return 0.0
    if p_match >= 1.0:
        return 1.0
    target = p_match
    lo, hi = (0.5, 0.999) if target >= 0.5 else (0.001, 0.5)
    for _ in range(50):
        mid = (lo + hi) / 2.0
        f = mid * mid * (3.0 - 2.0 * mid)
        if abs(f - target) < 1e-6:
            return mid
        if f < target:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def _match_prob_bo5(p_set: float) -> float:
    """Probability of winning best-of-5 given per-set win prob."""
    p = p_set
    return p ** 3 * (10.0 - 15.0 * p + 6.0 * p * p)


def _convert_match_prob(p_match_bo3: float, best_of: int) -> float:
    """Convert a BO3 match prob to BO5, or return BO3 unchanged.

    Elo training treats every match as one observation regardless of
    sets, so the rating-derived prob is implicitly BO3-shaped (most
    tour matches). For BO5 (Slams men's), invert per-set then expand.
    """
    if best_of == 5:
        p_set = _per_set_prob_from_match_bo3(p_match_bo3)
        return _match_prob_bo5(p_set)
    return p_match_bo3


# ── Public API ─────────────────────────────────────────────────

def predict_match(tour: str, p1_id: int, p2_id: int, *,
                   surface: str = "Hard", best_of: int = 3,
                   date: str | None = None) -> dict:
    """Predict outcome of a single match.

    Args:
        tour: 'atp' | 'wta'
        p1_id / p2_id: Sackmann player ids
        surface: 'Hard' | 'Clay' | 'Grass' | 'Carpet'
        best_of: 3 or 5
        date: lookup date for form / fatigue (defaults to today).
              Backtest pass it explicitly.

    Returns::

        {
          'p1_win_prob': float, 'p2_win_prob': float,
          'p1_rating': float, 'p2_rating': float,
          'surface_used': str,
          'best_of': int,
          'factors': {
            'elo_gap': float,
            'p1_form_delta': float, 'p2_form_delta': float,
            'p1_fatigue_delta': float, 'p2_fatigue_delta': float,
          }
        }
    """
    if tour not in ("atp", "wta"):
        raise ValueError(f"unknown tour: {tour!r}")
    if best_of not in (3, 5):
        raise ValueError(f"best_of must be 3 or 5, got {best_of}")
    cutoff = date or et_today_str()

    p1 = _rating_block(tour, p1_id, surface)
    p2 = _rating_block(tour, p2_id, surface)

    # Form + fatigue + health adjustments (rating-point deltas added
    # to each player's pre-match rating)
    p1_form = _recent_form_adjustment(tour, p1_id, cutoff)
    p2_form = _recent_form_adjustment(tour, p2_id, cutoff)
    p1_fatigue = _fatigue_adjustment(tour, p1_id, cutoff)
    p2_fatigue = _fatigue_adjustment(tour, p2_id, cutoff)
    p1_health, p1_health_break = _health_adjustment(tour, p1_id, cutoff)
    p2_health, p2_health_break = _health_adjustment(tour, p2_id, cutoff)
    p1_density, p1_match_count = _density_adjustment(tour, p1_id, cutoff)
    p2_density, p2_match_count = _density_adjustment(tour, p2_id, cutoff)

    p1_eff = p1["rating"] + p1_form + p1_fatigue + p1_health + p1_density
    p2_eff = p2["rating"] + p2_form + p2_fatigue + p2_health + p2_density

    # RD-weighted win prob — see _glicko_win_prob. When either player
    # has high rating uncertainty (low match count), the rating gap
    # shrinks toward 0. Without this, low-volume opponents like
    # Blockx (RD ~150, 13 matches) inherit too much win-prob extremity
    # from a fully-rated opponent like Zverev (RD ~30, 208 matches),
    # and the model emits +20-40% phantom edges against an informed
    # market. Form / fatigue / health / density adjustments still
    # apply on top of the RD-weighted base because those are concrete
    # observed signals (not rating uncertainty).
    p1_match_bo3 = _glicko_win_prob(p1_eff, p2_eff, p1["rd"], p2["rd"])
    p1_match = _convert_match_prob(p1_match_bo3, best_of)
    p2_match = 1.0 - p1_match

    surface_used = (p1["surface_used"]
                    if p1["surface_used"] == p2["surface_used"]
                    else f"{p1['surface_used']} / {p2['surface_used']}")

    # Elo-only base prediction
    base = {
        "tour": tour,
        "p1_id": int(p1_id), "p2_id": int(p2_id),
        "surface": surface,
        "surface_used": surface_used,
        "best_of": best_of,
        "p1_rating": round(p1["rating"], 1),
        "p2_rating": round(p2["rating"], 1),
        "p1_rd": round(p1["rd"], 1),
        "p2_rd": round(p2["rd"], 1),
        "p1_matches": int(p1.get("matches") or 0),
        "p2_matches": int(p2.get("matches") or 0),
        "p1_win_prob_elo": round(p1_match, 4),
        "p2_win_prob_elo": round(p2_match, 4),
        "p1_win_prob": round(p1_match, 4),
        "p2_win_prob": round(p2_match, 4),
        "factors": {
            "elo_gap": round(p1["rating"] - p2["rating"], 1),
            "p1_form_delta": round(p1_form, 1),
            "p2_form_delta": round(p2_form, 1),
            "p1_fatigue_delta": round(p1_fatigue, 1),
            "p2_fatigue_delta": round(p2_fatigue, 1),
            # Health: walkovers given + retirements in last 10 matches.
            "p1_health_delta": round(p1_health, 1),
            "p2_health_delta": round(p2_health, 1),
            "p1_walkovers_l10": p1_health_break.get("walkovers", 0),
            "p1_retirements_l10": p1_health_break.get("retirements", 0),
            "p2_walkovers_l10": p2_health_break.get("walkovers", 0),
            "p2_retirements_l10": p2_health_break.get("retirements", 0),
            # Density: matches in last 14 days (cumulative load proxy).
            "p1_density_delta": round(p1_density, 1),
            "p2_density_delta": round(p2_density, 1),
            "p1_matches_l14d": p1_match_count,
            "p2_matches_l14d": p2_match_count,
        },
    }

    # Blend in GBM when an artifact is available. Weight reduced from
    # 0.40 → 0.30 on 2026-05-02 after Sinner-Zverev anomaly: GBM
    # trained on Sackmann's 25-year corpus carries the same
    # career-long surface-specialization bias the new Elo shrinkage
    # corrects (GBM said Zverev 74% on clay, Elo post-shrinkage said
    # Sinner 59%, market said Sinner 86%). Until the GBM is retrained
    # on a time-decayed corpus, lean more on Elo.
    try:
        from .gbm.predict import predict_tennis as _gbm_predict, is_available
        if is_available(f"tennis_{tour}"):
            gbm = _gbm_predict(tour, p1_id, p2_id,
                                surface=surface, best_of=best_of,
                                date=cutoff)
            if gbm and "p1_win" in gbm and not isinstance(gbm["p1_win"], dict):
                p1_gbm = float(gbm["p1_win"])
                blended = 0.7 * p1_match + 0.3 * p1_gbm
                base["p1_win_prob_gbm"] = round(p1_gbm, 4)
                base["p2_win_prob_gbm"] = round(1.0 - p1_gbm, 4)
                base["p1_win_prob"] = round(blended, 4)
                base["p2_win_prob"] = round(1.0 - blended, 4)
                # Surface secondary GBM outputs for total_games / sets.
                if "total_games" in gbm and not isinstance(gbm["total_games"], dict):
                    base["expected_total_games"] = float(gbm["total_games"])
                if "straight_sets" in gbm and not isinstance(gbm["straight_sets"], dict):
                    base["straight_sets_prob"] = float(gbm["straight_sets"])
    except Exception as e:
        logger.debug("Tennis GBM blend skipped: %s", e)

    return base


def predict_match_by_name(tour: str, p1_name: str, p2_name: str,
                           **kwargs) -> dict | None:
    """Convenience wrapper that resolves names → ids before predicting.
    Returns None when either player isn't found in the local DB."""
    from .tennis_db import get_player_by_name
    p1 = get_player_by_name(tour, p1_name)
    p2 = get_player_by_name(tour, p2_name)
    if not p1 or not p2:
        logger.debug("predict_match_by_name: missing player(s) "
                     "%r=%s, %r=%s", p1_name, bool(p1),
                     p2_name, bool(p2))
        return None
    return predict_match(tour, p1["player_id"], p2["player_id"], **kwargs)


__all__ = ["predict_match", "predict_match_by_name"]
