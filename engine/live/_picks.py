"""
Live pick generation — Phase 3b stage 1.

Reads a normalized game-state blob (from `engine.live._store.get_state`)
plus its attached HR live odds, runs the conditional predictor, and
returns a list of pick candidates with edge already scored.

Stage 1 markets:
  - NBA full-game ML  (home / away)
  - NBA full-game TOTAL  (over / under, primary line + alt totals)

Stage 2+ will add NBA quarters/halves and NHL periods.

Output shape (one entry per surfaced pick) mirrors the prematch picker
contract — consumers in `engine.live_tracker` (Phase 3c) pipe straight
into the snapshot tracker schema:

    {
      "sport":          "nba",
      "game_id":        "401869386",
      "matchup":        "POR @ SA",
      "bet_type":       "ML" | "TOTAL" | "ALT TOTAL",
      "pick":           "SA" | "Over 213.5" | "Under 215.5",
      "odds":           int (American),
      "model_prob":     float [0,1],
      "implied_prob":   float [0,1],
      "edge_pct":       float (model_prob - implied_prob, in %),
      "snapshot": {                  # frozen state at pick time
        "period":       int,
        "clock":        str,
        "home_score":   int,
        "away_score":   int,
        "remaining_s":  int,
      },
    }

Edge floor + odds cap reuse the prematch NBA gates from `engine.config`
(MAIN_EDGE_FLOOR['nba'] + MAIN_ODDS_CAP['nba']) — live picks should
clear at least the same bar as prematch since live odds carry less
information asymmetry. Live ALT lines stay gated at 12% pending a
calibration backtest with live history.

Why no prematch-prediction blend in stage 1: the live state alone
gives us enough signal for ML/TOTAL once the game is past ~15% elapsed
(roughly mid-Q1). For the cold-start window (tip-off + first 7 min) we
suppress live picks entirely — see `_MIN_ELAPSED_FRAC_FOR_PICKS`.
Stage 2 introduces a prematch_baseline blend so the picker can fire
during the cold-start window.
"""

from __future__ import annotations
import logging
from typing import Any

from ._predict import (
    predict_live_nba_full,
    predict_live_nba_period,
    predict_live_nhl_period,
    nhl_period_total_over_prob,
    nhl_period_bts_yes_prob,
    nhl_period_winner_probs,
    total_over_prob,
    margin_cover_prob,
)
from ._intermissions import _state_period_ended, _pbp_period_ended

logger = logging.getLogger(__name__)


# ── Intermission gate ─────────────────────────────────────────
#
# User directive 2026-04-30: live picks must be blocked until an
# intermission. Mid-period the lines are still moving, the model is
# integrating fresh PBP every poll, and posting picks that re-shuffle
# every 15 seconds is noise. At the buzzer of each period:
#   - HR re-prices full-game + next-period markets in a discrete jump
#   - The intermission predictor (5h NBA / 5i NHL) re-fits the
#     remaining-game distribution from the just-completed period
#   - PBP for the just-ended period is final, so the score state is
#     no longer a moving target
# Picks emitted in this window are stable for the duration of the
# intermission (~2 min NBA quarter, 15 min halftime, ~17 min NHL
# between periods). Once the next period starts, generate_live_picks
# returns [] again until the next boundary.

def _intermission_period(state: dict) -> int | None:
    """Return the period number that just ended (so the engine knows
    we're at an intermission boundary), or None when the game is in
    active play / hasn't started.

    Two signals:
      1. ``status.detail`` regex match — "End of 1st Quarter" /
         "Halftime" / "End of 1st Period". Authoritative when ESPN's
         state stream is fresh.
      2. PBP last-play type-text — fallback for when the state stream
         lags behind the buzzer by a poll cycle.

    Either alone is enough. Reuses the same matchers the
    ``live_intermissions`` detector uses so the gate and the trigger
    table stay in lockstep.
    """
    if not state:
        return None
    sport = (state.get("sport") or "").lower()
    period = _state_period_ended(state.get("status") or {}, sport)
    if period is None:
        period = _pbp_period_ended(state.get("plays") or [])
    if period is None or period <= 0:
        return None
    # Reject end-of-final-period (game over). Without this, NBA Q4
    # buzzer (or NHL P3 buzzer in regulation) would surface picks for
    # a game that's about to settle. NBA: regulation ends after Q4
    # (period == 4); NHL: after P3 (period == 3). Beyond that we're in
    # OT and the live picker doesn't price OT markets yet.
    final_period = 4 if sport == "nba" else 3 if sport == "nhl" else 99
    if period >= final_period:
        return None
    return period


# ── Thresholds (mirror prematch where they exist) ──

# Don't surface live picks until the game has elapsed enough that the
# observed pace + margin start dominating noise. ~15% of regulation =
# 7:12 elapsed = mid-Q1. Earlier than that, prematch predictions are
# strictly more informative; live picks would be re-discovering
# pre-tip information at lower confidence.
_MIN_ELAPSED_FRAC_FOR_PICKS = 0.15

# Edge floors — pulled live so engine.config tweaks apply without
# restart. Stage 1 uses the same gates as prematch NBA full-game.
def _edge_floor(bet_type: str) -> float:
    try:
        from ..config import MAIN_EDGE_FLOOR  # type: ignore
        nba = MAIN_EDGE_FLOOR.get("nba", {})
        if bet_type in nba:
            return float(nba[bet_type])
    except Exception:
        pass
    # Defaults if config import fails: 4% standard, 12% for alts/totals
    if bet_type in ("TOTAL", "ALT TOTAL", "ALT SPREAD"):
        return 12.0
    return 4.0


def _odds_cap(bet_type: str) -> int | None:
    try:
        from ..config import MAIN_ODDS_CAP  # type: ignore
        nba = MAIN_ODDS_CAP.get("nba", {})
        if bet_type in nba:
            return int(nba[bet_type])
    except Exception:
        pass
    return None


# Universal juice wall (-200), aligned with prematch player-prop
# PROP_JUICE_WALL = -200. Live engine inherits the same gate: chalks
# below -200 (e.g. BTS No -500, late-game ML -2000) produce too little
# profit per win for the calibration risk to justify them. A single
# unlucky variance event wipes multiple wins.
LIVE_ODDS_FLOOR = -200


def _odds_floor(_bet_type: str) -> int:
    """Minimum-allowed American price (most-negative). Currently
    sport/bet-type agnostic — Phase 4 may differentiate (e.g. NHL
    period BTS gets a tighter floor than NBA full-game ML)."""
    return LIVE_ODDS_FLOOR


# ── Helpers ──

def _implied_prob(american_odds: int) -> float:
    """Convert American odds to implied probability — same formula as
    engine.nba_picks._implied_prob, duplicated here to avoid circular
    imports between engine.live and engine.nba_picks."""
    if american_odds < 0:
        return abs(american_odds) / (abs(american_odds) + 100.0)
    return 100.0 / (american_odds + 100.0)


def _valid_odds(ml: Any) -> bool:
    if ml is None:
        return False
    try:
        ml = int(ml)
    except (TypeError, ValueError):
        return False
    return abs(ml) >= 100


def _build_pick(
    state: dict,
    bet_type: str,
    pick_text: str,
    odds: int,
    model_prob: float,
    pred: dict,
) -> dict:
    """Wrap a candidate into the standard live-pick dict, computing
    edge_pct + implied_prob. Returns ``None`` if the pick fails its
    edge floor or odds cap so callers can filter inline.

    Calibration + closing-line prior (#175 / #174, 2026-05-03):
    raw model_prob → empirical Bayesian shrinkage (per-sport
    ``{sport}_live`` buckets) → optional live-line prior toward
    market implied. Both stages dormant until live picks accumulate
    settled samples. Same shape as picks_core for prematch.
    """
    sport = (state.get("sport") or "").lower()
    implied = _implied_prob(odds)
    raw_prob = float(model_prob)

    # Belief gate — universal across sports + markets. Don't pick a
    # side the model expects to lose. Mirrors picks_core.score_pick
    # for prematch (feedback_be_right_first.md). Without this, the
    # live picker fired on dogs the model rated 26-42% just because
    # the calibrated edge cleared the floor — pure edge-chase against
    # the user's own "underdog ML only when model believes dog wins"
    # directive. 8 NBA live ML picks tripped this between 2026-05-04
    # and 2026-05-06 (LAL @ +700 at 0.26 prob, MIN @ +300 at 0.42, …).
    if raw_prob < 0.50:
        return None

    # Calibration — Bayesian beta-binomial against live's own buckets.
    # Zero settled rows = passthrough (returns raw). As live picks
    # settle, this starts shrinking model_prob toward realized rates
    # for similar (bet_type, prob_bucket) cells.
    cal_prob = raw_prob
    try:
        from ..empirical_calibration import calibrate as _cal
        cal_prob = float(_cal(bet_type, raw_prob,
                               sport=f"{sport}_live",
                               odds=odds, pick_text=pick_text))
    except Exception as exc:
        logger.debug("live calibration skipped (%s)", exc)

    # Closing-line / live-line Bayesian prior — anchor to market.
    # Gated by ``ENABLE_LIVE_LINE_PRIOR`` per-sport flag; defaults
    # off until weights are fitted. With cold-start w=1.0 this is a
    # no-op anyway.
    try:
        from ..config import get_flag as _gf
        if _gf("ENABLE_LIVE_LINE_PRIOR", False, sport=sport):
            from ..closing_line_prior import (
                learned_weight as _clp_w,
                posterior_prob as _clp_post,
            )
            w = _clp_w(f"{sport}_live", bet_type)
            cal_prob = float(_clp_post(cal_prob, implied, w))
    except Exception as exc:
        logger.debug("live-line prior skipped (%s)", exc)

    edge_pct = (cal_prob - implied) * 100.0

    floor = _edge_floor(bet_type)
    if edge_pct < floor:
        return None

    cap = _odds_cap(bet_type)
    if cap is not None and odds > cap:
        return None

    floor = _odds_floor(bet_type)
    if floor is not None and odds < floor:
        return None

    status = state.get("status") or {}
    # Universal stake-units recommendation — same ladder as prematch
    # picks via picks_core.score_pick. Live picks don't go through
    # that pipeline (different shape, single-stage calibration), so we
    # attach the unit recommendation explicitly here.
    try:
        from .._pick_helpers import stake_units_for as _stake
        _stake_u = _stake(cal_prob, edge_pct)
    except Exception:
        _stake_u = None
    return {
        "sport": state.get("sport"),
        "game_id": state.get("game_id"),
        "matchup": state.get("matchup"),
        "bet_type": bet_type,
        "pick": pick_text,
        "odds": int(odds),
        "model_prob": float(cal_prob),
        "model_prob_raw": float(raw_prob),
        "implied_prob": float(implied),
        "edge_pct": float(edge_pct),
        "stake_units": _stake_u,
        "snapshot": {
            "period": int(status.get("period") or 0),
            "clock": str(status.get("clock") or ""),
            "home_score": int((state.get("home") or {}).get("score") or 0),
            "away_score": int((state.get("away") or {}).get("score") or 0),
            "remaining_s": int(pred.get("remaining_s") or 0),
        },
    }


# ── NBA full-game stage 1 picks ──

def _fetch_prematch_nba_baseline(home_abbr: str, away_abbr: str
                                 ) -> tuple[float | None, float | None]:
    """Pull the prematch model's predicted total + margin for the
    matchup. Used as the team-strength prior in live blending so the
    live engine regresses toward what we expected this specific game
    to score, not the league average.

    Wraps `engine.nba_predict.predict_full` which is internally cached
    for ~30 min — so calling this every poll tick is cheap. Returns
    (None, None) on any failure (DB miss, scrape error, etc.); the
    live predictor falls back to league-average behaviour.
    """
    try:
        from ..nba_predict import predict_full
        pred = predict_full(home_abbr, away_abbr)
        if not isinstance(pred, dict):
            return None, None
        return (pred.get("predicted_total"),
                pred.get("predicted_margin"))
    except Exception as e:
        logger.debug(
            "prematch baseline lookup failed for %s @ %s: %s",
            away_abbr, home_abbr, e,
        )
        return None, None


def _generate_nba_full(state: dict) -> list[dict]:
    """ML + TOTAL picks for one in-progress NBA game."""
    home_abbr = (state.get("home") or {}).get("abbr") or "HOME"
    away_abbr = (state.get("away") or {}).get("abbr") or "AWAY"

    # Team-strength prior from the prematch model (cached ~30 min so
    # this is cheap on the 15s poll cadence). Live predictor blends
    # remaining-game scoring toward this instead of league average —
    # a slow-pace matchup like ORL/DET (prematch ~215) regresses to
    # 215, not the league avg 228.
    prematch_total, prematch_margin = _fetch_prematch_nba_baseline(
        home_abbr, away_abbr,
    )

    pred = predict_live_nba_full(state,
                                  prematch_total=prematch_total,
                                  prematch_margin=prematch_margin)
    if not pred:
        return []
    if pred["elapsed_frac"] < _MIN_ELAPSED_FRAC_FOR_PICKS:
        return []

    odds = state.get("odds") or {}
    out: list[dict] = []

    # ── ML ──
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")
    if _valid_odds(home_ml):
        cand = _build_pick(state, "ML", home_abbr, int(home_ml),
                           pred["home_win_prob"], pred)
        if cand:
            out.append(cand)
    if _valid_odds(away_ml):
        away_prob = 1.0 - pred["home_win_prob"]
        cand = _build_pick(state, "ML", away_abbr, int(away_ml),
                           away_prob, pred)
        if cand:
            out.append(cand)

    # ── TOTAL — primary line ──
    main_line = odds.get("over_under")
    over_odds = odds.get("over_odds")
    under_odds = odds.get("under_odds")
    if main_line is not None:
        try:
            line_f = float(main_line)
        except (TypeError, ValueError):
            line_f = None
        if line_f is not None:
            over_prob = total_over_prob(line_f, pred["total_mean"], pred["total_std"])
            under_prob = 1.0 - over_prob
            if _valid_odds(over_odds):
                cand = _build_pick(state, "TOTAL", f"Over {line_f}",
                                   int(over_odds), over_prob, pred)
                if cand:
                    out.append(cand)
            if _valid_odds(under_odds):
                cand = _build_pick(state, "TOTAL", f"Under {line_f}",
                                   int(under_odds), under_prob, pred)
                if cand:
                    out.append(cand)

    # ── ALT TOTALS ──
    for alt in odds.get("alt_totals") or []:
        if not isinstance(alt, dict):
            continue
        try:
            alt_line = float(alt.get("line"))
        except (TypeError, ValueError):
            continue
        over_prob = total_over_prob(alt_line, pred["total_mean"], pred["total_std"])
        under_prob = 1.0 - over_prob
        o = alt.get("over_odds")
        u = alt.get("under_odds")
        if _valid_odds(o):
            cand = _build_pick(state, "ALT TOTAL", f"Over {alt_line}",
                               int(o), over_prob, pred)
            if cand:
                out.append(cand)
        if _valid_odds(u):
            cand = _build_pick(state, "ALT TOTAL", f"Under {alt_line}",
                               int(u), under_prob, pred)
            if cand:
                out.append(cand)

    # ── SPREAD — primary ──
    home_pt = odds.get("home_spread_point")
    home_spread_odds = odds.get("home_spread_odds")
    away_spread_odds = odds.get("away_spread_odds")
    if home_pt is not None:
        try:
            home_pt_f = float(home_pt)
        except (TypeError, ValueError):
            home_pt_f = None
        if home_pt_f is not None:
            # Home covers when (home_score - away_score) + home_pt > 0
            # → final margin > -home_pt
            home_cover_prob = margin_cover_prob(
                -home_pt_f, pred["margin_mean"], pred["margin_std"])
            away_cover_prob = 1.0 - home_cover_prob
            if _valid_odds(home_spread_odds):
                pick_text = (f"{home_abbr} {home_pt_f:+g}".replace("+", "+")
                             .replace("-", "-"))
                cand = _build_pick(state, "SPREAD", pick_text,
                                   int(home_spread_odds), home_cover_prob, pred)
                if cand:
                    out.append(cand)
            if _valid_odds(away_spread_odds):
                away_pt_f = -home_pt_f
                pick_text = f"{away_abbr} {away_pt_f:+g}"
                cand = _build_pick(state, "SPREAD", pick_text,
                                   int(away_spread_odds), away_cover_prob, pred)
                if cand:
                    out.append(cand)

    return out


# ── NBA per-period picks (Q2/Q3/Q4 + H1/H2) ──

# Bet-type names per period code. These mirror the prematch tracker
# bet_type taxonomy so settlement logic in Phase 3c reuses existing
# code paths instead of branching on a new "live"/"prematch" flag.
_PERIOD_BET_TYPES: dict[str, dict[str, str]] = {
    "Q1": {"ml": "Q1 ML",  "spread": "Q1 SPREAD",  "total": "Q1 TOTAL",
           "alt_total": "Q1 ALT TOTAL", "alt_spread": "Q1 ALT SPREAD"},
    "Q2": {"ml": "Q2 ML",  "spread": "Q2 SPREAD",  "total": "Q2 TOTAL",
           "alt_total": "Q2 ALT TOTAL", "alt_spread": "Q2 ALT SPREAD"},
    "Q3": {"ml": "Q3 ML",  "spread": "Q3 SPREAD",  "total": "Q3 TOTAL",
           "alt_total": "Q3 ALT TOTAL", "alt_spread": "Q3 ALT SPREAD"},
    "Q4": {"ml": "Q4 ML",  "spread": "Q4 SPREAD",  "total": "Q4 TOTAL",
           "alt_total": "Q4 ALT TOTAL", "alt_spread": "Q4 ALT SPREAD"},
    "H1": {"ml": "1H ML",  "spread": "1H SPREAD",  "total": "1H TOTAL",
           "alt_total": "1H ALT TOTAL", "alt_spread": "1H ALT SPREAD"},
    "H2": {"ml": "2H ML",  "spread": "2H SPREAD",  "total": "2H TOTAL",
           "alt_total": "2H ALT TOTAL", "alt_spread": "2H ALT SPREAD"},
}


def _generate_nba_period(state: dict, code: str, period_odds: dict) -> list[dict]:
    """Generate live picks for one NBA quarter/half market dict."""
    pred = predict_live_nba_period(state, code)
    if not pred or pred.get("status") == "closed":
        return []

    # Future-regime gate (stage 2 limitation): for periods that haven't
    # started yet, the predictor returns margin_mean=0 with a small σ
    # because there's no team-strength prior. That makes any underdog
    # ML/SPREAD price look like a +9-12% edge purely from the juice
    # asymmetry — a fictitious edge. Skip ML + SPREAD for future
    # periods until Phase 4 wires in the prematch model output as a
    # margin prior. TOTAL stays enabled because future-period total
    # predictions use league-average pace (a meaningful prior).
    is_future = pred.get("status") == "future"

    out: list[dict] = []
    bt = _PERIOD_BET_TYPES[code]
    home_abbr = (state.get("home") or {}).get("abbr") or "HOME"
    away_abbr = (state.get("away") or {}).get("abbr") or "AWAY"

    total_mean = pred["total_mean"]
    total_std = pred["total_std"]
    margin_mean = pred["margin_mean"]
    margin_std = pred["margin_std"]

    # ── ML ── (skip for future-regime periods)
    if not is_future:
        home_ml = period_odds.get("home_ml")
        away_ml = period_odds.get("away_ml")
        if margin_std > 1e-6:
            home_win_prob = 1.0 - _norm_cdf_local(-margin_mean / margin_std)
        else:
            home_win_prob = 1.0 if margin_mean > 0 else (0.5 if margin_mean == 0 else 0.0)
        if _valid_odds(home_ml):
            cand = _build_pick(state, bt["ml"], home_abbr, int(home_ml),
                               home_win_prob, pred)
            if cand:
                out.append(cand)
        if _valid_odds(away_ml):
            cand = _build_pick(state, bt["ml"], away_abbr, int(away_ml),
                               1.0 - home_win_prob, pred)
            if cand:
                out.append(cand)

    # ── Primary TOTAL ──
    line = period_odds.get("over_under")
    over_o = period_odds.get("over_odds")
    under_o = period_odds.get("under_odds")
    if line is not None:
        try:
            line_f = float(line)
        except (TypeError, ValueError):
            line_f = None
        if line_f is not None:
            over_prob = total_over_prob(line_f, total_mean, total_std)
            under_prob = 1.0 - over_prob
            if _valid_odds(over_o):
                cand = _build_pick(state, bt["total"], f"Over {line_f}",
                                   int(over_o), over_prob, pred)
                if cand:
                    out.append(cand)
            if _valid_odds(under_o):
                cand = _build_pick(state, bt["total"], f"Under {line_f}",
                                   int(under_o), under_prob, pred)
                if cand:
                    out.append(cand)

    # ── ALT TOTALS ──
    for alt in period_odds.get("alt_totals") or []:
        if not isinstance(alt, dict):
            continue
        try:
            alt_line = float(alt.get("line"))
        except (TypeError, ValueError):
            continue
        over_prob = total_over_prob(alt_line, total_mean, total_std)
        under_prob = 1.0 - over_prob
        o = alt.get("over_odds")
        u = alt.get("under_odds")
        if _valid_odds(o):
            cand = _build_pick(state, bt["alt_total"], f"Over {alt_line}",
                               int(o), over_prob, pred)
            if cand:
                out.append(cand)
        if _valid_odds(u):
            cand = _build_pick(state, bt["alt_total"], f"Under {alt_line}",
                               int(u), under_prob, pred)
            if cand:
                out.append(cand)

    # ── Primary SPREAD ── (skip for future-regime periods, same
    # rationale as ML — no team-strength prior on margin yet)
    if not is_future:
        home_pt = period_odds.get("home_spread_point")
        home_so = period_odds.get("home_spread_odds")
        away_so = period_odds.get("away_spread_odds")
        if home_pt is not None:
            try:
                home_pt_f = float(home_pt)
            except (TypeError, ValueError):
                home_pt_f = None
            if home_pt_f is not None:
                home_cover = margin_cover_prob(-home_pt_f, margin_mean, margin_std)
                away_cover = 1.0 - home_cover
                if _valid_odds(home_so):
                    cand = _build_pick(state, bt["spread"],
                                       f"{home_abbr} {home_pt_f:+g}",
                                       int(home_so), home_cover, pred)
                    if cand:
                        out.append(cand)
                if _valid_odds(away_so):
                    away_pt_f = -home_pt_f
                    cand = _build_pick(state, bt["spread"],
                                       f"{away_abbr} {away_pt_f:+g}",
                                       int(away_so), away_cover, pred)
                    if cand:
                        out.append(cand)

    return out


def _norm_cdf_local(x: float) -> float:
    """Local copy to avoid coupling _picks → math import order."""
    import math
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _pick_direction(bet_type: str, pick_text: str) -> str:
    """Categorise a pick as one of {'over', 'under', 'home', 'away',
    'yes', 'no', 'unknown'}. Used by `_dedupe_by_direction` to collapse
    line-ladders (ALT TOTAL Over 226.5 / 225.5 / 224.5 / ...) down to
    one pick per (bet_type, direction).

    The direction signal comes from the pick text — bet_type alone
    doesn't tell us which side of the market the pick is on.
    """
    if not isinstance(pick_text, str):
        return "unknown"
    t = pick_text.strip().lower()
    if t.startswith("over "):
        return "over"
    if t.startswith("under "):
        return "under"
    if t in ("yes", "no"):
        return t
    # Spread-style picks like "SA -8.5" / "ORL +5.5" carry the team
    # token first. ML picks are bare team names ("SA", "DET"). Both
    # need the team-vs-home/away resolution which the live picker has
    # already done at edge-calc time — just key the dedupe on the
    # team token so Spread "SA -8.5" and "SA -7.5" collapse to one
    # pick.
    first = t.split()[0] if t.split() else ""
    return first or "unknown"


def _bet_type_family(bet_type: str) -> str:
    """Collapse primary + ALT bet types into a single dedupe family.

    User feedback (2026-04-30): "1H Total" and "1H Alt Total" are the
    same trade with slightly different lines/prices — surfacing both
    in the live tab feels redundant. Same for SPREAD / ALT SPREAD.
    The dedupe machinery already keeps only the highest-edge per
    (family, direction) pair, so collapsing the family here means the
    user sees one Over/Under per scope no matter how many alt lines
    HR ships.

    Examples::

        "TOTAL"          → "TOTAL"
        "ALT TOTAL"      → "TOTAL"
        "Q2 TOTAL"       → "Q2 TOTAL"
        "Q2 ALT TOTAL"   → "Q2 TOTAL"
        "1H TOTAL"       → "1H TOTAL"
        "1H ALT TOTAL"   → "1H TOTAL"
        "ALT SPREAD"     → "SPREAD"
        "Q3 ALT SPREAD"  → "Q3 SPREAD"
    """
    if not isinstance(bet_type, str):
        return ""
    bt = bet_type.strip()
    return bt.replace(" ALT ", " ").replace("ALT TOTAL", "TOTAL") \
             .replace("ALT SPREAD", "SPREAD")


# Per-game cap on surfaced picks. After collapsing primary + alt by
# (family, direction), 4 still gives a usable spread across markets
# (typically 1-2 totals + 1-2 spreads/MLs across active scopes) but
# stops the panel from feeling like a firehose. User directive
# 2026-04-30 — "11 picks for one game is too many".
MAX_PICKS_PER_GAME = 4


def _dedupe_by_direction(picks: list[dict]) -> list[dict]:
    """Two-stage trim:

      1. Collapse every (family, direction) duplicate to its highest-
         edge variant. Family normalises ALT and primary together
         (TOTAL vs ALT TOTAL, SPREAD vs ALT SPREAD).
      2. Cap the surviving list at MAX_PICKS_PER_GAME by edge.

    Stable sort by edge descending so the top-edge picks lead.
    """
    if not picks:
        return picks
    best_per_key: dict[tuple, dict] = {}
    for p in picks:
        family = _bet_type_family(p.get("bet_type", ""))
        direction = _pick_direction(p.get("bet_type", ""), p.get("pick", ""))
        key = (family, direction)
        cur = best_per_key.get(key)
        if cur is None or (p.get("edge_pct", 0) > cur.get("edge_pct", 0)):
            best_per_key[key] = p
    ranked = sorted(best_per_key.values(),
                    key=lambda p: -(p.get("edge_pct", 0)))
    return ranked[:MAX_PICKS_PER_GAME]


# ── NHL period picks (stage 3) ──
#
# Bet-type names mirror the existing prematch NHL tracker taxonomy
# ("Period Total", "Period BTS", "Period Winner") so settler logic in
# Phase 3c reuses the existing handlers. Each emitter takes the
# parsed period_odds dict from the bucket plus an in-progress NHL
# state and surfaces over/under/yes/no/winner picks.

def _generate_nhl_period(state: dict, period_num: int,
                         period_odds_total: dict | None,
                         period_odds_dnb: dict | None,
                         period_odds_bts: dict | None) -> list[dict]:
    """Generate NHL P{period_num} picks for the markets HR ships."""
    pred = predict_live_nhl_period(state, period_num)
    if not pred or pred.get("status") == "closed":
        return []

    # Future-regime gate (same rationale as NBA stage 2): without team-
    # strength priors we can still project totals (league-avg pace) and
    # BTS (rate × time independence), but the period winner / spread
    # would be 50/50-ish for any future period and produce spurious
    # underdog edges. Skip Period Winner for future regimes; Total +
    # BTS continue.
    is_future = pred.get("status") == "future"

    out: list[dict] = []
    home_abbr = (state.get("home") or {}).get("abbr") or "HOME"
    away_abbr = (state.get("away") or {}).get("abbr") or "AWAY"

    # ── Period Total (over/under N goals in the period) ──
    if period_odds_total:
        line = period_odds_total.get("line")
        over_o = period_odds_total.get("over_odds")
        under_o = period_odds_total.get("under_odds")
        try:
            line_f = float(line) if line is not None else None
        except (TypeError, ValueError):
            line_f = None
        if line_f is not None:
            over_prob = nhl_period_total_over_prob(line_f, pred)
            under_prob = 1.0 - over_prob
            bt = f"P{period_num} TOTAL"
            if _valid_odds(over_o):
                cand = _build_pick(state, bt, f"Over {line_f}",
                                   int(over_o), over_prob, pred)
                if cand:
                    out.append(cand)
            if _valid_odds(under_o):
                cand = _build_pick(state, bt, f"Under {line_f}",
                                   int(under_o), under_prob, pred)
                if cand:
                    out.append(cand)

    # ── Period BTS (yes/no — both teams score this period) ──
    if period_odds_bts:
        yes_o = period_odds_bts.get("yes_odds")
        no_o = period_odds_bts.get("no_odds")
        yes_prob = nhl_period_bts_yes_prob(pred)
        no_prob = 1.0 - yes_prob
        bt = f"P{period_num} BTS"
        if _valid_odds(yes_o):
            cand = _build_pick(state, bt, "Yes", int(yes_o), yes_prob, pred)
            if cand:
                out.append(cand)
        if _valid_odds(no_o):
            cand = _build_pick(state, bt, "No", int(no_o), no_prob, pred)
            if cand:
                out.append(cand)

    # ── Period Winner (3-way: home / tie / away) ──
    # Skipped for future periods (no team-strength prior). Tie price
    # not currently captured by the parser (period_dnb only holds
    # home/away ML), so we surface only home/away winner picks.
    if not is_future and period_odds_dnb:
        home_ml = period_odds_dnb.get("home_ml")
        away_ml = period_odds_dnb.get("away_ml")
        p_home, _p_tie, p_away = nhl_period_winner_probs(pred)
        bt = f"P{period_num} Winner"
        if _valid_odds(home_ml):
            cand = _build_pick(state, bt, home_abbr, int(home_ml),
                               p_home, pred)
            if cand:
                out.append(cand)
        if _valid_odds(away_ml):
            cand = _build_pick(state, bt, away_abbr, int(away_ml),
                               p_away, pred)
            if cand:
                out.append(cand)

    return out


# ── Public dispatcher ──

def conviction_score(pick: dict) -> float:
    """Per-pick conviction score. Confidence-only ranker — no edge term.

    Per "be right, not edge-first" directive (memory:
    feedback_be_right_first): the live ★ marker should highlight the
    pick we're most CONFIDENT about, not the one with the biggest
    apparent market disagreement. A miscalibrated 90%-prob pick can
    show a phantom +30% edge, but if the calibrator says reality is
    closer to 60%, the "edge" is fake. Ranking by confidence ties
    the star to where the model is genuinely sure.

    Score = prob². Squared so the gap between 0.65 and 0.85 weighs
    in. Picks below the edge floor are dropped before scoring (edge
    remains a FILTER, not a ranker), so a non-zero score implies the
    pick is at least +EV at the configured floor.

    Mirrors ``engine.pick_of_day._scoring._score_pick`` (sans the
    reliability/class factors — those are bet-type multipliers
    downstream consumers can layer on; the bare conviction stays
    here so post-settle analytics can re-derive any weighted variant).
    """
    prob = float(pick.get("model_prob") or 0.0)
    edge = float(pick.get("edge_pct") or 0.0)
    if prob <= 0 or edge <= 0:
        return 0.0
    return prob * prob


def _mark_best_per_game(picks: list[dict]) -> list[dict]:
    """Stamp ``is_best=True`` on the highest-conviction pick. Operates
    in-place but also returns the list for caller convenience.

    Picks are assumed to already be filtered to a single game (this
    runs inside generate_live_picks before the dispatcher returns).
    Ties broken by edge then odds (higher edge wins; among equal
    edges, the dog price wins because the variance trade tilts +EV).
    """
    if not picks:
        return picks
    best_idx = -1
    best_score = -1.0
    best_edge = -1.0
    best_odds = -10_000
    for i, p in enumerate(picks):
        s = conviction_score(p)
        e = float(p.get("edge_pct") or 0.0)
        o = int(p.get("odds") or 0)
        if (s > best_score
                or (s == best_score and e > best_edge)
                or (s == best_score and e == best_edge and o > best_odds)):
            best_idx = i
            best_score = s
            best_edge = e
            best_odds = o
        # Default everyone to False so re-runs don't keep a stale flag.
        p["is_best"] = False
        p["conviction_score"] = round(s, 4)
    if best_idx >= 0 and best_score > 0:
        picks[best_idx]["is_best"] = True
    return picks


def generate_live_picks(state: dict) -> list[dict]:
    """Dispatcher — returns live pick candidates for the given game.

    NBA: full-game ML/SPREAD/TOTAL + Q2/Q3/Q4 + H1/H2 markets.
    NHL: P2/P3 Period Total / Period BTS / Period Winner.

    **Intermission-only emission.** Picks fire only when the state is
    at an end-of-period boundary (between Q1/Q2/Q3 for NBA, between
    P1/P2 for NHL). Mid-period polls return ``[]`` because:
      - HR is still moving lines as the period unfolds; a pick taken
        at 8:42 in the 3rd is already stale by 8:30
      - The conditional predictor is integrating fresh PBP every tick
      - At the buzzer, HR re-prices in a discrete jump and the
        intermission predictor (5h NBA / 5i NHL) re-fits the
        remaining-game distribution; THAT'S the moment to pick

    Final pass: collapse line-ladders via `_dedupe_by_direction` so
    the user sees one pick per (bet_type, direction) instead of five
    near-identical ALT TOTAL Overs. Then mark the highest-conviction
    survivor with ``is_best=True`` so the UI can star it.
    """
    sport = (state.get("sport") or "").lower()
    if sport not in ("nba", "nhl"):
        return []

    # Intermission gate — block emission during active play.
    if _intermission_period(state) is None:
        return []

    if sport == "nba":
        out = _generate_nba_full(state)
        odds = state.get("odds") or {}
        periods = odds.get("periods") or {}
        for code in ("Q2", "Q3", "Q4", "H1", "H2"):
            period_odds = periods.get(code)
            if not period_odds:
                continue
            out.extend(_generate_nba_period(state, code, period_odds))
        return _mark_best_per_game(_dedupe_by_direction(out))

    # NHL
    out: list[dict] = []
    odds = state.get("odds") or {}
    period_totals = odds.get("period_totals") or {}
    period_dnb    = odds.get("period_dnb") or {}
    period_bts    = odds.get("period_bts") or {}
    for pn in (1, 2, 3):
        key = f"P{pn}"
        tot = period_totals.get(key)
        dnb = period_dnb.get(key)
        bts = period_bts.get(key)
        if not (tot or dnb or bts):
            continue
        out.extend(_generate_nhl_period(state, pn, tot, dnb, bts))
    return _mark_best_per_game(_dedupe_by_direction(out))
