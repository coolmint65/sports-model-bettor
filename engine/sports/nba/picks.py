"""
NBA Q1 bet selection.

Separated from nba_q1_predict.py to keep game prediction and market
decision-making in distinct modules. This file converts Q1 model
probabilities into concrete betting picks by comparing against
real odds and applying edge/priority filtering.

Used by: NBA Best Bets, NBA Q1 Pick Tracker.
"""

import logging

from ...config import NBA_BET_RELIABILITY

logger = logging.getLogger(__name__)


def _implied_prob(american_odds: int) -> float:
    """Convert American odds to implied probability."""
    if american_odds < 0:
        return abs(american_odds) / (abs(american_odds) + 100)
    return 100 / (american_odds + 100)


def _valid_odds(ml) -> bool:
    """|ml| >= 100 shape check. See engine/picks.py for rationale."""
    if ml is None:
        return False
    try:
        ml = int(ml)
    except (TypeError, ValueError):
        return False
    return abs(ml) >= 100


def _sanitize_odds(odds: dict | None) -> dict:
    """Null out _ml / _odds fields that aren't valid American prices."""
    if not odds:
        return {}
    cleaned = dict(odds)
    for k, v in list(cleaned.items()):
        if (k.endswith("_ml") or k.endswith("_odds")) and not _valid_odds(v):
            cleaned[k] = None
    return cleaned


def generate_q1_picks(home_abbr: str, away_abbr: str,
                      odds: dict | None = None,
                      season: int | None = None,
                      pred: dict | None = None) -> list[dict]:
    """Generate Q1 spread, Q1 total, and Q1 ML picks with edges.

    Args:
        home_abbr: Home team abbreviation
        away_abbr: Away team abbreviation
        odds: Optional dict with Q1 odds
        season: Season year override
        pred: Optional pre-computed factor prediction (may carry mc / gbm
            subkeys). When provided we skip the inline predict_q1 call so
            ensemble_nba() blends factor + MC + GBM rather than re-running
            the factor model.

    Returns:
        List of pick dicts sorted by priority then edge.
    """
    from .q1_predict import predict_q1

    odds = _sanitize_odds(odds)
    q1_spread = odds.get("q1_spread")
    q1_total = odds.get("q1_total")

    if pred is None:
        pred = predict_q1(home_abbr, away_abbr,
                          spread=q1_spread, total=q1_total, season=season)

    # Compute the ensemble blend. pred may already carry factor + MC +
    # GBM (when the backend helper ran them); if only factor is present
    # ensemble_nba collapses to factor-only.
    try:
        from ...ensemble import ensemble_nba
        pred["ensemble"] = ensemble_nba(pred)
    except Exception as e:
        logger.debug("NBA ensemble blend failed: %s", e)
        pred["ensemble"] = {}

    # Route Q1 ML through the ensemble when present. Spread cover
    # and total over/under distributions stay on the factor model
    # (ensemble only blends scalar home_win + total_expected).
    ens = pred.get("ensemble") or {}
    if ens.get("q1_home_win") is not None:
        pred["q1_ml_home"] = float(ens["q1_home_win"])
        pred["q1_ml_away"] = 1.0 - float(ens["q1_home_win"])

    picks = []
    from ...config import NBA_JUICE_WALL as JUICE_WALL

    # ── Q1 Spread ──  Migrated 2026-05-02 to picks_core.score_pick.
    if q1_spread is not None:
        h_spread_odds = odds.get("q1_spread_home_odds", -110)
        a_spread_odds = odds.get("q1_spread_away_odds", -110)
        cover_prob = pred["spread_cover_prob"]
        if cover_prob is not None:
            from ...picks_core import score_pick as _score_pick
            for side_label, prob, side_odds in [
                (f"{home_abbr} {q1_spread:+.1f} Q1", cover_prob, h_spread_odds),
                (f"{away_abbr} {-q1_spread:+.1f} Q1", 1 - cover_prob, a_spread_odds),
            ]:
                scored = _score_pick({
                    "type": "Q1_SPREAD",
                    "pick": side_label,
                    "raw_prob": prob,
                    "odds": side_odds,
                }, sport="nba", juice_wall=JUICE_WALL)
                if scored:
                    picks.append(scored)

    # ── Q1 Total ──  Migrated to picks_core.score_pick.
    if q1_total is not None:
        over_odds = odds.get("q1_over_odds", -110)
        under_odds = odds.get("q1_under_odds", -110)
        over_prob = pred["over_prob"]
        if over_prob is not None:
            from ...picks_core import score_pick as _score_pick
            for label, prob, side_odds in [
                (f"Over {q1_total} Q1", over_prob, over_odds),
                (f"Under {q1_total} Q1", 1 - over_prob, under_odds),
            ]:
                scored = _score_pick({
                    "type": "Q1_TOTAL",
                    "pick": label,
                    "raw_prob": prob,
                    "odds": side_odds,
                }, sport="nba", juice_wall=JUICE_WALL)
                if scored:
                    picks.append(scored)

    # ── Q1 Moneyline ──
    # Q1-specific ML odds ONLY. The previous code used
    # `q1_home_ml or home_ml` — falling back to full-game ML when
    # HR dropped the Q1 market (which it does as games approach
    # tip-off). That fallback compares full-game implied prob
    # against the Q1 model's tighter probability and produces
    # phantom edges. User report 2026-04-28: POR Q1 ML appeared at
    # +425 (POR's full-game underdog price) against the Q1 model's
    # 24% — bogus 5% edge because the markets aren't comparable.
    # When Q1 ML isn't on the board, don't generate Q1_ML at all.
    home_ml_odds = odds.get("q1_home_ml")
    away_ml_odds = odds.get("q1_away_ml")

    # Migrated 2026-05-02 to engine.picks_core.score_pick.
    from ...picks_core import score_pick as _score_pick
    if home_ml_odds is not None:
        scored = _score_pick({
            "type": "Q1_ML",
            "pick": f"{home_abbr} Q1 ML",
            "raw_prob": pred["q1_ml_home"],
            "odds": home_ml_odds,
        }, sport="nba", juice_wall=JUICE_WALL)
        if scored:
            picks.append(scored)
    if away_ml_odds is not None:
        scored = _score_pick({
            "type": "Q1_ML",
            "pick": f"{away_abbr} Q1 ML",
            "raw_prob": pred["q1_ml_away"],
            "odds": away_ml_odds,
        }, sport="nba", juice_wall=JUICE_WALL)
        if scored:
            picks.append(scored)

    # ── Phase 1 derivatives ──
    # Q1 team totals (Gaussian tail off home/away_q1_expected) and Q1
    # total odd/even (sum total_probs over odd vs even). All pure
    # probability extraction — no factor stacking.
    from .derivative_picks import append_derivative_picks
    append_derivative_picks(picks, pred, odds, home_abbr, away_abbr)

    # ── Q1 Alt Line Shopping ──
    # Check Q1 alt spreads and totals for better edges than primary.
    # Prefer the exact discretized-Gaussian distributions exposed by
    # nba_q1_predict (margin_probs / total_probs) over ad-hoc CDF /
    # logistic approximations — the latter drifted ~5-8% at alt lines
    # more than 1 std away from the model spread.
    predicted_total = pred.get("predicted_total", 0)
    predicted_margin = pred.get("predicted_margin", 0)
    margin_probs = pred.get("margin_probs") or {}
    total_probs_dist = pred.get("total_probs") or {}
    q1_std = 8.67  # Q1 scoring margin std dev (calibrated from 2,714 games)

    q1_alt_spreads = odds.get("q1_alt_spreads", [])
    q1_alt_totals = odds.get("q1_alt_totals", [])
    juice_wall = JUICE_WALL

    # Best existing Q1 spread/total edge for comparison.
    # Use EDGE_LEAN (4%) as floor so alts only surface when they
    # beat the playability threshold, not just beat zero.
    from ...config import EDGE_LEAN as _EDGE_LEAN
    best_spread_edge = max(
        (p["edge"] for p in picks if "SPREAD" in p.get("type", "")),
        default=_EDGE_LEAN)
    best_total_edge = max(
        (p["edge"] for p in picks if "TOTAL" in p.get("type", "")),
        default=_EDGE_LEAN)

    # Collect alt candidates, then keep only the single best per market
    alt_spread_candidates = []
    alt_total_candidates = []

    import math
    from ...picks_core import score_pick as _score_pick
    for alt in q1_alt_spreads:
        point = alt.get("point")
        home_odds = alt.get("home_odds")
        away_odds = alt.get("away_odds")
        if point is None:
            continue
        # P(home covers spread X) — exact discretized distribution
        # when available, normal-CDF fallback otherwise.
        if margin_probs:
            home_cover = sum(p for m, p in margin_probs.items() if m > -point)
        else:
            z = (predicted_margin - (-point)) / q1_std
            home_cover = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        home_cover = max(0.05, min(0.95, home_cover))
        for side_label, prob, side_odds, side_point in [
            (f"{home_abbr} {'+' if point > 0 else ''}{point} Q1",
             home_cover, home_odds, point),
            (f"{away_abbr} {'+' if -point > 0 else ''}{-point} Q1",
             1.0 - home_cover, away_odds, -point),
        ]:
            if side_odds is None:
                continue
            scored = _score_pick({
                "type": "Q1_SPREAD",
                "pick": side_label,
                "raw_prob": prob,
                "odds": side_odds,
            }, sport="nba", juice_wall=juice_wall)
            if scored is None:
                continue
            # Alt-spread-vs-primary edge floor — alt only beats main
            # if it clears the primary's edge by 3pp+. Keeps alts from
            # spamming when the primary spread already has the value.
            if scored["edge"] <= best_spread_edge + 3.0:
                continue
            scored["is_alt"] = True
            alt_spread_candidates.append(scored)

    for alt in q1_alt_totals:
        line = alt.get("line")
        over_odds = alt.get("over_odds")
        under_odds = alt.get("under_odds")
        if line is None or not predicted_total:
            continue
        # P(total > line) — exact discretized Gaussian when available,
        # CDF fallback otherwise (calibrated σ=8.5 for Q1 total).
        if total_probs_dist:
            over_prob = sum(p for t, p in total_probs_dist.items() if t > line)
        else:
            z = (predicted_total - line) / 8.5
            over_prob = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        for label, prob, side_odds in [
            (f"Over {line} Q1", over_prob, over_odds),
            (f"Under {line} Q1", 1.0 - over_prob, under_odds),
        ]:
            if side_odds is None or abs(side_odds) < 100:
                continue
            scored = _score_pick({
                "type": "Q1_TOTAL",
                "pick": label,
                "raw_prob": prob,
                "odds": side_odds,
            }, sport="nba", juice_wall=juice_wall)
            if scored is None:
                continue
            # Alt-vs-primary edge differential — same +3pp rule as alt
            # spreads. Keeps alts from spamming when the primary line
            # already has the best edge.
            if scored["edge"] <= best_total_edge + 3.0:
                continue
            scored["is_alt"] = True
            alt_total_candidates.append(scored)

    # Keep only the single best alt per market type
    if alt_spread_candidates:
        alt_spread_candidates.sort(key=lambda p: -p["edge"])
        picks.append(alt_spread_candidates[0])
    if alt_total_candidates:
        alt_total_candidates.sort(key=lambda p: -p["edge"])
        picks.append(alt_total_candidates[0])

    # Legacy per-pick calibration loop DELETED 2026-05-02. Every NBA
    # pick (ML, SPREAD, TOTAL, ALT *, Q1_ML, Q1_SPREAD, Q1_TOTAL,
    # Q1 Team Total, Q1 Total O/E) now flows through
    # picks_core.score_pick which calibrates inline.

    # NOTE: q1_ml_home used to get re-stomped here with calibrated
    # value. Removed — same reasoning as engine.nhl_picks /
    # engine.picks. Per-pick prob is calibrated for ranking; the
    # display reads the factor model output.

    picks = [p for p in picks if (p.get("edge") or 0) > 0]

    # Adjusted EV: edge * reliability weight. Reliability is auto-tuned
    # from settled NBA tracker history when volume permits, falls back to
    # NBA_BET_RELIABILITY for cold-start bet types. See engine.dynamic_reliability.
    from ...dynamic_reliability import get_reliability as _get_reliability
    for p in picks:
        reliability = _get_reliability("nba", p["type"])
        p["adjusted_ev"] = round(p["edge"] * reliability, 2)
    picks.sort(key=lambda p: -p["adjusted_ev"])

    from ..._pick_helpers import tag_confidence
    tag_confidence(picks)
    # Filter out skips
    picks = [p for p in picks if p["confidence"] != "skip"]

    return picks


def generate_q1_picks_with_context(home_abbr: str, away_abbr: str,
                                   odds: dict | None = None,
                                   season: int | None = None
                                   ) -> tuple[list[dict], dict]:
    """Generate Q1 picks and return both picks and the full prediction context."""
    from .q1_predict import predict_q1

    odds = _sanitize_odds(odds)
    q1_spread = odds.get("q1_spread")
    q1_total = odds.get("q1_total")

    pred = predict_q1(home_abbr, away_abbr,
                      spread=q1_spread, total=q1_total, season=season)
    picks = generate_q1_picks(home_abbr, away_abbr, odds, season)

    return picks, pred


# ── Full-game pickers (Phase 2k) ──────────────────────────────────

def generate_full_picks(home_abbr: str, away_abbr: str,
                        odds: dict | None = None,
                        season: int | None = None,
                        pred: dict | None = None) -> list[dict]:
    """Generate full-game ML / SPREAD / TOTAL picks plus alt-line variants.

    Mirrors generate_q1_picks but reads full-game probabilities and
    odds. When `pred` is supplied (typically pred["full"] from the
    backend after running factor + MC + GBM), routes home_win and
    total/margin through ensemble_nba so all three signals contribute.
    """
    from .predict import predict_full

    odds = _sanitize_odds(odds)
    posted_spread = odds.get("home_spread_point")
    posted_total = odds.get("over_under")

    if pred is None:
        pred = predict_full(home_abbr, away_abbr,
                            spread=posted_spread, total=posted_total,
                            season=season)

    # CRITICAL: when the live pipeline calls us, `pred` is the parent
    # ensemble_input dict whose TOP-LEVEL fields (spread_cover_prob,
    # over_prob, total_probs, margin_probs, ml_home, ml_away) belong
    # to the Q1 prediction — totals centred ~55, margins ~3. Reading
    # them as full-game data produced calibration spikes (POR @ SA
    # 2026-04-28: ALT TOTAL Under 204.5 @ 99.92% because Q1 totals
    # all live below 70). The full-game distribution lives under
    # pred["full"]; resolve the working pred to that block when present
    # so every read below is full-game-correct. Standalone callers
    # (no "full" sub-block) keep using pred itself.
    full_block = pred.get("full") if isinstance(pred.get("full"), dict) else None
    work_pred = full_block if full_block is not None else pred

    # Apply ensemble blend to ml_home / total / margin if other signals
    # were attached upstream. Caller passes the parent pred dict (with
    # mc_full, gbm, full keys); for the standalone case we just call
    # ensemble_nba on what we have so it collapses cleanly.
    try:
        from ...ensemble import ensemble_nba
        ens_input = pred if "full" in pred else {**pred, "full": pred}
        ens = ensemble_nba(ens_input) or {}
    except Exception as e:
        logger.debug("NBA full-game ensemble blend failed: %s", e)
        ens = {}

    if ens.get("home_win") is not None:
        work_pred["ml_home"] = float(ens["home_win"])
        work_pred["ml_away"] = 1.0 - float(ens["home_win"])

    picks = []
    from ...config import NBA_JUICE_WALL as JUICE_WALL, MAIN_EDGE_FLOOR, MAIN_ODDS_CAP

    nba_floors = MAIN_EDGE_FLOOR.get("nba", {})
    nba_caps = MAIN_ODDS_CAP.get("nba", {})

    def _passes_floor(bt: str, edge: float,
                      pick_text: str | None = None) -> bool:
        # Static floor (textbook) + data-driven override from
        # engine.edge_floors which raises the floor when the cell has
        # been bleeding (post-2026-07-03 the NOPLAY sentinel is gone —
        # cells stay live at scaled stake instead).
        static = float(nba_floors.get(bt, 0.0))
        if edge < static:
            return False
        try:
            from ...edge_floors import required_edge
            data_floor = required_edge("nba", bt, pick_text=pick_text,
                                         default=static)
            return edge >= data_floor
        except Exception:
            return True  # static check already passed
    # _passes_odds_cap removed — picks_core.score_pick enforces it.

    # ── Full-game Spread ──  Migrated to picks_core.score_pick.
    if posted_spread is not None:
        h_spread_odds = odds.get("home_spread_odds", -110)
        a_spread_odds = odds.get("away_spread_odds", -110)
        cover_prob = work_pred.get("spread_cover_prob")
        if cover_prob is not None:
            from ...picks_core import score_pick as _score_pick
            for side_label, prob, side_odds in [
                (f"{home_abbr} {posted_spread:+.1f}", cover_prob, h_spread_odds),
                (f"{away_abbr} {-posted_spread:+.1f}", 1 - cover_prob, a_spread_odds),
            ]:
                scored = _score_pick({
                    "type": "SPREAD",
                    "pick": side_label,
                    "raw_prob": prob,
                    "odds": side_odds,
                }, sport="nba", juice_wall=JUICE_WALL)
                if scored:
                    picks.append(scored)

    # ── Full-game Total ──  Migrated to picks_core.score_pick.
    # picks_core's edge floor lookup consults engine.edge_floors which
    # already encodes the NBA TOTAL min-edge floor — _passes_floor
    # local check is no longer needed.
    if posted_total is not None:
        over_odds = odds.get("over_odds", -110)
        under_odds = odds.get("under_odds", -110)
        over_prob = work_pred.get("over_prob")
        if over_prob is not None:
            from ...picks_core import score_pick as _score_pick
            for label, prob, side_odds in [
                (f"Over {posted_total}", over_prob, over_odds),
                (f"Under {posted_total}", 1 - over_prob, under_odds),
            ]:
                scored = _score_pick({
                    "type": "TOTAL",
                    "pick": label,
                    "raw_prob": prob,
                    "odds": side_odds,
                }, sport="nba", juice_wall=JUICE_WALL)
                if scored:
                    picks.append(scored)

    # ── Full-game Moneyline ──
    # Migrated 2026-05-02 to engine.picks_core.score_pick.
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")
    from ...picks_core import score_pick as _score_pick
    if home_ml is not None:
        h_prob = work_pred.get("ml_home")
        if h_prob is not None:
            scored = _score_pick({
                "type": "ML", "pick": f"{home_abbr} ML",
                "raw_prob": h_prob, "odds": home_ml,
            }, sport="nba", juice_wall=JUICE_WALL)
            if scored:
                picks.append(scored)
    if away_ml is not None:
        a_prob = work_pred.get("ml_away")
        if a_prob is not None:
            scored = _score_pick({
                "type": "ML", "pick": f"{away_abbr} ML",
                "raw_prob": a_prob, "odds": away_ml,
            }, sport="nba", juice_wall=JUICE_WALL)
            if scored:
                picks.append(scored)

    # ── Alt-line shopping (spreads + totals) ──
    # Mirror the Q1 alt-line pass: scan HR's alt_spreads / alt_totals
    # arrays, compute edge against the model's discretized distribution,
    # keep only the highest-edge variant per side that's not just a
    # close cousin of the primary line.
    #
    # Read from pred["full"] FIRST — when generate_full_picks runs in
    # the live pipeline, `pred` is the parent ensemble_input whose
    # top-level total_probs / margin_probs belong to the **Q1** model
    # (totals centred ~55, margins ~3). Falling back to the parent
    # caused the POR @ SA 2026-04-28 calibration spike: ALT TOTAL
    # Under 204.5 came out at 99.92% because Q1's total distribution
    # trivially puts everything > ~70 in the Under bucket. Use the
    # full-game distribution attached under pred["full"] when present;
    # only fall back to the parent dict for the standalone-pred case
    # where generate_full_picks built `pred` itself.
    margin_probs = work_pred.get("margin_probs") or {}
    total_probs = work_pred.get("total_probs") or {}
    if margin_probs:
        from ...picks_core import score_pick as _score_pick
        for alt in odds.get("alt_spreads") or []:
            point = alt.get("point")
            if point is None:
                continue
            h_alt = alt.get("home_odds")
            a_alt = alt.get("away_odds")
            # cover prob = sum of margin prob > -point
            cover_p = sum(p for m, p in margin_probs.items() if m > -point)
            for side_label, prob, side_odds in [
                (f"{home_abbr} {point:+.1f}", cover_p, h_alt),
                (f"{away_abbr} {-point:+.1f}", 1 - cover_p, a_alt),
            ]:
                if side_odds is None:
                    continue
                scored = _score_pick({
                    "type": "ALT SPREAD",
                    "pick": side_label,
                    "raw_prob": prob,
                    "odds": side_odds,
                }, sport="nba", juice_wall=JUICE_WALL)
                if scored:
                    picks.append(scored)
    if total_probs:
        from ...picks_core import score_pick as _score_pick
        for alt in odds.get("alt_totals") or []:
            line = alt.get("line")
            if line is None:
                continue
            o_alt = alt.get("over_odds")
            u_alt = alt.get("under_odds")
            over_p = sum(p for t, p in total_probs.items() if t > line)
            for label, prob, side_odds in [
                (f"Over {line}", over_p, o_alt),
                (f"Under {line}", 1 - over_p, u_alt),
            ]:
                if side_odds is None:
                    continue
                scored = _score_pick({
                    "type": "ALT TOTAL",
                    "pick": label,
                    "raw_prob": prob,
                    "odds": side_odds,
                }, sport="nba", juice_wall=JUICE_WALL)
                if scored:
                    picks.append(scored)

    picks.sort(key=lambda p: -p.get("edge", 0))
    return picks
