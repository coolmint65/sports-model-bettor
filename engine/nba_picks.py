"""
NBA Q1 bet selection.

Separated from nba_q1_predict.py to keep game prediction and market
decision-making in distinct modules. This file converts Q1 model
probabilities into concrete betting picks by comparing against
real odds and applying edge/priority filtering.

Used by: NBA Best Bets, NBA Q1 Pick Tracker.
"""

import logging

from .config import NBA_BET_RELIABILITY

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
    from .nba_q1_predict import predict_q1

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
        from .ensemble import ensemble_nba
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
    from .config import NBA_JUICE_WALL as JUICE_WALL

    # ── Q1 Spread ──
    if q1_spread is not None:
        h_spread_odds = odds.get("q1_spread_home_odds", -110)
        a_spread_odds = odds.get("q1_spread_away_odds", -110)

        cover_prob = pred["spread_cover_prob"]
        if cover_prob is not None:
            implied = _implied_prob(h_spread_odds)
            edge = (cover_prob - implied) * 100
            if edge > 0 and h_spread_odds >= JUICE_WALL:
                picks.append({
                    "type": "Q1_SPREAD",
                    "pick": f"{home_abbr} {q1_spread:+.1f} Q1",
                    "prob": round(cover_prob, 4),
                    "edge": round(edge, 1),
                    "odds": h_spread_odds,
                })

            away_cover_prob = 1 - cover_prob
            away_implied = _implied_prob(a_spread_odds)
            away_edge = (away_cover_prob - away_implied) * 100
            if away_edge > 0 and a_spread_odds >= JUICE_WALL:
                picks.append({
                    "type": "Q1_SPREAD",
                    "pick": f"{away_abbr} {-q1_spread:+.1f} Q1",
                    "prob": round(away_cover_prob, 4),
                    "edge": round(away_edge, 1),
                    "odds": a_spread_odds,
                })

    # ── Q1 Total ──
    if q1_total is not None:
        over_odds = odds.get("q1_over_odds", -110)
        under_odds = odds.get("q1_under_odds", -110)

        over_prob = pred["over_prob"]
        if over_prob is not None:
            over_implied = _implied_prob(over_odds)
            over_edge = (over_prob - over_implied) * 100
            if over_edge > 0 and over_odds >= JUICE_WALL:
                picks.append({
                    "type": "Q1_TOTAL",
                    "pick": f"Over {q1_total} Q1",
                    "prob": round(over_prob, 4),
                    "edge": round(over_edge, 1),
                    "odds": over_odds,
                })

            under_prob = 1 - over_prob
            under_implied = _implied_prob(under_odds)
            under_edge = (under_prob - under_implied) * 100
            if under_edge > 0 and under_odds >= JUICE_WALL:
                picks.append({
                    "type": "Q1_TOTAL",
                    "pick": f"Under {q1_total} Q1",
                    "prob": round(under_prob, 4),
                    "edge": round(under_edge, 1),
                    "odds": under_odds,
                })

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

    if home_ml_odds is not None and home_ml_odds >= JUICE_WALL:
        home_ml_prob = pred["q1_ml_home"]
        implied = _implied_prob(home_ml_odds)
        edge = (home_ml_prob - implied) * 100
        if edge > 0:
            picks.append({
                "type": "Q1_ML",
                "pick": f"{home_abbr} Q1 ML",
                "prob": round(home_ml_prob, 4),
                "edge": round(edge, 1),
                "odds": home_ml_odds,
            })

    if away_ml_odds is not None and away_ml_odds >= JUICE_WALL:
        away_ml_prob = pred["q1_ml_away"]
        implied = _implied_prob(away_ml_odds)
        edge = (away_ml_prob - implied) * 100
        if edge > 0:
            picks.append({
                "type": "Q1_ML",
                "pick": f"{away_abbr} Q1 ML",
                "prob": round(away_ml_prob, 4),
                "edge": round(edge, 1),
                "odds": away_ml_odds,
            })

    # ── Phase 1 derivatives ──
    # Q1 team totals (Gaussian tail off home/away_q1_expected) and Q1
    # total odd/even (sum total_probs over odd vs even). All pure
    # probability extraction — no factor stacking.
    from .nba_derivative_picks import append_derivative_picks
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
    from .config import EDGE_LEAN as _EDGE_LEAN
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
    for alt in q1_alt_spreads:
        point = alt.get("point")
        home_odds = alt.get("home_odds")
        away_odds = alt.get("away_odds")
        if point is None:
            continue
        # P(home covers spread X) = P(home_margin > -X). Uses the exact
        # discretized distribution from nba_q1_predict when available so
        # alt edges at distant lines aren't subject to CDF approximation
        # error. Falls back to the normal CDF when the dict isn't present
        # (e.g. upstream pred was produced by an older code path).
        if margin_probs:
            threshold = -point
            home_cover = sum(p for m, p in margin_probs.items() if m > threshold)
        else:
            z = (predicted_margin - (-point)) / q1_std
            home_cover = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        home_cover = max(0.05, min(0.95, home_cover))

        if home_odds is not None and abs(home_odds) >= 100 and home_odds >= juice_wall:
            edge = (home_cover - _implied_prob(home_odds)) * 100
            if edge > 0 and edge > best_spread_edge + 3.0:
                sign = "+" if point > 0 else ""
                alt_spread_candidates.append({
                    "type": "Q1_SPREAD",
                    "pick": f"{home_abbr} {sign}{point} Q1",
                    "prob": round(home_cover, 4),
                    "edge": round(edge, 1),
                    "odds": home_odds,
                    "is_alt": True,
                })
        if away_odds is not None and abs(away_odds) >= 100 and away_odds >= juice_wall:
            away_cover = 1.0 - home_cover
            edge = (away_cover - _implied_prob(away_odds)) * 100
            if edge > 0 and edge > best_spread_edge + 3.0:
                sign = "+" if -point > 0 else ""
                alt_spread_candidates.append({
                    "type": "Q1_SPREAD",
                    "pick": f"{away_abbr} {sign}{-point} Q1",
                    "prob": round(away_cover, 4),
                    "edge": round(edge, 1),
                    "odds": away_odds,
                    "is_alt": True,
                })

    for alt in q1_alt_totals:
        line = alt.get("line")
        over_odds = alt.get("over_odds")
        under_odds = alt.get("under_odds")
        if line is None or not predicted_total:
            continue
        # P(total > line) from the exact discretized Gaussian distribution.
        # The previous fallback used a logistic with a 0.6 scale fudge which
        # systematically under-estimated tail mass; switching to the proper
        # CDF fixes alt-total edges at the extremes (O 50.5 / U 68.5 etc).
        total_std = 8.5  # Q1 total std dev (calibrated from 2,714 games)
        if total_probs_dist:
            over_prob = sum(p for t, p in total_probs_dist.items() if t > line)
        else:
            z = (predicted_total - line) / total_std
            over_prob = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        under_prob = 1.0 - over_prob

        if over_odds is not None and abs(over_odds) >= 100 and over_odds >= juice_wall and over_prob > 0.5:
            edge = (over_prob - _implied_prob(over_odds)) * 100
            if edge > 0 and edge > best_total_edge + 3.0:
                alt_total_candidates.append({
                    "type": "Q1_TOTAL", "pick": f"Over {line} Q1",
                    "prob": round(over_prob, 4), "edge": round(edge, 1),
                    "odds": over_odds, "is_alt": True,
                })
        if under_odds is not None and abs(under_odds) >= 100 and under_odds >= juice_wall and under_prob > 0.5:
            edge = (under_prob - _implied_prob(under_odds)) * 100
            if edge > 0 and edge > best_total_edge + 3.0:
                alt_total_candidates.append({
                    "type": "Q1_TOTAL", "pick": f"Under {line} Q1",
                    "prob": round(under_prob, 4), "edge": round(edge, 1),
                    "odds": under_odds, "is_alt": True,
                })

    # Keep only the single best alt per market type
    if alt_spread_candidates:
        alt_spread_candidates.sort(key=lambda p: -p["edge"])
        picks.append(alt_spread_candidates[0])
    if alt_total_candidates:
        alt_total_candidates.sort(key=lambda p: -p["edge"])
        picks.append(alt_total_candidates[0])

    # Empirical recalibration from the nba_picks tracker. See
    # engine/picks.py for the same pattern applied to MLB.
    from .empirical_calibration import calibrate as _calibrate
    for p in picks:
        prob = p.get("prob")
        odds = p.get("odds")
        if prob is None:
            continue
        cal = _calibrate(
            p["type"], float(prob), sport="nba",
            edge=p.get("edge"), odds=odds,
            pick_text=p.get("pick"),
        )
        p["prob_raw"] = round(float(prob), 4)
        p["prob"] = round(float(cal), 4)
        if odds is not None and _valid_odds(odds):
            p["edge"] = round((cal - _implied_prob(int(odds))) * 100, 1)

    # Keep pred's Q1 win prob consistent with the calibrated Q1 ML
    # pick so Projected Outcome and the pick card agree. predict_q1
    # exposes q1_ml_home / q1_ml_away as top-level fields.
    q1_home = pred.get("q1_ml_home")
    if q1_home is not None:
        # Preserve the pre-calibration factor value so Model Signals'
        # Factor column reads the true factor output.
        pred.setdefault("factor_q1_ml_home", q1_home)
        cal_q1 = float(_calibrate("Q1 ML", float(q1_home), sport="nba"))
        pred["q1_ml_home"] = round(cal_q1, 4)
        pred["q1_ml_away"] = round(1.0 - cal_q1, 4)

    picks = [p for p in picks if (p.get("edge") or 0) > 0]

    # Conservatism ladder: swap risky Q1 spread/total picks to safer
    # same-direction siblings (smaller spread, Q1_ML, nearer total)
    # when one still clears edge + juice guardrails.
    try:
        from .conservatism import apply_ladder as _conservatism_ladder
        picks = _conservatism_ladder(picks, pred, odds, "nba", home_abbr, away_abbr)
    except Exception as e:
        logger.warning("NBA conservatism ladder error: %s", e)

    # Adjusted EV: edge * reliability weight. Reliability is auto-tuned
    # from settled NBA tracker history when volume permits, falls back to
    # NBA_BET_RELIABILITY for cold-start bet types. See engine.dynamic_reliability.
    from .dynamic_reliability import get_reliability as _get_reliability
    for p in picks:
        reliability = _get_reliability("nba", p["type"])
        p["adjusted_ev"] = round(p["edge"] * reliability, 2)
    picks.sort(key=lambda p: -p["adjusted_ev"])

    from ._pick_helpers import tag_confidence
    tag_confidence(picks)
    # Filter out skips
    picks = [p for p in picks if p["confidence"] != "skip"]

    return picks


def generate_q1_picks_with_context(home_abbr: str, away_abbr: str,
                                   odds: dict | None = None,
                                   season: int | None = None
                                   ) -> tuple[list[dict], dict]:
    """Generate Q1 picks and return both picks and the full prediction context."""
    from .nba_q1_predict import predict_q1

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
    from .nba_predict import predict_full

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
        from .ensemble import ensemble_nba
        ens_input = pred if "full" in pred else {**pred, "full": pred}
        ens = ensemble_nba(ens_input) or {}
    except Exception as e:
        logger.debug("NBA full-game ensemble blend failed: %s", e)
        ens = {}

    if ens.get("home_win") is not None:
        work_pred["ml_home"] = float(ens["home_win"])
        work_pred["ml_away"] = 1.0 - float(ens["home_win"])

    picks = []
    from .config import NBA_JUICE_WALL as JUICE_WALL, MAIN_EDGE_FLOOR, MAIN_ODDS_CAP

    nba_floors = MAIN_EDGE_FLOOR.get("nba", {})
    nba_caps = MAIN_ODDS_CAP.get("nba", {})
    def _passes_floor(bt: str, edge: float) -> bool:
        return edge >= nba_floors.get(bt, 0.0)
    def _passes_odds_cap(bt: str, american_odds) -> bool:
        cap = nba_caps.get(bt)
        if cap is None or american_odds is None:
            return True
        try:
            return int(american_odds) <= int(cap)
        except (TypeError, ValueError):
            return True

    # ── Full-game Spread ──
    if posted_spread is not None:
        h_spread_odds = odds.get("home_spread_odds", -110)
        a_spread_odds = odds.get("away_spread_odds", -110)
        cover_prob = work_pred.get("spread_cover_prob")
        if cover_prob is not None:
            h_imp = _implied_prob(h_spread_odds)
            h_edge = (cover_prob - h_imp) * 100
            if h_edge > 0 and h_spread_odds >= JUICE_WALL:
                picks.append({
                    "type": "SPREAD",
                    "pick": f"{home_abbr} {posted_spread:+.1f}",
                    "prob": round(cover_prob, 4),
                    "edge": round(h_edge, 1),
                    "odds": h_spread_odds,
                })
            a_cover = 1 - cover_prob
            a_imp = _implied_prob(a_spread_odds)
            a_edge = (a_cover - a_imp) * 100
            if a_edge > 0 and a_spread_odds >= JUICE_WALL:
                picks.append({
                    "type": "SPREAD",
                    "pick": f"{away_abbr} {-posted_spread:+.1f}",
                    "prob": round(a_cover, 4),
                    "edge": round(a_edge, 1),
                    "odds": a_spread_odds,
                })

    # ── Full-game Total ──
    # Backtest 2026-04-27: NBA TOTAL bleeds at edges < 12%. Floor
    # gated via MAIN_EDGE_FLOOR['nba']['TOTAL']=12.0 so only the
    # near-break-even high-edge bucket surfaces.
    if posted_total is not None:
        over_odds = odds.get("over_odds", -110)
        under_odds = odds.get("under_odds", -110)
        over_prob = work_pred.get("over_prob")
        if over_prob is not None:
            o_imp = _implied_prob(over_odds)
            o_edge = (over_prob - o_imp) * 100
            if o_edge > 0 and over_odds >= JUICE_WALL and _passes_floor("TOTAL", o_edge):
                picks.append({
                    "type": "TOTAL",
                    "pick": f"Over {posted_total}",
                    "prob": round(over_prob, 4),
                    "edge": round(o_edge, 1),
                    "odds": over_odds,
                })
            u_prob = 1 - over_prob
            u_imp = _implied_prob(under_odds)
            u_edge = (u_prob - u_imp) * 100
            if u_edge > 0 and under_odds >= JUICE_WALL and _passes_floor("TOTAL", u_edge):
                picks.append({
                    "type": "TOTAL",
                    "pick": f"Under {posted_total}",
                    "prob": round(u_prob, 4),
                    "edge": round(u_edge, 1),
                    "odds": under_odds,
                })

    # ── Full-game Moneyline ──
    # Cap ML odds at MAIN_ODDS_CAP['nba']['ML'] (default +400). Live
    # money on high-American-odds longshots correlates with calibration
    # risk — cap blocks the trap.
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")
    if home_ml is not None and home_ml >= JUICE_WALL and _passes_odds_cap("ML", home_ml):
        h_prob = work_pred.get("ml_home")
        if h_prob is not None:
            h_imp = _implied_prob(home_ml)
            h_edge = (h_prob - h_imp) * 100
            if h_edge > 0:
                picks.append({
                    "type": "ML",
                    "pick": f"{home_abbr} ML",
                    "prob": round(h_prob, 4),
                    "edge": round(h_edge, 1),
                    "odds": home_ml,
                })
    if away_ml is not None and away_ml >= JUICE_WALL and _passes_odds_cap("ML", away_ml):
        a_prob = work_pred.get("ml_away")
        if a_prob is not None:
            a_imp = _implied_prob(away_ml)
            a_edge = (a_prob - a_imp) * 100
            if a_edge > 0:
                picks.append({
                    "type": "ML",
                    "pick": f"{away_abbr} ML",
                    "prob": round(a_prob, 4),
                    "edge": round(a_edge, 1),
                    "odds": away_ml,
                })

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
        for alt in odds.get("alt_spreads") or []:
            point = alt.get("point")
            if point is None:
                continue
            h_alt = alt.get("home_odds")
            a_alt = alt.get("away_odds")
            # cover prob = sum of margin prob > -point
            cover_p = sum(p for m, p in margin_probs.items() if m > -point)
            if h_alt is not None and h_alt >= JUICE_WALL:
                imp = _implied_prob(h_alt)
                e = (cover_p - imp) * 100
                if e > 0:
                    picks.append({
                        "type": "ALT SPREAD",
                        "pick": f"{home_abbr} {point:+.1f}",
                        "prob": round(cover_p, 4),
                        "edge": round(e, 1),
                        "odds": h_alt,
                    })
            if a_alt is not None and a_alt >= JUICE_WALL:
                a_cover = 1 - cover_p
                imp = _implied_prob(a_alt)
                e = (a_cover - imp) * 100
                if e > 0:
                    picks.append({
                        "type": "ALT SPREAD",
                        "pick": f"{away_abbr} {-point:+.1f}",
                        "prob": round(a_cover, 4),
                        "edge": round(e, 1),
                        "odds": a_alt,
                    })
    if total_probs:
        for alt in odds.get("alt_totals") or []:
            line = alt.get("line")
            if line is None:
                continue
            o_alt = alt.get("over_odds")
            u_alt = alt.get("under_odds")
            over_p = sum(p for t, p in total_probs.items() if t > line)
            under_p = sum(p for t, p in total_probs.items() if t < line)
            if o_alt is not None and o_alt >= JUICE_WALL:
                imp = _implied_prob(o_alt)
                e = (over_p - imp) * 100
                if e > 0 and _passes_floor("ALT TOTAL", e):
                    picks.append({
                        "type": "ALT TOTAL",
                        "pick": f"Over {line}",
                        "prob": round(over_p, 4),
                        "edge": round(e, 1),
                        "odds": o_alt,
                    })
            if u_alt is not None and u_alt >= JUICE_WALL:
                imp = _implied_prob(u_alt)
                e = (under_p - imp) * 100
                if e > 0 and _passes_floor("ALT TOTAL", e):
                    picks.append({
                        "type": "ALT TOTAL",
                        "pick": f"Under {line}",
                        "prob": round(under_p, 4),
                        "edge": round(e, 1),
                        "odds": u_alt,
                    })

    picks.sort(key=lambda p: -p.get("edge", 0))
    return picks
