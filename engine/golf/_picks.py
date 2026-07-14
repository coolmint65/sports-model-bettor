"""Golf picks engine.

For each market we have (WINNER / TOP_5 / TOP_10 / TOP_20 / MAKE_CUT),
take the predictor's per-player probability and price it against HR's
posted odds. Edge = model_prob - implied_prob.

Gates (mirror the team-sport picks_core):
  1. Belief gate — never pick a market where model_prob < some min
     (varies per bet_type: WINNER at 1% — golf winners are rare, you
     don't need a 50% pick to be EV+; TOP_X at 30% — same logic; cut
     picks at 50%).
  2. Max-odds cap — bookmakers price 500-1 longshots that the model
     can't possibly beat without overfitting. Cap WINNER at +5000.
  3. Edge floor — varies by bet_type (richer markets need bigger edges
     because the book is sharper).

Output picks are returned as a list of dicts (bet_type, player_id,
display_name, model_prob, edge, odds). Caller decides which subset to
record + how to display (e.g. cap to top N by edge per tournament).
"""
from __future__ import annotations

import logging
from typing import Any

from ._db import get_conn
from ._predict import predict_field
from ._odds import fetch_tournament_odds
from . import _calibration_apply as _golf_cal

logger = logging.getLogger(__name__)


# ── Per-market gates ────────────────────────────────────────

_MIN_PROB = {
    # Golf winners are inherently rare events — we don't need a 50%
    # belief gate to be EV+. A 1.5% pick on a +5000 dog the book
    # priced at +9000 is a real edge. Tighter floors per market.
    "WINNER":   0.008,   # 0.8% — top of the field, anyone below is noise
    "TOP_5":    0.05,
    "TOP_10":   0.08,
    "TOP_20":   0.12,
    "MAKE_CUT": 0.55,
}

# Edge ceiling kept as a defense-in-depth backstop above the calibration
# layer. Pre-calibration (Apr 2026 → 2026-05-17) this floor at 8% caught
# phantom edges from cold-start / thin-history players. With per-
# (tour, market, bucket) walk-forward calibration now applied, those
# phantom edges get shrunk to realistic values before the ceiling sees
# them. Kept at 15% so legitimate outlier picks (e.g. an injury news
# release the book is slow to price) can still ship, while truly absurd
# 30%+ overconfidence still gets blocked.
_MAX_EDGE_PCT = 15.0

# Plus-odds ceiling per market. Beyond these the book is pricing pure
# variance and our model can't reliably beat the closing line. Tuned
# to actual HR golf board (TOP_20 quotes 132 selections from +200 to
# +100000; cutting at +5000 keeps ~80% of the priced field reachable).
_MAX_ODDS = {
    "WINNER":   25000,
    "TOP_5":    8000,
    "TOP_10":   5000,
    "TOP_20":   5000,
    "MAKE_CUT": 500,
}

_MIN_EDGE_PCT = {
    "WINNER":   4.0,
    "TOP_5":    3.0,
    "TOP_10":   3.0,
    "TOP_20":   3.0,
    "MAKE_CUT": 3.0,
}


def _implied(odds: int) -> float:
    if odds == 0:
        return 0.5
    if odds < 0:
        return abs(odds) / (abs(odds) + 100.0)
    return 100.0 / (odds + 100.0)


# ── Generate picks ──────────────────────────────────────────

_PRED_KEY = {
    "WINNER":   "p_winner",
    "TOP_5":    "p_top_5",
    "TOP_10":   "p_top_10",
    "TOP_20":   "p_top_20",
    "MAKE_CUT": "p_made_cut",
}

# Minimum historical PGA Tour events the player must have for us to
# trust the prediction. Players below this are either club pros /
# qualifiers (no history) or LIV defectors (3+ years off PGA Tour).
# Both classes produce huge phantom edges — the model defaults them to
# field-mean while HR knows the truth and prices them sharply.
_MIN_TOUR_HISTORY = 6

# Hard cap on emitted picks per tournament. Each additional pick must
# keep up with the primary on edge — see _SECONDARY_EDGE_FRACTION —
# so a weak filler doesn't sneak in next to a strong headline.
_MAX_PICKS_PER_TOURNAMENT = 4
# Subsequent picks only fire when their edge is at least this fraction
# of the primary's edge.
_SECONDARY_EDGE_FRACTION = 0.65


def generate_picks(tour: str, tournament_id: str) -> list[dict]:
    """Run predict + HR fetch and emit per-market picks. Returns a list
    sorted by edge descending. Caller writes to DB."""
    odds = fetch_tournament_odds(tour, tournament_id)
    if not odds:
        return []
    pred = predict_field(tour, tournament_id)
    if not pred:
        return []
    conn = get_conn(tour)
    name_map = {r["id"]: r["name"]
                 for r in conn.execute("SELECT id, name FROM players").fetchall()}

    out: list[dict] = []
    skipped_no_history = 0
    for bet_type, prob_key in _PRED_KEY.items():
        market_odds = odds.get(bet_type) or {}
        if not market_odds:
            continue
        min_prob = _MIN_PROB.get(bet_type, 0.0)
        max_odds = _MAX_ODDS.get(bet_type, 10000)
        min_edge = _MIN_EDGE_PCT.get(bet_type, 0.0)
        for player_id, american in market_odds.items():
            if abs(american) <= 0:
                continue
            if american > max_odds:
                continue
            row = pred.get(player_id)
            if not row:
                continue
            # Skip thin-history players — see _MIN_TOUR_HISTORY comment.
            if int(row.get("skill_n", 0)) < _MIN_TOUR_HISTORY:
                skipped_no_history += 1
                continue
            raw_p = float(row.get(prob_key, 0.0))
            if raw_p < min_prob:
                continue
            # Walk-forward calibration shrinkage. For tours that have a
            # calibration JSON (PGA / LPGA / KornFerry as of 2026-05-17)
            # this maps raw_p → historical realized hit-rate in the bucket.
            # Cold-start tours pass through unchanged.
            p = float(_golf_cal.calibrate(tour, bet_type, raw_p))
            imp = _implied(int(american))
            edge_pct = (p - imp) * 100.0
            if edge_pct < min_edge:
                continue
            if edge_pct > _MAX_EDGE_PCT:
                # Reject implausibly large edges — overconfident model
                # output on cold-start players. Skip rather than cap so
                # the tracker doesn't record phantom edges.
                continue
            # Golf-tuned stake ladder. The universal stake_units_for
            # assumes prob >= 0.52 (team-sport ML scale), which golf
            # outrights never reach — even a MAKE_CUT favorite only
            # hits 95%. Use an edge-based ladder mirroring engine.
            # motorsports._picks (F1), tuned for golf's
            # percentage-point edges (3-15% range): 0.25u above the
            # market floor, 0.5u at 6%+, 1u at 10%+.
            if edge_pct >= 10.0:
                _u = 1.0
            elif edge_pct >= 6.0:
                _u = 0.5
            elif edge_pct >= 3.0:
                _u = 0.25
            else:
                _u = 0.0
            out.append({
                "bet_type": bet_type,
                "player_id": int(player_id),
                "pick": name_map.get(int(player_id), str(player_id)),
                "odds": int(american),
                "model_prob": round(p, 4),
                "model_prob_raw": round(raw_p, 4),
                "implied_prob": round(imp, 4),
                "edge": round(edge_pct, 2),
                "stake_units": _u,
            })
    out.sort(key=lambda x: -x["edge"])
    raw_count = len(out)
    if not out:
        logger.info("[golf:%s] picks generated: 0 (skipped %d thin-history)",
                    tour, skipped_no_history)
        return out
    # Cap at MAX_PICKS_PER_TOURNAMENT. The headline is always the
    # highest-edge candidate; subsequent picks need both:
    #   - edge ≥ primary.edge × _SECONDARY_EDGE_FRACTION (no fillers)
    #   - DIFFERENT player than picks already kept (one bet per player;
    #     two TOP_N picks on the same player is one stake, not two)
    primary = out[0]
    final: list[dict] = [primary]
    picked_players = {primary["player_id"]}
    threshold = primary["edge"] * _SECONDARY_EDGE_FRACTION
    for candidate in out[1:]:
        if len(final) >= _MAX_PICKS_PER_TOURNAMENT:
            break
        if candidate["edge"] < threshold:
            break
        if candidate["player_id"] in picked_players:
            continue
        final.append(candidate)
        picked_players.add(candidate["player_id"])
    logger.info(
        "[golf:%s] picks generated: %d (raw=%d, skipped %d thin-history)",
        tour, len(final), raw_count, skipped_no_history,
    )
    return final
