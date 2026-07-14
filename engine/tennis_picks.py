"""
Tennis picks engine.

Compares ``engine.tennis_predict.predict_match`` output against a
provided odds dict and emits edges that clear configured floors.

Markets in scope (MVP):

  - ``ML``        — match winner outright
  - ``SET_SPREAD``— set handicap (e.g. -1.5 sets in BO5)
  - ``TOTAL``     — total games over/under

Set spread requires per-set probabilities, which we derive from the
match prob via the same inversion the predictor uses for BO3↔BO5
conversion. Game total requires modelling each player's hold-percent;
for MVP we use a simple symmetric-around-Elo approximation.

Public API::

    generate_tennis_picks(prediction, odds, *,
                           min_edge_ml=4.0, min_edge_other=6.0,
                           tournament_level=None) -> list[dict]

Each pick dict matches the same shape used by other sports' picks:
``type``, ``pick``, ``odds``, ``model_prob``, ``edge``,
``confidence``, ``conviction_score``.

Tournament gating
-----------------
Pickers for the smaller ATP 250 / WTA 250 events are NOISY because
the field is uneven and our calibration window (matches per player)
is thinner. ``tournament_level`` lets the caller pass the Sackmann
level code; the engine refuses to emit picks for sub-Tier-1 events
unless explicitly opted in by the caller.

Slams (G), Masters/Premier Mandatory (M / P), and ATP/WTA Finals (F)
are always picked. Per the user directive that picker output should
be cap-1-3-best per slate, the caller is expected to call this per
match and rank across the slate.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# Per-tier edge-floor multipliers. Lower tiers require more edge
# because the predictor's calibration window thins out — Sackmann
# coverage is patchy on Challengers and ITFs, and 250-tier fields
# have wider variance round-to-round. Picks still emit; the bar
# just rises. Mirrors engine.tennis_schedule.TIER_EDGE_FLOOR_MULT.
_TIER_FLOOR_MULT = {
    "G":   1.0,
    "F":   1.0,
    "M":   1.0,
    "P":   1.0,
    "PM":  1.0,
    "P5":  1.0,
    "500": 1.10,
    "250": 1.25,
    "A":   1.25,
    "125": 1.50,
    "C":   1.75,
    "ITF": 2.00,
}


def _tier_mult(level: str | None) -> float:
    if not level:
        return 1.0
    return _TIER_FLOOR_MULT.get(level.upper(), 1.25)


# ── Market math ────────────────────────────────────────────────

def _implied_prob(odds: int | float | None) -> float | None:
    """American odds → implied probability."""
    if odds is None:
        return None
    try:
        n = float(odds)
    except (TypeError, ValueError):
        return None
    if n == 0:
        return None
    return abs(n) / (abs(n) + 100.0) if n < 0 else 100.0 / (n + 100.0)


def _per_set_prob(p_match_bo3: float) -> float:
    """Same inversion the predictor uses internally — re-export-ish so
    the picker doesn't have to reach into private helpers."""
    from .tennis_predict import _per_set_prob_from_match_bo3
    return _per_set_prob_from_match_bo3(p_match_bo3)


def _set_spread_probs(p_set: float, best_of: int) -> dict:
    """Per-set spread probabilities for a player who wins each set
    independently with prob ``p_set``.

    Returns the prob of:
      - winning by 2-0 (BO3) or 3-0 (BO5)  → 'cover_-1.5' / 'cover_-2.5'
      - winning by 2-1 (BO3) or 3-1/3-2 (BO5)
      - losing by 0-2 (BO3) / 0-3 / 1-3 / 2-3 (BO5)

    These slot into HR's set-spread shape (typically -1.5 / +1.5 sets
    for BO3, -2.5 / +2.5 for BO5).
    """
    p = p_set
    q = 1.0 - p
    out = {"best_of": best_of}
    if best_of == 3:
        # Score distribution: P(2-0) = p²; P(2-1) = 2pq×p = 2p²q;
        # losing 1-2 = 2qp×q = 2pq²; losing 0-2 = q².
        p_20 = p * p
        p_21 = 2.0 * p * p * q
        p_12 = 2.0 * p * q * q
        p_02 = q * q
        out["p_2_0"] = p_20
        out["p_2_1"] = p_21
        out["p_1_2"] = p_12
        out["p_0_2"] = p_02
        out["cover_minus_1_5"] = p_20  # winning a set 2-0 covers -1.5
        out["cover_plus_1_5"]  = p_21 + p_20  # winning the match (any way) covers +1.5
        return out
    # BO5
    # P(3-0) = p³; P(3-1) = 3p³q; P(3-2) = 6p³q²; mirror for losses.
    p_30 = p ** 3
    p_31 = 3.0 * p ** 3 * q
    p_32 = 6.0 * p ** 3 * q ** 2
    p_23 = 6.0 * p ** 2 * q ** 3
    p_13 = 3.0 * p * q ** 3
    p_03 = q ** 3
    out["p_3_0"] = p_30
    out["p_3_1"] = p_31
    out["p_3_2"] = p_32
    out["p_2_3"] = p_23
    out["p_1_3"] = p_13
    out["p_0_3"] = p_03
    out["cover_minus_2_5"] = p_30  # win 3-0
    out["cover_minus_1_5"] = p_30 + p_31  # win 3-0 or 3-1
    out["cover_plus_1_5"]  = p_30 + p_31 + p_32  # win the match
    out["cover_plus_2_5"]  = p_30 + p_31 + p_32 + p_23  # win or lose 2-3
    return out


# ── Confidence tiers ───────────────────────────────────────────

def _confidence_for_edge(edge_pct: float) -> str:
    if edge_pct >= 12:
        return "strong"
    if edge_pct >= 7:
        return "moderate"
    return "lean"


def _conviction_score(prob: float, edge_pct: float) -> float:
    """Confidence-only conviction score — same as POTD + live ★.
    Edge is a filter, not a ranker (per feedback_be_right_first)."""
    if prob <= 0 or edge_pct <= 0:
        return 0.0
    return prob * prob


def _juice_wall() -> int:
    """American-odds floor (most-negative). Picks at or below this are
    refused. Same -200 default as the team sports — the model can be
    right but a -400 winner only returns +25u for every -100u risked,
    and a single calibration miss inside that band wipes a week."""
    try:
        from .config import TENNIS_JUICE_WALL
        return int(TENNIS_JUICE_WALL)
    except Exception:
        return -200


def _passes_juice_wall(american_odds: int | float | None) -> bool:
    if american_odds is None:
        return False
    try:
        n = int(american_odds)
    except (TypeError, ValueError):
        return False
    return n >= _juice_wall()


# Note: odds-cap and edge-ceiling enforcement live in picks_core.score_pick.
# Earlier this module defined local _passes_odds_cap / _passes_edge_ceiling
# helpers — they were dead code (no callers) and duplicated picks_core's
# logic. Removed during the audit cleanup; if you need to inspect the
# tennis cap config see config.MAIN_ODDS_CAP['tennis'] and
# config.TENNIS_EDGE_CEILING.


# ── Public API ─────────────────────────────────────────────────

def generate_tennis_picks(prediction: dict, odds: dict | None, *,
                           min_edge_ml: float = 4.0,
                           min_edge_other: float = 6.0,
                           tournament_level: str | None = None,
                           include_lower_tiers: bool = False
                           ) -> list[dict]:
    """Compare a prediction against an odds dict and emit edges.

    ``odds`` accepts EITHER:

    A. Legacy flat shape (back-compat with the old matchup picker)::

        {p1_ml, p2_ml,
         p1_set_spread_point, p1_set_spread_odds,
         p2_set_spread_point, p2_set_spread_odds,
         total_games, over_odds, under_odds}

    B. New ``markets`` shape (from engine.tennis_odds.fetch_all)::

        {markets: {ml: {p1_odds, p2_odds},
                    set_spread: [{player, point, odds}, ...],
                    total_games: {line, over_odds, under_odds},
                    total_sets: {line, over_odds, under_odds},
                    p1_total_games: {line, over_odds, under_odds},
                    p2_total_games: {line, over_odds, under_odds},
                    p1_win_at_least_one_set: {yes_odds, no_odds},
                    p2_win_at_least_one_set: {yes_odds, no_odds},
                    set_betting: {'2-0': odds, '2-1': odds, ...},
                    most_games: {p1_odds, p2_odds, tie_odds}}}

    The picker auto-detects the shape — if ``markets`` key is present
    it uses that; otherwise falls back to the legacy flat dict.

    ``tournament_level`` if set is checked against ``_TIER_1_LEVELS``.
    Pass ``include_lower_tiers=True`` to bypass the gate (backtests).
    """
    if not prediction:
        return []
    if not odds:
        return []

    # Cold-start gate. Two layers:
    #
    # (1) Both players at the default 1500 Elo (rating drift < 0.5)
    #     means the model has zero signal. 50/50 prior fights HR's
    #     informed line and produces phantom edges (Matsuda vs Daniel
    #     2026-05-01: HR -1600 fav, our model 52/48, picker emitted
    #     a 0-2 longshot at +25000 with claimed +23% edge).
    #
    # (2) EITHER player has fewer than MIN_HISTORY_MATCHES historical
    #     matches in our corpus. Even a player whose Elo has drifted
    #     a few points away from 1500 may have only 5-10 matches of
    #     history — well below the volume where Elo + GBM converge to
    #     a meaningful estimate. The Zverev-class anomaly came from
    #     this case: Zverev rated 1850 vs Blockx rated 1505 (1 match
    #     of history, Elo barely moved from default) → model says
    #     Zverev 90%, market says 65%, picker emits +25% edge — but
    #     the model's 90% is overconfidence on a near-cold-start
    #     opponent, not real signal.
    INIT_RATING = 1500.0
    DEFAULT_TOLERANCE = 0.5
    MIN_HISTORY_MATCHES = 20
    p1_rating = float(prediction.get("p1_rating") or INIT_RATING)
    p2_rating = float(prediction.get("p2_rating") or INIT_RATING)
    p1_unrated = abs(p1_rating - INIT_RATING) < DEFAULT_TOLERANCE
    p2_unrated = abs(p2_rating - INIT_RATING) < DEFAULT_TOLERANCE
    if p1_unrated and p2_unrated:
        return []
    p1_matches = int(prediction.get("p1_matches") or 0)
    p2_matches = int(prediction.get("p2_matches") or 0)
    if p1_matches < MIN_HISTORY_MATCHES or p2_matches < MIN_HISTORY_MATCHES:
        return []

    # Tier-aware floor adjustment. Drops the old hard rejection of
    # below-Masters tournaments — picks now emit for every tier the
    # caller passes; the floor scales with calibration confidence.
    mult = _tier_mult(tournament_level)
    min_edge_ml = min_edge_ml * mult
    min_edge_other = min_edge_other * mult
    # If the odds dict carries the rich markets block (HR live path)
    # delegate to the multi-market scorer; otherwise fall through to
    # the legacy flat-dict scorer below.
    if isinstance(odds.get("markets"), dict):
        return _score_full_markets(prediction, odds["markets"],
                                     min_edge_ml=min_edge_ml,
                                     min_edge_other=min_edge_other)

    picks: list[dict] = []
    p1_prob = float(prediction.get("p1_win_prob") or 0.0)
    p2_prob = float(prediction.get("p2_win_prob") or 0.0)
    best_of = int(prediction.get("best_of") or 3)

    # ── Match winner (ML) ──
    # Migrated 2026-05-02 to engine.picks_core.score_pick.
    p1_ml = odds.get("p1_ml")
    p2_ml = odds.get("p2_ml")
    p1_label = prediction.get("p1_name") or f'p{prediction.get("p1_id")}'
    p2_label = prediction.get("p2_name") or f'p{prediction.get("p2_id")}'
    from .picks_core import score_pick as _score_pick
    for label, raw_p, ml in [(p1_label, p1_prob, p1_ml),
                              (p2_label, p2_prob, p2_ml)]:
        if ml is None:
            continue
        scored = _score_pick({
            "type": "ML", "pick": label,
            "raw_prob": raw_p, "odds": int(ml),
        }, sport="tennis", juice_wall=_juice_wall())
        if scored is None:
            continue
        # Tennis pick shape uses `model_prob` not `prob`, and adds
        # `confidence` + `conviction_score`. Reshape to match.
        scored["model_prob"] = scored.pop("prob")
        scored["confidence"] = _confidence_for_edge(scored["edge"])
        scored["conviction_score"] = round(
            _conviction_score(scored["model_prob"], scored["edge"]), 4)
        picks.append(scored)

    # ── Set spread (SET_SPREAD) ──
    p_set = _per_set_prob(p1_prob if best_of == 3 else _bo5_to_bo3(p1_prob))
    sp = _set_spread_probs(p_set, best_of)
    sp_inv = _set_spread_probs(1.0 - p_set, best_of)

    def _set_pick(player_label: str, point: float, odds_val: int | None,
                   prob: float) -> None:
        if odds_val is None:
            return
        sign = "+" if point > 0 else ""
        from .picks_core import score_pick as _score_pick
        scored = _score_pick({
            "type": "SET_SPREAD",
            "pick": f"{player_label} {sign}{point}",
            "raw_prob": prob,
            "odds": int(odds_val),
        }, sport="tennis", juice_wall=_juice_wall())
        if scored is None:
            return
        # Reshape to tennis pick schema (model_prob + confidence +
        # conviction_score) — same as ML migration.
        scored["model_prob"] = scored.pop("prob")
        scored["confidence"] = _confidence_for_edge(scored["edge"])
        scored["conviction_score"] = round(
            _conviction_score(scored["model_prob"], scored["edge"]), 4)
        picks.append(scored)

    p1_label = prediction.get("p1_name") or f'p{prediction.get("p1_id")}'
    p2_label = prediction.get("p2_name") or f'p{prediction.get("p2_id")}'
    p1_pt = odds.get("p1_set_spread_point")
    p1_pt_odds = odds.get("p1_set_spread_odds")
    p2_pt = odds.get("p2_set_spread_point")
    p2_pt_odds = odds.get("p2_set_spread_odds")

    # Map the requested point to the corresponding probability.
    def _prob_for_point(side_probs: dict, point: float) -> float | None:
        # BO3: -1.5 ↔ cover_minus_1_5; +1.5 ↔ cover_plus_1_5
        # BO5: -2.5 / -1.5 / +1.5 / +2.5 supported
        key = None
        if best_of == 3:
            if abs(point + 1.5) < 1e-6:
                key = "cover_minus_1_5"
            elif abs(point - 1.5) < 1e-6:
                key = "cover_plus_1_5"
        else:
            mapping = {-2.5: "cover_minus_2_5", -1.5: "cover_minus_1_5",
                        1.5: "cover_plus_1_5",  2.5: "cover_plus_2_5"}
            for pt, k in mapping.items():
                if abs(point - pt) < 1e-6:
                    key = k
                    break
        return side_probs.get(key) if key else None

    if p1_pt is not None and p1_pt_odds is not None:
        prob = _prob_for_point(sp, float(p1_pt))
        if prob is not None:
            _set_pick(p1_label, float(p1_pt), p1_pt_odds, prob)
    if p2_pt is not None and p2_pt_odds is not None:
        prob = _prob_for_point(sp_inv, float(p2_pt))
        if prob is not None:
            _set_pick(p2_label, float(p2_pt), p2_pt_odds, prob)

    return picks


def _bo5_to_bo3(p_match_bo5: float) -> float:
    """When the Elo-derived prob is in BO5 form (Slam) but we need
    the per-set prob (which our inversion is calibrated for BO3),
    convert BO5 → BO3 by going through per-set first.

    p_set = invert BO5 formula → re-apply BO3 formula.
    """
    # BO5 formula: p_match = p³(10 - 15p + 6p²). Invert via bisect.
    target = p_match_bo5
    if target <= 0:
        return 0.0
    if target >= 1:
        return 1.0
    lo, hi = (0.5, 0.999) if target >= 0.5 else (0.001, 0.5)
    for _ in range(50):
        mid = (lo + hi) / 2.0
        f = mid ** 3 * (10.0 - 15.0 * mid + 6.0 * mid * mid)
        if abs(f - target) < 1e-6:
            return mid * mid * (3.0 - 2.0 * mid)
        if f < target:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


def _score_full_markets(prediction: dict, markets: dict, *,
                          min_edge_ml: float, min_edge_other: float) -> list[dict]:
    """Score the rich HR-derived markets dict. One pick per
    market-side that clears its edge floor. Conviction follows the
    POTD formula and bet_type strings match what the tracker
    already understands."""
    picks: list[dict] = []
    p1_prob = float(prediction.get("p1_win_prob") or 0.0)
    p2_prob = float(prediction.get("p2_win_prob") or 0.0)
    best_of = int(prediction.get("best_of") or 3)
    p1_label = prediction.get("p1_name") or f'p{prediction.get("p1_id")}'
    p2_label = prediction.get("p2_name") or f'p{prediction.get("p2_id")}'
    p_set = _per_set_prob(p1_prob if best_of == 3 else _bo5_to_bo3(p1_prob))

    def _emit(bt: str, pick_text: str, odds_val: int | None,
              prob: float, floor: float) -> None:
        # Migrated 2026-05-02 to picks_core.score_pick. All tennis
        # markets (ML, SET_SPREAD, TOTAL_GAMES, TOTAL_SETS,
        # WIN_AT_LEAST_ONE_SET, P1/P2_TOTAL_GAMES) flow through this
        # helper, so migrating it here covers the whole rich-markets
        # path in one edit. The local `floor` arg is honored after
        # picks_core's data-driven floor — whichever is stricter wins.
        if odds_val is None:
            return
        from .picks_core import score_pick as _score_pick
        scored = _score_pick({
            "type": bt,
            "pick": pick_text,
            "raw_prob": prob,
            "odds": int(odds_val),
        }, sport="tennis", juice_wall=_juice_wall())
        if scored is None:
            return
        if scored["edge"] < floor:
            return
        # Reshape to tennis schema (model_prob + confidence +
        # conviction_score). picks_core ships `prob`; tennis uses
        # `model_prob` everywhere downstream.
        scored["model_prob"] = scored.pop("prob")
        scored["confidence"] = _confidence_for_edge(scored["edge"])
        scored["conviction_score"] = round(
            _conviction_score(scored["model_prob"], scored["edge"]), 4)
        picks.append(scored)

    # ── ML ──
    ml = markets.get("ml") or {}
    _emit("ML", p1_label, ml.get("p1_odds"), p1_prob, min_edge_ml)
    _emit("ML", p2_label, ml.get("p2_odds"), p2_prob, min_edge_ml)

    # ── Set spread ──
    sp_inv = _set_spread_probs(1.0 - p_set, best_of)
    sp = _set_spread_probs(p_set, best_of)
    for entry in (markets.get("set_spread") or []):
        player = entry.get("player")
        point = entry.get("point")
        odds_v = entry.get("odds")
        if point is None or odds_v is None:
            continue
        side_probs = sp if player == "p1" else sp_inv
        prob_key = None
        if best_of == 3:
            if abs(point + 1.5) < 1e-6:
                prob_key = "cover_minus_1_5"
            elif abs(point - 1.5) < 1e-6:
                prob_key = "cover_plus_1_5"
        else:
            mapping = {-2.5: "cover_minus_2_5", -1.5: "cover_minus_1_5",
                        1.5: "cover_plus_1_5",  2.5: "cover_plus_2_5"}
            prob_key = mapping.get(round(point, 1))
        prob = side_probs.get(prob_key) if prob_key else None
        if prob is None:
            continue
        sign = "+" if point > 0 else ""
        label = p1_label if player == "p1" else p2_label
        _emit("SET_SPREAD", f"{label} {sign}{point}",
              odds_v, prob, min_edge_other)

    # ── Total sets (BO3: 2.5; BO5: 3.5/4.5) ──
    ts = markets.get("total_sets") or {}
    line = ts.get("line")
    if isinstance(line, (int, float)):
        # P(over N.5 sets) — for BO3, only 2.5 line is meaningful;
        # match goes to 2 or 3 sets.
        if best_of == 3 and abs(line - 2.5) < 1e-6:
            # Over 2.5 = match goes 3 sets = 1 - (P(2-0) + P(0-2))
            p_2_0 = p_set ** 2
            p_0_2 = (1 - p_set) ** 2
            p_over = 1.0 - p_2_0 - p_0_2
            _emit("TOTAL_SETS", f"Over {line}",
                  ts.get("over_odds"), p_over, min_edge_other)
            _emit("TOTAL_SETS", f"Under {line}",
                  ts.get("under_odds"), 1.0 - p_over, min_edge_other)

    # ── Set betting (correct sets score) ──
    # Disabled 2026-05-02 (user directive). Tracker showed 0-30 W/L on
    # SET_BETTING — the worst single market in the entire tracker. The
    # parametric set-spread distribution we derive from match win prob
    # doesn't capture the variance of correct-score outcomes well, and
    # any small calibration miss compounds into 6 mispriced sub-buckets
    # per match. Set spread (cumulative ≥ N sets) and total sets remain
    # active because they don't fragment along the same axis.

    # ── Win at least one set (a/k/a "to win a set") ──
    sp = _set_spread_probs(p_set, best_of)
    sp_inv = _set_spread_probs(1.0 - p_set, best_of)
    p1_wls = markets.get("p1_win_at_least_one_set") or {}
    p2_wls = markets.get("p2_win_at_least_one_set") or {}
    if best_of == 3:
        # P(p1 wins at least 1 set) = 1 - P(p1 loses 0-2) = 1 - sp_inv['p_2_0']
        p1_at_least_1 = 1.0 - (sp_inv.get("p_2_0") or 0.0)
        p2_at_least_1 = 1.0 - (sp.get("p_2_0") or 0.0)
    else:
        p1_at_least_1 = 1.0 - (sp_inv.get("p_3_0") or 0.0)
        p2_at_least_1 = 1.0 - (sp.get("p_3_0") or 0.0)
    _emit("WIN_AT_LEAST_ONE_SET", f"{p1_label} Yes",
          p1_wls.get("yes_odds"), p1_at_least_1, min_edge_other)
    _emit("WIN_AT_LEAST_ONE_SET", f"{p1_label} No",
          p1_wls.get("no_odds"), 1.0 - p1_at_least_1, min_edge_other)
    _emit("WIN_AT_LEAST_ONE_SET", f"{p2_label} Yes",
          p2_wls.get("yes_odds"), p2_at_least_1, min_edge_other)
    _emit("WIN_AT_LEAST_ONE_SET", f"{p2_label} No",
          p2_wls.get("no_odds"), 1.0 - p2_at_least_1, min_edge_other)

    # ── Total games (full match) ──
    # Approximate: expected games per match ~ avg sets × avg games
    # per set. For a balanced match (p_set=0.5) avg sets is BO3=2.67
    # / BO5=3.92, avg games per set ~ 9.5. We use the GBM-derived
    # ``expected_total_games`` from prediction when available; falls
    # back to a parametric estimate.
    tg = markets.get("total_games") or {}
    line = tg.get("line")
    if isinstance(line, (int, float)):
        exp_games = float(prediction.get("expected_total_games") or 0)
        # Sanity-clamp: GBM occasionally returns pathologically high
        # totals when fed wrong best_of context (Madrid was stored
        # as BO5 → GBM weighted slam-shaped totals → exp_games ~35
        # when reality for a BO3 Masters is ~22). Cap at the
        # theoretical max for the format, plus a small slack.
        max_exp = 39 if best_of == 3 else 65
        if exp_games > max_exp:
            exp_games = 0  # force fallback
        if exp_games <= 0:
            # Parametric fallback: avg sets per match × avg games per set.
            p, q = p_set, 1.0 - p_set
            if best_of == 3:
                # Sets ∈ {2, 3}: P(2 sets) = p² + q² (sweep), else 3.
                avg_sets = (p ** 2 + q ** 2) * 2 + \
                           (1 - p ** 2 - q ** 2) * 3
            else:
                # BO5 sets ∈ {3, 4, 5}. Exact formula:
                #   3 * P(3-0 + 0-3) + 4 * P(3-1 + 1-3) + 5 * P(3-2 + 2-3)
                p_3_0 = p ** 3 + q ** 3
                p_3_1 = 3 * p ** 3 * q + 3 * p * q ** 3
                p_3_2 = 6 * p ** 3 * q ** 2 + 6 * p ** 2 * q ** 3
                avg_sets = 3 * p_3_0 + 4 * p_3_1 + 5 * p_3_2
            avg_games_per_set = 9.5  # league average
            exp_games = avg_sets * avg_games_per_set
        # Standard deviation in total games is typically ~6-8 over a match
        sigma = 7.0 if best_of == 3 else 9.0
        from math import erf, sqrt
        z = (line - exp_games) / sigma
        p_under = 0.5 * (1 + erf(z / sqrt(2)))
        p_over = 1.0 - p_under
        _emit("TOTAL_GAMES", f"Over {line}",
              tg.get("over_odds"), p_over, min_edge_other)
        _emit("TOTAL_GAMES", f"Under {line}",
              tg.get("under_odds"), p_under, min_edge_other)

    return picks


__all__ = ["generate_tennis_picks"]
