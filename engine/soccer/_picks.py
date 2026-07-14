"""Soccer picks engine.

Routes per-match predictions through the shared picks_core pipeline
(calibration + edge floors + odds caps + belief gate) so the same
universal rules apply across every sport.

Markets emitted (when HR has odds for them):

    ML (1X2)    — Home / Draw / Away each as separate ML candidates.
                   Three-way market, but picks_core treats each side
                   as an independent binary edge calc.
    OU          — Over/Under primary main line (usually 2.5)
    BTTS        — Both teams to score Yes / No
    DNB         — Draw No Bet (home or away)
    DC          — Double Chance (home_draw / draw_away / home_away)
    AH          — Asian Handicap (primary main line)

Per-bet-type belief gate (raw_prob ≥ 0.50) is applied inside
picks_core. Edge floors are data-driven (see engine.edge_floors).
"""
from __future__ import annotations

import logging
from typing import Iterable

from ._config import get_league_config
from ._db import get_conn
from ._predict import predict_slate

logger = logging.getLogger(__name__)


# Juice wall — American-odds floor (most-negative). Soccer 1X2 prices
# can sit at -300 or worse on PSG-vs-bottom-of-Ligue-1 type mismatches;
# beyond -250 the model needs an absurd edge to overcome implied vig
# and historically loses there. Same shape as MLB_JUICE_WALL = -200.
JUICE_WALL = -250


def _ml_picks(league: str, matchup: str, match_id: int,
              pred: dict, odds: dict) -> list[dict]:
    """Three independent 1X2 candidates — home / draw / away."""
    ml = odds.get("ml") or {}
    out = []
    from ..picks_core import score_pick as _score_pick
    for side, prob_key, odds_key, label in (
        ("home", "p_home", "home", pred["home_abbr"]),
        ("draw", "p_draw", "draw", "Draw"),
        ("away", "p_away", "away", pred["away_abbr"]),
    ):
        ml_odds = ml.get(odds_key)
        if ml_odds is None:
            continue
        scored = _score_pick({
            "type": "ML", "pick": label,
            "raw_prob": pred[prob_key],
            "odds": ml_odds,
            "direction": side,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["side"] = side
            out.append(scored)
    return out


def _ou_picks(league: str, matchup: str, match_id: int,
              pred: dict, odds: dict) -> list[dict]:
    """OU pick on the primary main line. Probability comes from the
    predictor's joint score grid — already line-specific."""
    total = odds.get("total") or {}
    line = total.get("line")
    if line is None:
        return []
    # Pick the over/under threshold the predictor returned. We carry
    # only 1.5 / 2.5 / 3.5 — match the closest.
    line_map = {1.5: ("p_over_15", "p_under_15"),
                 2.5: ("p_over_25", "p_under_25"),
                 3.5: ("p_over_35", "p_under_35")}
    closest = min(line_map.keys(), key=lambda x: abs(x - line))
    over_key, under_key = line_map[closest]
    p_over = pred.get(over_key, 0.5)
    p_under = pred.get(under_key, 0.5)
    out = []
    from ..picks_core import score_pick as _score_pick
    for direction, prob, ml in (
        ("over",  p_over,  total.get("over_odds")),
        ("under", p_under, total.get("under_odds")),
    ):
        if ml is None:
            continue
        scored = _score_pick({
            "type": "OU",
            "pick": f"{direction.capitalize()} {line}",
            "raw_prob": prob,
            "odds": ml,
            "direction": direction,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["line"] = line
            scored["side"] = direction
            out.append(scored)
    return out


def _btts_picks(league: str, matchup: str, match_id: int,
              pred: dict, odds: dict) -> list[dict]:
    btts = odds.get("btts") or {}
    if not btts:
        return []
    out = []
    from ..picks_core import score_pick as _score_pick
    for direction, prob, ml in (
        ("yes", pred.get("p_btts_yes", 0.0), btts.get("yes")),
        ("no",  pred.get("p_btts_no", 0.0),  btts.get("no")),
    ):
        if ml is None:
            continue
        scored = _score_pick({
            "type": "BTTS",
            "pick": f"BTTS {direction.capitalize()}",
            "raw_prob": prob,
            "odds": ml,
            "direction": direction,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["side"] = direction
            out.append(scored)
    return out


def _advance_picks(league: str, matchup: str, match_id: int,
                    pred: dict, odds: dict) -> list[dict]:
    """To Advance. Knockout-stage only; if HR doesn't ship advance odds
    for the fixture the emitter returns early. Binary (home/away),
    belief-gate compatible when one team is clearly favored to
    advance. Combines regulation + ET + shootout probability into a
    single side, so ML-derived edges don't double-count with this."""
    adv = odds.get("advance") or {}
    if not adv:
        return []
    out = []
    from ..picks_core import score_pick as _score_pick
    for side, prob, ml in (
        ("home", pred.get("p_advance_home", 0.5), adv.get("home")),
        ("away", pred.get("p_advance_away", 0.5), adv.get("away")),
    ):
        if ml is None:
            continue
        label = pred["home_abbr"] if side == "home" else pred["away_abbr"]
        scored = _score_pick({
            "type": "ADVANCE",
            "pick": f"{label} to Advance",
            "raw_prob": prob,
            "odds": ml,
            "direction": side,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["side"] = side
            out.append(scored)
    return out


def _gibh_picks(league: str, matchup: str, match_id: int,
              pred: dict, odds: dict) -> list[dict]:
    """Goal in Both Halves. Yes = ≥1 goal in H1 AND ≥1 goal in H2;
    No = at least one half scoreless. Same binary shape as BTTS so
    the belief gate handles it cleanly."""
    gibh = odds.get("gibh") or {}
    if not gibh:
        return []
    out = []
    from ..picks_core import score_pick as _score_pick
    for direction, prob, ml in (
        ("yes", pred.get("p_gibh_yes", 0.0), gibh.get("yes")),
        ("no",  pred.get("p_gibh_no", 0.0),  gibh.get("no")),
    ):
        if ml is None:
            continue
        scored = _score_pick({
            "type": "GIBH",
            "pick": f"Goal Both Halves {direction.capitalize()}",
            "raw_prob": prob,
            "odds": ml,
            "direction": direction,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["side"] = direction
            out.append(scored)
    return out


def _dnb_picks(league: str, matchup: str, match_id: int,
              pred: dict, odds: dict) -> list[dict]:
    dnb = odds.get("dnb") or {}
    if not dnb:
        return []
    out = []
    from ..picks_core import score_pick as _score_pick
    for side, prob, ml in (
        ("home", pred.get("p_dnb_home", 0.5), dnb.get("home")),
        ("away", pred.get("p_dnb_away", 0.5), dnb.get("away")),
    ):
        if ml is None:
            continue
        label = pred["home_abbr"] if side == "home" else pred["away_abbr"]
        scored = _score_pick({
            "type": "DNB",
            "pick": f"{label} (DNB)",
            "raw_prob": prob,
            "odds": ml,
            "direction": side,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["side"] = side
            out.append(scored)
    return out


def _p_home_covers_ah(lam_h: float, lam_a: float, line: float,
                       max_goals: int = 8) -> float:
    """P(home_score + line > away_score) under independent Poissons.
    Soccer Dixon-Coles ρ correction is small at the integer grid AH
    cares about; we skip it for AH (the inaccuracy is < 1pp and AH
    half-lines never land on the (0,0)/(0,1)/(1,0)/(1,1) cells the
    correction touches)."""
    import math as _math
    p = 0.0
    p_h = [_math.exp(-lam_h) * (lam_h ** k) / _math.factorial(k)
           for k in range(max_goals + 1)]
    p_a = [_math.exp(-lam_a) * (lam_a ** k) / _math.factorial(k)
           for k in range(max_goals + 1)]
    for h in range(max_goals + 1):
        for a in range(max_goals + 1):
            adj = (h - a) + line
            if adj > 0:
                p += p_h[h] * p_a[a]
            elif adj == 0:
                # push — half stake refund. For pricing purposes we
                # treat push as 0 (line is rarely an integer in soccer
                # AH; this branch fires only at +0/-0 which is
                # equivalent to DNB).
                pass
    return p


def _ah_picks(league: str, matchup: str, match_id: int,
              pred: dict, odds: dict) -> list[dict]:
    """Asian handicap — main line + relevant alts. We pick the cheapest
    line on each side (one home pick, one away pick) so the picks
    engine isn't spamming every alt on a chalk matchup."""
    ah = odds.get("ah") or {}
    if not ah:
        return []
    line = ah.get("line")
    home_odds = ah.get("home_odds")
    away_odds = ah.get("away_odds")
    if line is None or home_odds is None or away_odds is None:
        return []
    lam_h = pred.get("lambda_home")
    lam_a = pred.get("lambda_away")
    if lam_h is None or lam_a is None:
        return []
    p_home = _p_home_covers_ah(lam_h, lam_a, line)
    p_away = 1.0 - p_home
    out = []
    from ..picks_core import score_pick as _score_pick
    for side, prob, ml, label_line in (
        ("home", p_home, home_odds, line),
        # Away line is the negation: -line for the away side
        ("away", p_away, away_odds, -line),
    ):
        sign = "+" if label_line >= 0 else ""
        scored = _score_pick({
            "type": "AH",
            "pick": f"{pred['home_abbr'] if side == 'home' else pred['away_abbr']} {sign}{label_line}",
            "raw_prob": prob,
            "odds": ml,
            "direction": side,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["side"] = side
            scored["line"] = label_line
            out.append(scored)
    return out


def _dc_picks(league: str, matchup: str, match_id: int,
              pred: dict, odds: dict) -> list[dict]:
    dc = odds.get("dc") or {}
    if not dc:
        return []
    out = []
    from ..picks_core import score_pick as _score_pick
    for side, prob_key, odds_key, label in (
        ("home_draw", "p_dc_home", "home_draw", f"{pred['home_abbr']} or Draw"),
        ("draw_away", "p_dc_away", "draw_away", f"Draw or {pred['away_abbr']}"),
        ("home_away", "p_dc_draw", "home_away", f"{pred['home_abbr']} or {pred['away_abbr']}"),
    ):
        ml = dc.get(odds_key)
        if ml is None:
            continue
        scored = _score_pick({
            "type": "DC",
            "pick": label,
            "raw_prob": pred.get(prob_key, 0.5),
            "odds": ml,
            "direction": side,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["side"] = side
            out.append(scored)
    return out


def _h1_ml_picks(league: str, matchup: str, match_id: int,
              pred: dict, odds: dict) -> list[dict]:
    """H1 1X2 candidates. Same shape as full-game ML, just with
    h1-prefixed prob keys + odds from the h1 sub-dict."""
    h1_odds = odds.get("h1") or {}
    ml = h1_odds.get("ml") or {}
    if not ml:
        return []
    out = []
    from ..picks_core import score_pick as _score_pick
    for side, prob_key, odds_key, label in (
        ("home", "p_h1_home", "home", pred["home_abbr"]),
        ("draw", "p_h1_draw", "draw", "Draw"),
        ("away", "p_h1_away", "away", pred["away_abbr"]),
    ):
        ml_odds = ml.get(odds_key)
        if ml_odds is None:
            continue
        scored = _score_pick({
            "type": "H1_ML", "pick": f"{label} (H1)",
            "raw_prob": pred.get(prob_key, 0.0),
            "odds": ml_odds,
            "direction": side,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["side"] = side
            out.append(scored)
    return out


def _h1_ou_picks(league: str, matchup: str, match_id: int,
              pred: dict, odds: dict) -> list[dict]:
    h1_odds = odds.get("h1") or {}
    total = h1_odds.get("total") or {}
    line = total.get("line")
    if line is None:
        return []
    line_map = {0.5: ("p_h1_over_05", "p_h1_under_05"),
                 1.5: ("p_h1_over_15", "p_h1_under_15"),
                 2.5: ("p_h1_over_25", "p_h1_under_25")}
    closest = min(line_map.keys(), key=lambda x: abs(x - line))
    over_key, under_key = line_map[closest]
    out = []
    from ..picks_core import score_pick as _score_pick
    for direction, prob, ml in (
        ("over",  pred.get(over_key, 0.5),  total.get("over_odds")),
        ("under", pred.get(under_key, 0.5), total.get("under_odds")),
    ):
        if ml is None:
            continue
        scored = _score_pick({
            "type": "H1_TOTAL",
            "pick": f"{direction.capitalize()} {line} (H1)",
            "raw_prob": prob,
            "odds": ml,
            "direction": direction,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["line"] = line
            scored["side"] = direction
            out.append(scored)
    return out


def _h1_btts_picks(league: str, matchup: str, match_id: int,
              pred: dict, odds: dict) -> list[dict]:
    h1_odds = odds.get("h1") or {}
    btts = h1_odds.get("btts") or {}
    if not btts:
        return []
    out = []
    from ..picks_core import score_pick as _score_pick
    for direction, prob, ml in (
        ("yes", pred.get("p_h1_btts_yes", 0.0), btts.get("yes")),
        ("no",  pred.get("p_h1_btts_no", 0.0),  btts.get("no")),
    ):
        if ml is None:
            continue
        scored = _score_pick({
            "type": "H1_BTTS",
            "pick": f"BTTS {direction.capitalize()} (H1)",
            "raw_prob": prob,
            "odds": ml,
            "direction": direction,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["side"] = direction
            out.append(scored)
    return out


def _h1_dnb_picks(league: str, matchup: str, match_id: int,
              pred: dict, odds: dict) -> list[dict]:
    h1_odds = odds.get("h1") or {}
    dnb = h1_odds.get("dnb") or {}
    if not dnb:
        return []
    out = []
    from ..picks_core import score_pick as _score_pick
    for side, prob, ml in (
        ("home", pred.get("p_h1_dnb_home", 0.5), dnb.get("home")),
        ("away", pred.get("p_h1_dnb_away", 0.5), dnb.get("away")),
    ):
        if ml is None:
            continue
        label = pred["home_abbr"] if side == "home" else pred["away_abbr"]
        scored = _score_pick({
            "type": "H1_DNB",
            "pick": f"{label} (H1 DNB)",
            "raw_prob": prob,
            "odds": ml,
            "direction": side,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["side"] = side
            out.append(scored)
    return out


def _h1_dc_picks(league: str, matchup: str, match_id: int,
              pred: dict, odds: dict) -> list[dict]:
    h1_odds = odds.get("h1") or {}
    dc = h1_odds.get("dc") or {}
    if not dc:
        return []
    out = []
    from ..picks_core import score_pick as _score_pick
    for side, prob_key, odds_key, label in (
        ("home_draw", "p_h1_dc_home", "home_draw",
          f"{pred['home_abbr']} or Draw (H1)"),
        ("draw_away", "p_h1_dc_away", "draw_away",
          f"Draw or {pred['away_abbr']} (H1)"),
        ("home_away", "p_h1_dc_draw", "home_away",
          f"{pred['home_abbr']} or {pred['away_abbr']} (H1)"),
    ):
        ml = dc.get(odds_key)
        if ml is None:
            continue
        scored = _score_pick({
            "type": "H1_DC",
            "pick": label,
            "raw_prob": pred.get(prob_key, 0.5),
            "odds": ml,
            "direction": side,
        }, sport=league, juice_wall=JUICE_WALL)
        if scored:
            scored["matchup"] = matchup
            scored["match_id"] = match_id
            scored["side"] = side
            out.append(scored)
    return out


def _format_matchup(pred: dict) -> str:
    return f"{pred['away_abbr']} @ {pred['home_abbr']}"


# ── Public entry points ──────────────────────────────────────

def generate_picks_for_match(league: str, pred: dict, odds: dict
                             ) -> list[dict]:
    """All markets we model for one match. Caller supplies the predictor
    dict (from predict_match) + the parsed HR odds bundle.

    Returns at most ONE FULL-GAME pick + ONE H1 pick per match — the
    highest-edge candidate within each scope family. Soccer's market
    overlap means a MTL favorite often clears edge on ML + DNB + DC + AH
    all at once (different prices on the same underlying outcome) which
    drags the tracker into "4 picks per game" territory. Top-1 per
    family keeps the card honest and the tracker single-bet per scope
    — matches the "ONE pick per game" non-negotiable from the standard
    layout (Q1/H1 separately scoped from full game, same as basketball).
    """
    matchup = _format_matchup(pred)
    match_id = pred.get("match_id")
    full: list[dict] = []
    full += _ml_picks(league, matchup, match_id, pred, odds)
    full += _ou_picks(league, matchup, match_id, pred, odds)
    full += _btts_picks(league, matchup, match_id, pred, odds)
    full += _gibh_picks(league, matchup, match_id, pred, odds)
    full += _dnb_picks(league, matchup, match_id, pred, odds)
    full += _dc_picks(league, matchup, match_id, pred, odds)
    full += _ah_picks(league, matchup, match_id, pred, odds)

    h1: list[dict] = []
    h1 += _h1_ml_picks(league, matchup, match_id, pred, odds)
    h1 += _h1_ou_picks(league, matchup, match_id, pred, odds)
    h1 += _h1_btts_picks(league, matchup, match_id, pred, odds)
    h1 += _h1_dnb_picks(league, matchup, match_id, pred, odds)
    h1 += _h1_dc_picks(league, matchup, match_id, pred, odds)

    # Advance is its own family — resolves on regulation+ET+shootout,
    # not on 90-min outcome, so it doesn't compete with ML/DC/DNB for
    # the same slot. KO fixtures get up to 3 emissions: 1 full + 1 H1
    # + 1 advance. Group-stage games have no advance market at HR, so
    # the family is empty and the top-1 slot is skipped.
    advance: list[dict] = []
    advance += _advance_picks(league, matchup, match_id, pred, odds)

    # Drop anything that went negative-edge post-calibration.
    full = [p for p in full if (p.get("edge") or 0) > 0]
    h1 = [p for p in h1 if (p.get("edge") or 0) > 0]
    advance = [p for p in advance if (p.get("edge") or 0) > 0]

    def _top1(cands: list[dict]) -> list[dict]:
        if not cands:
            return []
        cands.sort(key=lambda p: (-(p.get("edge") or 0),
                                   -(p.get("raw_prob") or 0)))
        return [cands[0]]

    out = _top1(full) + _top1(h1) + _top1(advance)

    # World Cup over-confidence guard. Group-stage data (2026-06-11..15,
    # n=21 settled picks) showed a tight calibration pattern:
    #   model–market gap   WR
    #   <5pp              100% (2/2)
    #   5-15pp             55% (6/11)
    #   15-25pp            60% (3/5)
    #   ≥25pp               0% (0/3)
    # The 0/3 at ≥25pp covers HAI DNB (model 78% vs market 24%),
    # CUW +3.5 (81% vs 52%), ECU Over 1.5 (86% vs 62%). When the
    # market is THAT confident the model is wrong, the market is. Cap
    # the deviation by shadow-banning picks with model_prob - market_prob
    # > 0.25. They still record so we can keep learning — same shape as
    # the probation gate.
    if league == "fifa_world_cup":
        for p in out:
            odds = p.get("odds") or 0
            if odds:
                implied = (abs(odds) / (abs(odds) + 100) if odds < 0
                           else 100 / (odds + 100))
                # Try the calibrated `prob` first, fall back to raw_prob
                # (different pick shapes across markets).
                mp = (p.get("prob") if p.get("prob") is not None
                      else p.get("model_prob") if p.get("model_prob") is not None
                      else p.get("raw_prob") or 0)
                gap = mp - implied
                if gap > 0.25:
                    p["stake_units"] = 0.0
                    p["shadow"] = True
                    p["confidence"] = "over_confident"

    # World Cup probation. Originally lifted at 10 settled picks; the
    # n=10 threshold proved too low — picks 11-30 went 9W-13L at -$600
    # notional after early gate release. Two changes 2026-06-20:
    #   1. Threshold raised to 30 settled picks (whole group stage).
    #   2. Health-check: even after threshold, recent-20 settled picks
    #      must show WR ≥ 45%. If not, re-engage probation. Stays in
    #      until calibration recovers — prevents the model from sizing
    #      real while it's actively wrong.
    if league == "fifa_world_cup":
        try:
            from ._db import get_conn as _gc
            conn = _gc(league)
            n_settled = conn.execute(
                "SELECT COUNT(*) FROM picks WHERE result IN ('W','L','P')"
            ).fetchone()[0]
            recent_rows = conn.execute(
                "SELECT result FROM picks WHERE result IN ('W','L') "
                "ORDER BY id DESC LIMIT 20"
            ).fetchall()
            recent_w = sum(1 for r in recent_rows if r[0] == 'W')
            recent_wr = (recent_w / len(recent_rows)) if recent_rows else 0.0
        except Exception:
            n_settled = 0
            recent_wr = 0.0
        in_probation = n_settled < 30 or (
            len(recent_rows) >= 10 and recent_wr < 0.45
        ) if recent_rows else (n_settled < 30)
        if in_probation:
            for p in out:
                p["stake_units"] = 0.0
                p["shadow"] = True
                p["confidence"] = "probation"
    return out


def generate_picks_for_slate(league: str, date: str,
                              *, force_odds_refresh: bool = False
                              ) -> dict:
    """Generate picks for every match on ``date`` in ``league``.

    Returns ``{matches, picks, top_pick}`` so a single call serves both
    the slate route and the picks-recorder cron."""
    from ._odds import fetch_league_odds
    odds_map = fetch_league_odds(league, force=force_odds_refresh)
    preds = predict_slate(league, date)
    by_match: list[dict] = []
    all_picks: list[dict] = []
    for pred in preds:
        key = f"{pred['away_abbr']}@{pred['home_abbr']}"
        odds = odds_map.get(key) or {}
        picks = generate_picks_for_match(league, pred, odds)
        by_match.append({
            "match_id": pred["match_id"],
            "matchup": _format_matchup(pred),
            "start_time": pred.get("start_time"),
            "home": {"abbr": pred["home_abbr"], "name": pred.get("home_name"),
                      "logo": pred.get("home_logo")},
            "away": {"abbr": pred["away_abbr"], "name": pred.get("away_name"),
                      "logo": pred.get("away_logo")},
            "prediction": {
                "p_home": pred["p_home"],
                "p_draw": pred["p_draw"],
                "p_away": pred["p_away"],
                "p_over_25": pred["p_over_25"],
                "p_btts_yes": pred["p_btts_yes"],
                "p_gibh_yes": pred.get("p_gibh_yes"),
                "lambda_home": pred["lambda_home"],
                "lambda_away": pred["lambda_away"],
                "top_scores": pred.get("top_scores") or [],
                # Extended markets — surfaced for display / edge
                # inspection but not emitted as picks (see 2026-07-01
                # audit: CS/HTFT/WM's per-cell probs are below the
                # 50% belief gate by construction). Present them in
                # the drawer so a manual review can spot mispricings
                # even when the auto-picker doesn't act.
                "p_cs": pred.get("p_cs") or {},
                "p_htft": pred.get("p_htft") or {},
                "p_wm": pred.get("p_wm") or {},
                # KO-only, but harmless to expose in group stage
                # (advance market just isn't shipped for those).
                "p_advance_home": pred.get("p_advance_home"),
                "p_advance_away": pred.get("p_advance_away"),
            },
            "odds": odds,
            "picks": picks,
        })
        all_picks.extend(picks)
    all_picks.sort(key=lambda p: -(p.get("edge") or 0))
    return {
        "league": league,
        "date": date,
        "matches": by_match,
        "picks": all_picks,
        "top_pick": all_picks[0] if all_picks else None,
    }
