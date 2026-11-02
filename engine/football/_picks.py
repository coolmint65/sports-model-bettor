"""Football picks engine.

Routes ML / SPREAD / TOTAL candidates through ``engine.picks_core``
so the football leagues get the same juice-wall + odds-cap + edge-
floor + edge-ceiling gates the rest of the sports use.

Standard output dict shape::

    {"type", "pick", "raw_prob", "prob", "odds", "edge",
     "stake_units", "confidence", "side"}
"""
from __future__ import annotations

import logging
from typing import Any

from ..config import (
    NHL_JUICE_WALL,  # -200; matches the cross-sport baseline.
)
from ..picks_core import score_pick
from ._predict import predict_match
from ..market_blend import anchor_to_market as _anchor
from ..selection.edge_proven import market_weight

logger = logging.getLogger(__name__)


# Reuse the cross-sport juice wall. Football books are typically -110
# both sides on spread/total, so -200 catches the rare juicy ML
# favorites without blocking standard spread/total picks.
_JUICE_WALL = NHL_JUICE_WALL


# Leagues whose SHADOW slate records the RAW model's leans at market prices
# (pre-calibration) for the MAIN markets (ML/SPREAD/TOTAL) instead of score_pick
# survivors; derivative rungs are always score_pick-gated on both passes. Pick Authority v2 (Austin
# 2026-10-25): for a league with ZERO proven cells the calibrated/anchored pipeline
# yields ~3 survivors a week, so no bucket ever reaches STD_N=20 — the graduation
# loop is stuck. Raw leans (~30/wk for NFL) give every bucket a real ROI-at-market
# verdict in ~3-4 weeks. is_shadow ONLY: never headlined, and the calibrator fits on
# the tracker table (nfl.db picks + prediction_signals), not the unified store, so
# this cannot bleed into headline probabilities. Empty = off everywhere.
_SHADOW_RAW_LEAGUES: frozenset[str] = frozenset()
_SHADOW_RAW_MAX_EDGE = 20.0   # same ceiling as the headline pass (FBS-vs-FCS blowups)
_NEW_PERIOD_MAX_RUNGS = 3     # H2/Q2-Q4 ladders: price the 3 lines nearest the median (cost + correlation)


def _implied(american):
    try:
        o = float(american)
    except (TypeError, ValueError):
        return None
    if o == 0 or abs(o) < 100:
        return None
    return abs(o) / (abs(o) + 100.0) if o < 0 else 100.0 / (o + 100.0)


def _devig(american, opp):
    a, b = _implied(american), _implied(opp)
    if a is None or b is None or (a + b) <= 0:
        return None
    return a / (a + b)


def generate_picks(league: str, pred: dict, odds: dict | None,
                    *, game_id: str | None = None,
                    shadow: bool = False) -> list[Any]:
    """Return a list of edged picks for one game. Empty when no odds
    or no edge candidates clear the gates.

    Thin wrapper over :func:`generate_picks_both` — ``shadow=False`` returns the
    anchored HEADLINE candidates, ``shadow=True`` the un-anchored graduation-track
    candidates. Callers that need both should call generate_picks_both once (the
    MC derivative sampling is the expensive part and is shared)."""
    anchored, shadow_cands = generate_picks_both(league, pred, odds, game_id=game_id)
    return shadow_cands if shadow else anchored


def generate_picks_both(league: str, pred: dict, odds: dict | None,
                         *, game_id: str | None = None
                         ) -> tuple[list[Any], list[Any]]:
    """ONE pass, TWO lists: ``(headline, shadow)``.

    headline — the anchored pass: main markets anchored to the de-vigged market at
    the per-bucket ``market_weight`` (unproven buckets 0.15 = 85% market), then
    score_pick's gates (juice / odds cap / belief / calibration / edge floor+ceiling).
    This is the ONLY list the card / POTD / tracker headline select from.

    shadow — the graduation track (record_shadow_slate -> picks_unified is_shadow=1).
    Pick Authority, Austin 2026-10-25 "NFL — fix it up": feeding the anchored list here
    meant a league with zero proven cells (NFL: W=0.15 on every market) contributed
    NOTHING — an anchored dog never reaches the 0.50 belief gate and an anchored
    favorite is calibrated below its implied price — so edge_proven never accumulated
    and nothing could graduate. Default: the main markets scored on the model's OWN
    probability (anchor weight 1.0) through the same score_pick gates — exactly how
    the paper lane (_football_paper_picks) already scores. Leagues in
    _SHADOW_RAW_LEAGUES record RAW leans instead (see that constant).

    Derivative / period rungs anchor INSIDE engine.football._derivatives (price_samples /
    price_period call market_weight themselves), so they are priced TWICE off the SAME
    MC samples — anchor_w=None (headline) and anchor_w=1.0 (shadow); the simulation is
    the expensive part, the pricing is cheap. For every pick the shadow score runs FIRST
    so the anchored pass writes the LAST pick_provenance row: audits keep showing the
    decision that gated the card. Nothing un-anchored is ever selected or shown."""
    if not odds:
        return [], []
    picks: list[dict] = []
    shadow: list[dict] = []
    raw_mode = league in _SHADOW_RAW_LEAGUES
    sport = f"football_{league}"
    # The proven-edge gate scores FCS-only CFB games under their own segment ("cfb_fcs",
    # engine.football._segments, 2026-10-25); calibration / predict / recording keep `league`.
    from ._segments import edge_league
    ep_league = edge_league(league, pred.get("home_team_id"), pred.get("away_team_id"))

    matchup = f"{odds.get('away_abbr')}@{odds.get('home_abbr')}"

    def _attach(sc, side):
        # picks_core.score_pick drops anything not in its canonical output
        # dict — re-attach the matchup + game_id so the tracker can persist them.
        sc["matchup"] = matchup
        sc["game_id"] = game_id
        if side is not None:
            sc["side"] = side
        return sc

    def _raw_lean(bet_type, pick_text, prob, american, opp, side):
        """Raw-mode shadow candidate: the model's un-calibrated opinion at the
        market price. Juice wall + raw edge in (0, ceiling]; canonical shape."""
        if american is None or prob is None or float(american) < _JUICE_WALL:
            return None
        imp = _devig(american, opp)
        if imp is None:
            return None
        edge = (float(prob) - imp) * 100.0
        if edge <= 0 or edge > _SHADOW_RAW_MAX_EDGE:
            return None
        return _attach({"type": bet_type, "pick": pick_text, "raw_prob": float(prob),
                        "prob_raw": float(prob), "prob": float(prob), "odds": american,
                        "edge": round(edge, 1), "confidence": "lean",
                        "shadow_raw": True}, side)

    def _main(bet_type, pick_text, prob, american, opp, side):
        """A main-market side (ML / SPREAD / TOTAL): shadow first, headline second."""
        if raw_mode:
            rl = _raw_lean(bet_type, pick_text, prob, american, opp, side)
            if rl:
                shadow.append(rl)
        else:
            sc = score_pick({"type": bet_type, "pick": pick_text,
                             "raw_prob": _anchor(prob, american, opp, w=1.0),
                             "odds": american, "matchup": matchup, "game_id": game_id},
                            sport=sport, juice_wall=_JUICE_WALL)
            if sc:
                shadow.append(_attach(sc, side))
        w = market_weight("football", ep_league, bet_type, pick_text, american)
        sc = score_pick({"type": bet_type, "pick": pick_text,
                         "raw_prob": _anchor(prob, american, opp, w=w),
                         "odds": american, "matchup": matchup, "game_id": game_id},
                        sport=sport, juice_wall=_JUICE_WALL)
        if sc:
            picks.append(_attach(sc, side))

    # ── Moneyline ──
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")
    if home_ml is not None and away_ml is not None:
        for side, prob, american, opp in (
            ("home", pred["p_home"], home_ml, away_ml),
            ("away", pred["p_away"], away_ml, home_ml),
        ):
            label = (odds.get("home_abbr") if side == "home"
                      else odds.get("away_abbr"))
            _main("ML", label, prob, american, opp, side)

    # ── Spread ──
    spread = odds.get("home_spread_point")
    if (spread is not None
            and odds.get("home_spread_odds") is not None
            and odds.get("away_spread_odds") is not None):
        # Recompute cover probs against the actual posted line.
        from ._predict import predict_match as _pm
        with_spread = _pm(
            league, pred["home_team_id"], pred["away_team_id"],
            spread=spread,
        )
        for side, line, prob, american, opp in (
            ("home",  spread, with_spread["p_home_cover"],
             odds["home_spread_odds"], odds["away_spread_odds"]),
            ("away", -spread, with_spread["p_away_cover"],
             odds["away_spread_odds"], odds["home_spread_odds"]),
        ):
            label_team = (odds.get("home_abbr") if side == "home"
                            else odds.get("away_abbr"))
            sign = "+" if line > 0 else ""
            _main("SPREAD", f"{label_team} {sign}{line}", prob, american, opp, side)

    # ── Total ──
    total = odds.get("over_under")
    if (total is not None
            and odds.get("over_odds") is not None
            and odds.get("under_odds") is not None):
        with_total = predict_match(
            league, pred["home_team_id"], pred["away_team_id"],
            total_line=total,
        )
        for side, prob, american, opp in (
            ("over",  with_total["p_over"], odds["over_odds"], odds["under_odds"]),
            ("under", with_total["p_under"], odds["under_odds"], odds["over_odds"]),
        ):
            label = ("Over" if side == "over" else "Under") + f" {total}"
            _main("TOTAL", label, prob, american, opp, side)

    def _rung(c, lst, raw):
        """One priced derivative/period rung -> the given list. Headline (raw=False) and
        default shadow go through score_pick; raw-mode shadow keeps the pricer's own
        un-anchored edge (juice wall + (0, ceiling]) without calibration."""
        if raw:
            try:
                am = float(c["odds"]); e = float(c.get("edge") or 0.0)
            except (TypeError, ValueError):
                return
            if am < _JUICE_WALL or e <= 0 or e > _SHADOW_RAW_MAX_EDGE:
                return
            lst.append(_attach({"type": c["type"], "pick": c["pick"], "raw_prob": c["prob"],
                                "prob_raw": c["prob"], "prob": c["prob"], "odds": c["odds"],
                                "edge": round(e, 1), "confidence": "lean",
                                "shadow_raw": True}, None))
            return
        _sc = score_pick({"type": c["type"], "pick": c["pick"], "raw_prob": c["prob"],
                          "odds": c["odds"], "matchup": matchup, "game_id": game_id},
                         sport=sport, juice_wall=_JUICE_WALL)
        if _sc:
            lst.append(_attach(_sc, None))

    def _price_both(pricer, **kw):
        """Price the SAME samples twice: shadow (anchor_w=1.0) first, headline second.
        Derivative rungs ALWAYS go through score_pick on both passes — raw mode applies to
        the MAIN markets only. (Smoke test 2026-10-25: raw rungs produced 113-143 shadow rows
        per game — alt ladders are dozens of correlated copies of one opinion, not evidence.)"""
        for lst, aw in ((shadow, 1.0), (picks, None)):
            for _c in pricer(anchor_w=aw, **kw):
                _rung(_c, lst, False)

    # ── Derivatives (alt spread/total ladders + team totals) ──────────
    # Price every alt rung + team total off ONE extra sim run (Austin 2026-10-13,
    # market-expansion HIGH tier). Each candidate goes through the SAME
    # score_pick gates (juice/floor) + the 20pp ceiling below, so phantom
    # FBS-vs-FCS edges get dropped here exactly like the main markets.
    _alt_s = odds.get("alt_spreads") or []
    _alt_t = odds.get("alt_totals") or []
    _h_tt = odds.get("home_team_totals") or []
    _a_tt = odds.get("away_team_totals") or []
    _hh = _aa = None     # full-game MC samples, reused by the period block below
    if _alt_s or _alt_t or _h_tt or _a_tt:
        try:
            from ._derivatives import price_samples
            _base = predict_match(league, pred["home_team_id"], pred["away_team_id"],
                                  return_samples=True)
            _hh, _aa = _base.get("_h"), _base.get("_a")
            if _hh is not None:
                _price_both(lambda **k: price_samples(_hh, _aa, **k),
                            alt_spreads=_alt_s, alt_totals=_alt_t,
                            home_team_totals=_h_tt, away_team_totals=_a_tt,
                            home_abbr=odds.get("home_abbr"),
                            away_abbr=odds.get("away_abbr"), league=ep_league)
        except Exception as _e:
            logger.debug("derivative pricing failed (%s): %s", matchup, _e)

    # ── Period derivatives (1H/1Q spread/total/winner) (MintyBot 2026-10-16) ──
    # Same sim, scaled to the period; same score_pick gates + 20pp ceiling below.
    # All six periods (Austin 2026-10-25 "I like 1 and 2"): H1/Q1 as before (full ladders),
    # H2/Q2/Q3/Q4 new (ladders trimmed to _NEW_PERIOD_MAX_RUNGS nearest the median line),
    # period TEAM totals new on every period. HR ships ~1,300 H2/Q2-Q4 lines + ~650 period
    # team totals per NFL Sunday that the parser used to drop.
    for _per in ("H1", "Q1", "H2", "Q2", "Q3", "Q4"):
        _pd = odds.get(_per.lower())
        if not _pd or not (_pd.get("spreads") or _pd.get("totals")
                           or _pd.get("ml2") or _pd.get("ml3")
                           or _pd.get("home_team_totals") or _pd.get("away_team_totals")):
            continue
        try:
            from ._derivatives import price_period
            if _hh is None:
                _b = predict_match(league, pred["home_team_id"], pred["away_team_id"],
                                   return_samples=True)
                _hh, _aa = _b.get("_h"), _b.get("_a")
            if _hh is not None:
                _price_both(lambda **k: price_period(_hh, _aa, _per, **k),
                            spreads=_pd.get("spreads"), totals=_pd.get("totals"),
                            ml2=_pd.get("ml2"), ml3=_pd.get("ml3"),
                            home_team_totals=_pd.get("home_team_totals"),
                            away_team_totals=_pd.get("away_team_totals"),
                            btts=_pd.get("btts"),
                            max_rungs=(None if _per in ("H1", "Q1") else _NEW_PERIOD_MAX_RUNGS),
                            home_abbr=odds.get("home_abbr"),
                            away_abbr=odds.get("away_abbr"), league=ep_league)
        except Exception as _e:
            logger.debug("period pricing failed (%s %s): %s", matchup, _per, _e)

    # ── Game-script markets (2026-10-25, Austin "work on 3 to 5"): BTTS (+N+), lead-every-Q ──
    if odds.get("btts") or odds.get("btts_x") or odds.get("leq_home") or odds.get("leq_away"):
        try:
            from ._derivatives import price_game_script
            if _hh is None:
                _b = predict_match(league, pred["home_team_id"], pred["away_team_id"],
                                   return_samples=True)
                _hh, _aa = _b.get("_h"), _b.get("_a")
            if _hh is not None:
                _price_both(lambda **k: price_game_script(_hh, _aa, **k),
                            btts=odds.get("btts"), btts_x=odds.get("btts_x"),
                            leq_home=odds.get("leq_home"), leq_away=odds.get("leq_away"),
                            home_abbr=odds.get("home_abbr"), away_abbr=odds.get("away_abbr"),
                            league=ep_league)
        except Exception as _e:
            logger.debug("game-script pricing failed (%s): %s", matchup, _e)

    # Standard layout: one pick per game on the card. The picker keeps
    # all positive-edge candidates so the tracker can record them all,
    # but the headline is the top-edge candidate.
    # Edge ceiling (Austin 2026-09-19): a >20pp edge in football is almost always
    # the model wildly disagreeing with the market — the FBS-vs-FCS blind spot
    # (Elo rates an FCS team ~even vs a -3000 FBS favorite) or a similar blowup.
    # Drop them so garbage like "SDAK +1200 ML +25% edge" never becomes a pick.
    picks = [p for p in picks if abs(p.get("edge") or 0) <= 20]
    picks.sort(key=lambda p: -(p.get("edge") or 0))
    shadow = [p for p in shadow if abs(p.get("edge") or 0) <= 20]
    shadow.sort(key=lambda p: -(p.get("edge") or 0))
    return picks, shadow
