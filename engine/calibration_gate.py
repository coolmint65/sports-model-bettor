"""Calibration gate for POTD / 'best pick' headline selection.

Austin 2026-10-12: the cross-market merge started surfacing a P1 (period)
pick as the NHL POTD hero even though NHL period markets have almost no
tracked history and their calibrator is pure passthrough (raw 0.75 -> 0.75).
We were headlining an un-vetted, overconfident lean. Two rules fix it:

  1. Rank by CALIBRATED edge, not raw edge, so overconfident core markets get
     their haircut (NHL O/U raw 0.75 -> cal 0.59) BEFORE they compete for the
     headline. This is the "fix the calibration" half -- the displayed edge
     becomes the honest one.

  2. An "extra" market (period / quarter / derivative) may only HEADLINE once
     it has EARNED it with a real track record: >= EXTRA_MIN_SETTLED settled
     results over >= EXTRA_MIN_DAYS of history. Core full-game markets are the
     trusted default and are always eligible.

Both rules are data-driven: an extra market auto-qualifies for the headline
the moment it crosses the evidence bar -- no code flip. (Caveat: for its
calibrated edge to be meaningful once eligible, the extra market's results
must also feed refresh_calibration -- today NHL periods live in a separate
table that the calibrator doesn't ingest. Tracked as the follow-up.)

Shared on purpose so the per-sport sweep (apply to every sport's POTD) is a
one-call change rather than copy-paste.
"""
from __future__ import annotations

from engine.empirical_calibration import calibrated_edge, calibrate

# Football calibrates ONCE (Austin 2026-10-25 "A B and C", the Pick Authority spec): the
# de-vigged market anchor applied when football picks are priced (engine/football/_picks +
# _derivatives, weight = edge_proven.market_weight) IS the calibration, so every downstream
# re-calibration -- score_pick's empirical/closing-line/final stages, the card's cal_edge, the
# tracker's cal_prob -- passes football probs through unchanged. Those stages were fit on picks
# from retired football model versions (non-monotonic buckets, a flat ~7pp haircut). Keys: the
# score_pick sport ("football_<league>") and the bare league the football tracker passes.
_ANCHOR_ONLY = frozenset({"football_nfl", "football_cfb", "football_ufl", "nfl", "cfb", "ufl",
                          # NBA joined 2026-10-26 (Austin "NBA for PA"): its anchor
                          # (engine/sports/nba/_market_anchor, edge_proven.market_weight) was being
                          # re-calibrated by non-monotonic empirical buckets (SPREAD .55->.65,
                          # .60->.46) and the final haircut -- 0 of ~1,500 Q1 candidates survived.
                          "nba",
                          # College basketball joined 2026-10-26 (Austin "same standard as NBA"):
                          # engine.basketball._picks anchors each candidate with its PA weight;
                          # the framework walk-forward buckets were fit on the retired PPG model.
                          "ncaam", "ncaaw",
                          # NHL joined 2026-10-26 (Austin "make sure NHL is up to snuff"): the
                          # ratings model anchors each candidate with its PA weight
                          # (engine.sports.nhl._market_anchor); the empirical / blend / final /
                          # derivative stages were fit on the retired factor model and stacked
                          # four maps on one pick (puck line .595 -> .661 -> .627 -> .54).
                          "nhl",
                          # EPL joined 2026-10-28 (Austin "EPL up to standard"): the ridge xG model
                          # anchors each candidate with its PA weight (engine.soccer._market_anchor);
                          # the empirical / final stages were fit on the retired Elo model (an
                          # identity final_calibration entry had been papering over the global curve).
                          "eng_premier",
                          # China CBA joined 2026-10-28 (Austin backlog #5): the college-standard
                          # ratings model anchors each candidate with its PA weight.
                          "china_cba",
                          # Australia NBL joined 2026-10-29 (Austin "NBL ... up to standard fully"): same
                          # college-standard ratings model + PA anchor as CBA.
                          "australia_nbl"})


def anchor_only(sport) -> bool:
    """True for sports whose probs are already market-anchored = calibrated (football)."""
    return str(sport or "").lower() in _ANCHOR_ONLY


# Evidence bar for an EXTRA (period/quarter/derivative) market to be allowed
# to headline. Tunable -- NHL P1 totals today sit at ~19 settled / 11 days,
# well under this, so they stay off the hero until they're up to par.
EXTRA_MIN_SETTLED = 60
EXTRA_MIN_DAYS = 30


def _to_american(odds) -> int | None:
    """Coerce stored odds to an American integer. Picks tables are American
    throughout, but guard against a decimal sneaking in (|american| is always
    >= 100, so anything strictly inside (-100, 100) is treated as decimal)."""
    if odds is None:
        return None
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    if abs(o) >= 100:
        return int(round(o))
    if 1.0 < o < 100.0:                      # decimal -> american
        return int(round((o - 1) * 100)) if o >= 2.0 else int(round(-100.0 / (o - 1)))
    return int(round(o))


def _implied_am(am):
    """Implied probability from American odds (matches pick_of_day's _implied_from_odds)."""
    try:
        o = float(am)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    return (-o) / (-o + 100.0) if o < 0 else 100.0 / (o + 100.0)


def cal_edge(sport: str, bet_type, model_prob, odds,
             pick_text: str | None = None, raw_edge: float | None = None) -> float:
    """Calibrated edge (percentage points). Falls back to raw_edge (then a
    large-negative sentinel) if anything is missing or the calibrator errors,
    so a bad row never wins the max()."""
    try:
        am = _to_american(odds)
        if model_prob is None or am is None:
            return raw_edge if raw_edge is not None else -999.0
        _s = str(sport or "")
        if anchor_only(_s):
            _imp = _implied_am(am)
            return (float(model_prob) - _imp) * 100.0 if _imp is not None else (
                raw_edge if raw_edge is not None else -999.0)
        if _s in ("wnba", "tennis"):
            _cp = cal_prob(_s, bet_type, model_prob, odds, pick_text, raw_edge)
            _imp = _implied_am(am)
            if _cp is None or _imp is None:
                return raw_edge if raw_edge is not None else -999.0
            return (float(_cp) - _imp) * 100.0
        return calibrated_edge(str(bet_type or ""), float(model_prob), am,
                               sport=sport, pick_text=pick_text)
    except Exception:
        return raw_edge if raw_edge is not None else -999.0


def cal_prob(sport: str, bet_type, model_prob, odds,
             pick_text: str | None = None, raw_edge: float | None = None) -> float:
    """Calibrated probability for display. Falls back to the raw prob."""
    try:
        if model_prob is None:
            return model_prob
        _s = str(sport or "")
        if anchor_only(_s):
            return float(model_prob)
        if _s == "wnba":
            from engine.final_calibration import apply as _fc
            return float(_fc(_s, float(model_prob)))
        if _s == "tennis":
            from engine.tennis_platt_calibration import tennis_calibrate as _tc
            return float(_tc(str(bet_type or ""), float(model_prob)))
        return calibrate(str(bet_type or ""), float(model_prob), sport=sport,
                         edge=raw_edge, odds=_to_american(odds), pick_text=pick_text)
    except Exception:
        return model_prob


def extra_validated(settled_n: int | None, days_tracked: int | None) -> bool:
    """True once an extra market has earned headline eligibility."""
    return (settled_n or 0) >= EXTRA_MIN_SETTLED and (days_tracked or 0) >= EXTRA_MIN_DAYS


# +EV floor for headlining. Decision (Austin 2026-10-12): when the calibrated
# edge says there is no +EV play, headline NOTHING rather than a thin/negative
# lean -- CLV-first, don't headline what you'd be -EV placing. Tunable.
MIN_HEADLINE_CAL_EDGE = 0.0


def headline_eligible(cal_edge_val, is_extra: bool = False,
                      settled_n: int | None = None,
                      days_tracked: int | None = None) -> bool:
    """Whether a candidate may HEADLINE a POTD / the cross-sport hero.

    - Must clear the +EV floor (calibrated edge >= MIN_HEADLINE_CAL_EDGE).
    - An 'extra' market (period / quarter / derivative / F5) must ALSO have
      earned a track record (extra_validated). Core full-game markets are the
      trusted default and skip the track-record bar.
    """
    if cal_edge_val is None or cal_edge_val < MIN_HEADLINE_CAL_EDGE:
        return False
    if is_extra and not extra_validated(settled_n, days_tracked):
        return False
    return True
