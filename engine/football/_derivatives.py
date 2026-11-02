"""Football derivative pricer (Austin 2026-10-13, market-expansion HIGH tier).

A derivative is just a count over the SAME Monte-Carlo (home,away) score samples
the ML/spread/total picks already use -- one sim run prices every line:
alt spread/total ladders, team totals, winning-margin buckets, go-to-OT. No new
model. Period lines (1H/1Q) scale the sim (see price_period below). Returns RAW
priced candidates; the caller (engine.football._picks) applies the existing
juice/floor/CEILING gates so the 20pp FBS-vs-FCS guard still protects these.
"""
import math

import numpy as np
from ..market_blend import (anchor_to_market as _anchor,
                             devig_multiway as _devig_multi,
                             anchor_prob as _anchor_p)
from ..selection.edge_proven import market_weight
from . import _periods


def _implied(odds):
    o = float(odds)
    return abs(o) / (abs(o) + 100.0) if o < 0 else 100.0 / (o + 100.0)


def price_samples(h, a, *, alt_spreads=None, alt_totals=None,
                  home_team_totals=None, away_team_totals=None,
                  home_abbr="HOME", away_abbr="AWAY", league=None,
                  anchor_w=None):
    """h, a: per-sim home/away score arrays. Line inputs are lists of tuples:
      alt_spreads:      (home_point, home_odds, away_odds)
      alt_totals:       (line, over_odds, under_odds)
      home/away_team_totals: (line, over_odds, under_odds)
    Returns list of {type, pick, prob, implied, edge(pp), odds, line}.

    ``anchor_w``: None = the per-bucket market_weight (headline pass); a float
    overrides it — 1.0 = the model's own prob, un-anchored (the Pick Authority
    SHADOW pass, 2026-10-25). Pricing is cheap; the sim behind ``h``/``a`` is the
    expensive part, so callers price the same samples twice."""
    h = np.asarray(h, dtype=float); a = np.asarray(a, dtype=float)
    margin = h - a; total = h + a
    out = []

    def add(t, pick, prob, odds, line, opp_odds=None):
        if odds is None:
            return
        w = anchor_w if anchor_w is not None else market_weight("football", league, t, pick, odds)  # per-bucket CLV weight (MintyBot 2026-10-22)
        prob = _anchor(float(prob), odds, opp_odds, w=w)
        imp = _implied(odds)
        out.append({"type": t, "pick": pick, "prob": round(float(prob), 4),
                    "implied": round(imp, 4), "edge": round((float(prob) - imp) * 100.0, 1),
                    "odds": int(odds), "line": line})

    for pt, ho, ao in (alt_spreads or []):
        p_home = float((margin + pt > 0).mean())
        push = float((margin + pt == 0).mean())
        add("ALT_SPREAD", "%s %+g" % (home_abbr, pt), p_home, ho, pt, ao)
        add("ALT_SPREAD", "%s %+g" % (away_abbr, -pt), 1.0 - p_home - push, ao, -pt, ho)

    for ln, oo, uo in (alt_totals or []):
        p_over = float((total > ln).mean())
        push = float((total == ln).mean())
        add("ALT_TOTAL", "Over %g" % ln, p_over, oo, ln, uo)
        add("ALT_TOTAL", "Under %g" % ln, 1.0 - p_over - push, uo, ln, oo)

    for arr, abbr, pts in ((home_team_totals, home_abbr, h), (away_team_totals, away_abbr, a)):
        for ln, oo, uo in (arr or []):
            p_over = float((pts > ln).mean())
            push = float((pts == ln).mean())
            add("TEAM_TOTAL", "%s Over %g" % (abbr, ln), p_over, oo, ln, uo)
            add("TEAM_TOTAL", "%s Under %g" % (abbr, ln), 1.0 - p_over - push, uo, ln, oo)

    return out


# ── Period (1H/1Q) pricing (MintyBot 2026-10-16) ─────────────────────────────
# Share of full-game points landing in the period. NFL/CFB score ~48% of points
# in the 1st half, ~21% in the 1st quarter. Variance scales ~linearly with
# game-time, so the per-team sigma scales by sqrt(fraction).
# All six periods (MintyBot 2026-10-25, Austin "I like 1 and 2").
# Since 2026-10-25 (Austin "A B and C") periods are priced off REAL quarter splits
# (engine.football._periods); these Normal fractions are only the fallback when a league has
# no box-score history. Measured shares (NFL 2026 / CFB 2026): Q1 .19/.22, Q2 .30/.30,
# Q3 .21/.22, Q4 .30/.27 -- the old 21/27/23/29 overpriced Q1/Q3 and underpriced Q2.
_PERIOD_PTS_FRAC = {"H1": 0.50, "H2": 0.50, "Q1": 0.20, "Q2": 0.30, "Q3": 0.21, "Q4": 0.29}


def _nearest(items, n, key):
    """Keep the ``n`` ladder lines nearest the ladder's median line (by ``key``)."""
    if not items or n is None or len(items) <= n:
        return items
    ks = sorted(key(i) for i in items); med = ks[len(ks) // 2]
    return sorted(items, key=lambda i: (abs(key(i) - med), key(i)))[:n]


def _two_way(out, t, pick_yes, pick_no, p_yes, yes_odds, no_odds, league=None, anchor_w=None):
    """A Yes/No (or A/B) 2-way candidate pair, each leg anchored to the de-vigged 2-way market."""
    if yes_odds is None or no_odds is None:
        return
    for pick, p, od, opp in ((pick_yes, p_yes, yes_odds, no_odds), (pick_no, 1.0 - p_yes, no_odds, yes_odds)):
        w = anchor_w if anchor_w is not None else market_weight("football", league, t, pick, od)
        pa = _anchor(float(p), od, opp, w=w)
        imp = _implied(od)
        out.append({"type": t, "pick": pick, "prob": round(float(pa), 4), "implied": round(imp, 4),
                    "edge": round((float(pa) - imp) * 100.0, 1), "odds": int(od), "line": None})


def _quarter_leads(hh, aa, seed=4321, league=None):
    """P(home leads after EVERY quarter), P(away ...): cumulative regulation quarters from the
    real quarter splits (engine.football._periods); Normal per-quarter draws only as fallback."""
    hh = np.asarray(hh, dtype=float); aa = np.asarray(aa, dtype=float); n = len(hh)
    if n == 0:
        return 0.0, 0.0
    qs = _periods.quarter_splits(hh, aa, league, seed=seed)
    if qs is not None:
        hc = np.cumsum(qs[0][:, :4], axis=1); ac = np.cumsum(qs[1][:, :4], axis=1)
        return float((hc > ac).all(axis=1).mean()), float((ac > hc).all(axis=1).mean())
    rng = np.random.default_rng(seed)
    hs = np.zeros(n); as_ = np.zeros(n); lead_h = np.ones(n, dtype=bool); lead_a = np.ones(n, dtype=bool)
    for q in ("Q1", "Q2", "Q3", "Q4"):
        f = _PERIOD_PTS_FRAC[q]; sf = math.sqrt(f)
        hs = hs + np.round(np.clip(rng.normal(hh.mean() * f, max(hh.std(), 1e-6) * sf, n), 0, 70))
        as_ = as_ + np.round(np.clip(rng.normal(aa.mean() * f, max(aa.std(), 1e-6) * sf, n), 0, 70))
        lead_h &= hs > as_; lead_a &= as_ > hs
    return float(lead_h.mean()), float(lead_a.mean())


def price_game_script(h, a, *, btts=None, btts_x=None, leq_home=None, leq_away=None,
                      home_abbr="HOME", away_abbr="AWAY", league=None, anchor_w=None):
    """Game-script markets priceable from the (continuous) score samples (2026-10-25,
    Austin "work on 3 to 5"): both-teams-to-score (+ N+ variants) and lead-after-every-
    quarter. Odd/even, exact points and overtime need DISCRETE scores -> #4's scoring layer.
    All 2-way (a one-sided market would bypass anchoring and could headline a raw lean)."""
    h = np.asarray(h, dtype=float); a = np.asarray(a, dtype=float)
    out = []
    if h.size == 0:
        return out
    if btts:
        p = float(((h >= 0.5) & (a >= 0.5)).mean())
        _two_way(out, "BTTS", "Yes", "No", p, btts[0], btts[1], league=league, anchor_w=anchor_w)
    for n_pts, yn in (btts_x or []):
        thr = float(n_pts) - 0.5
        p = float(((h >= thr) & (a >= thr)).mean())
        _two_way(out, "BTTS", f"Yes {n_pts}+", f"No {n_pts}+", p, yn[0], yn[1], league=league, anchor_w=anchor_w)
    if leq_home or leq_away:
        ph, pa = _quarter_leads(h, a, league=league)
        if leq_home:
            _two_way(out, "LEAD_EVERY_Q", f"{home_abbr} Yes", f"{home_abbr} No", ph, leq_home[0], leq_home[1], league=league, anchor_w=anchor_w)
        if leq_away:
            _two_way(out, "LEAD_EVERY_Q", f"{away_abbr} Yes", f"{away_abbr} No", pa, leq_away[0], leq_away[1], league=league, anchor_w=anchor_w)
    return out


def price_period(hh, aa, period, *, spreads=None, totals=None,
                 ml2=None, ml3=None, home_abbr="HOME", away_abbr="AWAY", seed=1337, league=None,
                 anchor_w=None, home_team_totals=None, away_team_totals=None, max_rungs=None, btts=None):
    """Price period markets off the full-game MC score samples. Each simulated team final
    borrows the quarter split of a REAL team-game that finished on (nearly) the same points
    (engine.football._periods, Austin 2026-10-25 "A B and C"), so period scores are lumpy
    football numbers (0/3/7/10/14) with the real tie / both-score mass -- the old Normal put
    NFL Q1 ties at 8% (actual 22%). Falls back to Normal scaling (mean x points-fraction,
    sigma x sqrt(fraction)) only when the league has no box-score history.
    Reuses price_samples for the spread/total ladders; adds the 2-way (DNB) and
    3-way (home/tie/away) winners. Candidate dicts match price_samples, with the
    type prefixed by the period (H1_SPREAD, H1_TOTAL, H1_ML, H1_ML3, Q1_...)."""
    frac = _PERIOD_PTS_FRAC.get(period)
    if frac is None:
        return []
    hh = np.asarray(hh, dtype=float); aa = np.asarray(aa, dtype=float)
    n = len(hh)
    if n == 0:
        return []
    real = _periods.period_scores(hh, aa, period, league, seed=seed)
    if real is not None:
        h, a = real
    else:
        rng = np.random.default_rng(seed)
        sf = math.sqrt(frac)
        h = np.round(np.clip(rng.normal(hh.mean() * frac, max(hh.std(), 1e-6) * sf, n), 0, 70))
        a = np.round(np.clip(rng.normal(aa.mean() * frac, max(aa.std(), 1e-6) * sf, n), 0, 70))
    # ``max_rungs`` (new periods H2/Q2-Q4): price only the lines nearest each ladder's
    # median. Alt rungs are correlated copies of one opinion -- the proven-edge dedupe keeps
    # one trial per game/market/side anyway -- and every rung costs two score_pick calls.
    # H1/Q1 keep their full ladders (unchanged behaviour); period TEAM totals are new on all.
    if max_rungs:
        spreads = _nearest(spreads, max_rungs, key=lambda x: float(x[0]))
        totals = _nearest(totals, max_rungs, key=lambda x: float(x[0]))
        home_team_totals = _nearest(home_team_totals, max_rungs, key=lambda x: float(x[0]))
        away_team_totals = _nearest(away_team_totals, max_rungs, key=lambda x: float(x[0]))
    out = price_samples(h, a, alt_spreads=spreads, alt_totals=totals,
                        home_team_totals=home_team_totals, away_team_totals=away_team_totals,
                        home_abbr=home_abbr, away_abbr=away_abbr, league=league,
                        anchor_w=anchor_w)
    for c in out:                                        # H1_SPREAD / H1_TOTAL / Q1_...
        c["type"] = period + "_" + c["type"].replace("ALT_", "")

    margin = h - a
    ph = float((margin > 0).mean()); pa = float((margin < 0).mean()); pt = float((margin == 0).mean())
    if btts:                                             # "1st Half Both Teams To Score" (rounded period points)
        _two_way(out, period + "_BTTS", "Yes", "No", float(((h > 0) & (a > 0)).mean()), btts[0], btts[1], league=league, anchor_w=anchor_w)
    # 2-way winner (DNB, ties void): P(team | decided), anchored to the 2-way line.
    if ml2 and ml2[0] is not None and ml2[1] is not None:
        ho, ao = ml2
        dec = ph + pa
        if dec > 0:
            _winner(out, period + "_ML", "%s ML" % home_abbr, ph / dec, ho, ao, league=league, anchor_w=anchor_w)
            _winner(out, period + "_ML", "%s ML" % away_abbr, pa / dec, ao, ho, league=league, anchor_w=anchor_w)
    # 3-way winner (home / tie / away): de-vig the three legs to sum 1, then anchor.
    if ml3 and all(x is not None for x in ml3):
        ho, do, ao = ml3
        dv = _devig_multi([ho, do, ao], 1.0)
        for lbl, p, od, i in ((home_abbr + " (3way)", ph, ho, 0),
                              ("Tie (3way)", pt, do, 1),
                              (away_abbr + " (3way)", pa, ao, 2)):
            _winner3(out, period + "_ML3", lbl, p, od, dv[i] if dv else None, league=league, anchor_w=anchor_w)
    return out


def _winner(out, t, pick, prob, odds, opp_odds, league=None, anchor_w=None):
    """2-way winner candidate, anchored to the de-vigged 2-way market
    (``anchor_w`` overrides the per-bucket weight; 1.0 = un-anchored shadow pass)."""
    if odds is None:
        return
    w = anchor_w if anchor_w is not None else market_weight("football", league, t, pick, odds)
    prob = _anchor(float(prob), odds, opp_odds, w=w)
    imp = _implied(odds)
    out.append({"type": t, "pick": pick, "prob": round(float(prob), 4),
                "implied": round(imp, 4), "edge": round((float(prob) - imp) * 100.0, 1),
                "odds": int(odds), "line": None})


def _winner3(out, t, pick, prob, odds, devig, league=None, anchor_w=None):
    """3-way winner candidate: anchor the model prob to the de-vigged leg, and
    measure edge vs that de-vig (not the raw implied, which carries the 3-way vig).
    ``anchor_w`` overrides the per-bucket weight (1.0 = un-anchored shadow pass)."""
    if odds is None:
        return
    w = anchor_w if anchor_w is not None else market_weight("football", league, t, pick, odds)
    prob = _anchor_p(float(prob), devig, w=w) if devig is not None else float(prob)
    ref = devig if devig is not None else _implied(odds)
    out.append({"type": t, "pick": pick, "prob": round(float(prob), 4),
                "implied": round(float(ref), 4), "edge": round((float(prob) - float(ref)) * 100.0, 1),
                "odds": int(odds), "line": None})
