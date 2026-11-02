"""Shared PAPER-pick engine (Austin 2026-09-28, unification Phase 2).

One implementation of "surface the model's raw directional lean as a paper
(shadow) pick" so a newly-wired sport records the SAME shape into its own picks
table as is_shadow=1 -- graded + CLV-tracked by that sport's existing settle/
summary machinery, but NEVER staked and fully isolated from the real
(is_shadow=0) record.

This does NOT replace the working per-sport paper implementations
(WNBA/basketball full+Q1, NHL, MLB, football anytime, props). It is the shared
core the newly-wired sports call so we stop copy-pasting the ncdf/implied math
and the best-per-family selection.

Entry points:
  * select_paper_leans(picks, ...) -> the model's best lean per market family
    from a sport's ALREADY-COMPUTED picks list (bypasses the real-bet edge/skip
    gate by reading the ungated list). Sports that already emit a picks list
    (NBA, soccer, ...) use this -- no fragile re-derivation of prediction
    internals; records exactly what the model actually picked.
  * game_paper_picks(pred, odds, home_abbr, away_abbr, ...) -> re-derive
    ML/TOTAL/SPREAD leans from a prediction's margin/total + sigmas (the
    basketball/WNBA style) for sports that expose those but gate picks to
    nothing.

Both return normalized paper dicts:
  {type, market, pick, line, prob, odds, edge, paper=True}
capped to an edge ceiling (kills blowout longshots the way NHL's paper layer
does) so the shadow record stays a fair forward test.
"""
from __future__ import annotations
import math

# Full-game primary families surfaced as paper leans. Q1_*, ALT *, and
# derivative types are intentionally excluded -- the paper layer mirrors the
# primary-market lean, not every alt line.
PRIMARY_FAMILIES = ("ML", "SPREAD", "TOTAL")


def implied(american):
    """American odds -> implied win prob (vig included). None on garbage."""
    try:
        o = float(american)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    return (-o) / (-o + 100) if o < 0 else 100 / (o + 100)


def american_clv(entry_odds, closing_odds):
    """CLV in pp = implied(closing) - implied(entry). Positive = beat close."""
    e, c = implied(entry_odds), implied(closing_odds)
    if e is None or c is None:
        return None
    return round((c - e) * 100, 2)


def _ncdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def select_paper_leans(picks, *, families=PRIMARY_FAMILIES,
                       rank="edge", edge_ceiling=20.0, edge_floor=0.0):
    """Best model lean per market family from a sport's OWN picks list.

    ``picks`` items are the sport's native pick dicts (type/pick/prob/edge/odds
    -- the exact shape each tracker's record_picks already persists). The
    confidence/skip tag is ignored on purpose: the point of the paper layer is
    to show a directional lean the real-bet gate suppressed. Returns at most one
    pick per family, tagged paper=True, edge in (floor, ceiling].
    """
    if not picks:
        return []
    best = {}
    for p in picks:
        t = p.get("type")
        if t not in families:
            continue
        e = p.get("edge")
        if e is None:
            continue
        try:
            e = float(e)
        except (TypeError, ValueError):
            continue
        if e <= edge_floor or e > edge_ceiling:
            continue
        if p.get("pick") is None:
            continue
        score = e if rank == "edge" else (p.get("prob") or 0)
        cur = best.get(t)
        if cur is None or score > cur[0]:
            best[t] = (score, p)
    out = []
    for t in families:
        if t in best:
            p = best[t][1]
            out.append({
                "type": t, "market": t, "pick": p.get("pick"),
                "line": p.get("line"), "prob": p.get("prob"),
                "odds": p.get("odds"), "edge": round(float(p.get("edge")), 1),
                "paper": True,
            })
    return out


def game_paper_picks(pred, odds, home_abbr, away_abbr, *,
                     margin_std=16.0, total_std=21.0, edge_ceiling=20.0):
    """Re-derive ML/TOTAL/SPREAD paper leans from a prediction's margin/total +
    sigmas + a book's odds (the basketball/WNBA style)."""
    if not pred or not odds:
        return []
    ens = ((pred.get("ensemble") or {}).get("ensemble") or {})
    pmargin = ens.get("margin") if ens.get("margin") is not None else pred.get("predicted_margin")
    ptotal = ens.get("total") if ens.get("total") is not None else pred.get("predicted_total")
    consts = (pred.get("factors") or {}).get("constants") or {}
    sm = consts.get("margin_std") or margin_std
    st = consts.get("total_std") or total_std
    out = []
    ml = odds.get("ml") or {}
    p_home_ml = ens.get("ml_home")
    if p_home_ml is not None and (ml.get("home") is not None or ml.get("away") is not None):
        if p_home_ml >= 0.5:
            side, prob, o = (home_abbr or "HOME"), p_home_ml, ml.get("home")
        else:
            side, prob, o = (away_abbr or "AWAY"), 1 - p_home_ml, ml.get("away")
        imp = implied(o)
        if o is not None and imp is not None:
            e = round((prob - imp) * 100, 1)
            if 0 < e <= edge_ceiling:
                out.append(dict(type="ML", market="ML", pick=side, line=None,
                                prob=round(prob, 4), odds=o, edge=e, paper=True))
    tot = odds.get("total") or {}
    line = tot.get("line")
    if ptotal is not None and line is not None and st > 0:
        p_over = _ncdf((ptotal - line) / st)
        side, prob, o = ("Over", p_over, tot.get("over_odds")) if p_over >= 0.5 else ("Under", 1 - p_over, tot.get("under_odds"))
        imp = implied(o)
        if o is not None and imp is not None:
            e = round((prob - imp) * 100, 1)
            if 0 < e <= edge_ceiling:
                out.append(dict(type="TOTAL", market="TOTAL", pick=side, line=line,
                                prob=round(prob, 4), odds=o, edge=e, paper=True))
    sp = odds.get("spread") or {}
    hpt = sp.get("home_pt")
    if pmargin is not None and hpt is not None and sm > 0:
        p_home = _ncdf((pmargin - (-hpt)) / sm)
        if p_home >= 0.5:
            side, prob, ln, o = (home_abbr or "HOME"), p_home, hpt, sp.get("home_odds")
        else:
            side, prob, ln, o = (away_abbr or "AWAY"), 1 - p_home, sp.get("away_pt"), sp.get("away_odds")
        imp = implied(o)
        if o is not None and imp is not None and ln is not None:
            e = round((prob - imp) * 100, 1)
            if 0 < e <= edge_ceiling:
                out.append(dict(type="SPREAD", market="SPREAD", pick=side, line=ln,
                                prob=round(prob, 4), odds=o, edge=e, paper=True))
    return out
