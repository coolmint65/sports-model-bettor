"""Soccer adapter for the unified grading core (audit T2 Stage A).

Maps 3-way ML, OU/TOTAL, BTTS, DNB, DC, AH (+ H1 half-time variants) to the
core families. Uses the pick row's `side`/`line` columns (soccer stores the
parsed side rather than re-parsing text). Regulation scores are read from the
match; the ET-inclusive-score bug is fixed upstream in _espn_ingest.

Not yet: ADVANCE (needs pens/ET enrichment), GIBH (goal-in-both-halves),
Asian-handicap quarter-line half-win/half-loss (a fractional-result feature).
"""
from __future__ import annotations

from . import core


def _ml_side_from_text(row, match):
    pt = (row.get("pick") or "").strip()
    if pt and pt == match.get("home_abbr"):
        return "home"
    if pt and pt == match.get("away_abbr"):
        return "away"
    if pt.lower() == "draw":
        return "draw"
    return None


def grade_pick(row, match) -> str | None:
    btu = (row.get("bet_type") or "").strip().upper()
    side = (row.get("side") or "").lower()
    line = row.get("line")

    if btu.startswith("H1_") or btu.startswith("H1 "):
        h, a = match.get("home_score_ht"), match.get("away_score_ht")
        btu = btu[3:].strip()
    else:
        h, a = match.get("home_score"), match.get("away_score")
    if h is None or a is None:
        return None
    h, a = int(h), int(a)
    gr = core.GameResult(home_score=float(h), away_score=float(a),
                         winner="home" if h > a else ("away" if a > h else None))
    if btu == "TOTAL":
        btu = "OU"

    if btu == "ML":
        s = side if side in ("home", "away", "draw") else _ml_side_from_text(row, match)
        return core.grade(core.Family.ML3WAY, gr, side=s)
    if btu == "OU" and line is not None:
        if side not in ("over", "under"):
            return None
        return core.grade(core.Family.TOTAL, gr, direction=side, line=float(line))
    if btu == "BTTS":
        if side not in ("yes", "no"):
            return None
        return core.grade(core.Family.YESNO, gr, yes=(side == "yes"),
                          actual=(h > 0 and a > 0))
    if btu == "DNB":
        return core.grade(core.Family.ML, gr, side=side if side in ("home", "away") else None)
    if btu == "DC":
        pairs = {"home_draw": {"home", "draw"}, "draw_away": {"away", "draw"},
                 "home_away": {"home", "away"}}
        return core.grade(core.Family.DC, gr, dc_outcomes=pairs.get(side))
    if btu == "AH" and line is not None:
        return core.grade(core.Family.SPREAD, gr,
                          side=side if side in ("home", "away") else None, line=float(line))

    return None  # ADVANCE / GIBH / AH quarter-line: next increment
