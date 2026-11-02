"""NBA adapter for the unified grading core (audit T2 Stage A).

Full-game (ML / SPREAD+ALT / TOTAL+ALT) and Q1 derivative markets
(Q1_ML / Q1_SPREAD / Q1_TOTAL / Q1 Team Total) map to the core families.
Alias-based ABSTAIN team resolution replaces the bespoke assume-away spread
fallbacks. Q1 ties push the moneyline (handled by the core).

Not yet: Q1 Total O/E (odd/even is not a line family).
"""
from __future__ import annotations

import re

from . import core
from .mlb_adapter import _side_of_team


def _result_from(hs, as_, final=True) -> core.GameResult | None:
    if hs is None or as_ is None:
        return None
    hs, as_ = float(hs), float(as_)
    winner = "home" if hs > as_ else ("away" if as_ > hs else None)
    return core.GameResult(home_score=hs, away_score=as_, winner=winner,
                           final=final, complete=True)


def _spread(pk):
    parts = pk.split()
    try:
        return (" ".join(parts[:-1]) or parts[0]), float(parts[-1])
    except (ValueError, IndexError):
        return None, None


def _total(pk):
    m = re.match(r"^(over|under)\s+([\d.]+)$", pk, re.I)
    return (m.group(1).lower(), float(m.group(2))) if m else (None, None)


def grade_pick(pick, game, home_team=None, away_team=None) -> str | None:
    bt = (pick.get("bet_type") or "")
    pk = (pick.get("pick") or "").strip()
    # NBA labels append market tags to the pick text ("TOR ML", "Under 55.5
    # Q1", "HOU Q1 ML") — strip trailing Q1/ML tags so the side/line parse
    # sees only the bet.
    while True:
        stripped = re.sub(r"\s*\b(Q1|ML|1H|2H|H1|H2)\b\s*$", "", pk, flags=re.I).strip()
        if stripped == pk:
            break
        pk = stripped
    h, a = game.get("home_abbr", ""), game.get("away_abbr", "")
    if not game.get("is_completed", True):
        return None

    full = _result_from(game.get("home_score"), game.get("away_score"))
    q1 = _result_from(game.get("home_q1"), game.get("away_q1"))

    # ── full game ──
    if bt == "ML":
        return core.grade(core.Family.ML, full, side=_side_of_team(pk, h, a, home_team, away_team)) if full else None
    if bt in ("SPREAD", "ALT SPREAD"):
        team, line = _spread(pk)
        if line is None or full is None:
            return None
        return core.grade(core.Family.SPREAD, full, side=_side_of_team(team, h, a, home_team, away_team), line=line)
    if bt in ("TOTAL", "ALT TOTAL"):
        d, line = _total(pk)
        return core.grade(core.Family.TOTAL, full, direction=d, line=line) if (d and full) else None

    # ── Q1 derivatives ──
    if bt == "Q1_ML":
        return core.grade(core.Family.ML, q1, side=_side_of_team(pk, h, a, home_team, away_team)) if q1 else None
    if bt == "Q1_SPREAD":
        team, line = _spread(pk)
        if line is None or q1 is None:
            return None
        return core.grade(core.Family.SPREAD, q1, side=_side_of_team(team, h, a, home_team, away_team), line=line)
    if bt == "Q1_TOTAL":
        d, line = _total(pk)
        return core.grade(core.Family.TOTAL, q1, direction=d, line=line) if (d and q1) else None
    if bt == "Q1 Team Total":
        m = re.match(r"^(.*?)\s+(?:total\s+)?(over|under)\s+([\d.]+)$", pk, re.I)
        if not m or q1 is None:
            return None
        side = _side_of_team(m.group(1), h, a, home_team, away_team)
        if side is None:
            return None
        return core.grade(core.Family.TEAM_TOTAL, q1, direction=m.group(2).lower(),
                          line=float(m.group(3)), team_total=q1.side_score(side))

    # ── halves (2026-11-01): 1H = Q1+Q2; 2H = final - 1H (includes OT, same as the picks_unified
    # adapter). Graded at full time (is_completed gate above). ──
    if bt[:3] in ("H1_", "H2_"):
        hh, ah = game.get("home_h1"), game.get("away_h1")
        if hh is None or ah is None:
            return None
        if bt.startswith("H2_"):
            if game.get("home_score") is None or game.get("away_score") is None:
                return None
            hh, ah = game["home_score"] - hh, game["away_score"] - ah
        half = _result_from(hh, ah)
        kind = bt[3:]
        if kind == "ML":
            return core.grade(core.Family.ML, half, side=_side_of_team(pk, h, a, home_team, away_team))
        if kind == "SPREAD":
            team, line = _spread(pk)
            if line is None:
                return None
            return core.grade(core.Family.SPREAD, half, side=_side_of_team(team, h, a, home_team, away_team), line=line)
        if kind == "TOTAL":
            d, line = _total(pk)
            return core.grade(core.Family.TOTAL, half, direction=d, line=line) if d else None
        return None

    return None  # Q1 Total O/E: odd/even, not a line family
