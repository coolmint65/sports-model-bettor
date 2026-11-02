"""Tennis adapter for the unified grading core (audit T2 Stage A).

Maps tennis bet_types to (Family, side, line) and builds the canonical view
from a match result, so the family W/L/P math lives once in grading.core.
Parity-tested against the (fixed) tennis_tracker._resolve_pick — see
scripts/grading_parity.py.
"""
from __future__ import annotations

import re

from . import core
from ..tennis_tracker import _parse_score


def _last(name: str) -> str:
    n = (name or "").strip().lower()
    n = re.sub(r"\s+[a-z]\.?\s*$", "", n)
    toks = n.split()
    return toks[-1] if toks else n


def _side_of(name: str, winner_name: str, loser_name: str) -> str | None:
    """'home' (winner) / 'away' (loser) / None. Last-name-exact first, then a
    substring that matches EXACTLY one side — same discipline as the fixed
    _picked_is_winner, so a loser can't substring-match the winner."""
    t = (name or "").strip().lower()
    wn, ln = (winner_name or "").lower(), (loser_name or "").lower()
    lp, wl, ll = _last(t), _last(wn), _last(ln)
    if lp and wl and ll and wl != ll:
        if lp == wl:
            return "home"
        if lp == ll:
            return "away"
    in_w = bool(t) and (t in wn or wn.startswith(t) or wn.endswith(t))
    in_l = bool(t) and (t in ln or ln.startswith(t) or ln.endswith(t))
    if in_w and not in_l:
        return "home"
    if in_l and not in_w:
        return "away"
    return None


def _canonical(match: dict, best_of) -> dict | None:
    parsed = _parse_score(match.get("score") or "")
    if parsed is None:
        return None
    # Normalize so p1_* is the winner (higher set count), matching _resolve_pick.
    if parsed["p2_sets"] > parsed["p1_sets"]:
        parsed = {"p1_sets": parsed["p2_sets"], "p2_sets": parsed["p1_sets"],
                  "p1_games": parsed["p2_games"], "p2_games": parsed["p1_games"]}
    sets_to_win = int(best_of or 3) // 2 + 1
    return {
        "win_name": match.get("winner_name") or "",
        "los_name": match.get("loser_name") or "",
        "win_sets": parsed["p1_sets"], "los_sets": parsed["p2_sets"],
        "win_games": parsed["p1_games"], "los_games": parsed["p2_games"],
        "complete": parsed["p1_sets"] >= sets_to_win,
    }


def grade_pick(row: dict, match: dict) -> str | None:
    """W/L/P or None (abstain / not-yet-covered market)."""
    bt = (row.get("bet_type") or "").upper()
    pick = (row.get("pick") or "").strip()
    c = _canonical(match, row.get("best_of"))
    if c is None:
        return None

    if bt == "WIN_AT_LEAST_ONE_SET":
        m = re.match(r"^(.*?)\s+(yes|no)\s*$", pick, re.I)
        if not m:
            return None
        side = _side_of(m.group(1).strip(), c["win_name"], c["los_name"])
        if side is None:
            return None
        picked_sets = c["win_sets"] if side == "home" else c["los_sets"]
        return core.grade(core.Family.YESNO, core.GameResult(),
                          yes=(m.group(2).lower() == "yes"),
                          actual=(picked_sets >= 1))

    if bt == "ML":
        side = _side_of(pick, c["win_name"], c["los_name"])
        return core.grade(core.Family.ML, core.GameResult(winner="home"), side=side)

    if bt == "SET_SPREAD":
        m = re.match(r"^(.*?)([+-]\d+\.\d+)\s*$", pick)
        if not m:
            return None
        side = _side_of(m.group(1).strip(), c["win_name"], c["los_name"])
        gr = core.GameResult(home_score=c["win_sets"], away_score=c["los_sets"])
        return core.grade(core.Family.SPREAD, gr, side=side, line=float(m.group(2)))

    if bt in ("TOTAL_SETS", "TOTAL_GAMES"):
        m = re.match(r"^(over|under)\s+(\d+\.?\d*)$", pick, re.I)
        if not m:
            return None
        if bt == "TOTAL_SETS":
            hs, as_ = c["win_sets"], c["los_sets"]
        else:
            hs, as_ = c["win_games"], c["los_games"]
        gr = core.GameResult(home_score=hs, away_score=as_, complete=c["complete"])
        return core.grade(core.Family.TOTAL, gr, direction=m.group(1).lower(),
                          line=float(m.group(2)), require_complete=True)

    return None  # SET_BETTING / SET_WINNER not yet in the core
