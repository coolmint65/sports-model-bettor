"""NHL adapter for the unified grading core (audit T2 Stage A).

Maps NHL game markets to the core families. Same alias-based ABSTAIN team
resolution as the MLB adapter, replacing NHL's identical bespoke assume-away
fallbacks (ML bare `==`, PL `else as_ - hs`, Team Total).

Covered: ML, O/U (+ALT), PL (+ALT), Team Total, Period Total.
"""
from __future__ import annotations

import re

from . import core
from .mlb_adapter import _side_of_team  # shared alias-based ABSTAIN resolver


def build_result(game) -> core.GameResult | None:
    hs, as_ = game.get("home_score"), game.get("away_score")
    if hs is None or as_ is None:
        return None
    winner = "home" if hs > as_ else ("away" if as_ > hs else None)
    return core.GameResult(home=game.get("home_abbr", ""), away=game.get("away_abbr", ""),
                           home_score=float(hs), away_score=float(as_), winner=winner,
                           final=bool(game.get("is_completed")), complete=True)


def grade_pick(pick, game, home_team=None, away_team=None) -> str | None:
    bt = (pick.get("bet_type") or "")
    # Live shadow rows (engine.sports.nhl.live_model, 2026-10-28) are "LIVE P<n> ML / PL / O/U": the
    # period is only the evidence cell -- they settle on the final score like their pre-game twins.
    bt = re.sub(r"^LIVE P\d+\s+", "", bt)
    pk = (pick.get("pick") or "").strip()
    h, a = game.get("home_abbr", ""), game.get("away_abbr", "")
    gr = build_result(game)
    if gr is None:
        return None

    if bt == "ML":
        return core.grade(core.Family.ML, gr,
                          side=_side_of_team(pk, h, a, home_team, away_team))

    if bt in ("O/U", "ALT O/U"):
        m = re.match(r"^(over|under)\s+([\d.]+)$", pk, re.I)
        if not m:
            return None
        return core.grade(core.Family.TOTAL, gr,
                          direction=m.group(1).lower(), line=float(m.group(2)))

    if bt in ("PL", "ALT PL"):
        parts = pk.split()
        try:
            line = float(parts[-1])
            team = " ".join(parts[:-1]) or parts[0]
        except (ValueError, IndexError):
            return None
        return core.grade(core.Family.SPREAD, gr,
                          side=_side_of_team(team, h, a, home_team, away_team), line=line)

    if bt == "Team Total":
        m = re.match(r"^(.*?)\s+(over|under)\s+([\d.]+)$", pk, re.I)
        if not m:
            return None
        side = _side_of_team(m.group(1), h, a, home_team, away_team)
        if side is None:
            return None
        return core.grade(core.Family.TEAM_TOTAL, gr, direction=m.group(2).lower(),
                          line=float(m.group(3)), team_total=gr.side_score(side))

    if bt == "Period Total":
        hp = game.get("home_periods") or []
        ap = game.get("away_periods") or []
        m = re.match(r"^P(\d+)\s+(over|under)\s+([\d.]+)$", pk, re.I)
        if not m:
            return None
        n = int(m.group(1))
        if n < 1 or n > game.get("periods_locked", 0) or n > min(len(hp), len(ap)):
            return None  # period not locked -> abstain
        pr = core.GameResult(home_score=float(hp[n - 1]), away_score=float(ap[n - 1]))
        return core.grade(core.Family.TOTAL, pr,
                          direction=m.group(2).lower(), line=float(m.group(3)))

    return None  # Overtime / BTS / Total O/E: next increment
