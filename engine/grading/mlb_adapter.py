"""MLB adapter for the unified grading core (audit T2 Stage A).

Maps MLB game markets to the core families. Team resolution goes through the
alias table and ABSTAINS when a pick's team matches neither side — replacing
the two bespoke "assume away" fallbacks (ML bare `==`, RL `else as_ - hs`)
that silently sign-flipped an unresolved pick.

Covered here: ML, O/U (+ALT), run/puck-line (+ALT), NRFI/1st-inning.
Not yet: F5 innings markets, team totals, player props — next increment.
"""
from __future__ import annotations

import json
import re

from . import core


def _side_of_team(pick_team, h, a, home_team=None, away_team=None) -> str | None:
    """Resolve a pick's team token to 'home'/'away'/None via abbr + aliases.
    Abstains (None) on an unresolved or ambiguous token — never guesses."""
    from ..abbr import aliases_for
    pt = (pick_team or "").strip()
    if not pt:
        return None
    aliases = {pt.upper()}
    for tok in (pt, pt.split()[-1] if pt.split() else ""):
        if not tok:
            continue
        try:
            aliases.update(x.upper() for x in aliases_for(tok))
        except Exception:
            pass

    def _matches(team, abbr) -> bool:
        if abbr and abbr.upper() in aliases:
            return True
        if team and (team.get("name", "") or "").upper() in aliases:
            return True
        return False

    hm, am = _matches(home_team, h), _matches(away_team, a)
    if hm and not am:
        return "home"
    if am and not hm:
        return "away"
    # Exact-abbr fallback only when unambiguous.
    if pt == h and pt != a:
        return "home"
    if pt == a and pt != h:
        return "away"
    return None


def _linescore(game, key):
    try:
        return json.loads(game.get(key) or "[]")
    except Exception:
        return []


def build_result(game, h, a) -> core.GameResult | None:
    hs, as_ = game.get("home_score"), game.get("away_score")
    if hs is None or as_ is None:
        return None
    winner = "home" if hs > as_ else ("away" if as_ > hs else None)
    return core.GameResult(home=h, away=a, home_score=float(hs), away_score=float(as_),
                           winner=winner, final=(game.get("status") == "final"),
                           complete=True)


def grade_pick(pick, game, h, a, home_team=None, away_team=None) -> str | None:
    bt = (pick.get("bet_type") or "").upper()
    pk = (pick.get("pick") or "").strip()
    # Postponed/cancelled/suspended -> no action / refund (push), matching
    # sportsbook behaviour, before any scoring math.
    if (game.get("status") or "").lower() in ("postponed", "cancelled", "canceled", "suspended"):
        return "P"
    gr = build_result(game, h, a)
    if gr is None:
        return None

    if bt in ("ML",):
        return core.grade(core.Family.ML, gr,
                          side=_side_of_team(pk, h, a, home_team, away_team))

    if bt in ("O/U", "OU", "ALT O/U", "TOTAL", "ALT TOTAL"):
        m = re.match(r"^(over|under)\s+([\d.]+)$", pk, re.I)
        if not m:
            return None
        return core.grade(core.Family.TOTAL, gr,
                          direction=m.group(1).lower(), line=float(m.group(2)))

    if bt in ("RL", "PL", "ALT RL", "ALT PL", "SPREAD", "ALT SPREAD"):
        parts = pk.split()
        try:
            line = float(parts[-1])
            team = " ".join(parts[:-1]) or parts[0]
        except (ValueError, IndexError):
            return None
        return core.grade(core.Family.SPREAD, gr,
                          side=_side_of_team(team, h, a, home_team, away_team),
                          line=line)

    if bt.startswith("F5"):
        h_inn, a_inn = _linescore(game, "home_linescore"), _linescore(game, "away_linescore")
        status = (game.get("status") or "").lower()
        full = min(len(h_inn), len(a_inn))
        if (status == "final" and full < 5) or (status != "final" and full < 6):
            return None  # first 5 innings not complete -> abstain
        try:
            f5h = sum(int(x) for x in h_inn[:5])
            f5a = sum(int(x) for x in a_inn[:5])
        except (ValueError, TypeError):
            return None
        f5 = core.GameResult(home=h, away=a, home_score=float(f5h), away_score=float(f5a),
                             winner=("home" if f5h > f5a else "away" if f5a > f5h else None),
                             complete=True)
        sub = bt[2:].strip().upper()
        if sub == "ML":
            return core.grade(core.Family.ML, f5, side=_side_of_team(pk, h, a, home_team, away_team))
        if sub in ("O/U", "OU"):
            parts = pk.split()
            try:
                line = float(parts[-1])
            except (ValueError, IndexError):
                return None
            is_over = any(p.lower() == "over" for p in parts)
            return core.grade(core.Family.TOTAL, f5,
                              direction=("over" if is_over else "under"), line=line)
        if sub == "RL":
            parts = pk.split()
            team = parts[0] if parts else ""
            try:
                line = float(parts[1]) if len(parts) > 1 else 0.5
            except ValueError:
                line = 0.5
            return core.grade(core.Family.SPREAD, f5,
                              side=_side_of_team(team, h, a, home_team, away_team), line=line)
        return None

    if bt in ("NRFI", "1ST INN"):
        h_inn, a_inn = _linescore(game, "home_linescore"), _linescore(game, "away_linescore")
        status = (game.get("status") or "").lower()
        locked = status == "final" or (len(h_inn) >= 2 and len(a_inn) >= 2)
        if not (locked and h_inn and a_inn):
            return None
        try:
            scoreless = (int(h_inn[0]) == 0 and int(a_inn[0]) == 0)
        except (ValueError, TypeError, IndexError):
            return None
        return core.grade(core.Family.YESNO, gr,
                          yes=("nrfi" in pk.lower()), actual=scoreless)

    return None  # F5 / team-total / props: next increment
