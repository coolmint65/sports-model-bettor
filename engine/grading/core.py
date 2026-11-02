"""Unified, sport-agnostic bet grader — audit T2 Stage A.

ONE validated "did this bet win?" function, fed by a canonical GameResult
built from box-score / scoreboard truth. Replaces the N parallel hand-rolled
per-sport settlers whose independent bugs (assume-away sign flips, tiebreak
misparse, ET-inclusive scores, partial-set counting) each silently authorized
real money.

Three disciplines are baked in so a sport adapter cannot reintroduce them:
  * ABSTAIN, never assume — an unresolved side / line / score returns None
    (leave the pick pending), never a guessed W/L.
  * COMPLETED units only — a retired/incomplete match voids unit-total markets.
  * EXPLICIT push handling on every line market.

A sport "adapter" does two cheap things: (1) build a GameResult from that
sport's result row, (2) map a bet_type+pick to a (Family, side, line) tuple.
All the actual W/L/P math lives here, once.
"""
from __future__ import annotations

from dataclasses import dataclass

W, L, P = "W", "L", "P"


class Family:
    ML = "ML"                 # pick a side to win outright (tie -> push)
    ML3WAY = "ML3WAY"         # 3-way: pick home/away/draw (soccer)
    DC = "DC"                 # double chance: pick two of {home,draw,away}
    SPREAD = "SPREAD"         # pick a side, cover a signed line (margin + line)
    TOTAL = "TOTAL"           # over/under a combined-total line
    TEAM_TOTAL = "TEAM_TOTAL"  # over/under one side's total
    YESNO = "YESNO"           # boolean prop (did X happen)


@dataclass
class GameResult:
    """Canonical result. Scores are the GRADED (regulation for team sports,
    completed-set for tennis) values. Anything the adapter can't fill stays
    None so the grader abstains rather than guesses."""
    home: str = ""            # canonical home side id/name (lowercased by adapter)
    away: str = ""
    home_score: float | None = None
    away_score: float | None = None
    winner: str | None = None      # "home" | "away" | None (tie/undecided)
    final: bool = False            # reached a graded conclusion (not mid-game)
    complete: bool = True          # False for retirement/abandonment -> void unit totals

    def side_score(self, side: str) -> float | None:
        if side == "home":
            return self.home_score
        if side == "away":
            return self.away_score
        return None

    def other_side(self, side: str) -> str | None:
        return {"home": "away", "away": "home"}.get(side)


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def grade(family: str, result: GameResult, *,
          side: str | None = None,
          line: float | None = None,
          direction: str | None = None,
          team_total: float | None = None,
          yes: bool | None = None,
          actual: bool | None = None,
          dc_outcomes=None,
          require_complete: bool = False) -> str | None:
    """Return "W"/"L"/"P" or None (abstain). See Family for the market families.

    - ML:         side in {home, away}. Graded on result.winner.
    - SPREAD:     side in {home, away}, signed `line`. margin = side - other.
    - TOTAL:      `direction` in {over, under}, `line`. Uses home+away score.
    - TEAM_TOTAL: `direction`, `line`, `team_total` (that side's score).
    - YESNO:      `yes` (what the ticket backs) vs `actual` (what happened).
    require_complete voids (None) unit-total markets on a retired/incomplete game.
    """
    if require_complete and not result.complete:
        return None  # retirement/abandonment -> unit totals undefined (void)

    if family == Family.ML:
        if side not in ("home", "away"):
            return None
        if result.winner in ("home", "away"):
            return W if side == result.winner else L
        # No declared winner: a COMPLETED game with equal scores is a push
        # (period/quarter ties push the moneyline); a genuinely unknown
        # result (missing scores) abstains rather than guessing.
        if (result.home_score is not None and result.away_score is not None
                and result.home_score == result.away_score):
            return P
        return None

    if family in (Family.ML3WAY, Family.DC):
        if result.home_score is None or result.away_score is None:
            return None
        outcome = ("home" if result.home_score > result.away_score
                   else "away" if result.away_score > result.home_score else "draw")
        if family == Family.ML3WAY:
            if side not in ("home", "away", "draw"):
                return None
            return W if side == outcome else L
        # DC: win if the actual outcome is one of the two backed outcomes.
        if not dc_outcomes:
            return None
        return W if outcome in dc_outcomes else L

    if family == Family.SPREAD:
        if side not in ("home", "away") or line is None:
            return None
        s, o = result.side_score(side), result.side_score(result.other_side(side))
        if s is None or o is None:
            return None
        cover = (s - o) + float(line)
        return W if cover > 0 else (P if cover == 0 else L)

    if family == Family.TOTAL:
        if direction not in ("over", "under") or line is None:
            return None
        if result.home_score is None or result.away_score is None:
            return None
        total = result.home_score + result.away_score
        if total == float(line):
            return P
        is_over = total > float(line)
        return W if (is_over) == (direction == "over") else L

    if family == Family.TEAM_TOTAL:
        tt = _num(team_total)
        if direction not in ("over", "under") or line is None or tt is None:
            return None
        if tt == float(line):
            return P
        is_over = tt > float(line)
        return W if (is_over) == (direction == "over") else L

    if family == Family.YESNO:
        if yes is None or actual is None:
            return None
        return W if bool(actual) == bool(yes) else L

    return None  # unknown family -> abstain (never guess)
