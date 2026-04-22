"""
Conservatism ladder — raise WR% by swapping risky picks to safer
same-direction siblings that still clear our edge + juice guardrails.

Why this exists
---------------
The MC+GBM core is good at pricing spreads, but a high-edge spread pick
is still a sub-50% shot by construction (a -2.5 run line hits ~40% even
on a genuine 12% edge). During cold-streak variance this burns bankroll
before the long-run EV can assert itself. For each pick with primary
probability below `CONSERVATISM_ACTIVATE_UNDER_PROB`, this module walks
the ladder of safer lines in the same game / same direction (e.g.
-2.5 → -1.5 → ML for a favorite spread) and swaps to the highest-prob
option that still clears a reduced edge floor and the juice wall.

Selection rules
---------------
1. Activate only when primary pick prob < CONSERVATISM_ACTIVATE_UNDER_PROB.
2. Candidate must improve probability by at least
   CONSERVATISM_MIN_PROB_IMPROVEMENT over the primary.
3. Candidate's post-swap edge must stay at or above
   CONSERVATISM_MIN_EDGE_AFTER_SWAP.
4. Candidate's price must still clear the sport's juice wall.
5. Among qualifying candidates, pick the one with the highest prob.

Swapped picks carry `safened=True` and a `safened_from` snapshot of
the original line so the tracker / analytics can later measure whether
the trade was worth it in realized WR/ROI.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from . import config

logger = logging.getLogger(__name__)


# ── Constants ─────────────────────────────────────────────────

# Same pick regex used for both full-game and Q1 lines; the trailing
# " Q1" (or " Q1 ML") is handled by the caller.
_SPREAD_PICK_RE = re.compile(r"^(?P<abbr>\S+)\s+(?P<point>[+-]?\d+(?:\.\d+)?)")
_TOTAL_PICK_RE = re.compile(r"^(?P<dir>Over|Under)\s+(?P<line>\d+(?:\.\d+)?)", re.IGNORECASE)

_SPREAD_TYPES_MLB = {"RL", "ALT RL", "F5 RL"}
_SPREAD_TYPES_NHL = {"PL", "ALT PL"}
_SPREAD_TYPES_NBA = {"Q1_SPREAD"}
_ALL_SPREAD_TYPES = _SPREAD_TYPES_MLB | _SPREAD_TYPES_NHL | _SPREAD_TYPES_NBA

_TOTAL_TYPES_MLB = {"O/U", "ALT O/U", "F5 O/U"}
_TOTAL_TYPES_NHL = {"O/U", "ALT O/U"}
_TOTAL_TYPES_NBA = {"Q1_TOTAL"}
_ALL_TOTAL_TYPES = _TOTAL_TYPES_MLB | _TOTAL_TYPES_NHL | _TOTAL_TYPES_NBA


def _juice_wall(sport: str) -> int:
    return {
        "mlb": config.MLB_JUICE_WALL,
        "nhl": config.NHL_JUICE_WALL,
        "nba": config.NBA_JUICE_WALL,
    }.get(sport, -200)


def _implied(american: Any) -> float:
    try:
        a = int(american)
    except (TypeError, ValueError):
        return 0.0
    if a == 0:
        return 0.0
    if a < 0:
        return abs(a) / (abs(a) + 100)
    return 100 / (a + 100)


def _valid_odds(o: Any) -> bool:
    if o is None:
        return False
    try:
        return abs(int(o)) >= 100
    except (TypeError, ValueError):
        return False


# ── Entry point ──────────────────────────────────────────────

def apply_ladder(picks: list[dict], pred: dict, odds: dict,
                 sport: str, home_abbr: str, away_abbr: str) -> list[dict]:
    """Transform each pick through the ladder. Failures on a single
    pick leave that pick untouched rather than crashing pick generation."""
    if not getattr(config, "CONSERVATISM_ENABLED", True):
        return picks
    if not picks:
        return picks

    out = []
    for pick in picks:
        try:
            out.append(_apply_one(pick, pred, odds, sport, home_abbr, away_abbr))
        except Exception as e:
            logger.warning("conservatism ladder error on %s/%s: %s",
                           pick.get("type"), pick.get("pick"), e)
            out.append(pick)
    return out


def _apply_one(pick: dict, pred: dict, odds: dict,
               sport: str, h_abbr: str, a_abbr: str) -> dict:
    prob = float(pick.get("prob") or 0)
    threshold = getattr(config, "CONSERVATISM_ACTIVATE_UNDER_PROB", 0.55)
    if prob >= threshold:
        return pick

    bet_type = pick.get("type", "")
    if bet_type in _ALL_SPREAD_TYPES:
        candidates = _spread_ladder(pick, pred, odds, sport, h_abbr, a_abbr)
    elif bet_type in _ALL_TOTAL_TYPES:
        candidates = _total_ladder(pick, pred, odds, sport)
    else:
        return pick  # ML / 1st INN / Q1_ML — no safer sibling

    edge_floor = getattr(config, "CONSERVATISM_MIN_EDGE_AFTER_SWAP", 2.0)
    prob_bump = getattr(config, "CONSERVATISM_MIN_PROB_IMPROVEMENT", 0.05)
    juice = _juice_wall(sport)

    qualified = [
        c for c in candidates
        if c["prob"] >= prob + prob_bump
        and c["edge"] >= edge_floor
        and c["odds"] >= juice
    ]
    if not qualified:
        return pick

    winner = max(qualified, key=lambda c: c["prob"])
    return _swap(pick, winner)


def _swap(original: dict, winner: dict) -> dict:
    """Return a pick dict with the winner's line, carrying over confidence
    bands and preserving the original as `safened_from` metadata."""
    swapped = dict(original)
    swapped["type"] = winner["type"]
    swapped["pick"] = winner["pick"]
    swapped["prob"] = round(float(winner["prob"]), 4)
    swapped["edge"] = round(float(winner["edge"]), 1)
    swapped["odds"] = int(winner["odds"])
    swapped["safened"] = True
    swapped["safened_from"] = {
        "type": original.get("type"),
        "pick": original.get("pick"),
        "prob": original.get("prob"),
        "edge": original.get("edge"),
        "odds": original.get("odds"),
    }
    # adjusted_ev is recomputed by the caller after the ladder runs.
    swapped.pop("adjusted_ev", None)
    # Rebuild the UI's probability band around the new point estimate
    # using the half-width copied from the original pick.
    ci = float(original.get("ci_half_width") or 0.05)
    new_prob = swapped["prob"]
    swapped["prob_low"] = round(max(0.0, new_prob - ci), 4)
    swapped["prob_high"] = round(min(1.0, new_prob + ci), 4)
    swapped["ci_half_width"] = ci
    return swapped


# ── Spread ladder ─────────────────────────────────────────────

def _parse_spread_pick(pick_str: str) -> tuple[str, float] | None:
    m = _SPREAD_PICK_RE.match(pick_str.strip())
    if not m:
        return None
    try:
        return m.group("abbr"), float(m.group("point"))
    except ValueError:
        return None


def _sport_spread_bet_type(sport: str, is_alt: bool) -> str:
    if sport == "nba":
        return "Q1_SPREAD"  # Q1 alts get tagged the same type
    if sport == "nhl":
        return "ALT PL" if is_alt else "PL"
    return "ALT RL" if is_alt else "RL"


def _margin_probs(pred: dict) -> dict:
    """MLB nests margin_probs in run_line; NHL/NBA expose at top level."""
    nested = (pred.get("run_line") or {}).get("margin_probs")
    return nested or pred.get("margin_probs") or {}


def _total_probs(pred: dict) -> dict:
    nested = (pred.get("run_line") or {}).get("total_probs")
    return nested or pred.get("total_probs") or {}


def _enumerate_spread_lines(odds: dict, sport: str) -> list[tuple[float, Any, Any, bool]]:
    """Return [(home_point, home_odds, away_odds, is_alt), ...] for this
    sport's primary + alt spread markets. Points are in home's perspective."""
    lines: list[tuple[float, Any, Any, bool]] = []

    if sport == "nba":
        # Q1 primary
        q1_spread = odds.get("q1_spread")
        if q1_spread is not None:
            lines.append((float(q1_spread),
                          odds.get("q1_spread_home_odds"),
                          odds.get("q1_spread_away_odds"),
                          False))
        for alt in odds.get("q1_alt_spreads") or []:
            pt = alt.get("point")
            if pt is not None:
                lines.append((float(pt), alt.get("home_odds"),
                              alt.get("away_odds"), True))
        return lines

    # MLB/NHL share the same alt_spreads schema
    home_pt = odds.get("home_spread_point")
    if home_pt is not None:
        lines.append((float(home_pt),
                      odds.get("home_spread_odds"),
                      odds.get("away_spread_odds"),
                      False))
    else:
        # Fall back to the RL/PL fields used by MLB/NHL predicts.
        h_rl = odds.get("home_rl_odds")
        a_rl = odds.get("away_rl_odds")
        if h_rl is not None or a_rl is not None:
            # Primary point depends on who's favored — detect from ML
            h_ml = odds.get("home_ml")
            a_ml = odds.get("away_ml")
            if h_ml is not None and a_ml is not None:
                home_line = -1.5 if int(h_ml) < int(a_ml) else 1.5
                lines.append((home_line, h_rl, a_rl, False))
    for alt in odds.get("alt_spreads") or []:
        pt = alt.get("point")
        if pt is not None:
            lines.append((float(pt), alt.get("home_odds"),
                          alt.get("away_odds"), True))
    return lines


def _ml_candidate(pick_abbr: str, is_home: bool, pred: dict, odds: dict,
                  sport: str) -> dict | None:
    """Return an ML candidate for the pick team, or None if unavailable."""
    if sport == "nba":
        ml_home_odds = odds.get("q1_home_ml")
        ml_away_odds = odds.get("q1_away_ml")
        home_prob = pred.get("q1_ml_home")
        away_prob = pred.get("q1_ml_away")
        ml_type = "Q1_ML"
        suffix = " Q1 ML"
    else:
        ml_home_odds = odds.get("home_ml")
        ml_away_odds = odds.get("away_ml")
        wp = pred.get("win_prob") or {}
        home_prob = wp.get("home")
        away_prob = wp.get("away")
        ml_type = "ML"
        suffix = ""

    if is_home:
        ml_odds = ml_home_odds
        ml_prob = home_prob
    else:
        ml_odds = ml_away_odds
        ml_prob = away_prob

    if ml_odds is None or ml_prob is None:
        return None
    if not _valid_odds(ml_odds):
        return None

    ml_odds_i = int(ml_odds)
    prob_f = float(ml_prob)
    return {
        "type": ml_type,
        "pick": f"{pick_abbr}{suffix}",
        "prob": prob_f,
        "edge": round((prob_f - _implied(ml_odds_i)) * 100, 1),
        "odds": ml_odds_i,
    }


def _spread_ladder(pick: dict, pred: dict, odds: dict,
                   sport: str, h_abbr: str, a_abbr: str) -> list[dict]:
    parsed = _parse_spread_pick(pick.get("pick", ""))
    if not parsed:
        return []
    pick_abbr, pick_line = parsed

    if pick_abbr == h_abbr:
        is_home = True
    elif pick_abbr == a_abbr:
        is_home = False
    else:
        return []  # pick team doesn't match either side — bail

    margin_probs = _margin_probs(pred)
    if not margin_probs:
        return []

    is_fav = pick_line < 0
    is_q1 = sport == "nba"
    q1_suffix = " Q1" if is_q1 else ""

    candidates: list[dict] = []
    seen_lines: set[float] = set()

    for home_point, h_odds, a_odds, is_alt in _enumerate_spread_lines(odds, sport):
        # Line from the pick team's perspective
        team_point = home_point if is_home else -home_point
        # Safer iff the team gets MORE points than the current pick.
        if team_point <= pick_line:
            continue
        line_odds = h_odds if is_home else a_odds
        if not _valid_odds(line_odds):
            continue
        if team_point in seen_lines:
            continue
        seen_lines.add(team_point)

        # P(pick team covers team_point) = P(team_margin + team_point > 0)
        # team_margin (home perspective) = h - a. For home pick,
        # team_margin = h - a; for away pick, team_margin = a - h = -(h-a).
        if is_home:
            prob = sum(p for m, p in margin_probs.items() if m > -team_point)
        else:
            prob = sum(p for m, p in margin_probs.items() if -m > -team_point)
        prob = max(0.0, min(1.0, prob))
        if prob <= 0:
            continue

        line_odds_i = int(line_odds)
        edge = (prob - _implied(line_odds_i)) * 100
        if edge <= 0:
            continue

        sign = "+" if team_point > 0 else ""
        bet_type = _sport_spread_bet_type(sport, is_alt)
        # Primary-line picks should keep their primary bet_type even
        # when reached via the alt-shopping catalog, so ALT RL/PL/Q1
        # labels only attach when the line is actually secondary.
        if sport != "nba" and is_alt is False:
            bet_type = "PL" if sport == "nhl" else "RL"

        pick_str = f"{pick_abbr} {sign}{team_point:g}{q1_suffix}"
        candidates.append({
            "type": bet_type,
            "pick": pick_str,
            "prob": round(prob, 4),
            "edge": round(edge, 1),
            "odds": line_odds_i,
        })

    # ML is always safer than a favorite spread. For underdogs, ML is
    # RISKIER than +1.5 (you lose the cushion), so skip.
    if is_fav:
        ml = _ml_candidate(pick_abbr, is_home, pred, odds, sport)
        if ml is not None:
            candidates.append(ml)

    return candidates


# ── Total ladder ──────────────────────────────────────────────

def _parse_total_pick(pick_str: str) -> tuple[str, float] | None:
    m = _TOTAL_PICK_RE.match(pick_str.strip())
    if not m:
        return None
    direction = m.group("dir").capitalize()
    try:
        return direction, float(m.group("line"))
    except ValueError:
        return None


def _enumerate_total_lines(odds: dict, sport: str) -> list[tuple[float, Any, Any, bool]]:
    """[(line, over_odds, under_odds, is_alt), ...]"""
    lines: list[tuple[float, Any, Any, bool]] = []

    if sport == "nba":
        q1_total = odds.get("q1_total")
        if q1_total is not None:
            lines.append((float(q1_total),
                          odds.get("q1_over_odds"),
                          odds.get("q1_under_odds"),
                          False))
        for alt in odds.get("q1_alt_totals") or []:
            ln = alt.get("line")
            if ln is not None:
                lines.append((float(ln), alt.get("over_odds"),
                              alt.get("under_odds"), True))
        return lines

    # MLB / NHL
    ou = odds.get("over_under")
    over_odds = odds.get("over_odds")
    under_odds = odds.get("under_odds")
    if ou is not None:
        lines.append((float(ou), over_odds, under_odds, False))
    for alt in odds.get("alt_totals") or []:
        ln = alt.get("line")
        if ln is not None:
            lines.append((float(ln), alt.get("over_odds"),
                          alt.get("under_odds"), True))
    return lines


def _sport_total_bet_type(sport: str, is_alt: bool) -> str:
    if sport == "nba":
        return "Q1_TOTAL"
    return "ALT O/U" if is_alt else "O/U"


def _total_ladder(pick: dict, pred: dict, odds: dict, sport: str) -> list[dict]:
    parsed = _parse_total_pick(pick.get("pick", ""))
    if not parsed:
        return []
    direction, pick_line = parsed

    total_probs = _total_probs(pred)
    if not total_probs:
        return []

    is_q1 = sport == "nba"
    q1_suffix = " Q1" if is_q1 else ""

    candidates: list[dict] = []
    seen_lines: set[float] = set()

    for line_value, over_odds, under_odds, is_alt in _enumerate_total_lines(odds, sport):
        # Safer: Over N is safer when N is smaller; Under N is safer
        # when N is larger. Skip lines that aren't actually safer.
        if direction == "Over":
            if line_value >= pick_line:
                continue
            line_odds = over_odds
            prob = sum(p for t, p in total_probs.items() if t > line_value)
            label = f"Over {line_value:g}{q1_suffix}"
        else:
            if line_value <= pick_line:
                continue
            line_odds = under_odds
            prob = sum(p for t, p in total_probs.items() if t < line_value)
            label = f"Under {line_value:g}{q1_suffix}"

        if not _valid_odds(line_odds):
            continue
        if line_value in seen_lines:
            continue
        seen_lines.add(line_value)

        prob = max(0.0, min(1.0, prob))
        if prob <= 0:
            continue
        line_odds_i = int(line_odds)
        edge = (prob - _implied(line_odds_i)) * 100
        if edge <= 0:
            continue

        candidates.append({
            "type": _sport_total_bet_type(sport, is_alt),
            "pick": label,
            "prob": round(prob, 4),
            "edge": round(edge, 1),
            "odds": line_odds_i,
        })

    return candidates
