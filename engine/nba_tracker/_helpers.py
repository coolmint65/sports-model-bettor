"""NBA tracker helpers + abbreviation aliases."""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

# Phase 1 Q1 derivatives — excluded from main NBA tracker recording.
# Routed through engine.derivative_tracker for paper-bet evaluation.
_NBA_DERIVATIVE_TYPES: set[str] = {"Q1 Team Total", "Q1 Total O/E"}


# ESPN alternate abbreviation map (ESPN sometimes uses different abbrs).
_ALT_ABBRS = {
    "GS": "GSW", "GSW": "GS",
    "SA": "SAS", "SAS": "SA",
    "NO": "NOP", "NOP": "NO",
    "NY": "NYK", "NYK": "NY",
    "PHO": "PHX", "PHX": "PHO",
    "UTAH": "UTA", "UTA": "UTAH",
    "WSH": "WAS", "WAS": "WSH",
    "BKN": "BK", "BK": "BKN",
    "CHA": "CHO", "CHO": "CHA",
}

# ESPN scoreboard abbreviations that don't match the Odds API / internal
# abbrs used elsewhere. Extend this when a new mismatch shows up.
_ESPN_TO_INTERNAL_ABBR = {
    "GS": "GSW",
    "NOP": "NO", "NYK": "NY", "SAS": "SA", "UTA": "UTAH", "WAS": "WSH",
}


def _core_picks(picks: list[dict]) -> list[dict]:
    return [p for p in picks if p.get("type") not in _NBA_DERIVATIVE_TYPES]


def _compute_clv(bet_odds, closing_odds):
    """Compute closing line value.
    Positive CLV = got better price than closing line = sharp.
    """
    if not bet_odds or not closing_odds:
        return None
    bet_implied = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 else 100 / (bet_odds + 100)
    close_implied = abs(closing_odds) / (abs(closing_odds) + 100) if closing_odds < 0 else 100 / (closing_odds + 100)
    return round((close_implied - bet_implied) * 100, 2)


def _normalize_espn_abbr(abbr: str) -> str:
    """Map an ESPN scoreboard team abbreviation to the internal form used
    by nba_odds / nba_db / nba_picks."""
    return _ESPN_TO_INTERNAL_ABBR.get(abbr, abbr)


def _extract_nba_closing_for_pick(bet_type: str, pick_text: str,
                                  home_abbr: str, game_odds: dict) -> int | None:
    """Pure helper: pick the right Q1 closing-odds field for an NBA pick.

    Mirrors engine.tracker._extract_closing_for_pick but for the Q1
    markets that nba_tracker records (Q1_ML / Q1_SPREAD / Q1_TOTAL).
    """
    if not game_odds:
        return None
    pk = pick_text or ""
    parts = pk.split()
    if bet_type == "Q1_ML":
        if not parts:
            return None
        pick_team = parts[0]
        is_home = pick_team == home_abbr or pick_team == _ALT_ABBRS.get(home_abbr, "")
        return game_odds.get("q1_home_ml") if is_home else game_odds.get("q1_away_ml")
    if bet_type == "Q1_SPREAD":
        if len(parts) < 2:
            return None
        pick_team = parts[0]
        is_home = pick_team == home_abbr or pick_team == _ALT_ABBRS.get(home_abbr, "")
        return (game_odds.get("q1_spread_home_odds") if is_home
                else game_odds.get("q1_spread_away_odds"))
    if bet_type == "Q1_TOTAL":
        if not parts:
            return None
        return (game_odds.get("q1_over_odds") if parts[0].lower() == "over"
                else game_odds.get("q1_under_odds"))
    return None
