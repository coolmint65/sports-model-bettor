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
    """Pick the right closing-odds field for an NBA pick out of an HR
    odds bucket. Covers Q1 markets + full-game ML/SPREAD/TOTAL +
    ALT spreads/totals on both Q1 and full game.

    Pre-fix, only Q1_ML/Q1_SPREAD/Q1_TOTAL were wired. Result: full-
    game NBA picks (ML / SPREAD / TOTAL) tracked 0% closing-odds
    coverage even when HR returned the data, because this extractor
    returned None for them.
    """
    if not game_odds:
        return None
    pk = pick_text or ""
    parts = pk.split()

    def _is_home_team(team: str) -> bool:
        return team == home_abbr or team == _ALT_ABBRS.get(home_abbr, "")

    def _line_matches(a, b, tol: float = 0.001) -> bool:
        if a is None or b is None:
            return False
        try:
            return abs(float(a) - float(b)) < tol
        except (TypeError, ValueError):
            return False

    def _signed_line(idx: int) -> float | None:
        if len(parts) <= idx:
            return None
        try:
            return float(parts[idx])
        except (TypeError, ValueError):
            return None

    def _spread_close(primary_home: float, primary_away: float,
                       h_odds, a_odds, alt_key: str, is_home: bool,
                       bet_point: float | None):
        """Match the SPECIFIC spread number we bet before falling back to
        the primary side odds. Prevents comparing our -3.5 pick to the
        current market's -1.5 primary line — the whole reason CLV on
        Q1_SPREAD was reading -13pp on winning picks (root-caused 2026-
        07-03: closing odds were being stamped for a different line
        than the pick was placed at)."""
        if bet_point is None:
            return h_odds if is_home else a_odds
        # Signed comparison: home line and pick point should agree in
        # sign when home is the pick, away when away is the pick.
        home_target = bet_point if is_home else None
        away_target = -bet_point if is_home else bet_point
        # Primary line check first — cheapest hit.
        if is_home and _line_matches(primary_home, bet_point):
            return h_odds
        if (not is_home) and _line_matches(primary_away, bet_point):
            return a_odds
        # Fall through to alt lines.
        for alt in game_odds.get(alt_key) or []:
            pt = alt.get("point")
            if is_home and _line_matches(pt, home_target):
                return alt.get("home_odds")
            if (not is_home) and _line_matches(pt, away_target):
                return alt.get("away_odds")
        return None

    def _total_close(primary_line, over_odds, under_odds, alt_key: str,
                       is_over: bool, bet_line: float | None):
        if bet_line is None:
            return over_odds if is_over else under_odds
        if _line_matches(primary_line, bet_line):
            return over_odds if is_over else under_odds
        for alt in game_odds.get(alt_key) or []:
            if _line_matches(alt.get("line"), bet_line):
                return (alt.get("over_odds") if is_over
                        else alt.get("under_odds"))
        return None

    # ── Q1 primary markets ──
    if bet_type == "Q1_ML":
        if not parts: return None
        return (game_odds.get("q1_home_ml") if _is_home_team(parts[0])
                else game_odds.get("q1_away_ml"))
    if bet_type == "Q1_SPREAD":
        if len(parts) < 2: return None
        is_home = _is_home_team(parts[0])
        return _spread_close(
            game_odds.get("q1_spread_home_pt"),
            game_odds.get("q1_spread_away_pt"),
            game_odds.get("q1_spread_home_odds"),
            game_odds.get("q1_spread_away_odds"),
            "q1_alt_spreads", is_home, _signed_line(1),
        )
    if bet_type == "Q1_TOTAL":
        if not parts: return None
        is_over = parts[0].lower() == "over"
        return _total_close(
            game_odds.get("q1_total_line"),
            game_odds.get("q1_over_odds"),
            game_odds.get("q1_under_odds"),
            "q1_alt_totals", is_over, _signed_line(1),
        )

    # ── Full-game primary markets ──
    if bet_type == "ML":
        if not parts: return None
        return (game_odds.get("home_ml") if _is_home_team(parts[0])
                else game_odds.get("away_ml"))
    if bet_type == "SPREAD":
        if len(parts) < 2: return None
        is_home = _is_home_team(parts[0])
        return _spread_close(
            game_odds.get("home_spread_pt"),
            game_odds.get("away_spread_pt"),
            game_odds.get("home_spread_odds"),
            game_odds.get("away_spread_odds"),
            "alt_spreads", is_home, _signed_line(1),
        )
    if bet_type == "TOTAL":
        if not parts: return None
        is_over = parts[0].lower() == "over"
        return _total_close(
            game_odds.get("total_line"),
            game_odds.get("over_odds"),
            game_odds.get("under_odds"),
            "alt_totals", is_over, _signed_line(1),
        )

    # ── ALT spreads (both full + Q1) ──
    # Pick label looks like "BOS -3.5" or "PHI +8.5". Match by signed
    # spread point in alt_spreads (full) or q1_alt_spreads (Q1).
    if bet_type in ("ALT SPREAD", "Q1 ALT SPREAD"):
        if len(parts) < 2: return None
        try:
            spread = float(parts[1])
        except ValueError:
            return None
        is_home = _is_home_team(parts[0])
        target = spread if is_home else -spread
        alt_key = "q1_alt_spreads" if bet_type.startswith("Q1") else "alt_spreads"
        for alt in game_odds.get(alt_key) or []:
            if alt.get("point") == target:
                return alt.get("home_odds") if is_home else alt.get("away_odds")
        # Fall back to primary spread odds for that family.
        if bet_type.startswith("Q1"):
            return (game_odds.get("q1_spread_home_odds") if is_home
                    else game_odds.get("q1_spread_away_odds"))
        return (game_odds.get("home_spread_odds") if is_home
                else game_odds.get("away_spread_odds"))

    # ── ALT totals (both full + Q1) ──
    # Pick label looks like "Over 215.5" or "Under 49.5".
    if bet_type in ("ALT TOTAL", "Q1 ALT TOTAL"):
        if not parts: return None
        try:
            line = float(parts[-1])
        except (ValueError, IndexError):
            return None
        is_over = parts[0].lower() == "over"
        alt_key = "q1_alt_totals" if bet_type.startswith("Q1") else "alt_totals"
        for alt in game_odds.get(alt_key) or []:
            if alt.get("line") == line:
                return alt.get("over_odds") if is_over else alt.get("under_odds")
        # Fall back to primary total odds for that family.
        if bet_type.startswith("Q1"):
            return (game_odds.get("q1_over_odds") if is_over
                    else game_odds.get("q1_under_odds"))
        return (game_odds.get("over_odds") if is_over
                else game_odds.get("under_odds"))

    return None
