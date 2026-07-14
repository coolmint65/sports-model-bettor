"""Tracker helpers + shared constants.

Small functions every other tracker sub-module pulls from. Keeping
them in one tiny file avoids deeper cross-imports between _record /
_settle / _summary.
"""

from __future__ import annotations

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"

# Phase 1 derivative bet types — excluded from main tracker recording
# at sync time. Derivatives flow through engine.derivative_tracker
# instead so their performance is logged in isolation.
_MLB_DERIVATIVE_TYPES: set[str] = {
    "Team Total", "F5 Team Total", "Inning Total", "Inning BTS",
    "1st Inn Winner", "F5 Winner", "Total O/E", "Extra Innings",
}


def _core_picks(picks: list[dict]) -> list[dict]:
    """Drop derivative bet types so the main tracker stays focused on
    core markets (ML/RL/O/U/F5/1st INN/ALT)."""
    return [p for p in picks if p.get("type") not in _MLB_DERIVATIVE_TYPES]


def _compute_clv(bet_odds, closing_odds):
    """Compute closing line value.
    Positive CLV = got better price than closing line = sharp.
    """
    if not bet_odds or not closing_odds:
        return None
    bet_implied = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 else 100 / (bet_odds + 100)
    close_implied = abs(closing_odds) / (abs(closing_odds) + 100) if closing_odds < 0 else 100 / (closing_odds + 100)
    return round((close_implied - bet_implied) * 100, 2)  # positive = we got a better price


def _extract_closing_for_pick(bet_type: str, pick_text: str,
                               home_abbr: str, game_odds: dict) -> int | None:
    """Pure helper: pick the right field out of an odds dict for a bet.

    Used by both _fetch_closing_odds_for_pick (which fetches odds first)
    and the inline settle_picks() capture path (which already has odds
    in hand). Centralizing avoids two branches drifting apart when we
    add new market types.
    """
    if not game_odds:
        return None
    bt = bet_type
    pk = pick_text or ""
    if bt in ("ml", "ML"):
        return (game_odds.get("home_ml") if pk == home_abbr
                else game_odds.get("away_ml"))
    if bt in ("ou", "O/U"):
        return (game_odds.get("over_odds") if "Over" in pk
                else game_odds.get("under_odds"))
    if bt in ("rl", "RL"):
        pick_team = pk.split()[0] if pk.split() else ""
        return (game_odds.get("home_spread_odds") if pick_team == home_abbr
                else game_odds.get("away_spread_odds"))
    if bt in ("nrfi", "1st INN"):
        # NRFI = inning_totals.I1.under_odds; YRFI = .over_odds.
        # HR returns the 1st-inning totals market under inning_totals,
        # NOT under top-level nrfi_*_odds keys (which were never
        # populated). Pre-fix, this branch returned None for every
        # 1st INN pick → 0% closing-odds coverage on all NRFI/YRFI.
        i1 = (game_odds.get("inning_totals") or {}).get("I1") or {}
        if pk == "NRFI":
            return i1.get("under_odds")
        return i1.get("over_odds")
    if bt == "F5 ML":
        return (game_odds.get("f5_home_ml") if pk == home_abbr
                else game_odds.get("f5_away_ml"))
    if bt == "F5 O/U":
        return (game_odds.get("f5_over_odds") if "Over" in pk
                else game_odds.get("f5_under_odds"))
    if bt == "F5 RL":
        pick_team = pk.split()[0] if pk.split() else ""
        return (game_odds.get("f5_home_spread_odds") if pick_team == home_abbr
                else game_odds.get("f5_away_spread_odds"))
    # ALT O/U / ALT TOTAL — match by line value in alt_totals.
    # User reported on 2026-04-28 that most settled picks lack CLV;
    # root cause was this extractor skipping ALT lines entirely. ALT
    # picks are stored with a pick label like "Over 8.5" or
    # "Under 9.5" — same shape as primary O/U, just at a non-standard
    # line. Find the alt_totals entry whose line matches and pull
    # the right side's odds.
    if bt in ("ALT O/U", "ALT TOTAL"):
        parts = pk.split()
        try:
            line = float(parts[-1])
        except (ValueError, IndexError):
            return None
        is_over = "Over" in pk or "over" in pk
        for alt in game_odds.get("alt_totals") or []:
            if alt.get("line") == line:
                return alt.get("over_odds") if is_over else alt.get("under_odds")
        # Fall back to primary O/U if the alt line isn't present —
        # better stale CLV than missing CLV.
        return (game_odds.get("over_odds") if is_over
                else game_odds.get("under_odds"))
    # Team Total — pick label "BAL Over 4.5" or "MIL Under 3.5".
    # HR exposes per-team total under team_total_{home,away}.
    # Pre-fix this branch was missing → 0% closing-odds coverage on
    # all MLB Team Total picks.
    if bt == "Team Total":
        parts = pk.split()
        if len(parts) < 3:
            return None
        pick_team = parts[0]
        side = parts[1].lower()  # 'over' or 'under'
        is_home = pick_team == home_abbr
        tt_block = (game_odds.get("team_total_home") if is_home
                    else game_odds.get("team_total_away"))
        if not isinstance(tt_block, dict):
            return None
        return (tt_block.get("over_odds") if side == "over"
                else tt_block.get("under_odds"))
    # ALT RL — pick label "MIL +1.5" or "BAL -2.5". Match by signed
    # spread value in alt_spreads.
    if bt in ("ALT RL", "ALT SPREAD"):
        parts = pk.split()
        if len(parts) < 2:
            return None
        pick_team = parts[0]
        try:
            spread = float(parts[1])
        except ValueError:
            return None
        is_home = pick_team == home_abbr
        # alt_spreads stores `point` from home's perspective. Home's
        # spread is the literal point; away's spread is -point.
        target_point = spread if is_home else -spread
        for alt in game_odds.get("alt_spreads") or []:
            if alt.get("point") == target_point:
                return alt.get("home_odds") if is_home else alt.get("away_odds")
        # Fall back to primary spread odds.
        return (game_odds.get("home_spread_odds") if is_home
                else game_odds.get("away_spread_odds"))
    return None


def _fetch_closing_odds_for_pick(pick: dict, home_abbr: str, away_abbr: str) -> int | None:
    """Fetch current odds from the odds API for a specific pick.

    Returns the relevant moneyline/odds value for the pick's bet type and side,
    or None if unavailable.
    """
    try:
        from ..picks import fetch_real_odds_for_games, match_odds
        all_odds = fetch_real_odds_for_games()
        game_odds = match_odds(home_abbr, away_abbr, all_odds)
        return _extract_closing_for_pick(
            pick["bet_type"], pick["pick"], home_abbr, game_odds or {},
        )
    except Exception:
        return None
