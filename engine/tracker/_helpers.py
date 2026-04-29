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
        # NRFI / YRFI close on the per-event totals_1st_1_innings market.
        if pk == "NRFI":
            return game_odds.get("nrfi_under_odds")
        return game_odds.get("nrfi_over_odds")
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
