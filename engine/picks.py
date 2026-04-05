"""
Unified pick generation.

Single source of truth for computing betting picks for a game.
Used by Best Bets, Pick Tracker, and Game Detail sidebar.

Every pick includes: type, pick label, model probability,
edge vs real odds, the actual odds, and confidence level.
"""

import logging
from datetime import datetime

from .mlb_predict import predict_matchup, MLB_AVG_RPG
from .db import get_conn

logger = logging.getLogger(__name__)

# Juice wall — don't recommend bets with worse odds than this
JUICE_WALL = -180


def _implied(ml: int) -> float:
    """American odds to implied probability."""
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


def _payout(odds: int, won: bool) -> float:
    """Calculate profit on a $100 bet."""
    if won:
        if odds > 0:
            return float(odds)
        else:
            return 100 / abs(odds) * 100
    return -100.0


def generate_picks(home_team_id: int, away_team_id: int,
                    home_pitcher_id: int | None = None,
                    away_pitcher_id: int | None = None,
                    venue: str | None = None,
                    odds: dict | None = None) -> list[dict]:
    """
    Generate all betting picks for a game.

    Args:
        home/away_team_id: MLB team IDs
        home/away_pitcher_id: Starting pitcher IDs
        venue: Ballpark name
        odds: Real DraftKings odds dict from Odds API:
              {home_ml, away_ml, over_under, over_odds, under_odds,
               home_spread_odds, away_spread_odds,
               home_spread_point, away_spread_point}

    Returns list of picks, sorted by edge (best first):
    [
        {
            "type": "ML" | "O/U" | "1st INN" | "RL",
            "pick": "NYY",
            "prob": 0.542,
            "edge": 3.2,
            "odds": -120,
            "confidence": "medium",
        },
        ...
    ]
    """
    # Run prediction
    pred = predict_matchup(
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        home_pitcher_id=home_pitcher_id,
        away_pitcher_id=away_pitcher_id,
        venue=venue,
    )

    if "error" in pred or not pred:
        return []

    odds = odds or {}
    wp = pred.get("win_prob", {})
    rl = pred.get("run_line", {})
    fi = pred.get("first_inning", {})
    total = pred.get("total", 0)
    conf_score = pred.get("confidence", {}).get("score", 50)

    home = pred.get("home", {})
    away = pred.get("away", {})
    h_abbr = home.get("abbreviation", "HOME")
    a_abbr = away.get("abbreviation", "AWAY")

    # ── Dampen probabilities based on confidence ──
    dampen = conf_score / 100 if conf_score < 80 else 1.0

    home_wp = wp.get("home", 0.5) * dampen + 0.50 * (1 - dampen)
    away_wp = wp.get("away", 0.5) * dampen + 0.50 * (1 - dampen)

    rl_home_15 = rl.get("home_minus_1_5", 0.5) * dampen + 0.38 * (1 - dampen)
    rl_away_15 = rl.get("away_plus_1_5", 0.5) * dampen + 0.62 * (1 - dampen)

    # Minimum confidence to claim edge on big underdogs/favorites
    # If model is heavily dampened (near 50%), don't bet on +200 underdogs
    # or lay -250 favorites — the model doesn't actually "know" enough
    min_edge_confidence = 0.03  # Must deviate at least 3% from 50% to bet

    picks = []

    # ── Moneyline ──
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")

    if home_ml and home_ml >= JUICE_WALL:
        # Only claim edge if model has genuine conviction (not just dampened noise)
        if abs(home_wp - 0.5) > min_edge_confidence or conf_score >= 60:
            edge = (home_wp - _implied(home_ml)) * 100
            if edge > 0:
                picks.append({
                    "type": "ML", "pick": h_abbr, "prob": round(home_wp, 4),
                    "edge": round(edge, 1), "odds": home_ml,
                })

    if away_ml and away_ml >= JUICE_WALL:
        if abs(away_wp - 0.5) > min_edge_confidence or conf_score >= 60:
            edge = (away_wp - _implied(away_ml)) * 100
            if edge > 0:
                picks.append({
                    "type": "ML", "pick": a_abbr, "prob": round(away_wp, 4),
                    "edge": round(edge, 1), "odds": away_ml,
                })

    # ── Over/Under ──
    vegas_total = odds.get("over_under")
    if vegas_total and pred.get("over_under"):
        ou_data = _find_ou(pred["over_under"], vegas_total)
        if ou_data:
            ou_pick_over = ou_data["over"] > ou_data["under"]
            raw_prob = max(ou_data["over"], ou_data["under"])
            ou_prob = raw_prob * dampen + 0.50 * (1 - dampen)
            ou_label = f"{'Over' if ou_pick_over else 'Under'} {vegas_total}"

            real_ou_odds = odds.get("over_odds") if ou_pick_over else odds.get("under_odds")
            if real_ou_odds:
                ou_implied = _implied(real_ou_odds)
            else:
                ou_implied = 0.524
                real_ou_odds = -110

            edge = (ou_prob - ou_implied) * 100
            if edge > 0 and real_ou_odds >= JUICE_WALL:
                picks.append({
                    "type": "O/U", "pick": ou_label, "prob": round(ou_prob, 4),
                    "edge": round(edge, 1), "odds": real_ou_odds,
                })

    # ── First Inning (NRFI/YRFI) ──
    nrfi = fi.get("nrfi", 0.5)
    nrfi_pick = "NRFI" if nrfi > 0.5 else "YRFI"
    nrfi_prob = nrfi if nrfi > 0.5 else fi.get("yrfi", 0.5)
    nrfi_prob = nrfi_prob * dampen + 0.50 * (1 - dampen)
    nrfi_edge = (nrfi_prob - 0.545) * 100  # -120 implied
    if nrfi_edge > 1:
        picks.append({
            "type": "1st INN", "pick": nrfi_pick, "prob": round(nrfi_prob, 4),
            "edge": round(nrfi_edge, 1), "odds": -120,
        })

    # ── Run Line ──
    # Use real odds when available, otherwise derive from ML
    home_rl_odds = odds.get("home_spread_odds")
    away_rl_odds = odds.get("away_spread_odds")
    home_rl_point = odds.get("home_spread_point")
    away_rl_point = odds.get("away_spread_point")

    # If no RL data from API, derive from ML: favorite gets -1.5, dog gets +1.5
    if home_rl_point is None and home_ml and away_ml:
        home_is_fav = (home_ml < 0 and abs(home_ml) > abs(away_ml)) if home_ml < 0 else False
        if not home_is_fav and away_ml < 0:
            home_is_fav = False
        elif home_ml < 0:
            home_is_fav = True

        if home_is_fav:
            home_rl_point = -1.5
            away_rl_point = 1.5
            home_rl_odds = home_rl_odds or 120   # Fav -1.5 pays +120
            away_rl_odds = away_rl_odds or -140   # Dog +1.5 costs -140
        else:
            home_rl_point = 1.5
            away_rl_point = -1.5
            home_rl_odds = home_rl_odds or -140   # Dog +1.5 costs -140
            away_rl_odds = away_rl_odds or 120    # Fav -1.5 pays +120

    # Home side
    if home_rl_odds and home_rl_odds >= JUICE_WALL and home_rl_point is not None:
        rl_prob = rl_home_15 if home_rl_point < 0 else rl_away_15
        edge = (rl_prob - _implied(home_rl_odds)) * 100
        if edge > 0:
            sign = "+" if home_rl_point > 0 else ""
            picks.append({
                "type": "RL",
                "pick": f"{h_abbr} {sign}{home_rl_point}",
                "prob": round(rl_prob, 4),
                "edge": round(edge, 1),
                "odds": home_rl_odds,
            })

    # Away side
    if away_rl_odds and away_rl_odds >= JUICE_WALL and away_rl_point is not None:
        rl_prob = rl_away_15 if away_rl_point > 0 else rl_home_15
        edge = (rl_prob - _implied(away_rl_odds)) * 100
        if edge > 0:
            sign = "+" if away_rl_point > 0 else ""
            picks.append({
                "type": "RL",
                "pick": f"{a_abbr} {sign}{away_rl_point}",
                "prob": round(rl_prob, 4),
                "edge": round(edge, 1),
                "odds": away_rl_odds,
            })

    # Sort by edge
    picks.sort(key=lambda p: p["edge"], reverse=True)

    # Add confidence rating
    for p in picks:
        if p["edge"] > 8:
            p["confidence"] = "strong"
        elif p["edge"] > 4:
            p["confidence"] = "moderate"
        elif p["edge"] > 1.5:
            p["confidence"] = "lean"
        else:
            p["confidence"] = "skip"

    return picks


def get_best_pick(picks: list[dict]) -> dict | None:
    """Return the single best pick (highest edge) from a picks list."""
    playable = [p for p in picks if p.get("confidence") != "skip"]
    return playable[0] if playable else None


def _find_ou(ou_lines: dict, vegas_total: float) -> dict | None:
    """Find O/U entry closest to the Vegas total."""
    vt = float(vegas_total)
    for fmt in [str(vt), f"{vt:.1f}", str(int(vt))]:
        if fmt in ou_lines:
            return ou_lines[fmt]
    best_key = min(ou_lines.keys(), key=lambda k: abs(float(k) - vt), default=None)
    return ou_lines.get(best_key) if best_key else None


def fetch_real_odds_for_games() -> dict:
    """
    Fetch real DraftKings odds for all today's games.
    Returns {matchup_key: odds_dict}.
    Cached by the Odds API module.
    """
    try:
        from scrapers.odds_api import fetch_odds
        return fetch_odds()
    except Exception:
        return {}


def match_odds(home_abbr: str, away_abbr: str, all_odds: dict) -> dict:
    """Find odds for a specific matchup from the odds map."""
    # ESPN/Odds API abbreviation differences
    ALT = {"ARI": "AZ", "AZ": "ARI", "CHW": "CWS", "CWS": "CHW",
           "WSH": "WAS", "WAS": "WSH", "ATH": "OAK", "OAK": "ATH"}

    keys_to_try = [
        f"{away_abbr}@{home_abbr}",
        f"{ALT.get(away_abbr, away_abbr)}@{ALT.get(home_abbr, home_abbr)}",
        f"{ALT.get(away_abbr, away_abbr)}@{home_abbr}",
        f"{away_abbr}@{ALT.get(home_abbr, home_abbr)}",
    ]

    for key in keys_to_try:
        if key in all_odds:
            return all_odds[key]

    return {}
