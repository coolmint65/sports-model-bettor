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
from .config import (
    MLB_JUICE_WALL as JUICE_WALL,
    ENABLE_MLB_NRFI,
    ENABLE_MLB_F5,
    MLB_BET_RELIABILITY,
)
from .db import get_conn

logger = logging.getLogger(__name__)


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

    # Raw model probabilities - no dampening. Real odds are the calibration.
    home_wp = wp.get("home", 0.5)
    away_wp = wp.get("away", 0.5)
    # All four RL sides
    rl_home_minus = rl.get("home_minus_1_5", 0.5)   # P(home wins by 2+)
    rl_home_plus = rl.get("home_plus_1_5", 0.5)     # P(home covers +1.5)
    rl_away_minus = rl.get("away_minus_1_5", 0.5)   # P(away wins by 2+)
    rl_away_plus = rl.get("away_plus_1_5", 0.5)     # P(away covers +1.5)

    picks = []

    # ── Moneyline ──
    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")

    if home_ml and home_ml >= JUICE_WALL:
        edge = (home_wp - _implied(home_ml)) * 100
        if edge > 0:
            picks.append({
                "type": "ML", "pick": h_abbr, "prob": round(home_wp, 4),
                "edge": round(edge, 1), "odds": home_ml,
            })

    if away_ml and away_ml >= JUICE_WALL:
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
            ou_prob = max(ou_data["over"], ou_data["under"])
            ou_label = f"{'Over' if ou_pick_over else 'Under'} {vegas_total}"

            real_ou_odds = odds.get("over_odds") if ou_pick_over else odds.get("under_odds")
            if real_ou_odds:
                ou_implied = _implied(real_ou_odds)
            else:
                ou_implied = 0.524
                real_ou_odds = -110

            edge = (ou_prob - ou_implied) * 100
            # Direction filter: skip disabled sides (Overs or Unders)
            from .config import MLB_ALLOW_OU_OVER, MLB_ALLOW_OU_UNDER
            ou_allowed = (ou_pick_over and MLB_ALLOW_OU_OVER) or \
                         ((not ou_pick_over) and MLB_ALLOW_OU_UNDER)
            if edge > 0 and real_ou_odds >= JUICE_WALL and ou_allowed:
                picks.append({
                    "type": "O/U", "pick": ou_label, "prob": round(ou_prob, 4),
                    "edge": round(edge, 1), "odds": real_ou_odds,
                })

    # ── First Inning (NRFI/YRFI) ──
    # Disabled by default (ENABLE_MLB_NRFI in config.py).
    # Backtest shows 1st INN is a money loser (12-14, 46.2%, -$400).
    # The pitcher first-inning scoreless % blending produces unrealistic probs
    # (80%+) that don't calibrate to actual outcomes.
    if ENABLE_MLB_NRFI:
        from .config import MLB_ALLOW_NRFI, MLB_ALLOW_YRFI
        nrfi = fi.get("nrfi", 0.5)
        nrfi_pick = "NRFI" if nrfi > 0.5 else "YRFI"
        nrfi_prob = nrfi if nrfi > 0.5 else fi.get("yrfi", 0.5)
        # NRFI = under 0.5 first-inning runs; YRFI = over.
        # Use real DK odds when available, fall back to -120 synthetic.
        if nrfi_pick == "NRFI":
            nrfi_odds = odds.get("nrfi_under_odds") or -120
        else:
            nrfi_odds = odds.get("nrfi_over_odds") or -120
        nrfi_edge = (nrfi_prob - _implied(nrfi_odds)) * 100
        allow = (nrfi_pick == "NRFI" and MLB_ALLOW_NRFI) or \
                (nrfi_pick == "YRFI" and MLB_ALLOW_YRFI)
        if nrfi_edge > 1 and allow:
            picks.append({
                "type": "1st INN", "pick": nrfi_pick, "prob": round(nrfi_prob, 4),
                "edge": round(nrfi_edge, 1), "odds": nrfi_odds,
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

    # Direction filter for RL: tracker data shows +1.5 dogs are profitable
    # (40-27, 59.7%) while -1.5 favorites are disastrous (3-9, 25%).
    from .config import MLB_ALLOW_RL_FAVORITE, MLB_ALLOW_RL_UNDERDOG

    # Home side - use the correct probability based on spread direction
    if home_rl_odds and home_rl_odds >= JUICE_WALL and home_rl_point is not None:
        # home_rl_point < 0 = home is -1.5 favorite → use home_minus probability
        # home_rl_point > 0 = home is +1.5 underdog → use home_plus probability
        is_dog = home_rl_point > 0
        rl_allowed = (is_dog and MLB_ALLOW_RL_UNDERDOG) or \
                     ((not is_dog) and MLB_ALLOW_RL_FAVORITE)
        rl_prob = rl_home_minus if home_rl_point < 0 else rl_home_plus
        edge = (rl_prob - _implied(home_rl_odds)) * 100
        if edge > 0 and rl_allowed:
            sign = "+" if home_rl_point > 0 else ""
            picks.append({
                "type": "RL",
                "pick": f"{h_abbr} {sign}{home_rl_point}",
                "prob": round(rl_prob, 4),
                "edge": round(edge, 1),
                "odds": home_rl_odds,
            })

    # Away side - same logic
    if away_rl_odds and away_rl_odds >= JUICE_WALL and away_rl_point is not None:
        # away_rl_point > 0 = away is +1.5 underdog → use away_plus probability
        # away_rl_point < 0 = away is -1.5 favorite → use away_minus probability
        is_dog = away_rl_point > 0
        rl_allowed = (is_dog and MLB_ALLOW_RL_UNDERDOG) or \
                     ((not is_dog) and MLB_ALLOW_RL_FAVORITE)
        rl_prob = rl_away_plus if away_rl_point > 0 else rl_away_minus
        edge = (rl_prob - _implied(away_rl_odds)) * 100
        if edge > 0 and rl_allowed:
            sign = "+" if away_rl_point > 0 else ""
            picks.append({
                "type": "RL",
                "pick": f"{a_abbr} {sign}{away_rl_point}",
                "prob": round(rl_prob, 4),
                "edge": round(edge, 1),
                "odds": away_rl_odds,
            })

    # ── F5 (First 5 Innings) ──
    # Disabled by default (ENABLE_MLB_F5 in config.py). Requires real DK
    # F5 odds from the per-event Odds API markets -- synthetic pricing is
    # not supported for F5 (implied probabilities vary too much by SP).
    if ENABLE_MLB_F5:
        f5 = pred.get("f5") or {}
        _append_f5_picks(picks, f5, odds, h_abbr, a_abbr)

    # Adjusted EV: edge * reliability weight
    for p in picks:
        reliability = MLB_BET_RELIABILITY.get(p["type"], 0.5)
        p["adjusted_ev"] = round(p["edge"] * reliability, 2)
    picks.sort(key=lambda p: -p["adjusted_ev"])

    # Add confidence rating (thresholds centralised in engine.config)
    from .config import EDGE_STRONG, EDGE_MODERATE, EDGE_LEAN, EDGE_SKIP
    for p in picks:
        e = p["edge"]
        if e >= EDGE_STRONG:
            p["confidence"] = "strong"
        elif e >= EDGE_MODERATE:
            p["confidence"] = "moderate"
        elif e >= EDGE_LEAN:
            p["confidence"] = "lean"
        else:
            p["confidence"] = "skip"
        if e < EDGE_SKIP:
            p["confidence"] = "skip"

    return picks


def get_best_pick(picks: list[dict]) -> dict | None:
    """Return the single best pick (highest edge) from a picks list."""
    playable = [p for p in picks if p.get("confidence") != "skip"]
    return playable[0] if playable else None


def _append_f5_picks(picks: list, f5: dict, odds: dict,
                      h_abbr: str, a_abbr: str) -> None:
    """Generate F5 ML / O/U / RL picks from model + real DK F5 odds.

    Skips any sub-market when real DK odds are missing -- F5 pricing
    varies too much with starting pitchers to use a synthetic baseline.
    """
    from .config import (
        MLB_ALLOW_F5_ML,
        MLB_ALLOW_F5_OU_OVER, MLB_ALLOW_F5_OU_UNDER,
        MLB_ALLOW_F5_RL_FAVORITE, MLB_ALLOW_F5_RL_UNDERDOG,
    )

    if not f5:
        return

    wp = f5.get("win_prob") or {}
    f5_home_wp = wp.get("home", 0.5)
    f5_away_wp = wp.get("away", 0.5)

    # ── F5 Moneyline ──
    if MLB_ALLOW_F5_ML:
        f5_home_ml = odds.get("f5_home_ml")
        f5_away_ml = odds.get("f5_away_ml")
        if f5_home_ml and f5_home_ml >= JUICE_WALL:
            edge = (f5_home_wp - _implied(f5_home_ml)) * 100
            if edge > 0:
                picks.append({
                    "type": "F5 ML", "pick": h_abbr, "prob": round(f5_home_wp, 4),
                    "edge": round(edge, 1), "odds": f5_home_ml,
                })
        if f5_away_ml and f5_away_ml >= JUICE_WALL:
            edge = (f5_away_wp - _implied(f5_away_ml)) * 100
            if edge > 0:
                picks.append({
                    "type": "F5 ML", "pick": a_abbr, "prob": round(f5_away_wp, 4),
                    "edge": round(edge, 1), "odds": f5_away_ml,
                })

    # ── F5 Over/Under ──
    f5_total_line = odds.get("f5_total")
    if f5_total_line and f5.get("over_under"):
        ou_data = _find_ou(f5["over_under"], f5_total_line)
        if ou_data:
            f5_over = ou_data.get("over", 0.5)
            f5_under = ou_data.get("under", 0.5)
            pick_over = f5_over > f5_under
            ou_allowed = (pick_over and MLB_ALLOW_F5_OU_OVER) or \
                         ((not pick_over) and MLB_ALLOW_F5_OU_UNDER)
            ou_prob = f5_over if pick_over else f5_under
            ou_odds = odds.get("f5_over_odds") if pick_over else odds.get("f5_under_odds")
            if ou_odds and ou_odds >= JUICE_WALL and ou_allowed:
                edge = (ou_prob - _implied(ou_odds)) * 100
                if edge > 0:
                    label = f"F5 {'Over' if pick_over else 'Under'} {f5_total_line}"
                    picks.append({
                        "type": "F5 O/U", "pick": label,
                        "prob": round(ou_prob, 4),
                        "edge": round(edge, 1), "odds": ou_odds,
                    })

    # ── F5 Run Line (typically ±0.5) ──
    rl = f5.get("run_line") or {}
    home_f5_rl_odds = odds.get("f5_home_spread_odds")
    home_f5_rl_point = odds.get("f5_home_spread_point")
    away_f5_rl_odds = odds.get("f5_away_spread_odds")
    away_f5_rl_point = odds.get("f5_away_spread_point")

    def _f5_rl_prob(point: float, side: str) -> float | None:
        """Return model prob for covering `point` on `side` (home/away)."""
        if point is None:
            return None
        # DK typically offers ±0.5; support ±1.5 too by falling back to the
        # closest magnitude we modeled.
        if abs(point) == 0.5:
            if side == "home":
                return rl.get("home_minus_0_5") if point < 0 else rl.get("home_plus_0_5")
            else:
                return rl.get("away_minus_0_5") if point < 0 else rl.get("away_plus_0_5")
        return None

    if home_f5_rl_odds and home_f5_rl_odds >= JUICE_WALL and home_f5_rl_point is not None:
        is_dog = home_f5_rl_point > 0
        allowed = (is_dog and MLB_ALLOW_F5_RL_UNDERDOG) or \
                  ((not is_dog) and MLB_ALLOW_F5_RL_FAVORITE)
        prob = _f5_rl_prob(home_f5_rl_point, "home")
        if prob is not None and allowed:
            edge = (prob - _implied(home_f5_rl_odds)) * 100
            if edge > 0:
                sign = "+" if home_f5_rl_point > 0 else ""
                picks.append({
                    "type": "F5 RL",
                    "pick": f"{h_abbr} {sign}{home_f5_rl_point}",
                    "prob": round(prob, 4),
                    "edge": round(edge, 1), "odds": home_f5_rl_odds,
                })

    if away_f5_rl_odds and away_f5_rl_odds >= JUICE_WALL and away_f5_rl_point is not None:
        is_dog = away_f5_rl_point > 0
        allowed = (is_dog and MLB_ALLOW_F5_RL_UNDERDOG) or \
                  ((not is_dog) and MLB_ALLOW_F5_RL_FAVORITE)
        prob = _f5_rl_prob(away_f5_rl_point, "away")
        if prob is not None and allowed:
            edge = (prob - _implied(away_f5_rl_odds)) * 100
            if edge > 0:
                sign = "+" if away_f5_rl_point > 0 else ""
                picks.append({
                    "type": "F5 RL",
                    "pick": f"{a_abbr} {sign}{away_f5_rl_point}",
                    "prob": round(prob, 4),
                    "edge": round(edge, 1), "odds": away_f5_rl_odds,
                })


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
