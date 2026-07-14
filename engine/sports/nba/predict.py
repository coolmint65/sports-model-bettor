"""
NBA Full-Game Prediction Engine (Phase 2k).

Pace-adjusted efficiency model for full-game ML / spread / total picks.
Mirrors the Q1 module's factor stack but stripped of Q1-specific
weights and calibrated against full-game outcomes from the 3-season
backfill (n=4130 games, 2023-10 → 2026-04).

Calibrated constants (full-game, n=4130):
    Avg total       227.89   (Q1 was 58.8)
    Home edge       +2.14    (Q1 was +0.69)
    Margin std dev  16.06    (Q1 was 8.63)
    Total std dev   21.25
    Per-team PPG    113.95

Factors mirrored from Q1:
    1. Base expected points from off/def ratings × opponent
    2. Pace adjustment (matchup possessions / league)
    3. Home-court boost
    4. Rest / B2B penalty
    5. Recent form (last 10 full games, 70/30 vs season)
    6. Team quality (net rating diff)
    7. Roster availability (gated, off by default — same as Q1)
    8. Market spread anchor (blend toward posted line)

Q1-specific factors removed:
    - fast_start_pct quality lookup (Q1-only stat)
    - HOME_Q1_BOOST → HOME_BOOST (recalibrated for 4 quarters)
    - Q1_STD_DEV → MARGIN_STD_DEV (full-game variance is wider)

Usage:
    from engine.sports.nba.predict import predict_full, predict_full_matchup
    pred = predict_full('LAL', 'BOS', spread=-2.5, total=224.5)
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# ── Calibrated constants (n=4130, 2023-10 → 2026-04) ──

HOME_BOOST = 2.14            # Home teams outscore by ~2.14 over 48 min
B2B_PENALTY = -2.5           # Back-to-back tax (literature consensus 2–3 pts)
MARGIN_STD_DEV = 16.06       # Sigma on home–away margin
TOTAL_STD_DEV = 21.25        # Sigma on home+away total
LEAGUE_AVG_TOTAL = 227.89    # Both teams combined
LEAGUE_AVG_PPG = 113.95      # Per team
LEAGUE_AVG_PACE = 99.0
LEAGUE_AVG_OFF_RTG = 114.0
LEAGUE_AVG_DEF_RTG = 114.0
RECENT_WEIGHT = 0.70
SEASON_WEIGHT = 0.30

# Playoff shrinkage — same direction as Q1 (tighter defense, shorter
# rotations). Empirically a touch larger over 48 min than Q1.
PLAYOFF_PACE_FACTOR = 0.96
PLAYOFF_SCORING_FACTOR = 0.96


# ── Math helpers ──

def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _is_nba_playoffs(today: datetime | None = None) -> bool:
    """Same window as Q1 module — mid-April through mid-June."""
    today = today or datetime.now()
    if today.month == 4:
        return today.day >= 15
    if today.month == 5:
        return True
    if today.month == 6:
        return today.day <= 25
    return False


# ── Data loading ──

def _compute_team_full_ppg(team_id: int, season: int) -> dict:
    """Compute full-game team PPG and opp PPG from finalized games.

    The Q1 stats table pre-aggregates Q1-specific values; full-game
    averages aren't pre-computed, so we derive them here from
    nba_games on the fly. Caches on the function for the duration of
    the process.
    """
    if not hasattr(_compute_team_full_ppg, "_cache"):
        _compute_team_full_ppg._cache = {}
    cache_key = (team_id, season)
    if cache_key in _compute_team_full_ppg._cache:
        return _compute_team_full_ppg._cache[cache_key]

    from .db import get_conn
    conn = get_conn()
    rows = conn.execute(
        "SELECT home_team_id, away_team_id, home_score, away_score "
        "FROM nba_games "
        "WHERE season = ? AND status = 'final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "  AND (home_team_id = ? OR away_team_id = ?)",
        (season, team_id, team_id),
    ).fetchall()

    if not rows:
        out = {"games": 0, "ppg": LEAGUE_AVG_PPG, "opp_ppg": LEAGUE_AVG_PPG,
               "margin": 0.0, "home_ppg": None, "home_opp_ppg": None,
               "away_ppg": None, "away_opp_ppg": None}
        _compute_team_full_ppg._cache[cache_key] = out
        return out

    scored, allowed, h_scored, h_allowed, a_scored, a_allowed = [], [], [], [], [], []
    for r in rows:
        if r["home_team_id"] == team_id:
            scored.append(r["home_score"])
            allowed.append(r["away_score"])
            h_scored.append(r["home_score"])
            h_allowed.append(r["away_score"])
        else:
            scored.append(r["away_score"])
            allowed.append(r["home_score"])
            a_scored.append(r["away_score"])
            a_allowed.append(r["home_score"])

    out = {
        "games": len(scored),
        "ppg": round(sum(scored) / len(scored), 2),
        "opp_ppg": round(sum(allowed) / len(allowed), 2),
        "margin": round(sum(scored) / len(scored) - sum(allowed) / len(allowed), 2),
        "home_ppg": round(sum(h_scored) / len(h_scored), 2) if h_scored else None,
        "home_opp_ppg": round(sum(h_allowed) / len(h_allowed), 2) if h_allowed else None,
        "away_ppg": round(sum(a_scored) / len(a_scored), 2) if a_scored else None,
        "away_opp_ppg": round(sum(a_allowed) / len(a_allowed), 2) if a_allowed else None,
    }
    _compute_team_full_ppg._cache[cache_key] = out
    return out


def _get_team_data(abbr: str, season: int | None = None) -> dict:
    """Load team info + full-game stats. Mirrors the Q1 helper but
    swaps Q1-specific fields for full-game equivalents derived from
    the games table.
    """
    from .db import get_nba_team_by_abbr, get_team_q1_stats

    if season is None:
        now = datetime.now()
        season = now.year if now.month >= 9 else now.year - 1

    team = get_nba_team_by_abbr(abbr)
    if not team:
        if not hasattr(_get_team_data, "_warned"):
            _get_team_data._warned = set()
        if abbr not in _get_team_data._warned:
            _get_team_data._warned.add(abbr)
            logger.warning("Team not found: %s (using defaults)", abbr)
        return {
            "abbreviation": abbr, "season": season, "team_id": 0,
            "name": abbr, "city": "", "conference": "", "division": "",
            "games": 0,
            "ppg": LEAGUE_AVG_PPG, "opp_ppg": LEAGUE_AVG_PPG,
            "margin": 0.0,
            "home_ppg": None, "home_opp_ppg": None,
            "away_ppg": None, "away_opp_ppg": None,
            "pace": LEAGUE_AVG_PACE,
            "off_rating": LEAGUE_AVG_OFF_RTG,
            "def_rating": LEAGUE_AVG_DEF_RTG,
        }

    full = _compute_team_full_ppg(team["id"], season)
    # Pace + off/def ratings live on the q1 stats table (they're not
    # actually Q1-specific values — they're the team's full-season pace
    # and efficiency ratings; the table is just historically named).
    q1 = get_team_q1_stats(team["id"], season) or {}

    return {
        "team_id": team["id"],
        "abbreviation": team["abbreviation"],
        "name": team["name"],
        "city": team.get("city", ""),
        "conference": team.get("conference", ""),
        "division": team.get("division", ""),
        "season": season,
        "games": full["games"],
        "ppg": full["ppg"],
        "opp_ppg": full["opp_ppg"],
        "margin": full["margin"],
        "home_ppg": full["home_ppg"],
        "home_opp_ppg": full["home_opp_ppg"],
        "away_ppg": full["away_ppg"],
        "away_opp_ppg": full["away_opp_ppg"],
        "pace": q1.get("pace") or LEAGUE_AVG_PACE,
        "off_rating": q1.get("off_rating") or LEAGUE_AVG_OFF_RTG,
        "def_rating": q1.get("def_rating") or LEAGUE_AVG_DEF_RTG,
    }


def _get_recent_full_form(team_id: int, n: int = 10) -> dict:
    """Last-N full-game scoring form. Mirrors _get_recent_q1_form
    but reads home_score/away_score instead of home_q1/away_q1.
    """
    from .db import get_recent_nba_games

    games = get_recent_nba_games(team_id, n)
    if not games:
        return {"recent_scored": None, "recent_allowed": None,
                "recent_margin": None, "recent_games": 0}

    scored, allowed = [], []
    for g in games:
        if g.get("home_score") is None or g.get("away_score") is None:
            continue
        is_home = g["home_team_id"] == team_id
        if is_home:
            scored.append(g["home_score"])
            allowed.append(g["away_score"])
        else:
            scored.append(g["away_score"])
            allowed.append(g["home_score"])

    if not scored:
        return {"recent_scored": None, "recent_allowed": None,
                "recent_margin": None, "recent_games": 0}

    avg_scored = sum(scored) / len(scored)
    avg_allowed = sum(allowed) / len(allowed)
    return {
        "recent_scored": round(avg_scored, 2),
        "recent_allowed": round(avg_allowed, 2),
        "recent_margin": round(avg_scored - avg_allowed, 2),
        "recent_games": len(scored),
    }


def _check_back_to_back(team_abbr: str) -> bool:
    """Reuse the Q1 module's B2B check — it's not Q1-specific, just
    a yesterday-game lookup against ESPN scoreboard."""
    from .q1_predict import _check_back_to_back as _q1_b2b
    return _q1_b2b(team_abbr)


# ── Core prediction ──

def predict_full(home_abbr: str, away_abbr: str,
                 spread: float | None = None,
                 total: float | None = None,
                 season: int | None = None,
                 backtest: bool = False) -> dict:
    """Predict full-game NBA outcome (ML / spread / total).

    Args:
        home_abbr: 'LAL' etc.
        away_abbr: 'BOS' etc.
        spread:    Posted spread for home (negative = home favored)
        total:     Posted O/U total
        season:    Season start year (default: current)
        backtest:  Skip live HTTP fetches (B2B, series context)

    Returns:
        Dict with home_expected, away_expected, predicted_margin,
        predicted_total, ml_home/away, spread_cover_prob, over_prob,
        margin_probs, total_probs, factors, reasoning, etc.
    """
    if season is None:
        now = datetime.now()
        season = now.year if now.month >= 9 else now.year - 1

    home = _get_team_data(home_abbr, season)
    away = _get_team_data(away_abbr, season)
    reasoning = []

    # Step 1: Base expected points (opponent-adjusted attack vs defense).
    # Use venue splits when available, fall back to overall averages.
    home_off = home.get("home_ppg") or home["ppg"]
    home_def = home.get("home_opp_ppg") or home["opp_ppg"]
    away_off = away.get("away_ppg") or away["ppg"]
    away_def = away.get("away_opp_ppg") or away["opp_ppg"]

    from ...config import get_flag as _get_flag
    league_total = float(_get_flag("LEAGUE_AVG_NBA_TOTAL",
                                    LEAGUE_AVG_TOTAL, sport="nba"))
    league_team_avg = league_total / 2.0  # ~113.95

    if league_team_avg > 0:
        home_expected = (home_off * away_def) / league_team_avg
        away_expected = (away_off * home_def) / league_team_avg
    else:
        home_expected = home_off
        away_expected = away_off

    # Step 2: Pace adjustment.
    home_pace = home.get("pace", LEAGUE_AVG_PACE)
    away_pace = away.get("pace", LEAGUE_AVG_PACE)
    matchup_pace = (home_pace + away_pace) / 2.0
    pace_factor = matchup_pace / LEAGUE_AVG_PACE

    # Capture the team-relative pace BEFORE the playoff multiplier so
    # the "fast/slow tempo" reasoning describes the matchup itself, not
    # the league-wide playoff slowdown.
    raw_pace_factor = pace_factor
    in_playoffs = _is_nba_playoffs()
    if in_playoffs:
        pace_factor *= PLAYOFF_PACE_FACTOR
        reasoning.append(
            "Playoff intensity slows the pace, fewer scoring opportunities"
        )

    home_expected *= pace_factor
    away_expected *= pace_factor

    if in_playoffs:
        home_expected *= PLAYOFF_SCORING_FACTOR
        away_expected *= PLAYOFF_SCORING_FACTOR

    if raw_pace_factor > 1.03:
        reasoning.append("Both teams play at a fast tempo, expect more scoring")
    elif raw_pace_factor < 0.97:
        reasoning.append("Slow-paced matchup should keep scoring down")

    # Step 2.5: Playoff series context (skipped in backtest).
    series: dict = {}
    if in_playoffs and not backtest:
        try:
            from ...series_context import infer_series, apply_series_adjustments
            series = infer_series("nba", home_abbr, away_abbr)
            if series.get("in_series"):
                home_edge_adj = HOME_BOOST
                home_expected, away_expected, home_edge_adj, series_reasons = (
                    apply_series_adjustments(
                        "nba", home_expected, away_expected,
                        home_edge_adj, series))
                edge_delta = home_edge_adj - HOME_BOOST
                if abs(edge_delta) > 0.001:
                    home_expected += edge_delta / 2
                    away_expected -= edge_delta / 2
                reasoning.extend(series_reasons)
        except Exception as e:
            logger.warning("NBA full series context error: %s", e)

    # Step 3: Home-court boost.
    home_boost = float(_get_flag("NBA_HOME_BOOST", HOME_BOOST, sport="nba"))
    home_expected += home_boost / 2
    away_expected -= home_boost / 2
    reasoning.append(f"{home_abbr} has home court advantage")

    # Step 4: Rest / B2B.
    home_rest_adj = 0.0
    away_rest_adj = 0.0
    home_b2b = False if backtest else _check_back_to_back(home_abbr)
    away_b2b = False if backtest else _check_back_to_back(away_abbr)
    b2b_pen = float(_get_flag("NBA_B2B_PENALTY", B2B_PENALTY, sport="nba"))
    if home_b2b:
        home_rest_adj = b2b_pen
        home_expected += b2b_pen
        reasoning.append(f"{home_abbr} on a back-to-back")
    if away_b2b:
        away_rest_adj = b2b_pen
        away_expected += b2b_pen
        reasoning.append(f"{away_abbr} on a back-to-back")

    # Step 5: Recent form (70/30 weighted, dampened 0.5x).
    home_recent = _get_recent_full_form(home.get("team_id", 0), 10)
    away_recent = _get_recent_full_form(away.get("team_id", 0), 10)

    if (home_recent["recent_scored"] is not None
            and home_recent["recent_games"] >= 5):
        blended = (home_recent["recent_scored"] * RECENT_WEIGHT
                   + home_off * SEASON_WEIGHT)
        adj = blended - home_off
        home_expected += adj * 0.5
        if home_recent["recent_margin"] > 4:
            reasoning.append(
                f"{home_abbr} hot recently "
                f"(+{home_recent['recent_margin']:.1f} avg margin L{home_recent['recent_games']})")
        elif home_recent["recent_margin"] < -4:
            reasoning.append(
                f"{home_abbr} struggling lately "
                f"({home_recent['recent_margin']:+.1f} avg margin L{home_recent['recent_games']})")

    if (away_recent["recent_scored"] is not None
            and away_recent["recent_games"] >= 5):
        blended = (away_recent["recent_scored"] * RECENT_WEIGHT
                   + away_off * SEASON_WEIGHT)
        adj = blended - away_off
        away_expected += adj * 0.5

    # Step 6: Team quality (net rating diff, capped).
    home_off_rtg = home.get("off_rating", LEAGUE_AVG_OFF_RTG)
    home_def_rtg = home.get("def_rating", LEAGUE_AVG_DEF_RTG)
    away_off_rtg = away.get("off_rating", LEAGUE_AVG_OFF_RTG)
    away_def_rtg = away.get("def_rating", LEAGUE_AVG_DEF_RTG)
    home_net = (home_off_rtg - home_def_rtg) - (away_off_rtg - away_def_rtg)
    # 10 net rating pts ≈ 4 full-game pts (Q1 was ~1; full game is 4×).
    net_adj = max(-6.0, min(6.0, home_net / 2.5))
    home_expected += net_adj / 2
    away_expected -= net_adj / 2

    # Step 6.5: Roster availability (gated, off by default — same as Q1).
    home_roster_adj = {"delta": 0.0, "starters_out": 0,
                       "load_management": False, "out_players": []}
    away_roster_adj = {"delta": 0.0, "starters_out": 0,
                       "load_management": False, "out_players": []}
    try:
        from ...config import NBA_ENABLE_ROSTER_ADJUSTMENT as _ROSTER_ON
    except Exception:
        _ROSTER_ON = False
    if _ROSTER_ON and not backtest:
        try:
            from .injuries import compute_q1_adjustment
            # Scale Q1 deltas to full game (~4× since Q1 is ~1/4 of game,
            # but starters typically rest some Q4 minutes too — ~3.5×).
            FULL_GAME_SCALE = 3.5
            def _scale_player_impacts(players):
                """Augment each out_player with a `full_impact` field
                (q1_impact × FULL_GAME_SCALE) so the full-game detail
                panel can show the player's full-game points-lost
                instead of mislabelling Q1 numbers as full-game."""
                out = []
                for p in (players or []):
                    p2 = dict(p)
                    q1_imp = p.get("q1_impact")
                    if isinstance(q1_imp, (int, float)):
                        p2["full_impact"] = round(q1_imp * FULL_GAME_SCALE, 2)
                    out.append(p2)
                return out
            if home.get("team_id"):
                q1_h = compute_q1_adjustment(home["team_id"], season)
                home_roster_adj = {
                    "delta": round((q1_h.get("q1_delta") or 0) * FULL_GAME_SCALE, 2),
                    "starters_out": q1_h.get("starters_out", 0),
                    "load_management": q1_h.get("load_management", False),
                    "out_players": _scale_player_impacts(q1_h.get("out_players")),
                }
            if away.get("team_id"):
                q1_a = compute_q1_adjustment(away["team_id"], season)
                away_roster_adj = {
                    "delta": round((q1_a.get("q1_delta") or 0) * FULL_GAME_SCALE, 2),
                    "starters_out": q1_a.get("starters_out", 0),
                    "load_management": q1_a.get("load_management", False),
                    "out_players": _scale_player_impacts(q1_a.get("out_players")),
                }
            # Stack cap: keep combined delta from running away in
            # heavy-rest spots (Q1 used -4; scale to -14 for full game).
            COMBINED_CAP = -14.0
            combined = home_roster_adj["delta"] + away_roster_adj["delta"]
            if combined < COMBINED_CAP:
                scale = COMBINED_CAP / combined
                home_roster_adj["delta"] = round(home_roster_adj["delta"] * scale, 2)
                away_roster_adj["delta"] = round(away_roster_adj["delta"] * scale, 2)
            if home_roster_adj["delta"] < 0:
                home_expected += home_roster_adj["delta"]
                reasoning.append(
                    f"{home_abbr} roster: {home_roster_adj['delta']:+.1f} pts "
                    f"({home_roster_adj['starters_out']} starter(s) out)")
            if away_roster_adj["delta"] < 0:
                away_expected += away_roster_adj["delta"]
                reasoning.append(
                    f"{away_abbr} roster: {away_roster_adj['delta']:+.1f} pts "
                    f"({away_roster_adj['starters_out']} starter(s) out)")
        except Exception as e:
            logger.warning("Full-game roster adjustment failed: %s", e)

    # Step 7: Market spread anchor — blend model margin toward posted line.
    # Same convention as Q1: spread < 0 means home favored.
    if spread is not None:
        model_margin = home_expected - away_expected
        market_margin = -spread
        ANCHOR_WEIGHT = 0.30
        anchored = (model_margin * (1 - ANCHOR_WEIGHT)
                    + market_margin * ANCHOR_WEIGHT)
        delta = anchored - model_margin
        if abs(delta) > 0.5:
            home_expected += delta / 2
            away_expected -= delta / 2
            reasoning.append(f"Market consensus factored in (spread {spread:+.1f})")

    # Clamp to realistic full-game range.
    home_expected = max(80.0, min(150.0, home_expected))
    away_expected = max(80.0, min(150.0, away_expected))

    predicted_margin = home_expected - away_expected
    predicted_total = home_expected + away_expected

    # ML probability via normal CDF on margin.
    # Operator override (flag) wins; otherwise read the data-fit value
    # from the calibration table, falling back to the historical default.
    from ...mc_constants import nba_full_margin_std, nba_full_total_std
    margin_sigma = float(_get_flag("NBA_MARGIN_STD", nba_full_margin_std(), sport="nba"))
    ml_home = _norm_cdf(predicted_margin / margin_sigma)
    ml_away = 1 - ml_home

    # Spread cover probability.
    spread_cover_prob = None
    if spread is not None:
        z = (-spread - predicted_margin) / margin_sigma
        spread_cover_prob = 1 - _norm_cdf(z)

    # Over/under probability.
    total_sigma = float(_get_flag("NBA_TOTAL_STD", nba_full_total_std(), sport="nba"))
    over_prob = None
    if total is not None:
        z = (total - predicted_total) / total_sigma
        over_prob = 1 - _norm_cdf(z)

    # Distributions for alt-line shopping.
    margin_probs: dict[int, float] = {}
    for m in range(-50, 51):
        z_hi = (m + 0.5 - predicted_margin) / margin_sigma
        z_lo = (m - 0.5 - predicted_margin) / margin_sigma
        margin_probs[m] = _norm_cdf(z_hi) - _norm_cdf(z_lo)

    total_probs: dict[int, float] = {}
    for t in range(150, 301):
        z_hi = (t + 0.5 - predicted_total) / total_sigma
        z_lo = (t - 0.5 - predicted_total) / total_sigma
        total_probs[t] = _norm_cdf(z_hi) - _norm_cdf(z_lo)

    if predicted_margin > 4:
        reasoning.insert(0,
            f"{home_abbr} projected to win by {predicted_margin:.1f} points")
    elif predicted_margin < -4:
        reasoning.insert(0,
            f"{away_abbr} projected to win by {abs(predicted_margin):.1f} points")
    else:
        reasoning.insert(0, "Tight matchup expected")

    return {
        "home_abbr": home_abbr,
        "away_abbr": away_abbr,
        "home_expected": round(home_expected, 1),
        "away_expected": round(away_expected, 1),
        "predicted_margin": round(predicted_margin, 1),
        "predicted_total": round(predicted_total, 1),
        "spread_cover_prob": round(spread_cover_prob, 4) if spread_cover_prob is not None else None,
        "over_prob": round(over_prob, 4) if over_prob is not None else None,
        "ml_home": round(ml_home, 4),
        "ml_away": round(ml_away, 4),
        "margin_probs": margin_probs,
        "total_probs": total_probs,
        "posted_spread": spread,
        "posted_total": total,
        "factors": {
            "home_off": round(home_off, 1),
            "away_off": round(away_off, 1),
            "home_def": round(home_def, 1),
            "away_def": round(away_def, 1),
            "pace_factor": round(pace_factor, 3),
            "matchup_pace": round(matchup_pace, 1),
            "home_court_boost": home_boost,
            "rest_adj": {"home": home_rest_adj, "away": away_rest_adj},
            "home_b2b": home_b2b,
            "away_b2b": away_b2b,
            "recent_form": {
                "home": (f"{home_recent['recent_margin']:+.1f} avg margin L{home_recent['recent_games']}"
                         if home_recent.get("recent_margin") is not None else "N/A"),
                "away": (f"{away_recent['recent_margin']:+.1f} avg margin L{away_recent['recent_games']}"
                         if away_recent.get("recent_margin") is not None else "N/A"),
            },
            "home_off_rtg": round(home_off_rtg, 1),
            "home_def_rtg": round(home_def_rtg, 1),
            "away_off_rtg": round(away_off_rtg, 1),
            "away_def_rtg": round(away_def_rtg, 1),
            "home_games": home.get("games", 0),
            "away_games": away.get("games", 0),
            "home_roster": home_roster_adj,
            "away_roster": away_roster_adj,
        },
        "series_context": series if series.get("in_series") else None,
        "reasoning": reasoning,
    }


__all__ = ["predict_full", "_get_team_data", "_get_recent_full_form"]


# ── CLI for quick smoke-test ──

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if len(sys.argv) < 3:
        print("Usage: python -m engine.nba_predict <HOME> <AWAY> [spread] [total]")
        sys.exit(1)
    home = sys.argv[1].upper()
    away = sys.argv[2].upper()
    sp = float(sys.argv[3]) if len(sys.argv) > 3 else None
    tot = float(sys.argv[4]) if len(sys.argv) > 4 else None
    pred = predict_full(home, away, spread=sp, total=tot)
    print(f"{away} @ {home}")
    print(f"  expected: {pred['away_expected']}-{pred['home_expected']}")
    print(f"  margin: {pred['predicted_margin']:+.1f}")
    print(f"  total:  {pred['predicted_total']:.1f}")
    print(f"  ML home: {pred['ml_home']*100:.1f}%")
    if sp is not None:
        print(f"  spread {sp:+.1f}: home cover {pred['spread_cover_prob']*100:.1f}%")
    if tot is not None:
        print(f"  total {tot}: over {pred['over_prob']*100:.1f}%")
    for r in pred["reasoning"]:
        print(f"  · {r}")
