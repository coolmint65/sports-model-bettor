"""
NBA 1st Quarter Spread Prediction Engine.

Uses pace-adjusted efficiency ratings and a normal distribution model
to predict Q1 scoring, spreads, totals, and moneylines.  Basketball
Q1 scoring is approximately normal (unlike hockey/baseball which are
Poisson), so we use a Gaussian CDF for spread probabilities.

Factors:
    - Pace matchup (possessions per game)
    - Q1-specific offensive/defensive ratings
    - Home court Q1 boost (+0.69 pts, calibrated)
    - Rest / back-to-back penalty (-1.0 pt)
    - Recent Q1 form (last 10 games weighted 70/30 vs season)
    - Team quality / record
    - Conference/style matchups (pace differences)

Usage:
    python -m engine.nba_q1_predict LAL BOS
    python -m engine.nba_q1_predict LAL BOS --spread -2.5
"""

import logging
import math
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────

# Calibrated from 1,291-game analysis (calibration_report.py):
#   Home Q1 margin +0.69 (not +1.5)
#   Q1 total avg 58.8 (not 55.0)
#   Q1 margin std dev 8.63 (not 5.5)
HOME_Q1_BOOST = 0.69         # Home teams outscore by ~0.69 in Q1 (calibrated)
B2B_PENALTY = -1.0           # Back-to-back teams start slower in Q1
Q1_STD_DEV = 8.63            # Standard deviation for Q1 scoring margins (calibrated)
LEAGUE_AVG_Q1_TOTAL = 58.8   # Average Q1 total (calibrated)
LEAGUE_AVG_PACE = 99.0       # League-average possessions per game
LEAGUE_AVG_OFF_RTG = 112.0   # League-average offensive rating (pts per 100 poss)
LEAGUE_AVG_DEF_RTG = 112.0   # League-average defensive rating
RECENT_WEIGHT = 0.70         # Weight for recent form (last 10) vs season
SEASON_WEIGHT = 0.30         # Weight for full-season averages

# NBA playoff adjustments. Historical playoff data (last 5 postseasons)
# shows pace drops ~3 possessions/game and total scoring ~3% relative
# to the regular season. The dominant driver is tighter half-court
# defense and shorter rotations (8 man vs 10). Q1 specifically shrinks
# slightly less than later quarters because starters play the full Q1
# in both contexts, but the pace drop still propagates.
NBA_PLAYOFF_PACE_FACTOR = 0.97    # ~3 fewer possessions / 100 poss
NBA_PLAYOFF_SCORING_FACTOR = 0.97  # symmetric scoring shrinkage

# Default Q1 scoring if no data available (calibrated: 58.8 / 2 = 29.4)
DEFAULT_Q1_PPG = 29.4
DEFAULT_Q1_OPP_PPG = 29.4


# ── Math helpers ───────────────────────────────────────────


def _norm_cdf(x: float) -> float:
    """Standard normal CDF using math.erf (no scipy dependency)."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _implied_prob(american_odds: int) -> float:
    """Convert American odds to implied probability."""
    if american_odds < 0:
        return abs(american_odds) / (abs(american_odds) + 100)
    return 100 / (american_odds + 100)


def _is_nba_playoffs(today: datetime | None = None) -> bool:
    """True during the NBA postseason window (mid-April through mid-June).

    The NBA regular season ends ~April 13; play-in runs April 15-19;
    playoffs proper April 20 - early June; Finals through mid/late June.
    This check returns True for the entire playoff window including the
    play-in (play-in games share the tighter-defense / shorter-rotation
    dynamics that justify the adjustment).
    """
    today = today or datetime.now()
    if today.month == 4:
        return today.day >= 15
    if today.month == 5:
        return True
    if today.month == 6:
        return today.day <= 25
    return False


# ── Data loading helpers ───────────────────────────────────


def _get_team_data(abbr: str, season: int | None = None) -> dict:
    """Load team info + Q1 stats from the NBA DB.

    Returns a merged dict with team info and Q1 stats, using defaults
    where data is unavailable.
    """
    from .nba_db import get_nba_team_by_abbr, get_team_q1_stats

    if season is None:
        now = datetime.now()
        season = now.year if now.month >= 9 else now.year - 1

    team = get_nba_team_by_abbr(abbr)
    if not team:
        # Log once per process per abbr to avoid spam. If no teams are in
        # the DB at all, surface an actionable hint the first time it fires.
        if not hasattr(_get_team_data, "_warned"):
            _get_team_data._warned = set()
            # Check if ANY team is in the DB - if not, user hasn't run sync
            try:
                from .nba_db import get_conn as _nba_conn
                n = _nba_conn().execute("SELECT COUNT(*) FROM nba_teams").fetchone()[0]
                if n == 0:
                    logger.warning(
                        "NBA teams table empty - run 'sync_nba.bat --full' to "
                        "populate team data. Predictions will use league-average "
                        "defaults until then.")
            except Exception:
                pass
        if abbr not in _get_team_data._warned:
            _get_team_data._warned.add(abbr)
            logger.warning("Team not found: %s (using defaults)", abbr)
        return {
            "abbreviation": abbr,
            "season": season,
            "team_id": 0,
            "name": abbr,
            "city": "",
            "conference": "",
            "division": "",
            "games": 0,
            "q1_ppg": DEFAULT_Q1_PPG,
            "q1_opp_ppg": DEFAULT_Q1_OPP_PPG,
            "q1_margin": 0.0,
            "q1_home_ppg": None,
            "q1_home_opp_ppg": None,
            "q1_away_ppg": None,
            "q1_away_opp_ppg": None,
            "q1_cover_pct": None,
            "pace": LEAGUE_AVG_PACE,
            "off_rating": LEAGUE_AVG_OFF_RTG,
            "def_rating": LEAGUE_AVG_DEF_RTG,
            "fast_start_pct": None,
            "slow_start_pct": None,
        }

    q1 = get_team_q1_stats(team["id"], season)

    result = {
        "team_id": team["id"],
        "abbreviation": team["abbreviation"],
        "name": team["name"],
        "city": team.get("city", ""),
        "conference": team.get("conference", ""),
        "division": team.get("division", ""),
        "season": season,
    }

    if q1:
        result.update({
            "games": q1.get("games", 0),
            "q1_ppg": q1.get("q1_ppg") or DEFAULT_Q1_PPG,
            "q1_opp_ppg": q1.get("q1_opp_ppg") or DEFAULT_Q1_OPP_PPG,
            "q1_margin": q1.get("q1_margin") or 0.0,
            "q1_home_ppg": q1.get("q1_home_ppg"),
            "q1_home_opp_ppg": q1.get("q1_home_opp_ppg"),
            "q1_away_ppg": q1.get("q1_away_ppg"),
            "q1_away_opp_ppg": q1.get("q1_away_opp_ppg"),
            "q1_cover_pct": q1.get("q1_cover_pct"),
            "pace": q1.get("pace") or LEAGUE_AVG_PACE,
            "off_rating": q1.get("off_rating") or LEAGUE_AVG_OFF_RTG,
            "def_rating": q1.get("def_rating") or LEAGUE_AVG_DEF_RTG,
            "fast_start_pct": q1.get("fast_start_pct"),
            "slow_start_pct": q1.get("slow_start_pct"),
        })
    else:
        result.update({
            "games": 0,
            "q1_ppg": DEFAULT_Q1_PPG,
            "q1_opp_ppg": DEFAULT_Q1_OPP_PPG,
            "q1_margin": 0.0,
            "q1_home_ppg": None,
            "q1_home_opp_ppg": None,
            "q1_away_ppg": None,
            "q1_away_opp_ppg": None,
            "q1_cover_pct": None,
            "pace": LEAGUE_AVG_PACE,
            "off_rating": LEAGUE_AVG_OFF_RTG,
            "def_rating": LEAGUE_AVG_DEF_RTG,
            "fast_start_pct": None,
            "slow_start_pct": None,
        })

    return result


def _get_recent_q1_form(team_id: int, n: int = 10) -> dict:
    """Compute recent Q1 form from last N games.

    Returns dict with recent Q1 averages for scored, allowed, and margin.
    """
    from .nba_db import get_recent_nba_games

    games = get_recent_nba_games(team_id, n)

    if not games:
        return {"recent_q1_scored": None, "recent_q1_allowed": None,
                "recent_q1_margin": None, "recent_games": 0}

    scored = []
    allowed = []
    for g in games:
        if g.get("home_q1") is None or g.get("away_q1") is None:
            continue
        is_home = g["home_team_id"] == team_id
        if is_home:
            scored.append(g["home_q1"])
            allowed.append(g["away_q1"])
        else:
            scored.append(g["away_q1"])
            allowed.append(g["home_q1"])

    if not scored:
        return {"recent_q1_scored": None, "recent_q1_allowed": None,
                "recent_q1_margin": None, "recent_games": 0}

    avg_scored = sum(scored) / len(scored)
    avg_allowed = sum(allowed) / len(allowed)

    return {
        "recent_q1_scored": round(avg_scored, 2),
        "recent_q1_allowed": round(avg_allowed, 2),
        "recent_q1_margin": round(avg_scored - avg_allowed, 2),
        "recent_games": len(scored),
    }


def _check_back_to_back(team_abbr: str) -> bool:
    """Check if a team played yesterday (back-to-back).

    Uses ESPN scoreboard to check if the team had a game yesterday.
    """
    import json
    import urllib.request
    import urllib.error

    try:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        url = (
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
            f"/scoreboard?dates={yesterday}"
        )
        req = urllib.request.Request(url, headers={
            "User-Agent": "SportsBettor/1.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())

        for event in data.get("events", []):
            for comp in event.get("competitions", []):
                for team_entry in comp.get("competitors", []):
                    t = team_entry.get("team", {})
                    if t.get("abbreviation", "") == team_abbr:
                        return True
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.debug("B2B check failed for %s: %s", team_abbr, e)

    return False


# ── Core prediction ────────────────────────────────────────


def predict_q1(home_abbr: str, away_abbr: str,
               spread: float | None = None,
               total: float | None = None,
               season: int | None = None,
               backtest: bool = False) -> dict:
    """Predict 1st quarter spread using pace-adjusted efficiency.

    Q1 scoring model:
    1. Base expected Q1 points from Q1-specific off/def ratings
    2. Pace adjustment: faster matchups produce more Q1 points
    3. Home court boost (+0.69 pts in Q1, calibrated)
    4. Rest/B2B adjustment (-1.0 pt for back-to-back)
    5. Recent Q1 form (70% recent, 30% season average)
    6. Team quality adjustment (win% proxy)
    7. Spread = home_q1_expected - away_q1_expected
    8. Win probability via normal distribution

    Args:
        home_abbr: Home team abbreviation (e.g. 'LAL')
        away_abbr: Away team abbreviation (e.g. 'BOS')
        spread: Posted Q1 spread for home team (negative = home favored)
        total: Posted Q1 total
        season: Season start year (default: current)

    Returns:
        Prediction dict with expected scores, probabilities, and factors.
    """
    # Normalize season up-front so any downstream call (compute_q1_adjustment,
    # is_likely_resting_spot, etc.) sees a real int, not None. _get_team_data
    # does its own internal normalization, but the outer `season` variable
    # would stay None and silently break SQL lookups keyed on season.
    if season is None:
        now = datetime.now()
        season = now.year if now.month >= 9 else now.year - 1

    home = _get_team_data(home_abbr, season)
    away = _get_team_data(away_abbr, season)

    reasoning = []

    # ── Step 1: Base Q1 expected points ──
    # Use Q1-specific home/away splits when available, else overall Q1 stats
    home_q1_off = home.get("q1_home_ppg") or home["q1_ppg"]
    home_q1_def = home.get("q1_home_opp_ppg") or home["q1_opp_ppg"]
    away_q1_off = away.get("q1_away_ppg") or away["q1_ppg"]
    away_q1_def = away.get("q1_away_opp_ppg") or away["q1_opp_ppg"]

    # Opponent-adjusted: team's Q1 offense vs opponent's Q1 defense
    # home_expected = (home_off * away_def) / league_avg
    # Routed through get_flag so the adaptive baselines system
    # (engine.adaptive_baselines) can write a runtime override when
    # the trailing 30-game window diverges from the long-term baseline
    # — catches regime shifts (pace slowdowns in playoffs, etc.) without
    # a source-code edit. Falls back to the module-level constant when
    # no override exists.
    from .config import get_flag as _get_flag
    league_q1_total = _get_flag("LEAGUE_AVG_Q1_TOTAL",
                                  LEAGUE_AVG_Q1_TOTAL, sport="nba")
    league_q1_avg = float(league_q1_total) / 2  # ~29.4 per team

    if league_q1_avg > 0:
        home_q1_expected = (home_q1_off * away_q1_def) / league_q1_avg
        away_q1_expected = (away_q1_off * home_q1_def) / league_q1_avg
    else:
        home_q1_expected = home_q1_off
        away_q1_expected = away_q1_off

    # ── Step 2: Pace adjustment ──
    home_pace = home.get("pace", LEAGUE_AVG_PACE)
    away_pace = away.get("pace", LEAGUE_AVG_PACE)
    matchup_pace = (home_pace + away_pace) / 2
    pace_factor = matchup_pace / LEAGUE_AVG_PACE

    # Playoff pace shrinkage: tighter defense, shorter rotations, more
    # half-court sets. Applied AFTER the matchup pace_factor so a
    # normally-fast team still reads as relatively fast vs. a slow team
    # in the same round, just at a lower absolute level.
    in_playoffs = _is_nba_playoffs()
    if in_playoffs:
        pace_factor *= NBA_PLAYOFF_PACE_FACTOR
        reasoning.append("Playoff intensity slows the pace, fewer scoring opportunities early")

    home_q1_expected *= pace_factor
    away_q1_expected *= pace_factor

    # Symmetric playoff scoring shrinkage on top of the pace effect --
    # even when possessions stay constant, half-court offense is less
    # efficient under playoff defensive intensity.
    if in_playoffs:
        home_q1_expected *= NBA_PLAYOFF_SCORING_FACTOR
        away_q1_expected *= NBA_PLAYOFF_SCORING_FACTOR

    # ── Playoff series context ──
    series: dict = {}
    series_reasons: list[str] = []
    if in_playoffs and not backtest:
        try:
            from .series_context import infer_series, apply_series_adjustments
            series = infer_series("nba", home_abbr, away_abbr)
            if series.get("in_series"):
                # NBA Q1 adjustments are on expected points, not goals,
                # but the same multiplicative pattern works.
                home_edge_adj = HOME_Q1_BOOST
                home_q1_expected, away_q1_expected, home_edge_adj, series_reasons = (
                    apply_series_adjustments(
                        "nba", home_q1_expected, away_q1_expected,
                        home_edge_adj, series))
                # Apply any home edge change from series context
                edge_delta = home_edge_adj - HOME_Q1_BOOST
                if abs(edge_delta) > 0.001:
                    home_q1_expected += edge_delta / 2
                    away_q1_expected -= edge_delta / 2
                reasoning.extend(series_reasons)
        except Exception as e:
            logger.warning("NBA series context error: %s", e)

    if pace_factor > 1.03:
        reasoning.append("Both teams play at a fast tempo, expect more Q1 scoring")
    elif pace_factor < 0.97:
        reasoning.append("Slow-paced matchup should keep Q1 scoring down")

    # ── Step 3: Home court Q1 boost ──
    home_q1_expected += HOME_Q1_BOOST / 2
    away_q1_expected -= HOME_Q1_BOOST / 2
    reasoning.append(f"{home_abbr} has home court advantage")

    # ── Step 4: Rest / B2B adjustment ──
    home_rest_adj = 0.0
    away_rest_adj = 0.0
    home_b2b = False if backtest else _check_back_to_back(home_abbr)
    away_b2b = False if backtest else _check_back_to_back(away_abbr)

    if home_b2b:
        home_rest_adj = B2B_PENALTY
        home_q1_expected += B2B_PENALTY
        reasoning.append(f"{home_abbr} on a back-to-back, expect slower start")
    if away_b2b:
        away_rest_adj = B2B_PENALTY
        away_q1_expected += B2B_PENALTY
        reasoning.append(f"{away_abbr} on a back-to-back, expect slower start")

    # ── Step 5: Recent Q1 form (70/30 weighting) ──
    home_recent = _get_recent_q1_form(home.get("team_id", 0), 10)
    away_recent = _get_recent_q1_form(away.get("team_id", 0), 10)

    if home_recent["recent_q1_scored"] is not None and home_recent["recent_games"] >= 5:
        recent_off = home_recent["recent_q1_scored"]
        season_off = home_q1_off
        blended_off = recent_off * RECENT_WEIGHT + season_off * SEASON_WEIGHT
        adj = blended_off - season_off
        home_q1_expected += adj * 0.5  # Dampen to avoid overreaction
        if home_recent["recent_q1_margin"] > 2:
            reasoning.append(
                f"{home_abbr} has been dominating Q1s lately "
                f"(+{home_recent['recent_q1_margin']:.1f} avg margin over last {home_recent['recent_games']})"
            )
        elif home_recent["recent_q1_margin"] < -2:
            reasoning.append(
                f"{home_abbr} has been struggling in Q1s recently "
                f"({home_recent['recent_q1_margin']:+.1f} avg margin)"
            )

    if away_recent["recent_q1_scored"] is not None and away_recent["recent_games"] >= 5:
        recent_off = away_recent["recent_q1_scored"]
        season_off = away_q1_off
        blended_off = recent_off * RECENT_WEIGHT + season_off * SEASON_WEIGHT
        adj = blended_off - season_off
        away_q1_expected += adj * 0.5

    # ── Step 6: Team quality adjustment ──
    # Use Q1 fast-start percentage as a proxy for team quality in Q1
    home_fast = home.get("fast_start_pct")
    away_fast = away.get("fast_start_pct")
    if home_fast is not None and away_fast is not None:
        quality_diff = home_fast - away_fast
        # Cap at +-1.5 points from quality difference
        quality_adj = max(-1.5, min(1.5, quality_diff * 3.0))
        home_q1_expected += quality_adj / 2
        away_q1_expected -= quality_adj / 2
        if quality_adj > 0.5:
            reasoning.append(
                f"{home_abbr} wins Q1 more often ({home_fast:.0%}) than {away_abbr} ({away_fast:.0%})"
            )
        elif quality_adj < -0.5:
            reasoning.append(
                f"{away_abbr} is the stronger Q1 team ({away_fast:.0%} Q1 win rate vs {home_fast:.0%})"
            )

    # ── Step 6.5: Roster availability (injuries + load management) ──
    # Subtract Q1 impact of players who are Out/Questionable/Doubtful, and
    # apply a bonus penalty if 3+ starters sit (load-management signal).
    # Also apply a safety-net penalty for likely end-of-season rest spots.
    #
    # GATED behind NBA_ENABLE_ROSTER_ADJUSTMENT (default False). Live
    # data from 2026-04-12 showed heavy-rest games had +0.5 pt mean
    # shift vs baseline - the adjustment was encoding a phantom signal
    # that hurt picks more than it helped. Infrastructure stays intact
    # for informational display but predictions use the base model
    # unless this is explicitly enabled after backtest validation.
    home_roster_adj = {"q1_delta": 0.0, "starters_out": 0, "load_management": False, "out_players": []}
    away_roster_adj = {"q1_delta": 0.0, "starters_out": 0, "load_management": False, "out_players": []}
    from .config import NBA_ENABLE_ROSTER_ADJUSTMENT
    if NBA_ENABLE_ROSTER_ADJUSTMENT and not backtest:
        try:
            from .nba_injuries import compute_q1_adjustment, is_likely_resting_spot
            if home.get("team_id"):
                home_roster_adj = compute_q1_adjustment(home["team_id"], season)
            if away.get("team_id"):
                away_roster_adj = compute_q1_adjustment(away["team_id"], season)

            # Combined-delta cap: based on 2026-04-12 calibration data
            # (~10 heavy-rest games avg Q1 total 59.3 vs baseline 58.8).
            # Mean barely moves; variance widens. Cap combined delta at
            # -4 so the model only claims small edges on roster spots.
            COMBINED_DELTA_CAP = -4.0
            combined_raw = home_roster_adj["q1_delta"] + away_roster_adj["q1_delta"]
            stack_dampened = False
            if combined_raw < COMBINED_DELTA_CAP:
                scale = COMBINED_DELTA_CAP / combined_raw
                home_roster_adj["q1_delta"] = round(home_roster_adj["q1_delta"] * scale, 2)
                away_roster_adj["q1_delta"] = round(away_roster_adj["q1_delta"] * scale, 2)
                home_roster_adj["stack_dampened"] = True
                away_roster_adj["stack_dampened"] = True
                stack_dampened = True

            if home_roster_adj["q1_delta"] < 0:
                home_q1_expected += home_roster_adj["q1_delta"]
                reasoning.append(
                    f"{home_abbr} roster: {home_roster_adj['q1_delta']:+.1f} Q1 pts "
                    f"({home_roster_adj['starters_out']} starter(s) out"
                    + (f", load-mgmt spot)" if home_roster_adj["load_management"] else ")")
                    + (f" [combined cap {COMBINED_DELTA_CAP:+.0f}]" if stack_dampened else "")
                )
            if away_roster_adj["q1_delta"] < 0:
                away_q1_expected += away_roster_adj["q1_delta"]
                reasoning.append(
                    f"{away_abbr} roster: {away_roster_adj['q1_delta']:+.1f} Q1 pts "
                    f"({away_roster_adj['starters_out']} starter(s) out"
                    + (f", load-mgmt spot)" if away_roster_adj["load_management"] else ")")
                    + (f" [combined cap {COMBINED_DELTA_CAP:+.0f}]" if stack_dampened else "")
                )

            # Schedule-based safety net: end-of-regular-season rest risk.
            today = datetime.now().strftime("%Y-%m-%d")
            if home.get("team_id") and not home_roster_adj["load_management"]:
                if is_likely_resting_spot(home["team_id"], today, season):
                    home_q1_expected -= 3.0
                    home_roster_adj["q1_delta"] -= 3.0
                    home_roster_adj["resting_spot"] = True
                    reasoning.append(f"{home_abbr} at end-of-regular-season: -3.0 Q1 pts (rest risk)")
            if away.get("team_id") and not away_roster_adj["load_management"]:
                if is_likely_resting_spot(away["team_id"], today, season):
                    away_q1_expected -= 3.0
                    away_roster_adj["q1_delta"] -= 3.0
                    away_roster_adj["resting_spot"] = True
                    reasoning.append(f"{away_abbr} at end-of-regular-season: -3.0 Q1 pts (rest risk)")
        except Exception as e:
            logger.warning("Roster adjustment failed (%s/%s): %s",
                           home_abbr, away_abbr, e, exc_info=True)

    # ── Step 7: Efficiency rating adjustment ──
    # Teams with better off/def ratings perform better across all quarters
    home_off_rtg = home.get("off_rating", LEAGUE_AVG_OFF_RTG)
    home_def_rtg = home.get("def_rating", LEAGUE_AVG_DEF_RTG)
    away_off_rtg = away.get("off_rating", LEAGUE_AVG_OFF_RTG)
    away_def_rtg = away.get("def_rating", LEAGUE_AVG_DEF_RTG)

    # Net rating difference (scaled to Q1 impact)
    home_net = (home_off_rtg - home_def_rtg) - (away_off_rtg - away_def_rtg)
    # Every 10 points of net rating difference ~ 1 Q1 point
    net_adj = home_net / 10.0
    net_adj = max(-2.0, min(2.0, net_adj))  # Cap at +-2 points

    home_q1_expected += net_adj / 2
    away_q1_expected -= net_adj / 2

    # ── Step 8: Market spread anchor ──
    # The Q1 model's margin can be tighter than what the market expects.
    # The posted Q1 spread incorporates information (injuries, rotations,
    # matchup history) that the model may not capture. Blend the model's
    # predicted margin toward the posted spread to reduce the chance of
    # systematically over-valuing underdogs.
    #
    # Convention: spread < 0 means home favored (e.g. -3.5 = home -3.5).
    # Our predicted_margin > 0 means home leads. So the market-implied
    # margin is -spread (e.g. spread=-3.5 → implied margin = +3.5 for home).
    if spread is not None:
        model_margin = home_q1_expected - away_q1_expected
        market_margin = -spread  # posted spread is home's perspective
        SPREAD_ANCHOR_WEIGHT = 0.30
        anchored_margin = (model_margin * (1 - SPREAD_ANCHOR_WEIGHT) +
                          market_margin * SPREAD_ANCHOR_WEIGHT)
        delta = anchored_margin - model_margin
        if abs(delta) > 0.2:
            home_q1_expected += delta / 2
            away_q1_expected -= delta / 2
            reasoning.append(
                f"Market consensus factored in (Q1 spread {spread:+.1f})"
            )

    # ── Clamp expected scores to realistic range ──
    home_q1_expected = max(18.0, min(40.0, home_q1_expected))
    away_q1_expected = max(18.0, min(40.0, away_q1_expected))

    # ── Calculate margins and probabilities ──
    predicted_margin = home_q1_expected - away_q1_expected
    predicted_total = home_q1_expected + away_q1_expected

    # Q1 moneyline probability (home wins Q1 outright)
    q1_ml_home = _norm_cdf(predicted_margin / Q1_STD_DEV)
    q1_ml_away = 1 - q1_ml_home

    # Spread cover probability
    # Convention: spread = -2.5 means home favored by 2.5.
    # Home covers when actual_margin + spread > 0, i.e. actual_margin > -spread.
    # P(cover) = P(actual > -spread) = 1 - Phi((-spread - predicted) / sigma)
    spread_cover_prob = None
    if spread is not None:
        z = (-spread - predicted_margin) / Q1_STD_DEV
        spread_cover_prob = 1 - _norm_cdf(z)

    # Over/under probability
    over_prob = None
    total_sigma = Q1_STD_DEV * 0.98  # Calibrated: total var is slightly narrower than margin var
    if total is not None:
        # Calibration showed total std dev 8.46 vs margin std dev 8.63,
        # so total variance is actually slightly narrower (~0.98x).
        z = (total - predicted_total) / total_sigma
        over_prob = 1 - _norm_cdf(z)

    # Full margin / total distributions for alt-line shopping.
    # Matches the MLB/NHL output shape: {line: probability} keyed by the
    # integer bucket midpoint. Basketball Q1 scoring is continuous, so
    # the Gaussian CDF is discretized into 1-point buckets over the
    # realistic range. Callers can compute P(cover > X.5 line) as
    # sum(p for m, p in margin_probs.items() if m > line) — same API
    # the MLB alt-shopper uses against its Poisson matrix.
    margin_probs: dict[int, float] = {}
    for m in range(-30, 31):
        z_hi = (m + 0.5 - predicted_margin) / Q1_STD_DEV
        z_lo = (m - 0.5 - predicted_margin) / Q1_STD_DEV
        margin_probs[m] = _norm_cdf(z_hi) - _norm_cdf(z_lo)

    total_probs: dict[int, float] = {}
    for t in range(30, 91):
        z_hi = (t + 0.5 - predicted_total) / total_sigma
        z_lo = (t - 0.5 - predicted_total) / total_sigma
        total_probs[t] = _norm_cdf(z_hi) - _norm_cdf(z_lo)

    # Add overall reasoning
    if predicted_margin > 2:
        reasoning.insert(0, f"{home_abbr} projected to lead Q1 by {predicted_margin:.1f} points")
    elif predicted_margin < -2:
        reasoning.insert(0, f"{away_abbr} projected to lead Q1 by {abs(predicted_margin):.1f} points")
    else:
        reasoning.insert(0, "Tight Q1 expected, small margins either way")

    return {
        "home_abbr": home_abbr,
        "away_abbr": away_abbr,
        "home_q1_expected": round(home_q1_expected, 1),
        "away_q1_expected": round(away_q1_expected, 1),
        "predicted_margin": round(predicted_margin, 1),
        "predicted_total": round(predicted_total, 1),
        "spread_cover_prob": round(spread_cover_prob, 4) if spread_cover_prob is not None else None,
        "over_prob": round(over_prob, 4) if over_prob is not None else None,
        "q1_ml_home": round(q1_ml_home, 4),
        "q1_ml_away": round(q1_ml_away, 4),
        "margin_probs": margin_probs,
        "total_probs": total_probs,
        "posted_spread": spread,
        "posted_total": total,
        "factors": {
            "home_q1_off": round(home_q1_off, 1),
            "away_q1_off": round(away_q1_off, 1),
            "home_q1_def": round(home_q1_def, 1),
            "away_q1_def": round(away_q1_def, 1),
            "pace_factor": round(pace_factor, 3),
            "matchup_pace": round(matchup_pace, 1),
            "home_court_boost": HOME_Q1_BOOST,
            "rest_adj": {
                "home": home_rest_adj,
                "away": away_rest_adj,
            },
            "home_b2b": home_b2b,
            "away_b2b": away_b2b,
            "recent_form": {
                "home": (f"{home_recent['recent_q1_margin']:+.1f} avg Q1 margin L{home_recent['recent_games']}"
                         if home_recent.get("recent_q1_margin") is not None else "N/A"),
                "away": (f"{away_recent['recent_q1_margin']:+.1f} avg Q1 margin L{away_recent['recent_games']}"
                         if away_recent.get("recent_q1_margin") is not None else "N/A"),
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


# ── Pick generation ────────────────────────────────────────


def q1_spread_probability(predicted_margin: float, spread: float,
                          std_dev: float = Q1_STD_DEV) -> float:
    """Probability that home team covers the Q1 spread.

    Convention: spread = -2.5 means home favored by 2.5 points.
    Home covers when actual_margin + spread > 0, i.e. actual_margin > -spread.

    Args:
        predicted_margin: Model's predicted Q1 margin (positive = home favored)
        spread: Posted spread for home team (negative = home favored)
        std_dev: Standard deviation of Q1 scoring margins (~8.63, calibrated)

    Returns:
        Probability that the home team covers the posted spread.
    """
    z = (-spread - predicted_margin) / std_dev
    return 1 - _norm_cdf(z)


# ── Backward-compatible re-exports ──
# Pick generation has been moved to engine/nba_picks.py to separate
# game prediction from bet selection.
from .nba_picks import generate_q1_picks, generate_q1_picks_with_context  # noqa: F401, E402


def predict_q1_matchup(home_abbr: str, away_abbr: str,
                       odds: dict | None = None,
                       season: int | None = None,
                       pred: dict | None = None) -> dict | None:
    """Predict a Q1 matchup and include pick list for the server endpoint.

    Returns the full prediction dict plus a "picks" key with edge-ranked
    pick candidates. Returns None if prediction fails.

    When `pred` is provided the factor pipeline is skipped; callers that
    pre-compute MC + GBM alongside the factor model pass the augmented
    dict in so ensemble_nba() blends all three signals through
    generate_q1_picks. Without it we fall back to factor-only.
    """
    odds = odds or {}
    q1_spread = odds.get("q1_spread")
    q1_total = odds.get("q1_total")

    if pred is None:
        try:
            pred = predict_q1(home_abbr, away_abbr,
                              spread=q1_spread, total=q1_total, season=season)
        except Exception as e:
            # Elevated from silent swallow to warning so upstream bugs
            # (missing DB data, schema mismatch, etc.) surface in logs
            # instead of silently producing "no prediction available".
            logger.warning("predict_q1 failed for %s/%s: %s",
                           home_abbr, away_abbr, e, exc_info=True)
            return None
    if not pred:
        return None

    picks = generate_q1_picks(home_abbr, away_abbr, odds, season, pred=pred)

    from ._pick_helpers import tag_confidence
    tag_confidence(picks)

    # CI band on the win-probability point estimate (data-quality
    # uncertainty). Derived from team games played so the band narrows
    # as the season matures. Shape matches MLB/NHL so the shared
    # ProbHistogram renders identically.
    try:
        from ._confidence import ci_half_width, parse_record_games
        h_rec = (pred.get("home", {}) or {}).get("record", "")
        a_rec = (pred.get("away", {}) or {}).get("record", "")
        h_games = parse_record_games(h_rec)
        a_games = parse_record_games(a_rec)
        n_eff = min(h_games, a_games) if (h_games and a_games) else 0
        hw = ci_half_width(n_eff)
        pred["confidence_ci"] = {"ci_half_width": hw,
                                 "samples": {"home_games": h_games,
                                             "away_games": a_games}}
        for p in picks:
            prob = p.get("prob")
            if prob is None:
                continue
            p["prob_low"] = round(max(0.0, prob - hw), 4)
            p["prob_high"] = round(min(1.0, prob + hw), 4)
            p["ci_half_width"] = hw
    except Exception as ci_err:
        logger.debug("NBA CI annotation failed: %s", ci_err)

    pred["picks"] = picks
    return pred


# ── CLI entry point ────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = sys.argv[1:]
    if len(args) < 2:
        print("Usage: python -m engine.nba_q1_predict HOME_ABBR AWAY_ABBR [--spread X] [--total X]")
        print("Example: python -m engine.nba_q1_predict LAL BOS --spread -2.5 --total 55.5")
        sys.exit(1)

    home = args[0].upper()
    away = args[1].upper()

    # Parse optional spread/total
    spread_val = None
    total_val = None
    for i, a in enumerate(args):
        if a == "--spread" and i + 1 < len(args):
            spread_val = float(args[i + 1])
        if a == "--total" and i + 1 < len(args):
            total_val = float(args[i + 1])

    print(f"\n{'='*60}")
    print(f"  NBA Q1 Prediction: {away} @ {home}")
    print(f"{'='*60}")

    pred = predict_q1(home, away, spread=spread_val, total=total_val)

    print(f"\n  Home ({home}) Q1 Expected: {pred['home_q1_expected']}")
    print(f"  Away ({away}) Q1 Expected: {pred['away_q1_expected']}")
    print(f"  Predicted Margin: {pred['predicted_margin']:+.1f} ({home})")
    print(f"  Predicted Total: {pred['predicted_total']}")
    print(f"  Q1 ML Home: {pred['q1_ml_home']:.1%}")
    print(f"  Q1 ML Away: {pred['q1_ml_away']:.1%}")

    if pred.get("spread_cover_prob") is not None:
        print(f"  Spread Cover ({spread_val:+.1f}): {pred['spread_cover_prob']:.1%}")
    if pred.get("over_prob") is not None:
        print(f"  Over {total_val}: {pred['over_prob']:.1%}")

    print(f"\n  Factors:")
    factors = pred["factors"]
    print(f"    Pace factor: {factors['pace_factor']}")
    print(f"    Home court boost: +{factors['home_court_boost']} pts")
    print(f"    Home B2B: {factors['home_b2b']}")
    print(f"    Away B2B: {factors['away_b2b']}")
    print(f"    Recent form (home): {factors['recent_form']['home']}")
    print(f"    Recent form (away): {factors['recent_form']['away']}")
    print(f"    Off/Def ratings: {home} {factors['home_off_rtg']}/{factors['home_def_rtg']} | "
          f"{away} {factors['away_off_rtg']}/{factors['away_def_rtg']}")

    print(f"\n  Reasoning:")
    for r in pred["reasoning"]:
        print(f"    - {r}")

    # Generate picks if odds are provided
    if spread_val is not None or total_val is not None:
        odds_dict = {}
        if spread_val is not None:
            odds_dict["q1_spread"] = spread_val
            odds_dict["q1_spread_home_odds"] = -110
            odds_dict["q1_spread_away_odds"] = -110
        if total_val is not None:
            odds_dict["q1_total"] = total_val
            odds_dict["q1_over_odds"] = -110
            odds_dict["q1_under_odds"] = -110

        picks = generate_q1_picks(home, away, odds_dict)
        if picks:
            print(f"\n  Picks with edge:")
            for p in picks:
                print(f"    {p['type']:12s} | {p['pick']:20s} | "
                      f"{p['prob']:.1%} | edge: {p['edge']:+.1f}% | odds: {p['odds']}")
        else:
            print(f"\n  No picks with positive edge found.")

    print(f"{'='*60}\n")
