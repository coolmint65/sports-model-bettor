"""
NHL Prediction Engine.

Uses team stats from ESPN JSON files + Poisson distribution to model
expected goals and derive win probabilities, puck line, totals, and
period breakdowns.

Enhanced with special teams (PP/PK), goaltending (save%), shot volume,
faceoff dominance, form, and home/away splits.
"""

import math
import logging
from datetime import datetime, timedelta
from .data import load_team, list_teams, get_league_averages

logger = logging.getLogger(__name__)

LEAGUE = "NHL"
HOME_EDGE = 0.15  # ~0.15 goal home-ice advantage
MAX_GOALS = 10

# ── NHL division & conference mappings (for Factor 7) ──
_NHL_DIVISIONS = {
    # Atlantic
    "BOS": ("Atlantic", "Eastern"), "BUF": ("Atlantic", "Eastern"),
    "DET": ("Atlantic", "Eastern"), "FLA": ("Atlantic", "Eastern"),
    "MTL": ("Atlantic", "Eastern"), "OTT": ("Atlantic", "Eastern"),
    "TBL": ("Atlantic", "Eastern"), "TOR": ("Atlantic", "Eastern"),
    # Metropolitan
    "CAR": ("Metropolitan", "Eastern"), "CBJ": ("Metropolitan", "Eastern"),
    "NJD": ("Metropolitan", "Eastern"), "NYI": ("Metropolitan", "Eastern"),
    "NYR": ("Metropolitan", "Eastern"), "PHI": ("Metropolitan", "Eastern"),
    "PIT": ("Metropolitan", "Eastern"), "WSH": ("Metropolitan", "Eastern"),
    # Central
    "CHI": ("Central", "Western"), "COL": ("Central", "Western"),
    "DAL": ("Central", "Western"), "MIN": ("Central", "Western"),
    "NSH": ("Central", "Western"), "STL": ("Central", "Western"),
    "UTA": ("Central", "Western"), "WPG": ("Central", "Western"),
    # Pacific
    "ANA": ("Pacific", "Western"), "CGY": ("Pacific", "Western"),
    "EDM": ("Pacific", "Western"), "LAK": ("Pacific", "Western"),
    "SJS": ("Pacific", "Western"), "SEA": ("Pacific", "Western"),
    "VAN": ("Pacific", "Western"), "VGK": ("Pacific", "Western"),
}

# Empty net goal probability constant (Factor 9)
EN_GOAL_PROB = 0.08  # ~8% of all NHL goals are empty netters


def poisson(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def _score_matrix(home_xg: float, away_xg: float) -> list[list[float]]:
    """Build probability matrix[home][away] via independent Poisson."""
    matrix = []
    for h in range(MAX_GOALS + 1):
        row = []
        for a in range(MAX_GOALS + 1):
            row.append(poisson(home_xg, h) * poisson(away_xg, a))
        matrix.append(row)
    return matrix


def _expected_goals(off: float, opp_def: float, league_avg: float) -> float:
    """Attack * defense / league_avg formula."""
    if league_avg <= 0:
        return off
    return (off * opp_def) / league_avg


def _form_factor(team: dict) -> float:
    """Recent form adjustment (-0.12 to +0.12)."""
    sos = team.get("strength_of_schedule", {})
    recent = sos.get("recent_games", 0)
    if recent < 3:
        return 0.0
    wr = sos.get("recent_wins", 0) / recent
    margin = sos.get("avg_margin", 0)
    win_adj = (wr - 0.5) * 0.16
    margin_adj = max(-0.04, min(0.04, margin * 0.004))
    return max(-0.12, min(0.12, win_adj + margin_adj))


def _split_adj(team: dict, is_home: bool) -> float:
    """Home/away split multiplier."""
    splits = team.get("home_away_splits", {})
    if not splits:
        return 1.0
    key = "home_ppg" if is_home else "away_ppg"
    split_ppg = splits.get(key, 0)
    if split_ppg <= 0:
        return 1.0
    overall = team.get("stats", {}).get("goals_for_avg", 0)
    if overall <= 0:
        return 1.0
    return max(0.88, min(1.12, split_ppg / overall))


def _compute_recent_form_from_standings(team: dict) -> float:
    """
    Additional recent-form xG adjustment based on L10 record.

    Uses strength_of_schedule.recent_wins / recent_games as a proxy
    for L10 record.  Hot streak (7+ wins in 10) boosts xG up to 5%;
    cold streak (3- wins in 10) reduces up to 5%.  Stacks with the
    existing _form_factor().
    """
    sos = team.get("strength_of_schedule", {})
    recent_games = sos.get("recent_games", 0)
    recent_wins = sos.get("recent_wins", 0)
    if recent_games < 5:
        return 0.0
    # Normalise to a 10-game window
    win_rate = recent_wins / recent_games
    if win_rate >= 0.7:          # 7-3 or better
        # Scale linearly: 0.7 -> 0%, 1.0 -> 5%
        return min(0.05, (win_rate - 0.7) / 0.3 * 0.05)
    elif win_rate <= 0.3:        # 3-7 or worse
        # Scale linearly: 0.3 -> 0%, 0.0 -> -5%
        return max(-0.05, -(0.3 - win_rate) / 0.3 * 0.05)
    return 0.0


def _is_playoff_window() -> bool:
    """NHL playoffs typically start mid-April."""
    now = datetime.now()
    # Regular season ends ~April 17, playoffs run through June
    return now.month >= 4 and now.day >= 10


def _is_late_season() -> bool:
    """Last 2 weeks of regular season -- teams fighting for spots or resting."""
    now = datetime.now()
    return now.month == 4 and now.day < 17


# ── Factor 8: Blowout tendency ──────────────────────────────
def _compute_blowout_tendency(team_abbr: str) -> dict:
    """What percentage of a team's wins/losses are by 2+ goals?

    Uses the DB if available. Returns dict with cover rates for -1.5 and +1.5.
    League average is roughly 35% for -1.5 (wins by 2+).
    """
    result = {"wins_by_2plus_pct": 0.0, "losses_by_2plus_pct": 0.0, "games": 0}
    try:
        from .nhl_db import get_nhl_team_by_abbr, get_conn
        team = get_nhl_team_by_abbr(team_abbr)
        if not team:
            return result
        team_id = team["id"]
        conn = get_conn()
        games = conn.execute("""
            SELECT home_score, away_score, home_team_id FROM nhl_games
            WHERE (home_team_id = ? OR away_team_id = ?) AND status = 'final'
            ORDER BY date DESC LIMIT 30
        """, (team_id, team_id)).fetchall()
        if not games:
            return result

        wins = 0
        wins_by_2 = 0
        losses = 0
        losses_by_2 = 0
        for g in games:
            h_score, a_score, h_tid = g[0] or 0, g[1] or 0, g[2]
            is_home = (h_tid == team_id)
            team_score = h_score if is_home else a_score
            opp_score = a_score if is_home else h_score
            if team_score > opp_score:
                wins += 1
                if team_score - opp_score >= 2:
                    wins_by_2 += 1
            elif opp_score > team_score:
                losses += 1
                if opp_score - team_score >= 2:
                    losses_by_2 += 1

        result["games"] = len(games)
        if wins > 0:
            result["wins_by_2plus_pct"] = round(wins_by_2 / wins, 3)
        if losses > 0:
            result["losses_by_2plus_pct"] = round(losses_by_2 / losses, 3)
    except Exception as e:
        logger.debug("Blowout tendency unavailable: %s", e)
    return result


# ── Factor 10: Overtime tendencies ──────────────────────────
def _compute_ot_tendency(team_abbr: str) -> dict:
    """Compute OT win tendency from standings data.

    Uses otLosses from the live standings. A team with many OT losses relative
    to total games is worse in OT. Returns a multiplier for the OT split.
    League average OTL rate is ~6% of games.
    """
    result = {"ot_win_rate": 0.50, "otl_rate": 0.0, "ot_split_adj": 0.52}
    try:
        _ensure_standings_loaded()
        if not _standings_raw_cache:
            return result
        for entry in _standings_raw_cache.get("standings", []):
            abbr_obj = entry.get("teamAbbrev", {})
            abbr = abbr_obj.get("default", "") if isinstance(abbr_obj, dict) else str(abbr_obj)
            if abbr != team_abbr:
                continue

            wins = entry.get("wins", 0)
            losses = entry.get("losses", 0)
            otl = entry.get("otLosses", 0)
            ot_wins = entry.get("otWins", 0)  # Not always present
            gp = wins + losses + otl
            if gp == 0:
                return result

            result["otl_rate"] = round(otl / gp, 3)

            # OT games = OT wins + OT losses
            # If otWins is not available, estimate from shootout data
            if ot_wins > 0:
                ot_games = ot_wins + otl
                if ot_games > 0:
                    result["ot_win_rate"] = round(ot_wins / ot_games, 3)
            else:
                # Estimate: league average OTL rate is ~6%. If a team is above
                # that, they're bad in OT.
                league_avg_otl_rate = 0.06
                if result["otl_rate"] > league_avg_otl_rate * 1.5:
                    result["ot_win_rate"] = 0.40  # Bad in OT
                elif result["otl_rate"] < league_avg_otl_rate * 0.5:
                    result["ot_win_rate"] = 0.60  # Good in OT
                else:
                    result["ot_win_rate"] = 0.50

            # Convert OT win rate to the p_draw split multiplier
            # Default is 0.52 for home, we adjust based on OT prowess
            result["ot_split_adj"] = round(result["ot_win_rate"], 3)
            break
    except Exception as e:
        logger.debug("OT tendency unavailable: %s", e)
    return result


# ── Factor 12: Corsi/Fenwick proxy ──────────────────────────
def _compute_corsi_proxy(team_abbr: str) -> dict:
    """Compute Corsi proxy from DB game data (shots + blocks).

    True Corsi = shots on goal + blocked shots + missed shots.
    We only have shots and blocks, so Corsi proxy = shots + blocks.
    Returns the team's Corsi-for % (CF%) — share of shot attempts.
    """
    result = {"corsi_for_pct": 0.50, "games": 0, "source": "none"}
    try:
        # First check if nhl_team_stats has corsi_pct populated
        from .nhl_db import get_nhl_team_by_abbr, get_conn
        team = get_nhl_team_by_abbr(team_abbr)
        if not team:
            return result
        team_id = team["id"]
        conn = get_conn()

        now = datetime.now()
        season = now.year if now.month >= 9 else now.year - 1
        season_int = int(f"{season}{season + 1}")

        row = conn.execute(
            "SELECT corsi_pct, fenwick_pct FROM nhl_team_stats WHERE team_id = ? AND season = ?",
            (team_id, season_int)
        ).fetchone()
        if row and row[0] is not None:
            result["corsi_for_pct"] = round(row[0], 3)
            result["source"] = "team_stats"
            return result

        # Derive from recent game data: shots + blocks
        games = conn.execute("""
            SELECT home_shots, away_shots, home_blocks, away_blocks, home_team_id
            FROM nhl_games
            WHERE (home_team_id = ? OR away_team_id = ?) AND status = 'final'
            ORDER BY date DESC LIMIT 20
        """, (team_id, team_id)).fetchall()
        if not games:
            return result

        total_cf = 0
        total_ca = 0
        for g in games:
            h_shots = g[0] or 0
            a_shots = g[1] or 0
            h_blocks = g[2] or 0
            a_blocks = g[3] or 0
            h_tid = g[4]
            is_home = (h_tid == team_id)
            # Corsi proxy: team shots + opponent blocked (those were our attempts)
            if is_home:
                cf = h_shots + a_blocks  # our shots + shots they blocked
                ca = a_shots + h_blocks  # their shots + shots we blocked
            else:
                cf = a_shots + h_blocks
                ca = h_shots + a_blocks
            total_cf += cf
            total_ca += ca

        total = total_cf + total_ca
        if total > 0:
            result["corsi_for_pct"] = round(total_cf / total, 3)
            result["games"] = len(games)
            result["source"] = "derived"
    except Exception as e:
        logger.debug("Corsi proxy unavailable: %s", e)
    return result


# ── Unified NHL standings cache ──────────────────────────────
# All three data sources (live stats, records, playoff context) come from
# the same NHL API endpoint.  _ensure_standings_loaded() fetches it once
# and populates every cache from a single HTTP request.

_standings_raw_cache: dict | None = None
_standings_raw_time: float = 0

# Cached playoff context per team
_playoff_context_cache: dict | None = None
_playoff_context_time: float = 0


def _fetch_standings_api() -> dict | None:
    """Fetch raw standings JSON from the NHL API (single HTTP call)."""
    try:
        import json
        import urllib.error
        import urllib.request
        req = urllib.request.Request(
            "https://api-web.nhle.com/v1/standings/now",
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.debug("NHL standings API fetch failed: %s", e)
        return None


def _ensure_standings_loaded() -> None:
    """Fetch standings once and populate all caches (stats, records, playoff context)."""
    import time as _time
    global _standings_raw_cache, _standings_raw_time
    global _live_stats_cache, _live_stats_time
    global _live_standings_cache, _live_standings_time
    global _playoff_context_cache, _playoff_context_time

    # All caches share the same 30-min TTL; check one representative timestamp
    if _standings_raw_cache and (_time.time() - _standings_raw_time) < 1800:
        return  # Already fresh

    data = _fetch_standings_api()
    if not data:
        return

    _standings_raw_cache = data
    _standings_raw_time = _time.time()

    stats: dict = {}
    standings: dict = {}
    context: dict = {}

    for entry in data.get("standings", []):
        team_abbr_obj = entry.get("teamAbbrev", {})
        abbr = team_abbr_obj.get("default", "") if isinstance(team_abbr_obj, dict) else str(team_abbr_obj)

        # ── Records (for _get_live_record) ──
        wins = entry.get("wins", 0)
        losses = entry.get("losses", 0)
        otl = entry.get("otLosses", 0)
        standings[abbr] = f"{wins}-{losses}-{otl}"

        # ── Team stats (for _get_live_team_stats) ──
        gp = wins + losses + otl
        gf = entry.get("goalFor", 0)
        ga = entry.get("goalAgainst", 0)

        if gp > 0:
            team_stats = {
                "goals_for_avg": round(gf / gp, 2),
                "goals_against_avg": round(ga / gp, 2),
            }

            home_wins = entry.get("homeWins", 0)
            home_losses = entry.get("homeLosses", 0)
            home_otl = entry.get("homeOtLosses", 0)
            home_gp = home_wins + home_losses + home_otl
            road_wins = entry.get("roadWins", 0)
            road_losses = entry.get("roadLosses", 0)
            road_otl = entry.get("roadOtLosses", 0)
            road_gp = road_wins + road_losses + road_otl

            if home_gp > 0:
                home_pts_pct = (home_wins * 2 + home_otl) / (home_gp * 2)
                team_stats["home_pts_pct"] = round(home_pts_pct, 3)
                home_gf = entry.get("homeGoalsFor", 0)
                home_ga = entry.get("homeGoalsAgainst", 0)
                if home_gf > 0:
                    team_stats["home_gf_avg"] = round(home_gf / home_gp, 2)
                    team_stats["home_ga_avg"] = round(home_ga / home_gp, 2)

            if road_gp > 0:
                road_pts_pct = (road_wins * 2 + road_otl) / (road_gp * 2)
                team_stats["road_pts_pct"] = round(road_pts_pct, 3)
                road_gf = entry.get("roadGoalsFor", 0)
                road_ga = entry.get("roadGoalsAgainst", 0)
                if road_gf > 0:
                    team_stats["road_gf_avg"] = round(road_gf / road_gp, 2)
                    team_stats["road_ga_avg"] = round(road_ga / road_gp, 2)

            l10_gf = entry.get("l10GoalsFor", 0)
            l10_ga = entry.get("l10GoalsAgainst", 0)
            l10_gp_val = entry.get("l10GamesPlayed", 0)
            if l10_gp_val > 0:
                team_stats["l10_gf_avg"] = round(l10_gf / l10_gp_val, 2)
                team_stats["l10_ga_avg"] = round(l10_ga / l10_gp_val, 2)

            stats[abbr] = team_stats

        # ── Playoff context (for _get_team_playoff_context) ──
        clinch = entry.get("clinchIndicator", "")
        wildcard = entry.get("wildcardSequence", 99)
        pts_pct = entry.get("pointPctg", 0.5)
        l10w = entry.get("l10Wins", 0)
        l10l = entry.get("l10Losses", 0)
        l10o = entry.get("l10OtLosses", 0)
        l10_gp_ctx = entry.get("l10GamesPlayed", 10)
        l10_pts = l10w * 2 + l10o
        l10_pts_pct = l10_pts / (l10_gp_ctx * 2) if l10_gp_ctx > 0 else 0.5

        clinched = bool(clinch)
        eliminated = pts_pct < 0.40 and not clinched
        fighting = not clinched and not eliminated and wildcard <= 4

        if eliminated:
            motivation = 0.2
        elif clinched and wildcard == 0:
            motivation = 0.6
        elif clinched:
            motivation = 0.75
        elif fighting:
            motivation = 1.0
        else:
            motivation = 0.9

        if l10_pts_pct > 0.7:
            motivation = min(1.0, motivation + 0.1)
        elif l10_pts_pct < 0.3:
            motivation = max(0.2, motivation - 0.1)

        context[abbr] = {
            "clinched": clinched,
            "eliminated": eliminated,
            "fighting": fighting,
            "clinch_indicator": clinch,
            "wildcard_seq": wildcard,
            "points_pace": round(pts_pct, 3),
            "l10_record": f"{l10w}-{l10l}-{l10o}",
            "l10_pts_pct": round(l10_pts_pct, 3),
            "motivation": round(motivation, 2),
        }

    now = _time.time()
    _live_stats_cache = stats
    _live_stats_time = now
    _live_standings_cache = standings
    _live_standings_time = now
    _playoff_context_cache = context
    _playoff_context_time = now


def _get_team_playoff_context(abbr: str) -> dict:
    """Get detailed playoff context for a team from live standings.

    Returns:
        {
            "clinched": bool,        # Has clinched a playoff spot
            "eliminated": bool,      # Mathematically eliminated
            "fighting": bool,        # In the hunt but not clinched
            "clinch_indicator": "x", # x=clinched, y=division, z=conference, p=presidents
            "wildcard_seq": int,     # 0=division spot, 1-2=wildcard, 3+=out
            "points_pace": float,    # Points percentage
            "l10_record": str,       # e.g. "7-2-1"
            "l10_pts_pct": float,    # Recent performance
            "motivation": float,     # 0.0 (nothing to play for) to 1.0 (desperate)
        }
    """
    _ensure_standings_loaded()
    return (_playoff_context_cache or {}).get(abbr, {})


def _get_season_context_for_matchup(home_abbr: str, away_abbr: str) -> dict:
    """Build rich season context for a specific matchup."""
    home_ctx = _get_team_playoff_context(home_abbr)
    away_ctx = _get_team_playoff_context(away_abbr)

    if _is_playoff_window():
        phase = "playoffs"
    elif _is_late_season():
        phase = "late_regular"
    else:
        phase = "regular"

    # Build implications string
    implications = []
    if home_ctx.get("clinched") and away_ctx.get("clinched"):
        implications.append("Both teams clinched")
    elif home_ctx.get("eliminated") and away_ctx.get("eliminated"):
        implications.append("Both teams eliminated")
    else:
        if home_ctx.get("fighting"):
            implications.append(f"{home_abbr} fighting for playoff spot")
        elif home_ctx.get("eliminated"):
            implications.append(f"{home_abbr} eliminated")
        elif home_ctx.get("clinched"):
            ci = home_ctx.get("clinch_indicator", "")
            label = {"x": "clinched", "y": "clinched division", "z": "clinched conference", "p": "Presidents' Trophy"}.get(ci, "clinched")
            implications.append(f"{home_abbr} {label}")

        if away_ctx.get("fighting"):
            implications.append(f"{away_abbr} fighting for playoff spot")
        elif away_ctx.get("eliminated"):
            implications.append(f"{away_abbr} eliminated")
        elif away_ctx.get("clinched"):
            ci = away_ctx.get("clinch_indicator", "")
            label = {"x": "clinched", "y": "clinched division", "z": "clinched conference", "p": "Presidents' Trophy"}.get(ci, "clinched")
            implications.append(f"{away_abbr} {label}")

    return {
        "phase": phase,
        "implications": " | ".join(implications) if implications else None,
        "home": home_ctx,
        "away": away_ctx,
    }


_live_standings_cache: dict | None = None
_live_standings_time: float = 0
_live_stats_cache: dict | None = None
_live_stats_time: float = 0
_club_stats_cache: dict = {}   # Per-team club stats (PP%, PK%, SV%, shots, faceoff)
_club_stats_time: float = 0
_b2b_cache: dict = {}
_b2b_cache_time: float = 0


# NHL franchise team IDs used by the club-stats endpoint
_NHL_TEAM_IDS_FOR_STATS = {
    "ANA": 24, "BOS": 6, "BUF": 7, "CGY": 20, "CAR": 12,
    "CHI": 16, "COL": 21, "CBJ": 29, "DAL": 25, "DET": 17,
    "EDM": 22, "FLA": 13, "LAK": 26, "MIN": 30, "MTL": 8,
    "NSH": 18, "NJD": 1, "NYI": 2, "NYR": 3, "OTT": 9,
    "PHI": 4, "PIT": 5, "SJS": 28, "SEA": 55, "STL": 19,
    "TBL": 14, "TOR": 10, "UTA": 59, "VAN": 23, "VGK": 54,
    "WPG": 52, "WSH": 15,
}


def _fetch_club_stats_for_team(abbr: str) -> dict | None:
    """Fetch live club stats (PP%, PK%, SV%, shots, faceoff) for a team
    from the NHL club-stats endpoint. Returns stats dict or None.
    """
    import json
    import urllib.error
    import urllib.request

    try:
        url = f"https://api-web.nhle.com/v1/club-stats/{abbr}/now"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.debug("club-stats fetch failed for %s: %s", abbr, e)
        return None

    stats = {}

    # Aggregate skater stats to get team PP/PK-adjacent metrics
    skaters = data.get("skaters", []) or []
    total_shots = sum(s.get("shots", 0) or 0 for s in skaters)
    games_played = max(
        (s.get("gamesPlayed", 0) or 0 for s in skaters),
        default=0,
    )

    if games_played > 0:
        stats["shots_per_game"] = round(total_shots / games_played, 2)

    # Aggregate goalie stats for team save%
    goalies = data.get("goalies", []) or []
    total_saves = 0
    total_shots_against = 0
    for g in goalies:
        saves = g.get("saves", 0) or 0
        shots_against = g.get("shotsAgainst", 0) or 0
        total_saves += saves
        total_shots_against += shots_against

    if total_shots_against > 0:
        stats["save_pct"] = round(total_saves / total_shots_against, 4)
        # Derive shots against per game
        if games_played > 0:
            stats["shots_against_per_game"] = round(total_shots_against / games_played, 2)

    return stats if stats else None


def _fetch_team_summary_stats() -> dict:
    """Fetch team-wide PP%, PK%, faceoff% from the NHL team-stats endpoint.

    Returns {abbr: {pp_pct, pk_pct, faceoff_pct}}
    """
    import json
    import urllib.error
    import urllib.parse
    import urllib.request

    # Derive current season dynamically instead of hardcoding 20252026.
    from datetime import datetime as _dt
    _now = _dt.now()
    _start = _now.year if _now.month >= 9 else _now.year - 1
    _season_id = f"{_start}{_start + 1}"
    try:
        # Python 3.14 rejects unencoded spaces in URLs — must encode the query
        query = urllib.parse.urlencode({
            "cayenneExp": f"seasonId={_season_id} and gameTypeId=2"
        })
        url = f"https://api.nhle.com/stats/rest/en/team/summary?{query}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.warning("team-stats fetch failed (season %s): %s", _season_id, e)
        return {}

    if not data or not data.get("data"):
        logger.warning("team-stats returned empty for season %s (URL: %s)", _season_id, url)
        return {}

    # Map NHL API team names to our abbreviations
    _NAME_TO_ABBR = {
        "Anaheim Ducks": "ANA", "Boston Bruins": "BOS", "Buffalo Sabres": "BUF",
        "Calgary Flames": "CGY", "Carolina Hurricanes": "CAR",
        "Chicago Blackhawks": "CHI", "Colorado Avalanche": "COL",
        "Columbus Blue Jackets": "CBJ", "Dallas Stars": "DAL",
        "Detroit Red Wings": "DET", "Edmonton Oilers": "EDM",
        "Florida Panthers": "FLA", "Los Angeles Kings": "LAK",
        "Minnesota Wild": "MIN", "Montréal Canadiens": "MTL",
        "Montreal Canadiens": "MTL", "Nashville Predators": "NSH",
        "New Jersey Devils": "NJD", "New York Islanders": "NYI",
        "New York Rangers": "NYR", "Ottawa Senators": "OTT",
        "Philadelphia Flyers": "PHI", "Pittsburgh Penguins": "PIT",
        "San Jose Sharks": "SJS", "Seattle Kraken": "SEA",
        "St. Louis Blues": "STL", "Tampa Bay Lightning": "TBL",
        "Toronto Maple Leafs": "TOR", "Utah Hockey Club": "UTA",
        "Utah Mammoth": "UTA", "Vancouver Canucks": "VAN",
        "Vegas Golden Knights": "VGK", "Washington Capitals": "WSH",
        "Winnipeg Jets": "WPG",
    }

    result = {}
    rows = data.get("data", [])
    logger.info("team-stats returned %d rows for season %s", len(rows), _season_id)
    unmapped = []
    for row in rows:
        # NHL stats API uses teamFullName as the primary identifier
        full_name = row.get("teamFullName", "")
        abbr = _NAME_TO_ABBR.get(full_name, "")
        if not abbr:
            # Fallback: try teamAbbrev or first 3 chars of name
            abbr = row.get("teamAbbrev") or (full_name[:3].upper() if full_name else "")
        if not abbr:
            unmapped.append(full_name or "<no name>")
            continue

        stats = {}
        # PP% and PK% from stats.rest are decimals (0.225 for 22.5%), not percentages
        pp = row.get("powerPlayPct")
        if pp is not None:
            # Normalize: if >1, assume it's a percentage (22.5 -> 0.225)
            stats["pp_pct"] = round(pp / 100 if pp > 1 else pp, 4)
        pk = row.get("penaltyKillPct")
        if pk is not None:
            stats["pk_pct"] = round(pk / 100 if pk > 1 else pk, 4)
        fo = row.get("faceoffWinPct")
        if fo is not None:
            stats["faceoff_pct"] = round(fo / 100 if fo > 1 else fo, 4)
        if "shotsForPerGame" in row:
            stats["shots_per_game"] = round(row["shotsForPerGame"], 2)
        if "shotsAgainstPerGame" in row:
            stats["shots_against_per_game"] = round(row["shotsAgainstPerGame"], 2)

        # Derive save%: savePct or 1 - (goalsAgainstPerGame / shotsAgainstPerGame)
        sv = row.get("savePct")
        if sv is not None:
            stats["save_pct"] = round(sv / 100 if sv > 1 else sv, 4)
        elif stats.get("shots_against_per_game") and row.get("goalsAgainstPerGame"):
            ga_pg = row["goalsAgainstPerGame"]
            sa_pg = stats["shots_against_per_game"]
            if sa_pg > 0:
                stats["save_pct"] = round(1 - (ga_pg / sa_pg), 4)

        if stats:
            result[abbr] = stats
    if unmapped:
        logger.warning("team-stats: %d teams had no abbreviation mapping (sample: %s)",
                       len(unmapped), unmapped[:3])
    logger.info("team-stats: populated stats for %d teams", len(result))
    return result


def _ensure_club_stats_loaded() -> None:
    """Fetch live team-wide stats (PP%, PK%, SV%, shots, faceoff) once
    per 30 minutes and merge into _live_stats_cache.
    """
    import time as _time
    global _club_stats_cache, _club_stats_time, _live_stats_cache

    if _club_stats_cache and (_time.time() - _club_stats_time) < 1800:
        return

    # Ensure the base standings cache is loaded first
    _ensure_standings_loaded()

    # Fetch team summary stats (PP%, PK%, faceoff) from stats.rest
    summary = _fetch_team_summary_stats()

    # Merge into _live_stats_cache
    if _live_stats_cache is None:
        _live_stats_cache = {}

    for abbr, team_stats in summary.items():
        existing = _live_stats_cache.get(abbr, {})
        _live_stats_cache[abbr] = {**existing, **team_stats}

    _club_stats_cache = summary or {"_loaded": True}
    _club_stats_time = _time.time()


def _check_back_to_back(team_abbr: str) -> float:
    """Check if team played yesterday or recently. Returns xG multiplier.

    Back-to-back: 0.95 (5% penalty)
    3-in-4 nights: 0.97 (3% penalty)
    Rest advantage (2+ days off): 1.02 (2% bonus)
    """
    import time as _time
    global _b2b_cache, _b2b_cache_time

    # Cache the yesterday + day-before scoreboard for 30 min
    if _b2b_cache and (_time.time() - _b2b_cache_time) < 1800:
        return _b2b_cache.get(team_abbr, 1.0)

    import json
    import urllib.error
    import urllib.request

    try:
        today = datetime.utcnow().date()
        teams_yesterday: set[str] = set()
        teams_day_before: set[str] = set()

        for days_ago, target_set in [(1, teams_yesterday), (2, teams_day_before)]:
            check_date = today - timedelta(days=days_ago)
            espn_date = check_date.strftime("%Y%m%d")
            url = (
                "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl"
                f"/scoreboard?dates={espn_date}"
            )
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())

            for event in data.get("events", []):
                for comp in event.get("competitions", []):
                    for team_entry in comp.get("competitors", []):
                        t = team_entry.get("team", {})
                        abbr = t.get("abbreviation", "")
                        if abbr:
                            target_set.add(abbr)

        # Build multiplier map for all teams
        result: dict[str, float] = {}
        all_abbrs = teams_yesterday | teams_day_before
        for abbr in all_abbrs:
            if abbr in teams_yesterday:
                # Played yesterday = back-to-back
                result[abbr] = 0.95
            elif abbr in teams_day_before:
                # Played 2 days ago but not yesterday -- check 3-in-4
                # If they also played yesterday that's already caught above.
                # 3-in-4 is approximated as: played day-before but not yesterday
                # (a lighter fatigue signal).
                pass  # No penalty -- 1 day rest is normal in NHL

        # 3-in-4: played both yesterday AND day-before-yesterday
        for abbr in teams_yesterday & teams_day_before:
            result[abbr] = 0.95  # Already penalized as b2b, keep same

        # Rest advantage: didn't play in either of the last 2 days
        # We only know about teams that DID play; for any team not in either
        # set, they've had 2+ days off.  We'll store a positive factor.
        # But we can only do this for teams we know about -- live standings
        # gives us all abbreviations.
        try:
            live_stats = _get_live_team_stats()
            for abbr in live_stats:
                if abbr not in teams_yesterday and abbr not in teams_day_before:
                    result[abbr] = 1.02  # 2% rest bonus
        except Exception:
            pass

        _b2b_cache = result
        _b2b_cache_time = _time.time()
        return result.get(team_abbr, 1.0)

    except (urllib.error.URLError, json.JSONDecodeError, KeyError, OSError) as e:
        logger.debug("Back-to-back check failed: %s", e)
        return 1.0


def _get_live_team_stats() -> dict:
    """Get current season per-game stats from NHL API standings (cached 30 min).
    Returns {abbr: {goals_for_avg, goals_against_avg, ...}}
    """
    _ensure_standings_loaded()
    return _live_stats_cache or {}


def _get_live_record(abbr: str) -> str | None:
    """Get current W-L-OTL record from NHL API standings (cached 30 min)."""
    _ensure_standings_loaded()
    return (_live_standings_cache or {}).get(abbr)


def _record_to_points_pct(record: str) -> float:
    """
    Parse an NHL record string like "45-22-10" into a points percentage.

    Points percentage = (W*2 + OTL) / (GP * 2).
    Returns 0.5 (league-average) if the record can't be parsed.
    """
    if not record:
        return 0.5
    parts = record.split("-")
    if len(parts) < 2:
        return 0.5
    try:
        wins = int(parts[0])
        losses = int(parts[1])
        otl = int(parts[2]) if len(parts) >= 3 else 0
        gp = wins + losses + otl
        if gp == 0:
            return 0.5
        points = wins * 2 + otl
        return points / (gp * 2)
    except (ValueError, IndexError):
        return 0.5


def _build_factors_with_ranks(home, away, hs, as_,
                              h_pp, a_pp, h_pk, a_pk, h_sv, a_sv,
                              h_shots, a_shots, h_fo, a_fo):
    """Build factors dict with league rankings (1st = best)."""
    # Use LIVE stats for rankings, not stale JSON files.
    # Fall back to JSON only if live stats are unavailable.
    _ensure_club_stats_loaded()
    all_stats = []
    if _live_stats_cache:
        for abbr, team_stats in _live_stats_cache.items():
            if isinstance(team_stats, dict):
                all_stats.append(team_stats)

    # Fallback to JSON if live data is empty
    if not all_stats:
        all_teams = list_teams(LEAGUE)
        for t in all_teams:
            team = load_team(LEAGUE, t["key"])
            if team and team.get("stats"):
                all_stats.append(team["stats"])

    def rank_stat(val, key, higher_is_better=True):
        """Return rank out of 32 for a given stat value."""
        if not val or not all_stats:
            return None
        values = [s.get(key, 0) for s in all_stats if s.get(key)]
        if not values:
            return None
        if higher_is_better:
            values.sort(reverse=True)
        else:
            values.sort()
        # Find position
        for i, v in enumerate(values):
            if (higher_is_better and val >= v) or (not higher_is_better and val <= v):
                return i + 1
        return len(values)

    return {
        "home_pp": round(h_pp, 3) if h_pp else None,
        "away_pp": round(a_pp, 3) if a_pp else None,
        "home_pp_rank": rank_stat(h_pp, "pp_pct", True),
        "away_pp_rank": rank_stat(a_pp, "pp_pct", True),
        "home_pk": round(h_pk, 3) if h_pk else None,
        "away_pk": round(a_pk, 3) if a_pk else None,
        "home_pk_rank": rank_stat(h_pk, "pk_pct", True),
        "away_pk_rank": rank_stat(a_pk, "pk_pct", True),
        "home_sv": round(h_sv, 3) if h_sv else None,
        "away_sv": round(a_sv, 3) if a_sv else None,
        "home_sv_rank": rank_stat(h_sv, "save_pct", True),
        "away_sv_rank": rank_stat(a_sv, "save_pct", True),
        "home_shots": round(h_shots, 1),
        "away_shots": round(a_shots, 1),
        "home_shots_rank": rank_stat(h_shots, "shots_per_game", True),
        "away_shots_rank": rank_stat(a_shots, "shots_per_game", True),
        "home_fo": round(h_fo, 3),
        "away_fo": round(a_fo, 3),
        "home_fo_rank": rank_stat(h_fo, "faceoff_pct", True),
        "away_fo_rank": rank_stat(a_fo, "faceoff_pct", True),
    }


def predict_matchup(home_key: str, away_key: str,
                    home_goalie_id: int | None = None,
                    away_goalie_id: int | None = None) -> dict | None:
    """
    Run full NHL matchup prediction.

    Args:
        home_key: team JSON file stem (e.g. "bruins")
        away_key: team JSON file stem (e.g. "maple_leafs")
        home_goalie_id: optional starting goalie ID for home team
        away_goalie_id: optional starting goalie ID for away team

    Returns prediction dict or None on failure.
    """
    home = load_team(LEAGUE, home_key)
    away = load_team(LEAGUE, away_key)
    if not home or not away:
        logger.warning("Could not load teams: %s / %s", home_key, away_key)
        return None

    la = get_league_averages(LEAGUE)
    hs = home.get("stats", {})
    as_ = away.get("stats", {})

    # Override stale JSON stats with live NHL API data when available.
    # _ensure_club_stats_loaded() fetches PP%, PK%, faceoff%, shots, save%
    # from the stats.rest team summary endpoint and merges into _live_stats_cache.
    try:
        _ensure_club_stats_loaded()
    except Exception:
        pass
    live_stats = _get_live_team_stats()
    h_abbr_for_stats = home.get("abbreviation", "")
    a_abbr_for_stats = away.get("abbreviation", "")
    if h_abbr_for_stats in live_stats:
        hs = {**hs, **live_stats[h_abbr_for_stats]}
    if a_abbr_for_stats in live_stats:
        as_ = {**as_, **live_stats[a_abbr_for_stats]}

    # Use a local copy so repeated calls don't drift the module constant
    home_edge = HOME_EDGE

    # Try to enrich with DB data if available
    db_home_stats, db_away_stats = None, None
    h2h_data = {}
    home_goalie_info, away_goalie_info = None, None

    try:
        from .nhl_db import (get_nhl_team_by_abbr, get_team_goalies,
                             get_h2h_nhl, get_recent_nhl_games, get_goalie_stats)
        from .nhl_calibration import get_calibrated_home_edge, get_total_adjustment

        h_abbr = home.get("abbreviation", "")
        a_abbr = away.get("abbreviation", "")

        db_home = get_nhl_team_by_abbr(h_abbr)
        db_away = get_nhl_team_by_abbr(a_abbr)

        if db_home and db_away:
            # H2H history
            h2h_data = get_h2h_nhl(db_home["id"], db_away["id"])

            # Goalie matchup
            season = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1
            season_str = f"{season}{season+1}"

            if home_goalie_id:
                home_goalie_info = get_goalie_stats(home_goalie_id, int(season_str))
            else:
                # Get team's best goalie by games played
                goalies = get_team_goalies(db_home["id"], int(season_str))
                if goalies:
                    home_goalie_info = dict(goalies[0])

            if away_goalie_id:
                away_goalie_info = get_goalie_stats(away_goalie_id, int(season_str))
            else:
                goalies = get_team_goalies(db_away["id"], int(season_str))
                if goalies:
                    away_goalie_info = dict(goalies[0])

            # Use calibrated home edge if available
            calibrated_he = get_calibrated_home_edge()
            if calibrated_he:
                home_edge = calibrated_he

    except Exception as e:
        logger.debug("DB enrichment unavailable: %s", e)

    avg_gf = la.get("goals_for_avg", 3.0)
    avg_ga = la.get("goals_against_avg", 3.0)

    # ── Season context + motivation ──
    h_abbr_ctx = home.get("abbreviation", "")
    a_abbr_ctx = away.get("abbreviation", "")
    season_context = _get_season_context_for_matchup(h_abbr_ctx, a_abbr_ctx)

    home_motivation = season_context.get("home", {}).get("motivation", 0.8)
    away_motivation = season_context.get("away", {}).get("motivation", 0.8)

    if _is_late_season() or _is_playoff_window():
        home_edge *= 1.10  # Home ice matters more in high-stakes games

        # Motivation gap: a desperate team vs a resting team
        # If home is fighting (1.0) and away is eliminated (0.2), home gets a boost
        motivation_gap = home_motivation - away_motivation
        # Cap at ±5% xG adjustment from motivation difference
        # Positive = home more motivated, negative = away more motivated

    # Base expected goals
    home_off = hs.get("goals_for_avg", avg_gf)
    home_def = hs.get("goals_against_avg", avg_ga)
    away_off = as_.get("goals_for_avg", avg_gf)
    away_def = as_.get("goals_against_avg", avg_ga)

    home_xg = _expected_goals(home_off, away_def, avg_ga) + home_edge / 2
    away_xg = _expected_goals(away_off, home_def, avg_ga) - home_edge / 2

    # ── Special teams adjustment ──
    league_pp = la.get("pp_pct", 0.20)
    league_pk = la.get("pk_pct", 0.80)

    # In playoffs, refs call fewer penalties — reduce PP impact
    pp_weight = 2.5  # ~3 PP chances per game in regular season
    if _is_playoff_window():
        pp_weight = 1.8  # Fewer PP opportunities in playoffs

    # Home PP vs away PK
    h_pp = hs.get("pp_pct", league_pp)
    a_pk = as_.get("pk_pct", league_pk)
    if h_pp is not None and h_pp > 0 and a_pk is not None and a_pk > 0:
        pp_edge = (h_pp - league_pp) + (league_pk - a_pk)
        home_xg += pp_edge * pp_weight

    # Away PP vs home PK
    a_pp = as_.get("pp_pct", league_pp)
    h_pk = hs.get("pk_pct", league_pk)
    if a_pp is not None and a_pp > 0 and h_pk is not None and h_pk > 0:
        pp_edge = (a_pp - league_pp) + (league_pk - h_pk)
        away_xg += pp_edge * pp_weight

    # ── Goaltending / save% adjustment ──
    league_sv = la.get("save_pct", 0.905)
    h_sv = hs.get("save_pct", league_sv)
    a_sv = as_.get("save_pct", league_sv)
    # Better save% suppresses opponent's expected goals
    if h_sv is not None and h_sv > 0 and league_sv > 0:
        away_xg *= max(0.85, min(1.15, league_sv / h_sv))
    if a_sv is not None and a_sv > 0 and league_sv > 0:
        home_xg *= max(0.85, min(1.15, league_sv / a_sv))

    # ── Shot volume adjustment ──
    league_shots = la.get("shots_per_game", 30.0)
    h_shots = hs.get("shots_per_game", league_shots)
    a_shots = as_.get("shots_per_game", league_shots)
    h_shots_against = hs.get("shots_against_per_game", league_shots)
    a_shots_against = as_.get("shots_against_per_game", league_shots)

    if league_shots > 0:
        # More shots = more goals; combine with opponent allowing shots
        h_shot_factor = ((h_shots / league_shots) + (a_shots_against / league_shots)) / 2
        a_shot_factor = ((a_shots / league_shots) + (h_shots_against / league_shots)) / 2
        home_xg *= max(0.90, min(1.10, h_shot_factor))
        away_xg *= max(0.90, min(1.10, a_shot_factor))

    # ── Faceoff adjustment ──
    h_fo = hs.get("faceoff_pct", 0.50)
    a_fo = as_.get("faceoff_pct", 0.50)
    fo_diff = (h_fo - a_fo)
    home_xg += fo_diff * 0.3  # Small but real possession edge
    away_xg -= fo_diff * 0.3

    # ── Form + splits ──
    home_form = _form_factor(home)
    away_form = _form_factor(away)
    if _is_late_season() or _is_playoff_window():
        # Weight recent form more heavily in high-stakes games
        home_form *= (0.8 + home_motivation * 0.6)  # 0.8-1.4x based on motivation
        away_form *= (0.8 + away_motivation * 0.6)
    home_xg *= (1 + home_form)
    away_xg *= (1 + away_form)
    home_xg *= _split_adj(home, is_home=True)
    away_xg *= _split_adj(away, is_home=False)

    # ── Recent form (L10) adjustment — stacks with _form_factor ──
    home_xg *= (1 + _compute_recent_form_from_standings(home))
    away_xg *= (1 + _compute_recent_form_from_standings(away))

    # ── Motivation adjustment ──
    # A team fighting for their playoff life vs an eliminated team
    if _is_late_season() or _is_playoff_window():
        motivation_gap = home_motivation - away_motivation
        if abs(motivation_gap) > 0.2:
            home_xg *= (1 + motivation_gap * 0.05)  # max ±5% from motivation
            away_xg *= (1 - motivation_gap * 0.05)

    # ── Win percentage / team quality adjustment ──
    # Try live standings first (NHL API), fall back to JSON record
    home_record = _get_live_record(home.get("abbreviation", "")) or home.get("record", "")
    away_record = _get_live_record(away.get("abbreviation", "")) or away.get("record", "")
    home_pct = _record_to_points_pct(home_record)
    away_pct = _record_to_points_pct(away_record)
    quality_diff = home_pct - away_pct
    # Strengthened from 0.15 (±3%) to 0.50 (±12%). A .650 vs .400 team
    # should shift xG ~12%, not 3%. Previous setting was too weak to
    # correct for teams with genuine quality gaps.
    home_xg *= (1 + quality_diff * 0.50)
    away_xg *= (1 - quality_diff * 0.50)

    # ── Goalie matchup adjustment ──
    # If we have specific goalie data from the DB, use it to override
    # the generic team save% adjustment
    goalie_factor = {"home": None, "away": None}
    if home_goalie_info:
        g_sv = home_goalie_info.get("save_pct") or 0
        g_gaa = home_goalie_info.get("gaa") or 0
        if g_sv > 0 and league_sv > 0:
            # Better goalie suppresses opponent scoring
            goalie_adj = league_sv / g_sv
            away_xg *= max(0.82, min(1.18, goalie_adj))
            goalie_factor["home"] = {
                "name": home_goalie_info.get("name", ""),
                "save_pct": round(g_sv, 3),
                "gaa": round(g_gaa, 2),
            }

    if away_goalie_info:
        g_sv = away_goalie_info.get("save_pct") or 0
        g_gaa = away_goalie_info.get("gaa") or 0
        if g_sv > 0 and league_sv > 0:
            goalie_adj = league_sv / g_sv
            home_xg *= max(0.82, min(1.18, goalie_adj))
            goalie_factor["away"] = {
                "name": away_goalie_info.get("name", ""),
                "save_pct": round(g_sv, 3),
                "gaa": round(g_gaa, 2),
            }

    # ── Extra penalty for backup goalies ──
    # If the starting goalie's SV% is significantly worse than the team's
    # season average SV%, the opponent gets a scoring boost.
    league_sv_ref = la.get("save_pct", 0.905)
    if goalie_factor.get("home") and goalie_factor["home"].get("save_pct"):
        sv_gap = goalie_factor["home"]["save_pct"] - hs.get("save_pct", league_sv_ref)
        if sv_gap < -0.010:  # Backup is >1% worse than team avg
            away_xg *= (1 + abs(sv_gap) * 3)  # Amplify the difference
    if goalie_factor.get("away") and goalie_factor["away"].get("save_pct"):
        sv_gap = goalie_factor["away"]["save_pct"] - as_.get("save_pct", league_sv_ref)
        if sv_gap < -0.010:
            home_xg *= (1 + abs(sv_gap) * 3)

    # ── Live home/away venue split adjustment ──
    # Use the live standings home/away points% to adjust xG for venue.
    # A team much better at home vs road gets a boost when playing at home.
    if h_abbr_for_stats in live_stats:
        h_live = live_stats[h_abbr_for_stats]
        h_home_pct = h_live.get("home_pts_pct")
        h_road_pct = h_live.get("road_pts_pct")
        if h_home_pct and h_road_pct and h_home_pct != h_road_pct:
            # Positive = better at home, negative = better on road
            venue_diff = h_home_pct - h_road_pct
            # Cap at +/- 5% xG adjustment
            home_xg *= max(0.95, min(1.05, 1 + venue_diff * 0.15))

    if a_abbr_for_stats in live_stats:
        a_live = live_stats[a_abbr_for_stats]
        a_home_pct = a_live.get("home_pts_pct")
        a_road_pct = a_live.get("road_pts_pct")
        if a_home_pct and a_road_pct and a_home_pct != a_road_pct:
            venue_diff = a_road_pct - a_home_pct
            # Positive = better on road, negative = worse on road
            away_xg *= max(0.95, min(1.05, 1 + venue_diff * 0.15))

    # ── H2H adjustment ──
    h2h_adj = 0
    if h2h_data and isinstance(h2h_data, list) and len(h2h_data) >= 3:
        # h2h_data is a list of game dicts — compute summary
        h2h_games = len(h2h_data)
        h_abbr_val = home.get("abbreviation", "")
        team1_wins = sum(1 for g in h2h_data
                        if (g.get("home_abbr") == h_abbr_val and (g.get("home_score", 0) or 0) > (g.get("away_score", 0) or 0))
                        or (g.get("away_abbr") == h_abbr_val and (g.get("away_score", 0) or 0) > (g.get("home_score", 0) or 0)))
        h2h_wr = team1_wins / h2h_games
        h2h_adj = (h2h_wr - 0.5) * 0.08
        home_xg *= (1 + h2h_adj)
        away_xg *= (1 - h2h_adj)
        # Convert to summary dict for display
        h2h_data = {"games": h2h_games, "team1_wins": team1_wins, "team2_wins": h2h_games - team1_wins}
    elif h2h_data and isinstance(h2h_data, dict) and h2h_data.get("games", 0) >= 3:
        h2h_wr = h2h_data.get("team1_wins", 0) / h2h_data["games"]
        h2h_adj = (h2h_wr - 0.5) * 0.08
        home_xg *= (1 + h2h_adj)
        away_xg *= (1 - h2h_adj)

    # ── Calibrated total adjustment ──
    try:
        from .nhl_calibration import get_total_adjustment
        total_adj = get_total_adjustment()
        if total_adj:
            home_xg += total_adj / 2
            away_xg += total_adj / 2
    except Exception:
        pass

    # ── Injury adjustment ──
    injury_data = {"home": [], "away": [], "home_impact": 1.0, "away_impact": 1.0}
    try:
        from .injuries import fetch_nhl_injuries, compute_nhl_injury_impact
        nhl_injuries = fetch_nhl_injuries()
        h_abbr = home.get("abbreviation", "")
        a_abbr = away.get("abbreviation", "")

        # Try both original and alternate abbreviations (TBL/TB, NJD/NJ, etc.)
        _INJ_ALT = {
            "TBL": "TB", "TB": "TBL", "NJD": "NJ", "NJ": "NJD",
            "SJS": "SJ", "SJ": "SJS", "LAK": "LA", "LA": "LAK",
            "WSH": "WAS", "WAS": "WSH", "CBJ": "CLB", "CLB": "CBJ",
            "MTL": "MON", "MON": "MTL",
        }
        h_injuries = nhl_injuries.get(h_abbr, []) or nhl_injuries.get(_INJ_ALT.get(h_abbr, ""), [])
        a_injuries = nhl_injuries.get(a_abbr, []) or nhl_injuries.get(_INJ_ALT.get(a_abbr, ""), [])

        if h_injuries:
            h_impact = compute_nhl_injury_impact(h_abbr, h_injuries)
            home_xg *= h_impact
            injury_data["home"] = h_injuries[:5]  # Top 5 for display
            injury_data["home_impact"] = round(h_impact, 4)

        if a_injuries:
            a_impact = compute_nhl_injury_impact(a_abbr, a_injuries)
            away_xg *= a_impact
            injury_data["away"] = a_injuries[:5]
            injury_data["away_impact"] = round(a_impact, 4)
    except Exception as e:
        logger.debug("Injury data unavailable: %s", e)

    # ── Back-to-back / rest adjustment ──
    h_abbr_b2b = home.get("abbreviation", "")
    a_abbr_b2b = away.get("abbreviation", "")
    h_b2b = 1.0
    a_b2b = 1.0
    if h_abbr_b2b:
        h_b2b = _check_back_to_back(h_abbr_b2b)
        home_xg *= h_b2b
        if h_b2b < 1.0:
            # Tired team also concedes more
            away_xg *= (1 + (1 - h_b2b) * 0.5)
    if a_abbr_b2b:
        a_b2b = _check_back_to_back(a_abbr_b2b)
        away_xg *= a_b2b
        if a_b2b < 1.0:
            home_xg *= (1 + (1 - a_b2b) * 0.5)

    # Store rest/fatigue info for display
    rest_data = {
        "home_b2b": h_b2b < 1.0,
        "away_b2b": a_b2b < 1.0,
        "home_rest_advantage": h_b2b > 1.0,
        "away_rest_advantage": a_b2b > 1.0,
        "home_factor": round(h_b2b, 4),
        "away_factor": round(a_b2b, 4),
    }

    # ── Granular factors (Factors 1-12) ──────────────────────────
    # Each factor applies a SMALL adjustment (±1-5% max individually).
    # Factors 1-6 come from the nhl_granular module (imported in try/except).
    # Factors 7-12 are computed directly here.
    # All adjustments COMPOUND with existing adjustments — they do not replace.
    #
    # NOTE: Disabled by default. The tracker shows NHL at 34.1% WR across 47
    # picks since these were added, so we suspect at least one factor is
    # inverted or miscalibrated. Keeping the code intact so we can re-enable
    # factors one at a time once each is individually validated against the
    # pre-granular baseline. Toggle via config.NHL_ENABLE_GRANULAR_FACTORS.
    granular_data = {}
    try:
        from .config import NHL_ENABLE_GRANULAR_FACTORS as _GRANULAR_ON
    except Exception:
        _GRANULAR_ON = False

    # --- Factors 1-6: External granular module ---
    try:
        if not _GRANULAR_ON:
            raise RuntimeError("granular factors disabled in config")
        from .nhl_granular import (
            compute_schedule_fatigue,
            get_recent_travel,
            compute_goalie_workload,
            compute_special_teams_trend,
            compute_penalty_tendency,
            compute_shooting_regression,
        )

        game_date = datetime.now().strftime("%Y-%m-%d")
        h_abbr_g = home.get("abbreviation", "")
        a_abbr_g = away.get("abbreviation", "")

        # Factor 1: Schedule fatigue (stacks with B2B)
        try:
            h_fatigue = compute_schedule_fatigue(h_abbr_g, game_date)
            a_fatigue = compute_schedule_fatigue(a_abbr_g, game_date)
            if isinstance(h_fatigue, dict) and h_fatigue.get("fatigue_score") is not None:
                fatigue_mult = max(0.95, min(1.0, h_fatigue["fatigue_score"]))
                home_xg *= fatigue_mult
                granular_data["home_fatigue"] = h_fatigue
            if isinstance(a_fatigue, dict) and a_fatigue.get("fatigue_score") is not None:
                fatigue_mult = max(0.95, min(1.0, a_fatigue["fatigue_score"]))
                away_xg *= fatigue_mult
                granular_data["away_fatigue"] = a_fatigue
        except Exception as e:
            logger.debug("Factor 1 (schedule fatigue) failed: %s", e)

        # Factor 2: Travel distance
        try:
            h_travel = get_recent_travel(h_abbr_g, game_date)
            a_travel = get_recent_travel(a_abbr_g, game_date)
            if isinstance(h_travel, dict) and h_travel.get("total_km") is not None:
                # Tiny impact: cross-country trip (~5000km) -> ~5% penalty max
                travel_adj = max(0.95, 1.0 - (h_travel["total_km"] / 100000))
                home_xg *= travel_adj
                granular_data["home_travel"] = h_travel
            if isinstance(a_travel, dict) and a_travel.get("total_km") is not None:
                travel_adj = max(0.95, 1.0 - (a_travel["total_km"] / 100000))
                away_xg *= travel_adj
                granular_data["away_travel"] = a_travel
        except Exception as e:
            logger.debug("Factor 2 (travel) failed: %s", e)

        # Factor 3: Goalie workload
        try:
            h_goalie_wl = compute_goalie_workload(h_abbr_g, game_date)
            a_goalie_wl = compute_goalie_workload(a_abbr_g, game_date)
            if isinstance(h_goalie_wl, dict) and h_goalie_wl.get("workload_factor") is not None:
                # Tired goalie concedes more — boost opponent xG
                wl_factor = max(0.97, min(1.03, h_goalie_wl["workload_factor"]))
                away_xg *= wl_factor
                granular_data["home_goalie_workload"] = h_goalie_wl
            if isinstance(a_goalie_wl, dict) and a_goalie_wl.get("workload_factor") is not None:
                wl_factor = max(0.97, min(1.03, a_goalie_wl["workload_factor"]))
                home_xg *= wl_factor
                granular_data["away_goalie_workload"] = a_goalie_wl
        except Exception as e:
            logger.debug("Factor 3 (goalie workload) failed: %s", e)

        # Factor 4: Special teams trend (PP/PK trending up/down)
        try:
            h_st_trend = compute_special_teams_trend(h_abbr_g, game_date)
            a_st_trend = compute_special_teams_trend(a_abbr_g, game_date)
            if isinstance(h_st_trend, dict) and h_st_trend.get("pp_trend_adj") is not None:
                # Half a standard PP adjustment
                home_xg += h_st_trend["pp_trend_adj"] * 0.5
                granular_data["home_pp_trend"] = h_st_trend
            if isinstance(a_st_trend, dict) and a_st_trend.get("pp_trend_adj") is not None:
                away_xg += a_st_trend["pp_trend_adj"] * 0.5
                granular_data["away_pp_trend"] = a_st_trend
        except Exception as e:
            logger.debug("Factor 4 (special teams trend) failed: %s", e)

        # Factor 5: Penalty tendency (cross-referenced with opponent PP)
        try:
            h_pen = compute_penalty_tendency(h_abbr_g, game_date)
            a_pen = compute_penalty_tendency(a_abbr_g, game_date)
            if isinstance(a_pen, dict) and isinstance(h_pen, dict):
                # If away team is undisciplined (high PIMs) and home team has good PP,
                # boost home xG because they'll get more PP opportunities
                a_pim_per_game = a_pen.get("pim_per_game", 0)
                h_pp_pct = hs.get("pp_pct", league_pp)
                if a_pim_per_game >= 10 and h_pp_pct and h_pp_pct > 0.22:
                    # Undisciplined opponent + good PP = more goals
                    pen_boost = min(0.05, (a_pim_per_game - 8) * 0.005 * (h_pp_pct / 0.20))
                    home_xg *= (1 + pen_boost)

                h_pim_per_game = h_pen.get("pim_per_game", 0)
                a_pp_pct = as_.get("pp_pct", league_pp)
                if h_pim_per_game >= 10 and a_pp_pct and a_pp_pct > 0.22:
                    pen_boost = min(0.05, (h_pim_per_game - 8) * 0.005 * (a_pp_pct / 0.20))
                    away_xg *= (1 + pen_boost)

                granular_data["home_penalty_tendency"] = h_pen
                granular_data["away_penalty_tendency"] = a_pen
        except Exception as e:
            logger.debug("Factor 5 (penalty tendency) failed: %s", e)

        # Factor 6: Shooting regression
        try:
            h_shooting_reg = compute_shooting_regression(h_abbr_g, game_date)
            a_shooting_reg = compute_shooting_regression(a_abbr_g, game_date)
            if isinstance(h_shooting_reg, dict) and h_shooting_reg.get("regression_factor") is not None:
                reg_factor = max(0.95, min(1.05, h_shooting_reg["regression_factor"]))
                home_xg *= reg_factor
                granular_data["home_shooting_regression"] = h_shooting_reg
            if isinstance(a_shooting_reg, dict) and a_shooting_reg.get("regression_factor") is not None:
                reg_factor = max(0.95, min(1.05, a_shooting_reg["regression_factor"]))
                away_xg *= reg_factor
                granular_data["away_shooting_regression"] = a_shooting_reg
        except Exception as e:
            logger.debug("Factor 6 (shooting regression) failed: %s", e)

    except Exception as e:
        logger.debug("Granular factors module (1-6) unavailable: %s", e)

    # --- Factor 7: Division/Conference familiarity ---
    try:
        if not _GRANULAR_ON:
            raise RuntimeError("granular factors disabled")
        h_abbr_div = home.get("abbreviation", "")
        a_abbr_div = away.get("abbreviation", "")
        h_div_info = _NHL_DIVISIONS.get(h_abbr_div, ("", ""))
        a_div_info = _NHL_DIVISIONS.get(a_abbr_div, ("", ""))
        h_div, h_conf = h_div_info
        a_div, a_conf = a_div_info

        same_division = bool(h_div and a_div and h_div == a_div)
        same_conference = bool(h_conf and a_conf and h_conf == a_conf)

        # Division rivals play 4x/year — more familiarity means tighter games
        # Cross-conference teams play 2x/year — more variance
        if same_division:
            # Same division: games are tighter, reduce xG spread slightly
            # (the better team has less edge due to familiarity)
            xg_gap = home_xg - away_xg
            if abs(xg_gap) > 0.3:
                home_xg -= xg_gap * 0.02  # Pull 2% toward the mean
                away_xg += xg_gap * 0.02
        elif not same_conference:
            # Cross-conference: less familiarity, slight away disadvantage
            away_xg *= 0.99  # 1% away penalty for unfamiliar arena/style

        granular_data["division_match"] = same_division
        granular_data["conference_match"] = same_conference
        granular_data["home_division"] = h_div
        granular_data["away_division"] = a_div
    except Exception as e:
        logger.debug("Factor 7 (division familiarity) failed: %s", e)

    # --- Factor 8: Blowout tendency ---
    try:
        if not _GRANULAR_ON:
            raise RuntimeError("granular factors disabled")
        h_blowout = _compute_blowout_tendency(home.get("abbreviation", ""))
        a_blowout = _compute_blowout_tendency(away.get("abbreviation", ""))
        granular_data["home_blowout_tendency"] = h_blowout
        granular_data["away_blowout_tendency"] = a_blowout
        # Actual puck line adjustment applied after Poisson matrix below
    except Exception as e:
        logger.debug("Factor 8 (blowout tendency) failed: %s", e)

    # --- Factor 10: OT tendency ---
    h_ot = {"ot_win_rate": 0.50, "ot_split_adj": 0.52}
    a_ot = {"ot_win_rate": 0.50, "ot_split_adj": 0.52}
    try:
        if not _GRANULAR_ON:
            raise RuntimeError("granular factors disabled")
        h_ot = _compute_ot_tendency(home.get("abbreviation", ""))
        a_ot = _compute_ot_tendency(away.get("abbreviation", ""))
        granular_data["home_ot_tendency"] = h_ot
        granular_data["away_ot_tendency"] = a_ot
    except Exception as e:
        logger.debug("Factor 10 (OT tendency) failed: %s", e)

    # --- Factor 12: Corsi/Fenwick proxy ---
    try:
        if not _GRANULAR_ON:
            raise RuntimeError("granular factors disabled")
        h_corsi = _compute_corsi_proxy(home.get("abbreviation", ""))
        a_corsi = _compute_corsi_proxy(away.get("abbreviation", ""))
        # Corsi-for% above 0.52 is good, below 0.48 is bad
        # Apply a small xG adjustment for shot attempt dominance
        h_cf_adj = (h_corsi.get("corsi_for_pct", 0.50) - 0.50) * 0.10
        a_cf_adj = (a_corsi.get("corsi_for_pct", 0.50) - 0.50) * 0.10
        home_xg *= max(0.97, min(1.03, 1 + h_cf_adj))
        away_xg *= max(0.97, min(1.03, 1 + a_cf_adj))
        granular_data["home_corsi"] = h_corsi
        granular_data["away_corsi"] = a_corsi
    except Exception as e:
        logger.debug("Factor 12 (Corsi proxy) failed: %s", e)

    # Floor
    home_xg = max(home_xg, 1.0)
    away_xg = max(away_xg, 1.0)

    # ── O/U-specific adjustments (L10 scoring + goalie impact on totals) ──
    # These produce a separate xG pair used ONLY for the O/U Poisson matrix,
    # so they don't pollute the ML/puck-line probabilities.
    ou_home_xg = home_xg
    ou_away_xg = away_xg

    # Fix 1: Blend season avg with L10 scoring for totals
    # If both teams are scoring more in L10 than season average, total should
    # be pushed higher; if they've gone cold, pushed lower.
    if h_abbr_for_stats in live_stats:
        h_live = live_stats[h_abbr_for_stats]
        l10_gf = h_live.get("l10_gf_avg")
        season_gf = h_live.get("goals_for_avg")
        if l10_gf and season_gf and season_gf > 0:
            # 40% weight on recent form for totals
            ou_home_xg = ou_home_xg * 0.6 + (ou_home_xg * l10_gf / season_gf) * 0.4
        l10_ga = h_live.get("l10_ga_avg")
        season_ga = h_live.get("goals_against_avg")
        if l10_ga and season_ga and season_ga > 0:
            # If home team conceding more recently, away xG goes up for totals
            ou_away_xg = ou_away_xg * 0.6 + (ou_away_xg * l10_ga / season_ga) * 0.4

    if a_abbr_for_stats in live_stats:
        a_live = live_stats[a_abbr_for_stats]
        l10_gf = a_live.get("l10_gf_avg")
        season_gf = a_live.get("goals_for_avg")
        if l10_gf and season_gf and season_gf > 0:
            ou_away_xg = ou_away_xg * 0.6 + (ou_away_xg * l10_gf / season_gf) * 0.4
        l10_ga = a_live.get("l10_ga_avg")
        season_ga = a_live.get("goals_against_avg")
        if l10_ga and season_ga and season_ga > 0:
            ou_home_xg = ou_home_xg * 0.6 + (ou_home_xg * l10_ga / season_ga) * 0.4

    # Fix 2: Goalie impact on totals (separate from ML goalie adjustment)
    # A backup goalie (.890 SV%) vs starter (.920) should push the total up.
    ou_goalie_adj = 0.0
    if goalie_factor.get("home") and goalie_factor["home"].get("save_pct"):
        team_sv = hs.get("save_pct", 0.905)
        goalie_sv = goalie_factor["home"]["save_pct"]
        # Worse goalie = higher total (opponent scores more)
        ou_goalie_adj += (team_sv - goalie_sv) * 15
    if goalie_factor.get("away") and goalie_factor["away"].get("save_pct"):
        team_sv = as_.get("save_pct", 0.905)
        goalie_sv = goalie_factor["away"]["save_pct"]
        ou_goalie_adj += (team_sv - goalie_sv) * 15

    # Apply goalie adj evenly to both sides for totals
    ou_home_xg += ou_goalie_adj / 2
    ou_away_xg += ou_goalie_adj / 2

    # Floor the O/U-specific xGs
    ou_home_xg = max(ou_home_xg, 1.0)
    ou_away_xg = max(ou_away_xg, 1.0)

    # ── Poisson matrix (for ML and puck line — uses standard xG) ──
    matrix = _score_matrix(home_xg, away_xg)

    p_home = sum(matrix[h][a] for h in range(MAX_GOALS + 1) for a in range(MAX_GOALS + 1) if h > a)
    p_away = sum(matrix[h][a] for h in range(MAX_GOALS + 1) for a in range(MAX_GOALS + 1) if a > h)
    p_draw = sum(matrix[i][i] for i in range(MAX_GOALS + 1))

    # In NHL, ties go to OT — split 52/48 home/away by default.
    # Factor 10 (team-specific OT tendency) adjusts this but is gated
    # behind NHL_ENABLE_GRANULAR_FACTORS until validated.
    if _GRANULAR_ON:
        h_ot_rate = h_ot.get("ot_split_adj", 0.52)
        a_ot_rate = a_ot.get("ot_split_adj", 0.52)
        ot_total = h_ot_rate + (1 - a_ot_rate)
        home_ot_share = h_ot_rate / ot_total if ot_total > 0 else 0.52
        home_ot_share = max(0.45, min(0.60, home_ot_share))
    else:
        home_ot_share = 0.52
    away_ot_share = 1.0 - home_ot_share

    p_home_ml = p_home + p_draw * home_ot_share
    p_away_ml = p_away + p_draw * away_ot_share
    granular_data["ot_split_used"] = {"home": round(home_ot_share, 3),
                                      "away": round(away_ot_share, 3)}

    # ── Puck line (±1.5) ──
    p_home_m15 = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                     for a in range(MAX_GOALS + 1) if h - a >= 2)
    p_away_p15 = 1 - p_home_m15

    p_away_m15 = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                     for a in range(MAX_GOALS + 1) if a - h >= 2)
    p_home_p15 = 1 - p_away_m15

    # ── Factor 9: Empty net goal adjustment for puck line ──
    # Disabled with the rest of the granular factors until validated.
    if _GRANULAR_ON:
        p_home_1goal = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                           for a in range(MAX_GOALS + 1) if h - a == 1)
        p_away_1goal = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                           for a in range(MAX_GOALS + 1) if a - h == 1)
        en_conversion = 0.12  # ~30% trailing by 1 * 40% EN goal chance
        en_home_boost = min(0.04, p_home_1goal * en_conversion)
        en_away_boost = min(0.04, p_away_1goal * en_conversion)

        p_home_m15 += en_home_boost
        p_away_p15 -= en_home_boost
        p_away_m15 += en_away_boost
        p_home_p15 -= en_away_boost

        granular_data["en_goal_adj"] = {
            "home_m15_boost": round(en_home_boost, 4),
            "away_m15_boost": round(en_away_boost, 4),
        }

    # ── Factor 8: Blowout tendency puck line adjustment ──
    # If a team covers -1.5 at above-average rate (>35%), boost their -1.5 prob
    try:
        if not _GRANULAR_ON:
            raise RuntimeError("granular factors disabled")
        h_blowout = granular_data.get("home_blowout_tendency", {})
        a_blowout = granular_data.get("away_blowout_tendency", {})
        league_avg_blowout = 0.35  # ~35% of wins are by 2+

        if h_blowout.get("wins_by_2plus_pct", 0) > league_avg_blowout and h_blowout.get("games", 0) >= 10:
            excess = h_blowout["wins_by_2plus_pct"] - league_avg_blowout
            blowout_adj = min(0.03, excess * 0.15)  # Max 3% adjustment
            p_home_m15 += blowout_adj
            p_away_p15 -= blowout_adj
            granular_data["home_blowout_pl_adj"] = round(blowout_adj, 4)

        if a_blowout.get("wins_by_2plus_pct", 0) > league_avg_blowout and a_blowout.get("games", 0) >= 10:
            excess = a_blowout["wins_by_2plus_pct"] - league_avg_blowout
            blowout_adj = min(0.03, excess * 0.15)
            p_away_m15 += blowout_adj
            p_home_p15 -= blowout_adj
            granular_data["away_blowout_pl_adj"] = round(blowout_adj, 4)
    except Exception as e:
        logger.debug("Factor 8 puck line adjustment failed: %s", e)

    # Clamp puck line probabilities to valid range
    p_home_m15 = max(0.0, min(1.0, p_home_m15))
    p_away_p15 = max(0.0, min(1.0, p_away_p15))
    p_away_m15 = max(0.0, min(1.0, p_away_m15))
    p_home_p15 = max(0.0, min(1.0, p_home_p15))

    # ── Totals (must account for OT goal) ──
    # NHL O/U includes overtime. Tied games go to OT where exactly 1 more
    # goal is scored. So for each tie scenario (h == a), the actual total
    # is h + a + 1, not h + a.
    # Use the O/U-specific xGs (blended with L10 + goalie impact)
    ou_matrix = _score_matrix(ou_home_xg, ou_away_xg)
    ou_lines = {}
    for line in [4.5, 5.0, 5.5, 6.0, 6.5, 7.0]:
        p_over = 0
        for h in range(MAX_GOALS + 1):
            for a in range(MAX_GOALS + 1):
                if h == a:
                    # Game goes to OT — total becomes h + a + 1
                    actual_total = h + a + 1
                else:
                    actual_total = h + a
                if actual_total > line:
                    p_over += ou_matrix[h][a]
        ou_lines[str(line)] = {
            "over": round(p_over, 4),
            "under": round(1 - p_over, 4),
        }

    # ── Period breakdown (use real P1 data when available) ──
    weights = [0.33, 0.34, 0.33]
    p1_home = home_xg * weights[0]
    p1_away = away_xg * weights[0]
    p1_data_source = "generic"
    h_p1_stats = None
    a_p1_stats = None

    try:
        from .nhl_db import get_p1_stats, get_nhl_team_by_abbr as _p1_team_lookup

        h_abbr_p1 = home.get("abbreviation", "")
        a_abbr_p1 = away.get("abbreviation", "")

        h_team_p1 = _p1_team_lookup(h_abbr_p1) if h_abbr_p1 else None
        a_team_p1 = _p1_team_lookup(a_abbr_p1) if a_abbr_p1 else None

        now = datetime.now()
        p1_season = now.year if now.month >= 9 else now.year - 1

        if h_team_p1:
            h_p1_stats = get_p1_stats(h_team_p1["id"], p1_season)
        if a_team_p1:
            a_p1_stats = get_p1_stats(a_team_p1["id"], p1_season)

        if (h_p1_stats and a_p1_stats
                and h_p1_stats.get("games", 0) >= 10
                and a_p1_stats.get("games", 0) >= 10):
            # League-average P1 goals per team per game (~0.95)
            league_p1_avg = 0.95

            # Offense × opponent defense / league average
            raw_p1_home = (
                (h_p1_stats["p1_goals_for_home"] or league_p1_avg)
                * (a_p1_stats["p1_goals_against_away"] or league_p1_avg)
            ) / league_p1_avg
            raw_p1_away = (
                (a_p1_stats["p1_goals_for_away"] or league_p1_avg)
                * (h_p1_stats["p1_goals_against_home"] or league_p1_avg)
            ) / league_p1_avg

            # Blend with recent form (60% season, 40% last-10)
            if (h_p1_stats.get("recent_p1_gf") is not None
                    and h_p1_stats["recent_p1_gf"] > 0):
                raw_p1_home = raw_p1_home * 0.6 + h_p1_stats["recent_p1_gf"] * 0.4
            if (a_p1_stats.get("recent_p1_gf") is not None
                    and a_p1_stats["recent_p1_gf"] > 0):
                raw_p1_away = raw_p1_away * 0.6 + a_p1_stats["recent_p1_gf"] * 0.4

            # Scoreless-streak penalty: 25% reduction for 5+ game P1 droughts
            if h_p1_stats.get("p1_scoreless_streak", 0) >= 5:
                raw_p1_home *= 0.75
            elif h_p1_stats.get("p1_scoreless_streak", 0) >= 3:
                raw_p1_home *= 0.88

            if a_p1_stats.get("p1_scoreless_streak", 0) >= 5:
                raw_p1_away *= 0.75
            elif a_p1_stats.get("p1_scoreless_streak", 0) >= 3:
                raw_p1_away *= 0.88

            # Rest / B2B impacts P1 more than other periods
            # h_b2b < 1.0 means the team is on a back-to-back
            if h_b2b < 1.0:
                # B2B hits P1 harder: apply 1.5x the overall fatigue penalty
                p1_fatigue = 1 + (1 - h_b2b) * 1.5
                raw_p1_home /= p1_fatigue
            elif h_b2b > 1.0:
                # Rest advantage gives a small P1 boost
                raw_p1_home *= 1 + (h_b2b - 1.0) * 0.5

            if a_b2b < 1.0:
                p1_fatigue = 1 + (1 - a_b2b) * 1.5
                raw_p1_away /= p1_fatigue
            elif a_b2b > 1.0:
                raw_p1_away *= 1 + (a_b2b - 1.0) * 0.5

            # Floor at 0.15 (even the worst P1 team scores sometimes)
            p1_home = max(raw_p1_home, 0.15)
            p1_away = max(raw_p1_away, 0.15)
            p1_data_source = "real"

    except Exception as e:
        logger.debug("P1 real data unavailable, using generic estimate: %s", e)

    periods = []
    if p1_data_source == "real":
        # P1 from real data; P2/P3 use remaining xG split evenly
        remaining_home = max(home_xg - p1_home, 0.3)
        remaining_away = max(away_xg - p1_away, 0.3)
        periods.append({
            "period": "P1",
            "home": round(p1_home, 2),
            "away": round(p1_away, 2),
            "total": round(p1_home + p1_away, 2),
        })
        for label, w in [("P2", 0.52), ("P3", 0.48)]:
            periods.append({
                "period": label,
                "home": round(remaining_home * w, 2),
                "away": round(remaining_away * w, 2),
                "total": round((remaining_home + remaining_away) * w, 2),
            })
    else:
        for i, label in enumerate(["P1", "P2", "P3"]):
            periods.append({
                "period": label,
                "home": round(home_xg * weights[i], 2),
                "away": round(away_xg * weights[i], 2),
                "total": round((home_xg + away_xg) * weights[i], 2),
            })

    # ── First period total goals O/U ──
    # p1_home / p1_away are now set from real data or generic fallback above
    p1_total_probs = {}  # total_goals -> probability
    for hg in range(6):
        for ag in range(6):
            total = hg + ag
            prob = poisson(p1_home, hg) * poisson(p1_away, ag)
            p1_total_probs[total] = p1_total_probs.get(total, 0) + prob

    # O/U 0.5 (any goal in P1)
    p1_over_05 = 1 - p1_total_probs.get(0, 0)
    # O/U 1.5 (DraftKings standard line)
    p1_over_15 = sum(p for t, p in p1_total_probs.items() if t >= 2)
    # O/U 2.5
    p1_over_25 = sum(p for t, p in p1_total_probs.items() if t >= 3)

    # ── Top correct scores ──
    scores = []
    for h in range(MAX_GOALS + 1):
        for a in range(MAX_GOALS + 1):
            scores.append({"home": h, "away": a, "prob": matrix[h][a]})
    scores.sort(key=lambda s: s["prob"], reverse=True)
    top_scores = [{"score": f"{s['home']}-{s['away']}", "prob": round(s["prob"], 4)}
                  for s in scores[:5]]

    # Use live records if available — JSON files have stale last-season data
    h_live_record = _get_live_record(home.get("abbreviation", "")) or home.get("record", "")
    a_live_record = _get_live_record(away.get("abbreviation", "")) or away.get("record", "")

    return {
        "home": {
            "name": home.get("name", home_key),
            "abbreviation": home.get("abbreviation", ""),
            "record": h_live_record,
            "key": home_key,
        },
        "away": {
            "name": away.get("name", away_key),
            "abbreviation": away.get("abbreviation", ""),
            "record": a_live_record,
            "key": away_key,
        },
        "expected_score": {
            "home": round(home_xg, 2),
            "away": round(away_xg, 2),
        },
        "total": round(ou_home_xg + ou_away_xg + p_draw, 2),  # O/U-adjusted xGs + OT goal
        "spread": round(away_xg - home_xg, 1),
        "win_prob": {
            "home": round(p_home_ml, 4),
            "away": round(p_away_ml, 4),
        },
        "regulation_draw_prob": round(p_draw, 4),
        "puck_line": {
            "home_minus_1_5": round(p_home_m15, 4),
            "away_plus_1_5": round(p_away_p15, 4),
            "away_minus_1_5": round(p_away_m15, 4),
            "home_plus_1_5": round(p_home_p15, 4),
        },
        "over_under": ou_lines,
        "periods": periods,
        "first_period": {
            "over_05": round(p1_over_05, 4),
            "under_05": round(1 - p1_over_05, 4),
            "over_15": round(p1_over_15, 4),
            "under_15": round(1 - p1_over_15, 4),
            "over_25": round(p1_over_25, 4),
            "under_25": round(1 - p1_over_25, 4),
            "expected_home": round(p1_home, 3),
            "expected_away": round(p1_away, 3),
            "expected_total": round(p1_home + p1_away, 2),
            "data_source": p1_data_source,
            "home_p1_scoreless_streak": (
                h_p1_stats.get("p1_scoreless_streak", 0)
                if h_p1_stats else 0
            ),
            "away_p1_scoreless_streak": (
                a_p1_stats.get("p1_scoreless_streak", 0)
                if a_p1_stats else 0
            ),
        },
        "correct_scores": top_scores,
        "factors": _build_factors_with_ranks(
            home, away, hs, as_,
            h_pp, a_pp, h_pk, a_pk, h_sv, a_sv,
            h_shots, a_shots, h_fo, a_fo
        ),
        "goalie_matchup": goalie_factor,
        "h2h": h2h_data,
        "season_context": season_context,
        "injuries": injury_data,
        "rest": rest_data,
        "granular": granular_data,
    }


# ── Backward-compatible re-exports ──
# Pick generation has been moved to engine/nhl_picks.py to separate
# game prediction from bet selection. These re-exports keep existing
# callers working without changes.
from .nhl_picks import generate_nhl_picks, generate_nhl_picks_with_context  # noqa: F401, E402
