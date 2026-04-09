"""
NHL Model Backtester.

Runs the NHL prediction model against historical games with:
- DB-backed games from nhl_games table (primary)
- On-the-fly ESPN API fetch when DB is empty (fallback)
- Per-category results (ML, O/U, PL all shown independently)
- Point-in-time rolling stats (no lookahead bias)
- Probability-based realistic odds with vig

The output dict is compatible with the Backtest.jsx frontend component.

Usage:
    python -m engine.nhl_backtest                    # Last 30 days (PIT mode)
    python -m engine.nhl_backtest --days 60           # Last 60 days
    python -m engine.nhl_backtest --season 2025       # Full season
    python -m engine.nhl_backtest --min-edge 3        # Only 3%+ edge bets
    python -m engine.nhl_backtest --no-pit            # Use live model (lookahead)
    python -m engine.nhl_backtest --thresholds        # Compare edge thresholds
"""

import json
import logging
import math
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

SEASON = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1

# Realistic average NHL odds
AVG_FAV_ODDS = -150      # Average favorite line
AVG_DOG_ODDS = 130       # Corresponding underdog
PL_ODDS = -110           # Puck line standard
OU_ODDS = -110           # Over/under standard

MAX_GOALS = 10


def _poisson(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def _score_matrix(home_xg: float, away_xg: float) -> list[list[float]]:
    matrix = []
    for h in range(MAX_GOALS + 1):
        row = []
        for a in range(MAX_GOALS + 1):
            row.append(_poisson(home_xg, h) * _poisson(away_xg, a))
        matrix.append(row)
    return matrix


def _implied(ml: int) -> float:
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


def _payout(odds: int) -> float:
    """Return profit on a $100 bet at the given odds."""
    if odds > 0:
        return odds
    return (100 / abs(odds)) * 100


# ── Data loading ──────────────────────────────────────────────


def _load_games_from_db(days: int | None = None,
                        season: int | None = None) -> list[dict]:
    """Load completed NHL games from the nhl_games DB table."""
    try:
        from .nhl_db import get_conn
    except Exception:
        return []

    conn = get_conn()
    yr = season or SEASON

    # NHL API stores season as YYYYYYYY (e.g. 20252026)
    # Frontend sends just a year (e.g. 2025 or 2026)
    # For NHL: season 2025-26 is stored as 20252026
    # If user sends 2026, they mean the 2025-26 season (ends in 2026)
    # If user sends 2025, they could mean 2025-26 (starts) or 2024-25 (ends)
    season_ids = [yr]
    if yr < 10000:
        # Try both: year as start (2025->20252026) and as end (2025->20242025)
        season_ids.append(yr * 10000 + yr + 1)      # 2025 -> 20252026
        season_ids.append((yr - 1) * 10000 + yr)     # 2026 -> 20252026

    if days and days > 0:
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT g.*,
                   ht.abbreviation as home_abbr, ht.name as home_name,
                   at.abbreviation as away_abbr, at.name as away_name
            FROM nhl_games g
            LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
            LEFT JOIN nhl_teams at ON g.away_team_id = at.id
            WHERE g.status = 'final' AND g.date >= ?
            ORDER BY g.date
        """, (start_date,)).fetchall()
    else:
        placeholders = ",".join("?" for _ in season_ids)
        rows = conn.execute(f"""
            SELECT g.*,
                   ht.abbreviation as home_abbr, ht.name as home_name,
                   at.abbreviation as away_abbr, at.name as away_name
            FROM nhl_games g
            LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
            LEFT JOIN nhl_teams at ON g.away_team_id = at.id
            WHERE g.status = 'final' AND g.season IN ({placeholders})
            ORDER BY g.date
        """, season_ids).fetchall()

    return [dict(r) for r in rows]


def _load_games_from_api(days: int = 30) -> list[dict]:
    """Fetch recent completed games from the ESPN API when DB is empty."""
    import urllib.request

    games = []
    today = datetime.utcnow().date()

    for day_offset in range(days, 0, -1):
        check_date = today - timedelta(days=day_offset)
        espn_date = check_date.strftime("%Y%m%d")
        date_str = check_date.strftime("%Y-%m-%d")
        url = (
            "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl"
            f"/scoreboard?dates={espn_date}"
        )
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())

            for event in data.get("events", []):
                status_type = (event.get("status", {})
                               .get("type", {}).get("name", ""))
                if status_type != "STATUS_FINAL":
                    continue

                for comp in event.get("competitions", []):
                    home_team = away_team = None
                    home_score = away_score = None

                    for team_entry in comp.get("competitors", []):
                        t = team_entry.get("team", {})
                        abbr = t.get("abbreviation", "")
                        score = int(team_entry.get("score", 0))
                        if team_entry.get("homeAway") == "home":
                            home_team = abbr
                            home_score = score
                        else:
                            away_team = abbr
                            away_score = score

                    if home_team and away_team and home_score is not None:
                        games.append({
                            "date": date_str,
                            "home_abbr": home_team,
                            "away_abbr": away_team,
                            "home_score": home_score,
                            "away_score": away_score,
                            "status": "final",
                        })
        except Exception as e:
            logger.debug("Failed to fetch ESPN scoreboard for %s: %s",
                         date_str, e)
            continue

    return games


def _abbr_to_team_key(abbr: str) -> str | None:
    """Map a team abbreviation to the JSON file key used by load_team.

    Scans data/teams/NHL/ for a matching abbreviation in the JSON files.
    Result is cached.
    """
    if not hasattr(_abbr_to_team_key, "_cache"):
        _abbr_to_team_key._cache = {}

    if abbr in _abbr_to_team_key._cache:
        return _abbr_to_team_key._cache[abbr]

    try:
        from .data import list_teams, load_team
        for t in list_teams("NHL"):
            team = load_team("NHL", t["key"])
            if team and team.get("abbreviation", "").upper() == abbr.upper():
                _abbr_to_team_key._cache[abbr] = t["key"]
                return t["key"]
    except Exception:
        pass

    _abbr_to_team_key._cache[abbr] = None
    return None


# ── Prediction for backtest ───────────────────────────────────


def _prob_to_american(prob: float) -> int:
    """Convert a raw probability to American odds with standard vig."""
    if prob <= 0 or prob >= 1:
        return -110
    # Add standard 2.5% vig per side
    vigged = min(0.95, prob + 0.025)
    if vigged >= 0.5:
        return int(-vigged / (1 - vigged) * 100)
    else:
        return int((1 - vigged) / vigged * 100)


def _compute_market_prob(home_stats: dict, away_stats: dict) -> float:
    """Simulate what the market would price the home team at.

    Uses a simpler model than our prediction (just win%, goals) to represent
    what an average bettor/market maker would see. Our enhanced model should
    find edges OVER this baseline.
    """
    # Simple market: weighted average of win% and goals-based estimate
    # This is intentionally dumber than our PIT model
    h_wp = home_stats.get("win_pct", 0.5)
    a_wp = away_stats.get("win_pct", 0.5)

    # Win% based estimate (+ small home-ice bonus)
    wp_estimate = (h_wp / (h_wp + a_wp)) if (h_wp + a_wp) > 0 else 0.5
    wp_estimate = wp_estimate * 0.95 + 0.025  # Add 2.5% home-ice

    # Goals-based estimate (simpler than our full model)
    h_gf = home_stats.get("goals_for_avg", 3.0)
    a_gf = away_stats.get("goals_for_avg", 3.0)
    h_ga = home_stats.get("goals_against_avg", 3.0)
    a_ga = away_stats.get("goals_against_avg", 3.0)

    h_strength = h_gf / max(h_ga, 1.5)
    a_strength = a_gf / max(a_ga, 1.5)
    goals_estimate = h_strength / (h_strength + a_strength) + 0.02  # Home ice

    # Market = blend of both (no special teams, no momentum, no shots)
    market = wp_estimate * 0.5 + goals_estimate * 0.5
    return max(0.25, min(0.75, market))


def _compute_pit_stats(conn, team_id: int, game_date: str,
                       window: int = 20) -> dict | None:
    """Compute point-in-time stats for a team using only games before this date.

    Uses a rolling window of recent games from the nhl_games table so the
    backtest never peeks at future data. Enhanced with home/away splits,
    special teams proxy, momentum, save% proxy, and L5 scoring for O/U.
    """
    rows = conn.execute("""
        SELECT home_team_id, away_team_id, home_score, away_score,
               home_pp_goals, home_pp_opps, away_pp_goals, away_pp_opps,
               home_shots, away_shots
        FROM nhl_games
        WHERE status = 'final' AND date < ?
          AND (home_team_id = ? OR away_team_id = ?)
        ORDER BY date DESC LIMIT ?
    """, (game_date, team_id, team_id, window)).fetchall()

    if len(rows) < 5:
        return None

    gf, ga, wins, losses = 0, 0, 0, 0
    home_gf, home_ga, home_games = 0, 0, 0
    away_gf, away_ga, away_games = 0, 0, 0
    pp_goals, pp_opps = 0, 0
    shots_for, shots_against = 0, 0
    # Track last 5 for momentum and L5 scoring
    recent_wins = 0
    l5_gf, l5_ga = 0, 0
    l5_count = 0

    for i, r in enumerate(rows):
        is_home = r["home_team_id"] == team_id
        if is_home:
            my_score = r["home_score"] or 0
            opp_score = r["away_score"] or 0
            home_gf += my_score
            home_ga += opp_score
            home_games += 1
            pp_goals += r["home_pp_goals"] or 0
            pp_opps += r["home_pp_opps"] or 0
            shots_for += r["home_shots"] or 0
            shots_against += r["away_shots"] or 0
        else:
            my_score = r["away_score"] or 0
            opp_score = r["home_score"] or 0
            away_gf += my_score
            away_ga += opp_score
            away_games += 1
            pp_goals += r["away_pp_goals"] or 0
            pp_opps += r["away_pp_opps"] or 0
            shots_for += r["away_shots"] or 0
            shots_against += r["home_shots"] or 0

        gf += my_score
        ga += opp_score
        if my_score > opp_score:
            wins += 1
            if i < 5:
                recent_wins += 1
        else:
            losses += 1

        # L5 scoring accumulators (rows ordered DESC, so first 5 = most recent)
        if i < 5:
            l5_gf += my_score
            l5_ga += opp_score
            l5_count += 1

    n = len(rows)
    pp_pct = pp_goals / pp_opps if pp_opps > 0 else 0.20
    shots_pg = shots_for / n if shots_for > 0 else 30.0
    shots_against_pg = shots_against / n if shots_against > 0 else 30.0

    # Save percentage proxy: (1 - goals_against / shots_against)
    # This estimates the team's goaltending quality from PIT data
    total_shots_against = shots_against
    if total_shots_against > 0:
        save_pct_proxy = 1.0 - (ga / total_shots_against)
    else:
        save_pct_proxy = 0.905  # League average fallback

    return {
        "goals_for_avg": gf / n,
        "goals_against_avg": ga / n,
        "win_pct": wins / n,
        "home_gf_avg": home_gf / home_games if home_games > 0 else gf / n,
        "home_ga_avg": home_ga / home_games if home_games > 0 else ga / n,
        "away_gf_avg": away_gf / away_games if away_games > 0 else gf / n,
        "away_ga_avg": away_ga / away_games if away_games > 0 else ga / n,
        "pp_pct": pp_pct,
        "shots_per_game": shots_pg,
        "shots_against_pg": shots_against_pg,
        "save_pct_proxy": save_pct_proxy,
        "momentum": recent_wins / min(5, n),  # Last 5 game win rate
        "l5_gf_avg": l5_gf / l5_count if l5_count > 0 else gf / n,
        "l5_ga_avg": l5_ga / l5_count if l5_count > 0 else ga / n,
        "games": n,
    }


def _pit_predict(home_stats: dict, away_stats: dict) -> dict | None:
    """Enhanced Poisson prediction using point-in-time stats.

    Faithfully reproduces the production model's factor chain using only
    historical PIT data:
      1. Base xG from home/away splits (attack * defense / league_avg)
      2. Home ice edge (+0.15)
      3. Special teams (PP% vs league avg, ~2.5 PP/game weight)
      4. Goaltending / save% (suppress opponent xG)
      5. Shot volume adjustment
      6. Momentum (L5 win rate)
      7. Win% quality gap
      8. Poisson matrix -> ML (with OT home edge), PL, O/U
      9. Separate O/U xG using L5 scoring blend (40% recent, 60% season)
    """
    league_avg = 3.0
    home_edge = 0.15

    # ── 1. Base expected goals (home/away splits) ──
    home_off = home_stats["home_gf_avg"]
    home_def = home_stats["home_ga_avg"]
    away_off = away_stats["away_gf_avg"]
    away_def = away_stats["away_ga_avg"]

    # Production formula: (off * opp_def) / league_avg +/- home_edge/2
    home_xg = (home_off * away_def) / league_avg + home_edge / 2
    away_xg = (away_off * home_def) / league_avg - home_edge / 2

    # ── 2. Special teams: PP% vs league average ──
    # Production uses pp_weight=2.5 and combines PP% edge with PK edge.
    # PIT model only has PP% (no separate PK%), so we use PP% differential
    # against league average, weighted by ~2.5 PP opportunities per game.
    league_pp = 0.20
    pp_weight = 2.5

    # Home PP edge (home PP% above average = more goals for home)
    home_pp_edge = (home_stats["pp_pct"] - league_pp) * pp_weight
    # Away PP edge (away PP% above average = more goals for away)
    away_pp_edge = (away_stats["pp_pct"] - league_pp) * pp_weight
    home_xg += home_pp_edge
    away_xg += away_pp_edge

    # ── 3. Goaltending / save% adjustment ──
    # Production: better save% suppresses opponent xG via (league_sv / team_sv)
    # PIT: use save_pct_proxy computed from (1 - GA/SA) over the window
    league_sv = 0.905
    h_sv = home_stats.get("save_pct_proxy", league_sv)
    a_sv = away_stats.get("save_pct_proxy", league_sv)

    # Home goaltending suppresses away scoring
    if h_sv and h_sv > 0:
        away_xg *= max(0.85, min(1.15, league_sv / h_sv))
    # Away goaltending suppresses home scoring
    if a_sv and a_sv > 0:
        home_xg *= max(0.85, min(1.15, league_sv / a_sv))

    # ── 4. Shot volume adjustment ──
    # Production: combine team shots/game with opponent shots-against/game
    league_shots = 30.0
    if home_stats["shots_per_game"] > 0 and away_stats["shots_against_pg"] > 0:
        h_shot_factor = ((home_stats["shots_per_game"] / league_shots) +
                        (away_stats["shots_against_pg"] / league_shots)) / 2
        a_shot_factor = ((away_stats["shots_per_game"] / league_shots) +
                        (home_stats["shots_against_pg"] / league_shots)) / 2
        home_xg *= max(0.90, min(1.10, h_shot_factor))
        away_xg *= max(0.90, min(1.10, a_shot_factor))

    # ── 5. Momentum: L5 win rate ──
    # Production uses _form_factor (~±12%) + _compute_recent_form_from_standings (~±5%)
    # PIT: use momentum (L5 win rate) as a combined proxy
    home_momentum = (home_stats["momentum"] - 0.5) * 0.08
    away_momentum = (away_stats["momentum"] - 0.5) * 0.08
    home_xg *= (1 + home_momentum)
    away_xg *= (1 + away_momentum)

    # ── 6. Win% quality gap ──
    # Production uses points_pct * 0.15; PIT uses win_pct * 0.15
    quality_diff = home_stats["win_pct"] - away_stats["win_pct"]
    home_xg *= (1 + quality_diff * 0.15)
    away_xg *= (1 - quality_diff * 0.15)

    # ── Floor ──
    home_xg = max(home_xg, 1.0)
    away_xg = max(away_xg, 1.0)

    # ── O/U-specific xG: blend season avg with L5 scoring ──
    # Production blends 60% season + 40% L10 scoring for O/U.
    # PIT uses L5 as the recent window (that's our available data).
    ou_home_xg = home_xg
    ou_away_xg = away_xg

    # Home team recent scoring form
    season_gf_h = home_stats["goals_for_avg"]
    l5_gf_h = home_stats.get("l5_gf_avg", season_gf_h)
    if season_gf_h > 0:
        ou_home_xg = ou_home_xg * 0.6 + (ou_home_xg * l5_gf_h / season_gf_h) * 0.4

    # Home team recent defensive form (affects away xG for O/U)
    season_ga_h = home_stats["goals_against_avg"]
    l5_ga_h = home_stats.get("l5_ga_avg", season_ga_h)
    if season_ga_h > 0:
        ou_away_xg = ou_away_xg * 0.6 + (ou_away_xg * l5_ga_h / season_ga_h) * 0.4

    # Away team recent scoring form
    season_gf_a = away_stats["goals_for_avg"]
    l5_gf_a = away_stats.get("l5_gf_avg", season_gf_a)
    if season_gf_a > 0:
        ou_away_xg = ou_away_xg * 0.6 + (ou_away_xg * l5_gf_a / season_gf_a) * 0.4

    # Away team recent defensive form (affects home xG for O/U)
    season_ga_a = away_stats["goals_against_avg"]
    l5_ga_a = away_stats.get("l5_ga_avg", season_ga_a)
    if season_ga_a > 0:
        ou_home_xg = ou_home_xg * 0.6 + (ou_home_xg * l5_ga_a / season_ga_a) * 0.4

    # ── Goalie impact on totals (amplified vs ML) ──
    # Goalie impact on totals is stronger than on win probability.
    # A backup goalie doesn't change WHO wins as much as HOW MANY goals are scored.
    if home_stats.get("save_pct_proxy") and away_stats.get("save_pct_proxy"):
        league_sv = 0.905
        # For O/U, amplify the goaltending effect by 1.5x
        h_sv_factor = max(0.80, min(1.20, league_sv / home_stats["save_pct_proxy"]))
        a_sv_factor = max(0.80, min(1.20, league_sv / away_stats["save_pct_proxy"]))
        ou_away_xg *= h_sv_factor * 1.2  # Home goalie quality affects away scoring more for totals
        ou_home_xg *= a_sv_factor * 1.2

    # Floor O/U xGs
    ou_home_xg = max(ou_home_xg, 1.0)
    ou_away_xg = max(ou_away_xg, 1.0)

    # ── Poisson matrix for ML and puck line (standard xG) ──
    matrix = _score_matrix(home_xg, away_xg)

    p_home = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                 for a in range(MAX_GOALS + 1) if h > a)
    p_away = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                 for a in range(MAX_GOALS + 1) if a > h)
    p_draw = sum(matrix[i][i] for i in range(MAX_GOALS + 1))

    # OT split: slight home edge (matches production exactly)
    p_home_ml = p_home + p_draw * 0.52
    p_away_ml = p_away + p_draw * 0.48

    # ── Puck line (±1.5) ──
    # Production: home -1.5 = margin >= 2, away -1.5 = margin <= -2
    p_home_cover = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                       for a in range(MAX_GOALS + 1) if (h - a) >= 2)

    # ── O/U: use separate ou_matrix with L5-blended xGs ──
    # Production: tied games go to OT, adding exactly 1 goal
    ou_matrix = _score_matrix(ou_home_xg, ou_away_xg)
    ou_pred_total = ou_home_xg + ou_away_xg
    ou_p_draw = sum(ou_matrix[i][i] for i in range(MAX_GOALS + 1))
    ou_line = round(ou_pred_total * 2) / 2

    p_over = 0.0
    for h in range(MAX_GOALS + 1):
        for a in range(MAX_GOALS + 1):
            # NHL OT adds exactly 1 goal for tied regulation games
            eff_total = (h + a + 1) if h == a else (h + a)
            if eff_total > ou_line:
                p_over += ou_matrix[h][a]

    return {
        "home_xg": home_xg,
        "away_xg": away_xg,
        "ou_home_xg": ou_home_xg,
        "ou_away_xg": ou_away_xg,
        "p_home": p_home_ml,
        "p_away": p_away_ml,
        "total": ou_pred_total + ou_p_draw,  # O/U-adjusted total + OT goal approx
        "p_over": p_over,
        "ou_line": ou_line,
        "p_home_cover": p_home_cover,
        "puck_line": {
            "home_minus_1_5": p_home_cover,
            "away_plus_1_5": 1 - p_home_cover,
        },
        "over_under": {},
    }


def _predict_game(home_abbr: str, away_abbr: str) -> dict | None:
    """Run the NHL prediction model for a matchup by abbreviation.

    Returns dict with home_xg, away_xg, p_home, p_away, total, matrix
    or None if data can't be loaded.
    """
    from .nhl_predict import predict_matchup

    home_key = _abbr_to_team_key(home_abbr)
    away_key = _abbr_to_team_key(away_abbr)

    if not home_key or not away_key:
        return None

    pred = predict_matchup(home_key, away_key)
    if not pred:
        return None

    home_xg = pred["expected_score"]["home"]
    away_xg = pred["expected_score"]["away"]

    return {
        "home_xg": home_xg,
        "away_xg": away_xg,
        "p_home": pred["win_prob"]["home"],
        "p_away": pred["win_prob"]["away"],
        "total": pred["total"],
        "puck_line": pred["puck_line"],
        "over_under": pred.get("over_under", {}),
    }


# ── Back-to-back detection ───────────────────────────────────


def _check_b2b_from_games(games: list, game_idx: int, team_id: int) -> bool:
    """Check if team_id played yesterday by looking at previous games in the list."""
    current_date = games[game_idx].get("date", "")
    if not current_date:
        return False
    # Check if this team appears in yesterday's games
    yesterday = (datetime.strptime(current_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    for i in range(game_idx - 1, max(0, game_idx - 20), -1):
        g = games[i]
        if g.get("date") == yesterday:
            if g.get("home_team_id") == team_id or g.get("away_team_id") == team_id:
                return True
        elif g.get("date") < yesterday:
            break
    return False


# ── Backtest core ─────────────────────────────────────────────


def _empty_cat():
    return {"wins": 0, "losses": 0, "pushes": 0, "profit": 0.0}


def _record_bet(cat, won, odds):
    if won:
        cat["wins"] += 1
        cat["profit"] += _payout(odds)
    else:
        cat["losses"] += 1
        cat["profit"] -= 100


def _summarize(cat):
    total = cat["wins"] + cat["losses"]
    cat["total_bets"] = total
    cat["win_pct"] = round(cat["wins"] / total * 100, 1) if total > 0 else 0
    cat["roi"] = round(cat["profit"] / (total * 100) * 100, 1) if total > 0 else 0
    cat["profit"] = round(cat["profit"], 2)


def run_nhl_backtest(days: int = 30, min_edge: float = 3.0,
                     season: int | None = None,
                     pit_mode: bool = True) -> dict:
    """Run backtest on historical NHL games.

    Args:
        days: Number of recent days to include (0 = full season).
        min_edge: Minimum edge percentage to place a bet.
        season: NHL season year (e.g. 2025 for 2025-26).
        pit_mode: If True, use point-in-time rolling stats to avoid
            lookahead bias.  If False, use the live prediction model
            (current stats applied to historical games -- for comparison).

    Returns a dict compatible with the Backtest.jsx frontend component:
        {
            "season": int,
            "games_tested": int,
            "games_skipped": int,
            "moneyline": {wins, losses, pushes, profit, total_bets, win_pct, roi},
            "over_under": {...},
            "puck_line": {...},
            "best_bet": {...},
            # Aliases for frontend compatibility
            "nrfi": {...},       # Empty placeholder (NHL has no NRFI)
            "run_line": {...},   # Alias for puck_line
        }
    """
    # Try DB first, fall back to API
    games = _load_games_from_db(days=days, season=season)
    source = "db"

    if not games:
        effective_days = days or 30
        logger.info("No DB games found, fetching last %d days from ESPN API...",
                     effective_days)
        games = _load_games_from_api(days=effective_days)
        source = "api"

    if not games:
        return {"error": "No completed games found", "games_tested": 0}

    yr = season or SEASON

    # Get DB connection for PIT mode
    pit_conn = None
    if pit_mode:
        try:
            from .nhl_db import get_conn
            pit_conn = get_conn()
        except Exception:
            logger.warning("PIT mode requested but nhl_db unavailable, "
                           "falling back to live model")
            pit_mode = False

    # Debug: track data source
    game_dates = [g.get("date", "?") for g in games[:3]]
    game_seasons = [g.get("season", "?") for g in games[:3]]

    results = {
        "season": yr,
        "source": source,
        "pit_mode": pit_mode,
        "debug_game_count": len(games),
        "debug_sample_dates": game_dates,
        "debug_sample_seasons": game_seasons,
        "games_tested": 0,
        "games_skipped": 0,
        "moneyline": _empty_cat(),
        "over_under": _empty_cat(),
        "puck_line": _empty_cat(),
        "best_bet": _empty_cat(),
        # Frontend compatibility: these map to NHL equivalents
        "nrfi": _empty_cat(),       # NHL has no NRFI; stays empty
        "run_line": _empty_cat(),   # Alias -- will be set to puck_line at end
    }

    recent_picks = []
    calibration_buckets = {
        "0-10": [0, 0], "10-20": [0, 0], "20-30": [0, 0],
        "30-40": [0, 0], "40-50": [0, 0], "50-60": [0, 0],
        "60-70": [0, 0], "70-80": [0, 0], "80-90": [0, 0],
        "90-100": [0, 0],
    }

    for game_idx, game in enumerate(games):
        home_abbr = game.get("home_abbr", "")
        away_abbr = game.get("away_abbr", "")
        home_score = game.get("home_score")
        away_score = game.get("away_score")

        if not home_abbr or not away_abbr:
            results["games_skipped"] += 1
            continue
        if home_score is None or away_score is None:
            results["games_skipped"] += 1
            continue

        # ── Get prediction (PIT or live model) ──
        pred = None
        if pit_mode and pit_conn:
            home_tid = game.get("home_team_id")
            away_tid = game.get("away_team_id")
            game_date = game.get("date", "")
            if home_tid and away_tid and game_date:
                home_pit = _compute_pit_stats(pit_conn, home_tid, game_date)
                away_pit = _compute_pit_stats(pit_conn, away_tid, game_date)
                if home_pit and away_pit:
                    pred = _pit_predict(home_pit, away_pit)

        if pred is None and not pit_mode:
            # Fall back to live model (lookahead -- for comparison only)
            pred = _predict_game(home_abbr, away_abbr)

        if not pred:
            results["games_skipped"] += 1
            continue

        results["games_tested"] += 1
        home_won = home_score > away_score
        actual_total = home_score + away_score
        margin = home_score - away_score
        game_bets = []

        p_home = pred["p_home"]
        p_away = pred["p_away"]
        home_xg = pred["home_xg"]
        away_xg = pred["away_xg"]

        # ── Calibration tracking ──
        bucket_idx = min(int(max(p_home, p_away) * 100), 99)
        bucket_key = f"{(bucket_idx // 10) * 10}-{(bucket_idx // 10) * 10 + 10}"
        if bucket_key in calibration_buckets:
            calibration_buckets[bucket_key][0] += 1  # total
            fav_won = (p_home > p_away and home_won) or \
                      (p_away > p_home and not home_won)
            if fav_won:
                calibration_buckets[bucket_key][1] += 1  # correct

        # ── Compute market baseline (simpler model = what bookmakers see) ──
        if pit_mode and pit_conn and home_pit and away_pit:
            market_home = _compute_market_prob(home_pit, away_pit)
        else:
            market_home = 0.5  # No market estimate available
        market_away = 1 - market_home

        # ── Moneyline ──
        fav_home = p_home > p_away
        if fav_home:
            ml_prob = p_home
            ml_market = market_home
            ml_odds = _prob_to_american(ml_market)  # Market odds from baseline
            ml_pick = home_abbr
        else:
            ml_prob = p_away
            ml_market = market_away
            ml_odds = _prob_to_american(ml_market)
            ml_pick = away_abbr

        ml_edge = (ml_prob - ml_market) * 100  # Edge = our model vs market baseline
        if ml_edge >= min_edge:
            ml_correct = (fav_home and home_won) or \
                         (not fav_home and not home_won)
            _record_bet(results["moneyline"], ml_correct, ml_odds)
            game_bets.append((ml_edge, ml_correct, ml_odds, "ML", ml_pick))

            # Track recent picks (last 20)
            if len(recent_picks) < 50:
                recent_picks.append({
                    "date": game.get("date", ""),
                    "matchup": f"{away_abbr} @ {home_abbr}",
                    "type": "ML",
                    "pick": ml_pick,
                    "prob": round(ml_prob, 3),
                    "edge": round(ml_edge, 1),
                    "result": "W" if ml_correct else "L",
                    "score": f"{home_score}-{away_score}",
                })

        # ── Back-to-back adjustment for O/U ──
        # B2B teams give up more goals — boost opponent xG for totals
        ou_home_xg = pred.get("ou_home_xg", home_xg)
        ou_away_xg = pred.get("ou_away_xg", away_xg)
        b2b_adjusted = False

        if pit_mode and pit_conn:
            home_tid = game.get("home_team_id")
            away_tid = game.get("away_team_id")
            if home_tid and away_tid:
                home_b2b = _check_b2b_from_games(games, game_idx, home_tid)
                away_b2b = _check_b2b_from_games(games, game_idx, away_tid)
                if home_b2b:
                    ou_away_xg *= 1.04  # Tired team concedes 4% more goals
                    b2b_adjusted = True
                if away_b2b:
                    ou_home_xg *= 1.04
                    b2b_adjusted = True

        # ── Over/Under ──
        # Use pre-computed O/U values from _pit_predict (which already
        # use L5-blended xG and a separate O/U Poisson matrix)
        ou_line = pred.get("ou_line")
        p_over = pred.get("p_over", 0.5)

        if b2b_adjusted:
            # Recompute O/U probabilities with B2B-adjusted xGs
            ou_home_xg = max(ou_home_xg, 1.0)
            ou_away_xg = max(ou_away_xg, 1.0)
            ou_pred_total = ou_home_xg + ou_away_xg
            ou_line = round(ou_pred_total * 2) / 2
            ou_matrix = _score_matrix(ou_home_xg, ou_away_xg)
            ou_p_draw = sum(ou_matrix[i][i] for i in range(MAX_GOALS + 1))
            p_over = 0.0
            for h in range(MAX_GOALS + 1):
                for a in range(MAX_GOALS + 1):
                    eff_total = (h + a + 1) if h == a else (h + a)
                    if eff_total > ou_line:
                        p_over += ou_matrix[h][a]
        elif ou_line is None:
            # Fallback for live-model path which may not set ou_line
            pred_total = home_xg + away_xg
            ou_line = round(pred_total * 2) / 2
            matrix = _score_matrix(home_xg, away_xg)
            p_over = 0.0
            for h in range(MAX_GOALS + 1):
                for a in range(MAX_GOALS + 1):
                    eff_total = (h + a + 1) if h == a else (h + a)
                    if eff_total > ou_line:
                        p_over += matrix[h][a]

        ou_pick_over = p_over > 0.50
        ou_prob = p_over if ou_pick_over else (1 - p_over)
        # Market baseline for O/U: assume market prices at 52.4% (-110 vig)
        ou_market = 0.524
        ou_odds = -110  # Standard O/U odds
        ou_edge = (ou_prob - ou_market) * 100

        if ou_edge >= min_edge:
            if actual_total == ou_line:
                results["over_under"]["pushes"] += 1
            elif ou_pick_over:
                ou_correct = actual_total > ou_line
                _record_bet(results["over_under"], ou_correct, ou_odds)
                game_bets.append((ou_edge, ou_correct, ou_odds, "O/U",
                                  f"Over {ou_line}"))
            else:
                ou_correct = actual_total < ou_line
                _record_bet(results["over_under"], ou_correct, ou_odds)
                game_bets.append((ou_edge, ou_correct, ou_odds, "O/U",
                                  f"Under {ou_line}"))

        # ── Puck Line (-1.5) ──
        # Use pre-computed puck line probability from _pit_predict
        p_home_cover = pred.get("p_home_cover", 0.0)
        if p_home_cover == 0.0:
            # Fallback: compute from fresh matrix (live-model path)
            matrix = _score_matrix(home_xg, away_xg)
            p_home_cover = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                               for a in range(MAX_GOALS + 1) if (h - a) >= 2)
        p_away_cover = 1 - p_home_cover  # Away +1.5

        pl_pick_home = p_home_cover > 0.50
        pl_prob = p_home_cover if pl_pick_home else p_away_cover

        # Real puck line market pricing:
        #   +1.5 (underdog side): typically -180, implied ~64-67%
        #   -1.5 (favorite side): typically +150 to +170, implied ~37-40%
        if pl_pick_home:
            # Picking home -1.5 (favorite to win by 2+)
            pl_market = 0.37   # Market prices -1.5 at about +170
            pl_odds = 170      # +170 for -1.5 side
        else:
            # Picking away +1.5 (or home +1.5 -- underdog side)
            pl_market = 0.65   # Market prices +1.5 at about -180
            pl_odds = -180     # -180 for +1.5 side

        pl_edge = (pl_prob - pl_market) * 100

        if pl_edge >= min_edge:
            if pl_pick_home:
                pl_correct = margin >= 2
                pl_pick = f"{home_abbr} -1.5"
            else:
                pl_correct = margin <= 1
                pl_pick = f"{away_abbr} +1.5"
            _record_bet(results["puck_line"], pl_correct, pl_odds)
            game_bets.append((pl_edge, pl_correct, pl_odds, "PL", pl_pick))

        # ── Best bet per game ──
        if game_bets:
            game_bets.sort(key=lambda x: x[0], reverse=True)
            best_edge, best_correct, best_odds, _, _ = game_bets[0]
            _record_bet(results["best_bet"], best_correct, best_odds)

    # ── Compute summaries ──
    for cat in ["moneyline", "over_under", "puck_line", "best_bet"]:
        _summarize(results[cat])

    # Alias puck_line -> run_line for frontend compatibility
    results["run_line"] = results["puck_line"]

    # Trim recent_picks to last 20
    results["recent_picks"] = recent_picks[-20:]

    # Calibration data
    cal = {}
    for bucket, (total, correct) in calibration_buckets.items():
        if total > 0:
            cal[bucket] = {
                "total": total,
                "correct": correct,
                "actual_pct": round(correct / total * 100, 1),
            }
    results["calibration"] = cal
    results["source"] = source

    return results


# ── Edge threshold analysis ──────────────────────────────────


def analyze_edge_thresholds(days: int = 0, season: int | None = None,
                            pit_mode: bool = True) -> list[dict]:
    """Run backtest at multiple edge thresholds and report which is optimal.

    Returns a list of dicts, one per threshold, with bets/win_pct/roi/profit
    for each bet category plus the best-bet aggregation.
    """
    thresholds = [1, 3, 5, 8, 10, 15]
    results = []
    for threshold in thresholds:
        bt = run_nhl_backtest(days=days, min_edge=threshold, season=season,
                              pit_mode=pit_mode)
        entry = {
            "threshold": threshold,
            "games_tested": bt.get("games_tested", 0),
        }
        for cat in ["moneyline", "over_under", "puck_line", "best_bet"]:
            cat_data = bt.get(cat, {})
            entry[cat] = {
                "bets": cat_data.get("total_bets", 0),
                "wins": cat_data.get("wins", 0),
                "losses": cat_data.get("losses", 0),
                "win_pct": cat_data.get("win_pct", 0),
                "roi": cat_data.get("roi", 0),
                "profit": cat_data.get("profit", 0),
            }
        results.append(entry)
    return results


# ── Pretty print ──────────────────────────────────────────────


def print_backtest(results: dict) -> None:
    if "error" in results:
        print(f"Error: {results['error']}")
        return

    print(f"\n{'='*60}")
    print(f"  NHL MODEL BACKTEST -- {results.get('season', '?')} Season")
    print(f"{'='*60}")
    print(f"  Games tested:  {results['games_tested']}")
    print(f"  Games skipped: {results['games_skipped']}")
    print(f"  Data source:   {results.get('source', 'unknown')}")
    print()

    for name, label in [("moneyline", "Moneyline"), ("over_under", "Over/Under"),
                         ("puck_line", "Puck Line")]:
        bt = results[name]
        if bt["total_bets"] == 0:
            print(f"  {label}: No qualifying bets")
            continue
        status = "PROFITABLE" if bt["profit"] > 0 else "LOSING"
        print(f"  {label}:")
        print(f"    Record: {bt['wins']}-{bt['losses']} ({bt['win_pct']}%)")
        print(f"    Profit: ${bt['profit']:+.2f} per $100 flat bets")
        print(f"    ROI: {bt['roi']:+.1f}% [{status}]")
        print()

    bb = results.get("best_bet", {})
    if bb.get("total_bets", 0) > 0:
        print(f"  Best Bet (1 per game):")
        print(f"    Record: {bb['wins']}-{bb['losses']} ({bb['win_pct']}%)")
        print(f"    Profit: ${bb['profit']:+.2f}")
        print(f"    ROI: {bb['roi']:+.1f}%")
        print()

    # Calibration
    cal = results.get("calibration", {})
    if cal:
        print(f"  Calibration:")
        for bucket in sorted(cal.keys()):
            c = cal[bucket]
            print(f"    {bucket}%: {c['correct']}/{c['total']} "
                  f"({c['actual_pct']}% actual)")
        print()

    print(f"{'='*60}")


# ── CLI ───────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.WARNING)

    args = sys.argv[1:]
    season = None
    min_edge = 3.0
    days_val = 30
    pit = True
    run_thresholds = False

    i = 0
    while i < len(args):
        if args[i] == "--season" and i + 1 < len(args):
            season = int(args[i + 1])
            i += 2
        elif args[i] == "--min-edge" and i + 1 < len(args):
            min_edge = float(args[i + 1])
            i += 2
        elif args[i] == "--days" and i + 1 < len(args):
            days_val = int(args[i + 1])
            i += 2
        elif args[i] == "--no-pit":
            pit = False
            i += 1
        elif args[i] == "--thresholds":
            run_thresholds = True
            i += 1
        else:
            i += 1

    if run_thresholds:
        print(f"Running edge threshold analysis (season={season})...",
              flush=True)
        th_results = analyze_edge_thresholds(days=days_val, season=season,
                                             pit_mode=pit)
        print(f"\n{'='*70}")
        print(f"  EDGE THRESHOLD ANALYSIS")
        print(f"{'='*70}")
        for r in th_results:
            bb = r.get("best_bet", {})
            print(f"  {r['threshold']:>2}% min edge: "
                  f"{bb.get('bets', 0):>4} bets | "
                  f"{bb.get('win_pct', 0):>5.1f}% win | "
                  f"ROI {bb.get('roi', 0):>+6.1f}% | "
                  f"P/L ${bb.get('profit', 0):>+8.2f}")
        print(f"{'='*70}")
    else:
        print(f"Running NHL backtest (days={days_val}, min_edge={min_edge}%, "
              f"pit={'on' if pit else 'off'})...", flush=True)
        results = run_nhl_backtest(days=days_val, min_edge=min_edge,
                                   season=season, pit_mode=pit)
        print_backtest(results)
