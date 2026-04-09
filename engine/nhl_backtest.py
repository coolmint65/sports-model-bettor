"""
NHL Model Backtester.

Runs the NHL prediction model against historical games with:
- DB-backed games from nhl_games table (primary)
- On-the-fly ESPN API fetch when DB is empty (fallback)
- Per-category results (ML, O/U, PL all shown independently)
- Realistic NHL odds (-150 avg favorite, -110 for PL/OU)

The output dict is compatible with the Backtest.jsx frontend component.

Usage:
    python -m engine.nhl_backtest                    # Last 30 days
    python -m engine.nhl_backtest --days 60           # Last 60 days
    python -m engine.nhl_backtest --season 2025       # Full season
    python -m engine.nhl_backtest --min-edge 3        # Only 3%+ edge bets
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
                     season: int | None = None) -> dict:
    """Run backtest on historical NHL games.

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

    # Debug: track data source
    game_dates = [g.get("date", "?") for g in games[:3]]
    game_seasons = [g.get("season", "?") for g in games[:3]]

    results = {
        "season": yr,
        "source": source,
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

    for game in games:
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

        # Run prediction model
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

        # ── Moneyline ──
        fav_home = p_home > p_away
        if fav_home:
            ml_prob = p_home
            ml_implied = _implied(AVG_FAV_ODDS)
            ml_odds = AVG_FAV_ODDS
            ml_pick = home_abbr
        else:
            ml_prob = p_away
            ml_implied = _implied(AVG_DOG_ODDS)
            ml_odds = AVG_DOG_ODDS
            ml_pick = away_abbr

        ml_edge = (ml_prob - ml_implied) * 100
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

        # ── Over/Under ──
        pred_total = home_xg + away_xg
        # Round to nearest 0.5 for the OU line
        ou_line = round(pred_total * 2) / 2

        # Use Poisson matrix for exact probability
        matrix = _score_matrix(home_xg, away_xg)
        p_over = 0.0
        for h in range(MAX_GOALS + 1):
            for a in range(MAX_GOALS + 1):
                # NHL OT adds exactly 1 goal for ties
                eff_total = (h + a + 1) if h == a else (h + a)
                if eff_total > ou_line:
                    p_over += matrix[h][a]

        ou_pick_over = p_over > 0.50
        ou_prob = p_over if ou_pick_over else (1 - p_over)
        ou_implied = _implied(OU_ODDS)
        ou_edge = (ou_prob - ou_implied) * 100

        if ou_edge >= min_edge:
            if actual_total == ou_line:
                results["over_under"]["pushes"] += 1
            elif ou_pick_over:
                ou_correct = actual_total > ou_line
                _record_bet(results["over_under"], ou_correct, OU_ODDS)
                game_bets.append((ou_edge, ou_correct, OU_ODDS, "O/U",
                                  f"{'Over' if ou_pick_over else 'Under'} {ou_line}"))
            else:
                ou_correct = actual_total < ou_line
                _record_bet(results["over_under"], ou_correct, OU_ODDS)
                game_bets.append((ou_edge, ou_correct, OU_ODDS, "O/U",
                                  f"Under {ou_line}"))

        # ── Puck Line (-1.5) ──
        p_home_cover = 0.0
        for h in range(MAX_GOALS + 1):
            for a in range(MAX_GOALS + 1):
                if (h - a) >= 2:
                    p_home_cover += matrix[h][a]
        p_away_cover = 1 - p_home_cover  # Away +1.5

        pl_pick_home = p_home_cover > 0.50
        pl_prob = p_home_cover if pl_pick_home else p_away_cover
        pl_implied = _implied(PL_ODDS)
        pl_edge = (pl_prob - pl_implied) * 100

        if pl_edge >= min_edge:
            if pl_pick_home:
                pl_correct = margin >= 2
                pl_pick = f"{home_abbr} -1.5"
            else:
                pl_correct = margin <= 1
                pl_pick = f"{away_abbr} +1.5"
            _record_bet(results["puck_line"], pl_correct, PL_ODDS)
            game_bets.append((pl_edge, pl_correct, PL_ODDS, "PL", pl_pick))

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
        else:
            i += 1

    print(f"Running NHL backtest (days={days_val}, min_edge={min_edge}%)...",
          flush=True)
    results = run_nhl_backtest(days=days_val, min_edge=min_edge, season=season)
    print_backtest(results)
