"""
Pick of the Day — single highest-conviction play per sport per day.

Unlike the regular pick tracker (which can record many picks), POTD is
the ONE pick the model is most confident in. It's locked the first time
it's generated for a given date and never changes — so we can measure
whether the model's top-conviction plays are actually its best bets.

Selection criteria:
1. Must be a historically profitable bet type:
   - MLB: RL or ML (1st INN and O/U excluded based on backtest)
   - NHL: O/U or PL (ML excluded based on backtest)
2. Must have real DK odds (not derived)
3. Must have meaningful edge (>= 5%)
4. Among qualifying picks: highest edge-adjusted expected value

Storage: dedicated table in each sport's DB so POTD history is separate
from regular picks and can be summarized independently.

Usage:
    from engine.pick_of_day import get_or_create_potd, settle_potd, get_potd_summary
    potd = get_or_create_potd("mlb", games_with_bets)  # Returns today's POTD
    summary = get_potd_summary("mlb")  # Returns running W/L/profit
"""

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# Bet types that have proven profitable in backtesting
MLB_ALLOWED_TYPES = {"RL", "ML"}
NHL_ALLOWED_TYPES = {"O/U", "PL"}
NBA_ALLOWED_TYPES = {"Q1_SPREAD", "Q1_TOTAL", "Q1_ML"}

MIN_EDGE = 5.0  # Minimum edge % to qualify as POTD


def _get_conn(sport: str):
    """Get DB connection for the given sport."""
    if sport == "mlb":
        from .db import get_conn
        return get_conn()
    elif sport == "nhl":
        from .nhl_db import get_conn
        return get_conn()
    elif sport == "nba":
        from .nba_db import get_conn
        return get_conn()
    else:
        raise ValueError(f"Unknown sport: {sport}")


def _ensure_potd_table(sport: str) -> None:
    """Create the POTD table if it doesn't exist."""
    conn = _get_conn(sport)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pick_of_day (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL UNIQUE,
            game_id     TEXT,
            matchup     TEXT NOT NULL,
            bet_type    TEXT NOT NULL,
            pick        TEXT NOT NULL,
            model_prob  REAL,
            edge        REAL,
            odds        INTEGER,
            kelly_pct   REAL,
            reasoning   TEXT,
            result      TEXT,
            profit      REAL,
            created_at  TEXT DEFAULT (datetime('now')),
            settled_at  TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_potd_date ON pick_of_day(date)")
    conn.commit()


def _kelly_fraction(prob: float, odds: int) -> float:
    """Quarter-Kelly fraction for bet sizing."""
    if not odds or prob is None or prob <= 0 or prob >= 1:
        return 0.0
    decimal = (odds / 100) + 1 if odds > 0 else (100 / abs(odds)) + 1
    b = decimal - 1
    if b <= 0:
        return 0.0
    q = 1 - prob
    kelly = (b * prob - q) / b
    if kelly <= 0:
        return 0.0
    return max(0.0, min(0.25, kelly * 0.25))


def _score_pick(pick: dict) -> float:
    """Score a pick candidate for POTD selection.

    Uses Kelly fraction * edge as the ranking metric — this favors picks
    that combine high edge with favorable bet sizing.
    """
    prob = pick.get("prob", 0)
    odds = pick.get("odds", 0)
    edge = pick.get("edge", 0)
    kelly = _kelly_fraction(prob, odds)
    # Score = kelly fraction × edge × 100, scaled
    return kelly * edge * 100


def select_potd(sport: str, games_with_bets: list[dict]) -> dict | None:
    """
    Select the Pick of the Day from a list of games with bets.

    Args:
        sport: "mlb" or "nhl"
        games_with_bets: list of bet dicts from /api/best-bets or /api/nhl/best-bets

    Returns:
        POTD dict or None if no qualifying picks
    """
    allowed_types = {"mlb": MLB_ALLOWED_TYPES, "nhl": NHL_ALLOWED_TYPES, "nba": NBA_ALLOWED_TYPES}.get(sport, MLB_ALLOWED_TYPES)

    candidates = []
    for game in games_with_bets:
        # Check all picks from this game, not just the "best_pick"
        all_picks = game.get("all_picks", [])
        if not all_picks and game.get("best_pick"):
            all_picks = [game["best_pick"]]

        for pick in all_picks:
            pick_type = pick.get("type", "")
            if pick_type not in allowed_types:
                continue
            if pick.get("edge", 0) < MIN_EDGE:
                continue
            if not pick.get("odds"):
                continue

            # Safety: verify the pick team is actually in this matchup
            # Skip check for O/U and total picks (they don't contain team names)
            pick_name = pick.get("pick", "")
            matchup = game.get("matchup", "")
            is_total_pick = any(x in pick_name for x in ("Over", "Under", "over", "under"))
            if not is_total_pick and pick_name and matchup and not any(
                abbr in pick_name for abbr in matchup.replace(" @ ", "|").split("|")
            ):
                logger.warning("POTD: pick '%s' not in matchup '%s', skipping", pick_name, matchup)
                continue

            candidates.append({
                "game_id": str(game.get("game_id", "")),
                "matchup": game.get("matchup", ""),
                "type": pick_type,
                "pick": pick.get("pick", ""),
                "prob": pick.get("prob", 0),
                "edge": pick.get("edge", 0),
                "odds": pick.get("odds", 0),
                "home": game.get("home", {}),
                "away": game.get("away", {}),
                "time": game.get("time", ""),
                "venue": game.get("venue", ""),
            })

    if not candidates:
        return None

    # Rank by Kelly × edge
    candidates.sort(key=_score_pick, reverse=True)
    best = candidates[0]

    # Resolve full team names for display
    home_info = best.get("home", {})
    away_info = best.get("away", {})
    home_name = home_info.get("name", home_info.get("abbreviation", "Home"))
    away_name = away_info.get("name", away_info.get("abbreviation", "Away"))
    best["matchup_full"] = f"{away_name} at {home_name}"

    # Resolve pick to full team name
    pick_str = best.get("pick", "")
    home_abbr = home_info.get("abbreviation", "")
    away_abbr = away_info.get("abbreviation", "")
    if home_abbr and pick_str.startswith(home_abbr):
        best["pick_full"] = pick_str.replace(home_abbr, home_name, 1)
    elif away_abbr and pick_str.startswith(away_abbr):
        best["pick_full"] = pick_str.replace(away_abbr, away_name, 1)
    else:
        best["pick_full"] = pick_str

    # Add Kelly fraction and confidence
    best["kelly_pct"] = round(_kelly_fraction(best["prob"], best["odds"]) * 100, 1)
    best["reasoning"] = _build_reasoning(best, sport)

    return best


def _build_reasoning(pick: dict, sport: str) -> str:
    """Generate a human-readable explanation for the POTD selection."""
    edge = pick.get("edge", 0)
    prob = pick.get("prob", 0)
    bet_type = pick.get("type", "")
    kelly = pick.get("kelly_pct", 0)

    strength = "strong" if edge > 8 else "moderate" if edge > 5 else "lean"

    pick_display = pick.get("pick_full", pick.get("pick", ""))

    parts = []
    parts.append(f"Highest-EV {bet_type} play on the {sport.upper()} slate today.")
    parts.append(f"Model gives {pick_display} a {prob * 100:.1f}% probability "
                 f"vs market implied ({_implied_from_odds(pick.get('odds', 0)) * 100:.1f}%).")
    parts.append(f"{edge:+.1f}% edge — {strength} conviction.")
    parts.append(f"Kelly suggests {kelly}% of bankroll.")
    return " ".join(parts)


def _implied_from_odds(odds: int) -> float:
    if not odds:
        return 0.5
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100 / (odds + 100)


def get_or_create_potd(sport: str, games_with_bets: list[dict],
                      date: str | None = None) -> dict | None:
    """
    Get today's POTD, creating it if it doesn't exist.

    Once created, POTD is locked for the day — subsequent calls return
    the same pick regardless of updated predictions.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    target_date = date or datetime.now().strftime("%Y-%m-%d")

    # Check for existing POTD for this date
    existing = conn.execute(
        "SELECT * FROM pick_of_day WHERE date = ?", (target_date,)
    ).fetchone()

    if existing:
        return dict(existing)

    # No POTD yet — select one
    selected = select_potd(sport, games_with_bets)
    if not selected:
        return None

    # Lock it in — store full team names for display
    conn.execute("""
        INSERT OR IGNORE INTO pick_of_day (
            date, game_id, matchup, bet_type, pick,
            model_prob, edge, odds, kelly_pct, reasoning
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        target_date,
        selected.get("game_id"),
        selected.get("matchup_full", selected.get("matchup")),
        selected.get("type"),
        selected.get("pick_full", selected.get("pick")),
        selected.get("prob"),
        selected.get("edge"),
        selected.get("odds"),
        selected.get("kelly_pct"),
        selected.get("reasoning"),
    ))
    conn.commit()

    logger.info("Created %s POTD for %s: %s %s (%s edge %+.1f%%)",
                sport.upper(), target_date, selected.get("matchup"),
                selected.get("pick"), selected.get("type"), selected.get("edge", 0))

    # Return with additional display fields
    result = dict(selected)
    result["date"] = target_date
    return result


def settle_potd(sport: str) -> dict:
    """Settle any pending POTDs whose games have completed."""
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    pending = conn.execute(
        "SELECT * FROM pick_of_day WHERE result IS NULL"
    ).fetchall()

    if not pending:
        return {"settled": 0}

    settled = 0
    wins = 0
    losses = 0

    for potd in pending:
        potd = dict(potd)
        result, profit = _determine_outcome(sport, conn, potd)

        if result is None:
            continue  # Game not finished yet

        conn.execute("""
            UPDATE pick_of_day SET result = ?, profit = ?, settled_at = datetime('now')
            WHERE id = ?
        """, (result, profit, potd["id"]))
        settled += 1
        if result == "W":
            wins += 1
        elif result == "L":
            losses += 1

    conn.commit()
    return {"settled": settled, "wins": wins, "losses": losses}


def _determine_outcome(sport: str, conn, potd: dict) -> tuple[str | None, float]:
    """
    Figure out whether a POTD won, lost, or pushed. Returns (result, profit).
    Returns (None, 0) if the game isn't finished yet.
    """
    date = potd["date"]
    matchup = potd["matchup"]
    bet_type = potd["bet_type"]
    pick = potd["pick"]
    odds = potd.get("odds") or -110

    # Parse matchup "AWAY @ HOME"
    try:
        away_abbr, home_abbr = [s.strip() for s in matchup.split("@")]
    except ValueError:
        return None, 0

    # Find the final game
    if sport == "mlb":
        row = conn.execute("""
            SELECT g.*,
                   ht.abbreviation as home_abbr,
                   at.abbreviation as away_abbr
            FROM games g
            LEFT JOIN teams ht ON g.home_team_id = ht.mlb_id
            LEFT JOIN teams at ON g.away_team_id = at.mlb_id
            WHERE g.date = ? AND g.status = 'final'
              AND (ht.abbreviation = ? OR at.abbreviation = ?)
              AND (ht.abbreviation = ? OR at.abbreviation = ?)
            LIMIT 1
        """, (date, home_abbr, home_abbr, away_abbr, away_abbr)).fetchone()
    else:  # NHL
        row = conn.execute("""
            SELECT g.*,
                   ht.abbreviation as home_abbr,
                   at.abbreviation as away_abbr
            FROM nhl_games g
            LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
            LEFT JOIN nhl_teams at ON g.away_team_id = at.id
            WHERE g.date = ? AND g.status = 'final'
              AND (ht.abbreviation = ? OR at.abbreviation = ?)
              AND (ht.abbreviation = ? OR at.abbreviation = ?)
            LIMIT 1
        """, (date, home_abbr, home_abbr, away_abbr, away_abbr)).fetchone()

    if not row:
        return None, 0

    row = dict(row)
    hs = row.get("home_score", 0) or 0
    as_ = row.get("away_score", 0) or 0

    # Compute result based on bet type
    result = None

    if bet_type == "ML":
        home_won = hs > as_
        pick_home = pick == home_abbr
        won = (pick_home and home_won) or (not pick_home and not home_won)
        result = "W" if won else "L"

    elif bet_type == "O/U":
        total = hs + as_
        if "Over" in pick:
            line = float(pick.split()[-1])
            if total > line:
                result = "W"
            elif total < line:
                result = "L"
            else:
                result = "P"
        else:
            line = float(pick.split()[-1])
            if total < line:
                result = "W"
            elif total > line:
                result = "L"
            else:
                result = "P"

    elif bet_type == "RL" or bet_type == "PL":
        # Parse "TEAM ±1.5"
        parts = pick.split()
        pick_team = parts[0]
        try:
            spread = float(parts[1])
        except (IndexError, ValueError):
            spread = 1.5

        if pick_team == home_abbr:
            margin = hs - as_
        else:
            margin = as_ - hs

        covered = margin + spread > 0
        pushed = margin + spread == 0
        if pushed:
            result = "P"
        else:
            result = "W" if covered else "L"

    elif bet_type == "1st INN":
        # Use linescore if available (MLB) — inning 1 runs
        import json as _json
        if sport == "mlb":
            home_ls = row.get("home_linescore")
            away_ls = row.get("away_linescore")
            try:
                h_inn = _json.loads(home_ls) if home_ls else []
                a_inn = _json.loads(away_ls) if away_ls else []
                scoreless = (len(h_inn) > 0 and len(a_inn) > 0
                             and h_inn[0] == 0 and a_inn[0] == 0)
            except (_json.JSONDecodeError, TypeError):
                scoreless = False
            if pick == "NRFI":
                result = "W" if scoreless else "L"
            else:
                result = "W" if not scoreless else "L"

    if result is None:
        return None, 0

    if result == "W":
        profit = odds if odds > 0 else 10000 / abs(odds)
    elif result == "L":
        profit = -100.0
    else:
        profit = 0.0

    return result, round(profit, 2)


def get_potd_summary(sport: str, limit: int = 30) -> dict:
    """Return running POTD totals + recent history."""
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    overall = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
            SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
            COALESCE(SUM(profit), 0) as profit
        FROM pick_of_day
    """).fetchone()

    overall = dict(overall)
    w = overall.get("wins") or 0
    l = overall.get("losses") or 0
    settled_total = w + l

    recent = conn.execute(
        "SELECT * FROM pick_of_day ORDER BY date DESC LIMIT ?", (limit,)
    ).fetchall()

    return {
        "total": overall.get("total") or 0,
        "wins": w,
        "losses": l,
        "pushes": overall.get("pushes") or 0,
        "pending": overall.get("pending") or 0,
        "profit": round(overall.get("profit") or 0, 2),
        "win_pct": round(w / settled_total * 100, 1) if settled_total > 0 else 0,
        "roi": round((overall.get("profit") or 0) / settled_total, 1) if settled_total > 0 else 0,
        "recent": [dict(r) for r in recent],
    }


def get_today_potd(sport: str, date: str | None = None) -> dict | None:
    """Fetch just today's POTD (doesn't create one)."""
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT * FROM pick_of_day WHERE date = ?", (target_date,)
    ).fetchone()
    return dict(row) if row else None
