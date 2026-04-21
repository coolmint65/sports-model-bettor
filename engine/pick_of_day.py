"""
Pick of the Day - single highest-conviction play per sport per day.

Unlike the regular pick tracker (which can record many picks), POTD is
the ONE pick the model is most confident in. It's locked the first time
it's generated for a given date and never changes - so we can measure
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
    # Migration: add closing_odds + closing_odds_updated_at columns to
    # existing tables. Captured on each sync run while the POTD is still
    # pending so the latest pre-settle value is effectively the closing
    # line. CLV is computed lazily from (odds, closing_odds) -- no need
    # to persist it.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(pick_of_day)").fetchall()}
    if "closing_odds" not in cols:
        conn.execute("ALTER TABLE pick_of_day ADD COLUMN closing_odds INTEGER")
    if "closing_odds_updated_at" not in cols:
        conn.execute("ALTER TABLE pick_of_day ADD COLUMN closing_odds_updated_at TEXT")
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


def _get_market_win_rate(sport: str, bet_type: str, days: int = 30) -> float:
    """Get recent win rate for a bet type from the tracker DB.

    Returns win rate (0.0-1.0) or 0.5 if insufficient data.
    """
    try:
        conn = _get_conn(sport)
        table = {"mlb": "picks", "nhl": "nhl_picks", "nba": "nba_picks"}.get(sport, "picks")
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        row = conn.execute(
            f"SELECT COUNT(*) as total, "
            f"SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins "
            f"FROM {table} WHERE bet_type = ? AND date >= ? AND result IS NOT NULL",
            (bet_type, cutoff),
        ).fetchone()
        total = row["total"] if row else 0
        wins = row["wins"] if row else 0
        if total >= 5:
            return wins / total
    except Exception:
        pass
    return 0.5  # neutral when no data


def _score_pick(pick: dict, sport: str = "mlb") -> float:
    """Score a pick candidate for POTD selection.

    Ranking: edge × CI confidence × market reliability

    - edge: raw edge percentage (higher = better)
    - CI confidence: 1.0 / (1.0 + ci_half_width * 10). Narrow CI = more
      trustworthy. Picks with wide confidence bands score lower.
    - market reliability: recent win rate of this bet type from tracker.
      Markets that have been hitting recently get a boost.

    Also applies a penalty for extreme longshots (implied < 35%) where
    the model's calibration is least accurate.
    """
    edge = pick.get("edge", 0)
    prob = pick.get("prob", 0)
    odds = pick.get("odds", 0)
    ci_hw = pick.get("ci_half_width", 0.05)

    # CI confidence: narrow band = high confidence
    ci_confidence = 1.0 / (1.0 + ci_hw * 10)

    # Market reliability: how has this bet type been performing?
    bet_type = pick.get("type", "")
    market_wr = _get_market_win_rate(sport, bet_type)
    # Scale: 0.5 WR = 1.0x (neutral), 0.6 = 1.2x, 0.4 = 0.8x
    market_reliability = market_wr * 2.0

    # Longshot penalty: implied probability below 35% gets discounted
    implied = 0.5
    if odds and odds != 0:
        implied = abs(odds) / (abs(odds) + 100) if odds < 0 else 100 / (odds + 100)
    longshot_penalty = 1.0 if implied >= 0.35 else implied / 0.35

    return edge * ci_confidence * market_reliability * longshot_penalty


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

    # Rank by edge × CI confidence × market reliability
    candidates.sort(key=lambda c: _score_pick(c, sport), reverse=True)
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


_BET_TYPE_LABELS = {
    "ML": "moneyline", "RL": "run line", "PL": "puck line",
    "O/U": "over/under", "1st INN": "first inning",
    "ALT RL": "alt run line", "ALT O/U": "alt over/under",
    "ALT PL": "alt puck line",
    "Q1_ML": "Q1 moneyline", "Q1_SPREAD": "Q1 spread",
    "Q1_TOTAL": "Q1 total",
    "F5 ML": "first 5 moneyline", "F5 OU": "first 5 over/under",
}


def _build_reasoning(pick: dict, sport: str) -> str:
    """Generate a human-readable explanation for the POTD selection."""
    edge = pick.get("edge", 0)
    prob = pick.get("prob", 0)
    bet_type = pick.get("type", "")
    bt_label = _BET_TYPE_LABELS.get(bet_type, bet_type.replace("_", " "))

    strength = "strong" if edge > 8 else "moderate" if edge > 5 else "lean"

    pick_display = pick.get("pick_full", pick.get("pick", ""))
    implied = _implied_from_odds(pick.get("odds", 0)) * 100

    parts = []
    parts.append(f"Best {bt_label} play on today's {sport.upper()} slate.")
    parts.append(f"Model gives {pick_display} a {prob * 100:.1f}% chance "
                 f"vs the market's {implied:.1f}%.")
    parts.append(f"That's a {edge:+.1f}% edge with {strength} conviction.")
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

    Once created, POTD is locked for the day - subsequent calls return
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

    # No POTD yet - select one
    selected = select_potd(sport, games_with_bets)
    if not selected:
        return None

    # Lock it in - store full team names for display
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


def update_potd_closing_odds(sport: str) -> dict:
    """Refresh closing_odds on every un-settled POTD with the latest line.

    The pick selection itself stays locked from get_or_create_potd();
    this only updates the price we'll use for CLV measurement at settle
    time. Designed to be called on every sync run -- since each call
    overwrites with the freshest available price, the LAST update before
    settle_potd() runs is effectively the closing line we record.

    Supports mlb, nhl, nba. Dispatches to per-sport resolvers because
    each sport sources odds from a different module (MLB: engine.picks,
    NHL: inline odds-api fetch, NBA: scrapers.nba_odds).
    """
    if sport not in ("mlb", "nhl", "nba"):
        return {"updated": 0, "skipped": 0, "reason": f"unsupported sport {sport!r}"}

    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    pending = conn.execute(
        "SELECT id, game_id, matchup, bet_type, pick FROM pick_of_day "
        "WHERE result IS NULL"
    ).fetchall()
    if not pending:
        return {"updated": 0, "skipped": 0}

    resolver = {
        "mlb": _resolve_mlb_closing_for_pending,
        "nhl": _resolve_nhl_closing_for_pending,
        "nba": _resolve_nba_closing_for_pending,
    }[sport]

    try:
        pairs = resolver(pending)  # list of (pending_row_id, closing_price)
    except Exception as e:
        logger.warning("%s POTD closing-odds resolver crashed: %s", sport, e)
        return {"updated": 0, "skipped": len(pending), "reason": str(e)}

    updated = 0
    for pid, closing in pairs:
        if closing is None:
            continue
        conn.execute(
            "UPDATE pick_of_day SET closing_odds = ?, "
            "       closing_odds_updated_at = datetime('now') "
            "WHERE id = ?",
            (int(closing), pid),
        )
        updated += 1

    skipped = len(pending) - updated
    if updated:
        conn.commit()
        logger.info("POTD closing odds (%s): updated %d, skipped %d",
                    sport, updated, skipped)
    return {"updated": updated, "skipped": skipped}


def _resolve_mlb_closing_for_pending(pending: list) -> list[tuple]:
    """Per-POTD (id, closing_price) pairs for MLB."""
    from .picks import fetch_real_odds_for_games, match_odds
    from .tracker import _extract_closing_for_pick
    all_odds = fetch_real_odds_for_games() or {}
    if not all_odds:
        return [(r["id"], None) for r in pending]

    out = []
    for row in pending:
        row = dict(row)
        a_abbr, h_abbr = _matchup_to_abbrs(row["matchup"])
        if not (a_abbr and h_abbr):
            out.append((row["id"], None))
            continue
        game_odds = match_odds(h_abbr, a_abbr, all_odds) or {}
        closing = _extract_closing_for_pick(
            row["bet_type"], row["pick"], h_abbr, game_odds,
        ) if game_odds else None
        out.append((row["id"], closing))
    return out


def _resolve_nhl_closing_for_pending(pending: list) -> list[tuple]:
    """Per-POTD (id, closing_price) pairs for NHL.

    NHL odds are fetched inline via the-odds-api. Shape mirrors the MLB
    odds dict so the same bet-type -> field extractor can be reused.
    """
    all_odds = _fetch_nhl_odds_map()
    if not all_odds:
        return [(r["id"], None) for r in pending]

    out = []
    for row in pending:
        row = dict(row)
        a_abbr, h_abbr = _matchup_to_abbrs(row["matchup"], sport="nhl")
        if not (a_abbr and h_abbr):
            out.append((row["id"], None))
            continue
        game_odds = _lookup_by_abbr_with_aliases(
            h_abbr, a_abbr, all_odds, sport="nhl",
        ) or {}
        closing = _nhl_closing_for_bet_type(
            row["bet_type"], row["pick"], h_abbr, game_odds,
        ) if game_odds else None
        out.append((row["id"], closing))
    return out


def _resolve_nba_closing_for_pending(pending: list) -> list[tuple]:
    """Per-POTD (id, closing_price) pairs for NBA Q1 markets."""
    try:
        from scrapers.nba_odds import fetch_nba_odds
    except Exception as e:
        logger.warning("fetch_nba_odds unavailable: %s", e)
        return [(r["id"], None) for r in pending]

    all_odds = fetch_nba_odds() or {}
    if not all_odds:
        return [(r["id"], None) for r in pending]

    out = []
    for row in pending:
        row = dict(row)
        a_abbr, h_abbr = _matchup_to_abbrs(row["matchup"], sport="nba")
        if not (a_abbr and h_abbr):
            out.append((row["id"], None))
            continue
        game_odds = _lookup_by_abbr_with_aliases(
            h_abbr, a_abbr, all_odds, sport="nba",
        ) or {}
        closing = _nba_closing_for_bet_type(
            row["bet_type"], row["pick"], h_abbr, game_odds,
        ) if game_odds else None
        out.append((row["id"], closing))
    return out


def _fetch_nhl_odds_map() -> dict:
    """Pull current NHL h2h/spreads/totals from the-odds-api, keyed by AWAY@HOME.

    Duplicates the inline fetch in engine/nhl_tracker but returns the map
    in a shape compatible with the MLB odds-dict convention (so the
    bet-type extractor can work on it unmodified).
    """
    import os, urllib.request, json as _json
    from pathlib import Path as _Path
    key_file = _Path(__file__).resolve().parent.parent / "data" / "odds_api_key.txt"
    api_key = (os.environ.get("ODDS_API_KEY")
               or (key_file.read_text().strip() if key_file.exists() else None))
    if not api_key:
        return {}
    url = ("https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds/"
           f"?apiKey={api_key}&regions=us&markets=h2h,spreads,totals"
           "&oddsFormat=american&bookmakers=draftkings")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "POTDClosing/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = _json.loads(resp.read().decode())
    except Exception as e:
        logger.warning("NHL odds fetch failed: %s", e)
        return {}

    _NHL_TEAM_ABBR = {
        "Anaheim Ducks": "ANA", "Utah Hockey Club": "UTA",
        "Boston Bruins": "BOS", "Buffalo Sabres": "BUF",
        "Calgary Flames": "CGY", "Carolina Hurricanes": "CAR",
        "Chicago Blackhawks": "CHI", "Colorado Avalanche": "COL",
        "Columbus Blue Jackets": "CBJ", "Dallas Stars": "DAL",
        "Detroit Red Wings": "DET", "Edmonton Oilers": "EDM",
        "Florida Panthers": "FLA", "Los Angeles Kings": "LAK",
        "Minnesota Wild": "MIN", "Montreal Canadiens": "MTL",
        "Nashville Predators": "NSH", "New Jersey Devils": "NJD",
        "New York Islanders": "NYI", "New York Rangers": "NYR",
        "Ottawa Senators": "OTT", "Philadelphia Flyers": "PHI",
        "Pittsburgh Penguins": "PIT", "San Jose Sharks": "SJS",
        "Seattle Kraken": "SEA", "St. Louis Blues": "STL",
        "Tampa Bay Lightning": "TBL", "Toronto Maple Leafs": "TOR",
        "Vancouver Canucks": "VAN", "Vegas Golden Knights": "VGK",
        "Washington Capitals": "WSH", "Winnipeg Jets": "WPG",
    }
    out: dict = {}
    for g in data or []:
        home = g.get("home_team", "")
        away = g.get("away_team", "")
        h_ab = _NHL_TEAM_ABBR.get(home, home[:3].upper())
        a_ab = _NHL_TEAM_ABBR.get(away, away[:3].upper())
        parsed = {"provider": "DraftKings"}
        for book in g.get("bookmakers", []):
            for market in book.get("markets", []):
                mkey = market.get("key")
                for o in market.get("outcomes", []):
                    price = o.get("price")
                    point = o.get("point")
                    name = o.get("name", "")
                    if mkey == "h2h":
                        if name == home:
                            parsed["home_ml"] = price
                        elif name == away:
                            parsed["away_ml"] = price
                    elif mkey == "spreads":
                        if name == home:
                            parsed["home_spread_odds"] = price
                            parsed["home_spread_point"] = point
                        elif name == away:
                            parsed["away_spread_odds"] = price
                            parsed["away_spread_point"] = point
                    elif mkey == "totals":
                        if "over" in name.lower():
                            parsed["over_odds"] = price
                            parsed["over_under"] = point
                        elif "under" in name.lower():
                            parsed["under_odds"] = price
        if parsed.get("home_ml"):
            out[f"{a_ab}@{h_ab}"] = parsed
    return out


def _lookup_by_abbr_with_aliases(h_abbr: str, a_abbr: str,
                                  odds_map: dict, sport: str) -> dict:
    """Abbreviation-alias-aware lookup. Mirrors engine.picks.match_odds()
    but works for NHL/NBA via the canonical alias table in engine.abbr."""
    try:
        from .abbr import alt_abbr
    except Exception:
        return odds_map.get(f"{a_abbr}@{h_abbr}", {})
    h_alt = alt_abbr(h_abbr, sport)
    a_alt = alt_abbr(a_abbr, sport)
    for a, h in ((a_abbr, h_abbr), (a_alt, h_alt), (a_alt, h_abbr), (a_abbr, h_alt)):
        row = odds_map.get(f"{a}@{h}")
        if row:
            return row
    return {}


def _nhl_closing_for_bet_type(bet_type: str, pick: str,
                               home_abbr: str, game_odds: dict) -> int | None:
    """Pick out the right NHL price for the pick's bet_type (O/U, PL, ML)."""
    if not game_odds:
        return None
    bt = bet_type
    pk = pick or ""
    if bt in ("ml", "ML"):
        return (game_odds.get("home_ml") if pk == home_abbr
                else game_odds.get("away_ml"))
    if bt in ("ou", "O/U"):
        return (game_odds.get("over_odds") if "Over" in pk
                else game_odds.get("under_odds"))
    if bt in ("pl", "PL", "rl", "RL"):
        pick_team = pk.split()[0] if pk.split() else ""
        return (game_odds.get("home_spread_odds") if pick_team == home_abbr
                else game_odds.get("away_spread_odds"))
    return None


def _nba_closing_for_bet_type(bet_type: str, pick: str,
                               home_abbr: str, game_odds: dict) -> int | None:
    """Pick out the right NBA Q1 price for the pick's bet_type."""
    if not game_odds:
        return None
    bt = bet_type
    pk = pick or ""
    if bt == "Q1_ML":
        return (game_odds.get("home_ml") if pk.startswith(home_abbr)
                else game_odds.get("away_ml"))
    if bt == "Q1_SPREAD":
        # Pick format: "BOS +2.5 Q1" or "LAL -2.5 Q1". Home vs away by
        # leading team abbreviation.
        pick_home = pk.startswith(home_abbr)
        return (game_odds.get("q1_spread_home_odds") if pick_home
                else game_odds.get("q1_spread_away_odds"))
    if bt == "Q1_TOTAL":
        return (game_odds.get("q1_over_odds") if "Over" in pk
                else game_odds.get("q1_under_odds"))
    return None


def _matchup_to_abbrs(matchup: str, sport: str = "mlb") -> tuple[str | None, str | None]:
    """Best-effort split of the matchup string into (away_abbr, home_abbr).

    Handles both ``"BOS @ NYY"`` and ``"Boston Red Sox at New York Yankees"``
    by looking up team names in the sport-specific DB. Returns
    (None, None) when parsing fails so the caller can skip silently.
    """
    if not matchup:
        return None, None
    if " @ " in matchup:
        parts = matchup.split(" @ ", 1)
        return parts[0].strip(), parts[1].strip()
    if " at " not in matchup:
        return None, None
    try:
        away_name, home_name = matchup.split(" at ", 1)
        if sport == "mlb":
            from .db import get_conn as _gc
            table = "teams"
        elif sport == "nhl":
            from .nhl_db import get_conn as _gc
            table = "nhl_teams"
        elif sport == "nba":
            from .nba_db import get_conn as _gc
            table = "nba_teams"
        else:
            return None, None
        c = _gc()
        arow = c.execute(f"SELECT abbreviation FROM {table} WHERE name = ?",
                         (away_name.strip(),)).fetchone()
        hrow = c.execute(f"SELECT abbreviation FROM {table} WHERE name = ?",
                         (home_name.strip(),)).fetchone()
        if arow and hrow:
            return arow["abbreviation"], hrow["abbreviation"]
    except Exception:
        pass
    return None, None


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

    # Parse matchup - handles both "AWAY @ HOME" and "Away Team at Home Team"
    try:
        if " @ " in matchup:
            away_part, home_part = [s.strip() for s in matchup.split(" @ ")]
        elif " at " in matchup:
            away_part, home_part = [s.strip() for s in matchup.split(" at ")]
        else:
            return None, 0
    except ValueError:
        return None, 0

    # Find the game - try matching by game_id first, then by date + team names
    game_id = potd.get("game_id")

    if sport == "mlb":
        # Try game_id match first
        row = None
        if game_id:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM games g
                LEFT JOIN teams ht ON g.home_team_id = ht.mlb_id
                LEFT JOIN teams at ON g.away_team_id = at.mlb_id
                WHERE g.mlb_game_id = ? AND g.status = 'final'
                LIMIT 1
            """, (game_id,)).fetchone()
        # Fallback: search by date and team name substring
        if not row:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM games g
                LEFT JOIN teams ht ON g.home_team_id = ht.mlb_id
                LEFT JOIN teams at ON g.away_team_id = at.mlb_id
                WHERE g.date = ? AND g.status = 'final'
                  AND (ht.name LIKE ? OR ht.abbreviation = ?)
                  AND (at.name LIKE ? OR at.abbreviation = ?)
                LIMIT 1
            """, (date, f"%{home_part}%", home_part, f"%{away_part}%", away_part)).fetchone()
    elif sport == "nhl":
        row = None
        if game_id:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM nhl_games g
                LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nhl_teams at ON g.away_team_id = at.id
                WHERE g.game_id = ? AND g.status = 'final'
                LIMIT 1
            """, (game_id,)).fetchone()
        if not row:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM nhl_games g
                LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nhl_teams at ON g.away_team_id = at.id
                WHERE g.date = ? AND g.status = 'final'
                  AND (ht.name LIKE ? OR ht.abbreviation = ?)
                  AND (at.name LIKE ? OR at.abbreviation = ?)
                LIMIT 1
            """, (date, f"%{home_part}%", home_part, f"%{away_part}%", away_part)).fetchone()
    elif sport == "nba":
        row = None
        if game_id:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                FROM nba_games g
                LEFT JOIN nba_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nba_teams at ON g.away_team_id = at.id
                WHERE g.game_id = ? AND g.status = 'final'
                LIMIT 1
            """, (game_id,)).fetchone()
        # Fallback by date + team names -- the ESPN game_id stored on
        # the POTD doesn't match the nba_games primary key, so for NBA
        # this path is the usual one that resolves.
        if not row:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                FROM nba_games g
                LEFT JOIN nba_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nba_teams at ON g.away_team_id = at.id
                WHERE g.date = ? AND g.status = 'final'
                  AND (ht.name LIKE ? OR ht.abbreviation = ?)
                  AND (at.name LIKE ? OR at.abbreviation = ?)
                LIMIT 1
            """, (date, f"%{home_part}%", home_part, f"%{away_part}%", away_part)).fetchone()
    else:
        return None, 0

    if not row:
        # Differentiate "game not finished yet" from "we can't find
        # this game at all" -- the latter usually means a date/team
        # -name mismatch between the POTD row and the games table,
        # which is silent without a log.
        logger.debug(
            "POTD settle: no final game row for %s %s / %s (game_id=%s, "
            "home_part=%r, away_part=%r)",
            sport, date, matchup, game_id, home_part, away_part,
        )
        return None, 0

    row = dict(row)
    hs = row.get("home_score", 0) or 0
    as_ = row.get("away_score", 0) or 0

    # Compute result based on bet type
    result = None

    home_abbr = row.get("home_abbr", "")
    away_abbr = row.get("away_abbr", "")

    if bet_type == "ML":
        home_won = hs > as_
        # Pick can be abbreviation ("STL") or full name ("St. Louis Cardinals")
        pick_home = (pick == home_abbr
                     or home_part in pick
                     or (home_abbr and home_abbr in pick))
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

    elif bet_type in ("RL", "PL"):
        # Parse spread from pick string (e.g. "St. Louis Cardinals +1.5" or "STL +1.5")
        import re
        spread_match = re.search(r'([+-]?\d+\.?\d*)\s*$', pick)
        spread = float(spread_match.group(1)) if spread_match else 1.5

        # Determine which team was picked - check if home team name/abbr is in pick
        pick_is_home = (home_abbr and home_abbr in pick) or (home_part and home_part in pick)
        if pick_is_home:
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
        # Use linescore if available (MLB) - inning 1 runs
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

    elif bet_type == "Q1_ML":
        # Pick format: "BOS Q1 ML" or "Boston Celtics Q1 ML" (team name
        # resolved via _pick_full_name at create time).
        hq1 = row.get("home_q1", 0) or 0
        aq1 = row.get("away_q1", 0) or 0
        pick_home = ((home_abbr and home_abbr in pick)
                     or (home_part and home_part in pick))
        home_won_q1 = hq1 > aq1
        if hq1 == aq1:
            result = "P"
        else:
            won = (pick_home and home_won_q1) or (not pick_home and not home_won_q1)
            result = "W" if won else "L"

    elif bet_type == "Q1_SPREAD":
        # Pick format: "BOS -2.5 Q1" or "Boston Celtics -2.5 Q1".
        import re as _re
        hq1 = row.get("home_q1", 0) or 0
        aq1 = row.get("away_q1", 0) or 0
        # Spread is the first +/-N.N appearing anywhere in the pick.
        m = _re.search(r"([+-]\d+\.?\d*)", pick)
        spread = float(m.group(1)) if m else 0.0
        pick_home = ((home_abbr and home_abbr in pick)
                     or (home_part and home_part in pick))
        if pick_home:
            margin = hq1 - aq1
        else:
            margin = aq1 - hq1
        covered = margin + spread > 0
        pushed = margin + spread == 0
        if pushed:
            result = "P"
        else:
            result = "W" if covered else "L"

    elif bet_type == "Q1_TOTAL":
        # Pick format: "Over 55.5 Q1" or "Under 55.5 Q1".
        hq1 = row.get("home_q1", 0) or 0
        aq1 = row.get("away_q1", 0) or 0
        q1_total = hq1 + aq1
        import re as _re
        m = _re.search(r"(\d+\.?\d*)", pick)
        line = float(m.group(1)) if m else 0.0
        is_over = "Over" in pick or "over" in pick
        if q1_total == line:
            result = "P"
        elif is_over:
            result = "W" if q1_total > line else "L"
        else:
            result = "W" if q1_total < line else "L"

    if result is None:
        # Row was found but no outcome handler matched the bet_type.
        # Without logging, the POTD silently stays pending forever.
        logger.warning(
            "POTD settle: no outcome handler for bet_type=%r on %s %s/%s",
            bet_type, sport, date, matchup,
        )
        return None, 0

    return result, _profit_on_settled(result, odds)


def _profit_on_settled(result: str | None, odds: int | float | None) -> float:
    """$100-unit profit formula used by settle_potd. Exposed so
    recalc_potd_profit can rewrite stored profits to match current code
    when an older run left them on a different unit sizing."""
    if result == "W":
        o = odds if odds is not None else -110
        return round(o if o > 0 else 10000 / abs(o), 2)
    if result == "L":
        return -100.0
    return 0.0


def recalc_potd_profit(sport: str) -> dict:
    """Rewrite the ``profit`` column on every settled POTD row using
    the current $100-unit formula. No-op on pending rows (result NULL).

    Purpose: historical rows may have been saved under an older unit
    sizing (e.g. a Kelly-fraction dollarization that used a $1000 or
    $5000 bankroll instead of $100), so the POTD hero's running
    profit diverged from what the current code would compute. Running
    this once after a unit-sizing change realigns the ledger.

    Idempotent — running it twice produces the same values.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    rows = conn.execute(
        "SELECT id, result, odds, profit FROM pick_of_day "
        "WHERE result IN ('W', 'L', 'P')"
    ).fetchall()

    updated = 0
    for r in rows:
        new_profit = _profit_on_settled(r["result"], r["odds"])
        if r["profit"] is None or abs((r["profit"] or 0) - new_profit) > 0.01:
            conn.execute(
                "UPDATE pick_of_day SET profit = ? WHERE id = ?",
                (new_profit, r["id"]),
            )
            updated += 1

    conn.commit()
    return {"sport": sport, "settled_rows": len(rows), "updated": updated}


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
    """Fetch just today's POTD (doesn't create one).

    Annotates the response with a computed `clv` field when both odds
    and closing_odds are present, so the UI doesn't have to redo the
    arithmetic. Positive CLV = we got a better price than the close.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT * FROM pick_of_day WHERE date = ?", (target_date,)
    ).fetchone()
    if not row:
        return None
    out = dict(row)
    bet_odds = out.get("odds")
    close = out.get("closing_odds")
    if bet_odds and close:
        bet_imp = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 \
                  else 100 / (bet_odds + 100)
        close_imp = abs(close) / (abs(close) + 100) if close < 0 \
                    else 100 / (close + 100)
        out["clv"] = round((close_imp - bet_imp) * 100, 2)
    return out
