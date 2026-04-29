"""
NHL Pick tracker - records model picks and settles them against results.

Usage:
    python -m engine.nhl_tracker --record     # Record today's picks
    python -m engine.nhl_tracker --settle     # Settle completed picks
    python -m engine.nhl_tracker --summary    # Print running totals
"""

import json
import logging
import sqlite3
import threading
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

_local = threading.local()

# Phase 1 derivative bet types — excluded from main NHL tracker
# recording at sync time. Derivatives flow through
# engine.derivative_tracker for isolated paper-bet evaluation.
_NHL_DERIVATIVE_TYPES: set[str] = {
    "Team Total", "Period Total", "Period BTS", "Period DNB",
    "Total O/E", "Overtime", "BTS",
}


def _core_picks(picks: list[dict]) -> list[dict]:
    return [p for p in picks if p.get("type") not in _NHL_DERIVATIVE_TYPES]


def _compute_clv(bet_odds, closing_odds):
    """Compute closing line value.
    Positive CLV = got better price than closing line = sharp.
    """
    if not bet_odds or not closing_odds:
        return None
    bet_implied = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 else 100 / (bet_odds + 100)
    close_implied = abs(closing_odds) / (abs(closing_odds) + 100) if closing_odds < 0 else 100 / (closing_odds + 100)
    return round((close_implied - bet_implied) * 100, 2)  # positive = we got a better price


def _extract_nhl_closing_for_pick(bet_type: str, pick_text: str,
                                  home_abbr: str, game_odds: dict) -> int | None:
    """Pure helper: pick the right closing-odds field for an NHL pick
    out of a Hard Rock odds bucket. Mirrors
    engine.tracker._extract_closing_for_pick for the NHL-specific
    bet_types (ML / PL / O/U / ALT PL)."""
    if not game_odds:
        return None
    pk = pick_text or ""
    parts = pk.split()
    if bet_type == "ML":
        return (game_odds.get("home_ml") if pk == home_abbr
                else game_odds.get("away_ml"))
    if bet_type in ("O/U", "OU"):
        return (game_odds.get("over_odds") if "Over" in pk
                else game_odds.get("under_odds"))
    if bet_type == "PL":
        pick_team = parts[0] if parts else ""
        return (game_odds.get("home_spread_odds") if pick_team == home_abbr
                else game_odds.get("away_spread_odds"))
    if bet_type == "ALT PL":
        # Match the alt by line if possible — otherwise fall back to
        # main spread odds (still better than skipping).
        try:
            line = float(parts[1]) if len(parts) >= 2 else None
        except (ValueError, IndexError):
            line = None
        pick_team = parts[0] if parts else ""
        is_home = pick_team == home_abbr
        if line is not None:
            for alt in game_odds.get("alt_spreads", []) or []:
                if alt.get("point") == line or alt.get("point") == -line:
                    return (alt.get("home_odds") if is_home
                            else alt.get("away_odds"))
        return (game_odds.get("home_spread_odds") if is_home
                else game_odds.get("away_spread_odds"))
    return None


def capture_closing_odds() -> int:
    """Snapshot current Hard Rock NHL odds for all pending picks.

    Mirrors engine.nba_tracker.capture_closing_odds. Call before games
    start (sync script ~30 min pre-puck) so the per-row CLV column
    populates once games settle. Without it the tracker shows a "-"
    in the CLV column on every row even though the summary widget has
    enough data to show an aggregate."""
    conn = _get_nhl_db()
    pending = conn.execute(
        "SELECT id, matchup, bet_type, pick FROM nhl_picks "
        "WHERE result IS NULL AND closing_odds IS NULL"
    ).fetchall()
    if not pending:
        return 0

    try:
        from scrapers.hardrock_odds import fetch_nhl as _hr_nhl
        all_odds = _hr_nhl() or {}
    except Exception as e:
        logger.warning("NHL closing capture: HR fetch failed: %s", e)
        return 0
    if not all_odds:
        return 0

    from .picks import match_odds as _match_odds
    updated = 0
    for pick in pending:
        pick = dict(pick)
        matchup = pick["matchup"]
        sep = " @ " if " @ " in matchup else "@"
        parts = matchup.split(sep)
        if len(parts) != 2:
            continue
        away, home = parts[0].strip(), parts[1].strip()
        game_odds = _match_odds(home, away, all_odds)
        if not game_odds:
            continue
        closing = _extract_nhl_closing_for_pick(
            pick["bet_type"], pick["pick"], home, game_odds,
        )
        if closing is not None:
            conn.execute(
                "UPDATE nhl_picks SET closing_odds = ? WHERE id = ?",
                (int(closing), pick["id"]),
            )
            updated += 1

    conn.commit()
    logger.info("NHL closing capture: %d/%d pending picks updated",
                updated, len(pending))
    return updated


def _get_nhl_db():
    """Get NHL picks DB connection (SQLite, separate from MLB)."""
    db_path = Path(__file__).resolve().parent.parent / "data" / "nhl.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        _local.conn = conn

    conn = _local.conn

    conn.execute("""
        CREATE TABLE IF NOT EXISTS nhl_picks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT,
            date TEXT NOT NULL,
            matchup TEXT,
            bet_type TEXT NOT NULL,
            pick TEXT NOT NULL,
            model_prob REAL,
            edge REAL,
            odds INTEGER,
            closing_odds INTEGER,
            result TEXT,
            profit REAL,
            created_at TEXT DEFAULT (datetime('now')),
            settled_at TEXT
        )
    """)

    # Migration: add closing_odds column to existing databases
    try:
        existing = conn.execute("PRAGMA table_info(nhl_picks)").fetchall()
        col_names = [r[1] for r in existing]
        if "closing_odds" not in col_names:
            conn.execute("ALTER TABLE nhl_picks ADD COLUMN closing_odds INTEGER")
    except Exception:
        pass  # Column already exists or table just created with it

    conn.commit()
    return conn


def _fetch_nhl_scoreboard(date: str) -> list[dict]:
    """Fetch NHL scoreboard from ESPN for a given date."""
    import urllib.request
    espn_date = date.replace("-", "")
    url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={espn_date}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("events", [])
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to fetch NHL scoreboard for %s: %s", date, e)
        return []


def refresh_pending_for_today(bets: list[dict],
                               target_date: str | None = None) -> dict:
    """NHL twin of engine.tracker.refresh_pending_for_today. See that
    docstring for the design rationale."""
    target_date = target_date or datetime.now().strftime("%Y-%m-%d")
    conn = _get_nhl_db()
    # Track locked matchups separately so the void path leaves their
    # pending picks alone — see engine.tracker for the bug history.
    current_by_matchup: dict[str, dict] = {}
    locked_matchups: set[str] = set()
    # Defense: also derive lock from the bets entry's own start time.
    # See engine.tracker for the rationale.
    from datetime import datetime as _dt, timezone as _tz
    now_utc = _dt.now(_tz.utc)
    def _bet_started(bet: dict) -> bool:
        if bet.get("is_locked"):
            return True
        t = bet.get("time") or bet.get("date") or ""
        if not isinstance(t, str) or not t:
            return False
        try:
            s = t.replace("Z", "+00:00") if t.endswith("Z") else t
            ts = _dt.fromisoformat(s)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=_tz.utc)
            return ts < now_utc
        except (ValueError, TypeError):
            return False

    for b in bets:
        if _bet_started(b):
            locked_matchups.add(b["matchup"])
            continue  # locked: tracker entry stays frozen
        bp = b.get("best_pick")
        if bp:
            current_by_matchup[b["matchup"]] = bp

    pending = conn.execute(
        "SELECT id, matchup, bet_type, pick, game_id FROM nhl_picks "
        "WHERE date = ? AND result IS NULL",
        (target_date,),
    ).fetchall()

    # Belt-and-suspenders DB-level lock check — see engine.tracker for
    # the full rationale. NHL games table uses ``game_id`` as PK.
    def _pick_game_started(game_id) -> bool:
        if not game_id:
            return False
        row = conn.execute(
            "SELECT date, status FROM nhl_games WHERE game_id = ? LIMIT 1",
            (game_id,),
        ).fetchone()
        if not row:
            return False
        if row["status"] in ("live", "final", "postponed"):
            return True
        try:
            return str(row["date"]) < target_date
        except Exception:
            return False

    updated = swapped = voided = 0
    for p in pending:
        p = dict(p)
        if p["matchup"] in locked_matchups:
            continue  # frozen at lock time
        if _pick_game_started(p.get("game_id")):
            continue  # game underway per DB; freeze regardless of bets dict
        current = current_by_matchup.get(p["matchup"])
        if not current:
            matchup_in_response = any(b["matchup"] == p["matchup"] for b in bets)
            if matchup_in_response:
                conn.execute("DELETE FROM nhl_picks WHERE id = ?", (p["id"],))
                voided += 1
            continue

        if current.get("type") != p["bet_type"] or current.get("pick") != p["pick"]:
            conn.execute(
                "UPDATE nhl_picks SET bet_type = ?, pick = ?, model_prob = ?, "
                "edge = ?, odds = ? WHERE id = ?",
                (current.get("type"), current.get("pick"),
                 current.get("prob"), current.get("edge"),
                 current.get("odds"), p["id"]),
            )
            swapped += 1
        else:
            conn.execute(
                "UPDATE nhl_picks SET model_prob = ?, edge = ?, odds = ? "
                "WHERE id = ?",
                (current.get("prob"), current.get("edge"),
                 current.get("odds"), p["id"]),
            )
            updated += 1

    conn.commit()
    return {"updated": updated, "swapped": swapped, "voided": voided}


def record_picks(date: str | None = None, min_edge: float = 1.5,
                 force: bool = False) -> list[dict]:
    """
    Run NHL model on today's games and record the best pick per game.

    Args:
        date: Target YYYY-MM-DD (defaults to today).
        min_edge: Minimum edge percentage to record a pick.
        force: If True, delete any unsettled pick for today before
            recording so the latest model/odds take precedence.
    """
    conn = _get_nhl_db()
    target_date = date or datetime.now().strftime("%Y-%m-%d")

    if force:
        conn.execute(
            "DELETE FROM nhl_picks WHERE date = ? AND result IS NULL",
            (target_date,),
        )
        conn.commit()

    from engine.nhl_predict import generate_nhl_picks
    from engine.data import list_teams, load_team

    # Build abbreviation -> key map (include ESPN alternate abbreviations)
    _ALT_ABBRS = {
        "TBL": "TB", "TB": "TBL", "NJD": "NJ", "NJ": "NJD",
        "SJS": "SJ", "SJ": "SJS", "LAK": "LA", "LA": "LAK",
        "WSH": "WAS", "WAS": "WSH", "CBJ": "CLB", "CLB": "CBJ",
        "MTL": "MON", "MON": "MTL", "NSH": "NAS", "NAS": "NSH",
        "UTA": "UTAH", "UTAH": "UTA",
    }
    key_map = {}
    for t in list_teams("NHL"):
        team = load_team("NHL", t["key"])
        if team:
            abbr = team.get("abbreviation", "")
            if abbr:
                key_map[abbr] = t["key"]
                # Add alternate abbreviation
                alt = _ALT_ABBRS.get(abbr)
                if alt:
                    key_map[alt] = t["key"]

    # Fetch today's games from ESPN
    events = _fetch_nhl_scoreboard(target_date)
    if not events:
        logger.info("No NHL games found for %s", target_date)
        return []

    # Fetch odds — use the same scraper chain as the backend
    # (Hard Rock primary, DK/Odds API fallback).
    odds_map = {}
    try:
        from scrapers.hardrock_odds import fetch_nhl as _hr_nhl
        hr_odds = _hr_nhl()
        if hr_odds:
            odds_map = hr_odds
            logger.info("NHL tracker: %d games from Hard Rock", len(odds_map))
    except Exception as e:
        logger.warning("NHL tracker Hard Rock failed: %s", e)

    if not odds_map:
        try:
            import os
            from pathlib import Path
            key_file = Path(__file__).resolve().parent.parent / "data" / "odds_api_key.txt"
            api_key = os.environ.get("ODDS_API_KEY") or (
                key_file.read_text().strip() if key_file.exists() else None)
            if api_key:
                import urllib.request
                url = (f"https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds/"
                       f"?apiKey={api_key}&regions=us&markets=h2h,spreads,totals"
                       f"&oddsFormat=american&bookmakers=draftkings")
                req = urllib.request.Request(url, headers={"User-Agent": "NHLTracker/1.0"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    odds_data = json.loads(resp.read().decode())
                for game in (odds_data or []):
                    home = game.get("home_team", "")
                    away = game.get("away_team", "")
                    h = home[:3].upper()
                    a = away[:3].upper()
                    key = f"{a}@{h}"
                    result = {"provider": "DraftKings"}
                    for book in game.get("bookmakers", [])[:1]:
                        for market in book.get("markets", []):
                            mkey = market.get("key", "")
                            for o in market.get("outcomes", []):
                                if mkey == "h2h":
                                    if o.get("name") == home:
                                        result["home_ml"] = o.get("price")
                                    elif o.get("name") == away:
                                        result["away_ml"] = o.get("price")
                                elif mkey == "spreads":
                                    if o.get("name") == home:
                                        result["home_spread_odds"] = o.get("price")
                                        result["home_spread_point"] = o.get("point")
                                    elif o.get("name") == away:
                                        result["away_spread_odds"] = o.get("price")
                                        result["away_spread_point"] = o.get("point")
                                elif mkey == "totals":
                                    name = o.get("name", "").lower()
                                    if "over" in name:
                                        result["over_odds"] = o.get("price")
                                        result["over_under"] = o.get("point")
                                    elif "under" in name:
                                        result["under_odds"] = o.get("price")
                    if result.get("home_ml"):
                        odds_map[key] = result
        except Exception as e:
            logger.warning("Could not fetch NHL odds (fallback): %s", e)

    recorded = []
    for event in events:
        game_id = event.get("id", "")
        comps = event.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]

        # Skip completed games
        status = comp.get("status", {}).get("type", {})
        if status.get("completed", False):
            continue

        competitors = comp.get("competitors", [])
        h_abbr = ""
        a_abbr = ""
        for c in competitors:
            team = c.get("team", {})
            abbr = team.get("abbreviation", "")
            if c.get("homeAway") == "home":
                h_abbr = abbr
            else:
                a_abbr = abbr

        if not h_abbr or not a_abbr:
            continue

        matchup = f"{a_abbr} @ {h_abbr}"

        # Skip if already recorded
        existing = conn.execute(
            "SELECT COUNT(*) as c FROM nhl_picks WHERE game_id = ?", (game_id,)
        ).fetchone()["c"]
        if existing > 0:
            continue

        # Try direct and alternate abbreviations
        _ALT = {
            "TB": "TBL", "TBL": "TB", "NJ": "NJD", "NJD": "NJ",
            "SJ": "SJS", "SJS": "SJ", "LA": "LAK", "LAK": "LA",
            "WAS": "WSH", "WSH": "WAS", "CLB": "CBJ", "CBJ": "CLB",
            "MON": "MTL", "MTL": "MON", "NAS": "NSH", "NSH": "NAS",
        }
        h_key = key_map.get(h_abbr) or key_map.get(_ALT.get(h_abbr, ""))
        a_key = key_map.get(a_abbr) or key_map.get(_ALT.get(a_abbr, ""))
        if not h_key or not a_key:
            logger.warning("Could not find team keys for %s vs %s", a_abbr, h_abbr)
            continue

        # Read from shared picks store first (same picks the card shows)
        picks = None
        try:
            from backend.server import _picks_store_get
            stored = _picks_store_get("nhl", h_abbr, a_abbr)
            if stored and stored.get("picks"):
                picks = stored["picks"]
        except Exception:
            pass

        if not picks:
            from engine.picks import match_odds as _match_odds
            game_odds = _match_odds(h_abbr, a_abbr, odds_map)
            picks = generate_nhl_picks(h_key, a_key, game_odds)
        if not picks:
            continue

        # Filter out derivatives — they go to engine.derivative_tracker
        # via the /api/best-bets recorder, not the main tracker.
        core = _core_picks(picks)
        if not core:
            continue
        best = core[0]
        if best["edge"] < min_edge:
            continue
        from .nhl_picks import _valid_odds as _nhl_valid
        if not _nhl_valid(best.get("odds")):
            logger.warning("Skipping NHL pick with invalid odds=%s for %s",
                           best.get("odds"), matchup)
            continue

        conn.execute("""
            INSERT INTO nhl_picks (game_id, date, matchup, bet_type, pick,
                                   model_prob, edge, odds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (game_id, target_date, matchup, best["type"], best["pick"],
              best["prob"], best["edge"], best["odds"]))

        recorded.append({
            "matchup": matchup, "type": best["type"],
            "pick": best["pick"], "prob": round(best["prob"], 3),
            "edge": round(best["edge"], 1), "odds": best["odds"],
        })

    conn.commit()
    return recorded


def settle_picks() -> dict:
    """Settle all pending NHL picks against final game results."""
    conn = _get_nhl_db()

    # Self-heal: auto-push picks pending >7 days. See engine.tracker
    # for rationale — settler can fail to match (postponed games,
    # rescheduled events) and stale rows pollute WR%.
    from datetime import datetime, timedelta
    stale_cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    stale = conn.execute(
        "SELECT id, date, matchup, bet_type, pick FROM nhl_picks "
        "WHERE result IS NULL AND date < ?", (stale_cutoff,),
    ).fetchall()
    auto_pushed = 0
    for row in stale:
        conn.execute(
            "UPDATE nhl_picks SET result='P', profit=0, settled_at=datetime('now') "
            "WHERE id=?", (row["id"],),
        )
        logger.warning(
            "NHL settle: auto-pushed stale pending id=%s date=%s "
            "%s %s/%s — older than 7 days",
            row["id"], row["date"], row["matchup"], row["bet_type"], row["pick"],
        )
        auto_pushed += 1
    if auto_pushed:
        conn.commit()

    pending = conn.execute(
        "SELECT * FROM nhl_picks WHERE result IS NULL"
    ).fetchall()

    if not pending:
        return {"settled": 0, "auto_pushed": auto_pushed,
                "message": "No pending NHL picks"}

    # Group by date to fetch scoreboards
    dates = set()
    for p in pending:
        dates.add(p["date"])

    # Fetch scoreboard for each date. Include in-progress games so
    # period-specific picks (Period Total P1, Period BTS P1, Period
    # DNB P1 etc) can settle as soon as their period ends — no waiting
    # for the full game. Full-game bet types (ML, O/U, PL, Team Total,
    # Total O/E, Overtime, BTS) still gate on `is_completed`.
    final_scores = {}  # game_id -> {..., is_completed, periods_locked}
    for d in dates:
        events = _fetch_nhl_scoreboard(d)
        for event in events:
            eid = event.get("id", "")
            comp = event.get("competitions", [{}])[0]
            status_info = comp.get("status", {})
            status = status_info.get("type", {})
            state = status.get("state", "pre")
            if state == "pre":
                continue  # hasn't started — nothing to settle
            is_completed = status.get("completed", False)

            # Period 4 = overtime (regular season + playoffs); period 5
            # = shootout. Detail string ("Final/OT", "Final/SO") is the
            # most reliable cross-check since some events report period=4
            # for OT-only games even when regulation ended in a tie.
            cur_period = status_info.get("period") or 0
            detail = (status.get("detail") or status.get("shortDetail")
                      or "").lower()
            went_to_ot = (is_completed and (cur_period > 3
                          or "ot" in detail or "shootout" in detail))

            home_score = 0
            away_score = 0
            h_abbr = ""
            a_abbr = ""
            home_periods: list[int] = []
            away_periods: list[int] = []
            for c in comp.get("competitors", []):
                team = c.get("team", {})
                raw = c.get("score", "0")
                score = int(raw) if isinstance(raw, (int, str)) and str(raw).isdigit() else 0
                # ESPN ships per-period scoring under linescores as a
                # list of {value: N, displayValue: "N"} dicts. Length
                # 3 in regulation, 4 in OT, 5 if shootout was decisive.
                ls_raw = c.get("linescores") or []
                periods = []
                for ls in ls_raw:
                    val = ls.get("value")
                    try:
                        periods.append(int(val))
                    except (TypeError, ValueError):
                        periods.append(0)
                if c.get("homeAway") == "home":
                    home_score = score
                    h_abbr = team.get("abbreviation", "")
                    home_periods = periods
                else:
                    away_score = score
                    a_abbr = team.get("abbreviation", "")
                    away_periods = periods

            # A period N is "locked" when its linescore entry exists AND
            # either the game is completed OR the current period has
            # advanced past N. Mid-period scores are not locked — a goal
            # scored in P1 5:00 could be followed by another before P1
            # ends. ESPN populates the linescore entry for period N as
            # soon as N is in progress, so use cur_period as the guard.
            linescore_depth = min(len(home_periods), len(away_periods))
            if is_completed:
                periods_locked = linescore_depth
            else:
                # While game is in progress, only periods strictly before
                # current are locked.
                periods_locked = max(0, min(linescore_depth, cur_period - 1))

            final_scores[eid] = {
                "home_abbr": h_abbr, "away_abbr": a_abbr,
                "home_score": home_score, "away_score": away_score,
                "total": home_score + away_score,
                "home_periods": home_periods,
                "away_periods": away_periods,
                "went_to_ot": went_to_ot,
                "is_completed": is_completed,
                "periods_locked": periods_locked,
            }

    # Fetch current NHL odds for closing line capture
    closing_odds_map = {}
    try:
        import os
        from pathlib import Path as _Path
        key_file = _Path(__file__).resolve().parent.parent / "data" / "odds_api_key.txt"
        api_key = os.environ.get("ODDS_API_KEY") or (key_file.read_text().strip() if key_file.exists() else None)
        if api_key:
            import urllib.request as _urlreq
            _url = (f"https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds/"
                    f"?apiKey={api_key}&regions=us&markets=h2h,spreads,totals"
                    f"&oddsFormat=american&bookmakers=draftkings")
            _req = _urlreq.Request(_url, headers={"User-Agent": "NHLTracker/1.0"})
            with _urlreq.urlopen(_req, timeout=15) as _resp:
                _odds_data = json.loads(_resp.read().decode())

            _NHL_ABBR = {
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
            for _g in (_odds_data or []):
                _home = _g.get("home_team", "")
                _away = _g.get("away_team", "")
                _h_ab = _NHL_ABBR.get(_home, _home[:3].upper())
                _a_ab = _NHL_ABBR.get(_away, _away[:3].upper())
                _key = f"{_a_ab}@{_h_ab}"
                _res = {}
                for _bk in _g.get("bookmakers", [])[:1]:
                    for _mkt in _bk.get("markets", []):
                        _mk = _mkt.get("key", "")
                        for _o in _mkt.get("outcomes", []):
                            if _mk == "h2h":
                                if _o.get("name") == _home:
                                    _res["home_ml"] = _o.get("price")
                                elif _o.get("name") == _away:
                                    _res["away_ml"] = _o.get("price")
                            elif _mk == "spreads":
                                if _o.get("name") == _home:
                                    _res["home_spread_odds"] = _o.get("price")
                                elif _o.get("name") == _away:
                                    _res["away_spread_odds"] = _o.get("price")
                            elif _mk == "totals":
                                _nm = _o.get("name", "").lower()
                                if "over" in _nm:
                                    _res["over_odds"] = _o.get("price")
                                elif "under" in _nm:
                                    _res["under_odds"] = _o.get("price")
                if _res:
                    closing_odds_map[_key] = _res
    except Exception as e:
        logger.debug("Could not fetch NHL closing odds: %s", e)

    settled = 0
    wins = 0
    losses = 0

    for pick in pending:
        pick = dict(pick)
        game_id = pick["game_id"]

        game = final_scores.get(game_id)
        if not game:
            continue

        hs = game["home_score"]
        as_ = game["away_score"]
        total = game["total"]
        h = game["home_abbr"]
        a = game["away_abbr"]

        # Capture closing odds if not already stored
        if not pick.get("closing_odds") and h and a:
            _ALT_CL = {
                "TB": "TBL", "TBL": "TB", "NJ": "NJD", "NJD": "NJ",
                "SJ": "SJS", "SJS": "SJ", "LA": "LAK", "LAK": "LA",
                "WAS": "WSH", "WSH": "WAS", "CLB": "CBJ", "CBJ": "CLB",
                "MON": "MTL", "MTL": "MON", "NAS": "NSH", "NSH": "NAS",
            }
            game_cl_odds = None
            for a_try in [a, _ALT_CL.get(a, "")]:
                for h_try in [h, _ALT_CL.get(h, "")]:
                    if a_try and h_try:
                        game_cl_odds = closing_odds_map.get(f"{a_try}@{h_try}")
                        if game_cl_odds:
                            break
                if game_cl_odds:
                    break
            if game_cl_odds:
                bt_tmp = pick["bet_type"]
                pk_tmp = pick["pick"]
                closing = None
                if bt_tmp == "ML":
                    closing = game_cl_odds.get("home_ml") if pk_tmp == h else game_cl_odds.get("away_ml")
                elif bt_tmp == "O/U":
                    closing = game_cl_odds.get("over_odds") if "Over" in pk_tmp else game_cl_odds.get("under_odds")
                elif bt_tmp == "PL":
                    pick_team = pk_tmp.split()[0] if pk_tmp.split() else ""
                    closing = game_cl_odds.get("home_spread_odds") if pick_team == h else game_cl_odds.get("away_spread_odds")
                if closing is not None:
                    conn.execute("UPDATE nhl_picks SET closing_odds = ? WHERE id = ?",
                                 (int(closing), pick["id"]))
                    pick["closing_odds"] = int(closing)

        bt = pick["bet_type"]
        pk = pick["pick"]
        odds = pick["odds"] or -110
        result = None

        # Per-bet-type readiness check. Full-game markets need the
        # game completed; period-specific markets only need their
        # period locked (enables intra-game settlement — a Period 1
        # pick doesn't wait for Period 3).
        _FULL_GAME_TYPES = {
            "ML", "O/U", "ALT O/U", "PL", "ALT PL",
            "Team Total", "Total O/E", "Overtime", "BTS",
        }
        if bt in _FULL_GAME_TYPES and not game.get("is_completed", False):
            continue

        if bt == "ML":
            home_won = hs > as_
            if pk == h:
                won = home_won
            else:
                won = not home_won
            result = "W" if won else "L"

        elif bt in ("O/U", "ALT O/U"):
            # ALT O/U mirrors primary O/U settlement — same Over/Under
            # label, different line value. Without this the conservatism
            # ladder's safer-line swaps stayed PEND on the NHL tracker.
            if "Over" in pk:
                line = float(pk.split()[-1])
                if total > line:
                    result = "W"
                elif total < line:
                    result = "L"
                else:
                    result = "P"
            else:
                line = float(pk.split()[-1])
                if total < line:
                    result = "W"
                elif total > line:
                    result = "L"
                else:
                    result = "P"

        elif bt in ("PL", "ALT PL"):
            # ALT PL settles the same as primary PL — pick label has the
            # team + signed spread regardless of alt vs primary.
            parts = pk.split()
            pick_team = parts[0] if parts else ""
            spread = float(parts[1]) if len(parts) > 1 else 1.5

            if pick_team == h:
                team_margin = hs - as_
            else:
                team_margin = as_ - hs

            if team_margin + spread > 0:
                result = "W"
            elif team_margin + spread == 0:
                result = "P"
            else:
                result = "L"

        # ── Phase 1 derivative markets ──
        # Period-specific markets need linescores from ESPN (added to
        # final_scores above). Markets that only need final scores
        # (Team Total, Total O/E, BTS, Overtime) settle without
        # touching linescores.
        elif bt == "Team Total":
            # "BOS Over 3.5" / "NYR Under 2.5"
            parts = pk.split()
            if len(parts) >= 3:
                pick_team = parts[0]
                direction = parts[1].lower()
                try:
                    line = float(parts[2])
                except ValueError:
                    continue
                team_goals = hs if pick_team == h else as_
                if team_goals > line:
                    result = "W" if direction == "over" else "L"
                elif team_goals < line:
                    result = "L" if direction == "over" else "W"
                else:
                    result = "P"

        elif bt == "Period Total":
            # "P1 Over 1.5" / "P2 Under 1.5" — 3 tokens (P{n}, direction, line)
            home_periods = game.get("home_periods") or []
            away_periods = game.get("away_periods") or []
            parts = pk.split()
            if len(parts) >= 3 and parts[0].startswith("P"):
                try:
                    period_n = int(parts[0][1:])
                    line = float(parts[2])
                except ValueError:
                    continue
                direction = parts[1].lower()
                # Need that period to have been played. Regulation = 3;
                # OT = 4. We don't settle period totals on a period
                # that didn't happen (e.g. P4 in a regulation game).
                # Only settle once the period is locked (current period
                # advanced past it OR game completed). Guards against
                # mid-period snapshots where more goals could still come.
                if period_n < 1 or period_n > game.get("periods_locked", 0):
                    continue
                period_total = (home_periods[period_n - 1]
                                + away_periods[period_n - 1])
                if period_total > line:
                    result = "W" if direction == "over" else "L"
                elif period_total < line:
                    result = "L" if direction == "over" else "W"
                else:
                    result = "P"

        elif bt == "Period BTS":
            # "P1 BTS Yes" / "P1 BTS No" — 3 tokens (P{n}, "BTS", direction)
            home_periods = game.get("home_periods") or []
            away_periods = game.get("away_periods") or []
            parts = pk.split()
            if len(parts) >= 3 and parts[0].startswith("P"):
                try:
                    period_n = int(parts[0][1:])
                except ValueError:
                    continue
                direction = parts[2].lower()
                # Only settle once the period is locked (current period
                # advanced past it OR game completed). Guards against
                # mid-period snapshots where more goals could still come.
                if period_n < 1 or period_n > game.get("periods_locked", 0):
                    continue
                bts_yes = (home_periods[period_n - 1] > 0
                           and away_periods[period_n - 1] > 0)
                if direction == "yes":
                    result = "W" if bts_yes else "L"
                else:
                    result = "W" if not bts_yes else "L"

        elif bt == "Period DNB":
            # "P1 DNB BOS" / "P1 DNB NYR" — 3 tokens (P{n}, "DNB", team)
            home_periods = game.get("home_periods") or []
            away_periods = game.get("away_periods") or []
            parts = pk.split()
            if len(parts) >= 3 and parts[0].startswith("P"):
                try:
                    period_n = int(parts[0][1:])
                except ValueError:
                    continue
                pick_team = parts[2]
                # Only settle once the period is locked (current period
                # advanced past it OR game completed). Guards against
                # mid-period snapshots where more goals could still come.
                if period_n < 1 or period_n > game.get("periods_locked", 0):
                    continue
                hp = home_periods[period_n - 1]
                ap = away_periods[period_n - 1]
                if hp == ap:
                    result = "P"  # DNB pushes on a tied period
                elif pick_team == h:
                    result = "W" if hp > ap else "L"
                else:
                    result = "W" if ap > hp else "L"

        elif bt == "Total O/E":
            # "Total Odd" / "Total Even"
            parts = pk.split()
            if len(parts) >= 2:
                direction = parts[1].lower()
                is_odd = (total % 2 == 1)
                if direction == "odd":
                    result = "W" if is_odd else "L"
                else:
                    result = "W" if not is_odd else "L"

        elif bt == "Overtime":
            # "Overtime Yes" / "Overtime No"
            went_to_ot = game.get("went_to_ot", False)
            parts = pk.split()
            if len(parts) >= 2:
                direction = parts[1].lower()
                if direction == "yes":
                    result = "W" if went_to_ot else "L"
                else:
                    result = "W" if not went_to_ot else "L"

        elif bt == "BTS":
            # "BTS Yes" / "BTS No"
            bts_yes = (hs > 0 and as_ > 0)
            parts = pk.split()
            if len(parts) >= 2:
                direction = parts[1].lower()
                if direction == "yes":
                    result = "W" if bts_yes else "L"
                else:
                    result = "W" if not bts_yes else "L"

        if result is None:
            continue

        if result == "W":
            profit = (odds if odds > 0 else 100 / abs(odds) * 100)
            wins += 1
        elif result == "L":
            profit = -100
            losses += 1
        else:
            profit = 0

        conn.execute("""
            UPDATE nhl_picks SET result = ?, profit = ?, settled_at = datetime('now')
            WHERE id = ?
        """, (result, round(profit, 2), pick["id"]))
        settled += 1

    conn.commit()

    return {
        "settled": settled,
        "wins": wins,
        "losses": losses,
        "pending_remaining": conn.execute(
            "SELECT COUNT(*) as c FROM nhl_picks WHERE result IS NULL"
        ).fetchone()["c"],
    }


def get_pick_summary() -> dict:
    """Get running totals across all NHL picks."""
    conn = _get_nhl_db()

    summary = {}
    for bt in ["ML", "O/U", "PL"]:
        row = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
                COALESCE(SUM(profit), 0) as profit
            FROM nhl_picks WHERE bet_type = ?
        """, (bt,)).fetchone()

        total = row["total"] or 0
        w = row["wins"] or 0
        l = row["losses"] or 0
        settled_count = w + l
        summary[bt] = {
            "total": total,
            "wins": w,
            "losses": l,
            "pushes": row["pushes"],
            "pending": row["pending"],
            "profit": round(row["profit"], 2),
            "win_pct": round(w / settled_count * 100, 1) if settled_count > 0 else 0,
            "roi": round(row["profit"] / settled_count, 1) if settled_count > 0 else 0,
        }

    recent = conn.execute("""
        SELECT * FROM nhl_picks ORDER BY created_at DESC LIMIT 30
    """).fetchall()

    totals = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
            COALESCE(SUM(profit), 0) as profit
        FROM nhl_picks
    """).fetchone()

    tw = totals["wins"] or 0
    tl = totals["losses"] or 0

    # Compute CLV across all settled picks that have closing odds
    clv_rows = conn.execute("""
        SELECT odds, closing_odds FROM nhl_picks
        WHERE result IS NOT NULL AND odds IS NOT NULL AND closing_odds IS NOT NULL
    """).fetchall()
    clv_values = []
    for r in clv_rows:
        clv = _compute_clv(r["odds"], r["closing_odds"])
        if clv is not None:
            clv_values.append(clv)
    avg_clv = round(sum(clv_values) / len(clv_values), 2) if clv_values else None

    return {
        "by_type": summary,
        "overall": {
            "total": totals["total"] or 0,
            "wins": tw,
            "losses": tl,
            "pending": totals["pending"] or 0,
            "profit": round(totals["profit"] or 0, 2),
            "win_pct": round(tw / (tw + tl) * 100, 1) if (tw + tl) > 0 else 0,
            "avg_clv": avg_clv,
            "clv_sample": len(clv_values),
        },
        "recent": [dict(r) for r in recent],
    }


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = set(sys.argv[1:])

    if "--record" in args:
        force = "--force" in args
        print(f"Recording today's NHL picks{' (force reset)' if force else ''}...", flush=True)
        picks = record_picks(force=force)
        print(f"Recorded {len(picks)} NHL picks:")
        for p in picks:
            print(f"  {p['matchup']} | {p['type']:4s} | {p['pick']:15s} | {p['prob']:.1%} | edge: {p['edge']:+.1f}%")

    elif "--settle" in args:
        print("Settling completed NHL picks...", flush=True)
        result = settle_picks()
        print(f"Settled: {result.get('settled', 0)} ({result.get('wins', 0)}W-{result.get('losses', 0)}L)")
        print(f"Pending: {result['pending_remaining']}")

    elif "--summary" in args:
        summary = get_pick_summary()
        overall = summary["overall"]
        print(f"\n{'='*50}")
        print(f"  NHL PICK TRACKER - Running Totals")
        print(f"{'='*50}")
        print(f"  Total picks: {overall['total']}")
        print(f"  Record: {overall['wins']}-{overall['losses']} ({overall['win_pct']}%)")
        print(f"  Profit: ${overall['profit']:+.2f}")
        print(f"  Pending: {overall['pending']}")
        if overall.get("avg_clv") is not None:
            print(f"  Avg CLV: {overall['avg_clv']:+.2f}% ({overall['clv_sample']} picks)")
        print()
        for bt, label in [("ML", "Moneyline"), ("O/U", "Over/Under"), ("PL", "Puck Line")]:
            s = summary["by_type"][bt]
            if s["total"] == 0:
                continue
            print(f"  {label}: {s['wins']}-{s['losses']} ({s['win_pct']}%) ${s['profit']:+.2f}")
        print(f"{'='*50}")

    else:
        print("Usage: python -m engine.nhl_tracker --record | --settle | --summary")
