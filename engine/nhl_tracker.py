"""
NHL Pick tracker — records model picks and settles them against results.

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


def _compute_clv(bet_odds, closing_odds):
    """Compute closing line value.
    Positive CLV = got better price than closing line = sharp.
    """
    if not bet_odds or not closing_odds:
        return None
    bet_implied = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 else 100 / (bet_odds + 100)
    close_implied = abs(closing_odds) / (abs(closing_odds) + 100) if closing_odds < 0 else 100 / (closing_odds + 100)
    return round((close_implied - bet_implied) * 100, 2)  # positive = we got a better price


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


def record_picks(date: str | None = None, min_edge: float = 1.5) -> list[dict]:
    """
    Run NHL model on today's games and record the best pick per game.
    """
    conn = _get_nhl_db()
    target_date = date or datetime.now().strftime("%Y-%m-%d")

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

    # Fetch odds
    odds_map = {}
    try:
        import os
        from pathlib import Path
        key_file = Path(__file__).resolve().parent.parent / "data" / "odds_api_key.txt"
        api_key = os.environ.get("ODDS_API_KEY") or (key_file.read_text().strip() if key_file.exists() else None)
        if api_key:
            import urllib.request
            url = (f"https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds/"
                   f"?apiKey={api_key}&regions=us&markets=h2h,spreads,totals"
                   f"&oddsFormat=american&bookmakers=draftkings")
            req = urllib.request.Request(url, headers={"User-Agent": "NHLTracker/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                odds_data = json.loads(resp.read().decode())

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

            for game in (odds_data or []):
                home = game.get("home_team", "")
                away = game.get("away_team", "")
                h_abbr = _NHL_TEAM_ABBR.get(home, home[:3].upper())
                a_abbr = _NHL_TEAM_ABBR.get(away, away[:3].upper())
                key = f"{a_abbr}@{h_abbr}"
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
        logger.warning("Could not fetch NHL odds: %s", e)

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

        # Match odds (try alternate abbreviations too)
        game_odds = None
        for a_try in [a_abbr, _ALT.get(a_abbr, "")]:
            for h_try in [h_abbr, _ALT.get(h_abbr, "")]:
                if a_try and h_try:
                    game_odds = odds_map.get(f"{a_try}@{h_try}")
                    if game_odds:
                        break
            if game_odds:
                break

        picks = generate_nhl_picks(h_key, a_key, game_odds)
        if not picks:
            continue

        best = picks[0]
        if best["edge"] < min_edge:
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

    pending = conn.execute(
        "SELECT * FROM nhl_picks WHERE result IS NULL"
    ).fetchall()

    if not pending:
        return {"settled": 0, "message": "No pending NHL picks"}

    # Group by date to fetch scoreboards
    dates = set()
    for p in pending:
        dates.add(p["date"])

    # Fetch final scores for each date
    final_scores = {}  # game_id -> {home_abbr, away_abbr, home_score, away_score, total}
    for d in dates:
        events = _fetch_nhl_scoreboard(d)
        for event in events:
            eid = event.get("id", "")
            comp = event.get("competitions", [{}])[0]
            status = comp.get("status", {}).get("type", {})
            if not status.get("completed", False):
                continue

            home_score = 0
            away_score = 0
            h_abbr = ""
            a_abbr = ""
            for c in comp.get("competitors", []):
                team = c.get("team", {})
                raw = c.get("score", "0")
                score = int(raw) if isinstance(raw, (int, str)) and str(raw).isdigit() else 0
                if c.get("homeAway") == "home":
                    home_score = score
                    h_abbr = team.get("abbreviation", "")
                else:
                    away_score = score
                    a_abbr = team.get("abbreviation", "")

            final_scores[eid] = {
                "home_abbr": h_abbr, "away_abbr": a_abbr,
                "home_score": home_score, "away_score": away_score,
                "total": home_score + away_score,
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

        if bt == "ML":
            home_won = hs > as_
            if pk == h:
                won = home_won
            else:
                won = not home_won
            result = "W" if won else "L"

        elif bt == "O/U":
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

        elif bt == "PL":
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
        print("Recording today's NHL picks...", flush=True)
        picks = record_picks()
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
        print(f"  NHL PICK TRACKER — Running Totals")
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
