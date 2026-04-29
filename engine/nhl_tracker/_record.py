"""NHL recording: refresh-pending, record-picks, capture-closing-odds."""

from __future__ import annotations
import json
import logging
from datetime import datetime

from ._helpers import _core_picks, _get_nhl_db, _extract_nhl_closing_for_pick
from ._scoreboard import _fetch_nhl_scoreboard

logger = logging.getLogger(__name__)


def capture_closing_odds() -> int:
    """Snapshot current Hard Rock NHL odds for all pending picks.

    Mirrors engine.nba_tracker.capture_closing_odds. Call before games
    start (sync script ~30 min pre-puck) so the per-row CLV column
    populates once games settle.
    """
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

    from ..picks import match_odds as _match_odds
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


def refresh_pending_for_today(bets: list[dict],
                               target_date: str | None = None) -> dict:
    """NHL twin of engine.tracker.refresh_pending_for_today. See that
    docstring for the design rationale."""
    target_date = target_date or datetime.now().strftime("%Y-%m-%d")
    conn = _get_nhl_db()
    current_by_matchup: dict[str, dict] = {}
    locked_matchups: set[str] = set()
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
            continue
        bp = b.get("best_pick")
        if bp:
            current_by_matchup[b["matchup"]] = bp

    pending = conn.execute(
        "SELECT id, matchup, bet_type, pick, game_id FROM nhl_picks "
        "WHERE date = ? AND result IS NULL",
        (target_date,),
    ).fetchall()

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
            continue
        if _pick_game_started(p.get("game_id")):
            continue
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
    """Run NHL model on today's games and record the best pick per game."""
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
                alt = _ALT_ABBRS.get(abbr)
                if alt:
                    key_map[alt] = t["key"]

    events = _fetch_nhl_scoreboard(target_date)
    if not events:
        logger.info("No NHL games found for %s", target_date)
        return []

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
            key_file = Path(__file__).resolve().parent.parent.parent / "data" / "odds_api_key.txt"
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

        existing = conn.execute(
            "SELECT COUNT(*) as c FROM nhl_picks WHERE game_id = ?", (game_id,)
        ).fetchone()["c"]
        if existing > 0:
            continue

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
        except Exception as e:
            logger.debug("NHL picks_store fetch failed for %s: %s", matchup, e)

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
        from ..nhl_picks import _valid_odds as _nhl_valid
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
