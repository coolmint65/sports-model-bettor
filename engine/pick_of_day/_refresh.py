"""POTD line-movement refresh + closing-odds capture.

Two flavours of refresh, called from the per-sport sync scripts:

  refresh_potd_for_line_movement(sport, games_with_bets=None, view='q1')
    Re-stamp pending POTD rows with the live line/odds/edge so the
    displayed POTD card matches what HR is actually offering. Keeps
    the same (game_id, bet_type, side) — selection is locked, line is
    not. Also invalidates the lock when the GameCard's headline has
    drifted to a different bet_type (the "POTD shows +1.5 RL but card
    shows ML" case the user reported on 2026-04-27).

  update_potd_closing_odds(sport)
    Snapshot the current line as `closing_odds`. Called on every sync
    run; the LAST update before settle_potd() runs is effectively the
    closing line we record for CLV.
"""

from __future__ import annotations
import logging
import os
import urllib.request
import json as _json
from pathlib import Path

from ._storage import _get_conn, _ensure_potd_table
from ._select import _build_reasoning, _pick_side

logger = logging.getLogger(__name__)


def refresh_potd_for_line_movement(sport: str,
                                   games_with_bets: list[dict] | None = None,
                                   view: str = "q1") -> dict:
    """Re-stamp pending POTD rows with the current line/odds/edge if
    the line has moved since the lock. Selection itself stays frozen
    — same game, same bet_type, same direction (Over/Under or team).

    When ``games_with_bets`` is supplied, this also invalidates POTD
    locks whose bet_type no longer matches the per-game ``best_pick``
    on the dashboard. See pick_of_day._select for the design.
    """
    if sport not in ("mlb", "nhl", "nba"):
        return {"updated": 0, "reason": f"unsupported sport {sport!r}"}
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    # NBA splits POTDs across two tables (Q1 in pick_of_day, Full in
    # pick_of_day_full). Iterate both so neither view goes stale.
    pot_tables = ["pick_of_day"]
    if sport == "nba":
        pot_tables.append("pick_of_day_full")

    pending = []
    for tbl in pot_tables:
        rows = conn.execute(
            f"SELECT id, game_id, matchup, bet_type, pick, odds, edge, model_prob, "
            f"       '{tbl}' AS _table FROM {tbl} WHERE result IS NULL"
        ).fetchall()
        for r in rows:
            pending.append(dict(r))
    if not pending:
        return {"updated": 0, "skipped": 0, "invalidated": 0}

    # Build a (game_id -> headline_bet_type) map so we can invalidate
    # POTDs whose lock has drifted away from the GameCard's best_pick.
    headline_for_game: dict[str, str] = {}
    if games_with_bets:
        for game in games_with_bets:
            gid = str(game.get("game_id") or "")
            if not gid:
                continue
            if sport == "nba" and view == "full":
                bp = game.get("best_pick_full")
            elif sport == "nba":
                bp = game.get("best_pick_q1") or game.get("best_pick")
            else:
                bp = game.get("best_pick")
            if bp and bp.get("type"):
                headline_for_game[gid] = bp["type"]

    picks_table = "picks" if sport == "mlb" else f"{sport}_picks"
    updated = 0
    skipped = 0
    invalidated = 0
    for row in pending:
        pot_table = row.pop("_table", "pick_of_day")

        # Drift check: if the GameCard's current best_pick for this
        # game uses a different bet_type than the locked POTD, drop
        # the lock so the next select_potd re-aligns with the card.
        gid_key = str(row.get("game_id") or "")
        if gid_key and gid_key in headline_for_game:
            row_view = "full" if pot_table == "pick_of_day_full" else "q1"
            if sport != "nba" or row_view == view:
                live_bt = headline_for_game[gid_key]
                if live_bt and live_bt != row["bet_type"]:
                    conn.execute(f"DELETE FROM {pot_table} WHERE id = ?", (row["id"],))
                    logger.info("POTD %s invalidated #%s (%s): bet_type %s "
                                "no longer matches GameCard headline %s",
                                sport, row["id"], pot_table,
                                row["bet_type"], live_bt)
                    invalidated += 1
                    continue

        # Match: same game, same bet type, same direction. Pull every
        # pending pick for the (game, bet_type) pair, then filter to
        # same-side.
        live_rows = conn.execute(
            f"SELECT pick, odds, edge, model_prob FROM {picks_table} "
            f"WHERE game_id = ? AND bet_type = ? AND result IS NULL "
            f"ORDER BY id DESC LIMIT 10",
            (row["game_id"], row["bet_type"]),
        ).fetchall()
        if not live_rows:
            conn.execute(f"DELETE FROM {pot_table} WHERE id = ?", (row["id"],))
            logger.info("POTD %s invalidated #%s (%s/%s/%s): no live pick "
                        "still matches", sport, row["id"], pot_table,
                        row["bet_type"], row["pick"])
            invalidated += 1
            continue
        target_side = _pick_side(row["bet_type"], row["pick"])
        match = None
        for lr in live_rows:
            lr_d = dict(lr)
            if _pick_side(row["bet_type"], lr_d["pick"]) == target_side:
                match = lr_d
                break
        if not match:
            skipped += 1
            continue
        # Skip the no-op case so we don't churn the DB on every sync.
        if (match["pick"] == row["pick"]
                and match["odds"] == row["odds"]
                and match["edge"] == row["edge"]):
            continue
        refreshed = {
            "type": row["bet_type"],
            "pick": match["pick"],
            "pick_full": match["pick"],
            "prob": match["model_prob"],
            "edge": match["edge"],
            "odds": match["odds"],
        }
        new_reasoning = _build_reasoning(refreshed, sport)
        conn.execute(
            f"UPDATE {pot_table} SET pick = ?, odds = ?, edge = ?, "
            f"                       model_prob = ?, reasoning = ? "
            f"WHERE id = ?",
            (match["pick"], match["odds"], match["edge"],
             match["model_prob"], new_reasoning, row["id"]),
        )
        logger.info("POTD %s line-refresh #%s (%s): %r %+d edge=%.1f -> %r %+d edge=%.1f",
                    sport, row["id"], pot_table, row["pick"], int(row["odds"] or 0),
                    float(row["edge"] or 0), match["pick"],
                    int(match["odds"] or 0), float(match["edge"] or 0))
        updated += 1

    if updated or invalidated:
        conn.commit()
    return {"updated": updated, "skipped": skipped, "invalidated": invalidated}


def update_potd_closing_odds(sport: str) -> dict:
    """Refresh closing_odds on every un-settled POTD with the latest line.

    The pick selection itself stays locked from get_or_create_potd();
    this only updates the price we'll use for CLV measurement at settle
    time. Since each call overwrites with the freshest available price,
    the LAST update before settle_potd() runs is effectively the closing
    line.
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
        pairs = resolver(pending)
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


# ── Per-sport closing-odds resolvers ──

def _resolve_mlb_closing_for_pending(pending: list) -> list[tuple]:
    """Per-POTD (id, closing_price) pairs for MLB."""
    from ..picks import fetch_real_odds_for_games, match_odds
    from ..tracker import _extract_closing_for_pick
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
    """Per-POTD (id, closing_price) pairs for NHL."""
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
    """Pull current NHL h2h/spreads/totals from the-odds-api, keyed by AWAY@HOME."""
    key_file = Path(__file__).resolve().parent.parent.parent / "data" / "odds_api_key.txt"
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
        from ..abbr import alt_abbr
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
        away_name = away_name.strip()
        home_name = home_name.strip()
        if sport == "mlb":
            from ..db import get_conn as _gc
            table = "teams"
        elif sport == "nhl":
            from ..nhl_db import get_conn as _gc
            table = "nhl_teams"
        elif sport == "nba":
            from ..nba_db import get_conn as _gc
            table = "nba_teams"
        else:
            return None, None
        c = _gc()

        def _lookup(name: str):
            row = c.execute(
                f"SELECT abbreviation FROM {table} WHERE LOWER(name) = LOWER(?)",
                (name,),
            ).fetchone()
            if row:
                return row["abbreviation"]
            row = c.execute(
                f"SELECT abbreviation FROM {table} "
                f"WHERE LOWER(name) LIKE LOWER(?) OR LOWER(?) LIKE '%' || LOWER(name) || '%' "
                f"LIMIT 1",
                (f"%{name}%", name),
            ).fetchone()
            return row["abbreviation"] if row else None

        a_abbr = _lookup(away_name)
        h_abbr = _lookup(home_name)
        if a_abbr and h_abbr:
            return a_abbr, h_abbr
    except Exception as e:
        logger.debug("matchup_to_abbrs failed for %r (%s): %s", matchup, sport, e)
    return None, None
