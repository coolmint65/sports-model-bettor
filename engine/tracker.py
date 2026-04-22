"""
Pick tracker - records model picks and settles them against results.

Call record_picks() before games start to log today's picks.
Call settle_picks() after games finish to mark W/L and calculate profit.

Usage:
    python -m engine.tracker --record     # Record today's picks
    python -m engine.tracker --settle     # Settle completed picks
    python -m engine.tracker --summary    # Print running totals
"""

import json
import logging
import urllib.error
import urllib.request
from datetime import datetime

from .db import get_conn, get_team_by_id
from .mlb_predict import predict_matchup
from .bankroll import ml_to_implied_prob

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"


def _compute_clv(bet_odds, closing_odds):
    """Compute closing line value.
    Positive CLV = got better price than closing line = sharp.
    """
    if not bet_odds or not closing_odds:
        return None
    bet_implied = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 else 100 / (bet_odds + 100)
    close_implied = abs(closing_odds) / (abs(closing_odds) + 100) if closing_odds < 0 else 100 / (closing_odds + 100)
    return round((close_implied - bet_implied) * 100, 2)  # positive = we got a better price


def _extract_closing_for_pick(bet_type: str, pick_text: str,
                               home_abbr: str, game_odds: dict) -> int | None:
    """Pure helper: pick the right field out of an odds dict for a bet.

    Used by both _fetch_closing_odds_for_pick (which fetches odds first)
    and the inline settle_picks() capture path (which already has odds
    in hand). Centralizing avoids two branches drifting apart when we
    add new market types.
    """
    if not game_odds:
        return None
    bt = bet_type
    pk = pick_text or ""
    if bt in ("ml", "ML"):
        return (game_odds.get("home_ml") if pk == home_abbr
                else game_odds.get("away_ml"))
    if bt in ("ou", "O/U"):
        return (game_odds.get("over_odds") if "Over" in pk
                else game_odds.get("under_odds"))
    if bt in ("rl", "RL"):
        pick_team = pk.split()[0] if pk.split() else ""
        return (game_odds.get("home_spread_odds") if pick_team == home_abbr
                else game_odds.get("away_spread_odds"))
    if bt in ("nrfi", "1st INN"):
        # NRFI / YRFI close on the per-event totals_1st_1_innings market.
        if pk == "NRFI":
            return game_odds.get("nrfi_under_odds")
        return game_odds.get("nrfi_over_odds")
    if bt == "F5 ML":
        return (game_odds.get("f5_home_ml") if pk == home_abbr
                else game_odds.get("f5_away_ml"))
    if bt == "F5 O/U":
        return (game_odds.get("f5_over_odds") if "Over" in pk
                else game_odds.get("f5_under_odds"))
    if bt == "F5 RL":
        pick_team = pk.split()[0] if pk.split() else ""
        return (game_odds.get("f5_home_spread_odds") if pick_team == home_abbr
                else game_odds.get("f5_away_spread_odds"))
    return None


def _fetch_closing_odds_for_pick(pick: dict, home_abbr: str, away_abbr: str) -> int | None:
    """Fetch current odds from the odds API for a specific pick.

    Returns the relevant moneyline/odds value for the pick's bet type and side,
    or None if unavailable.
    """
    try:
        from .picks import fetch_real_odds_for_games, match_odds
        all_odds = fetch_real_odds_for_games()
        game_odds = match_odds(home_abbr, away_abbr, all_odds)
        return _extract_closing_for_pick(
            pick["bet_type"], pick["pick"], home_abbr, game_odds or {},
        )
    except Exception:
        return None


def _fetch_espn_scoreboard(date: str) -> list[dict]:
    """Fetch MLB scoreboard from ESPN for a given date."""
    espn_date = date.replace("-", "")
    url = f"{ESPN_BASE}/baseball/mlb/scoreboard?dates={espn_date}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.warning("ESPN scoreboard fetch failed: %s", e)
        return []

    games = []
    for event in data.get("events", []):
        comps = event.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        home_team, away_team = None, None
        for c in competitors:
            team = c.get("team", {})
            entry = {
                "abbreviation": team.get("abbreviation", ""),
                "name": team.get("displayName", ""),
                "team_id": None,
            }
            # Resolve team_id from DB. ESPN sometimes reports abbrs that
            # differ from what MLB Stats API seeds into the teams table
            # (CWS vs CHW, AZ vs ARI, ATH vs OAK). Walk every known alias
            # so the fallback finds the right team_id instead of leaving
            # it None and dropping the game.
            from .db import get_conn as _gc
            from .abbr import aliases_for as _aliases
            conn = _gc()
            row = None
            for candidate in _aliases(entry["abbreviation"], sport="mlb"):
                row = conn.execute(
                    "SELECT mlb_id FROM teams WHERE abbreviation = ?",
                    (candidate,),
                ).fetchone()
                if row:
                    break
            # Last-resort: match by displayName (catches cases where a
            # brand-new abbreviation hasn't been added to abbr.py yet).
            if not row and entry["name"]:
                row = conn.execute(
                    "SELECT mlb_id FROM teams WHERE name = ? OR name LIKE ?",
                    (entry["name"], f"%{entry['name']}%"),
                ).fetchone()
            if row:
                entry["team_id"] = row["mlb_id"]
            else:
                logger.warning(
                    "ESPN fallback: could not resolve team_id for '%s' (%s) — add to engine/abbr.py",
                    entry["abbreviation"], entry["name"],
                )

            if c.get("homeAway") == "home":
                home_team = entry
            else:
                away_team = entry

        if not home_team or not away_team:
            continue

        status = comp.get("status", {}).get("type", {})

        # Probable pitchers
        home_pid, away_pid = None, None
        for c in competitors:
            pp = c.get("probables", [])
            if pp:
                pid = pp[0].get("athlete", {}).get("id")
                if c.get("homeAway") == "home":
                    home_pid = pid
                else:
                    away_pid = pid

        games.append({
            "id": event.get("id", ""),
            "home": home_team,
            "away": away_team,
            "home_pitcher": {"id": home_pid} if home_pid else {},
            "away_pitcher": {"id": away_pid} if away_pid else {},
            "venue": comp.get("venue", {}).get("fullName", ""),
            "status": {
                "state": status.get("state", "pre"),
                "completed": status.get("completed", False),
            },
        })

    return games

logger = logging.getLogger(__name__)

SEASON = datetime.now().year


def record_picks(date: str | None = None, min_edge: float = 1.5,
                 force: bool = False) -> list[dict]:
    """
    Run model on today's games and record the best pick per game.
    Uses the unified picks engine for consistent edge calculations.
    Falls back to ESPN scoreboard if games aren't in the DB.

    Args:
        date: Target YYYY-MM-DD (defaults to today).
        min_edge: Minimum edge percentage to record a pick.
        force: If True, delete any unsettled pick for each game before
            recording so the latest model/odds take precedence. Use
            when the model changed during the day (e.g. lineup / SP
            update landed after the morning sync).
    """
    conn = get_conn()
    target_date = date or datetime.now().strftime("%Y-%m-%d")

    # Optional hard-reset for today's unsettled picks
    if force:
        conn.execute(
            "DELETE FROM picks WHERE date = ? AND result IS NULL",
            (target_date,),
        )
        conn.commit()

    games = conn.execute("""
        SELECT * FROM games WHERE date = ?
    """, (target_date,)).fetchall()

    # If no games in DB, try syncing today's schedule first
    if not games:
        logger.info("No games in DB for %s - fetching from MLB API", target_date)
        try:
            from scrapers.mlb_stats import fetch_schedule
            fetch_schedule(target_date, target_date)
            games = conn.execute("""
                SELECT * FROM games WHERE date = ?
            """, (target_date,)).fetchall()
        except Exception as e:
            logger.warning("Could not fetch today's schedule: %s", e)

    # Still no games? Try ESPN scoreboard as last resort
    if not games:
        try:
            scoreboard = _fetch_espn_scoreboard(target_date)
            if scoreboard:
                logger.info("Using ESPN scoreboard (%d games)", len(scoreboard))
                return _record_from_scoreboard(conn, scoreboard, target_date, min_edge)
        except Exception as e:
            logger.warning("Scoreboard fallback failed: %s", e)
        return []

    # Fetch real odds once for all games
    from .picks import (
        generate_picks, get_best_pick, fetch_real_odds_for_games,
        match_odds, _valid_odds,
    )

    all_odds = fetch_real_odds_for_games()

    # Build set of live/completed games from ESPN scoreboard so we
    # don't record picks for in-progress or finished games. ESPN abbrs
    # and DB abbrs don't always match (CWS/CHW etc.), so seed the set
    # with every known alias — otherwise a live-game check using the
    # DB abbr misses when ESPN reports the alternate form.
    from .abbr import aliases_for as _aliases
    _live_or_done: set[str] = set()
    try:
        scoreboard = _fetch_espn_scoreboard(target_date)
        for game_info in (scoreboard or []):
            state = (game_info.get("status") or {}).get("state", "pre")
            if state in ("in", "post"):
                h_abbr = (game_info.get("home") or {}).get("abbreviation", "")
                a_abbr = (game_info.get("away") or {}).get("abbreviation", "")
                if h_abbr:
                    _live_or_done.update(_aliases(h_abbr, sport="mlb"))
                if a_abbr:
                    _live_or_done.update(_aliases(a_abbr, sport="mlb"))
    except Exception:
        pass

    print(f"[RECORD] Found {len(games)} games for {target_date}", flush=True)

    recorded = []
    for game in games:
        game = dict(game)
        game_id = game.get("mlb_game_id")

        home_id = game.get("home_team_id")
        away_id = game.get("away_team_id")
        if not home_id or not away_id:
            print(f"[RECORD]   {game_id}: missing team IDs (home={home_id}, away={away_id}), skipping", flush=True)
            continue

        home_team = get_team_by_id(home_id)
        away_team = get_team_by_id(away_id)
        if not home_team or not away_team:
            print(f"[RECORD]   game {game_id}: team not in DB (home_id={home_id} found={bool(home_team)}, away_id={away_id} found={bool(away_team)})", flush=True)
            continue
        h = home_team["abbreviation"]
        a = away_team["abbreviation"]
        matchup = f"{a} @ {h}"

        # Skip live or completed games
        if h in _live_or_done or a in _live_or_done:
            continue

        # Skip if already recorded (check by game_id OR by matchup+date to prevent dupes)
        existing = conn.execute(
            "SELECT COUNT(*) as c FROM picks WHERE game_id = ? OR (matchup = ? AND date = ?)",
            (game_id, matchup, target_date)
        ).fetchone()["c"]
        if existing > 0:
            print(f"[RECORD]   {matchup}: already recorded, skipping", flush=True)
            continue

        # Read from the picks store (single source of truth from best-bets).
        # If best-bets already ran, this returns the exact same picks the
        # card shows. If not, fall back to generating with matched odds.
        try:
            from backend.server import _picks_store_get
            stored = _picks_store_get("mlb", h, a)
            if stored and stored.get("picks"):
                picks = stored["picks"]
                game_odds = stored.get("odds", {})
                best = get_best_pick(picks)
                if best and best["edge"] >= min_edge and _valid_odds(best.get("odds")):
                    conn.execute("""
                        INSERT INTO picks (game_id, date, matchup, bet_type, pick,
                                         model_prob, edge, odds)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (game_id, target_date, matchup, best["type"], best["pick"],
                          best["prob"], best["edge"], best["odds"]))
                    recorded.append({
                        "matchup": matchup, "type": best["type"],
                        "pick": best["pick"], "prob": round(best["prob"], 3),
                        "edge": round(best["edge"], 1), "odds": best["odds"],
                    })
                continue
        except Exception:
            pass

        game_odds = match_odds(h, a, all_odds)

        picks = generate_picks(
            home_team_id=home_id,
            away_team_id=away_id,
            home_pitcher_id=game.get("home_pitcher_id"),
            away_pitcher_id=game.get("away_pitcher_id"),
            venue=game.get("venue"),
            odds=game_odds,
        )

        # Take the best pick. Defense-in-depth odds sanity check -- if a
        # pick somehow slipped through generate_picks with |odds| < 100
        # (nonsense American price), don't record it. generate_picks now
        # sanitizes the odds dict, but guard at the write boundary too so
        # this can never re-manifest without a fresh bug upstream.
        best = get_best_pick(picks)
        if not best or best["edge"] < min_edge:
            continue
        if not _valid_odds(best.get("odds")):
            logger.warning("Skipping pick with invalid odds=%s for %s",
                           best.get("odds"), matchup)
            continue

        conn.execute("""
            INSERT INTO picks (game_id, date, matchup, bet_type, pick,
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


def _record_from_scoreboard(conn, scoreboard: list, target_date: str,
                            min_edge: float) -> list[dict]:
    """Record picks using live scoreboard data when DB has no games."""
    from .picks import (
        generate_picks, get_best_pick, match_odds, fetch_real_odds_for_games,
        _valid_odds,
    )

    all_odds = fetch_real_odds_for_games()
    recorded = []

    for game in scoreboard:
        # Skip completed games
        if game.get("status", {}).get("completed") or game.get("status", {}).get("state") == "post":
            continue

        game_id = game.get("id") or game.get("game_pk")
        if not game_id:
            continue

        h = game.get("home", {}).get("abbreviation", "?")
        a = game.get("away", {}).get("abbreviation", "?")
        matchup = f"{a} @ {h}"

        # Skip if already recorded (check by game_id OR matchup+date)
        existing = conn.execute(
            "SELECT COUNT(*) as c FROM picks WHERE game_id = ? OR (matchup = ? AND date = ?)",
            (game_id, matchup, target_date)
        ).fetchone()["c"]
        if existing > 0:
            continue

        home_id = game.get("home", {}).get("team_id")
        away_id = game.get("away", {}).get("team_id")
        if not home_id or not away_id:
            print(f"[RECORD]   {matchup}: no team_id (home={home_id}, away={away_id}), skipping", flush=True)
            continue

        # Get odds
        game_odds = game.get("odds") or match_odds(h, a, all_odds)

        # Get pitcher IDs
        hp = game.get("home_pitcher") or {}
        ap = game.get("away_pitcher") or {}
        try:
            h_pid = int(hp["id"]) if hp.get("id") else None
            a_pid = int(ap["id"]) if ap.get("id") else None
        except (ValueError, TypeError):
            h_pid, a_pid = None, None

        try:
            picks = generate_picks(
                home_team_id=home_id, away_team_id=away_id,
                home_pitcher_id=h_pid, away_pitcher_id=a_pid,
                venue=game.get("venue"), odds=game_odds,
            )
        except Exception as e:
            logger.warning("Prediction failed for %s: %s", matchup, e)
            continue

        best = get_best_pick(picks)
        if not best or best["edge"] < min_edge:
            continue

        conn.execute("""
            INSERT INTO picks (game_id, date, matchup, bet_type, pick,
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
    logger.info("Recorded %d picks from scoreboard", len(recorded))
    return recorded


def capture_closing_odds() -> int:
    """Snapshot current Hard Rock odds for all pending picks.

    Call this before games start (e.g. from sync script at 6pm ET)
    to capture the closing line. The CLV computed at settle time
    will then reflect the true closing odds, not post-game odds.

    Returns number of picks updated.
    """
    conn = get_conn()
    pending = conn.execute(
        "SELECT id, matchup, bet_type, pick, closing_odds FROM picks "
        "WHERE result IS NULL AND closing_odds IS NULL"
    ).fetchall()

    if not pending:
        return 0

    from .picks import fetch_real_odds_for_games, match_odds
    all_odds = fetch_real_odds_for_games()
    updated = 0

    for pick in pending:
        matchup = pick["matchup"]
        parts = matchup.split(" @ ")
        if len(parts) != 2:
            continue
        away, home = parts[0].strip(), parts[1].strip()
        game_odds = match_odds(home, away, all_odds)
        if not game_odds:
            continue

        closing = _extract_closing_for_pick(
            pick["bet_type"], pick["pick"], home, game_odds,
        )
        if closing is not None:
            conn.execute(
                "UPDATE picks SET closing_odds = ? WHERE id = ?",
                (int(closing), pick["id"]),
            )
            updated += 1

    conn.commit()
    logger.info("Captured closing odds for %d/%d pending picks", updated, len(pending))
    return updated


def settle_picks() -> dict:
    """
    Settle all pending picks against final game results.
    First refreshes recent game scores from MLB API, then settles.
    """
    conn = get_conn()

    # Re-fetch recent game results so completed games are marked 'final'
    try:
        from scrapers.mlb_stats import fetch_schedule
        from datetime import timedelta
        today = datetime.now().strftime("%Y-%m-%d")
        three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        fetch_schedule(three_days_ago, today)
    except Exception as e:
        logger.warning("Could not refresh recent games: %s", e)

    pending = conn.execute(
        "SELECT * FROM picks WHERE result IS NULL"
    ).fetchall()

    if not pending:
        return {"settled": 0, "message": "No pending picks"}

    # Fetch current odds once for closing line capture
    try:
        from .picks import fetch_real_odds_for_games, match_odds
        all_closing_odds = fetch_real_odds_for_games()
    except Exception:
        all_closing_odds = {}

    settled = 0
    wins = 0
    losses = 0

    for pick in pending:
        pick = dict(pick)
        game_id = pick["game_id"]
        bt_probe = (pick["bet_type"] or "").upper()

        # F5 + NRFI markets can settle before the full game ends -- once
        # the linescore has enough innings, the outcome is locked. Only
        # full-game markets (ML, OU, RL) must wait for status='final'.
        f5_or_inning = bt_probe.startswith("F5") or bt_probe in ("1ST INN", "NRFI")
        if f5_or_inning:
            game = conn.execute(
                "SELECT * FROM games WHERE mlb_game_id = ?",
                (game_id,),
            ).fetchone()
        else:
            game = conn.execute(
                "SELECT * FROM games WHERE mlb_game_id = ? AND status = 'final'",
                (game_id,),
            ).fetchone()

        if not game:
            continue  # Game not loaded yet

        game = dict(game)
        hs = game.get("home_score", 0) or 0
        as_ = game.get("away_score", 0) or 0
        total_runs = hs + as_
        margin = hs - as_

        home_team = get_team_by_id(game.get("home_team_id"))
        away_team = get_team_by_id(game.get("away_team_id"))
        h = home_team["abbreviation"] if home_team else ""
        a = away_team["abbreviation"] if away_team else ""

        # Capture closing odds if not already stored. Delegates the
        # bet-type -> field mapping to _fetch_closing_odds_for_pick so
        # NRFI / F5 markets get the same treatment as ML/OU/RL.
        if not pick.get("closing_odds") and h and a:
            try:
                from .picks import match_odds as _match_odds
                game_odds = _match_odds(h, a, all_closing_odds)
                if game_odds:
                    bt_tmp = pick["bet_type"]
                    pk_tmp = pick["pick"]
                    # Reuse the per-bet-type extractor (handles NRFI/F5
                    # via the per-event fields we now store).
                    closing = _extract_closing_for_pick(
                        bt_tmp, pk_tmp, h, game_odds,
                    )
                    if closing is not None:
                        conn.execute("UPDATE picks SET closing_odds = ? WHERE id = ?",
                                     (int(closing), pick["id"]))
                        pick["closing_odds"] = int(closing)
            except Exception:
                pass

        result = None
        profit = 0
        bt = pick["bet_type"]
        pk = pick["pick"]
        odds = pick["odds"] or -110

        if bt in ("ml", "ML"):
            home_won = hs > as_
            if pk == h:
                won = home_won
            else:
                won = not home_won
            result = "W" if won else "L"

        elif bt in ("ou", "O/U", "ALT O/U"):
            # ALT O/U picks (from edge_enhancements alt-line shopper /
            # conservatism ladder swap) settle exactly like primary O/U
            # — same "Over N.N" / "Under N.N" pick label, just a
            # different line value. Without this branch they sat PEND
            # forever after settle ran.
            if "Over" in pk:
                line = float(pk.split()[-1])
                if total_runs > line:
                    result = "W"
                elif total_runs < line:
                    result = "L"
                else:
                    result = "P"
            else:
                line = float(pk.split()[-1])
                if total_runs < line:
                    result = "W"
                elif total_runs > line:
                    result = "L"
                else:
                    result = "P"

        elif bt in ("nrfi", "1st INN"):
            # Use real linescore data when available. Only trust the 1st
            # inning when it's locked: either the game is final, or the
            # linescore already has a 2nd inning entry (which the MLB
            # API adds after the 1st wraps). Otherwise skip and retry
            # on the next settle pass.
            import json as _json
            home_ls = game.get("home_linescore")
            away_ls = game.get("away_linescore")
            status = (game.get("status") or "").lower()
            scoreless_1st = None
            if home_ls and away_ls:
                try:
                    h_inn = _json.loads(home_ls)
                    a_inn = _json.loads(away_ls)
                    full_innings = min(len(h_inn), len(a_inn))
                    first_inning_locked = (
                        status == "final" or full_innings >= 2
                    )
                    if first_inning_locked and full_innings >= 1:
                        scoreless_1st = (h_inn[0] == 0 and a_inn[0] == 0)
                except Exception:
                    pass
            if scoreless_1st is None:
                # Fall back to total-runs heuristic only when the game is
                # final and linescore was unavailable (legacy rows).
                if status == "final":
                    scoreless_1st = total_runs <= 6
                else:
                    continue  # 1st inning still in progress

            if pk == "NRFI":
                result = "W" if scoreless_1st else "L"
            else:
                result = "W" if not scoreless_1st else "L"

        elif bt in ("rl", "RL", "ALT RL"):
            # ALT RL settles identically to primary RL — pick label
            # carries the team + signed spread regardless of which side
            # of the alt-line catalog it came from. Adding ALT RL here
            # closed the gap that left CHC -2.5 / similar alt picks
            # stuck on PEND after a full game settled.
            parts = pk.split()
            pick_team = parts[0] if parts else ""
            spread = float(parts[1]) if len(parts) > 1 else 1.5

            # Calculate margin from the picked team's perspective
            if pick_team == h:
                team_margin = hs - as_
            else:
                team_margin = as_ - hs

            # Team covers if their margin + spread > 0
            if team_margin + spread > 0:
                result = "W"
            elif team_margin + spread == 0:
                result = "P"
            else:
                result = "L"

        elif bt.upper().startswith("F5"):
            # First-5-innings markets. Need 5 complete innings before we
            # settle -- a mid-5th linescore entry would be partial data.
            # Two ways to be sure:
            #   - game is final (entire linescore is settled) OR
            #   - linescore has a 6th inning entry, which the MLB Stats
            #     API only adds once the 5th wraps.
            import json as _json
            h_inn: list = []
            a_inn: list = []
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                pass
            status = (game.get("status") or "").lower()
            full_innings = min(len(h_inn), len(a_inn))
            if status == "final":
                if full_innings < 5:
                    continue  # game ended before F5 resolved (rain etc.)
            else:
                if full_innings < 6:
                    continue  # mid-F5 or earlier

            f5_home = sum(h_inn[:5])
            f5_away = sum(a_inn[:5])
            f5_total = f5_home + f5_away
            f5_margin = f5_home - f5_away
            sub = bt.upper()[2:].strip()  # "ML" / "O/U" / "RL"

            if sub == "ML":
                # Tied after 5 -> push (F5 ML books settle this way).
                if f5_margin == 0:
                    result = "P"
                elif pk == h:
                    result = "W" if f5_margin > 0 else "L"
                else:
                    result = "W" if f5_margin < 0 else "L"

            elif sub in ("O/U", "OU"):
                # Pick text is e.g. "F5 Over 4.5" or "F5 Under 4.5".
                parts = pk.split()
                try:
                    line = float(parts[-1])
                except (ValueError, IndexError):
                    continue
                is_over = any(p.lower() == "over" for p in parts)
                if f5_total > line:
                    result = "W" if is_over else "L"
                elif f5_total < line:
                    result = "L" if is_over else "W"
                else:
                    result = "P"

            elif sub == "RL":
                parts = pk.split()
                pick_team = parts[0] if parts else ""
                try:
                    spread = float(parts[1]) if len(parts) > 1 else 0.5
                except ValueError:
                    spread = 0.5
                if pick_team == h:
                    team_margin = f5_margin
                else:
                    team_margin = -f5_margin
                if team_margin + spread > 0:
                    result = "W"
                elif team_margin + spread == 0:
                    result = "P"
                else:
                    result = "L"

        if result is None:
            continue  # Could not determine result - skip

        if result == "W":
            profit = (odds if odds > 0 else 100 / abs(odds) * 100)
            wins += 1
        elif result == "L":
            profit = -100
            losses += 1
        else:
            profit = 0  # Push

        conn.execute("""
            UPDATE picks SET result = ?, profit = ?, settled_at = datetime('now')
            WHERE id = ?
        """, (result, round(profit, 2), pick["id"]))
        settled += 1

    conn.commit()

    # Auto-refresh empirical calibration after settling so the model
    # learns from its latest outcomes immediately.
    if settled > 0:
        try:
            from .empirical_calibration import refresh_calibration
            refresh_calibration("mlb")
        except Exception:
            pass

    return {
        "settled": settled,
        "wins": wins,
        "losses": losses,
        "pending_remaining": conn.execute(
            "SELECT COUNT(*) as c FROM picks WHERE result IS NULL"
        ).fetchone()["c"],
    }


def get_pick_summary() -> dict:
    """Get running totals across all recorded picks."""
    conn = get_conn()

    summary = {}
    # Map canonical keys to all possible bet_type values (old lowercase + new uppercase)
    bt_aliases = {
        "ML": ("ML", "ml"),
        "O/U": ("O/U", "ou"),
        "1st INN": ("1st INN", "nrfi"),
        "RL": ("RL", "rl"),
        # F5 / first-5-innings markets are stored with the exact display
        # label by engine/picks.generate_picks, so the aliases tuple just
        # has the one canonical string each.
        "F5 ML": ("F5 ML",),
        "F5 O/U": ("F5 O/U",),
        "F5 RL": ("F5 RL",),
    }
    for bt, aliases in bt_aliases.items():
        placeholders = ",".join("?" for _ in aliases)
        row = conn.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
                COALESCE(SUM(profit), 0) as profit
            FROM picks WHERE bet_type IN ({placeholders})
        """, aliases).fetchone()

        total = row["total"] or 0
        w = row["wins"] or 0
        l = row["losses"] or 0
        settled = w + l
        summary[bt] = {
            "total": total,
            "wins": w,
            "losses": l,
            "pushes": row["pushes"],
            "pending": row["pending"],
            "profit": round(row["profit"], 2),
            "win_pct": round(w / settled * 100, 1) if settled > 0 else 0,
            "roi": round(row["profit"] / settled, 1) if settled > 0 else 0,
        }

    # Aggregate F5 tile -- the UI tile is a single "First 5 Innings" card
    # summing ML + O/U + RL variants. Per-market splits remain under the
    # individual keys for anyone wanting the breakdown.
    f5_rows = [summary.get(k) for k in ("F5 ML", "F5 O/U", "F5 RL")]
    f5_rows = [r for r in f5_rows if r]
    if f5_rows:
        agg_total = sum(r["total"] for r in f5_rows)
        agg_w = sum(r["wins"] for r in f5_rows)
        agg_l = sum(r["losses"] for r in f5_rows)
        agg_p = sum((r["pushes"] or 0) for r in f5_rows)
        agg_pend = sum((r["pending"] or 0) for r in f5_rows)
        agg_profit = round(sum(r["profit"] for r in f5_rows), 2)
        settled = agg_w + agg_l
        summary["F5"] = {
            "total": agg_total,
            "wins": agg_w,
            "losses": agg_l,
            "pushes": agg_p,
            "pending": agg_pend,
            "profit": agg_profit,
            "win_pct": round(agg_w / settled * 100, 1) if settled > 0 else 0,
            "roi": round(agg_profit / settled, 1) if settled > 0 else 0,
        }

    # Recent picks
    recent = conn.execute("""
        SELECT * FROM picks ORDER BY created_at DESC LIMIT 20
    """).fetchall()

    # Overall
    totals = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
            COALESCE(SUM(profit), 0) as profit
        FROM picks
    """).fetchone()

    tw = totals["wins"] or 0
    tl = totals["losses"] or 0

    # Compute CLV across all settled picks that have closing odds
    clv_rows = conn.execute("""
        SELECT odds, closing_odds FROM picks
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
        print(f"Recording today's picks{' (force reset)' if force else ''}...", flush=True)
        picks = record_picks(force=force)
        print(f"Recorded {len(picks)} picks:")
        for p in picks:
            print(f"  {p['matchup']} | {p['type']:5s} | {p['pick']:15s} | {p['prob']:.1%} | edge: {p['edge']:+.1f}%")

    elif "--settle" in args:
        print("Settling completed picks...", flush=True)
        result = settle_picks()
        print(f"Settled: {result['settled']} ({result['wins']}W-{result['losses']}L)")
        print(f"Pending: {result['pending_remaining']}")

    elif "--summary" in args:
        summary = get_pick_summary()
        overall = summary["overall"]
        print(f"\n{'='*50}")
        print(f"  PICK TRACKER - Running Totals")
        print(f"{'='*50}")
        print(f"  Total picks: {overall['total']}")
        print(f"  Record: {overall['wins']}-{overall['losses']} ({overall['win_pct']}%)")
        print(f"  Profit: ${overall['profit']:+.2f}")
        print(f"  Pending: {overall['pending']}")
        if overall.get("avg_clv") is not None:
            print(f"  Avg CLV: {overall['avg_clv']:+.2f}% ({overall['clv_sample']} picks)")
        print()
        for bt, label in [("ML", "Moneyline"), ("O/U", "Over/Under"), ("1st INN", "1st Inning"), ("RL", "Run Line")]:
            s = summary["by_type"][bt]
            if s["total"] == 0:
                continue
            print(f"  {label}: {s['wins']}-{s['losses']} ({s['win_pct']}%) ${s['profit']:+.2f}")
        print(f"{'='*50}")

    else:
        print("Usage: python -m engine.tracker --record | --settle | --summary")
