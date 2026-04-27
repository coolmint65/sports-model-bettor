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

# Phase 1 derivative bet types — excluded from main tracker recording
# at sync time. Derivatives flow through engine.derivative_tracker
# instead so their performance is logged in isolation.
_MLB_DERIVATIVE_TYPES: set[str] = {
    "Team Total", "F5 Team Total", "Inning Total", "Inning BTS",
    "1st Inn Winner", "F5 Winner", "Total O/E", "Extra Innings",
}


def _core_picks(picks: list[dict]) -> list[dict]:
    """Drop derivative bet types so the main tracker stays focused on
    core markets (ML/RL/O/U/F5/1st INN/ALT)."""
    return [p for p in picks if p.get("type") not in _MLB_DERIVATIVE_TYPES]


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


def refresh_pending_for_today(bets: list[dict],
                               target_date: str | None = None) -> dict:
    """Reconcile today's tracker pending picks against the live model.

    Called from /api/best-bets after each refresh so the tracker
    dashboard reflects what the model thinks RIGHT NOW, not what it
    recorded at the morning sync. Three transitions:

        - same pick, same line  → update prob/edge/odds (calibration
          may have shifted, line may have moved within the same pick)
        - different bet_type or pick  → swap (model changed its mind)
        - no current best for that matchup  → delete (model no longer
          sees edge in this game)

    Pending picks for live or completed games are left alone — once
    the user could have placed the bet, the historical record stands.

    Returns ``{"updated": N, "swapped": N, "voided": N}``.
    """
    target_date = target_date or datetime.now().strftime("%Y-%m-%d")
    conn = get_conn()

    # Index current bests by matchup, ONLY for games still UNLOCKED
    # (more than 1hr from tip-off). Once locked, the tracker entry
    # stays frozen at whatever was recorded — no last-minute swaps.
    # Track locked matchups separately so the void path below can
    # tell "model lost edge" (DELETE) from "game is locked, leave the
    # frozen pick in place" — earlier code only checked the unlocked
    # index and silently deleted every locked pending row as soon as
    # the game entered the 1hr window.
    current_by_matchup: dict[str, dict] = {}
    locked_matchups: set[str] = set()
    # Defense: also derive lock from the bets entry's own start time.
    # ``is_locked`` is precomputed elsewhere and has been seen to return
    # False on games clearly underway. Treating any past-tip game as
    # locked here is the right semantic regardless of upstream flag.
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
        "SELECT id, matchup, bet_type, pick, game_id FROM picks "
        "WHERE date = ? AND result IS NULL",
        (target_date,),
    ).fetchall()

    # Per-pick fallback lock check: query games table directly for the
    # pick's own game start time. Belt-and-suspenders against the bets
    # dict missing the game (e.g. doubleheaders where the matchup
    # string is shared, or scoreboard filtering an in-progress game out
    # for any reason). Without this, a pending Under 7.5 can be silently
    # swapped to a NYM ML +2250 *during the game* because no one in the
    # bets dict claimed the matchup as locked at refresh time.
    def _pick_game_started(game_id) -> bool:
        if not game_id:
            return False
        row = conn.execute(
            "SELECT date, status FROM games WHERE mlb_game_id = ? LIMIT 1",
            (game_id,),
        ).fetchone()
        if not row:
            return False
        # Status-based lock first (most reliable): live/final/postponed
        # all mean "the user could have placed by now, freeze it".
        if row["status"] in ("live", "final", "postponed"):
            return True
        # Date-based fallback for scheduled rows: the row's `date` field
        # is the game date (not the precise tip-off ISO), so we only
        # treat it as "started" when date < today (yesterday's row that
        # never finalized).
        try:
            return str(row["date"]) < target_date
        except Exception:
            return False

    updated = swapped = voided = 0
    for p in pending:
        p = dict(p)
        # Locked games are frozen — never swap, never void. The pick
        # the user could have placed at lock time is the historical
        # record we want to keep.
        if p["matchup"] in locked_matchups:
            continue
        if _pick_game_started(p.get("game_id")):
            # Defense in depth: even if the bets dict didn't flag this
            # matchup as locked (game missing from /api/best-bets, dh
            # collision, etc.), the games table says it's already
            # underway. Don't touch the pending row.
            continue
        current = current_by_matchup.get(p["matchup"])
        if not current:
            # Model has no playable pick for this matchup any more —
            # could be no current edge OR the game already started.
            # We can only safely delete in the no-edge case; for the
            # game-started case we want to preserve history. Detect
            # by checking if the matchup exists in `bets` at all.
            matchup_in_response = any(b["matchup"] == p["matchup"] for b in bets)
            if matchup_in_response:
                # Game still in pre and shown on best-bets, but no
                # best_pick → model lost edge. Drop the pending row.
                conn.execute("DELETE FROM picks WHERE id = ?", (p["id"],))
                voided += 1
            # else: matchup absent (game live/completed) — leave alone
            continue

        if current.get("type") != p["bet_type"] or current.get("pick") != p["pick"]:
            conn.execute(
                "UPDATE picks SET bet_type = ?, pick = ?, model_prob = ?, "
                "edge = ?, odds = ? WHERE id = ?",
                (current.get("type"), current.get("pick"),
                 current.get("prob"), current.get("edge"),
                 current.get("odds"), p["id"]),
            )
            swapped += 1
        else:
            conn.execute(
                "UPDATE picks SET model_prob = ?, edge = ?, odds = ? "
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

    # Try to sync today's schedule when the DB is empty OR partial.
    # Partial-table case happens when a postponement got rescheduled
    # (so 1 row exists from yesterday's import) but the day's actual
    # 14-game slate hasn't been ingested yet — without the partial
    # check, record_picks would only process the lone postponed row.
    if len(games) < 5:
        logger.info("Only %d games in DB for %s — fetching MLB API to fill gaps",
                    len(games), target_date)
        try:
            from scrapers.mlb_stats import fetch_schedule
            fetch_schedule(target_date, target_date)
            games = conn.execute("""
                SELECT * FROM games WHERE date = ?
            """, (target_date,)).fetchall()
        except Exception as e:
            logger.warning("Could not fetch today's schedule: %s", e)

    # Still empty? Fall through to ESPN scoreboard as last resort
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
                best = get_best_pick(_core_picks(picks))
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
        best = get_best_pick(_core_picks(picks))
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

        best = get_best_pick(_core_picks(picks))
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

        # Postponed/cancelled games leave the row in the DB but with NULL
        # scores. Without this guard, settle_picks reads scores as 0,
        # computes total_runs=0, and silently turns "Under N.5" picks
        # into phantom W's. Skip until a real result is recorded.
        if game.get("status") == "postponed":
            continue
        if game.get("home_score") is None or game.get("away_score") is None:
            continue

        hs = game["home_score"]
        as_ = game["away_score"]
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

        elif bt.upper().startswith("F5") and bt not in ("F5 Team Total", "F5 Winner"):
            # First-5-innings markets (F5 ML / F5 O/U / F5 RL). The two
            # exclusions above have their own specific settlers later in
            # the chain — without the exclusion, this branch swallows them
            # ("F5 TEAM TOTAL"[2:] == "TEAM TOTAL" doesn't match ML/O/U/RL,
            # so the pick stays pending forever).
            #
            # Need 5 complete innings before we settle -- a mid-5th
            # linescore entry would be partial data. Two ways to be sure:
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

        # ── Phase 1 derivative markets ──
        # Linescore-aware settlers for the new MLB bet types. Most need
        # inning-by-inning data; we re-use the same h_inn/a_inn parse
        # the F5 block does, plus a tiny "is the Nth inning locked yet"
        # check (final OR linescore has inning N+1) so picks settle as
        # early as possible without snapping on partial data.
        elif bt == "Team Total":
            # "NYY Over 4.5" / "BOS Under 4.0"
            if (game.get("status") or "").lower() != "final":
                continue
            parts = pk.split()
            if len(parts) >= 3:
                pick_team = parts[0]
                direction = parts[1].lower()
                try:
                    line = float(parts[2])
                except ValueError:
                    continue
                team_runs = hs if pick_team == h else as_
                if team_runs > line:
                    result = "W" if direction == "over" else "L"
                elif team_runs < line:
                    result = "L" if direction == "over" else "W"
                else:
                    result = "P"

        elif bt == "F5 Team Total":
            # "NYY F5 Over 2.5" — same locking rule as F5 markets above.
            import json as _json
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                continue
            status = (game.get("status") or "").lower()
            full_innings = min(len(h_inn), len(a_inn))
            if status == "final":
                if full_innings < 5:
                    continue
            elif full_innings < 6:
                continue
            parts = pk.split()
            if len(parts) >= 4:
                pick_team = parts[0]
                direction = parts[2].lower()
                try:
                    line = float(parts[3])
                except ValueError:
                    continue
                team_f5 = sum(h_inn[:5]) if pick_team == h else sum(a_inn[:5])
                if team_f5 > line:
                    result = "W" if direction == "over" else "L"
                elif team_f5 < line:
                    result = "L" if direction == "over" else "W"
                else:
                    result = "P"

        elif bt == "Inning Total":
            # "Inn N Over 0.5" / "Inn N Under 0.5"
            import json as _json
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                continue
            parts = pk.split()
            if len(parts) >= 4:
                try:
                    inning_n = int(parts[1])
                    line = float(parts[3])
                except ValueError:
                    continue
                direction = parts[2].lower()
                # Inning N is locked when game is final OR linescore has
                # inning N+1 (mirror of NRFI's "next inning logged" rule).
                status = (game.get("status") or "").lower()
                full_innings = min(len(h_inn), len(a_inn))
                if not (status == "final" or full_innings > inning_n):
                    continue
                if inning_n < 1 or inning_n > full_innings:
                    continue
                inn_total = h_inn[inning_n - 1] + a_inn[inning_n - 1]
                if inn_total > line:
                    result = "W" if direction == "over" else "L"
                elif inn_total < line:
                    result = "L" if direction == "over" else "W"
                else:
                    result = "P"

        elif bt == "Inning BTS":
            # "Inn N BTS Yes" / "Inn N BTS No"
            import json as _json
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                continue
            parts = pk.split()
            if len(parts) >= 4:
                try:
                    inning_n = int(parts[1])
                except ValueError:
                    continue
                direction = parts[3].lower()
                status = (game.get("status") or "").lower()
                full_innings = min(len(h_inn), len(a_inn))
                if not (status == "final" or full_innings > inning_n):
                    continue
                if inning_n < 1 or inning_n > full_innings:
                    continue
                bts_yes = (h_inn[inning_n - 1] > 0 and a_inn[inning_n - 1] > 0)
                if direction == "yes":
                    result = "W" if bts_yes else "L"
                else:
                    result = "W" if not bts_yes else "L"

        elif bt == "1st Inn Winner":
            # "1st Inn NYY" / "1st Inn BOS" / "1st Inn Tie"
            import json as _json
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                continue
            status = (game.get("status") or "").lower()
            full_innings = min(len(h_inn), len(a_inn))
            if not (status == "final" or full_innings >= 2):
                continue
            if full_innings < 1:
                continue
            h1 = h_inn[0]
            a1 = a_inn[0]
            parts = pk.split()
            if len(parts) >= 3:
                pick_label = parts[2]  # team abbr or "Tie"
                if pick_label.lower() == "tie":
                    result = "W" if h1 == a1 else "L"
                elif pick_label == h:
                    result = "W" if h1 > a1 else "L"
                else:
                    result = "W" if a1 > h1 else "L"

        elif bt == "F5 Winner":
            # "F5 NYY" / "F5 BOS" / "F5 Tie" — 3-way, tie is a winnable
            # outcome (different from F5 ML which pushes on tie).
            import json as _json
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                continue
            status = (game.get("status") or "").lower()
            full_innings = min(len(h_inn), len(a_inn))
            if status == "final":
                if full_innings < 5:
                    continue
            elif full_innings < 6:
                continue
            f5_home = sum(h_inn[:5])
            f5_away = sum(a_inn[:5])
            parts = pk.split()
            if len(parts) >= 2:
                pick_label = parts[1]
                if pick_label.lower() == "tie":
                    result = "W" if f5_home == f5_away else "L"
                elif pick_label == h:
                    result = "W" if f5_home > f5_away else "L"
                else:
                    result = "W" if f5_away > f5_home else "L"

        elif bt == "Total O/E":
            if (game.get("status") or "").lower() != "final":
                continue
            parts = pk.split()
            if len(parts) >= 2:
                direction = parts[1].lower()
                is_odd = (total_runs % 2 == 1)
                if direction == "odd":
                    result = "W" if is_odd else "L"
                else:
                    result = "W" if not is_odd else "L"

        elif bt == "Extra Innings":
            # "Extra Innings Yes" / "Extra Innings No". Only settles on
            # final — partial linescores can't tell us whether the game
            # will reach a 10th inning.
            if (game.get("status") or "").lower() != "final":
                continue
            import json as _json
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                continue
            went_extras = max(len(h_inn), len(a_inn)) > 9
            parts = pk.split()
            if len(parts) >= 3:
                direction = parts[2].lower()
                if direction == "yes":
                    result = "W" if went_extras else "L"
                else:
                    result = "W" if not went_extras else "L"

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
