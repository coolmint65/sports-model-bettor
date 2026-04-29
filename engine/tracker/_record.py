"""Recording today's picks + reconciling pending rows against live model.

Three entry points:
  - record_picks(date, min_edge, force)
        Run the model on today's games, record each game's best_pick.
        Falls back to ESPN scoreboard if the games table is empty.
  - refresh_pending_for_today(bets, target_date)
        After /api/best-bets recomputes, sweep the tracker's pending
        rows so they reflect what the model thinks NOW. Three
        transitions: update / swap / void. Locked games stay frozen.
  - capture_closing_odds()
        Snapshot HR closing prices on every pending pick that doesn't
        have closing_odds yet. Called from the sync script ~30min
        before tip-off so CLV computed at settle time uses the true
        closing line.

A 1st INN side-track INSERT runs alongside the headline best_pick so
NRFI/YRFI picks (per-market 0.5% edge floor) settle independently of
whether they were the per-game headline.
"""

from __future__ import annotations
import logging
from datetime import datetime

from ..db import get_conn, get_team_by_id
from ._helpers import _core_picks, _extract_closing_for_pick
from ._scoreboard import _fetch_espn_scoreboard

logger = logging.getLogger(__name__)


def refresh_pending_for_today(bets: list[dict],
                               target_date: str | None = None) -> dict:
    """Reconcile today's tracker pending picks against the live model.

    Called from /api/best-bets after each refresh so the tracker
    dashboard reflects what the model thinks RIGHT NOW. Three
    transitions:
        - same pick, same line  → update prob/edge/odds
        - different bet_type or pick  → swap
        - no current best for that matchup  → delete

    Pending picks for live or completed games are left alone — once
    the user could have placed the bet, the historical record stands.

    Returns ``{"updated": N, "swapped": N, "voided": N}``.
    """
    target_date = target_date or datetime.now().strftime("%Y-%m-%d")
    conn = get_conn()

    # Index current bests by matchup, ONLY for games still UNLOCKED.
    # Once locked, the tracker entry stays frozen.
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
        "SELECT id, matchup, bet_type, pick, game_id FROM picks "
        "WHERE date = ? AND result IS NULL",
        (target_date,),
    ).fetchall()

    # Per-pick fallback lock check via the games table — belt-and-
    # suspenders against the bets dict missing the game.
    def _pick_game_started(game_id) -> bool:
        if not game_id:
            return False
        row = conn.execute(
            "SELECT date, status FROM games WHERE mlb_game_id = ? LIMIT 1",
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
                conn.execute("DELETE FROM picks WHERE id = ?", (p["id"],))
                voided += 1
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
    """Run model on today's games and record the best pick per game.
    Uses the unified picks engine for consistent edge calculations.
    Falls back to ESPN scoreboard if games aren't in the DB.

    Args:
        date: Target YYYY-MM-DD (defaults to today).
        min_edge: Minimum edge percentage to record a pick.
        force: If True, delete any unsettled pick for each game before
            recording so the latest model/odds take precedence.
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

    if not games:
        try:
            scoreboard = _fetch_espn_scoreboard(target_date)
            if scoreboard:
                logger.info("Using ESPN scoreboard (%d games)", len(scoreboard))
                return _record_from_scoreboard(conn, scoreboard, target_date, min_edge)
        except Exception as e:
            logger.warning("Scoreboard fallback failed: %s", e)
        return []

    from ..picks import (
        generate_picks, get_best_pick, fetch_real_odds_for_games,
        match_odds, _valid_odds,
    )

    all_odds = fetch_real_odds_for_games()

    # Build set of live/completed games from ESPN scoreboard so we
    # don't record picks for in-progress or finished games.
    from ..abbr import aliases_for as _aliases
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
    except Exception as e:
        logger.debug("live/done seed failed: %s", e)

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

        if h in _live_or_done or a in _live_or_done:
            continue

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
                        INSERT OR IGNORE INTO picks (game_id, date, matchup, bet_type, pick,
                                         model_prob, edge, odds)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (game_id, target_date, matchup, best["type"], best["pick"],
                          best["prob"], best["edge"], best["odds"]))
                    recorded.append({
                        "matchup": matchup, "type": best["type"],
                        "pick": best["pick"], "prob": round(best["prob"], 3),
                        "edge": round(best["edge"], 1), "odds": best["odds"],
                    })
                # 1st INN side-track: NRFI/YRFI runs its own per-market
                # edge floor (currently 0.5%). Log them separately so
                # the 1st INN tracker collects evidence even when ML or
                # RL is the dashboard headline.
                fi = next((p for p in picks
                           if p.get("type") == "1st INN"
                           and (p.get("confidence") or "lean") != "skip"
                           and _valid_odds(p.get("odds"))), None)
                if fi:
                    conn.execute("""
                        INSERT OR IGNORE INTO picks (game_id, date, matchup, bet_type, pick,
                                         model_prob, edge, odds)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (game_id, target_date, matchup, fi["type"], fi["pick"],
                          fi["prob"], fi["edge"], fi["odds"]))
                continue
        except Exception as e:
            logger.debug("picks_store branch failed for %s: %s — regenerating", matchup, e)

        game_odds = match_odds(h, a, all_odds)

        picks = generate_picks(
            home_team_id=home_id,
            away_team_id=away_id,
            home_pitcher_id=game.get("home_pitcher_id"),
            away_pitcher_id=game.get("away_pitcher_id"),
            venue=game.get("venue"),
            odds=game_odds,
        )

        # Defense-in-depth odds sanity check.
        best = get_best_pick(_core_picks(picks))
        if not best or best["edge"] < min_edge:
            continue
        if not _valid_odds(best.get("odds")):
            logger.warning("Skipping pick with invalid odds=%s for %s",
                           best.get("odds"), matchup)
            continue

        conn.execute("""
            INSERT OR IGNORE INTO picks (game_id, date, matchup, bet_type, pick,
                             model_prob, edge, odds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (game_id, target_date, matchup, best["type"], best["pick"],
              best["prob"], best["edge"], best["odds"]))

        recorded.append({
            "matchup": matchup, "type": best["type"],
            "pick": best["pick"], "prob": round(best["prob"], 3),
            "edge": round(best["edge"], 1), "odds": best["odds"],
        })

        # 1st INN side-track (see picks_store branch above for rationale).
        fi = next((p for p in picks
                   if p.get("type") == "1st INN"
                   and (p.get("confidence") or "lean") != "skip"
                   and _valid_odds(p.get("odds"))), None)
        if fi:
            conn.execute("""
                INSERT OR IGNORE INTO picks (game_id, date, matchup, bet_type, pick,
                                 model_prob, edge, odds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (game_id, target_date, matchup, fi["type"], fi["pick"],
                  fi["prob"], fi["edge"], fi["odds"]))

    conn.commit()
    return recorded


def _record_from_scoreboard(conn, scoreboard: list, target_date: str,
                            min_edge: float) -> list[dict]:
    """Record picks using live scoreboard data when DB has no games."""
    from ..picks import (
        generate_picks, get_best_pick, match_odds, fetch_real_odds_for_games,
        _valid_odds,
    )

    all_odds = fetch_real_odds_for_games()
    recorded = []

    for game in scoreboard:
        if game.get("status", {}).get("completed") or game.get("status", {}).get("state") == "post":
            continue

        game_id = game.get("id") or game.get("game_pk")
        if not game_id:
            continue

        h = game.get("home", {}).get("abbreviation", "?")
        a = game.get("away", {}).get("abbreviation", "?")
        matchup = f"{a} @ {h}"

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

        game_odds = game.get("odds") or match_odds(h, a, all_odds)

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
            INSERT OR IGNORE INTO picks (game_id, date, matchup, bet_type, pick,
                             model_prob, edge, odds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (game_id, target_date, matchup, best["type"], best["pick"],
              best["prob"], best["edge"], best["odds"]))

        recorded.append({
            "matchup": matchup, "type": best["type"],
            "pick": best["pick"], "prob": round(best["prob"], 3),
            "edge": round(best["edge"], 1), "odds": best["odds"],
        })

        # 1st INN side-track (see record_picks for rationale).
        fi = next((p for p in picks
                   if p.get("type") == "1st INN"
                   and (p.get("confidence") or "lean") != "skip"
                   and _valid_odds(p.get("odds"))), None)
        if fi:
            conn.execute("""
                INSERT OR IGNORE INTO picks (game_id, date, matchup, bet_type, pick,
                                 model_prob, edge, odds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (game_id, target_date, matchup, fi["type"], fi["pick"],
                  fi["prob"], fi["edge"], fi["odds"]))

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

    from ..picks import fetch_real_odds_for_games, match_odds
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
