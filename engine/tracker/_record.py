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
from .._tz import et_today_str

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
    target_date = target_date or et_today_str()
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

    # Lock-at-game-start rule (revised 2026-04-29):
    #   - Prematch (game still scheduled): swap freely. Model can
    #     re-rank bet_type / pick within the matchup; the row updates
    #     in place. closing_odds resets to NULL because it's a
    #     different bet now.
    #   - Live / final / postponed (game lock): freeze. The bet_started
    #     guard above + _pick_game_started already short-circuit those
    #     before we reach this loop.
    #
    # The earlier "freeze the moment the row is recorded" rule (set
    # 2026-04-28 after the PHI@BOS swap incident) was too aggressive —
    # it stopped the picker from refining mid-day in the cases the
    # user wants tracked. pick_events still keeps the breadcrumb of
    # every model decision regardless of whether the picks row mutates.
    updated = swapped = voided = 0
    for p in pending:
        p = dict(p)
        if p["matchup"] in locked_matchups:
            continue
        if _pick_game_started(p.get("game_id")):
            continue
        current = current_by_matchup.get(p["matchup"])
        # 'skip'-tier picks are below the lean floor; the card filter
        # already hides them. Void the tracker row to match — see
        # nhl_tracker._record for the bug-trigger context.
        if current and (current.get("confidence") or "lean") == "skip":
            current = None
        if not current:
            matchup_in_response = any(b["matchup"] == p["matchup"] for b in bets)
            if matchup_in_response:
                conn.execute("DELETE FROM picks WHERE id = ?", (p["id"],))
                voided += 1
            continue

        if current.get("type") != p["bet_type"] or current.get("pick") != p["pick"]:
            # Prematch swap — overwrite bet_type, pick, and price in
            # place. closing_odds resets because the new pick has its
            # own line.
            conn.execute(
                "UPDATE picks SET bet_type = ?, pick = ?, model_prob = ?, "
                "  edge = ?, odds = ?, closing_odds = NULL "
                "WHERE id = ?",
                (current.get("type"), current.get("pick"),
                 current.get("prob"), current.get("edge"),
                 current.get("odds"), p["id"]),
            )
            swapped += 1
            continue

        conn.execute(
            "UPDATE picks SET model_prob = ?, edge = ?, odds = ? "
            "WHERE id = ?",
            (current.get("prob"), current.get("edge"),
             current.get("odds"), p["id"]),
        )
        updated += 1

    conn.commit()
    return {"updated": updated, "swapped": swapped, "voided": voided}


def _live_or_done_game_ids(target_date: str) -> set[str]:
    """Return the set of MLB game_ids for ``target_date`` that are
    currently in live or final state per ESPN scoreboard. Used by
    ``record_picks(force=True)`` to scope the destructive DELETE so
    tracker rows for live games aren't wiped.

    Maps ESPN abbreviations → games.mlb_game_id via the games table.
    Empty set on any failure — callers must treat empty as "couldn't
    determine" (not "no games are live")."""
    try:
        from ..abbr import aliases_for as _aliases
    except Exception:
        return set()
    try:
        scoreboard = _fetch_espn_scoreboard(target_date)
    except Exception:
        return set()
    if not scoreboard:
        return set()
    abbr_set: set[str] = set()
    for game_info in scoreboard:
        state = (game_info.get("status") or {}).get("state", "pre")
        if state not in ("in", "post"):
            continue
        h = (game_info.get("home") or {}).get("abbreviation", "")
        a = (game_info.get("away") or {}).get("abbreviation", "")
        if h:
            abbr_set.update(_aliases(h, sport="mlb"))
        if a:
            abbr_set.update(_aliases(a, sport="mlb"))
    if not abbr_set:
        return set()
    # Look up the actual game_ids for those abbr matchups today.
    conn = get_conn()
    placeholders = ",".join("?" * len(abbr_set))
    rows = conn.execute(
        f"SELECT g.mlb_game_id FROM games g "
        f"LEFT JOIN teams ht ON g.home_team_id = ht.id "
        f"LEFT JOIN teams at ON g.away_team_id = at.id "
        f"WHERE g.date = ? AND ("
        f"  ht.abbreviation IN ({placeholders}) "
        f"  OR at.abbreviation IN ({placeholders}))",
        (target_date, *abbr_set, *abbr_set),
    ).fetchall()
    return {str(r["mlb_game_id"]) for r in rows if r["mlb_game_id"] is not None}


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
    target_date = date or et_today_str()

    # Note: when ``force=True``, deletion happens PER-GAME inside the
    # loop (not as a blanket pre-DELETE). This is intentional — the
    # loop skips live/done games, so per-game delete naturally scopes
    # the destructive op to games we're actually re-recording. Bug
    # surfaced 2026-05-03 when blanket pre-DELETE wiped MLB picks for
    # today's live games and the loop's live/done filter prevented
    # re-insertion → tracker empty.

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

        # Build matchup abbreviation alias set up-front — used by both
        # the force-delete and the existence check below. Same MLB game
        # surfaces as "AZ @ COL" from MLB stats and "ARI @ COL" from
        # ESPN; without alias expansion the second variant creates a
        # duplicate pending pick the settler can't match.
        from ..abbr import aliases_for as _aliases_for
        away_part, _, home_part = matchup.partition(" @ ")
        matchup_variants = {matchup}
        if away_part and home_part:
            for a in _aliases_for(away_part) or [away_part]:
                for h in _aliases_for(home_part) or [home_part]:
                    matchup_variants.add(f"{a} @ {h}")
        mv_list = list(matchup_variants)
        mv_placeholders = ",".join("?" * len(mv_list))

        # Per-game force-delete (replaces the old blanket pre-DELETE).
        # Only fires for games we're about to re-record (i.e. pre-game),
        # so live games' pending rows survive untouched. Matches across
        # all abbreviation aliases so a stale ARI @ COL row gets cleared
        # when the canonical AZ @ COL is re-recorded.
        if force:
            conn.execute(
                f"DELETE FROM picks WHERE result IS NULL AND date = ? "
                f"AND (game_id = ? OR matchup IN ({mv_placeholders}))",
                (target_date, game_id, *mv_list),
            )
        existing = conn.execute(
            f"SELECT COUNT(*) as c FROM picks "
            f"WHERE game_id = ? "
            f"   OR (date = ? AND matchup IN ({mv_placeholders}))",
            (game_id, target_date, *mv_list),
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
                                         model_prob, edge, odds, stake_units)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (game_id, target_date, matchup, best["type"], best["pick"],
                          best["prob"], best["edge"], best["odds"],
                          best.get("stake_units")))
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
                                         model_prob, edge, odds, stake_units)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (game_id, target_date, matchup, fi["type"], fi["pick"],
                          fi["prob"], fi["edge"], fi["odds"],
                          fi.get("stake_units")))
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
                             model_prob, edge, odds, stake_units)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (game_id, target_date, matchup, best["type"], best["pick"],
              best["prob"], best["edge"], best["odds"],
              best.get("stake_units")))

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
                                 model_prob, edge, odds, stake_units)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (game_id, target_date, matchup, fi["type"], fi["pick"],
                  fi["prob"], fi["edge"], fi["odds"],
                  fi.get("stake_units")))

    conn.commit()

    # Write-through to the legacy unified_tracker (predates picks_unified).
    try:
        from ..unified_tracker import sync_for_date
        sync_for_date("mlb", target_date)
    except Exception as e:
        logger.debug("unified write-through (mlb) skipped: %s", e)

    # Write-through to picks_unified (new canonical layer). Reads back
    # the rows we just inserted and mirrors them — cleaner than
    # touching each of the 4 INSERT sites above.
    try:
        from ..picks_unified._legacy_bridge import mirror_to_unified
        recent = conn.execute(
            "SELECT * FROM picks WHERE date=? "
            "ORDER BY id DESC LIMIT 100",
            (target_date,),
        ).fetchall()
        for row in recent:
            mirror_to_unified(
                sport="mlb", league="mlb",
                native_game_id=row["game_id"],
                pick_date=row["date"],
                matchup=row["matchup"] or "",
                bet_type=row["bet_type"] or "",
                pick_text=row["pick"] or "",
                odds=int(row["odds"] or 0),
                prob=float(row["model_prob"] or 0.0),
                edge_pct=float(row["edge"] or 0.0),
                stake_units=float(row["stake_units"] or 0.0)
                             if "stake_units" in row.keys() else 0.0,
                closing_odds=row["closing_odds"]
                              if "closing_odds" in row.keys() else None,
                result=row["result"],
                profit=row["profit"],
                created_at=row["created_at"],
                settled_at=row["settled_at"],
            )
    except Exception as e:
        logger.debug("picks_unified mirror (mlb) skipped: %s", e)

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
                             model_prob, edge, odds, stake_units)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (game_id, target_date, matchup, best["type"], best["pick"],
              best["prob"], best["edge"], best["odds"],
              best.get("stake_units")))

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
                                 model_prob, edge, odds, stake_units)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (game_id, target_date, matchup, fi["type"], fi["pick"],
                  fi["prob"], fi["edge"], fi["odds"],
                  fi.get("stake_units")))

    conn.commit()
    logger.info("Recorded %d picks from scoreboard", len(recorded))
    try:
        from ..unified_tracker import sync_for_date
        sync_for_date("mlb", target_date)
    except Exception as e:
        logger.debug("unified write-through (mlb scoreboard) skipped: %s", e)
    return recorded


def capture_closing_odds() -> int:
    """Snapshot current Hard Rock odds for all pending picks.
    Thin wrapper around ``engine.tracker_core.core_capture_closing_odds``
    with the MLB adapter — see that module for the refresh semantics."""
    from ..picks import fetch_real_odds_for_games
    from ..tracker_core import SportAdapter, core_capture_closing_odds
    adapter = SportAdapter(
        name="mlb",
        get_conn=get_conn,
        picks_table="picks",
        hr_fetch=fetch_real_odds_for_games,
        extract_closing=_extract_closing_for_pick,
    )
    return core_capture_closing_odds(adapter)
