"""
Player-props settler (Phase 2f-iii).

Mirrors ``engine.derivative_tracker.settle_derivative_picks`` but
points at ``player_props_picks``. Unlike derivatives, props don't
trampoline through the main game settler — game outcomes can't tell
us how many strikeouts a pitcher recorded. Instead the settler reads
``player_game_logs`` (populated by Phase 2g/2h/2i ingest) and compares
the observed stat against each pick's line.

Outcome rule is uniform Over/Under: ``actual > line`` wins for Over,
``actual < line`` wins for Under, ties push. Alt yes/no markets are
encoded by the scraper as Over X.5 (a "5+ Strikeouts" alt becomes
``line=4.5, side='Over'``) so this module never needs an ``is_alt``
branch — the .5 line + Over rule already gets actual≥5 right.

Because game-log ingest doesn't ship until 2g, this module
short-circuits cleanly when no log row exists for a pick: the row
stays pending. Once 2g lands, the same call drains the queue.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from .player_props_db import _conn_for, list_picks, settle_pick

logger = logging.getLogger(__name__)


# bet_type → key in the ``stats_json`` blob written by 2g/2h/2i ingest.
# Centralizing here so the ingest can target the same keys without
# duplicating the contract.
_MLB_STAT_KEY: dict[str, str] = {
    # Pitcher / batter Ks and BBs are namespaced per role so two-way
    # players (Ohtani-style) keep both lines on one row without a
    # collision. Other stats are uniquely pitcher-only or batter-only.
    "Pitcher Ks O/U":         "k_p",
    "Pitcher Walks O/U":      "bb_p",
    "Pitcher Outs Recorded":  "outs",
    "Pitcher Earned Runs":    "er",
    "Pitcher Hits Allowed":   "h_allowed",
    "Batter HR":              "hr",
    "Batter Hits O/U":        "h",
    "Batter TB":              "tb",
    "Batter RBI":             "rbi",
    "Batter Runs Scored":     "r",
    "Batter Stolen Bases":    "sb",
    "Batter Walks":           "bb_b",
    "Batter Strikeouts":      "k_b",
}


_NBA_STAT_KEY: dict[str, str] = {
    "Player Points":    "pts",
    "Player Rebounds":  "reb",
    "Player Assists":   "ast",
    "Player PRA":       "pra",        # synthesized from pts+reb+ast at lookup
    "Player 3PM":       "tpm",
    "Player Steals":    "stl",
    "Player Blocks":    "blk",
    "Player Turnovers": "to",
    "Player FT Made":   "ftm",
}

_NHL_STAT_KEY: dict[str, str] = {
    "Skater SOG":         "sog",
    "Skater Points":      "p",          # synthesized from g+a at lookup
    "Skater Goals":       "g",
    "Skater Assists":     "a",
    "Skater Hits":        "hits",
    "Skater Blocks":      "blocks",
    "Goalie Saves":       "saves",
    "Goalie Goals Against": "ga",
    # NHL anytime-goal PAPER model (engine.sports.nhl.anytime_goal):
    # Yes side on line 0.5, so actual goals > 0.5 (i.e. >=1 goal) wins.
    # Same "g" stat key as Skater Goals — settles via _determine_outcome's
    # existing over/yes rule, no new branch needed.
}

# NFL anytime-TD PAPER model. "td_total" = any TD (rush+rec+return+def),
# stored per game by engine.sports.nfl.player_logs; the market settles
# on actual > 0.5 (i.e. >=1 TD) for the Yes side. See engine.sports.nfl.
_NFL_STAT_KEY: dict[str, str] = {
    "Anytime TD": "td_total",
    "Passing Yards": "pass_yds",
    "Rushing Yards": "rush_yds",
    "Receiving Yards": "rec_yds",
    "Receptions": "receptions",
    "Rushing Attempts": "carries",
    "Pass Attempts": "pass_att",
    "Completions": "completions",
    "Rush+Rec Yards": "rr_yds",
}


def _stat_key_for(sport: str, bet_type: str) -> str | None:
    if sport == "mlb":
        return _MLB_STAT_KEY.get(bet_type)
    if sport == "nba":
        return _NBA_STAT_KEY.get(bet_type)
    if sport == "wnba":
        # WNBA shares NBA's bet-type → stat-key mapping (same box-stats,
        # same HR market labels).
        return _NBA_STAT_KEY.get(bet_type)
    if sport == "nhl":
        return _NHL_STAT_KEY.get(bet_type)
    if sport == "nfl":
        return _NFL_STAT_KEY.get(bet_type)
    return None


def _determine_outcome(line: float | None, side: str | None,
                       actual: float | None) -> str | None:
    """Returns 'W', 'L', 'P', or None when inputs are missing.

    Standard Over/Under semantics — Over wins when ``actual > line``,
    Under wins when ``actual < line``, equal pushes. Alt yes/no
    markets are normalized to Over X.5 by the scraper so the same
    rule covers them without a special case.
    """
    if line is None or actual is None or not side:
        return None
    s = side.strip().lower()
    if s in ("over", "yes"):
        if actual > line:  return "W"
        if actual < line:  return "L"
        return "P"
    if s in ("under", "no"):
        if actual < line:  return "W"
        if actual > line:  return "L"
        return "P"
    return None


def _profit_for(result: str, odds: int | None, stake: float = 100.0) -> float:
    """American-odds payout for a $stake bet. Push returns 0 (stake
    refunded by convention — we record P/L=0)."""
    if result == "P" or odds is None:
        return 0.0
    if result == "W":
        if odds > 0:
            return stake * (odds / 100.0)
        return stake * (100.0 / abs(odds))
    if result == "L":
        return -stake
    return 0.0


# Composite stat keys synthesized from base stats at settle time. Stored
# as derived rather than written into player_game_logs so a box-score
# correction on any base stat propagates without a re-ingest.
_COMPOSITE_KEYS: dict[str, tuple[str, ...]] = {
    "pra": ("pts", "reb", "ast"),    # NBA Player PRA
    "p":   ("g", "a"),                # NHL Skater Points
}


def _lookup_actual(sport: str, player_id: int, game_id: str,
                   stat_key: str) -> float | None:
    """Return the observed stat value from player_game_logs, or None
    when the log row doesn't exist or doesn't carry that stat. Composite
    keys (PRA, Points) sum their base stats — handles missing components
    by short-circuiting to None so we don't false-settle on partial data.
    """
    conn = _conn_for(sport)
    # NFL props v2 (2026-10-26): game_id is the real ESPN event id -> read the
    # player's line from THAT game (prop_model.settle_lookup: nflverse logs matched
    # by game date +-1 and the two teams; 0 when he took snaps without a touch).
    if (sport == "nfl" and stat_key in ("pass_yds", "rush_yds", "rec_yds", "receptions")
            and str(game_id).isdigit()):
        from .sports.nfl.prop_model import settle_lookup
        status, val = settle_lookup(int(player_id), str(game_id), stat_key)
        return val if status == "ok" else None
    # NFL yards/receptions props settle from player_prop_logs (nflverse ingest),
    # matched by player + game date (game_id carries the slate date). The
    # player's game on/after that date holds the box-score stat. td_total
    # (anytime) falls through to player_game_logs below.
    if sport == "nfl" and stat_key in ("pass_yds", "rush_yds", "rec_yds", "receptions"):
        # The slate date (game_id) can sit a day AFTER the real game date — a
        # Sunday game is recorded 09-27 but the pick stamped the 09-28 slate — so
        # a strict date>=game_id missed the stat and the prop wrongly PUSHED. Reach
        # back one day; still takes the earliest game on/after that.
        pr = conn.execute(
            "SELECT %s v FROM player_prop_logs WHERE espn_id = ? "
            "AND date >= date(?, '-1 day') ORDER BY date LIMIT 1" % stat_key,
            (int(player_id), str(game_id))).fetchone()
        if not pr or pr["v"] is None:
            return None
        try:
            return float(pr["v"])
        except (TypeError, ValueError):
            return None
    row = conn.execute(
        "SELECT stats_json FROM player_game_logs "
        "WHERE player_id = ? AND game_id = ?",
        (int(player_id), str(game_id)),
    ).fetchone()
    if not row:
        return None
    try:
        stats = json.loads(row["stats_json"] or "{}")
    except (ValueError, TypeError):
        return None

    components = _COMPOSITE_KEYS.get(stat_key)
    if components:
        total = 0.0
        for c in components:
            v = stats.get(c)
            if v is None:
                return None
            try:
                total += float(v)
            except (TypeError, ValueError):
                return None
        return total

    val = stats.get(stat_key)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _game_is_final(sport: str, game_id: str) -> bool:
    """Return True when the per-sport games table marks this game final.
    Used to detect DNP props — if the game is final but no log exists
    for the player, the prop should void rather than sit pending."""
    # NFL player-props (yards/receptions) store the slate DATE as game_id and
    # settle from player_prop_logs; a date-format game_id is final once the date
    # has passed AND the nflverse ingest has that slate's games (avoids voiding
    # a game that just hasn't been ingested yet).
    import re as _re_pp
    if sport == "nfl" and _re_pp.match(r"^\d{4}-\d{2}-\d{2}$", str(game_id)):
        from datetime import date as _date_pp
        if str(game_id) >= _date_pp.today().isoformat():
            return False
        try:
            from .player_props_db import _conn_for as _pc_pp
            r = _pc_pp("nfl").execute(
                "SELECT 1 FROM player_prop_logs WHERE date >= date(?, '-1 day') LIMIT 1",
                (str(game_id),)).fetchone()
            return bool(r)
        except Exception:
            return str(game_id) < _date_pp.today().isoformat()
    table_col = {
        "mlb":  ("games", "mlb_game_id"),  # picks store espn_event_id; mlb games keyed on mlb_game_id
        "nhl":  ("nhl_games", "game_id"),
        "nba":  ("nba_games", "game_id"),
        "wnba": ("games", "game_id"),      # basketball framework db
        "nfl":  ("games", "game_id"),       # football game-model nfl.db (read-only)
    }.get(sport)
    if not table_col:
        return False
    table, col = table_col
    try:
        if sport == "mlb":
            from .db import get_conn
            conn = get_conn()
        elif sport == "nhl":
            from .nhl_db import get_conn
            conn = get_conn()
        elif sport == "wnba":
            from .basketball._db import get_conn
            conn = get_conn("wnba")
        elif sport == "nfl":
            # Read the football GAME model's nfl.db READ-ONLY for game
            # status. We never write to it — the paper model's own tables
            # live in the isolated nfl_player.db.
            from engine.football._db import get_conn
            conn = get_conn("nfl")
        else:
            from .nba_db import get_conn
            conn = get_conn()
        if sport == "mlb":
            # MLB props store the MLB Stats game_pk (matches games.mlb_game_id).
            # The games table doesn't carry a separate espn_event_id column —
            # earlier code referenced one that doesn't exist and silently
            # returned False, suppressing every DNP-void.
            row = conn.execute(
                f"SELECT status FROM {table} WHERE mlb_game_id = ? LIMIT 1",
                (str(game_id),),
            ).fetchone()
        else:
            row = conn.execute(
                f"SELECT status FROM {table} WHERE {col} = ? LIMIT 1",
                (str(game_id),),
            ).fetchone()
        return bool(row and (row["status"] or "").lower() == "final")
    except Exception as e:
        logger.debug("_game_is_final lookup failed for %s/%s: %s",
                     sport, game_id, e)
        return False


def _nfl_player_game_done(sport: str, player_id: int, game_id: str) -> bool:
    """For NFL date-format props: True only when the player's TEAM has a game log
    in the slate window — i.e. the player's game actually played + was ingested.
    Distinguishes a real DNP (teammates logged, this player absent → OK to push)
    from an upcoming game (no teammate logged yet → leave pending). Without this,
    a whole upcoming slate mass-pushes the moment any one game that week lands in
    the feed (e.g. a Thursday game ingested while Sunday's slate is still pending).
    Non-NFL / non-date-format game_ids pass through (defer to _game_is_final)."""
    import re as _re_pg
    if sport == "nfl" and str(game_id).isdigit():
        # v2 rows (ESPN event id): push only once nflverse has the game AND its snap
        # counts and the player isn't in them (prop_model.settle_lookup -> 'dnp').
        try:
            from .sports.nfl.prop_model import settle_lookup
            from .player_props_db import _conn_for as _pc_v2
            r = _pc_v2("nfl").execute(
                "SELECT bet_type FROM player_props_picks WHERE game_id = ? AND player_id = ? "
                "AND result IS NULL LIMIT 1", (str(game_id), int(player_id))).fetchone()
            stat = _NFL_STAT_KEY.get(r["bet_type"]) if r else None
            return bool(stat) and settle_lookup(int(player_id), str(game_id), stat)[0] == "dnp"
        except Exception:
            return False
    if sport != "nfl" or not _re_pg.match(r"^\d{4}-\d{2}-\d{2}$", str(game_id)):
        return True
    try:
        from .player_props_db import _conn_for as _pc_pg
        conn = _pc_pg("nfl")
        trow = conn.execute(
            "SELECT team FROM player_prop_logs WHERE espn_id = ? AND team IS NOT NULL "
            "ORDER BY date DESC LIMIT 1", (int(player_id),)).fetchone()
        if not trow or not trow["team"]:
            return False  # unknown team → can't confirm the game played → don't push
        r = conn.execute(
            "SELECT 1 FROM player_prop_logs WHERE team = ? "
            "AND date BETWEEN date(?, '-1 day') AND date(?, '+1 day') LIMIT 1",
            (trow["team"], str(game_id), str(game_id))).fetchone()
        return bool(r)
    except Exception:
        return False


def settle_player_props(sport: str) -> dict:
    """Settle pending prop picks for ``sport``. Idempotent — re-runs
    only touch rows still pending at call time. Returns a per-call
    counter dict mirroring ``settle_derivative_picks``.

    DNP handling: a pending pick whose game is marked ``final`` but
    has no player_game_logs row settles as Push (profit=0). Most books
    void DNP props — encoding as Push matches that behaviour and stops
    the row sitting pending forever after a player scratch / late-OUT.
    """
    if sport not in ("mlb", "nhl", "nba", "wnba", "nfl"):
        raise ValueError(f"unknown sport: {sport}")

    pending = list_picks(sport, pending_only=True, limit=10_000)
    if not pending:
        return {"settled": 0, "wins": 0, "losses": 0, "pushes": 0}

    # Self-heal: the settler depends on the games table reporting
    # status='final' (via _game_is_final) before it can resolve a DNP
    # or pull a player log. The morning sync fetches once and never
    # re-runs as games progress — so a 4pm-tip game that finishes at
    # 7pm stays as status='scheduled' in the games table all evening,
    # blocking every prop on it from settling. User report 2026-04-28:
    # 'Nothing to settle' even though BOS/PHI was final. Refresh
    # today's games before settling so live → final transitions land.
    # Throttled per process (3 min) so back-to-back settle clicks
    # don't fan-out repeated ESPN fetches.
    import time as _t
    if not hasattr(settle_player_props, "_last_games_refresh"):
        settle_player_props._last_games_refresh = {}
    last_g = settle_player_props._last_games_refresh.get(sport, 0)
    if (_t.time() - last_g) > 180:
        try:
            from datetime import datetime as _dt, timedelta as _td
            today = _dt.now().strftime("%Y-%m-%d")
            yesterday = (_dt.now() - _td(days=1)).strftime("%Y-%m-%d")
            if sport == "mlb":
                from scrapers.mlb_stats import fetch_schedule as _fs
                _fs(yesterday, today)
            elif sport == "nhl":
                from scrapers.nhl_api import fetch_schedule as _fs
                _fs(yesterday, today)
            elif sport == "wnba":
                # WNBA lives under the basketball framework — refresh
                # both yesterday and today so any late finals land.
                from engine.basketball._espn_ingest import ingest_today as _fs
                _fs("wnba", date=yesterday)
                _fs("wnba", date=today)
            elif sport == "nfl":
                # Game status in nfl.db is maintained by the football
                # game-model sync; the paper settler doesn't trigger it.
                pass
            else:  # nba
                from scrapers.nba_espn import fetch_schedule as _fs
                _fs(yesterday, today)
            settle_player_props._last_games_refresh[sport] = _t.time()
            logger.debug("Pre-settle games refresh ran for %s", sport)
        except Exception as e:
            logger.warning("Pre-settle games-table refresh failed for %s: %s",
                           sport, e)

    # Self-heal: if any pending pick points at a finalized game with
    # no log row, pull recent finalized box scores so DNP-void only
    # fires when the player genuinely didn't appear, not when our
    # ingest just missed the night. Throttled per process (5 min) so
    # the API endpoint doesn't trigger an MLB-Stats fan-out on every
    # poll.
    import time as _t
    if not hasattr(settle_player_props, "_last_ingest"):
        settle_player_props._last_ingest = {}
    last = settle_player_props._last_ingest.get(sport, 0)
    needs_ingest = False
    if (_t.time() - last) > 300:
        for pick in pending[:25]:  # cheap sample; full check happens below
            stat_key = _stat_key_for(sport, pick.get("bet_type") or "")
            if not stat_key:
                continue
            actual = _lookup_actual(sport, int(pick["player_id"]),
                                    str(pick["game_id"]), stat_key)
            if actual is None and _game_is_final(sport, str(pick["game_id"])):
                needs_ingest = True
                break
    if needs_ingest:
        try:
            if sport == "mlb":
                from .mlb_player_logs import ingest_recent_finals as _ingest
            elif sport == "nhl":
                from .nhl_player_logs import ingest_recent_finals as _ingest
            elif sport == "wnba":
                from .basketball._wnba_player_logs import (
                    ingest_recent_finals as _ingest,
                )
            elif sport == "nfl":
                from .sports.nfl.player_logs import ingest_recent_finals as _ingest
            else:
                from .nba_player_logs import ingest_recent_finals as _ingest
            _ingest(lookback_days=3)
            settle_player_props._last_ingest[sport] = _t.time()
        except Exception as e:
            logger.warning("Pre-settle log refresh failed for %s: %s", sport, e)

    settled = wins = losses = pushes = voided = 0
    for pick in pending:
        bet_type = pick.get("bet_type") or ""
        stat_key = _stat_key_for(sport, bet_type)
        if not stat_key:
            # Bet type we don't yet know how to settle. Leave pending —
            # adding the stat key here is a one-line fix when it
            # surfaces, no need to mass-push existing rows.
            continue
        actual = _lookup_actual(sport, int(pick["player_id"]),
                                str(pick["game_id"]), stat_key)
        if actual is None:
            # Game log not in DB yet. If the game itself is final the
            # player didn't play — void as Push so the row leaves the
            # pending queue. Otherwise leave pending; next tick gets it.
            # _nfl_player_game_done guards against mass-pushing an upcoming NFL
            # slate just because another game that week is already in the feed.
            if (_game_is_final(sport, str(pick["game_id"]))
                    and _nfl_player_game_done(sport, int(pick["player_id"]), str(pick["game_id"]))):
                ok = settle_pick(sport, int(pick["id"]),
                                 actual_value=None, result="P", profit=0.0)
                if ok:
                    settled += 1
                    pushes += 1
                    voided += 1
            continue
        result = _determine_outcome(pick.get("line"), pick.get("side"), actual)
        if result is None:
            continue
        profit = _profit_for(result, pick.get("odds"))
        ok = settle_pick(sport, int(pick["id"]),
                         actual_value=actual, result=result, profit=profit)
        if not ok:
            continue
        settled += 1
        if result == "W":   wins   += 1
        elif result == "L": losses += 1
        else:               pushes += 1

    if settled:
        logger.info("player_props %s: settled=%d W=%d L=%d P=%d (DNP voids=%d)",
                    sport, settled, wins, losses, pushes, voided)
    return {"settled": settled, "wins": wins, "losses": losses,
            "pushes": pushes, "voided_dnp": voided}


__all__ = [
    "settle_player_props",
    "_determine_outcome",
    "_profit_for",
    "_stat_key_for",
]
