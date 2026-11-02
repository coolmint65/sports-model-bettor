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

2026-10-10 CLV correctness fixes (Austin -- CLV was wrong two independent
ways, both fixed here):

  (a) Freeze-on-live. This used to overwrite closing_odds on every
      call with "whatever HR shows right now", with no game-status
      check -- so a 3-hourly RepriceProps run could stamp a mid-game
      or post-game price as the "close" (verified: a CFB ML POTD
      stamped -600 while already FINAL). Now gated through
      _pregame_status_map(): a pending row only gets a (re)write while
      its game is still SCHEDULED; once live/final/postponed -- or
      once the join can't even find a status -- writes freeze so
      closing_odds holds the last real pre-game price. NHL *and* MLB
      can't resolve status at all today: their games tables key off
      each sport's own native id (NHL-API gamePk / MLB Gameday id),
      not the ESPN-style id POTD stores in `game_id`. That's a real
      gap, not a bug in this fix -- closing_odds capture is
      conservatively frozen (stays NULL) for those two sports until a
      proper id-reconciliation lands. Flagged as a follow-up, not
      fixed here (don't rabbit-hole it).

  (b) Side + line matching. Separately, the per-sport resolvers below
      picked the WRONG side's price whenever the picked team was HOME:
      spread/puck-line/moneyline picks are stored as the full team
      name ("New York Islanders +1.5"), not an abbreviation, but the
      old code compared `pick.split()[0]` ("New") against `home_abbr`
      ("NYI"), and on any mismatch blindly assumed "must be away" --
      correct only when the picked team happened to actually be away.
      (Verified against nhl.db: NYI POTD id=79, pick "New York
      Islanders +1.5" [home, entry -185] stored closing_odds=+140,
      which is TB's [away] price, not NYI's.) _resolve_pick_side() now
      matches the pick's team explicitly against BOTH home_abbr and
      away_abbr (alias-aware, falls back to a full-team-name lookup
      against the sport's teams table), returning None -- not a guess
      -- when neither side is a confident match. _spread_price_for_side
      / _total_price_for_side additionally verify the pick's own line
      number against the odds board's current line (falling back to
      alt_spreads/alt_totals if the primary line has since drifted),
      so a line move can't silently swap in a different number's
      price either. engine.tracker._helpers._extract_closing_for_pick
      (used by the main MLB tracker's CLV, outside POTD) has the
      identical side-matching bug but is NOT touched here -- fixing it
      is out of this task's scope (bigger blast radius than POTD) and
      is flagged as a follow-up.
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

    # Lock POTDs whose game has tipped off / first-pitched / dropped
    # the puck. Pre-game, the line / drift refresh can swap or invalidate
    # the row freely; once the game is live, the user is committed to
    # what they saw at lock time and we shouldn't change it under them.
    # Build a (game_id -> game_status) map from the canonical games
    # table so the loop below can short-circuit on live/final games.
    games_table = {"mlb": "games", "nhl": "nhl_games", "nba": "nba_games"}[sport]
    game_id_col = "mlb_game_id" if sport == "mlb" else "game_id"
    pending_gids = {str(p.get("game_id") or "") for p in pending}
    pending_gids.discard("")
    locked_gids: set[str] = set()
    if pending_gids:
        placeholders = ",".join("?" * len(pending_gids))
        rows = conn.execute(
            f"SELECT {game_id_col} AS gid, status FROM {games_table} "
            f"WHERE {game_id_col} IN ({placeholders})",
            tuple(pending_gids),
        ).fetchall()
        for r in rows:
            if (r["status"] or "").lower() in ("live", "final", "postponed"):
                locked_gids.add(str(r["gid"]))

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

    from ..hockey import HOCKEY_LEAGUE_REGISTRY as _HOCKEY
    from ..basketball import LEAGUE_REGISTRY as _BB
    if sport == "mlb" or (sport in _HOCKEY and sport != "nhl") or (
        sport in _BB and sport != "nba"
    ):
        picks_table = "picks"
    else:
        picks_table = f"{sport}_picks"
    updated = 0
    skipped = 0
    invalidated = 0
    locked_count = 0
    for row in pending:
        pot_table = row.pop("_table", "pick_of_day")

        # Lock-on-live: once the game has tipped off, the POTD is
        # frozen — no drift invalidation, no line refresh. The user
        # locked in their POTD pre-game and the displayed pick
        # shouldn't change mid-game just because the model now likes
        # a different bet on the same matchup. Postponed games also
        # land here; the settler auto-pushes them separately.
        gid_key = str(row.get("game_id") or "")
        if gid_key and gid_key in locked_gids:
            locked_count += 1
            continue

        # Drift check: if the GameCard's current best_pick for this
        # game uses a different bet_type than the locked POTD, drop
        # the lock so the next select_potd re-aligns with the card.
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
    return {"updated": updated, "skipped": skipped,
            "invalidated": invalidated, "locked_live": locked_count}


def update_potd_closing_odds(sport: str) -> dict:
    """Refresh closing_odds on every un-settled POTD with the latest line.

    The pick selection itself stays locked from get_or_create_potd();
    this only updates the price we'll use for CLV measurement at settle
    time. Since each call overwrites with the freshest available price,
    the LAST update before settle_potd() runs is effectively the closing
    line.

    NBA full-game POTDs live in ``pick_of_day_full``; this walks both
    tables for sport=="nba" so the full-game CLV pipeline doesn't go
    permanently dark.
    """
    # Per-sport closing-odds resolver. mlb/nhl/nba have proper
    # implementations that hit each sport's dedicated odds endpoint;
    # framework leagues (basketball + hockey + tennis) fall through to
    # the generic resolver which pulls the most recent HR odds we have
    # cached for each pending POTD's matchup. Returns (id, None) pairs
    # when there's no data — callers tolerate that gracefully.
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    builtin_resolvers = {
        "mlb": _resolve_mlb_closing_for_pending,
        "nhl": _resolve_nhl_closing_for_pending,
        "nba": _resolve_nba_closing_for_pending,
        "nfl": lambda pend: _resolve_football_closing_for_pending(pend, "nfl"),
        "cfb": lambda pend: _resolve_football_closing_for_pending(pend, "cfb"),
    }
    resolver = builtin_resolvers.get(sport, _resolve_generic_closing_for_pending)

    tables = ["pick_of_day"]
    if sport == "nba":
        tables.append("pick_of_day_full")

    total_pending = 0
    updated = 0
    for table in tables:
        pending = conn.execute(
            f"SELECT id, game_id, matchup, bet_type, pick FROM {table} "
            f"WHERE result IS NULL"
        ).fetchall()
        if not pending:
            continue
        # sqlite3.Row has no .get() -- convert once up front so both the
        # freeze-gate below and every resolver's dict(row) stay cheap/safe.
        pending = [dict(p) for p in pending]
        total_pending += len(pending)

        # Freeze-on-live (2026-10-10): only let the resolver (re)write
        # closing_odds for rows whose game is still SCHEDULED. Once
        # live/final/postponed -- or once we can't resolve a status at
        # all -- leave the existing closing_odds exactly as-is; that's
        # the "last real pre-game price", i.e. the close. See the
        # module docstring fix (a).
        status_map = _pregame_status_map(
            sport, {str(p.get("game_id") or "") for p in pending})
        pending_pregame = [
            p for p in pending
            if status_map.get(str(p.get("game_id") or "")) == "scheduled"
        ]
        frozen = len(pending) - len(pending_pregame)
        if frozen:
            logger.info(
                "POTD closing odds (%s/%s): froze %d row(s) already "
                "live/final/postponed/unresolved -- closing_odds left "
                "untouched", sport, table, frozen)
        pending = pending_pregame
        if not pending:
            continue

        try:
            pairs = resolver(pending)
        except Exception as e:
            logger.warning("%s POTD closing-odds resolver crashed (%s): %s",
                           sport, table, e)
            continue

        for pid, closing in pairs:
            if closing is None:
                continue
            conn.execute(
                f"UPDATE {table} SET closing_odds = ?, "
                f"       closing_odds_updated_at = datetime('now') "
                f"WHERE id = ?",
                (int(closing), pid),
            )
            updated += 1

    skipped = total_pending - updated
    if updated:
        conn.commit()
        logger.info("POTD closing odds (%s): updated %d, skipped %d",
                    sport, updated, skipped)
    elif total_pending > 0:
        # Loud-but-not-catastrophic warning: the resolver didn't
        # produce a single closing-odds match for any pending POTD.
        # Usually means HR's odds feed was unreachable for the
        # matchups we're holding. Without this log, the closing_odds
        # column quietly stays NULL and CLV vanishes for the day.
        logger.warning(
            "POTD closing odds (%s): no resolvable pairs from %d "
            "pending POTDs. CLV will be NULL until the next refresh.",
            sport, total_pending,
        )
    return {"updated": updated, "skipped": skipped}


_SPORT_GAMES_TABLE = {
    "mlb": ("games", "mlb_game_id"),
    "nhl": ("nhl_games", "game_id"),
    "nba": ("nba_games", "game_id"),
    "nfl": ("games", "game_id"),
    "cfb": ("games", "game_id"),
}


def _pregame_status_map(sport: str, game_ids: set[str]) -> dict[str, str]:
    """Best-effort {game_id: lowercased status} for the given sport,
    used by the freeze-on-live gate in update_potd_closing_odds.

    Only includes game_ids it could actually resolve to a row -- a
    missing key means "status unknown", and the caller must treat that
    the same as "not provably pre-game" (freeze, don't guess). NHL and
    MLB's games tables key off each sport's own native id (NHL-API
    gamePk / MLB Gameday id) rather than the ESPN-style id POTD stores
    in `game_id`, so neither ever resolves here today -- every NHL/MLB
    row freezes until a real id-reconciliation lands (see the module
    docstring). That's a known, accepted gap, not a crash."""
    game_ids = {g for g in game_ids if g}
    if not game_ids:
        return {}
    spec = _SPORT_GAMES_TABLE.get(sport)
    if not spec:
        return {}
    games_table, game_id_col = spec
    out: dict[str, str] = {}
    try:
        conn = _get_conn(sport)
        placeholders = ",".join("?" * len(game_ids))
        rows = conn.execute(
            f"SELECT {game_id_col} AS gid, status FROM {games_table} "
            f"WHERE {game_id_col} IN ({placeholders})",
            tuple(game_ids),
        ).fetchall()
        for r in rows:
            out[str(r["gid"])] = (r["status"] or "scheduled").lower()
    except Exception as e:
        logger.warning("POTD closing-odds pregame-status lookup failed (%s): %s",
                       sport, e)
        return {}
    return out


def _strip_pick_suffix(pick: str) -> str:
    """Strip a trailing ' Q1' / ' ML' tag and/or signed line number off
    a side-style pick string, leaving just the team token:
    "New York Islanders +1.5" -> "New York Islanders"
    "San Antonio Spurs +3.5 Q1" -> "San Antonio Spurs"
    "New York Knicks ML" -> "New York Knicks"
    "NYI +1.5" -> "NYI"      "St. Louis Cardinals" -> "St. Louis Cardinals"
    """
    import re
    s = (pick or "").strip()
    s = re.sub(r"\s+Q1\s*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*[+-]?\d+\.?\d*\s*$", "", s)
    s = re.sub(r"\s+ML\s*$", "", s, flags=re.IGNORECASE)
    return s.strip()


def _pick_line_value(pick: str) -> float | None:
    """Pull the trailing signed line number out of a pick string
    ("New York Islanders +1.5" -> 1.5, "... -0.5 Q1" -> -0.5,
    "Over 6.5" -> 6.5). None when there's no trailing number to parse."""
    import re
    s = (pick or "").strip()
    s = re.sub(r"\s+Q1\s*$", "", s, flags=re.IGNORECASE)
    m = re.search(r"([+-]?\d+\.?\d*)\s*$", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


_SPORT_TEAMS_TABLE = {"mlb": "teams", "nhl": "nhl_teams", "nba": "nba_teams"}


def _team_name_for_abbr(abbr: str, sport: str, conn, table: str) -> str:
    """Full team name for one abbreviation, trying its alias too (e.g.
    a matchup string may say "TB" while the teams table itself keys
    Tampa Bay under "TBL" -- confirmed in nhl_teams 2026-10-10)."""
    if not abbr:
        return ""
    try:
        row = conn.execute(
            f"SELECT name FROM {table} WHERE abbreviation = ?", (abbr,)
        ).fetchone()
        if row and row["name"]:
            return row["name"]
    except Exception:
        return ""
    try:
        from ..abbr import alt_abbr
        alt = alt_abbr(abbr, sport)
        if alt and alt != abbr:
            row = conn.execute(
                f"SELECT name FROM {table} WHERE abbreviation = ?", (alt,)
            ).fetchone()
            if row and row["name"]:
                return row["name"]
    except Exception:
        pass
    return ""


def _team_names_for_abbrs(home_abbr: str, away_abbr: str, sport: str) -> dict[str, str]:
    """{abbreviation: Full Team Name} for the two sides. Used to match
    a pick stored as a full team name (NHL/MLB/NBA all do this for
    side-style bets) back to an abbreviation. Best-effort: returns {}
    (or a partial dict) on any failure so callers fall back to
    abbreviation-only matching rather than crashing."""
    table = _SPORT_TEAMS_TABLE.get(sport)
    if not table or not (home_abbr or away_abbr):
        return {}
    try:
        conn = _get_conn(sport)
    except Exception:
        return {}
    out = {}
    if home_abbr:
        out[home_abbr] = _team_name_for_abbr(home_abbr, sport, conn, table)
    if away_abbr:
        out[away_abbr] = _team_name_for_abbr(away_abbr, sport, conn, table)
    return out


def _resolve_pick_side(pick: str, home_abbr: str, away_abbr: str,
                       sport: str) -> str | None:
    """Which side ('home' / 'away') a side-style pick refers to, or
    None when neither side can be confidently resolved (fail closed --
    callers must NOT guess "must be the other one").

    Root-cause fix for the 2026-10-10 NYI closing-odds bug: spread /
    puck-line / moneyline picks are stored as the full team name
    ("New York Islanders +1.5"), not the abbreviation, so comparing
    just the first whitespace token against home_abbr breaks silently
    ("New" never equals "NYI"). Tries, in order: (1) an abbreviation
    token match (alias-aware via engine.abbr), (2) a full-team-name
    match against the sport's teams table."""
    if not pick or not (home_abbr or away_abbr):
        return None
    token = _strip_pick_suffix(pick)
    tok_head = token.split()[0] if token.split() else token

    def _abbr_eq(a, b):
        if not a or not b:
            return False
        if a.upper() == b.upper():
            return True
        try:
            from ..abbr import alt_abbr
            return (alt_abbr(a, sport).upper() == b.upper()
                    or a.upper() == alt_abbr(b, sport).upper())
        except Exception:
            return False

    if _abbr_eq(tok_head, home_abbr):
        return "home"
    if _abbr_eq(tok_head, away_abbr):
        return "away"

    names = _team_names_for_abbrs(home_abbr, away_abbr, sport)
    tok_lower = token.lower()
    home_name = (names.get(home_abbr) or "").lower()
    away_name = (names.get(away_abbr) or "").lower()

    # Nickname-only (last word) match first: team-name formatting drifts
    # a lot between where picks get their text (looks ESPN-sourced, e.g.
    # "New York Islanders") and a sport's own teams table (nhl_teams
    # stores NYI as "NY Islanders Islanders" and keys TBL under "TBL",
    # not "TB" -- confirmed 2026-10-10). The nickname (final word) is
    # the one part that stays stable across those variants and is,
    # within a single matchup, always unique to one side.
    def _last_word(s):
        parts = s.split() if s else []
        return parts[-1] if parts else ""
    tok_last, home_last, away_last = (
        _last_word(tok_lower), _last_word(home_name), _last_word(away_name))
    if tok_last and home_last and tok_last == home_last and tok_last != away_last:
        return "home"
    if tok_last and away_last and tok_last == away_last and tok_last != home_last:
        return "away"

    # Full-string fallback (covers single-word names/cities the
    # nickname check alone wouldn't disambiguate).
    if home_name and (tok_lower == home_name or tok_lower.startswith(home_name)):
        return "home"
    if away_name and (tok_lower == away_name or tok_lower.startswith(away_name)):
        return "away"
    return None


def _spread_price_for_side(side: str, pick_line: float | None, game_odds: dict,
                           home_key: str = "home_spread_point",
                           away_key: str = "away_spread_point",
                           home_odds_key: str = "home_spread_odds",
                           away_odds_key: str = "away_spread_odds",
                           alt_key: str = "alt_spreads") -> int | None:
    """Price for `side` at exactly `pick_line`. Checks the primary line
    first, then the alt-lines list (HR keeps the freshest line as
    primary and appends anything else to `alt_key` when it moves -- see
    scrapers.hardrock_odds._apply_market). Falls back to the primary
    price when there's no line to verify against (pick_line is None)
    OR this market doesn't expose a point field at all -- strictly
    better than the old code either way, since the SIDE is now right.
    Returns None (not a guess) when we can see a line was quoted and it
    genuinely doesn't match anywhere -- that means a real line move and
    we don't have fresh odds for the pick's actual number."""
    if side not in ("home", "away") or not game_odds:
        return None
    primary_point = game_odds.get(home_key if side == "home" else away_key)
    primary_odds = game_odds.get(home_odds_key if side == "home" else away_odds_key)
    if pick_line is None or primary_point is None:
        return primary_odds
    try:
        if abs(float(primary_point) - pick_line) < 0.01:
            return primary_odds
    except (TypeError, ValueError):
        return primary_odds
    for alt in (game_odds.get(alt_key) or []):
        point = alt.get("point")
        if point is None:
            continue
        side_point = point if side == "home" else -point
        if abs(float(side_point) - pick_line) < 0.01:
            return alt.get("home_odds") if side == "home" else alt.get("away_odds")
    return None


def _total_price_for_side(is_over: bool, pick_line: float | None, game_odds: dict,
                          line_key: str = "over_under", over_key: str = "over_odds",
                          under_key: str = "under_odds",
                          alt_key: str = "alt_totals") -> int | None:
    """Over/Under price at exactly `pick_line`, mirroring
    _spread_price_for_side's primary-then-alt-then-fail-closed shape."""
    if not game_odds:
        return None
    primary_line = game_odds.get(line_key)
    primary_price = game_odds.get(over_key if is_over else under_key)
    if pick_line is None or primary_line is None:
        return primary_price
    try:
        if abs(float(primary_line) - pick_line) < 0.01:
            return primary_price
    except (TypeError, ValueError):
        return primary_price
    for alt in (game_odds.get(alt_key) or []):
        line = alt.get("line")
        if line is not None and abs(float(line) - pick_line) < 0.01:
            return alt.get("over_odds") if is_over else alt.get("under_odds")
    return None


# ── Per-sport closing-odds resolvers ──

def _resolve_mlb_closing_for_pending(pending: list) -> list[tuple]:
    """Per-POTD (id, closing_price) pairs for MLB.

    Was a thin wrapper around engine.tracker._helpers._extract_closing_for_pick;
    now resolves locally via _mlb_closing_for_bet_type so the POTD-scoped
    side/line fix doesn't touch that shared helper (it's also used by the
    main MLB tracker's CLV capture outside POTD -- bigger blast radius
    than this task, flagged as a follow-up instead; see module docstring)."""
    from ..picks import fetch_real_odds_for_games, match_odds
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
        closing = _mlb_closing_for_bet_type(
            row["bet_type"], row["pick"], h_abbr, a_abbr, game_odds,
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
            row["bet_type"], row["pick"], h_abbr, a_abbr, game_odds,
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
            row["bet_type"], row["pick"], h_abbr, a_abbr, game_odds,
        ) if game_odds else None
        out.append((row["id"], closing))
    return out


def _resolve_football_closing_for_pending(pending: list, league: str) -> list[tuple]:
    """Per-POTD (id, closing_price) pairs for football (nfl/cfb).

    Mirrors engine.football._tracker.capture_closing_odds but targets the
    POTD table. The POTD stores ESPN game_ids + abbreviation pick text (same
    as the football picks table), so join games/teams for the home/away
    abbrs, pull HR's current price for the pick's side, and return it as the
    closing. A bridge off picks.closing_odds is unreliable here: football
    POTDs lock days ahead and the picks-side closing isn't captured until
    near kickoff, so the matching picks row is usually still NULL. Re-fetching
    HR's board directly captures the close for the POTD game regardless.
    (MintyBot 2026-10-07, Austin: "fix CLV".)"""
    from ..football._odds import fetch_league_odds
    conn = _get_conn(league)
    try:
        odds = fetch_league_odds(league) or {}
    except Exception as e:
        logger.warning("football POTD odds fetch failed (%s): %s", league, e)
        return [(r["id"], None) for r in pending]
    if not odds:
        return [(r["id"], None) for r in pending]
    out = []
    for row in pending:
        r = dict(row)
        g = conn.execute(
            "SELECT ht.abbreviation AS h, at.abbreviation AS a "
            "FROM games g LEFT JOIN teams ht ON ht.id = g.home_team_id "
            "LEFT JOIN teams at ON at.id = g.away_team_id "
            "WHERE g.game_id = ?",
            (r["game_id"],),
        ).fetchone()
        if not g or not g["h"] or not g["a"]:
            out.append((r["id"], None))
            continue
        h, a = g["h"], g["a"]
        o = odds.get(f"{a}@{h}")
        if not o:
            out.append((r["id"], None))
            continue
        bt = (r["bet_type"] or "").upper()
        pick = (r["pick"] or "").strip()
        am = None
        if bt == "ML":
            am = o.get("home_ml") if pick == h else (o.get("away_ml") if pick == a else None)
        elif bt == "SPREAD":
            tok = pick.split()[0] if pick else ""
            am = o.get("home_spread_odds") if tok == h else (o.get("away_spread_odds") if tok == a else None)
        elif bt == "TOTAL":
            pl = pick.lower()
            am = o.get("over_odds") if pl.startswith("over") else (o.get("under_odds") if pl.startswith("under") else None)
        if am is None:
            out.append((r["id"], None))
            continue
        try:
            am = int(am)
        except (TypeError, ValueError):
            out.append((r["id"], None))
            continue
        # Reject HR market-locked sentinels (matches the football tracker gate).
        if abs(am) < 100 or abs(am) > 5000:
            out.append((r["id"], None))
            continue
        out.append((r["id"], am))
    return out


def _resolve_generic_closing_for_pending(pending: list) -> list[tuple]:
    """Cold-start resolver for sports without a dedicated closing-odds
    feed (basketball framework, hockey framework, tennis). Returns
    (id, None) for every pending POTD so the caller logs the warning
    and CLV stays NULL until a real resolver is wired up. Lives as a
    stub so the dispatch table doesn't silently exclude these sports."""
    return [(r["id"], None) for r in pending]


def _fetch_nhl_odds_map() -> dict:
    """Pull current NHL h2h/spreads/totals from Hard Rock, keyed by
    AWAY@HOME. Was sourced from the-odds-api until 2026-05-11."""
    try:
        from scrapers.hardrock_odds import fetch_nhl as _hr_nhl
        return _hr_nhl() or {}
    except Exception as e:
        logger.warning("NHL odds fetch (HR) failed: %s", e)
        return {}


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


def _nhl_closing_for_bet_type(bet_type: str, pick: str, home_abbr: str,
                               away_abbr: str, game_odds: dict) -> int | None:
    """Pick out the right NHL price for the pick's bet_type (O/U, PL,
    ML), matching the pick's exact side AND (for PL) its exact line --
    see _resolve_pick_side / _spread_price_for_side. 2026-10-10: the old
    version compared pick.split()[0] to home_abbr and blindly assumed
    "away" on any mismatch, which is wrong for every full-team-name PL
    pick on the home side (e.g. NYI id=79)."""
    if not game_odds:
        return None
    bt = bet_type
    pk = pick or ""
    if bt in ("ml", "ML"):
        side = _resolve_pick_side(pk, home_abbr, away_abbr, "nhl")
        if side == "home":
            return game_odds.get("home_ml")
        if side == "away":
            return game_odds.get("away_ml")
        return None
    if bt in ("ou", "O/U"):
        pl = pk.lower()
        if pl.startswith("over"):
            return _total_price_for_side(True, _pick_line_value(pk), game_odds)
        if pl.startswith("under"):
            return _total_price_for_side(False, _pick_line_value(pk), game_odds)
        return None
    if bt in ("pl", "PL", "rl", "RL"):
        side = _resolve_pick_side(pk, home_abbr, away_abbr, "nhl")
        if side not in ("home", "away"):
            return None
        return _spread_price_for_side(side, _pick_line_value(pk), game_odds)
    return None


def _mlb_closing_for_bet_type(bet_type: str, pick: str, home_abbr: str,
                              away_abbr: str, game_odds: dict) -> int | None:
    """Pick out the right MLB price for the pick's bet_type, matching
    side + line explicitly (see module docstring: local POTD-scoped
    replacement for engine.tracker._helpers._extract_closing_for_pick,
    which has the identical bug but isn't touched here)."""
    if not game_odds:
        return None
    bt = bet_type
    pk = pick or ""
    if bt in ("ml", "ML"):
        side = _resolve_pick_side(pk, home_abbr, away_abbr, "mlb")
        if side == "home":
            return game_odds.get("home_ml")
        if side == "away":
            return game_odds.get("away_ml")
        return None
    if bt in ("ou", "O/U", "ALT O/U", "ALT TOTAL"):
        pl = pk.lower()
        if pl.startswith("over"):
            return _total_price_for_side(True, _pick_line_value(pk), game_odds)
        if pl.startswith("under"):
            return _total_price_for_side(False, _pick_line_value(pk), game_odds)
        return None
    if bt in ("rl", "RL", "ALT RL"):
        side = _resolve_pick_side(pk, home_abbr, away_abbr, "mlb")
        if side not in ("home", "away"):
            return None
        return _spread_price_for_side(side, _pick_line_value(pk), game_odds)
    if bt in ("nrfi", "1st INN"):
        # NRFI = inning_totals.I1.under_odds; YRFI = .over_odds (no side
        # ambiguity -- it's a yes/no market, not team-based).
        i1 = (game_odds.get("inning_totals") or {}).get("I1") or {}
        pku = pk.upper()
        if pku == "NRFI":
            return i1.get("under_odds")
        if pku == "YRFI":
            return i1.get("over_odds")
        return None
    if bt == "F5 ML":
        side = _resolve_pick_side(pk, home_abbr, away_abbr, "mlb")
        if side == "home":
            return game_odds.get("f5_home_ml")
        if side == "away":
            return game_odds.get("f5_away_ml")
        return None
    if bt == "F5 O/U":
        pl = pk.lower()
        if pl.startswith("over"):
            return _total_price_for_side(
                True, _pick_line_value(pk), game_odds,
                line_key="f5_over_under", over_key="f5_over_odds",
                under_key="f5_under_odds", alt_key="f5_alt_totals")
        if pl.startswith("under"):
            return _total_price_for_side(
                False, _pick_line_value(pk), game_odds,
                line_key="f5_over_under", over_key="f5_over_odds",
                under_key="f5_under_odds", alt_key="f5_alt_totals")
        return None
    if bt == "F5 RL":
        side = _resolve_pick_side(pk, home_abbr, away_abbr, "mlb")
        if side not in ("home", "away"):
            return None
        return _spread_price_for_side(
            side, _pick_line_value(pk), game_odds,
            home_key="f5_home_spread_point", away_key="f5_away_spread_point",
            home_odds_key="f5_home_spread_odds", away_odds_key="f5_away_spread_odds",
            alt_key="f5_alt_spreads")
    return None


def _nba_closing_for_bet_type(bet_type: str, pick: str, home_abbr: str,
                               away_abbr: str, game_odds: dict) -> int | None:
    """Pick out the right NBA price for the pick's bet_type. Covers both
    Q1 markets (pick_of_day) and full-game markets (pick_of_day_full --
    previously unhandled here at all, so full-game NBA POTDs never got
    a closing_odds; added 2026-10-10 alongside the side-match fix).

    2026-10-10: the old Q1_ML branch also read the FULL-GAME home_ml/
    away_ml fields instead of q1_home_ml/q1_away_ml -- a second,
    separate bug (wrong market, not wrong side) fixed in passing since
    it's immediately adjacent to the side-match fix."""
    if not game_odds:
        return None
    bt = bet_type
    pk = pick or ""
    if bt == "Q1_ML":
        side = _resolve_pick_side(pk, home_abbr, away_abbr, "nba")
        if side == "home":
            return game_odds.get("q1_home_ml")
        if side == "away":
            return game_odds.get("q1_away_ml")
        return None
    if bt == "Q1_SPREAD":
        side = _resolve_pick_side(pk, home_abbr, away_abbr, "nba")
        if side not in ("home", "away"):
            return None
        pick_line = _pick_line_value(pk)
        primary_point = game_odds.get("q1_spread")  # home-perspective; away = -point
        primary_odds = game_odds.get(
            "q1_spread_home_odds" if side == "home" else "q1_spread_away_odds")
        if pick_line is None or primary_point is None:
            return primary_odds
        try:
            side_point = float(primary_point) if side == "home" else -float(primary_point)
            if abs(side_point - pick_line) < 0.01:
                return primary_odds
        except (TypeError, ValueError):
            return primary_odds
        for alt in (game_odds.get("q1_alt_spreads") or []):
            point = alt.get("point")
            if point is None:
                continue
            side_point = point if side == "home" else -point
            if abs(float(side_point) - pick_line) < 0.01:
                return alt.get("home_odds") if side == "home" else alt.get("away_odds")
        return None
    if bt == "Q1_TOTAL":
        pl = pk.lower()
        if pl.startswith("over"):
            return _total_price_for_side(
                True, _pick_line_value(pk), game_odds,
                line_key="q1_total", over_key="q1_over_odds",
                under_key="q1_under_odds", alt_key="q1_alt_totals")
        if pl.startswith("under"):
            return _total_price_for_side(
                False, _pick_line_value(pk), game_odds,
                line_key="q1_total", over_key="q1_over_odds",
                under_key="q1_under_odds", alt_key="q1_alt_totals")
        return None
    if bt == "ML":
        side = _resolve_pick_side(pk, home_abbr, away_abbr, "nba")
        if side == "home":
            return game_odds.get("home_ml")
        if side == "away":
            return game_odds.get("away_ml")
        return None
    if bt == "SPREAD":
        side = _resolve_pick_side(pk, home_abbr, away_abbr, "nba")
        if side not in ("home", "away"):
            return None
        return _spread_price_for_side(side, _pick_line_value(pk), game_odds)
    if bt == "TOTAL":
        pl = pk.lower()
        if pl.startswith("over"):
            return _total_price_for_side(True, _pick_line_value(pk), game_odds)
        if pl.startswith("under"):
            return _total_price_for_side(False, _pick_line_value(pk), game_odds)
        return None
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
