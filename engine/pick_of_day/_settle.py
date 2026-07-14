"""POTD settlement.

Two entry points:
  - settle_potd(sport)        — sweep pending POTDs, mark W/L/P
  - recalc_potd_profit(sport) — rewrite stored profit using current
                                $100-unit formula (idempotent)

Outcome resolution lives in `_determine_outcome` — the largest function
in the package. Per-bet-type handlers for ML / O/U / RL / PL / 1st INN
/ Q1_ML / Q1_SPREAD / Q1_TOTAL. Game lookup is two-step: try by
`game_id`, then fall back to date + team-name substring (the ESPN id
stored on POTDs doesn't always match the games table primary key).
"""

from __future__ import annotations
import json as _json
import logging
import re

from ._storage import _get_conn, _ensure_potd_table, _potd_table

logger = logging.getLogger(__name__)


def settle_potd(sport: str) -> dict:
    """Settle any pending POTDs whose games have completed.

    NBA stores Q1 picks in ``pick_of_day`` and full-game picks in
    ``pick_of_day_full`` (different markets, both can coexist for a
    given date). Earlier this function only swept the Q1 table —
    full-game POTDs sat pending forever and never showed W/L on the
    tracker. Now both tables are walked when sport == "nba"; every
    other sport touches just the one table they use.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    tables = ["pick_of_day"]
    if sport == "nba":
        tables.append("pick_of_day_full")

    settled = 0
    wins = 0
    losses = 0

    for table in tables:
        pending = conn.execute(
            f"SELECT * FROM {table} WHERE result IS NULL"
        ).fetchall()
        if not pending:
            continue

        # Self-heal any orphaned POTDs whose mirror INSERT crashed at
        # creation time. This keeps the per-sport picks table in sync
        # with pick_of_day so the tracker shows the POTD alongside
        # other picks.
        for potd in pending:
            try:
                _heal_potd_mirror(sport, conn, dict(potd))
            except Exception as e:
                logger.debug("POTD mirror heal failed for id=%s: %s",
                             potd["id"], e)

        for potd in pending:
            potd = dict(potd)
            result, profit = _determine_outcome(sport, conn, potd)

            if result is None:
                continue  # Game not finished yet

            conn.execute(
                f"UPDATE {table} SET result = ?, profit = ?, "
                f"  settled_at = datetime('now') "
                f"WHERE id = ?",
                (result, profit, potd["id"]),
            )
            settled += 1
            if result == "W":
                wins += 1
            elif result == "L":
                losses += 1

    conn.commit()
    return {"settled": settled, "wins": wins, "losses": losses}


def _heal_potd_mirror(sport: str, conn, potd: dict) -> None:
    """If this POTD has no matching row in the per-sport picks table,
    re-mint the mirror. No-op when the mirror exists.

    MLB cross-ingest normalization: POTDs are selected against an
    ESPN-sourced scoreboard, so ``potd["game_id"]`` is the ESPN event
    id (9-digit, 401XXXXXX) and ``potd["pick"]`` carries the full team
    name ("Washington Nationals +2.5"). The MLB picks settler joins
    ``games.mlb_game_id`` (6-digit) and parses abbreviations, so a
    verbatim mirror creates orphan rows: settler can't find a game,
    pick stays pending forever, and a separate primary-path pick on
    the same matchup creates a visible duplicate. Translate to the
    MLB-Stats game_pk and rewrite the pick label to use the team
    abbreviation before inserting. NHL/NBA/other sports use their own
    settler paths and don't share the same id-shape mismatch, so they
    fall through to the verbatim insert.
    """
    from ..hockey import HOCKEY_LEAGUE_REGISTRY as _HK
    from ..basketball import LEAGUE_REGISTRY as _BB
    if sport == "mlb" or (sport in _HK and sport != "nhl") or (
        sport in _BB and sport != "nba"
    ):
        picks_table = "picks"
    else:
        picks_table = f"{sport}_picks"

    mirror_game_id = potd["game_id"]
    mirror_pick = potd["pick"]

    if sport == "mlb":
        normalized = _normalize_mlb_potd_for_mirror(conn, potd)
        if normalized is None:
            # Game not yet in the MLB-Stats games table. Skip the
            # mirror rather than insert an orphan that the settler
            # can never match. POTD's own settler still grades from
            # ESPN's summary endpoint independently.
            logger.debug(
                "POTD mirror skipped — MLB game_pk unresolved for "
                "%s/%s/%s", potd["date"], potd.get("matchup"),
                potd.get("pick"),
            )
            return
        mirror_game_id, mirror_pick = normalized

    try:
        existing = conn.execute(
            f"SELECT id FROM {picks_table} "
            f"WHERE date=? AND game_id=? AND bet_type=? LIMIT 1",
            (potd["date"], mirror_game_id, potd["bet_type"]),
        ).fetchone()
    except Exception:
        return  # Table doesn't exist for this sport — nothing to heal.
    if existing:
        return

    # Matchup-level dedupe — a primary-path pick with a different
    # game_id format (cold-start when games table lags, or legacy
    # ESPN-id orphans we already healed manually) shouldn't be
    # duplicated under a new id. Include settled picks: if a previous
    # heal already mirrored this POTD, re-running settle shouldn't
    # mint a parallel row.
    if sport == "mlb" and potd.get("matchup"):
        matchup_existing = conn.execute(
            f"SELECT id FROM {picks_table} "
            f"WHERE date=? AND matchup=? AND bet_type=? LIMIT 1",
            (potd["date"], potd["matchup"], potd["bet_type"]),
        ).fetchone()
        if matchup_existing:
            return

    try:
        conn.execute(
            f"INSERT INTO {picks_table} ("
            "  date, game_id, matchup, bet_type, pick,"
            "  model_prob, edge, odds"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (potd["date"], mirror_game_id, potd.get("matchup"),
             potd["bet_type"], mirror_pick,
             potd.get("model_prob"), potd.get("edge"), potd.get("odds")),
        )
        conn.commit()
        logger.info("POTD mirror healed for %s/%s/%s/%s",
                    sport, potd["date"], potd["bet_type"], mirror_pick)
    except Exception as e:
        logger.debug("POTD mirror heal INSERT failed: %s", e)


def _normalize_mlb_potd_for_mirror(conn, potd: dict
                                    ) -> tuple[int, str] | None:
    """Translate an MLB POTD's (game_id, pick) pair into the shape the
    main picks table + settler expect.

    Returns (mlb_game_pk, abbreviated_pick) on success, or None when
    the underlying game isn't yet in the games table — the caller
    skips the mirror in that case.
    """
    from ..derivative_tracker import _resolve_mlb_game_pk
    matchup = potd.get("matchup") or ""
    game_pk = _resolve_mlb_game_pk(conn, str(potd.get("game_id") or ""),
                                    potd["date"], matchup)
    if game_pk is None:
        return None
    pick_text = _normalize_mlb_pick_label(conn, potd.get("pick") or "",
                                           matchup)
    return int(game_pk), pick_text


def _normalize_mlb_pick_label(conn, pick: str, matchup: str) -> str:
    """Strip a full team name off an MLB pick label, leaving the
    abbreviation. Idempotent — already-abbreviated picks pass through.

    Examples:
        "Washington Nationals +2.5"  ->  "WSH +2.5"
        "St. Louis Cardinals +1.5"   ->  "STL +1.5"
        "WSH +2.5"                   ->  "WSH +2.5"
        "Over 8.5"                   ->  "Over 8.5"  (not a team pick)
    """
    if not pick:
        return pick
    parts = pick.split()
    if not parts:
        return pick
    last = parts[-1]
    # Spread/RL pick? Last token should parse as a signed number.
    try:
        float(last)
    except ValueError:
        return pick
    team_phrase = " ".join(parts[:-1]).strip()
    if not team_phrase:
        return pick
    if len(team_phrase) <= 3 and team_phrase.isupper():
        return pick
    # Authoritative full→abbr lookup is the teams table. Avoid relying
    # on the abbr.aliases_for helper here — that table is keyed by
    # abbreviation, not full name, and returns the input unchanged
    # when fed a full name.
    try:
        row = conn.execute(
            "SELECT abbreviation FROM teams WHERE name = ? LIMIT 1",
            (team_phrase,),
        ).fetchone()
    except Exception:
        row = None
    abbr = None
    if row:
        abbr = row[0] if not isinstance(row, dict) else row.get("abbreviation")
    if not abbr and matchup and " @ " in matchup:
        # Fallback: pick whichever matchup side prefix-matches.
        a_abbr, h_abbr = matchup.split(" @ ", 1)
        tp_upper = team_phrase.upper()
        if tp_upper.startswith(a_abbr.strip()[:3].upper()):
            abbr = a_abbr.strip()
        elif tp_upper.startswith(h_abbr.strip()[:3].upper()):
            abbr = h_abbr.strip()
    if abbr:
        return f"{abbr} {last}"
    return pick


def _determine_outcome(sport: str, conn, potd: dict) -> tuple[str | None, float]:
    """Figure out whether a POTD won, lost, or pushed. Returns (result, profit).
    Returns (None, 0) if the game isn't finished yet."""
    date = potd["date"]
    matchup = potd["matchup"]
    bet_type = potd["bet_type"]
    pick = potd["pick"]
    odds = potd.get("odds") or -110

    # Parse matchup - handles both "AWAY @ HOME" and "Away Team at Home Team"
    try:
        if " @ " in matchup:
            away_part, home_part = [s.strip() for s in matchup.split(" @ ")]
        elif " at " in matchup:
            away_part, home_part = [s.strip() for s in matchup.split(" at ")]
        else:
            return None, 0
    except ValueError:
        return None, 0

    game_id = potd.get("game_id")

    if sport == "mlb":
        row = None
        if game_id:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM games g
                LEFT JOIN teams ht ON g.home_team_id = ht.mlb_id
                LEFT JOIN teams at ON g.away_team_id = at.mlb_id
                WHERE g.mlb_game_id = ? AND g.status = 'final'
                LIMIT 1
            """, (game_id,)).fetchone()
        if not row:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM games g
                LEFT JOIN teams ht ON g.home_team_id = ht.mlb_id
                LEFT JOIN teams at ON g.away_team_id = at.mlb_id
                WHERE g.date = ? AND g.status = 'final'
                  AND (ht.name LIKE ? OR ht.abbreviation = ?)
                  AND (at.name LIKE ? OR at.abbreviation = ?)
                LIMIT 1
            """, (date, f"%{home_part}%", home_part, f"%{away_part}%", away_part)).fetchone()
    elif sport == "nhl":
        row = None
        if game_id:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM nhl_games g
                LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nhl_teams at ON g.away_team_id = at.id
                WHERE g.game_id = ? AND g.status = 'final'
                LIMIT 1
            """, (game_id,)).fetchone()
        if not row:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM nhl_games g
                LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nhl_teams at ON g.away_team_id = at.id
                WHERE g.date = ? AND g.status = 'final'
                  AND (ht.name LIKE ? OR ht.abbreviation = ?)
                  AND (at.name LIKE ? OR at.abbreviation = ?)
                LIMIT 1
            """, (date, f"%{home_part}%", home_part, f"%{away_part}%", away_part)).fetchone()
    elif sport == "nba":
        row = None
        if game_id:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                FROM nba_games g
                LEFT JOIN nba_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nba_teams at ON g.away_team_id = at.id
                WHERE g.game_id = ? AND g.status = 'final'
                LIMIT 1
            """, (game_id,)).fetchone()
        if not row:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                FROM nba_games g
                LEFT JOIN nba_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nba_teams at ON g.away_team_id = at.id
                WHERE g.date = ? AND g.status = 'final'
                  AND (ht.name LIKE ? OR ht.abbreviation = ?)
                  AND (at.name LIKE ? OR at.abbreviation = ?)
                LIMIT 1
            """, (date, f"%{home_part}%", home_part, f"%{away_part}%", away_part)).fetchone()
    elif sport == "tennis":
        # Tennis bet types (ML / SET_BETTING / TOTAL_GAMES / etc.) and
        # settle logic don't fit the team-sport game-row model. Lean
        # on tennis_tracker.settle_tennis_picks — it already resolves
        # against scheduled / tennis_matches / tennis_match_results.
        # Mirror the same pick into pick_of_day by matching on
        # (date, tour, bet_type, pick) in tennis_picks. Tour scoping
        # matters: an ATP and WTA pick could share the same date /
        # bet_type / pick text (e.g. both "Total Games Over 21.5")
        # and without it the lookup would copy the wrong result.
        tour = potd.get("tour")
        try:
            if tour:
                tp_row = conn.execute(
                    "SELECT result, profit FROM tennis_picks "
                    " WHERE date = ? AND tour = ? "
                    "   AND bet_type = ? AND pick = ? "
                    "   AND result IS NOT NULL "
                    " ORDER BY id DESC LIMIT 1",
                    (date, tour, bet_type, pick),
                ).fetchone()
            else:
                tp_row = conn.execute(
                    "SELECT result, profit FROM tennis_picks "
                    " WHERE date = ? AND bet_type = ? AND pick = ? "
                    "   AND result IS NOT NULL "
                    " ORDER BY id DESC LIMIT 1",
                    (date, bet_type, pick),
                ).fetchone()
        except Exception as e:
            logger.warning("tennis POTD settle lookup failed: %s", e)
            return None, 0
        if not tp_row:
            return None, 0  # tracker hasn't settled the underlying pick yet
        tp_row = dict(tp_row)
        result = tp_row["result"]
        if result not in ("W", "L", "P"):
            return None, 0
        return result, _profit_on_settled(result, odds)
    else:
        # Framework leagues (basketball + hockey) — per-league DB,
        # uniform games/teams schema. Match the POTD by either game_id
        # or by date+team-substring against the framework games table.
        # POTD didn't auto-settle for CBA / WNBA / Euroleague / AHL /
        # PWHL before this branch existed.
        try:
            from ..basketball import LEAGUE_REGISTRY as _BB
            from ..hockey import HOCKEY_LEAGUE_REGISTRY as _HK
        except Exception:
            _BB, _HK = {}, {}
        try:
            from ..baseball import LEAGUE_REGISTRY as _BASE
        except Exception:
            _BASE = {}
        try:
            from ..soccer import LEAGUE_REGISTRY as _SOC
        except Exception:
            _SOC = {}
        try:
            from ..football import LEAGUE_REGISTRY as _FB
        except Exception:
            _FB = {}
        if sport in _BB:
            row = None
            if game_id:
                row = conn.execute("""
                    SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                    FROM games g
                    LEFT JOIN teams ht ON g.home_team_id = ht.id
                    LEFT JOIN teams at ON g.away_team_id = at.id
                    WHERE g.game_id = ? AND g.status = 'final'
                    LIMIT 1
                """, (str(game_id),)).fetchone()
            if not row:
                row = conn.execute("""
                    SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                    FROM games g
                    LEFT JOIN teams ht ON g.home_team_id = ht.id
                    LEFT JOIN teams at ON g.away_team_id = at.id
                    WHERE g.date = ? AND g.status = 'final'
                      AND (ht.name LIKE ? OR ht.abbreviation = ?)
                      AND (at.name LIKE ? OR at.abbreviation = ?)
                    LIMIT 1
                """, (date, f"%{home_part}%", home_part,
                       f"%{away_part}%", away_part)).fetchone()
        elif sport in _HK:
            row = None
            if game_id:
                row = conn.execute("""
                    SELECT g.*, ht.short_name AS home_abbr, at.short_name AS away_abbr
                    FROM games g
                    LEFT JOIN teams ht ON g.home_team_id = ht.id
                    LEFT JOIN teams at ON g.away_team_id = at.id
                    WHERE g.id = ? AND g.status = 'final'
                    LIMIT 1
                """, (game_id,)).fetchone()
            if not row:
                row = conn.execute("""
                    SELECT g.*, ht.short_name AS home_abbr, at.short_name AS away_abbr
                    FROM games g
                    LEFT JOIN teams ht ON g.home_team_id = ht.id
                    LEFT JOIN teams at ON g.away_team_id = at.id
                    WHERE g.date = ? AND g.status = 'final'
                      AND (ht.full_name LIKE ? OR ht.short_name = ?)
                      AND (at.full_name LIKE ? OR at.short_name = ?)
                    LIMIT 1
                """, (date, f"%{home_part}%", home_part,
                       f"%{away_part}%", away_part)).fetchone()
        elif sport in _BASE:
            # Baseball framework uses the same games/teams schema as
            # basketball (game_id TEXT, teams.id INT join). Without this
            # branch college baseball POTDs sat pending forever even
            # though the underlying picks-table mirror settled normally.
            row = None
            if game_id:
                row = conn.execute("""
                    SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                    FROM games g
                    LEFT JOIN teams ht ON g.home_team_id = ht.id
                    LEFT JOIN teams at ON g.away_team_id = at.id
                    WHERE g.game_id = ? AND g.status = 'final'
                    LIMIT 1
                """, (str(game_id),)).fetchone()
            if not row:
                row = conn.execute("""
                    SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                    FROM games g
                    LEFT JOIN teams ht ON g.home_team_id = ht.id
                    LEFT JOIN teams at ON g.away_team_id = at.id
                    WHERE g.date = ? AND g.status = 'final'
                      AND (ht.name LIKE ? OR ht.abbreviation = ?)
                      AND (at.name LIKE ? OR at.abbreviation = ?)
                    LIMIT 1
                """, (date, f"%{home_part}%", home_part,
                       f"%{away_part}%", away_part)).fetchone()
        elif sport in _FB:
            # Football framework (UFL today, NFL/NCAAF later). Same
            # games/teams shape as basketball — game_id TEXT, teams.id
            # INT. Without this branch UFL POTDs sat pending forever.
            row = None
            if game_id:
                row = conn.execute("""
                    SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                    FROM games g
                    LEFT JOIN teams ht ON g.home_team_id = ht.id
                    LEFT JOIN teams at ON g.away_team_id = at.id
                    WHERE g.game_id = ? AND g.status = 'final'
                    LIMIT 1
                """, (str(game_id),)).fetchone()
            if not row:
                row = conn.execute("""
                    SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                    FROM games g
                    LEFT JOIN teams ht ON g.home_team_id = ht.id
                    LEFT JOIN teams at ON g.away_team_id = at.id
                    WHERE g.date = ? AND g.status = 'final'
                      AND (ht.name LIKE ? OR ht.abbreviation = ?)
                      AND (at.name LIKE ? OR at.abbreviation = ?)
                    LIMIT 1
                """, (date, f"%{home_part}%", home_part,
                       f"%{away_part}%", away_part)).fetchone()
        elif sport in _SOC:
            # Soccer framework. Soccer's bet types (1X2 / OU / BTTS /
            # DC / DNB / AH / H1_*) don't share handlers with the
            # team-sport ML/SPREAD/TOTAL section below — so mirror the
            # tennis pattern: copy the result from the soccer picks
            # table where soccer's own settler has already graded it.
            #
            # Lookup uses match_id + bet_type, NOT date + pick text. Two
            # reasons: (1) POTD `pick` carries the friendly team name
            # ("South Africa +1.5") while the picks-table mirror writes
            # the abbreviated form ("RSA +1.5") — exact-text match never
            # hits. (2) POTD `date` is the day the lock was created
            # ("2026-05-31") which can be days before the match itself
            # ("2026-06-11"). Skip W-or-L-or-P only (exclude voids).
            try:
                sp_row = conn.execute(
                    "SELECT result, odds FROM picks "
                    " WHERE match_id = ? AND bet_type = ? "
                    "   AND result IN ('W','L','P') "
                    " ORDER BY id DESC LIMIT 1",
                    (game_id, bet_type),
                ).fetchone()
            except Exception as e:
                logger.warning("soccer POTD settle lookup failed: %s", e)
                return None, 0
            if not sp_row:
                return None, 0
            sp = dict(sp_row)
            result = sp["result"]
            if result not in ("W", "L", "P"):
                return None, 0
            return result, _profit_on_settled(result, odds)
        else:
            return None, 0

    if not row:
        # Differentiate "game not finished yet" from "we can't find
        # this game at all" — the latter usually means a date/team
        # -name mismatch between the POTD row and the games table.
        logger.debug(
            "POTD settle: no final game row for %s %s / %s (game_id=%s, "
            "home_part=%r, away_part=%r)",
            sport, date, matchup, game_id, home_part, away_part,
        )
        return None, 0

    row = dict(row)
    hs = row.get("home_score", 0) or 0
    as_ = row.get("away_score", 0) or 0

    result = None
    home_abbr = row.get("home_abbr", "")
    away_abbr = row.get("away_abbr", "")

    if bet_type == "ML":
        home_won = hs > as_
        # Case-insensitive token + substring match. Token check first
        # (exact word match) avoids the "NY in NYY/NYM" ambiguity that
        # bare substring would hit. Falls back to substring of full
        # team name (e.g. "Fremantle" → contains "FRE").
        pick_l = pick.lower()
        pick_tokens = pick_l.split()
        home_abbr_l = (home_abbr or "").lower()
        away_abbr_l = (away_abbr or "").lower()
        home_part_l = (home_part or "").lower()
        pick_home = (
            (home_abbr_l and home_abbr_l in pick_tokens)  # exact token
            or (home_part_l and home_part_l in pick_l)    # full-name substring
            or (home_abbr_l and home_abbr_l in pick_l
                and not (away_abbr_l and away_abbr_l in pick_tokens))
        )
        won = (pick_home and home_won) or (not pick_home and not home_won)
        result = "W" if won else "L"

    elif bet_type in ("O/U", "ALT O/U", "TOTAL", "ALT TOTAL"):
        # ALT O/U is identical to O/U for settlement — same "Over N.N" /
        # "Under N.N" pick label, just at a different line value.
        total = hs + as_
        if "Over" in pick:
            line = float(pick.split()[-1])
            if total > line:
                result = "W"
            elif total < line:
                result = "L"
            else:
                result = "P"
        else:
            line = float(pick.split()[-1])
            if total < line:
                result = "W"
            elif total > line:
                result = "L"
            else:
                result = "P"

    elif bet_type in ("RL", "PL", "ALT RL", "ALT PL",
                       "SPREAD", "ALT SPREAD"):
        spread_match = re.search(r'([+-]?\d+\.?\d*)\s*$', pick)
        spread = float(spread_match.group(1)) if spread_match else 1.5

        # Case-insensitive substring match (see ML branch above).
        pick_l = pick.lower()
        pick_is_home = ((home_abbr and home_abbr.lower() in pick_l)
                        or (home_part and home_part.lower() in pick_l))
        if pick_is_home:
            margin = hs - as_
        else:
            margin = as_ - hs

        covered = margin + spread > 0
        pushed = margin + spread == 0
        if pushed:
            result = "P"
        else:
            result = "W" if covered else "L"

    elif bet_type == "1st INN":
        if sport == "mlb":
            home_ls = row.get("home_linescore")
            away_ls = row.get("away_linescore")
            try:
                h_inn = _json.loads(home_ls) if home_ls else []
                a_inn = _json.loads(away_ls) if away_ls else []
                scoreless = (len(h_inn) > 0 and len(a_inn) > 0
                             and h_inn[0] == 0 and a_inn[0] == 0)
            except (_json.JSONDecodeError, TypeError):
                scoreless = False
            if pick == "NRFI":
                result = "W" if scoreless else "L"
            else:
                result = "W" if not scoreless else "L"

    elif bet_type == "Q1_ML":
        hq1 = row.get("home_q1", 0) or 0
        aq1 = row.get("away_q1", 0) or 0
        # Case-insensitive substring match (mirrors the ML branch fix
        # for Fremantle/FRE). Uppercase team abbreviations don't appear
        # in the longer Title-Case team name otherwise.
        pick_l = pick.lower()
        pick_home = ((home_abbr and home_abbr.lower() in pick_l)
                     or (home_part and home_part.lower() in pick_l))
        home_won_q1 = hq1 > aq1
        if hq1 == aq1:
            result = "P"
        else:
            won = (pick_home and home_won_q1) or (not pick_home and not home_won_q1)
            result = "W" if won else "L"

    elif bet_type == "Q1_SPREAD":
        hq1 = row.get("home_q1", 0) or 0
        aq1 = row.get("away_q1", 0) or 0
        m = re.search(r"([+-]\d+\.?\d*)", pick)
        spread = float(m.group(1)) if m else 0.0
        pick_l = pick.lower()
        pick_home = ((home_abbr and home_abbr.lower() in pick_l)
                     or (home_part and home_part.lower() in pick_l))
        if pick_home:
            margin = hq1 - aq1
        else:
            margin = aq1 - hq1
        covered = margin + spread > 0
        pushed = margin + spread == 0
        if pushed:
            result = "P"
        else:
            result = "W" if covered else "L"

    elif bet_type == "Q1_TOTAL":
        hq1 = row.get("home_q1", 0) or 0
        aq1 = row.get("away_q1", 0) or 0
        q1_total = hq1 + aq1
        m = re.search(r"(\d+\.?\d*)", pick)
        line = float(m.group(1)) if m else 0.0
        is_over = "Over" in pick or "over" in pick
        if q1_total == line:
            result = "P"
        elif is_over:
            result = "W" if q1_total > line else "L"
        else:
            result = "W" if q1_total < line else "L"

    if result is None:
        # Row was found but no outcome handler matched the bet_type.
        # Without logging, the POTD silently stays pending forever.
        logger.warning(
            "POTD settle: no outcome handler for bet_type=%r on %s %s/%s",
            bet_type, sport, date, matchup,
        )
        return None, 0

    return result, _profit_on_settled(result, odds)


def _profit_on_settled(result: str | None, odds: int | float | None) -> float:
    """$100-unit profit formula used by settle_potd. Exposed so
    recalc_potd_profit can rewrite stored profits to match current code
    when an older run left them on a different unit sizing."""
    if result == "W":
        o = odds if odds is not None else -110
        return round(o if o > 0 else 10000 / abs(o), 2)
    if result == "L":
        return -100.0
    return 0.0


def recalc_potd_profit(sport: str) -> dict:
    """Rewrite the ``profit`` column on every settled POTD row using
    the current $100-unit formula. No-op on pending rows (result NULL).

    Idempotent — running it twice produces the same values. Walks both
    NBA tables (Q1 + full) so a profit-formula change applies to both.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    tables = ["pick_of_day"]
    if sport == "nba":
        tables.append("pick_of_day_full")

    total_rows = 0
    updated = 0
    for table in tables:
        rows = conn.execute(
            f"SELECT id, result, odds, profit FROM {table} "
            f"WHERE result IN ('W', 'L', 'P')"
        ).fetchall()
        total_rows += len(rows)
        for r in rows:
            new_profit = _profit_on_settled(r["result"], r["odds"])
            if r["profit"] is None or abs((r["profit"] or 0) - new_profit) > 0.01:
                conn.execute(
                    f"UPDATE {table} SET profit = ? WHERE id = ?",
                    (new_profit, r["id"]),
                )
                updated += 1

    conn.commit()
    return {"sport": sport, "settled_rows": total_rows, "updated": updated}
