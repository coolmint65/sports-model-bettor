"""
Settle live picks against final game state.

A live pick is resolvable when its `market_scope` has closed:

  full:       game.status == 'final'
  Q1..Q4:     cur_period > N or game completed
  H1:         cur_period > 2 or game completed
  H2:         game completed
  P1..P3:     cur_period > N or game completed

The actual outcome is computed from per-period linescores when
available (Q1/Q2/Q3/Q4/P1/P2/P3 all read from the same linescore
arrays we already store on nba_games / nhl_games). 1H = sum of Q1+Q2;
2H = sum of Q3+Q4 (+OT for 'full', not for half markets).

Outcome → result + profit:

  Result is one of W/L/P (push). Profit is in dollars per $100 stake:
    W:  +100 * (odds/100)        if odds > 0
    W:  +100 * (100/abs(odds))    if odds < 0
    L:  -100
    P:    0
"""

from __future__ import annotations
import logging
import re
from datetime import datetime, timezone

from ._schema import ensure_table, get_conn, table_name

logger = logging.getLogger(__name__)


# ── Profit math ──

def _profit_for(result: str, american: int) -> float:
    if result == "W":
        if american > 0:
            return float(american)
        if american < 0:
            return round(100.0 * 100.0 / abs(american), 2)
        return 0.0
    if result == "L":
        return -100.0
    return 0.0


# ── Game-state lookup ──
#
# nba_games / nhl_games are updated by the schedule scrapers
# (fetch_schedule), which typically run on a cron / sync_*.bat schedule
# rather than mid-game. That leaves a window where a quarter / period
# has closed in real life but the canonical games-table row hasn't
# caught up yet — the live worker writes to engine.live._store every
# 15s/30s and that store has fresh linescores.
#
# Strategy: read the canonical row from games table, then merge in
# fresh per-period scores from the live_state cache when the canonical
# columns are still NULL. The live_state linescores carry the same
# semantics — for a *closed* period N, linescore[N-1] holds the final
# score for that period.


def _live_state_overlay(sport: str, game_id: str,
                        canonical: dict | None) -> dict | None:
    """Merge fresh per-period scores from engine.live._store into the
    canonical games-table row. While a game is in progress, live_state
    is the authoritative source — canonical can hold stale values from
    a previous schedule sync (e.g. mid-Q1 score still saved as Q1 final
    after the quarter ended).

    Strategy:
      - If live_state exists and game is in-progress (state='in') OR
        live_state.completed=True, live_state is authoritative for
        scores and any *closed* period linescores. Canonical fills
        non-conflicting fields only.
      - If no live_state row, return canonical unchanged.
      - If live_state row is stale (worker not running), get_state
        returns None (5-min staleness window) — same behaviour as
        no row.
    """
    try:
        from ..live._store import get_state
    except Exception:
        return canonical
    live = get_state(sport, str(game_id))
    if not live:
        return canonical

    out = dict(canonical) if canonical else {}
    out.setdefault("game_id", str(game_id))

    status = live.get("status") or {}
    live_state_field = (status.get("state") or "").lower()
    completed = bool(status.get("completed"))

    # Status promotion: live overrides 'scheduled' / 'live' on canonical
    # when it's seen the game tip off. Doesn't downgrade 'final'.
    if completed:
        out["status"] = "final"
    elif live_state_field == "in" and out.get("status") != "final":
        out["status"] = "live"

    # Score override: while the game is in-progress (or just-finalized
    # by live_state ahead of schedule sync), live_state is freshest.
    # Canonical's home_score/away_score may be stale snapshots.
    home = live.get("home") or {}
    away = live.get("away") or {}
    if (live_state_field == "in" or completed):
        if home.get("score") is not None:
            out["home_score"] = int(home["score"])
        if away.get("score") is not None:
            out["away_score"] = int(away["score"])

    # Per-period linescores. For a *closed* period N (cur_period > N,
    # or game completed, or intermission detail like "End of 1st
    # Period"), linescore[N-1] holds the final period score.
    cur_period = int(status.get("period") or 0)
    detail = (status.get("detail") or "").lower()
    clock_secs = int(status.get("clock_secs") or 0)
    # ESPN's intermission state: cur_period stays at N while clock
    # reads 0:00 and detail says "End of 1st Period" / "End of 1st
    # Quarter". Treat that as period N being closed too — settlement
    # otherwise waits ~30-90s until ESPN advances cur_period.
    intermission_at_n = (
        clock_secs == 0
        and ("end of" in detail or "halftime" in detail)
    )

    linescores = live.get("linescores") or {}
    h_ls = linescores.get("home") or []
    a_ls = linescores.get("away") or []

    # Basketball-shape (q1..q4) vs hockey-shape (p1..p3) column naming.
    # AFL ships 4 periods (quarters); NCAAM ships 2 halves but the
    # schema still uses q1/q2 columns so it slots in here too.
    if sport in ("nba", "wnba", "ncaam", "afl"):
        period_keys = ("q1", "q2", "q3", "q4")
    else:
        period_keys = ("p1", "p2", "p3")
    for idx, key in enumerate(period_keys):
        period_num = idx + 1
        is_closed = (
            completed
            or cur_period > period_num
            or (intermission_at_n and cur_period == period_num)
        )
        if not is_closed:
            continue
        if idx >= min(len(h_ls), len(a_ls)):
            continue
        out[f"home_{key}"] = int(h_ls[idx])
        out[f"away_{key}"] = int(a_ls[idx])

    return out


def _bb_game_state(sport: str, game_id: str) -> dict | None:
    """Return canonical basketball-framework game state for settlement.

    WNBA/NCAAM/AFL share the same games-table schema (id PK, status,
    home_score/away_score, home_q1..q4/away_q1..q4). NCAAM only fills
    q1+q2 since it's a 2-half sport; the resolver tolerates missing
    Q3/Q4 columns because picks for those scopes never get emitted.

    Live-state overlay still applies — `_live_state_overlay` writes
    per-period scores from the in-progress ESPN scoreboard as soon as
    each period closes, closing the gap between intermission and the
    next nightly basketball results refresh.
    """
    try:
        from ..basketball._db import get_conn as _bb_conn
        conn = _bb_conn(sport)
        row = conn.execute(
            "SELECT game_id, status, home_score, away_score, "
            "       home_q1, away_q1, home_q2, away_q2, "
            "       home_q3, away_q3, home_q4, away_q4 "
            "FROM games WHERE game_id = ?",
            (str(game_id),),
        ).fetchone()
    except Exception as e:
        logger.warning("bb_game_state(%s/%s) failed: %s", sport, game_id, e)
        return None
    canonical = dict(row) if row else None
    return _live_state_overlay(sport, game_id, canonical)


def _nba_game_state(game_id: str) -> dict | None:
    """Return canonical NBA game state used for settlement.

    Schema reference:
        nba_games(game_id, date, home_team_id, away_team_id,
                  home_score, away_score,
                  home_q1, away_q1, home_q2, away_q2,
                  home_q3, away_q3, home_q4, away_q4,
                  status, ...)

    Augments the canonical row with fresh per-period scores from the
    live_state cache when canonical values are still NULL — closes
    the gap between quarter close and next schedule sync.
    """
    from ..nba_db import get_conn
    conn = get_conn()
    row = conn.execute(
        "SELECT game_id, status, home_score, away_score, "
        "       home_q1, away_q1, home_q2, away_q2, "
        "       home_q3, away_q3, home_q4, away_q4 "
        "FROM nba_games WHERE game_id = ?",
        (str(game_id),),
    ).fetchone()
    canonical = dict(row) if row else None
    return _live_state_overlay("nba", game_id, canonical)


def _nhl_game_state(game_id: str) -> dict | None:
    """Return canonical NHL game state used for settlement.

    Same overlay pattern as NBA — fresh per-period scores from
    engine.live._store fill in for the schedule-sync lag window.

    nhl_games is keyed by NHL Stats API IDs (e.g. "2025030166"); the
    live tracker stores ESPN scoreboard event IDs (e.g. "401869777").
    The two ID spaces don't overlap, so a direct join misses every
    live-tracker row. Fall back to ESPN scoreboard for the pick's
    date when the canonical row isn't found — that's the same path
    the prematch NHL settler walks.
    """
    from ..nhl_db import get_conn
    conn = get_conn()
    row = conn.execute(
        "SELECT game_id, status, home_score, away_score, "
        "       home_p1, away_p1, home_p2, away_p2, home_p3, away_p3 "
        "FROM nhl_games WHERE game_id = ?",
        (str(game_id),),
    ).fetchone()
    canonical = dict(row) if row else None
    if canonical is None:
        canonical = _nhl_state_from_espn(game_id)
    return _live_state_overlay("nhl", game_id, canonical)


def _nhl_state_from_espn(game_id: str) -> dict | None:
    """Pull a single NHL game's state from ESPN's scoreboard.
    Used as a fallback when nhl_games doesn't have the row (which
    happens for every live-tracker pick because the live tracker
    stores ESPN event ids, not NHL Stats API ids).

    Returns the same shape `_nhl_game_state` returns:
      {game_id, status, home_score, away_score,
       home_p1, away_p1, home_p2, away_p2, home_p3, away_p3}
    """
    try:
        from datetime import datetime as _dt, timedelta as _td
        from engine.nhl_tracker._scoreboard import _fetch_nhl_scoreboard
    except Exception:
        return None

    # Look up the live-pick row to get the pick's date — bounds the
    # ESPN scoreboard query to the right calendar day(s).
    try:
        from ._schema import get_conn as _live_conn
        row = _live_conn("nhl").execute(
            "SELECT date, created_at FROM live_picks_nhl "
            "WHERE game_id = ? ORDER BY id DESC LIMIT 1",
            (str(game_id),),
        ).fetchone()
    except Exception:
        row = None

    candidate_dates: list[str] = []
    if row:
        if row["date"]:
            candidate_dates.append(row["date"][:10])
        if row["created_at"]:
            candidate_dates.append(row["created_at"][:10])
    # Also try the day-before / day-of in case the pick was logged
    # on a UTC-rollover edge — ESPN scopes by ET-day so the same game
    # can land on a different ESPN date than the local pick date.
    today = _dt.now().strftime("%Y-%m-%d")
    for offset in (0, -1, -2):
        d = (_dt.now() + _td(days=offset)).strftime("%Y-%m-%d")
        if d not in candidate_dates:
            candidate_dates.append(d)

    for date in candidate_dates:
        try:
            events = _fetch_nhl_scoreboard(date) or []
        except Exception:
            continue
        for event in events:
            if str(event.get("id")) != str(game_id):
                continue
            comps = event.get("competitions") or []
            if not comps:
                continue
            comp = comps[0]
            status_obj = comp.get("status") or {}
            type_obj = status_obj.get("type") or {}
            is_completed = bool(type_obj.get("completed"))
            state = (type_obj.get("state") or "").lower()
            # Current period the game is IN (1, 2, 3, or 4 for OT). A
            # period N is "closed" only when cur_period > N (or game
            # completed). Without this guard, ESPN's linescores array —
            # which often pre-populates 0 for future periods at game
            # start — gets read straight into home_p{n} / away_p{n}, and
            # every "Under" period-total pick auto-grades as W on the
            # very first tick (user flagged VGK @ CAR 2026-06-02:
            # P2 TOTAL Under 1.5 settled W while still in P1). Detail
            # field carries "End of {N}st Period" during intermission
            # which we treat as closing period N too.
            cur_period = int(status_obj.get("period") or 0)
            detail_l = (type_obj.get("detail") or
                          type_obj.get("shortDetail") or "").lower()
            intermission = ("end of" in detail_l or "intermission" in detail_l)

            home_score = away_score = None
            home_ls: list = []
            away_ls: list = []
            for c in comp.get("competitors") or []:
                ha = c.get("homeAway")
                try:
                    sc = int(c.get("score") or 0)
                except (TypeError, ValueError):
                    sc = 0
                ls = [int(x.get("value") or 0) for x in (c.get("linescores") or [])]
                if ha == "home":
                    home_score, home_ls = sc, ls
                else:
                    away_score, away_ls = sc, ls

            out = {
                "game_id": str(game_id),
                "status": ("final" if is_completed
                            else "live" if state == "in"
                            else "scheduled"),
                "home_score": home_score,
                "away_score": away_score,
            }
            for idx, key in enumerate(("p1", "p2", "p3")):
                period_num = idx + 1
                is_closed = (
                    is_completed
                    or cur_period > period_num
                    or (intermission and cur_period == period_num)
                )
                if not is_closed:
                    continue
                if idx < len(home_ls):
                    out[f"home_{key}"] = home_ls[idx]
                if idx < len(away_ls):
                    out[f"away_{key}"] = away_ls[idx]
            return out
    return None


# ── Scope resolvers ──

def _nba_scope_resolved(scope: str, game: dict) -> bool:
    """True iff the NBA market scope's outcome is fully determined.

    Reads game.status (live/final/scheduled) and per-quarter scores.
    The overlay in `_live_state_overlay` writes home_q{N} / away_q{N}
    as soon as period N closes (including intermission states like
    "End of 1st Quarter" / "Halftime"), so this resolver only has to
    check whether those columns are populated — no separate clock /
    detail logic.
    """
    if not game:
        return False
    if game.get("status") == "final":
        return True
    if scope == "full":
        return False  # full requires final
    if scope in ("Q1", "Q2", "Q3", "Q4"):
        n = int(scope[1])
        # Q4 only settles on final (partial Q4 score lands before
        # game end via OT logic — wait for canonical final).
        if n == 4:
            return False
        return (game.get(f"home_q{n}") is not None
                and game.get(f"away_q{n}") is not None)
    if scope == "H1":
        # 1H = Q1+Q2; closes when both quarter columns are written.
        return (game.get("home_q1") is not None
                and game.get("home_q2") is not None)
    if scope == "H2":
        return False  # 2H requires game final
    return False


def _nhl_scope_resolved(scope: str, game: dict) -> bool:
    """True iff the NHL market scope's outcome is fully determined.

    Same overlay-writes-on-close pattern as NBA — see
    `_live_state_overlay` for the intermission detection.
    """
    if not game:
        return False
    if game.get("status") == "final":
        return True
    if scope == "full":
        return False
    if scope in ("P1", "P2", "P3"):
        n = int(scope[1])
        if n == 3:
            return False  # P3 requires final (or OT — covered by final)
        return (game.get(f"home_p{n}") is not None
                and game.get(f"away_p{n}") is not None)
    return False


# ── Per-pick outcome resolvers ──

_LINE_RE = re.compile(r'(-?\d+\.?\d*)')


def _extract_line(pick_text: str) -> float | None:
    """Extract a line value (last signed-number token) from a pick
    string like 'SA -8.5' / 'Over 215.5' / 'CLE +3.5'."""
    if not pick_text:
        return None
    m = _LINE_RE.findall(pick_text)
    if not m:
        return None
    try:
        return float(m[-1])
    except ValueError:
        return None


def _scope_score(scope: str, game: dict, sport: str) -> tuple[int, int] | None:
    """Return (home_pts, away_pts) for the period scope. Used for
    SPREAD / TOTAL / ML / Winner / BTS settlement."""
    if scope == "full":
        h = game.get("home_score")
        a = game.get("away_score")
        if h is None or a is None:
            return None
        return int(h), int(a)

    if sport in ("nba", "wnba", "ncaam", "afl"):
        if scope in ("Q1", "Q2", "Q3", "Q4"):
            n = int(scope[1])
            h = game.get(f"home_q{n}")
            a = game.get(f"away_q{n}")
            if h is None or a is None:
                return None
            return int(h), int(a)
        if scope == "H1":
            h1 = game.get("home_q1"); h2 = game.get("home_q2")
            a1 = game.get("away_q1"); a2 = game.get("away_q2")
            if None in (h1, h2, a1, a2):
                return None
            return int(h1) + int(h2), int(a1) + int(a2)
        if scope == "H2":
            # 2H = (final - 1H). Use final scores minus Q1+Q2 sums to
            # cover any OT scoring as "part of 2H" by definition.
            h_full = game.get("home_score")
            a_full = game.get("away_score")
            h1 = game.get("home_q1"); h2 = game.get("home_q2")
            a1 = game.get("away_q1"); a2 = game.get("away_q2")
            if None in (h_full, a_full, h1, h2, a1, a2):
                return None
            return (int(h_full) - int(h1) - int(h2),
                    int(a_full) - int(a1) - int(a2))

    if sport == "nhl":
        if scope in ("P1", "P2", "P3"):
            n = int(scope[1])
            h = game.get(f"home_p{n}")
            a = game.get(f"away_p{n}")
            if h is None or a is None:
                return None
            return int(h), int(a)

    return None


def _resolve_pick(row: dict, game: dict, sport: str) -> str | None:
    """Compute W/L/P for one live pick. Returns None when the scope
    isn't resolvable yet (caller skips and retries next tick)."""
    scope = (row.get("market_scope") or "full").upper().strip()
    if scope.lower() == "full":
        scope = "full"
    bt = (row.get("bet_type") or "").upper().strip()
    pick = row.get("pick") or ""

    score = _scope_score(scope if scope != "full" else "full", game, sport)
    if score is None:
        return None
    h, a = score

    # ── ML / Period Winner ──
    # bet_type ends with "ML" or "WINNER" (e.g. "P1 Winner" / "Q2 ML").
    # Live NHL period winners ship as "P{n} Winner" rather than
    # "P{n} DNB" — the prior call (engine.nhl_derivative_picks line 299)
    # was that "DNB" reads like "doesn't score" to non-bettor eyes.
    # Legacy "Period DNB" bet_type from the derivative tracker still
    # passes through this branch via endswith("DNB") for back-compat
    # with rows already in the table.
    # pick text is the team abbr (e.g. "SA", "DET"). Match against
    # home/away abbreviations from the matchup string "AWAY @ HOME".
    if bt.endswith("ML") or bt.endswith("WINNER") or bt.endswith("DNB"):
        away_abbr, home_abbr = _abbrs_from_matchup(row.get("matchup") or "")
        pick_team = pick.strip().split()[0] if pick.strip() else ""
        # ML/WINNER treats ties as P (rare but possible in NHL period
        # cases or OT-resolved scenarios). All three resolve identically.
        if pick_team == home_abbr:
            return "W" if h > a else ("P" if h == a else "L")
        if pick_team == away_abbr:
            return "W" if a > h else ("P" if a == h else "L")
        # Unrecognised team token. Earlier this returned "P" to avoid
        # rows locking pending forever — but a P stamp silently buries
        # the bug AND grades incorrectly (NBA TOR@CLE 2026-05-03 had
        # three Q-ML picks stamped P at settle time, all actually L per
        # ESPN). Return None so the pick stays pending and a future
        # tick (with a re-ingested matchup) can grade it correctly.
        # Worst case the live tracker's 7-day stale-push (TBD) cleans
        # up rows that never recover.
        logger.warning("live_tracker: unrecognised ML pick team in %r "
                       "(matchup=%s) — leaving pending", pick,
                        row.get("matchup"))
        return None

    # ── TOTAL / Period Total / ALT TOTAL ──
    if "TOTAL" in bt:
        line = _extract_line(pick)
        if line is None:
            return None
        total = h + a
        if "OVER" in pick.upper():
            return "W" if total > line else ("L" if total < line else "P")
        if "UNDER" in pick.upper():
            return "W" if total < line else ("L" if total > line else "P")
        return None

    # ── SPREAD / Period Spread / PL (NHL puck line) ──
    # PL has identical math to SPREAD — team + line, covers when
    # margin_for_picked_team + line > 0. Different label, same shape.
    if "SPREAD" in bt or bt.endswith("PL"):
        away_abbr, home_abbr = _abbrs_from_matchup(row.get("matchup") or "")
        line = _extract_line(pick)
        if line is None:
            return None
        # Pick text like "SA -8.5" / "ORL +5.5". Team is first token.
        first = pick.strip().split()[0]
        if first == home_abbr:
            margin = h - a
        elif first == away_abbr:
            margin = a - h
        else:
            # Same fix as the ML branch: return None instead of silent
            # P so the pick stays pending and re-grades next tick.
            logger.warning("live_tracker: unrecognised spread pick team "
                           "in %r — leaving pending", pick)
            return None
        # margin + line > 0 → cover; == 0 → push; < 0 → loss.
        if margin + line > 0:
            return "W"
        if margin + line == 0:
            return "P"
        return "L"

    # ── BTS (NHL period BTS) ──
    if bt.endswith("BTS"):
        both_scored = h > 0 and a > 0
        if pick.strip().lower().startswith("yes"):
            return "W" if both_scored else "L"
        if pick.strip().lower().startswith("no"):
            return "L" if both_scored else "W"
        return None

    logger.warning("live_tracker: unhandled bet_type %r for pick id=%s",
                   bt, row.get("id"))
    return None


def _abbrs_from_matchup(matchup: str) -> tuple[str, str]:
    """Return (away_abbr, home_abbr) from 'AWAY @ HOME' string."""
    if not matchup or " @ " not in matchup:
        return "", ""
    parts = matchup.split(" @ ", 1)
    return parts[0].strip(), parts[1].strip()


# ── Public settler ──

def settle_live_picks(sport: str | None = None) -> dict:
    """Walk pending live picks and settle whatever has resolved.

    Returns ``{"settled": N, "wins": N, "losses": N, "pushes": N,
    "pending_remaining": N}`` per sport. With sport=None, runs both.
    """
    if sport is None:
        out = {s: settle_live_picks(s)
               for s in ("nba", "nhl", "wnba", "ncaam", "afl")}
        return out

    sport = sport.lower()
    if sport not in ("nba", "nhl", "wnba", "ncaam", "afl"):
        raise ValueError(f"unsupported sport {sport!r}")
    ensure_table(sport)
    conn = get_conn(sport)
    tbl = table_name(sport)
    pending = conn.execute(
        f"SELECT * FROM {tbl} WHERE result IS NULL"
    ).fetchall()
    if not pending:
        return {"settled": 0, "wins": 0, "losses": 0, "pushes": 0,
                "pending_remaining": 0}

    settled = wins = losses = pushes = 0
    for row in pending:
        row = dict(row)
        scope = (row.get("market_scope") or "full").upper().strip()
        if scope.lower() == "full":
            scope = "full"

        if sport == "nba":
            game = _nba_game_state(row["game_id"])
            resolved = _nba_scope_resolved(scope, game)
        elif sport == "nhl":
            game = _nhl_game_state(row["game_id"])
            resolved = _nhl_scope_resolved(scope, game)
        else:
            # WNBA/NCAAM/AFL — basketball-framework games table; same
            # column shape (home_q1..q4 / away_q1..q4) as NBA so the
            # NBA scope resolver works unchanged. NCAAM scopes itself
            # to H1/H2 since it's 2-half, but the picks engine never
            # emits Q1-Q4 scopes for NCAAM.
            game = _bb_game_state(sport, row["game_id"])
            resolved = _nba_scope_resolved(scope, game)
        if not resolved:
            continue

        result = _resolve_pick(row, game, sport)
        if result is None:
            continue

        odds = int(row.get("odds") or 0)
        profit = _profit_for(result, odds)
        now_utc = datetime.now(timezone.utc).isoformat()
        conn.execute(
            f"UPDATE {tbl} SET result = ?, profit = ?, settled_at = ? "
            f"WHERE id = ?",
            (result, profit, now_utc, row["id"]),
        )
        settled += 1
        if result == "W":
            wins += 1
        elif result == "L":
            losses += 1
        else:
            pushes += 1

    conn.commit()
    pending_remaining = conn.execute(
        f"SELECT COUNT(*) FROM {tbl} WHERE result IS NULL"
    ).fetchone()[0]
    logger.info(
        "live_tracker.settle_live_picks(%s): settled=%d W=%d L=%d P=%d pending=%d",
        sport, settled, wins, losses, pushes, pending_remaining,
    )
    return {"settled": settled, "wins": wins, "losses": losses,
            "pushes": pushes, "pending_remaining": int(pending_remaining)}
