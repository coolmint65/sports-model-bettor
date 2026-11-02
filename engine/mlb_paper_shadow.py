"""Isolated PAPER-pick shadow tracker for MLB (full / 1st-inning / derivative).

Records the raw-model paper picks (routes_mlb._mlb_paper_picks /
_mlb_first_inning_paper / _mlb_derivative_paper) into a SEPARATE `paper_shadow`
table in mlb.db and settles them against final scores + per-inning linescores,
so the app can show a paper P/L panel + each POTD's running record â€” WITHOUT
touching the real `picks` / `derivative_picks` tables or real P/L.

Three scopes:
  full          â€” ML / RUN_LINE / TOTAL (single best per game)
  first_inning  â€” NRFI / YRFI (1st-inning linescore)
  derivative    â€” F5_TOTAL + full/F5 TEAM_TOTAL (linescore)
Austin 2026-09-19.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - stripped runtimes
    _ET = None

from engine.db import get_conn

logger = logging.getLogger(__name__)

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS paper_shadow ("
    " id INTEGER PRIMARY KEY AUTOINCREMENT,"
    " date TEXT, game_id TEXT, matchup TEXT, scope TEXT,"
    " bet_type TEXT, pick TEXT, line REAL,"
    " tt_team TEXT, tt_span TEXT,"
    " model_prob REAL, edge REAL, odds INTEGER,"
    " result TEXT, profit REAL, settled_at TEXT, captured_at TEXT,"
    " UNIQUE(game_id, scope, bet_type, pick)"
    ")"
)

_LOCKED = ("final", "live", "in_progress", "inprogress", "in progress", "delayed")


def _ensure(conn):
    conn.execute(_SCHEMA)
    conn.commit()


def _payout(odds, won):
    if not won:
        return -100.0
    o = int(odds or 0)
    if o > 0:
        return float(o)
    if o < 0:
        return round(100.0 * 100.0 / abs(o), 2)
    return 0.0


def _tt_meta(pick):
    """Resolve (team, span) for a derivative TEAM_TOTAL from its market label
    ('TT KC' / 'F5 TT LAD') using the abbrs stamped on the pick."""
    market = (pick.get("market") or "")
    span = "f5" if market.upper().startswith("F5") else "full"
    ha, aa = pick.get("home_abbr"), pick.get("away_abbr")
    team = None
    if ha and ha in market:
        team = "home"
    elif aa and aa in market:
        team = "away"
    return team, span


def _et_date(iso):
    """Date (YYYY-MM-DD) of an ISO timestamp in US Eastern.

    A game's official slate date (games.date == StatsAPI officialDate) is keyed
    off local date, so the ET date of first pitch is the best single anchor for
    finding the right games row. Falls back to the raw date part.
    """
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if _ET is not None:
            return dt.astimezone(_ET).strftime("%Y-%m-%d")
        u = dt.astimezone(timezone.utc)
        off = -4 if 3 <= u.month <= 10 else -5
        return u.astimezone(timezone(timedelta(hours=off))).strftime("%Y-%m-%d")
    except Exception:
        return (str(iso)[:10] or None)


def _resolve_game_date_status(conn, home_abbr, away_abbr, game_iso):
    """(games.date, games.status) for this paper pick, resolved by matchup teams
    the SAME way the settler joins (teams.abbreviation -> mlb_id ->
    games.home/away_team_id), disambiguating the series by the game's own start
    time. Returns (None, None) if unresolved.

    Fixes the day-behind pick date (Austin 2026-09-29): record_paper_shadow was
    keyed on the ESPN event id, which never matches games.id / games.mlb_game_id,
    so the games lookup ALWAYS missed and pdate fell back to utcnow() -- a day
    off from officialDate whenever the model runs the day before first pitch /
    across the UTC-ET midnight boundary. Reading games.date directly makes the
    pick date == the date the settler joins on (its +1/+2 tolerance stays a net).
    """
    ha = (home_abbr or "").strip().upper()
    aa = (away_abbr or "").strip().upper()
    if not ha or not aa:
        return None, None
    amap = {}
    for t in conn.execute("SELECT abbreviation, mlb_id FROM teams"):
        if t["abbreviation"]:
            amap[t["abbreviation"].upper()] = t["mlb_id"]
    hi = amap.get(_ABBR_FIX.get(ha, ha))
    ai = amap.get(_ABBR_FIX.get(aa, aa))
    if not hi or not ai:
        return None, None
    anchor = _et_date(game_iso) or datetime.utcnow().strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT date, status FROM games WHERE home_team_id=? AND away_team_id=? "
        "AND date BETWEEN date(?, '-1 day') AND date(?, '+1 day') "
        "ORDER BY ABS(julianday(date)-julianday(?)) ASC, date ASC LIMIT 1",
        (hi, ai, anchor, anchor, anchor),
    ).fetchone()
    if not row:
        return None, None
    return row["date"], (row["status"] or "")


def _resolve_game_start(conn, matchup, slate_date):
    """games.start_time (ISO) for a POTD pick's game, resolved by matchup teams +
    date (same join as _resolve_game_date_status). None if unresolved -> caller
    falls back to lock-on-first (fail-safe). Austin 2026-10-14 (lock engine)."""
    mu = matchup or ""
    if " @ " not in mu:
        return None
    aa, ha = [x.strip().upper() for x in mu.split(" @ ", 1)]
    amap = {}
    for t in conn.execute("SELECT abbreviation, mlb_id FROM teams"):
        if t["abbreviation"]:
            amap[t["abbreviation"].upper()] = t["mlb_id"]
    hi = amap.get(_ABBR_FIX.get(ha, ha))
    ai = amap.get(_ABBR_FIX.get(aa, aa))
    if not hi or not ai:
        return None
    r = conn.execute(
        "SELECT start_time FROM games WHERE home_team_id=? AND away_team_id=? "
        "AND date BETWEEN date(?, '-1 day') AND date(?, '+1 day') "
        "ORDER BY ABS(julianday(date)-julianday(?)) ASC, date ASC LIMIT 1",
        (hi, ai, slate_date, slate_date, slate_date)).fetchone()
    return (r["start_time"] if r and r["start_time"] else None)


def record_paper_shadow(game_id, scope, pick, game_iso=None) -> dict:
    """Record ONE paper pick (single dict) for a game+scope. Idempotent;
    supersedes a prior pending pick in the same scope whose (bet_type,pick)
    changed. Status-locked (won't record once the game has begun).

    `game_iso` = the game's scheduled start (ESPN gameDate ISO); used only to
    resolve the correct games row for the pick's date + status.
    """
    if not pick:
        return {"recorded": 0}
    conn = get_conn()
    _ensure(conn)
    bt = pick.get("type") or ""
    pk = pick.get("pick") or ""
    if not bt or not pk:
        return {"recorded": 0}
    ha, aa = pick.get("home_abbr"), pick.get("away_abbr")
    matchup = pick.get("matchup") or (("%s @ %s" % (aa, ha)) if (aa and ha) else "")
    if (not ha or not aa) and " @ " in matchup:
        aa, ha = [x.strip() for x in matchup.split(" @ ", 1)]
    gdate, gstatus = _resolve_game_date_status(conn, ha, aa, game_iso)
    pdate = gdate or datetime.utcnow().strftime("%Y-%m-%d")
    status = (gstatus or "").lower()
    if any(s in status for s in _LOCKED):
        return {"recorded": 0, "locked": 1}
    tt_team = tt_span = None
    if bt == "TEAM_TOTAL":
        tt_team, tt_span = _tt_meta(pick)
    elif bt == "F5_TOTAL":
        tt_span = "f5"
    now_iso = datetime.utcnow().isoformat()
    line_val = float(pick["line"]) if pick.get("line") is not None else None
    prob_val = float(pick.get("prob") or 0.0)
    edge_val = float(pick.get("edge") or 0.0)
    odds_val = int(pick.get("odds") or 0)
    # Card-lock (Austin 2026-10-06): once a pending lean exists for this game+scope, HOLD it ΓÇö
    # don't void/swap to a different market or side as the line moves pre-game. The line moving
    # our way is CLV, not a reason to re-pick. Only the SAME (bet_type,pick) refreshes below.
    # (Football/NHL got the same lock; this was the "cards change for the wrong reasons" bug.)
    # Time-based lock (Austin 2026-10-14): hold the pending lean only once its game reaches
    # the lock_schedule time (7am day / 4:30pm night / prior-9pm overnight ET) off game_iso;
    # before that it is PROVISIONAL and may swap to the current best lean (was unconditional
    # lock-on-first). Fallback to lock-on-first if game_iso is missing / not a full datetime.
    _locked_now = None
    if game_iso and "T" in str(game_iso):
        try:
            from engine.lock_schedule import is_locked as _is_locked
            from datetime import datetime as _dt2
            _locked_now = _is_locked(_dt2.fromisoformat(str(game_iso).replace("Z", "+00:00")))
        except Exception:
            _locked_now = None
    _held = conn.execute(
        "SELECT bet_type, pick FROM paper_shadow "
        "WHERE game_id=? AND scope=? AND result IS NULL LIMIT 1",
        (str(game_id), scope),
    ).fetchone()
    if _held and (_held["bet_type"] != bt or _held["pick"] != pk) and (_locked_now is True or _locked_now is None):
        return {"recorded": 0, "locked_hold": 1}
    # Model changed its mind pre-game: void any OTHER still-pending pick.
    conn.execute(
        "UPDATE paper_shadow SET result='V', profit=0, settled_at=? "
        "WHERE game_id=? AND scope=? AND result IS NULL AND (bet_type!=? OR pick!=?)",
        (now_iso, str(game_id), scope, bt, pk),
    )
    # Revive-or-refresh THIS pick. If the model flip-flopped and later reverted
    # to a pick it had already voided, its UNIQUE(game_id,scope,bet_type,pick)
    # row survives as 'V'; a bare INSERT OR IGNORE would leave it voided and the
    # current pick would self-void (the CHW@HOU flip-flop bug, Austin
    # 2026-09-29). Un-void + refresh it so the reverted-to pick is the live
    # pending pick again. Never touch a row already graded W/L/P.
    rev = conn.execute(
        "UPDATE paper_shadow SET result=NULL, profit=NULL, settled_at=NULL, "
        "date=?, matchup=?, line=?, tt_team=?, tt_span=?, model_prob=?, edge=?, "
        "odds=?, captured_at=? WHERE game_id=? AND scope=? AND bet_type=? AND "
        "pick=? AND (result IS NULL OR result='V')",
        (pdate, matchup, line_val, tt_team, tt_span, prob_val, edge_val,
         odds_val, now_iso, str(game_id), scope, bt, pk),
    )
    cur = conn.execute(
        "INSERT OR IGNORE INTO paper_shadow "
        "(date, game_id, matchup, scope, bet_type, pick, line, tt_team, tt_span,"
        " model_prob, edge, odds, captured_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (pdate, str(game_id), matchup, scope, bt, pk, line_val,
         tt_team, tt_span, prob_val, edge_val, odds_val, now_iso),
    )
    conn.commit()
    return {"recorded": 1 if (cur.rowcount or rev.rowcount) else 0}


def _parse_ls(s):
    if not s:
        return None
    try:
        return [int(x or 0) for x in json.loads(s)]
    except Exception:
        return None


def _grade(row):
    """(verdict, profit) for one row given joined scores+linescores, or
    (None, None) if not gradable."""
    bt = (row["bet_type"] or "").upper()
    pick = row["pick"] or ""
    line = row["line"]
    odds = row["odds"]
    hs, as_ = row["home_score"], row["away_score"]
    if hs is None or as_ is None:
        return None, None
    hs, as_ = int(hs), int(as_)
    ls_h = _parse_ls(row["home_linescore"])
    ls_a = _parse_ls(row["away_linescore"])
    mu = row["matchup"] or ""
    away_abbr = home_abbr = ""
    if " @ " in mu:
        away_abbr, home_abbr = [x.strip() for x in mu.split(" @ ", 1)]

    def ou(total, ln, side):
        if ln is None:
            return None, None
        if total == ln:
            return "P", 0.0
        won = (total > ln) if side == "over" else (total < ln)
        return ("W", _payout(odds, True)) if won else ("L", _payout(odds, False))

    def side_of(p):
        return "over" if p.lower().startswith("o") else "under"

    if bt == "ML":
        if pick == home_abbr:
            won, tie = hs > as_, hs == as_
        elif pick == away_abbr:
            won, tie = as_ > hs, hs == as_
        else:
            return None, None
        if tie:
            return "P", 0.0
        return ("W", _payout(odds, True)) if won else ("L", _payout(odds, False))

    if bt == "RUN_LINE":
        if line is None:
            return None, None
        if pick == home_abbr:
            cover = (hs - as_) + line
        elif pick == away_abbr:
            cover = (as_ - hs) + line
        else:
            return None, None
        if cover > 0:
            return "W", _payout(odds, True)
        if cover < 0:
            return "L", _payout(odds, False)
        return "P", 0.0

    if bt == "TOTAL":
        return ou(hs + as_, line, side_of(pick))

    if bt == "1ST_INN":
        if not ls_h or not ls_a:
            return None, None
        first = ls_h[0] + ls_a[0]
        p = pick.upper()
        if p == "NRFI":
            won = first == 0
        elif p == "YRFI":
            won = first > 0
        else:
            return None, None
        return ("W", _payout(odds, True)) if won else ("L", _payout(odds, False))

    if bt == "F5_TOTAL":
        if ls_h is None or ls_a is None:
            return None, None
        f5 = sum(ls_h[:5]) + sum(ls_a[:5])
        return ou(f5, line, side_of(pick))

    if bt == "TEAM_TOTAL":
        team, span = row["tt_team"], row["tt_span"]
        if team == "home":
            val = hs if span != "f5" else (sum(ls_h[:5]) if ls_h else None)
        elif team == "away":
            val = as_ if span != "f5" else (sum(ls_a[:5]) if ls_a else None)
        else:
            return None, None
        if val is None:
            return None, None
        return ou(val, line, side_of(pick))

    return None, None


_ABBR_FIX = {"ARI": "AZ", "CHW": "CWS"}  # ESPN scoreboard abbr -> StatsAPI teams-table abbr


def settle_paper_shadow() -> dict:
    conn = get_conn()
    _ensure(conn)
    now = datetime.utcnow().isoformat()
    cutoff = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    # paper_shadow stores ESPN event ids, which do NOT key the StatsAPI-keyed
    # games table (games.id / games.mlb_game_id). Resolve each pick to its game
    # by matchup teams + date instead (teams.abbreviation -> mlb_id ->
    # games.home/away_team_id). Austin 2026-09-19 -- fixes the 0-settled bug.
    amap = {}
    for t in conn.execute("SELECT abbreviation, mlb_id FROM teams"):
        if t["abbreviation"]:
            amap[t["abbreviation"].upper()] = t["mlb_id"]

    def _tid(ab):
        ab = (ab or "").strip().upper()
        return amap.get(_ABBR_FIX.get(ab, ab))

    def _plus(dstr, n):
        # date string + n days (or None). Paper picks are recorded with the
        # model *run* date, which lags the game's actual date by a day or two
        # (Austin 2026-09-29), so an exact-date games join misses. We retry the
        # same matchup on the following day(s): a matchup plays at most once per
        # date, so a forward-date FINAL row is the same game, never a series
        # sibling, and forward-only never grabs an earlier in-progress game.
        try:
            return (datetime.strptime(dstr, "%Y-%m-%d")
                    + timedelta(days=n)).strftime("%Y-%m-%d")
        except Exception:
            return None

    def _find_game(hi, ai, dstr):
        return conn.execute(
            "SELECT home_score, away_score, status, home_linescore, "
            "away_linescore FROM games WHERE date=? AND home_team_id=? "
            "AND away_team_id=? LIMIT 1", (dstr, hi, ai)).fetchone()

    pend = conn.execute(
        "SELECT * FROM paper_shadow WHERE result IS NULL").fetchall()
    out = {"checked": len(pend), "settled": 0, "wins": 0,
           "losses": 0, "pushes": 0}
    for ps in pend:
        mu = ps["matchup"] or ""
        g = None
        if " @ " in mu:
            aa, ha = [x.strip() for x in mu.split(" @ ", 1)]
            ai, hi = _tid(aa), _tid(ha)
            if ai and hi:
                g = _find_game(hi, ai, ps["date"])
                if g is None:  # exact-date miss -> +1/+2-day offset, FINAL only
                    for _n in (1, 2):
                        _dt = _plus(ps["date"], _n)
                        cand = _find_game(hi, ai, _dt) if _dt else None
                        if cand is not None and "final" in ((cand["status"] or "").lower()):
                            g = cand
                            break
        if g is None:
            # No game row (future date / unresolved): push only once aged out.
            if (ps["date"] or "") < cutoff:
                conn.execute(
                    "UPDATE paper_shadow SET result='P', profit=0, "
                    "settled_at=? WHERE id=?", (now, ps["id"]))
            continue
        st = (g["status"] or "").lower()
        if st in ("postponed", "canceled", "cancelled", "suspended"):
            conn.execute(
                "UPDATE paper_shadow SET result='V', profit=0, "
                "settled_at=? WHERE id=?", (now, ps["id"]))
            continue
        if "final" not in st:
            continue
        row = dict(ps)
        row.update({"home_score": g["home_score"],
                    "away_score": g["away_score"], "status": g["status"],
                    "home_linescore": g["home_linescore"],
                    "away_linescore": g["away_linescore"]})
        verdict, profit = _grade(row)
        if verdict is None:
            continue
        conn.execute(
            "UPDATE paper_shadow SET result=?, profit=?, settled_at=? "
            "WHERE id=?", (verdict, profit, now, ps["id"]))
        out["settled"] += 1
        out["wins" if verdict == "W" else
            "losses" if verdict == "L" else "pushes"] += 1
    conn.commit()
    return out


def paper_shadow_summary(scope=None) -> dict:
    conn = get_conn()
    _ensure(conn)
    q = "SELECT result, profit FROM paper_shadow WHERE result IS NOT NULL AND result != 'V'"
    args = []
    if scope:
        q += " AND scope = ?"
        args.append(scope)
    rows = conn.execute(q, args).fetchall()
    w = sum(1 for r in rows if r["result"] == "W")
    l = sum(1 for r in rows if r["result"] == "L")
    pu = sum(1 for r in rows if r["result"] == "P")
    prof = sum((r["profit"] or 0.0) for r in rows)
    dec = w + l
    return {
        "wins": w, "losses": l, "pushes": pu, "total": w + l + pu,
        "profit": round(prof, 2),
        "win_pct": round(100.0 * w / dec, 1) if dec else 0.0,
        "roi": round(prof / dec, 1) if dec else 0.0,
    }


def paper_shadow_history(scope=None, limit: int = 200):
    conn = get_conn()
    _ensure(conn)
    q = ("SELECT date, matchup, scope, bet_type, pick, line, odds, edge, "
         "       model_prob, result, profit, tt_team, tt_span FROM paper_shadow "
         "WHERE result IS NOT NULL AND result != 'V'")
    args = []
    if scope:
        q += " AND scope = ?"
        args.append(scope)
    q += " ORDER BY settled_at DESC LIMIT ?"
    args.append(limit)
    rows = [dict(r) for r in conn.execute(q, args).fetchall()]
    # Team totals: show WHICH team (+ F5 span) — the stored pick is the bare side.
    for r in rows:
        if (r.get("bet_type") or "").upper() == "TEAM_TOTAL":
            r["pick"] = tt_side_text(r.get("matchup"), r.get("tt_team"),
                                     r.get("tt_span"), r.get("pick"))
    return rows


def recorded_paper_pick(game_id, scope="full"):
    """Best RECORDED paper pick for a game+scope (highest edge, non-voided), in
    the card's paper_pick shape â€” so live/final MLB games keep showing the pick
    (api_best_bets only computes pre-game). Carries the settled result. Austin 2026-09-19."""
    conn = get_conn()
    _ensure(conn)
    r = conn.execute(
        "SELECT bet_type, pick, line, odds, edge, model_prob, matchup, result, tt_team, tt_span "
        "FROM paper_shadow WHERE game_id = ? AND scope = ? AND (result IS NULL OR result != 'V') "
        "ORDER BY edge DESC LIMIT 1", (str(game_id), scope)).fetchone()
    if not r:
        return None
    mu = r["matchup"] or ""
    aa = ha = ""
    if " @ " in mu:
        aa, ha = [x.strip() for x in mu.split(" @ ", 1)]
    return {"type": r["bet_type"], "market": r["bet_type"], "pick": r["pick"],
            "line": r["line"], "odds": r["odds"], "edge": r["edge"],
            "prob": r["model_prob"], "matchup": mu, "away_abbr": aa, "home_abbr": ha,
            "tt_team": r["tt_team"], "tt_span": r["tt_span"],
            "result": r["result"], "paper": True}


# â”€â”€ Daily POTD lock + own record, per scope (Austin 2026-09-19) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# The paper Pick-of-the-Day was recomputed client-side from the LIVE slate every
# load, so it DRIFTED as odds moved / games finalized. Lock the day's POTD per
# scope once (highest-edge <=15 recorded pick among the slate's games, read from
# the STABLE paper_shadow rows) so the DISPLAYED pick holds; settle it with the
# same grader; report ITS OWN record (not the all-picks aggregate). Isolated.
# NOTE: settle_potd shares the shadow's paper_shadow<->games join, which is
# currently broken (paper_shadow.game_id = ESPN event id, games.id = internal /
# games.mlb_game_id = StatsAPI gamePk) â€” so it NO-OPS until that join is fixed.
# The DISPLAY lock (record_potd/get_locked_potd) works regardless (no join).
_POTD_CEILING = 15.0
_POTD_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS paper_potd ("
    " date TEXT, scope TEXT, game_id TEXT, matchup TEXT, bet_type TEXT,"
    " pick TEXT, line REAL, tt_team TEXT, tt_span TEXT, odds INTEGER,"
    " edge REAL, model_prob REAL, result TEXT, profit REAL,"
    " settled_at TEXT, locked_at TEXT, PRIMARY KEY (date, scope)"
    ")"
)


def _fmt_num(x):
    try:
        f = float(x)
        return str(int(f)) if f == int(f) else ("%g" % f)
    except Exception:
        return str(x)


def tt_side_text(matchup, tt_team, tt_span, pick, home_abbr=None, away_abbr=None):
    """Display side for a derivative TEAM_TOTAL: '<TEAM> Over' / '<TEAM> F5 Over'.

    The stored ``pick`` is the bare Over/Under — the UNIQUE(game_id,scope,bet_type,pick)
    key and _grade's side_of() both rely on it — and the team/span live in tt_team /
    tt_span, so every surface rendered 'Over 0.5' with no team (Austin 2026-10-25
    "Over 0.5 on which side?"). Resolve the abbr from the matchup ('TB @ MIL') unless
    explicit abbrs are passed; falls back to the bare pick when unresolvable. Display
    only — never write this back into ``pick``."""
    p = (pick or "").strip()
    if not (home_abbr and away_abbr):
        mu = matchup or ""
        if " @ " in mu:
            a, h = [x.strip() for x in mu.split(" @ ", 1)]
            away_abbr, home_abbr = away_abbr or a, home_abbr or h
    t = (tt_team or "").strip().lower()
    team = home_abbr if t == "home" else away_abbr if t == "away" else None
    if not team:
        return p
    span = " F5" if (tt_span or "").strip().lower() == "f5" else ""
    return f"{team}{span} {p}".strip()


def _potd_display(bt, pick, line, matchup=None, tt_team=None, tt_span=None):
    """Hero headline text for a locked pick (fold the line into totals/RL)."""
    bt = (bt or "").upper()
    if bt == "TEAM_TOTAL":
        pick = tt_side_text(matchup, tt_team, tt_span, pick)
    if line is None:
        return pick
    if bt in ("TOTAL", "F5_TOTAL", "TEAM_TOTAL"):
        return "%s %s" % (pick, _fmt_num(line))
    if bt == "RUN_LINE":
        try:
            sign = "+" if float(line) > 0 else ""
        except Exception:
            sign = ""
        return "%s %s%s" % (pick, sign, _fmt_num(line))
    return pick


def record_potd(scope, slate_date, game_ids=None):
    """Lock the day's POTD for a scope (idempotent per date+scope) = the
    highest-edge (<=ceiling) non-voided recorded pick among the slate's games.
    Reads the STABLE paper_shadow rows so it doesn't drift. Prefers the slate's
    game_ids (sidesteps date-fallback noise); falls back to the date filter."""
    if not slate_date:
        return
    conn = get_conn()
    _ensure(conn)
    conn.execute(_POTD_SCHEMA)
    q = ("SELECT game_id, matchup, bet_type, pick, line, tt_team, tt_span, odds, "
         "edge, model_prob FROM paper_shadow WHERE scope=? "
         "AND (result IS NULL OR result!='V') AND edge IS NOT NULL AND ABS(edge)<=? ")
    args = [scope, _POTD_CEILING]
    gids = [str(g) for g in (game_ids or []) if g is not None]
    if gids:
        q += "AND game_id IN (%s) " % ",".join("?" * len(gids))
        args += gids
    else:
        q += "AND date=? "
        args.append(slate_date)
    q += "ORDER BY edge DESC"  # fetch the pool; re-rank by CALIBRATED edge below
    cands = conn.execute(q, args).fetchall()
    if not cands:
        return
    # Calibration gate (Austin 2026-10-12): rank by calibrated edge (haircuts
    # overconfidence), enforce the +EV floor (no -EV headline), and hold 'extra'
    # scopes (first_inning / derivative) off the headline until they've earned a
    # track record. 'full' is the trusted core scope.
    from engine.calibration_gate import (cal_edge as _cg_edge, cal_prob as _cg_prob,
                                          headline_eligible as _cg_he)
    is_extra = (scope != "full")
    ev_n = ev_days = None
    if is_extra:
        r2 = conn.execute("SELECT COUNT(*) n, MIN(date) mn, MAX(date) mx FROM paper_shadow "
                          "WHERE scope=? AND result IS NOT NULL AND result!='V'", (scope,)).fetchone()
        ev_n = (r2["n"] if r2 else 0) or 0
        ev_days = 0
        if r2 and r2["mn"] and r2["mx"]:
            try:
                ev_days = int((conn.execute("SELECT julianday(?)-julianday(?)", (r2["mx"], r2["mn"])).fetchone()[0]) or 0)
            except Exception:
                ev_days = 0
    row = None; best_ce = None; best_cp = None
    for r in cands:
        ce = _cg_edge("mlb", r["bet_type"], r["model_prob"], r["odds"], pick_text=r["pick"], raw_edge=r["edge"])
        if not _cg_he(ce, is_extra, ev_n, ev_days):
            continue
        if row is None or (ce if ce is not None else -999) > (best_ce if best_ce is not None else -999):
            row = r; best_ce = ce
            best_cp = _cg_prob("mlb", r["bet_type"], r["model_prob"], r["odds"], pick_text=r["pick"], raw_edge=r["edge"])
    if row is None:
        return  # nothing clears the +EV floor / extra-market gate -> honest blank
    # Time-based lock (Austin 2026-10-14): provisional until the selected pick's
    # game reaches its lock_schedule time (7am/4:30pm/overnight ET), then frozen.
    # Was INSERT OR IGNORE = lock on the first record (~midnight). Fail-safe: an
    # unresolved start_time falls back to that lock-on-first behavior.
    _existing = conn.execute(
        "SELECT matchup FROM paper_potd WHERE date=? AND scope=?",
        (slate_date, scope)).fetchone()
    if _existing is not None:
        _ex_start = _resolve_game_start(conn, _existing["matchup"], slate_date)
        if _ex_start is None:
            return  # unresolved -> keep existing (lock-on-first fallback)
        try:
            from engine.lock_schedule import is_locked as _is_locked
            from datetime import datetime as _dt2
            if _is_locked(_dt2.fromisoformat(str(_ex_start).replace("Z", "+00:00"))):
                return  # existing is locked -> frozen, keep it
        except Exception:
            return
        conn.execute("DELETE FROM paper_potd WHERE date=? AND scope=?", (slate_date, scope))
    conn.execute(
        "INSERT OR IGNORE INTO paper_potd (date, scope, game_id, matchup, bet_type,"
        " pick, line, tt_team, tt_span, odds, edge, model_prob, locked_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (slate_date, scope, str(row["game_id"] or ""), row["matchup"] or "",
         row["bet_type"], row["pick"],
         (float(row["line"]) if row["line"] is not None else None),
         row["tt_team"], row["tt_span"], int(row["odds"] or 0),
         (round(best_ce, 2) if best_ce is not None else row["edge"]), best_cp,
         datetime.utcnow().isoformat()))
    conn.commit()


def settle_potd():
    """Grade locked POTDs whose game is final. Resolves the game by matchup
    teams + date (paper_potd stores ESPN ids that don't key the games table).
    Austin 2026-09-19 -- fixes the POTD-own record never settling."""
    conn = get_conn()
    _ensure(conn)
    conn.execute(_POTD_SCHEMA)
    now = datetime.utcnow().isoformat()
    amap = {}
    for t in conn.execute("SELECT abbreviation, mlb_id FROM teams"):
        if t["abbreviation"]:
            amap[t["abbreviation"].upper()] = t["mlb_id"]

    def _tid(ab):
        ab = (ab or "").strip().upper()
        return amap.get(_ABBR_FIX.get(ab, ab))

    pend = conn.execute(
        "SELECT date, scope, bet_type, pick, line, odds, matchup, "
        "tt_team, tt_span FROM paper_potd WHERE result IS NULL").fetchall()
    for p in pend:
        mu = p["matchup"] or ""
        g = None
        if " @ " in mu:
            aa, ha = [x.strip() for x in mu.split(" @ ", 1)]
            ai, hi = _tid(aa), _tid(ha)
            if ai and hi:
                g = conn.execute(
                    "SELECT home_score, away_score, status, home_linescore, "
                    "away_linescore FROM games WHERE date=? AND home_team_id=? "
                    "AND away_team_id=? LIMIT 1",
                    (p["date"], hi, ai)).fetchone()
        if g is None:
            continue
        st = (g["status"] or "").lower()
        if st in ("postponed", "canceled", "cancelled", "suspended"):
            conn.execute(
                "UPDATE paper_potd SET result='V', profit=0, settled_at=? "
                "WHERE date=? AND scope=?", (now, p["date"], p["scope"]))
            continue
        if "final" not in st:
            continue
        row = dict(p)
        row.update({"home_score": g["home_score"],
                    "away_score": g["away_score"], "status": g["status"],
                    "home_linescore": g["home_linescore"],
                    "away_linescore": g["away_linescore"]})
        verdict, profit = _grade(row)
        if verdict is None:
            continue
        conn.execute(
            "UPDATE paper_potd SET result=?, profit=?, settled_at=? "
            "WHERE date=? AND scope=?",
            (verdict, profit, now, p["date"], p["scope"]))
    conn.commit()


def potd_summary(scope):
    """The POTD's OWN record for a scope â€” settled locked POTDs only."""
    conn = get_conn()
    conn.execute(_POTD_SCHEMA)
    rows = conn.execute(
        "SELECT result, profit FROM paper_potd WHERE scope=? "
        "AND result IS NOT NULL AND result!='V'", (scope,)).fetchall()
    w = sum(1 for r in rows if r["result"] == "W")
    l = sum(1 for r in rows if r["result"] == "L")
    pu = sum(1 for r in rows if r["result"] == "P")
    prof = sum((r["profit"] or 0.0) for r in rows)
    dec = w + l
    lr = conn.execute("SELECT result FROM paper_potd WHERE scope=? AND "
                      "result IN ('W','L') ORDER BY date DESC LIMIT 1", (scope,)).fetchone()
    return {"wins": w, "losses": l, "pushes": pu, "total": w + l + pu,
            "profit": round(prof, 2),
            "win_pct": round(100.0 * w / dec, 1) if dec else 0.0,
            "roi": round(prof / dec, 1) if dec else 0.0,
            "last_result": lr["result"] if lr else None}


def get_locked_potd(scope, slate_date):
    """Today's locked POTD for a scope as a hero-ready dict, or None."""
    if not slate_date:
        return None
    conn = get_conn()
    conn.execute(_POTD_SCHEMA)
    r = conn.execute("SELECT * FROM paper_potd WHERE scope=? AND date=?",
                     (scope, slate_date)).fetchone()
    if not r:
        return None
    return {"game_id": r["game_id"], "matchup": r["matchup"],
            "bet_type": r["bet_type"], "type": r["bet_type"],
            "pick": _potd_display(r["bet_type"], r["pick"], r["line"],
                                  r["matchup"], r["tt_team"], r["tt_span"]),
            "line": r["line"], "odds": r["odds"], "edge": r["edge"],
            "model_prob": r["model_prob"], "prob": r["model_prob"],
            "tt_team": r["tt_team"], "tt_span": r["tt_span"],
            "result": r["result"], "paper": True}


def paper_pending_count(scope=None):
    """Recorded-but-unsettled paper picks â€” drives the panel's 'tracking N'
    state so the shadow is visible before anything settles."""
    conn = get_conn()
    _ensure(conn)
    q = "SELECT COUNT(*) c FROM paper_shadow WHERE result IS NULL"
    args = []
    if scope:
        q += " AND scope=?"
        args.append(scope)
    return conn.execute(q, args).fetchone()["c"]
