"""NBA PAPER (shadow) picks — fully isolated in a SEPARATE nba_paper_picks
table so the real nba_picks record, its unique indexes, and every real read
stay byte-for-byte untouched (Austin 2026-09-28, unification Phase 2).

Paper = the model's raw directional lean that the real-bet edge/skip gate
suppressed, recorded stake-free as a forward test; graded + CLV-captured by
reusing the REAL settle/capture core against this table (settle_picks accepts a
table arg; capture rides the SportAdapter's picks_table). Never staked, never
mixed into the real P/L. Mirrors the NHL paper layer, but table-isolated
instead of is_shadow-in-place because nba_picks' UNIQUE(date,game_id,bet_type)
index predates paper and would let a paper row block a later real insert.
"""
from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

PAPER_TABLE = "nba_paper_picks"


def _et_today() -> str:
    try:
        from ._record import et_today_str
        return et_today_str()
    except Exception:
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d")  # Beelink runs on ET


def _ensure_paper_table(conn) -> None:
    conn.execute(
        f"""CREATE TABLE IF NOT EXISTS {PAPER_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT, date TEXT NOT NULL, matchup TEXT,
            bet_type TEXT NOT NULL, pick TEXT NOT NULL,
            model_prob REAL, edge REAL, odds INTEGER,
            result TEXT, profit REAL,
            created_at TEXT DEFAULT (datetime('now')), settled_at TEXT,
            closing_odds INTEGER, stake_units REAL, closing_odds_captured_at TEXT
        )""")
    # One paper row per (date, game, family) — idempotent re-records.
    conn.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{PAPER_TABLE}_family "
        f"ON {PAPER_TABLE}(date, game_id, bet_type)")
    conn.commit()


# The card's two views (api_nba_bets): the Q1 view = primary Q1 markets + the Q1 team-total /
# odd-even derivatives; everything else (ML/SPREAD/TOTAL/ALT/H1/H2/quarters) is the Full view.
Q1_VIEW_TYPES = frozenset({"Q1_ML", "Q1_SPREAD", "Q1_TOTAL", "Q1 Team Total", "Q1 Total O/E"})


def paper_scope_of(bet_type) -> str:
    return "q1" if (bet_type or "") in Q1_VIEW_TYPES else "full"


def record_paper(matchup, game_id, paper_picks, target_date=None, families=None, scope=None) -> dict:
    """Record a view's authority pick (stake_units=0) into nba_paper_picks -- FIRST PICK WINS.

    "One pick, read everywhere" (Austin 2026-11-01): a game's pick is recorded ONCE, the first time
    it is chosen, at its entry price, and is NEVER rewritten as lines move. Per (game, view scope):
    once the game has any non-void row in that scope (pending OR settled) this is a no-op -- no
    DELETE of a "stale" row, no in-place UPDATE of its pick/line/odds/edge/prob, no second row that
    could out-rank it. closing_odds capture + settlement write their own columns and are untouched.
    (Was a LIVE MIRROR until 2026-11-01: it DELETEd a pending row whose pick moved and UPDATEd an
    unchanged one's odds/edge -- the card/tracker headline flipped between restarts.)

    ``scope`` is 'full' or 'q1' (see Q1_VIEW_TYPES). ``families`` is the legacy per-view bet_type
    tuple; it only infers the scope now (its tuples missed H1/H2 + the Q1 team-total types, which is
    how a second row per view slipped in). With neither, plain per-bet_type INSERT OR IGNORE (also
    never updates). NEVER touches nba_picks."""
    from ..nba_db import get_conn
    conn = get_conn()
    _ensure_paper_table(conn)
    target_date = target_date or _et_today()
    gid = str(game_id) if game_id is not None else None
    if scope is None and families:
        scope = "q1" if any(str(f).upper().startswith("Q1") for f in families) else "full"
    valid = [pp for pp in (paper_picks or [])
             if pp.get("type") and pp.get("pick") is not None
             and (scope is None or paper_scope_of(pp.get("type")) == scope)]
    if scope is not None:
        for r in conn.execute(
                f"SELECT bet_type FROM {PAPER_TABLE} WHERE game_id=? "
                f"AND (result IS NULL OR result != 'V')", (gid,)).fetchall():
            if paper_scope_of(r["bet_type"]) == scope:
                return {"recorded": 0, "held": 1}  # first pick already recorded -> hold it
        valid = valid[:1]  # one pick per view
    n = 0
    for pp in valid:
        bt, pk = pp.get("type"), pp.get("pick")
        try:
            cur = conn.execute(
                f"INSERT OR IGNORE INTO {PAPER_TABLE} "
                f"(game_id, date, matchup, bet_type, pick, model_prob, edge, "
                f" odds, stake_units) VALUES (?,?,?,?,?,?,?,?,0)",
                (gid, target_date, matchup, bt, pk,
                 pp.get("prob"), pp.get("edge"), pp.get("odds")))
            if cur.rowcount:
                n += 1
        except Exception as e:
            logger.debug("nba paper insert failed %s/%s: %s", matchup, bt, e)
    conn.commit()
    return {"recorded": n}


def capture_closing_odds_paper() -> int:
    """Snapshot HR NBA odds onto pending paper rows (paper CLV). Reuses the real
    capture core, pointed at the paper table via the adapter's picks_table."""
    from ..nba_db import get_conn
    from scrapers.hardrock_odds import fetch_nba as _hr_nba
    from ..tracker_core import SportAdapter, core_capture_closing_odds
    from ._helpers import _extract_nba_closing_for_pick
    conn = get_conn()
    _ensure_paper_table(conn)
    adapter = SportAdapter(
        name="nba-paper", get_conn=get_conn, picks_table=PAPER_TABLE,
        hr_fetch=_hr_nba, extract_closing=_extract_nba_closing_for_pick)
    return core_capture_closing_odds(adapter)


def settle_paper() -> dict:
    """Grade pending paper rows with the SAME core as the real settle."""
    from ..nba_db import get_conn
    from ._settle import settle_picks
    _ensure_paper_table(get_conn())
    return settle_picks(table=PAPER_TABLE)


def recorded_nba_paper_pick(game_id) -> dict | None:
    """The game's recorded pick for the live/final scoreboard card = the FIRST non-void
    nba_paper_picks row -- the same row routes_nba._nba_recorded_authority_best holds on the
    pre-game card and the unified tracker grades (first pick wins, 2026-11-01; was highest edge
    with a 20pp guard, which could surface the OTHER view's row once a game went live).
    Card-chip shaped + settled W/L. Best-bets is pre-game only (Austin 2026-09-29)."""
    if game_id is None:
        return None
    from ..nba_db import get_conn
    conn = get_conn()
    _ensure_paper_table(conn)
    r = conn.execute(
        f"SELECT bet_type, pick, odds, edge, model_prob, result FROM {PAPER_TABLE} "
        f"WHERE game_id = ? AND (result IS NULL OR result != 'V') "
        f"ORDER BY id ASC LIMIT 1", (str(game_id),)).fetchone()
    if not r:
        return None
    return {"type": r["bet_type"], "market": r["bet_type"], "pick": r["pick"],
            "odds": r["odds"], "edge": r["edge"], "prob": r["model_prob"],
            "result": r["result"], "paper": True}


def paper_summary() -> dict:
    """Flat-1u P/L + CLV over paper rows (stake-free forward test)."""
    from ..nba_db import get_conn
    from ._helpers import _compute_clv
    conn = get_conn()
    _ensure_paper_table(conn)
    by_type = {}
    for bt in ["ML", "SPREAD", "TOTAL", "ALT SPREAD", "ALT TOTAL",
               "Q1_ML", "Q1_SPREAD", "Q1_TOTAL"]:
        r = conn.execute(
            f"""SELECT COUNT(*) t,
                SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) w,
                SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) l,
                SUM(CASE WHEN result='P' THEN 1 ELSE 0 END) p,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) pend,
                COALESCE(SUM(profit),0) profit
                FROM {PAPER_TABLE} WHERE bet_type=?""", (bt,)).fetchone()
        tot = r["t"] or 0
        if not tot:
            continue
        w = r["w"] or 0; l = r["l"] or 0; sc = w + l
        by_type[bt] = {
            "total": tot, "wins": w, "losses": l, "pushes": r["p"] or 0,
            "pending": r["pend"] or 0, "profit": round(r["profit"], 2),
            "win_pct": round(w / sc * 100, 1) if sc else 0,
            "roi": round(r["profit"] / sc, 1) if sc else 0}
    tr = conn.execute(
        f"""SELECT COUNT(*) t,
            SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) w,
            SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) l,
            SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) pend,
            COALESCE(SUM(profit),0) profit FROM {PAPER_TABLE}""").fetchone()
    tw = tr["w"] or 0; tl = tr["l"] or 0; sc = tw + tl
    clv = []
    for r in conn.execute(
            f"SELECT odds, closing_odds FROM {PAPER_TABLE} "
            f"WHERE result IS NOT NULL AND odds IS NOT NULL "
            f"AND closing_odds IS NOT NULL"):
        v = _compute_clv(r["odds"], r["closing_odds"])
        if v is not None:
            clv.append(v)
    return {"by_type": by_type, "overall": {
        "total": tr["t"] or 0, "wins": tw, "losses": tl,
        "pending": tr["pend"] or 0, "profit": round(tr["profit"], 2),
        "win_pct": round(tw / sc * 100, 1) if sc else 0,
        "roi": round(tr["profit"] / sc, 1) if sc else 0,
        "avg_clv": round(sum(clv) / len(clv), 2) if clv else None,
        "clv_sample": len(clv)}}


def list_paper_history(limit=50) -> list:
    from ..nba_db import get_conn
    conn = get_conn()
    _ensure_paper_table(conn)
    rows = conn.execute(
        f"SELECT * FROM {PAPER_TABLE} ORDER BY created_at DESC LIMIT ?",
        (limit,)).fetchall()
    return [dict(r) for r in rows]


# ── PAPER Pick-of-the-Day (Austin 2026-10-03): game-day-locked, paper-only ──
# Mirrors engine.nhl_tracker._summary.record_paper_potd, adapted to the separate
# nba_paper_picks table + scope-aware (full vs Q1). INSERT OR IGNORE keyed
# (date, scope) = idempotent per day, so the pick FREEZES once the day arrives.
_FULL_POTD_FAM = ("ML", "SPREAD", "TOTAL", "ALT SPREAD", "ALT TOTAL")
_Q1_POTD_FAM = ("Q1_ML", "Q1_SPREAD", "Q1_TOTAL")


def _ensure_paper_potd_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nba_paper_pot_day (
            date        TEXT NOT NULL,
            scope       TEXT NOT NULL DEFAULT 'full',
            game_id     TEXT, matchup TEXT, bet_type TEXT, pick TEXT,
            model_prob  REAL, edge REAL, odds INTEGER,
            created_at  TEXT DEFAULT (datetime('now'))
        )""")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_nba_paper_pot_day "
                 "ON nba_paper_pot_day(date, scope)")
    conn.commit()


def _potd_market_proven(key, league, bet_type, pick_text="", odds=None):
    """True if (key, league, bet_type, condition) clears the proven-edge gate -- the SAME gate
    the cards use via select_pick. `condition` (fav/dog/over/under) is derived from pick_text/
    odds the same way select_pick does it. Fail-CLOSED (never headline) on any error. (Pick
    Authority 2026-10-18, league-segmented 2026-10-22, condition-segmented 2026-10-22.)"""
    try:
        from engine.selection import edge_proven
        cond = edge_proven._condition(pick_text, bet_type, odds)
        return bool(edge_proven.is_eligible(key, league, bet_type, cond))
    except Exception:
        return False


def record_nba_paper_potd(date=None, scope="full") -> dict | None:
    """Lock the day's PAPER POTD for a view = the highest-edge nba_paper_picks
    lean for ``date`` in that view's family group (<=20pp, non-void). Idempotent
    per (date, scope) — frozen once the first lock of the day lands."""
    from ..nba_db import get_conn
    conn = get_conn()
    _ensure_paper_table(conn)
    _ensure_paper_potd_table(conn)
    d = date or _et_today()
    fam = _Q1_POTD_FAM if scope == "q1" else _FULL_POTD_FAM
    ph = ",".join("?" * len(fam))
    cands = conn.execute(
        f"SELECT game_id, matchup, bet_type, pick, model_prob, edge, odds "
        f"FROM {PAPER_TABLE} WHERE date=? AND bet_type IN ({ph}) "
        f"AND (result IS NULL OR result != 'V') AND ABS(COALESCE(edge,0)) <= 20.0 "
        f"ORDER BY edge DESC", (d, *fam)).fetchall()
    if not cands:
        return None
    # Calibration gate (Austin 2026-10-12): calibrated-edge rank + +EV floor; Q1
    # is an 'extra' market gated on a track record, 'full' is the core default.
    from engine.calibration_gate import (cal_edge as _cg_edge, cal_prob as _cg_prob,
                                          headline_eligible as _cg_he)
    is_extra = (scope == "q1")
    ev_n = ev_days = None
    if is_extra:
        r2 = conn.execute(
            f"SELECT COUNT(*) n, MIN(date) mn, MAX(date) mx FROM {PAPER_TABLE} "
            f"WHERE bet_type IN ({ph}) AND result IS NOT NULL AND result!='V'", tuple(fam)).fetchone()
        ev_n = (r2["n"] if r2 else 0) or 0
        ev_days = 0
        if r2 and r2["mn"] and r2["mx"]:
            try:
                ev_days = int((conn.execute("SELECT julianday(?)-julianday(?)", (r2["mx"], r2["mn"])).fetchone()[0]) or 0)
            except Exception:
                ev_days = 0
    best = None; best_ce = None; best_cp = None
    for r in cands:
        if not _potd_market_proven("nba", "nba", r["bet_type"], r["pick"], r["odds"]):
            continue  # Pick Authority 2026-10-18: only PROVEN markets headline the POTD (matches the card gate)
        ce = _cg_edge("nba", r["bet_type"], r["model_prob"], r["odds"], pick_text=r["pick"], raw_edge=r["edge"])
        if not _cg_he(ce, is_extra, ev_n, ev_days):
            continue
        if best is None or (ce if ce is not None else -999) > (best_ce if best_ce is not None else -999):
            best = r; best_ce = ce
            best_cp = _cg_prob("nba", r["bet_type"], r["model_prob"], r["odds"], pick_text=r["pick"], raw_edge=r["edge"])
    if best is None:
        return None
    conn.execute(
        "INSERT OR IGNORE INTO nba_paper_pot_day "
        "(date, scope, game_id, matchup, bet_type, pick, model_prob, edge, odds) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (d, scope, best["game_id"], best["matchup"], best["bet_type"],
         best["pick"], best_cp, (round(best_ce,2) if best_ce is not None else best["edge"]), best["odds"]))
    conn.commit()
    return get_nba_paper_potd(d, scope)


def get_nba_paper_potd(date=None, scope="full") -> dict | None:
    """Today's locked PAPER POTD for a view, joined to its nba_paper_picks row
    for the live result/profit. Hero-ready (PaperPickOfDayHero's `locked`)."""
    from ..nba_db import get_conn
    conn = get_conn()
    _ensure_paper_potd_table(conn)
    d = date or _et_today()
    r = conn.execute("SELECT * FROM nba_paper_pot_day WHERE date=? AND scope=?",
                     (d, scope)).fetchone()
    if not r:
        return None
    row = dict(r)
    res = conn.execute(
        f"SELECT result, profit FROM {PAPER_TABLE} WHERE date=? AND game_id=? "
        f"AND bet_type=? AND pick=? LIMIT 1",
        (d, row["game_id"], row["bet_type"], row["pick"])).fetchone()
    return {"game_id": row["game_id"], "matchup": row["matchup"],
            "bet_type": row["bet_type"], "type": row["bet_type"],
            "pick": row["pick"], "odds": row["odds"], "edge": row["edge"],
            "model_prob": row["model_prob"], "prob": row["model_prob"],
            "result": res["result"] if res else None,
            "profit": res["profit"] if res else None, "paper": True}


def nba_paper_potd_summary(scope="full") -> dict:
    """Running W/L/profit across every locked PAPER POTD for a view (JOIN
    nba_paper_picks for the settled result). Same shape as the NHL paper POTD
    summary so it drops straight into the paper hero's `summary` prop."""
    from ..nba_db import get_conn
    conn = get_conn()
    _ensure_paper_potd_table(conn)
    rows = conn.execute(f"""
        SELECT p.result AS result, p.profit AS profit
        FROM nba_paper_pot_day pot
        LEFT JOIN {PAPER_TABLE} p
          ON p.date=pot.date AND p.game_id=pot.game_id
         AND p.bet_type=pot.bet_type AND p.pick=pot.pick
        WHERE pot.scope=?""", (scope,)).fetchall()
    w = sum(1 for r in rows if r["result"] == "W")
    l = sum(1 for r in rows if r["result"] == "L")
    pu = sum(1 for r in rows if r["result"] == "P")
    prof = sum((r["profit"] or 0.0) for r in rows if r["result"] is not None)
    pend = sum(1 for r in rows if r["result"] is None)
    dec = w + l
    return {"total": w + l + pu, "wins": w, "losses": l, "pushes": pu,
            "pending": pend, "profit": round(prof, 2),
            "win_pct": round(100.0 * w / dec, 1) if dec else 0.0,
            "roi": round(prof / dec, 1) if dec else 0.0}
