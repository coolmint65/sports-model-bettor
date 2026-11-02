"""Basketball PAPER Pick-of-the-Day — game-day-locked, paper-only (Austin 2026-10-03).

Per-league paper POTD, mirroring engine.nhl_tracker._summary.record_paper_potd,
built from each league's is_shadow=1 (paper) picks. INSERT OR IGNORE keyed
(date, scope) = idempotent per day -> frozen once the day arrives. Lives in each
league's own DB (get_conn(league) is per-league), so no league column is needed.
Covers WNBA + every framework basketball league; NBA has its own (nba_tracker).
"""
from __future__ import annotations

from ._db import get_conn, picks_table
from .._tz import et_today_str

_FULL_FAM = ("ML", "SPREAD", "TOTAL", "ALT SPREAD", "ALT TOTAL")
_Q1_FAM = ("Q1_ML", "Q1_SPREAD", "Q1_TOTAL")


def _ensure_paper_potd_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_pot_day (
            date        TEXT NOT NULL,
            scope       TEXT NOT NULL DEFAULT 'full',
            game_id     TEXT, matchup TEXT, bet_type TEXT, pick TEXT,
            model_prob  REAL, edge REAL, odds INTEGER,
            created_at  TEXT DEFAULT (datetime('now'))
        )""")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_paper_pot_day "
                 "ON paper_pot_day(date, scope)")
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


def record_paper_potd(league: str, date: str | None = None, scope: str = "full") -> dict | None:
    """Lock the day's PAPER POTD for a league+view = the highest-edge is_shadow=1
    pick for ``date`` in that view's family group (<=20pp, non-void). Idempotent
    per (date, scope) — frozen once the first lock of the day lands (game-day lock)."""
    conn = get_conn(league)
    tbl = picks_table(league)
    _ensure_paper_potd_table(conn)
    d = date or et_today_str()
    fam = _Q1_FAM if scope == "q1" else _FULL_FAM
    ph = ",".join("?" * len(fam))
    cands = conn.execute(
        f"SELECT game_id, matchup, bet_type, pick, model_prob, edge, odds "
        f"FROM {tbl} WHERE COALESCE(is_shadow,0)=1 AND date=? AND bet_type IN ({ph}) "
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
            f"SELECT COUNT(*) n, MIN(date) mn, MAX(date) mx FROM {tbl} "
            f"WHERE COALESCE(is_shadow,0)=1 AND bet_type IN ({ph}) "
            f"AND result IS NOT NULL AND result!='V'", tuple(fam)).fetchone()
        ev_n = (r2["n"] if r2 else 0) or 0
        ev_days = 0
        if r2 and r2["mn"] and r2["mx"]:
            try:
                ev_days = int((conn.execute("SELECT julianday(?)-julianday(?)", (r2["mx"], r2["mn"])).fetchone()[0]) or 0)
            except Exception:
                ev_days = 0
    best = None; best_ce = None; best_cp = None
    for r in cands:
        if not _potd_market_proven("basketball", league, r["bet_type"], r["pick"], r["odds"]):
            continue  # Pick Authority 2026-10-18: only PROVEN markets headline the POTD (matches the card gate)
        ce = _cg_edge(league, r["bet_type"], r["model_prob"], r["odds"], pick_text=r["pick"], raw_edge=r["edge"])
        if not _cg_he(ce, is_extra, ev_n, ev_days):
            continue
        if best is None or (ce if ce is not None else -999) > (best_ce if best_ce is not None else -999):
            best = r; best_ce = ce
            best_cp = _cg_prob(league, r["bet_type"], r["model_prob"], r["odds"], pick_text=r["pick"], raw_edge=r["edge"])
    if best is None:
        return None
    conn.execute(
        "INSERT OR IGNORE INTO paper_pot_day "
        "(date, scope, game_id, matchup, bet_type, pick, model_prob, edge, odds) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (d, scope, best["game_id"], best["matchup"], best["bet_type"],
         best["pick"], best_cp, (round(best_ce,2) if best_ce is not None else best["edge"]), best["odds"]))
    conn.commit()
    return get_paper_potd(league, d, scope)


def get_paper_potd(league: str, date: str | None = None, scope: str = "full") -> dict | None:
    """Today's locked PAPER POTD for a league+view, joined to its is_shadow=1 pick
    row for the live result/profit. Hero-ready (PaperPickOfDayHero's `locked`)."""
    conn = get_conn(league)
    tbl = picks_table(league)
    _ensure_paper_potd_table(conn)
    d = date or et_today_str()
    r = conn.execute("SELECT * FROM paper_pot_day WHERE date=? AND scope=?",
                     (d, scope)).fetchone()
    if not r:
        return None
    row = dict(r)
    res = conn.execute(
        f"SELECT result, profit FROM {tbl} WHERE COALESCE(is_shadow,0)=1 AND date=? "
        f"AND game_id=? AND bet_type=? AND pick=? LIMIT 1",
        (d, row["game_id"], row["bet_type"], row["pick"])).fetchone()
    return {"game_id": row["game_id"], "matchup": row["matchup"],
            "bet_type": row["bet_type"], "type": row["bet_type"],
            "pick": row["pick"], "odds": row["odds"], "edge": row["edge"],
            "model_prob": row["model_prob"], "prob": row["model_prob"],
            "result": res["result"] if res else None,
            "profit": res["profit"] if res else None, "paper": True}


def paper_potd_summary(league: str, scope: str = "full") -> dict:
    """Running W/L/profit across every locked PAPER POTD for a league+view (JOIN
    the is_shadow=1 picks for the settled result). Same shape as the NHL paper
    POTD summary so it drops straight into the paper hero's `summary` prop."""
    conn = get_conn(league)
    tbl = picks_table(league)
    _ensure_paper_potd_table(conn)
    rows = conn.execute(f"""
        SELECT p.result AS result, p.profit AS profit
        FROM paper_pot_day pot
        LEFT JOIN {tbl} p
          ON p.date=pot.date AND p.game_id=pot.game_id
         AND p.bet_type=pot.bet_type AND p.pick=pot.pick
         AND COALESCE(p.is_shadow,0)=1
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
