"""Tennis PAPER Pick-of-the-Day — game-day-locked, paper-only.

Mirrors engine.basketball._paper_potd / engine.nhl_tracker._summary's
record_paper_potd pattern: one row locked per day, idempotent (INSERT
OR IGNORE keyed on (date, tour)), built from tennis_picks' is_paper=1
rows (the model's calibrated leans while tennis is on probation and
has no real POTD to show). Feeds PaperPickOfDayHero's `locked` prop
directly and backend/server/routes_potd.py's cross-sport fallback, the
same two places every other sport's paper POTD lands.

Tennis has no Q1-style sub-view the way basketball/NHL do — ATP and
WTA are two independent slates on the same date instead, so `tour`
fills the role basketball's `scope` (full/q1) plays: one lock per
(date, tour).

PickMint tennis parity Phase 2/3, Austin 2026-10-10.
"""
from __future__ import annotations

from ._tz import et_today_str

# Same sanity ceiling basketball's record_paper_potd applies (Austin
# 2026-10-03 comment there: "the biggest edges are often the model's
# WORST picks" — proven across tennis/soccer specifically in the
# PaperPickOfDayHero.jsx POTD_EDGE_CEILING note). Tennis is a confirmed
# overconfident model (tennis_soccer_model_edge_analysis.md), so this
# matters here at least as much as it does for basketball. Tune from
# data if the real edge distribution says otherwise.
_EDGE_CEILING = 20.0


def _ensure_paper_potd_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tennis_paper_potd (
            date        TEXT NOT NULL,
            tour        TEXT NOT NULL DEFAULT 'atp',
            match_id    TEXT, matchup TEXT, bet_type TEXT, pick TEXT,
            model_prob  REAL, edge REAL, odds INTEGER,
            created_at  TEXT DEFAULT (datetime('now'))
        )""")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_tennis_paper_potd "
                 "ON tennis_paper_potd(date, tour)")
    conn.commit()


def record_tennis_paper_potd(tour: str = "atp", date: str | None = None) -> dict | None:
    """Lock today's PAPER POTD for ``tour`` = the highest-edge is_paper=1
    pick for ``date`` (<=20pp sanity ceiling, non-void). Idempotent per
    (date, tour) — frozen once the first lock of the day lands, same
    game-day-lock contract every other sport's paper POTD keeps."""
    from .tennis_db import get_conn
    conn = get_conn()
    _ensure_paper_potd_table(conn)
    d = date or et_today_str()
    best = conn.execute(
        "SELECT match_id, matchup, bet_type, pick, model_prob, edge, odds "
        "FROM tennis_picks "
        "WHERE is_paper = 1 AND tour = ? AND date = ? "
        "  AND (result IS NULL OR result != 'V') "
        "  AND ABS(COALESCE(edge, 0)) <= ? "
        # +EV subset only (Austin 2026-10-13): set-spreads + dogs >=+150,
        # drop TOTAL_GAMES + ATP high-edge; rank by CONVICTION not edge
        # (edge-ranking literally picked the worst bets). Matches the
        # card filter; verified +20% ROI vs -1% ungated.
        "  AND (bet_type = 'SET_SPREAD' OR odds >= 150) "
        "  AND bet_type != 'TOTAL_GAMES' "
        "  AND NOT (tour = 'atp' AND COALESCE(edge,0) >= 20) "
        "ORDER BY COALESCE(conviction_score,0) DESC LIMIT 1",
        (tour, d, _EDGE_CEILING),
    ).fetchone()
    if not best:
        return None
    conn.execute(
        "INSERT OR IGNORE INTO tennis_paper_potd "
        "(date, tour, match_id, matchup, bet_type, pick, model_prob, edge, odds) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (d, tour, best["match_id"], best["matchup"], best["bet_type"],
         best["pick"], best["model_prob"], best["edge"], best["odds"]),
    )
    conn.commit()
    return get_tennis_paper_potd(tour, d)


def get_tennis_paper_potd(tour: str = "atp", date: str | None = None) -> dict | None:
    """Today's locked PAPER POTD for ``tour``, joined to its is_paper=1
    pick row for the live result/profit. Hero-ready (PaperPickOfDayHero's
    `locked` prop) — same return shape basketball's get_paper_potd ships,
    so PotdHero/PaperPickOfDayHero render it identically to every other
    sport's paper POTD."""
    from .tennis_db import get_conn
    conn = get_conn()
    _ensure_paper_potd_table(conn)
    d = date or et_today_str()
    r = conn.execute(
        "SELECT * FROM tennis_paper_potd WHERE date = ? AND tour = ?",
        (d, tour),
    ).fetchone()
    if not r:
        return None
    row = dict(r)
    res = conn.execute(
        "SELECT result, profit FROM tennis_picks "
        "WHERE is_paper = 1 AND tour = ? AND date = ? "
        "  AND match_id = ? AND bet_type = ? AND pick = ? LIMIT 1",
        (tour, d, row["match_id"], row["bet_type"], row["pick"]),
    ).fetchone()
    return {
        "game_id": row["match_id"], "match_id": row["match_id"],
        "matchup": row["matchup"],
        "bet_type": row["bet_type"], "type": row["bet_type"],
        "pick": row["pick"], "odds": row["odds"], "edge": row["edge"],
        "model_prob": row["model_prob"], "prob": row["model_prob"],
        "tour": tour, "sport": "tennis",
        "result": res["result"] if res else None,
        "profit": res["profit"] if res else None,
        "paper": True,
    }


def tennis_paper_potd_summary(tour: str = "atp") -> dict:
    """Running W/L/profit across every locked PAPER POTD for ``tour``
    (JOIN tennis_picks' is_paper=1 rows for the settled result). Same
    {total, wins, losses, pushes, pending, profit, win_pct, roi} shape
    basketball's paper_potd_summary ships — drops straight into the
    paper hero's `summary` prop."""
    from .tennis_db import get_conn
    conn = get_conn()
    _ensure_paper_potd_table(conn)
    rows = conn.execute(
        "SELECT p.result AS result, p.profit AS profit "
        "FROM tennis_paper_potd pot "
        "LEFT JOIN tennis_picks p "
        "  ON p.date = pot.date AND p.match_id = pot.match_id "
        " AND p.bet_type = pot.bet_type AND p.pick = pot.pick "
        " AND p.is_paper = 1 "
        "WHERE pot.tour = ?",
        (tour,),
    ).fetchall()
    w = sum(1 for r in rows if r["result"] == "W")
    l = sum(1 for r in rows if r["result"] == "L")
    pu = sum(1 for r in rows if r["result"] == "P")
    prof = sum((r["profit"] or 0.0) for r in rows if r["result"] is not None)
    pend = sum(1 for r in rows if r["result"] is None)
    dec = w + l
    return {
        "total": w + l + pu, "wins": w, "losses": l, "pushes": pu,
        "pending": pend, "profit": round(prof, 2),
        "win_pct": round(100.0 * w / dec, 1) if dec else 0.0,
        "roi": round(prof / dec, 1) if dec else 0.0,
    }
