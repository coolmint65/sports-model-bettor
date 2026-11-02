"""NHL tracker summary read."""

from __future__ import annotations

from ._helpers import _get_nhl_db, _compute_clv
from .._tz import et_today_str


def get_pick_summary() -> dict:
    """Get running totals across all NHL picks.

    Profit + ROI are stake-weighted (see engine.tracker._summary for
    rationale). Legacy NULL stake_units rows fall back to 1.0u so
    historical totals remain consistent.
    """
    conn = _get_nhl_db()

    summary = {}
    for bt in ["ML", "O/U", "PL"]:
        row = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
                COALESCE(SUM(profit * COALESCE(stake_units, 1.0)), 0) as profit,
                COALESCE(SUM(CASE WHEN result IN ('W','L')
                                    THEN COALESCE(stake_units, 1.0)
                                    ELSE 0 END), 0) as stake_units_settled
            FROM nhl_picks WHERE bet_type = ? AND COALESCE(is_shadow,0)=0
        """, (bt,)).fetchone()

        total = row["total"] or 0
        w = row["wins"] or 0
        l = row["losses"] or 0
        settled_count = w + l
        staked_u = row["stake_units_settled"] or 0
        summary[bt] = {
            "total": total,
            "wins": w,
            "losses": l,
            "pushes": row["pushes"],
            "pending": row["pending"],
            "profit": round(row["profit"], 2),
            "win_pct": round(w / settled_count * 100, 1) if settled_count > 0 else 0,
            "roi": round(row["profit"] / staked_u, 1) if staked_u > 0 else 0,
        }

    # Real rows only — is_shadow=1 (paper) rows are summarized separately
    # by paper_summary() and must never enter the real P/L (Austin 2026-09-22).
    recent = conn.execute("""
        SELECT * FROM nhl_picks WHERE COALESCE(is_shadow,0)=0
        ORDER BY created_at DESC LIMIT 30
    """).fetchall()

    totals = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
            COALESCE(SUM(profit * COALESCE(stake_units, 1.0)), 0) as profit,
            COALESCE(SUM(CASE WHEN result IN ('W','L')
                                THEN COALESCE(stake_units, 1.0)
                                ELSE 0 END), 0) as stake_units_settled
        FROM nhl_picks WHERE COALESCE(is_shadow,0)=0
    """).fetchone()

    tw = totals["wins"] or 0
    tl = totals["losses"] or 0
    tstaked = totals["stake_units_settled"] or 0

    clv_rows = conn.execute("""
        SELECT odds, closing_odds FROM nhl_picks
        WHERE result IS NOT NULL AND odds IS NOT NULL AND closing_odds IS NOT NULL
          AND COALESCE(is_shadow,0)=0
    """).fetchall()
    clv_values = []
    for r in clv_rows:
        clv = _compute_clv(r["odds"], r["closing_odds"])
        if clv is not None:
            clv_values.append(clv)
    avg_clv = round(sum(clv_values) / len(clv_values), 2) if clv_values else None

    # Project stake-weighted profit per row so display matches totals.
    recent_out = []
    for r in recent:
        d = dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        recent_out.append(d)

    return {
        "by_type": summary,
        "overall": {
            "total": totals["total"] or 0,
            "wins": tw,
            "losses": tl,
            "pending": totals["pending"] or 0,
            "profit": round(totals["profit"] or 0, 2),
            "win_pct": round(tw / (tw + tl) * 100, 1) if (tw + tl) > 0 else 0,
            "roi": round((totals["profit"] or 0) / tstaked, 1) if tstaked > 0 else 0,
            "avg_clv": avg_clv,
            "clv_sample": len(clv_values),
        },
        "recent": recent_out,
    }


# ── Calibrated PAPER (is_shadow=1) reads (Austin 2026-09-22) ──────────
# Mirror of engine.football._tracker.paper_summary / list_history(shadow=1).
# Paper is UNSTAKED: profit is the raw 1u-basis value the settler wrote
# (+odds on a win, -100 on a loss); we don't stake-weight it.

def paper_summary() -> dict:
    """Paper (is_shadow=1) P/L — one best lean per game (max edge), so the
    count + P/L reflect what the card/POTD show instead of triple-counting
    ML+O/U+PL per game. Flat dict (wins/losses/.../avg_clv) matching the
    shape PickHistory's paper hero consumes."""
    conn = _get_nhl_db()
    best = conn.execute(
        "SELECT result, profit, odds, closing_odds FROM ("
        "  SELECT result, profit, odds, closing_odds, "
        "    ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY edge DESC) AS rn "
        "  FROM nhl_picks "
        "  WHERE COALESCE(is_shadow,0)=1 AND (result IS NULL OR result != 'V')"
        ") WHERE rn = 1"
    ).fetchall()
    wv = sum(1 for r in best if r["result"] == "W")
    lv = sum(1 for r in best if r["result"] == "L")
    pu = sum(1 for r in best if r["result"] == "P")
    prof = sum((r["profit"] or 0.0) for r in best if r["result"] is not None)
    dec = wv + lv
    pending = sum(1 for r in best if r["result"] is None)
    clv_values = []
    for r in best:
        if r["result"] is None or not r["odds"] or not r["closing_odds"]:
            continue
        clv = _compute_clv(r["odds"], r["closing_odds"])
        if clv is not None:
            clv_values.append(clv)
    avg_clv = round(sum(clv_values) / len(clv_values), 2) if clv_values else None
    return {"wins": wv, "losses": lv, "pushes": pu, "total": wv + lv + pu,
            "profit": round(prof, 2),
            "win_pct": round(100.0 * wv / dec, 1) if dec else 0.0,
            "roi": round(prof / dec, 1) if dec else 0.0,
            "pending": pending,
            "avg_clv": avg_clv, "clv_sample": len(clv_values)}


def list_paper_history(limit: int = 50) -> list[dict]:
    """Settled + pending paper (is_shadow=1) rows, most-recent first, for the
    'Paper Picks · shadow' table. Unstaked: profit returned as the raw 1u
    value (PicksTable renders these with the `unstaked` flag)."""
    conn = _get_nhl_db()
    rows = conn.execute(
        "SELECT * FROM nhl_picks "
        "WHERE COALESCE(is_shadow,0)=1 AND (result IS NULL OR result != 'V') "
        "ORDER BY created_at DESC LIMIT ?",
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]


def recorded_nhl_paper_pick(game_id, date: str | None = None) -> dict | None:
    """Best recorded PAPER (is_shadow=1) lean for a game — card-chip shaped.
    The one-bet-per-card PaperBadge source for live/final games where the
    live paper_picks have aged out of the slate (mirrors MLB's
    recorded_paper_pick backfill).

    ``date`` (optional, 'YYYY-MM-DD') scopes the match to picks recorded
    for that date. Without it, a stale pick from an unrelated past game
    can surface here if ESPN ever reuses/recycles a game_id across two
    different scheduled games (observed 2026-09-24 preseason vs. the
    2026-10-01 regular-season opener for the same two teams, same ESPN
    id) — Austin 2026-10-01 bugfix."""
    conn = _get_nhl_db()
    gid = str(game_id)
    # Hold the best RECORDED pick across ALL markets -- full-game headline
    # (nhl_picks, is_shadow=1) + first-period lean (nhl_p1_paper_picks) -- so the
    # card shows the SAME held pick the tracker does and stops flipping off a
    # high-edge P1 pick when its live edge collapses (Austin 2026-10-17).
    _c = "bet_type, pick, odds, edge, model_prob, result"
    if date:
        r = conn.execute(
            f"SELECT {_c} FROM nhl_picks "
            "WHERE game_id = ? AND date = ? AND COALESCE(is_shadow,0)=1 "
            "AND (result IS NULL OR result != 'V') AND ABS(edge) <= 20.0 "
            "UNION ALL "
            f"SELECT {_c} FROM nhl_p1_paper_picks "
            "WHERE game_id = ? AND date = ? "
            "AND (result IS NULL OR result != 'V') AND ABS(edge) <= 20.0 "
            "ORDER BY edge DESC LIMIT 1", (gid, date, gid, date)).fetchone()
    else:
        r = conn.execute(
            f"SELECT {_c} FROM nhl_picks "
            "WHERE game_id = ? AND COALESCE(is_shadow,0)=1 "
            "AND (result IS NULL OR result != 'V') AND ABS(edge) <= 20.0 "
            "UNION ALL "
            f"SELECT {_c} FROM nhl_p1_paper_picks "
            "WHERE game_id = ? "
            "AND (result IS NULL OR result != 'V') AND ABS(edge) <= 20.0 "
            "ORDER BY edge DESC LIMIT 1", (gid, gid)).fetchone()
    if not r:
        return None
    return {"type": r["bet_type"], "market": r["bet_type"], "pick": r["pick"],
            "odds": r["odds"], "edge": r["edge"], "prob": r["model_prob"],
            "result": r["result"], "paper": True}


# ── PAPER Pick-of-the-Day (calibrated shadow) ────────────────────────
# The single best calibrated-shadow lean of the day, locked once + tracked
# as its own W-L record — distinct from the REAL POTD (pick_of_day table).
# Feeds the NHL Bets-page paper hero (record) exactly like WNBA's paperRec.

def _ensure_paper_potd_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nhl_paper_pot_day (
            date        TEXT NOT NULL UNIQUE,
            game_id     TEXT,
            matchup     TEXT,
            bet_type    TEXT,
            pick        TEXT,
            model_prob  REAL,
            edge        REAL,
            odds        INTEGER,
            created_at  TEXT DEFAULT (datetime('now'))
        )
    """)


# ── started/finished + cross-market helpers (Bug 2, MintyBot 2026-10-12) ──
def _paper_potd_is_period(row: dict) -> bool:
    """A P1/period PAPER POTD: its result lives in nhl_p1_paper_picks, not the
    full-game nhl_picks table. Detected off the pick text ('P1 Under 1.5') or a
    period bet_type ('Period Total')."""
    pick = str(row.get("pick") or "")
    bt = str(row.get("bet_type") or "")
    return pick.startswith("P1 ") or "Period" in bt or bt.startswith("P1")


def _paper_potd_result(conn, d: str, row: dict) -> tuple:
    """(result, profit) for a locked paper POTD row, read from whichever shadow
    pool owns it — full-game picks from nhl_picks, P1/period picks from
    nhl_p1_paper_picks."""
    if _paper_potd_is_period(row):
        r = conn.execute(
            "SELECT result, profit FROM nhl_p1_paper_picks "
            "WHERE date = ? AND game_id = ? AND pick = ? LIMIT 1",
            (d, row["game_id"], row["pick"])).fetchone()
    else:
        r = conn.execute(
            "SELECT result, profit FROM nhl_picks WHERE COALESCE(is_shadow,0)=1 "
            "AND date = ? AND game_id = ? AND bet_type = ? AND pick = ? LIMIT 1",
            (d, row["game_id"], row["bet_type"], row["pick"])).fetchone()
    return (r["result"], r["profit"]) if r else (None, None)


def _finished_game_ids(conn, d: str) -> set:
    """game_ids whose game has already started/finished today = any shadow pick
    (full-game OR P1) that has SETTLED (result not null, not void). The full-game
    table settles reliably; the P1 table's result can lag on a final game, so a
    game counts as finished the moment EITHER pool settles. (A started-but-still-
    unsettled live game can't be detected from these tables alone — see note.)"""
    ids = set()
    for sql in (
        "SELECT DISTINCT game_id FROM nhl_picks WHERE COALESCE(is_shadow,0)=1 "
        "AND date = ? AND result IS NOT NULL AND result != 'V'",
        "SELECT DISTINCT game_id FROM nhl_p1_paper_picks "
        "WHERE date = ? AND result IS NOT NULL AND result != 'V'",
    ):
        try:
            for r in conn.execute(sql, (d,)).fetchall():
                ids.add(r["game_id"])
        except Exception:
            pass
    return ids


def record_paper_potd(date: str | None = None,
                      started_ids: set | None = None) -> dict | None:
    """Lock the day's PAPER POTD = the best calibrated-edge shadow lean of the
    day, taken ACROSS BOTH the full-game pool (nhl_picks is_shadow=1) AND the P1
    period pool (nhl_p1_paper_picks) — the same cross-market merge the 'Best'
    cards do, so the hero keeps the strongest lean even when it's a P1 Under, not
    only the full-game pick (Bug 2 pt 2, 2026-10-12). Both pools carry the same
    raw <=20pp-capped edge, so they rank apples-to-apples.

    Only PRE-GAME picks qualify: a game that has started/finished is excluded, and
    a lock whose game has since started is HEALED to the next-best still-upcoming
    lean so the POTD never points at a played/in-progress game (Bug 2 pt 1 — the
    FLA -1.5 'reverts to a FINISHED game' report). Idempotent per date while its
    game stays pre-game (never re-picks as the line moves).

    ``started_ids`` (optional) = game_ids the CALLER knows have already started or
    finished, read off the live slate's ESPN status. It's unioned with the DB-
    detected finished set so a game that's LIVE but not yet SETTLED (result still
    NULL) is excluded too. The best-bets compute passes it; the lightweight poll
    endpoint omits it and relies on DB-settled detection (a lock already held
    through go-live is maintained, not resurrected)."""
    conn = _get_nhl_db()
    _ensure_paper_potd_table(conn)
    d = date or et_today_str()
    blocked = _finished_game_ids(conn, d) | {g for g in (started_ids or set()) if g}

    # Cross-market candidate pool: pending (unsettled) shadow leans from BOTH pools,
    # excluding any game that's already started/finished.
    cands = []
    for sql in (
        "SELECT game_id, matchup, bet_type, pick, model_prob, edge, odds "
        "FROM nhl_picks WHERE COALESCE(is_shadow,0)=1 AND date = ? "
        "AND result IS NULL AND ABS(edge) <= 20.0",
        "SELECT game_id, matchup, bet_type, pick, model_prob, edge, odds "
        "FROM nhl_p1_paper_picks WHERE date = ? "
        "AND result IS NULL AND ABS(edge) <= 20.0",
    ):
        try:
            for r in conn.execute(sql, (d,)).fetchall():
                if r["game_id"] in blocked:
                    continue
                cands.append(r)
        except Exception:
            pass
    # ---- Calibration gate (Austin 2026-10-12) --------------------------
    # Was: best = max(cands, key=raw edge). That let an un-vetted P1 period
    # lean (passthrough calibrator, inflated raw edge) hijack the hero. Now:
    # rank by CALIBRATED edge, and only let an EXTRA market (period) headline
    # once it has a real track record. See engine.calibration_gate.
    from engine.calibration_gate import (cal_edge as _cg_edge, cal_prob as _cg_prob,
                                         extra_validated as _cg_ok, headline_eligible as _cg_he)

    def _extra_evidence() -> tuple:
        row = conn.execute(
            "SELECT COUNT(*) n, MIN(date) mn, MAX(date) mx FROM nhl_p1_paper_picks "
            "WHERE result IS NOT NULL AND result != 'V'").fetchone()
        n = (row["n"] if row else 0) or 0
        days = 0
        if row and row["mn"] and row["mx"]:
            try:
                days = int((conn.execute("SELECT julianday(?)-julianday(?)",
                            (row["mx"], row["mn"])).fetchone()[0]) or 0)
            except Exception:
                days = 0
        return n, days

    scored = []
    for r in cands:
        is_period = _paper_potd_is_period(dict(r))
        ce = _cg_edge("nhl", r["bet_type"], r["model_prob"], r["odds"],
                      pick_text=r["pick"], raw_edge=r["edge"])
        n = days = None
        if is_period:
            n, days = _extra_evidence()
        # headline_eligible enforces BOTH the +EV floor (cal edge >= 0 -- don't
        # headline a pick we'd be -EV placing) AND the extra-market track-record
        # bar for periods. Austin 2026-10-12 decision #2.
        eligible = _cg_he(ce, is_period, n, days)
        scored.append((eligible, ce, r))
    # Only eligible candidates can headline. If the whole slate is ineligible
    # extras (no trusted core pick), headline NOTHING rather than an un-vetted
    # lean -- exactly "don't let periods headline until they're up to par".
    pool = [s for s in scored if s[0]]
    chosen = (max(pool, key=lambda s: (s[1] if s[1] is not None else -999.0))
              if pool else None)
    best = chosen[2] if chosen else None
    best_cal_edge = (round(chosen[1], 2) if chosen and chosen[1] is not None else None)
    best_cal_prob = None
    if best is not None:
        best_cal_prob = _cg_prob("nhl", best["bet_type"], best["model_prob"],
                                 best["odds"], pick_text=best["pick"],
                                 raw_edge=best["edge"])

    existing = conn.execute(
        "SELECT game_id, bet_type, pick FROM nhl_paper_pot_day WHERE date = ?",
        (d,)).fetchone()
    if existing:
        # Heal a stale lock: drop it only if its game has started/finished AND a
        # still-upcoming lean is available to take over. A lock whose game is still
        # pre-game stays put (don't re-pick as the line moves).
        if existing["game_id"] in blocked and best is not None:
            conn.execute("DELETE FROM nhl_paper_pot_day WHERE date = ?", (d,))
            conn.commit()
        else:
            return get_paper_potd(d)

    if best is None:
        return get_paper_potd(d)
    conn.execute(
        "INSERT OR IGNORE INTO nhl_paper_pot_day "
        "(date, game_id, matchup, bet_type, pick, model_prob, edge, odds) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (d, best["game_id"], best["matchup"], best["bet_type"], best["pick"],
         best_cal_prob, best_cal_edge, best["odds"]))
    conn.commit()
    return get_paper_potd(d)


def get_paper_potd(date: str | None = None) -> dict | None:
    """Today's locked PAPER POTD, joined to its owning shadow row (full-game OR
    P1 period) for the live result/profit. Hero-ready (matches
    PaperPickOfDayHero's `locked` prop)."""
    conn = _get_nhl_db()
    _ensure_paper_potd_table(conn)
    d = date or et_today_str()
    r = conn.execute("SELECT * FROM nhl_paper_pot_day WHERE date = ?", (d,)).fetchone()
    if not r:
        return None
    row = dict(r)
    res, prof = _paper_potd_result(conn, d, row)
    return {"game_id": row["game_id"], "matchup": row["matchup"],
            "bet_type": row["bet_type"], "type": row["bet_type"],
            "pick": row["pick"], "odds": row["odds"], "edge": row["edge"],
            "model_prob": row["model_prob"], "prob": row["model_prob"],
            "result": res, "profit": prof, "paper": True}


def paper_potd_summary() -> dict:
    """Running W/L/profit across every locked PAPER POTD. Each day's POTD may be a
    full-game OR a P1 period pick, so the result is resolved per-row from the pool
    that owns it (nhl_picks / nhl_p1_paper_picks) rather than a single JOIN. Same
    shape as the real POTD summary so it drops straight into the hero `summary`."""
    conn = _get_nhl_db()
    _ensure_paper_potd_table(conn)
    pot_rows = conn.execute(
        "SELECT date, game_id, matchup, bet_type, pick FROM nhl_paper_pot_day"
    ).fetchall()
    results = [_paper_potd_result(conn, pr["date"], dict(pr)) for pr in pot_rows]
    w = sum(1 for (res, _p) in results if res == "W")
    l = sum(1 for (res, _p) in results if res == "L")
    pu = sum(1 for (res, _p) in results if res == "P")
    prof = sum((p or 0.0) for (res, p) in results if res is not None)
    pend = sum(1 for (res, _p) in results if res is None)
    dec = w + l
    return {"total": w + l + pu, "wins": w, "losses": l, "pushes": pu,
            "pending": pend, "profit": round(prof, 2),
            "win_pct": round(100.0 * w / dec, 1) if dec else 0.0,
            "roi": round(prof / dec, 1) if dec else 0.0}
