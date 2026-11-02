# -*- coding: utf-8 -*-
"""Point api_nba_tracker_unified at the CARD's recorded authority pick
(nba_paper_picks best_full/best_q1, max by edge*prob) instead of the
cal_edge/headline-gated re-derivation (_nba_merged_recorded_best).
Backup .bak-authrec-20261023 ; assert count==1 each ; py_compile."""
import shutil, py_compile

PATH = r"C:\sports-model-bettor\backend\server\routes_nba.py"
BAK  = PATH + ".bak-authrec-20261023"

s = open(PATH, "r", encoding="utf-8").read()  # universal newlines -> \n
shutil.copyfile(PATH, BAK)                     # raw-byte backup (keeps CRLF)

# ---------------------------------------------------------------- (A) helper
NEW_HELPER = '''def _nba_recorded_authority_best(game_id, date=None, conn=None):
    """The game's RECORDED AUTHORITY pick = EXACTLY what the GameCard headlined
    (api_nba_bets `best_pick`), read back from nba_paper_picks. The card headline
    is max(best_full, best_q1) by edge*prob, where best_full / best_q1 are the
    per-view authority picks (post select_pick / Pick-Authority gate) that the
    paper piggyback records as one Full-Game-scope row + one Q1-scope row per
    game. So: read the Full-scope authority row + the Q1-scope authority row
    (primary Q1, else the Q1 team-total / odd-even derivative the card falls back
    to), and return the max by edge*model_prob -- the SAME selection api_nba_bets
    runs. NO cal_edge / headline_eligible gate, NO derivative-pool merge, and NO
    edge guard: those re-derived a DIFFERENT pick than the card headlined (the
    bug this replaces). NEVER the real nba_picks (NBA active recording is off;
    the authority lives in the isolated nba_paper_picks). Chip-shaped + flat-$100
    profit + _scope, matching the old _nba_merged_recorded_best output so the
    tracker row shape is unchanged. Pass conn to share it across many games."""
    import sqlite3 as _sq
    from engine.nba_db import get_conn
    if conn is None:
        try:
            conn = get_conn(); conn.row_factory = _sq.Row
        except Exception:
            return None
    dq = " AND date = ?" if date else ""
    args = (str(game_id), date) if date else (str(game_id),)
    rows = []
    try:
        for r in conn.execute(
                "SELECT bet_type,pick,odds,edge,model_prob,result,closing_odds "
                "FROM nba_paper_picks WHERE game_id=?" + dq + " "
                "AND (result IS NULL OR result != 'V')", args):
            d = dict(r); d["_scope"] = _nba_market_scope(d.get("bet_type"), d.get("pick"))
            rows.append(d)
    except Exception:
        return None
    if not rows:
        return None
    def _epr(p):  # card ranking key: edge x prob (api_nba_bets `best`)
        return float(p.get("edge") or 0) * float(p.get("model_prob") or 0)
    full = [r for r in rows if r["_scope"] == "Full Game"]
    q1p  = [r for r in rows if r["_scope"] == "Q1"]
    q1d  = [r for r in rows if r["_scope"] == "Q1 Team Total"]
    cand_full = max(full, key=_epr) if full else None
    # Q1 view: primary Q1 if present, else the Q1 derivative fallback the card
    # uses when HR ships no primary Q1 market (api_nba_bets best_q1 fallback).
    cand_q1 = (max(q1p, key=_epr) if q1p else (max(q1d, key=_epr) if q1d else None))
    mains = [x for x in (cand_full, cand_q1) if x]
    if not mains:
        return None
    b = max(mains, key=_epr)  # == api_nba_bets: max(best_full, best_q1) by edge*prob
    return {"type": b["bet_type"], "market": b["bet_type"], "pick": b["pick"],
            "odds": b["odds"], "edge": b["edge"], "prob": b["model_prob"],
            "result": b["result"], "paper": True, "_scope": b["_scope"],
            "closing_odds": b.get("closing_odds"),
            "profit": _nba_flat_profit(b["result"], b["odds"]),
            "stake_units": 1.0}


'''
ANCHOR_A = ('@router.get("/api/nba/tracker/unified")\n'
            'def api_nba_tracker_unified(limit: int = 300):')
assert s.count(ANCHOR_A) == 1, f"A anchor count={s.count(ANCHOR_A)}"
s = s.replace(ANCHOR_A, NEW_HELPER + ANCHOR_A)

# ------------------------------------------------------------- (C) docstring
OLD_DOC = '''    """ONE NBA tracker = the card's 'Best' per game (the merged CALIBRATED best
    across all game markets: Full Game + Q1 + Q1 team-total/derivative). Mirrors
    the NHL tracker merge, adapted to NBA's Q1-centric, split real/paper tables
    (reads the PAPER stores nba_paper_picks + nba_derivative_picks; NEVER the
    real nba_picks). One row per game (the merged best), via
    _nba_merged_recorded_best; P/L graded FLAT $100. Read-only; per-market
    endpoints untouched."""'''
NEW_DOC = '''    """ONE NBA tracker = EXACTLY the card's headline `best_pick` per game, read
    back from the recorded authority rows in nba_paper_picks (the per-view
    best_full / best_q1 the card records post select_pick / Pick-Authority gate);
    one row per game = max(best_full, best_q1) by edge*prob, the SAME selection
    api_nba_bets runs. Pick Authority -- "one pick, read everywhere" (Austin
    2026-10-23): was a cal_edge/headline-gated re-derivation across
    nba_paper_picks + nba_derivative_picks (_nba_merged_recorded_best) that
    surfaced a DIFFERENT pick than the card. NEVER the real nba_picks (NBA active
    recording is off; the authority lives in the isolated nba_paper_picks). Via
    _nba_recorded_authority_best; P/L graded FLAT $100. Read-only; per-market
    endpoints untouched."""'''
assert s.count(OLD_DOC) == 1, f"C anchor count={s.count(OLD_DOC)}"
s = s.replace(OLD_DOC, NEW_DOC)

# ------------------------------------------------------------------ (B) body
OLD_BODY = '''    try:
        gids = [r[0] for r in conn.execute(
            "SELECT game_id, MAX(c) mc FROM ("
            "  SELECT game_id, MAX(created_at) c FROM nba_paper_picks GROUP BY game_id"
            "  UNION ALL SELECT game_id, MAX(created_at) c FROM nba_derivative_picks GROUP BY game_id"
            ") GROUP BY game_id ORDER BY mc DESC LIMIT ?", (limit,))]
    except Exception as e:
        logger.warning("nba unified tracker game list failed: %s", e)
        gids = []
    evm = _nba_extra_evidence_map(conn)
    rows = []
    for gid in gids:
        best = _nba_merged_recorded_best(gid, conn=conn, ev_map=evm)
        if not best:
            continue
        meta = (conn.execute("SELECT matchup,date FROM nba_paper_picks WHERE game_id=? LIMIT 1", (str(gid),)).fetchone()
                or conn.execute("SELECT matchup,date FROM nba_derivative_picks WHERE game_id=? LIMIT 1", (str(gid),)).fetchone())'''
NEW_BODY = '''    try:
        gids = [r[0] for r in conn.execute(
            "SELECT game_id, MAX(created_at) c FROM nba_paper_picks "
            "GROUP BY game_id ORDER BY c DESC LIMIT ?", (limit,))]
    except Exception as e:
        logger.warning("nba unified tracker game list failed: %s", e)
        gids = []
    rows = []
    for gid in gids:
        # Pick Authority: show EXACTLY the card's headline (best_pick), read back
        # from the recorded authority rows in nba_paper_picks -- NOT the old
        # cal_edge/headline-gated re-derivation across the paper+derivative pools
        # (_nba_merged_recorded_best), which surfaced a DIFFERENT pick than the
        # card headlined ("one pick, read everywhere", Austin 2026-10-23).
        best = _nba_recorded_authority_best(gid, conn=conn)
        if not best:
            continue
        meta = conn.execute("SELECT matchup,date FROM nba_paper_picks WHERE game_id=? LIMIT 1", (str(gid),)).fetchone()'''
assert s.count(OLD_BODY) == 1, f"B anchor count={s.count(OLD_BODY)}"
s = s.replace(OLD_BODY, NEW_BODY)

open(PATH, "w", encoding="utf-8", newline="\r\n").write(s)
py_compile.compile(PATH, doraise=True)
print("OK: patched + py_compile clean ->", PATH)
print("backup ->", BAK)
