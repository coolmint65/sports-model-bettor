"""Kalshi value scanner - read-only VALIDATION display endpoint.

LOG-ONLY: surfaces engine.kalshi_value_scanner's log (our model's P(over X) vs
Kalshi's price) for NHL/NBA game totals. NOT a bet board - the model currently
runs under Kalshi's totals (one-directional bias), so this is a validation view
until the track record shows our number beats Kalshi's close. Austin 2026-10-07.

2026-10-10 NBA pivot: the "flagged" query used to filter preseason=1 out
entirely (NBA rows are always logged with preseason=1 -- see
engine.kalshi_value_scanner's _ADAPTERS), which in practice meant NBA
could never show up here regardless of edge/liquidity, no matter which
sport scan() covers. Austin's current read is the opposite of the old
assumption: NBA value is GOOD especially in preseason, so that gate is
dropped below -- flagged now surfaces both sports on equal footing.
"""
import logging
from fastapi import APIRouter

logger = logging.getLogger(__name__)
router = APIRouter()

_MIN_OI = 100.0    # open-interest floor
_MAX_SUM = 1.12    # yes_ask + no_ask ceiling (wider = illiquid/garbage quote)
_MIN_EDGE = 0.05   # 5pp min edge on the better side


@router.get("/api/kalshi-value")
def api_kalshi_value():
    """Latest-scan flagged edges (filtered) + the settled validation record."""
    try:
        from engine.kalshi_value_scanner import _log_conn
        conn = _log_conn()
        last = conn.execute("SELECT MAX(scan_ts) FROM kalshi_value_log").fetchone()
        last_scan = last[0] if last else None
        meta = {
            "last_scan_ts": last_scan,
            "pending": conn.execute("SELECT COUNT(*) FROM kalshi_value_log WHERE settled=0").fetchone()[0],
            "total_logged": conn.execute("SELECT COUNT(*) FROM kalshi_value_log").fetchone()[0],
        }
        flagged, by_game = [], {}
        if last_scan:
            rows = conn.execute(
                "SELECT sport,event_ticker,game_date,home_abbr,away_abbr,line,our_prob_over,"
                "our_mean_total,kalshi_yes_ask,kalshi_no_ask,edge_yes,edge_no,volume,open_interest "
                "FROM kalshi_value_log WHERE scan_ts=?", (last_scan,)).fetchall()
            for (sport, ev, gd, ha, aa, line, po, mt, ya, na, ey, en, vol, oi) in rows:
                oi = oi or 0.0
                if oi < _MIN_OI or ya is None or na is None or (ya + na) > _MAX_SUM:
                    continue
                ey = ey if ey is not None else -9.0
                en = en if en is not None else -9.0
                edge = ey if ey >= en else en
                if edge < _MIN_EDGE:
                    continue
                best = "Over" if ey >= en else "Under"
                flagged.append({
                    "sport": sport, "matchup": "%s@%s" % (aa, ha), "game_date": gd, "line": line,
                    "our_prob_over": po, "our_mean_total": mt, "kalshi_yes_ask": ya, "kalshi_no_ask": na,
                    "best_side": best, "edge": round(edge, 4), "open_interest": oi,
                    "volume": vol or 0.0, "event_ticker": ev})
                by_game.setdefault(ev, set()).add(best)
            for it in flagged:
                it["one_directional"] = len(by_game.get(it["event_ticker"], set())) <= 1
            flagged.sort(key=lambda z: -z["edge"])

        def _agg(rs):
            graded = [r for r in rs if r[1] in ("W", "L")]
            n = len(graded); w = sum(1 for r in graded if r[1] == "W")
            clvs = [r[2] for r in rs if r[2] is not None]
            return {"n": n, "wins": w,
                    "win_pct": round(100.0 * w / n, 1) if n else None,
                    "avg_clv": round(sum(clvs) / len(clvs), 4) if clvs else None,
                    "total_rows": len(rs)}
        srows = conn.execute("SELECT sport,result,clv FROM kalshi_value_log WHERE settled=1").fetchall()
        validation = {"overall": _agg(srows) if srows else None, "by_sport": {}}
        if srows:
            for sp in sorted(set(r[0] for r in srows)):
                validation["by_sport"][sp] = _agg([r for r in srows if r[0] == sp])
        conn.close()
        return {"meta": meta, "flagged": flagged, "validation": validation}
    except Exception as e:
        logger.error("kalshi-value endpoint failed: %s", e, exc_info=True)
        return {"error": str(e), "meta": {}, "flagged": [], "validation": {}}
