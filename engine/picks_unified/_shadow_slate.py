"""Sport-agnostic shadow-slate recorder for the Pick Authority (MintyBot 2026-10-18,
Austin "capture all markets"). Mirrors EVERY generated candidate market for a game into
picks_unified as is_shadow=1, so markets we do not HEADLINE still accumulate a real settled
record and can graduate. record_pick's INSERT OR IGNORE keeps the real headline real; this
only fills the markets we did not headline. Recorded once per (game, market) -- the first pass
that SEES the market -- so intra-day line moves do not count as separate observations, while a
market the book posts later in the day (NBA Q1 / halves go up near tip) still gets its row
(was once per GAME: the morning pass locked out every later-posted market, 2026-10-26).
Harmless if a derivative cannot settle yet -- the proven-edge gate only counts rows whose
result is W/L."""
from __future__ import annotations


def record_shadow_slate(sport, league, native_game_id, matchup, candidates, pick_date=None):
    if not candidates:
        return {"mirrored": 0}
    try:
        from ._legacy_bridge import mirror_to_unified, _game_key_for
        from ._schema import get_conn
    except Exception:
        return {"mirrored": 0}
    seen = set()
    try:
        gk = str(_game_key_for(sport, league, native_game_id))
        seen = {r[0] for r in get_conn().execute(
            "SELECT DISTINCT bet_type FROM picks WHERE game_key=? AND is_shadow=1", (gk,))}
    except Exception:
        pass
    try:
        from engine._tz import et_now
        pdate = pick_date or et_now().strftime("%Y-%m-%d")
    except Exception:
        from datetime import datetime as _dt
        pdate = pick_date or _dt.utcnow().strftime("%Y-%m-%d")
    n = 0
    for p in (candidates or []):
        bt = p.get("type") or p.get("bet_type") or ""
        pk = p.get("pick") or p.get("display") or ""
        if not bt or not pk or bt in seen:
            continue
        try:
            mirror_to_unified(
                sport=sport, league=league, native_game_id=native_game_id,
                pick_date=pdate, matchup=matchup or "",
                bet_type=bt, pick_text=pk,
                odds=int(p.get("odds") or 0),
                prob=float(p.get("prob") or p.get("model_prob") or 0.0),
                edge_pct=float(p.get("edge") or 0.0),
                stake_units=float(p.get("stake_units") or 0.0),
                is_shadow=True,
            )
            n += 1
        except Exception:
            pass
    return {"mirrored": n, "already": len(seen)}
