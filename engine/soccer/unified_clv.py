"""Pick Authority CLV for soccer PA leagues (MintyBot 2026-10-28, Austin "finish up the closing
odds") -- the NBA / NHL twin (engine.sports.nhl.unified_clv). The EPL shadow lane mirrors every
market to picks_unified as is_shadow=1, but nothing closed those rows, so the graduation lanes
had no CLV read. Snapshot Hard Rock's current price at each pending row's own market + side +
line while its match hasn't kicked off (matches.start_time; HR keeps soccer matches in the feed
IN-PLAY, so the kickoff gate is what keeps a live price out). Overwrites every call, so the last
pre-kickoff read is the close. Prices come from the soccer tracker's own extractor
(engine.soccer._clv._extract_closing_for_pick), plus Goal In Both Halves, which only the shadow
lane records. |odds| outside 100..5000 = HR's locked sentinels, ignored."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _price(bet_type, pick, home_abbr, game_odds):
    from ._clv import _extract_closing_for_pick
    if (bet_type or "").upper() == "GIBH":
        g = game_odds.get("gibh") or {}
        return g.get("yes") if (pick or "").strip().lower().endswith("yes") else g.get("no")
    return _extract_closing_for_pick(bet_type, pick, home_abbr, game_odds)


def capture_unified_closing(league: str = "eng_premier") -> int:
    try:
        from ..picks_unified._schema import get_conn as _u_conn
        from ._db import get_conn
        from ._odds import fetch_league_odds
        from ..tracker_core import _cc_now_iso
    except Exception:
        return 0
    u = _u_conn()
    pend = u.execute("SELECT id, game_key, bet_type, pick_text FROM picks "
                     "WHERE sport = 'soccer' AND league = ? AND result IS NULL", (league,)).fetchall()
    if not pend:
        return 0
    conn = get_conn(league)
    now = datetime.now(timezone.utc)
    games = {}
    for _rid, gk, _bt, _pt in pend:
        mid = str(gk).rsplit(":", 1)[-1]
        if mid in games:
            continue
        row = conn.execute(
            "SELECT m.start_time, ht.abbreviation, at.abbreviation FROM matches m "
            "JOIN teams ht ON ht.id = m.home_team_id JOIN teams at ON at.id = m.away_team_id "
            "WHERE m.id = ? OR m.external_id = ?", (mid, mid)).fetchone()
        games[mid] = None
        if row and row[0]:
            try:
                ko = datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
                if ko.tzinfo is None:
                    ko = ko.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if now < ko:
                games[mid] = (row[1], row[2])
    rows = [r for r in pend if games.get(str(r[1]).rsplit(":", 1)[-1])]
    if not rows:
        return 0
    try:
        odds = fetch_league_odds(league) or {}
    except Exception as e:
        logger.debug("soccer unified closing: odds fetch failed: %s", e)
        return 0
    stamp = _cc_now_iso()
    n = 0
    for rid, gk, bt, pt in rows:
        h, a = games[str(gk).rsplit(":", 1)[-1]]
        o = odds.get(f"{a}@{h}")
        if not o:
            continue
        try:
            am = int(_price(bt, pt, h, o))
        except (TypeError, ValueError):
            continue
        if abs(am) < 100 or abs(am) > 5000:
            continue
        u.execute("UPDATE picks SET closing_odds = ?, closing_captured_at = ? WHERE id = ?",
                  (am, stamp, rid))
        n += 1
    u.commit()
    return n


__all__ = ["capture_unified_closing"]
