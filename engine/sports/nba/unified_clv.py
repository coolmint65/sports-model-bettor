"""Pick Authority CLV for NBA (MintyBot 2026-10-26).

picks_unified held closing odds for 0 of 291 NBA shadow rows (the PA graduation lane):
the NBA tracker closes only its own real picks, so every market the shadow lane records --
alt spreads/totals, team totals, halves, quarters -- reached settlement with no close.
Same gap football had until engine.football._tracker.capture_unified_closing (10-25).

capture_unified_closing() snapshots Hard Rock's current price at each pending row's OWN
market + side + line (main or alt ladder; full game, 1H/2H, Q1, team totals) while its
game hasn't tipped; it overwrites every call, so the last pre-tip value is the close. A line
HR stops offering keeps its last captured value. Prices outside |100..5000| are HR's
locked-market sentinels and are ignored.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_NUM = re.compile(r"^[+-]?\d+(\.\d+)?$")


def _parse(bet_type: str, pick: str):
    """(scope, market, team|None, side|None, number|None) from a recorded NBA pick, or None.
    scope: FULL / H1 / H2 / Q1;  market: ML / SPREAD / TOTAL / TT (team total)."""
    bt = (bet_type or "").upper().replace(" ", "_")
    toks = (pick or "").split()
    scope = "FULL"
    if bt.startswith("H1") or "1H" in toks:
        scope = "H1"
    elif bt.startswith("H2") or "2H" in toks:
        scope = "H2"
    elif bt.startswith("Q1") or "Q1" in toks:
        scope = "Q1"
    toks = [t for t in toks if t not in ("1H", "2H", "Q1", "ML")]
    side = next((t.lower() for t in toks if t.lower() in ("over", "under")), None)
    num = next((float(t) for t in toks if _NUM.match(t)), None)
    team = next((t for t in toks if t.lower() not in ("over", "under") and not _NUM.match(t)), None)
    if "TEAM_TOTAL" in bt:
        market = "TT"
    elif "TOTAL" in bt and "O/E" not in bt and "_OE" not in bt:
        market = "TOTAL"
    elif "SPREAD" in bt:
        market = "SPREAD"
    elif bt.endswith("ML"):
        market = "ML"
    else:
        return None
    if market in ("TOTAL", "TT") and (side is None or num is None):
        return None
    if market == "SPREAD" and (team is None or num is None):
        return None
    if market in ("ML", "TT") and team is None:
        return None
    return scope, market, team, side, num


def _block(o: dict, scope: str) -> dict | None:
    if scope == "FULL":
        return o
    if scope in ("H1", "H2"):
        return (o.get("periods") or {}).get(scope)
    return {"home_ml": o.get("q1_home_ml"), "away_ml": o.get("q1_away_ml"),
            "home_spread_point": o.get("q1_spread"), "home_spread_odds": o.get("q1_spread_home_odds"),
            "away_spread_odds": o.get("q1_spread_away_odds"), "alt_spreads": o.get("q1_alt_spreads"),
            "over_under": o.get("q1_total"), "over_odds": o.get("q1_over_odds"),
            "under_odds": o.get("q1_under_odds"), "alt_totals": o.get("q1_alt_totals"),
            "team_total_home": o.get("q1_team_total_home"),
            "team_total_away": o.get("q1_team_total_away")}


def _eq(a, b) -> bool:
    try:
        return abs(float(a) - float(b)) < 1e-6
    except (TypeError, ValueError):
        return False


def price_for(o: dict, bet_type: str, pick: str, is_home) -> int | None:
    """HR's current price for this exact market/side/line, or None. ``is_home``: whether the
    pick's team is the home side (None for totals)."""
    p = _parse(bet_type, pick)
    if p is None:
        return None
    scope, market, team, side, num = p
    b = _block(o, scope)
    if not b:
        return None
    if market == "ML":
        return b.get("home_ml") if is_home else b.get("away_ml")
    if market == "SPREAD":
        hp = num if is_home else -num               # every ladder is keyed by the HOME point
        if _eq(b.get("home_spread_point"), hp):
            return b.get("home_spread_odds") if is_home else b.get("away_spread_odds")
        for a in b.get("alt_spreads") or []:
            if _eq(a.get("point"), hp):
                return a.get("home_odds") if is_home else a.get("away_odds")
        return None
    if market == "TOTAL":
        if _eq(b.get("over_under"), num):
            return b.get("over_odds") if side == "over" else b.get("under_odds")
        for a in b.get("alt_totals") or []:
            if _eq(a.get("line"), num):
                return a.get("over_odds") if side == "over" else a.get("under_odds")
        return None
    tt = b.get("team_total_home" if is_home else "team_total_away") or {}
    if _eq(tt.get("line"), num):
        return tt.get("over_odds") if side == "over" else tt.get("under_odds")
    return None


def capture_unified_closing() -> int:
    """Close pending NBA picks_unified rows (shadow lane + mirrored real) at their own line
    while the game hasn't tipped. Returns rows updated."""
    try:
        from ...picks_unified._schema import get_conn as _u_conn
        from .db import get_conn
        from ...abbr import aliases_for
    except Exception:
        return 0
    now = datetime.now(timezone.utc)
    games = {}
    for r in get_conn().execute(
            "SELECT g.game_id, g.start_time, ht.abbreviation AS h, at.abbreviation AS a "
            "FROM nba_games g JOIN nba_teams ht ON ht.id = g.home_team_id "
            "JOIN nba_teams at ON at.id = g.away_team_id "
            "WHERE COALESCE(g.status, 'scheduled') = 'scheduled' "
            "AND g.date BETWEEN date('now', '-1 day') AND date('now', '+2 day')"):
        try:
            tip = datetime.fromisoformat(str(r["start_time"]).replace("Z", "+00:00"))
            if tip.tzinfo is None:
                tip = tip.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if tip > now:
            games[str(r["game_id"])] = (r["h"], r["a"])
    if not games:
        return 0
    u = _u_conn()
    rows = [r for r in u.execute(
        "SELECT id, game_key, bet_type, pick_text FROM picks "
        "WHERE sport = 'nba' AND result IS NULL").fetchall()
        if str(r[1]).rsplit(":", 1)[-1] in games]
    if not rows:
        return 0
    try:
        from scrapers.nba_odds import fetch_all_nba_odds
        odds = fetch_all_nba_odds() or {}
    except Exception as e:
        logger.debug("nba unified closing: odds fetch failed: %s", e)
        return 0
    from ...tracker_core import _cc_now_iso
    stamp = _cc_now_iso()
    n = 0
    for rid, gk, bt, pt in rows:
        h, a = games[str(gk).rsplit(":", 1)[-1]]
        o = None
        for aa in aliases_for(a, sport="nba") or [a]:
            for hh in aliases_for(h, sport="nba") or [h]:
                o = odds.get(f"{aa}@{hh}")
                if o:
                    break
            if o:
                break
        if not o:
            continue
        first = (pt or "").split()[:1]
        team = first[0] if first else ""
        is_home = None
        if team and team.lower() not in ("over", "under"):
            if team in (aliases_for(h, sport="nba") or [h]):
                is_home = True
            elif team in (aliases_for(a, sport="nba") or [a]):
                is_home = False
            else:
                continue
        try:
            am = int(price_for(o, bt, pt, is_home))
        except (TypeError, ValueError):
            continue
        if abs(am) < 100 or abs(am) > 5000:
            continue
        u.execute("UPDATE picks SET closing_odds = ?, closing_captured_at = ? WHERE id = ?",
                  (am, stamp, rid))
        n += 1
    u.commit()
    return n


__all__ = ["capture_unified_closing", "price_for"]
