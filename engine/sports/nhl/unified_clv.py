"""Pick Authority CLV for NHL (MintyBot 2026-10-26) -- the NBA twin
(engine.sports.nba.unified_clv): picks_unified held closing odds for 0 of 115 NHL shadow
rows (Regulation 3-Way, Period 3-Way, Period Spread...). Snapshot Hard Rock's current price
at each pending row's own market + side + line while its game hasn't started; overwrites
every call, so the last pre-drop value is the close. |odds| outside 100..5000 = HR's locked
sentinels, ignored."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def price_for(o: dict, bet_type: str, pick: str, home_names, away_names) -> int | None:
    toks = (pick or "").split()
    bt = (bet_type or "").upper()
    if not toks:
        return None

    def side_of(tok):
        if tok.lower() in ("tie", "draw"):
            return "tie"
        if tok in home_names:
            return "home"
        if tok in away_names:
            return "away"
        return None

    def period_block(key, tok):
        """o[key] is keyed by period as "1" or "P1"; tok is the pick's "P1"."""
        blk = o.get(key) or {}
        want = tok.upper().lstrip("P")
        for k, v in blk.items():
            if str(k).upper().lstrip("P") == want and isinstance(v, dict):
                return v
        return {}

    def ou_price(blk, side_tok, line):
        if blk and _f(blk.get("line")) == line:
            return blk.get("over_odds") if side_tok == "over" else blk.get("under_odds")
        return None

    # Derivative markets the ratings shadow lane records (2026-10-26).
    if bt == "TEAM TOTAL" and len(toks) >= 3 and toks[1].lower() in ("over", "under"):
        s = side_of(toks[0])
        return ou_price(o.get(f"team_total_{s}") or {}, toks[1].lower(), _f(toks[2])) if s in ("home", "away") else None
    if bt == "PERIOD TOTAL" and len(toks) >= 3 and toks[1].lower() in ("over", "under"):
        return ou_price(period_block("period_totals", toks[0]), toks[1].lower(), _f(toks[2]))
    if bt == "PERIOD BTS" and len(toks) >= 3 and toks[1].upper() == "BTS":
        return period_block("period_bts", toks[0]).get("yes_odds" if toks[2].lower() == "yes" else "no_odds")
    if bt == "PERIOD DNB" and len(toks) >= 2:
        s = side_of(toks[1])
        return period_block("period_dnb", toks[0]).get(f"{s}_ml") if s in ("home", "away") else None
    if bt == "BTS" and len(toks) >= 2 and toks[0].upper() == "BTS":
        return (o.get("bts_full") or {}).get("yes_odds" if toks[1].lower() == "yes" else "no_odds")
    if bt == "OVERTIME" and len(toks) >= 2:
        return (o.get("overtime") or {}).get("yes_odds" if toks[1].lower() == "yes" else "no_odds")
    if bt == "TOTAL O/E" and len(toks) >= 2:
        return (o.get("total_oe") or {}).get("odd_odds" if toks[1].lower() == "odd" else "even_odds")
    if bt == "REGULATION 3-WAY" and toks[0].upper() == "REG" and len(toks) >= 2:
        s = side_of(toks[1])
        return (o.get("regulation_3way") or {}).get(f"{s}_ml") if s else None
    if bt == "PERIOD 3-WAY" and len(toks) >= 2:
        s = side_of(toks[1])
        return period_block("period_3way", toks[0]).get(f"{s}_ml") if s else None
    if bt == "PERIOD SPREAD" and len(toks) >= 3:
        s = side_of(toks[1])
        pt = _f(toks[2])
        blk = period_block("period_spread", toks[0])
        if s in ("home", "away") and pt is not None and _f(blk.get(f"{s}_point")) == pt:
            return blk.get(f"{s}_odds")
        return None
    if bt in ("ML", "MONEYLINE") and side_of(toks[0]) in ("home", "away"):
        return o.get(f"{side_of(toks[0])}_ml")
    if bt in ("O/U", "TOTAL", "ALT O/U") and toks[0].lower() in ("over", "under") and len(toks) >= 2:
        line, over = _f(toks[1]), toks[0].lower() == "over"
        if _f(o.get("over_under")) == line:
            return o.get("over_odds") if over else o.get("under_odds")
        for a in o.get("alt_totals") or []:
            if _f(a.get("line")) == line:
                return a.get("over_odds") if over else a.get("under_odds")
    # Puck lines, main + alt (MintyBot 2026-10-27, alt lines priced): pick "VGK -2.5"; an alt
    # rung's "point" is the HOME point, so the away side matches its negation.
    if bt in ("PL", "ALT PL") and len(toks) >= 2 and side_of(toks[0]) in ("home", "away"):
        s, pt = side_of(toks[0]), _f(toks[1])
        if pt is None:
            return None
        if _f(o.get(f"{s}_spread_point")) == pt:
            return o.get(f"{s}_spread_odds")
        for a in o.get("alt_spreads") or []:
            hp = _f(a.get("point"))
            if hp is not None and (hp if s == "home" else -hp) == pt:
                return a.get(f"{s}_odds")
    return None


def _pre_games() -> dict:
    """{espn_event_id: (home_abbr, away_abbr)} for NHL games not yet started (ESPN state
    'pre', today and tomorrow ET) -- the unified rows carry ESPN ids, nhl_games doesn't."""
    from datetime import timedelta
    from ...nhl_tracker._scoreboard import _fetch_nhl_scoreboard
    from ..._tz import et_now
    out = {}
    base = et_now()
    for k in (0, 1):
        d = (base + timedelta(days=k)).strftime("%Y-%m-%d")
        try:
            events = _fetch_nhl_scoreboard(d) or []
        except Exception:
            events = []
        for ev in events:
            comp = (ev.get("competitions") or [{}])[0]
            if ((comp.get("status") or {}).get("type") or {}).get("state", "pre") != "pre":
                continue
            h = a = None
            for c in comp.get("competitors") or []:
                ab = ((c.get("team") or {}).get("abbreviation") or "").upper()
                if c.get("homeAway") == "home":
                    h = ab
                else:
                    a = ab
            if ev.get("id") and h and a:
                out[str(ev["id"])] = (h, a)
    return out


def capture_unified_closing() -> int:
    try:
        from ...picks_unified._schema import get_conn as _u_conn
        from ...abbr import aliases_for
    except Exception:
        return 0
    u = _u_conn()
    pend = u.execute("SELECT id, game_key, bet_type, pick_text FROM picks "
                     "WHERE sport = 'nhl' AND result IS NULL").fetchall()
    if not pend:
        return 0
    games = _pre_games()
    rows = [r for r in pend if str(r[1]).rsplit(":", 1)[-1] in games]
    if not rows:
        return 0
    try:
        from scrapers.hardrock_odds import fetch_nhl
        odds = fetch_nhl() or {}
    except Exception as e:
        logger.debug("nhl unified closing: odds fetch failed: %s", e)
        return 0
    from ...tracker_core import _cc_now_iso
    stamp = _cc_now_iso()
    n = 0
    for rid, gk, bt, pt in rows:
        h, a = games[str(gk).rsplit(":", 1)[-1]]
        hn = aliases_for(h, sport="nhl") or [h]
        an = aliases_for(a, sport="nhl") or [a]
        o = next((odds.get(f"{x}@{y}") for x in an for y in hn if odds.get(f"{x}@{y}")), None)
        if not o:
            continue
        try:
            am = int(price_for(o, bt, pt, hn, an))
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
