"""Football pick tracker + settler.

Records each emitted pick (one per market: ML / SPREAD / TOTAL) and
grades them once the game is final. Same shape as
``engine.hockey._tracker``.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

from ._db import get_conn

logger = logging.getLogger(__name__)


def _payout(odds: int, won: bool) -> float:
    if not won:
        return -100.0
    if odds > 0:
        return float(odds)
    if odds < 0:
        return round(100.0 * 100.0 / abs(odds), 2)
    return 0.0


def record_picks(league: str, game_id: str, picks: Iterable[dict]) -> dict:
    """Idempotent persist via the (game_id, bet_type, pick) unique
    index. Re-running on the same slate is a no-op.

    Also voids any prior pending pick for the same game_id whose
    (bet_type, pick) isn't in today's incoming set. Football frameworks
    re-fire daily for upcoming games — without this, the picker's best
    swap from TOTAL (Tuesday) to SPREAD (Thursday) leaves Tuesday's
    pick orphaned alongside Thursday's. User flagged UFL DC@ORL with
    TOTAL Under 46.5 + SPREAD DC +2.5 both pending for the same game.
    """
    from .._dedup_helpers import (
        enforce_one_per_game_per_family, is_settled_dup,
    )
    conn = get_conn(league)
    # Pick.date must match games.date (league-local calendar) so the
    # slate query buckets correctly under the kickoff day, not the
    # UTC date at insert time.
    g = conn.execute(
        "SELECT date, start_time FROM games WHERE game_id = ?",
        (str(game_id),),
    ).fetchone()
    pick_date = (g["date"] if g and g["date"]
                  else datetime.utcnow().strftime("%Y-%m-%d"))
    # Lock-at-game-start: once kickoff passes, the slate's prior pick
    # is the historical record — re-firing the picker on game day
    # shouldn't void a Tuesday TOTAL because Thursday's model now
    # ranks SPREAD higher. User flagged UFL DC@ORL: TOTAL Under 46.5
    # made 5/20 got auto-voided when a 5/22 (game day) re-fire picked
    # SPREAD instead. Skip the dedup + insert entirely once the
    # match has tipped.
    now_iso = datetime.utcnow().isoformat()
    start_iso = (g["start_time"] if g else None) or ""
    if start_iso and start_iso.replace("Z", "+00:00") <= now_iso + "+00:00":
        return {"recorded": 0, "duplicate": 0, "errors": 0,
                "blocked_settled": 0, "locked": 1}
    # FIRST PICK WINS (Austin 2026-11-01 "one pick, read everywhere"): the game's authority
    # headline is recorded ONCE, the first time it is chosen, at its entry price. Once the game
    # has ANY non-void real (is_shadow=0) row -- pending or settled -- write nothing: no void-and-
    # replace of a moved pick (enforce_one_per_game_per_family below used to void it and insert
    # the new one, and the tracker followed the latest swap), no second-family row, no in-place
    # update. closing-odds capture + settlement write their own columns and are unaffected. The
    # card + /tracker/unified both read routes_football._football_recorded_authority_pick (earliest).
    if conn.execute(
            "SELECT 1 FROM picks WHERE game_id=? AND COALESCE(is_shadow,0)=0 "
            "AND (result IS NULL OR result != 'V') LIMIT 1", (str(game_id),)).fetchone():
        return {"recorded": 0, "duplicate": 0, "errors": 0,
                "blocked_settled": 0, "held": 1}
    picks_list = list(picks or [])
    enforce_one_per_game_per_family(
        conn, table="picks", game_id=str(game_id), date=pick_date,
        picks=picks_list,
    )
    # 2026-09-07: reschedule/postponement supersede. ESPN assigns a NEW
    # game_id when a game is postponed to another day, so the game_id-
    # scoped dedup above cannot see the prior day's pending pick for the
    # SAME matchup. Void any still-pending pick on a DIFFERENT game_id for
    # this matchup within a 10-day window (WCU@CAM 2026-09-05 Under 68.5
    # orphaned when the game moved to 09-06 under a new id). The 10-day
    # window avoids collapsing a genuine later-season rematch.
    _matchup = next((p.get("matchup") for p in picks_list if p.get("matchup")), "")
    if _matchup:
        conn.execute(
            "UPDATE picks SET result='V', profit=0, settled_at=? "
            "WHERE result IS NULL AND matchup=? AND game_id!=? "
            "  AND date >= date(?, '-10 day')",
            (now_iso, _matchup, str(game_id), pick_date),
        )
        conn.commit()
    out = {"recorded": 0, "duplicate": 0, "errors": 0, "blocked_settled": 0}
    for p in picks_list:
        if out["recorded"]:
            break  # one authority pick per game (first pick wins)
        if float(p.get("edge") or 0.0) <= 0:
            # Never record a -EV pick. Guards the BAL/CLE + PIT/TB totals bug
            # (Austin 2026-10-17): an upstream path emitted an inconsistent
            # default (prob 0.5907 / edge -10.6%) that slipped past the gen gate.
            continue
        if is_settled_dup(conn, table="picks", game_id=str(game_id),
                            bet_type=p.get("type") or ""):
            out["blocked_settled"] += 1
            continue
        try:
            conn.execute(
                "INSERT INTO picks "
                "(date, game_id, matchup, bet_type, pick, "
                " model_prob, edge, odds, stake_units) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (pick_date, str(game_id), p.get("matchup") or "",
                  p.get("type"), p.get("pick"),
                  float(p.get("prob") or 0.0),
                  float(p.get("edge") or 0.0),
                  int(p.get("odds") or 0),
                  p.get("stake_units")),
            )
            out["recorded"] += 1
        except Exception as e:
            if "UNIQUE" in str(e).upper():
                out["duplicate"] += 1
            else:
                logger.warning("[football:%s] record pick failed: %s",
                                league, e)
                out["errors"] += 1
    conn.commit()

    # Phase-2 cutover: dual-write into picks_unified (canonical store).
    try:
        from ..picks_unified._legacy_bridge import mirror_to_unified
        for p in picks_list:
            mirror_to_unified(
                sport="football", league=league,
                native_game_id=game_id,
                pick_date=pick_date,
                matchup=p.get("matchup") or "",
                bet_type=p.get("type") or "",
                pick_text=p.get("pick") or "",
                odds=int(p.get("odds") or 0),
                prob=float(p.get("prob") or 0.0),
                edge_pct=float(p.get("edge") or 0.0),
                stake_units=float(p.get("stake_units") or 0.0),
            )
    except Exception as e:
        logger.debug("picks_unified mirror (football/%s) skipped: %s", league, e)

    return out


_PERIOD_RE = __import__("re").compile(r"^(H1|H2|Q1|Q2|Q3|Q4)_(.+)$")


def period_points(period: str, h_ls, a_ls) -> tuple[int, int] | None:
    """(home, away) points for a period from per-quarter linescores (JSON or list).
    Q1..Q4 = that quarter; H1 = Q1+Q2; H2 = Q3+Q4 PLUS overtime (books settle 2H incl. OT,
    Q4 excl. OT). None when the quarter data isn't there yet."""
    import json as _json
    def _as_list(v):
        if v is None:
            return None
        if isinstance(v, str):
            try:
                v = _json.loads(v)
            except Exception:
                return None
        try:
            return [int(x or 0) for x in v]
        except Exception:
            return None
    h, a = _as_list(h_ls), _as_list(a_ls)
    if not h or not a:
        return None
    need = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "H1": 2, "H2": 4}[period]
    if len(h) < need or len(a) < need:
        return None
    if period.startswith("Q"):
        i = int(period[1]) - 1
        return h[i], a[i]
    if period == "H1":
        return h[0] + h[1], a[0] + a[1]
    return sum(h[2:]), sum(a[2:])


def _resolve(bet_type: str, pick: str, h_score: int, a_score: int,
              h_abbr: str, a_abbr: str, odds: int,
              h_ls=None, a_ls=None) -> tuple[str | None, float | None]:
    """Return (W/L/P, profit) for one pick. None outcome when the
    pick text can't be parsed (or, for period markets, when the per-period
    points aren't stored yet — the row stays pending)."""
    bt = (bet_type or "").upper()
    # ── Period markets (H1/H2/Q1-Q4 _SPREAD/_TOTAL/_TEAM_TOTAL/_ML/_ML3), 2026-10-25 ──
    # Graded off games.*_linescore (ESPN per-quarter points). Before this the H1/Q1 rows
    # recorded since 10-16 could never settle, and the unified adapter mis-graded them
    # against the full-game score.
    _pm = _PERIOD_RE.match(bt)
    if _pm:
        pts = period_points(_pm.group(1), h_ls, a_ls)
        if pts is None:
            return None, None
        hp, ap = pts
        base = _pm.group(2)
        if base == "ML3":                      # 3-way: "NYJ (3way)" / "Tie (3way)" / "MIA (3way)"
            tok = (pick or "").split()[0] if pick else ""
            if tok.lower() == "tie":
                won = hp == ap
            elif tok == h_abbr:
                won = hp > ap
            elif tok == a_abbr:
                won = ap > hp
            else:
                return None, None
            return ("W" if won else "L"), _payout(odds, won)
        if base == "ML":                       # 2-way DNB: "NYJ ML" — tie = push
            tok = (pick or "").split()[0] if pick else ""
            return _resolve("ML", tok, hp, ap, h_abbr, a_abbr, odds)
        return _resolve(base, pick, hp, ap, h_abbr, a_abbr, odds, h_ls, a_ls)
    margin_h = h_score - a_score
    total = h_score + a_score
    # ── Game-script markets (2026-10-25) ──
    if bt == "BTTS":                           # "Yes" / "No" / "Yes 20+" / "No 20+": both teams >= N points
        toks = (pick or "").split()
        if not toks or toks[0].lower() not in ("yes", "no"):
            return None, None
        thr = 1
        if len(toks) > 1 and toks[1].rstrip("+").isdigit():
            thr = int(toks[1].rstrip("+"))
        both = h_score >= thr and a_score >= thr
        won = both if toks[0].lower() == "yes" else not both
        return ("W" if won else "L"), _payout(odds, won)
    if bt == "LEAD_EVERY_Q":                   # "TEAM Yes" / "TEAM No": strictly ahead after Q1..Q4 (OT ignored)
        toks = (pick or "").split()
        if len(toks) < 2 or toks[1].lower() not in ("yes", "no"):
            return None, None
        q = period_points("Q4", h_ls, a_ls)   # None unless 4 quarters are stored
        if q is None:
            return None, None
        import json as _json
        hl = _json.loads(h_ls) if isinstance(h_ls, str) else list(h_ls)
        al = _json.loads(a_ls) if isinstance(a_ls, str) else list(a_ls)
        ch = ca = 0; led = True
        for i in range(4):
            ch += int(hl[i] or 0); ca += int(al[i] or 0)
            if toks[0] == h_abbr:
                led = led and ch > ca
            elif toks[0] == a_abbr:
                led = led and ca > ch
            else:
                return None, None
        won = led if toks[1].lower() == "yes" else not led
        return ("W" if won else "L"), _payout(odds, won)
    if bt == "ML":
        if pick == h_abbr:
            return ("W" if margin_h > 0 else "L" if margin_h < 0 else "P",
                     _payout(odds, margin_h > 0))
        if pick == a_abbr:
            return ("W" if margin_h < 0 else "L" if margin_h > 0 else "P",
                     _payout(odds, margin_h < 0))
        return None, None
    if bt in ("SPREAD", "ALT_SPREAD"):  # ALT_SPREAD grades like SPREAD (line parsed from pick text) -- Austin 2026-10-17 CFB grading
        # Parse "TEAM +N" or "TEAM -N"
        parts = pick.split()
        if len(parts) < 2:
            return None, None
        team = parts[0]
        try:
            line = float(parts[1])
        except ValueError:
            return None, None
        if team == h_abbr:
            cover = margin_h + line
        elif team == a_abbr:
            cover = -margin_h + line
        else:
            return None, None
        if cover > 0:
            return "W", _payout(odds, True)
        if cover < 0:
            return "L", _payout(odds, False)
        return "P", 0.0
    if bt in ("TOTAL", "ALT_TOTAL"):  # ALT_TOTAL grades like TOTAL -- Austin 2026-10-17
        parts = pick.split()
        if len(parts) < 2:
            return None, None
        direction = parts[0].lower()
        try:
            line = float(parts[1])
        except ValueError:
            return None, None
        if direction == "over":
            if total > line:
                return "W", _payout(odds, True)
            if total < line:
                return "L", _payout(odds, False)
            return "P", 0.0
        if direction == "under":
            if total < line:
                return "W", _payout(odds, True)
            if total > line:
                return "L", _payout(odds, False)
            return "P", 0.0
    if bt == "TEAM_TOTAL":
        # "TEAM Over/Under N" -- grade vs that team's OWN score (Austin 2026-10-17)
        parts = pick.split()
        if len(parts) < 3:
            return None, None
        team = parts[0]
        direction = parts[1].lower()
        try:
            line = float(parts[2])
        except ValueError:
            return None, None
        if team == h_abbr:
            ts = h_score
        elif team == a_abbr:
            ts = a_score
        else:
            return None, None
        if direction == "over":
            return ("W", _payout(odds, True)) if ts > line else (("L", _payout(odds, False)) if ts < line else ("P", 0.0))
        if direction == "under":
            return ("W", _payout(odds, True)) if ts < line else (("L", _payout(odds, False)) if ts > line else ("P", 0.0))
        return None, None
    return None, None


def settle_picks(league: str) -> dict:
    """Walk pending picks. Grade any whose underlying game is final."""
    conn = get_conn(league)
    # Self-heal: auto-push picks pending >7 days. Same threshold the
    # MLB / NBA / NHL / soccer settlers use. Catches games whose ESPN
    # status never flipped to 'final' (the DAL@STL 2026-05-29 case)
    # before the slate-route backsweep was wired.
    from datetime import datetime as _dt2, timedelta as _td2
    stale_cutoff = (_dt2.now() - _td2(days=7)).strftime("%Y-%m-%d")
    auto_pushed = 0
    for row in conn.execute(
        "SELECT id, date, bet_type, pick FROM picks "
        "WHERE result IS NULL AND date < ?", (stale_cutoff,),
    ).fetchall():
        conn.execute(
            "UPDATE picks SET result='P', profit=0, "
            "  settled_at=? WHERE id=?",
            (_dt2.utcnow().isoformat(), row["id"]),
        )
        logger.warning(
            "[football:%s] settle: auto-pushed stale pending id=%s "
            "date=%s %s/%s — older than 7 days",
            league, row["id"], row["date"], row["bet_type"], row["pick"],
        )
        auto_pushed += 1
    if auto_pushed:
        conn.commit()
    pending = conn.execute(
        "SELECT p.id, p.game_id, p.bet_type, p.pick, p.line, p.odds, "
        "       g.home_score, g.away_score, g.status, "
        "       g.home_linescore, g.away_linescore, "
        "       ht.abbreviation AS h_abbr, at.abbreviation AS a_abbr "
        "FROM picks p "
        "LEFT JOIN games g ON g.game_id = p.game_id "
        "LEFT JOIN teams ht ON ht.id = g.home_team_id "
        "LEFT JOIN teams at ON at.id = g.away_team_id "
        "WHERE p.result IS NULL"
    ).fetchall()
    out = {"checked": len(pending), "settled": 0,
            "wins": 0, "losses": 0, "pushes": 0}
    for r in pending:
        _st = (r["status"] or "").lower()
        # 2026-09-07: postponed/canceled games get a fresh ESPN game_id on
        # reschedule, so the old id never flips to 'final' and its pick would
        # sit pending until the 7-day stale-push. Void these terminal-but-not-
        # played statuses now (functionally a refund; hidden from history).
        if _st in ("postponed", "canceled", "cancelled", "suspended"):
            conn.execute(
                "UPDATE picks SET result='V', profit=0, settled_at=? WHERE id=?",
                (datetime.utcnow().isoformat(), r["id"]),
            )
            out["voided"] = out.get("voided", 0) + 1
            continue
        if _st != "final":
            continue
        if r["home_score"] is None or r["away_score"] is None:
            continue
        _pick = r["pick"]
        _bt = (r["bet_type"] or "").upper()
        # DB stores spread/total pick as side-only ("MIN" / "Over") with the
        # number in a separate `line` column, but _resolve parses "TEAM +N" /
        # "Over N" out of the pick string. Recombine here so spread/total grade
        # instead of sitting PENDING forever — the settle SELECT omitted p.line,
        # the same line-drop as the display path (Austin 2026-09-21). ML needs
        # no line.
        if _bt in ("SPREAD", "TOTAL") and r["line"] is not None \
                and len(str(_pick or "").split()) < 2:
            _ln = r["line"]
            _sign = "+" if (_bt == "SPREAD" and _ln > 0) else ""
            _pick = "%s %s%s" % (_pick, _sign, _ln)
        verdict, profit = _resolve(
            r["bet_type"], _pick,
            int(r["home_score"]), int(r["away_score"]),
            r["h_abbr"] or "", r["a_abbr"] or "",
            int(r["odds"] or 0),
            r["home_linescore"], r["away_linescore"],
        )
        if verdict is None:
            continue
        conn.execute(
            "UPDATE picks SET result = ?, profit = ?, "
            "  settled_at = ? WHERE id = ?",
            (verdict, profit, datetime.utcnow().isoformat(), r["id"]),
        )
        out["settled"] += 1
        if verdict == "W":
            out["wins"] += 1
        elif verdict == "L":
            out["losses"] += 1
        else:
            out["pushes"] += 1
    conn.commit()
    if out["settled"]:
        logger.info("[football:%s] settled %d (W=%d L=%d P=%d)",
                    league, out["settled"], out["wins"], out["losses"],
                    out["pushes"])
    return out


def capture_closing_odds(league: str) -> int:
    """2026-09-04 (Austin: CFB CLV was blank): snapshot HR's CURRENT American odds for each of
    today's still-SCHEDULED, ungraded picks into picks.closing_odds. Overwrites every call while
    the game is pre-kickoff, so the last value before status flips live/final == the closing line.
    The frontend (PicksTable.clvFor) computes CLV% = impliedProb(closing_odds) - impliedProb(odds),
    so populating closing_odds is all the CLV column needs. Uses the cached HR odds fetch (the
    /today route already fetched them this request) — no extra HR hit."""
    from ._odds import fetch_league_odds
    from ..tracker_core import _cc_now_iso
    conn = get_conn(league)
    _has_ts = any(d[1] == "closing_odds_captured_at"
                  for d in conn.execute("PRAGMA table_info(picks)"))
    rows = conn.execute(
        "SELECT p.id, p.bet_type, p.pick, ht.abbreviation AS h, at.abbreviation AS a "
        "FROM picks p JOIN games g ON g.game_id = p.game_id "
        "LEFT JOIN teams ht ON ht.id = g.home_team_id "
        "LEFT JOIN teams at ON at.id = g.away_team_id "
        "WHERE p.result IS NULL AND g.status = 'scheduled'"
    ).fetchall()
    if not rows:
        return 0
    try:
        odds = fetch_league_odds(league)     # cached; no forced HR hit
    except Exception:
        return 0
    n = 0
    for r in rows:
        o = odds.get(f"{r['a']}@{r['h']}")
        if not o:
            continue
        bt = (r["bet_type"] or "").upper()
        pick = (r["pick"] or "").strip()
        am = None
        if bt == "ML":
            am = o.get("home_ml") if pick == r["h"] else o.get("away_ml") if pick == r["a"] else None
        elif bt == "SPREAD":
            tok = pick.split()[0] if pick else ""
            am = o.get("home_spread_odds") if tok == r["h"] else o.get("away_spread_odds") if tok == r["a"] else None
        elif bt == "TOTAL":
            pl = pick.lower()
            am = o.get("over_odds") if pl.startswith("over") else o.get("under_odds") if pl.startswith("under") else None
        if am is not None:
            try:
                am_int = int(am)
            except (TypeError, ValueError):
                continue
            # Reject HR market-locked sentinels (+/-100000 etc); real
            # football American odds stay within +/-5000 (matches the
            # canonical tracker_core gate so a locked placeholder can
            # not corrupt CLV).
            if abs(am_int) < 100 or abs(am_int) > 5000:
                continue
            if _has_ts:
                conn.execute(
                    "UPDATE picks SET closing_odds = ?, "
                    "closing_odds_captured_at = ? WHERE id = ?",
                    (am_int, _cc_now_iso(), r["id"]))
            else:
                conn.execute(
                    "UPDATE picks SET closing_odds = ? WHERE id = ?",
                    (am_int, r["id"]))
            n += 1
    conn.commit()
    return n


def _same_line_odds(bet_type, pick, line, o, h_abbr, a_abbr):
    """HR's CURRENT American odds for exactly this pick -- same market, side and LINE -- off
    the parsed board (engine.football._odds: main lines, alt ladders, period blocks, BTTS,
    lead-every-quarter). None when HR no longer offers that line."""
    bt = (bet_type or "").upper()
    toks = (pick or "").strip().split()
    src = o
    pm = _PERIOD_RE.match(bt)
    if pm:
        src = o.get(pm.group(1).lower()) or {}
        bt = pm.group(2)

    def _f(v):
        try:
            return float(str(v).rstrip("+"))
        except (TypeError, ValueError):
            return None
    L = _f(line) if line is not None else (_f(toks[-1]) if toks else None)
    tok0 = toks[0] if toks else ""
    side = toks[1].lower() if len(toks) > 1 else ""

    def _ladder(rows, want):
        for ln, a, b in rows or []:
            if L is not None and abs(float(ln) - L) < 1e-6:
                return a if want == 0 else b
        return None
    if bt == "ML":
        if pm:
            ml2 = src.get("ml2")
            return (ml2[0] if tok0 == h_abbr else ml2[1] if tok0 == a_abbr else None) if ml2 else None
        return o.get("home_ml") if tok0 == h_abbr else o.get("away_ml") if tok0 == a_abbr else None
    if bt == "ML3":
        ml3 = src.get("ml3")
        if not ml3:
            return None
        return ml3[0] if tok0 == h_abbr else ml3[2] if tok0 == a_abbr else ml3[1] if tok0.lower() == "tie" else None
    if bt in ("SPREAD", "ALT_SPREAD"):
        ladder = list(src.get("spreads") or []) if pm else list(o.get("alt_spreads") or [])
        if not pm and o.get("home_spread_point") is not None:
            ladder.append((o.get("home_spread_point"), o.get("home_spread_odds"), o.get("away_spread_odds")))
        for hp, ho, ao in ladder:
            if L is None or hp is None:
                continue
            if tok0 == h_abbr and abs(float(hp) - L) < 1e-6:
                return ho
            if tok0 == a_abbr and abs(-float(hp) - L) < 1e-6:
                return ao
        return None
    if bt in ("TOTAL", "ALT_TOTAL"):
        ladder = list(src.get("totals") or []) if pm else list(o.get("alt_totals") or [])
        if not pm and o.get("over_under") is not None:
            ladder.append((o.get("over_under"), o.get("over_odds"), o.get("under_odds")))
        d = tok0.lower()
        return _ladder(ladder, 0 if d == "over" else 1) if d in ("over", "under") else None
    if bt == "TEAM_TOTAL":
        rows = src.get("home_team_totals") if tok0 == h_abbr else src.get("away_team_totals") if tok0 == a_abbr else None
        return _ladder(rows, 0 if side == "over" else 1) if side in ("over", "under") else None
    if bt == "BTTS":
        if len(toks) > 1 and not pm:
            n = toks[1].rstrip("+")
            yn = dict(o.get("btts_x") or []).get(int(n)) if n.isdigit() else None
        else:
            yn = src.get("btts")
        if not yn:
            return None
        return yn[0] if tok0.lower() == "yes" else yn[1] if tok0.lower() == "no" else None
    if bt == "LEAD_EVERY_Q":
        yn = o.get("leq_home") if tok0 == h_abbr else o.get("leq_away") if tok0 == a_abbr else None
        if not yn:
            return None
        return yn[0] if side == "yes" else yn[1] if side == "no" else None
    return None


def capture_unified_closing(league: str) -> int:
    """Pick Authority CLV for football (MintyBot 2026-10-25, Austin PA CFB walkthrough "fix it
    all up"): the unified store held closing odds for 0 of 670 settled CFB rows -- football only
    captured closes into its own tracker table (above), and only at the MAIN line, so a spread
    or total whose line moved compared odds at two different lines. Snapshot HR's current odds
    at each pending unified row's OWN line (alt ladders + periods + game-script included) while
    its game is still scheduled; overwrites every call, so the last pre-kickoff value is the
    close. A line HR stops offering keeps its last captured value. Uses the cached HR fetch."""
    from ._odds import fetch_league_odds
    from ..tracker_core import _cc_now_iso
    try:
        from ..picks_unified._schema import get_conn as _u_conn
    except Exception:
        return 0
    conn = get_conn(league)
    sched = {str(r["game_id"]): (r["h"], r["a"]) for r in conn.execute(
        "SELECT g.game_id, ht.abbreviation AS h, at.abbreviation AS a FROM games g "
        "LEFT JOIN teams ht ON ht.id = g.home_team_id "
        "LEFT JOIN teams at ON at.id = g.away_team_id "
        "WHERE g.status = 'scheduled'")}
    if not sched:
        return 0
    u = _u_conn()
    rows = [r for r in u.execute(
        "SELECT id, game_key, bet_type, pick_text, line FROM picks "
        "WHERE sport = 'football' AND league = ? AND result IS NULL", (league,)).fetchall()
        if str(r[1]).rsplit(":", 1)[-1] in sched]
    if not rows:
        return 0
    try:
        odds = fetch_league_odds(league)     # cached; no forced HR hit
    except Exception:
        return 0
    now = _cc_now_iso()
    n = 0
    for rid, gk, bt, pt, ln in rows:
        h, a = sched[str(gk).rsplit(":", 1)[-1]]
        o = odds.get(f"{a}@{h}")
        if not o:
            continue
        try:
            am = int(_same_line_odds(bt, pt, ln, o, h, a))
        except (TypeError, ValueError):
            continue
        if abs(am) < 100 or abs(am) > 5000:    # HR market-locked sentinels (same gate as above)
            continue
        u.execute("UPDATE picks SET closing_odds = ?, closing_captured_at = ? WHERE id = ?",
                  (am, now, rid))
        n += 1
    u.commit()
    return n


def list_history(league: str, limit: int = 200, shadow: int | None = None) -> list[dict]:
    """Pending + settled picks, most-recent first. shadow=0 real / 1 paper / None all."""
    conn = get_conn(league)
    _sh = "" if shadow is None else ("AND COALESCE(p.is_shadow,0) = %d " % int(shadow))
    rows = conn.execute(
        "SELECT p.id, p.date, p.game_id, p.bet_type, p.pick, p.line, "
        "       p.model_prob, p.edge, p.odds, p.result, p.profit, "
        "       p.stake_units, "
        "       p.closing_odds, p.created_at, p.settled_at, "
        "       p.matchup, "
        "       g.home_score, g.away_score, g.status, "
        "       ht.abbreviation AS h_abbr, at.abbreviation AS a_abbr, "
        "       ht.logo_url AS home_logo, at.logo_url AS away_logo "
        "FROM picks p "
        "LEFT JOIN games g ON g.game_id = p.game_id "
        "LEFT JOIN teams ht ON ht.id = g.home_team_id "
        "LEFT JOIN teams at ON at.id = g.away_team_id "
        "WHERE (p.result IS NULL OR p.result != 'V') "  # Austin 2026-09-05: hide superseded/void picks from the records (one-best-bet-per-game re-picks void the prior; those clutter history)
        + _sh +
        "ORDER BY p.created_at DESC LIMIT ?",
        (int(limit),),
    ).fetchall()
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        # Fallback for legacy rows where matchup wasn't recorded
        # (picks_core score_pick dropped the field before the fix).
        if not d.get("matchup") and d.get("h_abbr") and d.get("a_abbr"):
            d["matchup"] = f"{d['a_abbr']} @ {d['h_abbr']}"
        out.append(d)
    return out


# ── Unified paper (is_shadow=1) reads (Stage 1+3 consolidation 2026-09-20) ──
# Replace engine.football._paper_shadow — paper picks now live as is_shadow=1
# rows in the shared `picks` table, graded by the real settle_picks.
def paper_summary(league: str) -> dict:
    """Paper (is_shadow=1) P/L — raw 1u-basis profit (paper is unstaked)."""
    conn = get_conn(league)
    # One best lean per game (max POSITIVE edge under the |edge|<=15 ceiling — the
    # SAME pick the card + POTD show), so the count + P/L reflect what's displayed
    # instead of triple-counting ML+spread+total per game (Austin 2026-09-21).
    best = conn.execute(
        "SELECT result, profit FROM ("
        "  SELECT result, profit, "
        "    ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY edge DESC) AS rn "
        "  FROM picks "
        "  WHERE is_shadow=1 AND (result IS NULL OR result != 'V') "
        "    AND ABS(edge) <= 15.0"
        ") WHERE rn = 1").fetchall()
    wv = sum(1 for r in best if r["result"] == "W")
    lv = sum(1 for r in best if r["result"] == "L")
    pu = sum(1 for r in best if r["result"] == "P")
    prof = sum((r["profit"] or 0.0) for r in best if r["result"] is not None)
    dec = wv + lv
    pending = sum(1 for r in best if r["result"] is None)
    return {"wins": wv, "losses": lv, "pushes": pu, "total": wv + lv + pu,
            "profit": round(prof, 2),
            "win_pct": round(100.0 * wv / dec, 1) if dec else 0.0,
            "roi": round(prof / dec, 1) if dec else 0.0,
            "pending": pending}


def recorded_paper_pick(league: str, game_id):
    """Recorded paper (is_shadow=1) pick for a game, card-shaped.

    Tie-break is the FIRST-LOCKED lean (lowest created_at), not the
    highest edge (Austin 2026-10-10): with the one-pending-row-per-game DB
    constraint now in place this only matters for already-settled/void
    history, but it also keeps this read in sync with list_history's
    tracker view, which has always surfaced rows in created_at order — the
    two disagreeing (max-edge here vs. earliest-created there) on the same
    underlying duplicate rows was the card-vs-tracker pick mismatch."""
    conn = get_conn(league)
    r = conn.execute(
        "SELECT bet_type, pick, line, odds, edge, model_prob, result "
        "FROM picks WHERE game_id = ? AND is_shadow=1 AND (result IS NULL OR result != 'V') "
        "AND ABS(edge) <= 20.0 "  # ceiling — aligned to the cards’ 20pp ceiling (Austin 2026-10-03)
        "ORDER BY created_at ASC LIMIT 1", (str(game_id),)).fetchone()
    if not r:
        return None
    return {"type": r["bet_type"], "market": r["bet_type"], "pick": r["pick"],
            "line": r["line"], "odds": r["odds"], "edge": r["edge"],
            "prob": r["model_prob"], "result": r["result"], "paper": True}


# ── POTD selection + lock, now targeting the shared pick_of_day table ──────
# (Stage 3 consolidation 2026-09-20 — replaces engine.football._paper_shadow's
# POTD fns; the record/settle/summary now go through engine.pick_of_day.)
_POTD_CEILING = 20.0  # aligned to the cards' 20pp ceiling so POTD == card (Austin 2026-10-03)
_PREGAME_STATUSES = ("scheduled", "pre_game", "pregame")

# New derivative market types (Austin 2026-10-13 market expansion). They can be
# PRICED + tracked immediately, but must not HEADLINE the POTD until they've
# earned a track record -- same extra-market bar the other sports use.
_EXTRA_DERIV_TYPES = {"ALT_SPREAD", "ALT_TOTAL", "TEAM_TOTAL"}


def _extra_ok(league, bet_type):
    """True once a derivative market type has enough settled football history
    to be allowed to headline. Fails closed (unvalidated) on any error."""
    try:
        from engine.calibration_gate import extra_validated
        from ._db import get_conn
        c = get_conn(league)
        r = c.execute(
            "SELECT COUNT(*) n, MIN(date) mn, MAX(date) mx FROM picks "
            "WHERE bet_type=? AND result IS NOT NULL AND result NOT IN ('V','')",
            (bet_type,)).fetchone()
        n = (r[0] if r else 0) or 0
        days = 0
        if r and r[1] and r[2]:
            days = int((c.execute("SELECT julianday(?)-julianday(?)", (r[2], r[1])).fetchone()[0]) or 0)
        return extra_validated(n, days)
    except Exception:
        return False


def _potd_market_proven(league: str, bet_type: str, pick_text: str = "", odds=None) -> bool:
    """True if (football, league, bet_type, condition) clears the proven-edge gate -- identical
    to the cards' select_pick gate. `condition` (fav/dog/over/under) is derived from pick_text/
    odds the same way select_pick does it. Fail-CLOSED (never headline) on any error. (League-
    segmented 2026-10-22 so nfl/cfb/ufl each earn their own eligibility; condition-segmented
    2026-10-22 so each market's fav/dog or over/under side earns its own eligibility.)"""
    try:
        from engine.selection import edge_proven
        cond = edge_proven._condition(pick_text, bet_type, odds)
        return bool(edge_proven.is_eligible("football", league, bet_type, cond))
    except Exception:
        return False


def _pick_potd(games, league=None):
    """The day's POTD = highest-edge paper pick (<= ceiling) across the slate.

    Candidates use the edge/prob ALREADY on the paper pick -- the same number the
    card shows for that game. score_pick already ran this through
    empirical_calibration before paper_picks ever reach this function (football_nfl/
    cfb/ufl calibration deployed 2026-10-05), so re-running empirical_calibration.
    calibrate() here a SECOND time double-calibrates an already-calibrated prob: it
    collapsed every CFB ALT_SPREAD candidate toward the same ~0.558 empirical-cell
    prob regardless of the real per-game edge, producing near-zero/negative
    recalibrated edges that the edge>0 filter below then dropped -- POTD empty (or
    picking on calibration-collapse noise) while the card still showed real 7-11pp
    edges. Recal call removed 2026-10-22 (MintyBot) -- same bug class + same fix as
    NHL's 2026-10-18 removal from _POTD_EMPIRICAL_SPORTS in
    engine/pick_of_day/_select.py ("NHL best_pick now carries the market-anchored/
    recorded edge, not the raw model prob, so re-running cal_prob here DOUBLE-
    calibrates it"). Only genuinely +EV (0 < edge <= ceiling) picks qualify.
    """
    try:
        from engine.config import NHL_JUICE_WALL as _JW
    except Exception:
        _JW = -200
    best = None; bg = None
    for g in (games or []):
        if (g.get("status") or "").lower() not in _PREGAME_STATUSES:
            continue
        for pk in (g.get("paper_picks") or []):
            _od = pk.get("odds")
            if _od is not None and float(_od) < _JW:
                continue  # juice wall: skip picks juiced past -200 (Austin 2026-10-07)
            _bt = (pk.get("type") or "").upper()
            from ._segments import edge_league
            _ep = edge_league(league, g.get("home_team_id"), g.get("away_team_id"))  # FCS-only CFB -> "cfb_fcs"
            if not _potd_market_proven(_ep, _bt, pk.get("pick"), _od):
                continue  # Pick Authority (Austin 2026-10-18 "all POTDs gated"): only PROVEN markets headline -- same edge_proven gate the cards use. Unproven (football ML/SPREAD/TOTAL) tracks as paper, never headlines.
            e = pk.get("edge")
            if e is None or e <= 0 or e > _POTD_CEILING:
                continue
            if best is None or e > (best.get("edge") or -99):
                best = pk; bg = g
    if best is None:
        return None
    return {**best, "game_id": bg.get("game_id"),
            "matchup": "%s @ %s" % (bg.get("away_abbr") or "", bg.get("home_abbr") or "")}


def record_potd(league, slate_date, potd):
    """Lock the day's POTD into the shared pick_of_day table.

    Re-syncs the still-pending row to the current best PRE-GAME pick on every
    write pass UNTIL the picked game kicks off, then freezes it (Austin
    2026-10-03). Was INSERT OR IGNORE, which froze the FIRST pick of the day and
    let the hero drift stale from the card as the line moved (POTD showed PHI
    +4.5 spread while the card had already flipped to PHI +160 ML)."""
    from datetime import datetime as _dt
    from engine.pick_of_day._storage import _ensure_potd_table
    _ensure_potd_table(league)
    conn = get_conn(league)
    now_iso = _dt.utcnow().isoformat()
    if not potd:
        # NO PICK (the proven-edge gate cleared nothing). Clear a still-PENDING pre-game
        # lock so the hero shows NO PICK instead of a stale pick the gate now rejects;
        # keep settled or already-started/locked picks (committed). (Pick Authority 2026-10-18.)
        _r = conn.execute("SELECT game_id, result FROM pick_of_day WHERE date=?", (slate_date,)).fetchone()
        if _r is not None and _r["result"] is None:
            _sgid = _r["game_id"]; _started = False
            if _sgid:
                _g = conn.execute("SELECT status, start_time, date FROM games WHERE game_id=?", (str(_sgid),)).fetchone()
                if _g is not None:
                    _st = (_g["status"] or "").lower()
                    _started = bool(_st and _st not in _PREGAME_STATUSES)
                    if not _started and _g["start_time"]:
                        try:
                            from engine.lock_schedule import is_locked as _is_locked
                            _started = _is_locked(_dt.fromisoformat(str(_g["start_time"]).replace("Z", "+00:00")))
                        except Exception:
                            _started = bool(_g["date"] and str(_g["date"]) <= str(slate_date))
            if not _started:
                conn.execute("DELETE FROM pick_of_day WHERE date=? AND result IS NULL", (slate_date,))
                conn.commit()
        return
    cols = (str(potd.get("game_id") or ""), potd.get("matchup") or "",
            potd.get("type"), potd.get("pick"),
            (float(potd["line"]) if potd.get("line") is not None else None),
            potd.get("prob"), potd.get("edge"), int(potd.get("odds") or 0))
    row = conn.execute(
        "SELECT game_id, result FROM pick_of_day WHERE date=?", (slate_date,)).fetchone()
    if row is None:
        conn.execute(
            "INSERT OR IGNORE INTO pick_of_day (date, game_id, matchup, bet_type, pick, "
            "line, model_prob, edge, odds, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (slate_date, *cols, now_iso))
        conn.commit()
        return
    # Existing row: freeze once settled, or once the game it points at has kicked
    # off. Otherwise re-sync it to the current best pre-game pick so the hero
    # tracks the card instead of staying stale on the first pick of the day.
    if row["result"] is not None:
        return
    stored_gid = row["game_id"]
    if stored_gid:
        g = conn.execute(
            "SELECT status, start_time, date FROM games WHERE game_id=?",
            (str(stored_gid),)).fetchone()
        if g is not None:
            status = (g["status"] or "").lower()
            start_iso = (g["start_time"] or "")
            gday = (g["date"] or "")
            # LOCK ON GAME DAY (Austin 2026-10-03): a pick of the day must not
            # shuffle once its game's day arrives. Freeze when the stored game's
            # date has reached the slate (POTD) date, OR it is already live/final,
            # OR kickoff passed. Pre-game-day (future) it keeps re-syncing.
            # Time-based lock via engine.lock_schedule (Austin 2026-10-14):
            # 7am day / 4:30pm night / prior-9pm overnight ET off the game
            # start_time -- replaces "lock on game day" so a late game is not
            # frozen at midnight and its odds mature first. Live/final = locked.
            started = bool(status and status not in _PREGAME_STATUSES)
            if not started and start_iso:
                try:
                    from engine.lock_schedule import is_locked as _is_locked
                    _gs = _dt.fromisoformat(start_iso.replace("Z", "+00:00"))
                    started = _is_locked(_gs)
                except Exception:
                    started = bool(gday and str(gday) <= str(slate_date))
            if started:
                return  # committed POTD — locked (game day reached / started)
    conn.execute(
        "UPDATE pick_of_day SET game_id=?, matchup=?, bet_type=?, pick=?, line=?, "
        "model_prob=?, edge=?, odds=? WHERE date=?",
        (*cols, slate_date))
    conn.commit()


def record_paper(league: str, game_id: str, matchup: str, paper_picks) -> dict:
    """Persist this game's ONE paper pick as an is_shadow=1 row in the
    shared picks table (unified replacement for
    _paper_shadow.record_paper_shadow).

    7am-ET lock (Austin 2026-10-10), via the shared
    ``_dedup_helpers.paper_pick_lock_gate`` (mirrors
    ``engine.pick_of_day._select.get_or_create_potd``): before 7am ET the
    pending row is PROVISIONAL and gets re-synced to the current best
    pre-game lean on every pass (tracks the line, same drift as before
    this fix); at/after 7am ET the first pass that has a candidate LOCKS
    it permanently — one pick per game, chosen once the market's had the
    night to settle, instead of whenever the day's first compute pass
    happened to land (which, under the old "first record wins" rule, was
    effectively the midnight-ET rollover — see the 2026-10-10 duplicate-
    row incident, where that race also let two markets land for one game).
    A row that's ALREADY pending with lock_passed True — true for the rest
    of today's slate, already cleaned to one row/game — is left exactly
    as-is: this function returns before any write whenever
    ``existing is not None and lock_passed``, so deploying this gate this
    afternoon cannot re-lock an already-pending pick to a fresh afternoon
    one."""
    from datetime import datetime as _dt
    from engine._tz import et_now
    from .._dedup_helpers import paper_pick_lock_gate
    conn = get_conn(league)
    g = conn.execute("SELECT date, start_time FROM games WHERE game_id = ?", (str(game_id),)).fetchone()
    pdate = (g["date"] if g and g["date"] else et_now().strftime("%Y-%m-%d"))
    now_iso = _dt.utcnow().isoformat()
    start_iso = (g["start_time"] if g else None) or ""
    if start_iso and start_iso.replace("Z", "+00:00") <= now_iso + "+00:00":
        return {"recorded": 0, "locked": 1}

    existing, lock_passed = paper_pick_lock_gate(
        conn, table="picks", game_id=str(game_id), start_iso=start_iso or None)
    if existing is not None and lock_passed:
        return {"recorded": 0, "locked": 1}  # already locked — frozen, no writes

    # One candidate per game: the current highest-edge lean among whatever
    # markets qualified THIS PASS (paper_picks already came back through
    # score_pick's juice wall / belief / calibration / edge floor+ceiling —
    # see _football_paper_picks — so every candidate here is eligible;
    # "best" just ranks among eligible options, it doesn't itself gate).
    # recorded_paper_pick/_pick_potd mean the same thing by "best".
    candidates = [p for p in (paper_picks or [])
                  if (p.get("type") or "") and (p.get("pick") or "")]
    best = max(candidates, key=lambda p: p.get("edge") or -999) if candidates else None

    if existing is not None:
        # Pre-lock: always clear the existing provisional row — either
        # we're about to replace it with a fresh eligible candidate below,
        # or (best is None) NOTHING currently clears the gates, and a
        # stale/now-ineligible pending pick must not keep showing/locking
        # just because an earlier pass wrote it. Without this, a game
        # whose only candidate stops qualifying (e.g. calibration/line
        # movement pushes it ineligible) would keep displaying the last
        # eligible pick forever instead of going pick-less — or worse,
        # under the OLD ungated path, keep an ineligible (juicy-ML,
        # negative-edge) pick frozen even though nothing should lock it
        # (Austin 2026-10-11: PHI@JAX/HOU@TEN/CIN@MIA/LV@NE/MIN@NO — all
        # 5 currently have NO candidate that clears the gates, so this
        # DELETE is what actually un-sticks their premature juicy ML).
        conn.execute(
            "DELETE FROM picks WHERE game_id=? AND is_shadow=1 AND result IS NULL",
            (str(game_id),))

    if best is None:
        conn.commit()
        return {"recorded": 0, "locked": int(lock_passed)}

    cur = conn.execute(
        "INSERT OR IGNORE INTO picks "
        "(date, game_id, matchup, bet_type, pick, line, model_prob, edge, odds, "
        " captured_at, stake_units, is_shadow) VALUES (?,?,?,?,?,?,?,?,?,?,0,1)",
        (pdate, str(game_id), matchup or "", best.get("type") or "", best.get("pick") or "",
         (float(best["line"]) if best.get("line") is not None else None),
         float(best.get("prob") or 0.0), float(best.get("edge") or 0.0),
         int(best.get("odds") or 0), now_iso))
    rec = 1 if cur.rowcount else 0
    conn.commit()
    return {"recorded": rec, "locked": int(lock_passed)}


def _fmt_num_potd(x):
    try:
        fx = float(x)
        return ("%d" % fx) if fx == int(fx) else ("%g" % fx)
    except Exception:
        return str(x)


def get_locked_potd(league, slate_date):
    """Today's locked POTD (from the shared pick_of_day table) as a hero-ready dict,
    with the line folded into the headline the way the client hero does."""
    if not slate_date:
        return None
    conn = get_conn(league)
    r = conn.execute("SELECT * FROM pick_of_day WHERE date=?", (slate_date,)).fetchone()
    if not r:
        return None
    d = dict(r)
    bt = (d.get("bet_type") or "")
    pick = d.get("pick"); line = d.get("line"); disp = pick
    if line is not None:
        u = bt.upper()
        if u == "TOTAL":
            disp = "%s %s" % (pick, _fmt_num_potd(line))
        elif u == "SPREAD":
            disp = "%s %s%s" % (pick, "+" if float(line) > 0 else "", _fmt_num_potd(line))
    return {"game_id": d.get("game_id"), "matchup": d.get("matchup"),
            "bet_type": bt, "type": bt, "pick": disp, "line": line,
            "odds": d.get("odds"), "edge": d.get("edge"),
            "model_prob": d.get("model_prob"), "prob": d.get("model_prob"),
            "result": d.get("result"), "paper": True}


def record_shadow_slate(league, game_id, matchup, candidates, pick_date=None):
    """Pick Authority graduation lane (Austin 2026-10-18, "how do we let these
    markets prove themselves if we chop them off entirely?"). Mirror EVERY
    candidate market lean for this game into picks_unified as is_shadow=1, so a
    market we don't HEADLINE (the proven-edge gate) still accumulates a real
    settled track record and can graduate. The headline was already written real
    by record_picks; record_pick's INSERT OR IGNORE keeps that family real, so
    these shadow rows only fill the markets we didn't headline. settle_pending/
    the _football_adapter grade them by game_key, is_shadow-agnostic -- the same
    path that settles the real rows. Recorded once per game (first pre-kickoff
    pass) so intra-day line moves don't count as separate observations."""
    if not candidates:
        return {"mirrored": 0}
    try:
        from ..picks_unified._legacy_bridge import mirror_to_unified, _game_key_for
        from ..picks_unified._schema import get_conn as _u_conn
    except Exception:
        return {"mirrored": 0}
    try:
        gk = str(_game_key_for("football", league, game_id))
        if _u_conn().execute(
                "SELECT 1 FROM picks WHERE game_key=? AND is_shadow=1 LIMIT 1",
                (gk,)).fetchone():
            return {"mirrored": 0, "already": 1}
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
        pk = p.get("pick") or ""
        if not bt or not pk:
            continue
        try:
            mirror_to_unified(
                sport="football", league=league, native_game_id=game_id,
                pick_date=pdate, matchup=matchup or "",
                bet_type=bt, pick_text=pk,
                odds=int(p.get("odds") or 0),
                prob=float(p.get("prob") or 0.0),
                edge_pct=float(p.get("edge") or 0.0),
                stake_units=float(p.get("stake_units") or 0.0),
                is_shadow=True,
            )
            n += 1
        except Exception:
            pass
    return {"mirrored": n}
