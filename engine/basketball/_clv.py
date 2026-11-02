"""Closing-line value capture for basketball-framework leagues.

The NBA-specific capture in ``engine.nba_tracker._record`` reads flat
keys (``home_ml``, ``home_spread_odds``, ``over_odds``) from its HR
fetcher. The basketball framework ships a nested shape from
``fetch_league_odds`` (``ml.home``, ``spread.home_odds``,
``total.over_odds``), so the extractor needs its own walker.

Public:
    capture_closing_odds(league) -> int  # rows updated/refreshed

Wired into the live worker's per-tick CLV loop alongside the NBA/NHL/
MLB captures.
"""
from __future__ import annotations

import logging

from ..tracker_core import SportAdapter, core_capture_closing_odds
from ._db import get_conn, picks_table
from ._odds import fetch_league_odds

logger = logging.getLogger(__name__)


# Team-abbreviation aliases — NBA's ALT_ABBRS handles UTA/UTAH style
# variation; for the basketball framework most leagues are clean but
# we keep the same fallback shape so the extractor can match liberally.
_ALT_ABBRS: dict[str, str] = {}


def _extract_closing_for_pick(bet_type: str, pick_text: str,
                                home_abbr: str, game_odds: dict
                                ) -> int | None:
    """Pull the right closing-odds field for one basketball-framework
    pick out of an HR odds bucket. Mirrors the NBA extractor's coverage:
    full-game ML/SPREAD/TOTAL plus Q1 markets when shipped."""
    if not game_odds:
        return None
    pk = (pick_text or "").strip()
    if not pk:
        return None
    parts = pk.split()

    def _is_home(team: str) -> bool:
        return (team == home_abbr
                or team == _ALT_ABBRS.get(home_abbr, ""))

    def _line_matches(a, b, tol: float = 0.001) -> bool:
        """Handles ±0.5 vs 0.5-with-mirrored-sign symmetry for SPREAD
        picks — a home -3.5 line means an away +3.5 line, so both
        representations should match the same pick."""
        if a is None or b is None:
            return False
        try:
            return abs(float(a) - float(b)) < tol
        except (TypeError, ValueError):
            return False

    def _spread_close(sp_dict: dict, alt_list, is_home: bool,
                       point: float | None):
        """Look up the closing odds for the SPECIFIC spread line we
        bet, not the current primary line. Falls back to None (skip
        the update) when the line moved and the exact number isn't in
        alt_spreads either — better to leave closing_odds NULL than
        stamp odds for a different market as if they were ours."""
        if point is None:
            # Pick text didn't carry a signed line (rare) — safe
            # fallback to the primary side odds.
            return (sp_dict.get("home_odds") if is_home
                    else sp_dict.get("away_odds"))
        primary_pt = (sp_dict.get("home_pt") if is_home
                      else sp_dict.get("away_pt"))
        if _line_matches(primary_pt, point):
            return (sp_dict.get("home_odds") if is_home
                    else sp_dict.get("away_odds"))
        for alt in alt_list or []:
            alt_home = alt.get("home_pt")
            alt_away = alt.get("away_pt")
            if is_home and _line_matches(alt_home, point):
                return alt.get("home_odds")
            if (not is_home) and _line_matches(alt_away, point):
                return alt.get("away_odds")
        return None

    def _total_close(tot_dict: dict, alt_list, is_over: bool,
                       line: float | None):
        if line is None:
            return (tot_dict.get("over_odds") if is_over
                    else tot_dict.get("under_odds"))
        if _line_matches(tot_dict.get("line"), line):
            return (tot_dict.get("over_odds") if is_over
                    else tot_dict.get("under_odds"))
        for alt in alt_list or []:
            if _line_matches(alt.get("line"), line):
                return (alt.get("over_odds") if is_over
                        else alt.get("under_odds"))
        return None

    def _signed_point(idx: int) -> float | None:
        if len(parts) <= idx:
            return None
        try:
            return float(parts[idx])
        except (TypeError, ValueError):
            return None

    # ── Full-game primary markets ──
    if bet_type == "ML":
        if not parts:
            return None
        ml = game_odds.get("ml") or {}
        return ml.get("home") if _is_home(parts[0]) else ml.get("away")
    if bet_type == "SPREAD":
        if len(parts) < 2:
            return None
        is_home = _is_home(parts[0])
        point = _signed_point(1)
        return _spread_close(game_odds.get("spread") or {},
                              game_odds.get("alt_spreads"),
                              is_home, point)
    if bet_type == "TOTAL":
        if not parts:
            return None
        is_over = parts[0].lower() == "over"
        line = _signed_point(1)
        return _total_close(game_odds.get("total") or {},
                              game_odds.get("alt_totals"),
                              is_over, line)

    # ── Q1 primary markets ──
    if bet_type == "Q1_ML":
        if not parts:
            return None
        q1 = game_odds.get("q1") or {}
        ml = q1.get("ml") or {}
        return ml.get("home") if _is_home(parts[0]) else ml.get("away")
    if bet_type == "Q1_SPREAD":
        if len(parts) < 2:
            return None
        is_home = _is_home(parts[0])
        point = _signed_point(1)
        q1 = game_odds.get("q1") or {}
        return _spread_close(q1.get("spread") or {},
                              q1.get("alt_spreads"),
                              is_home, point)
    if bet_type == "Q1_TOTAL":
        if not parts:
            return None
        is_over = parts[0].lower() == "over"
        line = _signed_point(1)
        q1 = game_odds.get("q1") or {}
        return _total_close(q1.get("total") or {},
                              q1.get("alt_totals"),
                              is_over, line)

    return None


def capture_closing_odds(league: str) -> int:
    """Snapshot current HR odds for all pending picks in the league.
    Returns rows updated (new + refreshed). Idempotent."""
    adapter = SportAdapter(
        name=league,
        get_conn=lambda: get_conn(league),
        picks_table=picks_table(league),
        hr_fetch=lambda: fetch_league_odds(league),
        extract_closing=_extract_closing_for_pick,
    )
    return core_capture_closing_odds(adapter)


def capture_unified_closing(league: str) -> int:
    """Pick Authority CLV for the framework's unified rows (MintyBot 2026-10-26, college
    basketball): the PA shadow lane lives in picks_unified, which nothing closed for basketball
    framework leagues -- capture_closing_odds above only touches the league's own picks table.
    Snapshot HR's price at each pending row's OWN market + side + line (main or alt; full game
    or first period) while its game is still scheduled; overwrites every call, so the last
    pre-tip value is the close. A line HR stops offering keeps its last captured value."""
    from ..tracker_core import _cc_now_iso
    from ._db import games_table, teams_table
    try:
        from ..picks_unified._schema import get_conn as _u_conn
    except Exception:
        return 0
    conn = get_conn(league)
    sched = {str(r["game_id"]): r["h"] for r in conn.execute(
        f"SELECT g.game_id, ht.abbreviation AS h FROM {games_table(league)} g "
        f"LEFT JOIN {teams_table(league)} ht ON ht.id = g.home_team_id "
        f"WHERE g.status = 'scheduled'")}
    if not sched:
        return 0
    u = _u_conn()
    rows = [r for r in u.execute(
        "SELECT id, game_key, bet_type, pick_text FROM picks "
        "WHERE sport = 'basketball' AND league = ? AND result IS NULL", (league,)).fetchall()
        if str(r[1]).rsplit(":", 1)[-1] in sched]
    if not rows:
        return 0
    try:
        odds = fetch_league_odds(league)     # cached; no forced HR hit
    except Exception:
        return 0
    by_gid = {str(v.get("game_id")): v for v in (odds or {}).values() if v.get("game_id")}
    now = _cc_now_iso()
    n = 0
    for rid, gk, bt, pt in rows:
        gid = str(gk).rsplit(":", 1)[-1]
        o = by_gid.get(gid)
        if not o:
            continue
        try:
            am = int(_extract_closing_for_pick(bt, pt, o.get("home_abbr") or sched[gid], o))
        except (TypeError, ValueError):
            continue
        if abs(am) < 100 or abs(am) > 5000:    # HR market-locked sentinels
            continue
        u.execute("UPDATE picks SET closing_odds = ?, closing_captured_at = ? WHERE id = ?",
                  (am, now, rid))
        n += 1
    u.commit()
    return n


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(
        prog="engine.basketball._clv")
    ap.add_argument("league")
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    n = capture_closing_odds(args.league)
    print(f"{args.league}: {n} rows updated")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
