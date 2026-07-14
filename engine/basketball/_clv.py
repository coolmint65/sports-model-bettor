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
