"""Closing-line value capture for hockey-framework leagues
(AHL / PWHL / AIHL / NZIHL).

Hockey framework's HR fetch ships FLAT keys
(home_ml/away_ml/over_odds/under_odds/home_pl_*/away_pl_*) — different
shape from basketball's nested ml/spread/total dicts and from NBA's
flat home_ml/spread keys. Need its own extractor.

Bet types in hockey picks table:
    ML     → home_ml / away_ml
    PL     → home/away puck-line. Pick text format: "<abbr> -1.5" or
             "<abbr> +1.5". Read home_pl_odds / away_pl_odds.
    OU     → over_odds / under_odds. Pick text: "Over 5.5" / "Under 5.5"

Public:
    capture_closing_odds(league) -> int  # rows updated/refreshed
"""
from __future__ import annotations

import logging

from .. import __init__  # noqa
from ._odds import fetch_league_odds

logger = logging.getLogger(__name__)


def _conn_for(league: str):
    """Reuse the per-league theScore DB."""
    mod = __import__(f"engine.sports.{league}.db", fromlist=["get_conn"])
    return mod.get_conn()


def _extract_closing(bet_type: str, pick_text: str, home_abbr: str,
                      game_odds: dict) -> int | None:
    """Return the closing-odds field for one hockey pick. Mirrors the
    NBA-flat-key extractor but with hockey markets."""
    if not game_odds:
        return None
    pk = (pick_text or "").strip()
    parts = pk.split()

    def _is_home(team: str) -> bool:
        return team == home_abbr

    if bet_type == "ML":
        if not parts:
            return None
        return (game_odds.get("home_ml") if _is_home(parts[0])
                else game_odds.get("away_ml"))

    if bet_type == "PL":
        if len(parts) < 2:
            return None
        # Pick: "<abbr> -1.5" or "<abbr> +1.5". HR puck-line stays on
        # the canonical ±1.5 split, so the side is what we need.
        return (game_odds.get("home_pl_odds") if _is_home(parts[0])
                else game_odds.get("away_pl_odds"))

    if bet_type == "OU":
        if not parts:
            return None
        return (game_odds.get("over_odds") if parts[0].lower() == "over"
                else game_odds.get("under_odds"))

    return None


def capture_closing_odds(league: str) -> int:
    """Snapshot HR closing odds for all pending picks in ``league``.
    Returns rows updated. Idempotent."""
    conn = _conn_for(league)
    pending = conn.execute(
        "SELECT id, matchup, bet_type, pick, closing_odds "
        "FROM picks WHERE result IS NULL"
    ).fetchall()
    if not pending:
        return 0

    try:
        all_odds = fetch_league_odds(league) or {}
    except Exception as e:
        logger.warning("HR hockey fetch (%s) failed: %s", league, e)
        return 0
    if not all_odds:
        return 0

    updated = refreshed = 0
    for row in pending:
        pick = dict(row)
        matchup = pick.get("matchup") or ""
        # Hockey matchup format: "AWAY @ HOME"
        if " @ " not in matchup:
            continue
        away_abbr, home_abbr = [s.strip() for s in matchup.split(" @ ", 1)]
        game_key = f"{away_abbr}@{home_abbr}"
        game_odds = all_odds.get(game_key)
        if not game_odds:
            continue
        try:
            closing = _extract_closing(
                pick["bet_type"], pick["pick"], home_abbr, game_odds,
            )
        except Exception as e:
            logger.debug("hockey extract crashed for id=%s: %s",
                         pick.get("id"), e)
            continue
        if closing is None:
            continue
        prior = pick.get("closing_odds")
        if prior is None:
            conn.execute(
                "UPDATE picks SET closing_odds = ? WHERE id = ?",
                (int(closing), pick["id"]),
            )
            updated += 1
        elif int(prior) != int(closing):
            conn.execute(
                "UPDATE picks SET closing_odds = ? WHERE id = ?",
                (int(closing), pick["id"]),
            )
            refreshed += 1
    conn.commit()
    if updated or refreshed:
        logger.info("hockey:%s CLV: %d new, %d refreshed (of %d pending)",
                    league, updated, refreshed, len(pending))
    return updated + refreshed


def capture_all() -> dict:
    """Run capture across every hockey-framework league."""
    out: dict[str, int] = {}
    for league in ("ahl", "pwhl", "aihl", "nzihl"):
        try:
            out[league] = capture_closing_odds(league)
        except Exception as e:
            logger.warning("hockey CLV %s crashed: %s", league, e)
            out[league] = 0
    return out


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.hockey._clv")
    ap.add_argument("league", nargs="?", default="all",
                     choices=("ahl", "pwhl", "aihl", "nzihl", "all"))
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.league == "all":
        print(capture_all())
    else:
        n = capture_closing_odds(args.league)
        print(f"{args.league}: {n} rows updated")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
