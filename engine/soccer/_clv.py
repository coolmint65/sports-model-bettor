"""Closing-line value capture for the soccer framework.

Mirrors the per-sport pattern: tracker_core.SportAdapter pairing an HR
fetcher with a sport-specific extractor that walks the nested odds
dict the soccer parser ships from ``fetch_league_odds``.

Soccer pick rows carry ``side`` + ``line`` columns so extraction reads
those directly instead of parsing pick_text — cleaner than the basket-
ball/NBA flow.

Public:
    capture_closing_odds(league) -> int  # rows updated/refreshed
"""
from __future__ import annotations

import logging

from ..tracker_core import SportAdapter, core_capture_closing_odds
from ._db import get_conn, picks_table
from ._odds import fetch_league_odds

logger = logging.getLogger(__name__)


def _extract_closing_for_pick(bet_type: str, pick_text: str,
                                home_abbr: str, game_odds: dict
                                ) -> int | None:
    """Pull closing odds for one soccer pick. Soccer pick rows carry
    side + line in dedicated columns but core_capture_closing_odds only
    passes (bet_type, pick_text, home_abbr, game_odds). We parse the
    pick_text — soccer formats are deterministic per bet_type."""
    if not game_odds:
        return None
    pk = (pick_text or "").strip()
    if not pk:
        return None
    parts = pk.split()

    # ── ML (1X2) ──
    if bet_type == "ML":
        ml = game_odds.get("ml") or {}
        if pk == "Draw":
            return ml.get("draw")
        # Single-token team abbr → match against home_abbr.
        if pk == home_abbr:
            return ml.get("home")
        return ml.get("away")

    # ── DNB ──
    if bet_type == "DNB":
        # Format: "<abbr> (DNB)"
        if not parts:
            return None
        team = parts[0]
        dnb = game_odds.get("dnb") or {}
        return dnb.get("home") if team == home_abbr else dnb.get("away")

    # ── Double Chance ──
    if bet_type == "DC":
        dc = game_odds.get("dc") or {}
        # pick formats: "<HOME> or Draw" | "Draw or <AWAY>" | "<HOME> or <AWAY>"
        if " or " not in pk:
            return None
        left, right = pk.split(" or ", 1)
        left, right = left.strip(), right.strip()
        if left == home_abbr and right.lower() == "draw":
            return dc.get("home_draw")
        if left.lower() == "draw":
            return dc.get("draw_away")
        return dc.get("home_away")

    # ── OU ──
    if bet_type == "OU":
        # "Over 2.5" / "Under 2.5"
        if len(parts) < 2:
            return None
        tot = game_odds.get("total") or {}
        line = None
        try:
            line = float(parts[1])
        except ValueError:
            pass
        primary_line = tot.get("line")
        is_over = parts[0].lower() == "over"
        # No line in the pick text (rare) — fall back to primary odds.
        if line is None:
            return tot.get("over_odds") if is_over else tot.get("under_odds")
        # Primary line matches → use primary odds.
        if primary_line is not None \
                and abs(float(primary_line) - line) < 0.01:
            return tot.get("over_odds") if is_over else tot.get("under_odds")
        # Line moved — search alt_totals for exact match.
        for alt in (game_odds.get("alt_totals") or []):
            if abs(float(alt.get("line", -999)) - line) < 0.01:
                return alt.get("over_odds") if is_over else alt.get("under_odds")
        # No match anywhere → skip closing_odds capture rather than
        # stamp odds for a different line as if they were ours. Pre-
        # fix this returned primary odds for the CURRENT line, which
        # is the CLV-tracked-wrong bug the user flagged 2026-07-03.
        return None

    # ── BTTS ──
    if bet_type == "BTTS":
        # "BTTS Yes" / "BTTS No"
        if len(parts) < 2:
            return None
        btts = game_odds.get("btts") or {}
        return (btts.get("yes") if parts[1].lower() == "yes"
                else btts.get("no"))

    # ── H1 ML (1H moneyline) ──
    # H1 markets nest under game_odds['h1'] keyed by market. HR ships
    # them for every prematch WC/soccer game; before this branch the
    # entire H1 family had 0% CLV coverage because the extractor bailed
    # out at the top-level ML/DNB/DC/OU checks (H1_* doesn't match any
    # of those and fell through to `return None`).
    if bet_type == "H1_ML":
        h1 = (game_odds.get("h1") or {}).get("ml") or {}
        if pk.startswith("Draw"):
            return h1.get("draw")
        # Pick text ends with "(H1)" so tokens[0] is the team abbr.
        if not parts:
            return None
        team = parts[0]
        return h1.get("home") if team == home_abbr else h1.get("away")

    # ── H1 Draw-No-Bet ──
    if bet_type == "H1_DNB":
        if not parts:
            return None
        team = parts[0]
        dnb = (game_odds.get("h1") or {}).get("dnb") or {}
        return dnb.get("home") if team == home_abbr else dnb.get("away")

    # ── H1 Double Chance ──
    if bet_type == "H1_DC":
        dc = (game_odds.get("h1") or {}).get("dc") or {}
        # "TEAM or Draw (H1)" — strip "(H1)" tail then reuse the FT DC parser.
        pk_body = pk[:-len("(H1)")].strip() if pk.endswith("(H1)") else pk
        if " or " not in pk_body:
            return None
        left, right = pk_body.split(" or ", 1)
        left, right = left.strip(), right.strip()
        if left == home_abbr and right.lower() == "draw":
            return dc.get("home_draw")
        if left.lower() == "draw":
            return dc.get("draw_away")
        return dc.get("home_away")

    # ── H1 Total ──
    if bet_type == "H1_TOTAL":
        # "Over 1.5 (H1)" — strip "(H1)" tail then reuse the FT OU logic.
        pk_body = pk[:-len("(H1)")].strip() if pk.endswith("(H1)") else pk
        body_parts = pk_body.split()
        if len(body_parts) < 2:
            return None
        tot = (game_odds.get("h1") or {}).get("total") or {}
        try:
            line = float(body_parts[1])
        except ValueError:
            return None
        primary_line = tot.get("line")
        is_over = body_parts[0].lower() == "over"
        if primary_line is not None \
                and abs(float(primary_line) - line) < 0.01:
            return (tot.get("over_odds") if is_over
                    else tot.get("under_odds"))
        # H1 alt-lines exist on HR for premier fixtures — same shape.
        for alt in ((game_odds.get("h1") or {}).get("alt_totals") or []):
            if abs(float(alt.get("line", -999)) - line) < 0.01:
                return alt.get("over_odds") if is_over else alt.get("under_odds")
        return None

    # ── Advance to next round ──
    # Knockout bracket: "TEAM to Advance" pointing at the game_odds.advance
    # market. Present only for KO fixtures — group stage matches carry no
    # advance market.
    if bet_type == "ADVANCE":
        adv = game_odds.get("advance") or {}
        # "AUS to Advance" → tokens[0] is abbr.
        if not parts:
            return None
        team = parts[0]
        return adv.get("home") if team == home_abbr else adv.get("away")

    # ── Asian Handicap ──
    if bet_type == "AH":
        # "<abbr> +0.5" / "<abbr> -1.5"
        if len(parts) < 2:
            return None
        team = parts[0]
        try:
            line = float(parts[1])
        except ValueError:
            return None
        ah = game_odds.get("ah") or {}
        # Primary line stored as positive home-side handicap.
        primary_line = ah.get("line")
        is_home = (team == home_abbr)
        # When picking the home side, the printed line matches game_odds.line.
        # When picking the away side, we negate.
        target_home_line = line if is_home else -line
        if primary_line is not None \
                and abs(float(primary_line) - target_home_line) < 0.01:
            return (ah.get("home_odds") if is_home
                    else ah.get("away_odds"))
        # Fall back to alt_ah ladder.
        for alt in (game_odds.get("alt_ah") or []):
            if abs(float(alt.get("line", -999)) - target_home_line) < 0.01:
                return (alt.get("home_odds") if is_home
                        else alt.get("away_odds"))
        return None

    return None


def capture_closing_odds(league: str) -> int:
    """Snapshot current HR odds for pending picks in the soccer league.
    Returns rows updated. Idempotent."""
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
    ap = argparse.ArgumentParser(prog="engine.soccer._clv")
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
