"""Closing-line value capture for tennis picks.

Tennis HR events are keyed by (p1_name, p2_name) — not a matchup-string
dict — so we can't reuse the ``tracker_core.SportAdapter`` flow which
assumes ``_match_odds(home, away, ...)``. This module walks the picks
table directly and resolves each pending pick to its current HR event
by name pair (order-independent), then extracts the closing odds for
the pick's bet_type.

Pick text → odds field map:
    ML                          ml.p{1|2}_odds          (player name → side)
    TOTAL_SETS_OVER             total_sets.over_odds
    TOTAL_SETS_UNDER            total_sets.under_odds
    TOTAL_GAMES_OVER            total_games.over_odds
    TOTAL_GAMES_UNDER           total_games.under_odds
    WIN_AT_LEAST_ONE_SET        at_least_one_set.p{1|2}_yes
    SET_BETTING                 set_betting.<key>
    SET_SPREAD                  set_spread[<line>].p{1|2}_odds

Public:
    capture_closing_odds() -> int  # rows updated/refreshed
"""
from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)


def _normalize_name(s: str) -> str:
    """Tennis name normalization for matching pick → HR event. Tennis
    is full-name based so we lowercase + collapse spaces + strip
    diacritics, leaving alphanumeric tokens that survive HR's vs the
    tracker's spelling drift (e.g. 'Jiří Lehečka' vs 'Jiri Lehecka')."""
    import unicodedata
    n = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore")
    n = n.decode("ascii").lower().strip()
    return re.sub(r"\s+", " ", n)


def _event_index(events: list[dict]) -> dict[tuple[str, str], dict]:
    """Build {(p1_norm, p2_norm): event} with both orderings — pick text
    only identifies the player, not which side they were on in HR's
    event tuple, so we want an order-independent lookup."""
    out: dict[tuple[str, str], dict] = {}
    for ev in events:
        p1 = _normalize_name(ev.get("p1_name") or "")
        p2 = _normalize_name(ev.get("p2_name") or "")
        if not p1 or not p2:
            continue
        out[(p1, p2)] = ev
        out[(p2, p1)] = ev
    return out


def _split_matchup(matchup: str) -> tuple[str, str] | None:
    """Tennis matchups ship as '<player1> vs <player2>'."""
    if not matchup:
        return None
    if " vs " in matchup:
        a, b = matchup.split(" vs ", 1)
        return a.strip(), b.strip()
    return None


def _extract_closing(pick: dict, event: dict) -> int | None:
    """Extract the right closing odds for ``pick`` from ``event``'s
    market sub-dicts. Returns None when bet_type doesn't have a clear
    field or when the market is missing in the HR feed for this match."""
    bet_type = pick.get("bet_type") or ""
    pick_text = (pick.get("pick") or "").strip()
    markets = event.get("markets") or {}

    # Resolve which side of the event the pick refers to (p1 or p2).
    p1_norm = _normalize_name(event.get("p1_name") or "")
    p2_norm = _normalize_name(event.get("p2_name") or "")
    pick_norm = _normalize_name(pick_text)

    # ML / WIN_AT_LEAST_ONE_SET include the player name. Direction
    # selectors (Over/Under) don't. Detect by leading token.
    def _which_player(text: str) -> str | None:
        n = _normalize_name(text)
        # Prefer prefix match (player names sometimes have a trailing
        # selector like "Yes" / "Over").
        for side, ref in (("p1", p1_norm), ("p2", p2_norm)):
            if not ref:
                continue
            if n.startswith(ref) or ref in n:
                return side
        return None

    if bet_type == "ML":
        side = _which_player(pick_text)
        ml = markets.get("ml") or {}
        if side == "p1":
            return ml.get("p1_odds")
        if side == "p2":
            return ml.get("p2_odds")
        return None

    if bet_type == "WIN_AT_LEAST_ONE_SET":
        side = _which_player(pick_text)
        # HR ships per-player markets: p1_win_at_least_one_set,
        # p2_win_at_least_one_set, each with yes/no odds.
        is_yes = pick_text.strip().lower().endswith("yes")
        if side in ("p1", "p2"):
            m = markets.get(f"{side}_win_at_least_one_set") or {}
            return m.get("yes_odds") if is_yes else m.get("no_odds")
        return None

    # Direction (Over/Under) lives in the pick_text, NOT the bet_type
    # — audit 2026-07-08 caught the extractor matching the wrong shape
    # ("TOTAL_GAMES_OVER"), which killed CLV capture for all TOTAL_GAMES /
    # TOTAL_SETS picks. Actual bet_type is "TOTAL_GAMES" with pick_text
    # like "Over 24.5" / "Under 24.5".
    def _is_over(text: str) -> bool:
        return text.strip().lower().startswith("over")

    if bet_type == "TOTAL_GAMES":
        m = markets.get("total_games") or {}
        # Primary line, no line-match fallback yet — HR alt-total ladder
        # for tennis TG isn't shipped through this event shape. If the
        # market moves the line before capture, we miss; log-count kept
        # in the summary rather than stamping a stale odd.
        return m.get("over_odds") if _is_over(pick_text) else m.get("under_odds")
    if bet_type == "TOTAL_SETS":
        m = markets.get("total_sets") or {}
        return m.get("over_odds") if _is_over(pick_text) else m.get("under_odds")

    if bet_type == "SET_SPREAD":
        side = _which_player(pick_text)
        # pick format: "<player> +1.5" / "<player> -1.5"
        m_re = re.search(r"([+-]?\d+(?:\.\d+)?)$", pick_text)
        if not m_re:
            return None
        try:
            line = float(m_re.group(1))
        except ValueError:
            return None
        for entry in (markets.get("set_spread") or []):
            if entry is None:
                continue
            entry_line = entry.get("line")
            if entry_line is None:
                continue
            try:
                el = float(entry_line)
            except (TypeError, ValueError):
                continue
            # set_spread entries store the line as the home-side handicap;
            # opposite side is the negation.
            if side == "p1" and abs(el - line) < 0.01:
                return entry.get("p1_odds")
            if side == "p2" and abs(-el - line) < 0.01:
                return entry.get("p2_odds")
        return None

    # SET_BETTING — pick text is something like "2-0 p1" / "2-1 p2"
    if bet_type == "SET_BETTING":
        sb = markets.get("set_betting") or {}
        return sb.get(pick_text)

    return None


def capture_closing_odds() -> int:
    """Snapshot HR closing odds for every pending tennis pick that
    doesn't yet have one (or whose previous capture changed). Returns
    rows updated. Idempotent."""
    from .tennis_db import get_conn
    from .tennis_odds import fetch_all
    conn = get_conn()
    pending = conn.execute(
        "SELECT id, tour, matchup, bet_type, pick, closing_odds "
        "FROM tennis_picks WHERE result IS NULL"
    ).fetchall()
    if not pending:
        return 0

    try:
        events = fetch_all()
    except Exception as e:
        logger.warning("HR tennis fetch failed: %s", e)
        return 0
    if not events:
        logger.info("tennis CLV: HR returned no events")
        return 0
    idx = _event_index(events)

    updated = refreshed = 0
    for row in pending:
        pick = dict(row)
        names = _split_matchup(pick["matchup"])
        if not names:
            continue
        a, b = names
        ev = idx.get((_normalize_name(a), _normalize_name(b)))
        if not ev:
            continue
        try:
            closing = _extract_closing(pick, ev)
        except Exception as e:
            logger.debug("tennis extract crashed for id=%s: %s",
                         pick.get("id"), e)
            continue
        if closing is None:
            continue
        # Sentinel guard — HR ships ±100000 / ±500000 placeholders when
        # a market is locked (about to start / already tipped). Mirrors
        # the fix in engine.tracker_core.core_capture_closing_odds so
        # tennis's own capture path can't stamp garbage either.
        try:
            closing_int = int(closing)
        except (TypeError, ValueError):
            continue
        if abs(closing_int) < 100 or abs(closing_int) > 5000:
            continue
        prior = pick.get("closing_odds")
        if prior is None:
            conn.execute(
                "UPDATE tennis_picks SET closing_odds = ? WHERE id = ?",
                (closing_int, pick["id"]),
            )
            updated += 1
        elif int(prior) != closing_int:
            conn.execute(
                "UPDATE tennis_picks SET closing_odds = ? WHERE id = ?",
                (closing_int, pick["id"]),
            )
            refreshed += 1
    conn.commit()
    logger.info("tennis CLV: %d new, %d refreshed (out of %d pending)",
                updated, refreshed, len(pending))
    return updated + refreshed


def _cli() -> int:
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    n = capture_closing_odds()
    print(f"tennis: {n} rows updated")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
