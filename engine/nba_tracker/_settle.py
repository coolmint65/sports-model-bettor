"""NBA pick settlement: Q1 markets via locked Q1 scores + full-game
markets gated on game completion."""

from __future__ import annotations
import logging
from datetime import datetime, timedelta

from ._helpers import _ALT_ABBRS, _extract_nba_closing_for_pick
from ._scoreboard import _fetch_nba_scoreboard, _parse_q1_scores

logger = logging.getLogger(__name__)


def settle_picks() -> dict:
    """Settle all pending NBA picks against final game results.

    Q1 markets settle the moment Q1 ends; full-game markets gate on
    state == "post".
    """
    from ..nba_db import get_conn

    conn = get_conn()

    # Self-heal: auto-push picks pending >7 days. Same rationale as
    # MLB / NHL settlers.
    stale_cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    stale = conn.execute(
        "SELECT id, date, matchup, bet_type, pick FROM nba_picks "
        "WHERE result IS NULL AND date < ?", (stale_cutoff,),
    ).fetchall()
    for row in stale:
        conn.execute(
            "UPDATE nba_picks SET result='P', profit=0, settled_at=datetime('now') "
            "WHERE id=?", (row["id"],),
        )
        logger.warning(
            "NBA settle: auto-pushed stale pending id=%s date=%s "
            "%s %s/%s — older than 7 days",
            row["id"], row["date"], row["matchup"], row["bet_type"], row["pick"],
        )
    if stale:
        conn.commit()

    pending = conn.execute(
        "SELECT * FROM nba_picks WHERE result IS NULL"
    ).fetchall()

    if not pending:
        return {"settled": 0, "message": "No pending NBA picks"}

    dates = set()
    for p in pending:
        dates.add(p["date"])

    final_q1: dict[str, dict] = {}
    for d in dates:
        events = _fetch_nba_scoreboard(d)
        for event in events:
            q1_data = _parse_q1_scores(event)
            if q1_data:
                final_q1[q1_data["game_id"]] = q1_data

    # Best-effort capture for any picks that didn't get closing odds
    # stamped pre-game by capture_closing_odds(). HR Q1 markets often
    # vanish post-game, so this rarely fires — the pre-game capture is
    # the real source.
    try:
        from scrapers.hardrock_odds import fetch_nba as _hr_nba
        hr_nba_now = _hr_nba() or {}
    except Exception as e:
        logger.debug("NBA settle-time HR fetch failed: %s", e)
        hr_nba_now = {}

    settled = 0
    wins = 0
    losses = 0

    for pick in pending:
        pick = dict(pick)
        game_id = pick["game_id"]

        game = final_q1.get(game_id)
        if not game:
            continue

        h_q1 = game["home_q1"]
        a_q1 = game["away_q1"]
        q1_total = game["q1_total"]
        q1_margin = game["q1_margin"]
        h = game["home_abbr"]
        a = game["away_abbr"]

        # Settle-time fallback capture — only fires if pre-game
        # capture_closing_odds() didn't get a chance to run.
        if not pick.get("closing_odds") and h and a and hr_nba_now:
            from ..picks import match_odds as _match_odds
            game_cl_odds = _match_odds(h, a, hr_nba_now)
            if game_cl_odds:
                closing = _extract_nba_closing_for_pick(
                    pick["bet_type"], pick["pick"], h, game_cl_odds,
                )
                if closing is not None:
                    conn.execute(
                        "UPDATE nba_picks SET closing_odds = ? WHERE id = ?",
                        (int(closing), pick["id"]),
                    )
                    pick["closing_odds"] = int(closing)

        bt = pick["bet_type"]
        pk = pick["pick"]
        odds = pick["odds"] or -110
        result = None

        if bt == "Q1_ML":
            pick_team = pk.split()[0]
            home_won_q1 = q1_margin > 0
            is_home_pick = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))
            is_away_pick = (pick_team == a or pick_team == _ALT_ABBRS.get(a, ""))
            if is_home_pick:
                if home_won_q1:
                    result = "W"
                elif q1_margin == 0:
                    result = "P"
                else:
                    result = "L"
            elif is_away_pick:
                if not home_won_q1 and q1_margin != 0:
                    result = "W"
                elif q1_margin == 0:
                    result = "P"
                else:
                    result = "L"

        elif bt == "Q1_SPREAD":
            parts = pk.split()
            if len(parts) >= 2:
                pick_team = parts[0]
                spread = float(parts[1])
                is_home_pick = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))
                if is_home_pick:
                    actual_margin = h_q1 - a_q1
                else:
                    actual_margin = a_q1 - h_q1
                covered = actual_margin + spread
                if covered > 0:
                    result = "W"
                elif covered == 0:
                    result = "P"
                else:
                    result = "L"

        elif bt == "Q1_TOTAL":
            parts = pk.split()
            if len(parts) >= 2:
                direction = parts[0].lower()
                line = float(parts[1])
                if direction == "over":
                    if q1_total > line:
                        result = "W"
                    elif q1_total < line:
                        result = "L"
                    else:
                        result = "P"
                else:
                    if q1_total < line:
                        result = "W"
                    elif q1_total > line:
                        result = "L"
                    else:
                        result = "P"

        # ── Phase 1 derivative markets ──
        elif bt == "Q1 Team Total":
            parts = pk.split()
            if len(parts) >= 4:
                pick_team = parts[0]
                direction = parts[2].lower()
                try:
                    line = float(parts[3])
                except ValueError:
                    continue
                is_home_pick = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))
                team_q1 = h_q1 if is_home_pick else a_q1
                if team_q1 > line:
                    result = "W" if direction == "over" else "L"
                elif team_q1 < line:
                    result = "L" if direction == "over" else "W"
                else:
                    result = "P"

        elif bt == "Q1 Total O/E":
            parts = pk.split()
            if len(parts) >= 3:
                direction = parts[2].lower()
                is_odd = (q1_total % 2 == 1)
                if direction == "odd":
                    result = "W" if is_odd else "L"
                else:
                    result = "W" if not is_odd else "L"

        # ── Phase 2k: full-game markets ──
        # Only settle when the game is fully complete; partial scores
        # mid-Q2/Q3/Q4 would mis-settle Over/Under and ML.
        elif bt in ("ML", "SPREAD", "TOTAL", "ALT SPREAD", "ALT TOTAL"):
            if not game.get("is_completed"):
                continue

            home_score = game["home_score"]
            away_score = game["away_score"]
            full_margin = home_score - away_score
            full_total = home_score + away_score

            if bt == "ML":
                pick_team = pk.split()[0]
                home_won = full_margin > 0
                is_home_pick = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))
                is_away_pick = (pick_team == a or pick_team == _ALT_ABBRS.get(a, ""))
                if is_home_pick:
                    result = "W" if home_won else ("P" if full_margin == 0 else "L")
                elif is_away_pick:
                    result = "W" if (not home_won and full_margin != 0) else ("P" if full_margin == 0 else "L")

            elif bt in ("SPREAD", "ALT SPREAD"):
                parts = pk.split()
                if len(parts) >= 2:
                    pick_team = parts[0]
                    try:
                        spread = float(parts[1])
                    except ValueError:
                        continue
                    is_home_pick = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))
                    actual = full_margin if is_home_pick else -full_margin
                    covered = actual + spread
                    if covered > 0:
                        result = "W"
                    elif covered == 0:
                        result = "P"
                    else:
                        result = "L"

            elif bt in ("TOTAL", "ALT TOTAL"):
                parts = pk.split()
                if len(parts) >= 2:
                    direction = parts[0].lower()
                    try:
                        line = float(parts[1])
                    except ValueError:
                        continue
                    if direction == "over":
                        result = "W" if full_total > line else ("P" if full_total == line else "L")
                    else:
                        result = "W" if full_total < line else ("P" if full_total == line else "L")

        if result is None:
            continue

        if result == "W":
            if odds > 0:
                profit = float(odds)
            else:
                profit = 100 / abs(odds) * 100
            wins += 1
        elif result == "L":
            profit = -100.0
            losses += 1
        else:
            profit = 0.0

        conn.execute("""
            UPDATE nba_picks SET result = ?, profit = ?, settled_at = datetime('now')
            WHERE id = ?
        """, (result, round(profit, 2), pick["id"]))
        settled += 1

    conn.commit()

    return {
        "settled": settled,
        "wins": wins,
        "losses": losses,
        "pending_remaining": conn.execute(
            "SELECT COUNT(*) as c FROM nba_picks WHERE result IS NULL"
        ).fetchone()["c"],
    }
