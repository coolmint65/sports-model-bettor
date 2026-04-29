"""MLB pick settlement against final game results.

settle_picks() is the largest function in the tracker — per-bet-type
outcome math for every market the picker emits (ML / O/U / RL / NRFI /
F5 ML/O/U/RL / Phase-1 derivatives Team Total / F5 Team Total / Inning
Total / Inning BTS / 1st Inn Winner / F5 Winner / Total O/E / Extra
Innings).

Self-heal: rows pending >7 days auto-push so a missed settlement
(postponed game ID rewrite, late doubleheader reschedule) doesn't rot
the tracker forever.
"""

from __future__ import annotations
import json as _json
import logging
from datetime import datetime, timedelta

from ..db import get_conn, get_team_by_id
from ._helpers import _extract_closing_for_pick

logger = logging.getLogger(__name__)


def settle_picks() -> dict:
    """Settle all pending picks against final game results.
    First refreshes recent game scores from MLB API, then settles."""
    conn = get_conn()

    # Re-fetch recent game results so completed games are marked 'final'
    try:
        from scrapers.mlb_stats import fetch_schedule
        today = datetime.now().strftime("%Y-%m-%d")
        three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
        fetch_schedule(three_days_ago, today)
    except Exception as e:
        logger.warning("Could not refresh recent games: %s", e)

    # Self-heal: auto-push picks pending >7 days. The settler can fail
    # to match a row (postponed game ID rewrite, late-rescheduled
    # doubleheader, etc.) and without a sweep they rot in the tracker
    # forever, polluting WR% and the /api/_health stale-POTD check.
    stale_cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    stale = conn.execute(
        "SELECT id, date, matchup, bet_type, pick FROM picks "
        "WHERE result IS NULL AND date < ?", (stale_cutoff,),
    ).fetchall()
    auto_pushed = 0
    for row in stale:
        conn.execute(
            "UPDATE picks SET result='P', profit=0, settled_at=datetime('now') "
            "WHERE id=?", (row["id"],),
        )
        logger.warning(
            "settle_picks: auto-pushed stale pending pick id=%s date=%s "
            "%s %s/%s — older than 7 days, settler never matched",
            row["id"], row["date"], row["matchup"], row["bet_type"], row["pick"],
        )
        auto_pushed += 1
    if auto_pushed:
        conn.commit()

    pending = conn.execute(
        "SELECT * FROM picks WHERE result IS NULL"
    ).fetchall()

    if not pending:
        return {"settled": 0, "auto_pushed": auto_pushed,
                "message": "No pending picks"}

    # Fetch current odds once for closing line capture
    try:
        from ..picks import fetch_real_odds_for_games, match_odds
        all_closing_odds = fetch_real_odds_for_games()
    except Exception as e:
        # Settle proceeds with whatever closing_odds was previously
        # captured; log so HR outages don't silently rot CLV data.
        logger.warning(
            "settle_picks: failed to fetch live odds for CLV refresh "
            "(%s) — using whatever closing_odds was previously captured", e,
        )
        all_closing_odds = {}

    settled = 0
    wins = 0
    losses = 0

    for pick in pending:
        pick = dict(pick)
        game_id = pick["game_id"]
        bt_probe = (pick["bet_type"] or "").upper()

        # F5 + NRFI markets can settle before the full game ends -- once
        # the linescore has enough innings, the outcome is locked.
        f5_or_inning = bt_probe.startswith("F5") or bt_probe in ("1ST INN", "NRFI")
        if f5_or_inning:
            game = conn.execute(
                "SELECT * FROM games WHERE mlb_game_id = ?",
                (game_id,),
            ).fetchone()
        else:
            game = conn.execute(
                "SELECT * FROM games WHERE mlb_game_id = ? AND status = 'final'",
                (game_id,),
            ).fetchone()

        if not game:
            continue  # Game not loaded yet

        game = dict(game)

        # Postponed/cancelled games: skip until a real result is recorded.
        if game.get("status") == "postponed":
            continue
        if game.get("home_score") is None or game.get("away_score") is None:
            continue

        hs = game["home_score"]
        as_ = game["away_score"]
        total_runs = hs + as_

        home_team = get_team_by_id(game.get("home_team_id"))
        away_team = get_team_by_id(game.get("away_team_id"))
        h = home_team["abbreviation"] if home_team else ""
        a = away_team["abbreviation"] if away_team else ""

        # Capture closing odds if not already stored.
        if not pick.get("closing_odds") and h and a:
            try:
                from ..picks import match_odds as _match_odds
                game_odds = _match_odds(h, a, all_closing_odds)
                if game_odds:
                    bt_tmp = pick["bet_type"]
                    pk_tmp = pick["pick"]
                    closing = _extract_closing_for_pick(
                        bt_tmp, pk_tmp, h, game_odds,
                    )
                    if closing is not None:
                        conn.execute("UPDATE picks SET closing_odds = ? WHERE id = ?",
                                     (int(closing), pick["id"]))
                        pick["closing_odds"] = int(closing)
            except Exception as e:
                logger.debug("closing-odds capture failed for pick %s: %s",
                             pick.get("id"), e)

        result = None
        profit = 0
        bt = pick["bet_type"]
        pk = pick["pick"]
        odds = pick["odds"] or -110

        if bt in ("ml", "ML"):
            home_won = hs > as_
            if pk == h:
                won = home_won
            else:
                won = not home_won
            result = "W" if won else "L"

        elif bt in ("ou", "O/U", "ALT O/U"):
            # ALT O/U settles exactly like primary O/U — same "Over N.N"
            # / "Under N.N" pick label, just a different line value.
            if "Over" in pk:
                line = float(pk.split()[-1])
                if total_runs > line:
                    result = "W"
                elif total_runs < line:
                    result = "L"
                else:
                    result = "P"
            else:
                line = float(pk.split()[-1])
                if total_runs < line:
                    result = "W"
                elif total_runs > line:
                    result = "L"
                else:
                    result = "P"

        elif bt in ("nrfi", "1st INN"):
            # Use real linescore data when available. Only trust the 1st
            # inning when it's locked: either the game is final, or the
            # linescore already has a 2nd inning entry.
            home_ls = game.get("home_linescore")
            away_ls = game.get("away_linescore")
            status = (game.get("status") or "").lower()
            scoreless_1st = None
            if home_ls and away_ls:
                try:
                    h_inn = _json.loads(home_ls)
                    a_inn = _json.loads(away_ls)
                    full_innings = min(len(h_inn), len(a_inn))
                    first_inning_locked = (
                        status == "final" or full_innings >= 2
                    )
                    if first_inning_locked and full_innings >= 1:
                        scoreless_1st = (h_inn[0] == 0 and a_inn[0] == 0)
                except Exception:
                    pass
            if scoreless_1st is None:
                # Fall back to total-runs heuristic only when the game is
                # final and linescore was unavailable.
                if status == "final":
                    scoreless_1st = total_runs <= 6
                else:
                    continue  # 1st inning still in progress

            if pk == "NRFI":
                result = "W" if scoreless_1st else "L"
            else:
                result = "W" if not scoreless_1st else "L"

        elif bt in ("rl", "RL", "ALT RL"):
            # ALT RL settles identically to primary RL.
            parts = pk.split()
            pick_team = parts[0] if parts else ""
            spread = float(parts[1]) if len(parts) > 1 else 1.5

            if pick_team == h:
                team_margin = hs - as_
            else:
                team_margin = as_ - hs

            if team_margin + spread > 0:
                result = "W"
            elif team_margin + spread == 0:
                result = "P"
            else:
                result = "L"

        elif bt.upper().startswith("F5") and bt not in ("F5 Team Total", "F5 Winner"):
            # First-5-innings markets (F5 ML / F5 O/U / F5 RL).
            h_inn: list = []
            a_inn: list = []
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                pass
            status = (game.get("status") or "").lower()
            full_innings = min(len(h_inn), len(a_inn))
            if status == "final":
                if full_innings < 5:
                    continue
            else:
                if full_innings < 6:
                    continue

            f5_home = sum(h_inn[:5])
            f5_away = sum(a_inn[:5])
            f5_total = f5_home + f5_away
            f5_margin = f5_home - f5_away
            sub = bt.upper()[2:].strip()

            if sub == "ML":
                if f5_margin == 0:
                    result = "P"
                elif pk == h:
                    result = "W" if f5_margin > 0 else "L"
                else:
                    result = "W" if f5_margin < 0 else "L"

            elif sub in ("O/U", "OU"):
                parts = pk.split()
                try:
                    line = float(parts[-1])
                except (ValueError, IndexError):
                    continue
                is_over = any(p.lower() == "over" for p in parts)
                if f5_total > line:
                    result = "W" if is_over else "L"
                elif f5_total < line:
                    result = "L" if is_over else "W"
                else:
                    result = "P"

            elif sub == "RL":
                parts = pk.split()
                pick_team = parts[0] if parts else ""
                try:
                    spread = float(parts[1]) if len(parts) > 1 else 0.5
                except ValueError:
                    spread = 0.5
                if pick_team == h:
                    team_margin = f5_margin
                else:
                    team_margin = -f5_margin
                if team_margin + spread > 0:
                    result = "W"
                elif team_margin + spread == 0:
                    result = "P"
                else:
                    result = "L"

        # ── Phase 1 derivative markets ──
        elif bt == "Team Total":
            if (game.get("status") or "").lower() != "final":
                continue
            parts = pk.split()
            if len(parts) >= 3:
                pick_team = parts[0]
                direction = parts[1].lower()
                try:
                    line = float(parts[2])
                except ValueError:
                    continue
                team_runs = hs if pick_team == h else as_
                if team_runs > line:
                    result = "W" if direction == "over" else "L"
                elif team_runs < line:
                    result = "L" if direction == "over" else "W"
                else:
                    result = "P"

        elif bt == "F5 Team Total":
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                continue
            status = (game.get("status") or "").lower()
            full_innings = min(len(h_inn), len(a_inn))
            if status == "final":
                if full_innings < 5:
                    continue
            elif full_innings < 6:
                continue
            parts = pk.split()
            if len(parts) >= 4:
                pick_team = parts[0]
                direction = parts[2].lower()
                try:
                    line = float(parts[3])
                except ValueError:
                    continue
                team_f5 = sum(h_inn[:5]) if pick_team == h else sum(a_inn[:5])
                if team_f5 > line:
                    result = "W" if direction == "over" else "L"
                elif team_f5 < line:
                    result = "L" if direction == "over" else "W"
                else:
                    result = "P"

        elif bt == "Inning Total":
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                continue
            parts = pk.split()
            if len(parts) >= 4:
                try:
                    inning_n = int(parts[1])
                    line = float(parts[3])
                except ValueError:
                    continue
                direction = parts[2].lower()
                status = (game.get("status") or "").lower()
                full_innings = min(len(h_inn), len(a_inn))
                if not (status == "final" or full_innings > inning_n):
                    continue
                if inning_n < 1 or inning_n > full_innings:
                    continue
                inn_total = h_inn[inning_n - 1] + a_inn[inning_n - 1]
                if inn_total > line:
                    result = "W" if direction == "over" else "L"
                elif inn_total < line:
                    result = "L" if direction == "over" else "W"
                else:
                    result = "P"

        elif bt == "Inning BTS":
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                continue
            parts = pk.split()
            if len(parts) >= 4:
                try:
                    inning_n = int(parts[1])
                except ValueError:
                    continue
                direction = parts[3].lower()
                status = (game.get("status") or "").lower()
                full_innings = min(len(h_inn), len(a_inn))
                if not (status == "final" or full_innings > inning_n):
                    continue
                if inning_n < 1 or inning_n > full_innings:
                    continue
                bts_yes = (h_inn[inning_n - 1] > 0 and a_inn[inning_n - 1] > 0)
                if direction == "yes":
                    result = "W" if bts_yes else "L"
                else:
                    result = "W" if not bts_yes else "L"

        elif bt == "1st Inn Winner":
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                continue
            status = (game.get("status") or "").lower()
            full_innings = min(len(h_inn), len(a_inn))
            if not (status == "final" or full_innings >= 2):
                continue
            if full_innings < 1:
                continue
            h1 = h_inn[0]
            a1 = a_inn[0]
            parts = pk.split()
            if len(parts) >= 3:
                pick_label = parts[2]
                if pick_label.lower() == "tie":
                    result = "W" if h1 == a1 else "L"
                elif pick_label == h:
                    result = "W" if h1 > a1 else "L"
                else:
                    result = "W" if a1 > h1 else "L"

        elif bt == "F5 Winner":
            # 3-way: tie is a winnable outcome.
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                continue
            status = (game.get("status") or "").lower()
            full_innings = min(len(h_inn), len(a_inn))
            if status == "final":
                if full_innings < 5:
                    continue
            elif full_innings < 6:
                continue
            f5_home = sum(h_inn[:5])
            f5_away = sum(a_inn[:5])
            parts = pk.split()
            if len(parts) >= 2:
                pick_label = parts[1]
                if pick_label.lower() == "tie":
                    result = "W" if f5_home == f5_away else "L"
                elif pick_label == h:
                    result = "W" if f5_home > f5_away else "L"
                else:
                    result = "W" if f5_away > f5_home else "L"

        elif bt == "Total O/E":
            if (game.get("status") or "").lower() != "final":
                continue
            parts = pk.split()
            if len(parts) >= 2:
                direction = parts[1].lower()
                is_odd = (total_runs % 2 == 1)
                if direction == "odd":
                    result = "W" if is_odd else "L"
                else:
                    result = "W" if not is_odd else "L"

        elif bt == "Extra Innings":
            if (game.get("status") or "").lower() != "final":
                continue
            try:
                h_inn = _json.loads(game.get("home_linescore") or "[]")
                a_inn = _json.loads(game.get("away_linescore") or "[]")
            except Exception:
                continue
            went_extras = max(len(h_inn), len(a_inn)) > 9
            parts = pk.split()
            if len(parts) >= 3:
                direction = parts[2].lower()
                if direction == "yes":
                    result = "W" if went_extras else "L"
                else:
                    result = "W" if not went_extras else "L"

        if result is None:
            continue  # Could not determine result - skip

        if result == "W":
            profit = (odds if odds > 0 else 100 / abs(odds) * 100)
            wins += 1
        elif result == "L":
            profit = -100
            losses += 1
        else:
            profit = 0  # Push

        conn.execute("""
            UPDATE picks SET result = ?, profit = ?, settled_at = datetime('now')
            WHERE id = ?
        """, (result, round(profit, 2), pick["id"]))
        settled += 1

    conn.commit()

    # Auto-refresh empirical calibration after settling so the model
    # learns from its latest outcomes immediately.
    if settled > 0:
        try:
            from ..empirical_calibration import refresh_calibration
            refresh_calibration("mlb")
        except Exception as e:
            logger.debug("post-settle calibration refresh failed: %s", e)

    return {
        "settled": settled,
        "wins": wins,
        "losses": losses,
        "pending_remaining": conn.execute(
            "SELECT COUNT(*) as c FROM picks WHERE result IS NULL"
        ).fetchone()["c"],
    }
