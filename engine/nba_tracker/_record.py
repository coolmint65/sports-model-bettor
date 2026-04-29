"""NBA recording: refresh-pending, record-picks, capture-closing-odds.

Q1/Full split: refresh keys per (matchup, family) so Q1_TOTAL never
morphs into TOTAL on refresh. Record writes one row per (Q1, Full)
family per game.
"""

from __future__ import annotations
import logging
from datetime import datetime

from ._helpers import (
    _core_picks, _extract_nba_closing_for_pick,
    _normalize_espn_abbr,
)
from ._scoreboard import _fetch_nba_scoreboard

logger = logging.getLogger(__name__)


def capture_closing_odds() -> int:
    """Snapshot current Hard Rock NBA Q1 odds for all pending picks.

    Call before games tip off (sync script ~10 min pre-tip) so the
    CLV computed at settle time reflects the true closing line. The
    inline capture inside settle_picks() can't see Q1 lines because
    HR drops the Q1 markets the moment Q1 ends.

    Returns number of picks updated.
    """
    from ..nba_db import get_conn
    conn = get_conn()
    pending = conn.execute(
        "SELECT id, matchup, bet_type, pick FROM nba_picks "
        "WHERE result IS NULL AND closing_odds IS NULL"
    ).fetchall()
    if not pending:
        return 0

    try:
        from scrapers.hardrock_odds import fetch_nba as _hr_nba
        all_odds = _hr_nba() or {}
    except Exception as e:
        logger.warning("NBA closing capture: HR fetch failed: %s", e)
        return 0
    if not all_odds:
        return 0

    from ..picks import match_odds as _match_odds
    updated = 0
    for pick in pending:
        matchup = pick["matchup"]
        sep = " @ " if " @ " in matchup else "@"
        parts = matchup.split(sep)
        if len(parts) != 2:
            continue
        away, home = parts[0].strip(), parts[1].strip()
        game_odds = _match_odds(home, away, all_odds)
        if not game_odds:
            continue
        closing = _extract_nba_closing_for_pick(
            pick["bet_type"], pick["pick"], home, game_odds,
        )
        if closing is not None:
            conn.execute(
                "UPDATE nba_picks SET closing_odds = ? WHERE id = ?",
                (int(closing), pick["id"]),
            )
            updated += 1

    conn.commit()
    logger.info("NBA closing capture: %d/%d pending picks updated", updated, len(pending))
    return updated


def refresh_pending_for_today(bets: list[dict],
                               target_date: str | None = None) -> dict:
    """NBA twin of engine.tracker.refresh_pending_for_today. See that
    docstring for the design rationale."""
    from ..nba_db import get_conn as _conn
    target_date = target_date or datetime.now().strftime("%Y-%m-%d")
    conn = _conn()
    locked_matchups: set[str] = set()
    from datetime import datetime as _dt, timezone as _tz
    now_utc = _dt.now(_tz.utc)
    def _bet_started(bet: dict) -> bool:
        if bet.get("is_locked"):
            return True
        t = bet.get("time") or bet.get("date") or ""
        if not isinstance(t, str) or not t:
            return False
        try:
            s = t.replace("Z", "+00:00") if t.endswith("Z") else t
            ts = _dt.fromisoformat(s)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=_tz.utc)
            return ts < now_utc
        except (ValueError, TypeError):
            return False

    # Phase 2k: track current pick PER (matchup, bet_type family).
    current_by_key: dict[tuple, dict] = {}
    Q1_TYPES = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
    FULL_TYPES = {"ML", "SPREAD", "TOTAL", "ALT SPREAD", "ALT TOTAL"}
    def _family(bt: str) -> str:
        if bt in Q1_TYPES:   return "q1"
        if bt in FULL_TYPES: return "full"
        return "other"

    for b in bets:
        if _bet_started(b):
            locked_matchups.add(b["matchup"])
            continue
        bq = b.get("best_pick_q1")
        bf = b.get("best_pick_full")
        if bq:
            current_by_key[(b["matchup"], "q1")] = bq
        if bf:
            current_by_key[(b["matchup"], "full")] = bf
        # Pre-2k bets without per-view picks fall back to legacy best_pick.
        if not bq and not bf:
            bp = b.get("best_pick")
            if bp:
                fam = _family(bp.get("type") or "")
                current_by_key[(b["matchup"], fam)] = bp

    pending = conn.execute(
        "SELECT id, matchup, bet_type, pick, game_id FROM nba_picks "
        "WHERE date = ? AND result IS NULL",
        (target_date,),
    ).fetchall()

    def _pick_game_started(game_id) -> bool:
        if not game_id:
            return False
        row = conn.execute(
            "SELECT date, status FROM nba_games WHERE game_id = ? LIMIT 1",
            (str(game_id),),
        ).fetchone()
        if not row:
            return False
        if row["status"] in ("live", "final", "postponed"):
            return True
        try:
            return str(row["date"]) < target_date
        except Exception:
            return False

    updated = swapped = voided = 0
    for p in pending:
        p = dict(p)
        if p["matchup"] in locked_matchups:
            continue
        if _pick_game_started(p.get("game_id")):
            continue
        # Match this pending row to the SAME family's current best pick.
        # Q1_TOTAL never gets morphed into TOTAL — they're distinct bets.
        fam = _family(p["bet_type"] or "")
        current = current_by_key.get((p["matchup"], fam))
        if not current:
            matchup_in_response = any(b["matchup"] == p["matchup"] for b in bets)
            if matchup_in_response:
                conn.execute("DELETE FROM nba_picks WHERE id = ?", (p["id"],))
                voided += 1
            continue

        if current.get("type") != p["bet_type"] or current.get("pick") != p["pick"]:
            conn.execute(
                "UPDATE nba_picks SET bet_type = ?, pick = ?, model_prob = ?, "
                "edge = ?, odds = ? WHERE id = ?",
                (current.get("type"), current.get("pick"),
                 current.get("prob"), current.get("edge"),
                 current.get("odds"), p["id"]),
            )
            swapped += 1
        else:
            conn.execute(
                "UPDATE nba_picks SET model_prob = ?, edge = ?, odds = ? "
                "WHERE id = ?",
                (current.get("prob"), current.get("edge"),
                 current.get("odds"), p["id"]),
            )
            updated += 1

    conn.commit()
    return {"updated": updated, "swapped": swapped, "voided": voided}


def record_picks(date: str | None = None, min_edge: float = 1.5,
                 force: bool = False) -> list[dict]:
    """Run NBA Q1 + Full models on today's games and record best per
    family per game.

    Args:
        date: Target date (YYYY-MM-DD). Defaults to today.
        min_edge: Minimum edge percentage to record a pick.
        force: If True, delete any unsettled pick for each game before
            recording so the latest model/odds take precedence.

    Returns:
        List of recorded pick dicts.
    """
    from ..nba_db import get_conn

    conn = get_conn()
    target_date = date or datetime.now().strftime("%Y-%m-%d")

    from ..nba_q1_predict import generate_q1_picks

    events = _fetch_nba_scoreboard(target_date)
    if not events:
        logger.info("No NBA games found for %s", target_date)
        return []

    q1_odds_map = {}
    try:
        from scrapers.hardrock_odds import fetch_nba as _hr_nba
        q1_odds_map = _hr_nba()
        if q1_odds_map:
            logger.info("NBA tracker: %d games from Hard Rock", len(q1_odds_map))
    except Exception as e:
        logger.debug("NBA tracker Hard Rock failed: %s", e)
    if not q1_odds_map:
        try:
            from scrapers.nba_odds import fetch_all_nba_odds
            q1_odds_map = fetch_all_nba_odds()
        except Exception as e:
            logger.debug("NBA Q1 odds fallback failed: %s", e)

    recorded = []

    for event in events:
        game_id = event.get("id", "")
        comp = event.get("competitions", [{}])[0]

        status = comp.get("status", {}).get("type", {})
        if status.get("completed", False):
            continue

        competitors = comp.get("competitors", [])
        h_abbr = ""
        a_abbr = ""
        for c in competitors:
            team = c.get("team", {})
            abbr = team.get("abbreviation", "")
            abbr = _normalize_espn_abbr(abbr)
            if c.get("homeAway") == "home":
                h_abbr = abbr
            else:
                a_abbr = abbr

        if not h_abbr or not a_abbr:
            continue

        matchup = f"{a_abbr} @ {h_abbr}"

        if force:
            conn.execute("DELETE FROM nba_picks WHERE game_id = ? "
                         "AND result IS NULL", (game_id,))
        else:
            existing = conn.execute(
                "SELECT COUNT(*) as c FROM nba_picks WHERE game_id = ?",
                (game_id,)
            ).fetchone()["c"]
            if existing > 0:
                continue

        # Read from shared picks store (same picks the card shows).
        picks = None
        try:
            from backend.server import _picks_store_get
            stored = _picks_store_get("nba", h_abbr, a_abbr)
            if stored and stored.get("picks"):
                picks = stored["picks"]
        except Exception as e:
            logger.debug("NBA picks_store fetch failed for %s: %s", matchup, e)

        if not picks:
            from engine.picks import match_odds as _match_odds
            odds_dict = _match_odds(h_abbr, a_abbr, q1_odds_map)
            picks = generate_q1_picks(h_abbr, a_abbr, odds_dict)
        if not picks:
            continue

        core = _core_picks(picks)
        if not core:
            continue
        from ..nba_picks import _valid_odds as _nba_valid

        Q1_TYPES = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
        FULL_PRIMARY = {"ML", "SPREAD", "TOTAL"}

        q1_picks = [p for p in core if p.get("type") in Q1_TYPES]
        full_primary = [p for p in core if p.get("type") in FULL_PRIMARY]
        # Primary-only on the tracker. ALT lines stay generated for
        # the picks list but don't become recorded picks until backtest
        # validates them.
        full_picks = full_primary

        for family_picks, label in ((q1_picks, "Q1"), (full_picks, "Full")):
            if not family_picks:
                continue
            best = max(family_picks, key=lambda p: p.get("edge", 0))
            if best["edge"] < min_edge:
                continue
            if not _nba_valid(best.get("odds")):
                logger.warning("Skipping NBA %s pick with invalid odds=%s for %s",
                               label, best.get("odds"), matchup)
                continue
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO nba_picks (
                        game_id, date, matchup, bet_type, pick,
                        model_prob, edge, odds
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (game_id, target_date, matchup, best["type"], best["pick"],
                      best["prob"], best["edge"], best["odds"]))
            except Exception as e:
                logger.warning("nba_picks insert failed for %s/%s: %s",
                               matchup, best["type"], e)
                continue
            recorded.append({
                "matchup": matchup, "type": best["type"],
                "pick": best["pick"], "prob": round(best["prob"], 3),
                "edge": round(best["edge"], 1), "odds": best["odds"],
            })

    conn.commit()
    return recorded
