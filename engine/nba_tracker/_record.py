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
from .._tz import et_today_str
from ._scoreboard import _fetch_nba_scoreboard

logger = logging.getLogger(__name__)


def capture_closing_odds() -> int:
    """Snapshot current Hard Rock NBA odds for all pending picks.
    Thin wrapper around ``engine.tracker_core.core_capture_closing_odds``
    with the NBA adapter — see that module for the refresh semantics."""
    from ..nba_db import get_conn
    from scrapers.hardrock_odds import fetch_nba as _hr_nba
    from ..tracker_core import SportAdapter, core_capture_closing_odds
    adapter = SportAdapter(
        name="nba",
        get_conn=get_conn,
        picks_table="nba_picks",
        hr_fetch=_hr_nba,
        extract_closing=_extract_nba_closing_for_pick,
    )
    return core_capture_closing_odds(adapter)


def refresh_pending_for_today(bets: list[dict],
                               target_date: str | None = None) -> dict:
    """NBA twin of engine.tracker.refresh_pending_for_today. See that
    docstring for the design rationale."""
    from ..nba_db import get_conn as _conn
    target_date = target_date or et_today_str()
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

    # Lock-at-game-start rule (revised 2026-04-29 — see engine.tracker
    # for the design): prematch picks may swap bet_type / pick within
    # the same family freely; live/final/postponed games stay frozen
    # via the locked_matchups + _pick_game_started gates above.
    # pick_events keeps the breadcrumb of every model decision
    # regardless of whether the picks row mutates.
    updated = swapped = voided = 0
    for p in pending:
        p = dict(p)
        if p["matchup"] in locked_matchups:
            continue
        if _pick_game_started(p.get("game_id")):
            continue
        fam = _family(p["bet_type"] or "")
        current = current_by_key.get((p["matchup"], fam))
        # 'skip'-tier picks are below the lean floor; card filter
        # already hides them. Void the tracker row to match.
        if current and (current.get("confidence") or "lean") == "skip":
            current = None
        if not current:
            matchup_in_response = any(b["matchup"] == p["matchup"] for b in bets)
            if matchup_in_response:
                conn.execute("DELETE FROM nba_picks WHERE id = ?", (p["id"],))
                voided += 1
            continue

        if current.get("type") != p["bet_type"] or current.get("pick") != p["pick"]:
            # Prematch swap — overwrite bet_type, pick, and price.
            # closing_odds resets to NULL because the new pick has its
            # own line.
            conn.execute(
                "UPDATE nba_picks SET bet_type = ?, pick = ?, "
                "  model_prob = ?, edge = ?, odds = ?, closing_odds = NULL "
                "WHERE id = ?",
                (current.get("type"), current.get("pick"),
                 current.get("prob"), current.get("edge"),
                 current.get("odds"), p["id"]),
            )
            swapped += 1
            continue

        # Same pick — refresh the live numbers so the card shows the
        # current line / edge / prob.
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
    target_date = date or et_today_str()

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
            # Skip the game ONLY if both pick families (Q1 + Full) are
            # already recorded. The earlier "any pick exists → skip"
            # optimization left the second-recorded family permanently
            # unrecorded (Q1 lands first → Full pick never inserted),
            # which is why the cards showed picks the tracker didn't.
            # INSERT OR IGNORE dedupes per (date, game_id, bet_type)
            # so re-running doesn't pile up duplicates.
            existing_families = {
                "q1": False,
                "full": False,
            }
            # Only count PENDING picks toward "already recorded" — a
            # settled/voided row from a prior day would otherwise
            # permanently lock out re-records (e.g. OKC @ SA 2026-05-28:
            # stale 5/27 Q1_TOTAL row made the guard think Q1 was
            # already handled even though it needed a fresh insert).
            for r in conn.execute(
                "SELECT DISTINCT bet_type FROM nba_picks "
                "WHERE game_id = ? AND result IS NULL",
                (game_id,),
            ).fetchall():
                bt = r["bet_type"] or ""
                if bt.startswith("Q1") or bt.startswith("Q1 ALT"):
                    existing_families["q1"] = True
                elif bt in ("ML", "SPREAD", "TOTAL",
                             "ALT SPREAD", "ALT TOTAL"):
                    existing_families["full"] = True
            if all(existing_families.values()):
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

        # Q1 + Full families now include ALT lines. ALTs already pass
        # the same hardened gates (edge floors, Bayesian calibration,
        # belief gate, edge ceiling, per-direction min-edge improvement
        # vs primary). The earlier "primary-only on tracker" rule meant
        # the card's headline ALT pick (e.g. DET -11.5) silently fell
        # back to the best primary (DET -8.5) in the tracker — card vs
        # tracker drift. Tracker should mirror what the card shows.
        Q1_TYPES = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL",
                     "Q1 ALT SPREAD", "Q1 ALT TOTAL"}
        FULL_TYPES = {"ML", "SPREAD", "TOTAL",
                       "ALT SPREAD", "ALT TOTAL"}

        q1_picks = [p for p in core if p.get("type") in Q1_TYPES]
        full_picks = [p for p in core if p.get("type") in FULL_TYPES]

        for family_picks, label, family_types in (
            (q1_picks, "Q1", Q1_TYPES),
            (full_picks, "Full", FULL_TYPES),
        ):
            if not family_picks:
                continue
            # Stake-aware selection. Edge alone surfaces the top-edge
            # pick, but Quarter-Kelly sizing already accounts for the
            # juice — a 16.5% edge on a -180 chalk gets 0u (skip) while
            # a 16.3% edge on a -105 line gets 0.5u (real stake). When
            # two candidates are within 1pp edge, prefer the one with
            # the larger stake_units recommendation. User flagged NY @
            # CLE 2026-05-25 — Q1_SPREAD NY +2.5 @ -180 (16.5%, 0u)
            # beat Q1_ML CLE @ -105 (16.3%, 0.5u) on edge alone, so the
            # card showed a 0u pick. With this tiebreak the 0.5u pick
            # wins. We keep edge as the primary ordering and only let
            # stake override within a narrow edge-equivalence band.
            EDGE_BAND = 1.0
            top_edge = max(p.get("edge", 0) or 0 for p in family_picks)
            near_top = [
                p for p in family_picks
                if (p.get("edge", 0) or 0) >= (top_edge - EDGE_BAND)
            ]
            best = max(
                near_top,
                key=lambda p: (
                    (p.get("stake_units") or 0),
                    (p.get("edge") or 0),
                ),
            )
            if best["edge"] < min_edge:
                continue
            if not _nba_valid(best.get("odds")):
                logger.warning("Skipping NBA %s pick with invalid odds=%s for %s",
                               label, best.get("odds"), matchup)
                continue
            # Void any prior pending pick in the SAME FAMILY for this
            # game that isn't this new best. Original scope included
            # `date = ?` which let stale picks from PRIOR days survive
            # (OKC @ SA 5/27 Q1 pick was still pending on 5/28+ because
            # the void filter only looked at today's date). Dropped the
            # date filter so any pending pick in the family gets cleared,
            # regardless of which day it was recorded.
            placeholders = ','.join('?' * len(family_types))
            conn.execute(
                f"""
                UPDATE nba_picks
                   SET result='V', profit=0,
                       settled_at=datetime('now')
                 WHERE game_id = ?
                   AND bet_type IN ({placeholders})
                   AND result IS NULL
                   AND NOT (bet_type = ? AND pick = ?)
                """,
                (game_id, *family_types,
                 best["type"], best["pick"]),
            )
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO nba_picks (
                        game_id, date, matchup, bet_type, pick,
                        model_prob, edge, odds, stake_units
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (game_id, target_date, matchup, best["type"], best["pick"],
                      best["prob"], best["edge"], best["odds"],
                      best.get("stake_units")))
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

    # Write-through to unified picks store (#160 / #163).
    try:
        from ..unified_tracker import sync_for_date
        sync_for_date("nba", target_date)
    except Exception as e:
        logger.debug("unified write-through (nba) skipped: %s", e)

    # Phase-2 cutover: dual-write into picks_unified (canonical store).
    try:
        from ..picks_unified._legacy_bridge import mirror_to_unified
        rows = conn.execute(
            "SELECT * FROM nba_picks WHERE date=? ORDER BY id DESC LIMIT 200",
            (target_date,),
        ).fetchall()
        for r in rows:
            d = dict(r)
            mirror_to_unified(
                sport="nba", league="nba",
                native_game_id=d.get("game_id"),
                pick_date=d.get("date") or target_date,
                matchup=d.get("matchup") or "",
                bet_type=d.get("bet_type") or "",
                pick_text=d.get("pick") or "",
                odds=int(d.get("odds") or 0),
                prob=float(d.get("model_prob") or 0.0),
                edge_pct=float(d.get("edge") or 0.0),
                stake_units=float(d.get("stake_units") or 0.0),
                closing_odds=d.get("closing_odds"),
                result=d.get("result"),
                profit=d.get("profit"),
                settled_at=d.get("settled_at"),
            )
    except Exception as e:
        logger.debug("picks_unified mirror (nba) skipped: %s", e)

    return recorded
