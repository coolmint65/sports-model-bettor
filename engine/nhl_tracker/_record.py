"""NHL recording: refresh-pending, record-picks, capture-closing-odds."""

from __future__ import annotations
import json
import logging
from datetime import datetime

from ._helpers import _core_picks, _get_nhl_db, _extract_nhl_closing_for_pick
from ._scoreboard import _fetch_nhl_scoreboard
from .._tz import et_today_str

logger = logging.getLogger(__name__)


def capture_closing_odds() -> int:
    """Snapshot current Hard Rock NHL odds for all pending picks.
    Thin wrapper around ``engine.tracker_core.core_capture_closing_odds``
    with the NHL adapter — see that module for the refresh semantics."""
    from scrapers.hardrock_odds import fetch_nhl as _hr_nhl
    from ..tracker_core import SportAdapter, core_capture_closing_odds
    adapter = SportAdapter(
        name="nhl",
        get_conn=_get_nhl_db,
        picks_table="nhl_picks",
        hr_fetch=_hr_nhl,
        extract_closing=_extract_nhl_closing_for_pick,
    )
    return core_capture_closing_odds(adapter)


def refresh_pending_for_today(bets: list[dict],
                               target_date: str | None = None) -> dict:
    """NHL twin of engine.tracker.refresh_pending_for_today. See that
    docstring for the design rationale."""
    target_date = target_date or et_today_str()
    conn = _get_nhl_db()
    current_by_matchup: dict[str, dict] = {}
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

    for b in bets:
        if _bet_started(b):
            locked_matchups.add(b["matchup"])
            continue
        bp = b.get("best_pick")
        if bp:
            current_by_matchup[b["matchup"]] = bp

    pending = conn.execute(
        "SELECT id, matchup, bet_type, pick, game_id FROM nhl_picks "
        "WHERE date = ? AND result IS NULL",
        (target_date,),
    ).fetchall()

    def _pick_game_started(game_id) -> bool:
        if not game_id:
            return False
        row = conn.execute(
            "SELECT date, status FROM nhl_games WHERE game_id = ? LIMIT 1",
            (game_id,),
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
    # for the design): prematch picks may swap freely; live/final
    # stays frozen via the gates above.
    updated = swapped = voided = 0
    for p in pending:
        p = dict(p)
        if p["matchup"] in locked_matchups:
            continue
        if _pick_game_started(p.get("game_id")):
            continue
        current = current_by_matchup.get(p["matchup"])
        # 'skip'-tier picks are below the lean floor — the card filter
        # already hides them. Treat them the same as "no current best"
        # so the tracker row gets voided too. Without this the tracker
        # ends up with phantom rows for picks the user can't see on
        # the card (UTA ML edge=3.4% bug, 2026-04-30).
        if current and (current.get("confidence") or "lean") == "skip":
            current = None
        if not current:
            matchup_in_response = any(b["matchup"] == p["matchup"] for b in bets)
            if matchup_in_response:
                conn.execute("DELETE FROM nhl_picks WHERE id = ?", (p["id"],))
                voided += 1
            continue

        if current.get("type") != p["bet_type"] or current.get("pick") != p["pick"]:
            # Prematch swap — overwrite bet_type, pick, and price.
            # closing_odds resets because the new pick has its own line.
            conn.execute(
                "UPDATE nhl_picks SET bet_type = ?, pick = ?, "
                "  model_prob = ?, edge = ?, odds = ?, closing_odds = NULL "
                "WHERE id = ?",
                (current.get("type"), current.get("pick"),
                 current.get("prob"), current.get("edge"),
                 current.get("odds"), p["id"]),
            )
            swapped += 1
            continue

        conn.execute(
            "UPDATE nhl_picks SET model_prob = ?, edge = ?, odds = ? "
            "WHERE id = ?",
            (current.get("prob"), current.get("edge"),
             current.get("odds"), p["id"]),
        )
        updated += 1

    conn.commit()
    return {"updated": updated, "swapped": swapped, "voided": voided}


def record_picks(date: str | None = None, min_edge: float = 1.5,
                 force: bool = False) -> list[dict]:
    """Run NHL model on today's games and record the best pick per game."""
    conn = _get_nhl_db()
    target_date = date or et_today_str()

    # Note: when ``force=True``, deletion happens PER-GAME inside the
    # event loop (not as a blanket pre-DELETE here). This is intentional —
    # the loop skips completed games via ``status.completed`` and the
    # "match HR slate" filter, so per-game delete naturally scopes the
    # destructive op to games we're actually re-recording. Bug surfaced
    # on the MLB twin 2026-05-03 when blanket pre-DELETE wiped picks
    # for live games and the loop's filter prevented re-insertion.

    from engine.nhl_predict import generate_nhl_picks
    from engine.data import list_teams, load_team

    _ALT_ABBRS = {
        "TBL": "TB", "TB": "TBL", "NJD": "NJ", "NJ": "NJD",
        "SJS": "SJ", "SJ": "SJS", "LAK": "LA", "LA": "LAK",
        "WSH": "WAS", "WAS": "WSH", "CBJ": "CLB", "CLB": "CBJ",
        "MTL": "MON", "MON": "MTL", "NSH": "NAS", "NAS": "NSH",
        "UTA": "UTAH", "UTAH": "UTA",
    }
    key_map = {}
    for t in list_teams("NHL"):
        team = load_team("NHL", t["key"])
        if team:
            abbr = team.get("abbreviation", "")
            if abbr:
                key_map[abbr] = t["key"]
                alt = _ALT_ABBRS.get(abbr)
                if alt:
                    key_map[alt] = t["key"]

    events = _fetch_nhl_scoreboard(target_date)
    if not events:
        logger.info("No NHL games found for %s", target_date)
        return []

    odds_map = {}
    try:
        from scrapers.hardrock_odds import fetch_nhl as _hr_nhl
        hr_odds = _hr_nhl()
        if hr_odds:
            odds_map = hr_odds
            logger.info("NHL tracker: %d games from Hard Rock", len(odds_map))
    except Exception as e:
        logger.warning("NHL tracker Hard Rock failed: %s", e)

    recorded = []
    for event in events:
        game_id = event.get("id", "")
        comps = event.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]

        status = comp.get("status", {}).get("type", {})
        if status.get("completed", False):
            continue

        competitors = comp.get("competitors", [])
        h_abbr = ""
        a_abbr = ""
        for c in competitors:
            team = c.get("team", {})
            abbr = team.get("abbreviation", "")
            if c.get("homeAway") == "home":
                h_abbr = abbr
            else:
                a_abbr = abbr

        if not h_abbr or not a_abbr:
            continue

        matchup = f"{a_abbr} @ {h_abbr}"

        _ALT = {
            "TB": "TBL", "TBL": "TB", "NJ": "NJD", "NJD": "NJ",
            "SJ": "SJS", "SJS": "SJ", "LA": "LAK", "LAK": "LA",
            "WAS": "WSH", "WSH": "WAS", "CLB": "CBJ", "CBJ": "CLB",
            "MON": "MTL", "MTL": "MON", "NAS": "NSH", "NSH": "NAS",
        }
        h_key = key_map.get(h_abbr) or key_map.get(_ALT.get(h_abbr, ""))
        a_key = key_map.get(a_abbr) or key_map.get(_ALT.get(a_abbr, ""))
        if not h_key or not a_key:
            logger.warning("Could not find team keys for %s vs %s", a_abbr, h_abbr)
            continue

        # Read from shared picks store first (same picks the card shows)
        picks = None
        try:
            from backend.server import _picks_store_get
            stored = _picks_store_get("nhl", h_abbr, a_abbr)
            if stored and stored.get("picks"):
                picks = stored["picks"]
        except Exception as e:
            logger.debug("NHL picks_store fetch failed for %s: %s", matchup, e)

        if not picks:
            from engine.picks import match_odds as _match_odds
            game_odds = _match_odds(h_abbr, a_abbr, odds_map)
            picks = generate_nhl_picks(h_key, a_key, game_odds)
        if not picks:
            continue

        # Filter out derivatives — they go to engine.derivative_tracker
        # via the /api/best-bets recorder, not the main tracker.
        core = _core_picks(picks)
        if not core:
            continue
        from ..nhl_picks import _valid_odds as _nhl_valid

        # Per-game force-delete (replaces the old blanket pre-DELETE).
        # Only fires for games we're about to re-record (i.e. pre-game
        # AND on HR's slate), so completed/live games' pending rows
        # survive untouched.
        if force:
            conn.execute(
                "DELETE FROM nhl_picks WHERE result IS NULL AND date = ? "
                "AND (game_id = ? OR matchup = ?)",
                (target_date, game_id, matchup),
            )

        # Postponement cleanup: when an NHL game gets rescheduled to a
        # later date, the original date's pending pick stays orphaned
        # (settle never finds the game on the original date's
        # scoreboard). The recorder now sees a new pick for the SAME
        # game_id on a LATER date, so void any pending rows with the
        # same game_id from prior dates — they're stale duplicates.
        # Marks as 'V' rather than DELETEs so the audit log preserves
        # what was originally chosen vs the rescheduled rerun.
        conn.execute(
            "UPDATE nhl_picks SET result='V', "
            "  profit=0, settled_at=datetime('now') "
            "WHERE result IS NULL AND game_id = ? AND date < ?",
            (game_id, target_date),
        )

        # Record every core pick above min_edge — not just the top
        # one. NHL surfaces ML + O/U + PL on the same card commonly;
        # pre-fix only the highest-edge survived (cards showed picks
        # the tracker silently dropped). INSERT OR IGNORE dedupes
        # against the (date, game_id, bet_type, pick) unique index
        # so re-runs don't pile up.
        for pick in core:
            if pick.get("edge", 0) < min_edge:
                continue
            if not _nhl_valid(pick.get("odds")):
                logger.warning("Skipping NHL pick with invalid odds=%s for %s",
                               pick.get("odds"), matchup)
                continue
            conn.execute("""
                INSERT OR IGNORE INTO nhl_picks (
                    game_id, date, matchup, bet_type, pick,
                    model_prob, edge, odds, stake_units
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (game_id, target_date, matchup, pick["type"], pick["pick"],
                  pick["prob"], pick["edge"], pick["odds"],
                  pick.get("stake_units")))
            recorded.append({
                "matchup": matchup, "type": pick["type"],
                "pick": pick["pick"], "prob": round(pick["prob"], 3),
                "edge": round(pick["edge"], 1), "odds": pick["odds"],
            })

    conn.commit()

    # Write-through to unified picks store (#160 / #163).
    try:
        from ..unified_tracker import sync_for_date
        sync_for_date("nhl", target_date)
    except Exception as e:
        logger.debug("unified write-through (nhl) skipped: %s", e)

    # Phase-2 cutover: dual-write into picks_unified (canonical store).
    try:
        from ..picks_unified._legacy_bridge import mirror_to_unified
        rows = conn.execute(
            "SELECT * FROM nhl_picks WHERE date=? ORDER BY id DESC LIMIT 200",
            (target_date,),
        ).fetchall()
        for r in rows:
            d = dict(r)
            mirror_to_unified(
                sport="nhl", league="nhl",
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
        logger.debug("picks_unified mirror (nhl) skipped: %s", e)

    return recorded
