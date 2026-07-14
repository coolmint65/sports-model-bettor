"""Golf pick tracker + settler.

record_picks(tour, tournament_id):
    Persist the picks emitted by ``generate_picks`` into the per-tour
    picks table. Idempotent on (date, tournament_id, bet_type, pick)
    so re-running on the same slate is a no-op.

settle_picks(tour):
    Walk pending picks for ``tour``. When the underlying tournament
    flips to status='final' with the player's finishing position
    available, grade the pick:
        WINNER     final_position == 1                       → W else L
        TOP_5      final_position <= 5                       → W else L
        TOP_10     final_position <= 10                      → W else L
        TOP_20     final_position <= 20                      → W else L
        MAKE_CUT   made_cut == 1                             → W else L
    WD / DQ → L. Ties on the boundary (T5 when picking TOP_5) win
    cleanly because final_position is already the integer rank.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

from ._db import get_conn

logger = logging.getLogger(__name__)


def _payout(odds: int, won: bool) -> float:
    """$100 stake → dollar PnL."""
    if not won:
        return -100.0
    if odds > 0:
        return float(odds)
    if odds < 0:
        return round(100.0 * 100.0 / abs(odds), 2)
    return 0.0


def record_picks(tour: str, tournament_id: str,
                  picks: Iterable[dict]) -> dict:
    """Insert each pick into the per-tour picks table.

    Golf picks are suggestions, not auto-placed bets — the latest
    top-N for a tournament fully supersedes the prior top-N. When the
    incoming list is non-empty we drop any open picks for this
    tournament that aren't in the new set, so the tracker reflects the
    current best ideas rather than an accumulating history of every
    superseded draft.
    """
    conn = get_conn(tour)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    out = {"recorded": 0, "duplicate": 0, "errors": 0, "pruned": 0}

    picks_list = list(picks)

    # Prune obsolete open picks for this tournament. Guard with
    # "only when we have fresh picks" so a transient odds/predict
    # failure (empty list) doesn't wipe the tracker.
    if picks_list:
        keep = {(p["bet_type"], p["pick"]) for p in picks_list}
        existing = conn.execute(
            "SELECT id, bet_type, pick FROM picks "
            "WHERE tournament_id = ? AND result IS NULL",
            (tournament_id,),
        ).fetchall()
        stale_ids = [r["id"] for r in existing
                      if (r["bet_type"], r["pick"]) not in keep]
        if stale_ids:
            conn.executemany(
                "DELETE FROM picks WHERE id = ?",
                [(i,) for i in stale_ids],
            )
            out["pruned"] = len(stale_ids)

    for p in picks_list:
        try:
            conn.execute(
                "INSERT INTO picks "
                "(date, tournament_id, bet_type, player_id, pick, "
                " model_prob, edge, odds, stake_units) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (today, tournament_id, p["bet_type"],
                 int(p["player_id"]), p["pick"],
                 float(p.get("model_prob") or 0.0),
                 float(p.get("edge") or 0.0),
                 int(p.get("odds") or 0),
                 p.get("stake_units")),
            )
            out["recorded"] += 1
        except Exception as e:
            # Unique index violation = duplicate (re-running on same
            # slate). Any other exception is real.
            if "UNIQUE" in str(e).upper():
                out["duplicate"] += 1
            else:
                logger.warning("[golf:%s] record pick failed: %s", tour, e)
                out["errors"] += 1
    conn.commit()
    if out["recorded"] or out["pruned"]:
        logger.info("[golf:%s] recorded %d picks (%d dup, %d err, %d pruned)",
                    tour, out["recorded"], out["duplicate"],
                    out["errors"], out["pruned"])

    # Phase-2 cutover: dual-write into picks_unified (canonical store).
    try:
        from ..picks_unified._legacy_bridge import mirror_to_unified
        for p in picks_list:
            mirror_to_unified(
                sport="golf", league=tour,
                native_game_id=tournament_id,
                pick_date=today,
                matchup="",
                bet_type=p["bet_type"],
                pick_text=p["pick"],
                odds=int(p.get("odds") or 0),
                prob=float(p.get("model_prob") or 0.0),
                edge_pct=float(p.get("edge") or 0.0),
                stake_units=float(p.get("stake_units") or 0.0),
            )
    except Exception as e:
        logger.debug("picks_unified mirror (golf/%s) skipped: %s", tour, e)

    return out


# ── Settle ───────────────────────────────────────────────────

def _resolve_bet(bet_type: str, entry: dict | None) -> str | None:
    """Return 'W' / 'L' for one pick given the player's field_entry
    row. Returns None when the entry data isn't sufficient (still in
    progress, missing cut info)."""
    if not entry:
        return None
    if entry.get("withdrew") or entry.get("disqualified"):
        return "L"
    pos = entry.get("final_position")
    made_cut = entry.get("made_cut")
    bt = (bet_type or "").upper()
    if bt == "MAKE_CUT":
        if made_cut is None:
            return None
        return "W" if int(made_cut) == 1 else "L"
    # Position-based markets
    if pos is None:
        return None
    pos = int(pos)
    thresholds = {"WINNER": 1, "TOP_5": 5, "TOP_10": 10, "TOP_20": 20}
    threshold = thresholds.get(bt)
    if threshold is None:
        return None
    return "W" if pos <= threshold else "L"


def settle_picks(tour: str) -> dict:
    """Walk pending picks for ``tour``, grade any whose tournament is
    final. Returns ``{checked, settled, wins, losses, pushes}``."""
    conn = get_conn(tour)
    pending = conn.execute(
        "SELECT id, tournament_id, bet_type, player_id, odds "
        "FROM picks WHERE result IS NULL"
    ).fetchall()
    out = {"checked": len(pending), "settled": 0,
           "wins": 0, "losses": 0, "pushes": 0}
    if not pending:
        return out
    # Cache tournament status + per-player entries to avoid repeated
    # joins.
    tournament_cache: dict[str, str] = {}
    entry_cache: dict[tuple[str, int], dict] = {}
    for r in pending:
        tid = r["tournament_id"]
        if tid not in tournament_cache:
            row = conn.execute(
                "SELECT status FROM tournaments WHERE id = ?", (tid,)
            ).fetchone()
            tournament_cache[tid] = (row["status"] if row else "") or ""
        if tournament_cache[tid] != "final":
            continue
        pid = int(r["player_id"])
        cache_key = (tid, pid)
        if cache_key not in entry_cache:
            entry_row = conn.execute(
                "SELECT final_position, made_cut, withdrew, disqualified "
                "FROM field_entries "
                "WHERE tournament_id = ? AND player_id = ?",
                (tid, pid),
            ).fetchone()
            entry_cache[cache_key] = dict(entry_row) if entry_row else {}
        verdict = _resolve_bet(r["bet_type"], entry_cache[cache_key])
        if verdict is None:
            continue
        odds = int(r["odds"] or 0)
        profit = _payout(odds, verdict == "W")
        conn.execute(
            "UPDATE picks SET result = ?, profit = ?, settled_at = ? "
            "WHERE id = ?",
            (verdict, profit, datetime.utcnow().isoformat(), r["id"]),
        )
        out["settled"] += 1
        if verdict == "W":
            out["wins"] += 1
        else:
            out["losses"] += 1
    conn.commit()
    if out["settled"]:
        logger.info("[golf:%s] settled %d (W=%d L=%d)",
                    tour, out["settled"], out["wins"], out["losses"])
    return out


def list_history(tour: str, limit: int = 200) -> list[dict]:
    """Pending + settled picks for ``tour``, most-recent first. Joined
    with player + tournament names for the tracker UI.

    Row shape matches the shared ``PicksTable`` primitive: ``matchup``
    aliases tournament_name so the same primitive renders golf rows
    alongside NBA/NHL/MLB without per-sport forks."""
    conn = get_conn(tour)
    rows = conn.execute(
        "SELECT p.id, p.date, p.tournament_id, p.bet_type, p.pick, "
        "       p.model_prob, p.edge, p.odds, p.result, p.profit, "
        "       p.stake_units, "
        "       p.closing_odds, p.created_at, p.settled_at, "
        "       t.name AS tournament_name, t.start_date, t.end_date "
        "FROM picks p "
        "JOIN tournaments t ON t.id = p.tournament_id "
        "ORDER BY p.created_at DESC LIMIT ?",
        (int(limit),),
    ).fetchall()
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        d["matchup"] = d.get("tournament_name") or ""
        out.append(d)
    return out
