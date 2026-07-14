"""Per-pick decision log (provenance trace).

When a pick fires through ``picks_core.score_pick``, log a JSON blob
with the full decision trace:

  - Inputs:        sport, bet_type, raw_prob, odds
  - Pipeline:     juice_wall, odds_cap, belief_gate decisions
  - Calibration:  prob after empirical, after blend, after closing-line prior
  - Edge calc:    edge vs implied
  - Gate outcomes: edge_ceiling, edge_floor pass/fail
  - Final:        prob, edge, accepted (true/false), reject_reason

Why this exists (#186)
----------------------
Today the picks table records final pick + odds + result. When a pick
loses (or wins) we can see WHAT happened but not WHY. After 10 losses
in a row on a market we can't tell whether:
  - Model was systematically wrong on that market
  - Calibration over-shrunk a real edge
  - Edge floor let in marginal picks
  - One specific component (factor / MC / GBM) drove the wrong call

A single "show me the trace for these 10 picks" query answers all of it.

Storage
-------
data/pick_provenance.db — separate from per-sport DBs so heavy logging
never contends with picks/tracker writes. One row per (sport, date,
pick_key) — pick_key is hash of (game_id, bet_type, pick_text).
Compressed JSON blob keeps row size small (~1-2 KB typical).

Cleanup: rows older than 90 days are pruned by ``purge_old(days=90)``
to bound disk. Last 90 days is enough for any meaningful debug.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from ._tz import et_today_str

logger = logging.getLogger(__name__)


_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "pick_provenance.db"

_DDL = """
CREATE TABLE IF NOT EXISTS pick_provenance (
    sport       TEXT NOT NULL,
    date        TEXT NOT NULL,
    pick_key    TEXT NOT NULL,
    bet_type    TEXT NOT NULL,
    pick_text   TEXT NOT NULL,
    accepted    INTEGER NOT NULL,
    final_prob  REAL,
    final_edge  REAL,
    trace_gz    BLOB NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (sport, date, pick_key)
);
CREATE INDEX IF NOT EXISTS idx_pp_sport_date ON pick_provenance(sport, date);
CREATE INDEX IF NOT EXISTS idx_pp_bet_type   ON pick_provenance(sport, bet_type);
CREATE INDEX IF NOT EXISTS idx_pp_accepted   ON pick_provenance(sport, accepted);
"""


def _conn() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_DDL)
    return conn


def _make_pick_key(game_id: str | None, bet_type: str, pick_text: str) -> str:
    """Stable hash so re-runs of the same pick UPSERT instead of pile up."""
    raw = f"{game_id or ''}|{bet_type}|{pick_text}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def record(sport: str, *,
            bet_type: str,
            pick_text: str,
            trace: dict[str, Any],
            accepted: bool,
            final_prob: float | None = None,
            final_edge: float | None = None,
            game_id: str | None = None,
            date: str | None = None) -> None:
    """Persist one pick's decision trace. Quiet failure (debug log
    only) so a logging hiccup never blocks a pick from being recorded.

    The trace dict can carry arbitrary keys — typical shape::

        {
          "raw_prob": 0.65,
          "odds": -110,
          "juice_wall": -200,
          "juice_wall_passed": True,
          "odds_cap_passed": True,
          "belief_gate_passed": True,
          "empirical_calibrated": 0.58,
          "blend_calibrated": 0.55,
          "closing_line_posterior": 0.53,
          "implied": 0.524,
          "edge_pct": 0.6,
          "edge_ceiling_passed": True,
          "edge_floor_passed": False,
          "reject_reason": "edge_floor",
          "components": {"factor": 0.61, "mc": 0.66, "gbm": 0.68},
        }
    """
    try:
        date = date or et_today_str()
        pick_key = _make_pick_key(game_id, bet_type, pick_text)
        blob = gzip.compress(json.dumps(trace, default=str).encode("utf-8"))
        conn = _conn()
        conn.execute(
            "INSERT INTO pick_provenance "
            "  (sport, date, pick_key, bet_type, pick_text, accepted, "
            "   final_prob, final_edge, trace_gz, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now')) "
            "ON CONFLICT(sport, date, pick_key) DO UPDATE SET "
            "  bet_type   = excluded.bet_type, "
            "  pick_text  = excluded.pick_text, "
            "  accepted   = excluded.accepted, "
            "  final_prob = excluded.final_prob, "
            "  final_edge = excluded.final_edge, "
            "  trace_gz   = excluded.trace_gz, "
            "  created_at = excluded.created_at",
            (sport.lower(), date, pick_key, bet_type, pick_text,
             1 if accepted else 0, final_prob, final_edge, blob),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.debug("pick_provenance.record skipped: %s", exc)


def get(sport: str, date: str, pick_key: str) -> dict | None:
    """Pull a single pick's full decoded trace. Returns None if absent."""
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT * FROM pick_provenance "
            "WHERE sport = ? AND date = ? AND pick_key = ?",
            (sport.lower(), date, pick_key),
        ).fetchone()
        conn.close()
        if not row:
            return None
        trace = json.loads(gzip.decompress(row["trace_gz"]).decode("utf-8"))
        return {
            "sport":      row["sport"],
            "date":       row["date"],
            "pick_key":   row["pick_key"],
            "bet_type":   row["bet_type"],
            "pick_text":  row["pick_text"],
            "accepted":   bool(row["accepted"]),
            "final_prob": row["final_prob"],
            "final_edge": row["final_edge"],
            "created_at": row["created_at"],
            "trace":      trace,
        }
    except Exception as exc:
        logger.warning("pick_provenance.get failed: %s", exc)
        return None


def query(sport: str, *,
           date_from: str | None = None,
           date_to: str | None = None,
           bet_type: str | None = None,
           accepted: bool | None = None,
           limit: int = 100) -> list[dict]:
    """List provenance rows matching filters. Returns trace metadata
    (not the full trace blob — use ``get()`` for that). Ordered by
    most recent first."""
    where = ["sport = ?"]
    args: list = [sport.lower()]
    if date_from:
        where.append("date >= ?")
        args.append(date_from)
    if date_to:
        where.append("date <= ?")
        args.append(date_to)
    if bet_type:
        where.append("bet_type = ?")
        args.append(bet_type)
    if accepted is not None:
        where.append("accepted = ?")
        args.append(1 if accepted else 0)
    args.append(limit)
    try:
        conn = _conn()
        rows = conn.execute(
            f"SELECT sport, date, pick_key, bet_type, pick_text, "
            f"       accepted, final_prob, final_edge, created_at "
            f"FROM pick_provenance WHERE {' AND '.join(where)} "
            f"ORDER BY date DESC, created_at DESC LIMIT ?",
            args,
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.warning("pick_provenance.query failed: %s", exc)
        return []


def stats() -> dict:
    """Quick row-count snapshot per (sport, accepted)."""
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT sport, accepted, COUNT(*) AS n FROM pick_provenance "
            "GROUP BY sport, accepted"
        ).fetchall()
        conn.close()
    except Exception:
        return {}
    out: dict = {}
    for r in rows:
        s = out.setdefault(r["sport"], {"accepted": 0, "rejected": 0, "total": 0})
        if r["accepted"]:
            s["accepted"] = r["n"]
        else:
            s["rejected"] = r["n"]
        s["total"] += r["n"]
    return out


def purge_old(days: int = 90) -> int:
    """Drop rows older than ``days``. Bounds disk growth."""
    try:
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        conn = _conn()
        cur = conn.execute(
            "DELETE FROM pick_provenance WHERE date < ?", (cutoff,)
        )
        conn.commit()
        n = cur.rowcount
        conn.close()
        return n
    except Exception as exc:
        logger.warning("pick_provenance.purge_old failed: %s", exc)
        return 0


# ── CLI ────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Pick provenance admin")
    p.add_argument("--stats", action="store_true",
                   help="Print row counts per (sport, accepted)")
    p.add_argument("--list", action="store_true",
                   help="List recent rows (use --sport / --bet-type / --rejected to filter)")
    p.add_argument("--show", type=str, metavar="SPORT/DATE/KEY",
                   help="Show full trace for one pick (e.g. mlb/2026-05-03/abc123)")
    p.add_argument("--sport", default="mlb")
    p.add_argument("--bet-type", default=None)
    p.add_argument("--rejected", action="store_true",
                   help="With --list: only rejected picks")
    p.add_argument("--accepted-only", action="store_true",
                   help="With --list: only accepted picks")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--purge", type=int, metavar="DAYS",
                   help="Drop rows older than DAYS days")
    args = p.parse_args()

    if args.stats:
        s = stats()
        if not s:
            print("(no provenance rows yet)")
        for sport in sorted(s):
            d = s[sport]
            print(f"  {sport:8s} accepted={d['accepted']:>5d}  rejected={d['rejected']:>5d}  total={d['total']:>5d}")
    elif args.list:
        accepted = (True if args.accepted_only else
                    (False if args.rejected else None))
        rows = query(args.sport, bet_type=args.bet_type,
                     accepted=accepted, limit=args.limit)
        for r in rows:
            tag = "OK" if r["accepted"] else "X "
            print(f"  {tag}  {r['date']}  {r['bet_type']:12s} {r['pick_text']:30s} "
                  f"prob={r['final_prob']}  edge={r['final_edge']}  key={r['pick_key']}")
    elif args.show:
        sport, date, key = args.show.split("/", 2)
        row = get(sport, date, key)
        if not row:
            print("(not found)")
        else:
            print(json.dumps(row, indent=2, default=str))
    elif args.purge:
        n = purge_old(args.purge)
        print(f"Purged {n} rows older than {args.purge} days")
    else:
        p.print_help()


if __name__ == "__main__":
    main()
