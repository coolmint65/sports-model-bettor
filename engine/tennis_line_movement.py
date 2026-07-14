"""
Tennis line-movement tracker.

Mirrors ``engine.line_movement`` for team sports. Snapshots HR's
opening odds per match once per day, then exposes the delta between
opening and current odds at API-serve time so the frontend can
render a "line moved" chip on the bet card.

Captures four signals per match:
  - ML opening price (p1, p2)
  - Total games line + over/under prices
  - Total sets line + over/under prices

The capture is **idempotent on (match_id, date)** — first capture of
the day wins. Subsequent calls in the same day no-op for already-
captured matches but will pick up new matches that appeared after
the morning capture (e.g. brackets that advance mid-day).

Storage
-------
``tennis_opening_odds``::

    match_id           TEXT NOT NULL  -- HR or ESPN id (matches schedule)
    date               TEXT NOT NULL  -- YYYY-MM-DD
    captured_at        TEXT NOT NULL  -- UTC ISO
    p1_ml              INTEGER
    p2_ml              INTEGER
    total_games_line   REAL
    total_games_over   INTEGER
    total_games_under  INTEGER
    total_sets_line    REAL
    total_sets_over    INTEGER
    total_sets_under   INTEGER
    PRIMARY KEY (match_id, date)

Wires
-----
- ``capture_opening_odds()`` runs from ``sync_tennis.bat`` once per
  day, reads HR through the cached fetcher
  (``engine.tennis_odds.fetch_all``) so it shares the rate-limit
  circuit breaker with everyone else.
- ``compute_movement(match_id, date, current_odds)`` returns a
  movement dict the API endpoint attaches to each match.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Any
from ._tz import et_today_str

logger = logging.getLogger(__name__)


_DDL = """
CREATE TABLE IF NOT EXISTS tennis_opening_odds (
    match_id           TEXT NOT NULL,
    date               TEXT NOT NULL,
    captured_at        TEXT NOT NULL,
    p1_ml              INTEGER,
    p2_ml              INTEGER,
    total_games_line   REAL,
    total_games_over   INTEGER,
    total_games_under  INTEGER,
    total_sets_line    REAL,
    total_sets_over    INTEGER,
    total_sets_under   INTEGER,
    PRIMARY KEY (match_id, date)
);
"""


def _ensure_table() -> None:
    from .tennis_db import get_conn
    conn = get_conn()
    conn.executescript(_DDL)
    conn.commit()


def _today() -> str:
    return et_today_str()


# ── Capture ───────────────────────────────────────────────────

def capture_opening_odds(date: str | None = None) -> dict:
    """Snapshot opening odds for today's HR matches. Idempotent on
    (match_id, date) — re-running the same day overwrites nothing.

    HR events come through the shared cache + breaker so this is
    safe to call from any cron path without piling on rate limits.
    """
    from .tennis_odds import fetch_all as _fetch_hr
    from .tennis_db import get_conn
    _ensure_table()
    conn = get_conn()
    target = date or _today()
    summary = {"date": target, "scanned": 0, "inserted": 0, "skipped": 0}

    events = _fetch_hr()
    if not events:
        logger.warning("capture_opening_odds: HR cache empty / circuit open — skipping")
        return summary

    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for ev in events:
        match_id = ev.get("match_id")
        if not match_id:
            continue
        markets = ev.get("markets") or {}
        ml = markets.get("ml") or {}
        tg = markets.get("total_games") or {}
        ts = markets.get("total_sets") or {}
        # Skip events that don't carry at least an ML market — those
        # are usually doubles / suspended matches and would store
        # all-NULL opening rows that can't drive a movement chip.
        if ml.get("p1_odds") is None and ml.get("p2_odds") is None:
            continue

        summary["scanned"] += 1
        cur = conn.execute(
            "INSERT OR IGNORE INTO tennis_opening_odds "
            "(match_id, date, captured_at, "
            " p1_ml, p2_ml, "
            " total_games_line, total_games_over, total_games_under, "
            " total_sets_line, total_sets_over, total_sets_under) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                str(match_id), target, now_utc,
                ml.get("p1_odds"), ml.get("p2_odds"),
                tg.get("line"), tg.get("over_odds"), tg.get("under_odds"),
                ts.get("line"), ts.get("over_odds"), ts.get("under_odds"),
            ),
        )
        if cur.rowcount > 0:
            summary["inserted"] += 1
        else:
            summary["skipped"] += 1
    conn.commit()
    logger.info("tennis_line_movement[%s]: scanned=%d inserted=%d skipped=%d",
                 target, summary["scanned"], summary["inserted"], summary["skipped"])
    return summary


# ── Read / movement compute ───────────────────────────────────

def get_opening(match_id: str, date: str | None = None) -> dict | None:
    from .tennis_db import get_conn
    _ensure_table()
    target = date or _today()
    row = get_conn().execute(
        "SELECT * FROM tennis_opening_odds WHERE match_id = ? AND date = ?",
        (str(match_id), target),
    ).fetchone()
    return dict(row) if row else None


def _implied(american: int | float | None) -> float | None:
    if american is None:
        return None
    try:
        n = float(american)
    except (TypeError, ValueError):
        return None
    if n == 0:
        return None
    return abs(n) / (abs(n) + 100.0) if n < 0 else 100.0 / (n + 100.0)


def compute_movement(match_id: str, current_odds: dict | None,
                      date: str | None = None) -> dict | None:
    """Compute a movement summary against today's opening capture.

    Returns ``None`` when no opening odds exist for the match yet
    (first time we've seen it — sync hasn't captured, or HR didn't
    have it earlier). Returns a dict shaped like::

        {
            "captured_at": "2026-05-01T12:00:00+00:00",
            "ml":          {"p1_delta": +12, "p2_delta": -10,
                            "p1_implied_delta_pp": +1.4, ...},
            "total_games": {"line_delta": +1.0,
                            "over_delta": -25, "under_delta": +20},
            "total_sets":  {...},
            "significance": "major" | "moderate" | "minor" | "none",
            "direction":   "p1" | "p2" | "over" | "under" | "mixed" | None,
        }
    """
    opening = get_opening(match_id, date)
    if not opening:
        return None
    markets = (current_odds or {}).get("markets") or {}
    ml = markets.get("ml") or {}
    tg = markets.get("total_games") or {}
    ts = markets.get("total_sets") or {}

    def _diff(cur, opn):
        if cur is None or opn is None:
            return None
        try:
            return int(cur) - int(opn)
        except (TypeError, ValueError):
            return None

    def _implied_delta_pp(cur, opn):
        ic = _implied(cur)
        io = _implied(opn)
        if ic is None or io is None:
            return None
        return round((ic - io) * 100, 2)

    out: dict[str, Any] = {
        "captured_at": opening.get("captured_at"),
    }
    # ML
    p1_d = _diff(ml.get("p1_odds"), opening.get("p1_ml"))
    p2_d = _diff(ml.get("p2_odds"), opening.get("p2_ml"))
    p1_pp = _implied_delta_pp(ml.get("p1_odds"), opening.get("p1_ml"))
    p2_pp = _implied_delta_pp(ml.get("p2_odds"), opening.get("p2_ml"))
    if p1_d is not None or p2_d is not None:
        out["ml"] = {
            "p1_delta": p1_d, "p2_delta": p2_d,
            "p1_implied_delta_pp": p1_pp,
            "p2_implied_delta_pp": p2_pp,
            "p1_open": opening.get("p1_ml"),
            "p2_open": opening.get("p2_ml"),
            "p1_now": ml.get("p1_odds"),
            "p2_now": ml.get("p2_odds"),
        }
    # Total Games
    line_d = None
    cur_line = tg.get("line"); opn_line = opening.get("total_games_line")
    if cur_line is not None and opn_line is not None:
        try:
            line_d = round(float(cur_line) - float(opn_line), 1)
        except (TypeError, ValueError):
            line_d = None
    over_d = _diff(tg.get("over_odds"), opening.get("total_games_over"))
    under_d = _diff(tg.get("under_odds"), opening.get("total_games_under"))
    if line_d is not None or over_d is not None or under_d is not None:
        out["total_games"] = {
            "line_delta": line_d, "over_delta": over_d, "under_delta": under_d,
            "line_open": opn_line, "line_now": cur_line,
        }
    # Total Sets
    cur_ts_line = ts.get("line"); opn_ts_line = opening.get("total_sets_line")
    if cur_ts_line is not None and opn_ts_line is not None:
        try:
            ts_line_d = round(float(cur_ts_line) - float(opn_ts_line), 1)
        except (TypeError, ValueError):
            ts_line_d = None
        if ts_line_d:
            out["total_sets"] = {"line_delta": ts_line_d,
                                  "line_open": opn_ts_line,
                                  "line_now": cur_ts_line}

    # Aggregate signal: which side moved + how meaningful
    # Implied-prob delta of >= 5pp on ML is "major", >=2pp "moderate".
    # Total-games line delta of >=1 game is "major", >=0.5 "moderate".
    direction = None
    significance = "none"
    abs_pp = max(abs(p1_pp or 0), abs(p2_pp or 0))
    if abs_pp >= 5:
        significance = "major"
    elif abs_pp >= 2:
        significance = "moderate"
    elif abs_pp >= 1:
        significance = "minor"

    if (p1_pp or 0) > (p2_pp or 0):
        direction = "p1"  # p1's implied prob went up — favored side
    elif (p2_pp or 0) > (p1_pp or 0):
        direction = "p2"
    out["direction"] = direction
    out["significance"] = significance
    return out


# ── CLI ───────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="engine.tennis_line_movement")
    ap.add_argument("--capture", action="store_true",
                    help="Snapshot opening odds for today.")
    ap.add_argument("--date", default=None,
                    help="YYYY-MM-DD (default today)")
    args = ap.parse_args(argv)
    if args.capture:
        import json as _json
        print(_json.dumps(capture_opening_odds(args.date), indent=2))
        return 0
    ap.print_help()
    return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())


__all__ = ["capture_opening_odds", "get_opening", "compute_movement"]
