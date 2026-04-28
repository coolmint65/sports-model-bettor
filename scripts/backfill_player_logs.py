"""One-shot full-history backfill of player_game_logs from the per-sport
games tables. UPSERT-safe — already-ingested games are no-ops.

Run as a script: python scripts/backfill_player_logs.py {mlb|nba|nhl} [--days N]
Default --days = 1200 (covers ~3.3 seasons, the full pool we have).
"""
from __future__ import annotations
import argparse
import sys
import time
from pathlib import Path

# Project root on sys.path so `import engine.*` works regardless of
# whether this is run as `python scripts/backfill_player_logs.py` or
# `python -m scripts.backfill_player_logs`. Mirrors the pattern used
# by scripts/run.py.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sport", choices=("mlb", "nba", "nhl"))
    ap.add_argument("--days", type=int, default=1200)
    args = ap.parse_args()

    if args.sport == "mlb":
        from engine.mlb_player_logs import ingest_recent_finals
    elif args.sport == "nba":
        from engine.nba_player_logs import ingest_recent_finals
    else:
        from engine.nhl_player_logs import ingest_recent_finals

    from engine.player_props_db import _conn_for
    conn = _conn_for(args.sport)
    before = conn.execute("SELECT COUNT(*) FROM player_game_logs").fetchone()[0]
    span_before = conn.execute("SELECT MIN(date), MAX(date) FROM player_game_logs").fetchone()

    start = time.time()
    print(f"[{args.sport}] backfill start: {before} rows, span {span_before[0]} to {span_before[1]}", flush=True)
    print(f"[{args.sport}] calling ingest_recent_finals(lookback_days={args.days}) ...", flush=True)

    result = ingest_recent_finals(lookback_days=args.days)
    elapsed = time.time() - start

    after = conn.execute("SELECT COUNT(*) FROM player_game_logs").fetchone()[0]
    span_after = conn.execute("SELECT MIN(date), MAX(date) FROM player_game_logs").fetchone()
    print(f"[{args.sport}] DONE in {elapsed/60:.1f} min", flush=True)
    print(f"[{args.sport}] result: {result}", flush=True)
    print(f"[{args.sport}] rows: {before} -> {after} (+{after-before})", flush=True)
    print(f"[{args.sport}] span: {span_after[0]} to {span_after[1]}", flush=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())
