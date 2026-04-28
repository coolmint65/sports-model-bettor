"""Retrain per-stat prop GBM models on the full backfilled history.

Runs train_all_{mlb,nba,nhl} with lookback_days=1200 (covers our full
pool). Each stat's model lands at engine/models/{sport}_prop_*_xgb_latest.json
when CV beats the rolling-mean baseline by SHIP_GATE_MAE_REDUCTION.

Usage: python scripts/retrain_props_gbm.py {mlb|nba|nhl} [--days N]
"""
from __future__ import annotations
import argparse
import json
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sport", choices=("mlb", "nba", "nhl"))
    ap.add_argument("--days", type=int, default=1200)
    args = ap.parse_args()

    if args.sport == "mlb":
        from engine.mlb_props_train_all import train_all_mlb as train
    elif args.sport == "nba":
        from engine.nba_nhl_props_train import train_all_nba as train
    else:
        from engine.nba_nhl_props_train import train_all_nhl as train

    start = time.time()
    print(f"[{args.sport}] retrain start (lookback_days={args.days})", flush=True)
    results = train(lookback_days=args.days)
    elapsed = time.time() - start

    shipped = sum(1 for r in results if r.get("decision") == "ship")
    killed = sum(1 for r in results if r.get("decision") == "kill")
    print(f"[{args.sport}] DONE in {elapsed/60:.1f} min  "
          f"shipped={shipped} killed={killed}", flush=True)
    for r in results:
        stat = r.get("stat")
        n = r.get("n_rows")
        decision = r.get("decision")
        if decision == "ship":
            red = r.get("reduction_pct", 0)
            base = r.get("baseline_mae", 0)
            gbm = r.get("gbm_mae", 0)
            print(f"  {stat:14s} SHIP n={n:>6d} baseline_mae={base:.4f} "
                  f"gbm_mae={gbm:.4f} reduction={red:+.1f}%", flush=True)
        else:
            reason = r.get("reason") or f"reduction={r.get('reduction_pct', 0):+.1f}% (gate not cleared)"
            print(f"  {stat:14s} kill n={n:>6d} {reason}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
