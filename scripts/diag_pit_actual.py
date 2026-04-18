"""Print the actual full return value of compute_team_stats_at_date.
My earlier diag used wrong key names (runs_per_game vs the real
runs_pg), so it falsely reported nulls. This shows the truth.

Usage:  python -m scripts.diag_pit_actual
"""
from datetime import datetime
from engine.pit_stats import compute_team_stats_at_date
from engine.mlb_predict import SEASON, predict_matchup

today = datetime.now().strftime("%Y-%m-%d")

for label, tid in [("NYY", 147), ("KC", 118), ("MIN", 142),
                    ("CIN", 113), ("CHC", 112), ("NYM", 121)]:
    pit = compute_team_stats_at_date(tid, today, SEASON)
    print(f"{label} ({tid}) PIT keys: {list(pit.keys())}")
    print(f"  runs_pg={pit.get('runs_pg')}, "
          f"runs_against_pg={pit.get('runs_against_pg')}, "
          f"games={pit.get('games_played')}, "
          f"wins={pit.get('wins')}, losses={pit.get('losses')}")

print("\n=== predict_matchup INTERNAL VARIABLES ===")
# Re-run predict_matchup but also peek at intermediate state by patching
# print into _blended_offense
import engine.mlb_predict as mp
import engine.mlb_factors as mf

orig_blended = mf._blended_offense if hasattr(mf, "_blended_offense") else mp._blended_offense

call_log = []
def traced(pit, stats, *args, **kwargs):
    out = orig_blended(pit, stats, *args, **kwargs)
    call_log.append({"pit_runs_pg": pit.get("runs_pg") if pit else None,
                     "stats_ops": stats.get("ops") if stats else None,
                     "out": out})
    return out

# Patch wherever it lives
if hasattr(mf, "_blended_offense"):
    mf._blended_offense = traced
else:
    mp._blended_offense = traced

for label, h, a in [("KC@NYY", 147, 118), ("CIN@MIN", 142, 113), ("NYM@CHC", 112, 121)]:
    print(f"\n--- {label} ---")
    call_log.clear()
    r = predict_matchup(h, a)
    for c in call_log:
        print(f"  blended_offense input: pit_runs_pg={c['pit_runs_pg']}, "
              f"stats_ops={c['stats_ops']} -> output={c['out']}")
    print(f"  win_prob: {r.get('win_prob')}")
    print(f"  expected_score: {r.get('expected_score')}")
    print(f"  total: {r.get('total')}")
