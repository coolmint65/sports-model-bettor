"""Verify the data sources predict_matchup consumes return distinct
values per team. If get_team_record / compute_team_stats_at_date all
return the SAME object for different team_ids, the bug is in the data
layer; if they differ but predict_matchup still gives the same output,
the bug is inside the predict pipeline.

Usage:  python -m scripts.diag_data_isolation
"""
from engine.db import get_team_by_id, get_team_record, get_pitcher_season
from engine.mlb_predict import SEASON
from engine.pit_stats import compute_team_stats_at_date
from datetime import datetime

TEAMS = [
    ("NYY", 147),
    ("KC",  118),
    ("MIN", 142),
    ("CIN", 113),
    ("CHC", 112),
    ("NYM", 121),
]

today = datetime.now().strftime("%Y-%m-%d")

print("=" * 60)
print("get_team_by_id (static team metadata)")
print("=" * 60)
for label, tid in TEAMS:
    t = get_team_by_id(tid)
    print(f"{label} (id={tid}): {t}")

print("\n" + "=" * 60)
print(f"get_team_record(team_id, SEASON={SEASON})")
print("=" * 60)
records = {}
for label, tid in TEAMS:
    rec = get_team_record(tid, SEASON)
    records[label] = rec
    short = {k: rec.get(k) for k in ("wins", "losses", "ops", "wrc_plus", "runs_per_game")} if rec else None
    print(f"{label}: {short}")

# All same?
non_null = [r for r in records.values() if r]
if non_null and all(r == non_null[0] for r in non_null):
    print(">>> ALL TEAM RECORDS ARE IDENTICAL <<<")
elif len({id(r) for r in non_null}) == 1:
    print(">>> ALL TEAM RECORDS POINT TO THE SAME OBJECT <<<")
else:
    print(">>> records vary per team (good)")

print("\n" + "=" * 60)
print(f"compute_team_stats_at_date(team_id, '{today}', SEASON)")
print("=" * 60)
pits = {}
for label, tid in TEAMS:
    pit = compute_team_stats_at_date(tid, today, SEASON)
    pits[label] = pit
    short = {k: pit.get(k) for k in ("runs_per_game", "runs_allowed_per_game", "win_pct")} if pit else None
    print(f"{label}: {short}")

non_null_pits = [p for p in pits.values() if p]
if non_null_pits and all(p == non_null_pits[0] for p in non_null_pits):
    print(">>> ALL PIT STATS ARE IDENTICAL <<<")
elif len({id(p) for p in non_null_pits}) == 1:
    print(">>> ALL PIT STATS POINT TO THE SAME OBJECT <<<")
else:
    print(">>> PIT stats vary per team (good)")
