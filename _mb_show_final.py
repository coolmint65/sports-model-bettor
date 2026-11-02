import sys, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
RM = r'C:\sports-model-bettor\backend\server\routes_mlb.py'
lines = open(RM, encoding='utf-8').readlines()
for i, l in enumerate(lines):
    if re.search(r'def _mlb_recorded_authority_pick|def api_mlb_tracker_unified|def _mlb_merged_recorded_best|@router.get\("/api/mlb/tracker/unified"\)|@router.get\("/api/tracker/paper"\)|GROUP BY game_id, date|for gid, gdate in grp|_mlb_recorded_authority_pick\(gid', l):
        print(f"{i+1}: {l.rstrip()}")
