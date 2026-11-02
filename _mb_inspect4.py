import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

RF = r'C:\sports-model-bettor\backend\server\routes_football.py'
DB = r'C:\sports-model-bettor\engine\db.py'
RM = r'C:\sports-model-bettor\backend\server\routes_mlb.py'

def dump(path, a, b, label):
    print(f"\n===== {label}  [{path}] lines {a}-{b} =====")
    lines = open(path, encoding='utf-8', errors='replace').readlines()
    b = min(b, len(lines))
    for i in range(a-1, b):
        print(f"{i+1}: {lines[i]}", end='')
    print()

# Football reference: merged-best, authority helper, unified tracker
dump(RF, 987, 1170, 'routes_football reference')
# picks table schema
dump(DB, 349, 410, 'engine/db.py picks schema')
# MLB helpers referenced inside merged-best, to reuse in new helper
# _mlb_market_scope, _mlb_flat_profit, _disp_pick, _mlb_extra_evidence_map
import re
for pat in [r'def _mlb_market_scope', r'def _mlb_flat_profit', r'def _disp_pick', r'def _mlb_extra_evidence_map', r'def _maybe_auto_settle_mlb']:
    print(f"\n----- GREP '{pat}' in routes_mlb.py -----")
    rx = re.compile(pat)
    for i,l in enumerate(open(RM, encoding='utf-8', errors='replace').readlines()):
        if rx.search(l): print(f"{i+1}: {l}", end='')
