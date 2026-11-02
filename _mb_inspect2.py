import sys, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

RM   = r'C:\sports-model-bettor\backend\server\routes_mlb.py'
REC  = r'C:\sports-model-bettor\engine\tracker\_record.py'
INIT = r'C:\sports-model-bettor\engine\tracker\__init__.py'
SRV  = r'C:\sports-model-bettor\backend\server\__init__.py'

def dump(path, a, b, label):
    print(f"\n===== {label}  [{path}] lines {a}-{b} =====")
    lines = open(path, encoding='utf-8', errors='replace').readlines()
    b = min(b, len(lines))
    for i in range(a-1, b):
        print(f"{i+1}: {lines[i]}", end='')
    print()

def grepfile(path, pattern, label, ctx=0):
    print(f"\n===== GREP '{pattern}' in {label} =====")
    rx = re.compile(pattern)
    lines = open(path, encoding='utf-8', errors='replace').readlines()
    for i, l in enumerate(lines):
        if rx.search(l):
            print(f"{i+1}: {l}", end='')
    print()

# 1. _get_recorded_pick in backend/server/__init__.py
for pat in [r'_get_recorded_pick', r'def _get_recorded_pick', r'_get_recorded_pick\s*=']:
    grepfile(SRV, pat, 'backend/server/__init__.py')

# 2. record_picks body
dump(REC, 214, 330, '_record.record_picks (part 1)')

# 3. tracker/__init__.py (ascii-safe now)
print("\n===== engine/tracker/__init__.py =====")
print(open(INIT, encoding='utf-8', errors='replace').read())

# 4. the current tracker + merged-best
dump(RM, 682, 800, 'routes_mlb _mlb_merged_recorded_best + api_mlb_tracker_unified')
