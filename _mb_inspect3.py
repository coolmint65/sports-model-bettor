import sys, re, glob, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

RM   = r'C:\sports-model-bettor\backend\server\routes_mlb.py'
REC  = r'C:\sports-model-bettor\engine\tracker\_record.py'
SRV  = r'C:\sports-model-bettor\backend\server\__init__.py'
DB   = r'C:\sports-model-bettor\engine\db.py'
SRVDIR = r'C:\sports-model-bettor\backend\server'

def dump(path, a, b, label):
    print(f"\n===== {label}  [{path}] lines {a}-{b} =====")
    lines = open(path, encoding='utf-8', errors='replace').readlines()
    b = min(b, len(lines))
    for i in range(a-1, b):
        print(f"{i+1}: {lines[i]}", end='')
    print()

def grepfile(path, pattern, label):
    print(f"\n----- GREP '{pattern}' in {label} -----")
    rx = re.compile(pattern)
    try:
        lines = open(path, encoding='utf-8', errors='replace').readlines()
    except Exception as e:
        print("  (err:", e, ")"); return
    for i, l in enumerate(lines):
        if rx.search(l):
            print(f"{i+1}: {l}", end='')

# 1. _get_recorded_pick definition (the AUTHORITY the card reads)
dump(SRV, 1297, 1400, 'backend/server.__init__ _get_recorded_pick')

# 2. rest of record_picks — where it WRITES
dump(REC, 330, 486, '_record.record_picks (part 2, the write)')

# 3. engine/db.py — picks table + readers
for pat in [r'CREATE TABLE', r'def get_conn', r'def .*pick', r'is_shadow', r'INSERT INTO picks',
            r'def save_pick', r'def insert_pick', r'def record_pick', r'def get_picks',
            r'def .*history', r'picks\b']:
    grepfile(DB, pat, 'engine/db.py')

# 4. football reference: find the authority helpers across backend/server
print("\n\n########## FOOTBALL REFERENCE (grep all of backend/server) ##########")
for f in glob.glob(os.path.join(SRVDIR, '*.py')):
    rx = re.compile(r'_football_recorded_authority_pick|_football_merged_recorded_best|api_football_tracker_unified')
    try:
        lines = open(f, encoding='utf-8', errors='replace').readlines()
    except Exception:
        continue
    hits = [(i+1,l) for i,l in enumerate(lines) if rx.search(l)]
    if hits:
        print(f"\n-- {os.path.basename(f)} --")
        for ln,l in hits:
            print(f"{ln}: {l}", end='')
