import re, os

RM = r'C:\sports-model-bettor\backend\server\routes_mlb.py'
REC = r'C:\sports-model-bettor\engine\tracker\_record.py'
INIT = r'C:\sports-model-bettor\engine\tracker\__init__.py'

def dump(path, a, b, label):
    print(f"\n===== {label}  [{path}] lines {a}-{b} =====")
    lines = open(path, encoding='utf-8', errors='replace').readlines()
    b = min(b, len(lines))
    for i in range(a-1, b):
        print(f"{i+1}: {lines[i]}", end='')
    print()

def grepfile(path, pattern, label):
    print(f"\n===== GREP '{pattern}' in {label} [{path}] =====")
    rx = re.compile(pattern)
    lines = open(path, encoding='utf-8', errors='replace').readlines()
    for i, l in enumerate(lines):
        if rx.search(l):
            print(f"{i+1}: {l}", end='')
    print()

# routes_mlb grep for key symbols
for pat in [r'_get_recorded_pick', r'_mlb_merged_recorded_best', r'_apply_selected',
            r'api_mlb_tracker_unified', r'record_picks', r'paper_shadow', r'def get_best_pick',
            r'_mlb_tracker', r'_recorded_authority', r'def api_mlb']:
    grepfile(RM, pat, 'routes_mlb.py')

# imports / lazy loaders
dump(RM, 1, 130, 'routes_mlb HEADER/IMPORTS')

print("\n\n########## _record.py ##########")
for pat in [r'^def ', r'^\s*def ', r'INSERT INTO', r'is_shadow', r'CREATE TABLE', r'record_picks']:
    grepfile(REC, pat, '_record.py')

print("\n\n########## tracker/__init__.py ##########")
print(open(INIT, encoding='utf-8', errors='replace').read())
