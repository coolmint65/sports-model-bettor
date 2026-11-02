import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
RM  = r'C:\sports-model-bettor\backend\server\routes_mlb.py'
REC = r'C:\sports-model-bettor\engine\tracker\_record.py'
def dump(path, a, b, label):
    print(f"\n===== {label} lines {a}-{b} =====")
    lines = open(path, encoding='utf-8', errors='replace').readlines()
    b = min(b, len(lines))
    for i in range(a-1, b):
        print(f"{i+1}: {lines[i]}", end='')
dump(RM, 604, 681, 'routes_mlb helpers: scope/flat_profit/disp_pick')
dump(REC, 486, 584, '_record_from_scoreboard (game_id in INSERT?)')
