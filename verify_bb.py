import os, sqlite3, glob
BB = r"C:\sports-model-bettor\data\basketball"
dbs = sorted(glob.glob(os.path.join(BB, "*.db")))
summ = []
for p in dbs:
    lg = os.path.basename(p)[:-3]
    try:
        c = sqlite3.connect(p); c.row_factory = sqlite3.Row
        # settled is_shadow=0 rows, max date, distinct games
        r0 = c.execute("SELECT COUNT(*) n, COUNT(DISTINCT game_id) g, MAX(date) mx FROM picks WHERE COALESCE(is_shadow,0)=0 AND result IS NOT NULL AND result!='V'").fetchone()
        r0all = c.execute("SELECT COUNT(*) n, COUNT(DISTINCT game_id) g, MAX(date) mx FROM picks WHERE COALESCE(is_shadow,0)=0").fetchone()
        r1 = c.execute("SELECT COUNT(*) n, COUNT(DISTINCT game_id) g, MAX(date) mx FROM picks WHERE COALESCE(is_shadow,0)=1").fetchone()
        summ.append((lg, r0all["n"], r0all["g"], r0all["mx"], r0["n"], r0["g"], r1["n"], r1["g"], r1["mx"]))
        c.close()
    except Exception as e:
        summ.append((lg, "ERR", str(e), "", "", "", "", "", ""))
# sort by most recent is_shadow=0 max date then count
summ.sort(key=lambda t: (str(t[3]), t[1] if isinstance(t[1],int) else 0), reverse=True)
print("league | s0_rows s0_games s0_maxdate | s0_settled_rows s0_settled_games | s1_rows s1_games s1_maxdate")
for t in summ[:25]:
    print(" | ".join(str(x) for x in t))
