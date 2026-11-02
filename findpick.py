import sqlite3
c = sqlite3.connect(r'C:\sports-model-bettor\data\mlb.db')
tabs = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
print("=== rows referencing TODAY (2026-10-24) or MIL/TB across all tables ===")
for t in tabs:
    cols = [r[1] for r in c.execute(f"PRAGMA table_info({t})")]
    conds = []
    if 'date' in cols: conds.append("date='2026-10-24'")
    # matchup-ish text columns
    textcols = [col for col in cols if col.lower() in ('matchup','game','game_id','pick','home','away','team','description','mlb_game_id','label')]
    try:
        if 'date' in cols:
            n = c.execute(f"SELECT COUNT(*) FROM {t} WHERE date='2026-10-24'").fetchone()[0]
            if n:
                print(f"\n[{t}] {n} row(s) dated 2026-10-24; cols={cols[:12]}")
                for r in c.execute(f"SELECT * FROM {t} WHERE date='2026-10-24' LIMIT 4"):
                    print("   ", tuple(r)[:14])
    except Exception as e:
        print(f"  {t}: err {e}")
# Specifically hunt for a MIL/TB matchup anywhere regardless of date
print("\n=== MIL/TB matchup hunt (any date, pick tables) ===")
for t in ('picks','paper_shadow','paper_potd','pick_of_day','player_props_picks','pick_events','derivative_picks','picks_cache'):
    if t not in tabs: continue
    cols = [r[1] for r in c.execute(f"PRAGMA table_info({t})")]
    mcol = next((x for x in cols if x.lower() in ('matchup','game')), None)
    if not mcol: 
        print(f"  {t}: no matchup col (cols={cols[:10]})"); continue
    rows = c.execute(f"SELECT {('date,' if 'date' in cols else '')}{mcol} FROM {t} WHERE {mcol} LIKE '%MIL%' OR {mcol} LIKE '%TB%' ORDER BY rowid DESC LIMIT 5").fetchall()
    print(f"  {t} [{mcol}]: {[tuple(r) for r in rows]}")
