import sqlite3, os, sys
ROOT = r"C:\sports-model-bettor\data"
def ro(path):
    c = sqlite3.connect("file:" + path + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c
paths = [os.path.join(ROOT, f) for f in ("game_odds.db", "nba.db", "nhl.db", "mlb.db", "picks_unified.db", "pick_provenance.db", "provenance.db", "live.db", "events.db")]
paths += [os.path.join(ROOT, "basketball", f) for f in ("wnba.db", "ncaam.db", "ncaaw.db", "china_cba.db", "australia_nbl.db")]
paths += [os.path.join(ROOT, "football", f) for f in ("nfl.db", "cfb.db")]
paths += [os.path.join(ROOT, "soccer", "eng_premier.db")]
for p in paths:
    if not os.path.exists(p) or os.path.getsize(p) == 0:
        continue
    c = ro(p)
    for (t,) in c.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        if "odds" in t.lower() or "snap" in t.lower() or "prov" in t.lower() or "trace" in t.lower():
            cols = [r[1] for r in c.execute(f"PRAGMA table_info({t})")]
            tcol = next((x for x in cols if x in ("captured_at", "fetched_at", "ts", "updated_at", "created_at", "snapshot_at", "recorded_at", "timestamp", "scraped_at")), None)
            n = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            mx = c.execute(f"SELECT MAX({tcol}) FROM {t}").fetchone()[0] if tcol else None
            print(os.path.basename(p), t, n, tcol, mx, "|", ",".join(cols[:25]))
    c.close()
sys.stdout.flush()
