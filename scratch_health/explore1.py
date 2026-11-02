import sqlite3, os, sys
ROOT = r"C:\sports-model-bettor\data"

def ro(path):
    return sqlite3.connect("file:" + path + "?mode=ro", uri=True)

def tables(path):
    c = ro(path)
    t = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    c.close()
    return t

def schema(path, tbl):
    c = ro(path)
    cols = [(r[1], r[2]) for r in c.execute(f"PRAGMA table_info({tbl})")]
    n = c.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    c.close()
    return n, cols

dbs = {
    "picks_unified": os.path.join(ROOT, "picks_unified.db"),
    "nba": os.path.join(ROOT, "nba.db"),
    "nhl": os.path.join(ROOT, "nhl.db"),
    "mlb": os.path.join(ROOT, "mlb.db"),
    "wnba_root": os.path.join(ROOT, "wnba.db"),
    "nfl_player_root": os.path.join(ROOT, "nfl_player.db"),
    "fb_nfl": os.path.join(ROOT, "football", "nfl.db"),
    "fb_cfb": os.path.join(ROOT, "football", "cfb.db"),
    "fb_nfl_player": os.path.join(ROOT, "football", "nfl_player.db"),
    "bb_wnba": os.path.join(ROOT, "basketball", "wnba.db"),
    "bb_ncaam": os.path.join(ROOT, "basketball", "ncaam.db"),
    "bb_ncaaw": os.path.join(ROOT, "basketball", "ncaaw.db"),
    "bb_cba": os.path.join(ROOT, "basketball", "china_cba.db"),
    "bb_nbl": os.path.join(ROOT, "basketball", "australia_nbl.db"),
    "soc_epl": os.path.join(ROOT, "soccer", "eng_premier.db"),
    "soc_picks": os.path.join(ROOT, "soccer", "picks.db"),
}
INTEREST = ("pick", "shadow", "prop", "potd", "pot_day", "eval", "paper", "derivative")
for k, p in dbs.items():
    if not os.path.exists(p):
        print("##", k, "MISSING", p); continue
    print("##", k, p, os.path.getsize(p))
    try:
        for t in tables(p):
            if any(s in t.lower() for s in INTEREST) or k == "picks_unified":
                n, cols = schema(p, t)
                print("   ", t, n, ", ".join(c for c, _ in cols))
    except Exception as e:
        print("   ERR", e)
sys.stdout.flush()
