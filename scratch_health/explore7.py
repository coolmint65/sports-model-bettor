import sqlite3, os, sys
ROOT = r"C:\sports-model-bettor\data"
def ro(path):
    c = sqlite3.connect("file:" + path + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c
targets = {
    "nba.db": ["nba_games", "games"],
    "nhl.db": ["nhl_games", "games", "nhl_espn_games"],
    "mlb.db": ["games"],
    "football/nfl.db": ["games"],
    "football/cfb.db": ["games"],
    "basketball/china_cba.db": ["games"],
    "basketball/australia_nbl.db": ["games"],
    "basketball/ncaam.db": ["games"],
    "basketball/wnba.db": ["games"],
    "soccer/eng_premier.db": ["matches", "games", "fixtures"],
}
for f, tbls in targets.items():
    p = os.path.join(ROOT, f)
    c = ro(p)
    have = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for t in tbls:
        if t not in have:
            continue
        cols = [r[1] for r in c.execute(f"PRAGMA table_info({t})")]
        print("##", f, t, c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0], ",".join(cols))
        for r in c.execute(f"SELECT * FROM {t} ORDER BY rowid DESC LIMIT 2"):
            print("   ", {k: r[k] for k in r.keys() if k in ("id","game_id","espn_id","espn_game_id","match_id","date","game_date","start_time","start","status","home","away","home_abbr","away_abbr","home_team","away_team","event_id","kickoff","utc")})
    c.close()
sys.stdout.flush()
