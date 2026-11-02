import sqlite3, os, sys
ROOT = r"C:\sports-model-bettor\data"
def ro(path):
    c = sqlite3.connect("file:" + path + "?mode=ro", uri=True); c.row_factory = sqlite3.Row; return c
u = ro(os.path.join(ROOT, "picks_unified.db"))
for sp, lg in ():  # ("football","cfb"),("football","nfl"),("basketball","china_cba"),("basketball","australia_nbl"),("soccer","eng_premier"),("nhl","nhl"),("basketball","wnba"),("mlb","mlb")):
    print("==", sp, lg)
    for r in u.execute("SELECT game_key, pick_date, bet_type, pick_text, line, odds, prob, result, is_shadow, stake_units FROM picks WHERE sport=? AND league=? ORDER BY id DESC LIMIT 4", (sp, lg)):
        print("  ", tuple(r))
for f, t, sh in (("football/cfb.db","picks","is_shadow"),("football/nfl.db","picks","is_shadow"),("basketball/china_cba.db","picks","is_shadow"),("soccer/eng_premier.db","picks",None),("basketball/wnba.db","picks","is_shadow")):
    c = ro(os.path.join(ROOT, f))
    print("== card table", f)
    q = f"SELECT {'is_shadow,' if sh else ''} COUNT(*), MIN(date), MAX(date), SUM(result IS NULL) FROM {t}" + (" GROUP BY is_shadow" if sh else "")
    for r in c.execute(q): print("  ", tuple(r))
    for r in c.execute(f"SELECT * FROM {t} ORDER BY id DESC LIMIT 3"):
        d = dict(r); print("  ", {k: d[k] for k in d if k in ("id","date","game_id","match_id","matchup","bet_type","pick","line","side","odds","model_prob","edge","result","is_shadow","stake_units","created_at")})
print("== props dup check")
for f in ("nhl.db","nba.db","football/nfl_player.db","mlb.db","basketball/wnba.db"):
    c = ro(os.path.join(ROOT, f))
    r = c.execute("SELECT COUNT(*), COUNT(DISTINCT game_id||'|'||player_id||'|'||bet_type||'|'||COALESCE(side,'')) FROM player_props_picks").fetchone()
    r2 = c.execute("SELECT COUNT(*), COUNT(DISTINCT game_id||'|'||player_id||'|'||bet_type||'|'||COALESCE(side,'')) FROM player_props_picks WHERE date>='2026-10-26'").fetchone()
    print(f, "all", tuple(r), "since10-26", tuple(r2), c.execute("SELECT MIN(date), MAX(date) FROM player_props_picks").fetchone()[:])
    print("   ", [x[0] for x in c.execute("SELECT sql FROM sqlite_master WHERE tbl_name='player_props_picks' AND type IN ('index','table')")][:3])
c = ro(os.path.join(ROOT, "football", "nfl_player.db"))
print("nfl eval log dates", tuple(c.execute("SELECT MIN(game_date), MAX(game_date), COUNT(*), SUM(actual IS NOT NULL) FROM prop_eval_log").fetchone()))
c = ro(os.path.join(ROOT, "nhl.db"))
print("nhl eval log dates", tuple(c.execute("SELECT MIN(game_date), MAX(game_date), COUNT(*), SUM(actual IS NOT NULL) FROM prop_eval_log").fetchone()))
c = ro(os.path.join(ROOT, "nba.db"))
print("nba eval log dates", tuple(c.execute("SELECT MIN(game_date), MAX(game_date), COUNT(*), SUM(actual IS NOT NULL) FROM prop_eval_log").fetchone()))
