import sys, os, sqlite3
sys.path.insert(0, r"C:\sports-model-bettor")
os.chdir(r"C:\sports-model-bettor")
from engine.football import get_league_config
import engine.sports.nba.db as nbadb
print("NBA DB:", getattr(nbadb, "DB_PATH", None) or [v for k, v in vars(nbadb).items() if "PATH" in k.upper()])
for lg in ("nfl", "cfb"):
    print(lg, get_league_config(lg)["db_path"])

def ro(p):
    c = sqlite3.connect("file:%s?mode=ro" % p, uri=True); c.row_factory = sqlite3.Row; return c

nb = None
for k, v in vars(nbadb).items():
    if "PATH" in k.upper():
        nb = str(v)
c = ro(nb)
print([tuple(r) for r in c.execute("SELECT name, sql FROM sqlite_master WHERE tbl_name='nba_paper_picks' AND type='index'")])
print("NBA paper rows last 3 days:")
for r in c.execute("SELECT id,date,game_id,matchup,bet_type,pick,odds,edge,model_prob,result,created_at,closing_odds FROM nba_paper_picks WHERE date>=date('now','-3 day') ORDER BY game_id,id"):
    print(" ", tuple(r))
for lg in ("nfl", "cfb"):
    c = ro(get_league_config(lg)["db_path"])
    print(lg, "indexes:", [tuple(r) for r in c.execute("SELECT name, sql FROM sqlite_master WHERE tbl_name='picks' AND type='index'")])
    n = c.execute("SELECT COUNT(*) FROM (SELECT game_id FROM picks WHERE COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V') GROUP BY game_id HAVING COUNT(*)>1)").fetchone()[0]
    tot = c.execute("SELECT COUNT(DISTINCT game_id) FROM picks WHERE COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V')").fetchone()[0]
    print(lg, "games w/ >1 non-void authority rows:", n, "of", tot)
    print(lg, "pending authority rows:")
    for r in c.execute("SELECT id,date,game_id,matchup,bet_type,pick,line,odds,edge,result,created_at FROM picks WHERE COALESCE(is_shadow,0)=0 AND date>=date('now','-2 day') ORDER BY game_id,id"):
        print(" ", tuple(r))
    # collisions: paper row with same (date,gid,bt,pick) as a game's would-be authority -> check same key across shadow
    print(lg, "paper pending:", c.execute("SELECT COUNT(*) FROM picks WHERE is_shadow=1 AND result IS NULL").fetchone()[0])
