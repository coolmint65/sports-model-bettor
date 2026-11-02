"""All-games odds capture (Austin 2026-09-16) — snapshot EVERY game's lines (not just picked ones)
so market biases validate at scale. Standalone; data/game_odds.db. Cron hourly; upsert => last snap ~ close.
Covers: football(nfl,cfb) basketball(nba,wnba,ncaam) hockey(nhl) soccer(majors, 3-way ML) mlb tennis."""
import sqlite3, datetime, sys
from datetime import timezone
sys.path.insert(0, r'C:\sports-model-bettor')
DB = r'C:\sports-model-bettor\data\game_odds.db'
F = ['home_ml','away_ml','draw_ml','home_spread_point','home_spread_odds','away_spread_point','away_spread_odds','over_under','over_odds','under_odds','start_time']
con = sqlite3.connect(DB)
con.execute("""CREATE TABLE IF NOT EXISTS game_odds(
  sport TEXT, league TEXT, game_key TEXT, capture_date TEXT, home_ml INT, away_ml INT, draw_ml INT,
  home_spread_point REAL, home_spread_odds INT, away_spread_point REAL, away_spread_odds INT,
  over_under REAL, over_odds INT, under_odds INT, start_time TEXT, first_seen TEXT, last_seen TEXT, n_snaps INT DEFAULT 1,
  PRIMARY KEY(sport,league,game_key,capture_date))""")
try: con.execute("ALTER TABLE game_odds ADD COLUMN draw_ml INT")
except Exception: pass
now = datetime.datetime.now(timezone.utc).isoformat(); today = datetime.date.today().isoformat()
def norm(o):
    if not isinstance(o,dict): return {}
    d={f:o.get(f) for f in F}
    ml=o.get('ml')
    if isinstance(ml,dict):
        for a,b in [('home_ml','home'),('away_ml','away'),('draw_ml','draw')]:
            if d.get(a) is None and ml.get(b) is not None: d[a]=ml.get(b)
    sp=o.get('spread')
    if isinstance(sp,dict):
        if d.get('home_spread_point') is None: d['home_spread_point']=sp.get('home_pt') or sp.get('home_line')
        if d.get('home_spread_odds') is None: d['home_spread_odds']=sp.get('home_odds')
        if d.get('away_spread_point') is None: d['away_spread_point']=sp.get('away_pt') or sp.get('away_line')
        if d.get('away_spread_odds') is None: d['away_spread_odds']=sp.get('away_odds')
    tot=o.get('total')
    if isinstance(tot,dict):
        if d.get('over_under') is None: d['over_under']=tot.get('line')
        if d.get('over_odds') is None: d['over_odds']=tot.get('over_odds')
        if d.get('under_odds') is None: d['under_odds']=tot.get('under_odds')
    return d
def upsert(sport,lg,key,o):
    v=[norm(o).get(x) for x in F]
    con.execute("""INSERT INTO game_odds(sport,league,game_key,capture_date,home_ml,away_ml,draw_ml,home_spread_point,home_spread_odds,away_spread_point,away_spread_odds,over_under,over_odds,under_odds,start_time,first_seen,last_seen,n_snaps)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
      ON CONFLICT(sport,league,game_key,capture_date) DO UPDATE SET
        home_ml=excluded.home_ml,away_ml=excluded.away_ml,draw_ml=excluded.draw_ml,home_spread_point=excluded.home_spread_point,
        home_spread_odds=excluded.home_spread_odds,away_spread_point=excluded.away_spread_point,away_spread_odds=excluded.away_spread_odds,
        over_under=excluded.over_under,over_odds=excluded.over_odds,under_odds=excluded.under_odds,start_time=excluded.start_time,last_seen=excluded.last_seen,n_snaps=n_snaps+1""",
      (sport,lg,key,today,*v,now,now))
total=0; bylg={}
SOCCER=['eng_premier']   # the other 16 leagues were removed 2026-10-27 (Austin: smaller leagues)
for sport,leagues in {'football':['nfl','cfb'],'basketball':['nba','wnba','ncaam','ncaaw'],'hockey':['nhl']}.items():
    try: fetch=__import__(f'engine.{sport}._odds',fromlist=['fetch_league_odds']).fetch_league_odds
    except Exception as e: print(f'{sport}: no fetch ({str(e)[:40]})'); continue
    for lg in leagues:
        try: odds=fetch(lg) or {}
        except Exception as e: print(f'{sport}/{lg}: {str(e)[:40]}'); continue
        if odds: bylg[lg]=len(odds)
        for k,o in odds.items(): upsert(sport,lg,k,o); total+=1
# MLB (separate no-arg fetch)
try:
    from engine.picks import fetch_real_odds_for_games as mlbf
    mo=mlbf() or {}; 
    if mo: bylg['mlb']=len(mo)
    for k,o in mo.items():
        if isinstance(o,dict): upsert('baseball','mlb',k,o); total+=1
except Exception as e: print('mlb err',str(e)[:60])
# Tennis (list of matches; markets.ml/total_games)
try:
    import engine.tennis_odds as to
    ms=to.fetch_all() or []
    for m in ms:
        mk=m.get('markets') or {}; ml=mk.get('ml') or {}
        o={'ml':{'home':ml.get('p1_odds'),'away':ml.get('p2_odds')},'total':mk.get('total_games'),'start_time':m.get('start_time')}
        upsert('tennis','tennis',f"{m.get('p1_name')}@{m.get('p2_name')}",o); total+=1
    bylg['tennis']=len(ms)
except Exception as e: print('tennis err',str(e)[:60])
# Soccer LAST (was 17 leagues, EPL-only since 2026-10-27): the heaviest burst, most likely to trip the CF
# breaker -- fetch it after the quick single pulls so tennis/mlb aren't starved
# behind it (they were going stale every run). (MintyBot 2026-10-13)
try:
    sfetch=__import__('engine.soccer._odds',fromlist=['fetch_league_odds']).fetch_league_odds
    for lg in SOCCER:
        try: odds=sfetch(lg) or {}
        except Exception as e: print(f'soccer/{lg}: {str(e)[:40]}'); continue
        if odds: bylg[lg]=len(odds)
        for k,o in odds.items(): upsert('soccer',lg,k,o); total+=1
except Exception as e: print('soccer err',str(e)[:50])
con.commit()
print(f'captured {total} rows | by league: {bylg}')
print('table total:', con.execute("SELECT COUNT(*) FROM game_odds").fetchone()[0])
con.close()
