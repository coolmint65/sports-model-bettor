import json,urllib.request
B='http://localhost:8000'
def get(p,t=300):
    try: return json.load(urllib.request.urlopen(B+p,timeout=t))
    except Exception as e: return {'_err':str(e)}
L=[l['key'] for l in get('/api/basketball/leagues')['leagues'] if l.get('in_season') and l['key']!='nba']
print('leagues',L)
def pk(p): return None if not p else (p.get('pick'), p.get('odds'), p.get('edge'))
for lg in L:
    c=get(f'/api/basketball/{lg}/today'); t=get(f'/api/basketball/{lg}/tracker/unified')
    g=c.get('games') if isinstance(c,dict) else c
    if not isinstance(g,list): print(lg,'card?',str(c)[:200]); continue
    tr={}
    for r in (t.get('rows') or []): tr.setdefault(str(r.get('game_id')),r)
    bad=0
    for x in g:
        r=tr.get(str(x.get('game_id'))); bp=pk(x.get('best_pick'))
        trp=(r['pick'],r['odds'],r['edge']) if r else None
        mism = (trp or bp) and (not trp or not bp or bp[0]!=trp[0])
        if mism: bad+=1; print(f"  XX {lg} {x.get('away_abbr')}@{x.get('home_abbr')} {x.get('status')} | card {bp} | tracker {trp}")
    print(lg,'games',len(g),'mismatch',bad)
