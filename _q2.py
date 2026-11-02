import json,urllib.request
for s in ('nhl','nba','nfl'):
    for p in (f'/api/{s}/player-props/potd',f'/api/{s}/player-props/potd/summary'):
        try: d=json.load(urllib.request.urlopen('http://localhost:8000'+p,timeout=200))
        except Exception as e: d={'err':str(e)}
        if isinstance(d,dict): d={k:v for k,v in d.items() if k not in('l10',)}
        print(s,p.split('/')[-1],json.dumps(d)[:700])
