import sqlite3, os, sys, re
APPLY="--apply" in sys.argv
os.chdir(r"C:\sports-model-bettor")
c=sqlite3.connect(r"data\mlb.db"); c.row_factory=sqlite3.Row
def last_odds(gid):
    return c.execute("select * from odds where game_id=? order by updated_at desc limit 1",(gid,)).fetchone()
def teams(matchup):
    sep=" @ " if " @ " in matchup else "@"; p=matchup.split(sep)
    return (p[0].strip(),p[1].strip()) if len(p)==2 else (None,None)
def extract(bt,pick,home,away,od):
    bt=bt.strip(); pk=pick.strip()
    if bt=="ML":
        t=pk.split()[0]
        if t==home: return od["home_ml"]
        if t==away: return od["away_ml"]
    elif bt=="RL":
        m=re.match(r"([A-Z0-9]{2,4})\s*[+-]",pk)
        if not m: return None
        t=m.group(1)
        if t==home: return od["home_spread_odds"]
        if t==away: return od["away_spread_odds"]
    elif bt in ("O/U","O-U"):
        m=re.search(r"(Over|Under)\s+([\d.]+)",pk,re.I)
        if not m: return None
        side=m.group(1).lower(); line=float(m.group(2))
        if od["total"] is None or abs(float(od["total"])-line)>1e-6: return None
        return od["over_odds"] if side=="over" else od["under_odds"]
    return None
miss=c.execute("select id,game_id,matchup,bet_type,pick,odds from picks where closing_odds is null and bet_type in ('ML','RL','O/U')").fetchall()
import collections
can=collections.Counter(); cant=collections.Counter(); samples=[]; to_write=[]
for p in miss:
    home,away=teams(p["matchup"])
    if not home: cant["bad_matchup"]+=1; continue
    od=last_odds(p["game_id"])
    if not od: cant["no_odds"]+=1; continue
    cl=extract(p["bet_type"],p["pick"],home,away,od)
    if cl is None: cant[p["bet_type"]+"_noextract"]+=1; continue
    try:
        ci=int(cl)
        if abs(ci)<100 or abs(ci)>5000: cant["sentinel"]+=1; continue
    except: cant["badint"]+=1; continue
    can[p["bet_type"]]+=1; to_write.append((ci,p["id"]))
    if len(samples)<6: samples.append(f"  {p['bet_type']:3} '{p['pick']}' ({away}@{home}) -> {ci} (entry {p['odds']})")
print("CAN:",dict(can),"=> total",sum(can.values()))
print("CANNOT:",dict(cant))
[print(s) for s in samples]
if APPLY:
    import time; ts=time.strftime("%Y-%m-%dT%H:%M:%S")
    hs=any(d[1]=="closing_odds_captured_at" for d in c.execute("PRAGMA table_info(picks)"))
    for ci,pid in to_write:
        if hs: c.execute("update picks set closing_odds=?,closing_odds_captured_at=? where id=? and closing_odds is null",(ci,ts,pid))
        else: c.execute("update picks set closing_odds=? where id=? and closing_odds is null",(ci,pid))
    c.commit()
    tot=c.execute("select count(*) from picks").fetchone()[0]; wc=c.execute("select count(*) from picks where closing_odds is not null").fetchone()[0]
    print(f"APPLIED {len(to_write)} | NEW coverage {wc}/{tot} ({100*wc//tot}%)")
else: print("(DRY-RUN)")
