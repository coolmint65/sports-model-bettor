import sqlite3, os, sys, re
APPLY = "--apply" in sys.argv
os.chdir(r"C:\sports-model-bettor")
c=sqlite3.connect(r"data\nhl.db"); c.row_factory=sqlite3.Row
def last_odds(date,home,away):
    return c.execute("select * from nhl_odds where game_date=? and home_abbr=? and away_abbr=? order by captured_at desc limit 1",(date,home,away)).fetchone()
def extract(bt,pick,home,away,od):
    bt=bt.strip(); pk=pick.strip()
    if bt=="ML":
        tok=pk.split()[0]
        if tok==home: return od["home_ml"]
        if tok==away: return od["away_ml"]
        return None
    if bt=="PL":
        m=re.match(r"([A-Z]{2,4})\s*([+-]?\d+\.?\d*)",pk)
        if not m: return None
        tok=m.group(1)
        if tok==home: return od["home_spread_odds"]
        if tok==away: return od["away_spread_odds"]
        return None
    if bt in ("O/U","O-U"):
        m=re.search(r"(Over|Under)\s+([\d.]+)",pk,re.I)
        if not m: return None
        side=m.group(1).lower(); line=float(m.group(2))
        if od["over_under"] is None or abs(float(od["over_under"])-line)>1e-6: return None  # line moved -> skip
        return od["over_odds"] if side=="over" else od["under_odds"]
    return None
miss=c.execute("select id,matchup,bet_type,pick,odds,date from nhl_picks where closing_odds is null").fetchall()
import collections
can=collections.Counter(); cant=collections.Counter(); samples=[]
to_write=[]
for p in miss:
    m=p["matchup"]; sep=" @ " if " @ " in m else "@"; parts=m.split(sep)
    if len(parts)!=2: cant["bad_matchup"]+=1; continue
    away,home=parts[0].strip(),parts[1].strip()
    od=last_odds(p["date"],home,away)
    if not od: cant["no_odds_row"]+=1; continue
    cl=extract(p["bet_type"],p["pick"],home,away,od)
    if cl is None: cant[p["bet_type"]+"_noextract"]+=1; continue
    try:
        ci=int(cl)
        if abs(ci)<100 or abs(ci)>5000: cant["sentinel"]+=1; continue
    except: cant["badint"]+=1; continue
    can[p["bet_type"]]+=1; to_write.append((ci,p["id"]))
    if len(samples)<6: samples.append(f"  {p['bet_type']:4} '{p['pick']}' ({home}v{away}) -> close {ci} (entry {p['odds']})")
print("CAN backfill by bet_type:",dict(can),"=> total",sum(can.values()))
print("CANNOT:",dict(cant))
print("samples:"); [print(s) for s in samples]
if APPLY:
    import time
    ts=time.strftime("%Y-%m-%dT%H:%M:%S")
    has_ts=any(d[1]=="closing_odds_captured_at" for d in c.execute("PRAGMA table_info(nhl_picks)"))
    for ci,pid in to_write:
        if has_ts: c.execute("update nhl_picks set closing_odds=?, closing_odds_captured_at=? where id=? and closing_odds is null",(ci,ts,pid))
        else: c.execute("update nhl_picks set closing_odds=? where id=? and closing_odds is null",(ci,pid))
    c.commit()
    print(f"APPLIED: wrote {len(to_write)} closing_odds")
    tot=c.execute("select count(*) from nhl_picks").fetchone()[0]
    wc=c.execute("select count(*) from nhl_picks where closing_odds is not null").fetchone()[0]
    print(f"NEW coverage: {wc}/{tot} ({100*wc//tot}%)")
else:
    print("(DRY-RUN — no writes. add --apply to write)")
