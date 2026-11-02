"""
MLB pitcher-factor calibration harness (point-in-time, no lookahead).

Unlike retrobt.py (which replays the CURRENT model with TODAY's live inputs
and is explicitly "approximate"), this reconstructs the live scoring CORE
  team_offense x pitcher_factor x bullpen_factor x park x home_edge -> NegBin
using ONLY data available before each game (real per-start pitcher lines from
player_game_logs; team runs and reliever ERA to date; park factors). Every
input is held identical across arms so metric deltas isolate the pitcher-factor
change under test. Temporal split: fit on 2023-2024/2025, validate on held-out
2025 and 2026. Metrics: run-total MAE, W/L logloss, Brier, accuracy.

Findings (2026-09-22):
  * Peripheral WHIP/K9/BB9 weights are near-inert as sized; a joint OLS of
    opp-runs on the standardized deviations gives era +0.04, whip +0.10,
    k9 -0.235 (t=-8.1), bb9 +0.03 -- current WHIP(0.10)/BB9(0.06) are already
    right; K9(0.08) looks light BUT raising it does NOT robustly help once ERA
    is regularized (K9-up alone hurt both held-out seasons).
  * The one robust, cross-season win is ERA-SHRINKAGE: point-in-time / proxy
    ERA is over-trusted. Regressing the ERA deviation toward league average
    (x~0.5-0.6) improved logloss + Brier on BOTH 2025 and 2026. Deployed in
    engine/sports/mlb/factors.py::_blended_pitcher (_ERA_SHRINK=0.6).
  * Recency-weighting the starter with real last-5 starts did NOT generalize
    (helped 2026, hurt 2025) -> NOT deployed.
  * Run-total MAE is ~irreducible (~3.57); gains are W/L-side and modest.

Run: python -m engine.sports.mlb.factor_calibrate   (needs numpy)
"""
import sqlite3, json, math, bisect
import numpy as np
from pathlib import Path as _P
DB=str(_P(__file__).resolve().parents[3] / "data" / "mlb.db")
AVG_ERA=4.10; AVG_WHIP=1.28; AVG_K9=8.5; AVG_BB9=3.2; AVG_RPG=4.49
NEGBIN_K=3.46; HOME_EDGE=0.1012; WP_FLOOR,WP_CAP=0.30,0.58; MAXR=20

def load():
    c=sqlite3.connect(DB); c.row_factory=sqlite3.Row
    # per-pitcher cumulative arrays for starts
    sp={}   # pid -> dict of np cumulative arrays + dates
    bp_raw={}  # team -> list (date,er,outs)
    tmp={}
    for r in c.execute("SELECT player_id,team_id,date,stats_json FROM player_game_logs ORDER BY date"):
        try: s=json.loads(r["stats_json"])
        except: continue
        if "outs" not in s and "er" not in s and "k_p" not in s: continue
        outs=s.get("outs") or 0; er=s.get("er") or 0
        if s.get("is_starter")==1:
            tmp.setdefault(r["player_id"],[]).append((r["date"],er,outs,s.get("k_p") or 0,s.get("bb_p") or 0,s.get("h_allowed") or 0))
        else:
            bp_raw.setdefault(r["team_id"],[]).append((r["date"],er,outs))
    for pid,rows in tmp.items():
        rows.sort(key=lambda x:x[0])
        dts=[x[0] for x in rows]
        er=np.cumsum([0]+[x[1] for x in rows]); ou=np.cumsum([0]+[x[2] for x in rows])
        kk=np.cumsum([0]+[x[3] for x in rows]); bb=np.cumsum([0]+[x[4] for x in rows]); hh=np.cumsum([0]+[x[5] for x in rows])
        sp[pid]={"d":dts,"er":er,"ou":ou,"k":kk,"bb":bb,"h":hh}
    bp={}
    for tid,rows in bp_raw.items():
        rows.sort(key=lambda x:x[0]); dts=[x[0] for x in rows]
        er=np.cumsum([0]+[x[1] for x in rows]); ou=np.cumsum([0]+[x[2] for x in rows])
        bp[tid]={"d":dts,"er":er,"ou":ou}
    tg={}
    games=[]
    for r in c.execute("""SELECT date,season,home_team_id,away_team_id,home_score,away_score,
                 home_pitcher_id,away_pitcher_id,venue FROM games WHERE status='final'
                 AND home_score IS NOT NULL AND away_score IS NOT NULL
                 AND home_pitcher_id IS NOT NULL AND away_pitcher_id IS NOT NULL ORDER BY date"""):
        d=dict(r); games.append(d)
        tg.setdefault(d["home_team_id"],[]).append((d["date"],d["home_score"]))
        tg.setdefault(d["away_team_id"],[]).append((d["date"],d["away_score"]))
    tgc={}
    for tid,rows in tg.items():
        rows.sort(key=lambda x:x[0]); dts=[x[0] for x in rows]
        rf=np.cumsum([0]+[x[1] for x in rows]); tgc[tid]={"d":dts,"rf":rf}
    park={}
    for r in c.execute("SELECT venue,run_factor FROM park_factors"): park[r["venue"]]=r["run_factor"] or 1.0
    # proxy: pitcher -> opp scores from games
    spopp={}
    for g in games:
        spopp.setdefault(g["home_pitcher_id"],[]).append((g["date"],g["away_score"]))
        spopp.setdefault(g["away_pitcher_id"],[]).append((g["date"],g["home_score"]))
    spoppc={}
    for pid,rows in spopp.items():
        rows.sort(key=lambda x:x[0]); dts=[x[0] for x in rows]; sc=np.cumsum([0]+[x[1] for x in rows])
        spoppc[pid]={"d":dts,"sc":sc}
    c.close()
    return sp,bp,tgc,park,games,spoppc

def idx(dts,date): return bisect.bisect_left(dts,date)

def sp_stat(spd, date, nrec=0):
    n=idx(spd["d"],date)
    if n==0: return None
    lo=max(0,n-nrec) if nrec>0 else 0
    er=spd["er"][n]-spd["er"][lo]; ou=spd["ou"][n]-spd["ou"][lo]
    k=spd["k"][n]-spd["k"][lo]; bb=spd["bb"][n]-spd["bb"][lo]; h=spd["h"][n]-spd["h"][lo]
    ip=ou/3.0
    if ip<3: return None
    return {"starts":n-lo,"season_starts":n,"era":9*er/ip,"whip":(bb+h)/ip,"k9":9*k/ip,"bb9":9*bb/ip}

def proxy_era(spd,date):
    n=idx(spd["d"],date)
    if n==0: return None
    tot=(spd["sc"][n])*0.60; ip=n*5.5
    return 9*tot/ip if ip>0 else None

def team_off(tgd,date):
    n=idx(tgd["d"],date)
    if n==0: return None
    rf=tgd["rf"][n]; runs_pg=rf/n
    w=min(1.0,n/60.0)
    return w*runs_pg+(1-w)*AVG_RPG

def bp_fac(bpd,date):
    if bpd is None: return 1.0
    n=idx(bpd["d"],date)
    if n==0: return 1.0
    er=bpd["er"][n]; ou=bpd["ou"][n]; ip=ou/3.0
    if ip<20: return 1.0
    return max(0.70,min(1.40,(9*er/ip)/AVG_ERA))

def build(sp,bp,tg,park,games,spopp,nrec=0,wrec=0.0):
    rows=[]
    for g in games:
        hp=sp.get(g["home_pitcher_id"]); ap=sp.get(g["away_pitcher_id"])
        if not hp or not ap: continue
        d=g["date"]
        hps=sp_stat(hp,d); aps=sp_stat(ap,d)
        if not hps or not aps: continue
        ho=team_off(tg.get(g["home_team_id"]),d) if tg.get(g["home_team_id"]) else None
        ao=team_off(tg.get(g["away_team_id"]),d) if tg.get(g["away_team_id"]) else None
        if ho is None or ao is None: continue
        if nrec>0 and wrec>0:
            for st,spd in ((hps,hp),(aps,ap)):
                rec=sp_stat(spd,d,nrec)
                if rec and rec["starts"]>=3:
                    for kk in ("era","whip","k9","bb9"): st[kk]=rec[kk]*wrec+st[kk]*(1-wrec)
        rows.append((ho,ao,hps["era"],hps["whip"],hps["k9"],hps["bb9"],
                     aps["era"],aps["whip"],aps["k9"],aps["bb9"],
                     bp_fac(bp.get(g["home_team_id"]),d),bp_fac(bp.get(g["away_team_id"]),d),
                     park.get(g["venue"],1.0),g["season"],g["home_score"],g["away_score"],
                     1 if g["home_score"]>g["away_score"] else 0,
                     proxy_era(spopp.get(g["home_pitcher_id"]),d) or hps["era"],
                     proxy_era(spopp.get(g["away_pitcher_id"]),d) or aps["era"],
                     hps["season_starts"],aps["season_starts"]))
    A=np.array(rows,dtype=object)
    cols=["ho","ao","he","hw_","hk","hb","ae","aw","ak","ab","hbp","abp","park","season","hs","as_","hwin","hpx","apx","hss","ass"]
    return {c:np.array([r[i] for r in rows],dtype=float) for i,c in enumerate(cols)}

def factor(era,whip,k9,bb9,W):
    s=W[3] if len(W)>3 else 1.0
    rf=1.0+s*(era/AVG_ERA-1.0)
    rf=rf+((whip-AVG_WHIP)/AVG_WHIP)*W[0]-((k9-AVG_K9)/AVG_K9)*W[1]+((bb9-AVG_BB9)/AVG_BB9)*W[2]
    return np.clip(rf,0.60,1.50)

def pmf(mu):
    mu=np.asarray(mu,float); p=NEGBIN_K/(NEGBIN_K+mu); out=np.zeros((len(mu),MAXR+1))
    for x in range(MAXR+1):
        C=math.lgamma(x+NEGBIN_K)-math.lgamma(x+1)-math.lgamma(NEGBIN_K)
        out[:,x]=np.exp(C+NEGBIN_K*np.log(p)+x*np.log(1-p))
    return out/out.sum(1,keepdims=True)

def winp(hx,ax):
    H=pmf(hx);A=pmf(ax);Ac=np.cumsum(A,1);Al=Ac-A
    ph=(H*Al).sum(1);pt=(H*A).sum(1);pa=1-ph-pt;tot=ph+pa
    return ph+np.where(tot>0,pt*(ph/np.where(tot>0,tot,1)),pt/2)

def ev(a,W,sp_share=0.60,era_key=("he","ae")):
    BP=1-sp_share
    hspf=factor(a[era_key[0]],a["hw_"],a["hk"],a["hb"],W)
    aspf=factor(a[era_key[1]],a["aw"],a["ak"],a["ab"],W)
    hx=a["ho"]*aspf*sp_share+a["ho"]*a["abp"]*BP
    ax=a["ao"]*hspf*sp_share+a["ao"]*a["hbp"]*BP
    hx=np.clip(hx*a["park"]+HOME_EDGE/2,2,6.5); ax=np.clip(ax*a["park"]-HOME_EDGE/2,2,6.5)
    pt=hx+ax; at=a["hs"]+a["as_"]; ph=np.clip(winp(hx,ax),1e-6,1-1e-6); y=a["hwin"]
    return {"n":len(y),"tot_mae":np.abs(pt-at).mean(),"tot_rmse":math.sqrt(((pt-at)**2).mean()),
            "logloss":-(y*np.log(ph)+(1-y)*np.log(1-ph)).mean(),
            "brier":((ph-y)**2).mean(),"acc":((ph>0.5)==(y==1)).mean()}
def f(m): return f"n={m['n']:5d} tot_MAE={m['tot_mae']:.4f} logloss={m['logloss']:.5f} brier={m['brier']:.5f} acc={m['acc']*100:.2f}%"
def sub(a,mask): return {k:v[mask] for k,v in a.items()}
def coord(a,metric,grids,start):
    W=list(start);best=ev(a,tuple(W))[metric];imp=True;p=0
    while imp and p<6:
        imp=False;p+=1
        for i in range(len(W)):
            for v in grids[i]:
                c=list(W);c[i]=v;mm=ev(a,tuple(c))[metric]
                if mm<best-1e-7:best=mm;W=c;imp=True
    return tuple(W),best

if __name__=="__main__":
    import sys
    print("loading (prefix sums)...")
    sp,bp,tg,park,games,spopp=load()
    a=build(sp,bp,tg,park,games,spopp)
    s=a["season"]; W_CUR=(0.10,0.08,0.06,1.0)
    tr=sub(a,np.isin(s,[2023,2024,2025])); te26=sub(a,s==2026); te25=sub(a,s==2025)
    tr2=sub(a,np.isin(s,[2023,2024]))
    print(f"recs {len(s)} train {len(tr['hs'])} test26 {len(te26['hs'])}\n")
    print("CURRENT   TEST26:",f(ev(te26,W_CUR)))
    # regression
    def dev(a,side):
        e="he" if side=="h" else "ae"; w="hw_" if side=="h" else "aw"; k="hk" if side=="h" else "ak"; b="hb" if side=="h" else "ab"
        return np.column_stack([a[e]/AVG_ERA-1,(a[w]-AVG_WHIP)/AVG_WHIP,(a[k]-AVG_K9)/AVG_K9,(a[b]-AVG_BB9)/AVG_BB9])
    X=np.vstack([dev(tr,"h"),dev(tr,"a")]); Y=np.concatenate([tr["as_"]/AVG_RPG-1,tr["hs"]/AVG_RPG-1])
    Xd=np.column_stack([np.ones(len(X)),X]); coef,_,_,_=np.linalg.lstsq(Xd,Y,rcond=None)
    resid=Y-Xd@coef; s2=(resid@resid)/(len(Y)-5); se=np.sqrt(np.diag(s2*np.linalg.inv(Xd.T@Xd)))
    print("\nREGRESSION opp_runs_rate ~ devs (2023-25):")
    for nm,cf,e in zip(["intcpt","era","whip","k9","bb9"],coef,se): print(f"  {nm:6s}{cf:+.4f} (t={cf/e:+.1f})")
    grids_sane=[[0,0.05,0.1,0.15],[0,0.08,0.15,0.25,0.35],[0,0.06,0.12,0.2],[0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]]
    grids_wide=[[-0.15,-0.1,-0.05,0,0.1,0.2],[0,0.1,0.2,0.3,0.4],[-0.1,0,0.1,0.2,0.3],[0.1,0.2,0.3,0.4,0.5,0.6,0.8,1.0]]
    Ws,bs=coord(tr,"logloss",grids_sane,W_CUR)
    Ww,bw=coord(tr,"logloss",grids_wide,W_CUR)
    print(f"\nSANE fit (signs enforced) W={tuple(round(x,3) for x in Ws)} trainLL={bs:.5f}")
    print("  TRAIN :",f(ev(tr,Ws))); print("  TEST26:",f(ev(te26,Ws)))
    print(f"WIDE fit W={tuple(round(x,3) for x in Ww)} trainLL={bw:.5f}")
    print("  TEST26:",f(ev(te26,Ww)))
    Wsimple=(0.10,0.20,0.06,0.6)
    print(f"\nSIMPLE deployable W={Wsimple}:  TEST26:",f(ev(te26,Wsimple)))
    print("\nROBUSTNESS fit(23-24) sane -> test 25 & 26:")
    Wr,_=coord(tr2,"logloss",grids_sane,W_CUR)
    print(f"  W={tuple(round(x,3) for x in Wr)}")
    print("  2025 CUR:",f(ev(te25,W_CUR))); print("  2025 TUN:",f(ev(te25,Wr)))
    print("  2026 CUR:",f(ev(te26,W_CUR))); print("  2026 TUN:",f(ev(te26,Wr)))
    print("\nERA INPUT A/B (real vs opp*0.60 proxy), curW TEST26:")
    print("  REAL :",f(ev(te26,W_CUR,era_key=("he","ae"))))
    print("  PROXY:",f(ev(te26,W_CUR,era_key=("hpx","apx"))))
    print("\nRECENCY (real last-N) sane W, TEST26:")
    for nrec,wrec in [(5,0.4),(5,0.5),(3,0.5)]:
        ar=build(sp,bp,tg,park,games,spopp,nrec,wrec); m=ar["season"]==2026
        print(f"  N={nrec} w={wrec}:",f(ev(sub(ar,m),Ws)))
    print("\nDONE_FC3")
