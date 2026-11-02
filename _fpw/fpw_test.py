"""First-pick-wins verification on throwaway DB COPIES (never the live DBs)."""
import sys, os, ast, sqlite3, shutil, logging
from pathlib import Path
ROOT = r"C:\sports-model-bettor"
sys.path.insert(0, ROOT); os.chdir(ROOT)
TMP = Path(ROOT) / "_fpw" / "dbcopy"
TMP.mkdir(parents=True, exist_ok=True)
OK = []; BAD = []
def check(name, cond, info=""):
    (OK if cond else BAD).append(name)
    print(("PASS " if cond else "FAIL ") + name + ("  " + str(info) if info else ""))

def snap(src, dst):
    if dst.exists():
        dst.unlink()
    s = sqlite3.connect("file:%s?mode=ro" % src, uri=True); d = sqlite3.connect(str(dst))
    s.backup(d); s.close(); d.close()

def extract(path, names, ns):
    tree = ast.parse(open(path, encoding="utf-8").read())
    for n in tree.body:
        if isinstance(n, ast.FunctionDef) and n.name in names:
            n.decorator_list = []
            exec(compile(ast.Module([n], []), path, "exec"), ns)

# ---------------- NBA ----------------
NBA_LIVE = Path(ROOT) / "data" / "nba.db"
NBA_COPY = TMP / "nba_copy.db"
snap(NBA_LIVE, NBA_COPY)
import engine.sports.nba.db as nbadb
nbadb.DB_PATH = NBA_COPY
from engine.nba_tracker import _paper as P
from engine.nba_db import get_conn as nba_conn
assert Path(nba_conn().execute("PRAGMA database_list").fetchone()[2]).resolve() == NBA_COPY.resolve()
ns = {"logger": logging.getLogger("t")}
extract(os.path.join(ROOT, "backend", "server", "routes_nba.py"),
        {"_nba_recorded_authority_best", "_nba_market_scope", "_nba_flat_profit"}, ns)
auth = ns["_nba_recorded_authority_best"]
G = "FPWTEST1"; D = "2026-11-01"; MU = "AAA @ BBB"
def row(g):
    return [tuple(r) for r in nba_conn().execute(
        "SELECT bet_type,pick,odds,edge,model_prob,closing_odds,result FROM nba_paper_picks WHERE game_id=? ORDER BY id", (g,))]
A = {"type": "H1_TOTAL", "pick": "Over 111.5 1H", "odds": -105, "edge": 6.8, "prob": 0.54}
r = P.record_paper(MU, G, [A], target_date=D, scope="full")
check("nba: first full pick recorded", r.get("recorded") == 1, r)
card = auth(G); conn = nba_conn(); conn.row_factory = sqlite3.Row
check("nba: held == first pick", card and (card["type"], card["pick"], card["odds"]) == ("H1_TOTAL", "Over 111.5 1H", -105), card)
# line/price move on the SAME pick -> no in-place rewrite
r = P.record_paper(MU, G, [dict(A, odds=130, edge=10.6, prob=0.56)], target_date=D, scope="full")
check("nba: price move = no-op", r.get("held") == 1 and row(G) == [("H1_TOTAL", "Over 111.5 1H", -105, 6.8, 0.54, None, None)], row(G))
# new higher-edge candidate in the same view -> still the first pick, no extra row
r = P.record_paper(MU, G, [{"type": "H2_SPREAD", "pick": "BBB -2.5 2H", "odds": 120, "edge": 15.0, "prob": 0.6}], target_date=D, scope="full")
check("nba: higher-edge full candidate = no-op", r.get("held") == 1 and len(row(G)) == 1, row(G))
# legacy families= call (as the old piggyback passed) also first-wins
r = P.record_paper(MU, G, [{"type": "ALT TOTAL", "pick": "Under 230.5", "odds": 110, "edge": 18.0, "prob": 0.6}], target_date=D,
                   families=("ML", "SPREAD", "TOTAL", "ALT SPREAD", "ALT TOTAL"))
check("nba: legacy families= full call = no-op", r.get("held") == 1 and len(row(G)) == 1, row(G))
# later Q1-view pick with a much higher edge*prob: recorded as the Q1 view's evidence, does NOT displace the headline
Q = {"type": "Q1 Team Total", "pick": "BBB Q1 Over 29.5", "odds": -105, "edge": 16.4, "prob": 0.676}
r = P.record_paper(MU, G, [Q], target_date=D, scope="q1")
check("nba: q1 view recorded once", r.get("recorded") == 1 and len(row(G)) == 2, row(G))
r = P.record_paper(MU, G, [{"type": "Q1_TOTAL", "pick": "Over 58.5 Q1", "odds": 160, "edge": 15.8, "prob": 0.45}], target_date=D, scope="q1")
check("nba: q1 swap candidate = no-op", r.get("held") == 1 and len(row(G)) == 2, row(G))
card = auth(G); trk = auth(G, conn=conn); sb = P.recorded_nba_paper_pick(G)
check("nba: card holds FIRST pick after later higher-ranked Q1 row", card["pick"] == "Over 111.5 1H" and card["odds"] == -105, card)
check("nba: tracker == card", (trk["type"], trk["pick"], trk["odds"]) == (card["type"], card["pick"], card["odds"]), trk)
check("nba: live/final scoreboard pick == card", (sb["type"], sb["pick"], sb["odds"]) == (card["type"], card["pick"], card["odds"]), sb)
# closing-odds capture + settlement still write their own columns; re-record stays a no-op
conn.execute("UPDATE nba_paper_picks SET closing_odds=115, closing_odds_captured_at=datetime('now') WHERE game_id=? AND bet_type='H1_TOTAL'", (G,)); conn.commit()
P.record_paper(MU, G, [dict(A, odds=150)], target_date=D, scope="full")
check("nba: closing odds survive re-record", row(G)[0][5] == 115 and row(G)[0][2] == -105, row(G)[0])
conn.execute("UPDATE nba_paper_picks SET result='W', profit=0.95 WHERE game_id=? AND bet_type='H1_TOTAL'", (G,)); conn.commit()
r = P.record_paper(MU, G, [{"type": "SPREAD", "pick": "BBB -3.5", "odds": -110, "edge": 9, "prob": 0.55}], target_date=D, scope="full")
card = auth(G)
check("nba: settled pick still blocks + held w/ result", r.get("held") == 1 and card["result"] == "W" and card["profit"] is not None, card)
# a VOIDED row does not block; the next pick records and becomes the held pick
G2 = "FPWTEST2"
P.record_paper(MU, G2, [A], target_date=D, scope="full")
conn.execute("UPDATE nba_paper_picks SET result='V' WHERE game_id=?", (G2,)); conn.commit()
r = P.record_paper(MU, G2, [{"type": "SPREAD", "pick": "BBB -3.5", "odds": -110, "edge": 9, "prob": 0.55}], target_date=D, scope="full")
check("nba: void row does not block", r.get("recorded") == 1 and auth(G2)["pick"] == "BBB -3.5", auth(G2))
# same-pass ordering: headline in the Q1 view -> piggyback records q1 FIRST -> earliest row = headline
G3 = "FPWTEST3"
for sc, pl in [("q1", [Q]), ("full", [A])]:
    P.record_paper(MU, G3, pl, target_date=D, scope=sc)
check("nba: q1-headline game holds the q1 pick", auth(G3)["pick"] == Q["pick"], auth(G3))
# real scope helper sanity
check("nba: scope helper", P.paper_scope_of("Q1 Team Total") == "q1" and P.paper_scope_of("H1_TOTAL") == "full")

# historic impact (copy, read-only): earliest-row vs the old max-of-two rule
old_ns = {"logger": logging.getLogger("t")}
extract(os.path.join(ROOT, "backend", "server", "routes_nba.py.bak-20261101-fpw"),
        {"_nba_recorded_authority_best", "_nba_market_scope", "_nba_flat_profit"}, old_ns)
gids = [x[0] for x in conn.execute("SELECT DISTINCT game_id FROM nba_paper_picks WHERE game_id NOT LIKE 'FPWTEST%'")]
diff = [];
def pl(b):
    return (b or {}).get("profit") or 0
for g in gids:
    o = old_ns["_nba_recorded_authority_best"](g, conn=conn); n = auth(g, conn=conn)
    if (o or {}).get("pick") != (n or {}).get("pick") or (o or {}).get("type") != (n or {}).get("type"):
        diff.append((g, (o or {}).get("result"), (n or {}).get("result")))
def tot(fn):
    rs = [fn(g, conn=conn) for g in gids]; rs = [x for x in rs if x and x.get("result") in ("W", "L")]
    return len(rs), sum(1 for x in rs if x["result"] == "W"), round(sum(pl(x) for x in rs), 2)
print("NBA history: games", len(gids), "changed headline", len(diff), "today-changed",
      [d for d in diff if d[1] is None], "old(n,W,$)", tot(old_ns["_nba_recorded_authority_best"]), "new(n,W,$)", tot(auth))

# ---------------- FOOTBALL ----------------
from engine.football import get_league_config, LEAGUE_REGISTRY
NFL_LIVE = Path(ROOT) / get_league_config("nfl")["db_path"]
NFL_COPY = TMP / "nfl_copy.db"
snap(NFL_LIVE, NFL_COPY)
LEAGUE_REGISTRY["nfl"]["db_path"] = str(NFL_COPY)
from engine.football._db import get_conn as fb_conn
fc = fb_conn("nfl")
check("fb: using copy", Path(fc.execute("PRAGMA database_list").fetchone()[2]).resolve() == NFL_COPY.resolve())
import engine.picks_unified._legacy_bridge as LB
LB.mirror_to_unified = lambda **k: None   # keep the test out of the live picks_unified store
from engine.football import _tracker as FT
t = fc.execute("SELECT home_team_id, away_team_id FROM games LIMIT 1").fetchone()
FG = "FPWFB1"
fc.execute("INSERT INTO games (game_id,date,start_time,home_team_id,away_team_id,status) VALUES (?,?,?,?,?,'scheduled')",
           (FG, "2026-11-08", "2026-11-08T18:00:00Z", t[0], t[1])); fc.commit()
fns = {"logger": logging.getLogger("t")}
extract(os.path.join(ROOT, "backend", "server", "routes_football.py"),
        {"_football_recorded_authority_pick", "_football_market_scope", "_disp_pick", "_football_flat_profit"}, fns)
fauth = lambda g: fns["_football_recorded_authority_pick"]("nfl", g, fc)
FA = {"type": "SPREAD", "pick": "BBB -3.5", "odds": -110, "edge": 4.2, "prob": 0.55, "matchup": "AAA@BBB"}
def frows(g):
    return [tuple(r) for r in fc.execute("SELECT bet_type,pick,odds,edge,result,is_shadow FROM picks WHERE game_id=? ORDER BY id", (g,))]
r = FT.record_picks("nfl", FG, [FA])
check("fb: first authority pick recorded", r.get("recorded") == 1 and fauth(FG)["pick"] == "BBB -3.5", (r, fauth(FG)))
r = FT.record_picks("nfl", FG, [dict(FA, pick="BBB -2.5", odds=-115, edge=6.0)])
check("fb: line move = no-op (no void, no new row)", r.get("held") == 1 and frows(FG) == [("SPREAD", "BBB -3.5", -110, 4.2, None, 0)], frows(FG))
r = FT.record_picks("nfl", FG, [{"type": "TOTAL", "pick": "Under 44.5", "odds": 100, "edge": 12.0, "prob": 0.56, "matchup": "AAA@BBB"}])
check("fb: higher-edge other-family candidate = no-op", r.get("held") == 1 and len(frows(FG)) == 1, frows(FG))
c1 = fauth(FG)
check("fb: tracker/card hold first pick at entry price", (c1["type"], c1["pick"], c1["odds"]) == ("SPREAD", "BBB -3.5", -110), c1)
# paper (is_shadow=1) row with the SAME key cannot block or be confused with the authority row
r = FT.record_paper("nfl", FG, "AAA@BBB", [dict(FA)])
check("fb: same-key paper row coexists (index has is_shadow)", len(frows(FG)) == 2 and fauth(FG)["pick"] == "BBB -3.5", frows(FG))
fc.execute("UPDATE picks SET closing_odds=-125 WHERE game_id=? AND is_shadow=0", (FG,)); fc.commit()
FT.record_picks("nfl", FG, [FA])
check("fb: closing odds survive", fc.execute("SELECT closing_odds FROM picks WHERE game_id=? AND is_shadow=0", (FG,)).fetchone()[0] == -125)
# old (latest-created) vs new (earliest) on real history, CFB + NFL copies
for lg in ("nfl", "cfb"):
    src = Path(ROOT) / get_league_config(lg)["db_path"]
    cp = TMP / (lg + "_hist.db"); snap(src, cp)
    hc = sqlite3.connect(str(cp)); hc.row_factory = sqlite3.Row
    ofns = {"logger": logging.getLogger("t")}
    extract(os.path.join(ROOT, "backend", "server", "routes_football.py.bak-20261101-fpw"),
            {"_football_recorded_authority_pick", "_football_market_scope", "_disp_pick", "_football_flat_profit"}, ofns)
    gs = [x[0] for x in hc.execute("SELECT DISTINCT game_id FROM picks WHERE COALESCE(is_shadow,0)=0 AND (result IS NULL OR result!='V')")]
    ch = [(g, ofns["_football_recorded_authority_pick"](lg, g, hc)["result"], fns["_football_recorded_authority_pick"](lg, g, hc)["result"])
          for g in gs if ofns["_football_recorded_authority_pick"](lg, g, hc)["pick"] != fns["_football_recorded_authority_pick"](lg, g, hc)["pick"]]
    print(lg, "history: games", len(gs), "changed headline", len(ch), ch)
    hc.close()

# ---------------- live DBs untouched ----------------
for p, q in ((NBA_LIVE, "SELECT COUNT(*) FROM nba_paper_picks WHERE game_id LIKE 'FPWTEST%'"),
             (NFL_LIVE, "SELECT COUNT(*) FROM picks WHERE game_id LIKE 'FPW%'")):
    lc = sqlite3.connect("file:%s?mode=ro" % p, uri=True)
    check("live untouched: " + p.name, lc.execute(q).fetchone()[0] == 0); lc.close()
try:
    from engine.picks_unified._schema import get_conn as uconn
    n = uconn().execute("SELECT COUNT(*) FROM picks WHERE game_key LIKE '%FPW%'").fetchone()[0]
    check("live untouched: picks_unified", n == 0, n)
except Exception as e:
    print("picks_unified check skipped", e)
print("\nRESULT: %d pass, %d fail %s" % (len(OK), len(BAD), BAD))
