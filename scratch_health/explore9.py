import sqlite3, os, sys, re
ROOT = r"C:\sports-model-bettor\data"
def ro(path):
    c = sqlite3.connect("file:" + path + "?mode=ro", uri=True); c.row_factory = sqlite3.Row; return c
n = ro(os.path.join(ROOT, "nba.db")); u = ro(os.path.join(ROOT, "picks_unified.db"))
teams = {}
for r in n.execute("SELECT * FROM nba_teams"):
    d = dict(r); teams[d.get("team_id") or d.get("id")] = d
print("team cols", list(next(iter(teams.values())).keys())[:12])
def abbr(tid):
    t = teams.get(tid) or {}
    return t.get("abbreviation") or t.get("abbr")
games = {str(r["game_id"]): dict(r) for r in n.execute("SELECT * FROM nba_games WHERE date>='2026-10-19'")}
def scope_pts(g, sc):
    hq = [g.get("home_q%d" % i) for i in range(1, 5)]; aq = [g.get("away_q%d" % i) for i in range(1, 5)]
    if sc == "Q1":
        if hq[0] is None: return None
        return hq[0], aq[0]
    if sc == "H1":
        if hq[1] is None: return None
        return hq[0] + hq[1], aq[0] + aq[1]
    if sc == "H2":
        if hq[1] is None or g.get("home_score") is None: return None
        return g["home_score"] - hq[0] - hq[1], g["away_score"] - aq[0] - aq[1]
    return g.get("home_score"), g.get("away_score")
agree = dis = skip = 0; bad = []
for r in u.execute("SELECT id, game_key, matchup, bet_type, pick_text, odds, result FROM picks WHERE sport='nba' AND result IN ('W','L','P') AND pick_date>='2026-10-19'"):
    gid = r["game_key"].split(":")[-1]; g = games.get(gid)
    if not g or g.get("status") != "final":
        skip += 1; continue
    bt = r["bet_type"].upper().replace(" ", "_"); pt = r["pick_text"]
    sc = "H1" if bt.startswith("H1") else "H2" if bt.startswith("H2") else "Q1" if bt.startswith("Q1") else "FULL"
    pts = scope_pts(g, sc)
    if pts is None:
        skip += 1; continue
    hp, ap = pts
    away, home = [x.strip() for x in r["matchup"].split("@")]
    txt = re.sub(r"\s+(Q[1-4]|[12]H|H[12])\s*$", "", pt.strip())
    txt = txt.replace(" Q1 ", " ")
    m_tot = re.match(r"^(?:(\w+)\s+)?(Over|Under)\s+([\d.]+)", txt)
    res = None
    if m_tot:
        team, side, line = m_tot.group(1), m_tot.group(2), float(m_tot.group(3))
        if team and team not in ("Q1",):
            val = hp if team == home else ap if team == away else None
            if val is None: skip += 1; continue
        else:
            val = hp + ap
        res = "P" if val == line else ("W" if (val > line) == (side == "Over") else "L")
    else:
        m_sp = re.match(r"^(\w+)\s+([+-][\d.]+)$", txt)
        m_ml = re.match(r"^(\w+)(?:\s+1H|\s+2H)?\s+ML$", txt) or re.match(r"^(\w+)$", txt)
        if m_sp:
            team, ln = m_sp.group(1), float(m_sp.group(2))
            mine, opp = (hp, ap) if team == home else (ap, hp) if team == away else (None, None)
            if mine is None: skip += 1; continue
            d = mine + ln - opp
            res = "P" if d == 0 else ("W" if d > 0 else "L")
        elif m_ml:
            team = m_ml.group(1)
            mine, opp = (hp, ap) if team == home else (ap, hp) if team == away else (None, None)
            if mine is None: skip += 1; continue
            res = "P" if mine == opp else ("W" if mine > opp else "L")
    if res is None:
        skip += 1; continue
    if res == r["result"]:
        agree += 1
    else:
        dis += 1; bad.append((r["id"], r["matchup"], r["bet_type"], pt, r["odds"], "stored", r["result"], "box", res, sc, pts))
print("agree", agree, "disagree", dis, "skip", skip)
from collections import Counter
print(Counter(b[2] for b in bad))
for b in bad[:40]:
    print(b)
