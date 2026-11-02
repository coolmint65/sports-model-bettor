"""Read-only live check: card headline (best_pick) == tracker row per game, all four boards."""
import json, sys, time, urllib.request
B = "http://localhost:8000/api/"
def get(u, t=180):
    for i in range(3):
        try:
            return json.loads(urllib.request.urlopen(B + u, timeout=t).read())
        except Exception as e:
            err = e; time.sleep(5)
    print("ERR", u, err); return None
def games_of(x):
    if isinstance(x, list): return x
    for k in ("games", "bets", "best_bets", "data"):
        if isinstance(x, dict) and isinstance(x.get(k), list): return x[k]
    return []
def key(p):
    return None if not p else (p.get("type") or p.get("bet_type"), p.get("pick"), p.get("odds"))
for name, card_u, trk_u in (("NBA", "nba/best-bets", "nba/tracker/unified"),
                            ("NHL", "nhl/best-bets", "nhl/tracker/unified"),
                            ("NFL", "football/nfl/today", "football/nfl/tracker/unified"),
                            ("CFB", "football/cfb/today", "football/cfb/tracker/unified")):
    cards = games_of(get(card_u)); trk = get(trk_u) or {}
    tmap = {str(r["game_id"]): r for r in trk.get("rows", [])}
    n = same = 0; bad = []
    for g in cards:
        gid = str(g.get("game_id") or g.get("id"))
        bp = g.get("best_pick")
        t = tmap.get(gid)
        if not bp and not t:
            continue
        n += 1
        tk = None if not t else (t.get("bet_type"), t.get("pick"), t.get("odds"))
        if key(bp) == tk:
            same += 1
        else:
            bad.append((gid, g.get("matchup") or "%s@%s" % (g.get("away_abbr"), g.get("home_abbr")), key(bp), tk))
        if name == "NBA":
            cl = g.get("card_lean") or {}
            print("   NBA %s %s card=%s held=%s | lean=%s grad=%s | tracker=%s" % (
                gid, g.get("matchup"), key(bp), (bp or {}).get("held"), key(cl), cl.get("graduated"), tk))
    print("%s: %d/%d card==tracker %s" % (name, same, n, ("MISMATCH " + str(bad[:6])) if bad else ""))
