"""HR board inventory for a football league: what Hard Rock offers vs what engine.football._odds
consumes. Usage: python hr_board_inventory.py cfb|nfl   (MintyBot 2026-10-25; Austin wanted the
CFB "markets we're missing" list -- HR only posts alts/periods/props close to game day, so the Pi
cron snapshots the board on a Saturday morning.)"""
import sys, logging, collections, datetime
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path[:0] = [r"C:\sports-model-bettor", r"C:\sports-model-bettor\backend"]
logging.basicConfig(level=logging.ERROR, stream=sys.stdout)
from scrapers.hardrock_odds import _fetch_events_for_comp
from engine.football._db import get_league_config

LEAGUE = (sys.argv[1] if len(sys.argv) > 1 else "cfb").lower()
# market types engine.football._odds parses today (periods = H1/H2/Q1-Q4 since 2026-10-25)
CONSUMED = {"FTOT:ML", "FTOT:SPRD", "FTOT:OU", "FTOT:A:OU", "FTOT:B:OU", "P:SPRD", "P:OU", "P:DNB", "P:AXB",
            "P:A:OU", "P:B:OU", "P:BTTSPTS", "FT:BTTSPTS", "FT:BTTSXPTS", "FT:A:LEADEVERYQUARTER", "FT:B:LEADEVERYQUARTER"}


def family(t):
    if t in CONSUMED:
        return "TAKEN (parsed + priced)"
    if "PROP" in t or t.startswith("FTOT:PCATCH") or "NTDP" in t or "PTD" in t and "KERO" in t:
        return "player props"
    if "KERO" in t:
        return "in-game drive/play markets (Kero)"
    if t.startswith("P:"):
        return "other period markets"
    if any(k in t for k in ("OE", "EXACTSCORE", "FT:OT", "BTTS", "MARGIN", "RACE", "SCOREEVERYQUARTER", "HIGHEST", "WINBOTHHALVES", "COMEBACK")):
        return "game-script"
    if any(k in t for k in ("YARD", "YDS", "RUSHATT", "OFFYARD", "COMBPASSRUSH", "RSRCYDS")):
        return "team yardage"
    if any(k in t for k in ("FG", "PAT", "KPTS", "PASSTD", "RUSHTD", "INTERCEPTION", "PUNT", "TOUCHBACK", "RZ", "REDZONE",
                            "2PT", "TWOPOINT", "SAFETY", "METH", "TD", "TURNOVER", "SACK", "PICK6", "TOORTD", "BLKPUNT")):
        return "scoring / game-stat props"
    return "other"


cfg = get_league_config(LEAGUE)
events, err = _fetch_events_for_comp(str(cfg.get("hr_comp_id")))
now = datetime.datetime.now()
print(f"HR {LEAGUE.upper()} board inventory @ {now:%Y-%m-%d %H:%M} local | comp {cfg.get('hr_comp_id')} | events {len(events or [])} | err {err}")
types = collections.Counter(); per = collections.defaultdict(collections.Counter); names = {}; ev_with = collections.Counter()
per_event = []
for ev in events or []:
    seen = set(); n = 0
    for m in ev.get("markets") or []:
        t = (m.get("type") or "?").replace("AMERICAN_FOOTBALL:", ""); p = (m.get("period") or "").upper() or "-"
        types[t] += 1; per[t][p] += 1; names.setdefault(t, m.get("name") or ""); n += 1
        if t not in seen:
            ev_with[t] += 1; seen.add(t)
    per_event.append((n, ev.get("name")))
total = sum(types.values())
per_event.sort(reverse=True)
print(f"markets {total} across {len(types)} types | per-event markets: max {per_event[0][0] if per_event else 0} "
      f"median {per_event[len(per_event)//2][0] if per_event else 0} min {per_event[-1][0] if per_event else 0}")
print("richest events:", per_event[:5])
fam = collections.defaultdict(lambda: [0, 0, []])
for t, n in types.items():
    f = family(t); fam[f][0] += n; fam[f][1] += 1; fam[f][2].append((n, t))
for f, (n, k, lst) in sorted(fam.items(), key=lambda kv: -kv[1][0]):
    print(f"\n== {f}: {n} markets ({100.0 * n / max(1, total):.0f}%), {k} types")
    for cnt, t in sorted(lst, reverse=True)[:25]:
        pp = ",".join(f"{p}:{x}" for p, x in per[t].most_common(6))
        print(f"   {t:32s} {cnt:6d} on {ev_with[t]:3d} ev  {pp[:40]:40s} {names.get(t, '')[:44]}")
