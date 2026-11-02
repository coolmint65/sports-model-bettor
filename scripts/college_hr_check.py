"""College HR board check (MintyBot 2026-10-26): how many NCAAM / NCAAW events HR lists, how
many tie to OUR scheduled game (engine.basketball._college_names), and every HR team name that
didn't resolve -- run before the openers and whenever the unresolved log grows.
usage: .venv\\Scripts\\python.exe scripts\\college_hr_check.py"""
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)

from scrapers.hardrock_odds import _fetch_events_for_comp      # noqa: E402
from engine.basketball._config import get_league_config        # noqa: E402
from engine.basketball import _college_names as C              # noqa: E402
from engine.basketball._odds import _build_name_lookup, _parse_event  # noqa: E402

for lg in ("ncaam", "ncaaw"):
    cfg = get_league_config(lg)
    evs, err = _fetch_events_for_comp(str(cfg.get("hr_comp_id")))
    evs = evs or []
    lookup = _build_name_lookup(lg)
    parsed = [p for p in (_parse_event(e, lookup, league_name=lg, league_key=lg) for e in evs) if p]
    ok, flipped, unresolved, unmatched = 0, 0, set(), []
    for p in parsed:
        a = C.attach(lg, p)
        if a:
            ok += 1
            flipped += bool(a.get("sides_flipped"))
            continue
        h, w = C.resolve(lg, p.get("home_full_name")), C.resolve(lg, p.get("away_full_name"))
        if h is None:
            unresolved.add(p.get("home_full_name"))
        if w is None:
            unresolved.add(p.get("away_full_name"))
        if h is not None and w is not None:
            unmatched.append(f"{p.get('away_full_name')} @ {p.get('home_full_name')} {p.get('start_time')}")
    print(f"{lg}: HR events {len(evs)} (err={err}) | with ML/spread/total {len(parsed)} | "
          f"tied to our game {ok} (sides flipped {flipped}) | unresolved names {len(unresolved)} | "
          f"resolved but no scheduled game {len(unmatched)}")
    for n in sorted(x for x in unresolved if x):
        print("   UNRESOLVED:", n)
    for u in unmatched[:20]:
        print("   NO GAME:", u)
