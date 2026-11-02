"""Import the NFL prop line set (Pi research archive data/research/nflcal/bt.jsonl, 9,753 lines: ESPN BET 2025
open+close, DK 2026 lines) into wh.db as PROP quotes + player_results. Idempotent (quotes keyed by ts/src).
    .venv\\Scripts\\python.exe -m warehouse.import_nflcal data\\warehouse\\import\\nflcal_bt.jsonl"""
from __future__ import annotations

import json
import sys

from . import db as D
from . import espn as E

BOOK = {"58": ("espnbet", "ESPN BET"), "100": ("dk", "DraftKings")}


def run(path: str) -> dict:
    c = D.open_db()
    ids = D.Ids(c)
    qw = D.QuoteWriter(c)
    lead = E.PROP_OPEN_LEAD["nfl"]
    st = {"rows": 0, "no_event": 0, "quotes": 0, "results": 0}
    ev_cache = {}
    for line in open(path):
        r = json.loads(line)
        st["rows"] += 1
        ek = str(r["event"])
        if ek not in ev_cache:
            ev_cache[ek] = c.execute("SELECT id, start_ts FROM events WHERE league='nfl' AND espn_id=?", (ek,)).fetchone()
        ev = ev_cache[ek]
        if not ev:
            st["no_event"] += 1
            continue
        eid, start = ev
        code, name = BOOK.get(str(r.get("prov")), ("espnbet", "ESPN BET"))
        book = ids.book(code, name, espn_provider=str(r.get("prov")))
        pid = D.player_espn(c, "football", r["espn_id"], pos=r.get("pg"))
        mid = ids.market(eid, "PROP", "FG", pid, r["stat"])
        for which, src, ts in (("open", D.SRC["open"], start - lead), ("close", D.SRC["close"], start - 60)):
            ln = r.get(f"{which}_line")
            if ln is None:
                continue
            o, u = D.am_to_dec(r.get(f"{which}_over")), D.am_to_dec(r.get(f"{which}_under"))
            lk = D.l100(ln)
            if o is None and u is None:
                st["quotes"] += qw.put(mid, book, D.SIDES["over"], lk, ts, src, None, 2)   # line-only (see store_props)
                continue
            if o:
                st["quotes"] += qw.put(mid, book, D.SIDES["over"], lk, ts, src, o, 1)
            if u:
                st["quotes"] += qw.put(mid, book, D.SIDES["under"], lk, ts, src, u, 1)
        if r.get("actual") is not None:
            c.execute("INSERT OR REPLACE INTO player_results(event_id, player_id, stat, value, src) VALUES (?,?,?,?,?)",
                      (eid, pid, r["stat"], float(r["actual"]), "nflcal"))
            st["results"] += 1
        D.mark(c, "props", f"nfl:{ek}:{code}", "imported", "nflcal")
    qw.flush()
    c.commit()
    return st


if __name__ == "__main__":
    print(run(sys.argv[1]))
