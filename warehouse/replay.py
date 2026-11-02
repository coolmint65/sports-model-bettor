"""Replay lab: walk-forward, no-peek backtests over wh.db (PickMint V3 #10, MintyBot 2026-11-02).

    from warehouse import replay as R
    bets = R.run(R.every("over"), league="nfl", seasons=[2025], family="PROP", price="close")
    print(R.report(bets))

How it walks: events in start order. For each event a Ctx is built at the decision time (price="open" -> the
book's open quote, "close" -> start - 60 s, "live" -> latest live quote at start - lead_s). The rule only gets
that event's lines priced at or before the decision time, plus ctx.history(): graded lines of events whose
result was VISIBLE (results.final_ts <= decision ts). Picks are graded afterwards against results /
period_scores / player_results. Flat 1u stakes at the quoted decimal price; pushes return the stake.
Report: W-L-P, units, ROI +/- SE, both halves (split at the median bet date), by month, and baselines
(every Over / every Under / every favourite) on the SAME candidate lines, so a rule must beat them.
"""
from __future__ import annotations

import math
import time
from collections import defaultdict
from datetime import datetime, timezone

from . import db as D

SRC_OF = {"open": 2, "close": 3, "live": 1}


def _lines(c, eids, family, book_ids, price, stat, period, main_only):
    """{event_id: [line dict]} for the decision quote of each (market, l100) pair."""
    if not eids:
        return {}
    q = ("SELECT m.event_id, m.id, m.family, m.period, m.subject, m.stat, q.book, q.side, q.l100, q.ts, q.price, q.main "
         "FROM quotes q JOIN markets m ON m.id=q.market_id WHERE m.event_id IN (%s) AND m.family=? AND q.src=?"
         % ",".join("?" * len(eids)))
    args = list(eids) + [family, SRC_OF[price]]
    if period:
        q += " AND m.period=?"; args.append(period)
    if stat:
        q += " AND m.stat=?"; args.append(stat)
    if book_ids:
        q += " AND q.book IN (%s)" % ",".join("?" * len(book_ids)); args += list(book_ids)
    if main_only:
        q += " AND q.main=1"
    pairs = {}
    for ev, mid, fam, per, subj, st, book, side, lk, ts, px, main in c.execute(q, args):
        if px is None:
            continue
        # spreads pair on the HOME-relative line (home -5.5 with away +5.5); each side keeps its own l100
        hl = (lk if side == D.SIDES["home"] else -lk) if fam == "SPREAD" else lk
        k = (ev, mid, book, hl)
        d = pairs.setdefault(k, {"event_id": ev, "market_id": mid, "family": fam, "period": per, "subject": subj,
                                 "stat": st, "book": book, "l100": hl, "ts": ts, "prices": {}, "l100s": {}})
        d["prices"][side] = px
        d["l100s"][side] = lk
        d["ts"] = max(d["ts"], ts)
    out = defaultdict(list)
    for d in pairs.values():
        out[d["event_id"]].append(d)
    return out


def _grade(c, ev, line, side):
    """'W' / 'L' / 'P' / None (no result)."""
    fam, per, subj = line["family"], line["period"], line["subject"]
    lk = line.get("l100s", {}).get(side, line["l100"])     # the side's own line (spreads are side-relative)
    L = lk / 100.0
    if fam == "PROP":
        r = c.execute("SELECT value FROM player_results WHERE event_id=? AND player_id=? AND stat=?",
                      (ev, subj, line["stat"])).fetchone()
        if not r or r[0] is None:
            return None
        v = r[0]
    else:
        if per == "FG":
            r = c.execute("SELECT home, away FROM results WHERE event_id=? AND status='final'", (ev,)).fetchone()
        else:
            r = c.execute("SELECT home, away FROM period_scores WHERE event_id=? AND period=?", (ev, per)).fetchone()
        if not r or r[0] is None or r[1] is None:
            return None
        h, a = r
        if fam in ("TOTAL",):
            v = h + a
        elif fam == "TT":
            v = h if subj == -1 else a
        elif fam in ("ML", "ML3"):
            win = 1 if h > a else 2 if a > h else 3
            return "W" if win == side else ("P" if (win == 3 and fam == "ML") else "L")
        elif fam == "SPREAD":
            m = (h - a) if side == D.SIDES["home"] else (a - h)
            adj = m + L
            return "W" if adj > 0 else ("P" if adj == 0 else "L")
        else:
            return None
    if v == L:
        return "P"
    over = v > L
    return "W" if (over if side == D.SIDES["over"] else not over) else "L"


class Ctx:
    def __init__(self, c, event, lines, decision_ts, league):
        self.c, self.event, self.lines, self.ts, self.league = c, event, lines, decision_ts, league

    def history(self, family=None, since_ts=None):
        """Results visible at decision time (final_ts <= ts) for this league: [(event_id, home, away, final_ts)]."""
        q = ("SELECT r.event_id, r.home, r.away, r.final_ts FROM results r JOIN events e ON e.id=r.event_id "
             "WHERE e.league=? AND r.status='final' AND r.final_ts <= ?")
        args = [self.league, self.ts]
        if since_ts:
            q += " AND r.final_ts >= ?"; args.append(since_ts)
        return self.c.execute(q, args).fetchall()


def every(side_name):
    """Baseline rule: bet one side of every candidate line."""
    sd = D.SIDES[side_name]
    return lambda ctx: [(ln, sd) for ln in ctx.lines if sd in ln["prices"]]


def favourite(ctx):
    out = []
    for ln in ctx.lines:
        p = ln["prices"]
        if len(p) >= 2:
            sd = min(p, key=lambda s: p[s])
            out.append((ln, sd))
    return out


def run(rule, league, seasons, family, price="close", books=("espnbet", "dk"), stat=None, period="FG",
        main_only=True, lead_s=600, two_way=True, db=None):
    c = db or D.connect(readonly=True)
    book_ids = [r[0] for r in c.execute("SELECT id FROM books WHERE code IN (%s)" % ",".join("?" * len(books)),
                                        list(books))] if books else None
    evs = c.execute("SELECT id, start_ts, name FROM events WHERE league=? AND season IN (%s) AND start_ts IS NOT NULL "
                    "ORDER BY start_ts" % ",".join("?" * len(seasons)), [league] + list(seasons)).fetchall()
    bets = []
    for i in range(0, len(evs), 400):
        chunk = evs[i:i + 400]
        lines = _lines(c, [e[0] for e in chunk], family, book_ids, price, stat, period, main_only)
        for eid, start, name in chunk:
            dts = start - (60 if price == "close" else lead_s)
            cand = [ln for ln in lines.get(eid, []) if price != "live" or ln["ts"] <= dts]
            if two_way and family in ("PROP", "TOTAL", "TT", "SPREAD"):   # both sides priced = a real two-way line
                cand = [ln for ln in cand if len(ln["prices"]) >= 2]
            if not cand:
                continue
            if price == "open":   # several books can quote the same line: keep one per (market, line), lowest book id
                seen = {}
                for ln in sorted(cand, key=lambda x: x["book"]):
                    seen.setdefault((ln["market_id"], ln["l100"]), ln)
                cand = list(seen.values())
            ctx = Ctx(c, {"id": eid, "start_ts": start, "name": name}, cand, dts, league)
            for ln, side in rule(ctx) or []:
                px = ln["prices"].get(side)
                if px is None:
                    continue
                g = _grade(c, eid, ln, side)
                if g is None:
                    continue
                pr = (px - 1.0) if g == "W" else (0.0 if g == "P" else -1.0)
                bets.append({"event_id": eid, "date": datetime.fromtimestamp(start, timezone.utc).strftime("%Y-%m-%d"),
                             "market_id": ln["market_id"], "side": side, "l100": ln["l100s"].get(side, ln["l100"]), "price": px,
                             "result": g, "profit": pr, "stat": ln["stat"], "book": ln["book"]})
    return bets


def _roi(p):
    n = len(p)
    if not n:
        return {"n": 0}
    m = sum(p) / n
    se = math.sqrt(sum((x - m) ** 2 for x in p) / (n - 1) / n) if n > 1 else float("nan")
    return {"n": n, "units": round(sum(p), 2), "roi": round(100 * m, 1), "se": round(100 * se, 1)}


def report(bets, label="rule"):
    if not bets:
        return f"{label}: no bets"
    p = [b["profit"] for b in bets if b["result"] != "P"]
    w = sum(1 for b in bets if b["result"] == "W"); l_ = sum(1 for b in bets if b["result"] == "L")
    pu = sum(1 for b in bets if b["result"] == "P")
    ds = sorted(b["date"] for b in bets); split = ds[len(ds) // 2]
    h1 = [b["profit"] for b in bets if b["date"] < split and b["result"] != "P"]
    h2 = [b["profit"] for b in bets if b["date"] >= split and b["result"] != "P"]
    fm = lambda r: (f"n={r['n']} {r['units']:+.1f}u ROI {r['roi']:+.1f}% ±{r['se']}" if r["n"] else "n=0")
    lines = [f"{label}: {w}-{l_}-{pu}  {fm(_roi(p))}",
             f"   H1 (<{split}) {fm(_roi(h1))} | H2 {fm(_roi(h2))}"]
    bym = defaultdict(list)
    for b in bets:
        if b["result"] != "P":
            bym[b["date"][:7]].append(b["profit"])
    lines.append("   by month: " + "  ".join(f"{k} {_roi(v)['roi']:+.0f}% (n={len(v)})" for k, v in sorted(bym.items())))
    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    lg, season, fam, px = (sys.argv[1:5] + ["nfl", "2025", "PROP", "close"][len(sys.argv[1:5]):])[:4]
    t0 = time.time()
    for nm, rl in (("every Over", every("over")), ("every Under", every("under"))):
        print(report(run(rl, lg, [int(season)], fam, px), f"{lg} {season} {fam} @{px} {nm}"))
    print(f"({time.time() - t0:.1f}s)")
