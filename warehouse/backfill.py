"""Resumable, polite ESPN backfill into wh.db.

    .venv\\Scripts\\python.exe -m warehouse.backfill run              # the whole plan, in priority order
    .venv\\Scripts\\python.exe -m warehouse.backfill run --only nhl:2027,nba:2027 --phases days,odds
    .venv\\Scripts\\python.exe -m warehouse.backfill status

Plan (brief order): game lines NHL -> NBA -> NFL -> CFB -> NCAAB -> WNBA -> EPL (current season + 2 prior),
then props + box scores (NBA, NHL via ESPN BET where the imported DK history has none, NFL 2024),
then MLB game lines last.  Phases per (league, season):
  days  : scoreboard per date -> teams, events, results, period scores
  odds  : core odds per event -> every provider's open / close (and current for upcoming games)
  props : propBets per event (provider 100 DK, else 58 ESPN BET) -> player O/U + yes/no, team period markets
  box   : summary per event with props -> player_results (+ dnp)
Progress: wh.db ``bf_done`` (kind, key). A finished key is never fetched again; dates/events that were
not final yet are retried on later runs. Pace: warehouse.espn MIN_INTERVAL (0.4 s => 2.5 req/s).
Log: data/warehouse/logs/backfill.log.  Safe to kill at any time.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

from . import db as D
from . import espn as E

log = logging.getLogger("wh.backfill")

SEASONS = {"nhl": [2027, 2026, 2025], "nba": [2027, 2026, 2025], "ncaab": [2027, 2026, 2025],
           "nfl": [2026, 2025, 2024], "cfb": [2026, 2025, 2024], "wnba": [2026, 2025, 2024],
           "epl": [2026, 2025, 2024], "mlb": [2026, 2025, 2024]}
GAME_ORDER = ["nhl", "nba", "nfl", "cfb", "ncaab", "wnba", "epl"]
PROP_LEAGUES = ["nba", "nhl", "nfl"]
# NFL 2025 + 2026 props are imported from the nflcal set (Pi research archive), not refetched.
PROP_SKIP = {("nfl", 2025), ("nfl", 2026)}
REG_PERIODS = {"nhl": 3, "nba": 4, "wnba": 4, "ncaab": 2, "nfl": 4, "cfb": 4, "mlb": 9, "epl": 2}


def season_window(league: str, s: int) -> tuple[date, date]:
    if league in ("nhl", "nba"):
        return date(s - 1, 9, 15), date(s, 6, 30)
    if league == "ncaab":
        return date(s - 1, 11, 1), date(s, 4, 10)
    if league == "nfl":
        return date(s, 8, 1), date(s + 1, 2, 20)
    if league == "cfb":
        return date(s, 8, 20), date(s + 1, 1, 25)
    if league == "wnba":
        return date(s, 5, 1), date(s, 10, 31)
    if league == "mlb":
        return date(s, 3, 15), date(s, 11, 10)
    if league == "epl":
        return date(s, 8, 1), date(s + 1, 6, 1)
    raise ValueError(league)


def period_code(league: str, i: int) -> str:
    """i = 1-based linescore index -> period code."""
    reg = REG_PERIODS[league]
    if league == "nhl":
        return f"P{i}" if i <= 3 else ("OT" if i == 4 else ("SO" if i == 5 else f"OT{i - 3}"))
    if league in ("nba", "wnba", "nfl", "cfb"):
        return f"Q{i}" if i <= 4 else f"OT{i - 4}"
    if league in ("ncaab", "epl"):
        return f"H{i}" if i <= 2 else f"OT{i - 2}"
    if league == "mlb":
        return f"I{i}"
    return str(i)


# ------------------------------------------------------------------------------------------ events

def store_event(c, ids: D.Ids, league: str, ev: dict, observed_ts: int | None = None) -> int:
    h, a = ev["home"], ev["away"]

    def team(t):
        return ids.team(league, t["espn_id"], t.get("abbr"), t.get("name"), t.get("short"),
                        aliases=(t.get("nick"),) if league not in ("ncaab", "cfb") else ())
    hid, aid = team(h), team(a)
    eid = D.upsert_espn_event(c, league, ev["espn_id"], ev.get("season"), ev.get("stype"), ev.get("start_ts"),
                              hid, aid, ev.get("neutral", 0), ev.get("name"))
    st = ev["status"]
    now = int(time.time())
    if st in ("final", "postponed", "canceled", "void", "suspended"):
        start = ev.get("start_ts") or now
        fin = int(start + E.FINAL_AFTER.get(league, 4 * 3600))
        if observed_ts and st == "final":
            prev = c.execute("SELECT final_ts, status FROM results WHERE event_id=?", (eid,)).fetchone()
            if prev and prev[1] == "final" and prev[0]:
                fin = prev[0]
            else:
                fin = max(fin, observed_ts) if observed_ts < start + 24 * 3600 else fin
        nper = max(len(h.get("lines") or []), len(a.get("lines") or []))
        ot = 1 if (st == "final" and nper > REG_PERIODS.get(league, 99)) else 0
        c.execute("INSERT INTO results(event_id, status, home, away, periods, ot, final_ts, src, updated_ts) "
                  "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET status=excluded.status, "
                  "home=excluded.home, away=excluded.away, periods=excluded.periods, ot=excluded.ot, "
                  "final_ts=COALESCE(results.final_ts, excluded.final_ts), src=excluded.src, "
                  "updated_ts=excluded.updated_ts",
                  (eid, st, h.get("score") if st == "final" else None, a.get("score") if st == "final" else None,
                   nper or None, ot, fin if st == "final" else None, "espn", now))
        if st == "final":
            for i in range(nper):
                hv = (h.get("lines") or [None] * nper)[i] if i < len(h.get("lines") or []) else None
                av = (a.get("lines") or [None] * nper)[i] if i < len(a.get("lines") or []) else None
                c.execute("INSERT OR REPLACE INTO period_scores(event_id, period, home, away) VALUES (?,?,?,?)",
                          (eid, period_code(league, i + 1), hv, av))
    return eid


def phase_days(c, ids, league, season, stop_at=None) -> int:
    d0, d1 = season_window(league, season)
    today = datetime.now(timezone.utc).date()
    end = min(d1, today + timedelta(days=7))
    n = 0
    d = d0
    while d <= end:
        if stop_at and time.time() > stop_at:
            break
        key = f"{league}:{d:%Y%m%d}"
        if D.done(c, "day", key) != "ok":
            sb = E.get_json(E.scoreboard_url(league, f"{d:%Y%m%d}"))
            evs = [E.parse_event(e, league) for e in (sb or {}).get("events") or []]
            evs = [e for e in evs if e]
            keep = [e for e in evs if (e.get("season") in (season, None))]
            for ev in keep:
                store_event(c, ids, league, ev)
            settled = all(e["status"] in ("final", "postponed", "canceled", "void") for e in keep)
            if d < today - timedelta(days=2) or (settled and d < today):
                D.mark(c, "day", key, "ok", f"n={len(keep)}")
            c.commit()
            n += len(keep)
        d += timedelta(days=1)
    return n


# ------------------------------------------------------------------------------------------ odds

def store_game_odds(c, ids, qw: D.QuoteWriter, league, event_id, start_ts, rows, now=None, final=True) -> int:
    now = now or int(time.time())
    lead = E.OPEN_LEAD.get(league, 18 * 3600)
    n = 0
    for r in rows:
        book = ids.book(r["book"], r["book_name"], espn_provider=r["provider"])
        mid = ids.market(event_id, r["family"], "FG", 0, "")
        side = D.SIDES[r["side"]]
        lk = D.l100(r["line"]) if r["family"] in ("SPREAD", "TOTAL") else 0
        w = r["which"]
        if w == "open":
            ts, src = (start_ts - lead) if start_ts else now, D.SRC["open"]
        elif w == "close":
            if not final:
                continue
            ts, src = (start_ts - 60) if start_ts else now, D.SRC["close"]
        else:  # current
            if final:
                continue
            ts, src = now, D.SRC["live"]
        if qw.put(mid, book, side, lk, ts, src, r["price"], 1):
            n += 1
    return n


def phase_odds(c, ids, league, season, stop_at=None, only_final=True) -> int:
    qw = D.QuoteWriter(c)
    q = ("SELECT e.id, e.espn_id, e.start_ts, r.status FROM events e LEFT JOIN results r ON r.event_id=e.id "
         "WHERE e.league=? AND e.season=? AND e.espn_id IS NOT NULL ORDER BY e.start_ts")
    n = 0
    for eid, espn_id, start, status in c.execute(q, (league, season)).fetchall():
        if stop_at and time.time() > stop_at:
            break
        if status in ("postponed", "canceled", "void"):
            continue
        final = status == "final"
        if only_final and not final:
            continue
        key = f"{league}:{espn_id}"
        if D.done(c, "odds", key) == "ok":
            continue
        d = E.get_json(E.odds_url(league, espn_id))
        items = (d or {}).get("items") or []
        rows = E.parse_game_odds(items, league)
        n += store_game_odds(c, ids, qw, league, eid, start, rows, final=final)
        qw.flush()
        provs = sorted({str((it.get("provider") or {}).get("id")) for it in items})
        if final:
            D.mark(c, "odds", key, "ok" if items else "empty", "prov=" + ",".join(provs))
        c.commit()
    qw.flush()
    c.commit()
    return n


# ------------------------------------------------------------------------------------------ props

def store_props(c, ids, qw, league, event_id, start_ts, parsed, book_code, book_name, provider, final=True,
                now=None) -> int:
    sport = D.SPORT_OF[league]
    now = now or int(time.time())
    lead = E.PROP_OPEN_LEAD.get(league, 8 * 3600)
    book = ids.book(book_code, book_name, espn_provider=provider)
    n = 0
    for r in parsed["props"]:
        pid = D.player_espn(c, sport, r["ath"])
        mid = ids.market(event_id, r["fam"], "FG", pid, r["stat"])
        lk = D.l100(r["line"])
        if r["which"] == "open":
            ts, src = (start_ts - lead) if start_ts else now, D.SRC["open"]
            main = r["main"]
            if r["over"] and r["under"]:
                io, iu = 1 / r["over"], 1 / r["under"]
                if abs(io / (io + iu) - 0.5) > 0.25:
                    main = 0
        else:
            if final:
                ts, src = (start_ts - 60) if start_ts else now, D.SRC["close"]
            else:
                ts, src = now, D.SRC["live"]
            main = r["main"]
        if r["fam"] == "PROPY":
            if r["over"]:
                n += qw.put(mid, book, D.SIDES["yes"], lk, ts, src, r["over"], main)
            continue
        if r["over"] is None and r["under"] is None:
            # line-only quote (e.g. DK NFL 2026): keep the line, price unknown -> stored with price NULL on
            # a dedicated main=2 flag so "line-only" never looks like a removal.
            n += qw.put(mid, book, D.SIDES["over"], lk, ts, src, None, 2) if src != D.SRC["live"] else 0
            continue
        if r["over"]:
            n += qw.put(mid, book, D.SIDES["over"], lk, ts, src, r["over"], main)
        if r["under"]:
            n += qw.put(mid, book, D.SIDES["under"], lk, ts, src, r["under"], main)
    # team-level period markets (ESPN BET)
    ev = c.execute("SELECT e.home_id, e.away_id, h.espn_id, a.espn_id FROM events e JOIN teams h ON h.id=e.home_id "
                   "JOIN teams a ON a.id=e.away_id WHERE e.id=?", (event_id,)).fetchone()
    if ev and parsed["team"]:
        home_espn, away_espn = ev[2], ev[3]
        tt_seen = {}
        # period "moneylines" can be 3-way (hockey periods: tie possible) -> ML3 when implied sum < .97
        mlsum = {}
        for r in parsed["team"]:
            if r["family"] == "ML" and r["price"]:
                k = (r["period"], r["which"])
                mlsum[k] = mlsum.get(k, 0.0) + 1.0 / r["price"]
        for r in parsed["team"]:
            if r["team_espn_id"] == home_espn:
                sd = "home"
            elif r["team_espn_id"] == away_espn:
                sd = "away"
            else:
                continue
            if r["which"] == "open":
                ts, src = (start_ts - E.OPEN_LEAD.get(league, 18 * 3600)) if start_ts else now, D.SRC["open"]
            else:
                ts, src = ((start_ts - 60) if start_ts else now, D.SRC["close"]) if final else (now, D.SRC["live"])
            if r["family"] == "TT":
                subj = -1 if sd == "home" else -2
                mid = ids.market(event_id, "TT", r["period"], subj, "")
                k = (subj, r["period"], r["which"], r["line"])
                side = D.SIDES["over"] if k not in tt_seen else D.SIDES["under"]  # first of the pair = Over
                tt_seen[k] = 1
                n += qw.put(mid, book, side, D.l100(r["line"]), ts, src, r["price"], 1)
            elif r["family"] in ("ML", "SPREAD"):
                fam = r["family"]
                if fam == "ML" and mlsum.get((r["period"], r["which"]), 2.0) < 0.97:
                    fam = "ML3"
                mid = ids.market(event_id, fam, r["period"], 0, "")
                lk = D.l100(r["line"]) if r["family"] == "SPREAD" else 0
                n += qw.put(mid, book, D.SIDES[sd], lk, ts, src, r["price"], 1)
    return n


def _prop_provider(c, league, espn_id):
    info = (c.execute("SELECT info FROM bf_done WHERE kind='odds' AND key=?", (f"{league}:{espn_id}",)).fetchone()
            or [None])[0] or ""
    provs = set(info.replace("prov=", "").split(",")) if info else set()
    out = []
    if "100" in provs:
        out.append(("100", "dk", "DraftKings"))
    if "58" in provs:
        out.append(("58", "espnbet", "ESPN BET"))
    if not provs:
        out = [("100", "dk", "DraftKings"), ("58", "espnbet", "ESPN BET")]
    return out


def phase_props(c, ids, league, season, stop_at=None) -> int:
    if (league, season) in PROP_SKIP:
        return 0
    qw = D.QuoteWriter(c)
    n = 0
    rows = c.execute("SELECT e.id, e.espn_id, e.start_ts FROM events e JOIN results r ON r.event_id=e.id "
                     "WHERE e.league=? AND e.season=? AND r.status='final' ORDER BY e.start_ts",
                     (league, season)).fetchall()
    for eid, espn_id, start in rows:
        if stop_at and time.time() > stop_at:
            break
        key = f"{league}:{espn_id}"
        if D.done(c, "props", key) in ("ok", "empty", "imported"):
            continue
        got = None
        for prov, code, name in _prop_provider(c, league, espn_id):
            if league == "nhl" and prov == "100" and D.done(c, "props", key + ":dk") == "imported":
                continue
            items, page = [], 1
            while True:
                d = E.get_json(E.propbets_url(league, espn_id, prov, page))
                if not d:
                    break
                items += d.get("items") or []
                if page >= int(d.get("pageCount") or 1):
                    break
                page += 1
            if items:
                parsed = E.parse_propbets(items, D.SPORT_OF[league])
                n += store_props(c, ids, qw, league, eid, start, parsed, code, name, prov, final=True)
                got = (got or "") + f"{prov}:{len(items)} "
        qw.flush()
        D.mark(c, "props", key, "ok" if got else "empty", got)
        c.commit()
    return n


def phase_box(c, league, season, stop_at=None) -> int:
    """Box scores for events that have prop quotes (or all final events for prop leagues)."""
    sport = D.SPORT_OF[league]
    n = 0
    rows = c.execute(
        "SELECT e.id, e.espn_id FROM events e JOIN results r ON r.event_id=e.id WHERE e.league=? AND e.season=? "
        "AND r.status='final' AND EXISTS (SELECT 1 FROM markets m WHERE m.event_id=e.id AND m.family IN ('PROP','PROPY')) "
        "ORDER BY e.start_ts", (league, season)).fetchall()
    for eid, espn_id in rows:
        if stop_at and time.time() > stop_at:
            break
        key = f"{league}:{espn_id}"
        if D.done(c, "box", key) in ("ok", "imported"):
            continue
        if c.execute("SELECT 1 FROM player_results WHERE event_id=? LIMIT 1", (eid,)).fetchone():
            D.mark(c, "box", key, "imported")
            continue
        s = E.get_json(E.summary_url(league, espn_id))
        recs = E.parse_box(s, sport)
        for r in recs:
            pid = D.player_espn(c, sport, r["ath"], r.get("name"))
            c.execute("INSERT OR REPLACE INTO player_results(event_id, player_id, stat, value, src) VALUES (?,?,?,?,?)",
                      (eid, pid, r["stat"], r["value"], "espn_box"))
        D.mark(c, "box", key, "ok" if recs else "empty", f"n={len(recs)}")
        c.commit()
        n += len(recs)
    return n


# ------------------------------------------------------------------------------------------ driver

def plan(only=None):
    steps = []
    for lg in GAME_ORDER:
        for s in SEASONS[lg]:
            steps.append((lg, s, ("days", "odds")))
    for lg in PROP_LEAGUES:
        for s in SEASONS[lg]:
            steps.append((lg, s, ("props", "box")))
    for s in SEASONS["mlb"]:
        steps.append(("mlb", s, ("days", "odds")))
    if only:
        want = {tuple(x.split(":")) for x in only}
        steps = [st for st in steps if (st[0], str(st[1])) in want]
    return steps


def run(only=None, phases=None, max_hours=None):
    c = D.open_db()
    ids = D.Ids(c)
    stop_at = time.time() + max_hours * 3600 if max_hours else None
    for lg, s, ph in plan(only):
        for p in ph:
            if phases and p not in phases:
                continue
            t0 = time.time()
            r0 = E.STATS["requests"]
            try:
                if p == "days":
                    n = phase_days(c, ids, lg, s, stop_at)
                elif p == "odds":
                    n = phase_odds(c, ids, lg, s, stop_at)
                elif p == "props":
                    n = phase_props(c, ids, lg, s, stop_at) if lg in PROP_LEAGUES else 0
                else:
                    n = phase_box(c, lg, s, stop_at) if lg in PROP_LEAGUES else 0
            except E.EspnError as e:
                log.warning("%s %s %s: ESPN error %s -- moving on", lg, s, p, e)
                c.commit()
                continue
            log.info("%s %s %-5s rows=%d requests=%d %.0fs", lg, s, p, n, E.STATS["requests"] - r0, time.time() - t0)
            if stop_at and time.time() > stop_at:
                log.info("max-hours reached; stopping (resumable)")
                return
    log.info("plan complete. ESPN stats %s", E.STATS)


def status():
    c = D.connect(readonly=True)
    print("bf_done:")
    for r in c.execute("SELECT kind, status, COUNT(*) FROM bf_done GROUP BY kind, status ORDER BY kind"):
        print("  ", r)
    print("events by league/season (with result final, with any quote):")
    for r in c.execute(
            "SELECT e.league, e.season, COUNT(*), SUM(r.status='final'), "
            "SUM(EXISTS(SELECT 1 FROM markets m WHERE m.event_id=e.id)) FROM events e "
            "LEFT JOIN results r ON r.event_id=e.id GROUP BY e.league, e.season ORDER BY e.league, e.season"):
        print("  ", r)
    print("quotes:", c.execute("SELECT COUNT(*) FROM quotes").fetchone()[0])


def _setup_logging():
    os.makedirs(os.path.join(D.WH_DIR, "logs"), exist_ok=True)
    fh = logging.FileHandler(os.path.join(D.WH_DIR, "logs", "backfill.log"), encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    root.addHandler(sh)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="warehouse.backfill")
    ap.add_argument("cmd", choices=["run", "status"])
    ap.add_argument("--only", help="comma list league:season, e.g. nhl:2027,nba:2026")
    ap.add_argument("--phases", help="comma list of days,odds,props,box")
    ap.add_argument("--max-hours", type=float)
    a = ap.parse_args(argv)
    if a.cmd == "status":
        return status()
    _setup_logging()
    lock = os.path.join(D.WH_DIR, "backfill.lock")
    try:
        if os.path.exists(lock) and time.time() - os.path.getmtime(lock) < 900:
            pid = open(lock).read().strip()
            if pid and _pid_alive(int(pid)):
                log.info("another backfill (pid %s) is running; exiting", pid)
                return
        with open(lock, "w") as fh:
            fh.write(str(os.getpid()))
        _start_heartbeat(lock)
        run(a.only.split(",") if a.only else None, set(a.phases.split(",")) if a.phases else None, a.max_hours)
    finally:
        try:
            if open(lock).read().strip() == str(os.getpid()):
                os.remove(lock)
        except Exception:
            pass


def _pid_alive(pid: int) -> bool:
    try:
        if os.name == "nt":
            import ctypes
            h = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
            if not h:
                return False
            code = ctypes.c_ulong()
            ctypes.windll.kernel32.GetExitCodeProcess(h, ctypes.byref(code))
            ctypes.windll.kernel32.CloseHandle(h)
            return code.value == 259
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _start_heartbeat(lock):
    import threading

    def beat():
        while True:
            try:
                os.utime(lock, None)
            except Exception:
                pass
            time.sleep(120)
    threading.Thread(target=beat, daemon=True).start()


if __name__ == "__main__":
    main()
