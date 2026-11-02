"""Kalshi sports value scanner (log-only). Compares our Monte-Carlo model's
P(total > line) against Kalshi's full-game total ladder and logs the signed
edge forward, settling later with the actual result for validation.

LOG-ONLY: no placement, no cron, no frontend. Austin 2026-10-07.
Trustworthy only once the model proves it beats Kalshi's number.

NBA is the primary/default target (2026-10-10: Austin finds the value edge
stronger on NBA, especially preseason) with NHL kept as the secondary sport
scanned alongside it. Rows still carry the per-sport `preseason` tag (NBA=1,
NHL=0) for downstream analysis, but that tag is no longer treated as
"unreliable, hide it" -- see routes_kalshi.py, which used to filter
preseason=1 out of the flagged list entirely.
"""
from __future__ import annotations
import os, json, sqlite3, urllib.request
from datetime import datetime, timezone
import numpy as np

KHOST = "api.elections.kalshi.com"
_DB = os.path.join(os.path.dirname(__file__), "..", "data", "kalshi_value.db")
_MON = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
        "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"}


def _kget(path):
    req = urllib.request.Request("https://" + KHOST + path,
                                 headers={"User-Agent": "curl/8.4.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def _log_conn():
    c = sqlite3.connect(_DB)
    c.execute("""CREATE TABLE IF NOT EXISTS kalshi_value_log (
        id INTEGER PRIMARY KEY, scan_ts TEXT, sport TEXT, event_ticker TEXT, market_ticker TEXT,
        game_date TEXT, home_abbr TEXT, away_abbr TEXT, line REAL,
        our_prob_over REAL, our_mean_total REAL, kalshi_yes_ask REAL, kalshi_no_ask REAL,
        edge_yes REAL, edge_no REAL, volume REAL, open_interest REAL, preseason INTEGER DEFAULT 0,
        settled INTEGER DEFAULT 0, actual_total REAL, result TEXT, clv REAL)""")
    c.commit()
    return c


def _markets(series):
    out, cursor = [], None
    for _ in range(12):
        p = "/trade-api/v2/markets?series_ticker=%s&status=open&limit=200" % series
        if cursor:
            p += "&cursor=" + cursor
        j = _kget(p)
        out += (j.get("markets") or [])
        cursor = j.get("cursor")
        if not cursor:
            break
    return out


def _parse_event(ev):
    try:
        suf = ev.split("-", 1)[1]
        yy, mon, dd = suf[0:2], suf[2:5], suf[5:7]
        if mon not in _MON:
            return None, None
        return "20%s-%s-%s" % (yy, _MON[mon], dd), suf[7:]
    except Exception:
        return None, None


def _split_teams(teams, abbr_set):
    for i in (3, 2, 4):
        a, b = teams[:i], teams[i:]
        if a in abbr_set and b in abbr_set:
            return a, b
    for i in range(2, len(teams) - 1):
        a, b = teams[:i], teams[i:]
        if a in abbr_set and b in abbr_set:
            return a, b
    return None, None


def _team_abbrs(conn, teams_table):
    return set(r[0] for r in conn.execute("SELECT abbreviation FROM %s" % teams_table).fetchall())


def _lookup_game(conn, games, teams, date_iso, ab1, ab2):
    q = ("SELECT g.game_id, ht.abbreviation, at.abbreviation, g.status, g.home_score, g.away_score "
         "FROM {g} g JOIN {t} ht ON g.home_team_id = ht.id JOIN {t} at ON g.away_team_id = at.id "
         "WHERE g.date LIKE ? AND ((ht.abbreviation=? AND at.abbreviation=?) "
         "OR (ht.abbreviation=? AND at.abbreviation=?)) LIMIT 1").format(g=games, t=teams)
    return conn.execute(q, (date_iso + "%", ab1, ab2, ab2, ab1)).fetchone()


def _nhl_totals(home_abbr, away_abbr, n):
    from engine.mc_nhl_run import _load_team_profile
    from engine.mc_nhl import simulate_games
    raw = simulate_games(_load_team_profile(home_abbr, None), _load_team_profile(away_abbr, None),
                         n_sims=n, is_playoff=False)
    return np.asarray(raw["home_goals"]) + np.asarray(raw["away_goals"])


def _nba_totals(home_abbr, away_abbr, n):
    from engine.mc_nba_run import _load_full_profile
    from engine.mc_nba import simulate_full
    raw = simulate_full(_load_full_profile(home_abbr), _load_full_profile(away_abbr), n_sims=n)
    return np.asarray(raw["home_score"]) + np.asarray(raw["away_score"])


_ADAPTERS = {
    "nhl": dict(series="KXNHLTOTAL", mod="nhl_db", games="nhl_games", teams="nhl_teams",
                totals=_nhl_totals, preseason=0),
    "nba": dict(series="KXNBATOTAL", mod="nba_db", games="nba_games", teams="nba_teams",
                totals=_nba_totals, preseason=1),
}


def scan(sports=("nba", "nhl"), n_sims=20000):  # NBA primary/default (2026-10-10 pivot)
    scan_ts = datetime.now(timezone.utc).isoformat()
    log = _log_conn()
    summary = {}
    for sport in sports:
        A = _ADAPTERS[sport]
        s = {"markets_fetched": 0, "games_matched": 0, "rows_logged": 0, "sim_errors": 0}
        try:
            tdb = __import__("engine." + A["mod"], fromlist=[A["mod"]])
            tconn = tdb.get_conn()
            abbrs = _team_abbrs(tconn, A["teams"])
        except Exception as e:
            summary[sport] = {"error": "db/teams: " + str(e)[:140]}
            continue
        mkts = _markets(A["series"])
        s["markets_fetched"] = len(mkts)
        by_event = {}
        for m in mkts:
            ev = m.get("event_ticker")
            if ev:
                by_event.setdefault(ev, []).append(m)
        sim_cache = {}
        for ev, ms in by_event.items():
            date_iso, teams = _parse_event(ev)
            if not date_iso or not teams:
                continue
            a1, a2 = _split_teams(teams, abbrs)
            if not a1:
                continue
            g = _lookup_game(tconn, A["games"], A["teams"], date_iso, a1, a2)
            if not g:
                continue
            _st = (g[3] or "").lower()
            if _st in ("live", "in", "in_progress", "final", "post", "completed", "closed", "postponed", "suspended"):
                continue  # pre-game only: the sim assumes a 0-0 start, so live/final edges are invalid (Austin 2026-10-07)
            home_abbr, away_abbr = g[1], g[2]
            if ev not in sim_cache:
                try:
                    sim_cache[ev] = A["totals"](home_abbr, away_abbr, n_sims)
                except Exception:
                    s["sim_errors"] += 1
                    continue
            totals = sim_cache[ev]
            mean_total = float(totals.mean())
            for m in ms:
                line = m.get("floor_strike")
                if line is None:
                    continue
                line = float(line)
                ya = m.get("yes_ask_dollars"); na = m.get("no_ask_dollars")
                ya = float(ya) if ya not in (None, "") else None
                na = float(na) if na not in (None, "") else None
                p_over = float((totals > line).mean())
                e_yes = (p_over - ya) if ya is not None else None
                e_no = ((1.0 - p_over) - na) if na is not None else None
                log.execute(
                    "INSERT INTO kalshi_value_log (scan_ts,sport,event_ticker,market_ticker,game_date,"
                    "home_abbr,away_abbr,line,our_prob_over,our_mean_total,kalshi_yes_ask,kalshi_no_ask,"
                    "edge_yes,edge_no,volume,open_interest,preseason,settled) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                    (scan_ts, sport, ev, m.get("ticker"), date_iso, home_abbr, away_abbr, line,
                     round(p_over, 4), round(mean_total, 3), ya, na,
                     round(e_yes, 4) if e_yes is not None else None,
                     round(e_no, 4) if e_no is not None else None,
                     float(m.get("volume_fp") or 0), float(m.get("open_interest_fp") or 0), A["preseason"]))
                s["rows_logged"] += 1
        s["games_matched"] = len(sim_cache)
        log.commit()
        summary[sport] = s
    log.close()
    return summary


def settle():
    log = _log_conn()
    pend = log.execute("SELECT DISTINCT sport,event_ticker,game_date,home_abbr,away_abbr "
                       "FROM kalshi_value_log WHERE settled=0").fetchall()
    done = 0
    for sport, ev, gd, ha, aa in pend:
        A = _ADAPTERS.get(sport)
        if not A:
            continue
        try:
            tdb = __import__("engine." + A["mod"], fromlist=[A["mod"]])
            g = _lookup_game(tdb.get_conn(), A["games"], A["teams"], gd, ha, aa)
            if not g:
                continue
            status = (g[3] or "").lower(); hs, as_ = g[4], g[5]
            if "final" not in status or hs is None or as_ is None:
                continue
            actual = float(hs) + float(as_)
            for r in log.execute("SELECT id,line,edge_yes,edge_no,our_prob_over,kalshi_yes_ask "
                                 "FROM kalshi_value_log WHERE event_ticker=? AND settled=0", (ev,)).fetchall():
                rid, line, ey, en, po, ya = r
                eyv = ey if ey is not None else -9.0
                env = en if en is not None else -9.0
                lean = "over" if (eyv >= env and eyv > 0) else ("under" if env > 0 else None)
                over_won = True if actual > line else (False if actual < line else None)
                if lean == "over":
                    res = "W" if over_won else ("P" if over_won is None else "L")
                elif lean == "under":
                    res = "W" if over_won is False else ("P" if over_won is None else "L")
                else:
                    res = "no-lean"
                clv = (po - ya) if (po is not None and ya is not None) else None
                log.execute("UPDATE kalshi_value_log SET settled=1,actual_total=?,result=?,clv=? WHERE id=?",
                            (actual, res, round(clv, 4) if clv is not None else None, rid))
                done += 1
        except Exception:
            continue
    log.commit()
    log.close()
    return {"settled_rows": done}


if __name__ == "__main__":
    print("scan:", json.dumps(scan(), indent=2))
    print("settle:", json.dumps(settle(), indent=2))
