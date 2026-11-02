"""Derivative / Q1 / period line capture (MintyBot 2026-09-16) -- sibling of
scripts/capture_all_odds.py. Snapshots EVERY game's DERIVATIVE market lines
(not just picked ones) into data/game_odds.db :: game_derivative_odds, so the
derivative-market biases + CLV validate at scale alongside the main-market
capture. Standalone; long/EAV format (one row per selection); upsert => the
last snapshot per (game, market, selection, line) approximates the close.

Reuses the SAME fetch functions the model already uses, and pulls the
derivative fields those odds dicts already carry:
  MLB (engine.picks.fetch_real_odds_for_games): f5_total/f5_over_odds/
      f5_under_odds, f5_ml, f5_team_total_{home,away}, team_total_{home,away},
      alt_totals, alt_spreads, inning_totals, total_oe, extra_innings.
  basketball (engine.basketball._odds.fetch_league_odds): q1_total/
      q1_over_odds/q1_under_odds, q1_spread(+home/away odds), q1_{home,away}_ml,
      q1_total_oe, q1_alt_totals, q1_alt_spreads.
  hockey  (engine.hockey._odds.fetch_league_odds): period_totals, team_total,
      total_oe, period_bts, period_dnb, overtime, bts_full (best-effort walk).

Run hourly. NOT covered by the existing capture_all_odds scheduled task --
needs its own entry (see the calibration build notes / report).
"""
import sqlite3, datetime, sys, json
from datetime import timezone

sys.path.insert(0, r"C:\sports-model-bettor")
DB = r"C:\sports-model-bettor\data\game_odds.db"

con = sqlite3.connect(DB)
con.execute("""CREATE TABLE IF NOT EXISTS game_derivative_odds(
  sport TEXT, league TEXT, game_key TEXT, capture_date TEXT,
  market TEXT, selection TEXT, line REAL NOT NULL DEFAULT 0,
  odds INT, start_time TEXT, first_seen TEXT, last_seen TEXT, n_snaps INT DEFAULT 1,
  PRIMARY KEY(sport,league,game_key,capture_date,market,selection,line))""")
now = datetime.datetime.now(timezone.utc).isoformat()
today = datetime.date.today().isoformat()


def _num(x):
    return x if isinstance(x, (int, float)) else None


def upsert(sport, lg, key, start_time, market, selection, line, odds):
    o = _num(odds)
    if o is None:
        return 0
    ln = _num(line)
    ln = 0.0 if ln is None else float(ln)
    con.execute("""INSERT INTO game_derivative_odds(
        sport,league,game_key,capture_date,market,selection,line,odds,
        start_time,first_seen,last_seen,n_snaps)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,1)
      ON CONFLICT(sport,league,game_key,capture_date,market,selection,line)
      DO UPDATE SET odds=excluded.odds, start_time=excluded.start_time,
        last_seen=excluded.last_seen, n_snaps=n_snaps+1""",
      (sport, lg, key, today, market, selection, ln, int(o),
       start_time, now, now))
    return 1


# ── extraction helpers ────────────────────────────────────────
def _ou(rows, market, node):
    """node = {line, over_odds, under_odds}."""
    if isinstance(node, dict):
        ln = node.get("line")
        rows.append((market, "over", ln, node.get("over_odds")))
        rows.append((market, "under", ln, node.get("under_odds")))


def _pairs(rows, market, node, line=None):
    """Emit every '<sel>_odds' key of a flat dict (odd/even, yes/no,
    home/away, ...) as selection '<sel>'."""
    if isinstance(node, dict):
        for k, v in node.items():
            if k.endswith("_odds") and isinstance(v, (int, float)):
                rows.append((market, k[:-5], line, v))


def _walk(rows, market, node):
    """Best-effort recursive parse for unknown-shape derivative fields
    (NHL): line-bearing over/under dicts, flat *_odds dicts, dict-of-dicts
    (label -> node), and lists of such nodes."""
    if isinstance(node, dict):
        if "over_odds" in node or "under_odds" in node:
            _ou(rows, market, node)
        elif any(k.endswith("_odds") for k in node):
            _pairs(rows, market, node)
        else:
            for label, sub in node.items():
                _walk(rows, "%s_%s" % (market, str(label).upper()), sub)
    elif isinstance(node, list):
        for item in node:
            if isinstance(item, dict) and "line" in item:
                _ou(rows, market, item)
            elif isinstance(item, dict) and "point" in item:
                pt = item.get("point")
                rows.append((market, "home", pt, item.get("home_odds")))
                rows.append((market, "away", pt, item.get("away_odds")))
            else:
                _walk(rows, market, item)


# ── per-sport extractors: return list of (market, selection, line, odds) ──
def extract_mlb(o):
    r = []
    if o.get("f5_total") is not None:
        r.append(("F5_TOTAL", "over", o.get("f5_total"), o.get("f5_over_odds")))
        r.append(("F5_TOTAL", "under", o.get("f5_total"), o.get("f5_under_odds")))
    _pairs(r, "F5_ML", o.get("f5_ml"))          # {home_odds,away_odds} when present
    _ou(r, "F5_TEAM_TOTAL_HOME", o.get("f5_team_total_home"))
    _ou(r, "F5_TEAM_TOTAL_AWAY", o.get("f5_team_total_away"))
    _ou(r, "TEAM_TOTAL_HOME", o.get("team_total_home"))
    _ou(r, "TEAM_TOTAL_AWAY", o.get("team_total_away"))
    for a in (o.get("alt_totals") or []):
        _ou(r, "ALT_TOTAL", a)
    for a in (o.get("alt_spreads") or []):
        pt = a.get("point") if isinstance(a, dict) else None
        r.append(("ALT_SPREAD", "home", pt, (a or {}).get("home_odds")))
        r.append(("ALT_SPREAD", "away", pt, (a or {}).get("away_odds")))
    it = o.get("inning_totals")
    if isinstance(it, dict):
        for inn, node in it.items():
            if isinstance(node, dict):
                ln = node.get("line")
                r.append(("INNING_TOTAL", "%s:over" % inn, ln, node.get("over_odds")))
                r.append(("INNING_TOTAL", "%s:under" % inn, ln, node.get("under_odds")))
    _pairs(r, "TOTAL_OE", o.get("total_oe"))
    _pairs(r, "EXTRA_INNINGS", o.get("extra_innings"))
    return r


def extract_basketball(o):
    r = []
    if o.get("q1_total") is not None:
        r.append(("Q1_TOTAL", "over", o.get("q1_total"), o.get("q1_over_odds")))
        r.append(("Q1_TOTAL", "under", o.get("q1_total"), o.get("q1_under_odds")))
    if o.get("q1_spread") is not None:
        r.append(("Q1_SPREAD", "home", o.get("q1_spread"), o.get("q1_spread_home_odds")))
        r.append(("Q1_SPREAD", "away", o.get("q1_spread"), o.get("q1_spread_away_odds")))
    r.append(("Q1_ML", "home", None, o.get("q1_home_ml")))
    r.append(("Q1_ML", "away", None, o.get("q1_away_ml")))
    _pairs(r, "Q1_TOTAL_OE", o.get("q1_total_oe"))
    for a in (o.get("q1_alt_totals") or []):
        _ou(r, "Q1_ALT_TOTAL", a)
    for a in (o.get("q1_alt_spreads") or []):
        pt = a.get("point") if isinstance(a, dict) else None
        r.append(("Q1_ALT_SPREAD", "home", pt, (a or {}).get("home_odds")))
        r.append(("Q1_ALT_SPREAD", "away", pt, (a or {}).get("away_odds")))
    return r


def extract_hockey(o):
    r = []
    for fld, market in [("period_totals", "PERIOD_TOTAL"), ("team_total", "TEAM_TOTAL"),
                        ("total_oe", "TOTAL_OE"), ("period_bts", "PERIOD_BTS"),
                        ("period_dnb", "PERIOD_DNB"), ("overtime", "OVERTIME"),
                        ("bts_full", "BTS_FULL")]:
        if o.get(fld) is not None:
            _walk(r, market, o.get(fld))
    return r


def run_sport(sport, lg, key, start_time, extractor, o):
    if not isinstance(o, dict):
        return 0
    n = 0
    for market, selection, line, odds in extractor(o):
        n += upsert(sport, lg, key, start_time, market, selection, line, odds)
    return n


total = 0
bylg = {}

# basketball + hockey via engine.<sport>._odds.fetch_league_odds
for sport, leagues, extractor in [
        ("basketball", ["nba", "wnba", "ncaam"], extract_basketball),
        ("hockey", ["nhl"], extract_hockey)]:
    try:
        fetch = __import__("engine.%s._odds" % sport,
                           fromlist=["fetch_league_odds"]).fetch_league_odds
    except Exception as e:
        print("%s: no fetch (%s)" % (sport, str(e)[:50])); continue
    for lg in leagues:
        try:
            odds = fetch(lg) or {}
        except Exception as e:
            print("%s/%s: %s" % (sport, lg, str(e)[:50])); continue
        c = 0
        for k, o in odds.items():
            c += run_sport(sport, lg, k, (o or {}).get("start_time"), extractor, o)
        if c:
            bylg["%s:%s" % (sport, lg)] = c; total += c

# MLB (separate no-arg fetch)
try:
    from engine.picks import fetch_real_odds_for_games as mlbf
    mo = mlbf() or {}
    c = 0
    for k, o in mo.items():
        c += run_sport("baseball", "mlb", k, (o or {}).get("start_time"),
                       extract_mlb, o)
    if c:
        bylg["baseball:mlb"] = c; total += c
except Exception as e:
    print("mlb err", str(e)[:80])

con.commit()
print("captured %d derivative-line rows | by league: %s" % (total, bylg))
print("table total:",
      con.execute("SELECT COUNT(*) FROM game_derivative_odds").fetchone()[0])
con.close()
