"""wh.db: schema, connection, and get-or-create helpers.

ONE SQLite file (``data/warehouse/wh.db``, WAL). Writers keep transactions short (one event / one
board / one page per commit) so the backfill, the live capture and the HR ingest can interleave.

Schema (all ids are INTEGER; strings only on small dimension tables):

  books(id, code, name, kind)                     hr, dk, espnbet, mgm, ..., kalshi (kind=exchange)
  teams(id, league, espn_id, abbr, name, short)    + team_alias(league, alias, team_id)
  events(id, sport, league, season, stype, start_ts, home_id, away_id, neutral, name, espn_id)
      + event_xref(src, ext_id, event_id)          src: espn | hr | kalshi | nhlapi | gk:<game_key> ...
  players(id, sport, name, norm, espn_id, team_id, pos) + player_xref(src, ext_id, player_id)
  markets(id, event_id, family, period, subject, stat)       UNIQUE(event_id, family, period, subject, stat)
      family: ML (2-way incl. OT) | ML3 (3-way regulation / 90') | SPREAD | TOTAL | TT (team total)
              | PROP (player over/under) | PROPY (player yes/no) | OE | BTTS | DNB | X:<code> (other)
      period: FG | REG | 1H | 2H | Q1..Q4 | P1..P3 | I1..I9 | F5 | ...
      subject: 0 game, -1 home team, -2 away team, >0 players.id
      stat: '' for game markets; prop stat code ('pts','sog','rec_yds',...)
  quotes(market_id, book, side, l100, ts, src, price, main)   WITHOUT ROWID, PK = all-but-price
      side: 1 home 2 away 3 draw 4 over 5 under 6 yes 7 no (>=100: labels table)
      l100: line x100 for SPREAD/TOTAL/TT/PROP (side-relative for spreads, e.g. home -1.5 -> -150); 0 for ML
      ts: unix seconds. price: DECIMAL odds (NULL = side removed/suspended). main: 1 main line, 0 alt
      src: 1 live (observed at ts) | 2 open (backfill; ts = start - OPEN_LEAD, approximate)
           | 3 close (backfill; ts = start - 60) | 4 import (existing PickMint tables; real ts)
      write-on-change: a live row is written only when (price, main) differs from the latest row of
      the same key; a NULL-price row marks removal.  ``board_obs`` records when a book's board for an
      event was observed (so a stale last-change can be told from a still-live price).
  board_obs(event_id, book, first_ts, last_ts, n)  intervals of continuous observation (gap > 45 min = new row)
  results(event_id, status, home, away, periods, ot, final_ts, src, updated_ts)
      final_ts = when the result is VISIBLE for replay (observed-final time live; start + conservative
      duration for backfill). The replay only shows results with final_ts <= decision time.
  period_scores(event_id, period, home, away)
  player_results(event_id, player_id, stat, value, src)          stat 'dnp' value 1 = listed, did not play
  news(id, event_id, ts, kind, subject, team_id, detail, src)     timestamped info (goalie confirmations, injuries)
  decisions(id, src, src_id, event_id, market_id, side, l100, book, ts, price, close_price, prob, edge,
            stake, is_shadow, result, profit, bet_type, pick_text, model_version)    our live picks
  coverage(...), bf_done(kind, key, status, ts, info), meta(k, v)
"""
from __future__ import annotations

import os
import re
import sqlite3
import time
import unicodedata

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WH_DIR = os.path.join(ROOT, "data", "warehouse")
DB_PATH = os.environ.get("WH_DB") or os.path.join(WH_DIR, "wh.db")
SCHEMA_VERSION = 1

SIDES = {"home": 1, "away": 2, "draw": 3, "over": 4, "under": 5, "yes": 6, "no": 7}
SIDE_NAMES = {v: k for k, v in SIDES.items()}
SRC = {"live": 1, "open": 2, "close": 3, "import": 4}
SRC_NAMES = {v: k for k, v in SRC.items()}

SPORT_OF = {"nhl": "hockey", "nba": "basketball", "wnba": "basketball", "ncaab": "basketball",
            "nfl": "football", "cfb": "football", "mlb": "baseball", "epl": "soccer"}

# Books we know up front (ESPN providers get created on demand by normalized name).
BOOKS = [
    ("hr", "Hard Rock", "book"), ("dk", "DraftKings", "book"), ("espnbet", "ESPN BET", "book"),
    ("mgm", "BetMGM", "book"), ("fanduel", "FanDuel", "book"), ("caesars", "Caesars", "book"),
    ("bet365", "Bet365", "book"), ("kalshi", "Kalshi", "exchange"), ("novig", "Novig", "exchange"),
    ("prophetx", "ProphetX", "exchange"), ("polymarket", "Polymarket US", "exchange"),
]

DDL = """
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS books(id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE, name TEXT, kind TEXT,
    espn_provider TEXT);
CREATE TABLE IF NOT EXISTS labels(id INTEGER PRIMARY KEY, text TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS teams(id INTEGER PRIMARY KEY, league TEXT NOT NULL, espn_id TEXT, abbr TEXT,
    name TEXT, short TEXT, UNIQUE(league, espn_id));
CREATE TABLE IF NOT EXISTS team_alias(league TEXT NOT NULL, alias TEXT NOT NULL, team_id INTEGER NOT NULL,
    PRIMARY KEY(league, alias)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, sport TEXT NOT NULL, league TEXT NOT NULL,
    season INTEGER, stype INTEGER, start_ts INTEGER, home_id INTEGER, away_id INTEGER,
    neutral INTEGER DEFAULT 0, name TEXT, espn_id TEXT, created_ts INTEGER);
CREATE UNIQUE INDEX IF NOT EXISTS ux_events_espn ON events(league, espn_id) WHERE espn_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_events_league_start ON events(league, start_ts);
CREATE TABLE IF NOT EXISTS event_xref(src TEXT NOT NULL, ext_id TEXT NOT NULL, event_id INTEGER NOT NULL,
    PRIMARY KEY(src, ext_id)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_event_xref_event ON event_xref(event_id);
CREATE TABLE IF NOT EXISTS players(id INTEGER PRIMARY KEY, sport TEXT NOT NULL, name TEXT, norm TEXT,
    espn_id TEXT, team_id INTEGER, pos TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS ux_players_espn ON players(sport, espn_id) WHERE espn_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_players_norm ON players(sport, norm);
CREATE TABLE IF NOT EXISTS player_xref(src TEXT NOT NULL, ext_id TEXT NOT NULL, player_id INTEGER NOT NULL,
    PRIMARY KEY(src, ext_id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS markets(id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL, family TEXT NOT NULL,
    period TEXT NOT NULL DEFAULT 'FG', subject INTEGER NOT NULL DEFAULT 0, stat TEXT NOT NULL DEFAULT '',
    UNIQUE(event_id, family, period, subject, stat));
CREATE TABLE IF NOT EXISTS quotes(market_id INTEGER NOT NULL, book INTEGER NOT NULL, side INTEGER NOT NULL,
    l100 INTEGER NOT NULL DEFAULT 0, ts INTEGER NOT NULL, src INTEGER NOT NULL, price REAL,
    main INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(market_id, book, side, l100, ts, src)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS board_obs(event_id INTEGER NOT NULL, book INTEGER NOT NULL, first_ts INTEGER NOT NULL,
    last_ts INTEGER NOT NULL, n INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(event_id, book, first_ts)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS results(event_id INTEGER PRIMARY KEY, status TEXT NOT NULL, home INTEGER,
    away INTEGER, periods INTEGER, ot INTEGER DEFAULT 0, final_ts INTEGER, src TEXT, updated_ts INTEGER);
CREATE TABLE IF NOT EXISTS period_scores(event_id INTEGER NOT NULL, period TEXT NOT NULL, home INTEGER,
    away INTEGER, PRIMARY KEY(event_id, period)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS player_results(event_id INTEGER NOT NULL, player_id INTEGER NOT NULL,
    stat TEXT NOT NULL, value REAL, src TEXT, PRIMARY KEY(event_id, player_id, stat)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS news(id INTEGER PRIMARY KEY, event_id INTEGER, ts INTEGER NOT NULL, kind TEXT,
    subject INTEGER, team_id INTEGER, detail TEXT, src TEXT);
CREATE INDEX IF NOT EXISTS ix_news_event ON news(event_id, ts);
CREATE UNIQUE INDEX IF NOT EXISTS ux_news ON news(src, event_id, ts, kind, subject, detail);
CREATE TABLE IF NOT EXISTS decisions(id INTEGER PRIMARY KEY, src TEXT NOT NULL, src_id TEXT NOT NULL,
    event_id INTEGER, market_id INTEGER, side INTEGER, l100 INTEGER, book INTEGER, ts INTEGER, price REAL,
    close_price REAL, prob REAL, edge REAL, stake REAL, is_shadow INTEGER, result TEXT, profit REAL,
    bet_type TEXT, pick_text TEXT, model_version TEXT, league TEXT, UNIQUE(src, src_id));
CREATE INDEX IF NOT EXISTS ix_decisions_event ON decisions(event_id);
CREATE TABLE IF NOT EXISTS coverage(league TEXT NOT NULL, season INTEGER NOT NULL, book TEXT NOT NULL,
    family TEXT NOT NULL, period TEXT NOT NULL, src TEXT NOT NULL, n_events INTEGER, n_markets INTEGER,
    n_quotes INTEGER, first_date TEXT, last_date TEXT, updated TEXT,
    PRIMARY KEY(league, season, book, family, period, src)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS bf_done(kind TEXT NOT NULL, key TEXT NOT NULL, status TEXT, ts INTEGER, info TEXT,
    PRIMARY KEY(kind, key)) WITHOUT ROWID;
"""


def connect(path: str | None = None, readonly: bool = False, timeout: float = 60.0) -> sqlite3.Connection:
    p = path or DB_PATH
    if readonly:
        c = sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=timeout)
    else:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        c = sqlite3.connect(p, timeout=timeout)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
    c.execute("PRAGMA busy_timeout=%d" % int(timeout * 1000))
    c.execute("PRAGMA temp_store=MEMORY")
    return c


def init(c: sqlite3.Connection) -> None:
    c.executescript(DDL)
    for code, name, kind in BOOKS:
        c.execute("INSERT OR IGNORE INTO books(code, name, kind) VALUES (?,?,?)", (code, name, kind))
    c.execute("INSERT OR REPLACE INTO meta(k, v) VALUES ('schema_version', ?)", (str(SCHEMA_VERSION),))
    c.commit()


def open_db(path: str | None = None) -> sqlite3.Connection:
    c = connect(path)
    init(c)
    return c


# ---------------------------------------------------------------- normalization helpers

def norm(s: str | None) -> str:
    """Lowercase ASCII alnum-only (accents stripped). 'St. Louis Blues' -> 'stlouisblues'."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", s.lower())


def norm_person(s: str | None) -> str:
    """Player-name key: drop suffixes (jr/sr/ii/iii/iv) and punctuation."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii").lower()
    s = re.sub(r"[.'`]", "", s)
    parts = [p for p in re.split(r"[^a-z0-9]+", s) if p and p not in ("jr", "sr", "ii", "iii", "iv", "v")]
    return "".join(parts)


def am_to_dec(am) -> float | None:
    """American odds (int / '+195' / 'EVEN') -> decimal odds."""
    try:
        t = str(am).replace("+", "").strip().upper()
        if t in ("EVEN", "EV"):
            return 2.0
        a = float(t)
    except (TypeError, ValueError):
        return None
    if a >= 100:
        return round(1.0 + a / 100.0, 6)
    if a <= -100:
        return round(1.0 + 100.0 / -a, 6)
    return None


def dec_to_am(d) -> int | None:
    if d is None or d <= 1.0:
        return None
    return int(round((d - 1.0) * 100)) if d >= 2.0 else int(round(-100.0 / (d - 1.0)))


def l100(line) -> int:
    if line is None:
        return 0
    return int(round(float(line) * 100))


# ---------------------------------------------------------------- get-or-create (cached per connection)

class Ids:
    """Small cached id resolver bound to one connection (use one per process / thread)."""

    def __init__(self, c: sqlite3.Connection):
        self.c = c
        self._book = {}
        self._mkt = {}
        self._team = {}
        self._alias = {}
        self._label = {}

    # books -----------------------------------------------------------------
    def book(self, code: str, name: str | None = None, kind: str = "book", espn_provider=None) -> int:
        b = self._book.get(code)
        if b:
            return b
        r = self.c.execute("SELECT id FROM books WHERE code=?", (code,)).fetchone()
        if not r:
            self.c.execute("INSERT INTO books(code, name, kind, espn_provider) VALUES (?,?,?,?)",
                           (code, name or code, kind, espn_provider))
            r = self.c.execute("SELECT id FROM books WHERE code=?", (code,)).fetchone()
        self._book[code] = r[0]
        return r[0]

    def label(self, text: str) -> int:
        k = self._label.get(text)
        if k:
            return k
        r = self.c.execute("SELECT id FROM labels WHERE text=?", (text,)).fetchone()
        if not r:
            self.c.execute("INSERT INTO labels(id, text) VALUES ((SELECT COALESCE(MAX(id), 99) + 1 FROM labels), ?)",
                           (text,))
            r = self.c.execute("SELECT id FROM labels WHERE text=?", (text,)).fetchone()
        self._label[text] = r[0]
        return r[0]

    # teams -----------------------------------------------------------------
    def team(self, league: str, espn_id, abbr=None, name=None, short=None, aliases=()) -> int:
        key = (league, str(espn_id))
        t = self._team.get(key)
        if t:
            return t
        r = self.c.execute("SELECT id FROM teams WHERE league=? AND espn_id=?", key).fetchone()
        if not r:
            self.c.execute("INSERT INTO teams(league, espn_id, abbr, name, short) VALUES (?,?,?,?,?)",
                           (league, str(espn_id), abbr, name, short))
            r = self.c.execute("SELECT id FROM teams WHERE league=? AND espn_id=?", key).fetchone()
        tid = r[0]
        self._team[key] = tid
        for a in (abbr, name, short, *aliases):
            self.alias(league, a, tid)
        return tid

    def alias(self, league: str, text, team_id: int, overwrite: bool = False) -> None:
        a = norm(text)
        if not a:
            return
        if overwrite:
            self.c.execute("INSERT OR REPLACE INTO team_alias(league, alias, team_id) VALUES (?,?,?)",
                           (league, a, team_id))
        else:
            self.c.execute("INSERT OR IGNORE INTO team_alias(league, alias, team_id) VALUES (?,?,?)",
                           (league, a, team_id))
        self._alias[(league, a)] = team_id if overwrite else self._alias.get((league, a), team_id)

    def team_by_alias(self, league: str, text) -> int | None:
        a = norm(text)
        if not a:
            return None
        k = (league, a)
        if k in self._alias:
            return self._alias[k]
        r = self.c.execute("SELECT team_id FROM team_alias WHERE league=? AND alias=?", k).fetchone()
        self._alias[k] = r[0] if r else None
        return self._alias[k]

    # markets ---------------------------------------------------------------
    def market(self, event_id: int, family: str, period: str = "FG", subject: int = 0, stat: str = "") -> int:
        key = (event_id, family, period, subject, stat)
        m = self._mkt.get(key)
        if m:
            return m
        r = self.c.execute("SELECT id FROM markets WHERE event_id=? AND family=? AND period=? AND subject=? AND stat=?",
                           key).fetchone()
        if not r:
            cur = self.c.execute("INSERT INTO markets(event_id, family, period, subject, stat) VALUES (?,?,?,?,?)", key)
            mid = cur.lastrowid
        else:
            mid = r[0]
        if len(self._mkt) > 200000:
            self._mkt.clear()
        self._mkt[key] = mid
        return mid


def event_by_xref(c: sqlite3.Connection, src: str, ext_id) -> int | None:
    r = c.execute("SELECT event_id FROM event_xref WHERE src=? AND ext_id=?", (src, str(ext_id))).fetchone()
    return r[0] if r else None


def add_xref(c: sqlite3.Connection, src: str, ext_id, event_id: int) -> None:
    c.execute("INSERT OR IGNORE INTO event_xref(src, ext_id, event_id) VALUES (?,?,?)", (src, str(ext_id), event_id))


def upsert_espn_event(c, league: str, espn_id, season, stype, start_ts, home_id, away_id, neutral, name) -> int:
    r = c.execute("SELECT id, start_ts FROM events WHERE league=? AND espn_id=?", (league, str(espn_id))).fetchone()
    if r:
        c.execute("UPDATE events SET season=?, stype=?, start_ts=?, home_id=?, away_id=?, neutral=?, name=? WHERE id=?",
                  (season, stype, start_ts, home_id, away_id, neutral, name, r[0]))
        return r[0]
    cur = c.execute("INSERT INTO events(sport, league, season, stype, start_ts, home_id, away_id, neutral, name, espn_id,"
                    " created_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (SPORT_OF.get(league, league), league, season, stype, start_ts, home_id, away_id, neutral, name,
                     str(espn_id), int(time.time())))
    eid = cur.lastrowid
    add_xref(c, "espn", f"{league}:{espn_id}", eid)
    return eid


def player_espn(c, sport: str, espn_id, name=None, team_id=None, pos=None, _cache={}) -> int:
    key = (sport, str(espn_id))
    pid = _cache.get(key)
    if pid:
        return pid
    r = c.execute("SELECT id, name FROM players WHERE sport=? AND espn_id=?", key).fetchone()
    if r:
        pid = r[0]
        if name and not r[1]:
            c.execute("UPDATE players SET name=?, norm=? WHERE id=?", (name, norm_person(name), pid))
    else:
        cur = c.execute("INSERT INTO players(sport, name, norm, espn_id, team_id, pos) VALUES (?,?,?,?,?,?)",
                        (sport, name, norm_person(name), str(espn_id), team_id, pos))
        pid = cur.lastrowid
    if len(_cache) > 100000:
        _cache.clear()
    _cache[key] = pid
    return pid


# ---------------------------------------------------------------- quote writer (write-on-change)

class QuoteWriter:
    """Buffers quotes; ``put`` applies write-on-change for src=live against the latest stored row.

    For src open/close/import the row is keyed by its own ts, so re-imports are idempotent
    (INSERT OR REPLACE on the primary key)."""

    def __init__(self, c: sqlite3.Connection):
        self.c = c
        self.rows = []
        self.n_written = 0
        self.n_skipped = 0
        self._last = {}

    def last(self, key):
        if key in self._last:
            return self._last[key]
        r = self.c.execute("SELECT price, main FROM quotes WHERE market_id=? AND book=? AND side=? AND l100=? "
                           "AND src=1 ORDER BY ts DESC LIMIT 1", key).fetchone()
        v = (r[0], r[1]) if r else None
        self._last[key] = v
        return v

    def put(self, market_id, book, side, l100_, ts, src, price, main=1):
        if price is not None:
            price = round(float(price), 6)
        key = (market_id, book, side, l100_)
        if src == 1:
            prev = self.last(key)
            if prev is not None and prev[0] == price and prev[1] == main:
                self.n_skipped += 1
                return False
            if prev is None and price is None:
                self.n_skipped += 1
                return False
            self._last[key] = (price, main)
        self.rows.append((market_id, book, side, l100_, int(ts), src, price, main))
        if len(self.rows) >= 5000:
            self.flush()
        return True

    def flush(self):
        if self.rows:
            self.c.executemany("INSERT OR REPLACE INTO quotes(market_id, book, side, l100, ts, src, price, main) "
                               "VALUES (?,?,?,?,?,?,?,?)", self.rows)
            self.n_written += len(self.rows)
            self.rows = []

    def forget(self):
        self._last.clear()


def obs(c, event_id: int, book: int, ts: int, gap: int = 2700) -> None:
    """Record that `book`'s board for `event_id` was observed at ts (interval merge, 45-min gap)."""
    r = c.execute("SELECT first_ts, last_ts FROM board_obs WHERE event_id=? AND book=? ORDER BY first_ts DESC LIMIT 1",
                  (event_id, book)).fetchone()
    if r and ts - r[1] <= gap and ts >= r[0]:
        if ts > r[1]:
            c.execute("UPDATE board_obs SET last_ts=?, n=n+1 WHERE event_id=? AND book=? AND first_ts=?",
                      (ts, event_id, book, r[0]))
        return
    c.execute("INSERT OR IGNORE INTO board_obs(event_id, book, first_ts, last_ts, n) VALUES (?,?,?,?,1)",
              (event_id, book, ts, ts))


def done(c, kind: str, key: str):
    r = c.execute("SELECT status FROM bf_done WHERE kind=? AND key=?", (kind, key)).fetchone()
    return r[0] if r else None


def mark(c, kind: str, key: str, status: str = "ok", info: str | None = None) -> None:
    c.execute("INSERT OR REPLACE INTO bf_done(kind, key, status, ts, info) VALUES (?,?,?,?,?)",
              (kind, key, status, int(time.time()), info))
