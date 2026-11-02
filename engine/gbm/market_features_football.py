"""Market-derived features for the football GBM. Parallel of ``market_features_nhl``
-- but football has NO separate historical-odds DB, so this reads the live odds-
capture DB (data/game_odds.db) directly. game_odds stores 2-8 snapshots per game
(sport='football'); within a game's window, open = first capture, close = last.
game_key is 'AWAY@HOME' in the model's own team abbreviations (verified: all odds
abbrs match {cfb,nfl}.db teams.abbreviation). Features mirror NHL: de-vig implied
+ lines + line movement (the sharp-money signal Elo is blind to).
"""
from __future__ import annotations
import logging, sqlite3, threading
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)
_ROOT = Path(__file__).resolve().parent.parent.parent
_ODDS_DB = _ROOT / "data" / "game_odds.db"

FEATURE_KEYS = ("has_market_data", "market_home_implied", "market_total_line",
                "market_spread_line", "market_spread_move", "market_total_move")

def _empty():
    return {k: 0.0 for k in FEATURE_KEYS}

def _implied(odds):
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0 or abs(o) < 100:
        return None
    return abs(o) / (abs(o) + 100.0) if o < 0 else 100.0 / (o + 100.0)

def _devig(home_ml, away_ml):
    h = _implied(home_ml); a = _implied(away_ml)
    if h is None or a is None:
        return None
    s = h + a
    return (h / s) if s > 0 else None

@lru_cache(maxsize=4)
def _abbr_map(league):
    db = _ROOT / "data" / "football" / ("%s.db" % league)
    conn = sqlite3.connect(str(db))
    try:
        return {int(r[0]): str(r[1]) for r in
                conn.execute("SELECT id, abbreviation FROM teams").fetchall()}
    finally:
        conn.close()

_tls = threading.local()


def _odds_conn():
    # Per-thread conn (was @lru_cache(maxsize=1) = ONE conn shared across all
    # threads). The slate recompute runs in a bg refresh thread, so reusing a
    # conn created in another thread raised "SQLite objects created in a thread
    # can only be used in that same thread" -> the GBM leg threw -> the whole
    # slate cache silently stopped refreshing (frozen cards). MintyBot 2026-10-17.
    conn = getattr(_tls, "c", None)
    if conn is None:
        conn = sqlite3.connect(str(_ODDS_DB), timeout=10)
        conn.row_factory = sqlite3.Row
        _tls.c = conn
    return conn

def extract_market_features(league, home_team_id, away_team_id, game_date):
    if not _ODDS_DB.exists():
        return _empty()
    try:
        amap = _abbr_map(league)
    except Exception as e:
        logger.debug("football abbr map unavailable (%s): %s", league, e)
        return _empty()
    h_abbr = amap.get(int(home_team_id)); a_abbr = amap.get(int(away_team_id))
    if not h_abbr or not a_abbr:
        return _empty()
    gk = "%s@%s" % (a_abbr, h_abbr)
    rows = _odds_conn().execute(
        "SELECT capture_date, home_ml, away_ml, over_under, home_spread_point "
        "FROM game_odds WHERE sport='football' AND league=? AND game_key=? "
        "  AND capture_date <= ? AND capture_date >= date(?, '-16 days') "
        "ORDER BY capture_date ASC",
        (league, gk, game_date, game_date)).fetchall()
    if not rows:
        return _empty()
    first, last = rows[0], rows[-1]
    out = _empty(); out["has_market_data"] = 1.0
    di = _devig(last["home_ml"], last["away_ml"])
    if di is not None:
        out["market_home_implied"] = float(di)
    if last["over_under"] is not None:
        out["market_total_line"] = float(last["over_under"])
    if last["home_spread_point"] is not None:
        out["market_spread_line"] = float(last["home_spread_point"])
    if last["home_spread_point"] is not None and first["home_spread_point"] is not None:
        out["market_spread_move"] = float(last["home_spread_point"]) - float(first["home_spread_point"])
    if last["over_under"] is not None and first["over_under"] is not None:
        out["market_total_move"] = float(last["over_under"]) - float(first["over_under"])
    return out
