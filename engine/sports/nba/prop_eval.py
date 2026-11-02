"""NBA player-prop line log + weekly market-blend refit (MintyBot 2026-10-26).

The NHL / NFL evidence loop (engine.sports.nhl.prop_eval, engine.prop_refit), plus a
DraftKings reference:
  * engine.sports.nba.prop_picks logs EVERY Hard Rock main line it prices -- the model's
    raw P(over) and projection with HR's price -- via log_lines(), together with
    DraftKings' main line for the same player-stat (ESPN core API) when ESPN has one.
  * stamp_prices() (worker, every minute alongside the props closing-odds capture) writes
    HR's (and DK's) current price onto each logged line until tip, so "last" is the close.
  * grade() fills the outcome from the NBA box scores (tracker's _lookup_actual, so PRA is
    pts + reb + ast); a player with no box once the game is final + 12h is a DNP.
  * maybe_refit() refits the model's blend weight per stat weekly (engine.prop_refit) into
    data/nba_prop_weights.json. Each stat STARTS from its DraftKings backtest weight
    (DEFAULT_W below), not the shared 0.35, fit against DK's OPENING price -- the pick-time
    analog, the NHL standard (fitting against the close makes an informative model look
    worthless; MintyBot 2026-10-27, Austin "So how come NHL has picks?"). With HR lines
    graded, the refit moves it -- up if HR is softer than DK, toward 0 if not.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ... import prop_refit as _core

logger = logging.getLogger(__name__)

MARKETS = ("Player Points", "Player Rebounds", "Player Assists", "Player 3PM", "Player PRA",
           "Player Steals", "Player Blocks", "Player Turnovers", "Player FT Made")
# Model share in the market blend before Hard Rock evidence: the DraftKings backtest's best
# log-loss blend per stat against DK's OPEN (n=10,505 lines, 2026-10-27), capped at the refit's
# .60. When the model was 5pp+ off the open the close moved its way: ast 68%, reb 61%, stl 58%,
# pts 49% (none). Was fit against DK's CLOSE (pts .05 reb .25 ast .20 3PM .05 PRA .05 stl .30
# blk .20) -- the mistake the NHL refit corrected the same day; it left NBA props pick-less.
# Turnovers / FT made had no DK history: kept at .10.
DEFAULT_W = {"Player Points": 0.10, "Player Rebounds": 0.40, "Player Assists": 0.60,
             "Player 3PM": 0.15, "Player PRA": 0.20, "Player Steals": 0.60,
             "Player Blocks": 0.25, "Player Turnovers": 0.10, "Player FT Made": 0.10}
_PATH = Path(__file__).resolve().parents[3] / "data" / "nba_prop_weights.json"
_TS = "%Y-%m-%dT%H:%M:%SZ"
_COLS = (("pick_seen", "TEXT"), ("pick_line", "REAL"), ("pick_over", "INTEGER"),
         ("pick_under", "INTEGER"), ("pick_p_over", "REAL"),
         ("first_proj", "REAL"), ("last_proj", "REAL"),
         ("dk_line", "REAL"), ("dk_over", "INTEGER"), ("dk_under", "INTEGER"),
         ("dk_pick_line", "REAL"), ("dk_pick_over", "INTEGER"), ("dk_pick_under", "INTEGER"),
         ("dk_last_line", "REAL"), ("dk_last_over", "INTEGER"), ("dk_last_under", "INTEGER"))


def _conn():
    from ...player_props_db import _conn_for
    return _conn_for("nba")         # thread-local nba.db connection (never close it)


def _now() -> str:
    return datetime.now(timezone.utc).strftime(_TS)


def _ensure(c) -> None:
    c.execute("""CREATE TABLE IF NOT EXISTS prop_eval_log(
        game_id TEXT NOT NULL, game_date TEXT, player_id INTEGER NOT NULL, player_name TEXT,
        bet_type TEXT NOT NULL,
        first_seen TEXT, first_line REAL, first_over INTEGER, first_under INTEGER,
        first_p_over REAL,
        last_seen TEXT, last_line REAL, last_over INTEGER, last_under INTEGER,
        last_p_over REAL, n_seen INTEGER DEFAULT 0, price_stamped_at TEXT,
        actual REAL, graded_at TEXT,
        PRIMARY KEY (game_id, player_id, bet_type))""")
    have = {r[1] for r in c.execute("PRAGMA table_info(prop_eval_log)")}
    for col, typ in _COLS:
        if col not in have:
            c.execute("ALTER TABLE prop_eval_log ADD COLUMN %s %s" % (col, typ))


def log_lines(rows: list, pick_day: str | None = None) -> None:
    """Upsert priced main lines: dicts with game_id, game_date, player_id, player_name,
    bet_type, line, over_odds, under_odds, p_over (raw model), proj, dk (line, over, under)
    or None. Lines dated ``pick_day`` (game day once the card can lock) also fill the
    pick_* snapshot -- the price the picker decided against, what the refit fits on."""
    rows = [r for r in rows if r.get("over_odds") is not None and r.get("under_odds") is not None]
    if not rows:
        return
    c = _conn()
    try:
        _ensure(c)
        now = _now()
        for r in rows:
            dk = r.get("dk") or (None, None, None)
            v = (str(r["game_id"]), r["game_date"], int(r["player_id"]), r["player_name"],
                 r["bet_type"], now, float(r["line"]), int(r["over_odds"]), int(r["under_odds"]),
                 round(float(r["p_over"]), 4), r.get("proj"), dk[0], dk[1], dk[2])
            c.execute(
                "INSERT INTO prop_eval_log(game_id, game_date, player_id, player_name, bet_type, "
                "first_seen, first_line, first_over, first_under, first_p_over, first_proj, "
                "dk_line, dk_over, dk_under, "
                "last_seen, last_line, last_over, last_under, last_p_over, last_proj, "
                "dk_last_line, dk_last_over, dk_last_under, n_seen) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1) "
                "ON CONFLICT(game_id, player_id, bet_type) DO UPDATE SET "
                "last_seen = excluded.last_seen, last_line = excluded.last_line, "
                "last_over = excluded.last_over, last_under = excluded.last_under, "
                "last_p_over = excluded.last_p_over, last_proj = excluded.last_proj, "
                "dk_last_line = COALESCE(excluded.dk_last_line, prop_eval_log.dk_last_line), "
                "dk_last_over = COALESCE(excluded.dk_last_over, prop_eval_log.dk_last_over), "
                "dk_last_under = COALESCE(excluded.dk_last_under, prop_eval_log.dk_last_under), "
                "dk_line = COALESCE(prop_eval_log.dk_line, excluded.dk_line), "
                "dk_over = COALESCE(prop_eval_log.dk_over, excluded.dk_over), "
                "dk_under = COALESCE(prop_eval_log.dk_under, excluded.dk_under), "
                "n_seen = prop_eval_log.n_seen + 1 "
                "WHERE prop_eval_log.actual IS NULL AND prop_eval_log.graded_at IS NULL",
                v + v[5:])
            if pick_day and r["game_date"] == pick_day:
                c.execute("UPDATE prop_eval_log SET pick_seen = ?, pick_line = ?, pick_over = ?, "
                          "pick_under = ?, pick_p_over = ?, dk_pick_line = ?, dk_pick_over = ?, "
                          "dk_pick_under = ? WHERE game_id = ? AND player_id = ? AND bet_type = ? "
                          "AND pick_seen IS NULL AND graded_at IS NULL",
                          v[5:10] + v[11:14] + (v[0], v[2], v[4]))
        c.commit()
    except Exception as e:
        c.rollback()
        logger.warning("nba prop_eval: log write failed: %s", e)


def stamp_prices() -> int:
    """Write Hard Rock's (and DraftKings') current two-way price onto every logged line
    whose game hasn't tipped (same player, stat and line). Returns lines stamped; fetches
    the boards only when there is something to stamp (scraper cache 60s, DK cache 10min)."""
    c = _conn()
    try:
        _ensure(c)
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        rows = c.execute(
            "SELECT l.game_id, l.player_id, l.player_name, l.bet_type, l.last_line "
            "FROM prop_eval_log l JOIN nba_games g ON CAST(g.game_id AS TEXT) = l.game_id "
            "WHERE l.graded_at IS NULL AND COALESCE(g.status, 'scheduled') = 'scheduled' "
            "AND REPLACE(REPLACE(g.start_time, 'T', ' '), 'Z', '') > REPLACE(?, 'T', ' ')",
            (now_iso,)).fetchall()
    except sqlite3.OperationalError:
        return 0
    if not rows:
        return 0
    try:
        from scrapers.hardrock_props import fetch_nba_props
        board = fetch_nba_props() or {}
    except Exception as e:
        logger.warning("nba prop_eval: board fetch failed: %s", e)
        return 0
    idx: dict = {}
    for brows in board.values():
        for b in brows:
            if b.get("line") is None or b.get("over_odds") is None or b.get("under_odds") is None:
                continue
            key = (" ".join((b.get("player_name") or "").lower().split()), b.get("bet_type"),
                   float(b["line"]))
            idx[key] = (int(b["over_odds"]), int(b["under_odds"]))
    from .prop_model import dk_lines, MARKETS as _MK
    stamp, n = _now(), 0
    for r in rows:
        hit = idx.get((" ".join((r["player_name"] or "").lower().split()), r["bet_type"],
                       float(r["last_line"])))
        dk = dk_lines(r["game_id"]).get((int(r["player_id"]), _MK.get(r["bet_type"])))
        if not hit and not dk:
            continue
        sets, vals = [], []
        if hit:
            sets += ["last_over = ?", "last_under = ?", "price_stamped_at = ?"]
            vals += [hit[0], hit[1], stamp]
        if dk:
            sets += ["dk_last_line = ?", "dk_last_over = ?", "dk_last_under = ?"]
            vals += list(dk)
        c.execute("UPDATE prop_eval_log SET " + ", ".join(sets) + " WHERE game_id = ? AND "
                  "player_id = ? AND bet_type = ? AND graded_at IS NULL",
                  vals + [r["game_id"], r["player_id"], r["bet_type"]])
        n += 1
    if n:
        c.commit()
    return n


def grade(limit: int = 3000) -> int:
    """Fill ``actual`` from the NBA box scores for lines whose game is final."""
    from ...player_props_tracker import _NBA_STAT_KEY, _lookup_actual, _game_is_final
    c = _conn()
    try:
        _ensure(c)
        rows = c.execute(
            "SELECT l.game_id, l.player_id, l.bet_type, g.start_time FROM prop_eval_log l "
            "LEFT JOIN nba_games g ON CAST(g.game_id AS TEXT) = l.game_id "
            "WHERE l.graded_at IS NULL LIMIT ?", (int(limit),)).fetchall()
    except sqlite3.OperationalError:
        return 0
    done, stamp, now = 0, _now(), datetime.now(timezone.utc)
    for r in rows:
        stat = _NBA_STAT_KEY.get(r["bet_type"])
        if not stat or not _game_is_final("nba", r["game_id"]):
            continue
        actual = _lookup_actual("nba", int(r["player_id"]), r["game_id"], stat)
        if actual is None:
            try:
                st = datetime.fromisoformat(str(r["start_time"]).replace("Z", "+00:00"))
                if st.tzinfo is None:
                    st = st.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if now - st < timedelta(hours=12):
                continue                  # box score may not be ingested yet
        c.execute("UPDATE prop_eval_log SET actual = ?, graded_at = ? "
                  "WHERE game_id = ? AND player_id = ? AND bet_type = ?",
                  (actual, stamp, r["game_id"], r["player_id"], r["bet_type"]))
        done += 1
    if done:
        c.commit()
    return done


def _graded_rows() -> list:
    c = _conn()
    try:
        _ensure(c)
        return c.execute(
            "SELECT bet_type, "
            "CASE WHEN pick_seen IS NOT NULL THEN pick_line ELSE first_line END AS ref_line, "
            "CASE WHEN pick_seen IS NOT NULL THEN pick_over ELSE first_over END AS ref_over, "
            "CASE WHEN pick_seen IS NOT NULL THEN pick_under ELSE first_under END AS ref_under, "
            "CASE WHEN pick_seen IS NOT NULL THEN pick_p_over ELSE first_p_over END AS ref_p_over, "
            "last_line, last_over, last_under, actual "
            "FROM prop_eval_log WHERE actual IS NOT NULL").fetchall()
    except sqlite3.OperationalError:
        return []


def refit() -> dict:
    doc = _core.fit(_graded_rows(), MARKETS, default_w=DEFAULT_W)
    _core.write(_PATH, doc)
    logger.info("nba prop refit: %s", {bt: (e["n"], e["w"], e["status"], e.get("clv_move_pp"))
                                       for bt, e in doc["markets"].items()})
    return doc


def maybe_refit() -> dict | None:
    """Refit when the weights file is missing or a week old; else None."""
    return refit() if _core.due(_PATH) else None


def weight_for(bet_type: str) -> float:
    return _core.weight_for(_PATH, bet_type, DEFAULT_W.get(bet_type, _core.DEFAULT_W))


__all__ = ["log_lines", "stamp_prices", "grade", "refit", "maybe_refit", "weight_for",
           "MARKETS", "DEFAULT_W"]
