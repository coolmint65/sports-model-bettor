"""NHL player-prop line log + weekly market-blend refit (MintyBot 2026-10-26, Austin "Go for it").

The same evidence loop NFL props v2 runs (engine.sports.nfl.prop_picks / prop_refit):
  * engine.sports.nhl.prop_picks logs EVERY main line it prices -- the model's raw prob
    (after the per-stat shrink, before the market anchor) with Hard Rock's price -- via
    log_lines(); the picker itself runs only once or twice a day.
  * stamp_prices() (worker, every minute alongside the props closing-odds capture) writes
    Hard Rock's current price for each logged line onto its last_* columns until puck
    drop, so "last" is the closing price, not whenever the picker last ran.
  * grade() fills the outcome from the NHL box scores (tracker's _lookup_actual, so
    Points = G + A); a player with no log once the game is final + 12h is a DNP.
  * maybe_refit() refits the model's blend weight per stat weekly (engine.prop_refit),
    written to data/nhl_prop_weights.json; the picker reads weight_for(bet_type) instead
    of a fixed 0.35. Each stat STARTS from its DraftKings backtest weight (DEFAULT_W).
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
import unicodedata
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ... import prop_refit as _core

logger = logging.getLogger(__name__)

MARKETS = ("Skater SOG", "Skater Points", "Skater Assists", "Skater Goals",
           "Goalie Saves", "Goalie Goals Against")
# Starting / shrink-target model weight per stat (MintyBot 2026-10-26, Austin "fix the edge
# now"): the live model replayed as of each game date on 11,503 DraftKings main lines this
# season (ESPN propBets + results), best blend of w*model + (1-w)*DK by log-loss on outcomes,
# against DK's OPENING price -- the price a pick is made at (the refit's own rule: fitting
# against the close hides what the model knows that the open hasn't priced yet). vs the open:
# SOG .55, Points .25, Assists .20, Saves .15, Blocks .10 (vs the close they'd be .25/0/.15/.10/0).
# The SOG model predicts DK's open->close move: when it sits >=5pp off the open (1,640 of 2,925
# lines) the close moved its way 68% of the time, +4.0pp on average (corr .64). Goals: anytime is
# one-sided, no open price -> .05 from the close fit. Goals Against: no DK data, .10.
DEFAULT_W = {"Skater SOG": 0.55, "Skater Points": 0.25, "Skater Assists": 0.20,
             "Skater Goals": 0.05, "Goalie Saves": 0.15, "Goalie Goals Against": 0.10}
_PATH = Path(__file__).resolve().parents[3] / "data" / "nhl_prop_weights.json"
_TS = "%Y-%m-%dT%H:%M:%SZ"

# DraftKings' price on the same player-stat, logged beside Hard Rock's on every line (Austin
# 2026-10-26 "add logging to NHL", the NBA eval-log pattern): ESPN's core API carries DK's NHL
# props (provider 100; first item of each priced pair = the Over, verified on 11.5k graded
# lines). Our ids are NHL API ids, so games map by date + teams and players by first initial
# + last name. Anytime Goalscorer is one-sided (Over 0.5 only).
_DK_TYPES = {"Total Points": "Skater Points", "Total Assists": "Skater Assists",
             "Total Shots on Goal": "Skater SOG", "Total Saves": "Goalie Saves",
             "Anytime Goalscorer": "Skater Goals"}
_DK_COLS = (("dk_line", "REAL"), ("dk_over", "INTEGER"), ("dk_under", "INTEGER"),
            ("dk_pick_line", "REAL"), ("dk_pick_over", "INTEGER"), ("dk_pick_under", "INTEGER"),
            ("dk_last_line", "REAL"), ("dk_last_over", "INTEGER"), ("dk_last_under", "INTEGER"))
# SOG v2 (MintyBot 2026-11-01): which model produced first_p_over / pick_p_over, plus the v1 number
# logged beside it, so the old and new SOG models keep accumulating evidence side by side.
_MODEL_COLS = (("model", "TEXT"), ("first_p_over_v1", "REAL"), ("pick_model", "TEXT"),
               ("pick_p_over_v1", "REAL"))
_DK_TTL_S = 600
_DK_CACHE: dict = {}
_EV_CACHE: dict = {}
_ATH_PATH = Path(__file__).resolve().parents[3] / "data" / "nhl_espn_athletes.json"
_ATH: dict | None = None
_ESPN_CORE = "https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl"


def _conn():
    from ...player_props_db import _conn_for
    return _conn_for("nhl")         # thread-local nhl.db connection (never close it)


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
        pick_seen TEXT, pick_line REAL, pick_over INTEGER, pick_under INTEGER, pick_p_over REAL,
        PRIMARY KEY (game_id, player_id, bet_type))""")
    have = {r[1] for r in c.execute("PRAGMA table_info(prop_eval_log)")}
    for col, typ in (("pick_seen", "TEXT"), ("pick_line", "REAL"), ("pick_over", "INTEGER"),
                     ("pick_under", "INTEGER"), ("pick_p_over", "REAL")) + _DK_COLS + _MODEL_COLS:
        if col not in have:
            c.execute("ALTER TABLE prop_eval_log ADD COLUMN %s %s" % (col, typ))


def _get_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "curl/8.4.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def _name_key(name: str):
    """("s", "couturier") from "S. Couturier" (our logs) or "Sean Couturier" (ESPN)."""
    t = re.sub(r"[^a-z ]", " ", unicodedata.normalize("NFKD", name or "")
               .encode("ascii", "ignore").decode().lower()).split()
    return (t[0][0], " ".join(t[1:])) if len(t) >= 2 else None


def _am(v):
    try:
        v = str(v).replace("+", "").strip()
        return 100 if v.lower() == "even" else int(round(float(v)))
    except (TypeError, ValueError):
        return None


def _espn_event_id(game_id: str):
    """ESPN event id for one of our NHL games (date + teams off the ESPN scoreboard)."""
    from ._ratings import canon
    c = _conn()
    g = c.execute("SELECT g.date, h.abbreviation ha, a.abbreviation aa FROM nhl_games g "
                  "JOIN nhl_teams h ON h.id = g.home_team_id JOIN nhl_teams a ON a.id = g.away_team_id "
                  "WHERE CAST(g.game_id AS TEXT) = ?", (str(game_id),)).fetchone()
    if not g:
        return None
    hit = _EV_CACHE.get(g["date"])
    if hit is None or time.time() - hit[0] > 3600:
        evs = {}
        try:
            d = _get_json("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates="
                          + str(g["date"]).replace("-", ""))
            for ev in d.get("events") or []:
                cs = {x.get("homeAway"): canon((x.get("team") or {}).get("abbreviation"))
                      for x in ((ev.get("competitions") or [{}])[0].get("competitors") or [])}
                evs[(cs.get("home"), cs.get("away"))] = str(ev.get("id"))
        except Exception as e:
            logger.debug("nhl dk: scoreboard %s failed: %s", g["date"], e)
        hit = (time.time(), evs)
        _EV_CACHE[g["date"]] = hit
    return hit[1].get((canon(g["ha"]), canon(g["aa"])))


def _athlete_name(aid: str):
    global _ATH
    if _ATH is None:
        try:
            _ATH = json.loads(_ATH_PATH.read_text())
        except Exception:
            _ATH = {}
    if aid not in _ATH:
        try:
            _ATH[aid] = _get_json(f"{_ESPN_CORE}/athletes/{aid}").get("displayName")
            _ATH_PATH.write_text(json.dumps(_ATH))
        except Exception as e:
            logger.debug("nhl dk: athlete %s failed: %s", aid, e)
            return None
    return _ATH.get(aid)


def dk_lines(game_id: str) -> dict:
    """{(name_key, bet_type): (line, over_odds, under_odds|None)} -- DraftKings' main line per
    player-stat for one of our NHL games (pair nearest an even price). {} if ESPN has none.
    Cached 10 min."""
    gid = str(game_id)
    hit = _DK_CACHE.get(gid)
    if hit and time.time() - hit[0] < _DK_TTL_S:
        return hit[1]
    out: dict = {}
    try:
        eid = _espn_event_id(gid)
        if eid:
            url = f"{_ESPN_CORE}/events/{eid}/competitions/{eid}/odds/100/propBets?limit=1000"
            d = _get_json(url)
            items = list(d.get("items") or [])
            for pg in range(2, int(d.get("pageCount") or 1) + 1):
                items += _get_json(f"{url}&page={pg}").get("items") or []
            grp = defaultdict(lambda: defaultdict(list))
            for k, it in enumerate(items):
                bt = _DK_TYPES.get((it.get("type") or {}).get("name"))
                aid = ((it.get("athlete") or {}).get("$ref") or "").split("/athletes/")[-1].split("?")[0]
                o = it.get("odds") or {}
                am = _am((o.get("american") or {}).get("value"))
                if not bt or not aid.isdigit() or am is None:
                    continue
                line = 0.5 if bt == "Skater Goals" else (o.get("total") or {}).get("value")
                if line is not None:
                    grp[(aid, bt)][str(line)].append((k, am))
            for (aid, bt), bylines in grp.items():
                nk = _name_key(_athlete_name(aid) or "")
                if nk is None:
                    continue
                if bt == "Skater Goals":
                    v = bylines.get("0.5") or []
                    if v:
                        out[(nk, bt)] = (0.5, v[0][1], None)
                    continue
                best = None
                for ln, v in bylines.items():
                    if len(v) != 2:
                        continue
                    v = sorted(v)
                    io, iu = _core_imp(v[0][1]), _core_imp(v[1][1])
                    bal = abs(io / (io + iu) - 0.5)
                    if best is None or bal < best[0]:
                        best = (bal, (float(ln), v[0][1], v[1][1]))
                if best:
                    out[(nk, bt)] = best[1]
    except Exception as e:
        logger.debug("nhl dk_lines %s failed: %s", gid, e)
    _DK_CACHE[gid] = (time.time(), out)
    return out


def _core_imp(a: int) -> float:
    return 100.0 / (a + 100.0) if a > 0 else -a / (-a + 100.0)


def _dk_for(row) -> tuple:
    """(dk_line, dk_over, dk_under) for one logged row, or (None, None, None)."""
    try:
        hit = dk_lines(row["game_id"]).get((_name_key(row["player_name"]), row["bet_type"]))
    except Exception:
        hit = None
    return hit or (None, None, None)


def log_lines(rows: list) -> None:
    """Upsert priced main lines: dicts with game_id, game_date, player_id, player_name,
    bet_type, line, over_odds, under_odds, p_over (raw model). The first pass on game day
    also fills pick_* -- the price the picker actually decided against (it records its
    picks on that pass) -- which is what the weekly refit fits on."""
    rows = [r for r in rows if r.get("over_odds") is not None and r.get("under_odds") is not None]
    if not rows:
        return
    from ..._tz import et_today_str
    today = et_today_str()
    c = _conn()
    try:
        _ensure(c)
        now = _now()
        for r in rows:
            v = (str(r["game_id"]), r["game_date"], int(r["player_id"]), r["player_name"],
                 r["bet_type"], now, float(r["line"]), int(r["over_odds"]), int(r["under_odds"]),
                 round(float(r["p_over"]), 4))
            dk = _dk_for(r)
            _p1 = r.get("p_over_v1")
            mv = (r.get("model"), round(float(_p1), 4) if _p1 is not None else None)
            c.execute(
                "INSERT INTO prop_eval_log(game_id, game_date, player_id, player_name, bet_type, "
                "first_seen, first_line, first_over, first_under, first_p_over, "
                "last_seen, last_line, last_over, last_under, last_p_over, "
                "dk_line, dk_over, dk_under, dk_last_line, dk_last_over, dk_last_under, "
                "model, first_p_over_v1, n_seen) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1) "
                "ON CONFLICT(game_id, player_id, bet_type) DO UPDATE SET "
                "last_seen = excluded.last_seen, last_line = excluded.last_line, "
                "last_over = excluded.last_over, last_under = excluded.last_under, "
                "last_p_over = excluded.last_p_over, n_seen = prop_eval_log.n_seen + 1, "
                "dk_line = COALESCE(prop_eval_log.dk_line, excluded.dk_line), "
                "dk_over = COALESCE(prop_eval_log.dk_over, excluded.dk_over), "
                "dk_under = COALESCE(prop_eval_log.dk_under, excluded.dk_under), "
                "dk_last_line = COALESCE(excluded.dk_last_line, prop_eval_log.dk_last_line), "
                "dk_last_over = COALESCE(excluded.dk_last_over, prop_eval_log.dk_last_over), "
                "dk_last_under = COALESCE(excluded.dk_last_under, prop_eval_log.dk_last_under) "
                "WHERE prop_eval_log.actual IS NULL AND prop_eval_log.graded_at IS NULL",
                v + v[5:] + dk + dk + mv)
            if r["game_date"] == today:
                c.execute("UPDATE prop_eval_log SET pick_seen = ?, pick_line = ?, pick_over = ?, "
                          "pick_under = ?, pick_p_over = ?, dk_pick_line = ?, dk_pick_over = ?, "
                          "dk_pick_under = ?, pick_model = ?, pick_p_over_v1 = ? "
                          "WHERE game_id = ? AND player_id = ? "
                          "AND bet_type = ? AND pick_seen IS NULL AND graded_at IS NULL",
                          v[5:] + dk + mv + v[:1] + (v[2], v[4]))
        c.commit()
    except Exception as e:
        c.rollback()
        logger.warning("nhl prop_eval: log write failed: %s", e)


def stamp_prices() -> int:
    """Write Hard Rock's current two-way price onto every logged line whose game hasn't
    started (same player, stat and line). Returns lines stamped; fetches the board only
    when there is something to stamp (the props scraper caches it 60s)."""
    c = _conn()
    try:
        _ensure(c)
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        rows = c.execute(
            "SELECT l.game_id, l.player_id, l.player_name, l.bet_type, l.last_line "
            "FROM prop_eval_log l JOIN nhl_games g ON CAST(g.game_id AS TEXT) = l.game_id "
            "WHERE l.graded_at IS NULL AND g.status = 'scheduled' "
            "AND REPLACE(REPLACE(g.start_time, 'T', ' '), 'Z', '') > REPLACE(?, 'T', ' ')",
            (now_iso,)).fetchall()
    except sqlite3.OperationalError:
        return 0
    if not rows:
        return 0
    try:
        from scrapers.hardrock_props import fetch_nhl_props
        board = fetch_nhl_props() or {}
    except Exception as e:
        logger.warning("nhl prop_eval: board fetch failed: %s", e)
        return 0
    idx: dict = {}
    for brows in board.values():
        for b in brows:
            if b.get("line") is None or b.get("over_odds") is None or b.get("under_odds") is None:
                continue
            key = (" ".join((b.get("player_name") or "").lower().split()), b.get("bet_type"),
                   float(b["line"]))
            idx[key] = (int(b["over_odds"]), int(b["under_odds"]))
    stamp, n = _now(), 0
    for r in rows:
        dk = _dk_for(r)
        if dk[1] is not None:
            c.execute("UPDATE prop_eval_log SET dk_last_line = ?, dk_last_over = ?, dk_last_under = ? "
                      "WHERE game_id = ? AND player_id = ? AND bet_type = ? AND graded_at IS NULL",
                      dk + (r["game_id"], r["player_id"], r["bet_type"]))
        hit = idx.get((" ".join((r["player_name"] or "").lower().split()), r["bet_type"],
                       float(r["last_line"])))
        if not hit:
            continue
        c.execute("UPDATE prop_eval_log SET last_over = ?, last_under = ?, price_stamped_at = ? "
                  "WHERE game_id = ? AND player_id = ? AND bet_type = ? AND graded_at IS NULL",
                  (hit[0], hit[1], stamp, r["game_id"], r["player_id"], r["bet_type"]))
        n += 1
    if n:
        c.commit()
    return n


def grade(limit: int = 3000) -> int:
    """Fill ``actual`` from the NHL box scores for lines whose game is final."""
    from ...player_props_tracker import _NHL_STAT_KEY, _lookup_actual, _game_is_final
    c = _conn()
    try:
        _ensure(c)
        rows = c.execute(
            "SELECT l.game_id, l.player_id, l.bet_type, g.start_time FROM prop_eval_log l "
            "LEFT JOIN nhl_games g ON CAST(g.game_id AS TEXT) = l.game_id "
            "WHERE l.graded_at IS NULL LIMIT ?", (int(limit),)).fetchall()
    except sqlite3.OperationalError:
        return 0
    done, stamp, now = 0, _now(), datetime.now(timezone.utc)
    for r in rows:
        stat = _NHL_STAT_KEY.get(r["bet_type"])
        if not stat or not _game_is_final("nhl", r["game_id"]):
            continue
        actual = _lookup_actual("nhl", int(r["player_id"]), r["game_id"], stat)
        if actual is None:
            try:
                st = datetime.fromisoformat(str(r["start_time"]).replace("Z", "+00:00"))
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
            "FROM prop_eval_log WHERE actual IS NOT NULL "
            # SOG weights are fit only on lines the v2 model priced (2026-11-01): the v1 numbers
            # would hand the v2 model the old model's (much lower) weight.
            "AND (bet_type != 'Skater SOG' OR "
            "(CASE WHEN pick_seen IS NOT NULL THEN pick_model ELSE model END) = 'sog_v2')").fetchall()
    except sqlite3.OperationalError:
        return []


def refit() -> dict:
    doc = _core.fit(_graded_rows(), MARKETS, default_w=DEFAULT_W)
    _core.write(_PATH, doc)
    logger.info("nhl prop refit: %s", {bt: (e["n"], e["w"], e["status"], e.get("clv_move_pp"))
                                       for bt, e in doc["markets"].items()})
    return doc


def maybe_refit() -> dict | None:
    """Refit when the weights file is missing or a week old; else None."""
    return refit() if _core.due(_PATH) else None


def weight_for(bet_type: str) -> float:
    return _core.weight_for(_PATH, bet_type, DEFAULT_W.get(bet_type, _core.DEFAULT_W))


__all__ = ["log_lines", "stamp_prices", "grade", "refit", "maybe_refit", "weight_for", "MARKETS",
           "DEFAULT_W", "dk_lines"]
