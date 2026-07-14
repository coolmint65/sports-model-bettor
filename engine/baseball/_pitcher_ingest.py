"""Per-game pitcher box-score ingest for the baseball framework.

ESPN's NCAA box score includes a `pitching` statistics group per team
with one row per pitcher who appeared. Each row carries IP, H, R, ER,
BB, K, pitch count, and a `starter` boolean. We persist every
appearance into ``pitcher_starts`` so the GBM feature builder can
compute rolling ERA / K-rate / IP per start over the prior N
appearances of whichever pitcher starts tonight's game.

Heaviest single source of MLB Brier improvement that's missing from
the college baseball framework — pitcher matchup is the single most
explanatory variable for any individual baseball game outcome.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime

from . import get_league_config
from ._db import get_conn

logger = logging.getLogger(__name__)


_USER_AGENT = "sports-model-bettor/baseball-pitcher"
_HEADERS = {"User-Agent": _USER_AGENT}


_DDL = """
CREATE TABLE IF NOT EXISTS pitcher_starts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id         TEXT NOT NULL,
    game_date       TEXT NOT NULL,
    team_id         INTEGER NOT NULL,
    pitcher_id      INTEGER,                  -- ESPN athlete id when available
    pitcher_name    TEXT,
    is_starter      INTEGER NOT NULL,         -- 1 = starter, 0 = reliever
    ip_outs         INTEGER,                  -- innings pitched as outs (e.g. 6.2 IP -> 20)
    hits            INTEGER,
    runs            INTEGER,
    earned_runs     INTEGER,
    walks           INTEGER,
    strikeouts      INTEGER,
    home_runs       INTEGER,
    pitches         INTEGER,
    strikes         INTEGER,
    game_era        REAL,                     -- ESPN-shipped single-game ERA
    UNIQUE(game_id, pitcher_name, is_starter)
);
CREATE INDEX IF NOT EXISTS idx_pitcher_starts_pid
    ON pitcher_starts(pitcher_id);
CREATE INDEX IF NOT EXISTS idx_pitcher_starts_team
    ON pitcher_starts(team_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pitcher_starts_date
    ON pitcher_starts(game_date);
"""


def _ensure_schema(league: str) -> None:
    conn = get_conn(league)
    conn.executescript(_DDL)
    conn.commit()


def _ip_to_outs(ip_str: str | None) -> int | None:
    """ESPN ships IP as "6.2" meaning 6 full innings + 2 outs (NOT 6.2
    innings as a decimal). 1 IP = 3 outs."""
    if not ip_str:
        return None
    try:
        full, _, partial = str(ip_str).partition(".")
        return int(full) * 3 + (int(partial) if partial else 0)
    except (ValueError, TypeError):
        return None


def _safe_int(v) -> int | None:
    if v in (None, ""):
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


def _safe_float(v) -> float | None:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _fetch_summary(league: str, game_id: str) -> dict | None:
    cfg = get_league_config(league)
    espn_path = cfg.get("espn_league_path") or f"baseball/{league}"
    url = (f"https://site.api.espn.com/apis/site/v2/sports/"
            f"{espn_path}/summary?event={game_id}")
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            time.sleep(0.4 * (attempt + 1))
        except Exception:
            time.sleep(0.4 * (attempt + 1))
    return None


def ingest_box(league: str, game_id: str) -> dict:
    """Pull one game's box score, persist every pitcher row that has a
    starter flag. Idempotent — UNIQUE(game_id, pitcher_name, is_starter)
    keeps re-runs from accumulating duplicates."""
    data = _fetch_summary(league, game_id)
    if not data:
        return {"game_id": game_id, "pitchers": 0, "skipped": True}
    conn = get_conn(league)
    # Game date from header.
    iso = (data.get("header") or {}).get("competitions", [{}])[0].get("date")
    game_date = (iso or "")[:10] if iso else ""
    if not game_date:
        # Fall back to our games table
        row = conn.execute(
            "SELECT date FROM games WHERE game_id = ?", (game_id,),
        ).fetchone()
        game_date = (row["date"] if row else "")
    boxscore = data.get("boxscore") or {}
    teams = boxscore.get("players") or []
    n_added = 0
    for team_block in teams:
        team_id = (team_block.get("team") or {}).get("id")
        try:
            team_id = int(team_id) if team_id else None
        except (ValueError, TypeError):
            team_id = None
        if not team_id:
            continue
        # `statistics` is a list with two groups (batting, pitching).
        for stat_group in team_block.get("statistics") or []:
            if (stat_group.get("type") or "").lower() != "pitching":
                continue
            keys = stat_group.get("keys") or []
            # Find each known stat's index in the per-row stats list.
            idx_ip = keys.index("fullInnings.partInnings") if "fullInnings.partInnings" in keys else None
            idx_h  = keys.index("hits") if "hits" in keys else None
            idx_r  = keys.index("runs") if "runs" in keys else None
            idx_er = keys.index("earnedRuns") if "earnedRuns" in keys else None
            idx_bb = keys.index("walks") if "walks" in keys else None
            idx_k  = keys.index("strikeouts") if "strikeouts" in keys else None
            idx_hr = keys.index("homeRuns") if "homeRuns" in keys else None
            idx_ps = keys.index("pitches-strikes") if "pitches-strikes" in keys else None
            idx_era = keys.index("ERA") if "ERA" in keys else None
            for ath in (stat_group.get("athletes") or []):
                a = ath.get("athlete") or {}
                pid = _safe_int(a.get("id"))
                pname = a.get("displayName") or ""
                stats = ath.get("stats") or []
                if not (pname and stats):
                    continue
                ip_str = stats[idx_ip] if idx_ip is not None and idx_ip < len(stats) else None
                ps_str = stats[idx_ps] if idx_ps is not None and idx_ps < len(stats) else ""
                pitches = strikes = None
                if ps_str and "-" in ps_str:
                    try:
                        p_, s_ = ps_str.split("-", 1)
                        pitches = int(p_); strikes = int(s_)
                    except (ValueError, TypeError):
                        pitches = strikes = None
                conn.execute(
                    "INSERT OR IGNORE INTO pitcher_starts "
                    "(game_id, game_date, team_id, pitcher_id, "
                    " pitcher_name, is_starter, ip_outs, hits, runs, "
                    " earned_runs, walks, strikeouts, home_runs, "
                    " pitches, strikes, game_era) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        str(game_id), game_date, team_id, pid, pname,
                        1 if ath.get("starter") else 0,
                        _ip_to_outs(ip_str),
                        _safe_int(stats[idx_h]) if idx_h is not None else None,
                        _safe_int(stats[idx_r]) if idx_r is not None else None,
                        _safe_int(stats[idx_er]) if idx_er is not None else None,
                        _safe_int(stats[idx_bb]) if idx_bb is not None else None,
                        _safe_int(stats[idx_k]) if idx_k is not None else None,
                        _safe_int(stats[idx_hr]) if idx_hr is not None else None,
                        pitches, strikes,
                        _safe_float(stats[idx_era]) if idx_era is not None else None,
                    ),
                )
                n_added += 1
    conn.commit()
    return {"game_id": game_id, "pitchers": n_added}


def backfill(league: str, *, since_date: str | None = None,
              throttle: float = 0.12, limit: int | None = None) -> dict:
    """Walk every finalized game and ingest its pitcher box. Idempotent
    via the unique index — re-running on already-ingested games does
    nothing."""
    _ensure_schema(league)
    conn = get_conn(league)
    where = "status='final' AND home_score IS NOT NULL"
    params: list = []
    if since_date:
        where += " AND date >= ?"
        params.append(since_date)
    sql = f"SELECT game_id FROM games WHERE {where} ORDER BY date ASC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    games = conn.execute(sql, params).fetchall()
    totals = {"games": 0, "pitchers": 0, "skipped": 0}
    for r in games:
        try:
            res = ingest_box(league, r["game_id"])
            totals["games"] += 1
            if res.get("skipped"):
                totals["skipped"] += 1
            else:
                totals["pitchers"] += res["pitchers"]
        except Exception as e:
            logger.debug("ingest_box %s failed: %s", r["game_id"], e)
            totals["skipped"] += 1
        time.sleep(throttle)
        if totals["games"] % 100 == 0 and totals["games"]:
            logger.info("[baseball:%s] pitcher_ingest progress: %d games, "
                        "%d pitchers", league, totals["games"],
                        totals["pitchers"])
    logger.info("[baseball:%s] pitcher_ingest done: %s", league, totals)
    return totals


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.baseball._pitcher_ingest")
    ap.add_argument("league")
    ap.add_argument("--since-date", default=None)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--throttle", type=float, default=0.12)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    print(backfill(args.league, since_date=args.since_date,
                    limit=args.limit, throttle=args.throttle))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
