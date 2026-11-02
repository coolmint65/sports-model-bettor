"""Daily NBA + NHL roster refresh.

Wired 2026-09-30: nothing on the box scheduled a roster refresh, so the
NBA roster sat on last-season's players from an April manual run until
Sept. NFL/CFB self-refresh via game-log/schedule ingest; only NBA + NHL
need an explicit roster pull.

  NBA  fetch_nba_rosters()  -> ESPN /teams/{id}/roster (current camp roster)
  NHL  one-pass over /roster/{abbr}/current, upsert + reconcile `active`
       to the true current set (deactivate players off every roster).
       Guarded on all 32 teams fetching cleanly so a network blip can't
       wrongly empty a roster.
"""
from __future__ import annotations
import os, sys, sqlite3, datetime


def _log(msg: str) -> None:
    print("[%s] %s" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg),
          flush=True)


def _nhl_conn():
    try:
        from engine.nhl_db import get_conn
        return get_conn()
    except Exception:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return sqlite3.connect(os.path.join(root, "data", "nhl.db"))


def refresh_nba() -> int:
    from scrapers.nba_espn import fetch_nba_rosters, _current_season_year
    n = fetch_nba_rosters()
    _log("NBA rosters: %d players upserted (season %d)" % (n, _current_season_year()))
    return n


def refresh_nhl() -> int:
    from scrapers.nhl_api import NHL_API, NHL_TEAMS, _fetch, _nhl_str
    from engine.nhl_db import upsert_nhl_player, get_nhl_team_by_abbr

    current: set = set()
    teams_ok = 0
    saved = 0
    for abbr in NHL_TEAMS:
        data = _fetch("%s/roster/%s/current" % (NHL_API, abbr))
        if not data:
            _log("NHL: no data for %s" % abbr)
            continue
        team = get_nhl_team_by_abbr(abbr)
        tid = team["id"] if team else None
        cnt = 0
        for group in ("forwards", "defensemen", "goalies"):
            for p in data.get(group, []):
                pid = p.get("id")
                name = (_nhl_str(p, "firstName") + " " + _nhl_str(p, "lastName")).strip()
                if pid and name:
                    upsert_nhl_player(pid, name, tid, p.get("positionCode", ""),
                                      p.get("shootsCatches", ""))
                    current.add(pid)
                    cnt += 1
        if cnt >= 15:
            teams_ok += 1
        saved += cnt
    _log("NHL rosters: %d players upserted across %d teams" % (saved, teams_ok))

    conn = _nhl_conn()
    before = conn.execute("SELECT COUNT(*) FROM nhl_players WHERE active=1").fetchone()[0]
    if teams_ok == len(NHL_TEAMS) and len(current) >= 700:
        ph = ",".join("?" * len(current))
        conn.execute("UPDATE nhl_players SET active=0")
        conn.execute("UPDATE nhl_players SET active=1 WHERE id IN (%s)" % ph, list(current))
        conn.commit()
        after = conn.execute("SELECT COUNT(*) FROM nhl_players WHERE active=1").fetchone()[0]
        _log("NHL active reconciled: %d -> %d active" % (before, after))
    else:
        _log("NHL reconcile SKIPPED (guard: teams_ok=%d, current=%d) — active left as-is"
             % (teams_ok, len(current)))
    return saved


if __name__ == "__main__":
    _log("=== roster refresh start ===")
    for name, fn in (("NBA", refresh_nba), ("NHL", refresh_nhl)):
        try:
            fn()
        except Exception as e:
            import traceback
            _log("%s FAILED: %s" % (name, e))
            traceback.print_exc()
    _log("=== roster refresh done ===")
