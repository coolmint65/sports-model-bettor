"""NHL per-period goals + shots on goal from the NHL API daily score feed (MintyBot 2026-10-26).

nhl_games had period scores for 32 of 184 games of 2026-27 and shots for 27 (2025-26: 73 of
1,312): nothing scheduled scrapers.nhl_api's sync any more, so finals landed (scores come from
the settle path) without their box. The ratings model (engine.sports.nhl._ratings) fits on
regulation goals and shots, and the period markets settle on period goals.

One request per date (``/v1/score/{date}``) carries every game's state, shots on goal and goal
list with periods, keyed by the NHL game id our rows already use. Fills only what is missing
(period goals, shots, neutral-site flag); a game whose goal list doesn't add up to its final
score is left alone.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

_SCORE_URL = "https://api-web.nhle.com/v1/score/{date}"


def _get(url: str, retries: int = 3):
    for k in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "curl/8.4.0",
                                                       "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            logger.debug("nhl score feed %s attempt %d: %s", url, k + 1, e)
            time.sleep(1.5 * (k + 1))
    return None


def _ensure_neutral_column(conn) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(nhl_games)")}
    if "neutral" not in cols:
        conn.execute("ALTER TABLE nhl_games ADD COLUMN neutral INTEGER")
        conn.commit()


def sync_date(date_str: str, conn=None) -> int:
    """Fill period goals / shots / neutral flag for one date's finals. Returns games updated."""
    if conn is None:
        from .db import get_conn
        conn = get_conn()
    _ensure_neutral_column(conn)
    data = _get(_SCORE_URL.format(date=date_str))
    if not data:
        return 0
    n = 0
    for g in data.get("games") or []:
        if g.get("gameState") not in ("FINAL", "OFF") or g.get("gameType") not in (2, 3):
            continue
        gid = g.get("id")
        ht, at = g.get("homeTeam") or {}, g.get("awayTeam") or {}
        hab, aab = ht.get("abbrev"), at.get("abbrev")
        per = {("h", p): 0 for p in (1, 2, 3)}
        per.update({("a", p): 0 for p in (1, 2, 3)})
        for goal in g.get("goals") or []:
            p = (goal.get("periodDescriptor") or {}).get("number") or goal.get("period")
            if p not in (1, 2, 3):
                continue
            side = "h" if goal.get("teamAbbrev") == hab else ("a" if goal.get("teamAbbrev") == aab else None)
            if side:
                per[(side, p)] += 1
        hs, as_ = ht.get("score"), at.get("score")
        rh = per[("h", 1)] + per[("h", 2)] + per[("h", 3)]
        ra = per[("a", 1)] + per[("a", 2)] + per[("a", 3)]
        last = ((g.get("gameOutcome") or {}).get("lastPeriodType") or "").upper()
        if hs is None or as_ is None:
            continue
        if last == "REG" and (rh, ra) != (hs, as_):
            continue                                   # incomplete goal list -- leave the row
        if last in ("OT", "SO") and not (rh == ra and abs(hs - as_) == 1 and max(hs, as_) == rh + 1):
            continue
        row = conn.execute("SELECT home_p1, home_shots, neutral FROM nhl_games WHERE game_id = ?",
                           (gid,)).fetchone()
        if row is None:
            continue
        sets, vals = [], []
        if row[0] is None:
            sets += ["home_p1 = ?", "away_p1 = ?", "home_p2 = ?", "away_p2 = ?", "home_p3 = ?", "away_p3 = ?"]
            vals += [per[("h", 1)], per[("a", 1)], per[("h", 2)], per[("a", 2)], per[("h", 3)], per[("a", 3)]]
        if (row[1] is None or row[1] == 0) and ht.get("sog") and at.get("sog"):
            sets += ["home_shots = ?", "away_shots = ?"]
            vals += [int(ht["sog"]), int(at["sog"])]
        if row[2] is None:
            sets.append("neutral = ?")
            vals.append(1 if g.get("neutralSite") else 0)
        if sets:
            conn.execute(f"UPDATE nhl_games SET {', '.join(sets)} WHERE game_id = ?", vals + [gid])
            n += 1
    conn.commit()
    return n


def missing_dates(conn=None, since: str = "2023-09-01") -> list[str]:
    """Dates with a final regular-season/playoff game still missing period goals or shots."""
    if conn is None:
        from .db import get_conn
        conn = get_conn()
    _ensure_neutral_column(conn)
    return [r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM nhl_games WHERE status = 'final' AND game_type IN (2, 3) "
        "AND date >= ? AND (home_p1 IS NULL OR home_shots IS NULL OR home_shots = 0 OR neutral IS NULL) "
        "ORDER BY date", (since,))]


def sync_recent(days: int = 10) -> int:
    """Worker entry: every date in the last ``days`` with a final still missing its box."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    n = 0
    for d in missing_dates(since=cutoff):
        n += sync_date(d)
    return n


def backfill(since: str = "2023-09-01", pause: float = 0.3) -> int:
    n = 0
    for d in missing_dates(since=since):
        n += sync_date(d)
        time.sleep(pause)
    return n


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    print("updated", backfill(sys.argv[1] if len(sys.argv) > 1 else "2023-09-01"))
