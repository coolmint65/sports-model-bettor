"""Box-score-level ingest for ESPN-tracked basketball leagues.

Pulls per-game per-team stats from ESPN's summary endpoint and persists
into the framework's ``game_team_stats`` table. Computes pace, offensive
rating, and defensive rating per team-game in the same pass.

Run::

    python -m engine.basketball._boxscore_ingest wnba             # backfill all final games
    python -m engine.basketball._boxscore_ingest wnba --since 2025-08-01

Pace formula (Dean Oliver):
    POSS = FGA - ORB + TOV + 0.475 * FTA
ORtg/DRtg = points / possessions * 100 (per-team per-game).

Persisted values are *raw* per-game; rolling-team averages are computed
on read (engine.basketball._team_stats).
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request
from typing import Iterable

from ._config import get_league_config
from ._db import get_conn

logger = logging.getLogger(__name__)


_USER_AGENT = "SportsBettor/1.0 (basketball-boxscore)"


def _fetch(url: str, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": _USER_AGENT,
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            logger.debug("ESPN summary %d/%d failed (%s): %s",
                         attempt + 1, retries, url, e)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def _summary_url(league: str, game_id: str) -> str:
    cfg = get_league_config(league)
    path = cfg.get("espn_league_path")
    if not path:
        raise ValueError(f"League {league!r} has no espn_league_path")
    return f"https://site.api.espn.com/apis/site/v2/sports/{path}/summary?event={game_id}"


# ESPN stat label → our column name. ESPN ships statistics as labeled
# rows; the ``name`` field is the stable key. We use the labels as
# fallbacks when ``name`` isn't populated for some leagues.
_STAT_KEYS = {
    "fieldGoalsMade-fieldGoalsAttempted": ("fgm", "fga"),
    "threePointFieldGoalsMade-threePointFieldGoalsAttempted": ("tpm", "tpa"),
    "freeThrowsMade-freeThrowsAttempted": ("ftm", "fta"),
    "totalRebounds": ("totr",),
    "offensiveRebounds": ("orb",),
    "defensiveRebounds": ("drb",),
    "assists": ("ast",),
    "steals": ("stl",),
    "blocks": ("blk",),
    "turnovers": ("tov",),
    "fouls": ("fouls",),
    "fastBreakPoints": ("fastbreak",),
    "pointsInPaint": ("paint",),
}


def _parse_stat(label: dict) -> dict:
    """Parse one ESPN statistics row into {col: value}. Handles the
    'fgm-fga' compound shape ESPN ships for FG/3PT/FT."""
    name = label.get("name") or ""
    val = label.get("displayValue") or ""
    cols = _STAT_KEYS.get(name)
    if not cols:
        return {}
    if len(cols) == 2 and "-" in val:
        try:
            a, b = val.split("-", 1)
            return {cols[0]: int(a), cols[1]: int(b)}
        except ValueError:
            return {}
    if len(cols) == 1:
        try:
            return {cols[0]: int(val)}
        except ValueError:
            try:
                return {cols[0]: int(float(val))}
            except ValueError:
                return {}
    return {}


def _compute_pace(stats: dict) -> float | None:
    """Possessions per Dean Oliver. Returns None when required fields
    are missing (early-season ESPN sometimes omits ORB/TOV)."""
    fga = stats.get("fga")
    fta = stats.get("fta")
    orb = stats.get("orb")
    tov = stats.get("tov")
    if None in (fga, fta, orb, tov):
        return None
    return float(fga - orb + tov + 0.475 * fta)


def ingest_game(league: str, game_id: str, points_by_team: dict[int, int]) -> bool:
    """Pull box-score for one game and upsert per-team stats. Returns
    True when both team rows were written."""
    url = _summary_url(league, game_id)
    data = _fetch(url)
    if not data:
        return False
    teams = (data.get("boxscore") or {}).get("teams") or []
    if len(teams) != 2:
        return False

    conn = get_conn(league)
    rows: list[dict] = []
    for t in teams:
        team_id = int((t.get("team") or {}).get("id") or 0)
        if not team_id:
            return False
        is_home = 1 if t.get("homeAway") == "home" else 0
        stats: dict = {}
        for s in t.get("statistics") or []:
            stats.update(_parse_stat(s))
        pace = _compute_pace(stats)
        points = points_by_team.get(team_id)
        ortg = (points / pace * 100.0) if (points and pace and pace > 0) else None
        rows.append({
            "team_id": team_id,
            "is_home": is_home,
            "points": points,
            "stats": stats,
            "pace": pace,
            "ortg": ortg,
        })

    # DRtg requires the OPPONENT's points/pace pair, so we resolve
    # cross-team after both rows are parsed.
    if len(rows) == 2:
        rows[0]["drtg"] = (
            (rows[1]["points"] / rows[0]["pace"] * 100.0)
            if (rows[1]["points"] and rows[0]["pace"] and rows[0]["pace"] > 0)
            else None
        )
        rows[1]["drtg"] = (
            (rows[0]["points"] / rows[1]["pace"] * 100.0)
            if (rows[0]["points"] and rows[1]["pace"] and rows[1]["pace"] > 0)
            else None
        )

    for row in rows:
        s = row["stats"]
        conn.execute(
            "INSERT OR REPLACE INTO game_team_stats "
            "(game_id, team_id, is_home, points, "
            " fga, fgm, fta, ftm, tpa, tpm, "
            " orb, drb, ast, stl, blk, tov, fouls, "
            " fastbreak, paint, totp, pace, ortg, drtg) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                game_id, row["team_id"], row["is_home"], row["points"],
                s.get("fga"), s.get("fgm"), s.get("fta"), s.get("ftm"),
                s.get("tpa"), s.get("tpm"),
                s.get("orb"), s.get("drb"), s.get("ast"),
                s.get("stl"), s.get("blk"), s.get("tov"), s.get("fouls"),
                s.get("fastbreak"), s.get("paint"),
                # totp = total possessions (alias of pace, kept for clarity)
                row["pace"],
                row["pace"], row["ortg"], row["drtg"],
            ),
        )
    conn.commit()
    return True


def backfill(league: str, since: str | None = None,
              throttle_s: float = 0.25) -> dict:
    """Ingest box-scores for every finalized game without one. Idempotent
    on (game_id, team_id) so re-runs only fill gaps.

    Args:
        since: optional date floor (YYYY-MM-DD) to limit backfill.
        throttle_s: per-call sleep to avoid ESPN rate caps.
    """
    conn = get_conn(league)
    where = ("WHERE g.status = 'final' "
             "AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL "
             "AND NOT EXISTS (SELECT 1 FROM game_team_stats s "
             "                WHERE s.game_id = g.game_id)")
    params: list = []
    if since:
        where += " AND g.date >= ?"
        params.append(since)
    rows = conn.execute(
        f"SELECT g.game_id, g.home_team_id, g.away_team_id, "
        f"       g.home_score, g.away_score, g.date "
        f"FROM games g {where} ORDER BY g.date",
        params,
    ).fetchall()
    out = {"checked": len(rows), "ingested": 0, "skipped": 0}
    for r in rows:
        points = {
            int(r["home_team_id"]): int(r["home_score"]),
            int(r["away_team_id"]): int(r["away_score"]),
        }
        try:
            ok = ingest_game(league, r["game_id"], points)
        except Exception as e:
            logger.warning("[%s] game %s failed: %s",
                           league, r["game_id"], e)
            ok = False
        if ok:
            out["ingested"] += 1
        else:
            out["skipped"] += 1
        time.sleep(throttle_s)
    logger.info("[%s] box-score backfill: %s", league, out)
    return out


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.basketball._boxscore_ingest")
    ap.add_argument("league")
    ap.add_argument("--since", default=None,
                    help="Date floor YYYY-MM-DD")
    ap.add_argument("--throttle", type=float, default=0.25)
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    res = backfill(args.league, since=args.since, throttle_s=args.throttle)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
