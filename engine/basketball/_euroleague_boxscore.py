"""Euroleague box-score ingest.

Source: ``api-live.euroleague.net/v1/games?seasonCode=...&gameCode=...``
returns the full game record including per-player stat lines for both
clubs. We sum the player rows to derive team totals (Euroleague
doesn't ship a pre-aggregated team-totals element), compute pace via
the same Dean Oliver formula NBA/WNBA use, and persist into the
framework's ``game_team_stats`` table.

CLI::

    python -m engine.basketball._euroleague_boxscore --since 2024-10-01

Idempotent on (game_id, team_id).
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request
import xml.etree.ElementTree as ET

from ._db import get_conn
from ._euroleague_ingest import _team_id_from_code

logger = logging.getLogger(__name__)


_BASE = "https://api-live.euroleague.net/v1"
_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "SportsBettor/1.0"),
    "Accept": "text/xml,application/xml",
}


def _fetch(url: str) -> ET.Element | None:
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as r:
            return ET.fromstring(r.read())
    except Exception as e:
        logger.debug("Euroleague boxscore fetch failed (%s): %s", url, e)
        return None


def _safe_int(v, default=0):
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _sum_player_stats(playerstats: ET.Element | None) -> dict:
    """Sum per-player stat rows into team totals. Skips the trailing
    "Team" row (team-level rebounds/turnovers only — already counted
    elsewhere in the player rows for individual contribution)."""
    if playerstats is None:
        return {}
    totals = {
        "fga2": 0, "fga3": 0, "fgm2": 0, "fgm3": 0,
        "fta": 0, "ftm": 0, "orb": 0, "drb": 0,
        "ast": 0, "stl": 0, "blk": 0, "tov": 0,
    }
    for row in playerstats:
        # Per-player rows; skip the "Team" virtual player at the end.
        name = (row.findtext("PlayerName") or "").strip()
        if name.lower() == "team":
            continue
        totals["fga2"] += _safe_int(row.findtext("FieldGoalsAttempted2"))
        totals["fga3"] += _safe_int(row.findtext("FieldGoalsAttempted3"))
        totals["fgm2"] += _safe_int(row.findtext("FieldGoalsMade2"))
        totals["fgm3"] += _safe_int(row.findtext("FieldGoalsMade3"))
        totals["fta"]  += _safe_int(row.findtext("FreeThrowsAttempted"))
        totals["ftm"]  += _safe_int(row.findtext("FreeThrowsMade"))
        totals["orb"]  += _safe_int(row.findtext("OffensiveRebounds"))
        totals["drb"]  += _safe_int(row.findtext("DefensiveRebounds"))
        totals["ast"]  += _safe_int(row.findtext("Assistances"))
        totals["stl"]  += _safe_int(row.findtext("Steals"))
        totals["blk"]  += _safe_int(row.findtext("BlocksFavour"))
        totals["tov"]  += _safe_int(row.findtext("Turnovers"))
    totals["fga"] = totals["fga2"] + totals["fga3"]
    totals["fgm"] = totals["fgm2"] + totals["fgm3"]
    totals["tpa"] = totals["fga3"]
    totals["tpm"] = totals["fgm3"]
    return totals


def _compute_pace(stats: dict) -> float | None:
    fga = stats.get("fga")
    fta = stats.get("fta")
    orb = stats.get("orb")
    tov = stats.get("tov")
    if None in (fga, fta, orb, tov):
        return None
    return float(fga - orb + tov + 0.475 * fta)


def ingest_game(season_code: str, game_code: str | int) -> bool:
    """Pull one Euroleague game's box-score and upsert per-team stats."""
    url = f"{_BASE}/games?seasonCode={season_code}&gameCode={game_code}"
    root = _fetch(url)
    if root is None:
        return False
    # The framework's games table key is the gamecode (e.g. "E2024_1")
    game_id = f"{season_code}_{game_code}"
    if root.get("played") != "true":
        return False

    rows = []
    for tag, is_home in (("localclub", True), ("roadclub", False)):
        club = root.find(tag)
        if club is None:
            return False
        code = (club.get("code") or "").strip()
        if not code:
            return False
        team_id = _team_id_from_code(code)
        points = _safe_int(club.get("score"), default=None)
        ps = club.find("playerstats")
        stats = _sum_player_stats(ps)
        pace = _compute_pace(stats)
        rows.append({
            "team_id": team_id, "is_home": 1 if is_home else 0,
            "points": points, "stats": stats, "pace": pace,
        })

    # Cross-team DRtg
    if len(rows) == 2:
        rows[0]["ortg"] = (
            rows[0]["points"] / rows[0]["pace"] * 100.0
            if (rows[0]["points"] and rows[0]["pace"]) else None
        )
        rows[1]["ortg"] = (
            rows[1]["points"] / rows[1]["pace"] * 100.0
            if (rows[1]["points"] and rows[1]["pace"]) else None
        )
        rows[0]["drtg"] = (
            rows[1]["points"] / rows[0]["pace"] * 100.0
            if (rows[1]["points"] and rows[0]["pace"]) else None
        )
        rows[1]["drtg"] = (
            rows[0]["points"] / rows[1]["pace"] * 100.0
            if (rows[0]["points"] and rows[1]["pace"]) else None
        )

    conn = get_conn("euroleague")
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
                s.get("stl"), s.get("blk"), s.get("tov"),
                None,            # fouls — Euroleague doesn't break out
                None, None,      # fastbreak, paint not in feed
                row["pace"],
                row["pace"], row["ortg"], row["drtg"],
            ),
        )
    conn.commit()
    return True


def backfill(season_code: str, throttle_s: float = 0.4) -> dict:
    """Walk every finalized Euroleague game lacking a box-score row."""
    conn = get_conn("euroleague")
    rows = conn.execute(
        "SELECT g.game_id FROM games g "
        "WHERE g.status = 'final' "
        "  AND g.season = ? "
        "  AND NOT EXISTS (SELECT 1 FROM game_team_stats s "
        "                  WHERE s.game_id = g.game_id) "
        "ORDER BY g.date",
        (int(season_code.lstrip("Ee")),),
    ).fetchall()
    out = {"checked": len(rows), "ingested": 0, "skipped": 0}
    for r in rows:
        # game_id format: "E2024_1" → game_code 1
        gc = r["game_id"].split("_", 1)[-1]
        try:
            ok = ingest_game(season_code, gc)
        except Exception as e:
            logger.warning("[euroleague] game %s failed: %s", r["game_id"], e)
            ok = False
        if ok:
            out["ingested"] += 1
        else:
            out["skipped"] += 1
        time.sleep(throttle_s)
    logger.info("[euroleague] box-score backfill %s: %s", season_code, out)
    return out


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(prog="engine.basketball._euroleague_boxscore")
    ap.add_argument("--season", default=None)
    ap.add_argument("--throttle", type=float, default=0.4)
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    season = args.season
    if not season:
        from ._euroleague_ingest import _current_season_code
        season = _current_season_code()
    res = backfill(season, throttle_s=args.throttle)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
