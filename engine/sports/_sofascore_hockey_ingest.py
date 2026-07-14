"""SofaScore hockey ingest — covers leagues theScore doesn't carry.

theScore only ships AHL/PWHL (plus a couple of niche feeds that return
empty). For Oceania leagues (AIHL, NZIHL) SofaScore is the only free
data source we've found that exposes full schedule + results.

Schema-compatible with the shared theScore schema (``teams`` /
``games`` tables created by ``_thescore_ingest.init_schema``) so every
downstream consumer — predictor, picks engine, tracker — keeps the
same row shape regardless of upstream.

Public:

    fetch_teams(tournament_id)
    fetch_events(tournament_id, *, status=None)
    backfill(league_slug, conn, tournament_id, *,
              teams_table='teams', games_table='games',
              status='final')
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from ._thescore_ingest import init_schema  # reuse the unified DDL

logger = logging.getLogger(__name__)


_API_BASE = "https://api.sofascore.com"
_HEADERS = {
    "Origin":  "https://www.sofascore.com",
    "Referer": "https://www.sofascore.com/",
    "Accept":  "application/json",
}


def _get_json(path: str, retries: int = 3) -> Any:
    """GET ``path`` via curl_cffi/Firefox impersonation. SofaScore
    blocks plain urllib + Python UA; the same JA3-impersonation hack
    the basketball SofaScore scraper uses works here. Returns parsed
    JSON or None on any failure."""
    try:
        from curl_cffi import requests as _cc
    except ImportError:
        logger.warning("SofaScore: curl_cffi not installed")
        return None
    url = f"{_API_BASE}{path}"
    for attempt in range(retries):
        try:
            r = _cc.get(url, headers=_HEADERS,
                         impersonate="firefox133", timeout=15)
            if r.status_code == 404:
                return None
            if r.status_code == 200:
                return r.json()
            logger.debug("SofaScore HTTP %s for %s", r.status_code, path)
        except Exception as e:
            logger.debug("SofaScore fetch failed (%d/%d): %s",
                          attempt + 1, retries, e)
        time.sleep(0.5 * (attempt + 1))
    return None


# ── Teams ───────────────────────────────────────────────────

def fetch_teams(tournament_id: int) -> list[dict]:
    """All teams in the tournament's current season. SofaScore returns
    them lazily under the standings endpoint; we walk seasons to get a
    union across the active season window."""
    season_id = _current_season_id(tournament_id)
    if season_id is None:
        return []
    data = _get_json(
        f"/api/v1/unique-tournament/{tournament_id}/season/"
        f"{season_id}/standings/total"
    )
    if not data:
        return []
    out: list[dict] = []
    for tbl in (data.get("standings") or []):
        for row in (tbl.get("rows") or []):
            team = row.get("team") or {}
            tid = team.get("id")
            if not tid:
                continue
            out.append({
                "id": int(tid),
                "full_name": team.get("name") or "",
                "short_name": team.get("shortName") or "",
                "abbreviation": (team.get("nameCode")
                                  or _abbr_from_name(team.get("name") or "")),
                "country": (team.get("country") or {}).get("name") or "",
                # Team logo lives at SofaScore's public image endpoint —
                # stable per-team-id URL, served as PNG. The frontend
                # references it directly so we don't have to mirror.
                "logo_url": f"https://api.sofascore.com/api/v1/team/{int(tid)}/image",
            })
    # Dedupe by id (a team can appear in multiple standings tables —
    # overall + home + away — when SofaScore split them).
    seen: set[int] = set()
    unique: list[dict] = []
    for t in out:
        if t["id"] in seen:
            continue
        seen.add(t["id"])
        unique.append(t)
    # Disambiguate abbreviation collisions (AIHL: Melbourne Ice +
    # Melbourne Mustangs both ship "MEL"; same for Sydney pair). Without
    # this, the HR odds resolver attributes the wrong matchup to one
    # of the two teams. Append the second-token initial to each
    # colliding entry's abbreviation in alphabetical order so it's
    # deterministic across re-ingests.
    by_abbr: dict[str, list[dict]] = {}
    for t in unique:
        by_abbr.setdefault(t["abbreviation"], []).append(t)
    for abbr, group in by_abbr.items():
        if len(group) <= 1:
            continue
        group.sort(key=lambda x: x["full_name"])
        for t in group:
            tokens = (t["full_name"] or "").split()
            # First word stays (e.g. Melbourne); pick a distinguishing
            # letter from the second-or-later word ("Ice" → I,
            # "Mustangs" → M).
            tail = ""
            for tok in tokens[1:]:
                if tok and tok[0].isalpha():
                    tail = tok[0].upper()
                    break
            if tail:
                t["abbreviation"] = (abbr + tail)[:4]
    return unique


def _abbr_from_name(name: str) -> str:
    """Cheap 3-4 char abbreviation when SofaScore doesn't ship one.
    Token initials, padded with first-word characters when too short."""
    if not name:
        return ""
    tokens = name.split()
    if len(tokens) >= 2:
        abbr = "".join(t[0] for t in tokens[:3]).upper()
        if len(abbr) >= 3:
            return abbr
    return name.replace(" ", "").upper()[:4]


# ── Events ───────────────────────────────────────────────

def _current_season_id(tournament_id: int) -> int | None:
    """Most recent season id for ``tournament_id``. SofaScore's
    seasons list comes ordered most-recent first."""
    data = _get_json(f"/api/v1/unique-tournament/{tournament_id}/seasons")
    if not data:
        return None
    seasons = data.get("seasons") or []
    if not seasons:
        return None
    return int(seasons[0].get("id"))


def list_seasons(tournament_id: int) -> list[dict]:
    """All known seasons for ``tournament_id`` — ordered most-recent
    first. Each entry has ``id``, ``name``, ``year``. Used by the
    backfill path to walk prior seasons for calibration sample."""
    data = _get_json(f"/api/v1/unique-tournament/{tournament_id}/seasons")
    if not data:
        return []
    out = []
    for s in data.get("seasons") or []:
        if not s.get("id"):
            continue
        out.append({
            "id": int(s["id"]),
            "name": s.get("name") or "",
            "year": s.get("year") or "",
        })
    return out


def fetch_events(tournament_id: int, *, status: str | None = None,
                  season_id: int | None = None) -> list[dict]:
    """All events (matches) for the current season. status='final'
    filters to finished games; None returns everything."""
    if season_id is None:
        season_id = _current_season_id(tournament_id)
    if season_id is None:
        return []
    out: list[dict] = []
    # SofaScore paginates by 'last' (finished) and 'next' (scheduled).
    # Pull both so a refresh covers backlog + upcoming. Each direction
    # paginates by 0-indexed page; 30 events/page is the default.
    for direction in ("last", "next"):
        page = 0
        while page < 50:   # hard cap = 1500 events per direction
            data = _get_json(
                f"/api/v1/unique-tournament/{tournament_id}/season/"
                f"{season_id}/events/{direction}/{page}"
            )
            if not data:
                break
            events = data.get("events") or []
            if not events:
                break
            for ev in events:
                norm = _normalize_event(ev)
                if not norm:
                    continue
                if status and norm.get("status") != status:
                    continue
                out.append(norm)
            if not data.get("hasNextPage"):
                break
            page += 1
            time.sleep(0.1)   # polite throttle
    return out


def _normalize_event(ev: dict) -> dict | None:
    home = ev.get("homeTeam") or {}
    away = ev.get("awayTeam") or {}
    home_id = home.get("id")
    away_id = away.get("id")
    if not (home_id and away_id):
        return None
    ts = ev.get("startTimestamp")
    if not ts:
        return None
    date_iso = datetime.fromtimestamp(int(ts), tz=timezone.utc) \
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    date_str = date_iso[:10]
    status_block = (ev.get("status") or {})
    status_type = (status_block.get("type") or "").lower()
    # SofaScore: 'finished' = final, 'inprogress' = live, 'notstarted'
    # = scheduled, 'postponed', 'canceled'.
    status = {
        "finished":   "final",
        "inprogress": "live",
        "notstarted": "pre_game",
        "postponed":  "postponed",
        "canceled":   "canceled",
    }.get(status_type, "pre_game")
    home_score = ((ev.get("homeScore") or {}).get("current"))
    away_score = ((ev.get("awayScore") or {}).get("current"))
    return {
        "id":            int(ev.get("id")),
        "date":          date_str,
        "start_time":    date_iso,
        "home_team_id":  int(home_id),
        "away_team_id":  int(away_id),
        "home_team":     home.get("name") or "",
        "away_team":     away.get("name") or "",
        "home_score":    int(home_score) if home_score is not None else None,
        "away_score":    int(away_score) if away_score is not None else None,
        "status":        status,
        "season":        ((ev.get("season") or {}).get("year")) or None,
        "has_overtime":  1 if "overtime" in (status_block.get("description") or "").lower() else 0,
        "has_shootout":  1 if "shootout" in (status_block.get("description") or "").lower() else 0,
    }


# ── Backfill ────────────────────────────────────────────────

def backfill(league_slug: str, conn, tournament_id: int, *,
              teams_table: str = "teams",
              games_table: str = "games",
              status: str | None = None,
              seasons: int | list[int] | None = None) -> dict:
    """Pull events into the league's DB. Idempotent (INSERT OR REPLACE).

    ``seasons``:
        * ``None`` (default) — current season only.
        * ``int N`` — N most recent seasons (e.g. 3 = current + 2 prior).
        * ``list[int]`` — explicit season-id list (use ``list_seasons``).

    Walking prior seasons gives the calibrator enough sample to fit
    home_boost + league_avg_gpg properly; one current-season's 30
    games leaves the predictor anchored on the global Poisson prior."""
    init_schema(conn, teams_table=teams_table, games_table=games_table)

    # Resolve which season IDs to walk.
    if isinstance(seasons, int):
        all_seasons = list_seasons(tournament_id)
        season_ids = [s["id"] for s in all_seasons[:seasons]]
    elif isinstance(seasons, list):
        season_ids = [int(s) for s in seasons]
    else:
        cur = _current_season_id(tournament_id)
        season_ids = [cur] if cur else []

    # Teams — pull from each season's standings so a team that joined
    # mid-history gets a row. Synth a stub for any team a game later
    # references that wasn't in standings (rare; cup-style scratches).
    seen_team_ids: set[int] = set()
    teams = fetch_teams(tournament_id)
    for t in teams:
        conn.execute(
            f"INSERT OR REPLACE INTO {teams_table} "
            f"(id, full_name, short_name, abbreviation, "
            f" division, conference, logo_url) "
            f"VALUES (?, ?, ?, ?, NULL, NULL, ?)",
            (t["id"], t["full_name"], t["short_name"],
             t["abbreviation"], t.get("logo_url")),
        )
        seen_team_ids.add(t["id"])

    n_games = 0
    for sid in season_ids:
        if sid is None:
            continue
        events = fetch_events(tournament_id, status=status, season_id=sid)
        for ev in events:
            for team_id_key, team_name_key in (
                ("home_team_id", "home_team"),
                ("away_team_id", "away_team"),
            ):
                tid = ev[team_id_key]
                if tid in seen_team_ids:
                    continue
                seen_team_ids.add(tid)
                conn.execute(
                    f"INSERT OR IGNORE INTO {teams_table} "
                    f"(id, full_name, short_name, abbreviation, "
                    f" division, conference, logo_url) "
                    f"VALUES (?, ?, ?, ?, NULL, NULL, ?)",
                    (tid, ev[team_name_key], ev[team_name_key],
                     _abbr_from_name(ev[team_name_key]),
                     f"https://api.sofascore.com/api/v1/team/{int(tid)}/image"),
                )
            conn.execute(
                f"INSERT OR REPLACE INTO {games_table} "
                f"(id, date, start_time, home_team_id, away_team_id, "
                f" home_score, away_score, status, season, "
                f" has_overtime, has_shootout) "
                f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ev["id"], ev["date"], ev["start_time"],
                 ev["home_team_id"], ev["away_team_id"],
                 ev["home_score"], ev["away_score"], ev["status"],
                 ev["season"], ev["has_overtime"], ev["has_shootout"]),
            )
            n_games += 1
    conn.commit()
    logger.info("[%s] sofascore backfill: %d teams, %d events "
                "(seasons walked: %d)",
                league_slug, len(seen_team_ids), n_games, len(season_ids))
    return {"teams": len(seen_team_ids), "events": n_games,
             "seasons": len(season_ids)}
