"""SofaScore soccer closing-odds scraper.

Covers cup competitions and leagues that football-data.co.uk doesn't —
Copa Libertadores, UEFA Champions/Europa/Conference, US Open Cup,
NWSL, FIFA internationals, etc. The football-data scraper handles the
European Big-5 + USA/ARG/BRA via per-country CSVs; this one fills the
remaining gap so V3.1 market-blend can extend across every league.

Public API:
    fetch_season_events(unique_tournament_id, season_id, page=0)
        Page through a season's events; one page = ~30 events.

    fetch_event_odds(event_id) -> dict | None
        Pull the 1X2 + over/under 2.5 + AH closing odds for one event.

    backfill(league_key, *, seasons=None, throttle=1.0) -> dict
        Walk every event in the configured season list and write to
        ``data/soccer/{league_key}/historical_odds.db`` using the same
        schema as ``scrapers.football_data_soccer`` so the V3.1
        ``_market_join`` consumer is data-source agnostic.

Anti-bot: SofaScore sits behind Cloudflare, same JA3 fingerprinting as
HR. The shared ``hardrock_odds._graphql_post`` already learned to walk
a chain of curl_cffi impersonation profiles when the active one starts
returning 403; we replicate that here so this scraper self-heals as
Cloudflare rotates accepted fingerprints.

"Closing odds" caveat: SofaScore's odds endpoint returns one
consolidated set per event (their default aggregator's view), not a
specific bookmaker. After event finish the `fractionalValue` reflects
the last line on the board — close enough to a Pinnacle-class closing
line for the V3.1 market-blend feature (we vig-strip downstream so the
aggregator's overround doesn't bias the blend).
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path

try:
    from curl_cffi import requests as _cc_requests  # type: ignore
    _HAS_CURL_CFFI = True
except ImportError:
    _HAS_CURL_CFFI = False
    _cc_requests = None  # type: ignore

logger = logging.getLogger(__name__)


_BASE = "https://api.sofascore.com/api/v1"
_TIMEOUT_S = 15.0


# Per-league configuration. ``unique_tournament_id`` is SofaScore's id
# for the competition; ``seasons`` is the historical pool to backfill
# (most recent first). League keys match ``engine.soccer._config``.
_LEAGUE_SPECS: dict[str, dict] = {
    "conmebol_libertadores": {
        "unique_tournament_id": 384,
        # Verified via /unique-tournament/384/seasons on 2026-05-28.
        "seasons": [
            (87760, "2026"), (70083, "2025"),
            (57296, "2024"), (47974, "2023"),
        ],
    },
    "uefa_champions": {
        "unique_tournament_id": 7,
        "seasons": [
            (76953, "25/26"), (61644, "24/25"),
            (52162, "23/24"), (41897, "22/23"),
        ],
    },
    "uefa_europa": {
        "unique_tournament_id": 679,
        "seasons": [
            (76984, "25/26"), (61645, "24/25"),
            (53654, "23/24"), (44509, "22/23"),
        ],
    },
    "uefa_conference": {
        "unique_tournament_id": 17015,
        "seasons": [
            (76960, "25/26"), (61648, "24/25"),
            (52327, "23/24"), (42224, "22/23"),
        ],
    },
    "us_open_cup": {
        "unique_tournament_id": 495,
        "seasons": [
            (90046, "2026"), (71737, "2025"),
            (58896, "2024"), (48837, "2023"),
        ],
    },
    "us_nwsl": {
        "unique_tournament_id": 1690,
        "seasons": [
            (88711, "2026"), (71412, "2025"),
            (58145, "2024"), (48864, "2023"),
        ],
    },
    "usl_championship": {
        "unique_tournament_id": 13363,
        "seasons": [
            (87611, "2026"), (70263, "2025"),
            (57319, "2024"), (48258, "2023"),
        ],
    },
}


# curl_cffi probe order. Same self-healing pattern as
# scrapers/hardrock_odds.py — Cloudflare rotates accepted JA3
# fingerprints periodically, so we walk the chain and remember the
# winner. firefox133 last 200'd on 2026-05-26 then started 403ing the
# next day; chrome124 has been the most consistent across HR + Sofa.
_IMPERSONATE_PROFILES = (
    "chrome124", "chrome131", "chrome120", "safari17_0", "firefox133",
)
_active_profile: str | None = None


def _get_json(path: str) -> dict | None:
    """Fetch ``path`` and return parsed JSON. Walks the impersonation
    profile chain on 403; remembers the last working profile so the
    next call doesn't retry from scratch."""
    global _active_profile
    if not _HAS_CURL_CFFI:
        logger.warning("sofascore_soccer: curl_cffi missing — disabled")
        return None
    url = path if path.startswith("http") else f"{_BASE}{path}"
    order = ((_active_profile,) + tuple(p for p in _IMPERSONATE_PROFILES
                                          if p != _active_profile)
             if _active_profile else _IMPERSONATE_PROFILES)
    last_status = 0
    for profile in order:
        if profile is None:
            continue
        try:
            r = _cc_requests.get(url, impersonate=profile,
                                  timeout=_TIMEOUT_S)
        except Exception as e:
            logger.debug("sofa GET %s [%s] crashed: %s", url, profile, e)
            continue
        if r.status_code == 200:
            _active_profile = profile
            try:
                return r.json()
            except Exception as e:
                logger.debug("sofa parse %s: %s", url, e)
                return None
        last_status = r.status_code
    _active_profile = None
    logger.debug("sofa GET %s exhausted profiles, last status=%s",
                  url, last_status)
    return None


# ── Tournament probing ─────────────────────────────────────────

def search_unique_tournament(name: str) -> list[dict]:
    """Return SofaScore unique-tournament hits matching ``name``."""
    data = _get_json(f"/search/all?q={name.replace(' ', '%20')}")
    if not data:
        return []
    out = []
    for hit in (data.get("results") or []):
        if hit.get("type") != "uniqueTournament":
            continue
        e = hit.get("entity") or {}
        out.append({
            "id":          e.get("id"),
            "name":        e.get("name"),
            "category":    (e.get("category") or {}).get("name"),
            "slug":        e.get("slug"),
        })
    return out


def fetch_seasons(unique_tournament_id: int) -> list[dict]:
    """Return season list for a unique tournament. Most recent first."""
    data = _get_json(
        f"/unique-tournament/{unique_tournament_id}/seasons"
    )
    if not data:
        return []
    return [{"id": s.get("id"), "year": s.get("year"),
              "name": s.get("name")}
             for s in (data.get("seasons") or [])]


# ── Event walk ─────────────────────────────────────────────────

def fetch_season_events(unique_tournament_id: int, season_id: int,
                         page: int = 0,
                         completed: bool = True) -> list[dict]:
    """One page (~30 events) of completed-or-upcoming events for a
    season. ``completed=True`` walks finished events (oldest first);
    False walks upcoming events. Returns [] when the page is past the
    last available."""
    direction = "last" if completed else "next"
    data = _get_json(
        f"/unique-tournament/{unique_tournament_id}/season/"
        f"{season_id}/events/{direction}/{page}"
    )
    if not data:
        return []
    return data.get("events") or []


def fetch_event_odds(event_id: int) -> dict | None:
    """Pull the V3.1 market subset for one event: 1X2, OU 2.5, and the
    main AH line. Returns None when the event isn't priced."""
    data = _get_json(f"/event/{event_id}/odds/1/all")
    if not data:
        return None
    markets = data.get("markets") or []
    out: dict = {
        "psch": None, "pscd": None, "psca": None,
        "pc_over25": None, "pc_under25": None,
        "pcahh": None, "pcaha": None,
    }
    for m in markets:
        mid = m.get("marketId")
        choices = m.get("choices") or []
        if mid == 1 and m.get("marketPeriod") == "Full-time":
            # Full time 1X2.
            for c in choices:
                name = c.get("name")
                dec = _frac_to_decimal(c.get("fractionalValue"))
                if   name == "1": out["psch"] = dec
                elif name == "X": out["pscd"] = dec
                elif name == "2": out["psca"] = dec
        elif mid == 9 and m.get("choiceGroup") == "2.5":
            # Match goals at the 2.5 line.
            for c in choices:
                name = c.get("name")
                dec = _frac_to_decimal(c.get("fractionalValue"))
                if   name == "Over":  out["pc_over25"] = dec
                elif name == "Under": out["pc_under25"] = dec
        elif mid == 17:
            # Asian handicap — pick the line closest to zero (i.e. the
            # main line). Multiple AH lines come back; choiceGroup
            # carries the handicap value.
            ah = m.get("choiceGroup")
            try:
                ah_v = float(ah) if ah is not None else None
            except (TypeError, ValueError):
                ah_v = None
            if ah_v is None:
                continue
            current_main = out.get("_ah_line")
            if current_main is None or abs(ah_v) < abs(current_main):
                out["_ah_line"] = ah_v
                for c in choices:
                    name = c.get("name")
                    dec = _frac_to_decimal(c.get("fractionalValue"))
                    if   name == "1": out["pcahh"] = dec
                    elif name == "2": out["pcaha"] = dec
    out.pop("_ah_line", None)
    return out


def _frac_to_decimal(frac: str | None) -> float | None:
    """SofaScore fractional ("11/2") → decimal odds. Returns None for
    malformed inputs."""
    if not frac:
        return None
    try:
        num_str, den_str = frac.split("/", 1)
        num = float(num_str)
        den = float(den_str)
        if den == 0:
            return None
        return round(num / den + 1.0, 4)
    except (ValueError, ZeroDivisionError):
        return None


# ── Backfill ───────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS historical_odds (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    league        TEXT,
    season        TEXT,
    match_date    TEXT,
    home_name     TEXT,
    away_name     TEXT,
    home_abbr     TEXT,
    away_abbr     TEXT,
    psch          REAL,
    pscd          REAL,
    psca          REAL,
    pc_over25     REAL,
    pc_under25    REAL,
    pcahh         REAL,
    pcaha         REAL,
    fthg          INTEGER,
    ftag          INTEGER,
    fetched_at    TEXT,
    UNIQUE(league, season, match_date, home_name, away_name)
);
"""


def _open_db(league_key: str) -> sqlite3.Connection:
    """Open (and create-if-needed) the per-league historical_odds DB.
    Schema matches ``scrapers.football_data_soccer`` so the V3.1
    ``_market_join`` consumer treats both sources uniformly."""
    repo_root = Path(__file__).resolve().parent.parent
    db_dir = repo_root / "data" / "soccer" / league_key
    db_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_dir / "historical_odds.db"))
    conn.executescript(_SCHEMA)
    return conn


def _normalize_event(event: dict) -> dict | None:
    """Translate one SofaScore event into our row shape, scores
    included. Returns None when the event is unusable."""
    home = (event.get("homeTeam") or {}).get("name")
    away = (event.get("awayTeam") or {}).get("name")
    ts = event.get("startTimestamp")
    if not home or not away or not ts:
        return None
    match_date = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")
    status = (event.get("status") or {}).get("type") or ""
    fthg = (event.get("homeScore") or {}).get("normaltime")
    ftag = (event.get("awayScore") or {}).get("normaltime")
    return {
        "event_id":   event.get("id"),
        "match_date": match_date,
        "home_name":  home,
        "away_name":  away,
        "fthg":       fthg,
        "ftag":       ftag,
        "status":     status,
    }


def backfill(league_key: str, *,
             seasons: list[tuple] | None = None,
             throttle: float = 1.0,
             max_pages_per_season: int = 30,
             include_upcoming: bool = True) -> dict:
    """Walk every event for ``league_key``'s seasons and persist their
    closing odds. Idempotent on (league, season, date, home, away).
    Returns counts.

    ``throttle`` seconds between event-odds requests to avoid Sofa's
    Cloudflare rate-limit. 1.0s is comfortable; tune down once a run
    proves it.
    """
    spec = _LEAGUE_SPECS.get(league_key)
    if not spec:
        return {"error": f"no spec for {league_key!r}"}
    season_list = seasons or spec.get("seasons") or []
    if not season_list:
        return {"error": f"no seasons configured for {league_key!r}"}

    conn = _open_db(league_key)
    now = datetime.utcnow().isoformat(timespec="seconds")
    n_events = 0
    n_with_odds = 0
    n_skipped_dup = 0
    n_errors = 0

    for season_id, season_year in season_list:
        # Walk completed events oldest-first via pagination. Then walk
        # upcoming so the DB carries today's slate too.
        directions = [("completed", True)]
        if include_upcoming:
            directions.append(("upcoming", False))
        for label, completed in directions:
            for page in range(max_pages_per_season):
                events = fetch_season_events(
                    spec["unique_tournament_id"], season_id,
                    page=page, completed=completed,
                )
                if not events:
                    break
                logger.info("[sofa:%s] season %s %s page %d: %d events",
                             league_key, season_year, label, page,
                             len(events))
                for event in events:
                    n_events += 1
                    norm = _normalize_event(event)
                    if not norm:
                        continue
                    # Existing row?
                    existing = conn.execute(
                        "SELECT id FROM historical_odds "
                        "WHERE league=? AND season=? AND match_date=? "
                        "  AND home_name=? AND away_name=? LIMIT 1",
                        (league_key, season_year, norm["match_date"],
                         norm["home_name"], norm["away_name"]),
                    ).fetchone()
                    if existing:
                        n_skipped_dup += 1
                        continue
                    # Only finished events have meaningful closing odds.
                    odds: dict | None = None
                    if norm["status"] == "finished":
                        try:
                            odds = fetch_event_odds(norm["event_id"])
                        except Exception as e:
                            logger.debug("[sofa:%s] odds %s failed: %s",
                                          league_key, norm["event_id"], e)
                            n_errors += 1
                        time.sleep(throttle)
                    odds = odds or {}
                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO historical_odds "
                            "(league, season, match_date, home_name, "
                            " away_name, psch, pscd, psca, pc_over25, "
                            " pc_under25, pcahh, pcaha, fthg, ftag, "
                            " fetched_at) VALUES "
                            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                            " ?, ?)",
                            (league_key, season_year, norm["match_date"],
                             norm["home_name"], norm["away_name"],
                             odds.get("psch"), odds.get("pscd"),
                             odds.get("psca"), odds.get("pc_over25"),
                             odds.get("pc_under25"), odds.get("pcahh"),
                             odds.get("pcaha"),
                             norm["fthg"], norm["ftag"], now),
                        )
                        if odds.get("psch"):
                            n_with_odds += 1
                    except Exception as e:
                        logger.debug("[sofa:%s] insert failed for %s: %s",
                                      league_key, norm["event_id"], e)
                        n_errors += 1
                conn.commit()
    return {
        "league":         league_key,
        "events_seen":    n_events,
        "rows_with_odds": n_with_odds,
        "skipped_dup":    n_skipped_dup,
        "errors":         n_errors,
    }


# ── CLI ────────────────────────────────────────────────────────

def _cli() -> int:
    ap = argparse.ArgumentParser(prog="scrapers.sofascore_soccer")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_search = sub.add_parser("search",
                                help="find a unique tournament by name")
    p_search.add_argument("name")

    p_seasons = sub.add_parser("seasons",
                                 help="list seasons for a unique tournament")
    p_seasons.add_argument("unique_tournament_id", type=int)

    p_backfill = sub.add_parser("backfill",
                                  help="run backfill for a league key")
    p_backfill.add_argument("league_key")
    p_backfill.add_argument("--throttle", type=float, default=1.0)
    p_backfill.add_argument("--no-upcoming", action="store_true")

    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")

    if args.cmd == "search":
        for hit in search_unique_tournament(args.name):
            print(hit)
        return 0
    if args.cmd == "seasons":
        for s in fetch_seasons(args.unique_tournament_id):
            print(s)
        return 0
    if args.cmd == "backfill":
        res = backfill(args.league_key, throttle=args.throttle,
                        include_upcoming=not args.no_upcoming)
        import json as _json
        print(_json.dumps(res, indent=2))
        return 0
    return 1


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
