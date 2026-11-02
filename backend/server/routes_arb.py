"""Cross-book Arb scanner routes (display-only).

Serves the latest cross-book arbitrage snapshot produced on the Pi by
``~/pibot/scanners/arb/arb-feed.js`` (game ML/total/spread + deep team
totals / period totals / player props, HR <-> Kalshi <-> Vave). The Pi
scp's ``arb-latest.json`` into ``<repo>/data/`` every few minutes; this
endpoint just reads that file and hands it to the PickMint "Arb" tab.

READ-ONLY. No odds are fetched here and NO bets are ever placed — the tab
is a monitor. All the phantom guards (incl-OT-vs-regulation, period-prop-
vs-full-game scope, liquidity gate, scope-caveat exclusion) live in the Pi
scanners; by the time an arb reaches this file it is already vetted.

Added 2026-09-30 (Arb tab). Isolated: a single new GET, no shared state.

Player headshots (added 2026-09-30) piggyback on the SAME player-id
namespaces the anytime-TD/prop tabs already use (see ``headshot_for``
below + ``frontend/src/lib/playerPhoto.js``) — read-only lookups against
each sport's existing player-props DB, never a new ingest.
"""
from __future__ import annotations

import os
import re
import json
import logging
import sqlite3
import threading
import unicodedata
from datetime import datetime, timezone

from fastapi import APIRouter

logger = logging.getLogger(__name__)
router = APIRouter()

# <repo>/data/arb-latest.json — resolved from this file so it works no
# matter what CWD uvicorn is launched with. (backend/server/ -> backend/ -> repo)
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
ARB_JSON_PATH = os.getenv(
    "ARB_JSON_PATH",
    os.path.join(_REPO_ROOT, "data", "arb-latest.json"),
)

# updated_at older than this reads as stale (the Pi feed runs ~every 10 min).
STALE_SECONDS = int(os.getenv("ARB_STALE_SECONDS", "1200"))


# ---------------------------------------------------------------------------
# Player headshots for player-prop arbs.
#
# The feed gives a player NAME (parsed from a prop arb's ``line``, e.g.
# "jessica shepard 7.5"); the headshot CDNs need an ID. Every sport's
# player-props DB (``engine.player_props_db``, the same tables the anytime-
# TD/prop tabs already read for headshots) carries a
# ``player_game_logs(player_id, player_name)`` table where ``player_id`` is
# EXACTLY the id those CDNs expect:
#   MLB   -> MLBAM id   (ingested from MLB Stats API boxscores)
#   NFL/NBA/WNBA -> ESPN id (ingested from ESPN boxscores)
#   NHL   -> NHL-API id (assets.nhle.com) — but player_game_logs' name is
#            boxscore-abbreviated ("J. Staal"), so NHL resolves against
#            ``nhl_players(id, name)`` instead, which carries full names on
#            the SAME id namespace (verified: same id either table).
# URL patterns mirror frontend/src/lib/playerPhoto.js exactly so a card here
# looks identical to one on the anytime/prop tabs.
#
# Matched by normalized (accent/punctuation-insensitive) name and cached at
# module scope: the DBs are static enough that a live process should never
# re-query per request. Everything in this block is defensive — a lookup
# failure returns None, never raises; headshots are cosmetic and must not
# be able to break the arb feed.
# ---------------------------------------------------------------------------
_DATA_DIR = os.path.join(_REPO_ROOT, "data")
_HEADSHOT_DB_PATH = {
    "mlb": os.path.join(_DATA_DIR, "mlb.db"),
    "nfl": os.path.join(_DATA_DIR, "football", "nfl_player.db"),
    "nba": os.path.join(_DATA_DIR, "nba.db"),
    "wnba": os.path.join(_DATA_DIR, "basketball", "wnba.db"),
    "nhl": os.path.join(_DATA_DIR, "nhl.db"),
}

# sport -> {normalized_name: player_id_str}, built lazily once per sport.
_NAME_INDEX: dict[str, dict[str, str]] = {}
# (sport, normalized_name) -> url|None — the actual per-lookup cache.
_HEADSHOT_CACHE: dict[tuple[str, str], str | None] = {}
# Guards the OFF-THREAD index builds so a cold /api/arb never blocks ~15s on
# the big MLB/NHL GROUP BY (headshots are cosmetic — see _ensure_index_async).
_INDEX_BUILD_LOCK = threading.Lock()
_INDEX_BUILDING: set[str] = set()

_PROP_MARKET_RE = re.compile(r"^([a-z]+)\s+prop$", re.IGNORECASE)
_PROP_LINE_RE = re.compile(r"^(.+?)\s+([\d.]+)$")


def _normalize_player_name(name: str | None) -> str:
    """Lowercase/accent/punctuation-insensitive match key. Mirrors
    ``engine.sports.mlb.prop_picks._normalize_name`` (and its nfl/nba
    twins) so names line up with how those tables were populated."""
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    cleaned = re.sub(r"[^a-zA-Z\s]", " ", ascii_only).lower()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"\s+(jr|sr|ii|iii|iv)\b", "", cleaned)
    return cleaned


def _headshot_url(sport: str, player_id: str | None) -> str | None:
    """player_id -> CDN URL. Pattern-only; no I/O. Mirrors
    frontend/src/lib/playerPhoto.js::playerPhotoUrl."""
    if not player_id:
        return None
    pid = str(player_id).strip()
    if not pid:
        return None
    if sport == "nba":
        return f"https://a.espncdn.com/i/headshots/nba/players/full/{pid}.png"
    if sport == "wnba":
        return f"https://a.espncdn.com/i/headshots/wnba/players/full/{pid}.png"
    if sport == "nfl":
        return f"https://a.espncdn.com/i/headshots/nfl/players/full/{pid}.png"
    if sport == "mlb":
        return ("https://img.mlbstatic.com/mlb-photos/image/upload/"
                 "d_people:generic:headshot:silo:current.png/w_480,q_auto:best/"
                 f"v1/people/{pid}/headshot/silo/current")
    if sport == "nhl":
        return f"https://assets.nhle.com/mugs/nhl/latest/{pid}.png"
    return None


def _ro_connect(path: str) -> sqlite3.Connection:
    # uri=True + mode=ro: never blocks on / contends with the app's own
    # writer connections to these DBs (WAL-mode, but no reason to risk it).
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)


def _build_name_index(sport: str) -> dict[str, str]:
    """player_name -> player_id for one sport, read once from its
    player-props DB. Returns {} on any failure (missing DB/table/etc) —
    partial or zero coverage for a sport is fine, never fatal."""
    index: dict[str, str] = {}
    try:
        if sport == "nhl":
            # player_game_logs.player_name is boxscore-abbreviated ("J.
            # Staal"); nhl_players carries full names on the same id
            # namespace, which is what assets.nhle.com expects anyway.
            path = _HEADSHOT_DB_PATH["nhl"]
            if not os.path.exists(path):
                return index
            conn = _ro_connect(path)
            try:
                rows = conn.execute("SELECT id, name FROM nhl_players").fetchall()
            finally:
                conn.close()
            for pid, name in rows:
                norm = _normalize_player_name(name)
                if norm:
                    index[norm] = str(pid)
            return index

        path = _HEADSHOT_DB_PATH.get(sport)
        if not path or not os.path.exists(path):
            return index
        conn = _ro_connect(path)
        try:
            # ORDER BY last_date ASC so the dict-overwrite below leaves the
            # most-recently-active player winning a normalized-name
            # collision — mirrors engine.sports.nba.prop_picks._build_name_index.
            rows = conn.execute(
                "SELECT player_id, player_name, MAX(date) AS last_date "
                "FROM player_game_logs GROUP BY player_id ORDER BY last_date"
            ).fetchall()
        finally:
            conn.close()
        for pid, name, _last in rows:
            norm = _normalize_player_name(name)
            if norm:
                index[norm] = str(pid)
        return index
    except Exception as exc:
        logger.warning("headshot_for: failed to index %s: %s", sport, exc)
        return index


def _ensure_index_async(sport_key: str) -> None:
    """Build a sport's name index in the BACKGROUND, once. Returns immediately
    so a cold request never waits on the ~15s MLB/NHL GROUP BY — headshots for
    that sport are simply absent until the build lands and caches."""
    with _INDEX_BUILD_LOCK:
        if sport_key in _NAME_INDEX or sport_key in _INDEX_BUILDING:
            return
        _INDEX_BUILDING.add(sport_key)

    def _run() -> None:
        try:
            _NAME_INDEX[sport_key] = _build_name_index(sport_key)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("headshot index build failed for %s: %s", sport_key, exc)
            _NAME_INDEX[sport_key] = {}  # cache empty so we don't rebuild-spin
        finally:
            with _INDEX_BUILD_LOCK:
                _INDEX_BUILDING.discard(sport_key)

    threading.Thread(target=_run, name=f"headshot-index-{sport_key}", daemon=True).start()


def headshot_for(sport: str | None, player_name: str | None) -> str | None:
    """player NAME (as parsed from a prop arb's ``line``) -> headshot URL,
    or None if unresolvable. Never raises. Resolutions (incl. misses) are
    cached at module scope for the life of the process."""
    try:
        sport_key = (sport or "").lower()
        norm = _normalize_player_name(player_name)
        if not sport_key or not norm:
            return None
        cache_key = (sport_key, norm)
        if cache_key in _HEADSHOT_CACHE:
            return _HEADSHOT_CACHE[cache_key]
        index = _NAME_INDEX.get(sport_key)
        if index is None:
            # Cold index: build OFF-THREAD and skip the headshot this request —
            # a cosmetic image must never make /api/arb wait ~15s on the big
            # MLB/NHL GROUP BY. It resolves on a later poll once cached.
            _ensure_index_async(sport_key)
            return None
        url = _headshot_url(sport_key, index.get(norm))
        _HEADSHOT_CACHE[cache_key] = url
        return url
    except Exception as exc:
        logger.warning("headshot_for(%r, %r) failed: %s", sport, player_name, exc)
        return None


# Warm every sport's name index at import (module-level — @app.on_event startup
# hooks don't fire reliably here) so headshots are ready without the first
# /api/arb paying the cold-build cost. ONE background thread, SEQUENTIAL and
# smallest-DB-first: concurrent GROUP BYs over the big MLB/NHL prop DBs saturate
# disk/CPU and brown the whole backend out for ~20-30s on every restart. Serial
# keeps startup gentle; requests stay fast throughout (a not-yet-warmed sport
# just yields no headshot until its turn lands). Marks _INDEX_BUILDING so an
# on-demand _ensure_index_async never double-builds the same sport.
def _warm_indices_sequential() -> None:
    for sp in ("nfl", "wnba", "nba", "nhl", "mlb"):  # ascending DB size
        with _INDEX_BUILD_LOCK:
            if sp in _NAME_INDEX or sp in _INDEX_BUILDING:
                continue
            _INDEX_BUILDING.add(sp)
        try:
            _NAME_INDEX[sp] = _build_name_index(sp)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("headshot warm failed for %s: %s", sp, exc)
            _NAME_INDEX[sp] = {}
        finally:
            with _INDEX_BUILD_LOCK:
                _INDEX_BUILDING.discard(sp)


threading.Thread(target=_warm_indices_sequential, name="headshot-warm", daemon=True).start()


def _prop_player_name(market: str | None, line: str | None) -> str | None:
    """market/line -> player name, for prop arbs only. Mirrors
    ArbPanel.jsx's propMeta() parsing exactly so the player resolved here
    is always the same one the card displays. None for non-prop markets
    (game/team-total arbs carry no player)."""
    if not _PROP_MARKET_RE.match(market or ""):
        return None
    m = _PROP_LINE_RE.match(line or "")
    name = m.group(1).strip() if m else (line or "").strip()
    return name or None


def _enrich_prop_headshots(data: dict) -> None:
    """Walk sports[].fillable[] and attach ``player_img`` to player-prop
    arbs in place. Additive only — never touches existing keys, and a
    failure here must never take down the whole /api/arb response."""
    try:
        for sport_block in data.get("sports") or []:
            if not isinstance(sport_block, dict):
                continue
            sport = sport_block.get("sport")
            for arb in sport_block.get("fillable") or []:
                if not isinstance(arb, dict):
                    continue
                player = _prop_player_name(arb.get("market"), arb.get("line"))
                if player:
                    arb["player_img"] = headshot_for(sport, player)
    except Exception as exc:
        logger.warning("_enrich_prop_headshots failed: %s", exc)


def _empty_payload(note: str) -> dict:
    """A valid, empty contract payload so the tab always renders."""
    return {
        "updated_at": None,
        "fillable_total": 0,
        "sports": [],
        "note": note,
        "stale": True,
        "age_seconds": None,
    }


def _age_seconds(updated_at: str | None) -> float | None:
    if not updated_at:
        return None
    try:
        ts = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - ts).total_seconds()
    except Exception:
        return None


@router.get("/api/arb")
def api_arb() -> dict:
    """Latest cross-book arb snapshot for the Arb tab.

    Returns the Pi feed's DATA CONTRACT verbatim plus a ``stale`` flag
    (True when ``updated_at`` is older than STALE_SECONDS) and
    ``age_seconds``. Missing/unreadable file -> graceful empty payload.
    """
    if not os.path.exists(ARB_JSON_PATH):
        return _empty_payload("Arb feed not available yet — no snapshot file on the backend.")
    try:
        with open(ARB_JSON_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception as exc:  # corrupt/partial write, permissions, etc.
        logger.warning("api_arb: failed to read %s: %s", ARB_JSON_PATH, exc)
        return _empty_payload(f"Arb feed unreadable: {exc}")

    if not isinstance(data, dict):
        return _empty_payload("Arb feed malformed (expected an object).")

    # Normalize to the contract shape defensively so the tab never crashes.
    data.setdefault("updated_at", None)
    data.setdefault("fillable_total", 0)
    data.setdefault("sports", [])
    data.setdefault("note", "")

    age = _age_seconds(data.get("updated_at"))
    data["age_seconds"] = round(age) if age is not None else None
    data["stale"] = (age is None) or (age > STALE_SECONDS)

    _enrich_prop_headshots(data)
    return data


# ---------------------------------------------------------------------------
# Arb tracker (Austin 2026-10-27: "seeing how much I'd be missing out on would help").
# The Pi's arb-feed logs every fillable arb once (first seen, at its first price) and refreshes
# it each 15-min scan it survives; it ships ``arb-tracker.json`` next to the snapshot. A one-scan
# arb is often gone before both legs could be placed, so the "lasted 2+ scans" figures are the
# honest ones. Profit is what the arb locked on a $100 total stake at its first-seen price.
# ---------------------------------------------------------------------------
ARB_TRACKER_PATH = os.getenv(
    "ARB_TRACKER_PATH", os.path.join(_REPO_ROOT, "data", "arb-tracker.json"))
_SCAN_MINUTES = 15


def _arb_profit_100(ci) -> float:
    try:
        ci = float(ci)
    except (TypeError, ValueError):
        return 0.0
    return 100.0 * (1.0 - ci) / ci if ci > 0 else 0.0


@router.get("/api/arb/tracker")
def api_arb_tracker(limit: int = 300) -> dict:
    """Tracker summary (today / 7d / 30d, all vs persisted), per-sport and per-book-pair
    rollups, and the newest ``limit`` logged arbs."""
    try:
        with open(ARB_TRACKER_PATH, "r", encoding="utf-8") as fh:
            t = json.load(fh)
    except FileNotFoundError:
        return {"since": None, "windows": [], "by_sport": [], "by_books": [], "rows": []}
    except Exception as exc:
        logger.warning("api_arb_tracker: failed to read %s: %s", ARB_TRACKER_PATH, exc)
        return {"since": None, "windows": [], "by_sport": [], "by_books": [], "rows": [],
                "error": str(exc)}

    from zoneinfo import ZoneInfo
    et = ZoneInfo("America/New_York")
    now = datetime.now(timezone.utc)
    rows = []
    for r in (t.get("arbs") or {}).values():
        try:
            first = datetime.fromisoformat(str(r["first_seen"]).replace("Z", "+00:00"))
            last = datetime.fromisoformat(str(r.get("last_seen") or r["first_seen"]).replace("Z", "+00:00"))
        except Exception:
            continue
        scans = int(r.get("scans") or 1)
        rows.append({
            **r,
            "profit_100": round(_arb_profit_100(r.get("combined_implied")), 2),
            "lasted_min": round((last - first).total_seconds() / 60.0),
            "persisted": scans >= 2,
            "live": (now - last).total_seconds() < 2 * _SCAN_MINUTES * 60,
            "pair": "/".join(sorted(str(r.get("books") or "?").split("/"))),
            "_first": first,
        })
    rows.sort(key=lambda x: x["_first"], reverse=True)

    today_et = now.astimezone(et).date()

    def _window(label, pred):
        sel = [x for x in rows if pred(x)]
        per = [x for x in sel if x["persisted"]]
        return {"label": label, "n": len(sel), "profit_100": round(sum(x["profit_100"] for x in sel), 2),
                "persisted": len(per), "persisted_profit_100": round(sum(x["profit_100"] for x in per), 2)}

    windows = [
        _window("Today", lambda x: x["_first"].astimezone(et).date() == today_et),
        _window("7 days", lambda x: (now - x["_first"]).days < 7),
        _window("30 days", lambda x: True),
    ]

    def _rollup(key):
        agg: dict = {}
        for x in rows:
            a = agg.setdefault(x.get(key) or "?", {"key": x.get(key) or "?", "n": 0, "persisted": 0,
                                                   "profit_100": 0.0, "persisted_profit_100": 0.0})
            a["n"] += 1
            a["profit_100"] += x["profit_100"]
            if x["persisted"]:
                a["persisted"] += 1
                a["persisted_profit_100"] += x["profit_100"]
        out = sorted(agg.values(), key=lambda a: a["persisted_profit_100"], reverse=True)
        for a in out:
            a["profit_100"] = round(a["profit_100"], 2)
            a["persisted_profit_100"] = round(a["persisted_profit_100"], 2)
        return out

    by_sport, by_books = _rollup("sport"), _rollup("pair")
    for x in rows:
        x.pop("_first", None)
    return {"since": t.get("since"), "scan_minutes": _SCAN_MINUTES, "windows": windows,
            "by_sport": by_sport, "by_books": by_books[:12], "rows": rows[:max(1, min(limit, 1000))]}
