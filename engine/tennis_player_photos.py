"""
Tennis player photo + flag resolver with persistent cache.

Why this exists
---------------
HR-supplemented matches and lower-tier (ITF / Challenger / WTA125)
players often don't appear in ESPN's tennis scoreboard payload, so
the in-row headshot fields stay null and the bet card falls back to
a generic silhouette. ESPN's *search* API does cover those players,
but hitting it on every API request would burn rate limit and
double-fetch the same players forever.

This module sits one layer up: takes a player name, returns the
resolved ``(espn_id, image_url, flag_url)`` triple, and caches the
result to a SQLite table so subsequent lookups are free. Negative
lookups (player not found) are also cached so we don't re-query
the same misses on every refresh.

Storage
-------
``tennis_player_photos``::

    name_key       TEXT PRIMARY KEY  -- normalized (accent-stripped, lower)
    name           TEXT NOT NULL     -- canonical display name
    espn_id        TEXT              -- numeric id from playercard link
    image_url      TEXT              -- absolute headshot href
    flag_url       TEXT              -- absolute country flag href
    not_found      INTEGER NOT NULL DEFAULT 0
    fetched_at     TEXT NOT NULL

CLI::

    python -m engine.tennis_player_photos --resolve "Daria Kasatkina"
    python -m engine.tennis_player_photos --backfill  # walk tennis_players
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


# ── HTTP ──────────────────────────────────────────────────────

_SEARCH_URL = "https://site.web.api.espn.com/apis/search/v2"
_HEADERS = {"User-Agent": "tennis-photo-resolver/1.0"}
_REQUEST_TIMEOUT = 8.0
_REQUEST_DELAY_S = 0.2  # gentle pacing to avoid burst rate limits


def _normalize(name: str) -> str:
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(ascii_only.lower().split())


# ── DB ────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS tennis_player_photos (
    name_key   TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    espn_id    TEXT,
    image_url  TEXT,
    flag_url   TEXT,
    not_found  INTEGER NOT NULL DEFAULT 0,
    fetched_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tennis_player_photos_espn_id
    ON tennis_player_photos(espn_id);
"""


def _ensure_table() -> None:
    from .tennis_db import get_conn
    conn = get_conn()
    conn.executescript(_DDL)
    conn.commit()


def _read_cache(name: str) -> dict | None:
    from .tennis_db import get_conn
    _ensure_table()
    key = _normalize(name)
    if not key:
        return None
    row = get_conn().execute(
        "SELECT * FROM tennis_player_photos WHERE name_key = ?", (key,)
    ).fetchone()
    return dict(row) if row else None


def _write_cache(name: str, *, espn_id: str | None, image_url: str | None,
                 flag_url: str | None, not_found: bool = False) -> None:
    from .tennis_db import get_conn
    _ensure_table()
    key = _normalize(name)
    if not key:
        return
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO tennis_player_photos
            (name_key, name, espn_id, image_url, flag_url, not_found, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name_key) DO UPDATE SET
            name = excluded.name,
            espn_id = COALESCE(excluded.espn_id, tennis_player_photos.espn_id),
            image_url = COALESCE(excluded.image_url, tennis_player_photos.image_url),
            flag_url = COALESCE(excluded.flag_url, tennis_player_photos.flag_url),
            not_found = excluded.not_found,
            fetched_at = excluded.fetched_at
        """,
        (key, name, espn_id, image_url, flag_url,
         1 if not_found else 0, now),
    )
    conn.commit()


# ── ESPN search ───────────────────────────────────────────────

_PLAYERCARD_ID_RE = re.compile(r"/player/_/id/(\d+)/")


def _search_espn(name: str) -> dict | None:
    """Hit ESPN's search v2 endpoint. Returns the FIRST tennis-typed
    player hit or None on miss / network failure. Does not cache —
    caller wraps in ``resolve_photo`` for caching."""
    if not name:
        return None
    qs = urllib.parse.urlencode({
        "region": "us", "lang": "en",
        "query": name, "limit": 5,
    })
    url = f"{_SEARCH_URL}?{qs}"
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        logger.debug("ESPN search failed for %r: %s", name, e)
        return None

    target = _normalize(name)
    target_tokens = frozenset(target.split())
    for rt in data.get("results", []) or []:
        if rt.get("type") != "player":
            continue
        for c in rt.get("contents", []) or []:
            if c.get("sport") != "tennis":
                continue
            display = c.get("displayName") or ""
            display_norm = _normalize(display)
            # Match strategies, in order of strictness:
            #   1. Exact normalized match
            #   2. Same set of name tokens (handles ESPN's last-name-first
            #      convention for Asian names: "Hanyu Guo" ≡ "Guo Hanyu")
            # Looser strategies than this would risk false positives
            # (e.g. picking the wrong Daria) so we stop here.
            if display_norm != target:
                if frozenset(display_norm.split()) != target_tokens:
                    continue
            link = (c.get("link") or {}).get("web") or ""
            espn_id = None
            m = _PLAYERCARD_ID_RE.search(link)
            if m:
                espn_id = m.group(1)
            image_url = (c.get("image") or {}).get("default")
            # When ESPN returns only an id (lower-tier players often
            # have no image URL on the search response itself), build
            # the URL from the deterministic CDN path. The frontend
            # handles 404 gracefully via the silhouette fallback —
            # better to try and miss than skip the lookup entirely.
            if not image_url and espn_id:
                image_url = f"https://a.espncdn.com/i/headshots/tennis/players/full/{espn_id}.png"
            return {
                "espn_id": espn_id,
                "image_url": image_url,
                "name": display,
                "league": c.get("defaultLeagueSlug"),
            }
    return None


# ── Public API ────────────────────────────────────────────────

def resolve_photo(name: str, *, force: bool = False) -> dict | None:
    """Return ``{espn_id, image_url, flag_url}`` for the player or
    None when ESPN doesn't list them. Cache hit by default; pass
    ``force=True`` to bypass cache (e.g. nightly backfill).

    Negative lookups are cached too — players that never resolve
    don't get re-queried on every API serve.
    """
    if not name or len(name.strip()) < 2:
        return None
    cached = _read_cache(name)
    if cached and not force:
        if cached.get("not_found"):
            return None
        return {
            "espn_id": cached.get("espn_id"),
            "image_url": cached.get("image_url"),
            "flag_url": cached.get("flag_url"),
        }
    hit = _search_espn(name)
    if not hit:
        # ESPN doesn't list lower-tier players (Challenger / ITF /
        # WTA125 most often miss). Tennis Explorer's profile page
        # ships a headshot for almost every player on those tours —
        # fall back when ESPN comes up dry. Lookup is by Sackmann
        # tour + name → te_id → photo URL; the te_id mapping lives
        # in tennis_match_results from the daily TE results scrape.
        te_url = _lookup_via_tennis_explorer(name)
        if te_url:
            _write_cache(name, espn_id=None, image_url=te_url,
                         flag_url=None)
            return {"espn_id": None, "image_url": te_url, "flag_url": None}
        _write_cache(name, espn_id=None, image_url=None, flag_url=None,
                     not_found=True)
        return None
    _write_cache(
        name,
        espn_id=hit.get("espn_id"),
        image_url=hit.get("image_url"),
        flag_url=None,  # search endpoint doesn't include flag
    )
    return {
        "espn_id": hit.get("espn_id"),
        "image_url": hit.get("image_url"),
        "flag_url": None,
    }


def _lookup_via_tennis_explorer(name: str) -> str | None:
    """Look up a player's te_id from a recent tennis_match_results
    row, then fetch their headshot URL from the Tennis Explorer
    profile page. Returns absolute URL or None."""
    try:
        from .tennis_db import get_conn
        from scrapers.tennis_results import fetch_photo_for
    except ImportError:
        return None
    nkey = _normalize(name)
    if not nkey:
        return None
    last = nkey.split()[-1]
    initial = nkey.split()[0][0] if nkey.split() else ""
    # tennis_match_results stores TE-format names ("Sinner J."). Match
    # by last-name prefix + first initial. (Two columns to scan: p1
    # and p2, since we don't know which side this player was on.)
    conn = get_conn()
    pat = f"{last.capitalize()} {initial.upper()}%"
    row = conn.execute(
        """SELECT p1_te_id, p2_te_id, p1_name, p2_name
           FROM tennis_match_results
           WHERE p1_name LIKE ? OR p2_name LIKE ?
           ORDER BY date DESC LIMIT 1""",
        (pat, pat),
    ).fetchone()
    if not row:
        return None
    row = dict(row)
    p1n = _normalize(row.get("p1_name") or "").split()
    if p1n and p1n[0].startswith(last):
        te_id = row.get("p1_te_id")
    else:
        te_id = row.get("p2_te_id")
    if not te_id:
        return None
    return fetch_photo_for(te_id)


def lookup_image(name: str) -> str | None:
    """Cache-only lookup. Returns the image URL or None when the
    cache has no record (does NOT trigger a network request).
    Used at API serve time so a missing photo doesn't slow the
    request — backfill populates the cache out-of-band."""
    cached = _read_cache(name)
    if cached and not cached.get("not_found"):
        return cached.get("image_url")
    return None


# ── Backfill ──────────────────────────────────────────────────

def backfill(*, only_missing: bool = True, max_n: int | None = None,
             delay_s: float = _REQUEST_DELAY_S) -> dict:
    """Walk every player on the tennis_scheduled_matches slate (so
    the resolved set is bounded by what we actually need to render)
    and resolve photos for any whose name isn't already cached.

    Returns ``{checked, hits, misses}``.
    """
    from .tennis_db import get_conn
    _ensure_table()
    conn = get_conn()

    rows = conn.execute(
        "SELECT DISTINCT p1_name AS name FROM tennis_scheduled_matches "
        "WHERE p1_name IS NOT NULL "
        "UNION "
        "SELECT DISTINCT p2_name AS name FROM tennis_scheduled_matches "
        "WHERE p2_name IS NOT NULL"
    ).fetchall()
    names = [r["name"] for r in rows]
    if max_n:
        names = names[: int(max_n)]

    checked = hits = misses = 0
    for name in names:
        if only_missing:
            cached = _read_cache(name)
            if cached:
                continue
        checked += 1
        result = resolve_photo(name)
        if result and result.get("image_url"):
            hits += 1
        else:
            misses += 1
        if delay_s > 0:
            time.sleep(delay_s)
    return {"checked": checked, "hits": hits, "misses": misses,
            "total_names": len(names)}


# ── CLI ───────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="engine.tennis_player_photos")
    ap.add_argument("--resolve", type=str,
                    help="Look up one player by name (uses cache).")
    ap.add_argument("--backfill", action="store_true",
                    help="Resolve every distinct player on the slate "
                         "whose name isn't already cached.")
    ap.add_argument("--max", type=int, default=None,
                    help="Cap the backfill at N players (testing).")
    args = ap.parse_args(argv)
    if args.resolve:
        out = resolve_photo(args.resolve)
        print(json.dumps(out, indent=2))
        return 0
    if args.backfill:
        summary = backfill(max_n=args.max)
        print(json.dumps(summary, indent=2))
        return 0
    ap.print_help()
    return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())


__all__ = ["resolve_photo", "lookup_image", "backfill"]
