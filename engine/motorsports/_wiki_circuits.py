"""Wikipedia track-image ingest for motorsports circuits.

Fetches each circuit's Wikipedia page summary and pulls the thumbnail
URL — Wikimedia hosts SVG track-layout diagrams for every F1 circuit
on the calendar, served as PNG thumbs at standardized sizes. Persists
``circuit_image_url`` per race so the frontend can render the track
map without a per-request Wikipedia call.

Ingest path (one HTTP per unique circuit, throttled at 0.3s):

  1. Read distinct ``circuit_wiki_url`` values from races
  2. For each: parse the page title from the URL, hit
     ``/api/rest_v1/page/summary/{title}``
  3. Persist the resulting ``thumbnail.source`` to every race that
     shares this circuit_wiki_url

Idempotent: races with a non-null ``circuit_image_url`` skip the
fetch entirely. Re-run cheaply after each calendar ingest.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from urllib.parse import quote, unquote

from ._db import get_conn

logger = logging.getLogger(__name__)

_USER_AGENT = "sports-model-bettor/1.0 (track-image ingest)"
_THROTTLE_S = 0.3


def ingest_track_images(series: str, force: bool = False) -> dict:
    """Fetch + persist circuit thumbnails for every race that has a
    Wikipedia URL but no image cached. Returns counts."""
    conn = get_conn(series)
    if force:
        rows = conn.execute(
            "SELECT DISTINCT circuit_wiki_url FROM races "
            "WHERE circuit_wiki_url IS NOT NULL"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT circuit_wiki_url FROM races "
            "WHERE circuit_wiki_url IS NOT NULL "
            "  AND (circuit_image_url IS NULL OR circuit_image_url = '')"
        ).fetchall()

    out = {"checked": 0, "updated": 0, "skipped": 0, "errors": 0}
    for r in rows:
        wiki_url = r["circuit_wiki_url"]
        out["checked"] += 1
        title = _wiki_title(wiki_url)
        if not title:
            out["errors"] += 1
            continue
        thumb = _fetch_thumbnail(title)
        if not thumb:
            out["errors"] += 1
            continue
        # Update every race that shares this wiki URL.
        cur = conn.execute("""
            UPDATE races
            SET circuit_image_url = ?
            WHERE circuit_wiki_url = ?
        """, (thumb, wiki_url))
        if cur.rowcount > 0:
            out["updated"] += cur.rowcount
        else:
            out["skipped"] += 1
        time.sleep(_THROTTLE_S)
    conn.commit()
    logger.info("[%s] track images: %s", series, out)
    return out


def _wiki_title(url: str) -> str | None:
    """Extract the page title from a Wikipedia URL.
    'https://en.wikipedia.org/wiki/Albert_Park_Circuit' → 'Albert_Park_Circuit'
    """
    if not url:
        return None
    if "/wiki/" not in url:
        return None
    title = url.rsplit("/wiki/", 1)[-1]
    # Strip URL fragments + decode percent-encoding (Ergast sometimes
    # ships URL-encoded titles for circuits with diacritics).
    title = title.split("#", 1)[0].split("?", 1)[0]
    return unquote(title)


def _fetch_thumbnail(title: str) -> str | None:
    """Hit Wikipedia's REST summary endpoint and return the best
    available image URL for ``title``. Prefers the larger original
    image when present (track-layout SVGs render crisper at full size)
    and falls back to the smaller thumbnail."""
    # Re-encode with safe='' so diacritics in circuit titles
    # ('Autódromo Hermanos Rodríguez') don't trip urllib's ASCII-only
    # request encoder.
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{quote(title, safe='')}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, TimeoutError) as e:
        logger.debug("wiki summary %s failed: %s", title, e)
        return None
    # Prefer original (full-resolution SVG-as-PNG) over the smaller
    # 320px thumbnail — the track diagrams are line art, scale cleanly,
    # and the originalimage is still typically <100KB.
    orig = (body.get("originalimage") or {}).get("source")
    thumb = (body.get("thumbnail") or {}).get("source")
    return orig or thumb
