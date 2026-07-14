"""
ESPN tennis scoreboard fetcher.

Sackmann gives us the historical training corpus and per-player
ratings, but Sackmann doesn't publish the day's draw — for "what
matches are happening today" we hit ESPN's tennis API:

    https://site.api.espn.com/apis/site/v2/sports/tennis/{tour}/scoreboard
        ?dates=YYYYMMDD

ESPN groups the response by tournament. Each tournament has up to
several hundred competitions (matches across all rounds — completed,
in-progress, and scheduled). We filter by date to surface only what's
playing today.

Output shape mirrors what the team-sport scoreboards (NBA / NHL)
produce so the picker / frontend can consume tennis matches the same
way:

    {
        "tour": "atp",
        "match_id":     "ESPN competition id",
        "tournament":   "Mutua Madrid Open",
        "tournament_id": str,
        "surface":      str | None,
        "date":         "YYYY-MM-DDTHH:MMZ",
        "best_of":      3 | 5 | None,
        "round":        str | None,
        "status":       "pre" | "in" | "post",
        "p1_name":      "Carlos Alcaraz",
        "p1_country":   "ESP",
        "p2_name":      "Jannik Sinner",
        "p2_country":   "ITA",
        "score":        str | None,
        "winner":       "p1" | "p2" | None,
    }

Player names use ``athlete.fullName`` because Sackmann ships
"First Last" as the player-table ``name`` field — exact-match works
for the vast majority of names. The fuzzy resolver in
``engine.tennis_db`` handles edge cases (extended surnames,
accent normalization).
"""

from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


_BASE = "https://site.api.espn.com/apis/site/v2/sports/tennis"
_HEADSHOT_BASE = "https://a.espncdn.com/i/headshots/tennis/players/full"
_PLAYERCARD_ID_RE = re.compile(r"/player/_/id/(\d+)/")


def _build_headshot_url(athlete: dict | None) -> str | None:
    """Construct an ESPN headshot URL from the athlete's playercard
    link. ESPN doesn't ship a headshot href on the scoreboard payload,
    but it does ship a profile link of the form
    ``.../tennis/player/_/id/{NUMERIC_ID}/{slug}``. The CDN serves
    the player image at ``{_HEADSHOT_BASE}/{NUMERIC_ID}.png`` so a
    deterministic build avoids a per-player follow-up request.
    Returns None when no playercard id is found."""
    if not athlete:
        return None
    for link in athlete.get("links") or []:
        rel = link.get("rel") or []
        if "athlete" not in rel and "playercard" not in rel:
            continue
        m = _PLAYERCARD_ID_RE.search(link.get("href") or "")
        if m:
            return f"{_HEADSHOT_BASE}/{m.group(1)}.png"
    return None


def _fetch_scoreboard(tour: str, date: str | None = None) -> dict | None:
    """Single-shot scoreboard fetch. ``date`` is YYYY-MM-DD; converted
    to ESPN's YYYYMMDD format. None = today per ESPN."""
    if tour not in ("atp", "wta"):
        raise ValueError(f"unknown tour: {tour!r}")
    url = f"{_BASE}/{tour}/scoreboard"
    if date:
        try:
            d = datetime.strptime(date, "%Y-%m-%d")
            url += f"?dates={d.strftime('%Y%m%d')}"
        except ValueError:
            pass
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "tennis-fetcher/1.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.warning("ESPN tennis %s scoreboard fetch failed: %s", tour, e)
        return None


def _state_from_status(status: dict) -> str:
    if not status:
        return "pre"
    t = status.get("type") or {}
    s = (t.get("state") or "").lower()
    if s in ("pre", "in", "post"):
        return s
    if t.get("completed"):
        return "post"
    return "pre"


def _surface_for(event: dict, comp: dict) -> str | None:
    """ESPN doesn't expose surface on tennis events. Delegate to the
    shared name → surface inferencer used by all schedule sources
    (`engine.tennis_surface.infer_surface`) so the ESPN, Tennis
    Explorer, and HR-stub paths all assign the same surface to the
    same tournament. Default-Hard fallback inside the inferencer is
    intentional — most unknown / sub-tour events are indoor hard."""
    from engine.tennis_surface import infer_surface
    return infer_surface(event.get("name"))


def _winner_side(competitors: list) -> str | None:
    for c in competitors or []:
        if c.get("winner"):
            order = c.get("order")
            if order in (1, "1"):
                return "p1"
            if order in (2, "2"):
                return "p2"
    return None


def _format_score(competitors: list) -> str | None:
    """Stitch per-set linescores into 'X-Y X-Y X-Y' format.

    ESPN ships `linescores` as a list of {value: score} per
    competitor. p1's set1 + p2's set1 give the first set token."""
    if not competitors or len(competitors) != 2:
        return None
    a = (competitors[0].get("linescores") or [])
    b = (competitors[1].get("linescores") or [])
    if not a or not b:
        return None
    sets = []
    for sa, sb in zip(a, b):
        try:
            v1 = int(sa.get("value") or 0)
            v2 = int(sb.get("value") or 0)
        except (TypeError, ValueError):
            continue
        sets.append(f"{v1}-{v2}")
    return " ".join(sets) if sets else None


def _normalize_match(tour: str, event: dict, comp: dict) -> dict | None:
    """Map ESPN's competition dict to our flat match shape."""
    competitors = comp.get("competitors") or []
    if len(competitors) != 2:
        return None
    # ESPN sorts competitors by order (1 or 2). Force that order so
    # p1/p2 are stable — same convention Sackmann uses for winner_id /
    # loser_id when we ingest later from the historical CSVs.
    competitors_sorted = sorted(
        competitors, key=lambda c: int(c.get("order") or 99))
    a1 = competitors_sorted[0].get("athlete") or {}
    a2 = competitors_sorted[1].get("athlete") or {}
    p1_name = a1.get("fullName") or a1.get("displayName")
    p2_name = a2.get("fullName") or a2.get("displayName")
    if not p1_name or not p2_name:
        return None
    p1_country = ((a1.get("flag") or {}).get("alt"))
    p2_country = ((a2.get("flag") or {}).get("alt"))
    # Headshot URLs for the bet-card avatars (mirrors team-sport
    # team logos). ESPN's scoreboard payload doesn't include the
    # headshot href directly but the playercard link carries the
    # athlete id, and the CDN serves headshots at a stable path:
    #   https://a.espncdn.com/i/headshots/tennis/players/full/{id}.png
    # Extract id from the link, build the URL. Falls back to flag
    # only when no playercard id is present.
    p1_image = ((a1.get("headshot") or {}).get("href")
                or _build_headshot_url(a1))
    p2_image = ((a2.get("headshot") or {}).get("href")
                or _build_headshot_url(a2))
    p1_flag = (a1.get("flag") or {}).get("href")
    p2_flag = (a2.get("flag") or {}).get("href")
    state = _state_from_status(comp.get("status") or {})
    return {
        "tour": tour,
        "match_id": str(comp.get("id") or ""),
        "tournament": event.get("name"),
        "tournament_id": str(event.get("id") or ""),
        "surface": _surface_for(event, comp),
        "date": comp.get("date"),
        "best_of": _safe_best_of(comp),
        "round": (comp.get("notes") or [{}])[0].get("headline") if comp.get("notes") else None,
        "status": state,
        "p1_name": p1_name,
        "p1_country": p1_country,
        "p1_image": p1_image,
        "p1_flag": p1_flag,
        "p2_name": p2_name,
        "p2_country": p2_country,
        "p2_image": p2_image,
        "p2_flag": p2_flag,
        "score": _format_score(competitors_sorted),
        "winner": _winner_side(competitors_sorted),
    }


def _safe_best_of(comp: dict) -> int | None:
    """Pull best_of from ESPN's `format` block when available; otherwise
    None. Slams main draw is BO5 men / BO3 women; everything else BO3.
    Caller can fill defaults from tour + tournament context."""
    fmt = comp.get("format") or {}
    bo = fmt.get("regulation", {}).get("periods")
    if isinstance(bo, int) and bo in (3, 5):
        return bo
    return None


def fetch_today(tour: str, date: str | None = None) -> list[dict]:
    """Return all matches scheduled / in-progress / completed on
    ``date`` (default today) for ``tour``.

    ESPN's scoreboard returns the FULL tournament; we filter to the
    requested date. The returned list is unordered — caller can sort
    by ``date`` for chronological display."""
    target = date or datetime.now().strftime("%Y-%m-%d")
    payload = _fetch_scoreboard(tour, date=target)
    if not payload:
        return []
    out: list[dict] = []
    for event in (payload.get("events") or []):
        for grouping in (event.get("groupings") or []):
            for comp in (grouping.get("competitions") or []):
                comp_date = (comp.get("date") or "")[:10]
                if comp_date != target:
                    continue
                m = _normalize_match(tour, event, comp)
                if m is not None:
                    out.append(m)
    return out


def fetch_today_all_tours(date: str | None = None) -> list[dict]:
    """ATP + WTA combined."""
    out: list[dict] = []
    out.extend(fetch_today("atp", date=date))
    out.extend(fetch_today("wta", date=date))
    return out


__all__ = ["fetch_today", "fetch_today_all_tours"]
