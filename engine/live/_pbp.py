"""
ESPN play-by-play fetcher + normalizer.

Phase 5b foundation. The scoreboard fetcher in _state.py gives us
score/period/clock — enough for the analytical predictor — but not
enough for intermission-window picks: we need to know WHO scored,
WHO got fouled into trouble, and WHICH lineup was on the floor when
the period ended.

ESPN's summary endpoint exposes the full plays array per game::

    GET /apis/site/v2/sports/{path}/summary?event={game_id}

Each play carries id + period + clock + home/away score + team +
type.text + text + participants + scoringPlay. We normalize that into
a flat dict per play and store append-only in live_pbp (PK
(sport, game_id, play_id)) so re-fetches are idempotent — ESPN
re-emits the full plays array on every summary call, so we filter
to "plays we haven't seen" before persisting.

Coverage (NBA):
- Made / missed shots with shooter
- Assists, rebounds, blocks, steals
- Personal fouls + technical fouls (by player)
- Substitutions (player_in / player_out)
- Timeouts
- End-of-period markers

Coverage (NHL):
- Goals + assists + strength (EV/PP/SH)
- Shots on goal, missed shots
- Penalties (player + duration)
- Faceoffs
- Period start / end markers

Cadence: every 30s per active game in the live_worker. Lighter than
state polling (which is 5s/10s) because plays accumulate steadily —
we don't need sub-5s freshness on a play that already happened.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"
_SPORT_PATH = {
    "nba":   "basketball/nba",
    "nhl":   "hockey/nhl",
    # WNBA shares the same ESPN summary shape — same site.api host,
    # same play/period schema. Adding here unlocks historical_pbp
    # backfill for the basketball framework's WNBA league.
    "wnba":  "basketball/wnba",
    # NCAAM (Men's College Basketball) — same ESPN summary path,
    # 2-period (20-min halves) game structure. Periods 1+2 in PBP.
    "ncaam": "basketball/mens-college-basketball",
    # AFL — Aussie Rules. Same ESPN summary shape (period, sequence,
    # home/awayScore, type). 4 × 20-min quarters, ~50 plays/game
    # since ESPN only tracks goals/behinds/period-end (not every
    # disposal). Much coarser PBP than basketball but enough for
    # state-end snapshots.
    "afl":   "australian-football/afl",
}


def _fetch_summary(sport: str, game_id: str) -> dict | None:
    """Single-shot summary fetch for one game. Returns the raw ESPN
    payload or None on failure — caller decides whether to retry."""
    path = _SPORT_PATH.get(sport)
    if not path:
        raise ValueError(f"unknown sport {sport!r}")
    url = f"{_ESPN_BASE}/{path}/summary?event={game_id}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "live_worker/1.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.debug("ESPN %s summary fetch failed for %s: %s",
                     sport, game_id, e)
        return None


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _parse_clock_secs(clock: dict | None) -> int:
    """Pull seconds remaining in the current period out of ESPN's
    clock object. ``value`` is preferred (already numeric); fall back
    to parsing displayValue ('8:42') when missing."""
    if not clock:
        return 0
    v = clock.get("value")
    if isinstance(v, (int, float)):
        return int(v)
    disp = clock.get("displayValue") or ""
    if ":" in disp:
        parts = disp.split(":")
        try:
            return int(parts[0]) * 60 + int(parts[1])
        except (TypeError, ValueError):
            return 0
    try:
        return int(float(disp))
    except (TypeError, ValueError):
        return 0


def _participants(play: dict) -> list[dict]:
    """Flatten ESPN's participants array. Each entry is
    {athlete_id, name, role}. ``role`` is what the play is tagging the
    player as ('shooter', 'assister', 'rebounder', 'fouler',
    'recipient', 'subIn', 'subOut'). ESPN's role labels vary across
    sports so we keep them as-is for downstream consumers (5c) to map.
    """
    out: list[dict] = []
    for p in (play.get("participants") or []):
        athlete = p.get("athlete") or {}
        out.append({
            "athlete_id": athlete.get("id"),
            "name": athlete.get("displayName") or athlete.get("shortName"),
            "role": p.get("type") or p.get("role"),
        })
    return out


def _normalize_play(sport: str, game_id: str, play: dict) -> dict | None:
    """Map an ESPN play dict into our flat schema. Returns None on
    plays missing the basics (no id or no period); we don't store
    dataless rows."""
    if not isinstance(play, dict):
        return None
    play_id = play.get("id")
    if play_id is None:
        return None
    period_obj = play.get("period") or {}
    period = _safe_int(period_obj.get("number"), default=0)
    if period <= 0:
        return None
    clock_obj = play.get("clock") or {}
    type_obj = play.get("type") or {}
    team_obj = play.get("team") or {}

    return {
        "sport": sport,
        "game_id": str(game_id),
        "play_id": str(play_id),
        "sequence": _safe_int(play.get("sequenceNumber"), default=0),
        "period": period,
        "clock_secs": _parse_clock_secs(clock_obj),
        "clock_display": clock_obj.get("displayValue"),
        "home_score": _safe_int(play.get("homeScore")),
        "away_score": _safe_int(play.get("awayScore")),
        "team_id": team_obj.get("id"),
        "type_id": type_obj.get("id"),
        "type_text": type_obj.get("text") or "",
        "text": play.get("text") or "",
        "scoring_play": bool(play.get("scoringPlay")),
        "score_value": _safe_int(play.get("scoreValue")),
        "shooting_play": bool(play.get("shootingPlay")),
        "wallclock": play.get("wallclock"),
        "participants": _participants(play),
        "raw": play,
    }


def fetch_plays(sport: str, game_id: str) -> list[dict]:
    """Pull every play ESPN currently exposes for ``game_id`` and
    return them normalized. Filtering against already-stored plays is
    the store layer's job — this returns the full pull each call.

    Returns ``[]`` on fetch failure or when ESPN hasn't started
    populating plays yet (early in pre-game)."""
    payload = _fetch_summary(sport, game_id)
    if not payload:
        return []
    plays_block = payload.get("plays")
    if not isinstance(plays_block, list):
        return []
    out: list[dict] = []
    for p in plays_block:
        n = _normalize_play(sport, game_id, p)
        if n is not None:
            out.append(n)
    return out


__all__ = ["fetch_plays"]
