"""
NHL Stats API play-by-play fetcher.

The NHL game_id format in our local DB is NHL Stats API gamePk
(e.g. 2025020001 = season 2025, game 20001 of regular season),
which doesn't map to ESPN event ids without a separate lookup pass.
Rather than maintain a mapping table, we hit NHL's own free
play-by-play endpoint:

    https://api-web.nhle.com/v1/gamecenter/{gamePk}/play-by-play

NHL's data is actually richer than ESPN's NHL feed for our purposes:

- Per-play XY coordinates (xCoord, yCoord) for shot-quality features
- Zone codes (O, D, N) for zone-time / forecheck pressure analysis
- ``situationCode`` for exact strength state (5551 = pulled-G 6v5,
  1551 = even strength, 1451 = 4-on-5, etc.)
- Player-resolved details (scoringPlayerId, hittingPlayerId,
  blockingPlayerId, etc.)

We normalize NHL's schema into the same dict shape as
``engine.live._pbp.fetch_plays`` so downstream consumers (5e live
GBM, 5f game-state MC) can union ESPN-NBA + NHL-Stats data without
caring which source produced each row.

Score tracking: NHL plays don't carry a running score. We walk plays
in sortOrder and accumulate ``home_score`` / ``away_score`` from
goal events, stamping the post-play score on each row. That matches
the shape ESPN gives us natively.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


_NHL_BASE = "https://api-web.nhle.com/v1/gamecenter"


# NHL Stats API typeDescKey values seen 2026-04-30 — kept here so the
# normaliser can synthesise readable text and downstream consumers
# can dispatch off type_text without re-parsing typeCode integers.
# Map preserves NHL's lowercase keys; downstream code that wants
# title-case can apply str.title().
_PLAY_TYPES = {
    "faceoff", "hit", "shot-on-goal", "missed-shot", "blocked-shot",
    "giveaway", "takeaway", "goal", "stoppage", "penalty",
    "period-start", "period-end", "game-end", "delayed-penalty",
    "shootout-complete", "failed-shot-attempt",
}


def _fetch_pbp(gamePk: str | int) -> dict | None:
    """Single-shot NHL PBP fetch. Returns the raw payload or None on
    failure. Caller decides whether to retry."""
    url = f"{_NHL_BASE}/{gamePk}/play-by-play"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "live_worker/1.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.debug("NHL PBP fetch failed for %s: %s", gamePk, e)
        return None


def _parse_mmss(value: str | None) -> int:
    """Parse 'MM:SS' to seconds. NHL's clock fields are zero-padded
    so we don't need to worry about parsing single-digit minutes."""
    if not isinstance(value, str) or ":" not in value:
        return 0
    try:
        m, s = value.split(":", 1)
        return int(m) * 60 + int(s)
    except (TypeError, ValueError):
        return 0


def _participants_for(play: dict) -> list[dict]:
    """Pull every player ID out of a play's details, tagging each with
    a role label so downstream lineup/foul derivation has a starting
    point.

    The role tags are NHL's own field names normalized to the same
    shape ESPN uses (athlete_id + role)."""
    details = play.get("details") or {}
    out: list[dict] = []

    def _add(field: str, role: str) -> None:
        pid = details.get(field)
        if pid is None:
            return
        out.append({"athlete_id": str(pid), "name": None, "role": role})

    # Goal-style plays
    _add("scoringPlayerId",   "scorer")
    _add("assist1PlayerId",   "assist1")
    _add("assist2PlayerId",   "assist2")
    _add("goalieInNetId",     "goalie")
    # Shot-style plays
    _add("shootingPlayerId",  "shooter")
    _add("blockingPlayerId",  "blocker")
    # Hit
    _add("hittingPlayerId",   "hitter")
    _add("hitteePlayerId",    "hittee")
    # Faceoff
    _add("winningPlayerId",   "faceoff_won")
    _add("losingPlayerId",    "faceoff_lost")
    # Penalty
    _add("committedByPlayerId", "penalty_committed_by")
    _add("drawnByPlayerId",     "penalty_drawn_by")
    _add("servedByPlayerId",    "penalty_served_by")
    # Generic single-player events (giveaway / takeaway)
    _add("playerId",           "actor")
    return out


def _synthesize_text(play: dict) -> str:
    """Emit a one-line description so downstream code that runs on
    play.text (NBA-style) doesn't see empty strings for NHL rows.

    NHL Stats API doesn't ship a pre-formatted text field — we build
    it from typeDescKey + the most relevant detail fields."""
    type_key = play.get("typeDescKey") or ""
    details = play.get("details") or {}
    if type_key == "goal":
        scorer = details.get("scoringPlayerId")
        a1 = details.get("assist1PlayerId")
        a2 = details.get("assist2PlayerId")
        bits = [f"Goal #{scorer}"] if scorer else ["Goal"]
        assists = [str(a) for a in (a1, a2) if a]
        if assists:
            bits.append(f"({', '.join(assists)})")
        return " ".join(bits)
    if type_key in ("shot-on-goal", "missed-shot", "blocked-shot"):
        shot_type = details.get("shotType") or ""
        shooter = details.get("shootingPlayerId")
        return f"{shot_type} #{shooter}".strip() if shooter else type_key
    if type_key == "hit":
        return f"Hit by #{details.get('hittingPlayerId')} on #{details.get('hitteePlayerId')}"
    if type_key == "penalty":
        desc = details.get("descKey") or ""
        dur = details.get("duration")
        return f"Penalty {desc} ({dur}min)".strip()
    if type_key == "faceoff":
        return f"Faceoff won by #{details.get('winningPlayerId')}"
    if type_key in ("giveaway", "takeaway"):
        return f"{type_key.title()} by #{details.get('playerId')}"
    if type_key == "stoppage":
        return f"Stoppage ({details.get('reason') or 'unknown'})"
    return type_key.replace("-", " ").title() if type_key else ""


def _normalize_plays(gamePk: str, payload: dict) -> list[dict]:
    """Turn an NHL Stats API plays array into our normalized schema.

    Iterates plays in sortOrder and tracks a running (home_score,
    away_score) so each row carries the post-play score the same way
    ESPN's NBA PBP does. The team-id-to-side mapping is read from the
    payload's homeTeam.id / awayTeam.id at the top level.
    """
    home_team = payload.get("homeTeam") or {}
    away_team = payload.get("awayTeam") or {}
    home_id = str(home_team.get("id") or "")
    away_id = str(away_team.get("id") or "")

    raw_plays = payload.get("plays") or []
    plays_sorted = sorted(raw_plays, key=lambda p: p.get("sortOrder") or 0)

    out: list[dict] = []
    home_score = 0
    away_score = 0
    for p in plays_sorted:
        type_key = p.get("typeDescKey") or ""
        period_obj = p.get("periodDescriptor") or {}
        period = int(period_obj.get("number") or 0)
        if period <= 0:
            continue
        details = p.get("details") or {}
        team_id = str(details.get("eventOwnerTeamId") or "")

        # Goal: bump the running score before we stamp it. NHL emits
        # the goal play with the team_id as the scoring team's id, so
        # that's the side that gets the +1.
        is_goal = (type_key == "goal")
        if is_goal:
            if team_id == home_id:
                home_score += 1
            elif team_id == away_id:
                away_score += 1

        time_in = p.get("timeInPeriod") or "00:00"
        time_remaining = p.get("timeRemaining") or "20:00"
        clock_secs = _parse_mmss(time_remaining)

        out.append({
            "sport": "nhl",
            "game_id": str(gamePk),
            "play_id": str(p.get("eventId") or ""),
            "sequence": int(p.get("sortOrder") or 0),
            "period": period,
            "clock_secs": clock_secs,
            "clock_display": time_remaining,
            "home_score": home_score,
            "away_score": away_score,
            "team_id": team_id or None,
            "type_id": str(p.get("typeCode") or ""),
            "type_text": type_key,
            "text": _synthesize_text(p),
            "scoring_play": is_goal,
            "score_value": 1 if is_goal else 0,
            "shooting_play": type_key in (
                "shot-on-goal", "missed-shot", "blocked-shot", "goal",
            ),
            "wallclock": payload.get("startTimeUTC"),
            "participants": _participants_for(p),
            "raw": p,
        })
    return out


def fetch_plays(gamePk: str | int) -> list[dict]:
    """Pull every NHL play for ``gamePk`` and return them normalized.
    Returns ``[]`` on fetch failure (matches the ESPN fetcher's
    contract so downstream backfill code can branch by sport without
    different exception paths)."""
    payload = _fetch_pbp(gamePk)
    if not payload:
        return []
    plays = payload.get("plays")
    if not isinstance(plays, list) or not plays:
        return []
    try:
        return _normalize_plays(str(gamePk), payload)
    except Exception as e:
        logger.warning("NHL normalize failed for %s: %s", gamePk, e)
        return []


__all__ = ["fetch_plays"]
