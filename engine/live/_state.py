"""
ESPN scoreboard fetcher + per-sport live-state parser.

Pulls today's scoreboard for NBA + NHL, filters to in-progress games,
and emits a normalized state dict per game. The state shape is the
single source of truth for the conditional predictor in 3b — every
field the predictor reads goes here.

Why ESPN scoreboard (not play-by-play): see Phase 3 spec in
project_sports_model_roadmap.md. PBP buys ~1-2pp accuracy on a narrow
subset of NHL period BTS picks at 50× the bandwidth — marginal for
our markets and deferred to Phase 4.

State dict shape (per game):
    {
        "sport":         "nba" | "nhl",
        "game_id":       str,                # ESPN event id
        "matchup":       "AWAY @ HOME",
        "home":          {"abbr": "BOS", "score": 97, "name": "Boston Celtics"},
        "away":          {"abbr": "PHI", "score": 113, "name": "Philadelphia 76ers"},
        "status": {
            "state":       "in" | "post" | "pre",
            "completed":   bool,
            "period":      int,                # 1..4 for NBA, 1..3 (or 4=OT, 5=SO) NHL
            "clock":       "8:42",             # display clock; empty when stopped
            "clock_secs":  522,                # parsed seconds remaining in period
            "detail":      "End of 1st Quarter",  # ESPN status detail
        },
        "linescores": {
            "home": [23, 22, 25, 27],          # per-period scoring
            "away": [21, 24, 30, 38],
        },
        "fetched_at":   "2026-04-29T01:46:38+00:00",
    }

Fetch is read-only; no DB writes. The worker layer pipes results
into engine.live._store.upsert_state().
"""

from __future__ import annotations
import json
import logging
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"
_SPORT_PATH = {
    "nba": "basketball/nba",
    "nhl": "hockey/nhl",
    # WNBA matches NBA's scoreboard schema exactly (4 quarters, same
    # linescore/status fields). Adding the path here unlocks live state
    # polling without forking the parser.
    "wnba": "basketball/wnba",
    # NCAAM (Men's College Basketball) — same ESPN scoreboard schema as
    # NBA but 2-period (20-min halves). The downstream predictor in
    # _nba_intermission_predict.py handles the period count via
    # _SPORT_CONSTANTS.
    "ncaam": "basketball/mens-college-basketball",
    # AFL (Aussie Rules) — ESPN ships scoreboard at this path with the
    # standard competitions/status/linescores shape. 4 × 20-min quarters.
    "afl": "australian-football/afl",
}


def _fetch_scoreboard(sport: str) -> dict | None:
    """Single-shot scoreboard fetch. Returns the raw ESPN payload or
    None on failure — caller decides whether to retain stale state."""
    path = _SPORT_PATH.get(sport)
    if not path:
        raise ValueError(f"unknown sport {sport!r}")
    url = f"{_ESPN_BASE}/{path}/scoreboard"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "live_worker/1.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.warning("ESPN %s scoreboard fetch failed: %s", sport, e)
        return None


def _parse_clock(raw: str | None) -> int:
    """Parse ESPN clock string ("8:42", "0.5") into seconds remaining
    in the current period. Returns 0 when the period is over."""
    if not raw or not isinstance(raw, str):
        return 0
    s = raw.strip()
    if not s:
        return 0
    # Format M:SS (most common)
    m = re.match(r"^(\d+):(\d{1,2}(?:\.\d+)?)$", s)
    if m:
        mins = int(m.group(1))
        secs = float(m.group(2))
        return int(mins * 60 + secs)
    # Format S.S (final seconds, NBA reports clock as 0.5 etc.)
    try:
        return int(float(s))
    except ValueError:
        return 0


def _parse_event(sport: str, event: dict) -> dict | None:
    """Parse one ESPN event into our state shape. Returns None for
    pre-game / postponed events the live engine should ignore."""
    comps = event.get("competitions") or []
    if not comps:
        return None
    comp = comps[0]
    status = comp.get("status") or {}
    status_type = status.get("type") or {}
    state = status_type.get("state", "pre")  # 'pre' | 'in' | 'post'
    # Phase 3 only cares about in-progress games. Pre + post stay out
    # of the live store; pre is handled by the prematch picker, post
    # is handled by the standard tracker settler.
    if state != "in":
        return None

    # Teams + per-period linescore
    home = {"abbr": "", "score": 0, "name": ""}
    away = {"abbr": "", "score": 0, "name": ""}
    home_periods: list[int] = []
    away_periods: list[int] = []
    for c in comp.get("competitors") or []:
        team = c.get("team") or {}
        try:
            score = int(c.get("score") or 0)
        except (ValueError, TypeError):
            score = 0
        entry = {
            "abbr": team.get("abbreviation", ""),
            "score": score,
            "name": team.get("displayName", ""),
        }
        ls = c.get("linescores") or []
        period_scores = []
        for x in ls:
            try:
                period_scores.append(int(x.get("value", 0)))
            except (ValueError, TypeError):
                period_scores.append(0)
        if c.get("homeAway") == "home":
            home = entry
            home_periods = period_scores
        else:
            away = entry
            away_periods = period_scores

    if not (home["abbr"] and away["abbr"]):
        return None

    period = int(status.get("period") or 0)
    clock_str = status.get("displayClock") or ""
    clock_secs = _parse_clock(clock_str)

    return {
        "sport": sport,
        "game_id": str(event.get("id", "")),
        "matchup": f"{away['abbr']} @ {home['abbr']}",
        "home": home,
        "away": away,
        "status": {
            "state": state,
            "completed": bool(status_type.get("completed", False)),
            "period": period,
            "clock": clock_str,
            "clock_secs": clock_secs,
            "detail": status_type.get("detail", ""),
        },
        "linescores": {"home": home_periods, "away": away_periods},
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_states(sport: str) -> list[dict]:
    """Returns a list of normalized state dicts for every in-progress
    game of `sport`. Empty list when nothing is live or ESPN is down."""
    raw = _fetch_scoreboard(sport)
    if not raw:
        return []
    out: list[dict] = []
    for ev in raw.get("events") or []:
        parsed = _parse_event(sport, ev)
        if parsed:
            out.append(parsed)
    return out


def fetch_all_states() -> list[dict]:
    """NBA + NHL combined — the worker calls this once per tick."""
    return fetch_states("nba") + fetch_states("nhl")
