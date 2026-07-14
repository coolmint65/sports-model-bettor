"""ESPN basketball injuries fetcher + per-team status counts.

Pulls the league-wide injury feed and exposes a count of OUT players
per team. Predictor uses this as a roster-availability gate; the
penalty per OUT player is league-tunable (smaller rosters → bigger
per-injury impact).

The richer per-player VORP/usage adjustment lands in B2 with the GBM
feature pipeline. B1's roster gate is intentionally coarse: catch the
"team is missing 3 starters tonight" case without claiming to know
each player's exact value.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.request

from ._config import get_league_config

logger = logging.getLogger(__name__)


_USER_AGENT = "SportsBettor/1.0 (basketball-injuries)"
_CACHE: dict[str, tuple[float, dict[int, list[dict]]]] = {}
_CACHE_TTL_S = 900  # 15 min — injuries don't move per-minute


# Per-OUT-player margin penalty. Calibrated rough — WNBA's 12-player
# roster makes each absence ~30% more impactful than NBA's 15-player.
# These values are entry-points for B2 calibration; the GBM will refit
# them empirically once we have a labeled set.
_OUT_PENALTY_PER_PLAYER = {
    "wnba": 1.8,
    "ncaam": 1.4,
    "ncaaw": 1.4,
    # NBA is handled by its own injuries module; left here for reference.
    "nba": 1.4,
}


def _fetch(url: str) -> dict | None:
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": _USER_AGENT,
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        logger.debug("ESPN injuries fetch failed (%s): %s", url, e)
        return None


def _injuries_url(league: str) -> str:
    cfg = get_league_config(league)
    path = cfg.get("espn_league_path")
    if not path:
        raise ValueError(f"League {league!r} has no espn_league_path")
    return f"https://site.api.espn.com/apis/site/v2/sports/{path}/injuries"


def fetch_league_injuries(league: str, force: bool = False) -> dict[int, list[dict]]:
    """Return ``{team_id: [injury_dict, ...]}`` for the league.

    Each injury dict carries the bits the predictor needs:
    ``{player, status, type, return_date}``. Status comes from ESPN's
    ``INJURY_STATUS_*`` taxonomy; the predictor's penalty function
    only acts on the OUT bucket.
    """
    now = time.time()
    cached = _CACHE.get(league)
    if cached and not force and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]

    cfg = get_league_config(league)
    if cfg.get("data_source") != "espn":
        # Other sources (Euroleague XML, RealGM scrape) don't ship a
        # comparable feed here. Each will need its own injury source
        # in their respective B1 passes.
        _CACHE[league] = (now, {})
        return {}

    data = _fetch(_injuries_url(league))
    if not data:
        _CACHE[league] = (now, {})
        return {}

    out: dict[int, list[dict]] = {}
    for team in data.get("injuries") or []:
        team_id = int(team.get("id") or 0)
        if not team_id:
            continue
        team_injuries: list[dict] = []
        for item in team.get("injuries") or []:
            ath = item.get("athlete") or {}
            status = item.get("status") or ""
            inj_type = (item.get("type") or {}).get("name") or ""
            details = item.get("details") or {}
            team_injuries.append({
                "player": ath.get("displayName") or "",
                "status": status,
                "status_type": inj_type,
                "body_part": details.get("type") or "",
                "side": details.get("side") or "",
                "return_date": details.get("returnDate") or "",
            })
        if team_injuries:
            out[team_id] = team_injuries
    _CACHE[league] = (now, out)
    logger.info("[%s] injuries fetched for %d teams", league, len(out))
    return out


def out_count_for_team(league: str, team_id: int) -> int:
    """Count of players with OUT status for a team. Returns 0 when
    the league isn't covered or the team has no injuries."""
    feed = fetch_league_injuries(league)
    items = feed.get(int(team_id)) or []
    return sum(1 for i in items if i.get("status") == "Out")


def margin_penalty_for_team(league: str, team_id: int) -> float:
    """Expected-points cost to a team's scoring from current OUT
    players. ``-1.8 * out_count`` for WNBA by default."""
    n = out_count_for_team(league, team_id)
    per_player = _OUT_PENALTY_PER_PLAYER.get(league, 1.4)
    return n * per_player


def reset_cache() -> None:
    _CACHE.clear()
