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


_USER_AGENT = "curl/8.4.0"
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

# Player-WEIGHTED availability (2026-08-09, Austin "lets upgrade"): the flat
# per-head penalty above treats a 20-PPG star and a 12th-woman identically.
# The weighted path replaces the flat count with each OUT player's actual
# recent scoring, shrunk for replacement production (a starter sitting means
# minutes/usage redistribute to the bench, who produce SOME of it back).
# REPLACEMENT_RECOVERY = fraction the replacement recovers; team loses the
# rest of the OUT player's PPG. A prior, refittable once labeled.
REPLACEMENT_RECOVERY = 0.35
_PPG_LOOKBACK = 8

# LIVE stays on the flat penalty until the shadow log proves weighting
# beats it (same gate discipline as calibration). Flip to True to make the
# weighted penalty drive real pricing.
USE_WEIGHTED_AVAIL = False


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
    players. ``-1.8 * out_count`` for WNBA by default (flat per head)."""
    n = out_count_for_team(league, team_id)
    per_player = _OUT_PENALTY_PER_PLAYER.get(league, 1.4)
    return n * per_player


def _out_names_for_team(league: str, team_id: int) -> list[str]:
    feed = fetch_league_injuries(league)
    items = feed.get(int(team_id)) or []
    return [i.get("player") for i in items
            if i.get("status") == "Out" and i.get("player")]


def _recent_ppg(conn, player_name: str, team_id: int,
                lookback: int = _PPG_LOOKBACK) -> float | None:
    """Mean points over a player's last ``lookback`` logged games.
    Matches on name (exact then case-insensitive), preferring rows for
    this team but falling back to name-only (covers a recent trade)."""
    import json as _json
    for extra, params in (
        (" AND team_id=?", (player_name, player_name.lower(), int(team_id), lookback)),
        ("", (player_name, player_name.lower(), lookback)),
    ):
        try:
            rows = conn.execute(
                "SELECT stats_json FROM player_game_logs "
                "WHERE (player_name=? OR lower(player_name)=?)" + extra +
                " ORDER BY date DESC LIMIT ?", params).fetchall()
        except Exception:
            return None
        pts = []
        for (sj,) in rows:
            try:
                pts.append(float(_json.loads(sj).get("pts") or 0))
            except Exception:
                pass
        if pts:
            return sum(pts) / len(pts)
    return None


def weighted_penalty_for_team(league: str, team_id: int):
    """Player-weighted availability penalty for a team's OUT list, plus a
    per-player breakdown ``[(name, ppg_or_None, contribution), ...]``.
    Star out costs ~0.65*PPG; a benchwarmer costs almost nothing; a
    player with no logs (rookie/callup) falls back to the flat per-head
    value so an unknown is never treated as free."""
    outs = _out_names_for_team(league, team_id)
    if not outs:
        return 0.0, []
    try:
        from ._db import get_conn
        conn = get_conn(league)
    except Exception:
        return margin_penalty_for_team(league, team_id), []
    flat = _OUT_PENALTY_PER_PLAYER.get(league, 1.4)
    detail, total = [], 0.0
    for name in outs:
        ppg = _recent_ppg(conn, name, team_id)
        if ppg is None:
            contrib = flat
            detail.append((name, None, round(contrib, 2)))
        else:
            contrib = ppg * (1 - REPLACEMENT_RECOVERY)
            detail.append((name, round(ppg, 1), round(contrib, 2)))
        total += contrib
    return round(total, 2), detail


def _shadow_path() -> str:
    import os
    root = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))
    return os.path.join(root, "data", "basketball", "avail_shadow.jsonl")


def log_avail_shadow(league, home_abbr, away_abbr, home_id, away_id, date,
                     flat_home, flat_away, wtd_home, wtd_away,
                     det_home, det_away) -> None:
    """Append one row comparing flat vs weighted availability penalty per
    game. Append-only; dedupe on read (last write per league:matchup:date
    wins). This is the labeled set the flip-to-live gate reads."""
    import json as _json
    if not (det_home or det_away):
        return   # no OUT players either side — nothing to measure
    row = {"ts": None, "league": league, "date": date,
           "home": home_abbr, "away": away_abbr,
           "home_id": home_id, "away_id": away_id,
           "flat_home": round(flat_home, 2), "flat_away": round(flat_away, 2),
           "wtd_home": wtd_home, "wtd_away": wtd_away,
           "out_home": det_home, "out_away": det_away}
    try:
        from datetime import datetime
        row["ts"] = datetime.utcnow().isoformat()
    except Exception:
        pass
    try:
        with open(_shadow_path(), "a", encoding="utf-8") as f:
            f.write(_json.dumps(row) + "\n")
    except Exception:
        pass


def reset_cache() -> None:
    _CACHE.clear()
