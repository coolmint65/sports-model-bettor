"""ESPN NHL scoreboard fetcher."""

from __future__ import annotations
import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)


def _fetch_nhl_scoreboard(date: str) -> list[dict]:
    """Fetch NHL scoreboard from ESPN for a given date."""
    espn_date = date.replace("-", "")
    url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={espn_date}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "curl/8.4.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("events", [])
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to fetch NHL scoreboard for %s: %s", date, e)
        return []


# ---------------------------------------------------------------
# Shared ESPN game-state builder (MintyBot 2026-10-21). Factored out
# of _settle.settle_picks so the UNIFIED settler's NHL adapter can
# resolve game state from the SAME canonical source (ESPN linescores,
# period-aware) instead of nhl_games -- which is keyed by NHL-API ids,
# a different namespace from the ESPN ids the picks carry, so the
# unified resolver always returned None and the period / spread /
# 3-way shadow markets never graded (then false-pushed at 7 days).
# ---------------------------------------------------------------

_ABBR_ALIASES_SB = [{"SJ", "SJS"}, {"TB", "TBL"}, {"LA", "LAK"},
                    {"NJ", "NJD"}, {"UTA", "UTAH"}]


def build_game_state_map(dates) -> dict:
    """Return {espn_event_id: game_state} for the given dates.

    game_state carries home/away abbr + name forms, final score,
    per-period linescores, OT flag, completion flag and periods_locked
    (count of FINAL periods; the in-progress period is excluded).
    'pre' games are skipped. Mirrors the native settler's parsing so
    both paths resolve identically."""
    out: dict = {}
    for d in dates:
        try:
            events = _fetch_nhl_scoreboard(d)
        except Exception:
            events = []
        for event in events:
            eid = event.get("id", "")
            if not eid:
                continue
            comp = (event.get("competitions") or [{}])[0]
            status_info = comp.get("status", {})
            status = status_info.get("type", {})
            state = status.get("state", "pre")
            if state == "pre":
                continue
            is_completed = status.get("completed", False)
            cur_period = status_info.get("period") or 0
            detail = (status.get("detail") or status.get("shortDetail")
                      or "").lower()
            went_to_ot = (is_completed and (cur_period > 3
                          or "ot" in detail or "shootout" in detail))
            home_score = away_score = 0
            h_abbr = a_abbr = ""
            home_forms = []; away_forms = []
            home_periods = []; away_periods = []
            for c in comp.get("competitors", []):
                team = c.get("team", {})
                raw = c.get("score", "0")
                score = int(raw) if isinstance(raw, (int, str)) and str(raw).isdigit() else 0
                ls_raw = c.get("linescores") or []
                periods = []
                for ls in ls_raw:
                    val = ls.get("value")
                    try:
                        periods.append(int(val))
                    except (TypeError, ValueError):
                        periods.append(0)
                _forms = [team.get("abbreviation", ""), team.get("displayName", ""),
                          team.get("shortDisplayName", ""), team.get("location", ""),
                          team.get("name", "")]
                _ab = str(team.get("abbreviation", "")).upper()
                for _grp in _ABBR_ALIASES_SB:
                    if _ab in _grp:
                        _forms.extend(_grp)
                if c.get("homeAway") == "home":
                    home_score = score; h_abbr = team.get("abbreviation", "")
                    home_periods = periods; home_forms = _forms
                else:
                    away_score = score; a_abbr = team.get("abbreviation", "")
                    away_periods = periods; away_forms = _forms
            linescore_depth = min(len(home_periods), len(away_periods))
            if is_completed:
                periods_locked = linescore_depth
            else:
                periods_locked = max(0, min(linescore_depth, cur_period - 1))
            out[eid] = {
                "home_abbr": h_abbr, "away_abbr": a_abbr,
                "home_forms": home_forms, "away_forms": away_forms,
                "home_score": home_score, "away_score": away_score,
                "total": home_score + away_score,
                "home_periods": home_periods, "away_periods": away_periods,
                "went_to_ot": went_to_ot, "is_completed": is_completed,
                "periods_locked": periods_locked,
            }
    return out
