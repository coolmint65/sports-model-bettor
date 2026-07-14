"""Hockey league registry — single source of truth for the per-league
metadata the frontend nav + backend routes need.

Currently a thin overlay: NHL, AHL, and PWHL each have their full
implementation under ``engine.sports.{league}/``. This module just
exposes the registry + helpers so the sidebar, league switcher, and
HR odds matcher can iterate uniformly.

Public surface mirrors ``engine.basketball``:

    HOCKEY_LEAGUE_REGISTRY  — registry dict
    get_league_config(key)  — one league's config
    active_leagues()        — leagues currently in season
"""
from __future__ import annotations

from datetime import datetime

# ── Registry ─────────────────────────────────────────────────

HOCKEY_LEAGUE_REGISTRY: dict[str, dict] = {
    "nhl": {
        "display_name": "NHL",
        "country": "USA",
        "region": "USA",
        "hr_comp_id": "691036012789334019",
        "hr_comp_name": "NHL",
        "data_source": "espn",
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5, 6),
        "db_path": "data/nhl.db",
        "status": "active",
    },
    "ahl": {
        "display_name": "AHL",
        "country": "USA",
        "region": "USA",
        "hr_comp_id": "699430754717007875",
        "hr_comp_name": "USA - AHL",
        "data_source": "thescore",
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5, 6),
        "db_path": "data/hockey/ahl.db",
        # Calibrated 2026-05-04 from n=1190 (theScore backfill).
        "home_boost": 0.169,
        "league_avg_total": 6.10,
        "league_avg_gpg": 3.05,
        "status": "beta",
    },
    "pwhl": {
        "display_name": "PWHL (W)",
        "country": "USA",
        "region": "USA",
        "hr_comp_id": "652940712900034580",
        "hr_comp_name": "USA - PWHL (W)",
        "data_source": "thescore",
        # Season wrapped 2026-05; trimmed May out so the worker stops
        # picking it up until 2026-11 puck drop. Restore to
        # (11, 12, 1, 2, 3, 4, 5) before next season start.
        "season_months": (11, 12, 1, 2, 3, 4),
        "db_path": "data/hockey/pwhl.db",
        # Calibrated 2026-05-04 from n=131 (theScore backfill).
        "home_boost": 0.443,
        "league_avg_total": 4.72,
        "league_avg_gpg": 2.36,
        "status": "offseason",
    },
    "aihl": {
        "display_name": "AIHL",
        "country": "Australia",
        "region": "Oceania",
        "hr_comp_id": "756905967852847123",
        "hr_comp_name": "Australia - Ice Hockey League",
        "data_source": "sofascore",
        # AIHL runs Apr-Sep (Australian winter is Northern summer).
        "season_months": (4, 5, 6, 7, 8, 9),
        "db_path": "data/hockey/aihl.db",
        # SofaScore tournament id — confirmed 2026-05-14.
        "sofascore_tournament_id": 11059,
        "status": "beta",
    },
    "nzihl": {
        "display_name": "NZIHL",
        "country": "New Zealand",
        "region": "Oceania",
        "hr_comp_id": "772726156954304520",
        "hr_comp_name": "New Zealand - NZIHL",
        "data_source": "sofascore",
        # NZIHL same season window as AIHL (Apr-Sep).
        "season_months": (4, 5, 6, 7, 8, 9),
        "db_path": "data/hockey/nzihl.db",
        "sofascore_tournament_id": 11133,
        "status": "beta",
    },
}


def get_league_config(league: str) -> dict:
    if league not in HOCKEY_LEAGUE_REGISTRY:
        known = ", ".join(sorted(HOCKEY_LEAGUE_REGISTRY))
        raise KeyError(
            f"Unknown hockey league {league!r}. Known: {known}"
        )
    return HOCKEY_LEAGUE_REGISTRY[league]


def active_leagues(today: datetime | None = None) -> list[str]:
    today = today or datetime.now()
    out = []
    for key, cfg in HOCKEY_LEAGUE_REGISTRY.items():
        if cfg.get("status") not in ("active", "beta"):
            continue
        months = cfg.get("season_months") or ()
        if today.month in months:
            out.append(key)
    return out


def leagues_by_status(status: str) -> list[str]:
    return [k for k, v in HOCKEY_LEAGUE_REGISTRY.items()
             if v.get("status") == status]


__all__ = [
    "HOCKEY_LEAGUE_REGISTRY",
    "get_league_config",
    "active_leagues",
    "leagues_by_status",
]
