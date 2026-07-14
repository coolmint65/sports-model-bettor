"""Baseball league framework — multi-league. ``college`` is the first
onboarded league; MLB stays on its dedicated engine.sports.mlb path
(heavier feature set + pitcher-level data this framework doesn't
attempt to match). Future leagues (KBO, NPB) can register here.

Mirrors engine.football: registry → predictor → odds → picks →
tracker, all keyed off the ``league`` string.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


_CONSTANTS_DIR = (Path(__file__).resolve().parent.parent.parent
                  / "data" / "baseball")


LEAGUE_REGISTRY: dict[str, dict] = {
    "college": {
        "display_name":      "College Baseball",
        "country":           "USA",
        "region":            "USA",
        # Refreshed 2026-05-29 — old comp 7416371663697346579 returned
        # zero events live. HR's current College Baseball comp is
        # 766596040456077332 (33 events at refresh time).
        "hr_comp_id":        "766596040456077332",
        "hr_comp_name":      "College Baseball",
        "data_source":       "espn",
        "espn_league_path":  "baseball/college-baseball",
        # NCAA baseball regular season runs Feb-May, then conference
        # tournaments + College World Series push through June.
        "season_months":     (2, 3, 4, 5, 6),
        "db_path":           "data/baseball/college.db",
        # Filled by engine.baseball._calibrate.fit once history exists.
        "home_advantage":    None,
        "league_avg_total":  None,
        "margin_sigma":      None,
        "total_sigma":       None,
        "status":            "pending_data",
    },
}


def _apply_fitted_overrides() -> None:
    if not _CONSTANTS_DIR.exists():
        return
    for p in _CONSTANTS_DIR.glob("*_constants.json"):
        league = p.stem.replace("_constants", "")
        if league not in LEAGUE_REGISTRY:
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        LEAGUE_REGISTRY[league].update(
            {k: v for k, v in data.items() if v is not None}
        )


_apply_fitted_overrides()


def get_league_config(league: str) -> dict:
    if league not in LEAGUE_REGISTRY:
        known = ", ".join(sorted(LEAGUE_REGISTRY))
        raise KeyError(
            f"unknown baseball league {league!r}. Known: {known}"
        )
    return LEAGUE_REGISTRY[league]


def active_leagues(*, ref_month: int | None = None) -> list[str]:
    m = ref_month if ref_month is not None else datetime.utcnow().month
    return [k for k, v in LEAGUE_REGISTRY.items()
             if m in (v.get("season_months") or ())]


__all__ = [
    "LEAGUE_REGISTRY",
    "get_league_config",
    "active_leagues",
]
