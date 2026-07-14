"""Football league framework — single source of truth for every
gridiron-football competition we model.

UFL is the first onboarded league; NFL/NCAAF can register here later
without code forks. Architecture mirrors ``engine.hockey``:
registry → predictor → odds → picks → tracker, all keyed by the
``league`` string the frontend sidebar uses.

Public surface:

    LEAGUE_REGISTRY     — registry dict
    get_league_config   — one league's config
    active_leagues      — leagues currently in season (per season_months)
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


_CONSTANTS_DIR = (Path(__file__).resolve().parent.parent.parent
                  / "data" / "football")


LEAGUE_REGISTRY: dict[str, dict] = {
    "ufl": {
        "display_name":      "UFL",
        "country":           "USA",
        "region":            "USA",
        "hr_comp_id":        "3300533233685102645",
        "hr_comp_name":      "UFL",
        "data_source":       "espn",
        "espn_league_path":  "football/ufl",
        # UFL season runs March-June (spring league after NFL ends).
        "season_months":     (3, 4, 5, 6),
        "db_path":           "data/football/ufl.db",
        # Filled by `engine.football._calibrate` once we have enough
        # historical games. Defaults below match the calibrated PWHL/
        # AHL pattern — sport-wide priors until the per-league fit
        # lands.
        "home_advantage":    None,
        "league_avg_total":  None,
        "status":            "beta",
    },
}


def _apply_fitted_overrides() -> None:
    """Overlay any per-league fitted constants from
    ``data/football/{league}_constants.json`` onto LEAGUE_REGISTRY.
    Missing files are silently skipped — the predictor's defaults pick
    up the slack.
    """
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
            f"unknown football league {league!r}. Known: {known}"
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
