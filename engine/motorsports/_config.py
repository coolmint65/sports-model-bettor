"""Motorsports series registry.

Mirrors the basketball/hockey framework pattern: each series gets a
config dict the rest of the framework reads. Series differ from team-
sport leagues in two key ways the registry has to express:

  - Field size: F1 has 20-22 entrants per race, IndyCar 25-33. The
    predictor field-normalizes win probabilities so they sum to 1.0,
    so it needs to know the typical entry count per race.
  - HR market codes: HR uses a sport-specific prefix per series
    (MOTOR_SPORTS:FT:F1_TOP1 for F1 winner vs INDYCAR:OTW:WIN). The
    odds parser keys off these.

F1 uses the public Ergast MRD API (ergast.com/mrd) for historical
race + qualifying data. IndyCar has no comparable open API; left as
a v2 expansion (would need ESPN scoreboard scraping or a paid feed).
"""
from __future__ import annotations

from typing import Any

# Canonical series registry. Add a series by appending a row here +
# wiring its data_source ingest. The framework discovers series via
# this dict — no hardcoded enums elsewhere.
SERIES_REGISTRY: dict[str, dict[str, Any]] = {
    "f1": {
        "display_name": "Formula 1",
        "short_name": "F1",
        "data_source": "ergast",
        # Ergast endpoints: /api/f1/{season}.json,
        # /api/f1/{season}/qualifying.json, /api/f1/{season}/results.json
        "ergast_base": "https://api.jolpi.ca/ergast/f1",
        # HR competition ID for current season's race calendar.
        # Verified live 2026-05-10; HR rotates these per-season so a
        # late-Feb refresh of the registry is part of the season cutover.
        "hr_comp_id": "4035294407933690191",
        "hr_comp_name": "Formula 1 2026",
        # HR market type prefixes — used by _odds.py to identify which
        # selections belong to which market. F1-specific because HR ships
        # different prefixes for IndyCar.
        "hr_market_winner": "MOTOR_SPORTS:FT:F1_TOP1",
        "hr_market_podium": "MOTOR_SPORTS:FT:F1_TOP3",
        # Average grid size — used by the predictor to set the no-info
        # baseline (1/N for win, 3/N for podium).
        "field_size": 20,
        # F1 season runs March-December. Out-of-season the panel falls
        # through to "no upcoming race" rather than backfilling.
        "season_months": (3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
        "db_path": "data/motorsports/f1.db",
        "status": "beta",
    },
    "indycar": {
        "display_name": "IndyCar",
        "short_name": "INDY",
        # ESPN's core.api hosts full IndyCar event history under
        # /v2/sports/racing/leagues/irl. Ingest walks event list per
        # season and pulls competitors + finishing order from the
        # embedded competitor blob (no per-competitor request needed).
        "data_source": "espn_core",
        "espn_league_slug": "irl",
        "hr_comp_id": "1949163508763590991",
        "hr_comp_name": "IndyCar 2026",
        # HR uses OTW:WIN + TOP3 for both NASCAR & IndyCar outright
        # markets (verified live 2026-07-03 vs 2026 events). F1 has
        # separate F1_TOP1/F1_TOP3 keys.
        "hr_market_winner": "MOTOR_SPORTS:FT:OTW:WIN",
        "hr_market_podium": "MOTOR_SPORTS:FT:TOP3",
        "field_size": 27,
        "season_months": (3, 4, 5, 6, 7, 8, 9),
        "db_path": "data/motorsports/indycar.db",
        "status": "beta",
    },
    "nascar": {
        "display_name": "NASCAR Cup Series",
        "short_name": "NASCAR",
        # ESPN core.api slug 'nascar-premier'. Xfinity is 'nascar-
        # secondary'; Truck is 'nascar-truck'. Only Cup wired for v1 —
        # HR carries all three but Xfinity/Truck have smaller markets
        # and higher variance still. Add them as separate series
        # entries once Cup has settled ROI data.
        "data_source": "espn_core",
        "espn_league_slug": "nascar-premier",
        "hr_comp_id": "6881569372431843663",
        "hr_comp_name": "NASCAR Cup Series 2026",
        "hr_market_winner": "MOTOR_SPORTS:FT:OTW:WIN",
        "hr_market_podium": "MOTOR_SPORTS:FT:TOP3",
        # NASCAR Cup regularly runs 38-40 entries per race (37 chartered
        # + open-team qualifiers). Field size feeds the softmax baseline.
        "field_size": 38,
        "season_months": (2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
        "db_path": "data/motorsports/nascar.db",
        "status": "beta",
    },
}


def get_series_config(series: str) -> dict[str, Any]:
    """Return the registry entry for ``series``. Raises KeyError on miss
    so callers get a loud failure rather than silently picking up an
    unrelated config."""
    if series not in SERIES_REGISTRY:
        raise KeyError(f"unknown motorsports series: {series!r}")
    return SERIES_REGISTRY[series]
