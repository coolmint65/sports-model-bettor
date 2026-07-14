"""Golf tour registry.

Mirrors ``engine.motorsports._config`` (series registry) — golf is
fundamentally outright structure (1-of-N field, softmax across players)
not pairwise like team sports, so it shares more shape with F1 than
with NBA/MLB.

PGA Tour is the v1 entry. LPGA / DP World / Korn Ferry / Champions
add later by registering their ESPN scoreboard path + HR comp_id.

A "tour" here is the broader org (PGA Tour, LPGA); a "tournament" is
a single event on the calendar (PGA Championship, Travelers, etc).
The tour owns the schedule, players, ratings, and DB file.
"""
from __future__ import annotations

from pathlib import Path

_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "golf"


TOUR_REGISTRY: dict[str, dict] = {
    "pga": {
        "display_name": "PGA Tour",
        "espn_scoreboard_path": "golf/pga/scoreboard",
        # HR's tour-level category id — used to discover the active
        # tournament's competition id at fetch time. Beats hand-mapping
        # per tournament (which would need updates every week).
        "hr_category_id": "2137224545403273462",
        # The Majors live under their own HR category but always count
        # as PGA Tour events for our purposes (PGA Championship, US
        # Open, The Open, Masters). We probe both categories.
        "hr_majors_category_id": "5469734070470312193",
        "default_rounds": 4,
        "cut_after_round": 2,
        "typical_field_size": 144,
        "db_path": str(_DATA_DIR / "pga.db"),
        "status": "beta",
    },
    "lpga": {
        "display_name": "LPGA Tour",
        "espn_scoreboard_path": "golf/lpga/scoreboard",
        "hr_category_id": "1627543371620942072",
        "hr_majors_category_id": None,
        "default_rounds": 4,
        "cut_after_round": 2,
        "typical_field_size": 144,
        "db_path": str(_DATA_DIR / "lpga.db"),
        "status": "beta",
    },
    "kornferry": {
        "display_name": "Korn Ferry Tour",
        # ESPN uses 'ntw' (legacy "Nationwide Tour" slug) for the
        # Korn Ferry Tour — 'korn-ferry' returns HTTP 400.
        "espn_scoreboard_path": "golf/ntw/scoreboard",
        "hr_category_id": "1965963247190016253",
        "hr_majors_category_id": None,
        "default_rounds": 4,
        "cut_after_round": 2,
        "typical_field_size": 156,
        "db_path": str(_DATA_DIR / "kornferry.db"),
        "status": "beta",
    },
}


def get_tour_config(tour: str) -> dict:
    if tour not in TOUR_REGISTRY:
        known = ", ".join(sorted(TOUR_REGISTRY))
        raise KeyError(f"Unknown golf tour {tour!r}. Known: {known}")
    return TOUR_REGISTRY[tour]
