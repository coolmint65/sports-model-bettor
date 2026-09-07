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
        "lookahead_days":    7,
        # Filled by `engine.football._calibrate` once we have enough
        # historical games. Defaults below match the calibrated PWHL/
        # AHL pattern — sport-wide priors until the per-league fit
        # lands.
        "home_advantage":    None,
        "league_avg_total":  None,
        "status":            "beta",
    },
    "nfl": {
        "display_name":      "NFL",
        "country":           "USA",
        "region":            "USA",
        "hr_comp_id":        "2295257447167426824",  # HR "NFL" regular season (discovered 2026-08-20; preseason is a separate comp 691198679103111169)
        "hr_comp_name":      "NFL",
        "data_source":       "espn",
        "espn_league_path":  "football/nfl",
        "skip_preseason":    True,     # 2026-09-03: preseason finals excluded from ingest/Elo
        "lookahead_days":    7,        # 2026-09-03 (Austin): show the coming week's slate
        # walk-forward fit 2026-09-03 on 2023-25 (858 games): K=20 best Brier (.2209); slope 30 Elo/pt
        "elo_k":             20.0,
        "elo_per_point":     30.0,
        "season_months":     (8, 9, 10, 11, 12, 1, 2),
        "db_path":           "data/football/nfl.db",
        "home_advantage":    None,
        "league_avg_total":  None,
        "status":            "beta",
    },
    "cfb": {
        "display_name":      "College Football",
        "country":           "USA",
        "region":            "USA",
        "hr_comp_id":        "700696001136984066",  # HR "NCAAF" (discovered 2026-08-20; 161 events live for the opening slate)
        "hr_comp_name":      "NCAAF",
        "data_source":       "espn",
        "espn_league_path":  "football/college-football",
        "espn_teams_suffix": "?limit=900",
        # full FBS board (default scoreboard = featured slate only) — 2026-09-03
        "espn_scoreboard_groups": [80, 81],   # FBS + FCS boards (HR prices both) — 2026-09-03
        # Elo K fit walk-forward 2026-09-03 on 2023-25 (K=40: corr .532, Brier .2004 vs
        # K=20 .513/.2021); Elo->pts slope fit 24.5 Elo/pt, so the 25 convention stands.
        "elo_k":             40.0,
        "elo_per_point":     22.0,   # refit with FCS board in (5,031 games): 22.2 Elo/pt @K40
        "lookahead_days":    0,      # today only (Saturday slates are 70+ games); ?days=N to peek
        "season_months":     (8, 9, 10, 11, 12, 1),
        "db_path":           "data/football/cfb.db",
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
        data = {k: v for k, v in data.items() if v is not None}
        # 2026-09-03: `_calibrate.fit` writes home_advantage in POINTS (mean home margin),
        # but `_elo.replay` + `_predict` consume `home_advantage` as ELO points. Convert
        # (football convention 25 Elo ≈ 1 pt) and cap at 4 pts — the raw mean margin is
        # inflated by scheduling (cupcake home games), not pure venue edge.
        if isinstance(data.get("home_advantage"), (int, float)):
            pts = float(data["home_advantage"])
            data["home_advantage_pts"] = pts
            per_pt = float(LEAGUE_REGISTRY[league].get("elo_per_point") or 25.0)
            # 2026-09-07: convert fitted home-margin (points) to Elo HFA using the
            # league's OWN fitted Elo/pt slope (CFB 22, NFL 30) for consistency with
            # _predict (expected_margin = elo_diff / elo_per_point), capped at a realistic
            # pure-venue edge (~3 pts). The old `min(pts,4.0)*25` pinned any league with
            # a >4-pt mean margin to a flat 100 Elo (CFB 7.15 -> 100, the reported bug).
            data["home_advantage"] = round(min(pts, 3.0) * per_pt, 1)
        LEAGUE_REGISTRY[league].update(data)


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
