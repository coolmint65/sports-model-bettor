"""LEAGUE_REGISTRY — every soccer competition we model.

Each entry tells the framework how to: pull HR odds, ingest results,
run the predictor, and persist picks. Onboarding a new league is
config + data ingest (no code fork) — every downstream module reads
from this dict.

Field reference:

    display_name     — UI label
    country          — host country / region (UI grouping)
    confederation    — UEFA / CONCACAF / CONMEBOL / AFC / CAF / OFC / FIFA
    tier             — 1 = top flight, 2 = second tier, etc.
                       Used to keep the relegation/promotion shuffle
                       coherent when teams move between leagues.
    competition_type — 'league' (round-robin) or 'cup' (knockout)
    hr_comp_id       — Hard Rock comp ID for odds. Verified 2026-05-14
                       via sports-tree probe. HR rotates IDs so the
                       scraper falls back to name-match on miss.
    hr_comp_name     — Display string HR ships back; used for name match
    data_source      — 'espn' (most), 'sofascore' (fallback for small),
                       'football_data_org' (long history) etc
    espn_league_path — ESPN's slug after /soccer/ (e.g., 'eng.1')
    season_months    — calendar months when this league is live. ESPN
                       organizes by start-year; a season-month tuple
                       handles overlap years (e.g., Big-5 are Aug–May).
    db_path          — per-league sqlite file
    status           — see __init__ docstring

Calibration constants (None until fit):
    home_advantage   — additive home-goal Poisson rate bonus
    avg_home_goals   — historical mean home goals per match
    avg_away_goals   — historical mean away goals per match
    dc_rho           — Dixon-Coles low-score correlation tuning param
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


_CONSTANTS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "soccer"


# ── League registry ────────────────────────────────────────────

LEAGUE_REGISTRY: dict[str, dict] = {

    # ─── USA / North America ───────────────────────────────────
    "mls": {
        "display_name": "MLS",
        "country": "USA",
        "confederation": "CONCACAF",
        "tier": 1,
        "competition_type": "league",
        "hr_comp_id": "7101049710921384194",
        "hr_comp_name": "USA - MLS",
        "data_source": "espn",
        "espn_league_path": "usa.1",
        "season_months": (2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/mls.db",
        "status": "beta",
    },
    "usl_championship": {
        "display_name": "USL Championship",
        "country": "USA",
        "confederation": "CONCACAF",
        "tier": 2,
        "competition_type": "league",
        # Resolved 2026-05-20 via HR sports-tree probe.
        "hr_comp_id": "691036095001853955",
        "hr_comp_name": "USA - USL Championship",
        "data_source": "espn",
        "espn_league_path": "usa.usl.1",
        "season_months": (3, 4, 5, 6, 7, 8, 9, 10, 11),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/usl_championship.db",
        "status": "beta",
    },
    "us_nwsl": {
        "display_name": "NWSL",
        "country": "USA",
        "confederation": "CONCACAF",
        "tier": 1,
        "competition_type": "league",
        # Resolved 2026-05-22 via HR sports-tree probe.
        "hr_comp_id": "691159042359984131",
        "hr_comp_name": "USA - National Soccer League (W)",
        "data_source": "espn",
        "espn_league_path": "usa.nwsl",
        "season_months": (3, 4, 5, 6, 7, 8, 9, 10, 11),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/us_nwsl.db",
        "status": "beta",
    },
    "us_open_cup": {
        # US Open Cup is the knockout-format domestic cup. Teams from
        # MLS, USL Championship, USL One, NISA, and amateur ranks all
        # enter at staggered rounds, so the field strength ranges
        # widely (an MLS side vs a USL One team is a typical mismatch).
        # The Dixon-Coles per-league calibration captures the average
        # goals-per-match for the cup as a whole; team-level strength
        # comes through Elo since most clubs also appear in MLS or USL.
        "display_name": "US Open Cup",
        "country": "USA",
        "confederation": "CONCACAF",
        "tier": 1,
        "competition_type": "cup",
        "hr_comp_id": "745658914151399444",
        "hr_comp_name": "USA - US Open Cup",
        "data_source": "espn",
        "espn_league_path": "usa.open",
        "season_months": (3, 4, 5, 6, 7, 8, 9),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/us_open_cup.db",
        "status": "beta",
    },

    # ─── Big-5 European leagues ────────────────────────────────
    "eng_premier": {
        "display_name": "Premier League",
        "country": "England",
        "confederation": "UEFA",
        "tier": 1,
        "competition_type": "league",
        "hr_comp_id": "8214517063054262534",
        "hr_comp_name": "England - Premier League",
        "data_source": "espn",
        "espn_league_path": "eng.1",
        "season_months": (8, 9, 10, 11, 12, 1, 2, 3, 4, 5),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        # V3.1 market-blend weight. 0.5 = 50/50 DC + Pinnacle closing;
        # validated -2.18% Brier vs raw DC on 2024-07-01+ holdout.
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/eng_premier.db",
        "status": "beta",
    },
    "esp_laliga": {
        "display_name": "La Liga",
        "country": "Spain",
        "confederation": "UEFA",
        "tier": 1,
        "competition_type": "league",
        "hr_comp_id": "1916690381013778691",
        "hr_comp_name": "Spain - La Liga",
        "data_source": "espn",
        "espn_league_path": "esp.1",
        "season_months": (8, 9, 10, 11, 12, 1, 2, 3, 4, 5),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/esp_laliga.db",
        "status": "beta",
    },
    "ita_seriea": {
        "display_name": "Serie A",
        "country": "Italy",
        "confederation": "UEFA",
        "tier": 1,
        "competition_type": "league",
        "hr_comp_id": "7840846839937761530",
        "hr_comp_name": "Italy - Serie A",
        "data_source": "espn",
        "espn_league_path": "ita.1",
        "season_months": (8, 9, 10, 11, 12, 1, 2, 3, 4, 5),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/ita_seriea.db",
        "status": "beta",
    },
    "ger_bundesliga": {
        "display_name": "Bundesliga",
        "country": "Germany",
        "confederation": "UEFA",
        "tier": 1,
        "competition_type": "league",
        "hr_comp_id": "691033453060521986",
        "hr_comp_name": "Germany - Bundesliga",
        "data_source": "espn",
        "espn_league_path": "ger.1",
        "season_months": (8, 9, 10, 11, 12, 1, 2, 3, 4, 5),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/ger_bundesliga.db",
        "status": "beta",
    },
    "fra_ligue1": {
        "display_name": "Ligue 1",
        "country": "France",
        "confederation": "UEFA",
        "tier": 1,
        "competition_type": "league",
        "hr_comp_id": "1914297843711738115",
        "hr_comp_name": "France - Ligue 1",
        "data_source": "espn",
        "espn_league_path": "fra.1",
        "season_months": (8, 9, 10, 11, 12, 1, 2, 3, 4, 5),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/fra_ligue1.db",
        "status": "beta",
    },

    # ─── UEFA club competitions ────────────────────────────────
    "uefa_champions": {
        "display_name": "UEFA Champions League",
        "country": "International",
        "confederation": "UEFA",
        "tier": 1,
        "competition_type": "cup",
        "hr_comp_id": "691035985490411523",
        "hr_comp_name": "UEFA - Champions League",
        "data_source": "espn",
        "espn_league_path": "uefa.champions",
        "season_months": (7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/uefa_champions.db",
        "status": "beta",
    },
    "uefa_europa": {
        "display_name": "UEFA Europa League",
        "country": "International",
        "confederation": "UEFA",
        "tier": 2,
        "competition_type": "cup",
        "hr_comp_id": "691035937796161539",
        "hr_comp_name": "UEFA - Europa League",
        "data_source": "espn",
        "espn_league_path": "uefa.europa",
        "season_months": (7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/uefa_europa.db",
        "status": "beta",
    },
    "uefa_conference": {
        "display_name": "UEFA Conference League",
        "country": "International",
        "confederation": "UEFA",
        "tier": 3,
        "competition_type": "cup",
        "hr_comp_id": "691167639143120897",
        "hr_comp_name": "UEFA - Europa Conference League",
        "data_source": "espn",
        "espn_league_path": "uefa.europa.conf",
        "season_months": (7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/uefa_conference.db",
        "status": "beta",
    },

    # ─── South America ─────────────────────────────────────────
    "bra_seriea": {
        "display_name": "Brazil Série A",
        "country": "Brazil",
        "confederation": "CONMEBOL",
        "tier": 1,
        "competition_type": "league",
        "hr_comp_id": "12866206310334726",
        "hr_comp_name": "Brazil - Serie A",
        "data_source": "espn",
        "espn_league_path": "bra.1",
        "season_months": (4, 5, 6, 7, 8, 9, 10, 11, 12),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/bra_seriea.db",
        "status": "beta",
    },
    "arg_lpf": {
        "display_name": "Argentine Liga Profesional",
        "country": "Argentina",
        "confederation": "CONMEBOL",
        "tier": 1,
        "competition_type": "league",
        "hr_comp_id": "1374569575869055235",
        "hr_comp_name": "Argentina - Liga Profesional de Futbol",
        "data_source": "espn",
        "espn_league_path": "arg.1",
        "season_months": (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        # Argentina LPF has football-data.co.uk historical_odds backfilled
        # (in data/soccer/arg_lpf/historical_odds.db) but the v31 flag
        # wasn't flipped during the initial productionization sweep.
        # Activated alongside the sofa-sourced leagues on 2026-05-28.
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/arg_lpf.db",
        "status": "beta",
    },
    "conmebol_libertadores": {
        "display_name": "Copa Libertadores",
        "country": "International",
        "confederation": "CONMEBOL",
        "tier": 1,
        "competition_type": "cup",
        "hr_comp_id": "691036256803094531",
        "hr_comp_name": "CONMEBOL - Copa Libertadores",
        "data_source": "espn",
        "espn_league_path": "conmebol.libertadores",
        "season_months": (2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        # V3.1 market-blend. Activated 2026-05-28 after sofascore_soccer
        # backfilled the historical_odds.db (football-data.co.uk doesn't
        # carry CONMEBOL competitions, so the standard EU per-league
        # scraper couldn't help — sofascore became the source). Flag
        # silently no-ops when historical_odds has no row for the match,
        # so adding it ahead of full coverage is safe.
        "v31_market_blend": 0.5,
        "db_path": "data/soccer/conmebol_libertadores.db",
        "status": "beta",
    },

    # ─── National team competitions ────────────────────────────
    # Pool of every national-team competitive match — qualifiers across
    # all five confederations plus friendlies + Nations Leagues. One DB
    # so a single Elo ladder spans the whole international game; that
    # ladder seeds the WC predictor when actual WC samples are zero.
    "fifa_internationals": {
        "display_name": "International (FIFA Pool)",
        "country": "International",
        "confederation": "FIFA",
        "tier": 1,
        "competition_type": "league",
        "hr_comp_id": None,
        "hr_comp_name": None,
        "data_source": "espn",
        "espn_league_path": "fifa.worldq.uefa",
        # Multi-path: ingest walks every entry below into the same DB.
        # ``fifa.world`` is the actual World Cup; without it the Elo
        # pool stops updating once group-stage qualifiers end in early
        # June, leaving every WC prediction running on 3-week-stale
        # ratings. Discovered 2026-06-30 mid-tournament audit.
        "espn_extra_paths": [
            "fifa.worldq.conmebol",
            "fifa.worldq.concacaf",
            "fifa.worldq.afc",
            "fifa.worldq.caf",
            "fifa.friendly",
            "fifa.world",
        ],
        "season_months": tuple(range(1, 13)),  # always on
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "db_path": "data/soccer/fifa_internationals.db",
        "status": "beta",
    },
    "fifa_world_cup": {
        "display_name": "FIFA World Cup",
        "country": "International",
        "confederation": "FIFA",
        "tier": 1,
        "competition_type": "cup",
        "hr_comp_id": "906220875823186255",
        "hr_comp_name": "World Cup 2026",
        "status": "beta",
        "data_source": "espn",
        "espn_league_path": "fifa.world",
        # 2026 USA/MEX/CAN tournament runs 6/11 - 7/19. Expanded to
        # 5/29 onward so worker warmup, schedule pre-checks, and pre-
        # tournament odds capture all start ahead of kick-off. After
        # the final on 7/19 the August month covers settle of the last
        # round of picks before falling dormant again until 2030.
        "season_months": (5, 6, 7, 8),
        # Constants borrowed from fifa_internationals (1121-match pool
        # of qualifiers + friendlies). Same teams play in both pools so
        # the goal-rate prior carries over cleanly. Walk-forward fit on
        # WC-only data isn't possible — only 64 matches every 4 years
        # so the sample never accumulates.
        "home_advantage": None,
        "avg_home_goals": None,
        "avg_away_goals": None,
        "dc_rho": None,
        "db_path": "data/soccer/fifa_world_cup.db",
    },
}


# ── Calibration-constants loader ──────────────────────────────

def _load_constants() -> dict[str, dict]:
    """Read every ``<league>_constants.json`` in the data dir and return
    ``{league: constants_dict}``. Missing files are silently skipped —
    a league without a constants file falls back to the in-source None
    placeholders. Picks emit warnings, not exceptions, so the predictor
    just uses sport-wide priors instead."""
    out: dict[str, dict] = {}
    if not _CONSTANTS_DIR.exists():
        return out
    for p in _CONSTANTS_DIR.glob("*_constants.json"):
        league = p.stem.replace("_constants", "")
        try:
            out[league] = json.loads(p.read_text())
        except (json.JSONDecodeError, OSError):
            continue
    return out


# Apply on-disk overrides at import time so anything that imports
# LEAGUE_REGISTRY sees the latest fits without an explicit reload step.
for _league, _consts in _load_constants().items():
    if _league in LEAGUE_REGISTRY:
        LEAGUE_REGISTRY[_league].update({
            k: v for k, v in _consts.items() if v is not None
        })


# ── Accessors ──────────────────────────────────────────────────

def get_league_config(league: str) -> dict:
    """Lookup with a friendly error so callers don't see a KeyError."""
    cfg = LEAGUE_REGISTRY.get(league)
    if not cfg:
        raise ValueError(
            f"unknown soccer league {league!r}. "
            f"Known: {sorted(LEAGUE_REGISTRY.keys())}"
        )
    return cfg


def active_leagues(*, ref_month: int | None = None) -> list[str]:
    """Leagues currently in season per ``season_months``. ``ref_month``
    defaults to the current calendar month — pass a specific month for
    testing."""
    m = ref_month if ref_month is not None else datetime.utcnow().month
    return [
        key for key, cfg in LEAGUE_REGISTRY.items()
        if m in (cfg.get("season_months") or ())
    ]
