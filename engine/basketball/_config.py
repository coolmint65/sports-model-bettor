"""LEAGUE_REGISTRY — the single source of truth for every basketball
league we model.

Each entry is what the framework needs to fetch HR odds, ingest game
results, run the predictor, and persist picks. Adding a league here +
its data ingest is the entire onboarding cost — every downstream module
(predict / picks / mc / tracker / frontend) reads from this dict.

The HR comp_id values came from a 2026-05-03 sports-tree probe; HR
sometimes rotates IDs, so the scraper falls back to a name-based match
when the configured ID misses.

Game-format fields drive sigma/mu calibration:
    quarters / period_minutes / total_minutes — all three needed because
    Euroleague (4×10 = 40min), NCAAM (2×20 = 40min), NBA (4×12 = 48min),
    WNBA (4×10 = 40min) span different physical structures even when the
    total minutes are equal (NCAAM halves vs Euro quarters).

Constants stay None until per-league backfill calibrates them. The
predictor falls back to a sport-wide prior when constants are None;
that's good enough for paper-bet beta but not live picks.

Status taxonomy:
    active            — backfilled, calibrated, picks emit live
    beta              — calibrated but ROI unproven; paper-bet only
    pending_calibration — data source identified, constants not yet fitted

In-season vs off-season is derived from ``season_months`` at request
time, not stored as status — a league's data path doesn't change with
the calendar.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

# Fitted constants live alongside the league DBs and override the
# placeholder None values in LEAGUE_REGISTRY at import time. This lets
# the calibrate CLI persist new fits without touching this file.
_CONSTANTS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "basketball"


# ── League registry ────────────────────────────────────────────

# SofaScore uniqueTournament IDs — secondary results source for the
# RealGM-backed leagues. RealGM lags 12-36h on finals for small leagues;
# SofaScore publishes same-day. See scrapers.sofascore_basketball.
# Discovered 2026-05-13 via /api/v1/category/{country}/unique-tournaments;
# add new entries as we onboard more leagues.
#
# Leagues NOT here fall back to RealGM-only (no harm — just slower
# results). Add new IDs by browsing
#     https://www.sofascore.com/basketball/{country}
# or by running the discovery script in scrapers.sofascore_basketball.
SOFASCORE_TOURNAMENT_IDS: dict[str, int] = {
    "nz_nbl":              13476,
    "china_cba":            1566,
    "argentina_lnb":        1680,
    "brazil_nbb":           1562,
    "brazil_lbf_w":        19970,
    "korea_kbl":            1540,
    "australia_nbl":        1524,
    "puerto_rico_bsn":     17374,
    "dominican_lnb":       14089,
    "lithuania_lkl":         975,
    "israel_super":         1197,
    "czech_nbl":             250,
    "latvia_lbl":            982,
    "bulgaria_nbl":         1920,
    "germany_bbl":           227,
    "hungary_nb1":         10594,
    "denmark_basketligaen":  843,
    "finland_korisliiga":    226,
    "slovakia_extraliga":   1410,  # men's Tipos SBL (was 1412 women)
    "iceland_urvalsdeild":  9507,
    "iceland_urvalsdeild_w": 9504,
    "greece_a1":             304,   # Stoiximan GBL — A1's rebrand
    "sweden_ligan":          277,   # Swedish Basketball League
    "france_pro_b":         1189,   # LNB Élite 2 (verified via cat 110)
    "japan_b2":             1516,   # B2.League (verified via cat 483)
    "slovenia_skl":          745,   # Liga OTP banka (verified via cat 292)
    "fiba_champions":       9357,   # Basketball Champions League (cat 103)
}


def sofascore_tournament_id(league: str) -> int | None:
    """SofaScore uniqueTournament id for ``league``, or None if we
    haven't mapped it yet. Used as a fallback when RealGM lags."""
    return SOFASCORE_TOURNAMENT_IDS.get(league)


LEAGUE_REGISTRY: dict[str, dict] = {

    # ─── USA ───────────────────────────────────────────────────
    "nba": {
        "display_name": "NBA",
        "country": "USA",
        "hr_comp_id": "691033199537586178",
        "hr_comp_name": "NBA",
        "data_source": "espn",
        "espn_league_path": "basketball/nba",
        "quarters": 4,
        "period_minutes": 12,
        "total_minutes": 48,
        # Calibrated 2026-04-28 from n=4130 backfill.
        "home_boost": 2.14,
        "b2b_penalty": -2.5,
        "margin_std": 16.06,
        "total_std": 21.25,
        "league_avg_total": 227.89,
        "league_avg_ppg": 113.95,
        "league_avg_pace": 99.0,
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5, 6),
        "db_path": "data/nba.db",  # legacy path — keep to avoid cutover
        "status": "active",
    },
    "wnba": {
        "display_name": "WNBA",
        "country": "USA",
        "hr_comp_id": "691031701106622466",
        "hr_comp_name": "WNBA",
        "data_source": "espn",
        "espn_league_path": "basketball/wnba",
        "quarters": 4,
        "period_minutes": 10,
        "total_minutes": 40,
        # Pending calibration from backfill.
        "home_boost": None,
        "b2b_penalty": None,
        "margin_std": None,
        "total_std": None,
        "league_avg_total": None,
        "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (5, 6, 7, 8, 9, 10),
        "db_path": "data/basketball/wnba.db",
        "status": "beta",  # Calibrated 2026-05-04 from n=599 (2024 + 2025 ESPN backfill)
    },
    "ncaam": {
        "display_name": "NCAA Men's Basketball",
        "country": "USA",
        "hr_comp_id": "691032891339243522",
        "hr_comp_name": "NCAAM",
        "data_source": "espn",
        "espn_league_path": "basketball/mens-college-basketball",
        "quarters": 2,
        "period_minutes": 20,
        "total_minutes": 40,
        # Calibrated 2026-05-04 from n=6125 (full 2025-26 season).
        # Home boost is large (~7.8 pts vs NBA's 2.1) because D1 home
        # courts swing far harder than the pros — matches widely-cited
        # college research (~3.5pt SRS-adjusted home edge that compounds
        # via crowd-driven officiating). Constants stored on disk at
        # data/basketball/ncaam_constants.json; re-running the fit
        # updates them without a code edit.
        "home_boost": 7.79,
        "b2b_penalty": None,
        "margin_std": 17.91,
        "total_std": 21.01,
        "league_avg_total": 149.51,
        "league_avg_ppg": 74.75,
        "league_avg_pace": None,
        "season_months": (11, 12, 1, 2, 3, 4),
        "db_path": "data/basketball/ncaam.db",
        "status": "beta",
    },
    "ncaaw": {
        "display_name": "NCAA Women's Basketball",
        "country": "USA",
        # HR ships NCAAW under a separate competition id; populated at
        # first sports-tree probe (left None here per framework convention).
        "hr_comp_id": None,
        "hr_comp_name": "NCAAW",
        "data_source": "espn",
        "espn_league_path": "basketball/womens-college-basketball",
        # 4 × 10-min quarters since the 2015-16 rules change. Total
        # minutes 40 matches WNBA's structure exactly.
        "quarters": 4,
        "period_minutes": 10,
        "total_minutes": 40,
        # Pending calibration — November 2026 season onwards.
        "home_boost": None,
        "b2b_penalty": None,
        "margin_std": None,
        "total_std": None,
        "league_avg_total": None,
        "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (11, 12, 1, 2, 3, 4),
        "db_path": "data/basketball/ncaaw.db",
        # Data + model + walk-forward calibration all in place
        # (2026-05-17: 1862 finals backfilled from ESPN, GBM trained
        # on 19072 train + 4769 test, all 6 markets seeded). Only gap
        # is odds — HR doesn't carry NCAAW (confirmed via sports tree
        # probe). Picks fire automatically the moment any odds source
        # surfaces NCAAW.
        "status": "pending_odds",
    },
    "nba_summer_league": {
        "display_name": "NBA Summer League",
        "country": "USA",
        "hr_comp_id": "691033199537586178",
        "hr_comp_name": "NBA Summer League",
        "data_source": "espn",
        # Vegas is the anchor path (7/10-7/20, longest + best-covered).
        # California Classic + Salt Lake City run 7/5-7/9 and ship
        # under HR's same comp id, so multi-slug ingest keeps the DB
        # in sync with HR's pre-Vegas market.
        "espn_league_path": "basketball/nba-summer-las-vegas",
        "espn_extra_paths": (
            "basketball/nba-summer-california",
            "basketball/nba-summer-utah",
        ),
        "quarters": 4,
        "period_minutes": 10,
        "total_minutes": 40,
        # No calibration yet — Summer League runs 2 wks/yr, this is
        # the first cycle we're capturing. Predictor falls back to
        # WNBA-shaped priors (4×10 min, similar total_minutes) until
        # we accumulate enough games to fit constants. Live picks
        # are gated by the "pending_calibration" status; ingest +
        # tracker still work, so the DB fills up organically.
        "home_boost": None,
        "b2b_penalty": None,
        "margin_std": None,
        "total_std": None,
        "league_avg_total": None,
        "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (7,),
        "db_path": "data/basketball/nba_summer_league.db",
        # Constants fitted 2026-07-03 from n=228 (2023 + 2024 + 2025
        # Vegas SL backfill via ESPN). GBM trained on 182 train / 46
        # test, walk-forward calibration seeded across ML/SPREAD/TOTAL
        # + Q1 splits. Non-canonical minor league — active for the
        # 2026 cycle; ROI stays paper-bet until we have a full run of
        # settled picks to validate against.
        "status": "beta",
    },

    # ─── International umbrella ────────────────────────────────
    "euroleague": {
        "display_name": "Euroleague",
        "country": "International",
        "hr_comp_id": "2782205879401578742",
        "hr_comp_name": "Euroleague",
        "data_source": "euroleague_api",
        "espn_league_path": None,
        "quarters": 4,
        "period_minutes": 10,
        "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/euroleague.db",
        "status": "beta",  # Calibrated 2026-05-04 from n=697 (E2024 + E2025)
    },
    "fiba_champions": {
        "display_name": "FIBA Champions League",
        "country": "International",
        "hr_comp_id": "802029418242113782",
        "hr_comp_name": "FIBA Champions League",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/fiba_champions.db",
        "status": "beta",
    },

    # ─── Europe ────────────────────────────────────────────────
    "france_pro_b": {
        "display_name": "France Pro B",
        "country": "France",
        "hr_comp_id": "701707744217825287",
        "hr_comp_name": "France - Pro B",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5, 6),
        "db_path": "data/basketball/france_pro_b.db",
        "status": "beta",
    },
    "lithuania_lkl": {
        "display_name": "Lithuania LKL",
        "country": "Lithuania",
        "hr_comp_id": "691174830643773442",
        "hr_comp_name": "Lithuania - LKL",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/lithuania_lkl.db",
        "status": "beta",
    },
    "israel_super": {
        "display_name": "Israel Super League",
        "country": "Israel",
        "hr_comp_id": "696547330670034947",
        "hr_comp_name": "Israel - Super League",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5, 6),
        "db_path": "data/basketball/israel_super.db",
        "status": "beta",
    },
    "greece_a1": {
        "display_name": "Greece A1",
        "country": "Greece",
        "hr_comp_id": "691622158611644418",
        "hr_comp_name": "Greece - A1",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5, 6),
        "db_path": "data/basketball/greece_a1.db",
        "status": "beta",
    },
    "czech_nbl": {
        "display_name": "Czech NBL",
        "country": "Czech Republic",
        # HR rotates comp_ids — refreshed 2026-05-17 from sports tree
        # probe (was 693798393514590210).
        "hr_comp_id": "6965250987485036798",
        "hr_comp_name": "Czech - NBL",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/czech_nbl.db",
        "status": "beta",
    },
    "latvia_lbl": {
        "display_name": "Latvia LBL",
        "country": "Latvia",
        "hr_comp_id": "752902912330006536",
        "hr_comp_name": "Latvia - LBL",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/latvia_lbl.db",
        "status": "beta",
    },
    "bulgaria_nbl": {
        "display_name": "Bulgaria NBL",
        "country": "Bulgaria",
        # HR rotates comp_ids — refreshed 2026-05-17 from sports tree
        # probe (was 699692241837752323).
        "hr_comp_id": "9121867526689259777",
        "hr_comp_name": "Bulgaria - NBL",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/bulgaria_nbl.db",
        "status": "beta",
    },
    "iceland_urvalsdeild": {
        "display_name": "Iceland Úrvalsdeild",
        "country": "Iceland",
        "hr_comp_id": "698271512199004162",
        "hr_comp_name": "Iceland - Urvalsdeild",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/iceland_urvalsdeild.db",
        "status": "beta",
    },
    "iceland_urvalsdeild_w": {
        "display_name": "Iceland Úrvalsdeild (W)",
        "country": "Iceland",
        "hr_comp_id": "699103396344594433",
        "hr_comp_name": "Iceland - Urvalsdeild (W)",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/iceland_urvalsdeild_w.db",
        "status": "beta",
    },
    "germany_bbl": {
        "display_name": "Germany BBL",
        "country": "Germany",
        "hr_comp_id": "691036274911412226",
        "hr_comp_name": "Germany - BBL",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3, 4, 5, 6),
        "db_path": "data/basketball/germany_bbl.db",
        "status": "beta",
    },
    "hungary_nb1": {
        "display_name": "Hungary NB I",
        "country": "Hungary",
        "hr_comp_id": "695157479341654018",
        "hr_comp_name": "Hungary - NB I",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/hungary_nb1.db",
        "status": "beta",
    },
    "slovenia_skl": {
        "display_name": "Slovenia 1.A SKL",
        "country": "Slovenia",
        "hr_comp_id": "696835972410867714",
        "hr_comp_name": "Slovenia - 1. A SKL",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/slovenia_skl.db",
        "status": "beta",
    },
    "denmark_basketligaen": {
        "display_name": "Denmark Basketligaen",
        "country": "Denmark",
        "hr_comp_id": "695220927351685123",
        "hr_comp_name": "Denmark - Basketligaen",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/denmark_basketligaen.db",
        "status": "beta",
    },
    "finland_korisliiga": {
        "display_name": "Finland Korisliiga",
        "country": "Finland",
        "hr_comp_id": "697140870108545027",
        "hr_comp_name": "Finland - Korisliiga",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/finland_korisliiga.db",
        "status": "beta",
    },
    "slovakia_extraliga": {
        "display_name": "Slovakia Extraliga",
        "country": "Slovakia",
        "hr_comp_id": "694566215660961795",
        "hr_comp_name": "Slovakia - Extraliga",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/slovakia_extraliga.db",
        "status": "beta",
    },
    "sweden_ligan": {
        "display_name": "Sweden Ligan",
        "country": "Sweden",
        "hr_comp_id": "695689202059116547",
        "hr_comp_name": "Sweden - Ligan",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/sweden_ligan.db",
        "status": "beta",
    },

    # ─── Americas (non-USA) ────────────────────────────────────
    "dominican_lnb": {
        "display_name": "Dominican Rep. LNB",
        "country": "Dominican Republic",
        "hr_comp_id": "691213372427141121",
        "hr_comp_name": "Dominican Rep. - LNB",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (4, 5, 6, 7, 8, 9, 10),
        "db_path": "data/basketball/dominican_lnb.db",
        "status": "beta",
    },
    "puerto_rico_bsn": {
        "display_name": "Puerto Rico BSN",
        "country": "Puerto Rico",
        "hr_comp_id": "691085198109704195",
        "hr_comp_name": "Puerto Rico - Superior Nacional",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (3, 4, 5, 6, 7, 8, 9),
        "db_path": "data/basketball/puerto_rico_bsn.db",
        "status": "beta",
    },
    "brazil_nbb": {
        "display_name": "Brazil NBB",
        "country": "Brazil",
        "hr_comp_id": "702989912102502407",
        "hr_comp_name": "Brazil - NBB",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/brazil_nbb.db",
        "status": "beta",
    },
    "brazil_lbf_w": {
        "display_name": "Brazil LBF (W)",
        "country": "Brazil",
        "hr_comp_id": "3309346829349683219",
        "hr_comp_name": "Brazil - LBF (W)",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/brazil_lbf_w.db",
        "status": "beta",
    },
    "argentina_lnb": {
        "display_name": "Argentina LNB",
        "country": "Argentina",
        "hr_comp_id": "703854956586565637",
        "hr_comp_name": "Argentina - LNB",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/argentina_lnb.db",
        "status": "beta",
    },

    # ─── Asia / Oceania ────────────────────────────────────────
    "japan_b2": {
        "display_name": "Japan B2 League",
        "country": "Japan",
        "hr_comp_id": "698303885819478019",
        "hr_comp_name": "Japan - B2 League",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/japan_b2.db",
        "status": "beta",
    },

    # ─── User-named, off-season on HR right now ────────────────
    # HR ships these comps when they're in season but they weren't in the
    # 2026-05-03 tree probe. hr_comp_id stays None until first sighting;
    # the scraper falls back to name-match on hr_comp_name. RealGM covers
    # all four for results-side data (verified 2026-05-04).
    "korea_kbl": {
        "display_name": "Korea KBL",
        "country": "South Korea",
        "hr_comp_id": None,
        "hr_comp_name": "Korea - KBL",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (10, 11, 12, 1, 2, 3, 4),
        "db_path": "data/basketball/korea_kbl.db",
        "status": "beta",
    },
    "australia_nbl": {
        "display_name": "Australia NBL",
        "country": "Australia",
        "hr_comp_id": None,
        "hr_comp_name": "Australia - NBL",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (9, 10, 11, 12, 1, 2, 3),
        "db_path": "data/basketball/australia_nbl.db",
        "status": "beta",
    },
    "nz_nbl": {
        "display_name": "New Zealand NBL",
        "country": "New Zealand",
        "hr_comp_id": "756649104672849939",
        "hr_comp_name": "New Zealand - NBL",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 10, "total_minutes": 40,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (3, 4, 5, 6, 7),
        "db_path": "data/basketball/nz_nbl.db",
        "status": "beta",
    },
    # ── Australian Rules Football (AFL) ──
    # Hosted under the basketball framework because the structural math
    # is identical: 4 quarters, 80-110 pt totals, ML/Line/Total markets,
    # ESPN linescores. The AFL frontend gets its own sidebar entry but
    # routes through /api/basketball/afl/* on the backend.
    "afl": {
        "display_name": "AFL",
        "country": "Australia",
        "hr_comp_id": "691035930859372547",
        "hr_comp_name": "AFL",
        "data_source": "espn",
        "espn_league_path": "australian-football/afl",
        "quarters": 4, "period_minutes": 20, "total_minutes": 80,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        # AFL season runs Mar–Sep (premiership season + Sep finals).
        "season_months": (3, 4, 5, 6, 7, 8, 9, 10),
        "db_path": "data/basketball/afl.db",
        "status": "beta",
    },
    "china_cba": {
        "display_name": "China CBA",
        "country": "China",
        "hr_comp_id": "700747368356478979",
        "hr_comp_name": "China - CBA",
        "data_source": "realgm",
        "espn_league_path": None,
        "quarters": 4, "period_minutes": 12, "total_minutes": 48,
        "home_boost": None, "b2b_penalty": None,
        "margin_std": None, "total_std": None,
        "league_avg_total": None, "league_avg_ppg": None,
        "league_avg_pace": None,
        "season_months": (10, 11, 12, 1, 2, 3, 4, 5),
        "db_path": "data/basketball/china_cba.db",
        "status": "beta",
    },
}


# ── Helpers ───────────────────────────────────────────────────

def _apply_fitted_overrides() -> None:
    """At import time, load any persisted constants from
    ``data/basketball/<league>_constants.json`` and overlay them onto
    LEAGUE_REGISTRY so the predictor sees fitted values automatically.

    The on-disk JSON is the source of truth for fits — re-running the
    calibrator updates the JSON, and the next process pickup sees the
    new values without a code edit."""
    if not _CONSTANTS_DIR.exists():
        return
    for league in LEAGUE_REGISTRY:
        path = _CONSTANTS_DIR / f"{league}_constants.json"
        if not path.exists():
            continue
        try:
            fitted = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for key in ("home_boost", "margin_std", "total_std",
                    "league_avg_total", "league_avg_ppg",
                    "league_avg_pace", "b2b_penalty",
                    "q1_home_boost", "q1_margin_std", "q1_total_std",
                    "q1_avg_total"):
            if fitted.get(key) is not None:
                LEAGUE_REGISTRY[league][key] = fitted[key]


_apply_fitted_overrides()


def get_league_config(league: str) -> dict:
    """Return the registry entry for ``league`` or raise KeyError.

    Use sparingly — most callers should just ``LEAGUE_REGISTRY[league]``
    and accept the KeyError. This wrapper exists for the path where the
    league name comes from user input and a clearer message helps."""
    if league not in LEAGUE_REGISTRY:
        known = ", ".join(sorted(LEAGUE_REGISTRY))
        raise KeyError(f"Unknown basketball league {league!r}. Known: {known}")
    return LEAGUE_REGISTRY[league]


def active_leagues(today: datetime | None = None) -> list[str]:
    """Return league keys that are in-season today AND have status active/beta.

    Used by the frontend league sub-nav to default the user to a league
    that actually has events. Off-season leagues stay clickable but
    don't auto-select."""
    today = today or datetime.now()
    out = []
    for key, cfg in LEAGUE_REGISTRY.items():
        if cfg.get("status") not in ("active", "beta"):
            continue
        months = cfg.get("season_months") or ()
        if today.month in months:
            out.append(key)
    return out


def leagues_by_status(status: str) -> list[str]:
    """Filter the registry by status. Mostly for the audit CLI."""
    return [k for k, v in LEAGUE_REGISTRY.items() if v.get("status") == status]


# Asia/Oceania countries whose local game-night straddles ET midnight.
# 7 PM local in Beijing/Tokyo/Seoul/Sydney/Auckland = 3-7 AM ET *next day*,
# so games their fans consider "tonight" stamp as tomorrow ET in our DB.
# Without special-casing the slate filter + ingest cadence, the panel for
# these leagues is always 24h behind — the user opening at 8 PM ET sees
# yesterday's already-completed games instead of the upcoming ones.
# Source of truth lives at the engine level so hockey + future sport
# frameworks can reuse the same set without re-defining it.
from .._geo import FAR_EAST_COUNTRIES as _FAR_EAST_COUNTRIES, is_far_east_country


def is_far_east(league: str) -> bool:
    """True when ``league`` plays in an Asia/Oceania timezone whose
    local evening lands in our overnight hours. Slate route + ingest
    use this to extend the today window to include tomorrow ET."""
    cfg = LEAGUE_REGISTRY.get(league) or {}
    return is_far_east_country(cfg.get("country"))
