"""Playoff series awareness for NHL and NBA predictions.

Infers series state (game number, series score, elimination/closeout)
from recent game results between the same two teams. No external API
needed — works purely from the games already in the database + today's
schedule.

Historical tendencies applied as adjustments:

  - **Elimination games** (team facing 0-3, 1-3, 2-3): desperate teams
    cover at ~55%. Boost underdog xG/pts, slight total bump.
  - **Closeout games** (team up 3-0, 3-1, 3-2): leading team sometimes
    coasts. Slight fade on favorite, slight total drop.
  - **Game 1** at home: home team wins ~58% historically. Extra home
    edge boost.
  - **Series tied** (1-1, 2-2, 3-3): true coin flip, home edge matters
    more. Boost home edge.
  - **Rest days**: 0 days rest (back-to-back) → fatigue penalty on
    total and slight away boost. 2+ days rest → slight bump.

All adjustments are multiplicative on xG/expected points and additive
on home edge, keeping them composable with the existing playoff
factors in nhl_predict.py and nba_q1_predict.py.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ── Toggle ────────────────────────────────────────────────────
ENABLE_SERIES_CONTEXT = True

# ── Adjustment magnitudes ─────────────────────────────────────
# These are conservative starting points. Track performance and
# tune via the same factor-ablation pattern the model already uses.

# Elimination game: team facing elimination "gets a boost".
#
# Disabled 2026-04-28 per user directive — set to 0 until empirical
# data validates a non-zero magnitude. History:
#   - Started at 0.03 (+3% xG) based on a comment citing "~55% ATS"
#     in NHL/NBA elimination games — no actual NBA-specific evidence.
#   - Halved to 0.015 the same day after the +3% knob was the sole
#     driver of multiple plus-money picks (POR +3.5 @ +275, PHI ML
#     @ +375, both down 1-3, both edges ~9% from this boost alone).
#   - Removed entirely this commit. User: "they do play harder, but
#     there's a reason they're about to get eliminated." Until ≥30
#     elimination games settle in tracker and the realised ATS rate
#     justifies a non-zero boost, run without the adjustment. Same
#     for ELIMINATION_TOTAL_BUMP — 2% scoring lift was equally
#     unvalidated and rode the same logic. Apply via empirical
#     evidence, not vibes.
ELIMINATION_UNDERDOG_BOOST = 0.0
ELIMINATION_TOTAL_BUMP = 1.0

# Closeout game: leading team sometimes coasts
CLOSEOUT_FAVORITE_FADE = 0.02         # -2% win prob for leading team
CLOSEOUT_TOTAL_DIP = 0.98             # 2% lower scoring

# Game 1 home edge: historically ~58% home win rate in Game 1
GAME1_HOME_BOOST = 0.04              # +4% additive home edge

# Series tied: home court/ice matters more
TIED_SERIES_HOME_BOOST = 0.03        # +3% additive home edge

# Rest days between games
REST_0_TOTAL_FACTOR = 0.97           # back-to-back: 3% scoring drop
REST_0_AWAY_BOOST = 0.01             # slight away edge (home tired too)
REST_2PLUS_TOTAL_FACTOR = 1.01       # well-rested: 1% scoring bump


def _get_db_path(sport: str) -> Path:
    if sport == "nhl":
        return _DATA_DIR / "nhl.db"
    if sport == "nba":
        return _DATA_DIR / "nba.db"
    raise ValueError(f"Unsupported sport: {sport}")


def _get_games_table(sport: str) -> str:
    return "nhl_games" if sport == "nhl" else "nba_games"


def _get_teams_table(sport: str) -> str:
    return "nhl_teams" if sport == "nhl" else "nba_teams"


def infer_series(sport: str, home_abbr: str, away_abbr: str,
                 today: datetime | None = None) -> dict[str, Any]:
    """Infer the current playoff series state from recent games.

    Looks at games between these two teams in the current playoff
    window (last 30 days, game_type=3 for NHL or during April-June
    for NBA). Counts wins for each side to determine series score
    and game number.

    Returns:
        {
            "in_series": True/False,
            "game_number": 1-7,
            "home_wins": int,
            "away_wins": int,
            "series_leader": "home" | "away" | "tied",
            "home_is_desperate": bool,  # facing elimination
            "away_is_desperate": bool,
            "home_can_close": bool,     # can win series this game
            "away_can_close": bool,
            "is_elimination": bool,     # either team facing elim
            "is_closeout": bool,        # either team can close
            "is_game1": bool,
            "is_tied": bool,
            "rest_days": int | None,    # days since last game in series
        }
    """
    if not ENABLE_SERIES_CONTEXT:
        return {"in_series": False}

    if today is None:
        today = datetime.now()

    result: dict[str, Any] = {
        "in_series": False,
        "game_number": 0,
        "home_wins": 0,
        "away_wins": 0,
        "series_leader": "tied",
        "home_is_desperate": False,
        "away_is_desperate": False,
        "home_can_close": False,
        "away_can_close": False,
        "is_elimination": False,
        "is_closeout": False,
        "is_game1": False,
        "is_tied": False,
        "rest_days": None,
    }

    db_path = _get_db_path(sport)
    if not db_path.exists():
        return result

    games_table = _get_games_table(sport)
    teams_table = _get_teams_table(sport)

    # Abbreviation aliases — some scrapers use different abbreviations
    # than the DB (e.g. Hard Rock "TB" vs DB "TBL").
    # Map scraper abbreviations to DB abbreviations (and vice versa).
    # NHL DB uses: TBL, LAK, SJS, NJD
    # NBA DB uses: NY, GS, NO, SA
    _ABBR_ALIASES: dict[str, str] = {
        # NHL aliases
        "TB": "TBL", "TBL": "TBL",
        "SJ": "SJS", "SJS": "SJS",
        "LA": "LAK", "LAK": "LAK",
        "NJ": "NJD", "NJD": "NJD",
        # NBA aliases (DB uses short forms)
        "NYK": "NY", "NY": "NY",
        "GSW": "GS", "GS": "GS",
        "NOP": "NO", "NO": "NO",
        "SAS": "SA", "SA": "SA",
        "UTA": "UTAH", "UTAH": "UTAH",
        "WAS": "WSH", "WSH": "WSH",
    }
    home_db_abbr = _ABBR_ALIASES.get(home_abbr, home_abbr)
    away_db_abbr = _ABBR_ALIASES.get(away_abbr, away_abbr)

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        # Resolve team IDs from abbreviations. NHL UTA was rebranded
        # from Arizona Coyotes mid-2024 and the league re-issued the
        # team_id from 53 to 68 — both can appear in nhl_teams. Pull
        # ALL ids that match the abbreviation so the games-table JOIN
        # below catches both old- and new-id rows for the same team.
        home_rows = conn.execute(
            f"SELECT id FROM {teams_table} WHERE abbreviation IN (?, ?)",
            (home_abbr, home_db_abbr)).fetchall()
        away_rows = conn.execute(
            f"SELECT id FROM {teams_table} WHERE abbreviation IN (?, ?)",
            (away_abbr, away_db_abbr)).fetchall()
        if not home_rows or not away_rows:
            conn.close()
            return result

        home_ids = [r["id"] for r in home_rows]
        away_ids = [r["id"] for r in away_rows]
        home_id = home_ids[0]  # canonical (used for win-tally home/away comparisons)
        away_id = away_ids[0]

        # Find recent PLAYOFF games between these two teams.
        # Use game_type=3 for NHL (playoff). For NBA the DB may not
        # have game_type, so fall back to date-based filtering (only
        # games after April 15 of the current year).
        today_str = today.strftime("%Y-%m-%d")

        # Build game_type filter if the column exists.
        # NHL has game_type=3 for playoffs.
        # NBA doesn't have game_type, so we use a tighter date window
        # starting from the playoff start date (~April 18) to avoid
        # counting late-season regular-season games between the same teams.
        game_type_filter = ""
        if sport == "nhl":
            game_type_filter = "AND game_type = 3"
            cutoff = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        elif sport == "nba":
            # NBA/NHL playoffs start around April 18. Use the current
            # year's playoff start as cutoff to exclude regular-season
            # games between the same teams.
            playoff_start = f"{today.year}-04-18"
            cutoff = playoff_start
        else:
            cutoff = (today - timedelta(days=30)).strftime("%Y-%m-%d")

        # Count any past game between these two teams in the window —
        # NOT just games with scores in our DB. Otherwise a missed
        # score sync (which happens during late-night NHL playoff
        # finals) makes today's game look one earlier in the series
        # than it actually is. The win tally is computed below from
        # whichever subset DOES have scores; the game_number itself
        # reflects how many games have been *played*.
        # Build IN-list placeholders so a team with multiple ids (UTA
        # 53/68) catches games stamped with either ID.
        h_ph = ",".join("?" * len(home_ids))
        a_ph = ",".join("?" * len(away_ids))
        query = f"""
            SELECT date, home_team_id, away_team_id, home_score, away_score, status
            FROM {games_table}
            WHERE date >= ? AND date < ?
              AND ((home_team_id IN ({h_ph}) AND away_team_id IN ({a_ph}))
                OR (home_team_id IN ({a_ph}) AND away_team_id IN ({h_ph})))
              {game_type_filter}
            ORDER BY date ASC
        """
        params = [cutoff, today_str] + home_ids + away_ids + away_ids + home_ids
        rows = conn.execute(query, params).fetchall()
        conn.close()

        if not rows:
            # No prior games — this is Game 1
            result["in_series"] = True
            result["game_number"] = 1
            result["is_game1"] = True
            result["is_tied"] = True
            return result

        # Count wins for each team (by abbreviation, not home/away).
        # Track played games separately from games-with-known-result so
        # the game_number is correct even when one of the prior games
        # is missing a score in our DB.
        home_team_wins = 0
        away_team_wins = 0
        played_games = 0
        last_game_date = None

        for row in rows:
            game_home_id = row["home_team_id"]
            h_score = row["home_score"]
            a_score = row["away_score"]
            played_games += 1
            last_game_date = row["date"]

            # Tally wins only when both scores are known. Missing-score
            # games still count toward played_games (the game number)
            # but are skipped for the home_wins / away_wins display.
            if h_score is None or a_score is None or h_score == a_score:
                continue

            # Compare against the SET of home_ids (handles UTA 53/68
            # and any other multi-id team).
            if game_home_id in home_ids:
                # Today's home team was home in this game
                if h_score > a_score:
                    home_team_wins += 1
                else:
                    away_team_wins += 1
            else:
                # Today's home team was away in this game
                if a_score > h_score:
                    home_team_wins += 1
                else:
                    away_team_wins += 1

        # game_number reflects games actually played (incl. ones we
        # missed scores for) so the UI shows the correct round-number
        # even when sync hasn't caught up.
        game_number = played_games + 1  # +1 for today

        # Calculate rest days
        if last_game_date:
            try:
                last_dt = datetime.strptime(last_game_date, "%Y-%m-%d")
                result["rest_days"] = (today - last_dt).days - 1  # subtract 1 (game day doesn't count)
            except (ValueError, TypeError):
                pass

        result["in_series"] = True
        result["game_number"] = min(game_number, 7)
        result["home_wins"] = home_team_wins
        result["away_wins"] = away_team_wins

        # Series leader
        if home_team_wins > away_team_wins:
            result["series_leader"] = "home"
        elif away_team_wins > home_team_wins:
            result["series_leader"] = "away"
        else:
            result["series_leader"] = "tied"
            result["is_tied"] = True

        result["is_game1"] = game_number == 1

        # Elimination: a team needs 4 wins. Facing elim = opponent has 3
        result["home_is_desperate"] = away_team_wins == 3
        result["away_is_desperate"] = home_team_wins == 3
        result["is_elimination"] = result["home_is_desperate"] or result["away_is_desperate"]

        # Closeout: a team has 3 wins and can end it
        result["home_can_close"] = home_team_wins == 3
        result["away_can_close"] = away_team_wins == 3
        result["is_closeout"] = result["home_can_close"] or result["away_can_close"]

        # Tied series check (including 0-0 which we handled above)
        result["is_tied"] = home_team_wins == away_team_wins

        logger.info(
            "Series %s@%s: Game %d, %s leads %d-%d%s%s",
            away_abbr, home_abbr, game_number,
            result["series_leader"], home_team_wins, away_team_wins,
            " [ELIMINATION]" if result["is_elimination"] else "",
            " [CLOSEOUT]" if result["is_closeout"] else "",
        )

    except Exception as e:
        logger.warning("Series context error for %s@%s: %s", away_abbr, home_abbr, e)

    return result


def apply_series_adjustments(
    sport: str,
    home_xg: float,
    away_xg: float,
    home_edge: float,
    series: dict[str, Any],
) -> tuple[float, float, float, list[str]]:
    """Apply series-aware adjustments to expected goals/points and home edge.

    Args:
        sport: "nhl" or "nba"
        home_xg: home expected goals/points (pre-adjustment)
        away_xg: away expected goals/points (pre-adjustment)
        home_edge: current home edge value
        series: dict from infer_series()

    Returns:
        (adjusted_home_xg, adjusted_away_xg, adjusted_home_edge, reasoning_lines)
    """
    if not series.get("in_series") or not ENABLE_SERIES_CONTEXT:
        return home_xg, away_xg, home_edge, []

    reasons: list[str] = []
    gn = series["game_number"]
    hw = series["home_wins"]
    aw = series["away_wins"]

    # ── Game 1: home team historically wins ~58% ──
    if series["is_game1"]:
        home_edge += GAME1_HOME_BOOST
        reasons.append(
            f"Game 1 home boost (+{GAME1_HOME_BOOST:.0%} home edge; "
            f"~58% historical home Game 1 win rate)"
        )

    # ── Series tied: home court/ice matters more ──
    elif series["is_tied"] and gn > 1:
        home_edge += TIED_SERIES_HOME_BOOST
        reasons.append(
            f"Series tied {hw}-{aw}, home edge boosted "
            f"(+{TIED_SERIES_HOME_BOOST:.0%})"
        )

    # ── Elimination game ──
    if series["is_elimination"]:
        # Boost is intentionally 0 (see ELIMINATION_UNDERDOG_BOOST
        # comment). Math still runs so future tuning lights up
        # automatically; reasoning lines suppressed when boost is 0
        # so the user doesn't see "+0% xG boost" cruft on cards.
        if series["home_is_desperate"]:
            home_xg *= (1 + ELIMINATION_UNDERDOG_BOOST)
            away_xg *= (1 - ELIMINATION_UNDERDOG_BOOST * 0.5)
            if ELIMINATION_UNDERDOG_BOOST > 0:
                reasons.append(
                    f"Home facing elimination (down {hw}-{aw}): "
                    f"desperate team boost (+{ELIMINATION_UNDERDOG_BOOST:.0%} xG)"
                )
        if series["away_is_desperate"]:
            away_xg *= (1 + ELIMINATION_UNDERDOG_BOOST)
            home_xg *= (1 - ELIMINATION_UNDERDOG_BOOST * 0.5)
            if ELIMINATION_UNDERDOG_BOOST > 0:
                reasons.append(
                    f"Away facing elimination (down {aw}-{hw}): "
                    f"desperate team boost (+{ELIMINATION_UNDERDOG_BOOST:.0%} xG)"
                )
        home_xg *= ELIMINATION_TOTAL_BUMP
        away_xg *= ELIMINATION_TOTAL_BUMP
        if ELIMINATION_TOTAL_BUMP != 1.0:
            reasons.append(
                f"Elimination game scoring bump "
                f"(x{ELIMINATION_TOTAL_BUMP:.2f})"
            )

    # ── Closeout game (not also elimination — that's Game 7) ──
    elif series["is_closeout"] and not series["is_elimination"]:
        if series["home_can_close"]:
            # Home can close it out — slight fade
            home_xg *= (1 - CLOSEOUT_FAVORITE_FADE)
            reasons.append(
                f"Home can close series ({hw}-{aw}): "
                f"slight favorite fade (-{CLOSEOUT_FAVORITE_FADE:.0%} xG)"
            )
        if series["away_can_close"]:
            away_xg *= (1 - CLOSEOUT_FAVORITE_FADE)
            reasons.append(
                f"Away can close series ({aw}-{hw}): "
                f"slight favorite fade (-{CLOSEOUT_FAVORITE_FADE:.0%} xG)"
            )
        # Closeout games tend to be slightly lower scoring
        home_xg *= CLOSEOUT_TOTAL_DIP
        away_xg *= CLOSEOUT_TOTAL_DIP
        reasons.append(
            f"Closeout game scoring dip "
            f"(x{CLOSEOUT_TOTAL_DIP:.2f})"
        )

    # ── Rest days ──
    rest = series.get("rest_days")
    if rest is not None:
        if rest == 0:
            home_xg *= REST_0_TOTAL_FACTOR
            away_xg *= REST_0_TOTAL_FACTOR
            home_edge -= REST_0_AWAY_BOOST
            reasons.append(
                f"Back-to-back games: scoring drop "
                f"(x{REST_0_TOTAL_FACTOR:.2f}), slight away edge"
            )
        elif rest >= 2:
            home_xg *= REST_2PLUS_TOTAL_FACTOR
            away_xg *= REST_2PLUS_TOTAL_FACTOR
            reasons.append(
                f"{rest} days rest: well-rested scoring bump "
                f"(x{REST_2PLUS_TOTAL_FACTOR:.2f})"
            )

    return home_xg, away_xg, home_edge, reasons
