"""
MLB Monte Carlo runner -- glue between engine.mc_mlb and the rest of
the app. Loads lineup / pitcher / bullpen data from the MLB DB, builds
HitterProfile / PitcherProfile objects, invokes the simulator, and
returns the aggregated market-probability dict that /api/predict will
surface on the prediction payload.

Kept separate from engine.mc_mlb so the simulator itself stays pure
and unit-testable with synthetic inputs.
"""

from __future__ import annotations

import logging
from datetime import datetime

from .db import (
    get_conn, get_team_by_id, get_pitcher_season, get_bullpen,
    get_park_factor, get_recent_games,
)
from .mc_mlb import (
    HitterProfile, PitcherProfile, TeamProfile,
    simulate_games, LEAGUE_K_PCT, LEAGUE_BB_PCT, LEAGUE_HR_RATE,
    LEAGUE_BABIP, LEAGUE_ISO,
)
from .mc_sim import aggregate_mc_outcomes

logger = logging.getLogger(__name__)

SEASON = datetime.now().year


def run_mlb_mc(home_team_id: int, away_team_id: int,
                home_pitcher_id: int | None,
                away_pitcher_id: int | None,
                venue: str | None = None,
                n_sims: int = 50_000,
                seed: int | None = None) -> dict:
    """Load DB data, run the simulator, return aggregated market probs.

    Return shape matches what engine.mc_sim.aggregate_mc_outcomes produces
    so the prediction endpoint can drop it onto the response under a
    ``mc`` key alongside the factor-model output.
    """
    conn = get_conn()

    home_profile = _build_team_profile(
        conn, home_team_id, home_pitcher_id, venue, is_home=True,
    )
    away_profile = _build_team_profile(
        conn, away_team_id, away_pitcher_id, venue, is_home=False,
    )

    raw = simulate_games(home_profile, away_profile, n_sims=n_sims, seed=seed)
    agg = aggregate_mc_outcomes(
        raw["home_runs"], raw["away_runs"],
        home_inning_runs=raw["home_inning_runs"],
        away_inning_runs=raw["away_inning_runs"],
    )
    # Attach a minimal metadata block so the UI can display what drove the sim
    agg["meta"] = {
        "home_sp": home_profile.starter.name,
        "away_sp": away_profile.starter.name,
        "park_hr_factor": round(home_profile.park_hr_factor, 3),
        "n_sims": n_sims,
    }
    return agg


def _build_team_profile(conn, team_id: int, pitcher_id: int | None,
                         venue: str | None, is_home: bool) -> TeamProfile:
    """Assemble the TeamProfile for one side of the matchup."""
    starter = _load_pitcher(conn, pitcher_id) if pitcher_id else _fallback_pitcher()
    bullpen = _load_bullpen(conn, team_id)
    lineup = _load_lineup(conn, team_id)
    park_hr = _load_park_hr_factor(venue) if is_home else 1.0
    return TeamProfile(
        lineup=lineup,
        starter=starter,
        bullpen=bullpen,
        park_hr_factor=park_hr,
    )


# ── Pitcher loading ───────────────────────────────────────────

def _load_pitcher(conn, pitcher_id: int) -> PitcherProfile:
    """Load a starter's season stats + handedness. Falls back to league
    average when the row is missing or thin (fewer than 3 starts)."""
    sp = get_pitcher_season(pitcher_id, SEASON) or {}
    throws = _player_throws(conn, pitcher_id)
    gs = sp.get("games_started") or 0
    # IP/GS as the stamina anchor -- league average is ~5.2.
    ip = sp.get("innings") or 0.0
    expected_ip = (ip / gs) if gs >= 3 else 5.2

    if gs < 3:
        # Not enough sample to use this pitcher's rates; use league averages
        # but keep the actual handedness for platoon effects.
        return PitcherProfile(
            handedness=throws or "R",
            expected_innings=expected_ip,
            name=_player_name(conn, pitcher_id) or "?",
        )

    return PitcherProfile(
        k_pct=_safe_rate(sp.get("k_pct"), LEAGUE_K_PCT),
        bb_pct=_safe_rate(sp.get("bb_pct"), LEAGUE_BB_PCT),
        hr_per_pa=_hr_per_pa_from_hr9(sp.get("hr_per_9")),
        babip_against=_safe_rate(sp.get("babip"), LEAGUE_BABIP),
        handedness=throws or "R",
        expected_innings=max(3.0, min(7.5, expected_ip)),
        name=_player_name(conn, pitcher_id) or "?",
    )


def _fallback_pitcher() -> PitcherProfile:
    """When pitcher is TBD, use league-average profile with short stamina."""
    return PitcherProfile(expected_innings=5.0, name="TBD")


def _load_bullpen(conn, team_id: int) -> PitcherProfile:
    """Build a bullpen-average pitcher profile for innings after the SP is out."""
    bp = get_bullpen(team_id, SEASON) or {}
    # Bullpens tend to have HIGHER K rates but also HIGHER BB rates than SPs
    return PitcherProfile(
        k_pct=_safe_rate(bp.get("k_pct"), 0.235),
        bb_pct=_safe_rate(bp.get("bb_pct"), 0.090),
        hr_per_pa=_hr_per_pa_from_hr9(bp.get("hr_per_9"), default=0.032),
        babip_against=_safe_rate(bp.get("babip"), LEAGUE_BABIP),
        handedness="R",  # bullpen is a mix; treat as neutral
        expected_innings=9.0,   # never gets "pulled" within this model
        name="Bullpen",
    )


def _hr_per_pa_from_hr9(hr_per_9: float | None, default: float = LEAGUE_HR_RATE) -> float:
    """Approximate HR per PA from HR/9 using 4.2 PA/inning."""
    if hr_per_9 is None or hr_per_9 <= 0:
        return default
    return max(0.005, min(0.08, hr_per_9 / (9 * 4.2)))


# ── Lineup loading ────────────────────────────────────────────

def _load_lineup(conn, team_id: int) -> list[HitterProfile]:
    """Load the 9-batter lineup for a team.

    We don't have confirmed per-day lineups reliably in the DB, so this
    falls back to the 9 most-used batters in recent games (by PA) for
    the team. For each batter we pull per-season rates + vs-L / vs-R
    OPS splits if available.
    """
    batters = _recent_lineup_ids(conn, team_id) or []
    if len(batters) < 9:
        # Backfill with league-average fillers so the sim always has 9 slots.
        batters = list(batters) + ["?"] * (9 - len(batters))
    lineup: list[HitterProfile] = []
    for bid in batters[:9]:
        if bid == "?":
            lineup.append(_league_avg_hitter("?"))
            continue
        lineup.append(_load_hitter(conn, bid))
    return lineup


def _recent_lineup_ids(conn, team_id: int) -> list[int]:
    """Pull the 9 batters with most at-bats in this team's last 20 games.

    Falls back to the top season-AB batters if recent-games tracking
    isn't populated for this team.
    """
    try:
        rows = conn.execute(
            """
            SELECT player_id, SUM(at_bats) AS ab
            FROM batter_stats
            WHERE team_id = ? AND season = ?
            GROUP BY player_id
            HAVING ab > 20
            ORDER BY ab DESC
            LIMIT 9
            """,
            (team_id, SEASON),
        ).fetchall()
        ids = [r["player_id"] for r in rows]
        if len(ids) >= 8:
            return ids
    except Exception as e:
        logger.debug("lineup fallback for team %s: %s", team_id, e)

    try:
        rows = conn.execute(
            """
            SELECT bs.player_id, bs.at_bats
            FROM batter_stats bs
            WHERE bs.team_id = ? AND bs.season = ?
            ORDER BY bs.at_bats DESC
            LIMIT 9
            """,
            (team_id, SEASON),
        ).fetchall()
        return [r["player_id"] for r in rows]
    except Exception:
        return []


def _load_hitter(conn, player_id) -> HitterProfile:
    """Load per-batter rate stats + platoon splits for the current season."""
    try:
        row = conn.execute(
            "SELECT * FROM batter_stats WHERE player_id = ? AND season = ?",
            (player_id, SEASON),
        ).fetchone()
    except Exception:
        row = None
    if not row:
        return _league_avg_hitter(str(player_id))
    r = dict(row)
    bats = _player_bats(conn, player_id) or "R"
    return HitterProfile(
        k_pct=_safe_rate(r.get("k_pct"), LEAGUE_K_PCT),
        bb_pct=_safe_rate(r.get("bb_pct"), LEAGUE_BB_PCT),
        iso=_safe_rate(r.get("iso"), LEAGUE_ISO, lo=0.02, hi=0.40),
        babip=_safe_rate(r.get("babip"), LEAGUE_BABIP),
        ops_vs_l=r.get("ops_vs_left"),
        ops_vs_r=r.get("ops_vs_right"),
        ops=_safe_rate(r.get("ops"), 0.720, lo=0.400, hi=1.200),
        handedness=bats,
        name=_player_name(conn, player_id) or f"#{player_id}",
    )


def _league_avg_hitter(label: str) -> HitterProfile:
    return HitterProfile(name=label)


# ── Players table helpers (handedness, names) ────────────────

def _player_name(conn, player_id) -> str | None:
    try:
        row = conn.execute(
            "SELECT name FROM players WHERE mlb_id = ?", (player_id,),
        ).fetchone()
        return row["name"] if row else None
    except Exception:
        return None


def _player_throws(conn, player_id) -> str | None:
    try:
        row = conn.execute(
            "SELECT throws FROM players WHERE mlb_id = ?", (player_id,),
        ).fetchone()
        return row["throws"] if row else None
    except Exception:
        return None


def _player_bats(conn, player_id) -> str | None:
    try:
        row = conn.execute(
            "SELECT bats FROM players WHERE mlb_id = ?", (player_id,),
        ).fetchone()
        return row["bats"] if row else None
    except Exception:
        return None


# ── Park factor ───────────────────────────────────────────────

def _load_park_hr_factor(venue: str | None) -> float:
    """Park HR factor from engine.db.get_park_factor."""
    if not venue:
        return 1.0
    try:
        park = get_park_factor(venue) or {}
        # Prefer hr_factor when available, otherwise run_factor as proxy
        return float(park.get("hr_factor")
                     or park.get("run_factor")
                     or 1.0)
    except Exception:
        return 1.0


# ── Rate-value sanitization ──────────────────────────────────

def _safe_rate(v, default: float, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    if x <= 0 or x != x:   # NaN check
        return default
    return max(lo, min(hi, x))
