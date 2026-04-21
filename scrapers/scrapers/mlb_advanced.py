"""
Advanced MLB stats via pybaseball.

Pulls Statcast data, FanGraphs advanced metrics, batter-vs-pitcher
H2H matchups, and park factors.

Requires: pip install pybaseball

Usage:
    python -m scrapers.mlb_advanced                 # All advanced data
    python -m scrapers.mlb_advanced --statcast      # Statcast only
    python -m scrapers.mlb_advanced --h2h           # H2H matchups only
    python -m scrapers.mlb_advanced --parks         # Park factors only
    python -m scrapers.mlb_advanced --fangraphs     # FanGraphs team stats
"""

import logging
import time
from datetime import datetime

import pybaseball

logger = logging.getLogger(__name__)

SEASON = datetime.now().year

# Suppress pybaseball's own logging noise
pybaseball.cache.enable()


# ── Statcast Pitcher Data ───────────────────────────────────

def sync_statcast_pitchers(season: int | None = None):
    """
    Pull Statcast pitching data: avg velocity, spin rate, whiff%,
    barrel% against, hard-hit% against, xERA.
    """
    yr = season or SEASON
    logger.info("Fetching Statcast pitching leaderboard for %d...", yr)

    try:
        df = pybaseball.statcast_pitcher_exitvelo_barrels(yr, minBBE=50)
    except Exception as e:
        logger.error("Failed to fetch Statcast pitcher data: %s", e)
        return

    if df is None or df.empty:
        logger.warning("No Statcast pitcher data returned")
        return

    from engine.db import get_conn
    conn = get_conn()

    updated = 0
    for _, row in df.iterrows():
        player_id = int(row.get("player_id", 0))
        if not player_id:
            continue

        barrel_pct = _safe_float(row.get("barrel_batted_rate"))
        hard_hit = _safe_float(row.get("hard_hit_percent"))
        avg_velo = _safe_float(row.get("avg_hit_speed"))  # exit velo against

        conn.execute("""
            UPDATE pitcher_stats SET
                barrel_pct_against = COALESCE(?, barrel_pct_against),
                hard_hit_pct_against = COALESCE(?, hard_hit_pct_against),
                updated_at = datetime('now')
            WHERE player_id = ? AND season = ?
        """, (barrel_pct, hard_hit, player_id, yr))
        updated += 1

    conn.commit()
    logger.info("Updated Statcast data for %d pitchers", updated)

    # Also get pitch velocity/spin from statcast
    try:
        pitch_df = pybaseball.statcast_pitcher_spin(yr)
        if pitch_df is not None and not pitch_df.empty:
            for _, row in pitch_df.iterrows():
                player_id = int(row.get("player_id", 0))
                if not player_id:
                    continue
                avg_spin = _safe_float(row.get("spin_rate"))
                conn.execute("""
                    UPDATE pitcher_stats SET
                        spin_rate = COALESCE(?, spin_rate),
                        updated_at = datetime('now')
                    WHERE player_id = ? AND season = ?
                """, (avg_spin, player_id, yr))
            conn.commit()
    except Exception as e:
        logger.warning("Could not fetch pitcher spin data: %s", e)


# ── Statcast Batter Data ───────────────────────────────────

def sync_statcast_batters(season: int | None = None):
    """
    Pull Statcast batting data: exit velo, barrel%, hard-hit%,
    launch angle, xBA, xSLG, xwOBA.
    """
    yr = season or SEASON
    logger.info("Fetching Statcast batting leaderboard for %d...", yr)

    try:
        df = pybaseball.statcast_batter_exitvelo_barrels(yr, minBBE=50)
    except Exception as e:
        logger.error("Failed to fetch Statcast batter data: %s", e)
        return

    if df is None or df.empty:
        logger.warning("No Statcast batter data returned")
        return

    from engine.db import get_conn
    conn = get_conn()

    updated = 0
    for _, row in df.iterrows():
        player_id = int(row.get("player_id", 0))
        if not player_id:
            continue

        conn.execute("""
            UPDATE batter_stats SET
                avg_exit_velo = COALESCE(?, avg_exit_velo),
                max_exit_velo = COALESCE(?, max_exit_velo),
                barrel_pct = COALESCE(?, barrel_pct),
                hard_hit_pct = COALESCE(?, hard_hit_pct),
                launch_angle = COALESCE(?, launch_angle),
                updated_at = datetime('now')
            WHERE player_id = ? AND season = ?
        """, (
            _safe_float(row.get("avg_hit_speed")),
            _safe_float(row.get("max_hit_speed")),
            _safe_float(row.get("barrel_batted_rate")),
            _safe_float(row.get("hard_hit_percent")),
            _safe_float(row.get("avg_hit_angle")),
            player_id, yr,
        ))
        updated += 1

    conn.commit()
    logger.info("Updated Statcast data for %d batters", updated)

    # xBA, xSLG, xwOBA from expected stats
    try:
        xdf = pybaseball.statcast_batter_expected_stats(yr, minPA=50)
        if xdf is not None and not xdf.empty:
            for _, row in xdf.iterrows():
                player_id = int(row.get("player_id", 0))
                if not player_id:
                    continue
                conn.execute("""
                    UPDATE batter_stats SET
                        xba = COALESCE(?, xba),
                        xslg = COALESCE(?, xslg),
                        xwoba = COALESCE(?, xwoba),
                        updated_at = datetime('now')
                    WHERE player_id = ? AND season = ?
                """, (
                    _safe_float(row.get("est_ba")),
                    _safe_float(row.get("est_slg")),
                    _safe_float(row.get("est_woba")),
                    player_id, yr,
                ))
            conn.commit()
            logger.info("Updated expected stats for batters")
    except Exception as e:
        logger.warning("Could not fetch expected batting stats: %s", e)


# ── FanGraphs Team Stats ───────────────────────────────────

def sync_fangraphs_team_stats(season: int | None = None):
    """Pull team-level advanced stats from FanGraphs via pybaseball."""
    yr = season or SEASON
    logger.info("Fetching FanGraphs team batting stats for %d...", yr)

    from engine.db import get_conn, get_all_teams
    conn = get_conn()

    # Map FanGraphs team names to our team IDs
    teams = get_all_teams()
    name_map = {}
    for t in teams:
        # FanGraphs uses different names sometimes
        name_map[t["name"]] = t["mlb_id"]
        name_map[t["abbreviation"]] = t["mlb_id"]

    # Team batting
    try:
        bat_df = pybaseball.team_batting(yr)
        if bat_df is not None and not bat_df.empty:
            for _, row in bat_df.iterrows():
                team_name = row.get("Team", "")
                team_id = _resolve_team(team_name, name_map)
                if not team_id:
                    continue

                conn.execute("""
                    UPDATE team_stats SET
                        avg = COALESCE(?, avg),
                        obp = COALESCE(?, obp),
                        slg = COALESCE(?, slg),
                        ops = COALESCE(?, ops),
                        wrc_plus = COALESCE(?, wrc_plus),
                        iso = COALESCE(?, iso),
                        babip = COALESCE(?, babip),
                        k_pct = COALESCE(?, k_pct),
                        bb_pct = COALESCE(?, bb_pct),
                        updated_at = datetime('now')
                    WHERE team_id = ? AND season = ?
                """, (
                    _safe_float(row.get("AVG")),
                    _safe_float(row.get("OBP")),
                    _safe_float(row.get("SLG")),
                    _safe_float(row.get("OPS")),
                    _safe_float(row.get("wRC+")),
                    _safe_float(row.get("ISO")),
                    _safe_float(row.get("BABIP")),
                    _pct_to_float(row.get("K%")),
                    _pct_to_float(row.get("BB%")),
                    team_id, yr,
                ))
            conn.commit()
            logger.info("Updated FanGraphs batting for %d teams", len(bat_df))
    except Exception as e:
        logger.error("Failed to fetch FanGraphs team batting: %s", e)

    # Team pitching
    try:
        pitch_df = pybaseball.team_pitching(yr)
        if pitch_df is not None and not pitch_df.empty:
            for _, row in pitch_df.iterrows():
                team_name = row.get("Team", "")
                team_id = _resolve_team(team_name, name_map)
                if not team_id:
                    continue

                conn.execute("""
                    UPDATE team_stats SET
                        era = COALESCE(?, era),
                        whip = COALESCE(?, whip),
                        fip = COALESCE(?, fip),
                        k_per_9 = COALESCE(?, k_per_9),
                        bb_per_9 = COALESCE(?, bb_per_9),
                        updated_at = datetime('now')
                    WHERE team_id = ? AND season = ?
                """, (
                    _safe_float(row.get("ERA")),
                    _safe_float(row.get("WHIP")),
                    _safe_float(row.get("FIP")),
                    _safe_float(row.get("K/9")),
                    _safe_float(row.get("BB/9")),
                    team_id, yr,
                ))
            conn.commit()
            logger.info("Updated FanGraphs pitching for %d teams", len(pitch_df))
    except Exception as e:
        logger.error("Failed to fetch FanGraphs team pitching: %s", e)


# ── Batter vs Pitcher H2H ──────────────────────────────────

def sync_h2h_for_game(home_team_id: int, away_team_id: int,
                       home_pitcher_id: int | None, away_pitcher_id: int | None):
    """
    Fetch batter-vs-pitcher matchup data for a specific game.
    Gets H2H for home batters vs away pitcher and vice versa.
    """
    from engine.db import get_conn
    conn = get_conn()

    if away_pitcher_id:
        _sync_team_vs_pitcher(conn, home_team_id, away_pitcher_id)
    if home_pitcher_id:
        _sync_team_vs_pitcher(conn, away_team_id, home_pitcher_id)


def _sync_team_vs_pitcher(conn, team_id: int, pitcher_id: int):
    """Fetch H2H data for all batters on a team vs a specific pitcher."""
    # Get active batters on the team
    batters = conn.execute(
        "SELECT mlb_id, name FROM players WHERE team_id = ? AND position != 'P' AND active = 1",
        (team_id,)
    ).fetchall()

    for batter in batters:
        batter_id = batter["mlb_id"]
        try:
            df = pybaseball.playerid_reverse_lookup([batter_id], key_type="mlbam")
            if df is None or df.empty:
                continue

            # Use statcast for H2H
            h2h_df = pybaseball.statcast_batter(
                start_dt="2020-01-01",
                end_dt=datetime.now().strftime("%Y-%m-%d"),
                player_id=batter_id,
            )
            if h2h_df is None or h2h_df.empty:
                continue

            # Filter for this pitcher
            pitcher_data = h2h_df[h2h_df["pitcher"] == pitcher_id]
            if pitcher_data.empty:
                continue

            # Calculate H2H stats from pitch-level data
            abs_data = pitcher_data[pitcher_data["events"].notna()]
            total_ab = len(abs_data[abs_data["events"].isin([
                "single", "double", "triple", "home_run", "field_out",
                "strikeout", "grounded_into_double_play", "force_out",
                "fielders_choice", "field_error", "double_play",
                "strikeout_double_play", "fielders_choice_out",
            ])])
            hits = len(abs_data[abs_data["events"].isin(
                ["single", "double", "triple", "home_run"])])
            doubles = len(abs_data[abs_data["events"] == "double"])
            triples = len(abs_data[abs_data["events"] == "triple"])
            hrs = len(abs_data[abs_data["events"] == "home_run"])
            walks = len(abs_data[abs_data["events"].isin(["walk", "hit_by_pitch"])])
            ks = len(abs_data[abs_data["events"].isin(
                ["strikeout", "strikeout_double_play"])])

            avg = round(hits / total_ab, 3) if total_ab > 0 else None
            pa = total_ab + walks
            obp = round((hits + walks) / pa, 3) if pa > 0 else None
            tb = hits + doubles + 2 * triples + 3 * hrs
            slg = round(tb / total_ab, 3) if total_ab > 0 else None
            ops = round((obp or 0) + (slg or 0), 3) if obp and slg else None

            conn.execute("""
                INSERT INTO h2h_matchups (batter_id, pitcher_id,
                    at_bats, hits, doubles, triples, home_runs,
                    walks, strikeouts, avg, ops)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(batter_id, pitcher_id) DO UPDATE SET
                    at_bats=excluded.at_bats, hits=excluded.hits,
                    doubles=excluded.doubles, triples=excluded.triples,
                    home_runs=excluded.home_runs, walks=excluded.walks,
                    strikeouts=excluded.strikeouts, avg=excluded.avg,
                    ops=excluded.ops, updated_at=datetime('now')
            """, (batter_id, pitcher_id, total_ab, hits, doubles, triples,
                  hrs, walks, ks, avg, ops))

        except Exception as e:
            logger.debug("H2H fetch failed for batter %d vs pitcher %d: %s",
                        batter_id, pitcher_id, e)
            continue

        time.sleep(0.3)  # Rate limiting

    conn.commit()


# ── Park Factors ────────────────────────────────────────────

# From FanGraphs historical data — these are relatively stable year to year.
# Values > 100 = hitter-friendly, < 100 = pitcher-friendly.
# Normalized: 1.0 = neutral, >1.0 = more runs, <1.0 = fewer runs.
PARK_FACTORS = {
    "Coors Field":          {"run": 1.15, "hr": 1.20, "h": 1.10},
    "Globe Life Field":     {"run": 1.08, "hr": 1.12, "h": 1.05},
    "Fenway Park":          {"run": 1.07, "hr": 0.98, "h": 1.10},
    "Great American Ball Park": {"run": 1.06, "hr": 1.15, "h": 1.03},
    "Yankee Stadium":       {"run": 1.05, "hr": 1.15, "h": 1.00},
    "Citizens Bank Park":   {"run": 1.04, "hr": 1.08, "h": 1.03},
    "Wrigley Field":        {"run": 1.03, "hr": 1.05, "h": 1.02},
    "Guaranteed Rate Field":{"run": 1.02, "hr": 1.08, "h": 1.00},
    "Minute Maid Park":     {"run": 1.01, "hr": 1.03, "h": 1.00},
    "Rogers Centre":        {"run": 1.01, "hr": 1.05, "h": 0.99},
    "Busch Stadium":        {"run": 1.00, "hr": 1.00, "h": 1.00},
    "Chase Field":          {"run": 1.00, "hr": 1.02, "h": 1.00},
    "Angel Stadium":        {"run": 0.99, "hr": 1.00, "h": 0.99},
    "Target Field":         {"run": 0.99, "hr": 1.00, "h": 0.99},
    "American Family Field":{"run": 0.99, "hr": 1.02, "h": 0.98},
    "Dodger Stadium":       {"run": 0.98, "hr": 1.00, "h": 0.97},
    "Kauffman Stadium":     {"run": 0.98, "hr": 0.95, "h": 1.00},
    "PNC Park":             {"run": 0.97, "hr": 0.95, "h": 0.98},
    "Nationals Park":       {"run": 0.97, "hr": 1.00, "h": 0.96},
    "Truist Park":          {"run": 0.97, "hr": 0.98, "h": 0.97},
    "Comerica Park":        {"run": 0.96, "hr": 0.92, "h": 0.98},
    "loanDepot park":       {"run": 0.96, "hr": 0.93, "h": 0.97},
    "T-Mobile Park":        {"run": 0.95, "hr": 0.93, "h": 0.96},
    "Oracle Park":          {"run": 0.94, "hr": 0.88, "h": 0.96},
    "Tropicana Field":      {"run": 0.94, "hr": 0.90, "h": 0.96},
    "Petco Park":           {"run": 0.93, "hr": 0.90, "h": 0.95},
    "Citi Field":           {"run": 0.93, "hr": 0.92, "h": 0.94},
    "Oakland Coliseum":     {"run": 0.92, "hr": 0.88, "h": 0.95},
    "Progressive Field":    {"run": 0.97, "hr": 0.98, "h": 0.97},
}


def sync_park_factors():
    """Store park factors into the database."""
    from engine.db import get_conn, get_all_teams
    conn = get_conn()

    teams = get_all_teams()
    venue_to_team = {t["venue"]: t["mlb_id"] for t in teams if t.get("venue")}

    for venue, factors in PARK_FACTORS.items():
        team_id = venue_to_team.get(venue)
        conn.execute("""
            INSERT INTO park_factors (venue, team_id, season, run_factor, hr_factor, h_factor)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(venue, season) DO UPDATE SET
                team_id=excluded.team_id,
                run_factor=excluded.run_factor, hr_factor=excluded.hr_factor,
                h_factor=excluded.h_factor, updated_at=datetime('now')
        """, (venue, team_id, SEASON, factors["run"], factors["hr"], factors["h"]))

    conn.commit()
    logger.info("Stored park factors for %d venues", len(PARK_FACTORS))


# ── FanGraphs Individual Pitcher Advanced ───────────────────

def sync_fangraphs_pitchers(season: int | None = None):
    """Pull FIP, xFIP, BABIP, LOB%, GB%, HR/FB from FanGraphs."""
    yr = season or SEASON
    logger.info("Fetching FanGraphs pitcher leaderboard for %d...", yr)

    try:
        df = pybaseball.pitching_stats(yr, qual=20)
    except Exception as e:
        logger.error("Failed to fetch FanGraphs pitching: %s", e)
        return

    if df is None or df.empty:
        return

    from engine.db import get_conn
    conn = get_conn()

    updated = 0
    for _, row in df.iterrows():
        # pybaseball uses 'IDfg' for FanGraphs ID, we need MLBAM ID
        mlbam_id = _safe_int(row.get("MLBAMID") or row.get("mlbam_id"))
        if not mlbam_id:
            # Try to look up by name
            name = row.get("Name", "")
            result = conn.execute(
                "SELECT mlb_id FROM players WHERE name = ? LIMIT 1", (name,)
            ).fetchone()
            if result:
                mlbam_id = result["mlb_id"]
            else:
                continue

        conn.execute("""
            UPDATE pitcher_stats SET
                fip = COALESCE(?, fip),
                x_fip = COALESCE(?, x_fip),
                babip = COALESCE(?, babip),
                lob_pct = COALESCE(?, lob_pct),
                gb_pct = COALESCE(?, gb_pct),
                hr_per_fb = COALESCE(?, hr_per_fb),
                k_pct = COALESCE(?, k_pct),
                bb_pct = COALESCE(?, bb_pct),
                updated_at = datetime('now')
            WHERE player_id = ? AND season = ?
        """, (
            _safe_float(row.get("FIP")),
            _safe_float(row.get("xFIP")),
            _safe_float(row.get("BABIP")),
            _pct_to_float(row.get("LOB%")),
            _pct_to_float(row.get("GB%")),
            _pct_to_float(row.get("HR/FB")),
            _pct_to_float(row.get("K%")),
            _pct_to_float(row.get("BB%")),
            mlbam_id, yr,
        ))
        updated += 1

    conn.commit()
    logger.info("Updated FanGraphs advanced stats for %d pitchers", updated)


def sync_fangraphs_batters(season: int | None = None):
    """Pull wRC+, wOBA, WAR, ISO, BABIP from FanGraphs."""
    yr = season or SEASON
    logger.info("Fetching FanGraphs batter leaderboard for %d...", yr)

    try:
        df = pybaseball.batting_stats(yr, qual=50)
    except Exception as e:
        logger.error("Failed to fetch FanGraphs batting: %s", e)
        return

    if df is None or df.empty:
        return

    from engine.db import get_conn
    conn = get_conn()

    updated = 0
    for _, row in df.iterrows():
        mlbam_id = _safe_int(row.get("MLBAMID") or row.get("mlbam_id"))
        if not mlbam_id:
            name = row.get("Name", "")
            result = conn.execute(
                "SELECT mlb_id FROM players WHERE name = ? LIMIT 1", (name,)
            ).fetchone()
            if result:
                mlbam_id = result["mlb_id"]
            else:
                continue

        conn.execute("""
            UPDATE batter_stats SET
                wrc_plus = COALESCE(?, wrc_plus),
                woba = COALESCE(?, woba),
                war = COALESCE(?, war),
                iso = COALESCE(?, iso),
                babip = COALESCE(?, babip),
                k_pct = COALESCE(?, k_pct),
                bb_pct = COALESCE(?, bb_pct),
                updated_at = datetime('now')
            WHERE player_id = ? AND season = ?
        """, (
            _safe_float(row.get("wRC+")),
            _safe_float(row.get("wOBA")),
            _safe_float(row.get("WAR")),
            _safe_float(row.get("ISO")),
            _safe_float(row.get("BABIP")),
            _pct_to_float(row.get("K%")),
            _pct_to_float(row.get("BB%")),
            mlbam_id, yr,
        ))
        updated += 1

    conn.commit()
    logger.info("Updated FanGraphs advanced stats for %d batters", updated)


# ── Helpers ─────────────────────────────────────────────────

def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int:
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _pct_to_float(val) -> float | None:
    """Convert percentage strings like '25.3%' or '0.253' to float."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        # If it's > 1, assume it's already a percentage (like 25.3)
        return float(val) / 100 if float(val) > 1 else float(val)
    s = str(val).strip().rstrip("%")
    try:
        v = float(s)
        return v / 100 if v > 1 else v
    except ValueError:
        return None


# FanGraphs uses abbreviated team names that differ from MLB API names.
_FG_TEAM_MAP = {
    "ARI": "ARI", "ATL": "ATL", "BAL": "BAL", "BOS": "BOS",
    "CHC": "CHC", "CWS": "CWS", "CIN": "CIN", "CLE": "CLE",
    "COL": "COL", "DET": "DET", "HOU": "HOU", "KCR": "KC",
    "LAA": "LAA", "LAD": "LAD", "MIA": "MIA", "MIL": "MIL",
    "MIN": "MIN", "NYM": "NYM", "NYY": "NYY", "OAK": "OAK",
    "PHI": "PHI", "PIT": "PIT", "SDP": "SD", "SFG": "SF",
    "SEA": "SEA", "STL": "STL", "TBR": "TB", "TEX": "TEX",
    "TOR": "TOR", "WSN": "WSH",
    # Reverse mappings
    "KC": "KC", "SD": "SD", "SF": "SF", "TB": "TB", "WSH": "WSH",
}


def _resolve_team(name_or_abbr: str, name_map: dict) -> int | None:
    """Try to resolve a team name/abbr to our mlb_id."""
    if not name_or_abbr:
        return None

    # Direct match
    if name_or_abbr in name_map:
        return name_map[name_or_abbr]

    # Try FanGraphs abbreviation mapping
    mapped = _FG_TEAM_MAP.get(name_or_abbr)
    if mapped and mapped in name_map:
        return name_map[mapped]

    # Fuzzy match on team name
    lower = name_or_abbr.lower()
    for key, team_id in name_map.items():
        if lower in key.lower() or key.lower() in lower:
            return team_id

    return None


# ── Full Advanced Sync ──────────────────────────────────────

def full_advanced_sync():
    """Run all advanced data syncs."""
    logger.info("=== MLB Advanced Stats Sync ===")

    logger.info("--- Park Factors ---")
    sync_park_factors()

    logger.info("--- FanGraphs Team Stats ---")
    sync_fangraphs_team_stats()

    logger.info("--- FanGraphs Pitcher Advanced ---")
    sync_fangraphs_pitchers()

    logger.info("--- FanGraphs Batter Advanced ---")
    sync_fangraphs_batters()

    logger.info("--- Statcast Pitchers ---")
    sync_statcast_pitchers()

    logger.info("--- Statcast Batters ---")
    sync_statcast_batters()

    logger.info("=== Advanced sync complete ===")


# ── CLI ─────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("data/logs/mlb_advanced.log", mode="a"),
        ]
    )

    args = set(sys.argv[1:])

    if "--statcast" in args:
        sync_statcast_pitchers()
        sync_statcast_batters()
    elif "--h2h" in args:
        logger.info("H2H sync requires specific game context. Use full sync.")
    elif "--parks" in args:
        sync_park_factors()
    elif "--fangraphs" in args:
        sync_fangraphs_team_stats()
        sync_fangraphs_pitchers()
        sync_fangraphs_batters()
    else:
        full_advanced_sync()
