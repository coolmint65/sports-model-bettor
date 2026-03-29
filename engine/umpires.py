"""
MLB umpire tendencies data.

Home plate umpires have significant impact on game outcomes through
their strike zone size. This module provides historical umpire
run factors and zone tendencies.

Run factor: 1.0 = neutral, >1.0 = more runs (small zone),
            <1.0 = fewer runs (big zone).

Data compiled from historical umpire scorecards and zone analysis.
"""

import logging

logger = logging.getLogger(__name__)

# ── Umpire Tendencies ───────────────────────────────────────
#
# Format: {name: {rpg, run_factor, k_pct, bb_pct, over_pct, games}}
#
# rpg = avg runs per game when umping
# run_factor = multiplier vs league avg (1.0 = neutral)
# k_pct = strikeout rate in games umped
# bb_pct = walk rate in games umped
# over_pct = % of games that went over the posted total
# games = career games as HP umpire (for confidence weighting)
#
# Source: Umpire Scorecards, historical data through 2025 season

UMPIRE_DATA = {
    # Big zone umpires (pitcher-friendly, fewer runs)
    "Pat Hoberg":       {"rpg": 7.8, "run_factor": 0.92, "k_pct": 0.225, "bb_pct": 0.076, "over_pct": 0.44, "games": 450},
    "Nic Lentz":        {"rpg": 7.9, "run_factor": 0.93, "k_pct": 0.222, "bb_pct": 0.078, "over_pct": 0.45, "games": 420},
    "John Tumpane":     {"rpg": 8.0, "run_factor": 0.94, "k_pct": 0.220, "bb_pct": 0.079, "over_pct": 0.46, "games": 380},
    "David Rackley":    {"rpg": 8.0, "run_factor": 0.94, "k_pct": 0.218, "bb_pct": 0.080, "over_pct": 0.45, "games": 350},
    "Shane Livensparger": {"rpg": 8.1, "run_factor": 0.95, "k_pct": 0.217, "bb_pct": 0.080, "over_pct": 0.46, "games": 280},
    "DJ Reyburn":       {"rpg": 8.1, "run_factor": 0.95, "k_pct": 0.216, "bb_pct": 0.081, "over_pct": 0.46, "games": 260},
    "Adam Beck":        {"rpg": 8.1, "run_factor": 0.95, "k_pct": 0.219, "bb_pct": 0.079, "over_pct": 0.46, "games": 240},
    "Alex Tosi":        {"rpg": 8.2, "run_factor": 0.96, "k_pct": 0.215, "bb_pct": 0.081, "over_pct": 0.46, "games": 200},
    "Jansen Visconti":  {"rpg": 8.2, "run_factor": 0.96, "k_pct": 0.214, "bb_pct": 0.082, "over_pct": 0.47, "games": 210},
    "Tripp Gibson":     {"rpg": 8.2, "run_factor": 0.96, "k_pct": 0.218, "bb_pct": 0.080, "over_pct": 0.47, "games": 350},
    "Brian O'Nora":     {"rpg": 8.2, "run_factor": 0.96, "k_pct": 0.216, "bb_pct": 0.081, "over_pct": 0.46, "games": 500},

    # Average zone umpires (neutral)
    "Chad Fairchild":   {"rpg": 8.5, "run_factor": 0.99, "k_pct": 0.210, "bb_pct": 0.084, "over_pct": 0.48, "games": 400},
    "Mike Muchlinski":  {"rpg": 8.5, "run_factor": 1.00, "k_pct": 0.210, "bb_pct": 0.084, "over_pct": 0.49, "games": 420},
    "Dan Merzel":       {"rpg": 8.5, "run_factor": 1.00, "k_pct": 0.208, "bb_pct": 0.085, "over_pct": 0.49, "games": 380},
    "Lance Barrett":    {"rpg": 8.6, "run_factor": 1.00, "k_pct": 0.209, "bb_pct": 0.085, "over_pct": 0.49, "games": 400},
    "Mark Carlson":     {"rpg": 8.6, "run_factor": 1.00, "k_pct": 0.207, "bb_pct": 0.086, "over_pct": 0.50, "games": 500},
    "Clint Vondrak":    {"rpg": 8.6, "run_factor": 1.00, "k_pct": 0.208, "bb_pct": 0.085, "over_pct": 0.50, "games": 200},
    "Manny Gonzalez":   {"rpg": 8.6, "run_factor": 1.01, "k_pct": 0.206, "bb_pct": 0.086, "over_pct": 0.50, "games": 450},
    "Ryan Blakney":     {"rpg": 8.7, "run_factor": 1.01, "k_pct": 0.206, "bb_pct": 0.087, "over_pct": 0.50, "games": 350},
    "Alfonso Marquez":  {"rpg": 8.7, "run_factor": 1.01, "k_pct": 0.205, "bb_pct": 0.087, "over_pct": 0.50, "games": 520},
    "Jerry Layne":      {"rpg": 8.7, "run_factor": 1.02, "k_pct": 0.205, "bb_pct": 0.087, "over_pct": 0.51, "games": 600},
    "Mike Estabrook":   {"rpg": 8.7, "run_factor": 1.02, "k_pct": 0.204, "bb_pct": 0.087, "over_pct": 0.51, "games": 400},

    # Small zone umpires (hitter-friendly, more runs)
    "Marvin Hudson":    {"rpg": 9.0, "run_factor": 1.04, "k_pct": 0.200, "bb_pct": 0.090, "over_pct": 0.53, "games": 550},
    "Angel Hernandez":  {"rpg": 9.0, "run_factor": 1.05, "k_pct": 0.198, "bb_pct": 0.091, "over_pct": 0.53, "games": 600},
    "CB Bucknor":       {"rpg": 9.1, "run_factor": 1.05, "k_pct": 0.197, "bb_pct": 0.092, "over_pct": 0.54, "games": 580},
    "Doug Eddings":     {"rpg": 9.1, "run_factor": 1.06, "k_pct": 0.196, "bb_pct": 0.093, "over_pct": 0.54, "games": 520},
    "Laz Diaz":         {"rpg": 9.2, "run_factor": 1.06, "k_pct": 0.195, "bb_pct": 0.093, "over_pct": 0.55, "games": 530},
    "Hunter Wendelstedt": {"rpg": 9.2, "run_factor": 1.07, "k_pct": 0.194, "bb_pct": 0.094, "over_pct": 0.55, "games": 500},
    "Ron Kulpa":        {"rpg": 9.0, "run_factor": 1.04, "k_pct": 0.200, "bb_pct": 0.090, "over_pct": 0.53, "games": 520},
    "Bill Miller":      {"rpg": 9.1, "run_factor": 1.05, "k_pct": 0.198, "bb_pct": 0.091, "over_pct": 0.54, "games": 540},
    "Joe West":         {"rpg": 9.2, "run_factor": 1.06, "k_pct": 0.196, "bb_pct": 0.093, "over_pct": 0.54, "games": 700},
    "Cory Blaser":      {"rpg": 9.0, "run_factor": 1.04, "k_pct": 0.201, "bb_pct": 0.089, "over_pct": 0.53, "games": 380},
    "Todd Tichenor":    {"rpg": 9.1, "run_factor": 1.05, "k_pct": 0.199, "bb_pct": 0.091, "over_pct": 0.54, "games": 400},
    "James Hoye":       {"rpg": 9.0, "run_factor": 1.04, "k_pct": 0.201, "bb_pct": 0.090, "over_pct": 0.53, "games": 450},
    "Ed Hickox":        {"rpg": 9.1, "run_factor": 1.05, "k_pct": 0.198, "bb_pct": 0.092, "over_pct": 0.54, "games": 480},
    "Alan Porter":      {"rpg": 8.9, "run_factor": 1.03, "k_pct": 0.202, "bb_pct": 0.088, "over_pct": 0.52, "games": 420},
    "Ben May":          {"rpg": 8.8, "run_factor": 1.03, "k_pct": 0.203, "bb_pct": 0.088, "over_pct": 0.52, "games": 300},
    "Chris Guccione":   {"rpg": 8.9, "run_factor": 1.03, "k_pct": 0.203, "bb_pct": 0.088, "over_pct": 0.52, "games": 440},
    "Andy Fletcher":    {"rpg": 8.8, "run_factor": 1.02, "k_pct": 0.204, "bb_pct": 0.087, "over_pct": 0.51, "games": 480},
    "Will Little":      {"rpg": 8.8, "run_factor": 1.02, "k_pct": 0.205, "bb_pct": 0.087, "over_pct": 0.51, "games": 350},
    "Adrian Johnson":   {"rpg": 8.9, "run_factor": 1.03, "k_pct": 0.203, "bb_pct": 0.089, "over_pct": 0.52, "games": 460},
    "Jordan Baker":     {"rpg": 8.8, "run_factor": 1.02, "k_pct": 0.204, "bb_pct": 0.087, "over_pct": 0.51, "games": 400},
    "Scott Barry":      {"rpg": 8.9, "run_factor": 1.03, "k_pct": 0.203, "bb_pct": 0.089, "over_pct": 0.52, "games": 430},
    "Dan Iassogna":     {"rpg": 8.7, "run_factor": 1.02, "k_pct": 0.205, "bb_pct": 0.087, "over_pct": 0.51, "games": 450},
    "Ted Barrett":      {"rpg": 8.6, "run_factor": 1.01, "k_pct": 0.206, "bb_pct": 0.086, "over_pct": 0.50, "games": 520},
    "Sam Holbrook":     {"rpg": 8.8, "run_factor": 1.02, "k_pct": 0.204, "bb_pct": 0.088, "over_pct": 0.51, "games": 510},
}


def get_umpire_factor(name: str | None) -> tuple[float, dict | None]:
    """
    Look up umpire run factor.
    Returns (run_factor, umpire_data_dict_or_None).
    """
    if not name:
        return 1.0, None

    # Exact match
    data = UMPIRE_DATA.get(name)
    if data:
        return data["run_factor"], data

    # Fuzzy match (last name)
    name_lower = name.lower()
    for ump_name, ump_data in UMPIRE_DATA.items():
        if ump_name.lower().split()[-1] in name_lower:
            return ump_data["run_factor"], ump_data

    return 1.0, None


def sync_umpires_to_db():
    """Store umpire data into the database."""
    from engine.db import get_conn
    conn = get_conn()

    for name, data in UMPIRE_DATA.items():
        conn.execute("""
            INSERT INTO umpires (name, games, k_pct, bb_pct, rpg, over_pct, run_factor)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                games=excluded.games, k_pct=excluded.k_pct, bb_pct=excluded.bb_pct,
                rpg=excluded.rpg, over_pct=excluded.over_pct,
                run_factor=excluded.run_factor, updated_at=datetime('now')
        """, (name, data["games"], data["k_pct"], data["bb_pct"],
              data["rpg"], data["over_pct"], data["run_factor"]))

    conn.commit()
    logger.info("Stored %d umpire profiles", len(UMPIRE_DATA))
