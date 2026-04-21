"""
League definitions and sport-specific configuration.

Each league knows its sport type, scoring periods, typical scoring rates,
and how to break down a game prediction.
"""

LEAGUES = {
    # ── American Football ──
    "NFL": {
        "sport": "football",
        "name": "NFL",
        "periods": ["Q1", "Q2", "Q3", "Q4"],
        "halves": ["1H", "2H"],
        "has_overtime": True,
        "period_minutes": 15,
        "avg_total": 45.0,
        "avg_home_edge": 2.5,
        # Typical period scoring distribution (% of total)
        "period_weights": [0.22, 0.28, 0.22, 0.28],
        "half_weights": [0.50, 0.50],
    },
    "CFB": {
        "sport": "football",
        "name": "College Football",
        "periods": ["Q1", "Q2", "Q3", "Q4"],
        "halves": ["1H", "2H"],
        "has_overtime": True,
        "period_minutes": 15,
        "avg_total": 52.0,
        "avg_home_edge": 3.0,
        "period_weights": [0.22, 0.28, 0.22, 0.28],
        "half_weights": [0.50, 0.50],
    },

    # ── Basketball ──
    "NBA": {
        "sport": "basketball",
        "name": "NBA",
        "periods": ["Q1", "Q2", "Q3", "Q4"],
        "halves": ["1H", "2H"],
        "has_overtime": True,
        "period_minutes": 12,
        "avg_total": 224.0,
        "avg_home_edge": 3.0,
        "period_weights": [0.25, 0.25, 0.25, 0.25],
        "half_weights": [0.50, 0.50],
    },
    "NCAAB": {
        "sport": "basketball",
        "name": "NCAA Men's Basketball",
        "periods": [],
        "halves": ["1H", "2H"],
        "has_overtime": True,
        "period_minutes": 20,
        "avg_total": 143.0,
        "avg_home_edge": 3.5,
        "period_weights": [],
        "half_weights": [0.50, 0.50],
    },
    "NCAAW": {
        "sport": "basketball",
        "name": "NCAA Women's Basketball",
        "periods": ["Q1", "Q2", "Q3", "Q4"],
        "halves": ["1H", "2H"],
        "has_overtime": True,
        "period_minutes": 10,
        "avg_total": 135.0,
        "avg_home_edge": 3.5,
        "period_weights": [0.25, 0.25, 0.25, 0.25],
        "half_weights": [0.50, 0.50],
    },

    # ── Baseball ──
    "MLB": {
        "sport": "baseball",
        "name": "MLB",
        "periods": ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
        "halves": ["F5", "L4"],  # First 5 innings, Last 4 innings
        "has_overtime": True,  # Extra innings
        "period_minutes": None,
        "avg_total": 8.5,
        "avg_home_edge": 0.3,
        # Scoring by inning (slight front-load, pitchers tire)
        "period_weights": [0.11, 0.10, 0.10, 0.11, 0.11, 0.11, 0.12, 0.12, 0.12],
        "half_weights": [0.55, 0.45],  # F5 vs L4
    },

    # ── Hockey ──
    "NHL": {
        "sport": "hockey",
        "name": "NHL",
        "periods": ["P1", "P2", "P3"],
        "halves": [],
        "has_overtime": True,
        "period_minutes": 20,
        "avg_total": 6.0,
        "avg_home_edge": 0.3,
        "period_weights": [0.33, 0.34, 0.33],
        "half_weights": [],
    },

    # ── Soccer ──
    "EPL": {
        "sport": "soccer",
        "name": "Premier League",
        "periods": [],
        "halves": ["1H", "2H"],
        "has_overtime": False,
        "period_minutes": 45,
        "avg_total": 2.7,
        "avg_home_edge": 0.35,
        "period_weights": [],
        "half_weights": [0.45, 0.55],
    },
    "UCL": {
        "sport": "soccer",
        "name": "Champions League",
        "periods": [],
        "halves": ["1H", "2H"],
        "has_overtime": False,
        "period_minutes": 45,
        "avg_total": 2.9,
        "avg_home_edge": 0.30,
        "period_weights": [],
        "half_weights": [0.45, 0.55],
    },
    "LALIGA": {
        "sport": "soccer",
        "name": "La Liga",
        "periods": [],
        "halves": ["1H", "2H"],
        "has_overtime": False,
        "period_minutes": 45,
        "avg_total": 2.5,
        "avg_home_edge": 0.40,
        "period_weights": [],
        "half_weights": [0.45, 0.55],
    },
    "BUNDESLIGA": {
        "sport": "soccer",
        "name": "Bundesliga",
        "periods": [],
        "halves": ["1H", "2H"],
        "has_overtime": False,
        "period_minutes": 45,
        "avg_total": 3.1,
        "avg_home_edge": 0.35,
        "period_weights": [],
        "half_weights": [0.45, 0.55],
    },
    "MLS": {
        "sport": "soccer",
        "name": "MLS",
        "periods": [],
        "halves": ["1H", "2H"],
        "has_overtime": False,
        "period_minutes": 45,
        "avg_total": 2.9,
        "avg_home_edge": 0.45,
        "period_weights": [],
        "half_weights": [0.45, 0.55],
    },
    "NWSL": {
        "sport": "soccer",
        "name": "NWSL",
        "periods": [],
        "halves": ["1H", "2H"],
        "has_overtime": False,
        "period_minutes": 45,
        "avg_total": 2.6,
        "avg_home_edge": 0.40,
        "period_weights": [],
        "half_weights": [0.45, 0.55],
    },
    "LIGAMX": {
        "sport": "soccer",
        "name": "Liga MX",
        "periods": [],
        "halves": ["1H", "2H"],
        "has_overtime": False,
        "period_minutes": 45,
        "avg_total": 2.5,
        "avg_home_edge": 0.40,
        "period_weights": [],
        "half_weights": [0.45, 0.55],
    },
}


def get_league(key: str) -> dict:
    """Get league config by key (case-insensitive)."""
    return LEAGUES[key.upper()]


def list_leagues() -> list[str]:
    """Return all league keys grouped by sport."""
    order = ["NFL", "CFB", "NBA", "NCAAB", "NCAAW", "MLB", "NHL",
             "EPL", "UCL", "LALIGA", "BUNDESLIGA", "MLS", "NWSL", "LIGAMX"]
    return [k for k in order if k in LEAGUES]
