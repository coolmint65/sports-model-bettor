"""
Centralized configuration for the sports model.

All tunable thresholds, market penalties, and model constants live here
instead of being scattered across prediction and pick modules.
"""

# ── Market config ──
# Juice wall: don't recommend bets with worse odds than this
MLB_JUICE_WALL = -180
NHL_JUICE_WALL = -200
NBA_JUICE_WALL = -180

# Minimum edge (%) to consider a pick playable
MIN_EDGE_PCT = 1.5

# ── MLB config ──
MLB_AVG_RPG = 4.6  # League average runs per game
MLB_WIN_PROB_FLOOR = 0.30
MLB_WIN_PROB_CAP = 0.72
MLB_EXPECTED_RUNS_FLOOR = 2.0
MLB_EXPECTED_RUNS_CAP = 6.5

# ── NHL config ──
NHL_HOME_EDGE = 0.15  # ~0.15 goal home-ice advantage
NHL_MAX_GOALS = 10

# ── Bet-type reliability weights ──
# Based on backtest results. Higher = more reliable.
# Used for adjusted-EV ranking instead of hard priority.
MLB_BET_RELIABILITY = {
    "RL": 1.00,    # 59.0% hit rate, +$752
    "ML": 0.85,    # 57.9% hit rate, +$241
    "O/U": 0.60,   # 42.9% hit rate, -$134
    "1st INN": 0.0, # 46.2% hit rate, -$400 — DISABLED
}

NHL_BET_RELIABILITY = {
    "O/U": 1.00,   # Best ROI per backtest
    "PL": 0.90,    # Second best
    "ML": 0.50,    # Negative ROI historically
}

NBA_BET_RELIABILITY = {
    "Q1_SPREAD": 1.00,
    "Q1_TOTAL": 0.80,
    "Q1_ML": 0.60,
}

# ── Weak markets — disabled by default ──
# Set to True to enable picks in these markets
ENABLE_MLB_NRFI = False   # Backtest shows -$400, 46.2% hit rate
ENABLE_NHL_ML = True      # Kept on but deprioritized (low reliability)
