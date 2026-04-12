"""
Centralized configuration for the sports model.

All tunable thresholds, market penalties, and model constants live here
instead of being scattered across prediction and pick modules.

Tuning philosophy: if the tracker shows a market is losing money, its
reliability weight goes to 0 or the market gets disabled outright.
Better to miss a hot streak than to keep recording -EV picks.
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

# Toggle: set False to skip the 12 granular factors in nhl_predict.py.
# These were added in an attempt to improve accuracy but the tracker
# shows the model is now 34% WR across 47 picks. Stripping back to the
# core xG + goalie + rest factors until each granular factor is
# individually validated against a baseline.
NHL_ENABLE_GRANULAR_FACTORS = False

# ── Bet-type reliability weights ──
# Based on live tracker results, not historical backtest.
# 0.0 disables the market entirely (pick is generated but will never
# rank as best pick, and confidence=skip suppresses UI badges).
MLB_BET_RELIABILITY = {
    "RL": 1.00,     # 55.6% hit rate, +$384 — the only market with real edge
    "ML": 0.0,      # 48% hit rate, -$150 — DISABLED until calibration fixed
    "O/U": 0.0,     # 33.3% hit rate, -$334 — DISABLED (tiny sample, bad signal)
    "1st INN": 0.0, # 46.2% hit rate, -$400 — DISABLED
}

NHL_BET_RELIABILITY = {
    "O/U": 0.0,   # 34.1% WR overall, disabled until model is rebuilt
    "PL": 0.0,    # same
    "ML": 0.0,    # same — NHL model is systematically wrong right now
}

NBA_BET_RELIABILITY = {
    "Q1_SPREAD": 1.00,
    "Q1_TOTAL": 0.80,
    "Q1_ML": 0.60,
}

# ── Weak markets — disabled by default ──
ENABLE_MLB_NRFI = False   # Backtest shows -$400, 46.2% hit rate
ENABLE_NHL_ML = False     # Everything NHL is losing right now
ENABLE_NHL_OU = False
ENABLE_NHL_PL = False
