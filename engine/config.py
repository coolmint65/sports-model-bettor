"""
Centralized configuration for the sports model.

All tunable thresholds, market penalties, and model constants live here
instead of being scattered across prediction and pick modules.

Philosophy: keep all markets ENABLED so the tracker keeps recording
picks across every market. Use reliability weights to adjust ranking,
not to disable recording. We need data from losing markets to know
whether they stay losing or eventually recover.
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
# shows the model is now 34% WR. Stripping back to the core xG +
# goalie + rest factors until each granular factor is individually
# validated against a baseline.
NHL_ENABLE_GRANULAR_FACTORS = False

# ── Bet-type reliability weights ──
# Based on live tracker results. Higher = more reliable.
# Used for adjusted-EV ranking (adjusted_ev = edge * reliability).
# Low weights demote a market in "best pick" ordering but DO NOT
# stop picks from being generated/recorded — we still want the data.
MLB_BET_RELIABILITY = {
    "RL": 1.00,     # 55.6% hit rate, +$384 — only profitable market
    "ML": 0.50,     # 48% hit rate, -$150
    "O/U": 0.30,    # 33.3% hit rate, -$334 (small sample)
    "1st INN": 0.20, # 46.2% hit rate, -$400
}

NHL_BET_RELIABILITY = {
    "O/U": 0.40,   # Sub-50% overall, but keep recording
    "PL": 0.40,
    "ML": 0.40,
}

NBA_BET_RELIABILITY = {
    "Q1_SPREAD": 1.00,
    "Q1_TOTAL": 0.80,
    "Q1_ML": 0.60,
}

# ── Market toggles — keep ALL on by default to collect tracker data ──
# Only set False when a market is so broken it's not worth watching.
ENABLE_MLB_NRFI = True
ENABLE_NHL_ML = True
ENABLE_NHL_OU = True
ENABLE_NHL_PL = True
