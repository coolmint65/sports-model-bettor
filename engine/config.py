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

# Granular factors (Factors 1-12 in nhl_predict.py).
# PERMANENTLY OFF until each factor is individually validated.
# Retrospective sweep of 41 settled picks showed enabling these factors
# dropped WR from 53.7% to 34.1% — a ~20pt degradation. Never turn this
# on globally again. If you want to experiment with a factor, flip it
# on one at a time in an ablation test.
NHL_ENABLE_GRANULAR_FACTORS = False

# ── Bet-type reliability weights ──
# Based on live tracker results + retrospective sweep against current model.
# Used for adjusted-EV ranking (adjusted_ev = edge * reliability).
# Low weights demote a market in "best pick" ordering but do NOT stop
# picks from being generated/recorded.
MLB_BET_RELIABILITY = {
    "RL": 1.00,     # 55.6% hit rate, +$384 — proven profitable
    "ML": 0.70,     # 48% hit rate, slightly losing — watch
    "O/U": 0.50,    # 33.3% hit rate — small sample, demote
    "1st INN": 0.30, # 46.2% hit rate, -$400 — keep but heavily demoted
}

NHL_BET_RELIABILITY = {
    # Retro sweep with granular OFF puts the model at 53.7% across all
    # markets. Treat all three as roughly equal for ranking until we
    # have enough live picks to differentiate.
    "O/U": 1.00,
    "PL": 1.00,
    "ML": 0.85,
}

NBA_BET_RELIABILITY = {
    "Q1_SPREAD": 1.00,
    "Q1_TOTAL": 0.80,
    "Q1_ML": 0.60,
}

# ── Weak markets — disabled by default ──
ENABLE_MLB_NRFI = True
ENABLE_NHL_ML = True
ENABLE_NHL_OU = True
ENABLE_NHL_PL = True
