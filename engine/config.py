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
MIN_EDGE_PCT = 4.0

# Confidence tier thresholds — used uniformly across MLB/NHL/NBA picks.
# Picks with edge < EDGE_SKIP are marked confidence="skip" and will not
# be chosen as "best pick" by tracker/best-bets endpoints.
#
# EDGE_SKIP raised from 1.5 -> 4.0 after engine.edge_report showed:
#   MLB <4% edge: 2-3 record, heavy loss ($-208 on 5 picks = -42% ROI)
#   MLB 6-10% edge: 5-7 record, -23% ROI (still losing)
#   MLB 10%+ edge: 65-59 record, -0.91% ROI (near-breakeven — best bucket)
# NHL has same pattern — sub-4% picks are 1-4 combined. Filtering them
# out reduces volume but improves aggregate ROI.
EDGE_STRONG = 8.0
EDGE_MODERATE = 6.0
EDGE_LEAN = 4.0
EDGE_SKIP = 4.0  # raised from 1.5 based on tracker data

# ── MLB config ──
MLB_AVG_RPG = 4.6  # League average runs per game
MLB_WIN_PROB_FLOOR = 0.30
MLB_WIN_PROB_CAP = 0.72
MLB_EXPECTED_RUNS_FLOOR = 2.0
MLB_EXPECTED_RUNS_CAP = 6.5

# Home-field advantage in expected runs.
# Data point: with home_edge=0.28 the live tracker showed MLB home picks
# at 48.9% WR / -10% ROI vs away picks at 57.4% WR / +11% ROI across 106
# side-resolvable picks. That 20pt ROI gap says the model was overstating
# home advantage. Pulled down from 0.28 -> 0.15 to rebalance.
MLB_HOME_EDGE = 0.15

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

# ── MLB situational factors toggle ──
# MLB predict stacks 16+ multiplicative adjustments on expected runs
# (pitcher, lineup, team cal, bullpen, bullpen fatigue, park, coors,
# situational aggregate, umpire, weather, travel, platoon LHP, matchup
# interaction, form, injuries). The backtest shows MLB is only
# profitable on RL (55.6%) and is losing on ML / O/U / 1st INN — similar
# compounding risk to what broke NHL.
#
# Default: True (keep current behavior — MLB is not catastrophically
# broken, just underperforming on 3/4 markets). Flip to False to
# disable the "situational" group (weather, umpire, travel, matchup
# interaction, bullpen fatigue) and run the retrospective sweep to see
# whether ML / O/U / 1st INN WR improves. If it does, those factors are
# net-negative and should be investigated individually.
MLB_ENABLE_SITUATIONAL_FACTORS = True

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
