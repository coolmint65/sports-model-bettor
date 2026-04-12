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
MLB_WIN_PROB_CAP = 0.65  # tightened from 0.72 — calibration data shows
                          # 75%+ predicted = 51.6% actual across 62 picks

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
# MLB predict stacks 16+ multiplicative adjustments on expected runs.
#
# Re-enabled 2026-04 after mlb_retrobt showed disabling them drops
# 120/143 picks and the remaining 22 go 6-16 (27% WR). Unlike NHL
# granular, MLB situational factors are actually load-bearing — they
# help the model FIND edge spots, not invert them. The "rl" vs "RL"
# 16-point WR gap was misleading (bet-type casing artifact, not a
# model-version split).
#
# The real MLB improvement lever is DIRECTION filtering, not factor
# ablation. See MLB_ALLOW_* flags below.
MLB_ENABLE_SITUATIONAL_FACTORS = True

# ── MLB direction filters ──
# Based on 143 tracked picks showing strong per-direction biases:
#   RL +1.5 dogs:       40-27  59.7% WR  (profitable)
#   RL -1.5 favorites:   3- 9  25.0% WR  (disastrous)
#   NRFI:                3- 1  75.0% WR  (profitable)
#   YRFI:                9-14  39.1% WR  (losing)
#   O/U Over:            2- 0           (tiny sample; hold)
#   O/U Under:           1- 6  14.3% WR (disastrous)
# Setting False stops that direction from being selected as a pick.
MLB_ALLOW_RL_FAVORITE = False   # -1.5 picks disabled
MLB_ALLOW_RL_UNDERDOG = True    # +1.5 picks — the profitable side
MLB_ALLOW_NRFI = True           # NRFI has real edge
MLB_ALLOW_YRFI = False          # YRFI consistently loses
MLB_ALLOW_OU_OVER = True        # hold while sample is tiny
MLB_ALLOW_OU_UNDER = False      # Unders hit 14% over 7 picks

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
