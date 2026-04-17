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

# Confidence tier thresholds - used uniformly across MLB/NHL/NBA picks.
# Picks with edge < EDGE_SKIP are marked confidence="skip" and will not
# be chosen as "best pick" by tracker/best-bets endpoints.
#
# EDGE_SKIP raised from 1.5 -> 4.0 after engine.edge_report showed:
#   MLB <4% edge: 2-3 record, heavy loss ($-208 on 5 picks = -42% ROI)
#   MLB 6-10% edge: 5-7 record, -23% ROI (still losing)
#   MLB 10%+ edge: 65-59 record, -0.91% ROI (near-breakeven - best bucket)
# NHL has same pattern - sub-4% picks are 1-4 combined. Filtering them
# out reduces volume but improves aggregate ROI.
EDGE_STRONG = 8.0
EDGE_MODERATE = 6.0
EDGE_LEAN = 4.0
EDGE_SKIP = 4.0  # raised from 1.5 based on tracker data

# ── MLB config ──
# NOTE: predict_matchup imports MLB_AVG_RPG from engine.mlb_scoring,
# not from here. This constant is unused in the live model. Keep in
# sync with mlb_scoring.MLB_AVG_RPG for readers; don't rely on it.
MLB_AVG_RPG = 4.85  # League average runs per game (mirror of
                    # mlb_scoring.MLB_AVG_RPG)
MLB_WIN_PROB_FLOOR = 0.30
MLB_WIN_PROB_CAP = 0.58  # tightened from 0.65. engine.train on 159 picks
                          # showed 70%+ bucket (N=95) hit 52.6% actual vs 79.8%
                          # stated - off by 27pp. 0.58 cap aligns the top
                          # bucket with real WR (~53-58%).

MLB_EXPECTED_RUNS_FLOOR = 2.0
MLB_EXPECTED_RUNS_CAP = 6.5

# Home-field advantage in expected runs.
# Data point: with home_edge=0.28 the live tracker showed MLB home picks
# at 48.9% WR / -10% ROI vs away picks at 57.4% WR / +11% ROI across 106
# side-resolvable picks. That 20pt ROI gap says the model was overstating
# home advantage. Pulled down from 0.28 -> 0.15 to rebalance.
MLB_HOME_EDGE = 0.15

# Share of game innings attributed to the starter in the runs-scored
# projection. Based on 2024 league average of 5.4 IP per start out of
# 8.9 game-average innings = 0.61. The remainder (0.39) is attributed
# to the bullpen. Pre-change code applied the SP factor to 100% of
# expected runs and the bullpen as a tiny 35% multiplier, which made
# every pick effectively a referendum on the starter - ignoring that
# the bullpen pitches ~40% of the game. If the SP/BP usage split
# shifts (league-wide trend toward openers, etc.), tune here.
MLB_SP_INNINGS_SHARE = 0.60

# ── NBA config ──
# Roster availability / load-management adjustment toggle.
# Shipped 2026-04-12 based on intuition that starter rest lowers Q1
# scoring. Actual 15-game data from 2026-04-12 slate (heaviest rest
# slate of the year) showed mean Q1 total of 59.3 vs baseline 58.8 -
# a 0.5-pt shift. Variance widens but the MEAN barely moves, so the
# model was encoding a phantom signal.
#
# DEFAULT OFF until a proper backtest proves the adjustment lifts
# WR/ROI on held-out data. Infrastructure (nba_players, nba_injuries,
# nba_injuries.py) stays intact for informational display, but
# predict_q1 ignores it when this flag is False.
NBA_ENABLE_ROSTER_ADJUSTMENT = False

# ── NHL config ──
NHL_HOME_EDGE = 0.15  # ~0.15 goal home-ice advantage
NHL_MAX_GOALS = 10

# Win-prob clamp matching the MLB/NBA pattern. engine.train on 47
# settled picks showed every prob bucket hit 15-40pp below stated
# (55-60% predicted -> 25% actual, 70%+ predicted -> 40% actual).
# The model is systematically overconfident even with granular
# factors off - capping raw p_home_ml prevents picks driven by
# stated probabilities that the data says are fiction.
NHL_WIN_PROB_FLOOR = 0.35
NHL_WIN_PROB_CAP = 0.55

# Granular factors (Factors 1-12 in nhl_predict.py).
# PERMANENTLY OFF until each factor is individually validated.
# Retrospective sweep of 41 settled picks showed enabling these factors
# dropped WR from 53.7% to 34.1% - a ~20pt degradation. Never turn this
# on globally again. If you want to experiment with a factor, flip it
# on one at a time in an ablation test.
NHL_ENABLE_GRANULAR_FACTORS = False

# ── NHL per-factor ablation toggles ──
# Individual gates for each NHL xG multiplier so factor_backtest can
# flip each one and measure WR/ROI impact. Added 2026-04 after
# engine.train on 47 settled picks showed 66% flip-WR (every
# probability bucket 15-40pp below stated - systemic overconfidence).
# NHL_WIN_PROB_CAP above is the blunt mitigation; these toggles are
# the scalpel for finding WHICH factor is broken.
#
# Defaults preserve current production behavior (all True). Flip
# individually via factor_backtest only after proving lift.
# Defaults updated 2026-04-14 from factor_backtest on 47 settled NHL
# picks (Base WR 34.0%). Only factors that demonstrably lifted WR on
# the held-out sample are kept ON. See ABLATION column for evidence:
#   Avg Δp = avg absolute prob shift, Abl WR = WR with factor OFF.
#   Abl WR < Base -> factor helps (keep ON).
#   Abl WR > Base -> factor hurts (turn OFF).
#   Avg Δp ~ 0    -> factor inert on this sample (turn OFF).
NHL_ENABLE_GOALIE_SV         = False  # ablated 0.7% Δp / 34.0% WR (no signal)
NHL_ENABLE_GOALIE_STARTER    = False  # ablated 0.0% Δp (inert - no live goalie data)
NHL_ENABLE_GOALIE_BACKUP_PEN = False  # ablated 0.0% Δp (inert)
NHL_ENABLE_PP_PK             = False  # ablated 1.1% Δp / 34.8% WR (mildly hurts: +0.8pp)
NHL_ENABLE_SHOT_DIFF         = True   # ablated 4.0% Δp / 30.2% WR (HELPS: -3.8pp) - keep
NHL_ENABLE_FACEOFF           = False  # ablated 0.1% Δp / 34.8% WR (1 wrong flip)
NHL_ENABLE_FORM              = False  # ablated 0.0% Δp (inert)
NHL_ENABLE_HOME_AWAY_SPLIT   = False  # ablated 0.0% Δp (inert)
NHL_ENABLE_STANDINGS_FORM    = False  # ablated 0.0% Δp (inert)
NHL_ENABLE_VENUE_SPLIT       = False  # ablated 0.7% Δp / 34.0% WR (no signal)
NHL_ENABLE_MOTIVATION        = False  # ablated 0.2% Δp (inert despite late-season)
NHL_ENABLE_QUALITY_DIFF      = False  # ablated 2.2% Δp / 34.9% WR (mildly hurts: +0.9pp)
NHL_ENABLE_H2H               = True   # ablated 0.6% Δp / 32.6% WR (HELPS: -1.4pp) - keep

# ── MLB situational factors toggle ──
# MLB predict stacks 16+ multiplicative adjustments on expected runs.
#
# Re-enabled 2026-04 after mlb_retrobt showed disabling them drops
# 120/143 picks and the remaining 22 go 6-16 (27% WR). Unlike NHL
# granular, MLB situational factors are actually load-bearing - they
# help the model FIND edge spots, not invert them. The "rl" vs "RL"
# 16-point WR gap was misleading (bet-type casing artifact, not a
# model-version split).
#
# The real MLB improvement lever is DIRECTION filtering, not factor
# ablation. See MLB_ALLOW_* flags below.
MLB_ENABLE_SITUATIONAL_FACTORS = True

# Team calibration (learned offense/defense/home/away multipliers from
# recent games). In theory this adapts the model to a team's actual
# performance vs expected; in practice, with 17-18 games of April
# sample, it amplifies variance into signal.
#
# Live example from 2026-04-13 SEA@HOU:
#   SEA team_cal: offense 0.89x, defense 0.90x  (cold start -> penalize)
#   HOU team_cal: offense 1.32x, defense 1.22x  (hot start -> reward)
#   Combined swing: ~1.6 runs (SEA 4.85->4.31, HOU 4.50->5.95)
#   Market had SEA -181 (correctly ignoring the 18-game noise).
#   Model had HOU 65% (tricked by team_cal).
#
# DEFAULT OFF until a backtest proves team_cal adds WR/ROI on held-out
# games. Fundamentals (wRC+/OPS/runs_pg, pitcher ERA, park) handle
# most of the real signal without this layer.
MLB_ENABLE_TEAM_CAL = False

# ── MLB per-factor ablation toggles ──
# Individual gates for each MLB multiplier so engine.factor_backtest
# can flip them one at a time and measure the real WR/ROI impact.
#
# Defaults preserve current production behavior (all True). Do NOT
# flip these by hand - always run a backtest first and keep factors
# that demonstrably help on held-out data. This is the discipline
# layer that prevents intuition-driven factor stacking.
#
# [S] = part of the SITUATIONAL group (also gated by
#       MLB_ENABLE_SITUATIONAL_FACTORS above)
# [V] = validated in literature / external research
# [?] = unvalidated - prime suspects for ablation
# Defaults updated 2026-04-14 from factor_backtest on 159 settled MLB
# picks (Base WR 50.3%). Result: NO factor earned its slot. The four
# active factors that moved probabilities all PUSHED PICKS TOWARD
# LOSERS (Abl WR > Base WR by 0.4-0.7pp each). The remaining factors
# moved no probability at all - pure wasted compute. The MLB model
# wants the bare core: team offense, pitcher ERA/FIP, park factor,
# SP/BP innings split, Poisson baseline. Format below: ablated
# Avg Δp / Abl WR (vs Base 50.3%).
MLB_ENABLE_BULLPEN_FATIGUE    = False  # 0.0% Δp / 50.3% (inert)
MLB_ENABLE_SITUATIONAL_AGG    = False  # 1.0% Δp / 51.0% (HURTS: +0.7pp)
MLB_ENABLE_UMPIRE_FACTOR      = False  # 0.0% Δp / 50.3% (inert)
MLB_ENABLE_WEATHER_ADJ        = False  # already off (was double-counting)
MLB_ENABLE_TRAVEL_FATIGUE     = False  # 0.5% Δp / 51.0% (HURTS: +0.7pp)
MLB_ENABLE_MATCHUP_INTERACTION = False # 0.9% Δp / 50.7% (HURTS: +0.4pp)
MLB_ENABLE_COORS_BOOST        = False  # 0.0% Δp / 50.0% (inert)
MLB_ENABLE_PLATOON_DUP        = False  # 0.3% Δp / 50.3% (inert)
MLB_ENABLE_H2H_VS_PITCHER     = False  # 0.0% Δp / 50.3% (inert)
MLB_ENABLE_LINEUP_STRENGTH    = False  # 1.1% Δp / 51.0% (HURTS: +0.7pp)

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
MLB_ALLOW_RL_UNDERDOG = True    # +1.5 picks - the profitable side
MLB_ALLOW_NRFI = True           # NRFI has real edge
MLB_ALLOW_YRFI = False          # YRFI consistently loses
MLB_ALLOW_OU_OVER = True        # hold while sample is tiny
MLB_ALLOW_OU_UNDER = False      # Unders hit 14% over 7 picks

# ── Bet-type reliability weights ──
# Based on live tracker results + retrospective sweep against current model.
# Used for adjusted-EV ranking (adjusted_ev = edge * reliability).
# Low weights demote a market in "best pick" ordering but do NOT stop
# picks from being generated/recorded.
def get_flag(name: str, default=None, sport: str = "mlb"):
    """Return a config flag, consulting the runtime overrides table first.

    Source-code module-level constants in this file are the human's
    intended defaults; ``engine.train --apply`` may have written a
    runtime override (e.g., when WR data is statistically significantly
    below break-even). Reading via ``get_flag()`` instead of importing
    the constant ensures every consumer respects active overrides.

    Falls back to the module-level constant when no override exists,
    and to ``default`` when the constant is missing entirely (shouldn't
    happen in practice but keeps test/probe code from crashing).
    """
    try:
        from .model_overrides import get_override
        ov = get_override(sport, name)
        if ov is not None:
            return ov
    except Exception:
        # Override layer must never crash the prediction path.
        pass
    return globals().get(name, default)


MLB_BET_RELIABILITY = {
    "RL": 1.00,     # 55.6% hit rate, +$384 - proven profitable
    "ML": 0.70,     # 48% hit rate, slightly losing - watch
    "O/U": 0.50,    # 33.3% hit rate - small sample, demote
    "1st INN": 0.30, # 46.2% hit rate, -$400 - keep but heavily demoted
    # F5 markets - no live history yet, start conservative
    "F5 ML":  0.60,
    "F5 O/U": 0.50,
    "F5 RL":  0.70,
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

# ── Weak markets - disabled by default ──
ENABLE_MLB_NRFI = True
ENABLE_NHL_ML = True
ENABLE_NHL_OU = True
ENABLE_NHL_PL = True

# F5 (First 5 innings) picks. Go-live decision (Apr 2026): historical
# F5 odds coverage too thin to backtest (0 lined games 2023-2025, ~20
# in early 2026 from the per-event Odds API capture), so we flip on
# and let the live tracker build the track record. Picks only generate
# for games where stored DK F5 odds exist, so volume ramps with odds-API
# coverage. Per-direction allow flags below stay open until a market
# shows clear EV-negative behavior in the tracker.
ENABLE_MLB_F5 = True
MLB_ALLOW_F5_ML = True
MLB_ALLOW_F5_OU_OVER = True
MLB_ALLOW_F5_OU_UNDER = True
MLB_ALLOW_F5_RL_FAVORITE = True
MLB_ALLOW_F5_RL_UNDERDOG = True

# Monte Carlo simulators (engine.mc_mlb / mc_nhl / mc_nba).
# MLB is live; NHL/NBA still shadow-only while their MC calibration
# accumulates outcome data.
ENABLE_MLB_MC = True
MLB_MC_N_SIMS = 50_000
ENABLE_NHL_MC = False
NHL_MC_N_SIMS = 50_000
ENABLE_NBA_MC = False
NBA_MC_N_SIMS = 50_000

# Gradient-boosted-tree (GBM) model (engine.gbm). MLB GBM trained on
# ~7500 historical games (Brier 0.2413 home_win, 0.2361 F5 home_win).
# Turn on the per-sport flag once engine.gbm.train has produced
# artifacts for that sport.
ENABLE_MLB_GBM = True
ENABLE_NHL_GBM = False   # training pipeline not built yet
ENABLE_NBA_GBM = False
