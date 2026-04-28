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
# Juice wall: don't recommend bets with worse odds than this.
# Set to -200 across the board — acceptable if the model is confident.
MLB_JUICE_WALL = -200
NHL_JUICE_WALL = -200
NBA_JUICE_WALL = -200

# ── Conservatism ladder ──
# Post-selection step that swaps a high-edge risky pick to the safest
# same-direction sibling (alt spread/total, ML for favorites) that
# still clears a reduced edge floor + the juice wall. Trades some
# theoretical EV for higher hit-rate so bankroll survives cold streaks
# while the empirical calibration tables build up. Lives in
# engine/conservatism.py; each sport's generate_picks invokes it
# after calibration and before confidence tiers.
CONSERVATISM_ENABLED = False  # Disabled 2026-04-22. Shipped on a
# hypothesis (trade EV for WR by swapping risky picks to safer
# siblings) but we never instrumented the `safened` flag to the DB,
# so there was no way to measure whether swapped picks actually
# performed better. Live-feel after ~1 week running was that the
# ladder wasn't helping — picks felt over-compressed into marginal
# +2% edge territory. The ladder module + ML→underdog extension
# stay in engine/conservatism.py as dormant infrastructure in case
# we want to re-enable with proper per-pick instrumentation later.
# Only consider a swap when the primary pick's probability is below
# this threshold — there's no reason to "safen" a pick already north
# of 55% that's also beating the market.
CONSERVATISM_ACTIVATE_UNDER_PROB = 0.55
# Candidate must still clear this edge AFTER the swap. Sits below
# EDGE_LEAN (4%) by design — we're deliberately trading edge for WR.
CONSERVATISM_MIN_EDGE_AFTER_SWAP = 2.0
# Candidate must improve the probability by at least this much vs
# the primary. Prevents swaps that barely move the needle but lose
# meaningful edge in the process.
CONSERVATISM_MIN_PROB_IMPROVEMENT = 0.05

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

# Same compression band, applied to O/U side probabilities. The tracker
# (244 picks, 04-25 train report) showed the 70-100% bucket hit 54.1%
# real WR vs 78.9% predicted — same pattern as ML, fixed via the same
# soft-compression. NegBin tails (engine.mlb_scoring) widened the raw
# distribution so the predicted probs should already drop toward the
# real-WR zone; this is defense-in-depth for any residual overconfidence.
MLB_OU_PROB_FLOOR = 0.40
MLB_OU_PROB_CAP = 0.60

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
NBA_ENABLE_ROSTER_ADJUSTMENT = True  # re-enabled 2026-04-21 with
# OUT-only gate (see engine/nba_injuries._classify_status). The old
# probabilistic weights (questionable=0.25, doubtful=0.5) averaged
# into a phantom 0.5pt shift because 75-80% of those listings ended
# up playing. Confirmed-OUT stars are a per-game 3-6pt Q1 swing that
# aggregate means drown. Pairs with engine/nba_injury_refresh.py which
# invalidates stale picks when a star flips to/from OUT on game day.

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
# Successor to NHL_ENABLE_GOALIE_SV — the old path used TEAM save%
# (lumped starter + backup together) which diluted any real goalie
# signal. The recency-weighted path blends the starting goalie's last
# 5 starts with season SV% and applies it as an xG multiplier. Default
# ON because it addresses the root cause of the ablation result.
NHL_ENABLE_GOALIE_RECENCY    = True
# Extend the L10 goals_for / goals_against blend (previously applied
# only to the O/U-specific xG) to the BASE xG used by ML + PL too.
# Hot/cold team form was flowing into totals but NOT into moneyline
# predictions — which left ML prices relying on season averages even
# when recent results clearly diverged. 60% season / 40% L10 matches
# the O/U blend so callers see a consistent response to recent form.
NHL_ENABLE_L10_BLEND         = True
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
MLB_ALLOW_NRFI = True           # NRFI has real edge (3-1 in tracker)
# YRFI off (final): 1st-INN GBM deep dive proved the legacy
# 2-signal model is the best we can do (18-feature GBM lost on
# every CV fold; engine.mlb_first_inning_train). The 9-14 /
# -$650 YRFI track wasn't a feature-quality problem we can fix.
# Re-enable when forward backtest with materially better data
# (lineup-aware top-3 OPS, pitch-mix feeds) shows positive ROI.
MLB_ALLOW_YRFI = False
# NRFI/YRFI floor lowered to 0.5% per user directive: first inning is
# effectively a coin flip, so we surface even razor-thin edges to
# accumulate sample size and validate the model on this market.
MLB_NRFI_MIN_EDGE = 0.5
MLB_YRFI_MIN_EDGE = 0.5
MLB_ALLOW_OU_OVER = True
MLB_ALLOW_OU_UNDER = True       # re-enabled 2026-04-28 per user;
                                # bias addressed via direction-aware
                                # empirical calibration (see
                                # engine.empirical_calibration —
                                # OU_DIRECTION shrinkage).

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
    # Phase 1 derivatives — start at 0.50 across the board until live
    # tracker history accumulates. The empirical-calibration loop will
    # bucket each new bet_type after ~50 settled samples and we can
    # tune these manually based on observed WR/ROI.
    "Team Total":      0.50,
    "F5 Team Total":   0.50,
    "Inning Total":    0.50,
    "Inning BTS":      0.40,  # rare events; demote slightly
    "1st Inn Winner":  0.50,
    "F5 Winner":       0.55,
    "Total O/E":       0.40,  # 50/50-shaped market; small edges suspect
    "Extra Innings":   0.50,
}

NHL_BET_RELIABILITY = {
    # Retro sweep with granular OFF puts the model at 53.7% across all
    # markets. Treat all three as roughly equal for ranking until we
    # have enough live picks to differentiate.
    "O/U": 1.00,
    "PL": 1.00,
    "ML": 0.85,
    # Phase 1 derivatives — start at 0.50 across the board until live
    # tracker history accumulates. Empirical-calibration loop will
    # bucket each new bet_type after ~50 settled samples and we can
    # tune these manually based on observed WR/ROI.
    "Team Total":   0.50,
    "Period Total": 0.50,
    "Period BTS":   0.40,  # rare events; demote slightly
    "Period DNB":   0.50,
    "Total O/E":    0.40,  # 50/50-shaped market; small edges suspect
    "Overtime":     0.50,
    "BTS":          0.50,
}

NBA_BET_RELIABILITY = {
    "Q1_SPREAD": 1.00,
    "Q1_TOTAL": 0.80,
    "Q1_ML": 0.60,
    # Phase 1 derivatives — start at 0.50 across the board until live
    # tracker history accumulates.
    "Q1 Team Total": 0.50,
    "Q1 Total O/E":  0.40,  # 50/50-shaped market; small edges suspect
}

# ── Derivative-market gating ──
# Hard-blocked markets never get recorded by the auto-recorder or
# selected as derivative POTD. Established from the leak-free retro
# backtest on 2026-04-25. These markets are not "small sample" — they
# fail at every confidence bucket, so a higher edge floor wouldn't help.
DERIVATIVE_BLOCKED_MARKETS = {
    "mlb": frozenset({
        # Inning BTS: 5.3% WR vs 27% predicted across 6,911 picks (-21pp
        # gap, every bucket overconfident, -73.67 ROI).
        "Inning BTS",
        # Inning Total: 45.8% WR vs 67% predicted across 71,906 picks
        # (-18pp gap, -8.41 ROI). Pure systemic overconfidence.
        "Inning Total",
    }),
    "nhl": frozenset(),
    "nba": frozenset(),
}

# Per-market edge floor override. Picks must clear this to be recorded
# or selected as POTD. Values omitted use the caller's default (4.0).
# Set when the standard 4% floor is profitable in aggregate but the
# higher-confidence buckets specifically lose money — raising the floor
# trades volume for survival.
DERIVATIVE_EDGE_FLOOR = {
    "mlb": {
        # F5 Winner: +7.94 ROI in aggregate but 60-80% predicted buckets
        # hit ~44%. The profit comes only from the lowest-conf bucket;
        # higher edges are all losers. Bump floor to drop the winners.
        "F5 Winner": 8.0,
    },
    "nhl": {
        # Team Total: +1.20 ROI evaporates with real-world juice. 60-70%
        # bucket hits 34.6%, 70-80% hits 40% — strong overconfidence at
        # high-edge picks. 8% floor cuts to the rare picks where
        # calibration cooperates.
        "Team Total": 8.0,
    },
    "nba": {},
}


# ── Main-market overrides ──
# Same shape as DERIVATIVE_EDGE_FLOOR but applies to ML/SPREAD/TOTAL.
# Per-bet-type floors are required when a backtest shows certain
# markets bleed at the standard 4% floor. Picks below the override
# get marked confidence='skip' and don't surface on the dashboard.
MAIN_EDGE_FLOOR = {
    "mlb": {},
    "nhl": {},
    "nba": {
        # NBA full-game backtest 2026-04-27 (n=1305 games):
        #   ML at 4%+   →  +33.6% ROI (synthetic -110)  → ship
        #   TOTAL at 4-12%  →  losing -16% to -34% ROI
        #   TOTAL at 12+%   →  near-break-even (-2.5%)
        # Floor TOTAL at 12% so we only surface the bucket that wasn't
        # actively losing at synthetic juice. Re-evaluate after a month
        # of live picks.
        "TOTAL":      12.0,
        # ALT lines couldn't be backtested (no historical posted alts).
        # Player props at high alt-line prices have lost money live —
        # same calibration risk applies here. Gate hard at 12% until
        # tracker data validates a lower floor.
        "ALT SPREAD": 12.0,
        "ALT TOTAL":  12.0,
        # SPREAD on the standard line: 4% floor, paper-track until ROI.
    },
}


# Per-bet-type max American odds. Picks at extreme prices (longshots,
# heavy chalks) carry calibration risk that backtest data can't always
# catch. Capping caps the damage from any one mispricing.
MAIN_ODDS_CAP = {
    "mlb": {
        # MLB heavy-underdog ML (e.g. CHC +2250 over SD's ace) — same
        # longshot calibration trap as NBA. Model says +45% edge but
        # the underdog wins ~5% of the time, and one bad break makes
        # the line move farther rather than closer to true. Cap at
        # +400 until live tracker data validates a higher cap.
        "ML": 400,
    },
    "nhl": {
        # NHL ML rarely goes higher than +200 in non-playoff games but
        # playoffs can stretch to +300+. Cap at +400 for symmetry with
        # MLB/NBA — same longshot risk in elimination spots.
        "ML": 400,
    },
    "nba": {
        # ML at +1000+ is a longshot trap — even a calibrated 12% prob
        # is one bad Q1 away from a -100 loss, and 12% mispriced as 18%
        # creates a fictional +18% edge that costs more than it gains.
        # Cap at +400 (~20% implied) so the picker won't surface
        # heavy-underdog ML picks until the model's longshot tail is
        # validated live.
        "ML": 400,
    },
}

# ── Weak markets - disabled by default ──
# 1st INN / NRFI: raw model is overconfident at the upper tail (predicted
# 80%+ buckets landed ~50% real WR in early tracker data). Kept ON
# because engine.picks.generate_picks calibrates prob against the
# tracker's empirical bucket hit rates and then re-filters picks whose
# calibrated edge went negative (see the re-filter step after the
# calibration loop). So 1st INN only surfaces when its empirical
# bucket actually beats the market price -- if calibration says the
# model is wrong about a bucket, that pick gets dropped before it
# reaches the Scoreboard / Best Bets / POTD surfaces.
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

# Phase 1 derivative markets — pure probability extraction from the
# existing prediction (per-team xR, per-inning xR, F5 split, total/margin
# distributions). No new factor multipliers. Master gate plus per-market
# flags so a single misbehaving market can be turned off without losing
# the others. Live tracker history will inform the per-market flips
# after ~50 settled samples per bet_type. See engine/mlb_derivative_picks.py.
ENABLE_MLB_DERIVATIVES = True
ENABLE_MLB_TEAM_TOTAL = True
ENABLE_MLB_F5_TEAM_TOTAL = True
ENABLE_MLB_INNING_TOTAL = True
ENABLE_MLB_INNING_BTS = True
ENABLE_MLB_INNING_WINNER = True
ENABLE_MLB_F5_WINNER_3WAY = True
ENABLE_MLB_TOTAL_OE = True
ENABLE_MLB_EXTRA_INNINGS = True

# NHL Phase 1 derivative markets — pure probability extraction from the
# existing prediction (per-team xG, per-period xG split, regulation-draw
# probability, total/margin distributions). No new factor multipliers.
# See engine/nhl_derivative_picks.py.
ENABLE_NHL_DERIVATIVES = True
ENABLE_NHL_TEAM_TOTAL = True
ENABLE_NHL_PERIOD_TOTAL = True
ENABLE_NHL_PERIOD_BTS = True
ENABLE_NHL_PERIOD_DNB = True
ENABLE_NHL_TOTAL_OE = True
ENABLE_NHL_OVERTIME = True
ENABLE_NHL_BTS_FULL = True

# NBA Phase 1 derivative markets — Q1-specific extensions priced
# directly off predict_q1's existing outputs (per-team Q1 expected
# points, total_probs distribution). Per-team Q1 sigma is derived as
# Q1_STD_DEV / sqrt(2) (independence assumption between the two team
# scores). The HR scraper gates these on period.startswith("Q1") so
# 1st-half markets sharing the same type code are dropped silently.
# See engine/nba_derivative_picks.py.
ENABLE_NBA_DERIVATIVES = True
ENABLE_NBA_Q1_TEAM_TOTAL = True
ENABLE_NBA_Q1_TOTAL_OE = True

# Monte Carlo simulators (engine.mc_mlb / mc_nhl / mc_nba).
# MLB is live; NHL/NBA still shadow-only while their MC calibration
# accumulates outcome data. Bumped 50k → 100k on 2026-04-24 alongside
# deterministic seeding (backend._mc_seed). 100k roughly halves the
# standard error on probability estimates (1/√N scaling) — modest
# accuracy gain, ~2x runtime per game; still fits in the per-slate
# refresh budget.
ENABLE_MLB_MC = True
MLB_MC_N_SIMS = 100_000
# NHL/NBA MC enabled 2026-04-24 so all derivative pickers can read
# from MC empirical distributions (per-period, total odd/even,
# overtime, BTS, team totals). Without MC enabled these derivatives
# fall back to factor Poisson which the backtest showed is materially
# overconfident on rare-event markets (BTS, Extras, Inning BTS).
ENABLE_NHL_MC = True
NHL_MC_N_SIMS = 100_000
ENABLE_NBA_MC = True
NBA_MC_N_SIMS = 100_000

# Gradient-boosted-tree (GBM) model (engine.gbm). Each sport trains
# ~4000-7500 historical games into 2-5 targets (home_win, total, and
# sport-specific period/inning markets).
# - MLB:  home_win Brier 0.2422, f5 Brier 0.2364 (accuracy 58.1%/59.2%)
# - NHL:  home_win Brier 0.2462 / 56.2% accuracy, total_goals RMSE 2.30
#         (p1 markets skipped -- nhl_games period scores only populated
#         for 37 of 4209 games; backfill period scores to unlock P1)
# - NBA:  home_win 70.6% / 0.1942 (late-season/playoff-weighted split),
#         q1_home_win 58.5% / 0.2343, q1_total RMSE 8.59
ENABLE_MLB_GBM = True
ENABLE_NHL_GBM = True
ENABLE_NBA_GBM = True
