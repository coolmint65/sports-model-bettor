"""Dixon-Coles bivariate Poisson predictor for the soccer framework.

Inputs:
    Home + away team Elo (skill prior)
    League-average home / away goal rates (calibration constants)
    Dixon-Coles ρ low-score correlation (calibration constant)

Outputs: a 1X2 + OU + BTTS market sheet derived from the joint
score-grid distribution.

Mathematical model
==================

For a match between Home (H) and Away (A) with Elo difference δ:

    λ_H = avg_home_goals · 10 ** (+δ_eff / 400 · k)
    λ_A = avg_away_goals · 10 ** (-δ_eff / 400 · k)

where δ_eff = δ + home_advantage_elo (in pure-skill mode; the home
advantage already lives inside avg_home_goals so we don't double-count
it). ``k`` scales Elo difference to a goal-rate ratio — empirically
~0.4 for top-flight men's club football (a 100-Elo edge corresponds
to ~10% higher scoring rate). League calibration replaces this default.

The joint score grid P(X=x, Y=y) is then Poisson(λ_H) ⊗ Poisson(λ_A),
adjusted by Dixon-Coles τ at low scores to correct the Poisson
independence assumption (0-0 and 1-1 occur more often than independent
Poissons predict; 1-0 and 0-1 occur less):

    τ(0,0) = 1 - λ_H · λ_A · ρ
    τ(0,1) = 1 + λ_H · ρ
    τ(1,0) = 1 + λ_A · ρ
    τ(1,1) = 1 - ρ
    τ(*,*) = 1  for any (x,y) outside the 2x2 low-score block

We compute up to MAX_GOALS×MAX_GOALS and renormalize so the grid sums
to 1 even when τ pushes mass around.

Derived markets
===============
    1X2     — sum the grid by sign(home - away)
    OU 2.5  — sum the grid by (home + away)
    BTTS    — sum the grid by (home > 0 AND away > 0)
    Correct score (top-N) — direct grid lookup (reserved for v2)

What's deliberately NOT here
============================

Per-team α/β attack/defense ratings. Full Dixon-Coles fits one α + β
per team via MLE over a season's matches — high-skill model but
expensive (n_teams parameters per league, refit weekly). v1 uses Elo
as a unified skill scalar; the MLE-fit per-team α/β is a v2 upgrade
that doesn't change any downstream consumer (1X2 / OU / BTTS shapes
stay identical).
"""
from __future__ import annotations

import logging
import math
from typing import Sequence

from ._config import get_league_config
from ._db import get_conn
from ._elo import get_rating, INIT_ELO, HOME_ADVANTAGE_ELO

logger = logging.getLogger(__name__)


# ── Tunables ────────────────────────────────────────────────

# Max goals per side we evaluate. 8 covers ~99.9% of probability mass
# for any realistic λ ≤ 4. Capping keeps the grid 81 cells (8+1 squared)
# — well within microsecond eval territory.
MAX_GOALS: int = 8

# Default league-mean goal rates when LEAGUE_REGISTRY has None (pre-
# calibration). Picked from ~80 years of European top-flight averages.
# These are home/away splits, not per-team — they already encode the
# home advantage in the goal rate.
DEFAULT_AVG_HOME_GOALS: float = 1.55
DEFAULT_AVG_AWAY_GOALS: float = 1.20

# Dixon-Coles low-score correlation. Negative-ish in most leagues —
# 1-0 and 0-1 happen LESS often than independent Poissons predict;
# 0-0 and 1-1 happen MORE often. -0.10 is a global prior; per-league
# calibration can fit it precisely.
DEFAULT_DC_RHO: float = -0.10

# Elo → goal-rate scaling factor. Each 400-Elo edge ≈ (10 ^ K) ratio
# of goal expectations. Lower K compresses the spread (more parity);
# higher K widens it (more dominant favorites). K=0.4 derived from
# empirical EPL backfit — re-tuned per league at calibration time.
ELO_GOAL_K_DEFAULT: float = 0.4


# ── Helpers ─────────────────────────────────────────────────

def _pmf_poisson(lam: float, k_max: int) -> list[float]:
    """Poisson PMF P(X=0..k_max) as a list. We don't use scipy.stats
    here because the soccer predictor needs to be importable without
    pulling scipy — basketball/golf already gate on numpy alone.

    Stable recurrence: P(0) = exp(-λ); P(k) = P(k-1) · λ / k."""
    out = [0.0] * (k_max + 1)
    if lam <= 0:
        out[0] = 1.0
        return out
    out[0] = math.exp(-lam)
    for k in range(1, k_max + 1):
        out[k] = out[k - 1] * lam / k
    return out


def _dixon_coles_tau(home: int, away: int, lam_h: float, lam_a: float,
                     rho: float) -> float:
    """Low-score correlation correction. Identity (1.0) outside the
    four cells in the 2x2 low-score block — the Poisson independence
    assumption is fine for scores like 3-1 or 4-2."""
    if home == 0 and away == 0:
        return 1.0 - lam_h * lam_a * rho
    if home == 0 and away == 1:
        return 1.0 + lam_h * rho
    if home == 1 and away == 0:
        return 1.0 + lam_a * rho
    if home == 1 and away == 1:
        return 1.0 - rho
    return 1.0


def _score_grid(lam_h: float, lam_a: float, rho: float,
                k_max: int = MAX_GOALS) -> list[list[float]]:
    """Joint probability matrix grid[home][away]. Renormalized to 1
    so we can integrate Dixon-Coles tweaks without breaking
    probability conservation."""
    p_h = _pmf_poisson(lam_h, k_max)
    p_a = _pmf_poisson(lam_a, k_max)
    grid = [[0.0] * (k_max + 1) for _ in range(k_max + 1)]
    total = 0.0
    for h in range(k_max + 1):
        for a in range(k_max + 1):
            cell = p_h[h] * p_a[a] * _dixon_coles_tau(h, a, lam_h, lam_a, rho)
            if cell < 0:
                cell = 0.0  # τ can push a few low-score cells negative for extreme ρ
            grid[h][a] = cell
            total += cell
    if total > 0:
        for h in range(k_max + 1):
            for a in range(k_max + 1):
                grid[h][a] /= total
    return grid


# ── Public API ──────────────────────────────────────────────

def predict_match(league: str, home_team_id: int, away_team_id: int,
                  *, neutral_site: bool = False,
                  home_side: str | None = None,
                  r_home_override: float | None = None,
                  r_away_override: float | None = None) -> dict:
    """Predict a single fixture for ``league``. Returns a dict carrying
    every soccer market we currently model — the downstream picks
    engine fans this out into per-bet edge calcs.

    Shape::

        {
          "lambda_home", "lambda_away",
          "p_home", "p_draw", "p_away",        # 1X2
          "p_over_15", "p_under_15",
          "p_over_25", "p_under_25",
          "p_over_35", "p_under_35",
          "p_btts_yes", "p_btts_no",
          "p_dc_home", "p_dc_draw", "p_dc_away",  # double-chance
          "p_dnb_home", "p_dnb_away",              # draw no bet
          "top_scores": [{home, away, p}, …]       # top 6 by prob
        }

    ``r_{home,away}_override`` bypasses the team_elo DB lookup with a
    caller-supplied Elo. Walk-forward calibration uses this to replay
    Elo state in memory at each holdout match without leaking future
    results back into the rating prior.
    """
    cfg = get_league_config(league)
    avg_home = cfg.get("avg_home_goals") or DEFAULT_AVG_HOME_GOALS
    avg_away = cfg.get("avg_away_goals") or DEFAULT_AVG_AWAY_GOALS
    rho = cfg.get("dc_rho") if cfg.get("dc_rho") is not None else DEFAULT_DC_RHO
    elo_k = cfg.get("elo_goal_k") or ELO_GOAL_K_DEFAULT
    # Double-count guard. HOME_ADVANTAGE_ELO=65 exists for leagues where
    # the goal-rate scaffold is SYMMETRIC (WC neutral pitches, unfitted
    # leagues, tour-shared leagues that don't ship per-league averages).
    # For every league where avg_home_goals != avg_away_goals, the
    # asymmetry ALREADY encodes the home boost — layering the Elo
    # fallback on top double-counts and pushes home-side probs 5-8pp
    # above reality (USL DNB went 0-11 on the home side because of this,
    # 2026-07-08 audit). Only apply the Elo fallback when the goal
    # scaffold is symmetric OR the league has explicitly fitted its
    # own home_advantage_elo.
    fitted_elo_adv = cfg.get("home_advantage_elo")
    if fitted_elo_adv is not None:
        base_adv = fitted_elo_adv
    elif abs(avg_home - avg_away) < 0.02:
        base_adv = HOME_ADVANTAGE_ELO
    else:
        base_adv = 0.0

    # Effective home_advantage sign:
    #   "home"    →  +base_adv  (labeled home is at home)
    #   "neutral" →   0
    #   "away"    →  -base_adv  (labeled away is at home — flip)
    # Legacy callers passing only `neutral_site` fall through to a
    # two-state {home, neutral} interpretation.
    if home_side is None:
        home_side = "neutral" if neutral_site else "home"
    if home_side == "home":
        home_adv = base_adv
    elif home_side == "away":
        home_adv = -base_adv
    else:
        home_adv = 0.0

    # On a neutral pitch the "home"/"away" labels are an arbitrary
    # ESPN tag; using asymmetric per-side goal averages bakes a phantom
    # +0.5 goal tilt into the lambda scaffold. World Cup ROI on
    # neutral-site picks was being skewed by this — labeled-home side
    # always scored higher in expectation regardless of skill. Collapse
    # to the pooled mean so only the Elo delta differentiates the two
    # sides on neutral pitches. Away-team-at-home case also uses the
    # pooled mean since the labels remain arbitrary; the sign-flipped
    # home_adv above does the work of redirecting the tilt.
    if home_side != "home":
        pooled = (avg_home + avg_away) / 2.0
        avg_home = avg_away = pooled

    if r_home_override is not None and r_away_override is not None:
        r_home = float(r_home_override)
        r_away = float(r_away_override)
    else:
        # WC borrows its Elo from the fifa_internationals pool. The WC DB
        # itself only contains the 64-match bracket — way too few samples
        # to derive credible team strength. The pool aggregates every
        # qualifier + friendly across all 5 confederations so the same
        # Spain / Argentina / Brazil rating drives both qualifier picks AND
        # the WC predictor.
        elo_source = "fifa_internationals" if league == "fifa_world_cup" else league
        conn = get_conn(elo_source)
        # Team IDs are ESPN-stable across leagues — the WC games table
        # carries the same team_id ESPN ships for the qualifiers, so the
        # cross-DB lookup resolves without an extra join.
        r_home = get_rating(conn, home_team_id)
        r_away = get_rating(conn, away_team_id)
    delta = (r_home + home_adv) - r_away

    # Scale Elo delta into a multiplicative goal-rate tilt. Each 400
    # Elo points = factor 10^K. Half of that lifts the home rate, the
    # other half cuts the away rate (zero-sum on the goal-ratio).
    tilt = 10 ** (delta / 400.0 * elo_k)
    lam_h = avg_home * math.sqrt(tilt)
    lam_a = avg_away / math.sqrt(tilt)
    # Floor + ceiling to keep Poisson stable for extreme mismatches.
    lam_h = min(max(lam_h, 0.1), 5.0)
    lam_a = min(max(lam_a, 0.1), 5.0)

    grid = _score_grid(lam_h, lam_a, rho)

    p_home = 0.0
    p_draw = 0.0
    p_away = 0.0
    p_btts_yes = 0.0
    p_over = {1.5: 0.0, 2.5: 0.0, 3.5: 0.0}
    for h in range(len(grid)):
        for a in range(len(grid[h])):
            p = grid[h][a]
            if h > a:
                p_home += p
            elif h < a:
                p_away += p
            else:
                p_draw += p
            total = h + a
            for line in p_over:
                if total > line:
                    p_over[line] += p
            if h > 0 and a > 0:
                p_btts_yes += p

    # H1 markets — derived from H1 lambdas. h1_share scales full-game
    # lambdas down to first-half rates; soccer's tactical opening +
    # less-tired-legs effect lowers H1 scoring to ~45% of full game.
    # When h1_share isn't fitted (legacy constants without HT data) we
    # fall back to 0.45 as a sane prior so H1 picks can still fire.
    h1_share = cfg.get("h1_share") or 0.45
    lam_h1_h = max(lam_h * h1_share, 0.05)
    lam_h1_a = max(lam_a * h1_share, 0.05)
    h1_grid = _score_grid(lam_h1_h, lam_h1_a, rho)
    p_h1_home = p_h1_draw = p_h1_away = p_h1_btts_yes = 0.0
    p_h1_over = {0.5: 0.0, 1.5: 0.0, 2.5: 0.0}
    for h in range(len(h1_grid)):
        for a in range(len(h1_grid[h])):
            p = h1_grid[h][a]
            if h > a:
                p_h1_home += p
            elif h < a:
                p_h1_away += p
            else:
                p_h1_draw += p
            t = h + a
            for line in p_h1_over:
                if t > line:
                    p_h1_over[line] += p
            if h > 0 and a > 0:
                p_h1_btts_yes += p

    # Top correct scores (display only — not yet a picks market)
    top = []
    for h in range(len(grid)):
        for a in range(len(grid[h])):
            top.append((h, a, grid[h][a]))
    top.sort(key=lambda t: -t[2])

    # ── Extended team-line markets (2026-07-01) ─────────────
    # HR ships CS / HTFT / GIBH / Winning Margin on WC games (and
    # league fixtures) with real prices; every one derives from the
    # score grid we already compute, so exposing them is bookkeeping,
    # not modelling. Adds ~4 new bet_types the picks engine can pick
    # against without adding a factor.

    # 1) Correct Score. Straight score-grid lookup. Ship the top 20
    #    cells so the pick emitter can rank against HR's price list
    #    (HR ships 30-40 CS cells per game); the tail bleeds into a
    #    residual "AOS" (Any Other Score) bucket the emitter aggregates.
    cs_flat = sorted(
        [(h, a, grid[h][a]) for h in range(len(grid))
                             for a in range(len(grid[h]))],
        key=lambda t: -t[2],
    )
    p_cs = {f"{h}-{a}": p for h, a, p in cs_flat[:20]}
    p_cs_aos = max(1.0 - sum(p_cs.values()), 0.0)

    # 2) Half-time / Full-time joint. Uses independent H1/H2 lambdas
    #    to build a 4-D joint over (h1_h, h1_a, h2_h, h2_a). 8×8×8×8 =
    #    4096 cells, weighted by p_h1(h1_grid) × p_h2(h2_grid) — fast.
    lam_h2_h = max(lam_h - lam_h1_h, 0.05)
    lam_h2_a = max(lam_a - lam_h1_a, 0.05)
    h2_grid = _score_grid(lam_h2_h, lam_h2_a, rho)
    _ht_ft_keys = ("H", "D", "A")
    p_htft: dict[str, float] = {
        f"{ht}/{ft}": 0.0 for ht in _ht_ft_keys for ft in _ht_ft_keys
    }
    p_gibh_yes = 0.0
    for h1h in range(len(h1_grid)):
        for h1a in range(len(h1_grid[h1h])):
            p_h1 = h1_grid[h1h][h1a]
            if p_h1 <= 0:
                continue
            ht_result = ("H" if h1h > h1a
                          else "A" if h1a > h1h else "D")
            h1_scored = (h1h + h1a) >= 1
            for h2h in range(len(h2_grid)):
                for h2a in range(len(h2_grid[h2h])):
                    p_h2 = h2_grid[h2h][h2a]
                    if p_h2 <= 0:
                        continue
                    p_joint = p_h1 * p_h2
                    fh = h1h + h2h
                    fa = h1a + h2a
                    ft_result = ("H" if fh > fa
                                  else "A" if fa > fh else "D")
                    p_htft[f"{ht_result}/{ft_result}"] += p_joint
                    if h1_scored and (h2h + h2a) >= 1:
                        p_gibh_yes += p_joint

    # 3) To Advance. Knockout-stage only. Same H1×H2 machinery above
    #    already tells us P(reg win) via p_home/p_away; the only new
    #    piece is P(win in extra time OR shootout | tied @90). Model
    #    ET as another Poisson trial at 30/90 of the full-game rates,
    #    penalty shootout as ~50/50 (skill gap in individual takers
    #    is empirically noisy — assume symmetric). Group-stage callers
    #    just ignore the field.
    _ET_SHARE = 30.0 / 90.0
    lam_h_et = max(lam_h * _ET_SHARE, 0.05)
    lam_a_et = max(lam_a * _ET_SHARE, 0.05)
    et_grid = _score_grid(lam_h_et, lam_a_et, rho)
    p_home_wins_et = p_draw_et = p_away_wins_et = 0.0
    for he in range(len(et_grid)):
        for ae in range(len(et_grid[he])):
            pe = et_grid[he][ae]
            if he > ae:
                p_home_wins_et += pe
            elif ae > he:
                p_away_wins_et += pe
            else:
                p_draw_et += pe
    _PENS_HOME_SHARE = 0.5     # empirically flat; refine if we ship
                                # a penalty-taker model
    p_home_after_et = p_home_wins_et + p_draw_et * _PENS_HOME_SHARE
    p_away_after_et = p_away_wins_et + p_draw_et * (1.0 - _PENS_HOME_SHARE)
    p_advance_home = p_home + p_draw * p_home_after_et
    p_advance_away = p_away + p_draw * p_away_after_et

    # 4) Winning Margin. Bin by (home_score - away_score); HR ships
    #    "USA by 1/2/3/4+" and "Draw 0-0/1-1/2-2 etc." Positive =
    #    home margin, negative = away margin, 0 = any draw.
    p_wm: dict[str, float] = {}
    for h in range(len(grid)):
        for a in range(len(grid[h])):
            p = grid[h][a]
            if p <= 0:
                continue
            diff = h - a
            if diff == 0:
                key = "draw"
            elif diff >= 4:
                key = "home+4"
            elif diff >= 1:
                key = f"home+{diff}"
            elif diff <= -4:
                key = "away+4"
            else:
                key = f"away+{-diff}"
            p_wm[key] = p_wm.get(key, 0.0) + p

    return {
        "lambda_home": lam_h,
        "lambda_away": lam_a,
        "elo_home": r_home,
        "elo_away": r_away,
        "neutral_site": bool(neutral_site),

        "p_home": p_home,
        "p_draw": p_draw,
        "p_away": p_away,

        "p_over_15": p_over[1.5],
        "p_under_15": 1.0 - p_over[1.5],
        "p_over_25": p_over[2.5],
        "p_under_25": 1.0 - p_over[2.5],
        "p_over_35": p_over[3.5],
        "p_under_35": 1.0 - p_over[3.5],

        "p_btts_yes": p_btts_yes,
        "p_btts_no": 1.0 - p_btts_yes,

        # Double-chance = "win or draw" pair; complements the 1X2 cell
        # the bookmaker omits. Computed from the 1X2 distribution.
        "p_dc_home": p_home + p_draw,
        "p_dc_away": p_away + p_draw,
        "p_dc_draw": p_home + p_away,    # "no draw" = either side wins

        # Draw-no-bet = remove the draw mass, renormalize to 1.
        "p_dnb_home": p_home / (p_home + p_away) if (p_home + p_away) > 0 else 0.5,
        "p_dnb_away": p_away / (p_home + p_away) if (p_home + p_away) > 0 else 0.5,

        # H1 (first-half) predictions. Same set of 1X2/DC/DNB/BTTS/OU
        # outputs, just keyed with the h1_ prefix.
        "lambda_h1_home": lam_h1_h,
        "lambda_h1_away": lam_h1_a,
        "p_h1_home": p_h1_home,
        "p_h1_draw": p_h1_draw,
        "p_h1_away": p_h1_away,
        "p_h1_over_05": p_h1_over[0.5],
        "p_h1_under_05": 1.0 - p_h1_over[0.5],
        "p_h1_over_15": p_h1_over[1.5],
        "p_h1_under_15": 1.0 - p_h1_over[1.5],
        "p_h1_over_25": p_h1_over[2.5],
        "p_h1_under_25": 1.0 - p_h1_over[2.5],
        "p_h1_btts_yes": p_h1_btts_yes,
        "p_h1_btts_no": 1.0 - p_h1_btts_yes,
        "p_h1_dc_home": p_h1_home + p_h1_draw,
        "p_h1_dc_away": p_h1_away + p_h1_draw,
        "p_h1_dc_draw": p_h1_home + p_h1_away,
        "p_h1_dnb_home": (p_h1_home / (p_h1_home + p_h1_away)
                          if (p_h1_home + p_h1_away) > 0 else 0.5),
        "p_h1_dnb_away": (p_h1_away / (p_h1_home + p_h1_away)
                          if (p_h1_home + p_h1_away) > 0 else 0.5),

        "top_scores": [
            {"home": h, "away": a, "p": p}
            for (h, a, p) in top[:6]
        ],

        # Extended markets — derived from the same Poisson grid.
        "p_cs": p_cs,                    # {"1-0": 0.15, ...} top-20 scores
        "p_cs_aos": p_cs_aos,            # "Any Other Score" residual
        "p_htft": p_htft,                # 9-way {"H/H", "H/D", ...}
        "p_gibh_yes": p_gibh_yes,
        "p_gibh_no": 1.0 - p_gibh_yes,
        "p_wm": p_wm,                    # winning margin bins
        # Advance — only meaningful for KO fixtures. Includes ET + pens
        # conditional on regulation tie. Sums to ~1.0 (no draw bucket).
        "p_advance_home": p_advance_home,
        "p_advance_away": p_advance_away,
    }


def predict_slate(league: str, date: str) -> list[dict]:
    """Predict every match for ``league`` on ``date`` (UTC). Returns a
    list of ``{match_id, …}`` dicts; the picks engine wraps each with
    market-specific edge calculations.

    V3.1 market blend: when the league has ``v31_market_blend`` set
    in the registry AND we have Pinnacle closing odds for the match
    in ``historical_odds.db``, the 1X2/DC/DNB probability legs are
    blended with the market-implied probabilities. Validated -1.7 to
    -3.3% Brier per league on the 2024-25+ holdout.
    """
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT m.id, m.start_time, m.home_team_id, m.away_team_id, "
        "       m.neutral_site, m.home_side, m.round_name, m.date, "
        "       ht.name AS home_name, ht.abbreviation AS home_abbr, "
        "       ht.logo_url AS home_logo, "
        "       at.name AS away_name, at.abbreviation AS away_abbr, "
        "       at.logo_url AS away_logo "
        "FROM matches m "
        "JOIN teams ht ON ht.id = m.home_team_id "
        "JOIN teams at ON at.id = m.away_team_id "
        "WHERE m.date = ? AND m.status IN ('scheduled', 'live') "
        "ORDER BY m.start_time ASC",
        (date,),
    ).fetchall()
    cfg = get_league_config(league)
    blend_w = cfg.get("v31_market_blend")
    out: list[dict] = []
    for r in rows:
        pred = predict_match(
            league,
            int(r["home_team_id"]),
            int(r["away_team_id"]),
            neutral_site=bool(r["neutral_site"]),
            home_side=r["home_side"],
        )
        if blend_w is not None:
            _apply_v31_blend(pred, league, r["date"],
                              r["home_abbr"], r["away_abbr"],
                              float(blend_w))
        pred["match_id"] = int(r["id"])
        pred["start_time"] = r["start_time"]
        pred["home_team_id"] = int(r["home_team_id"])
        pred["away_team_id"] = int(r["away_team_id"])
        pred["home_name"] = r["home_name"]
        pred["away_name"] = r["away_name"]
        pred["home_abbr"] = r["home_abbr"]
        pred["away_abbr"] = r["away_abbr"]
        pred["home_logo"] = r["home_logo"]
        pred["away_logo"] = r["away_logo"]
        pred["matchup"] = f"{r['away_abbr']} @ {r['home_abbr']}"
        out.append(pred)
    return out


def _apply_v31_blend(pred: dict, league: str, match_date: str,
                      home_abbr: str, away_abbr: str, w: float) -> None:
    """Replace pred's 1X2 / DC / DNB legs with the v31 blended values
    when market data exists. Mutates ``pred`` in place.

    ``w`` is the DC weight (0.5 = 50/50 blend with market). Higher w
    favors DC; lower w favors the book's closing line. Untouched
    probabilities (OU / BTTS / lambdas / H1) stay raw because Pinnacle
    closing data only covers 1X2 + the 2.5 totals line.
    """
    try:
        from ._market_join import market_features_for_match
    except Exception:
        return
    feat = market_features_for_match(
        league, match_date=match_date,
        home_abbr=home_abbr, away_abbr=away_abbr,
    )
    if not feat or not feat.get("has_market_data"):
        return
    m_h = feat.get("market_home_implied")
    m_d = feat.get("market_draw_implied")
    m_a = feat.get("market_away_implied")
    if m_h is None or m_d is None or m_a is None:
        return
    # Blend the 3 1X2 legs.
    p_h_new = w * pred["p_home"] + (1.0 - w) * m_h
    p_d_new = w * pred["p_draw"] + (1.0 - w) * m_d
    p_a_new = w * pred["p_away"] + (1.0 - w) * m_a
    pred["p_home"] = p_h_new
    pred["p_draw"] = p_d_new
    pred["p_away"] = p_a_new
    # Recompute derived legs so DC + DNB stay consistent with the
    # blended 1X2 distribution.
    pred["p_dc_home"]  = p_h_new + p_d_new
    pred["p_dc_away"]  = p_a_new + p_d_new
    pred["p_dc_draw"]  = p_h_new + p_a_new
    if (p_h_new + p_a_new) > 0:
        pred["p_dnb_home"] = p_h_new / (p_h_new + p_a_new)
        pred["p_dnb_away"] = p_a_new / (p_h_new + p_a_new)
    pred["v31_market_blend_applied"] = round(w, 3)
