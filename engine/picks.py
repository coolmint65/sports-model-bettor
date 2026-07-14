"""
Unified pick generation.

Single source of truth for computing betting picks for a game.
Used by Best Bets, Pick Tracker, and Game Detail sidebar.

Every pick includes: type, pick label, model probability,
edge vs real odds, the actual odds, and confidence level.
"""

import logging
from datetime import datetime

from .mlb_predict import predict_matchup, MLB_AVG_RPG
from .config import MLB_JUICE_WALL as JUICE_WALL, MLB_BET_RELIABILITY, get_flag
from .db import get_conn
from ._tz import et_today_str

logger = logging.getLogger(__name__)


def _implied(ml: int) -> float:
    """American odds to implied probability."""
    if ml < 0:
        return abs(ml) / (abs(ml) + 100)
    return 100 / (ml + 100)


def _valid_odds(ml) -> bool:
    """Shape check: real American odds are |ml| >= 100.

    Guards against garbage values (-2, +3, 0) that some upstream odds
    parsers have leaked into the odds dict. Left unguarded, -2 reads as
    1.96% implied, which inflates edge by ~70pp and triggers a $5000
    profit on a $100 "win" via the payout formula.
    """
    if ml is None:
        return False
    try:
        ml = int(ml)
    except (TypeError, ValueError):
        return False
    return abs(ml) >= 100


def _sanitize_odds(odds: dict | None) -> dict:
    """Null out any *_ml / *_odds field that fails the _valid_odds shape
    check. Run once at the top of generate_picks so every downstream
    guard (which already tests `if x and x >= JUICE_WALL`) naturally
    skips invalid values without extra inline checks."""
    if not odds:
        return {}
    cleaned = dict(odds)
    for k, v in list(cleaned.items()):
        if (k.endswith("_ml") or k.endswith("_odds")) and not _valid_odds(v):
            cleaned[k] = None
    return cleaned


def _payout(odds: int, won: bool) -> float:
    """Calculate profit on a $100 bet."""
    if won:
        if odds > 0:
            return float(odds)
        else:
            return 100 / abs(odds) * 100
    return -100.0


def generate_picks(home_team_id: int, away_team_id: int,
                    home_pitcher_id: int | None = None,
                    away_pitcher_id: int | None = None,
                    venue: str | None = None,
                    odds: dict | None = None,
                    pred: dict | None = None) -> list[dict]:
    """
    Generate all betting picks for a game.

    Args:
        home/away_team_id: MLB team IDs
        home/away_pitcher_id: Starting pitcher IDs
        venue: Ballpark name
        odds: Real sportsbook odds dict (Hard Rock primary):
              {home_ml, away_ml, over_under, over_odds, under_odds,
               home_spread_odds, away_spread_odds,
               home_spread_point, away_spread_point}

    Returns list of picks, sorted by edge (best first):
    [
        {
            "type": "ML" | "O/U" | "1st INN" | "RL",
            "pick": "NYY",
            "prob": 0.542,
            "edge": 3.2,
            "odds": -120,
            "confidence": "medium",
        },
        ...
    ]
    """
    # Run prediction (or reuse the pre-computed one from the caller).
    # /api/predict passes its own pred in to avoid computing twice.
    if pred is None:
        pred = predict_matchup(
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            home_pitcher_id=home_pitcher_id,
            away_pitcher_id=away_pitcher_id,
            venue=venue,
        )

    if "error" in pred or not pred:
        return []

    odds = _sanitize_odds(odds)
    wp = pred.get("win_prob", {})
    rl = pred.get("run_line", {})
    fi = pred.get("first_inning", {})
    total = pred.get("total", 0)
    conf = pred.get("confidence", {}) or {}
    conf_score = conf.get("score", 50)
    # CI half-width on the win-prob estimate, driven by data quality
    # (pitcher-start + team-game sample sizes). Surface via prob_low /
    # prob_high so the UI can render a band around the point estimate.
    ci_hw = conf.get("ci_half_width", 0.05)

    # Monte Carlo shadow: replace the factor model's full F5 distribution
    # (expected_runs, over_under, run_line) with MC's at-bat-level sim.
    # MC captures inning-level stochasticity that the factor Poisson
    # blend can't. NRFI and F5-ML *scalar* probabilities fall through
    # to the ensemble block below, which blends MC + factor + GBM with
    # per-market tuned weights (strictly better than 100% MC).
    mc = pred.get("mc") or {}
    if mc and "error" not in mc and mc.get("f5"):
        f5_rl = mc["f5"]["run_line"]
        mc_f5 = {
            "home": mc["f5"]["expected_runs"]["home"],
            "away": mc["f5"]["expected_runs"]["away"],
            "total": mc["f5"]["expected_runs"]["total"],
            "win_prob": mc["f5"]["win_prob"],
            "over_under": mc["f5"]["over_under"],
            "run_line": {
                "home_minus_0_5": f5_rl.get("home_-0.5", 0.5),
                "home_plus_0_5":  f5_rl.get("home_+0.5", 0.5),
                "away_minus_0_5": 1.0 - f5_rl.get("home_+0.5", 0.5),
                "away_plus_0_5":  1.0 - f5_rl.get("home_-0.5", 0.5),
            },
        }
        pred["f5"] = mc_f5

    home = pred.get("home", {})
    away = pred.get("away", {})
    h_abbr = home.get("abbreviation", "HOME")
    a_abbr = away.get("abbreviation", "AWAY")

    # Scalar market probabilities route through the ensemble (factor +
    # MC + GBM with per-market tuned weights) when the caller populated
    # pred["ensemble"]; otherwise they fall back to the factor model.
    # O/U and RL distributions stay on factor -- ensemble only blends
    # scalar EVs and probabilities, not full distributions.
    ens = pred.get("ensemble") or {}

    # Moneyline home WP
    if ens.get("home_win") is not None:
        home_wp = float(ens["home_win"])
        away_wp = 1.0 - home_wp
    else:
        home_wp = wp.get("home", 0.5)
        away_wp = wp.get("away", 0.5)

    # NRFI scalar (blended across all three components)
    if ens.get("nrfi") is not None:
        fi = {"nrfi": float(ens["nrfi"]), "yrfi": 1.0 - float(ens["nrfi"])}

    # F5 home-win scalar: blend into the MC-derived distribution above
    # so _append_f5_picks reads the blended prob, but keeps MC's O/U
    # and RL distributions (which the ensemble can't express).
    if ens.get("f5_home_win") is not None and pred.get("f5"):
        pred["f5"].setdefault("win_prob", {})
        pred["f5"]["win_prob"]["home"] = float(ens["f5_home_win"])
        pred["f5"]["win_prob"]["away"] = 1.0 - float(ens["f5_home_win"])
    # All four RL sides
    rl_home_minus = rl.get("home_minus_1_5", 0.5)   # P(home wins by 2+)
    rl_home_plus = rl.get("home_plus_1_5", 0.5)     # P(home covers +1.5)
    rl_away_minus = rl.get("away_minus_1_5", 0.5)   # P(away wins by 2+)
    rl_away_plus = rl.get("away_plus_1_5", 0.5)     # P(away covers +1.5)

    picks = []

    # ── Moneyline ──
    # Cap at MAIN_ODDS_CAP['mlb']['ML'] (default +400). Heavy-underdog
    # ML picks at +1500+ surface fictional +30-50% edges from model
    # overconfidence on longshots; capping blocks the trap.
    from .config import MAIN_ODDS_CAP as _MAIN_ODDS_CAP
    _ml_cap = (_MAIN_ODDS_CAP.get("mlb") or {}).get("ML")
    def _ml_within_cap(o):
        if _ml_cap is None or o is None:
            return True
        try:
            return int(o) <= int(_ml_cap)
        except (TypeError, ValueError):
            return True

    home_ml = odds.get("home_ml")
    away_ml = odds.get("away_ml")

    # Migrated 2026-05-02 to engine.picks_core.score_pick. Shared
    # core handles juice wall + odds cap + belief gate + calibration
    # + edge ceiling + edge floor in one place.
    from .picks_core import score_pick as _score_pick
    if home_ml is not None:
        scored = _score_pick({
            "type": "ML", "pick": h_abbr,
            "raw_prob": home_wp, "odds": home_ml,
        }, sport="mlb", juice_wall=JUICE_WALL)
        if scored:
            picks.append(scored)
    if away_ml is not None:
        scored = _score_pick({
            "type": "ML", "pick": a_abbr,
            "raw_prob": away_wp, "odds": away_ml,
        }, sport="mlb", juice_wall=JUICE_WALL)
        if scored:
            picks.append(scored)

    # ── Over/Under ──  Migrated 2026-05-02 to picks_core.score_pick.
    # MLB-specific operator policy (MLB_ALLOW_OU_OVER/UNDER) and the
    # NegBin-residual soft-compress (compress_win_prob) stay here as
    # pre-pipeline shaping; everything past that goes through the
    # shared core.
    vegas_total = odds.get("over_under")
    if vegas_total and pred.get("over_under"):
        ou_data = _find_ou(pred["over_under"], vegas_total)
        if ou_data:
            ou_pick_over = ou_data["over"] > ou_data["under"]
            ou_allowed = (ou_pick_over and get_flag("MLB_ALLOW_OU_OVER", True)) or \
                         ((not ou_pick_over) and get_flag("MLB_ALLOW_OU_UNDER", True))
            if ou_allowed:
                from .win_prob import compress_win_prob
                ou_prob = compress_win_prob(
                    max(ou_data["over"], ou_data["under"]),
                    get_flag("MLB_OU_PROB_FLOOR", 0.40),
                    get_flag("MLB_OU_PROB_CAP", 0.60),
                )
                real_ou_odds = (odds.get("over_odds") if ou_pick_over
                                else odds.get("under_odds")) or -110
                from .picks_core import score_pick as _score_pick
                scored = _score_pick({
                    "type": "O/U",
                    "pick": f"{'Over' if ou_pick_over else 'Under'} {vegas_total}",
                    "raw_prob": ou_prob,
                    "odds": real_ou_odds,
                }, sport="mlb", juice_wall=JUICE_WALL)
                if scored:
                    picks.append(scored)

    # ── First Inning (NRFI/YRFI) ──  Migrated to picks_core.score_pick.
    # Operator policy (ENABLE_MLB_NRFI, MLB_ALLOW_NRFI/YRFI direction
    # flags) and the side selection by max(nrfi, yrfi) stay here.
    if get_flag("ENABLE_MLB_NRFI", True):
        nrfi = fi.get("nrfi", 0.5)
        nrfi_pick = "NRFI" if nrfi > 0.5 else "YRFI"
        nrfi_prob = nrfi if nrfi > 0.5 else fi.get("yrfi", 0.5)
        allow = (nrfi_pick == "NRFI" and get_flag("MLB_ALLOW_NRFI", True)) or \
                (nrfi_pick == "YRFI" and get_flag("MLB_ALLOW_YRFI", True))
        if allow:
            i1 = (odds.get("inning_totals") or {}).get("I1") or {}
            if nrfi_pick == "NRFI":
                nrfi_odds = (i1.get("under_odds")
                             or odds.get("nrfi_under_odds")
                             or _nrfi_fallback_odds("NRFI"))
            else:
                nrfi_odds = (i1.get("over_odds")
                             or odds.get("nrfi_over_odds")
                             or _nrfi_fallback_odds("YRFI"))
            from .picks_core import score_pick as _score_pick
            scored = _score_pick({
                "type": "1st INN",
                "pick": nrfi_pick,
                "raw_prob": nrfi_prob,
                "odds": nrfi_odds,
            }, sport="mlb", juice_wall=JUICE_WALL)
            if scored:
                picks.append(scored)

    # ── Run Line ──
    # Use real odds when available, otherwise derive from ML
    home_rl_odds = odds.get("home_spread_odds")
    away_rl_odds = odds.get("away_spread_odds")
    home_rl_point = odds.get("home_spread_point")
    away_rl_point = odds.get("away_spread_point")

    # If no RL data from API, derive from ML: favorite gets -1.5, dog gets +1.5
    if home_rl_point is None and home_ml and away_ml:
        home_is_fav = (home_ml < 0 and abs(home_ml) > abs(away_ml)) if home_ml < 0 else False
        if not home_is_fav and away_ml < 0:
            home_is_fav = False
        elif home_ml < 0:
            home_is_fav = True

        if home_is_fav:
            home_rl_point = -1.5
            away_rl_point = 1.5
            home_rl_odds = home_rl_odds or 120   # Fav -1.5 pays +120
            away_rl_odds = away_rl_odds or -140   # Dog +1.5 costs -140
        else:
            home_rl_point = 1.5
            away_rl_point = -1.5
            home_rl_odds = home_rl_odds or -140   # Dog +1.5 costs -140
            away_rl_odds = away_rl_odds or 120    # Fav -1.5 pays +120

    # Direction filter for RL: tracker data shows +1.5 dogs are profitable
    # (40-27, 59.7%) while -1.5 favorites are disastrous (3-9, 25%).

    # Migrated 2026-05-02 to engine.picks_core.score_pick. The flag
    # gate (MLB_ALLOW_RL_UNDERDOG / FAVORITE) stays here because it's
    # MLB-specific operator policy, not a structural pipeline gate —
    # operator can disable an entire RL direction independently.
    from .picks_core import score_pick as _score_pick
    if home_rl_point is not None:
        is_dog = home_rl_point > 0
        rl_allowed = (is_dog and get_flag("MLB_ALLOW_RL_UNDERDOG", True)) or \
                     ((not is_dog) and get_flag("MLB_ALLOW_RL_FAVORITE", True))
        if rl_allowed and home_rl_odds is not None:
            rl_prob = rl_home_minus if home_rl_point < 0 else rl_home_plus
            sign = "+" if home_rl_point > 0 else ""
            scored = _score_pick({
                "type": "RL",
                "pick": f"{h_abbr} {sign}{home_rl_point}",
                "raw_prob": rl_prob,
                "odds": home_rl_odds,
            }, sport="mlb", juice_wall=JUICE_WALL)
            if scored:
                picks.append(scored)
    if away_rl_point is not None:
        is_dog = away_rl_point > 0
        rl_allowed = (is_dog and get_flag("MLB_ALLOW_RL_UNDERDOG", True)) or \
                     ((not is_dog) and get_flag("MLB_ALLOW_RL_FAVORITE", True))
        if rl_allowed and away_rl_odds is not None:
            rl_prob = rl_away_plus if away_rl_point > 0 else rl_away_minus
            sign = "+" if away_rl_point > 0 else ""
            scored = _score_pick({
                "type": "RL",
                "pick": f"{a_abbr} {sign}{away_rl_point}",
                "raw_prob": rl_prob,
                "odds": away_rl_odds,
            }, sport="mlb", juice_wall=JUICE_WALL)
            if scored:
                picks.append(scored)

    # ── F5 (First 5 Innings) ──
    # Disabled by default (ENABLE_MLB_F5 in config.py). Requires real F5
    # odds from a per-event market source -- synthetic pricing is not
    # supported for F5 (implied probabilities vary too much by SP).
    if get_flag("ENABLE_MLB_F5", False):
        f5 = pred.get("f5") or {}
        _append_f5_picks(picks, f5, odds, h_abbr, a_abbr)

    # ── Phase 1 derivatives ──
    # Team totals, inning totals/BTS, 1st-inning winner, F5 3-way winner,
    # total odd/even, extra innings. All pure probability extraction
    # from existing pred data — no factor stacking. Master gate +
    # per-market gates live in engine/config.py.
    from .mlb_derivative_picks import append_derivative_picks
    append_derivative_picks(picks, pred, odds, h_abbr, a_abbr)

    # Legacy per-pick calibration loop DELETED 2026-05-02. Every MLB
    # pick (ML, RL, O/U, 1st INN, F5 ML/OU/RL, ALT RL via shop_alt_*)
    # now flows through picks_core.score_pick which calibrates inline.

    # NOTE: pred["win_prob"] used to get re-stomped with the
    # calibrated ML prob to match the "Projected Outcome" panel to the
    # pick card. Two problems with that:
    #   (1) calibrators are trained on winning-side prob and inverted
    #       the bar when home was the dog;
    #   (2) blend_calibration on overconfident sports (NHL especially)
    #       crushed favorites below 50%, flipping the perceived
    #       favorite even when it shouldn't.
    # Per-pick prob in the loop above is already calibrated for
    # ranking. Leave pred["win_prob"] as the raw factor/MC/GBM blend
    # output so the display bar reflects the model's natural belief.

    # Annotate each pick with a probability band so the UI can render a
    # confidence histogram around the calibrated point estimate. Clamp
    # to [0, 1] (a -0.04 lower bound on a 0.51 prediction is just 0).
    for p in picks:
        prob = p.get("prob")
        if prob is None:
            continue
        p["prob_low"]  = round(max(0.0, prob - ci_hw), 4)
        p["prob_high"] = round(min(1.0, prob + ci_hw), 4)
        p["ci_half_width"] = ci_hw

    # Defense-in-depth edge recheck. picks_core.score_pick already
    # rejects edge<=0 after calibration (line ~256 in picks_core.py),
    # so this is theoretically redundant. Kept because the legacy
    # paths above this point may add picks via direct append outside
    # picks_core; the sweep ensures nothing leaks through.
    picks = [p for p in picks if (p.get("edge") or 0) > 0]

    # ── Edge Enhancements ──
    # Four signals that sharpen pick quality beyond the base model.
    try:
        from .edge_enhancements import (
            shop_alt_spreads, shop_alt_totals,
            get_clv_reliability, annotate_line_movement,
        )

        # 1. Alt Line Shopping: find better edges on alt spreads/totals
        alt_rl = shop_alt_spreads(pred, odds, h_abbr, a_abbr, picks)
        alt_ou = shop_alt_totals(pred, odds, picks)
        picks.extend(alt_rl)
        picks.extend(alt_ou)

        # 4. CLV Confidence: adjust reliability based on historical CLV
        # in the (bet_type x direction) cell. Direction-blind aggregates
        # were averaging Over+Under, masking asymmetric markets.
        clv_cache: dict[tuple[str, str], float] = {}
        for p in picks:
            bt = p.get("type", "")
            pk = p.get("pick", "")
            cache_key = (bt, pk[:5].lower())  # cheap direction proxy
            if cache_key not in clv_cache:
                clv_cache[cache_key] = (
                    get_clv_reliability("mlb", bt, pick_text=pk) or 1.0
                )
            p["clv_reliability"] = clv_cache[cache_key]

        # 5. Line Movement: annotate picks with sharp money signals
        date_str = et_today_str()
        matchup_key = f"{date_str}_{a_abbr}@{h_abbr}"
        annotate_line_movement(picks, "mlb", matchup_key, odds)

    except Exception as e:
        logger.warning("Edge enhancements error: %s", e)

    # Adjusted EV: edge * reliability weight * CLV modifier * line move modifier.
    # Reliability is auto-tuned from settled tracker history when there's
    # enough volume (>=30 settled picks of this bet type); below that, falls
    # back to the hand-tuned MLB_BET_RELIABILITY dict. Lets profitable
    # markets (e.g. F5 Team Total at +25 ROI) outrank stale-default ones
    # without hand-editing config.py.
    from .dynamic_reliability import get_reliability as _get_reliability
    for p in picks:
        reliability = _get_reliability("mlb", p["type"])
        clv_mult = p.get("clv_reliability", 1.0)
        lm_mult = p.get("line_move_modifier", 1.0)
        p["adjusted_ev"] = round(p["edge"] * reliability * clv_mult * lm_mult, 2)
    picks.sort(key=lambda p: -p["adjusted_ev"])

    # Confidence tags + 1st INN exemption live in engine._pick_helpers
    # so a future tweak lands in one place across all three sports.
    from ._pick_helpers import tag_confidence, filter_by_data_driven_floors
    # Apply data-driven per-cell floor (engine.edge_floors) BEFORE
    # confidence tagging so a NOPLAY-banned cell never surfaces with
    # a "lean"/"moderate" badge.
    picks = filter_by_data_driven_floors("mlb", picks)
    tag_confidence(picks)

    # Cache picks on the prediction dict so any surface that shares
    # the same cached prediction gets identical picks. This is the
    # single source of truth — whoever generates first (best-bets or
    # predict endpoint) writes the cache; the other reads it.
    if pred is not None and isinstance(pred, dict):
        pred["_cached_picks"] = picks
        pred["_cached_odds"] = odds

    return picks


def get_best_pick(picks: list[dict]) -> dict | None:
    """Return the single best pick (highest edge) from a picks list.

    Prefers picks with a real stake recommendation (`stake_units > 0`)
    so shadow picks (NOPLAY cells / below-floor edges that emit at 0u
    for calibration learning) don't headline the user-facing card.
    User flagged MLB CHC@STL 2026-05-29 showing 'MONEYLINE STL [0u]
    +5.5%' as the card's main pick — shadow picks should be recorded
    but not surfaced as the bet recommendation.

    Falls through to a 0u pick only when every non-skip candidate is
    shadow — keeps the card showing *something* over going blank.
    """
    playable = [p for p in picks if p.get("confidence") != "skip"]
    if not playable:
        return None
    staked = [p for p in playable if (p.get("stake_units") or 0) > 0]
    return staked[0] if staked else playable[0]


def _append_f5_picks(picks: list, f5: dict, odds: dict,
                      h_abbr: str, a_abbr: str) -> None:
    """Generate F5 ML / O/U / RL picks from model + real F5 odds.

    Skips any sub-market when real F5 odds are missing -- F5 pricing
    varies too much with starting pitchers to use a synthetic baseline.
    """
    if not f5:
        return

    wp = f5.get("win_prob") or {}
    f5_home_wp = wp.get("home", 0.5)
    f5_away_wp = wp.get("away", 0.5)

    # ── F5 Moneyline ──  Migrated to picks_core.score_pick.
    if get_flag("MLB_ALLOW_F5_ML", True):
        from .picks_core import score_pick as _score_pick
        f5_home_ml = odds.get("f5_home_ml")
        f5_away_ml = odds.get("f5_away_ml")
        if f5_home_ml is not None:
            scored = _score_pick({
                "type": "F5 ML", "pick": h_abbr,
                "raw_prob": f5_home_wp, "odds": f5_home_ml,
            }, sport="mlb", juice_wall=JUICE_WALL)
            if scored:
                picks.append(scored)
        if f5_away_ml is not None:
            scored = _score_pick({
                "type": "F5 ML", "pick": a_abbr,
                "raw_prob": f5_away_wp, "odds": f5_away_ml,
            }, sport="mlb", juice_wall=JUICE_WALL)
            if scored:
                picks.append(scored)

    # ── F5 Over/Under ──  Migrated to picks_core.score_pick.
    f5_total_line = odds.get("f5_total")
    if f5_total_line and f5.get("over_under"):
        ou_data = _find_ou(f5["over_under"], f5_total_line)
        if ou_data:
            f5_over = ou_data.get("over", 0.5)
            f5_under = ou_data.get("under", 0.5)
            pick_over = f5_over > f5_under
            ou_allowed = (pick_over and get_flag("MLB_ALLOW_F5_OU_OVER", True)) or \
                         ((not pick_over) and get_flag("MLB_ALLOW_F5_OU_UNDER", True))
            if ou_allowed:
                ou_prob = f5_over if pick_over else f5_under
                ou_odds = (odds.get("f5_over_odds") if pick_over
                           else odds.get("f5_under_odds")) or -110
                from .picks_core import score_pick as _score_pick
                scored = _score_pick({
                    "type": "F5 O/U",
                    "pick": f"F5 {'Over' if pick_over else 'Under'} {f5_total_line}",
                    "raw_prob": ou_prob,
                    "odds": ou_odds,
                }, sport="mlb", juice_wall=JUICE_WALL)
                if scored:
                    picks.append(scored)

    # ── F5 Run Line (typically ±0.5) ──
    rl = f5.get("run_line") or {}
    home_f5_rl_odds = odds.get("f5_home_spread_odds")
    home_f5_rl_point = odds.get("f5_home_spread_point")
    away_f5_rl_odds = odds.get("f5_away_spread_odds")
    away_f5_rl_point = odds.get("f5_away_spread_point")

    def _f5_rl_prob(point: float, side: str) -> float | None:
        """Return model prob for covering `point` on `side` (home/away)."""
        if point is None:
            return None
        # Books typically offer ±0.5 F5 RL; support ±1.5 too by falling
        # back to the closest magnitude we modeled.
        if abs(point) == 0.5:
            if side == "home":
                return rl.get("home_minus_0_5") if point < 0 else rl.get("home_plus_0_5")
            else:
                return rl.get("away_minus_0_5") if point < 0 else rl.get("away_plus_0_5")
        return None

    # F5 RL — migrated to picks_core.score_pick.
    from .picks_core import score_pick as _score_pick
    for abbr, rl_pt, rl_odds, side in [
        (h_abbr, home_f5_rl_point, home_f5_rl_odds, "home"),
        (a_abbr, away_f5_rl_point, away_f5_rl_odds, "away"),
    ]:
        if rl_odds is None or rl_pt is None:
            continue
        is_dog = rl_pt > 0
        allowed = (is_dog and get_flag("MLB_ALLOW_F5_RL_UNDERDOG", True)) or \
                  ((not is_dog) and get_flag("MLB_ALLOW_F5_RL_FAVORITE", True))
        if not allowed:
            continue
        prob = _f5_rl_prob(rl_pt, side)
        if prob is None:
            continue
        sign = "+" if rl_pt > 0 else ""
        scored = _score_pick({
            "type": "F5 RL",
            "pick": f"{abbr} {sign}{rl_pt}",
            "raw_prob": prob,
            "odds": rl_odds,
        }, sport="mlb", juice_wall=JUICE_WALL)
        if scored:
            picks.append(scored)


# NRFI/YRFI rolling-median fallback. The original code hardcoded -120
# regardless of how the market actually priced NRFI; that's roughly
# accurate for an average matchup but biased on heavy-NRFI games
# (books often hit -140 to -160). Sample the real prices we've already
# stored in the odds table and use the median when we have enough
# data; fall back to -120 only when the table is too sparse.
_NRFI_FALLBACK_CACHE: dict[str, tuple[float, int]] = {}
_NRFI_FALLBACK_TTL = 300  # 5 min -- balances staleness vs DB churn
_NRFI_FALLBACK_DEFAULT = -120
_NRFI_FALLBACK_MIN_SAMPLES = 5
_NRFI_FALLBACK_LOOKBACK_DAYS = 60


def _nrfi_fallback_odds(side: str) -> int:
    """Return the rolling-median historical NRFI price for the given side.

    side == 'NRFI' -> looks at nrfi_under_odds; 'YRFI' -> nrfi_over_odds.
    Falls back to -120 when fewer than _NRFI_FALLBACK_MIN_SAMPLES samples
    exist in the lookback window (typically the first few weeks of a
    season after we start storing the per-event data).
    """
    import time as _time
    cached = _NRFI_FALLBACK_CACHE.get(side)
    if cached and (_time.time() - cached[0]) < _NRFI_FALLBACK_TTL:
        return cached[1]

    col = "nrfi_under_odds" if side == "NRFI" else "nrfi_over_odds"
    try:
        from datetime import timedelta
        conn = get_conn()
        cutoff = (datetime.now() - timedelta(days=_NRFI_FALLBACK_LOOKBACK_DAYS))\
            .strftime("%Y-%m-%d")
        rows = conn.execute(
            f"SELECT o.{col} AS px "
            "  FROM odds o JOIN games g ON g.mlb_game_id = o.game_id "
            f" WHERE g.date >= ? AND o.{col} IS NOT NULL",
            (cutoff,),
        ).fetchall()
        prices = [int(r["px"]) for r in rows if r["px"] is not None]
    except Exception as e:
        logger.debug("NRFI fallback query failed: %s", e)
        prices = []

    if len(prices) < _NRFI_FALLBACK_MIN_SAMPLES:
        result = _NRFI_FALLBACK_DEFAULT
    else:
        sorted_p = sorted(prices)
        n = len(sorted_p)
        result = sorted_p[n // 2] if n % 2 else int(
            (sorted_p[n // 2 - 1] + sorted_p[n // 2]) / 2
        )

    _NRFI_FALLBACK_CACHE[side] = (_time.time(), result)
    return result


def _find_ou(ou_lines: dict, vegas_total: float) -> dict | None:
    """Find O/U entry closest to the Vegas total."""
    vt = float(vegas_total)
    for fmt in [str(vt), f"{vt:.1f}", str(int(vt))]:
        if fmt in ou_lines:
            return ou_lines[fmt]
    best_key = min(ou_lines.keys(), key=lambda k: abs(float(k) - vt), default=None)
    return ou_lines.get(best_key) if best_key else None


def fetch_real_odds_for_games() -> dict:
    """Fetch real MLB odds from Hard Rock Bet (FL operator, full slate +
    alts). Cached inside the scraper so repeated calls are cheap."""
    merged: dict = {}
    try:
        from scrapers.hardrock_odds import fetch_mlb as _hr_mlb
        hr = _hr_mlb()
        if hr:
            merged.update(hr)
    except Exception:
        pass
    return merged


def _swap_odds(odds: dict) -> dict:
    """Swap home/away fields in an odds dict.

    When the sportsbook and schedule disagree on who's home, the odds
    dict has home_ml = sportsbook's home (our away) and vice versa.
    This swaps all home_*/away_* pairs so the odds align with our
    schedule's home/away designation.

    IMPORTANT: the returned dict is a FULLY INDEPENDENT copy. Previous
    versions used dict(odds) which left nested alt_spreads lists (and
    their inner alt dicts) as shared references to the scraper's
    in-memory cache. Each match_odds() call would then mutate the
    cache in place — swapping points/odds — so the NEXT match_odds()
    call saw already-swapped data and flipped it back (double-swap).
    Observed as NBA Q1 picks labeled "HOU +1.5 Q1 edge 35.9%" against
    HR's actual HOU -1.5 line. Deepcopy kills the aliasing.
    """
    import copy
    swapped = copy.deepcopy(odds)
    swap_pairs = [
        ("home_ml", "away_ml"),
        ("home_spread_point", "away_spread_point"),
        ("home_spread_odds", "away_spread_odds"),
        ("q1_home_ml", "q1_away_ml"),
        ("q1_spread_home_odds", "q1_spread_away_odds"),
    ]
    for a, b in swap_pairs:
        va, vb = swapped.get(a), swapped.get(b)
        if va is not None or vb is not None:
            swapped[a] = vb
            swapped[b] = va

    # Q1 spread: negate because it's expressed as "home team spread"
    # and the home team identity changed
    if swapped.get("q1_spread") is not None:
        swapped["q1_spread"] = -swapped["q1_spread"]

    # Alt spreads: swap odds and negate points (point is relative to
    # the home team). Apply to both full-game and Q1 alts. Safe to
    # mutate the dicts in place now because deepcopy above gave us
    # fresh copies disconnected from the scraper cache.
    for key in ("alt_spreads", "q1_alt_spreads"):
        for alt in swapped.get(key, []):
            alt["home_odds"], alt["away_odds"] = alt.get("away_odds"), alt.get("home_odds")
            if alt.get("point") is not None:
                alt["point"] = -alt["point"]

    # Q1 alt totals: swap over/under odds (totals are symmetric but
    # odds may differ if the swap changes the perspective)
    # Actually totals don't need swapping — over is over regardless
    # of who's home. But include for completeness.

    # Flip the identity tags
    swapped["odds_home"], swapped["odds_away"] = (
        swapped.get("odds_away"), swapped.get("odds_home"))

    return swapped


def _detect_sport(all_odds: dict) -> str:
    """Guess the sport from odds keys or content.

    Fielded signals first (unambiguous), numeric O/U as a fallback. The
    prior over_under<12 heuristic misfired for MLB (~8.5 totals) and
    routed everything to NHL, which quietly killed MLB alias expansion
    inside ``_find_odds_by_team_pair`` — the root cause of the 0% MLB
    CLV coverage the tracker showed in June+July 2026.
    """
    for v in all_odds.values():
        if not isinstance(v, dict):
            continue
        # Unambiguous fielded signals.
        if v.get("q1_home_ml") is not None or v.get("q1_spread") is not None:
            return "nba"
        if v.get("home_pitcher") or v.get("away_pitcher") \
                or v.get("nrfi_over_odds") is not None \
                or v.get("inning_totals") is not None \
                or v.get("f5_home_ml") is not None:
            return "mlb"
        if v.get("puck_line") is not None \
                or v.get("pl_home_odds") is not None:
            return "nhl"
        # Numeric fallback — MLB (8-11), NHL (5-7), NBA (200+).
        ou = v.get("over_under")
        if isinstance(ou, (int, float)):
            if ou > 100:
                return "nba"
            if ou < 7.5:
                return "nhl"
            # 7.5+ but < 100 → MLB (typical range).
            return "mlb"
        break
    return "mlb"


def _hhmm_utc_from_iso(start_time: str) -> str:
    """UTC HHMM from an ISO-8601 timestamp; '' on parse failure."""
    if not isinstance(start_time, str) or not start_time:
        return ""
    try:
        from datetime import datetime as _dt, timezone as _tz
        s = start_time.replace("Z", "+00:00") if start_time.endswith("Z") else start_time
        ts = _dt.fromisoformat(s)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=_tz.utc)
        return ts.astimezone(_tz.utc).strftime("%H%M")
    except (ValueError, TypeError):
        return ""


def _find_odds_by_team_pair(team_a: str, team_b: str,
                            all_odds: dict,
                            start_time: str | None = None) -> dict | None:
    """Find an odds dict containing these two teams, regardless of key order.

    Uses the ``odds_home`` / ``odds_away`` fields embedded in the odds
    dict by the scraper, plus abbreviation aliases. Returns the raw
    odds dict (not yet aligned to any schedule).

    When ``start_time`` (ISO-8601) is provided, prefers the entry keyed
    ``AWAY@HOME@HHMM`` over the plain ``AWAY@HOME`` so doubleheader
    games route to the right odds bucket. Without this, both DH games
    inherit Game 1's odds blob and Game 2 reads as "crazy ML/spread".
    """
    from .abbr import alt_abbr, aliases_for
    sport = _detect_sport(all_odds)
    a_alts = set(aliases_for(team_a, sport))
    b_alts = set(aliases_for(team_b, sport))
    hhmm = _hhmm_utc_from_iso(start_time or "")

    # Collect every entry matching this abbr-pair across all key shapes
    # (plain ``A@B``, suffixed ``A@B@HHMM``, and identity-tag scan). With
    # the full candidate set we can pick the entry whose start_time is
    # closest to the target — a fast-path early-return on the plain key
    # would silently grab Game 1's odds when the target is Game 2.
    candidates: list[dict] = []
    seen_ids: set[int] = set()
    def _add(odds_dict):
        if not isinstance(odds_dict, dict):
            return
        oid = id(odds_dict)
        if oid in seen_ids:
            return
        seen_ids.add(oid)
        candidates.append(odds_dict)

    for a in a_alts:
        for b in b_alts:
            for key in (f"{a}@{b}", f"{b}@{a}"):
                if key in all_odds:
                    _add(all_odds[key])
            # All HHMM-suffixed variants (key has 3+ ``@`` segments).
            for k, v in all_odds.items():
                parts = k.split("@")
                if len(parts) == 3 and parts[0] in (a, b) and parts[1] in (a, b):
                    _add(v)
    # Identity-tag fallback (catches scrapers that don't use abbr keys).
    for odds in all_odds.values():
        oh = odds.get("odds_home", "")
        oa = odds.get("odds_away", "")
        if (oh in a_alts and oa in b_alts) or (oh in b_alts and oa in a_alts):
            _add(odds)

    if not candidates:
        return None

    # Filter out candidates whose start_time is in the past. HR's API
    # ships stale "live odds" snapshots from already-played games on
    # the same matchup — observed 2026-04-28 with SD@CHC: yesterday's
    # blowout shipped with home_ml=2250 (CHC's late-game implied prob),
    # which created a fictional +45% edge against the actual upcoming
    # game's -120 line. Anything that already started belongs to the
    # past — drop it before picking.
    from datetime import datetime as _dt, timezone as _tz
    now_utc = _dt.now(_tz.utc)
    def _started(m: dict) -> bool:
        s = str(m.get("start_time") or "")
        if not s:
            return False
        try:
            iso = s.replace("Z", "+00:00") if s.endswith("Z") else s
            ts = _dt.fromisoformat(iso)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=_tz.utc)
            return ts < now_utc
        except (ValueError, TypeError):
            return False
    future_candidates = [c for c in candidates if not _started(c)]
    if future_candidates:
        candidates = future_candidates
    # If every candidate is in the past, fall through to the original
    # behavior so the caller still gets *something* — better than an
    # empty bet card. The is_locked / refresh_pending flags downstream
    # handle stale-odds suppression at the tracker level.

    if len(candidates) == 1 or not hhmm:
        return candidates[0]

    # Closest start_time wins (minute-of-day so 10-30 min drift between
    # ESPN and HR's reschedule timestamps still routes correctly).
    target_min = int(hhmm[:2]) * 60 + int(hhmm[2:])
    def _drift(m: dict) -> int:
        m_hhmm = _hhmm_utc_from_iso(str(m.get("start_time") or ""))
        if not m_hhmm:
            return 24 * 60
        m_min = int(m_hhmm[:2]) * 60 + int(m_hhmm[2:])
        return abs(target_min - m_min)
    candidates.sort(key=_drift)
    return candidates[0]


def match_odds(home_abbr: str, away_abbr: str, all_odds: dict,
               start_time: str | None = None) -> dict:
    """Find odds for a matchup and align home/away to our schedule.

    Matches by team pair (ignoring key order), then checks whether
    the sportsbook's home team matches our home team. If not, swaps
    all home_*/away_* fields so the odds align with our designation.

    The schedule source (ESPN / DB) is always the authority on
    which team is home.

    ``start_time`` (ISO-8601 from the scoreboard) disambiguates
    doubleheader games — without it, both games of a DH share the same
    abbr-pair key and the second game inherits the first game's odds.
    """
    from .abbr import aliases_for
    raw = _find_odds_by_team_pair(home_abbr, away_abbr, all_odds, start_time)
    if not raw:
        return {}

    # Check if the sportsbook's home matches our home.
    odds_home = raw.get("odds_home", "")
    odds_away = raw.get("odds_away", "")
    sport = _detect_sport(all_odds)
    home_alts = set(aliases_for(home_abbr, sport))
    away_alts = set(aliases_for(away_abbr, sport))

    if odds_home in home_alts:
        return raw  # already aligned to our schedule

    # User report 2026-04-28: SA was shown as +10.5 dog when HR had
    # SA -10.5 favorite. Root cause: when the HR scraper failed to
    # tag odds_home / odds_away (left them None), the old code took
    # the empty `odds_home` as "different home" and called _swap_odds
    # unconditionally — flipping correctly-keyed data into the wrong
    # perspective. Only swap when we can VERIFY a mismatch (the
    # sportsbook's away == our home, ie. odds_away in home_alts, OR
    # vice versa). When both identity tags are unknown, trust the raw
    # values rather than flip them blind.
    if odds_away in home_alts and odds_home in away_alts:
        return _swap_odds(raw)
    if odds_away in home_alts:
        # One-sided mismatch: away tag matches our home → still a swap.
        return _swap_odds(raw)
    if odds_home in away_alts:
        # Sportsbook's home is our away → swap.
        return _swap_odds(raw)

    # Neither identity tag matches; could be a None / empty identity
    # or a brand-new abbreviation. Trust raw as-is — flipping blindly
    # was the original bug.
    return raw
