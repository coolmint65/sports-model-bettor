"""Football matchup predictor.

Given Elo ratings + per-league average total points, derive the full
slate of model probabilities the picker needs:

    p_home / p_away              — moneyline
    expected_margin              — for spread bets (home POV)
    expected_total               — for over/under
    p_home_cover / p_away_cover   — spread cover under N(margin, σ²)
    p_over / p_under              — total cover under N(total, σ²)

Standard deviations are sport-wide constants tuned from realized
score differences. UFL plays ~10 games per team per season — enough
to detect signal in walk-forward but not enough to fit per-team σ.
"""
from __future__ import annotations

import logging
import math
import os
from datetime import datetime

from . import get_league_config
from ._db import get_conn
from ._elo import INIT_ELO, expected_score, init_rating, replay, serving_params

logger = logging.getLogger(__name__)


# Realized standard deviations from UFL 2024-25 sample (n≈110).
# Margin σ ~ 13.5 (NFL is ~13.86 per 538), total σ ~ 14 (UFL is
# higher-variance per game than NFL because some teams play "rest"
# games with depleted rosters).
_DEFAULT_MARGIN_SIGMA = 13.5

# Key-number-aware empirical margin distribution (2026-09-15). The plain Normal
# underweights the 3/7/10/14 spikes by ~6-7pp on the most common spreads. We load
# the league's historical signed margins and price cover off the real frequencies,
# shifted to the model's expected margin. Falls back to the Normal when no dist exists.
_MARGIN_DIST_CACHE = {}
def _load_margins(league):
    if league not in _MARGIN_DIST_CACHE:
        import os, json
        p = os.path.join(os.path.dirname(__file__), "..", "..", "data", "football", str(league) + "_margin_dist.json")
        try:
            d = json.load(open(p)); _MARGIN_DIST_CACHE[league] = (d["margins"], float(d["mean"]))
        except Exception:
            _MARGIN_DIST_CACHE[league] = None
    return _MARGIN_DIST_CACHE[league]

def _empirical_cover(league, expected_margin, spread):
    md = _load_margins(league)
    if not md:
        return None
    margins, mean = md
    shift = expected_margin - mean
    thr = -spread  # home covers iff margin > -spread
    n = len(margins)
    return sum(1 for m in margins if (m + shift) > thr) / n if n else None
_DEFAULT_TOTAL_SIGMA  = 14.0
# Expected total points across both teams. UFL averaged ~42 in 2024;
# refit per-league via _calibrate.
_DEFAULT_AVG_TOTAL    = 42.0
# Slope: how many points one Elo point is "worth" in expected margin.
# Football convention is ~25 Elo = 1 point. Tunable per-league.
_DEFAULT_ELO_PER_POINT = 25.0


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def predict_match(league: str, home_team_id: int, away_team_id: int,
                   *, ratings: dict[int, float] | None = None,
                   spread: float | None = None,
                   total_line: float | None = None,
                   return_samples: bool = False) -> dict:
    """Predict every market for one matchup.

    ``spread`` and ``total_line`` are optional — when provided, the
    response includes the implied cover/over probabilities so the
    picker doesn't have to recompute. Otherwise the picker probes
    its own (line, odds) candidates against the predicted
    distribution.
    """
    cfg = get_league_config(league)
    if ratings is None:
        ratings = replay(league)

    r_home = ratings.get(int(home_team_id))
    r_away = ratings.get(int(away_team_id))
    if r_home is None:
        r_home = init_rating(league, home_team_id)
    if r_away is None:
        r_away = init_rating(league, away_team_id)
    # HFA + Elo->pts slope that match the serving ratings (CFB is points-based since 2026-10-25)
    hfa, _epp = serving_params(cfg)

    # Injury adjustment (Austin 2026-09-19): the team-Elo model is blind to a
    # starting QB being out — the biggest "great on paper, QB hurt" blind spot.
    # Dock the effective rating ~100 Elo (~3-4 pts) when a team's QB is
    # Out/IR/Doubtful. Prior magnitude — NOT backtested (no historical injury
    # data); tune once we log injuries against outcomes. Benign no-op with no data.
    # Bumped 100->175 (Austin 2026-10-11): 100 Elo @30/pt = only ~3.3 pts, far
    # too small for a starting QB (consensus ~5-7 pts; a star like Lamar more).
    # Flat for now (calibrate_qb_penalty n=24 still too thin/noisy to fit).
    # The injury-BLIND GBM leg is dropped separately in _ensemble when a QB is out.
    _QB_OUT_ELO = 175.0
    _ih = _ia = {}
    try:
        from ._injuries import team_injury_signal as _inj
        _ih = _inj(league, home_team_id) or {}
        _ia = _inj(league, away_team_id) or {}
        if league == "nfl":
            # Snap-share weighted injuries (Austin 2026-10-11 v2): QB penalty
            # scales with the OUT QB's snap-share (backup out ~= 0, starter ~175)
            # + non-QB by snap-share (x30, cap 120). Stash for ensemble + display.
            # 2026-10-25 (Austin "fade it ... confirm if the player is actually out before
            # the game"): each injured player counts the chance he misses THIS game -- faded
            # when the game is a week away, confirmed by the official report / game-day
            # inactives (engine.football._injuries._miss_prob). `players` = the audit trail.
            from ._injuries import team_injury_penalty as _tip, team_injury_detail as _tid, game_for as _game_for
            _g = _game_for(league, home_team_id, away_team_id)
            _qp_h, _nqh, _sqbh = _tip(league, home_team_id, game=_g)
            _qp_a, _nqa, _sqba = _tip(league, away_team_id, game=_g)
            r_home -= (_qp_h + _nqh)
            r_away -= (_qp_a + _nqa)
            _pl = lambda _t: [{"player": d[0], "pos": d[1], "p_miss": d[2], "elo": d[4], "basis": d[5]}
                              for d in _tid(league, _t, game=_g)[:6]]
            _ih = dict(_ih); _ih["qb_penalty"] = _qp_h; _ih["non_qb_penalty"] = _nqh; _ih["starter_qb_out"] = _sqbh; _ih["qb_out"] = _sqbh; _ih["players"] = _pl(home_team_id)
            _ia = dict(_ia); _ia["qb_penalty"] = _qp_a; _ia["non_qb_penalty"] = _nqa; _ia["starter_qb_out"] = _sqba; _ia["qb_out"] = _sqba; _ia["players"] = _pl(away_team_id)
        else:
            if _ih.get("qb_out"):
                r_home -= _QB_OUT_ELO
            if _ia.get("qb_out"):
                r_away -= _QB_OUT_ELO
    except Exception:
        pass

    p_home = expected_score(r_home, r_away, hfa)
    p_away = 1.0 - p_home

    avg_total = cfg.get("league_avg_total") or _DEFAULT_AVG_TOTAL
    elo_diff = r_home - r_away + hfa
    # 2026-09-03: per-league slope fit walk-forward (cfb 24.5 Elo/pt @K40, nfl ~30 @K20)
    expected_margin = elo_diff / (_epp or _DEFAULT_ELO_PER_POINT)
    # CFB weeks 1-4: recruiting-talent prior (engine.football.cfbd_talent; walk-forward RMSE 16.0 -> 15.6)
    if league == "cfb":
        try:
            from .cfbd_talent import margin_adjustment as _talent_adj
            expected_margin += _talent_adj(home_team_id, away_team_id)
        except Exception:
            pass
    # NFL: play-by-play EPA ratings replace the Elo margin + win prob (engine.football.nfl_epa, Austin 2026-10-31
    # "lets work on our log loss"). Walk-forward vs the close, 2023-26: .645/.607/.645/.660 vs old Elo
    # .668/.610/.662/.687 (close .627/.589/.607/.660). The injury layer still applies, in points: non-QB always;
    # the QB penalty only when nflverse still lists last game's QB (a changed starter is already in the EPA QB term).
    # Falls back to Elo while the fit is building or a team is unmapped. NFL_EPA=0 turns it off.
    _epa = None
    if league == "nfl" and os.environ.get("NFL_EPA", "1") != "0":
        try:
            from . import nfl_epa as _nfl_epa
            _epa = _nfl_epa.predict_ids(home_team_id, away_team_id)
        except Exception:
            _epa = None
    if _epa is not None:
        _ppe = 1.0 / (_epp or _DEFAULT_ELO_PER_POINT)
        _st = _nfl_epa.state() or {}
        _lq = _st.get("last_qb") or {}
        _qb_same_h = _epa.get("qb_home") == _lq.get(_nfl_epa.nv(_nfl_epa.abbr_for(home_team_id)))
        _qb_same_a = _epa.get("qb_away") == _lq.get(_nfl_epa.nv(_nfl_epa.abbr_for(away_team_id)))
        _adj = -(float(_ih.get("non_qb_penalty") or 0) + (float(_ih.get("qb_penalty") or 0) if _qb_same_h else 0.0)) \
               + (float(_ia.get("non_qb_penalty") or 0) + (float(_ia.get("qb_penalty") or 0) if _qb_same_a else 0.0))
        elo_margin = expected_margin
        expected_margin = _epa["margin"] + _adj * _ppe
        p_home = 1.0 / (1.0 + math.exp(-_st.get("slope", 0.1275) * expected_margin))
        p_away = 1.0 - p_home
    # TOTAL from the opponent-adjusted scoring model (Elo only rates margin and
    # pins the total to league-avg -- see _scoring.py). Margin stays Elo; fall back
    # to the league-avg total if the scoring model is unavailable. (2026-10-20)
    total_for_split = avg_total
    try:
        from ._scoring import expected_scoring
        _sp = expected_scoring(league, home_team_id, away_team_id)
        if _sp is not None:
            total_for_split = _sp[0] + _sp[1]
    except Exception:
        pass
    # Split the total by win-prob skew so the favorite carries more
    # expected points than the dog.
    expected_home = (total_for_split + expected_margin) / 2.0
    expected_away = (total_for_split - expected_margin) / 2.0
    expected_total = expected_home + expected_away

    out = {
        "league":          league,
        "home_team_id":    int(home_team_id),
        "away_team_id":    int(away_team_id),
        "elo_home":        round(r_home, 1),
        "elo_away":        round(r_away, 1),
        "p_home":          round(p_home, 4),
        "p_away":          round(p_away, 4),
        "expected_margin": round(expected_margin, 2),
        "expected_total":  round(expected_total, 2),
        "expected_home":   round(expected_home, 2),
        "expected_away":   round(expected_away, 2),
        "injury_home":     _ih,
        "injury_away":     _ia,
    }
    if _epa is not None:
        out["margin_source"] = "nfl_epa"
        out["elo_margin"] = round(elo_margin, 2)

    if spread is not None:
        # Home covers a spread of ``spread`` (negative = home is favored
        # by abs(spread)) iff (home - away) > -spread, i.e. margin >
        # -spread. P(margin > -spread) under N(expected_margin, σ²).
        # 2026-09-03: use the league's FITTED sigma (CFB ≈ 22, not the UFL 13.5 default) —
        # the default made every double-digit dog look like a 40-50pp "edge".
        p_home_cover = _empirical_cover(league, expected_margin, spread)
        if p_home_cover is None:  # no empirical dist (e.g. UFL) -> Normal fallback
            z = (-spread - expected_margin) / (cfg.get("margin_sigma") or _DEFAULT_MARGIN_SIGMA)
            p_home_cover = 1.0 - _norm_cdf(z)
        out["spread"]           = float(spread)
        out["p_home_cover"]     = round(p_home_cover, 4)
        out["p_away_cover"]     = round(1.0 - p_home_cover, 4)

    if total_line is not None:
        z = (total_line - expected_total) / (cfg.get("total_sigma") or _DEFAULT_TOTAL_SIGMA)
        p_under = _norm_cdf(z)
        out["total_line"]   = float(total_line)
        out["p_over"]       = round(1.0 - p_under, 4)
        out["p_under"]      = round(p_under, 4)

    # ── MC layer ─────────────────────────────────────────────
    # Runs a 10k-sim Monte Carlo over the same expected scores so the
    # picker has a second signal leg for the V3.2 ensemble explain.
    # Cheap (~5ms per game) — runs every prediction. GBM layer is
    # gated separately on training-data volume; see ``_gbm.py``.
    from ._mc import simulate as _mc_simulate
    _md_mc = _load_margins(league)
    mc = _mc_simulate(
        expected_home=expected_home,
        expected_away=expected_away,
        margin_sigma=cfg.get("margin_sigma") or _DEFAULT_MARGIN_SIGMA,
        spread=spread,
        total_line=total_line,
        return_samples=return_samples,
        margin_samples=(_md_mc[0] if _md_mc else None),
        margin_mean=(_md_mc[1] if _md_mc else 0.0),
    )
    out.update(mc)

    # ── GBM layer ────────────────────────────────────────────
    # Gated on sample size — UFL has 110 games which is too thin for a
    # reliable GBM. The hook is here so future sports/leagues with
    # more history pick it up automatically, and V3.2's signal_explain
    # endpoint reports `gbm_prob: null` cleanly rather than silently
    # omitting the leg.
    from ._gbm import predict_gbm as _predict_gbm
    out.update(_predict_gbm(
        league=league,
        home_team_id=int(home_team_id),
        away_team_id=int(away_team_id),
        expected_margin=expected_margin,
        expected_total=expected_total,
        spread=spread,
        total_line=total_line,
    ))

    # ── Ensemble ─────────────────────────────────────────────
    # Blend the three legs (factor / MC / GBM) into a final
    # probability per market. When GBM is gated (gbm_prob is None),
    # the blender renormalizes the remaining weights so a missing
    # leg doesn't drag the prob toward 0 — same pattern the basketball
    # _ensemble module uses.
    from ._ensemble import blend as _ensemble_blend
    _ensemble_blend(league, out)
    if _epa is not None:
        # The EPA win prob is calibrated by construction (logistic slope fit on outcomes, walk-forward-tested);
        # the factor/MC/GBM ensemble weights were fit for the Elo legs and pulled it toward .5 (CLE@PIT .67 -> .59).
        out["p_home_ensemble"] = out.get("p_home")
        out["p_home"] = round(p_home, 4)
        out["p_away"] = round(1.0 - p_home, 4)

    # Signal logging is the route's responsibility, NOT the predictor's
    # — see ``log_signals`` below. Calling it from here would write 3×
    # per game (factor predict + spread re-predict + total re-predict),
    # which races the shared cached connection from concurrent slate
    # hits and produced intermittent 500s.

    return out


def log_signals(league: str, pred: dict) -> None:
    """Insert one prediction_signals row per market into the league's
    DB. Called by the slate-route entry point (NOT the predictor) so
    each game logs exactly once per slate hit.

    Uses a short-lived dedicated sqlite3 connection rather than the
    module's cached one — concurrent slate hits would otherwise race
    on the same connection across threads and intermittently fail
    with `database is locked`.
    """
    gid = pred.get("game_id")
    if not gid:
        return
    home_id = pred.get("home_team_id")
    away_id = pred.get("away_team_id")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    f = pred.get("_signal_factor") or {}
    m = pred.get("_signal_mc") or {}
    g = pred.get("_signal_gbm") or {}
    rows = []
    for market, key in (("home_win", "p_home"),
                         ("home_cover", "p_home_cover"),
                         ("total_over", "p_over")):
        if f.get(key) is None and m.get(key) is None and g.get(key) is None:
            continue
        rows.append((
            "football", league, str(gid), today, home_id, away_id, market,
            f.get(key), m.get(key), g.get(key),
        ))
    if not rows:
        return
    # Short-lived writer connection. WAL mode + a fresh connection
    # means the writer doesn't contend with the cached reader-and-
    # everyone-else connection that the rest of the framework uses.
    import sqlite3
    from . import get_league_config
    cfg = get_league_config(league)
    try:
        wconn = sqlite3.connect(cfg["db_path"], timeout=5.0,
                                  isolation_level=None)
        wconn.execute("PRAGMA journal_mode = WAL")
        try:
            wconn.executemany(
                "INSERT INTO prediction_signals "
                "(sport, league, game_id, game_date, home_team_id, "
                " away_team_id, market, factor_prob, mc_prob, gbm_prob) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
        finally:
            wconn.close()
    except Exception as e:
        # Never propagate — the slate must render even if the signal
        # log fails on a contention burst.
        logger.debug("[football:%s] signal log skipped: %s", league, e)
