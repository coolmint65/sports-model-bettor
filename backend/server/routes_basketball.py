"""Basketball routes — exposes LEAGUE_REGISTRY to the frontend so the
nested sidebar can render every league HR offers (NBA + WNBA + NCAAM +
27 others) without hardcoding the list.

NBA's existing routes (``/api/nba/best-bets`` etc.) stay untouched.
This module is for the framework-managed leagues + cross-league reads.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from fastapi import APIRouter

from ._tz import et_today_str, et_now, et_month, _US_EASTERN
from engine.selection import apply_selected as _apply_selected  # Pick Authority (Workstream #1)
from engine.selection import select_pick as _authority_select  # Pick Authority: proven-gated headline, key="basketball" (2026-10-18)

logger = logging.getLogger(__name__)

# Per-league monotonic timestamp of last upstream ingest. Acts as a
# request-scoped debounce: repeat slate hits inside _SLATE_INGEST_TTL_S
# skip the ingest call so we don't hammer RealGM/ESPN with one
# scrape per user click.
_SLATE_INGEST_TS: dict[str, float] = {}
_SLATE_INGEST_TTL_S = 60.0

router = APIRouter()

# Pick Authority shadow lane for framework leagues with "pick_authority" (NCAAM / NCAAW,
# MintyBot 2026-10-26): the second pricing pass runs at most every _PA_SHADOW_EVERY_S per
# game -- the recorder keeps each market's first sighting anyway (NBA's pattern).
_PA_SHADOW_EVERY_S = 1800
_PA_SHADOW_LAST: dict = {}
_PA_SHADOW_CANDS: dict = {}    # game_id -> last shadow-lane candidates: the card's model lean (2026-10-26)

def _basketball_q1_paper_picks(pred, odds, home_abbr, away_abbr, league):
    """PAPER Q1 best-of (Q1_ML / Q1_SPREAD / Q1_TOTAL). Mirrors the full-game paper pathway but
    derives the Q1 predicted margin/total from the full-game numbers via the league's fitted
    q1_share (= q1_avg_total/league_avg_total) + q1_home_boost — the SAME math as
    engine.basketball._picks' Q1 block — then normal-CDF vs the odds.q1 lines, bypassing the gates.
    Austin 2026-09-19. Returns [] for leagues with no odds.q1 or no fitted Q1 constants."""
    import math
    q1 = (odds or {}).get("q1") or {}
    if not pred or not q1:
        return []
    try:
        from engine.basketball._config import get_league_config
        cfg = get_league_config(league) or {}
    except Exception:
        return []
    q1_margin_std = cfg.get("q1_margin_std")
    q1_total_std = cfg.get("q1_total_std")
    q1_home_boost = cfg.get("q1_home_boost") or 0.0
    q1_avg_total = cfg.get("q1_avg_total")
    _per = pred.get("period")   # college ratings: fitted period model (MintyBot 2026-10-26)
    if not _per and not (q1_margin_std and q1_total_std and q1_avg_total is not None):
        return []
    def ncdf(x):
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))
    def implied(o):
        try:
            o = float(o)
        except (TypeError, ValueError):
            return None
        return (-o) / (-o + 100) if o < 0 else 100 / (o + 100)
    full_margin = pred.get("predicted_margin")
    full_total = pred.get("predicted_total")
    if _per:
        q1_margin, q1_total = _per["margin"], _per["total"]
        q1_margin_std, q1_total_std = _per["margin_std"], _per["total_std"]
    else:
        full_avg_total = cfg.get("league_avg_total") or 163.0
        q1_share = q1_avg_total / float(full_avg_total) if full_avg_total else 0.256
        q1_margin = ((full_margin or 0.0) * q1_share + q1_home_boost) if full_margin is not None else None
        q1_total = (full_total * q1_share) if full_total is not None else q1_avg_total
    out = []
    ml = q1.get("ml") or {}
    if q1_margin is not None and (ml.get("home") is not None or ml.get("away") is not None):
        p_home = ncdf(q1_margin / q1_margin_std)
        side, prob, o = ((home_abbr or "HOME"), p_home, ml.get("home")) if p_home >= 0.5 else ((away_abbr or "AWAY"), 1 - p_home, ml.get("away"))
        imp = implied(o)
        if o is not None and imp is not None:
            out.append(dict(type="Q1_ML", market="Q1 ML", pick=side, line=None, prob=round(prob, 4), odds=o, edge=round((prob - imp) * 100, 1), paper=True, model_value=round(q1_margin, 1)))
    sp = q1.get("spread") or {}
    hpt = sp.get("home_pt")
    if q1_margin is not None and hpt is not None:
        p_home = ncdf((q1_margin - (-float(hpt))) / q1_margin_std)
        if p_home >= 0.5:
            side, prob, ln, o = (home_abbr or "HOME"), p_home, hpt, sp.get("home_odds")
        else:
            side, prob, ln, o = (away_abbr or "AWAY"), 1 - p_home, sp.get("away_pt"), sp.get("away_odds")
        imp = implied(o)
        if o is not None and imp is not None and ln is not None:
            out.append(dict(type="Q1_SPREAD", market="Q1 SPR", pick=side, line=ln, prob=round(prob, 4), odds=o, edge=round((prob - imp) * 100, 1), paper=True, model_value=round(q1_margin, 1)))
    tot = q1.get("total") or {}
    line = tot.get("line")
    if q1_total is not None and line is not None:
        p_over = ncdf((q1_total - float(line)) / q1_total_std)
        side, prob, o = ("Over", p_over, tot.get("over_odds")) if p_over >= 0.5 else ("Under", 1 - p_over, tot.get("under_odds"))
        imp = implied(o)
        if o is not None and imp is not None:
            out.append(dict(type="Q1_TOTAL", market="Q1 O/U", pick=side, line=line, prob=round(prob, 4), odds=o, edge=round((prob - imp) * 100, 1), paper=True, model_value=round(q1_total, 1)))
    return out


def _basketball_card_wp(league, pred, odds):
    """Card % = the Pick Authority's market blend of the ENSEMBLE home win (the number the card showed and
    the ML pick uses) with the de-vigged ML board (Austin 2026-10-26 "Any percentages for any card in any
    sport, should be that way")."""
    try:
        from ._display_cal import market_card_wp
        p = ((pred.get("ensemble") or {}).get("ensemble") or {}).get("ml_home")
        if p is None:
            p = pred.get("ml_home")
        ml = (odds or {}).get("ml") or {}
        return market_card_wp("basketball", league, p, ml.get("home"), ml.get("away"))
    except Exception:
        return None


def _basketball_paper_picks(pred, odds, home_abbr, away_abbr):
    """PAPER total + spread picks from the RAW model (predicted total/margin + sigma), bypassing the
    real-bet gates/calibration that currently zero WNBA out — so the model's directional edges (e.g.
    a 7pt under) SHOW on the card, tagged paper + CLV-trackable, without ever being staked.
    Austin 2026-09-18. Sides are the model's lean (p>=0.5); edge is vs the de-vig-free implied price."""
    import math
    if not pred or not odds:
        return []
    def ncdf(x):
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))
    def implied(o):
        try:
            o = float(o)
        except (TypeError, ValueError):
            return None
        return (-o) / (-o + 100) if o < 0 else 100 / (o + 100)
    ens = ((pred.get("ensemble") or {}).get("ensemble") or {})
    pmargin = ens.get("margin") if ens.get("margin") is not None else pred.get("predicted_margin")
    ptotal = ens.get("total") if ens.get("total") is not None else pred.get("predicted_total")
    consts = (pred.get("factors") or {}).get("constants") or {}
    sm = consts.get("margin_std") or 16.0
    st = consts.get("total_std") or 21.0
    out = []
    ml = odds.get("ml") or {}
    p_home_ml = ens.get("ml_home")
    if p_home_ml is not None and (ml.get("home") is not None or ml.get("away") is not None):
        if p_home_ml >= 0.5:
            side, prob, o = (home_abbr or "HOME"), p_home_ml, ml.get("home")
        else:
            side, prob, o = (away_abbr or "AWAY"), 1 - p_home_ml, ml.get("away")
        imp = implied(o)
        if o is not None and imp is not None:
            out.append(dict(type="ML", market="ML", pick=side, line=None, prob=round(prob, 4),
                            odds=o, edge=round((prob - imp) * 100, 1), paper=True, model_value=round(p_home_ml, 3)))
    tot = odds.get("total") or {}
    line = tot.get("line")
    if ptotal is not None and line is not None and st > 0:
        p_over = ncdf((ptotal - line) / st)
        side, prob, o = ("Over", p_over, tot.get("over_odds")) if p_over >= 0.5 else ("Under", 1 - p_over, tot.get("under_odds"))
        imp = implied(o)
        if o is not None and imp is not None:
            out.append(dict(type="TOTAL", market="TOTAL", pick=side, line=line, prob=round(prob, 4),
                            odds=o, edge=round((prob - imp) * 100, 1), paper=True, model_value=round(ptotal, 1)))
    sp = odds.get("spread") or {}
    hpt = sp.get("home_pt")
    if pmargin is not None and hpt is not None and sm > 0:
        p_home = ncdf((pmargin - (-hpt)) / sm)
        if p_home >= 0.5:
            side, prob, ln, o = (home_abbr or "HOME"), p_home, hpt, sp.get("home_odds")
        else:
            side, prob, ln, o = (away_abbr or "AWAY"), 1 - p_home, sp.get("away_pt"), sp.get("away_odds")
        imp = implied(o)
        if o is not None and imp is not None and ln is not None:
            out.append(dict(type="SPREAD", market="SPREAD", pick=side, line=ln, prob=round(prob, 4),
                            odds=o, edge=round((prob - imp) * 100, 1), paper=True, model_value=round(pmargin, 1)))
    return out



# Statuses the slate considers "settled" or "not yet started." Anything
# outside this set is treated as in-flight (live, intermission, between
# periods) and triggers the lock-on-live behavior below.
_NOT_LIVE_STATUSES = frozenset({
    "scheduled", "pre_game", "pregame",
    "final", "complete", "completed",
    "cancelled", "postponed", "voided",
})

# Leagues with a Q1 tracker tab record BOTH best_pick_full AND best_pick_q1 —
# they're independent markets with separate P/L, and the bet queue's per-cell
# eligibility filter decides which of the two is actually placeable. Recording
# only the combined headline let an un-queue-eligible full-game total crowd out
# a queue-eligible Q1 (e.g. MIN@GS 2026-08-19: full-game Over recorded @ +32.5%
# edge but not a qualifying cell, so its Q1 Over @ +17.8% never reached the
# queue). Hoisted to module scope so the /today piggyback recorder and the
# explicit /tracker/record endpoint stay in lockstep. Everything else records
# best_pick only (the combined highest-edge pick from the slate).
Q1_TRACKER_LEAGUES = frozenset({
    "wnba", "afl", "ncaam", "euroleague", "ncaaw",
    "china_cba", "bulgaria_nbl", "czech_nbl",
    "germany_bbl", "denmark_basketligaen",
    "finland_korisliiga", "france_pro_b",
    "greece_a1", "hungary_nb1",
    "iceland_urvalsdeild", "iceland_urvalsdeild_w",
    "israel_super", "latvia_lbl",
    "lithuania_lkl", "slovakia_extraliga",
    "slovenia_skl", "sweden_ligan",
    "argentina_lnb", "brazil_lbf_w",
    "brazil_nbb", "dominican_lnb",
    "puerto_rico_bsn", "japan_b2",
    "nz_nbl", "australia_nbl", "korea_kbl",
})


def _bb_parse_tip(s):
    """ISO start time (``2026-11-02T08:30Z`` / ``...:00Z`` / ``+00:00``) -> aware UTC datetime, else None."""
    if not s:
        return None
    try:
        t = datetime.fromisoformat(str(s).strip().replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if len(str(s).strip()) <= 10:
        return None   # date only -- no tip time
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def _lock_basketball_paper_pick(conn, d: dict, league: str) -> None:
    """Persist the PAPER (shadow) pick on live/final cards — so 'the pick it had' stays visible once
    odds pull at tip. Attaches the best-edge recorded shadow pick per family to d['paper_picks'] /
    d['paper_picks_q1'] with a pre-formatted `display` + `result`, matching the fresh scheduled shape
    so the card renders it. Scheduled games keep their fresh on-the-fly paper picks. Austin 2026-09-19."""
    status = (d.get("status") or "scheduled").lower()
    if status in ("scheduled", "pre_game", "pregame"):
        return
    gid = d.get("game_id")
    if not gid:
        return
    from engine.basketball._db import picks_table
    p_tbl = picks_table(league)
    try:
        rows = conn.execute(
            f"SELECT bet_type, pick, model_prob, edge, odds, result "
            f"FROM {p_tbl} WHERE game_id = ? AND is_shadow = 1 ORDER BY edge DESC",
            (str(gid),),
        ).fetchall()
    except Exception:
        return
    if not rows:
        return
    q1_types = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
    def _to(r):
        return {"type": r["bet_type"], "display": r["pick"], "odds": r["odds"],
                "edge": r["edge"], "prob": r["model_prob"], "result": r["result"], "locked": True}
    full = [_to(r) for r in rows if (r["bet_type"] or "") not in q1_types]
    q1 = [_to(r) for r in rows if (r["bet_type"] or "") in q1_types]
    if full:
        d["paper_picks"] = [_apply_selected(league, _p) for _p in full[:1]]
    if q1:
        d["paper_picks_q1"] = [_apply_selected(league, _p) for _p in q1[:1]]


def _lock_basketball_live_pick(conn, d: dict, league: str) -> None:
    """When a basketball framework game is live, surface the open pick
    that was recorded pre-tipoff instead of whatever the fresh predict
    + odds run produces (or in the gated case, nothing at all). The
    tracker grades the pre-tipoff pick; the card has to match.

    Mutates ``d`` in place — ``best_pick`` / ``best_pick_full`` /
    ``best_pick_q1`` get replaced with the locked rows, and ``picks``
    is rebuilt from them.
    """
    from engine.basketball._db import picks_table
    status = (d.get("status") or "scheduled").lower()
    if status in _NOT_LIVE_STATUSES:
        return
    gid = d.get("game_id")
    if not gid:
        return
    p_tbl = picks_table(league)
    try:
        rows = conn.execute(
            f"SELECT bet_type, pick, model_prob, edge, odds "
            f"FROM {p_tbl} "
            f"WHERE game_id = ? AND is_shadow = 0 AND result IS NULL "
            f"ORDER BY edge DESC",
            (str(gid),),
        ).fetchall()
    except Exception:
        return
    if not rows:
        return
    q1_types = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
    def _to_pick(r):
        return {
            "type":       r["bet_type"],
            "pick":       r["pick"],
            "model_prob": r["model_prob"],
            "edge":       r["edge"],
            "odds":       r["odds"],
            "locked":     True,
        }
    full_locked = next((_to_pick(r) for r in rows
                        if (r["bet_type"] or "") not in q1_types), None)
    q1_locked = next((_to_pick(r) for r in rows
                      if (r["bet_type"] or "") in q1_types), None)
    all_locked = max(
        (p for p in (full_locked, q1_locked) if p),
        key=lambda p: (float(p.get("edge") or 0)) * (float(p.get("prob") or p.get("model_prob") or p.get("raw_prob") or 0)),  # "both"=edge x prob (Austin 2026-10-17)
        default=None,
    )
    if all_locked is None:
        return
    d["best_pick_full"] = full_locked
    d["best_pick_q1"] = q1_locked
    _auth_best = _authority_select("basketball", league, [p for p in (full_locked, q1_locked) if p])  # proven-gated live headline (Pick Authority; per-league since 2026-10-22)
    d["best_pick"] = _auth_best
    d["picks"] = [_auth_best] if _auth_best else []
    d["pick_locked"] = True


def _build_potd_bets(games: list[dict], today: str) -> tuple[list[dict], dict]:
    """Reshape slate games into the bet-list shape select_potd expects.
    Returns (bets, game_date_by_id) so callers can look up the chosen
    pick's actual ET date when locking the POTD for far-east leagues."""
    bets: list[dict] = []
    game_date_by_id: dict[str, str] = {}
    for g in games:
        if g.get("status") != "scheduled":
            continue
        bp = g.get("best_pick")
        if not bp:
            continue
        bp_norm = dict(bp)
        if "prob" not in bp_norm and "model_prob" in bp_norm:
            bp_norm["prob"] = bp_norm["model_prob"]
        home_abbr = g.get("home_abbr") or "?"
        away_abbr = g.get("away_abbr") or "?"
        gid = g.get("game_id")
        if gid is not None:
            game_date_by_id[str(gid)] = g.get("date") or today
        bets.append({
            "game_id": gid,
            "matchup": f"{away_abbr} @ {home_abbr}",
            "best_pick": bp_norm,
            "home": {"name": g.get("home_name") or home_abbr,
                      "abbreviation": home_abbr},
            "away": {"name": g.get("away_name") or away_abbr,
                      "abbreviation": away_abbr},
            "time": g.get("start_time") or "",
            "venue": "",
            "status": {"state": "pre"},
        })
    return bets, game_date_by_id


def _resolve_potd_lock_date(league: str, today: str,
                              bets: list[dict],
                              game_date_by_id: dict) -> str:
    """For far-east leagues, lock the POTD under the chosen pick's
    actual ET date so today and tomorrow lookups return the same row.
    Other leagues lock under today. Centralizing this prevents the
    /potd endpoint and the slate route's lazy-create from forking."""
    from engine.basketball._config import is_far_east
    if not is_far_east(league):
        return today
    from engine.pick_of_day._select import select_potd as _sel
    chosen = _sel(league, bets, view="q1")
    if chosen and str(chosen.get("game_id")) in game_date_by_id:
        return game_date_by_id[str(chosen["game_id"])]
    return today


def _read_active_potd(league: str, today: str):
    """Today's POTD with a tomorrow-fallback for far-east leagues
    (locked under game_date which can be tomorrow ET). Single source
    of truth so /potd and the slate route can't drift."""
    from engine.basketball._config import is_far_east
    from engine.pick_of_day import get_today_potd as _potd_read
    from datetime import datetime as _dt, timedelta as _td
    potd = _potd_read(league, view="q1")
    if not potd and is_far_east(league):
        tmrw = (_dt.strptime(today, "%Y-%m-%d") + _td(days=1)
                ).strftime("%Y-%m-%d")
        potd = _potd_read(league, date=tmrw, view="q1")
    return potd


from backend.server._slate_cache import SlateCache
_BASKETBALL_CACHE = SlateCache("basketball")


@router.get("/api/basketball/{league}/today")
def api_basketball_today(league: str) -> dict:
    from engine.basketball import LEAGUE_REGISTRY
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA uses /api/nba/best-bets — don't route through here"}
    return _BASKETBALL_CACHE.serve((league,), lambda: _compute_api_basketball_today(league),
                                   {"league": league, "games": [], "loading": True})


def _compute_api_basketball_today(league: str) -> dict:
    """Return today's slate for ``league`` with per-game predictions,
    HR odds, and picks (when both prediction + odds are available).

    NBA falls through to its existing /api/nba/best-bets endpoint —
    callers should special-case the route on the frontend rather than
    routing NBA through here.
    """
    from datetime import datetime
    from engine.basketball import (
        LEAGUE_REGISTRY, predict_full, generate_picks,
    )
    from engine.basketball._db import get_conn, games_table, teams_table
    from engine.basketball._odds import fetch_league_odds
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA uses /api/nba/best-bets — don't route through here"}

    today = et_today_str()
    real_today = today  # preserved across fall-forward for stub-gate window

    # Refresh upstream BEFORE we query — but debounce per-league so
    # repeat hits inside a minute don't re-hammer RealGM/ESPN. First
    # hit pays the ingest latency (2-12s); subsequent hits inside the
    # window serve fresh-enough state immediately.
    cfg = LEAGUE_REGISTRY[league]
    _now_ts = time.monotonic()
    last_ingest = _SLATE_INGEST_TS.get(league, 0.0)
    if (_now_ts - last_ingest) >= _SLATE_INGEST_TTL_S:
        try:
            ds = cfg.get("data_source") or ""
            # `data_source: "realgm"` is historical — many leagues
            # tagged that way actually backfill via SofaScore (the
            # primary ingest path superseded RealGM for most minor
            # Euro/Asia leagues 2026-05). Route by what's actually
            # mapped: if the league has a sofascore_tournament_id
            # AND isn't in the RealGM map, take the SofaScore path so
            # results refresh and pending picks can settle.
            from engine.basketball._config import sofascore_tournament_id
            from engine.basketball._realgm_ingest import _REALGM_LEAGUES
            has_realgm = league in _REALGM_LEAGUES
            has_sofa = bool(sofascore_tournament_id(league))
            if ds == "espn":
                from engine.basketball._espn_ingest import ingest_today as _esp
                _esp(league)
                # 2-day backsweep — late-finalized games (WNBA CON@POR
                # 2026-05-18 was stuck at status='in' for two weeks
                # because the ingest only ever pulled today's scoreboard)
                # now get re-fetched so the settler sees the final.
                # Same defense pattern as soccer/baseball/football routes.
                from datetime import datetime as _dt0, timedelta as _td0
                today_dt = _dt0.now().date()
                for back in (1, 2):
                    d = (today_dt - _td0(days=back)).strftime("%Y-%m-%d")
                    try:
                        _esp(league, date=d)
                    except Exception as e:
                        logger.debug("[basketball:%s] backsweep %s failed: %s",
                                      league, d, e)
            elif ds == "euroleague_api":
                from engine.basketball._euroleague_ingest import (
                    ingest_today as _eu,
                )
                _eu()
                from datetime import datetime as _dt0, timedelta as _td0
                today_dt = _dt0.now().date()
                for back in (1, 2):
                    d = (today_dt - _td0(days=back)).strftime("%Y-%m-%d")
                    try:
                        _eu(date=d)
                    except Exception as e:
                        logger.debug("[euroleague] backsweep %s failed: %s",
                                      d, e)
            elif has_realgm:
                from engine.basketball._realgm_ingest import ingest_today as _rg
                _rg(league)
            elif has_sofa:
                from engine.basketball._sofascore_primary_ingest import (
                    ingest_today as _sofa_today,
                )
                # Sofascore ingest already takes days_back; bump to 2.
                _sofa_today(league, days_back=2)
            _SLATE_INGEST_TS[league] = _now_ts
        except Exception as e:
            logger.debug("slate refresh %s failed: %s", league, e)

    conn = get_conn(league)
    g_tbl = games_table(league)
    t_tbl = teams_table(league)
    # Stale-game sweep — flip any 'scheduled' row older than 2 days
    # with no score into 'cancelled'. ESPN / RealGM / SofaScore each
    # silently drop postponed events, and the unswept rows pollute
    # the slate + leave picks dangling. Runs cheap (single UPDATE)
    # so we eat the cost per route hit rather than scattering it
    # across four ingest modules.
    try:
        from datetime import datetime as _stale_dt, timedelta as _stale_td
        cutoff = (_stale_dt.now() - _stale_td(days=2)
                   ).strftime("%Y-%m-%d")
        conn.execute(
            f"UPDATE {g_tbl} SET status = 'cancelled' "
            f"WHERE date < ? "
            f"  AND status NOT IN ('final', 'complete', 'completed', "
            f"                       'cancelled', 'postponed', 'voided') "
            f"  AND home_score IS NULL",
            (cutoff,),
        )
        # Force-finalize games stuck at status='in' for >2 days with a
        # populated home_score (i.e. ESPN told us the game tipped off
        # but never sent the final-status update). If linescore quarters
        # are populated, the current home_score/away_score are the
        # finals — flip status so the settler can grade pending picks.
        # WNBA CON@POR 2026-05-18 was the trigger: stuck at status='in'
        # with Q1-Q3 populated but no Q4 for two weeks until the user
        # flagged it.
        conn.execute(
            f"UPDATE {g_tbl} SET status = 'final' "
            f"WHERE date < ? "
            f"  AND status IN ('in', 'live', 'in_progress') "
            f"  AND home_score IS NOT NULL AND away_score IS NOT NULL",
            (cutoff,),
        )
        conn.commit()
    except Exception as _e:
        logger.debug("[basketball:%s] stale-sweep failed: %s", league, _e)
    select_cols = (
        f"g.game_id, g.date, g.start_time, g.status, "
        f"g.home_team_id, h.abbreviation AS home_abbr, h.name AS home_name, "
        f"h.logo_url AS home_logo, "
        f"g.away_team_id, a.abbreviation AS away_abbr, a.name AS away_name, "
        f"a.logo_url AS away_logo, "
        f"g.home_score, g.away_score "
    )
    # Pick Authority leagues (NCAAM / NCAAW): the college ratings model predicts by team
    # id with the game's neutral-site / conference flags; headlines are PA-gated per view
    # and every market is shadow-recorded at the model's own price (MintyBot 2026-10-26).
    _pa = bool(cfg.get("pick_authority"))
    if _pa:
        select_cols += ", g.neutral AS neutral, g.conf_game AS conf_game "
    try:   # live clock ("4:12 - 2nd Half") where the league's ingest records it (ESPN-fed leagues, 2026-10-31)
        if any(r[1] == "status_detail" for r in conn.execute(f"PRAGMA table_info({g_tbl})")):
            select_cols += ", g.status_detail AS status_detail "
    except Exception:
        pass
    join_clause = (
        f"FROM {g_tbl} g "
        f"LEFT JOIN {t_tbl} h ON g.home_team_id = h.id "
        f"LEFT JOIN {t_tbl} a ON g.away_team_id = a.id "
    )
    # Far-east leagues (China/Japan/Korea/Australia/NZ) include tomorrow
    # ET in the same window because their local "tonight" tipoff (7 PM
    # AEDT/JST/KST/CST) maps to 3-7 AM ET the next day. Without this the
    # panel is permanently 24h behind — by the time a game's date stamp
    # rolls into the user's "today", the game has already finished.
    from engine.basketball._config import is_far_east
    from datetime import datetime as _dt, timedelta as _td
    if is_far_east(league):
        tmrw = (_dt.strptime(today, "%Y-%m-%d") + _td(days=1)
                ).strftime("%Y-%m-%d")
        rows = conn.execute(
            f"SELECT {select_cols}{join_clause}WHERE g.date IN (?, ?) "
            f"  AND (g.status IS NULL "
            f"    OR g.status NOT IN ('cancelled','postponed','voided')) "
            f"ORDER BY g.date ASC, COALESCE(g.start_time, '9999') ASC, "
            f"  g.game_id",
            (today, tmrw),
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT {select_cols}{join_clause}WHERE g.date = ? "
            f"  AND (g.status IS NULL "
            f"    OR g.status NOT IN ('cancelled','postponed','voided')) "
            f"ORDER BY COALESCE(g.start_time, '9999') ASC, g.game_id",
            (today,),
        ).fetchall()
    # Empty today *or* all-final today → fall forward to TOMORROW only
    # (max 1 day ahead). All-final triggers when the day's slate has
    # already wrapped — AFL had this on 2026-05-10 (single 01:15 ET game,
    # final by morning) and the panel showed a finished game with no HR
    # odds even though HR's next-round slate (May 14-17) was on the
    # board. Capping at +1 day means the panel shows nothing rather than
    # a phantom "tonight" from days out. (Far-east leagues already
    # include tomorrow, so this only fires for leagues without overnight
    # ET coverage.)
    has_scheduled = any((r["status"] or "scheduled") not in
                         ("final", "complete", "completed", "cancelled",
                          "postponed", "voided")
                         for r in rows)
    fell_forward = False
    # Walk forward up to 7 days. Earlier this was capped at +1 day to
    # avoid surfacing phantom-tonight cards with no date pill, but the
    # response now exposes `fell_forward` + the actual `date`, and the
    # frontend renders an explicit "no games today, showing next slate
    # (DATE)" banner — so a 4-day jump is unambiguous to the user.
    # Without a wider walk, AFL on 2026-05-10 (one finished game today,
    # next round May 14-17) would still show empty.
    if not has_scheduled and not is_far_east(league):
        for delta in range(1, 8):
            cand = (_dt.strptime(today, "%Y-%m-%d") + _td(days=delta)
                    ).strftime("%Y-%m-%d")
            next_rows = conn.execute(
                f"SELECT {select_cols}{join_clause}WHERE g.date = ? "
                f"  AND (g.status IS NULL "
                f"    OR g.status NOT IN ('cancelled','postponed','voided')) "
                f"ORDER BY COALESCE(g.start_time, '9999') ASC, g.game_id",
                (cand,),
            ).fetchall()
            if any((r["status"] or "scheduled") not in
                    ("final", "complete", "completed", "cancelled",
                     "postponed", "voided") for r in next_rows):
                rows = next_rows
                today = cand
                fell_forward = True
                break
    # HR odds — single fetch per request, keyed off LEAGUE_REGISTRY's
    # hr_comp_id. Empty dict on any failure; downstream just gets
    # no-odds-no-picks.
    try:
        odds_by_key = fetch_league_odds(league)
    except Exception as e:
        logger.warning("HR odds %s failed: %s", league, e)
        odds_by_key = {}

    games = []
    for r in rows:
        d = dict(r)
        odds = None
        picks: list = []
        if d["status"] == "scheduled" and d["home_abbr"] and d["away_abbr"]:
            try:
                if _pa:
                    pred = predict_full(league, d["home_abbr"], d["away_abbr"],
                                        home_id=d["home_team_id"], away_id=d["away_team_id"],
                                        game_date=d["date"], neutral=d.get("neutral"),
                                        conf_game=d.get("conf_game"), game_id=d["game_id"])
                else:
                    pred = predict_full(league, d["home_abbr"], d["away_abbr"])
            except Exception as e:
                logger.debug("predict %s/%s@%s failed: %s",
                             league, d["away_abbr"], d["home_abbr"], e)
                pred = {"error": str(e)}
            # Attach odds + run picks pipeline if odds matched.
            key = f"{d['away_abbr']}@{d['home_abbr']}"
            _college = league in ("ncaam", "ncaaw")
            if _pa and _college:
                # College odds are already tied to OUR game id (_college_names.attach). Other PA
                # leagues (China CBA, 2026-10-28) join by key + the full-name fallback below.
                odds = next((v for v in (odds_by_key or {}).values()
                             if str(v.get("game_id")) == str(d["game_id"])), None)
            else:
                odds = odds_by_key.get(key)
            # Fallback: HR's abbreviation may differ from our canonical
            # one when their team name is generic (CBA: HR ships
            # "Zhejiang" → ZHE; our DB has Zhejiang Chouzhou → ZHEC).
            # Fuzzy-match against HR odds by team full-name first word
            # so the canonical row picks up the live market instead of
            # a side-by-side HR-stub card.
            if not odds and not (_pa and _college) and (d.get('home_name') or d.get('away_name')):
                hn_first = (d.get('home_name') or '').split()[:1]
                an_first = (d.get('away_name') or '').split()[:1]
                for hr_key, hr_v in (odds_by_key or {}).items():
                    h_full = (hr_v.get('home_full_name') or '').lower()
                    a_full = (hr_v.get('away_full_name') or '').lower()
                    if (hn_first and h_full.startswith(hn_first[0].lower())
                            and an_first and a_full.startswith(an_first[0].lower())):
                        odds = hr_v
                        break
            # STARTED-GAME CHECK (2026-11-01): the DB status lags (RealGM ingest ~1h behind; NBL/CBA
            # start_time NULL), so in-play lines were priced as pre-game (lean rg-9398d792c5 locked 56 min
            # after tip). Fill a NULL start_time from HR's event BEFORE pricing, and call the game started
            # when HR flags it in-play OR now >= tip (HR time when within 12h of the DB time, else the DB
            # time; DB time alone when there are no odds).
            if odds and odds.get("start_time") and not d.get("start_time"):
                d["start_time"] = odds["start_time"]
            _bb_tip = _bb_parse_tip(d.get("start_time"))
            _hr_tip = _bb_parse_tip((odds or {}).get("start_time"))
            if _hr_tip and (_bb_tip is None or abs((_hr_tip - _bb_tip).total_seconds()) <= 12 * 3600):
                _bb_tip = _hr_tip
            _bb_now = datetime.now(timezone.utc)
            _bb_started = bool((odds and odds.get("inplay")) or (_bb_tip is not None and _bb_now >= _bb_tip))
            if _bb_started:
                d["status"] = "in"      # live (HR in-play or past tip): no pre-game picks, shadow rows or lean locks off live lines
            if pred and not pred.get("error") and odds and not _bb_started:
                # Predictor already has home_abbr/away_abbr in output
                pred.setdefault("home_abbr", d["home_abbr"])
                pred.setdefault("away_abbr", d["away_abbr"])
                try:
                    picks = generate_picks(league, pred, odds,
                                            game_id=d["game_id"])
                except Exception as e:
                    logger.warning("picks %s/%s failed: %s",
                                   league, d["game_id"], e)
                # PA shadow lane: every market priced on the MODEL's own probability (W=1)
                # and mirrored to picks_unified is_shadow=1 so unproven markets build a
                # record and graduate (NBA's design). Today's games only.
                # (2026-10-29) the shadow candidates also price FUTURE-dated games so their card
                # lean comes from the same pool (card_lean), but only TODAY's games are recorded.
                if _pa and (
                        time.time() - _PA_SHADOW_LAST.get(str(d["game_id"]), 0.0)
                        >= _PA_SHADOW_EVERY_S):
                    try:
                        from engine.basketball._picks import shadow_mode as _bb_shadow
                        from engine.picks_unified import record_shadow_slate as _rss_bb
                        with _bb_shadow():
                            _sh = generate_picks(league, pred, odds, game_id=d["game_id"])
                        _PA_SHADOW_CANDS[str(d["game_id"])] = _sh or []
                        if (_bb_tip is not None and 0 < (_bb_tip - _bb_now).total_seconds() <= 24 * 3600) or (
                                _bb_tip is None and d.get("date") == real_today):   # tip within 24h (2026-11-01), was ET date == today
                            _rss_bb("basketball", league, d["game_id"],
                                    f"{d['away_abbr']} @ {d['home_abbr']}", _sh or [])
                        _PA_SHADOW_LAST[str(d["game_id"])] = time.time()
                    except Exception as e:
                        logger.debug("PA shadow %s/%s failed: %s", league, d["game_id"], e)
            # HR's start_time is more reliable than RealGM's parsed time
            # (RealGM ships rounded HH:MM ET that can be 1-2h off the
            # actual tipoff — Nelson @ Franklin on 2026-05-08 was 5:00 AM
            # in RealGM but 3:30 AM per HR / SofaScore). Prefer HR when
            # available — BUT only when the swap is a minor refinement,
            # not a wholesale day-of-week move. Toronto Tempo @ LA on
            # 2026-05-15 had ESPN at Fri 22:00 ET (correct) and HR at
            # Sun 19:00 ET (wrong — HR had the matchup listed twice
            # under different events). Cap the swap window at ±12h vs
            # the DB time so a wildly-wrong HR row can't relabel the
            # whole card to a different day.
            if odds and odds.get("start_time") and d.get("start_time"):
                try:
                    from datetime import datetime as _dt
                    _hr_iso = odds["start_time"].replace("Z", "+00:00")
                    _db_iso = d["start_time"].replace("Z", "+00:00")
                    _hr_dt = _dt.fromisoformat(_hr_iso)
                    _db_dt = _dt.fromisoformat(_db_iso)
                    if abs((_hr_dt - _db_dt).total_seconds()) <= 12 * 3600:
                        d["start_time"] = odds["start_time"]
                except (ValueError, TypeError, AttributeError):
                    # Fall through to DB time on any parse failure.
                    pass
            elif odds and odds.get("start_time") and not d.get("start_time"):
                # DB has no time — accept HR's unconditionally.
                d["start_time"] = odds["start_time"]
        else:
            pred = None
        # Standard layout: ONE pick per game card. Pick the highest-edge
        # non-skip candidate; the rest stay reachable via the game-detail
        # pane but never bloat the card or the slate-header pick count.
        # See feedback_standard_layout.md #2.
        #
        # For leagues that emit Q1 markets (WNBA from 2026-05-13),
        # split the best pick across two views — best_pick_full picks
        # from the full-game ML/SPREAD/TOTAL candidates only, and
        # best_pick_q1 picks from Q1_ML/Q1_SPREAD/Q1_TOTAL only. NBA's
        # frontend already keys on these per-view fields; WNBA gets
        # parity once the toggle is wired.
        Q1_TYPES = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
        full_candidates = [p for p in picks if p.get("type") not in Q1_TYPES]
        q1_candidates = [p for p in picks if p.get("type") in Q1_TYPES]
        # Prefer a staked pick (stake_units>0) over any 0u pick when
        # picking the card headline. 0u happens for two reasons:
        # (a) NOPLAY shadow — banned cell emitted only for learning;
        # (b) calibrated prob below the stake floor (e.g. < 0.52) — the
        # model isn't confident enough to bet. Either way it should
        # never headline the card over a real recommended bet. Same
        # pattern as engine.picks.get_best_pick.
        def _pick_best(cands):
            if not cands:
                return None
            staked = [p for p in cands if (p.get("stake_units") or 0) > 0]
            pool = staked if staked else cands
            return max(pool, key=lambda p: (float(p.get("edge") or 0)) * (float(p.get("prob") or p.get("model_prob") or p.get("raw_prob") or 0)))  # "both"=edge x prob (Austin 2026-10-17)
        best_full = (_authority_select("basketball", league, full_candidates) if _pa
                     else _pick_best(full_candidates))
        if best_full and (best_full.get("confidence") or "lean") == "skip":
            best_full = None
        # Full-game RUNNER-UP: best remaining full-game market (ML/SPREAD/TOTAL)
        # after best_full. Recorded alongside best_full so a valid, gate-cleared
        # pick isn't silently dropped just because a same-game market had a higher
        # (often calibration-inflated) edge — this is what zeroed out WNBA ML for
        # days (Total/Spread out-edged it). bet_queue's cell gate, not this
        # card-layout code, then decides what actually places. (2026-08-20)
        _full_rest = [p for p in full_candidates if p is not best_full]
        best_full_runner_up = None if _pa else _pick_best(_full_rest)   # PA: the view's ONE gated pick
        if best_full_runner_up and (best_full_runner_up.get("confidence") or "lean") == "skip":
            best_full_runner_up = None
        best_q1 = (_authority_select("basketball", league, q1_candidates) if _pa
                   else _pick_best(q1_candidates))
        if best_q1 and (best_q1.get("confidence") or "lean") == "skip":
            best_q1 = None
        # `best_pick` stays as the headline across all markets so existing
        # callers (POTD selector, single-pick cards) keep working. Same
        # staked-over-shadow preference applies here.
        all_best = _pick_best([p for p in (best_full, best_q1) if p])
        d["prediction"] = pred
        d["odds"] = odds
        d["card_win_prob"] = _basketball_card_wp(league, pred or {}, odds)
        _auth_best = _authority_select("basketball", league, picks)  # proven-gated across all markets (Pick Authority 2026-10-18); per-league since 2026-10-22
        # Card == tracker (Austin 2026-11-01 "one pick, read everywhere"): once the game has a RECORDED
        # authority pick (first-recorded is_shadow=1 row = the best_pick this card headlined when the
        # recorder wrote it), the headline HOLDS it at its entry price -- pre-game, live and final --
        # instead of re-deriving a live pick as lines move. /tracker/unified reads the SAME function.
        # The live selection above still headlines until a pick is recorded (so the recorder below,
        # which reads best_pick, records it once; afterwards it re-reads the held pick = no-op).
        # Non-PA leagues (WNBA) record paper LEANS, not the headline -> the held row drives card_lean.
        _held_best = None
        _rec_lean = None
        try:
            _rec = _basketball_recorded_authority_best(league, d.get("game_id"), conn=conn)
        except Exception:
            _rec = None
        if _rec and _pa:
            from engine.config import EDGE_STRONG, EDGE_MODERATE
            _re = float(_rec.get("edge") or 0)
            _held_best = {**_rec, "model_prob": _rec.get("prob"), "held": True, "locked": True,
                          "confidence": ("strong" if _re >= EDGE_STRONG else "moderate" if _re >= EDGE_MODERATE else "lean")}
            _auth_best = _held_best
        elif _rec:
            _rec_lean = {**_rec, "model_prob": _rec.get("prob"), "held": True, "locked": True, "lean": True,
                         "graduated": bool(_auth_best and _auth_best.get("type") == _rec.get("type")
                                           and _auth_best.get("pick") == _rec.get("pick"))}
        try:
            from engine.selection import card_lean as _card_lean
            d["card_lean"] = _card_lean("basketball", league,
                                        _PA_SHADOW_CANDS.get(str(d.get("game_id"))) or picks, _auth_best)  # card's best lean (Austin 2026-10-26): graduated pick, else the ungraduated lean -- display only
        except Exception:
            d["card_lean"] = None
        try:   # held per game (Austin 2026-10-27): first lean kept pre-game; live/final show the held lean, never a live recompute
            from engine.selection import lean_lock as _lean_lock
            if (d.get("status") or "scheduled").lower() in ("scheduled", "pre_game", "pregame"):
                d["card_lean"] = _lean_lock.hold(f"basketball:{league}", d.get("game_id"), d.get("card_lean"))
            else:
                d["card_lean"] = _lean_lock.held_with_result(f"basketball:{league}", league, d.get("game_id"),
                                                             game_key=f"basketball:{league}:{d.get('game_id')}")
        except Exception:
            pass
        if _held_best:   # the pill shows the held graduated headline, never a different held lean (2026-11-01)
            try:
                from engine.selection import card_lean as _card_lean
                d["card_lean"] = _card_lean("basketball", league, [], _held_best)
            except Exception:
                d["card_lean"] = dict(_held_best, graduated=True, lean=False)
        elif _rec_lean:
            d["card_lean"] = _rec_lean
        d["picks"] = [_auth_best] if _auth_best else []
        d["best_pick"] = _auth_best
        d["best_pick_full"] = best_full
        d["best_pick_full_runner_up"] = best_full_runner_up
        d["best_pick_q1"] = best_q1
        d["paper_picks"] = [_apply_selected(league, _p) for _p in (_basketball_paper_picks(pred, odds, d.get("home_abbr"), d.get("away_abbr")) or [])]  # Austin 2026-09-18 + calibrated (Pick Authority)
        d["paper_picks_q1"] = [_apply_selected(league, _p) for _p in (_basketball_q1_paper_picks(pred, odds, d.get("home_abbr"), d.get("away_abbr"), league) or [])]  # Austin 2026-09-19 + calibrated
        # Lock the pre-tipoff pick once the game goes live. Mutates d
        # in place; no-op for scheduled/final games.
        _lock_basketball_live_pick(conn, d, league)
        _lock_basketball_paper_pick(conn, d, league)
        if _pa:
            # The live lock above re-selects from the real (is_shadow=0) rows; the headline must stay the
            # recorded authority pick the tracker grades. Started game with none recorded -> no headline
            # (never a live recompute). Austin 2026-11-01.
            if _held_best:
                d["best_pick"] = _held_best
                d["picks"] = [_held_best]
            elif (d.get("status") or "scheduled").lower() not in ("scheduled", "pre_game", "pregame"):
                d["best_pick"] = None
                d["picks"] = []
        # Austin 2026-10-04: never surface a live recompute once non-scheduled -- if the lock
        # did not attach a recorded (locked) pick, blank it rather than leak the fresh recompute.
        if (d.get("status") or "scheduled").lower() not in ("scheduled", "pre_game", "pregame"):
            for _k in ("paper_picks", "paper_picks_q1"):
                _v = d.get(_k)
                if _v and not (isinstance(_v[0], dict) and _v[0].get("locked")):
                    d[_k] = []
        from ._livefreeze import freeze_live_paper
        freeze_live_paper(d, league)
        games.append(d)

    # HR-stub backfill — same pattern as routes_hockey.py. Some leagues
    # in the registry have no canonical schedule source (china_cba uses
    # RealGM but no scraper exists yet; brazil_nbb / other RealGM tiers
    # similarly lack ingest). When HR posts an odds market we still want
    # the user to see the matchup; otherwise CBA looks like an empty
    # tab even though HR is trading 2 games tonight.
    seen_keys = {f"{g.get('away_abbr')}@{g.get('home_abbr')}" for g in games
                  if g.get('away_abbr') and g.get('home_abbr')}
    # Track game_ids already on the slate so a stub matched to an
    # existing canonical game (CBA: HR's "Zhejiang" → ZHEC@SHE) doesn't
    # render twice — once from the games-table iteration and again
    # from the HR-stub backfill.
    seen_game_ids = {str(g.get('game_id')) for g in games
                      if g.get('game_id') is not None}
    for hr_key, hr_odds in (odds_by_key or {}).items():
        if hr_key in seen_keys:
            continue
        if _pa:
            break   # college: ESPN schedules every D1 game; odds only attach to OUR games
        away_abbr = (hr_odds.get("away_abbr")
                     or hr_key.split("@")[0])
        home_abbr = (hr_odds.get("home_abbr")
                     or hr_key.split("@")[1])
        # Resolve full team names + logos when present in the per-league
        # teams table; fall back to abbreviation otherwise.
        h_row = conn.execute(
            f"SELECT * FROM {t_tbl} WHERE abbreviation = ?",
            (home_abbr,)).fetchone()
        a_row = conn.execute(
            f"SELECT * FROM {t_tbl} WHERE abbreviation = ?",
            (away_abbr,)).fetchone()
        # Fuzzy-match against full team names when abbreviation lookup
        # fails. HR ships "Zhejiang" generically; our DB has the more
        # specific "Zhejiang Chouzhou" / "Zhejiang Guangsha" — without
        # this, the stub minted a fresh ZHE@SHE card alongside the
        # canonical RealGM ZHEC@SHE row and the user saw the same
        # game twice. We trust the match only when ONE row matches
        # (avoids picking the wrong Zhejiang when both options exist).
        if not h_row and (hr_odds.get("home_full_name") or "").strip():
            hn = hr_odds["home_full_name"].split()[0]
            cands = conn.execute(
                f"SELECT * FROM {t_tbl} WHERE name LIKE ?",
                (f"{hn}%",)).fetchall()
            if len(cands) == 1:
                h_row = cands[0]
        if not a_row and (hr_odds.get("away_full_name") or "").strip():
            an = hr_odds["away_full_name"].split()[0]
            cands = conn.execute(
                f"SELECT * FROM {t_tbl} WHERE name LIKE ?",
                (f"{an}%",)).fetchall()
            if len(cands) == 1:
                a_row = cands[0]
        h_team = dict(h_row) if h_row else {}
        a_team = dict(a_row) if a_row else {}
        # Fall back to HR's full participant name when our teams table
        # doesn't have the team yet — for pre-launch leagues like CBA,
        # Brazil NBB, etc. without a canonical schedule scraper.
        h_full = h_team.get("name") or hr_odds.get("home_full_name") or home_abbr
        a_full = a_team.get("name") or hr_odds.get("away_full_name") or away_abbr
        # Stable stub-id derived from the matchup (deterministic across
        # restarts — see hockey route's MD5 fix for the rationale).
        import hashlib as _hashlib
        _stub_id = int(_hashlib.md5(hr_key.encode("utf-8"))
                        .hexdigest()[:8], 16) % (10 ** 9)
        start_iso = hr_odds.get("start_time") or ""
        # HR's start_time is UTC. A "tonight 8 PM ET" game stamps as
        # midnight UTC tomorrow — without an ET conversion the stub
        # filter rejects it as a future game. Convert to ET-day so
        # the matching aligns with the user's mental model.
        stub_date_utc = start_iso[:10] if len(start_iso) >= 10 else today
        stub_date = stub_date_utc
        if start_iso:
            try:
                from datetime import datetime as _dt, timezone as _tz, timedelta as _td
                if start_iso.endswith("Z"):
                    dt = _dt.strptime(start_iso, "%Y-%m-%dT%H:%M:%SZ")
                    dt = dt.replace(tzinfo=_tz.utc)
                else:
                    dt = _dt.fromisoformat(start_iso)
                # Convert UTC -> ET using zoneinfo so the offset is
                # right whether we're in EDT (mid-Mar to early-Nov) or
                # EST (rest of year). Earlier this was hardcoded -4
                # which silently rounded Nov-Mar games into the wrong
                # ET date.
                if _US_EASTERN is not None:
                    et = dt.astimezone(_US_EASTERN)
                else:
                    # Fallback: month-keyed offset, EDT roughly Mar-Oct.
                    offset_hours = -4 if 3 <= dt.month <= 10 else -5
                    et = dt.astimezone(_tz(_td(hours=offset_hours)))
                stub_date = et.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass
        # Stubs must match the slate's effective date in ET. Strict
        # match prevents the Euroleague case (5-08 canonical leaking
        # onto 5-07 slate) while still surfacing late-night ET games
        # whose UTC stamp rolls into tomorrow's date.
        # Far-east leagues additionally accept tomorrow ET so HR's
        # markets for their local-tonight games (3-7 AM ET tipoff)
        # mint stubs even before the canonical schedule scraper sees
        # them.
        if is_far_east(league):
            tmrw_stub = (_dt.strptime(real_today, "%Y-%m-%d") + _td(days=1)
                          ).strftime("%Y-%m-%d")
            if stub_date not in (real_today, tmrw_stub):
                continue
        elif stub_date != today:
            continue
        # Prefer an existing games-table row on the stub_date so the
        # stub inherits the canonical game_id (and picks can settle
        # against final scores later). Without this, MAD@HTA on
        # 2026-05-07 generated a hr-* stub even though E2025_398 was
        # already in our games table — so picks recorded against the
        # synthetic id would never settle.
        gid_real = None
        if h_team.get("id") and a_team.get("id"):
            row = conn.execute(
                f"SELECT game_id FROM {g_tbl} WHERE date = ? "
                f"  AND home_team_id = ? AND away_team_id = ? LIMIT 1",
                (stub_date, h_team.get("id"), a_team.get("id"))
            ).fetchone()
            if row:
                gid_real = row["game_id"]
        # When fuzzy team-resolution couldn't pick exactly one team
        # (CBA: HR's "Zhejiang" matches both Zhejiang Chouzhou and
        # Zhejiang Guangsha), still try to link by name-substring on
        # the games table for the exact date. If only one game on that
        # date matches the partial names from BOTH sides, link it.
        if not gid_real:
            h_first = (hr_odds.get("home_full_name") or "").split()[:1]
            a_first = (hr_odds.get("away_full_name") or "").split()[:1]
            if h_first and a_first:
                row = conn.execute(
                    f"SELECT g.game_id FROM {g_tbl} g "
                    f"JOIN {t_tbl} h ON g.home_team_id = h.id "
                    f"JOIN {t_tbl} a ON g.away_team_id = a.id "
                    f"WHERE g.date = ? AND h.name LIKE ? AND a.name LIKE ? "
                    f"LIMIT 1",
                    (stub_date, f"{h_first[0]}%", f"{a_first[0]}%")
                ).fetchone()
                if row:
                    gid_real = row["game_id"]
        # If we resolved to a canonical game already in `games`, skip
        # the stub entirely — the canonical row is being shown.
        if gid_real and str(gid_real) in seen_game_ids:
            continue

        # When fuzzy-matched, fill in canonical team data so the stub
        # renders with proper names + logos instead of synthetic abbrevs.
        if gid_real:
            if not h_team.get("id"):
                hr_full = conn.execute(
                    f"SELECT t.* FROM {t_tbl} t "
                    f"JOIN {g_tbl} g ON g.home_team_id = t.id "
                    f"WHERE g.game_id = ?", (gid_real,)).fetchone()
                if hr_full:
                    h_team = dict(hr_full)
                    h_full = h_team.get("name") or h_full
            if not a_team.get("id"):
                ar_full = conn.execute(
                    f"SELECT t.* FROM {t_tbl} t "
                    f"JOIN {g_tbl} g ON g.away_team_id = t.id "
                    f"WHERE g.game_id = ?", (gid_real,)).fetchone()
                if ar_full:
                    a_team = dict(ar_full)
                    a_full = a_team.get("name") or a_full

        # Run prediction + picks when both teams resolve (most leagues
        # populate the teams table from canonical-source ingest, so we
        # can model Euroleague's tomorrow-stub MAD@HTA the same as a
        # row from the games table). For pre-launch leagues with empty
        # teams tables, this falls through silently and the card just
        # shows odds without a model lean.
        stub_pred = None
        stub_picks: list = []
        stub_best = None
        if home_abbr and away_abbr and h_team.get("id") and a_team.get("id"):
            try:
                stub_pred = predict_full(league, home_abbr, away_abbr)
                if stub_pred and not stub_pred.get("error"):
                    stub_pred.setdefault("home_abbr", home_abbr)
                    stub_pred.setdefault("away_abbr", away_abbr)
                    stub_picks = generate_picks(
                        league, stub_pred, hr_odds,
                        game_id=gid_real or f"hr-{_stub_id}")
            except Exception as e:
                logger.debug("stub predict %s/%s failed: %s",
                              league, hr_key, e)
        # Same Q1-vs-Full split + stake-aware "best" selection as the
        # canonical path above. Without this the stub card sets only
        # `best_pick` and BasketballPanel's `best_pick_full`/`q1` view
        # key reads null — the card renders without a pick badge even
        # when the model has a strong recommendation. WNBA 2026-06-21
        # GS@LV, WSH@MIN, NY@LA all hit this (HR scoreboard had today's
        # games before ESPN did).
        STUB_Q1 = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
        stub_full_cands = [p for p in stub_picks if p.get("type") not in STUB_Q1]
        stub_q1_cands   = [p for p in stub_picks if p.get("type") in STUB_Q1]
        def _stub_pick_best(cands):
            if not cands:
                return None
            staked = [p for p in cands if (p.get("stake_units") or 0) > 0]
            chosen = max(staked or cands, key=lambda p: p.get("edge", 0))
            if (chosen.get("confidence") or "lean") == "skip":
                return None
            return chosen
        stub_best_full = _stub_pick_best(stub_full_cands)
        stub_best_q1   = _stub_pick_best(stub_q1_cands)
        stub_best = _stub_pick_best(
            [p for p in (stub_best_full, stub_best_q1) if p]
        )
        stub = {
            "game_id": gid_real or f"hr-{_stub_id}",
            "date": stub_date,
            "start_time": start_iso or None,
            "status": "scheduled",
            "home_team_id": h_team.get("id"),
            "home_abbr": home_abbr,
            "home_name": h_full,
            "home_logo": h_team.get("logo_url"),
            "away_team_id": a_team.get("id"),
            "away_abbr": away_abbr,
            "away_name": a_full,
            "away_logo": a_team.get("logo_url"),
            "home_score": None, "away_score": None,
            "prediction": stub_pred,
            "odds": hr_odds,
            "card_win_prob": _basketball_card_wp(league, stub_pred or {}, hr_odds),
            "picks": [stub_best] if stub_best else [],
            "best_pick": stub_best,
            "best_pick_full": stub_best_full,
            "best_pick_q1": stub_best_q1,
        }
        games.append(stub)

    # Piggyback record + settle on every slate hit. Mirrors NHL/NBA so
    # picks land in the tracker the moment they're emitted, and games
    # that just went final settle without waiting for the worker tick.
    # record_pick is idempotent on (date, game_id, bet_type, pick) for
    # pending rows, so re-hitting this route is a no-op.
    try:
        from engine.basketball._tracker import (record_pick as _bb_record,
                                                  settle_picks as _bb_settle)
        recorded = 0
        for g in games:
            # Q1-tracker leagues record BOTH markets (full + Q1) so a placeable
            # Q1 isn't crowded out of the queue by an un-placeable full-game
            # headline; the queue's cell filter decides which actually places.
            # Mirrors the explicit /tracker/record endpoint. record_pick is
            # idempotent on (date, game_id, bet_type, pick), so this stays a
            # no-op on re-hit.
            if league in Q1_TRACKER_LEAGUES:
                cands = [g.get("best_pick_full"), g.get("best_pick_full_runner_up"), g.get("best_pick_q1")]
            else:
                cands = [g.get("best_pick")]
            for best in cands:
                if not best:
                    continue
                try:
                    pid = _bb_record(league, {
                        "date": g.get("date") or today,
                        "game_id": g.get("game_id"),
                        "matchup": f"{g.get('away_abbr')} @ {g.get('home_abbr')}",
                        "bet_type": best.get("type"),
                        "pick": best.get("pick"),
                        "model_prob": best.get("prob"),
                        "edge": best.get("edge"),
                        "odds": best.get("odds"),
                        "stake_units": best.get("stake_units"),
                    })
                    if pid:
                        recorded += 1
                except Exception as e:
                    logger.debug("basketball record %s/%s failed: %s",
                                  league, g.get("game_id"), e)
        if recorded:
            logger.info("basketball tracker (api hit) %s: recorded=%d",
                        league, recorded)
        sett = _bb_settle(league)
        if sett.get("settled"):
            logger.info("basketball settle (api hit) %s: %s", league, sett)
    except Exception as e:
        logger.warning("basketball tracker piggyback %s failed: %s", league, e)

    # PAPER shadow picks (Austin 2026-09-19): record the model's best paper lean per market view
    # (full + Q1) as is_shadow=1 — grades + CLVs via the same machinery, but isolated in its own
    # is_shadow namespace inside record_pick so it NEVER touches the real record. Proof-of-edge tracker.
    try:
        from engine.basketball._tracker import record_pick as _bb_record_p
        _paper_n = 0
        _pa_lg = bool(cfg.get("pick_authority"))
        for g in games:
            if _pa_lg:
                # Pick Authority leagues (Austin 2026-10-29, NCAAM: "being recorded in the tracker even
                # though they're leans and not graduated picks"): the tracker gets the card's GRADUATED
                # pick only (best_pick = the authority's gated pick), the NBA behaviour. Leans build
                # their record in the picks_unified shadow lane, never here.
                _gb = g.get("best_pick")
                if not _gb or not _gb.get("pick"):
                    continue
                _t = _gb.get("type", "")
                _txt = str(_gb.get("pick"))
                if _t.startswith("Q1_") and not _txt.endswith(" Q1"):
                    _txt += " Q1"
                try:
                    if _bb_record_p(league, {
                        "date": g.get("date") or today,
                        "game_id": g.get("game_id"),
                        "matchup": f"{g.get('away_abbr')} @ {g.get('home_abbr')}",
                        "bet_type": _t, "pick": _txt,
                        "model_prob": _gb.get("prob"), "edge": _gb.get("edge"),
                        "odds": _gb.get("odds"), "stake_units": 0, "is_shadow": 1,
                    }):
                        _paper_n += 1
                except Exception:
                    pass
                continue
            for _plist in (g.get("paper_picks") or [], g.get("paper_picks_q1") or []):
                if not _plist:
                    continue
                _pp = max(_plist, key=lambda p: p.get("edge", -99))
                _t = _pp.get("type", "")
                _isq1 = _t.startswith("Q1_")
                _base = _t[3:] if _isq1 else _t
                _pk, _ln = _pp.get("pick"), _pp.get("line")
                if _pk is None:
                    continue  # no side -> skip (kills "None None"), Austin 2026-09-19
                if _base == "ML":
                    _txt = f"{_pk}"
                elif _base == "TOTAL":
                    if _ln is None:
                        continue
                    _txt = f"{_pk} {_ln}"
                else:
                    try:
                        _txt = f"{_pk} {float(_ln):+g}"
                    except (TypeError, ValueError):
                        continue
                if _isq1:
                    _txt += " Q1"
                try:
                    if _bb_record_p(league, {
                        "date": g.get("date") or today,
                        "game_id": g.get("game_id"),
                        "matchup": f"{g.get('away_abbr')} @ {g.get('home_abbr')}",
                        "bet_type": _t, "pick": _txt,
                        "model_prob": _pp.get("prob"), "edge": _pp.get("edge"),
                        "odds": _pp.get("odds"), "stake_units": 0, "is_shadow": 1,
                    }):
                        _paper_n += 1
                except Exception:
                    pass
        if _paper_n:
            logger.info("basketball PAPER (api hit) %s: recorded=%d shadow", league, _paper_n)
    except Exception as e:
        logger.warning("basketball paper piggyback %s failed: %s", league, e)

    # Mark which slate card holds the active POTD so the frontend can
    # render a star badge. For far-east leagues the POTD is locked under
    # the GAME's date (which may be tomorrow ET), so check today AND
    # tomorrow when looking up the active row.
    # When no POTD exists yet, lazily create one from the slate's bets
    # so the star appears the first time the slate is viewed — without
    # this, only leagues whose /potd endpoint had been hit show the star.
    try:
        from engine.pick_of_day import get_or_create_potd as _potd_lock
        active_potd = _read_active_potd(league, real_today)
        if not active_potd:
            bets, game_date_by_id = _build_potd_bets(games, real_today)
            if bets:
                lock_date = _resolve_potd_lock_date(
                    league, real_today, bets, game_date_by_id)
                active_potd = _potd_lock(league, bets, date=lock_date, view="q1")
        if active_potd and active_potd.get("game_id"):
            potd_gid = str(active_potd["game_id"])
            for g in games:
                if str(g.get("game_id")) == potd_gid:
                    g["is_potd"] = True
                    break
    except Exception as e:
        logger.debug("POTD slate-flag lookup %s failed: %s", league, e)

    return {
        "league": league,
        "display_name": cfg.get("display_name") or league,
        "date": today,
        "real_today": real_today,
        "fell_forward": fell_forward,
        "status": cfg.get("status"),
        # in season = a season month OR games on the board (Austin 2026-10-29: NCAAM showed
        # "Off-season" with HR already posting the openers in late October)
        "in_season": et_month() in (cfg.get("season_months") or ()) or bool(games),
        "games": games,
    }


# -- basketball unified tracker (MintyBot 2026-10-22, NBA "tracker merge" -> basketball) --
# ONE per-league tracker = the card's single BEST per game = the merged CALIBRATED
# best across all game markets (Full Game core + Q1), GATED like NBA/NHL: core
# full-game always eligible; an EXTRA (Q1) market may only HEADLINE once it has
# EARNED a track record (>=60 settled / >=30 days, calibration_gate.headline_eligible)
# AND clears the +EV floor. PAPER / display only -- reads the league's is_shadow=1
# rows in its picks table; NEVER the real (is_shadow=0) picks. League-scoped: WNBA
# + every framework basketball league (NBA keeps its own /api/nba/tracker/unified).
# cal_edge is keyed on the LEAGUE (wnba/ncaam/...), matching _paper_potd.record_paper_potd.

_BB_EV_CACHE = {}  # {league: {"ts": float, "map": {bet_type: (n, days)}}}

def _basketball_extra_evidence_map(conn, league):
    """{bet_type: (settled_n, days_tracked)} for the EXTRA (Q1) markets in this
    league's picks table (is_shadow=1 only). Cached 300s per league so the gate
    costs one GROUP BY per window. Mirror of _nba_extra_evidence_map."""
    import time as _t
    from engine.basketball._db import picks_table
    c = _BB_EV_CACHE.get(league)
    if c and c["map"] and (_t.monotonic() - c["ts"]) < 300:
        return c["map"]
    tbl = picks_table(league)
    m = {}
    try:
        for row in conn.execute(
                f"SELECT bet_type, COUNT(*) n, MIN(date) mn, MAX(date) mx FROM {tbl} "
                f"WHERE COALESCE(is_shadow,0)=1 AND result IS NOT NULL AND result != 'V' "
                f"GROUP BY bet_type"):
            n = (row["n"] or 0)
            days = 0
            if row["mn"] and row["mx"]:
                try:
                    days = int((conn.execute("SELECT julianday(?)-julianday(?)",
                                (row["mx"], row["mn"])).fetchone()[0]) or 0)
                except Exception:
                    days = 0
            m[row["bet_type"]] = (n, days)
    except Exception:
        return {}
    _BB_EV_CACHE[league] = {"ts": _t.monotonic(), "map": m}
    return m


def _basketball_market_scope(bet_type, pick):
    """Market bucket for the tracker chip, keyed on the BET: Full Game
    (ML/SPREAD/TOTAL/ALT) vs Q1 (Q1_ML/Q1_SPREAD/Q1_TOTAL). Mirror of
    _nba_market_scope (basketball framework has no Q1 derivative/team-total store)."""
    bt = (bet_type or "").strip().upper()
    if bt.startswith("Q1_") or bt.startswith("Q1 ") or bt == "Q1":
        return "Q1"
    return "Full Game"


def _basketball_flat_profit(result, odds):
    """Flat $100-stake P/L from (result, American odds) -- the standard settle
    convention (win: +odds if positive else 100/abs(odds)*100; loss: -$100;
    push/void: $0). Paper rows record stake_units~=0, so profit*stake_units would
    read a useless $0 -- grade every row flat $100 instead. Mirror of
    _nba_flat_profit (Austin 2026-10-22)."""
    r = (result or "").strip().upper()
    if not r:
        return None  # pending -- excluded from the P/L sum
    if r == "W":
        try:
            o = float(odds if odds is not None else -110)
        except (TypeError, ValueError):
            o = -110.0
        if o == 0:
            return 0.0
        return round(o, 2) if o > 0 else round(100.0 / abs(o) * 100.0, 2)
    if r == "L":
        return -100.0
    return 0.0  # P / PUSH / V


_LAST_BB_AUTO_SETTLE = {}  # {league: monotonic ts}
def _maybe_auto_settle_basketball(league):
    """Throttled, non-blocking settle so a league's paper (is_shadow=1) picks
    grade shortly after games go final. settle_picks(league) grades BOTH real and
    shadow rows in the one picks table. Fires on a tracker view, at most once per
    10 min/league, in a daemon thread. get_conn is thread-local so the daemon gets
    its own connection. Mirror of _maybe_auto_settle_nba."""
    import time as _t, threading as _th
    now = _t.monotonic()
    if now - _LAST_BB_AUTO_SETTLE.get(league, 0.0) < 600.0:
        return
    _LAST_BB_AUTO_SETTLE[league] = now
    def _run():
        try:
            from engine.basketball import settle_picks as _sp
            _sp(league)
        except Exception as _e:
            logger.warning("auto-settle basketball %s failed: %s", league, _e)
    _th.Thread(target=_run, daemon=True).start()


def _basketball_merged_recorded_best(league, game_id, date=None, conn=None, ev_map=None):
    """The game's single BEST recorded PAPER pick = the merged CALIBRATED best
    across all game markets (Full Game core + Q1), GATED: core full-game always
    eligible; an EXTRA (Q1) market only once it has EARNED a track record
    (headline_eligible) AND clears the +EV floor. Reads the league's is_shadow=1
    picks ONLY; NEVER the real (is_shadow=0) picks. Chip-shaped + flat-$100 profit
    + _scope. Pass conn/ev_map to share them across many games (the tracker).
    Mirror of _nba_merged_recorded_best; cal_edge keyed on the league."""
    import sqlite3 as _sq
    from engine.basketball._db import get_conn, picks_table
    from engine.calibration_gate import cal_edge as _ce, headline_eligible as _he
    from engine.selection import edge_proven as _edge_proven  # Pick Authority gate: also trust proven extras (2026-10-22)
    if conn is None:
        try:
            conn = get_conn(league); conn.row_factory = _sq.Row
        except Exception:
            return None
    if ev_map is None:
        ev_map = _basketball_extra_evidence_map(conn, league)
    tbl = picks_table(league)
    dq = " AND date = ?" if date else ""
    args = (str(game_id), date) if date else (str(game_id),)
    cands = []
    try:
        # The ABS(edge)<=20 guard drops a blown-up calibration edge (mirror NBA/NHL).
        for r in conn.execute(
                f"SELECT bet_type,pick,odds,edge,model_prob,result,profit,stake_units,closing_odds "
                f"FROM {tbl} WHERE game_id=?" + dq + " "
                f"AND COALESCE(is_shadow,0)=1 AND (result IS NULL OR result != 'V') "
                f"AND ABS(COALESCE(edge,0))<=20.0", args):
            d = dict(r); d["_scope"] = _basketball_market_scope(d.get("bet_type"), d.get("pick"))
            d["_extra"] = (d["_scope"] != "Full Game"); cands.append(d)
    except Exception:
        return None
    if not cands:
        return None
    scored = []
    for p in cands:
        ce = _ce(league, p.get("bet_type"), p.get("model_prob"), p.get("odds"),
                 pick_text=p.get("pick"), raw_edge=p.get("edge"))
        n = days = None
        if p["_extra"]:
            n, days = ev_map.get(p["bet_type"], (0, 0))
        eligible = _he(ce, p["_extra"], n, days)
        if p["_extra"] and not eligible:
            # Also trust the Pick Authority gate (MintyBot 2026-10-22): an EXTRA (Q1)
            # market the per-sport evidence map still shows as thin (n<60) is surfaced
            # when edge_proven (picks_unified -- the gate the CARDS use) already rates
            # its (sport, league, bet_type, condition) side PROVEN +EV (e.g. basketball/
            # wnba/Q1_TOTAL/over, n~70). Core full-game is untouched; -EV extras (NBA Q1)
            # stay hidden since edge_proven does not rate them eligible.
            try:
                _cond = _edge_proven._condition(p.get("pick"), p.get("bet_type"), p.get("odds"))
                if _edge_proven.is_eligible("basketball", league, p.get("bet_type"), _cond):
                    eligible = True
            except Exception:
                pass
        scored.append((eligible, ce if ce is not None else -999.0, p))
    pool = [sc for sc in scored if sc[0]]
    if not pool:
        return None
    b = max(pool, key=lambda sc: sc[1])[2]
    return {"type": b["bet_type"], "market": b["bet_type"], "pick": b["pick"],
            "odds": b["odds"], "edge": b["edge"], "prob": b["model_prob"],
            "result": b["result"], "paper": True, "_scope": b["_scope"],
            "closing_odds": b.get("closing_odds"),
            "profit": _basketball_flat_profit(b["result"], b["odds"]),
            "stake_units": 1.0}


def _basketball_recorded_authority_best(league, game_id, date=None, conn=None):
    """The game's RECORDED AUTHORITY pick = EXACTLY what the card headlined, read back from the
    league's is_shadow=1 rows (Austin 2026-11-01 "one pick, read everywhere"; mirror of
    _nba_recorded_authority_best). Shared by the card hold (_compute_api_basketball_today) and
    /api/basketball/{league}/tracker/unified, so the two can never disagree.

    What the recorder writes (the /today paper piggyback -> engine.basketball._tracker.record_pick,
    is_shadow=1, deduped per game+market family, settled rows never re-written):
      * Pick Authority leagues (ncaam / ncaaw / australia_nbl / china_cba): the card's `best_pick`
        (the select_pick headline) ONLY, one row per game. The card holds the first row it recorded,
        so later refreshes re-record the same pick (no-op) -> the FIRST recorded non-void row IS the
        headline. (Rows from before 2026-10-29 are the old per-view leans; the first one still wins.)
      * other leagues (WNBA): the per-view max-edge paper lean (Full + Q1) -> same rule as NBA:
        best Full row + best Q1 row, max by edge x prob.
    NO cal_edge / headline_eligible regate and NO edge guard (that re-derivation picked a DIFFERENT
    row than the card headlined). NEVER the real (is_shadow=0) rows. Chip-shaped + flat-$100 profit
    + _scope, the row shape the tracker already used."""
    import sqlite3 as _sq
    from engine.basketball._config import get_league_config as _glc
    from engine.basketball._db import get_conn, picks_table
    if conn is None:
        try:
            conn = get_conn(league); conn.row_factory = _sq.Row
        except Exception:
            return None
    if not game_id:
        return None
    tbl = picks_table(league)
    dq = " AND date = ?" if date else ""
    args = (str(game_id), date) if date else (str(game_id),)
    rows = []
    try:
        for r in conn.execute(
                f"SELECT id,bet_type,pick,odds,edge,model_prob,result,closing_odds "
                f"FROM {tbl} WHERE game_id=?" + dq + " "
                f"AND COALESCE(is_shadow,0)=1 AND (result IS NULL OR result != 'V') ORDER BY id", args):
            d = dict(r); d["_scope"] = _basketball_market_scope(d.get("bet_type"), d.get("pick"))
            rows.append(d)
    except Exception:
        return None
    if not rows:
        return None
    try:
        _pa = bool(_glc(league).get("pick_authority"))
    except Exception:
        _pa = False
    if _pa:
        b = rows[0]   # first recorded headline wins
    else:
        def _epr(p):
            return float(p.get("edge") or 0) * float(p.get("model_prob") or 0)
        full = [r for r in rows if r["_scope"] == "Full Game"]
        q1 = [r for r in rows if r["_scope"] == "Q1"]
        mains = [x for x in ((max(full, key=_epr) if full else None),
                             (max(q1, key=_epr) if q1 else None)) if x]
        if not mains:
            return None
        b = max(mains, key=_epr)
    return {"type": b["bet_type"], "market": b["bet_type"], "pick": b["pick"],
            "odds": b["odds"], "edge": b["edge"], "prob": b["model_prob"],
            "result": b["result"], "paper": True, "_scope": b["_scope"],
            "closing_odds": b.get("closing_odds"),
            "profit": _basketball_flat_profit(b["result"], b["odds"]),
            "stake_units": 1.0}


@router.get("/api/basketball/{league}/tracker/unified")
def api_basketball_tracker_unified(league: str, limit: int = 300):
    """ONE basketball tracker (per league) = the card's 'Best' per game (the merged
    CALIBRATED best across Full Game + Q1). Mirrors the NBA/NHL tracker merge. Reads
    the league's PAPER (is_shadow=1) picks ONLY; NEVER the real picks. One row per
    game (the merged best), via _basketball_merged_recorded_best; P/L graded FLAT
    $100. Read-only; per-market endpoints untouched. NBA keeps its own route."""
    from engine.basketball import LEAGUE_REGISTRY
    empty = {"rows": [], "summary": {"all": {"n": 0, "wins": 0, "losses": 0,
             "pushes": 0, "pending": 0, "profit": 0.0, "win_pct": 0.0}}, "by_market": {}}
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}", **empty}
    if league == "nba":
        return {"error": "NBA uses its own tracker route (/api/nba/tracker/unified)", **empty}
    _maybe_auto_settle_basketball(league)
    import sqlite3 as _sq
    from engine.basketball._db import get_conn, picks_table
    conn = get_conn(league); conn.row_factory = _sq.Row
    tbl = picks_table(league)
    try:
        gids = [r[0] for r in conn.execute(
            f"SELECT game_id, MAX(created_at) c FROM {tbl} "
            f"WHERE COALESCE(is_shadow,0)=1 GROUP BY game_id ORDER BY c DESC LIMIT ?", (limit,))]
    except Exception as e:
        logger.warning("basketball %s unified tracker game list failed: %s", league, e)
        gids = []
    evm = _basketball_extra_evidence_map(conn, league)
    from engine.basketball._db import games_table as _gt
    def _logos(gid):   # team logos for the tracker's matchup cell (Austin 2026-10-29: "missing logos in tracker")
        try:
            r = conn.execute(f"SELECT ht.logo_url AS h, at.logo_url AS a FROM {_gt(league)} g "
                             f"LEFT JOIN teams ht ON ht.id = g.home_team_id LEFT JOIN teams at ON at.id = g.away_team_id "
                             f"WHERE g.game_id = ?", (str(gid),)).fetchone()
            return (r["h"], r["a"]) if r else (None, None)
        except Exception:
            return (None, None)
    rows = []
    for gid in gids:
        best = _basketball_recorded_authority_best(league, gid, conn=conn)  # == the card's held headline (2026-11-01)
        if not best:
            continue
        meta = conn.execute(f"SELECT matchup,date FROM {tbl} WHERE game_id=? AND COALESCE(is_shadow,0)=1 LIMIT 1", (str(gid),)).fetchone()
        _hl, _al = _logos(gid)
        rows.append({
            "game_id": gid,
            "date": meta["date"] if meta else None,
            "matchup": meta["matchup"] if meta else None,
            "market": best.get("_scope"),
            "bet_type": best["type"], "pick": best["pick"], "odds": best["odds"],
            "edge": best["edge"], "result": best["result"], "profit": best.get("profit"),
            "model_prob": best.get("prob"), "closing_odds": best.get("closing_odds"),
            "stake_units": best.get("stake_units"),
            "home_logo": _hl, "away_logo": _al,
        })

    def _sum(rs):
        w = sum(1 for r in rs if r["result"] == "W")
        l = sum(1 for r in rs if r["result"] == "L")
        pu = sum(1 for r in rs if r["result"] in ("P", "PUSH"))
        pend = sum(1 for r in rs if not r["result"])
        prof = round(sum((r["profit"] or 0) for r in rs if r["result"]), 2)
        dec = w + l
        return {"n": len(rs), "wins": w, "losses": l, "pushes": pu, "pending": pend,
                "profit": prof, "win_pct": round(100.0 * w / dec, 1) if dec else 0.0}

    by_market = {}
    for r in rows:
        by_market.setdefault(r["market"] or "?", []).append(r)
    return {
        "rows": rows,
        "summary": {"all": _sum(rows)},
        "by_market": {m: _sum(rs) for m, rs in by_market.items()},
    }


@router.get("/api/basketball/{league}/tracker/history")
def api_basketball_tracker_history(league: str, limit: int = 200) -> dict:
    """Pending + settled picks for ``league`` plus the summary shape
    PickHistory expects (overall + per-bet-type tiles). Settles any
    newly-final games before returning so the user sees fresh results
    without clicking Settle."""
    from engine.basketball import (
        LEAGUE_REGISTRY, list_history, settle_picks,
    )
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA uses its existing tracker route"}
    try:
        settle_picks(league)
    except Exception as e:
        logger.debug("settle %s failed: %s", league, e)
    # Real picks only for the history / Active-Picks list — paper (is_shadow=1)
    # renders in its own Paper Picks section, never mixed in. Austin 2026-09-19.
    rows = list_history(league, limit=limit, real_only=True)
    rows_out = []
    for r in rows:
        d = dict(r) if not isinstance(r, dict) else dict(r)
        stake_u = 1.0 if d.get("is_shadow") else (d["stake_units"] if d.get("stake_units") is not None else 1.0)
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        rows_out.append(d)

    # Build the summary shape PickHistory consumes — mirrors what
    # tennis/MLB/NHL routes already return so the frontend renders
    # identically across sports. Summary scans the FULL picks table
    # (not the LIMIT-capped history slice) so the hero P/L reflects
    # every settled pick on file. Stake-weighted profit/ROI: each row
    # contributes profit*stake_units; ROI denom is sum(stake_units).
    from engine.basketball._db import get_conn as _bb_conn
    conn = _bb_conn(league)
    all_picks = conn.execute(
        "SELECT bet_type, result, profit, stake_units FROM picks WHERE is_shadow = 0"
    ).fetchall()
    wins = losses = pushes = pending = 0
    profit_total = 0.0
    stake_settled = 0.0
    by_type: dict[str, dict] = {}
    for r in all_picks:
        bt = r["bet_type"] or "?"
        b = by_type.setdefault(bt, {"total": 0, "wins": 0, "losses": 0,
                                      "pushes": 0, "pending": 0,
                                      "profit": 0.0, "win_pct": 0.0,
                                      "roi": 0.0, "stake_settled": 0.0})
        b["total"] += 1
        res = r["result"]
        stake_u = float(r["stake_units"] if r["stake_units"] is not None else 1.0)
        pr = float(r["profit"] or 0) * stake_u
        if res == "W":
            b["wins"] += 1; wins += 1
            b["profit"] += pr; profit_total += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        elif res == "L":
            b["losses"] += 1; losses += 1
            b["profit"] += pr; profit_total += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        elif res == "P":
            b["pushes"] += 1; pushes += 1
        else:
            b["pending"] += 1; pending += 1
    decided = wins + losses
    for bt, b in by_type.items():
        d = b["wins"] + b["losses"]
        st = b.pop("stake_settled", 0)
        b["win_pct"] = round(b["wins"] / d * 100, 1) if d else 0.0
        b["roi"] = round(b["profit"] / st, 1) if st else 0.0
        b["profit"] = round(b["profit"], 2)

    summary = {
        "overall": {
            "total": wins + losses + pushes,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "pending": pending,
            "profit": round(profit_total, 2),
            "win_pct": round(wins / decided * 100, 1) if decided else 0.0,
            "roi": round(profit_total / stake_settled, 1) if stake_settled else 0.0,
            "avg_clv": None,
            "clv_sample": 0,
        },
        "by_type": by_type,
    }

    # PAPER (shadow) aggregate — the proof-of-edge record for the model's best paper leans.
    # Tracked in the SAME table under is_shadow=1 but NEVER counted in the real summary above.
    # ROI on a flat 1-unit ($100) basis (paper is unstaked); CLV = implied(close) - implied(entry).
    def _imp(o):
        o = float(o)
        return (-o) / (-o + 100) if o < 0 else 100 / (o + 100)
    paper_rows = conn.execute(
        "SELECT bet_type, result, profit, odds, closing_odds FROM picks WHERE is_shadow = 1"
    ).fetchall()
    # Split by market family so the panel can follow the FULL GAME / Q1 toggle — Full and Q1
    # shadow track separately. full = ML/SPREAD/TOTAL, q1 = Q1_*.
    def _blank():
        return {"wins": 0, "losses": 0, "pushes": 0, "pending": 0, "profit": 0.0, "clv_sum": 0.0, "clv_n": 0}
    _acc = {"overall": _blank(), "full": _blank(), "q1": _blank()}
    p_by_type: dict[str, dict] = {}
    for r in paper_rows:
        bt = r["bet_type"] or "?"
        fam = "q1" if bt.upper().startswith("Q1_") else "full"
        res = r["result"]
        pr = float(r["profit"] or 0)
        pb = p_by_type.setdefault(bt, {"total": 0, "wins": 0, "losses": 0, "pushes": 0, "pending": 0, "profit": 0.0})
        pb["total"] += 1
        has_clv = r["closing_odds"] is not None and r["odds"] is not None
        for key in ("overall", fam):
            a = _acc[key]
            if res == "W":
                a["wins"] += 1; a["profit"] += pr
            elif res == "L":
                a["losses"] += 1; a["profit"] += pr
            elif res == "P":
                a["pushes"] += 1
            else:
                a["pending"] += 1
            if has_clv:
                try:
                    a["clv_sum"] += (_imp(r["closing_odds"]) - _imp(r["odds"])) * 100
                    a["clv_n"] += 1
                except (TypeError, ValueError, ZeroDivisionError):
                    pass
        if res == "W":
            pb["wins"] += 1; pb["profit"] += pr
        elif res == "L":
            pb["losses"] += 1; pb["profit"] += pr
        elif res == "P":
            pb["pushes"] += 1
        else:
            pb["pending"] += 1
    for bt, pb in p_by_type.items():
        pb["profit"] = round(pb["profit"], 2)

    def _fin(a):
        dec = a["wins"] + a["losses"]
        return {
            "total": a["wins"] + a["losses"] + a["pushes"],
            "wins": a["wins"], "losses": a["losses"], "pushes": a["pushes"], "pending": a["pending"],
            "profit": round(a["profit"], 2),
            "win_pct": round(a["wins"] / dec * 100, 1) if dec else 0.0,
            "roi": round(a["profit"] / dec, 1) if dec else 0.0,
            "avg_clv": round(a["clv_sum"] / a["clv_n"], 1) if a["clv_n"] else None,
            "clv_sample": a["clv_n"],
        }
    paper = _fin(_acc["overall"])
    paper["by_type"] = p_by_type
    paper["full"] = _fin(_acc["full"])
    paper["q1"] = _fin(_acc["q1"])
    # Paper display rows (Austin 2026-09-19): settled shadow picks as a list for
    # the tracker's "Paper Picks" panel (mirrors the football paper list).
    paper["rows"] = [dict(_pr) for _pr in conn.execute(
        "SELECT date, matchup, bet_type, pick, odds, edge, model_prob, result, "
        "profit, closing_odds FROM picks WHERE is_shadow = 1 AND result IS NOT NULL "
        "AND result != 'V' ORDER BY date DESC, id DESC LIMIT ?", (limit,)).fetchall()]
    return {"league": league, "rows": rows_out, "summary": summary, "paper": paper}


@router.post("/api/basketball/{league}/tracker/record")
def api_basketball_tracker_record(league: str) -> dict:
    """Walk today's slate, record any picks above the framework gates
    into the per-league picks table. Idempotent on
    (date, game_id, bet_type, pick) so re-running is safe."""
    from engine.basketball import LEAGUE_REGISTRY, record_pick
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA uses its existing tracker route"}
    slate = api_basketball_today(league)
    games = slate.get("games") or []
    out = {"recorded": 0, "skipped": 0, "errors": 0}
    # Q1_TRACKER_LEAGUES is module-level (shared with the /today piggyback
    # recorder) so the two record paths stay in lockstep: record BOTH
    # best_pick_full and best_pick_q1 for those leagues, else best_pick only.
    for g in games:
        if league in Q1_TRACKER_LEAGUES:
            candidates = [g.get("best_pick_full"), g.get("best_pick_full_runner_up"), g.get("best_pick_q1")]
        else:
            candidates = [g.get("best_pick")]
        for best in candidates:
            if not best:
                continue
            try:
                pid = record_pick(league, {
                    "date": g.get("date"),
                    "game_id": g.get("game_id"),
                    "matchup": f"{g.get('away_abbr')} @ {g.get('home_abbr')}",
                    "bet_type": best.get("type"),
                    "pick": best.get("pick"),
                    "model_prob": best.get("prob"),
                    "edge": best.get("edge"),
                    "odds": best.get("odds"),
                    "stake_units": best.get("stake_units"),
                })
                if pid:
                    out["recorded"] += 1
                else:
                    out["skipped"] += 1
            except Exception as e:
                logger.warning("record %s/%s failed: %s",
                               league, g.get("game_id"), e)
                out["errors"] += 1
    return out


@router.post("/api/basketball/{league}/tracker/settle")
def api_basketball_tracker_settle(league: str) -> dict:
    """Manual settle trigger for ``league``."""
    from engine.basketball import LEAGUE_REGISTRY, settle_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA uses its existing tracker route"}
    return settle_picks(league)


@router.get("/api/basketball/{league}/standings")
def api_basketball_standings(league: str) -> list[dict]:
    """NBA-shape standings: W-L record, PCT, GB, PF/PA/Diff per game,
    L10, streak, conference/division grouping when present.
    Mirrors /api/nba/standings so the same StandingsView can render
    basketball-framework leagues without per-league branching."""
    from engine.basketball import LEAGUE_REGISTRY
    from engine.basketball._db import get_conn, games_table, teams_table
    if league not in LEAGUE_REGISTRY:
        return []
    if league == "nba":
        return []
    conn = get_conn(league)
    g_tbl = games_table(league)
    t_tbl = teams_table(league)
    season = _current_season(league)

    # Defensive SELECT — basketball-framework team tables don't all
    # have division/conference columns (international leagues skip them).
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({t_tbl})").fetchall()}
    div_col = "COALESCE(division, '')" if "division" in cols else "''"
    conf_col = "COALESCE(conference, '')" if "conference" in cols else "''"
    logo_col = "logo_url" if "logo_url" in cols else "''"
    teams_map = {r["id"]: dict(r) for r in conn.execute(
        f"SELECT id, abbreviation, name, "
        f"       {div_col} AS division, {conf_col} AS conference, "
        f"       {logo_col} AS logo_url "
        f"FROM {t_tbl}"
    ).fetchall()}

    # game_id is the basketball-framework PK (vs hockey's `id`).
    games = conn.execute(
        f"SELECT game_id, date, home_team_id, away_team_id, home_score, "
        f"       away_score "
        f"FROM {g_tbl} "
        f"WHERE status = 'final' AND season = ? "
        f"  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        f"ORDER BY date ASC, game_id ASC",
        (season,),
    ).fetchall()

    by_team: dict[int, list] = {}
    for g in games:
        for tid in (g["home_team_id"], g["away_team_id"]):
            by_team.setdefault(tid, []).append(g)

    out = []
    for tid, meta in teams_map.items():
        team_games = by_team.get(tid, [])
        wins = losses = 0
        pf = pa = 0
        results: list[str] = []
        for g in team_games:
            is_home = g["home_team_id"] == tid
            scored = g["home_score"] if is_home else g["away_score"]
            allowed = g["away_score"] if is_home else g["home_score"]
            pf += scored; pa += allowed
            if scored > allowed:
                wins += 1; results.append("W")
            elif scored < allowed:
                losses += 1; results.append("L")
        gp = wins + losses
        pct = round(wins / gp, 3) if gp else 0.0
        last10 = results[-10:]
        l10w = sum(1 for r in last10 if r == "W")
        l10l = sum(1 for r in last10 if r == "L")
        streak = ""
        if results:
            kind = results[-1]
            n = 0
            for r in reversed(results):
                if r != kind:
                    break
                n += 1
            streak = f"{kind}{n}"
        out.append({
            "team_id": tid,
            "abbreviation": meta.get("abbreviation") or str(tid),
            "name": meta.get("name") or "",
            "logo": meta.get("logo_url") or "",
            "division": meta.get("division") or "",
            "conference": meta.get("conference") or "",
            "wins": wins, "losses": losses,
            "games_played": gp,
            "win_pct": pct,
            "points_for": pf, "points_against": pa,
            "ppg": round(pf / gp, 1) if gp else 0.0,
            "papg": round(pa / gp, 1) if gp else 0.0,
            "diff": pf - pa,
            "l10": f"{l10w}-{l10l}",
            "streak": streak,
        })

    out.sort(key=lambda x: (-x["win_pct"], -x["wins"], x["abbreviation"]))
    # Compute games-back relative to the leader within each
    # conference/division group (NBA convention: GB = ((leader_wins -
    # team_wins) + (team_losses - leader_losses)) / 2). When there's no
    # grouping (international leagues), GB is computed against the
    # overall leader.
    def _set_gb(group: list[dict]):
        if not group: return
        leader = group[0]
        for t in group:
            gb = ((leader["wins"] - t["wins"]) + (t["losses"] - leader["losses"])) / 2
            t["gb"] = "-" if gb == 0 else f"{gb:.1f}"
    grouped: dict[str, list] = {}
    for t in out:
        k = t["division"] or t["conference"] or "_"
        grouped.setdefault(k, []).append(t)
    for g in grouped.values():
        _set_gb(g)
    return out


@router.get("/api/basketball/{league}/calibration")
def api_basketball_calibration(league: str) -> dict:
    """Surface the league's walk-forward calibration table (B4 output)."""
    from engine.basketball import LEAGUE_REGISTRY
    import json
    from pathlib import Path
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA calibration uses /api/calibration/nba"}
    path = Path("data/basketball") / f"{league}_calibration.json"
    if not path.exists():
        return {"league": league, "method": "none",
                "buckets": {}, "n_processed": 0,
                "deferred": ["No walk-forward seed run for this league yet"]}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        return {"error": f"calibration load failed: {e}"}


def _current_season(league: str) -> int:
    # The league's DB season label (college labels by END year: 2026-27 = 2027).
    from engine.basketball._config import season_label
    return season_label(league, et_now())


@router.get("/api/basketball/{league}/potd")
def api_basketball_potd(league: str) -> dict:
    """Pick of the Day for a basketball framework league.

    Locks at first creation per (league, date). Reads the day's slate,
    selects the highest-conviction non-skip pick, and persists. NBA
    falls through to its existing /api/pick-of-day/nba endpoint.
    """
    from datetime import datetime
    from engine.basketball import LEAGUE_REGISTRY
    from engine.pick_of_day import (
        get_or_create_potd, get_today_potd,
    )
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA POTD lives at /api/pick-of-day/nba"}

    today = et_today_str()
    # Reuse the slate route to assemble (game, prediction, picks).
    slate = api_basketball_today(league)
    games = slate.get("games") or []

    # Read first; today + tomorrow-fallback for far-east leagues. The
    # lock-date and bet-shape helpers are shared with the slate route's
    # lazy-create so the two paths can't drift.
    potd = _read_active_potd(league, today)
    if potd:
        return potd

    bets, game_date_by_id = _build_potd_bets(games, today)
    if not bets:
        return {"message": "No slate available", "sport": league}
    lock_date = _resolve_potd_lock_date(league, today, bets, game_date_by_id)
    potd = get_or_create_potd(league, bets, date=lock_date, view="q1",
                              db_sport="basketball", league=league)
    return potd or {"message": "No qualifying picks today", "sport": league}


@router.get("/api/basketball/{league}/potd/summary")
def api_basketball_potd_summary(league: str) -> dict:
    """POTD running totals for the league."""
    from engine.basketball import LEAGUE_REGISTRY
    from engine.pick_of_day import get_potd_summary, settle_potd
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA POTD lives at /api/pick-of-day/nba/summary"}
    try:
        settle_potd(league)
    except Exception as e:
        logger.debug("POTD auto-settle failed for %s: %s", league, e)
    return get_potd_summary(league)


@router.get("/api/basketball/{league}/paper-potd")
def api_basketball_paper_potd(league: str) -> dict:
    """Game-day-locked PAPER Pick-of-the-Day per view (full + Q1). Mirrors the
    NHL/NBA paper POTD: the day's highest-edge is_shadow=1 lean, FROZEN once the
    day arrives (record_paper_potd is idempotent per date+scope). Feeds the
    PaperPickOfDayHero's `locked` + `summary` so the hero shows a STABLE day pick
    instead of recomputing live. Austin 2026-10-03 (paper-only POTD)."""
    from engine.basketball import LEAGUE_REGISTRY
    from engine.basketball._paper_potd import (
        record_paper_potd, get_paper_potd, paper_potd_summary,
    )
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA paper POTD lives at /api/nba/tracker/paper"}
    out = {}
    for _sc in ("full", "q1"):
        try:
            record_paper_potd(league, scope=_sc)  # idempotent game-day lock
        except Exception as e:
            logger.debug("bball paper POTD lock (%s/%s) failed: %s", league, _sc, e)
        try:
            out[_sc] = {"potd": get_paper_potd(league, scope=_sc),
                        "potd_record": paper_potd_summary(league, scope=_sc)}
        except Exception as e:
            logger.debug("bball paper POTD read (%s/%s) failed: %s", league, _sc, e)
            out[_sc] = {"potd": None, "potd_record": {}}
    return out


@router.get("/api/basketball/leagues")
def api_basketball_leagues() -> dict:
    """Return every basketball league the framework knows about, with
    per-league metadata the frontend nav needs (display name, country,
    region grouping, season months, current in-season flag, status,
    and today's game count for the sidebar badges).

    Response shape::

        {
          "leagues": [
            {"key": "nba", "display_name": "NBA", "country": "USA",
             "region": "USA", "in_season": true, "status": "active",
             "data_source": "espn", "game_count_today": 4},
            ...
          ],
          "regions": ["USA", "International", "Europe",
                      "Americas", "Asia/Oceania"],
        }
    """
    from engine.basketball import LEAGUE_REGISTRY
    from engine.basketball._db import get_conn, games_table
    today = et_now()
    today_str = today.strftime("%Y-%m-%d")
    leagues = []
    for key, cfg in LEAGUE_REGISTRY.items():
        months = cfg.get("season_months") or ()
        # Per-league count for the sidebar badge — matches what the
        # /today route would actually render (today's games when
        # present; otherwise the next slate the panel falls forward
        # to). Avoids the "0 today" badge on a league that's about
        # to play later this week.
        try:
            conn = get_conn(key)
            tbl = games_table(key)
            n_today = conn.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE date = ?", (today_str,),
            ).fetchone()[0]
            if n_today == 0:
                # Tomorrow only — matches the slate route's +1-day
                # fallforward. Earlier this looked 14 days ahead and
                # the badge would advertise "3 games" for a league the
                # user clicked into and saw an empty slate.
                nxt = conn.execute(
                    f"SELECT date, COUNT(*) FROM {tbl} "
                    f"WHERE date > ? AND date <= date(?, '+1 days') "
                    f"GROUP BY date ORDER BY date LIMIT 1",
                    (today_str, today_str),
                ).fetchone()
                n_today = int(nxt[1]) if nxt else 0
            # Live fallback for ESPN-tracked leagues when the DB is
            # stale. NBA scoreboard occasionally drifts behind ESPN
            # because the worker poll runs on a cadence; without this
            # the sidebar shows '0' even though ESPN says there are
            # games today.
            if n_today == 0 and cfg.get("data_source") == "espn":
                live_n = _live_espn_count(cfg.get("espn_league_path"),
                                            today_str)
                if live_n is not None:
                    n_today = live_n
            # HR-event fallback for RealGM / SofaScore leagues. RealGM
            # only ships RESULTS — no future schedule — so the DB
            # never knows a game is coming up. HR's competition tree
            # carries `numEvents` (count of upcoming events) per
            # competition. Use it when DB shows 0 and we have an
            # hr_comp_id wired. Bulgaria/Czech NBL hit this path —
            # season finalized 5-15, playoffs scheduled on HR but not
            # in the RealGM-sourced DB.
            if n_today == 0 and cfg.get("hr_comp_id"):
                hr_n = _live_hr_basketball_count(cfg.get("hr_comp_id"))
                if hr_n is not None:
                    n_today = hr_n
        except Exception:
            n_today = 0
        leagues.append({
            "key": key,
            "display_name": cfg.get("display_name") or key,
            "country": cfg.get("country") or "Unknown",
            "region": _region_for(cfg.get("country") or ""),
            "in_season": today.month in months or int(n_today) > 0,   # games on the board count (2026-10-29)
            "status": cfg.get("status"),
            "data_source": cfg.get("data_source"),
            "hr_comp_name": cfg.get("hr_comp_name"),
            "game_count_today": int(n_today),
        })
    return {
        "leagues": leagues,
        "regions": ["USA", "International", "Europe",
                    "Americas", "Asia/Oceania"],
    }


def _live_espn_count(espn_path: str | None, date_str: str) -> int | None:
    """Count games on ESPN's scoreboard for ``date_str``. Returns None
    on any failure (caller falls through to the DB count). Cached for
    60 seconds per (path, date) to keep the leagues endpoint snappy
    when the user clicks around quickly."""
    if not espn_path:
        return None
    return _swr_count(_live_espn_cache, (espn_path, date_str),
                      lambda: _fetch_espn_count(espn_path, date_str))


def _swr_count(cache: dict, key, fetch):
    """Sidebar badge counts never block the nav (speed pass 2026-10-28: the inline ESPN + HR
    fetches made /api/basketball/leagues take 3-5 s once a minute). Serve whatever is cached,
    refreshing in the background once it is 60 s old; with nothing cached yet return None so
    the caller uses its DB count for that one load."""
    import threading
    import time as _t
    cached = cache.get(key)
    if (cached is None or _t.time() - cached[0] >= 60) and key not in _swr_inflight:
        _swr_inflight.add(key)

        def _bg():
            try:
                cache[key] = (_t.time(), fetch())
            except Exception:
                cache[key] = (_t.time(), None)
            finally:
                _swr_inflight.discard(key)

        threading.Thread(target=_bg, daemon=True).start()
    return cached[1] if cached else None


_swr_inflight: set = set()


def _fetch_espn_count(espn_path: str, date_str: str) -> int | None:
    import urllib.request, json as _json
    try:
        url = (f"https://site.api.espn.com/apis/site/v2/sports/{espn_path}"
                f"/scoreboard?dates={date_str.replace('-','')}")
        req = urllib.request.Request(url, headers={
            "User-Agent": "SportsBettor/1.0 (leagues-count-fallback)",
        })
        with urllib.request.urlopen(req, timeout=4) as r:
            data = _json.loads(r.read())
        n = len(data.get("events") or [])
    except Exception:
        n = None
    return n


_live_espn_cache: dict[tuple[str, str], tuple[float, int | None]] = {}


def _live_hr_basketball_count(comp_id: str | None) -> int | None:
    """Sidebar-badge fallback for RealGM/SofaScore-sourced leagues whose
    DBs only hold results (no future schedule). HR's sports tree carries
    a ``numEvents`` counter per competition — use it when our DB shows
    0 upcoming and HR has events on its board. Cached 60s per comp_id."""
    if not comp_id:
        return None
    return _swr_count(_live_hr_bb_cache, comp_id, lambda: _fetch_hr_bb_count(comp_id))


def _fetch_hr_bb_count(comp_id: str) -> int | None:
    try:
        from scrapers.hardrock_odds import _fetch_sports_tree
        tree, _ = _fetch_sports_tree()
        sports = (tree.get("data", {}).get("betSync", {}).get("sports") or [])
        n: int | None = None
        for sp in sports:
            if sp.get("code") != "BASKETBALL":
                continue
            for cat in sp.get("categories") or []:
                for comp in cat.get("competitions") or []:
                    if str(comp.get("id") or "") == str(comp_id):
                        n = int(comp.get("numEvents") or 0)
                        break
                if n is not None:
                    break
            if n is not None:
                break
    except Exception:
        n = None
    return n


_live_hr_bb_cache: dict[str, tuple[float, int | None]] = {}


# Country → region grouping. Kept here (not in _config.py) because it's
# UI/UX taxonomy, not a model property — different frontends might
# regroup the same leagues differently.
_REGION_MAP = {
    # USA
    "USA": "USA",
    # International umbrella (multi-nation comps)
    "International": "International",
    # Europe
    "France": "Europe", "Lithuania": "Europe", "Israel": "Europe",
    "Greece": "Europe", "Czech Republic": "Europe", "Latvia": "Europe",
    "Bulgaria": "Europe", "Iceland": "Europe", "Germany": "Europe",
    "Hungary": "Europe", "Slovenia": "Europe", "Denmark": "Europe",
    "Finland": "Europe", "Slovakia": "Europe", "Sweden": "Europe",
    # Americas (non-USA)
    "Dominican Republic": "Americas", "Puerto Rico": "Americas",
    "Brazil": "Americas", "Argentina": "Americas",
    # Asia / Oceania
    "Japan": "Asia/Oceania", "South Korea": "Asia/Oceania",
    "Australia": "Asia/Oceania", "New Zealand": "Asia/Oceania",
    "China": "Asia/Oceania",
}


def _region_for(country: str) -> str:
    return _REGION_MAP.get(country, "Other")
