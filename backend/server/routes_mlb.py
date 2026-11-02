"""MLB API routes â€” extracted from backend/server/__init__.py.

Stage-2 of #317 (MLB monolith extraction). 17 of 19 MLB endpoints now
live here:
    /api/teams, /api/teams/{team_id}, /api/teams/{team_id}/profile
    /api/standings
    /api/pitcher/{pitcher_id}
    /api/scoreboard
    /api/tracker/history, /api/tracker/record, /api/tracker/settle,
        /api/tracker/summary
    /api/calibrate, /api/calibrate/teams
    /api/debug/odds, /api/debug/teams
    /api/backtest
    /api/mlb/refresh-lineups
    /api/accuracy

The two remaining (``/api/predict``, ``/api/best-bets``) stay in
__init__.py because each pulls on a chain of MLB-specific helpers
(_predict_mlb_full, _picks_store_*, _get_scoreboard, _enrich_games,
_resolve_abbr, _alt_abbr, _safe_game_pk, _parse_espn_scoreboard,
_compute_injury_impact_pct, _mlb_form_from_reasoning, _mlb_season_context,
_implied, _find_ou, _mc_seed, PredictRequest model). Migrating those
safely requires extracting the helpers first â€” separate sprint.

Helper access from this module follows the same lazy-proxy pattern
``routes_nba.py`` / ``routes_nhl.py`` use, so the shared helpers
don't need to move yet either.

Mounted from __init__.py via ``app.include_router(mlb_router)``.
"""
from __future__ import annotations

import logging

from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from engine.db import (
    get_conn, get_all_teams, get_team_by_id, get_team_record,
    get_pitcher_season, get_bullpen, get_recent_games,
)
# Hot-path imports â€” moved up from inside per-request handlers. The
# best-bets pipeline fans these out to 8 worker threads which each
# previously paid the import-table lookup cost (~50-80ms per cold req).
from engine.picks import (
    generate_picks, get_best_pick, match_odds, fetch_real_odds_for_games,
)

from ._tz import et_now, et_today_str
from ._bestbets import (
    _bb_progress_set, _bb_progress_increment, _bb_reset_on_exit,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _season() -> int:
    """Current MLB season year in ET. Recomputed each request so the
    January-1 boundary doesn't snap to last year on a UTC server."""
    return et_now().year


# â”€â”€ Lazy proxies for helpers that still live in __init__.py â”€â”€
def _lazy(name):
    def proxy(*args, **kwargs):
        from backend import server as _srv
        return getattr(_srv, name)(*args, **kwargs)
    proxy.__name__ = name
    return proxy


_get_scoreboard = _lazy("_get_scoreboard")
_pred_cache_clear_sport = _lazy("_pred_cache_clear_sport")
_predict_mlb_full = _lazy("_predict_mlb_full")
_picks_store_get = _lazy("_picks_store_get")
_picks_store_put = _lazy("_picks_store_put")
_odds_from_scoreboard_cache = _lazy("_odds_from_scoreboard_cache")
_get_recorded_pick = _lazy("_get_recorded_pick")
_is_game_locked = _lazy("_is_game_locked")
_is_game_imminent = _lazy("_is_game_imminent")
_mlb_season_context = _lazy("_mlb_season_context")
_compute_injury_impact_pct = _lazy("_compute_injury_impact_pct")
_mlb_form_from_reasoning = _lazy("_mlb_form_from_reasoning")


class _LazyConst:
    """Proxy for a constant collection that lives in __init__.py.

    Mirrors routes_nba.py's _LazyConst â€” bare-name lookups inside
    handler bodies need the symbol bound at module load, but the real
    constant only exists after __init__.py finishes initialising."""
    def __init__(self, name: str):
        object.__setattr__(self, "_name", name)
        object.__setattr__(self, "_resolved", None)

    def _get(self):
        if self._resolved is None:
            from backend import server as _srv
            object.__setattr__(self, "_resolved", getattr(_srv, self._name))
        return self._resolved

    def __contains__(self, item):
        return item in self._get()

    def __iter__(self):
        return iter(self._get())

    def __len__(self):
        return len(self._get())

    def __getattr__(self, attr):
        return getattr(self._get(), attr)


_MLB_DERIV_TYPES = _LazyConst("_MLB_DERIV_TYPES")


# â”€â”€ Teams â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _mlb_card_wp(wp, odds):
    """Card % = the Pick Authority's market blend (model + de-vigged ML; Austin 2026-10-26 "Any percentages
    for any card in any sport, should be that way"). Unchanged when there is no price."""
    try:
        from ._display_cal import blend_card_wp
        return blend_card_wp("mlb", "mlb", wp, (odds or {}).get("home_ml"), (odds or {}).get("away_ml"))
    except Exception:
        return wp


@router.get("/api/teams")
def api_teams():
    """Return all 30 MLB teams."""
    teams = get_all_teams()
    season = _season()
    result = []
    for t in teams:
        record = get_team_record(t["mlb_id"], season)
        result.append({
            "id": t["mlb_id"],
            "name": t["name"],
            "abbreviation": t["abbreviation"],
            "city": t.get("city", ""),
            "venue": t.get("venue", ""),
            "league": t.get("league", ""),
            "division": t.get("division", ""),
            "record": f"{record['wins']}-{record['losses']}" if record else "",
            "streak": record.get("streak", "") if record else "",
            "last_10": (
                f"{record.get('last_10_wins', 0)}-"
                f"{record.get('last_10_losses', 0)}"
            ) if record else "",
            "run_diff": record.get("run_diff", 0) if record else 0,
        })
    return result


@router.get("/api/teams/{team_id}")
def api_team_detail(team_id: int):
    """Return full team data with stats."""
    team = get_team_by_id(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")
    season = _season()
    record = get_team_record(team_id, season) or {}
    bp = get_bullpen(team_id, season) or {}
    recent = get_recent_games(team_id, 10)
    return {
        "team": team,
        "record": record,
        "bullpen": bp,
        "recent_games": recent,
    }


@router.get("/api/teams/{team_id}/profile")
def api_team_profile(team_id: int):
    """Full team profile with stats, recent games, and pitcher roster."""
    team = get_team_by_id(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")
    season = _season()
    record = get_team_record(team_id, season) or {}
    recent = get_recent_games(team_id, 15)
    bp = get_bullpen(team_id, season) or {}

    from engine.pit_stats import compute_team_stats_at_date
    today = et_today_str()
    pit = compute_team_stats_at_date(team_id, today, season)

    conn = get_conn()
    pitchers = conn.execute("""
        SELECT p.mlb_id, p.name, p.throws,
               ps.era, ps.whip, ps.k_per_9, ps.wins, ps.losses, ps.innings
        FROM players p
        LEFT JOIN pitcher_stats ps ON p.mlb_id = ps.player_id AND ps.season = ?
        WHERE p.team_id = ? AND p.position = 'P' AND p.active = 1
        ORDER BY ps.innings DESC
    """, (season, team_id)).fetchall()

    return {
        "team": team,
        "record": record,
        "pit_stats": pit,
        "bullpen": bp,
        "recent_games": recent,
        "pitchers": [dict(p) for p in pitchers],
    }


# â”€â”€ Standings â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@router.get("/api/standings")
def api_standings():
    """MLB standings grouped by division."""
    conn = get_conn()
    season = _season()

    team_count = conn.execute(
        "SELECT COUNT(*) as c FROM teams"
    ).fetchone()["c"]
    if team_count == 0:
        return []

    rows = conn.execute("""
        SELECT t.mlb_id, t.name, t.abbreviation, t.league, t.division,
               ts.wins, ts.losses, ts.run_diff, ts.streak,
               ts.last_10_wins, ts.last_10_losses,
               ts.home_wins, ts.home_losses, ts.away_wins, ts.away_losses,
               ts.era, ts.ops, ts.wrc_plus
        FROM teams t
        LEFT JOIN team_stats ts ON t.mlb_id = ts.team_id AND ts.season = ?
        ORDER BY t.league, t.division, ts.wins DESC
    """, (season,)).fetchall()
    if not rows:
        return []

    divisions: dict[str, dict] = {}
    for r in rows:
        league = (r["league"] or "").strip()
        division = (r["division"] or "").strip()
        if not league or not division:
            continue
        div_key = f"{league} {division}"
        if div_key not in divisions:
            divisions[div_key] = {
                "league": league,
                "division": division,
                "teams": [],
            }
        w = r["wins"] or 0
        l = r["losses"] or 0
        divisions[div_key]["teams"].append({
            "id": r["mlb_id"],
            "name": r["name"],
            "abbreviation": r["abbreviation"],
            "wins": w,
            "losses": l,
            "pct": f".{int(w / (w + l) * 1000):03d}" if (w + l) > 0 else ".000",
            "run_diff": r["run_diff"] or 0,
            "streak": r["streak"] or "",
            "last_10": f"{r['last_10_wins'] or 0}-{r['last_10_losses'] or 0}",
            "home": f"{r['home_wins'] or 0}-{r['home_losses'] or 0}",
            "away": f"{r['away_wins'] or 0}-{r['away_losses'] or 0}",
            "era": r["era"],
            "ops": r["ops"],
            "wrc_plus": r["wrc_plus"],
        })
    return list(divisions.values())


# â”€â”€ Pitcher â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@router.get("/api/pitcher/{pitcher_id}")
def api_pitcher(pitcher_id: int):
    """Pitcher stats + recent starts."""
    from engine.db import get_pitcher_recent_starts
    conn = get_conn()
    season = _season()
    player = conn.execute(
        "SELECT * FROM players WHERE mlb_id = ?", (pitcher_id,)
    ).fetchone()
    if not player:
        raise HTTPException(status_code=404, detail="Pitcher not found")
    stats = get_pitcher_season(pitcher_id, season)
    recent = get_pitcher_recent_starts(pitcher_id, 5)
    return {
        "player": dict(player),
        "stats": dict(stats) if stats else None,
        "recent_starts": recent,
    }


# â”€â”€ Scoreboard â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


# â”€â”€ PAPER best-pick (Austin 2026-09-18) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Every MLB card shows the single market the model likes best (ML / run line /
# total), tagged paper, never staked â€” same feature as the framework sports.
# MLB is root-mounted with no per-game today endpoint, so best-bets (which
# already predicts EVERY game) stashes the per-game best paper pick here and
# /api/scoreboard merges it onto each card by game id. Uses the MC model's own
# probabilities (win_prob, run_line covers, over_under sim table) vs HR's price.
_MLB_PAPER_BY_GAME: dict = {}
_MLB_1ST_PAPER_BY_GAME: dict = {}      # 1st-inning (NRFI/YRFI) paper, by game id
_MLB_DERIV_PAPER_BY_GAME: dict = {}    # best derivative (F5/team-total) paper, by game id


def _mlb_implied(o):
    try:
        o = float(o)
    except (TypeError, ValueError):
        return None
    return (-o) / (-o + 100) if o < 0 else 100 / (o + 100)


def _mlb_ou_prob(pred, line):
    """P(over) at `line`: prefer the MC sim's over_under table, else full/mc total + std via normal-CDF."""
    import math
    key = str(line)
    for src in (pred.get("over_under"), (pred.get("mc") or {}).get("over_under")):
        if isinstance(src, dict) and isinstance(src.get(key), dict) and src[key].get("over") is not None:
            return float(src[key]["over"])
    total = pred.get("total") or ((pred.get("mc") or {}).get("expected_runs") or {}).get("total")
    std = ((pred.get("mc") or {}).get("total_std")) or 4.0
    if total is None or not std:
        return None
    return 0.5 * (1 + math.erf((float(total) - float(line)) / (float(std) * math.sqrt(2))))


def _mlb_paper_picks(pred, odds, home_abbr, away_abbr):
    """Single best PAPER pick (highest edge across ML / run line / total) from the MC model's own
    probabilities vs HR's price. Returns one dict tagged paper (never staked), or None."""
    if not pred or not odds:
        return None
    out = []
    wp = pred.get("win_prob") or {}
    if wp.get("home") is not None and (odds.get("home_ml") is not None or odds.get("away_ml") is not None):
        if wp["home"] >= wp.get("away", 1 - wp["home"]):
            side, prob, o = (home_abbr or "HOME"), wp["home"], odds.get("home_ml")
        else:
            side, prob, o = (away_abbr or "AWAY"), wp.get("away"), odds.get("away_ml")
        imp = _mlb_implied(o)
        if o is not None and imp is not None and prob is not None:
            out.append(dict(type="ML", market="ML", pick=side, line=None, prob=round(prob, 4),
                            odds=o, edge=round((prob - imp) * 100, 1), paper=True))
    rl = pred.get("run_line") or {}
    hsp = odds.get("home_spread_point")
    if hsp is not None and rl:
        if float(hsp) < 0:   # home is the -1.5 favorite
            h_cover, a_cover = rl.get("home_minus_1_5"), rl.get("away_plus_1_5")
        else:                # home is the +1.5 dog
            h_cover, a_cover = rl.get("home_plus_1_5"), rl.get("away_minus_1_5")
        h_ln, a_ln = hsp, odds.get("away_spread_point")
        if h_cover is not None and a_cover is not None:
            if h_cover >= a_cover:
                side, prob, ln, o = (home_abbr or "HOME"), h_cover, h_ln, odds.get("home_spread_odds")
            else:
                side, prob, ln, o = (away_abbr or "AWAY"), a_cover, a_ln, odds.get("away_spread_odds")
            imp = _mlb_implied(o)
            if o is not None and imp is not None and ln is not None:
                out.append(dict(type="RUN_LINE", market="RUN_LINE", pick=side, line=ln, prob=round(prob, 4),
                                odds=o, edge=round((prob - imp) * 100, 1), paper=True))
    line = odds.get("over_under")
    if line is not None:
        ou = _mlb_ou_prob(pred, line)
        if ou is not None:
            side, prob, o = ("Over", ou, odds.get("over_odds")) if ou >= 0.5 else ("Under", 1 - ou, odds.get("under_odds"))
            imp = _mlb_implied(o)
            if o is not None and imp is not None:
                out.append(dict(type="TOTAL", market="TOTAL", pick=side, line=line, prob=round(prob, 4),
                                odds=o, edge=round((prob - imp) * 100, 1), paper=True))
    if not out:
        return None
    best = max(out, key=lambda pk: pk.get("edge", -99))
    best["matchup"] = "%s @ %s" % (away_abbr or "", home_abbr or "")
    best["away_abbr"], best["home_abbr"] = away_abbr, home_abbr
    return best


def _mlb_first_inning_paper(pred, odds, h_abbr, a_abbr):
    """PAPER 1st-inning (NRFI/YRFI): MC no-run-in-1st prob vs HR's I1 first-inning-total-0.5 market
    (NRFI = Under 0.5, YRFI = Over 0.5). The model's directional lean + edge, never staked."""
    if not pred or not odds:
        return None
    mc = pred.get("mc") or {}
    nrfi = (mc.get("nrfi") or {}).get("nrfi")
    if nrfi is None:
        nrfi = (pred.get("first_inning") or {}).get("nrfi")
    i1 = (odds.get("inning_totals") or {}).get("I1") or {}
    if nrfi is None or not i1:
        return None
    if nrfi >= 0.5:
        pick, prob, o = "NRFI", nrfi, i1.get("under_odds")
    else:
        pick, prob, o = "YRFI", 1 - nrfi, i1.get("over_odds")
    imp = _mlb_implied(o)
    if o is None or imp is None:
        return None
    return {"type": "1ST_INN", "market": "1st Inn", "pick": pick, "line": 0.5,
            "prob": round(prob, 4), "odds": o, "edge": round((prob - imp) * 100, 1), "paper": True,
            "matchup": "%s @ %s" % (a_abbr or "", h_abbr or ""), "away_abbr": a_abbr, "home_abbr": h_abbr}


def _mlb_derivative_paper(pred, odds, h_abbr, a_abbr):
    """PAPER best derivative across F5 total + F5/full team totals â€” MC over/under sim probs at HR's
    line (nearest line in the sim table) vs implied, best-by-edge, never staked."""
    if not pred or not odds:
        return None
    mc = pred.get("mc") or {}
    def _lk(table, line):
        if not isinstance(table, dict) or not table or line is None:
            return None
        try:
            fl = float(line)
            key = min(table.keys(), key=lambda k: abs(float(k) - fl))
        except (TypeError, ValueError):
            return None
        return table.get(key)
    cands = []
    f5t_line = odds.get("f5_total")
    cell = _lk((mc.get("f5") or {}).get("over_under") or {}, f5t_line)
    if cell and f5t_line is not None:
        po, pu = cell.get("over"), cell.get("under")
        if po is not None and pu is not None:
            side, prob, o = ("Over", po, odds.get("f5_over_odds")) if po >= pu else ("Under", pu, odds.get("f5_under_odds"))
            imp = _mlb_implied(o)
            if o is not None and imp is not None:
                cands.append({"type": "F5_TOTAL", "market": "F5 O/U", "pick": side, "line": f5t_line,
                          "tt_team": None, "tt_span": "f5",
                              "prob": round(prob, 4), "odds": o, "edge": round((prob - imp) * 100, 1), "paper": True})
    for tkey, okey, label, mckey in (
        ("home", "team_total_home", "TT " + (h_abbr or "H"), "team_totals"),
        ("away", "team_total_away", "TT " + (a_abbr or "A"), "team_totals"),
        ("home", "f5_team_total_home", "F5 TT " + (h_abbr or "H"), "f5_team_totals"),
        ("away", "f5_team_total_away", "F5 TT " + (a_abbr or "A"), "f5_team_totals"),
    ):
        oo = odds.get(okey) or {}
        line = oo.get("line")
        cell = _lk(((mc.get(mckey) or {}).get(tkey) or {}).get("lines") or {}, line)
        if cell and line is not None:
            po, pu = cell.get("over"), cell.get("under")
            if po is not None and pu is not None:
                side, prob, o = ("Over", po, oo.get("over_odds")) if po >= pu else ("Under", pu, oo.get("under_odds"))
                imp = _mlb_implied(o)
                if o is not None and imp is not None:
                    cands.append({"type": "TEAM_TOTAL", "market": label, "pick": side, "line": line,
                                  "tt_team": tkey, "tt_span": ("f5" if "f5" in okey else "full"),
                                  "prob": round(prob, 4), "odds": o, "edge": round((prob - imp) * 100, 1), "paper": True})
    # Guard vs mispaired/stale odds (Austin 2026-09-19): the HR scraper sometimes labels a
    # team-total with the wrong LINE (e.g. Dodgers "2.5" carrying the ~5.5-line price), so the
    # MC prob for the labeled line prices against the wrong odds and fabricates a huge false
    # edge. Real derivative edges are small; drop anything > 15% as a data error until the
    # scraper's line<->odds pairing is fixed. See memory pickmint_ui_punchlist_2026-09-19.
    cands = [c for c in cands if abs(c.get("edge") or 0) <= 15]
    if not cands:
        return None
    best = max(cands, key=lambda c: c["edge"])
    best["matchup"] = "%s @ %s" % (a_abbr or "", h_abbr or "")
    best["away_abbr"], best["home_abbr"] = a_abbr, h_abbr
    return best


@router.get("/api/scoreboard")
def api_scoreboard(date: str = Query(default="")):
    """Today's MLB games. Heavy work happens in __init__.py:_get_scoreboard."""
    games = _get_scoreboard(date)
    pby = _MLB_PAPER_BY_GAME
    fiby = _MLB_1ST_PAPER_BY_GAME
    dvby = _MLB_DERIV_PAPER_BY_GAME
    if isinstance(games, list):
        # The fresh best-bets maps only cover PRE-GAME games (api_best_bets is
        # pre-game only). Live/final games fall back to the RECORDED paper pick,
        # which carries the settled W/L â€” so final cards keep showing the pick +
        # won/lost like WNBA does, instead of a bare score (Austin 2026-09-30).
        from engine.mlb_paper_shadow import recorded_paper_pick as _rpp
        def _pp(gid, by, scope):
            # Card-lock (Austin 2026-10-06): show the RECORDED (locked first lean) pick, not the
            # live recompute that flips as the line moves pre-game. Fall back to the live map only
            # if nothing was recorded (record failed / status-locked).
            try:
                r = _rpp(gid, scope)
            except Exception:
                r = None
            # Pick Authority (Workstream #1): calibrate the displayed pick so the
            # scoreboard card == the POTD/hero (raw 13.7 -> calibrated 10.24).
            # cal_edge recomputes from model_prob, so this is idempotent.
            _disp = r if r is not None else by.get(gid)
            try:
                from engine.selection import apply_selected as _apply_selected
                return _apply_selected("mlb", _disp) if _disp else _disp
            except Exception:
                return _disp
        games = [{**g,
                  "paper_pick": _pp(str(g.get("id")), pby, "full"),
                  "first_inning_paper": _pp(str(g.get("id")), fiby, "1stinn"),
                  "derivative_paper": _pp(str(g.get("id")), dvby, "derivs")}
                 for g in games]
    return games


# â”€â”€ Tracker (pick history / record / settle / summary) â”€â”€â”€â”€â”€â”€â”€â”€

@router.get("/api/tracker/history")
def api_pick_history():
    """Recent main-tracker pick history with results.

    Filters out derivative bet types defensively. The MLB derivative
    settler trampoline (engine.derivative_tracker._settle_mlb_derivatives)
    inserts temp rows into the main `picks` table mid-flight to reuse
    settle_picks(), then deletes them in the finally block. During that
    window â€” observed via fast tab-switching between Pick Tracker and
    Derivative Tracker â€” the temp rows leak into this endpoint's
    response. Excluding derivative types here makes the leak invisible
    to the UI even if the trampoline race condition fires.
    """
    conn = get_conn()
    deriv_types = list(_MLB_DERIV_TYPES)
    placeholders = ",".join("?" * len(deriv_types))
    picks = conn.execute(
        f"SELECT * FROM picks WHERE bet_type NOT IN ({placeholders}) "
        f"ORDER BY created_at DESC LIMIT 50",
        tuple(deriv_types),
    ).fetchall()
    out = []
    for p in picks:
        d = dict(p)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        out.append(d)
    return out


@router.post("/api/tracker/record")
def api_record_picks():
    """Record today's model picks."""
    try:
        from engine.tracker import record_picks
        picks = record_picks()
        return {"recorded": len(picks), "picks": picks}
    except Exception as e:
        logger.error("Record picks failed: %s", e, exc_info=True)
        return {"error": str(e), "recorded": 0}


@router.post("/api/tracker/settle")
def api_settle_picks():
    """Settle completed picks + POTD against final scores."""
    try:
        from engine.tracker import settle_picks
        result = settle_picks()
        try:
            from engine.pick_of_day import settle_potd
            settle_potd("mlb")
        except Exception as e:
            logger.warning("MLB POTD settle failed: %s", e)
        return result
    except Exception as e:
        logger.error("Settle picks failed: %s", e, exc_info=True)
        return {"error": str(e), "settled": 0}


@router.get("/api/tracker/summary")
def api_pick_summary():
    """Running pick totals."""
    from engine.tracker import get_pick_summary
    return get_pick_summary()


# -- MLB unified tracker (MintyBot 2026-10-22, NBA "tracker merge" -> MLB) --
# ONE MLB tracker = the card's single BEST per game = the merged CALIBRATED best
# across all game markets (Full Game core + First Inning + Derivative F5/team-total),
# GATED like NBA/NHL: core full-game always eligible; an EXTRA scope (first_inning /
# derivative) may only HEADLINE once it has EARNED a track record (>=60 settled /
# >=30 days, calibration_gate.headline_eligible) AND clears the +EV floor. PAPER /
# display only -- reads the isolated paper_shadow table (engine.db / mlb.db) that the
# per-scope paper panels already use; NEVER the real picks / derivative_picks. cal_edge
# is keyed "mlb" and the extra-evidence is per-SCOPE, EXACTLY matching
# engine.mlb_paper_shadow.record_potd's own gate. Forked from routes_nba.

_MLB_EV_CACHE = {"ts": 0.0, "map": {}}

def _mlb_extra_evidence_map(conn):
    """{scope: (settled_n, days_tracked)} for the EXTRA scopes (first_inning,
    derivative) in paper_shadow. MLB's extra-market unit is the SCOPE (matching
    engine.mlb_paper_shadow.record_potd), not the bet_type. Cached 300s so the gate
    costs one GROUP BY per window. (full is also counted but never looked up.)"""
    import time as _t
    if _MLB_EV_CACHE["map"] and (_t.monotonic() - _MLB_EV_CACHE["ts"]) < 300:
        return _MLB_EV_CACHE["map"]
    m = {}
    try:
        for row in conn.execute(
                "SELECT scope, COUNT(*) n, MIN(date) mn, MAX(date) mx FROM paper_shadow "
                "WHERE result IS NOT NULL AND result != 'V' GROUP BY scope"):
            n = (row["n"] or 0)
            days = 0
            if row["mn"] and row["mx"]:
                try:
                    days = int((conn.execute("SELECT julianday(?)-julianday(?)",
                                (row["mx"], row["mn"])).fetchone()[0]) or 0)
                except Exception:
                    days = 0
            m[row["scope"]] = (n, days)
    except Exception:
        return {}
    _MLB_EV_CACHE["ts"] = _t.monotonic(); _MLB_EV_CACHE["map"] = m
    return m


def _mlb_market_scope(scope, bet_type=None):
    """Market bucket for the tracker chip, keyed on the paper_shadow SCOPE column
    (set per-bet at record time, so it classifies by the BET): full -> Full Game;
    first_inning -> First Inning; derivative -> Derivative (F5 total / team total)."""
    s = (scope or "").strip().lower()
    if s == "first_inning":
        return "First Inning"
    if s == "derivative":
        return "Derivative"
    return "Full Game"


def _mlb_flat_profit(result, odds):
    """Flat $100-stake P/L from (result, American odds) -- the EXACT convention
    engine.mlb_paper_shadow._payout already uses at settle (win: +odds if positive
    else 100/abs(odds)*100; loss: -$100; push/void: $0). paper_shadow has no
    stake_units, so grading every row flat $100 is the apples-to-apples basis
    across sports. Mirror of _nba_flat_profit (Austin 2026-10-22)."""
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


_LAST_MLB_AUTO_SETTLE = [0.0]
def _maybe_auto_settle_mlb():
    """Throttled, non-blocking settle so MLB paper picks (all scopes) grade shortly
    after games go final. Fires on a tracker view, at most once per 10 min, in a
    daemon thread. engine.db.get_conn is per-thread-safe. Mirror of
    _maybe_auto_settle_nba."""
    import time as _t, threading as _th
    now = _t.monotonic()
    if now - _LAST_MLB_AUTO_SETTLE[0] < 600.0:
        return
    _LAST_MLB_AUTO_SETTLE[0] = now
    def _run():
        try:
            from engine.mlb_paper_shadow import settle_paper_shadow as _sp
            _sp()
        except Exception as _e:
            logger.warning("auto-settle mlb (paper shadow) failed: %s", _e)
    _th.Thread(target=_run, daemon=True).start()


def _disp_pick(bet_type, pick, line):
    """Combine a bare side/team pick with its line for display ("Under 53.5",
    "ELON +10.5", "CHW -1.5"); ML-type picks pass through, and a pick that already
    carries its number is left alone. (MintyBot 2026-10-22: football/mlb store
    pick + line separately, so the unified tracker showed just "Over"/"ELON".)"""
    p = (pick or "").strip()
    if line is None or line == "":
        return p
    try:
        lf = float(line)
    except (TypeError, ValueError):
        return p
    toks = p.split()
    if toks and any(c.isdigit() for c in toks[-1]):
        return p
    ls = ("%g" % lf)
    bt = (bet_type or "").upper()
    if "TOTAL" in bt or bt in ("O/U", "OU"):
        return "%s %s" % (p, ls)
    if "SPREAD" in bt or bt in ("RL", "RUN_LINE", "PL", "PUCK_LINE", "SPRD"):
        return "%s %s%s" % (p, "+" if lf > 0 else "", ls)
    return p


def _mlb_merged_recorded_best(game_id, date=None, conn=None, ev_map=None):
    """The game's single BEST recorded PAPER pick = the merged CALIBRATED best
    across all scopes (Full Game core + First Inning + Derivative), GATED: core
    full-game always eligible; an EXTRA scope only once it has EARNED a track record
    (headline_eligible) AND clears the +EV floor. Reads the isolated paper_shadow
    table ONLY; NEVER the real picks. Chip-shaped + flat-$100 profit + _scope. Pass
    conn/ev_map to share them across many games. Mirror of _nba_merged_recorded_best;
    cal_edge keyed "mlb", extra-evidence per-scope."""
    import sqlite3 as _sq
    from engine.db import get_conn
    from engine.calibration_gate import cal_edge as _ce, headline_eligible as _he
    if conn is None:
        try:
            conn = get_conn(); conn.row_factory = _sq.Row
        except Exception:
            return None
    if ev_map is None:
        ev_map = _mlb_extra_evidence_map(conn)
    dq = " AND date = ?" if date else ""
    args = (str(game_id), date) if date else (str(game_id),)
    cands = []
    try:
        # All scopes for the game; the ABS(edge)<=20 guard drops a blown-up
        # calibration edge (mirror NBA/NHL).
        for r in conn.execute(
                "SELECT scope,bet_type,pick,line,odds,edge,model_prob,result,profit,"
                "tt_team,tt_span,matchup "
                "FROM paper_shadow WHERE game_id=?" + dq + " "
                "AND (result IS NULL OR result != 'V') AND ABS(COALESCE(edge,0))<=20.0", args):
            d = dict(r)
            d["_scope"] = _mlb_market_scope(d.get("scope"), d.get("bet_type"))
            d["_extra"] = ((d.get("scope") or "").strip().lower() != "full")
            cands.append(d)
    except Exception:
        return None
    if not cands:
        return None
    scored = []
    for p in cands:
        ce = _ce("mlb", p.get("bet_type"), p.get("model_prob"), p.get("odds"),
                 pick_text=p.get("pick"), raw_edge=p.get("edge"))
        n = days = None
        if p["_extra"]:
            n, days = ev_map.get((p.get("scope") or "").strip().lower(), (0, 0))
        eligible = _he(ce, p["_extra"], n, days)
        scored.append((eligible, ce if ce is not None else -999.0, p))
    pool = [sc for sc in scored if sc[0]]
    if not pool:
        return None
    b = max(pool, key=lambda sc: sc[1])[2]
    # Team totals: name the team (+ F5 span) — stored pick is the bare side (Austin
    # 2026-10-25 "Over 0.5 on which side?"). _ce above keeps the raw pick for side detection.
    _pk = b["pick"]
    if (b.get("bet_type") or "").upper() == "TEAM_TOTAL":
        from engine.mlb_paper_shadow import tt_side_text as _tts
        _pk = _tts(b.get("matchup"), b.get("tt_team"), b.get("tt_span"), b["pick"])
    return {"type": b["bet_type"], "market": b["bet_type"], "pick": _disp_pick(b["bet_type"], _pk, b.get("line")),
            "odds": b["odds"], "edge": b["edge"], "prob": b["model_prob"],
            "result": b["result"], "paper": True, "_scope": b["_scope"],
            "closing_odds": None,
            "profit": _mlb_flat_profit(b["result"], b["odds"]),
            "stake_units": 1.0}


def _mlb_matchup_variants(matchup):
    """All abbreviation-alias spellings of 'AWAY @ HOME' (ESPN ARI/CHW/WSH vs HR AZ/CWS/WAS)."""
    out = {matchup}
    sep = " @ " if " @ " in (matchup or "") else "@"
    parts = [x.strip() for x in (matchup or "").split(sep)]
    if len(parts) == 2:
        try:
            from engine.abbr import aliases_for
            for a in (aliases_for(parts[0], sport="mlb") or [parts[0]]):
                for h in (aliases_for(parts[1], sport="mlb") or [parts[1]]):
                    out.add(f"{a} @ {h}")
        except Exception:
            pass
    return list(out)


def _mlb_recorded_authority_row(matchup, date, conn):
    """The game's RECORDED authority pick = the real tracker `picks` row that
    engine.tracker.record_picks wrote from the card's own best_pick (what the card
    headlined; same (date, matchup) lookup _get_recorded_pick uses, alias-tolerant,
    settled rows included). None when the game predates / skipped recording (MLB
    recording silently stopped 2026-09-15 — see engine/tracker/_record.py 2026-10-23).
    Pick Authority: one pick, read everywhere (MintyBot 2026-10-23, Austin "good")."""
    if not matchup or not date:
        return None
    mv = _mlb_matchup_variants(matchup)
    try:
        r = conn.execute(
            "SELECT matchup,date,bet_type,pick,odds,edge,model_prob,result,closing_odds,stake_units "
            f"FROM picks WHERE date=? AND matchup IN ({','.join('?' * len(mv))}) "
            "AND (result IS NULL OR result != 'V') ORDER BY edge DESC LIMIT 1",
            (date, *mv)).fetchone()
    except Exception:
        return None
    if not r:
        return None
    d = dict(r)
    _bt = (d.get("bet_type") or "").strip().upper()
    if _bt in ("1ST INN", "1ST_INN", "NRFI", "YRFI"):
        _scope_tok = "first_inning"
    elif _bt.startswith("F5") or "TEAM TOTAL" in _bt or "TEAM_TOTAL" in _bt:
        _scope_tok = "derivative"
    else:
        _scope_tok = "full"
    return {"type": d["bet_type"], "market": d["bet_type"],
            "pick": _disp_pick(d["bet_type"], d["pick"], None),
            "odds": d["odds"], "edge": d["edge"], "prob": d["model_prob"],
            "result": d["result"], "paper": False, "authority": True,
            "_scope": _mlb_market_scope(_scope_tok),
            "closing_odds": d.get("closing_odds"),
            "profit": _mlb_flat_profit(d["result"], d["odds"]),
            "stake_units": 1.0}


_MLB_UNIFIED_SETTLE_TS = [0.0]


@router.get("/api/mlb/tracker/unified")
def api_mlb_tracker_unified(limit: int = 300):
    """ONE MLB tracker, Pick Authority order (2026-10-23): per game, the RECORDED
    authority pick from the real tracker `picks` table (= what the card headlined, via
    _mlb_recorded_authority_row) when one exists; otherwise the paper_shadow merged
    CALIBRATED best (_mlb_merged_recorded_best) — the fallback keeps the Sep-16..Oct
    history that predates the recording fix instead of blanking it. Games recorded in
    `picks` but absent from paper_shadow are appended too. P/L graded FLAT $100.
    Read-only; per-scope endpoints (/api/tracker/paper) untouched."""
    _maybe_auto_settle_mlb()
    # Settle MLB rows in picks_unified too (ESPN-id fallback lives in the adapter), ≤ 1/5min.
    try:
        import time as _t
        if _t.time() - _MLB_UNIFIED_SETTLE_TS[0] > 300:
            _MLB_UNIFIED_SETTLE_TS[0] = _t.time()
            from engine.picks_unified._legacy_bridge import ensure_adapters_registered as _ear
            from engine.picks_unified import settle_pending as _usp
            _ear(); _usp(sport="mlb")
    except Exception as _e:
        logger.debug("mlb unified settle failed: %s", _e)
    import sqlite3 as _sq
    from engine.db import get_conn
    conn = get_conn()
    conn.row_factory = _sq.Row
    try:
        gids = [r[0] for r in conn.execute(
            "SELECT game_id, MAX(captured_at) c FROM paper_shadow "
            "GROUP BY game_id ORDER BY c DESC LIMIT ?", (limit,))]
    except Exception as e:
        logger.warning("mlb unified tracker game list failed: %s", e)
        gids = []
    evm = _mlb_extra_evidence_map(conn)
    rows = []
    seen = set()
    def _row(gid, date, matchup, best):
        return {
            "game_id": gid, "date": date, "matchup": matchup,
            "market": best.get("_scope"),
            "bet_type": best["type"], "pick": best["pick"], "odds": best["odds"],
            "edge": best["edge"], "result": best["result"], "profit": best.get("profit"),
            "model_prob": best.get("prob"), "closing_odds": best.get("closing_odds"),
            "stake_units": best.get("stake_units"),
            "source": "authority" if best.get("authority") else "paper",
        }
    min_date = None
    for gid in gids:
        meta = conn.execute("SELECT matchup,date FROM paper_shadow WHERE game_id=? LIMIT 1", (str(gid),)).fetchone()
        m_date = meta["date"] if meta else None
        m_matchup = meta["matchup"] if meta else None
        best = _mlb_recorded_authority_row(m_matchup, m_date, conn) or \
            _mlb_merged_recorded_best(gid, conn=conn, ev_map=evm)
        if not best:
            continue
        if m_date and (min_date is None or m_date < min_date):
            min_date = m_date
        for v in _mlb_matchup_variants(m_matchup or ""):
            seen.add((m_date, v))
        rows.append(_row(gid, m_date, m_matchup, best))
    # Authority-recorded games that never produced a paper_shadow row (e.g. recording
    # resumed on a day the paper layer skipped) — append so the tracker never hides a
    # recorded pick. Same window as the paper rows.
    try:
        for pr in conn.execute(
                "SELECT DISTINCT date, matchup FROM picks WHERE date >= ? "
                "AND (result IS NULL OR result != 'V') ORDER BY date DESC LIMIT ?",
                (min_date or "2026-10-01", limit)):
            if any((pr[0], v) in seen for v in _mlb_matchup_variants(pr[1] or "")):
                continue
            auth = _mlb_recorded_authority_row(pr[1], pr[0], conn)
            if not auth:
                continue
            for v in _mlb_matchup_variants(pr[1] or ""):
                seen.add((pr[0], v))
            rows.append(_row(None, pr[0], pr[1], auth))
    except Exception as e:
        logger.debug("mlb unified tracker authority-only append failed: %s", e)
    rows.sort(key=lambda r: (r.get("date") or ""), reverse=True)

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


@router.get("/api/tracker/paper")
def api_paper_shadow():
    """PAPER-pick shadow, per scope (full / first_inning / derivative) â€” each
    with a summary + recent rows. Powers the per-tab paper P/L panels and each
    POTD's running record. Fully isolated from the real picks / real P/L."""
    from engine.mlb_paper_shadow import (
        settle_paper_shadow, paper_shadow_summary, paper_shadow_history,
        settle_potd, record_potd, potd_summary, get_locked_potd,
        paper_pending_count,
    )
    try:
        settle_paper_shadow()
        settle_potd()
    except Exception as e:
        logger.debug("paper shadow settle (endpoint) failed: %s", e)
    _sd = et_today_str()
    out = {}
    for scope in ("full", "first_inning", "derivative"):
        try:
            record_potd(scope, _sd)
        except Exception:
            pass
        out[scope] = {"summary": paper_shadow_summary(scope),
                      "rows": paper_shadow_history(scope, limit=100),
                      "pending": paper_pending_count(scope),
                      "potd": get_locked_potd(scope, _sd),
                      "potd_record": potd_summary(scope)}
    return out


# â”€â”€ Calibration (MLB-side) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@router.post("/api/calibrate")
def api_calibrate(days: int = Query(default=30)):
    """Run model self-calibration on recent games."""
    from engine.calibration import calibrate
    from engine.mlb_predict import reload_weights
    report = calibrate(days=days)
    reload_weights()  # Refresh cached weights
    return report


@router.post("/api/calibrate/teams")
def api_calibrate_teams():
    """Run per-team calibration."""
    from engine.team_calibration import calibrate_teams
    return calibrate_teams()


# â”€â”€ Backtest â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@router.get("/api/backtest")
def api_backtest(days: int = Query(default=0), min_edge: float = Query(default=3),
                  season: int = Query(default=0)):
    """Run model backtest against historical games."""
    from engine.backtest import run_backtest

    yr = season if season > 0 else None

    # Auto-load historical season data if requesting a past season we
    # haven't ingested yet. 100-game threshold = "this season has barely
    # any data" â€” fetch it before running the backtest.
    if yr:
        conn = get_conn()
        game_count = conn.execute(
            "SELECT COUNT(*) as c FROM games WHERE season = ? AND status = 'final'",
            (yr,)
        ).fetchone()["c"]
        if game_count < 100:
            logger.info("Loading %d season data for backtest...", yr)
            from scrapers.mlb_stats import fetch_teams, fetch_season_results
            fetch_teams()
            fetch_season_results(season=yr)

    # Full-season replay runs 2+ minutes, so the Backtest tab always timed out. Serve the last
    # result for these params (persisted, survives restarts) and re-run it in the background at
    # most every 6h (speed pass 2026-10-28). Only the very first run for a param set blocks.
    ck = f"backtest_mlb_{yr or 0}_{days}_{min_edge:g}"

    def _run():
        r = run_backtest(season=yr, days=days if days > 0 else None, min_edge=min_edge)
        r.pop("game_log", None)  # too large for the wire
        return r
    from ._bestbets import cached_backtest
    return cached_backtest(ck, _run)


# â”€â”€ Lineup refresh (MLB) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@router.post("/api/mlb/refresh-lineups")
def api_refresh_mlb_lineups(date: str | None = Query(None),
                              record: bool = Query(True)):
    """Re-check confirmed MLB lineups for `date` and invalidate picks
    whose lineup has changed. Day-of scratches and late lineup shuffles
    otherwise leave a stale pick on the card."""
    try:
        from engine.lineup_refresh import refresh_for_date
        result = refresh_for_date(date=date, record_picks=record)
        # Drop the in-process pred cache when picks_cache rows were
        # invalidated so the next best-bets call recomputes against the
        # confirmed lineup instead of returning the stale blob until TTL
        # expires.
        if (result.get("invalidated") or 0) > 0:
            cleared = _pred_cache_clear_sport("mlb")
            result["pred_cache_cleared"] = cleared
        return result
    except Exception as e:
        logger.error("lineup refresh failed: %s", e, exc_info=True)
        return {"error": str(e), "deltas": 0, "invalidated": 0}


# â”€â”€ Debug â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@router.get("/api/debug/odds")
def api_debug_odds():
    """Test all odds sources."""
    result: dict = {}

    try:
        from scrapers.espn_odds import fetch_game_odds
        games = _get_scoreboard()
        if games:
            espn = fetch_game_odds(games[0].get("id"))
            result["espn"] = {
                "game": games[0].get("short_name", ""),
                "odds": espn,
            }
    except Exception as e:
        result["espn"] = {"error": str(e)}

    return result


@router.get("/api/debug/teams")
def api_debug_teams():
    """Dump raw team data to inspect league/division values."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT mlb_id, abbreviation, name, league, division FROM teams"
    ).fetchall()
    return [dict(r) for r in rows]


# â”€â”€ Accuracy â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Cross-sport in spirit (sport= query param) but defaults to MLB and
# shipped with the MLB router historically.

@router.get("/api/accuracy")
def api_accuracy(sport: str = Query(default="mlb")):
    """Get prediction accuracy / calibration data."""
    try:
        from engine.accuracy import compute_calibration
        return compute_calibration(sport=sport)
    except Exception as e:
        logger.error("Accuracy computation failed: %s", e, exc_info=True)
        return {"error": str(e)}


# â”€â”€ Predict â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class PredictRequest(BaseModel):
    home_team_id: int
    away_team_id: int
    home_pitcher_id: int | None = None
    away_pitcher_id: int | None = None
    venue: str | None = None


@router.post("/api/predict")
def api_predict(req: PredictRequest):
    """Run a game prediction."""
    result = _predict_mlb_full(
        home_team_id=req.home_team_id,
        away_team_id=req.away_team_id,
        home_pitcher_id=req.home_pitcher_id,
        away_pitcher_id=req.away_pitcher_id,
        venue=req.venue,
    )
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    # Attach season_context so the GameDetail banner renders when it
    # would render on best-bets cards too.
    if result.get("season_context") is None:
        sc = _mlb_season_context(req.home_team_id, req.away_team_id)
        if sc:
            result["season_context"] = sc

    # Expose home_impact / away_impact on the injuries block so the
    # GameDetail Injuries card can render the "X% weaker" summary the
    # NHL version has.
    inj = result.get("injuries") or {}
    if isinstance(inj, dict):
        if inj.get("home_impact") is None:
            inj["home_impact"] = _compute_injury_impact_pct(inj.get("home") or [])
        if inj.get("away_impact") is None:
            inj["away_impact"] = _compute_injury_impact_pct(inj.get("away") or [])
        result["injuries"] = inj

    # Surface MLB direction-allow flags so the frontend's
    # PredictionResults / EdgeCallout can suppress recommendations for
    # disabled directions.
    from engine.config import get_flag as _get_flag
    result["direction_filters"] = {
        "rl_favorite": bool(_get_flag("MLB_ALLOW_RL_FAVORITE", True)),
        "rl_underdog": bool(_get_flag("MLB_ALLOW_RL_UNDERDOG", True)),
        "nrfi":        bool(_get_flag("MLB_ALLOW_NRFI", True)),
        "yrfi":        bool(_get_flag("MLB_ALLOW_YRFI", True)),
        "ou_over":     bool(_get_flag("MLB_ALLOW_OU_OVER", True)),
        "ou_under":    bool(_get_flag("MLB_ALLOW_OU_UNDER", True)),
    }

    # Surface active runtime overrides so GameDetail can show a notice
    # ("2 markets currently suppressed by data: OU_UNDER, RL_FAVORITE")
    # instead of silently dropping rows.
    try:
        from engine.model_overrides import list_overrides as _list_ov
        result["active_overrides"] = _list_ov("mlb")
    except Exception as e:
        logger.debug("active_overrides lookup failed: %s", e)
        result["active_overrides"] = []

    # Log component probabilities per market so engine.ensemble_auto_tune
    # can grade each signal against actual outcomes once the game ends
    # and produce tuned blend weights.
    try:
        from engine.prediction_log import log_signals as _log_signals
        from engine.db import get_conn as _gc
        mc = result.get("mc") or {}
        gbm = result.get("gbm") or {}
        if "error" in mc: mc = {}
        if "error" in gbm: gbm = {}
        # Look up the mlb_game_id for this matchup on today's slate so
        # the signal row can JOIN against games cleanly later.
        today = et_today_str()
        _mlb_id = _gc().execute(
            "SELECT mlb_game_id FROM games "
            "WHERE date = ? AND home_team_id = ? AND away_team_id = ? "
            "LIMIT 1",
            (today, req.home_team_id, req.away_team_id),
        ).fetchone()
        game_id = _mlb_id["mlb_game_id"] if _mlb_id else None

        signals = {}
        # home_win
        factor_hw = (result.get("win_prob") or {}).get("home")
        mc_hw = (mc.get("win_prob") or {}).get("home")
        gbm_hw = gbm.get("home_win")
        if factor_hw is not None or mc_hw is not None or gbm_hw is not None:
            signals["home_win"] = {
                "factor": factor_hw, "mc": mc_hw, "gbm": gbm_hw,
            }
        # total (expected runs)
        factor_t = result.get("total")
        mc_t = (mc.get("expected_runs") or {}).get("total")
        gbm_t = gbm.get("total_runs")
        if factor_t is not None or mc_t is not None or gbm_t is not None:
            signals["total"] = {
                "factor": factor_t, "mc": mc_t, "gbm": gbm_t,
            }
        # NRFI
        factor_n = (result.get("first_inning") or {}).get("nrfi")
        mc_n = (mc.get("nrfi") or {}).get("nrfi")
        gbm_n = gbm.get("nrfi_hit")
        if factor_n is not None or mc_n is not None or gbm_n is not None:
            signals["nrfi"] = {
                "factor": factor_n, "mc": mc_n, "gbm": gbm_n,
            }
        # F5 home_win
        factor_f5 = ((result.get("f5") or {}).get("win_prob") or {}).get("home")
        mc_f5 = ((mc.get("f5") or {}).get("win_prob") or {}).get("home")
        gbm_f5 = gbm.get("f5_home_win")
        if factor_f5 is not None or mc_f5 is not None or gbm_f5 is not None:
            signals["f5_home_win"] = {
                "factor": factor_f5, "mc": mc_f5, "gbm": gbm_f5,
            }
        # F5 total
        factor_f5t = (result.get("f5") or {}).get("total")
        mc_f5t = ((mc.get("f5") or {}).get("expected_runs") or {}).get("total")
        gbm_f5t = gbm.get("f5_total")
        if factor_f5t is not None or mc_f5t is not None or gbm_f5t is not None:
            signals["f5_total"] = {
                "factor": factor_f5t, "mc": mc_f5t, "gbm": gbm_f5t,
            }

        if signals:
            _log_signals("mlb", game_id, today,
                         req.home_team_id, req.away_team_id, signals)
    except Exception as e:
        logger.debug("prediction signal logging failed: %s", e)

    # Run the unified picks engine so GameDetail renders the SAME rows
    # as the Scoreboard best-bet badge and POTD selection.
    try:
        from engine.db import get_team_by_id as _get_team_by_id

        # Prefer picks store (single source of truth from best-bets)
        home_team = _get_team_by_id(req.home_team_id) or {}
        away_team = _get_team_by_id(req.away_team_id) or {}
        h_abbr = home_team.get("abbreviation", "")
        a_abbr = away_team.get("abbreviation", "")

        stored = _picks_store_get("mlb", h_abbr, a_abbr)
        if stored:
            picks = stored["picks"]
            odds = stored["odds"]
        elif result.get("_cached_picks"):
            picks = result["_cached_picks"]
            odds = result.get("_cached_odds", {})
        else:
            odds = _odds_from_scoreboard_cache(h_abbr, a_abbr) if (h_abbr and a_abbr) else {}
            if not odds and h_abbr and a_abbr:
                all_odds = fetch_real_odds_for_games()
                odds = match_odds(h_abbr, a_abbr, all_odds)

            picks = generate_picks(
                home_team_id=req.home_team_id,
                away_team_id=req.away_team_id,
                home_pitcher_id=req.home_pitcher_id,
                away_pitcher_id=req.away_pitcher_id,
                venue=req.venue,
                odds=odds or None,
                pred=result,
            )

        result["picks"] = picks
        # Use recorded tracker pick as canonical best_pick
        predict_matchup_str = f"{a_abbr} @ {h_abbr}"
        predict_date = et_today_str()
        recorded = _get_recorded_pick("mlb", predict_matchup_str, predict_date)
        try:
            from engine.selection import apply_selected as _apply_selected
            result["best_pick"] = (_apply_selected("mlb", recorded) if recorded
                                   else get_best_pick(picks))
        except Exception:
            result["best_pick"] = recorded if recorded else get_best_pick(picks)
        result["odds"] = odds
    except Exception as e:
        logger.warning("Unified picks generation failed for %s/%s: %s",
                        req.home_team_id, req.away_team_id, e)
        result["picks"] = []
        result["best_pick"] = None
        result["odds"] = {}

    return result


# â”€â”€ Best Bets â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _bb_predict_one(game: dict, all_odds) -> dict | None:
    """Compute the full prediction + odds payload for a single game.

    Returns a dict with everything /api/best-bets needs (pred, h/a IDs,
    odds, abbreviations) or None if the game should be skipped (live,
    completed, missing IDs, prediction error).
    """
    home_id = game["home"].get("team_id")
    away_id = game["away"].get("team_id")
    if not home_id or not away_id:
        return None

    state = game["status"].get("state", "pre")
    if state in ("post", "in") or game["status"].get("completed"):
        return None

    home_pid = game.get("home_pitcher") or {}
    away_pid = game.get("away_pitcher") or {}
    try:
        h_pitcher_id = int(home_pid["id"]) if home_pid.get("id") else None
        a_pitcher_id = int(away_pid["id"]) if away_pid.get("id") else None
    except (ValueError, TypeError):
        h_pitcher_id = None
        a_pitcher_id = None

    h_abbr = game["home"]["abbreviation"]
    a_abbr = game["away"]["abbreviation"]
    # Merge HR's full scrape with whatever the scoreboard attached.
    matched = match_odds(h_abbr, a_abbr, all_odds) or {}
    game_odds = {**(game.get("odds") or {}), **matched}

    # Bypass the 30-min pred cache for games starting within 1hr â€” late
    # lineup scratches, weather, and line moves all matter most in the
    # final hour. Outside the 1hr window the cache stands.
    use_cache = not _is_game_imminent(game.get("date"))
    try:
        pred = _predict_mlb_full(
            home_team_id=home_id,
            away_team_id=away_id,
            home_pitcher_id=h_pitcher_id,
            away_pitcher_id=a_pitcher_id,
            venue=game.get("venue"),
            use_cache=use_cache,
        )
    except Exception as e:
        logger.error("  Prediction crashed for %s: %s",
                     game.get("short_name", "?"), e, exc_info=True)
        return None

    if "error" in pred:
        logger.error("  Prediction failed for %s: %s",
                     game.get("short_name", "?"), pred["error"])
        return None

    return {
        "game": game,
        "home_id": home_id, "away_id": away_id,
        "h_pitcher_id": h_pitcher_id, "a_pitcher_id": a_pitcher_id,
        "h_abbr": h_abbr, "a_abbr": a_abbr,
        "odds": game_odds,
        "pred": pred,
    }


# Best-bets response cache (Austin 2026-09-19 perf): the MLB best-bets MC
# compute is >4min cold; serve a cached response and refresh it in a background
# thread (stale-while-revalidate) so the dashboard never waits on it after the
# first. The paper globals + paper_shadow recording still run inside the compute
# (cold inline, then in the bg refresh).
_BB_CACHE: dict = {}
_BB_TTL_S = 60.0

# Persisted best-bets cache: serve the last-known slate instantly on a cold
# process (SWR then refreshes it), instead of a synchronous full recompute on
# the first request after a restart. MintyBot 2026-10-15.
from ._bestbets import persist_bb as _persist_bb, load_bb as _load_bb  # noqa: E402
try:
    import time as _t_rh
    _bb_rh, _ = _load_bb("mlb")
    if _bb_rh is not None:
        _BB_CACHE["mlb"] = (_t_rh.monotonic() - _BB_TTL_S - 1.0, _bb_rh)
except Exception:
    pass
_BB_REFRESHING: set = set()


@router.get("/api/best-bets")
def api_best_bets():
    """Served from a short-TTL cache and refreshed in a background thread so the
    heavy MC compute never blocks a dashboard load after the first (Austin
    2026-09-19). First hit on a cold cache still computes inline."""
    import threading
    import time as _t
    ck = "mlb"
    cached = _BB_CACHE.get(ck)
    if cached is not None:
        built, resp = cached
        if (_t.monotonic() - built) >= _BB_TTL_S and ck not in _BB_REFRESHING:
            _BB_REFRESHING.add(ck)

            def _bg():
                try:
                    r = _compute_best_bets()
                    _BB_CACHE[ck] = (_t.monotonic(), r)
                    _persist_bb("mlb", r)
                except Exception as e:
                    logger.warning("[mlb] bg best-bets refresh failed: %s", e)
                finally:
                    _BB_REFRESHING.discard(ck)

            threading.Thread(target=_bg, daemon=True).start()
        return resp
    resp = _compute_best_bets()
    _BB_CACHE[ck] = (_t.monotonic(), resp)
    _persist_bb("mlb", resp)
    return resp


@_bb_reset_on_exit("mlb")
def _compute_best_bets():
    """Run predictions on all today's games using the unified picks engine.

    Per-game predictions run in parallel (ThreadPoolExecutor) so cold
    dashboard load isn't N x serial MC sims. Progress is published via
    /api/best-bets/progress so the frontend can render a percentage."""
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    target_date = et_today_str()
    games = _get_scoreboard()

    all_odds = fetch_real_odds_for_games()

    predictable = [
        g for g in games
        if g["home"].get("team_id") and g["away"].get("team_id")
        and g["status"].get("state", "pre") not in ("post", "in")
        and not g["status"].get("completed")
    ]

    _bb_progress_set(total=len(predictable), done=0, phase="predicting",
                      started_at=_time.time(), finished_at=None)
    logger.info("Best bets: analyzing %d games (parallel)", len(predictable))

    prepped: list[dict] = []
    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="bb-pred") as pool:
        futures = {pool.submit(_bb_predict_one, g, all_odds): g for g in predictable}
        for fut in as_completed(futures):
            try:
                row = fut.result()
            except Exception as e:
                logger.error("  Worker crashed: %s", e, exc_info=True)
                row = None
            _bb_progress_increment()
            if row:
                prepped.append(row)

    # Stash the per-game best PAPER pick for /api/scoreboard to merge onto cards.
    # Paper-pick shadow (Austin 2026-09-19): also persist each scope into the
    # ISOLATED paper_shadow table (never the real picks/derivative_picks) so the
    # tracker can show per-tab paper P/L + each POTD's record.
    from engine.mlb_paper_shadow import record_paper_shadow, settle_paper_shadow
    from engine.selection import apply_selected as _apply_selected  # Pick Authority: calibrated DISPLAY copy (record stays raw)
    _paper = {}
    for _row in prepped:
        try:
            _pp = _mlb_paper_picks(_row["pred"], _row["odds"], _row["h_abbr"], _row["a_abbr"])
            if _pp:
                _paper[str(_row["game"]["id"])] = _apply_selected("mlb", _pp)
                record_paper_shadow(_row["game"]["id"], "full", _pp, game_iso=_row["game"].get("date"))
        except Exception as _e:
            logger.debug("paper pick failed: %s", _e)
    global _MLB_PAPER_BY_GAME
    _MLB_PAPER_BY_GAME = _paper
    _fi_paper = {}
    _dv_paper = {}
    for _row in prepped:
        try:
            _fp = _mlb_first_inning_paper(_row["pred"], _row["odds"], _row["h_abbr"], _row["a_abbr"])
            if _fp:
                _fi_paper[str(_row["game"]["id"])] = _apply_selected("mlb", _fp)
                record_paper_shadow(_row["game"]["id"], "first_inning", _fp, game_iso=_row["game"].get("date"))
            _dp = _mlb_derivative_paper(_row["pred"], _row["odds"], _row["h_abbr"], _row["a_abbr"])
            if _dp:
                _dv_paper[str(_row["game"]["id"])] = _apply_selected("mlb", _dp)
                record_paper_shadow(_row["game"]["id"], "derivative", _dp, game_iso=_row["game"].get("date"))
        except Exception as _e:
            logger.debug("mlb deriv paper failed: %s", _e)
    global _MLB_1ST_PAPER_BY_GAME, _MLB_DERIV_PAPER_BY_GAME
    _MLB_1ST_PAPER_BY_GAME = _fi_paper
    _MLB_DERIV_PAPER_BY_GAME = _dv_paper
    # Persist picks after kickoff/final (Austin 2026-09-19): api_best_bets only
    # computes PRE-game, so live/final games drop out of the globals. Backfill them
    # with the RECORDED pick (+ result). _paper/_fi_paper/_dv_paper ARE the global
    # dict objects (assigned by ref above), so mutating them updates the globals.
    try:
        from engine.mlb_paper_shadow import recorded_paper_pick as _rpp
        _bygame = {}
        for _row in prepped:
            _bygame[str(_row["game"]["id"])] = 1
        for _g in _get_scoreboard():
            _gid = str(_g.get("id"))
            if _gid not in _paper:
                _r = _rpp(_gid, "full")
                if _r: _paper[_gid] = _apply_selected("mlb", _r)
            if _gid not in _fi_paper:
                _r = _rpp(_gid, "first_inning")
                if _r: _fi_paper[_gid] = _apply_selected("mlb", _r)
            if _gid not in _dv_paper:
                _r = _rpp(_gid, "derivative")
                if _r: _dv_paper[_gid] = _apply_selected("mlb", _r)
    except Exception as _e:
        logger.debug("mlb paper backfill failed: %s", _e)
    try:
        settle_paper_shadow()
    except Exception as _e:
        logger.debug("paper_shadow settle failed: %s", _e)
    # Lock today's paper POTD per scope (Austin 2026-09-19) so the DISPLAYED
    # pick-of-the-day holds across reloads instead of recomputing off the live
    # slate. Uses the slate's recorded (ESPN) game_ids -> stable paper_shadow rows.
    try:
        from engine.mlb_paper_shadow import record_potd as _rp, settle_potd as _sp
        _slate_gids = list(set(_paper) | set(_fi_paper) | set(_dv_paper))
        _sd = et_today_str()
        for _sc in ("full", "first_inning", "derivative"):
            _rp(_sc, _sd, _slate_gids)
        _sp()
    except Exception as _e:
        logger.debug("paper POTD lock failed: %s", _e)

    # Dual-write today's MLB paper picks into picks_unified (canonical store).
    # MLB was the ONLY sport not mirroring -> stale since cutover. (Pick Authority L2,
    # Austin 2026-10-14.) Idempotent upsert; never raises (debug-logged).
    try:
        from engine.picks_unified._legacy_bridge import mirror_to_unified as _m2u
        from engine.db import get_conn as _mlb_conn
        _mc = _mlb_conn(); _sd2 = et_today_str()
        _psc = [c[0] for c in _mc.execute("SELECT * FROM paper_shadow LIMIT 0").description]
        from engine.mlb_paper_shadow import tt_side_text as _tts
        for _pr in _mc.execute("SELECT * FROM paper_shadow WHERE date=? ORDER BY id DESC LIMIT 300", (_sd2,)).fetchall():
            _pd = dict(zip(_psc, _pr))
            try:
                # Team totals: unified pick_text names the team (+ F5 span) — paper_shadow's
                # pick is the bare side and the team lives in tt_team/tt_span (Austin
                # 2026-10-25 "Over 0.5 on which side?"). side = over/under (TOTAL convention;
                # the unified MLB adapter grades TEAM_TOTAL from team token + side + line).
                _pk = _pd.get("pick") or ""
                _side = _pk.strip().lower() if _pk.strip().lower() in ("over", "under") else None
                if (_pd.get("bet_type") or "").upper() == "TEAM_TOTAL":
                    _pk = _tts(_pd.get("matchup"), _pd.get("tt_team"), _pd.get("tt_span"), _pk)
                _m2u(sport="mlb", league="mlb", native_game_id=_pd.get("game_id"),
                     pick_date=_pd.get("date") or _sd2, matchup=_pd.get("matchup") or "",
                     bet_type=_pd.get("bet_type") or "", pick_text=_pk, side=_side,
                     odds=int(_pd.get("odds") or 0), prob=float(_pd.get("model_prob") or 0.0),
                     edge_pct=float(_pd.get("edge") or 0.0), line=_pd.get("line"),
                     result=_pd.get("result"), is_shadow=True)
            except Exception:
                pass
    except Exception as _e:
        logger.debug("mlb unified mirror failed: %s", _e)

    _bb_progress_set(phase="building")

    bets = []
    for row in prepped:
        game = row["game"]
        home_id = row["home_id"]
        away_id = row["away_id"]
        h_pitcher_id = row["h_pitcher_id"]
        a_pitcher_id = row["a_pitcher_id"]
        h_abbr = row["h_abbr"]
        a_abbr = row["a_abbr"]
        game_odds = row["odds"]
        pred = row["pred"]

        try:
            picks = generate_picks(
                home_team_id=home_id,
                away_team_id=away_id,
                home_pitcher_id=h_pitcher_id,
                away_pitcher_id=a_pitcher_id,
                venue=game.get("venue"),
                odds=game_odds,
                pred=pred,
            )
        except Exception as e:
            logger.error("  Pick generation failed for %s: %s",
                         game.get("short_name", "?"), e)
            continue

        # Don't drop the card when picks is empty: if the live picker
        # filtered out a previously-recorded pick (HR moved the line past
        # an edge ceiling, calibration shifted, etc.), the tracker's
        # recorded pick should still surface on the card. Only drop when
        # nothing was ever recorded for the matchup.
        matchup = f"{a_abbr} @ {h_abbr}"
        recorded_check = _get_recorded_pick("mlb", matchup, target_date)
        if not picks and not recorded_check:
            continue

        _picks_store_put("mlb", h_abbr, a_abbr, picks, game_odds)

        # Card + POTD must always agree on which pick is "best". To
        # guarantee that, both compute against CORE picks only â€”
        # derivatives flow exclusively through `derivative_picks` and
        # the dedicated tracker.
        deriv_types = list(_MLB_DERIV_TYPES)
        core_picks = [p for p in picks if p.get("type") not in deriv_types]
        # matchup already computed above for the empty-picks fallback;
        # reuse recorded_check so we don't query twice.
        recorded = recorded_check

        # Per-game lock: once the game is within 1 hour of tip-off,
        # freeze the card's best_pick to whatever the tracker recorded.
        # Pre-game line drift: if HR moved the line enough for the live
        # picker to fall below EDGE_SKIP, fall back to the recorded
        # tracker pick instead of showing "NO PICK".
        locked = _is_game_locked(game.get("date"))
        live_best = get_best_pick(core_picks) if core_picks else None
        if locked and recorded:
            best = recorded
        elif live_best and (live_best.get("confidence") or "lean") != "skip":
            best = live_best
        elif recorded:
            from engine.config import EDGE_STRONG, EDGE_MODERATE, EDGE_LEAN
            r_edge = float(recorded.get("edge") or 0)
            r_conf = ("strong" if r_edge >= EDGE_STRONG else
                      "moderate" if r_edge >= EDGE_MODERATE else
                      "lean")
            best = {**recorded, "confidence": r_conf}
        else:
            best = live_best
        # Pick Authority (Workstream #1): calibrate the recorded pick so the
        # card best_pick == the POTD/hero (same as NHL/football).
        if recorded:
            try:
                from engine.selection import apply_selected as _apply_selected
                best = _apply_selected("mlb", recorded)
            except Exception:
                pass
        # Top derivative pick for the secondary card badge.
        deriv_sorted = sorted(
            (p for p in picks if p.get("type") in deriv_types
                and (p.get("confidence") or "lean") != "skip"),
            key=lambda p: -(p.get("edge") or 0),
        )
        best_derivative = deriv_sorted[0] if deriv_sorted else None
        if not best and not best_derivative:
            continue

        # Card-enrich: derive rest / factors / injuries / season_context
        # from the prediction we already computed above.
        win_prob = {}
        rest = {}
        factors = {}
        injuries = {}
        season_context = None
        try:
            if pred and "error" not in pred:
                win_prob = pred.get("win_prob") or {}
                tr = pred.get("travel") or {}
                home_travel = tr.get("home", 1.0) or 1.0
                away_travel = tr.get("away", 1.0) or 1.0
                rest = {
                    "home_short_rest": home_travel < 0.97,
                    "away_short_rest": away_travel < 0.97,
                    "home_rest_advantage": home_travel > away_travel + 0.02,
                    "away_rest_advantage": away_travel > home_travel + 0.02,
                }
                try:
                    from engine.db import get_team_record as _gtr
                    from engine.mlb_predict import SEASON as _MLB_SEASON
                    h_rec = _gtr(home_id, _MLB_SEASON) or {}
                    a_rec = _gtr(away_id, _MLB_SEASON) or {}
                except Exception:
                    h_rec, a_rec = {}, {}
                factors = {
                    "home_wrc_plus": h_rec.get("wrc_plus"),
                    "away_wrc_plus": a_rec.get("wrc_plus"),
                    "home_form": _mlb_form_from_reasoning(pred.get("reasoning"), h_abbr),
                    "away_form": _mlb_form_from_reasoning(pred.get("reasoning"), a_abbr),
                    "park_factor": pred.get("park_factor"),
                    "home_ops": h_rec.get("ops"),
                    "away_ops": a_rec.get("ops"),
                }
                inj_data = pred.get("injuries") or {}
                injuries = {
                    "home_impact": _compute_injury_impact_pct(inj_data.get("home") or []),
                    "away_impact": _compute_injury_impact_pct(inj_data.get("away") or []),
                    "home_list": inj_data.get("home") or [],
                    "away_list": inj_data.get("away") or [],
                }
                season_context = pred.get("season_context") or _mlb_season_context(home_id, away_id)
        except Exception as e:
            logger.debug("  Enrich-best-bet failed for %s/%s: %s",
                         h_abbr, a_abbr, e)

        # all_picks = top 4 CORE picks (no derivatives) plus the 1st INN
        # top-up if outside top 4.
        all_picks = list(core_picks[:4])
        if not any(p.get("type") == "1st INN" for p in all_picks):
            for p in core_picks[4:]:
                if p.get("type") == "1st INN":
                    all_picks.append(p)
                    break

        derivative_picks = sorted(
            (p for p in picks if p.get("type") in deriv_types),
            key=lambda p: -(p.get("edge") or 0),
        )

        bets.append({
            "game_id": game["id"],
            "matchup": f"{a_abbr} @ {h_abbr}",
            "home": game["home"],
            "away": game["away"],
            "time": game["date"],
            "venue": game.get("venue", ""),
            "best_pick": best,
            "best_derivative": best_derivative,
            "all_picks": all_picks,
            "derivative_picks": derivative_picks,
            "confidence": (best.get("confidence", "lean") if best else "skip"),
            "win_prob": _mlb_card_wp(win_prob, game_odds),
            "rest": rest,
            "factors": factors,
            "injuries": injuries,
            "season_context": season_context,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "recorded_pick": recorded,
            "is_locked": locked,
        })

    # Sort by best edge â€” cores rank above derivatives.
    def _sort_key(b):
        bp = b.get("best_pick")
        if bp:
            return (1, bp.get("edge", 0))
        bd = b.get("best_derivative")
        return (0, (bd or {}).get("edge", 0))
    bets.sort(key=_sort_key, reverse=True)

    # Reconcile tracker pending picks with the live model.
    try:
        from engine.tracker import refresh_pending_for_today
        diff = refresh_pending_for_today(bets, target_date)
        if any(diff.values()):
            logger.info("Tracker pending sync (mlb): %s", diff)
    except Exception as e:
        logger.warning("Tracker pending refresh failed: %s", e)

    # Piggyback record_picks on every /api/best-bets call so the
    # tracker stays current without depending on the cron alone.
    try:
        from engine.tracker import record_picks
        new_recorded = record_picks(date=target_date)
        if new_recorded:
            logger.info("Tracker recorded (mlb api hit): %d new pick(s) for %s",
                        len(new_recorded), target_date)
    except Exception as e:
        logger.warning("Tracker record_picks (mlb api hit) failed: %s", e)

    # Piggyback CLV capture on every games-endpoint hit so pending
    # picks get their closing odds stamped close to game time, not
    # from the once-or-twice-daily sync.
    try:
        from engine.tracker import capture_closing_odds as _mlb_cap
        n = _mlb_cap()
        if n:
            logger.info("MLB closing-odds capture (api hit): %d", n)
    except Exception as e:
        logger.warning("MLB closing-odds capture (api hit) failed: %s", e)

    # Paper-bet log for derivative markets â€” separate table so we can
    # evaluate derivative profitability in isolation.
    try:
        from engine.derivative_tracker import record_top_derivatives
        diff = record_top_derivatives("mlb", bets, n_per_game=1,
                                       min_edge=4.0, target_date=target_date)
        if diff.get("inserted") or diff.get("updated"):
            logger.info("Derivative tracker (mlb): %s", diff)
    except Exception as e:
        logger.warning("MLB derivative recorder failed: %s", e)

    # Pick-events breadcrumb log â€” drives the ðŸ“œ badge on BestBets cards.
    try:
        from engine.pick_events import detect_transitions
        ev = detect_transitions("mlb", [
            {"game_id": b["game_id"], "matchup": b["matchup"],
             "best_pick": b.get("best_pick")}
            for b in bets
        ], date=target_date)
        if any(ev.values()):
            logger.info("Pick events (mlb): %s", ev)
    except Exception as e:
        logger.warning("MLB pick-events failed: %s", e)

    _bb_progress_set(phase="idle", finished_at=_time.time())
    return bets
