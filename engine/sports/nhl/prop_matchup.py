"""
NHL prop matchup multiplier (Austin 2026-10-22).

Covers the prop stats that otherwise price BLIND on the player's flat
career mean -- which, pre-game, is all of them:

    g, a, sog, saves, ga, blocks

Why all of them: the GBM (prop_gbm) meant to matchup-adjust g/a reads
its opponent features from a player_game_logs row that does not exist
until the game is actually played, so predict_mu returns None for every
pre-game pick; sog/saves have no trained model at all. So without this,
tonight's props use the player's season average regardless of opponent.

MODEL per stat (all rates derived from nhl_games box scores -- self-
contained, since nhl_team_stats goal cols are unpopulated early season;
each factor shrunk toward 1.0 by sample size and clamped):
  * sog    = opp shots ALLOWED / league         (leaky D => more shots)
  * blocks = opp shots TAKEN / league           (more incoming to block)
  * saves  = opp shots TAKEN / league           (more pucks the G faces)
  * ga     = opp goals SCORED / league          (stronger O => more GA)
  * g, a   = SHOT-VOLUME x CONVERSION:
       (opp shots ALLOWED / league)             -- the skater's team's
                                                   shot volume, and
       (1 - opp STARTING-goalie save%) / (1 - league save%)
                                                 -- conversion vs the
                                                   confirmed starter.
     This reduces to the team goals-allowed factor when the usual goalie
     starts, and correctly swings up for a backup / down for an ace.
     Falls back to opp goals-allowed / league when the starter is not
     confirmed. The opponent's own goalie (saves/ga) keeps its baseline.

The multiplier scales mu via the SAME _override_mu hook the MC already
uses, so _sample draws from the fitted family at the adjusted mean and
family variance stays correct. The GBM still wins whenever it actually
produces a mu.

Reversible: config flag ENABLE_NHL_PROP_MATCHUP (default True).
"""
from __future__ import annotations

import json
import logging
import statistics as _st
from datetime import date
from functools import lru_cache

from ...player_props_db import _conn_for

logger = logging.getLogger(__name__)

# single-factor stats -> the opponent _team_rates field that drives them
_SIMPLE_RATE = {"sog": "sa", "blocks": "sf", "saves": "sf", "ga": "gf"}
# skater goals/assists: shot-volume x opponent starting-goalie conversion
_GOALIE_STATS = frozenset({"g", "a"})
MATCHUP_STATS = frozenset(set(_SIMPLE_RATE) | _GOALIE_STATS)

_SHRINK_C = 6.0            # team-rate shrink: k = games / (games + C)
_SHRINK_SHOTS = 200.0      # goalie save% shrink: k = shots / (shots + C)
_CLAMP_LO, _CLAMP_HI = 0.80, 1.25          # single-factor stats
_CLAMP_GA_LO, _CLAMP_GA_HI = 0.75, 1.30    # g/a swing a touch wider


def _team_rates(season: int) -> dict:
    return _team_rates_cached(int(season), date.today().isoformat())


@lru_cache(maxsize=8)
def _team_rates_cached(season: int, _daystamp: str) -> dict:
    """Per-team per-game {sf,sa,gf,ga} means + games, plus league means
    and league save%, from this season's completed games. Re-derived
    daily (the _daystamp cache key)."""
    conn = _conn_for("nhl")
    rows = conn.execute(
        "SELECT home_team_id h, away_team_id a, home_score hs, away_score g_as, "
        "home_shots hsh, away_shots ash FROM nhl_games "
        "WHERE season = ? AND status = 'final' AND home_score IS NOT NULL",
        (season,),
    ).fetchall()
    acc: dict = {}

    def _add(t, sf, sa, gf, ga):
        d = acc.setdefault(t, {"sf": [], "sa": [], "gf": [], "ga": []})
        if sf is not None and sa is not None:
            d["sf"].append(float(sf)); d["sa"].append(float(sa))
        if gf is not None and ga is not None:
            d["gf"].append(float(gf)); d["ga"].append(float(ga))

    for r in rows:
        _add(r["h"], r["hsh"], r["ash"], r["hs"], r["g_as"])
        _add(r["a"], r["ash"], r["hsh"], r["g_as"], r["hs"])
    team: dict = {}
    for t, d in acc.items():
        team[t] = {
            "games": len(d["gf"]),
            "sf": _st.mean(d["sf"]) if d["sf"] else None,
            "sa": _st.mean(d["sa"]) if d["sa"] else None,
            "gf": _st.mean(d["gf"]) if d["gf"] else None,
            "ga": _st.mean(d["ga"]) if d["ga"] else None,
        }
    league = {}
    for k in ("sf", "sa", "gf", "ga"):
        vals = [v[k] for v in team.values() if v[k] is not None]
        league[k] = _st.mean(vals) if vals else None
    # League save% on the SAME basis _goalie_savepct measures a starter on: goalie-charged
    # saves / shots against over each goalie's last 40 games, pooled across goalies. The old
    # 1 - GF/SF also counted empty-net and shootout goals (and a this-season league vs a
    # starter's 40-game window mixes in last season's higher save%), so an AVERAGE starter's
    # (1 - sv) / (1 - league) sat ~0.91 and every g/a multiplier ran ~9% low -- the model
    # leaned Under on every goal/assist/point line (MintyBot 2026-10-26).
    league["save"] = _league_goalie_savepct(date.today().isoformat())
    if league["save"] is None and league.get("gf") and league.get("sf"):
        league["save"] = 1.0 - league["gf"] / league["sf"]
    return {"team": team, "league": league}


@lru_cache(maxsize=4)
def _league_goalie_savepct(_daystamp: str):
    """Pooled save% over every goalie's last 40 game logs (the _goalie_savepct window)."""
    conn = _conn_for("nhl")
    per: dict = {}
    for r in conn.execute(
            "SELECT player_id, stats_json FROM player_game_logs "
            "WHERE stats_json LIKE '%shots_against%' ORDER BY player_id, date DESC"):
        lst = per.setdefault(r["player_id"], [])
        if len(lst) >= 40:
            continue
        try:
            s = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        if s.get("shots_against"):
            lst.append((float(s.get("saves", 0) or 0), float(s["shots_against"])))
    sv = sum(a for lst in per.values() for a, _ in lst)
    sa = sum(b for lst in per.values() for _, b in lst)
    return (sv / sa) if sa else None


def _resolve(player_id: int, game_id: str):
    """(opp_team_id, season) for this player's game, or (None, None).
    Opponent from nhl_games (present for scheduled games too); player's
    own team from their most-recent game log."""
    conn = _conn_for("nhl")
    try:
        gid = int(game_id)
    except (TypeError, ValueError):
        return None, None
    g = conn.execute(
        "SELECT home_team_id, away_team_id, season FROM nhl_games WHERE game_id = ?",
        (gid,),
    ).fetchone()
    if not g:
        return None, None
    pt = conn.execute(
        "SELECT team_id FROM player_game_logs WHERE player_id = ? "
        "AND team_id IS NOT NULL ORDER BY date DESC LIMIT 1",
        (int(player_id),),
    ).fetchone()
    if not pt:
        return None, None
    my_team = pt["team_id"]
    home, away = g["home_team_id"], g["away_team_id"]
    if my_team == home:
        return away, g["season"]
    if my_team == away:
        return home, g["season"]
    return None, None


def _opp_starter(game_id: int, opp_team_id: int):
    """Opponent's CONFIRMED starting goalie player_id for this game, or
    None (unconfirmed / no snapshot / can't map side)."""
    conn = _conn_for("nhl")
    snap = conn.execute(
        "SELECT home_goalie_id, away_goalie_id, confirmed FROM goalie_snapshots "
        "WHERE game_id = ?", (int(game_id),),
    ).fetchone()
    if not snap or not snap["confirmed"]:
        return None
    g = conn.execute(
        "SELECT home_team_id, away_team_id FROM nhl_games WHERE game_id = ?",
        (int(game_id),),
    ).fetchone()
    if not g:
        return None
    if opp_team_id == g["home_team_id"]:
        return snap["home_goalie_id"]
    if opp_team_id == g["away_team_id"]:
        return snap["away_goalie_id"]
    return None


@lru_cache(maxsize=512)
def _goalie_savepct(player_id: int, _daystamp: str):
    """(save_pct, shots_faced) from the goalie's recent game logs
    (up to 40 games), or (None, 0)."""
    conn = _conn_for("nhl")
    rows = conn.execute(
        "SELECT stats_json FROM player_game_logs WHERE player_id = ? "
        "ORDER BY date DESC LIMIT 40", (int(player_id),),
    ).fetchall()
    sv = sa = 0
    for r in rows:
        try:
            s = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        if s.get("shots_against"):
            sv += s.get("saves", 0); sa += s["shots_against"]
    return (sv / sa, sa) if sa else (None, 0)


def _shrink(ratio: float, n: float, c: float) -> float:
    k = n / (n + c)
    return 1.0 + k * (ratio - 1.0)


def matchup_mu_mult(stat_key: str, player_id: int, game_id: str) -> float:
    """Multiplier on mu for this player's stat vs tonight's opponent.
    Returns 1.0 (no-op) when disabled, opponent unknown, or data thin."""
    if stat_key not in MATCHUP_STATS:
        return 1.0
    try:
        from ...config import get_flag as _gf
        if not _gf("ENABLE_NHL_PROP_MATCHUP", True, sport="nhl"):
            return 1.0
    except Exception:
        pass
    try:
        opp, season = _resolve(player_id, game_id)
        if not opp or season is None:
            return 1.0
        rates = _team_rates(season)
        tr = rates["team"].get(opp)
        league = rates["league"]
        if not tr:
            return 1.0

        if stat_key in _GOALIE_STATS:               # g, a
            sa, lg_sa = tr.get("sa"), league.get("sa")
            if sa is None or not lg_sa:
                return 1.0
            shots_factor = _shrink(sa / lg_sa, tr["games"], _SHRINK_C)
            mult = None
            try:
                starter = _opp_starter(int(game_id), opp)
            except (TypeError, ValueError):
                starter = None
            if starter:
                sv, shots = _goalie_savepct(starter, date.today().isoformat())
                lg_sv = league.get("save")
                if sv is not None and lg_sv and lg_sv < 1.0:
                    goalie_factor = _shrink((1.0 - sv) / (1.0 - lg_sv),
                                            shots, _SHRINK_SHOTS)
                    mult = shots_factor * goalie_factor
            if mult is None:                          # no confirmed starter
                ga, lg_ga = tr.get("ga"), league.get("ga")
                if ga is None or not lg_ga:
                    return 1.0
                mult = _shrink(ga / lg_ga, tr["games"], _SHRINK_C)
            return max(_CLAMP_GA_LO, min(_CLAMP_GA_HI, mult))

        rk = _SIMPLE_RATE[stat_key]                   # sog, blocks, saves, ga
        opp_rate, lg = tr.get(rk), league.get(rk)
        if opp_rate is None or not lg:
            return 1.0
        mult = _shrink(opp_rate / lg, tr["games"], _SHRINK_C)
        return max(_CLAMP_LO, min(_CLAMP_HI, mult))
    except Exception as e:
        logger.debug("matchup_mu_mult(%s, %s, %s) failed: %s",
                     stat_key, player_id, game_id, e)
        return 1.0


__all__ = ["matchup_mu_mult", "MATCHUP_STATS"]
