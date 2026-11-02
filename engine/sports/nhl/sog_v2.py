"""NHL Skater SOG model v2 (MintyBot 2026-11-01, Austin "Yes" to the SOG rebuild).

Replaces the v1 SOG mean (flat mean of the player's games in the adaptive 60/120/365-day window x the
opponent shots-allowed multiplier, NegBin with a player-sample / pooled k=3.86) with a GLM on as-of
features, all from games BEFORE the game date:

  log mu = c + b1 log(rate) + b2 log(toi5) + b3 log(opp_sa) + b4 home + b5 isD + b6 playoff
    rate    = SOG per 60, decayed sums (half-life 40 played games, x0.6 at a season change),
              shrunk to the F/D league rate with a 2-hour prior
    toi5    = decayed mean TOI (half-life 5 games), minutes
    opp_sa  = opponent shots allowed per game (decayed, half-life 20 games, 5-game league prior) / league
  X ~ NegBin(mu, k), k fit by MLE on out-of-sample residuals (NOT across players: that was the v1 bug).

Coefficients, priors and k live in data/nhl_sog_v2.json (fit on ESPN boxes 2024-25..2026-11-01).
Backtest (Pi scratchpad nhlsog/): 5,694 DK main lines at the DK OPEN, log-loss v2 .6859 vs v1 .6922 vs
DK de-vig .6881; HR week 10-26..11-01 (781 lines) v2 .6774 vs v1 .7016 vs HR de-vig .6833.

Inputs are exactly what player_game_logs / nhl_games carry (sog, toi_min FLOORED minutes, position,
team shots); the backtest floored TOI the same way. Reversible: model override
ENABLE_NHL_SOG_V2 = False (engine.model_overrides.apply_override("nhl", "ENABLE_NHL_SOG_V2", False)).
"""
from __future__ import annotations

import json
import logging
import math
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path

from ...player_props_db import _conn_for

logger = logging.getLogger(__name__)

_PATH = Path(__file__).resolve().parents[3] / "data" / "nhl_sog_v2.json"
_GAME_TYPES = ("02", "03")           # regular season + playoffs (no preseason / all-star)


@lru_cache(maxsize=1)
def params() -> dict:
    return json.loads(_PATH.read_text())


def enabled() -> bool:
    try:
        from ...config import get_flag
        if not get_flag("ENABLE_NHL_SOG_V2", True, sport="nhl"):
            return False
    except Exception:
        pass
    return _PATH.exists()


def _season(d: str) -> int:
    dd = date.fromisoformat(d[:10])
    return dd.year + 1 if dd.month >= 7 else dd.year


def _team_state(game_date: str) -> dict:
    return _team_state_cached(game_date, datetime.utcnow().strftime("%Y-%m-%dT%H"))


@lru_cache(maxsize=8)
def _team_state_cached(game_date: str, _hour: str) -> dict:
    """Decayed shots-for/against per team from final reg/playoff games strictly before game_date."""
    P = params()
    td = 0.5 ** (1.0 / P["team_half_life_games"])
    carry = P["team_season_carry"]
    conn = _conn_for("nhl")
    rows = conn.execute(
        "SELECT date, home_team_id, away_team_id, home_shots, away_shots, game_type FROM nhl_games "
        "WHERE status = 'final' AND home_shots IS NOT NULL AND away_shots IS NOT NULL "
        "AND game_type IN (2, 3) AND date < ? ORDER BY date", (game_date,)).fetchall()
    st: dict = {}
    for r in rows:
        s = _season(r["date"])
        for t, sf, sa in ((r["home_team_id"], r["home_shots"], r["away_shots"]),
                          (r["away_team_id"], r["away_shots"], r["home_shots"])):
            e = st.setdefault(t, {"w": 0.0, "sa": 0.0, "sf": 0.0, "season": None})
            if e["season"] is not None and e["season"] != s:
                e["w"] *= carry; e["sa"] *= carry; e["sf"] *= carry
            e["season"] = s
            e["w"] = e["w"] * td + 1.0
            e["sa"] = e["sa"] * td + float(sa)
            e["sf"] = e["sf"] * td + float(sf)
    lg = [e["sa"] / e["w"] for e in st.values() if e["w"] > 3]
    return {"team": st, "lg": (sum(lg) / len(lg)) if lg else 30.0}


def _game(conn, game_id):
    return conn.execute("SELECT date, home_team_id, away_team_id, game_type FROM nhl_games WHERE game_id = ?",
                        (int(game_id),)).fetchone()


def features(player_id: int, game_id) -> dict | None:
    """As-of features for (player, game), or None when the game / player history is unknown."""
    P = params()
    conn = _conn_for("nhl")
    g = _game(conn, game_id)
    if not g:
        return None
    gd = str(g["date"])[:10]
    rows = conn.execute(
        "SELECT date, game_id, team_id, stats_json FROM player_game_logs "
        "WHERE player_id = ? AND date < ? ORDER BY date, game_id", (int(player_id), gd)).fetchall()
    hr, ht = P["rate_half_life_games"], P["toi_half_life_games"]
    dr, dt = 0.5 ** (1.0 / hr), 0.5 ** (1.0 / ht)
    carry = P["season_carry"]
    S_sog = S_toi = 0.0            # rate half-life sums
    T_w = T_toi = 0.0              # toi half-life sums
    n = 0
    season = None
    pos = None
    team = None
    for r in rows:
        if str(r["game_id"])[4:6] not in _GAME_TYPES:
            continue
        try:
            s = json.loads(r["stats_json"] or "{}")
        except (TypeError, ValueError):
            continue
        if "sog" not in s or s.get("position") == "G":
            continue
        team = r["team_id"] if r["team_id"] is not None else team
        pos = s.get("position") or pos
        toi = float(s.get("toi_min") or 0) * 60.0
        if toi <= 0:
            continue
        se = _season(r["date"])
        if season is not None and se != season:
            S_sog *= carry; S_toi *= carry; T_w *= carry; T_toi *= carry
        season = se
        S_sog = S_sog * dr + float(s.get("sog") or 0)
        S_toi = S_toi * dr + toi
        T_w = T_w * dt + 1.0
        T_toi = T_toi * dt + toi
        n += 1
    if n < P["min_games"] or team is None:
        return None
    if team == g["home_team_id"]:
        home, opp = 1, g["away_team_id"]
    elif team == g["away_team_id"]:
        home, opp = 0, g["home_team_id"]
    else:
        return None                  # player's latest team isn't in this game (trade / bad id)
    isD = 1.0 if pos == "D" else 0.0
    pr = P["priors"]
    r0 = pr["rD"] if isD else pr["rF"]
    m = P["prior_hours"]
    rate = (S_sog + m * r0) / (S_toi / 3600.0 + m)
    toi5 = T_toi / T_w
    ts = _team_state(gd)
    e = ts["team"].get(opp, {"w": 0.0, "sa": 0.0})
    lg = ts["lg"]
    k0 = P["team_prior_games"]
    opp_sa = (e["sa"] + k0 * lg) / (e["w"] + k0) / lg
    gt = g["game_type"]
    return {"rate": rate, "toi5_min": toi5 / 60.0, "opp_sa": opp_sa, "home": home, "isD": isD,
            "playoff": 1.0 if gt == 3 else 0.0, "n_games": n}


def predict(player_id: int, game_id) -> tuple[float, float] | None:
    """(mu, k) for the player's SOG in this game, or None (thin history / unknown game)."""
    try:
        f = features(player_id, game_id)
        if f is None:
            return None
        P = params()
        x = {"c": 1.0, "lrate": math.log(f["rate"]), "ltoi5": math.log(max(f["toi5_min"], 2.0)),
             "lopp": math.log(f["opp_sa"]), "home": f["home"], "isD": f["isD"], "playoff": f["playoff"]}
        eta = sum(b * x[nm] for nm, b in zip(P["names"], P["beta"]))
        return math.exp(max(-5.0, min(4.0, eta))), float(P["k"])
    except Exception as e:
        logger.debug("sog_v2.predict(%s, %s) failed: %s", player_id, game_id, e)
        return None


__all__ = ["predict", "features", "enabled", "params"]
