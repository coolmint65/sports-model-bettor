"""NFL red-zone / goal-line usage + opponent-defense factors from nflverse
play-by-play — the opportunity signal the box-score anytime-TD model lacks.

Anytime-TD is fundamentally about who gets the ball inside the 5, not raw
volume. This module ingests nflverse PBP (every play's yardline + ball carrier
+ defense), computes per-player goal-line / red-zone touch rates and
per-defense TD-suppression factors, and stores them in the ISOLATED
``nfl_player.db`` for the anytime model to fold in. PAPER — it never touches the
football GAME model, real money, or placement.

Join: our ``player_id`` IS the ESPN athlete id; nflverse ``players.parquet``
maps ``gsis_id -> espn_id`` (100% of our roster matched in testing), and the
pbp ``rusher/receiver_player_id`` are gsis_ids. pandas/requests are imported
LAZILY inside ``refresh()`` so the model's read path (the getters) stays
dependency-free and works even if pandas were unavailable.
"""
from __future__ import annotations

import io
import json
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_DB = Path(__file__).resolve().parents[3] / "data" / "football" / "nfl_player.db"
_PBP_CACHE = _DB.parent / "_nflverse_pbp_2026.parquet"
_SEASON = 2026
_PBP_URL = ("https://github.com/nflverse/nflverse-data/releases/download/"
            "pbp/play_by_play_%d.parquet" % _SEASON)
_PLAYERS_URL = ("https://github.com/nflverse/nflverse-data/releases/download/"
                "players/players.parquet")

_REFRESH_TTL = 86400          # 24h
_DEF_LO, _DEF_HI = 0.7, 1.4   # clamp on the opponent-defense multiplier
_lock = threading.Lock()

# Hardcoded fallbacks for the league rates (used only if the corpus is empty).
_FALLBACK = {
    "gl_rush": 0.20, "nongl_rush": 0.008,
    "rz_rec": 0.055, "nonrz_rec": 0.018,
    "gl_share": 0.05, "rz_target_share": 0.16,
}


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(_DB), isolation_level=None)
    c.row_factory = sqlite3.Row
    return c


def _ensure_tables(c) -> None:
    c.execute("""CREATE TABLE IF NOT EXISTS redzone_usage(
        espn_id INTEGER PRIMARY KEY, player_name TEXT, team TEXT, position TEXT,
        games INTEGER, rush_att_pg REAL, gl10_pg REAL, gl5_pg REAL,
        targets_pg REAL, rz20_pg REAL, rz10_pg REAL, updated_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS defense_factors(
        team TEXT PRIMARY KEY, def_rush_factor REAL, def_rec_factor REAL,
        rush_td_allowed_pg REAL, rec_td_allowed_pg REAL, updated_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS _redzone_meta(
        key TEXT PRIMARY KEY, value TEXT)""")


def _meta_get(c, key, default=None):
    r = c.execute("SELECT value FROM _redzone_meta WHERE key=?", (key,)).fetchone()
    if not r:
        return default
    try:
        return json.loads(r["value"])
    except Exception:
        return default


def _meta_set(c, key, value) -> None:
    c.execute("INSERT OR REPLACE INTO _redzone_meta(key, value) VALUES(?, ?)",
              (key, json.dumps(value)))


def last_refresh() -> float:
    try:
        c = _conn(); _ensure_tables(c)
        v = _meta_get(c, "last_refresh", 0); c.close()
        return float(v or 0)
    except Exception:
        return 0.0


# ── Ingest ────────────────────────────────────────────────────
def refresh(force: bool = False) -> dict:
    """Ingest nflverse PBP → per-player RZ usage + per-defense factors +
    league rates, into nfl_player.db. Throttled 24h. Returns a summary."""
    with _lock:
        if not force and (time.time() - last_refresh()) < _REFRESH_TTL:
            return {"skipped": "fresh"}
        try:
            import pandas as pd          # lazy: only the refresh path needs it
            import requests
        except Exception as e:
            logger.warning("nfl redzone: pandas/requests unavailable: %s", e)
            return {"error": "deps unavailable"}

        # PBP (cached), players crosswalk (fresh — small).
        if (_PBP_CACHE.exists() and not force
                and (time.time() - _PBP_CACHE.stat().st_mtime) < _REFRESH_TTL):
            pbp = pd.read_parquet(_PBP_CACHE)
        else:
            r = requests.get(_PBP_URL, timeout=180); r.raise_for_status()
            _PBP_CACHE.write_bytes(r.content)
            pbp = pd.read_parquet(io.BytesIO(r.content))
        pr = requests.get(_PLAYERS_URL, timeout=120); pr.raise_for_status()
        players = pd.read_parquet(io.BytesIO(pr.content))

        # gsis_id -> espn_id (int), and -> position
        players = players.dropna(subset=["gsis_id"])
        gsis_espn, gsis_pos = {}, {}
        for g, e, pos in zip(players["gsis_id"], players.get("espn_id"),
                             players.get("position")):
            if e is not None and str(e) not in ("nan", "None", ""):
                try:
                    gsis_espn[g] = int(float(e))
                except (TypeError, ValueError):
                    pass
            gsis_pos[g] = (pos or "")

        def _td_col(df, native, is_rush):
            if native in df.columns:
                return df[native].fillna(0).astype(float)
            # derive: this play was a TD and the ball-carrier scored
            tp = df.get("td_player_id")
            who = df["rusher_player_id"] if is_rush else df["receiver_player_id"]
            return (((df.get("touchdown", 0).fillna(0) == 1)
                     & (tp == who)).astype(float)) if tp is not None else 0.0

        # ── rushing ──
        rush = pbp[pbp["rush_attempt"] == 1].copy()
        rush = rush[rush["yardline_100"].notna()]
        rush["rush_td"] = _td_col(rush, "rush_touchdown", True)
        rush["gl10"] = (rush["yardline_100"] <= 10).astype(int)
        rush["gl5"] = (rush["yardline_100"] <= 5).astype(int)
        rush_g = (rush[rush["rusher_player_id"].notna()]
                  .groupby("rusher_player_id")
                  .agg(rush_att=("rush_attempt", "sum"),
                       rush_gl10=("gl10", "sum"),
                       rush_gl5=("gl5", "sum"),
                       rush_wk=("week", "nunique"))
                  .reset_index().rename(columns={"rusher_player_id": "gsis"}))

        # ── receiving (targets = pass attempts with a receiver) ──
        pas = pbp[(pbp["pass_attempt"] == 1)
                  & (pbp["receiver_player_id"].notna())].copy()
        pas = pas[pas["yardline_100"].notna()]
        pas["rec_td"] = _td_col(pas, "pass_touchdown", False)
        pas["rz20"] = (pas["yardline_100"] <= 20).astype(int)
        pas["rz10"] = (pas["yardline_100"] <= 10).astype(int)
        rec_g = (pas.groupby("receiver_player_id")
                 .agg(targets=("pass_attempt", "sum"),
                      tgt_rz20=("rz20", "sum"),
                      tgt_rz10=("rz10", "sum"),
                      rec_wk=("week", "nunique"))
                 .reset_index().rename(columns={"receiver_player_id": "gsis"}))

        usage = rush_g.merge(rec_g, on="gsis", how="outer")
        for col in ["rush_att", "rush_gl10", "rush_gl5", "rush_wk",
                    "targets", "tgt_rz20", "tgt_rz10", "rec_wk"]:
            if col not in usage.columns:
                usage[col] = 0
        usage = usage.fillna(0)
        usage["games"] = usage[["rush_wk", "rec_wk"]].max(axis=1).clip(lower=1)
        usage["espn_id"] = usage["gsis"].map(gsis_espn)
        usage = usage.dropna(subset=["espn_id"])
        usage["espn_id"] = usage["espn_id"].astype(int)
        usage["position"] = usage["gsis"].map(gsis_pos).fillna("")

        # ── league conversion rates ──
        def _rate(df, mask, tdcol):
            sub = df[mask]
            n = len(sub)
            return (float(sub[tdcol].sum()) / n) if n else None
        L = {
            "gl_rush": _rate(rush, rush["yardline_100"] <= 10, "rush_td"),
            "nongl_rush": _rate(rush, rush["yardline_100"] > 10, "rush_td"),
            "rz_rec": _rate(pas, pas["yardline_100"] <= 20, "rec_td"),
            "nonrz_rec": _rate(pas, pas["yardline_100"] > 20, "rec_td"),
        }
        for k, v in list(L.items()):
            if v is None or v <= 0:
                L[k] = _FALLBACK[k]
        # sanity bounds
        L["gl_rush"] = min(max(L["gl_rush"], 0.10), 0.35)
        L["nongl_rush"] = min(max(L["nongl_rush"], 0.002), 0.03)
        L["rz_rec"] = min(max(L["rz_rec"], 0.02), 0.12)
        L["nonrz_rec"] = min(max(L["nonrz_rec"], 0.005), 0.05)
        # League share of carries/targets that fall inside-10 / inside-20 —
        # used to shrink a player's small-sample goal-line usage toward his
        # volume-implied baseline (stabilises early-season noise).
        L["gl_share"] = min(max(
            len(rush[rush["yardline_100"] <= 10]) / max(len(rush), 1), 0.02), 0.10)
        L["rz_target_share"] = min(max(
            len(pas[pas["yardline_100"] <= 20]) / max(len(pas), 1), 0.08), 0.30)

        # ── per-defense factors (rush TDs & rec TDs allowed per game) ──
        games_def = pbp.groupby("defteam")["game_id"].nunique()
        rush_td_def = rush.groupby("defteam")["rush_td"].sum()
        rec_td_def = pas.groupby("defteam")["rec_td"].sum()
        rush_pg = (rush_td_def / games_def).dropna()
        rec_pg = (rec_td_def / games_def).dropna()
        Lr = float(rush_pg.mean()) if len(rush_pg) else 0.0
        Lc = float(rec_pg.mean()) if len(rec_pg) else 0.0
        now_iso = time.strftime("%Y-%m-%dT%H:%M:%S")

        c = _conn(); _ensure_tables(c)
        c.execute("BEGIN")
        c.execute("DELETE FROM redzone_usage")
        for _, r_ in usage.iterrows():
            c.execute(
                "INSERT OR REPLACE INTO redzone_usage(espn_id, player_name, team,"
                " position, games, rush_att_pg, gl10_pg, gl5_pg, targets_pg,"
                " rz20_pg, rz10_pg, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (int(r_["espn_id"]), None, None, str(r_["position"]),
                 int(r_["games"]),
                 r_["rush_att"] / r_["games"], r_["rush_gl10"] / r_["games"],
                 r_["rush_gl5"] / r_["games"], r_["targets"] / r_["games"],
                 r_["tgt_rz20"] / r_["games"], r_["tgt_rz10"] / r_["games"],
                 now_iso))
        c.execute("DELETE FROM defense_factors")
        for team in games_def.index:
            rp = float(rush_pg.get(team, Lr)); cp = float(rec_pg.get(team, Lc))
            drf = min(max(rp / Lr, _DEF_LO), _DEF_HI) if Lr else 1.0
            dcf = min(max(cp / Lc, _DEF_LO), _DEF_HI) if Lc else 1.0
            c.execute(
                "INSERT OR REPLACE INTO defense_factors(team, def_rush_factor,"
                " def_rec_factor, rush_td_allowed_pg, rec_td_allowed_pg,"
                " updated_at) VALUES(?,?,?,?,?,?)",
                (str(team), drf, dcf, rp, cp, now_iso))
        _meta_set(c, "league_rates", L)
        _meta_set(c, "last_refresh", time.time())
        c.execute("COMMIT")
        c.close()
        return {"players": int(len(usage)), "defenses": int(len(games_def)),
                "league_rates": L, "pbp_rows": int(len(pbp))}


# ── Read path (no pandas) ─────────────────────────────────────
def league_rates() -> dict:
    try:
        c = _conn(); _ensure_tables(c)
        v = _meta_get(c, "league_rates", None); c.close()
        return v or dict(_FALLBACK)
    except Exception:
        return dict(_FALLBACK)


def player_rz(espn_id: int) -> dict | None:
    try:
        c = _conn(); _ensure_tables(c)
        r = c.execute("SELECT * FROM redzone_usage WHERE espn_id=?",
                      (int(espn_id),)).fetchone()
        c.close()
        return dict(r) if r else None
    except Exception:
        return None


def def_factor(team_abbr: str, position: str) -> float:
    """Opponent's TD-suppression multiplier for the scorer's position —
    rush factor for RB/QB, rec factor for WR/TE. 1.0 when unknown."""
    try:
        c = _conn(); _ensure_tables(c)
        r = c.execute("SELECT def_rush_factor, def_rec_factor FROM "
                      "defense_factors WHERE team=?",
                      ((team_abbr or "").upper(),)).fetchone()
        c.close()
        if not r:
            return 1.0
        pos = (position or "").upper()
        is_rec = pos in ("WR", "TE", "WR/TE")
        return float(r["def_rec_factor"] if is_rec else r["def_rush_factor"])
    except Exception:
        return 1.0


__all__ = ["refresh", "last_refresh", "league_rates", "player_rz", "def_factor"]
