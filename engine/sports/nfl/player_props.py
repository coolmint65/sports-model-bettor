"""NFL player-props PAPER model — passing / rushing / receiving yards + receptions.

Mirrors the NHL/WNBA prop framework (project a per-game stat distribution →
probability the stat clears the line → edge vs HR → record via the shared
``player_props_db``), adapted to Hard Rock's NFL board, which prices these as
ALTERNATE-LINE thresholds ("125+ Pass Yards" YES/NO) rather than a single
over/under line. We project each player's per-game distribution from nflverse
play-by-play and price every HR threshold with P(stat >= line).

Data: nflverse PBP (shared cache with ``engine.sports.nfl.redzone``) →
per-player-game passing/rushing/receiving yards + receptions/attempts, keyed by
ESPN player_id via the nflverse players gsis->espn_id crosswalk.

PAPER-only: records into ``player_props_picks`` via the shared framework — never
placed, never touches the football GAME model. The isolated ``nfl_player.db``
holds the prop logs (same DB the anytime-TD + redzone tables use).
"""
from __future__ import annotations

import io
import json
import logging
import math
import re
import sqlite3
import threading
import time
import unicodedata
from pathlib import Path

from ...player_props_db import insert_pick, _conn_for
from ..._tz import et_today_str

logger = logging.getLogger(__name__)

_DB = Path(__file__).resolve().parents[3] / "data" / "football" / "nfl_player.db"
# Shared PBP cache written by redzone.py — reuse it (don't double-download).
_PBP_CACHE = _DB.parent / "_nflverse_pbp_2026.parquet"
_SEASON = 2026
_PBP_URL = ("https://github.com/nflverse/nflverse-data/releases/download/"
            "pbp/play_by_play_%d.parquet" % _SEASON)
_PLAYERS_URL = ("https://github.com/nflverse/nflverse-data/releases/download/"
                "players/players.parquet")
# v2 (2026-10-26): the prop model (prop_model.py) projects off three seasons of
# player-games, not this season's last 3-8. Past seasons are static (downloaded
# once); the current season re-pulls every 6h so Sunday's games land Monday
# without waiting a full day (settlement + L10 read these logs too).
_SEASONS = (2024, 2025, 2026)
_PBP_URL_T = ("https://github.com/nflverse/nflverse-data/releases/download/"
              "pbp/play_by_play_%d.parquet")
_SNAPS_URL_T = ("https://github.com/nflverse/nflverse-data/releases/download/"
                "snap_counts/snap_counts_%d.parquet")
_PBP_COLS = ["game_id", "season", "week", "season_type", "game_date", "posteam",
             "defteam", "home_team", "away_team", "passer_player_id",
             "rusher_player_id", "receiver_player_id", "passing_yards",
             "rushing_yards", "receiving_yards", "complete_pass", "pass_attempt",
             "rush_attempt", "sack", "two_point_attempt"]
_SKILL_POS = ("QB", "RB", "FB", "WR", "TE")
_REFRESH_TTL = 6 * 3600
_lock = threading.Lock()
_rz_check = {"ts": 0.0}

# bet_type -> (stat_key, kind). kind: "yards" (Normal) or "count" (Poisson).
MARKETS = {
    "Passing Yards":   ("pass_yds", "yards"),
    "Rushing Yards":   ("rush_yds", "yards"),
    "Receiving Yards": ("rec_yds", "yards"),
    "Receptions":      ("receptions", "count"),
}
PROJ_GAMES = 8            # recent games for the projection
MIN_GAMES = 3
PROP_MIN_EDGE_PCT = 4.0   # +EV flag / record threshold (matches other prop models)
# Per-yards-market residual sd as a floor (fraction of the mean) — real NFL
# game-to-game variance is high; don't let a thin sample understate it.
_SD_FLOOR_FRAC = {"pass_yds": 0.32, "rush_yds": 0.45, "rec_yds": 0.55}
_SIDE = "Over"


def _norm(name: str) -> str:
    if not name:
        return ""
    nk = unicodedata.normalize("NFKD", name)
    a = "".join(c for c in nk if not unicodedata.combining(c))
    a = re.sub(r"[^a-zA-Z\s]", " ", a).lower()
    a = re.sub(r"\s+", " ", a).strip()
    a = re.sub(r"\s+(jr|sr|ii|iii|iv|v)\b", "", a)
    return a


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(_DB), isolation_level=None)
    c.row_factory = sqlite3.Row
    return c


def _ensure(c) -> None:
    c.execute("""CREATE TABLE IF NOT EXISTS player_prop_logs(
        espn_id INTEGER, game_id TEXT, date TEXT, player_name TEXT,
        team TEXT, position TEXT,
        pass_yds REAL, pass_att REAL, completions REAL,
        rush_yds REAL, carries REAL,
        rec_yds REAL, receptions REAL, targets REAL,
        PRIMARY KEY(espn_id, game_id))""")
    # v2 columns (2026-10-26). offense_snaps is NULL until nflverse publishes the
    # game's snap counts; a player with snaps but no stat line gets an all-zero row
    # (he played -> an Over loses, an Under wins) instead of reading as a DNP.
    have = {r[1] for r in c.execute("PRAGMA table_info(player_prop_logs)")}
    for col, typ in (("season", "INTEGER"), ("week", "INTEGER"), ("season_type", "TEXT"),
                     ("opp", "TEXT"), ("is_home", "INTEGER"), ("offense_snaps", "REAL")):
        if col not in have:
            c.execute("ALTER TABLE player_prop_logs ADD COLUMN %s %s" % (col, typ))
    c.execute("CREATE INDEX IF NOT EXISTS idx_ppl_player_date ON player_prop_logs(espn_id, date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ppl_date_team ON player_prop_logs(date, team)")
    c.execute("CREATE TABLE IF NOT EXISTS _prop_meta(key TEXT PRIMARY KEY, value TEXT)")


def _meta_get(c, k, d=None):
    r = c.execute("SELECT value FROM _prop_meta WHERE key=?", (k,)).fetchone()
    if not r:
        return d
    try:
        return json.loads(r["value"])
    except Exception:
        return d


def last_refresh() -> float:
    try:
        c = _conn(); _ensure(c)
        v = _meta_get(c, "last_refresh", 0); c.close()
        return float(v or 0)
    except Exception:
        return 0.0


# ── Ingest (nflverse) ─────────────────────────────────────────
def _season_file(kind: str, season: int) -> Path:
    if kind == "pbp":
        return _PBP_CACHE if season == _SEASON else _DB.parent / ("_nflverse_pbp_%d.parquet" % season)
    return _DB.parent / ("_prop_snaps_%d.parquet" % season)


def _download(url: str, path: Path, timeout: int = 300) -> None:
    import os
    import requests
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "curl/8.4.0"})
    r.raise_for_status()
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(r.content)
    os.replace(tmp, path)


def _season_frame(pd, kind: str, season: int, force: bool):
    """One season's PBP / snap-count parquet: past seasons download once, the
    current season re-pulls when older than _REFRESH_TTL (or on force)."""
    path = _season_file(kind, season)
    url = (_PBP_URL_T if kind == "pbp" else _SNAPS_URL_T) % season
    stale = (not path.exists()) or (season == _SEASON and (
        force or (time.time() - path.stat().st_mtime) >= _REFRESH_TTL))
    if stale:
        try:
            _download(url, path)
        except Exception as e:
            if not path.exists():
                raise
            logger.warning("nfl player_props: %s %d refresh failed, using cache: %s", kind, season, e)
    if kind == "pbp":
        return pd.read_parquet(path, columns=_PBP_COLS)
    return pd.read_parquet(path)


# ── Ingest (nflverse) ─────────────────────────────────────────
def refresh(force: bool = False) -> dict:
    """Ingest nflverse PBP (2024-26) + snap counts -> one row per player-game in
    nfl_player.db ``player_prop_logs``: passing/rushing/receiving yards,
    receptions, attempts/carries/targets, opponent, home/away and offense snaps.
    Two-point tries and sacks are excluded from the official stat lines. Throttled
    by _REFRESH_TTL. pandas is imported LAZILY (only this path needs it)."""
    with _lock:
        if not force and (time.time() - last_refresh()) < _REFRESH_TTL:
            return {"skipped": "fresh"}
        try:
            import pandas as pd
            import requests
        except Exception as e:
            logger.warning("nfl player_props: pandas/requests unavailable: %s", e)
            return {"error": "deps unavailable"}

        pbp = pd.concat([_season_frame(pd, "pbp", s, force) for s in _SEASONS],
                        ignore_index=True)
        snaps = []
        for s in _SEASONS:
            try:
                snaps.append(_season_frame(pd, "snaps", s, force))
            except Exception as e:
                logger.warning("nfl player_props: snap counts %d unavailable: %s", s, e)
        pr = requests.get(_PLAYERS_URL, timeout=120, headers={"User-Agent": "curl/8.4.0"})
        pr.raise_for_status()
        players = pd.read_parquet(io.BytesIO(pr.content)).dropna(subset=["gsis_id"])
        gsis_espn, gsis_name, gsis_pos, pfr_gsis = {}, {}, {}, {}
        for g, e, nm, pos, pfr in zip(players["gsis_id"], players.get("espn_id"),
                                      players.get("display_name"), players.get("position"),
                                      players.get("pfr_id", pd.Series([None] * len(players)))):
            if e is not None and str(e) not in ("nan", "None", ""):
                try:
                    gsis_espn[g] = int(float(e))
                except (TypeError, ValueError):
                    pass
            gsis_name[g] = nm or ""
            gsis_pos[g] = (pos or "")
            if pfr is not None and str(pfr) not in ("nan", "None", ""):
                pfr_gsis[str(pfr)] = g

        def _num(col):
            return pd.to_numeric(pbp[col], errors="coerce").fillna(0.0)

        pbp = pbp[pbp["game_id"].notna()].copy()
        for col in ("passing_yards", "rushing_yards", "receiving_yards", "complete_pass",
                    "pass_attempt", "rush_attempt", "sack", "two_point_attempt"):
            pbp[col] = _num(col)
        meta = pbp.groupby("game_id").agg(
            season=("season", "first"), week=("week", "first"),
            season_type=("season_type", "first"), date=("game_date", "first"),
            home_team=("home_team", "first"), away_team=("away_team", "first")).reset_index()
        plays = pbp[pbp["two_point_attempt"] == 0]
        pas = plays[plays["passer_player_id"].notna()]
        g_pass = pas.groupby(["passer_player_id", "game_id"]).agg(
            team=("posteam", "last"), opp=("defteam", "last"),
            pass_yds=("passing_yards", "sum"), pass_att=("pass_attempt", "sum"),
            sacks=("sack", "sum"), completions=("complete_pass", "sum"),
        ).reset_index().rename(columns={"passer_player_id": "gsis"})
        g_pass["pass_att"] = g_pass["pass_att"] - g_pass["sacks"]
        g_pass = g_pass.drop(columns=["sacks"])
        rush = plays[plays["rusher_player_id"].notna()]
        g_rush = rush.groupby(["rusher_player_id", "game_id"]).agg(
            team=("posteam", "last"), opp=("defteam", "last"),
            rush_yds=("rushing_yards", "sum"), carries=("rush_attempt", "sum"),
        ).reset_index().rename(columns={"rusher_player_id": "gsis"})
        rec = plays[plays["receiver_player_id"].notna()]
        g_rec = rec.groupby(["receiver_player_id", "game_id"]).agg(
            team=("posteam", "last"), opp=("defteam", "last"),
            rec_yds=("receiving_yards", "sum"), receptions=("complete_pass", "sum"),
            targets=("pass_attempt", "sum"),
        ).reset_index().rename(columns={"receiver_player_id": "gsis"})
        merged = g_pass.merge(g_rush, on=["gsis", "game_id"], how="outer", suffixes=("", "_r"))
        merged = merged.merge(g_rec, on=["gsis", "game_id"], how="outer", suffixes=("", "_c"))
        for base in ("team", "opp"):
            for suf in ("_r", "_c"):
                if base + suf in merged.columns:
                    merged[base] = merged[base].fillna(merged[base + suf])
        merged = merged[[c for c in merged.columns if not c.endswith(("_r", "_c"))]]
        stat_cols = ["pass_yds", "pass_att", "completions", "rush_yds", "carries",
                     "rec_yds", "receptions", "targets"]
        for col in stat_cols:
            if col not in merged.columns:
                merged[col] = 0.0
            merged[col] = merged[col].fillna(0.0)
        merged["espn_id"] = merged["gsis"].map(gsis_espn)
        merged = merged.dropna(subset=["espn_id"])
        merged["espn_id"] = merged["espn_id"].astype(int)

        # Snap counts: offense snaps per (espn_id, game); players who took snaps but
        # never touched the ball get an all-zero row.
        snap_map, snap_games = {}, set()
        if snaps:
            sn = pd.concat(snaps, ignore_index=True)
            sn = sn[pd.to_numeric(sn["offense_snaps"], errors="coerce").fillna(0) > 0]
            for gid, pfr, team, opp, osn, pos in zip(sn["game_id"], sn["pfr_player_id"], sn["team"],
                                                     sn["opponent"], sn["offense_snaps"], sn["position"]):
                snap_games.add(gid)
                g = pfr_gsis.get(str(pfr))
                eid = gsis_espn.get(g) if g else None
                if eid is None or str(pos or "").upper() not in _SKILL_POS:
                    continue
                snap_map[(int(eid), gid)] = (float(osn), g, team, opp)
        have_rows = set(zip(merged["espn_id"], merged["game_id"]))
        pbp_games = set(meta["game_id"])
        extra = [{"gsis": g, "game_id": gid, "team": team, "opp": opp, "espn_id": eid,
                  **{col: 0.0 for col in stat_cols}}
                 for (eid, gid), (osn, g, team, opp) in snap_map.items()
                 if (eid, gid) not in have_rows and gid in pbp_games]
        if extra:
            merged = pd.concat([merged, pd.DataFrame(extra)], ignore_index=True)
        merged = merged.merge(meta, on="game_id", how="left")
        merged["is_home"] = (merged["team"] == merged["home_team"]).astype(int)
        merged["player_name"] = merged["gsis"].map(gsis_name).fillna("")
        merged["position"] = merged["gsis"].map(gsis_pos).fillna("")

        def _snap(eid, gid):
            v = snap_map.get((int(eid), gid))
            if v is not None:
                return v[0]
            return 0.0 if gid in snap_games else None

        c = _conn(); _ensure(c)
        c.execute("BEGIN")
        c.execute("DELETE FROM player_prop_logs")
        for r_ in merged.itertuples(index=False):
            c.execute(
                "INSERT OR REPLACE INTO player_prop_logs(espn_id, game_id, date,"
                " player_name, team, position, pass_yds, pass_att, completions,"
                " rush_yds, carries, rec_yds, receptions, targets,"
                " season, week, season_type, opp, is_home, offense_snaps)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (int(r_.espn_id), str(r_.game_id), str(r_.date),
                 str(r_.player_name), str(r_.team), str(r_.position),
                 float(r_.pass_yds), float(r_.pass_att), float(r_.completions),
                 float(r_.rush_yds), float(r_.carries),
                 float(r_.rec_yds), float(r_.receptions), float(r_.targets),
                 int(r_.season) if r_.season == r_.season else None,
                 int(r_.week) if r_.week == r_.week else None,
                 str(r_.season_type), str(r_.opp), int(r_.is_home),
                 _snap(r_.espn_id, r_.game_id)))
        c.execute("INSERT OR REPLACE INTO _prop_meta(key,value) VALUES('last_refresh',?)",
                  (json.dumps(time.time()),))
        c.execute("COMMIT")
        rows = c.execute("SELECT COUNT(*), COUNT(DISTINCT espn_id), MAX(date) FROM player_prop_logs").fetchone()
        c.close()
        return {"rows": rows[0], "players": rows[1], "last_date": rows[2],
                "zero_rows_from_snaps": len(extra)}


def _ensure_refresh() -> None:
    """Throttled background refresh (mirrors redzone._ensure_redzone)."""
    now = time.time()
    if now - _rz_check["ts"] < 3600:
        return
    _rz_check["ts"] = now

    def _run():
        try:
            refresh()
        except Exception as e:
            logger.warning("nfl player_props: refresh failed: %s", e)

    threading.Thread(target=_run, daemon=True).start()


# ── Projection + probability ──────────────────────────────────
def _name_index() -> dict:
    """normalized name -> {espn_id, position, team, last_date} (most recent)."""
    c = _conn(); _ensure(c)
    rows = c.execute(
        "SELECT espn_id, player_name, position, team, MAX(date) d "
        "FROM player_prop_logs GROUP BY espn_id").fetchall()
    c.close()
    idx: dict[str, dict] = {}
    for r in rows:
        cand = {"espn_id": int(r["espn_id"]), "position": (r["position"] or "").upper(),
                "team": (r["team"] or "").upper(), "last_date": r["d"] or ""}
        for key in {_norm(r["player_name"])} | _initial_keys(_norm(r["player_name"])):
            if not key:
                continue
            cur = idx.get(key)
            if cur is None or cand["last_date"] > cur["last_date"]:
                idx[key] = cand
    return idx


def _initial_keys(norm: str) -> set:
    parts = norm.split()
    if len(parts) >= 2:
        return {f"{parts[0][0]} {' '.join(parts[1:])}"}
    return set()


def _observations(espn_id: int, stat_key: str) -> list[float]:
    c = _conn(); _ensure(c)
    rows = c.execute(
        "SELECT %s v FROM player_prop_logs WHERE espn_id=? ORDER BY date DESC LIMIT ?"
        % stat_key, (int(espn_id), PROJ_GAMES)).fetchall()
    c.close()
    return [float(r["v"] or 0.0) for r in rows]


def _project(espn_id: int, stat_key: str) -> dict | None:
    """Recency-weighted mean + sd for a stat. Returns None if too few games."""
    obs = _observations(espn_id, stat_key)
    if len(obs) < MIN_GAMES:
        return None
    n = len(obs)
    # Recency weights (newest heaviest): most-recent first.
    w = [0.85 ** i for i in range(n)]
    sw = sum(w)
    mean = sum(o * wi for o, wi in zip(obs, w)) / sw
    var = sum(wi * (o - mean) ** 2 for o, wi in zip(obs, w)) / sw
    sd = math.sqrt(max(var, 0.0))
    floor = _SD_FLOOR_FRAC.get(stat_key)
    if floor:
        sd = max(sd, floor * max(mean, 1.0), 1.0)
    else:  # counts
        sd = max(sd, math.sqrt(max(mean, 0.5)))
    return {"mean": round(mean, 2), "sd": round(sd, 2), "games": n}


def _prob_ge(mean: float, sd: float, threshold: float, kind: str) -> float:
    """P(stat >= threshold). Yards: Normal (continuity-corrected). Counts:
    Poisson."""
    if kind == "count":
        # P(N >= ceil(threshold)) under Poisson(mean).
        import math as _m
        k = int(math.ceil(threshold))
        if mean <= 0:
            return 0.0 if k > 0 else 1.0
        # survival = 1 - CDF(k-1)
        cdf = 0.0
        term = math.exp(-mean)
        cdf += term
        for i in range(1, k):
            term *= mean / i
            cdf += term
        return max(0.0, min(1.0, 1.0 - cdf))
    # Normal survival with 0.5 continuity correction on the threshold.
    if sd <= 0:
        return 1.0 if mean >= threshold else 0.0
    z = (threshold - 0.5 - mean) / sd
    return max(0.0, min(1.0, 0.5 * math.erfc(z / math.sqrt(2.0))))


def _implied(odds) -> float | None:
    if odds is None:
        return None
    try:
        n = float(odds)
    except (TypeError, ValueError):
        return None
    if n == 0:
        return None
    return 100.0 / (n + 100.0) if n > 0 else abs(n) / (abs(n) + 100.0)


def _confidence_for(edge):
    if edge is None:
        return None
    if edge >= 10:
        return "strong"
    if edge >= 6:
        return "moderate"
    if edge >= PROP_MIN_EDGE_PCT:
        return "lean"
    return "skip"


# ── Picker ────────────────────────────────────────────────────
_ESPN_HEAD = "https://a.espncdn.com/i/headshots/nfl/players/full/{}.png"
_last_gen = {"ts": 0.0, "date": None, "result": None}
_GEN_TTL = 120
_GEN_LOCK = threading.Lock()


def project_slate(target_date: str | None = None) -> dict:
    """Price HR's NFL player-prop thresholds against our projections; return the
    best-edge prop per (player, market). One entry per qualifying prop."""
    today = target_date or et_today_str()
    _ensure_refresh()
    idx = _name_index()

    hr_props = []
    try:
        from scrapers.hardrock_props import fetch_nfl_player_props
        hr_props = fetch_nfl_player_props() or []
    except Exception as e:
        logger.warning("nfl player_props: HR fetch failed: %s", e)

    # team logos (read-only from game model)
    from engine.football._db import get_conn as _fb
    logo = {}
    try:
        for tr in _fb("nfl").execute("SELECT abbreviation, logo_url FROM teams").fetchall():
            logo[(tr["abbreviation"] or "").upper()] = tr["logo_url"]
    except Exception:
        pass

    proj_cache: dict = {}
    best: dict[tuple, dict] = {}   # (espn_id, bet_type) -> best pick
    for p in hr_props:
        bt = p.get("bet_type")
        if bt not in MARKETS:
            continue
        stat_key, kind = MARKETS[bt]
        line = p.get("line")
        yes_odds = p.get("yes_odds")
        if line is None:
            continue
        cand = idx.get(_norm(p.get("player_name") or ""))
        if not cand:
            continue
        eid = cand["espn_id"]
        pk = (eid, stat_key)
        if pk not in proj_cache:
            proj_cache[pk] = _project(eid, stat_key)
        proj = proj_cache[pk]
        if not proj:
            continue
        prob = _prob_ge(proj["mean"], proj["sd"], float(line), kind)
        implied = _implied(yes_odds)
        edge = round((prob - implied) * 100.0, 2) if implied is not None else None
        entry = {
            "player_id": eid, "player_name": p.get("player_name"),
            "position": cand["position"] or "", "team": cand["team"] or p.get("team"),
            "matchup": p.get("event_name") or "", "date": today,
            "bet_type": bt, "market": bt, "stat_key": stat_key,
            "line": float(line), "side": _SIDE,
            "projection": proj["mean"], "proj_sd": proj["sd"], "games": proj["games"],
            "model_prob": round(prob, 4), "odds": yes_odds,
            "implied_prob": round(implied, 4) if implied is not None else None,
            "edge": edge,
            "plus_ev": bool(edge is not None and edge >= PROP_MIN_EDGE_PCT),
            "headshot": _ESPN_HEAD.format(eid),
            "team_logo": logo.get((cand["team"] or "").upper()),
        }
        cur = best.get(pk)
        # keep the threshold with the highest edge (priced), else closest to a
        # coin-flip projection (most informative) when unpriced.
        key = (entry["edge"] if entry["edge"] is not None else -999,
               -abs(prob - 0.5))
        if cur is None or key > (cur["edge"] if cur["edge"] is not None else -999,
                                 -abs(cur["model_prob"] - 0.5)):
            best[pk] = entry

    picks = list(best.values())
    picks.sort(key=lambda x: (x["edge"] is not None,
                              x["edge"] if x["edge"] is not None else -999,
                              x["model_prob"]), reverse=True)
    return {"date": today, "have_odds": bool(hr_props),
            "count": len(picks), "picks": picks}


def _record(picks: list[dict]) -> int:
    conn = _conn_for("nfl")
    rec = 0
    for pk in picks:
        if pk.get("edge") is None or pk["edge"] < PROP_MIN_EDGE_PCT:
            continue   # only record qualifying (priced, +edge) props as paper bets
        # game_id = the slate date (settlement matches player_prop_logs by
        # espn_id + date>=game_id). line stored as threshold-0.5 so the shared
        # Over-semantics settler (actual > line) grades an "N+" threshold as a
        # win at actual>=N. model_prob was already P(stat >= threshold).
        pid = insert_pick(
            "nfl", game_id=pk["date"], date=pk["date"],
            matchup=pk.get("matchup") or "", player_id=int(pk["player_id"]),
            player_name=pk["player_name"], bet_type=pk["bet_type"],
            pick="%s %d+" % (pk["bet_type"], int(pk["line"])),
            line=float(pk["line"]) - 0.5, side=_SIDE, model_prob=pk["model_prob"],
            edge=pk["edge"], odds=pk["odds"], confidence=_confidence_for(pk["edge"]))
        if pid:
            rec += 1
    return rec


def generate_picks(target_date: str | None = None, force: bool = False) -> dict:
    today = target_date or et_today_str()
    now = time.time()
    if (not force and _last_gen["result"] is not None
            and _last_gen["date"] == today and (now - _last_gen["ts"]) < _GEN_TTL):
        return _last_gen["result"]
    with _GEN_LOCK:
        now = time.time()
        if (not force and _last_gen["result"] is not None
                and _last_gen["date"] == today and (now - _last_gen["ts"]) < _GEN_TTL):
            return _last_gen["result"]
        out = project_slate(today)
        try:
            out["recorded"] = _record(out["picks"])
        except Exception as e:
            logger.warning("nfl player_props: record failed: %s", e)
            out["recorded"] = 0
        _last_gen.update({"ts": now, "date": today, "result": out})
        return out


__all__ = ["refresh", "last_refresh", "project_slate", "generate_picks"]
