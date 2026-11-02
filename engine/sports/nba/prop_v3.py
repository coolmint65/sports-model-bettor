"""NBA player props v3 -- Austin's "NBA props V2" (MintyBot 2026-11-02, "NBA needs V2" / "Player props v2").

Research (Pi ~/pibot/data/research/nbav2, run on the Beelink): as-of minutes / per-minute-rate / starter / rest / team +
opponent / game-line / vacated-minutes features from nba.db's box scores, LightGBM poisson per base stat (pts reb ast
fg3m stl blk tov; combos = sums), walk-forward in 2-month blocks over 2024-25..2026-27. Graded on DK lines only (the
warehouse's ESPN BET prop sides are scrambled) with only pregame-known actives:
  * log-loss vs DK: open .6846 -> .6818 (blend w .35), close .6833 -> .6824 (market-level)
  * the picker's rule (w .35, edge 4, p .40, raw band .2-.8): OPEN 849-685, 55.3% vs 52.1% BE, +6.5% +/-2.5 (both
    seasons); CLOSE +1.3% +/-2.4 -- an early-price edge, so it runs in SHADOW behind prop_picks' card gate.
WHO PLAYS = only what's known before the game, exactly as trained: last game's actives fill the team's minute sums, and
a rotation player who missed the last game is counted as vacated (the injury report isn't in the features; the picker
still skips ESPN 'Out' players).
Pipeline (scripts/nba_v3_build.bat, scheduled; build() is idempotent): features over every logged game + one synthetic
row per expected active for each team's next game (HR's live spread / total from the odds warehouse), models retrained
when older than RETRAIN_DAYS, projections for the slate -> nba.db nbav3_preds, finished projections graded into the
neighbour pool.  python -m engine.sports.nba.prop_v3 build [--retrain]
"""
from __future__ import annotations

import bisect
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
D3 = os.path.join(ROOT, "data", "nba_v3")
WH = os.path.join(ROOT, "data", "warehouse", "wh.db")
RETRAIN_DAYS = 6
BASE = ["pts", "reb", "ast", "fg3m", "stl", "blk", "tov"]
COMBO = {"pra": ("pts", "reb", "ast"), "pr": ("pts", "reb"), "pa": ("pts", "ast"), "ar": ("ast", "reb")}
STATS = BASE + list(COMBO)
PARAMS = dict(objective="poisson", learning_rate=0.03, num_leaves=15, min_data_in_leaf=60, feature_fraction=0.7,
              bagging_fraction=0.8, bagging_freq=1, lambda_l2=5.0, verbose=-1, num_threads=2, seed=11)
ROUNDS = 500
SW = {0: 1.0, 1: 1.0, 2: 0.5}
_S = ["min", "pts", "reb", "ast", "fg3m", "stl", "blk", "tov", "oreb", "dreb", "ftm", "starter"]


def _nba():
    from ...nba_db import get_conn
    return get_conn()


# ------------------------------------------------------------------ data
def _box():
    import pandas as pd
    c = _nba()
    rows = c.execute("SELECT l.player_id, l.player_name, CAST(l.game_id AS TEXT), l.date, l.team_id, l.opp_team_id, "
                     "l.is_home, l.stats_json FROM player_game_logs l").fetchall()
    recs = []
    for pid, nm, gid, d, tid, oid, home, sj in rows:
        try:
            s = json.loads(sj or "{}")
        except ValueError:
            continue
        recs.append(dict(player_id=int(pid), name=nm, game_id=str(gid), date=str(d)[:10], team_id=tid, opp_id=oid,
                         is_home=int(home or 0), min=s.get("min"), pts=s.get("pts"), reb=s.get("reb"), ast=s.get("ast"),
                         fg3m=s.get("tpm"), stl=s.get("stl"), blk=s.get("blk"), tov=s.get("to"), oreb=s.get("oreb"),
                         dreb=s.get("dreb"), ftm=s.get("ftm"), starter=s.get("starter")))
    b = pd.DataFrame(recs)
    for col in _S[:-1]:
        b[col] = pd.to_numeric(b[col], errors="coerce")
    b["starter"] = b.starter.map(lambda v: 1.0 if v in (1, True, "1", "true", "True") else 0.0)
    g = pd.DataFrame([tuple(r) for r in c.execute("SELECT CAST(game_id AS TEXT), date, season, home_team_id, away_team_id, "
                                                  "status, start_time FROM nba_games").fetchall()],
                     columns=["game_id", "gdate", "season", "home_id", "away_id", "status", "start_time"])
    b = b.merge(g[["game_id", "season"]], on="game_id", how="left")
    stype = _stypes()
    b["stype"] = b.game_id.map(stype)
    early = b.stype.isna() & (b.date < "2024-06-30")                      # 2023-24 predates the warehouse
    b.loc[early, "stype"] = (b.loc[early, "date"] < "2023-10-24").map({True: 1, False: 2})
    b["stype"] = b.stype.fillna(2)
    b["pos"] = b.player_id.map(dict(c.execute("SELECT player_id, position FROM nba_players").fetchall()))
    b["synthetic"] = 0
    return b, g


def _wh():
    return sqlite3.connect(f"file:{WH}?mode=ro", uri=True, timeout=30)


def _stypes() -> dict:
    try:
        w = _wh()
        out = {str(e): s for e, s in w.execute("SELECT espn_id, stype FROM events WHERE league='nba'")}
        w.close()
        return out
    except Exception as e:
        logger.warning("nba v3: warehouse game types unavailable: %s", e)
        return {}


def _game_lines():
    """{game_id: (home spread, total)}: DK / ESPN BET OPEN (else close) for finished games -- the research's lines --
    and Hard Rock's latest main line (live feed) for games not yet in the backfill."""
    out = {}
    try:
        w = _wh()
        rows = w.execute(
            "SELECT e.espn_id, m.family, q.book, q.src, q.side, q.l100, q.ts FROM quotes q JOIN markets m ON m.id=q.market_id "
            "JOIN events e ON e.id=m.event_id WHERE e.league='nba' AND m.family IN ('SPREAD','TOTAL') AND m.period='FG' "
            "AND m.subject=0 AND q.main=1 AND q.price IS NOT NULL AND ((q.book IN (2,3) AND q.src IN (2,3)) OR q.book=1) "
            "AND ((m.family='SPREAD' AND q.side=1) OR (m.family='TOTAL' AND q.side=4))").fetchall()
        w.close()
    except Exception as e:
        logger.warning("nba v3: warehouse game lines unavailable: %s", e)
        return out
    rank = {}
    for eid, fam, book, src, side, l100, ts in rows:
        # research order: DK open, ESPN BET open, DK close, ESPN BET close; HR live (latest ts) only as the fallback
        r = (0 if book in (2, 3) else 1, 0 if src == 2 else 1, book if book in (2, 3) else 9, -(ts or 0))
        k = (str(eid), fam)
        if k not in rank or r < rank[k][0]:
            rank[k] = (r, l100 / 100.0)
    for (eid, fam), (_, v) in rank.items():
        sp, tt = out.get(eid, (None, None))
        out[eid] = (v, tt) if fam == "SPREAD" else (sp, v)
    return out


def _upcoming(b, g, horizon_h=36):
    """One synthetic row per expected active (last game's actives) for each team's next unplayed game."""
    import pandas as pd
    now = datetime.now(timezone.utc)
    have = set(b.game_id)
    up = []
    for r in g.itertuples(index=False):
        if r.game_id in have or str(r.status or "").lower() in ("final", "in", "live", "postponed", "canceled"):
            continue
        try:
            st = datetime.fromisoformat(str(r.start_time).replace("Z", "+00:00"))
        except Exception:
            continue
        if not (now - timedelta(hours=1) <= st <= now + timedelta(hours=horizon_h)):
            continue
        up.append((st, r))
    up.sort(key=lambda x: x[0])
    seen, rows = set(), []
    hist = b[(b.synthetic == 0) & (b.stype != 1) & b["min"].notna() & (b["min"] > 0)]
    for st, r in up:
        if {r.home_id, r.away_id} & seen:
            continue                                     # a team's NEXT game only (a second would sit on an empty row)
        seen |= {r.home_id, r.away_id}
        gdate = st.strftime("%Y-%m-%d")      # the box scores' convention: the UTC date of tip-off (not the ET slate date)
        for team, opp, home in ((r.home_id, r.away_id, 1), (r.away_id, r.home_id, 0)):
            past = hist[hist.team_id == team]
            if past.empty:
                continue
            last = past[past.date == past.date.max()]
            for p in last.itertuples(index=False):
                d = {col: float("nan") for col in _S}
                d.update(player_id=p.player_id, name=p.name, game_id=r.game_id, date=gdate, team_id=team, opp_id=opp,
                         is_home=home, season=r.season, stype=2, pos=p.pos, synthetic=1)
                rows.append(d)
    if not rows:
        return b, []
    u = pd.DataFrame(rows)
    return pd.concat([b, u[[c for c in b.columns if c in u.columns]]], ignore_index=True), sorted({x["game_id"] for x in rows})


# ------------------------------------------------------------------ features (research model.py, ACT=honest)
def features(b, lines):
    import numpy as np
    import pandas as pd
    syn = b.synthetic == 1
    b = b[syn | ((b.stype != 1) & b["min"].notna() & (b["min"] > 0))].copy()
    b = b.sort_values(["date", "game_id"]).reset_index(drop=True)
    dt = pd.to_datetime(b.date)
    b["season"] = b.season.fillna(dt.dt.year.where(dt.dt.month >= 8, dt.dt.year - 1)).astype(int)
    b["post"] = (b.stype.isin([3, 5])).astype(float)
    syn = b.synthetic == 1
    for col in _S:
        b[col] = b[col].astype(float)
        b.loc[~syn, col] = b.loc[~syn, col].fillna(0.0)
    tm = b.groupby(["game_id", "team_id"]).agg(date=("date", "first"), opp_id=("opp_id", "first"), season=("season", "first"),
                                               syn=("synthetic", "max"), t_min=("min", "sum"),
                                               **{f"t_{c}": (c, "sum") for c in _S[1:-1]}).reset_index()
    for col in [c for c in tm.columns if c.startswith("t_")]:
        tm.loc[tm.syn == 1, col] = np.nan                  # an unplayed game has no totals
    AL = ["pts", "reb", "ast", "fg3m", "tov", "oreb", "stl", "blk"]
    op = tm[["game_id", "team_id"] + [f"t_{c}" for c in AL]].rename(
        columns={"team_id": "opp_id", **{f"t_{c}": f"a_{c}" for c in AL}})
    tm = tm.merge(op, on=["game_id", "opp_id"], how="left")
    tm["g_total"] = tm.t_pts + tm.a_pts
    tm = tm.sort_values(["team_id", "date"]).reset_index(drop=True)

    def ewm_asof(df, by, cols, hl, prefix):
        sh = df.groupby(by, sort=False)[cols].shift(1)
        sh[by] = df[by].values
        r = sh.groupby(by, sort=False)[cols].ewm(halflife=hl, min_periods=1).mean().reset_index(level=0, drop=True)
        for col in cols:
            df[prefix + col] = r[col].reindex(df.index)
        return df
    OFF = ["t_pts", "t_reb", "t_ast", "t_fg3m", "t_tov", "t_oreb", "g_total"]
    tm = ewm_asof(tm, "team_id", OFF, 8.0, "o_")
    DEF = ["a_" + c for c in AL]
    tm = ewm_asof(tm, "team_id", DEF, 10.0, "d_")
    tm["tg_n"] = tm.groupby(["team_id", "season"]).cumcount()
    od = tm[["game_id", "team_id"] + ["d_" + c for c in DEF] + ["o_g_total"]].rename(
        columns={"team_id": "opp_id", **{"d_" + c: "od_" + c for c in DEF}, "o_g_total": "opp_o_g_total"})
    tm = tm.merge(od, on=["game_id", "opp_id"], how="left")
    tm = tm.sort_values(["date", "game_id"]).reset_index(drop=True)
    for col in DEF:
        lm = tm.groupby("season")["od_" + col].transform(lambda s: s.expanding().mean().shift(1))
        tm["odr_" + col] = tm["od_" + col] / lm
    b = b.merge(tm[["game_id", "team_id"] + ["o_" + c for c in OFF] + ["od_" + c for c in DEF] + ["odr_" + c for c in DEF]
                   + ["opp_o_g_total", "tg_n"]], on=["game_id", "team_id"], how="left")
    b["spread_h"] = b.game_id.map(lambda x: (lines.get(x) or (None, None))[0]).astype(float)
    b["total"] = b.game_id.map(lambda x: (lines.get(x) or (None, None))[1]).astype(float)
    b["spread"] = np.where(b.is_home == 1, -b.spread_h, b.spread_h)
    b["implied"] = (b.total + b.spread) / 2
    b = b.sort_values(["player_id", "date", "game_id"]).reset_index(drop=True)
    b = ewm_asof(b, "player_id", ["min", "starter"], 3.0, "s_")
    b = ewm_asof(b, "player_id", ["min", "starter"], 10.0, "l_")
    R = ["pts", "reb", "ast", "fg3m", "stl", "blk", "tov", "oreb", "dreb", "ftm"]
    for hl, pf in ((6.0, "r6_"), (20.0, "r20_")):
        tmp = b[["player_id", "min"] + R].copy()
        sh = tmp.groupby("player_id")[["min"] + R].shift(1)
        sh["player_id"] = tmp.player_id.values
        e = sh.groupby("player_id")[["min"] + R].ewm(halflife=hl, min_periods=1).mean().reset_index(level=0, drop=True).reindex(b.index)
        for col in R:
            b[pf + col] = e[col] / e["min"].where(e["min"] > 0)
    b["cum_min"] = b.groupby("player_id")["min"].shift(1).fillna(0).groupby(b.player_id).cumsum()
    b["pos_g"] = b.pos.fillna("").str[0].map({"G": 0, "F": 1, "C": 2}).fillna(1)
    first = b.date < "2024-07-01"
    for col in R:
        pri = b.pos_g.map(b[first].groupby("pos_g")["r20_" + col].median())
        n = b.cum_min.clip(upper=3000)
        b["rs_" + col] = (b["r20_" + col].fillna(pri) * n + pri * 300) / (n + 300)
    b["last_min"] = b.groupby("player_id")["min"].shift(1)
    b["last_starter"] = b.groupby("player_id")["starter"].shift(1)
    b["n_g"] = b.groupby("player_id").cumcount()
    b["n_gs"] = b.groupby(["player_id", "season"]).cumcount()
    b["days_since"] = (pd.to_datetime(b.date) - pd.to_datetime(b.groupby("player_id").date.shift(1))).dt.days
    b["b2b"] = (b.days_since == 1).astype(float)
    b["same_team"] = (b.groupby("player_id").team_id.shift(1) == b.team_id).astype(float)
    b["x_min"] = b.groupby("player_id")["min"].transform(lambda s: s.ewm(halflife=3.0).mean())
    b["x_use"] = b.x_min * b.r6_pts.fillna(0)
    tgs = tm[["team_id", "game_id", "date"]].sort_values(["team_id", "date"])
    for k in (1, 2):
        tgs[f"prev{k}"] = tgs.groupby("team_id").game_id.shift(k)
    last_by = b.set_index(["player_id", "game_id"])[["x_min", "x_use"]]
    xm, xu = last_by.x_min.to_dict(), last_by.x_use.to_dict()
    pg = b.groupby(["team_id", "game_id"]).player_id.apply(set).to_dict()
    vac = []
    for r in tgs.itertuples(index=False):
        p1 = r.prev1 if isinstance(r.prev1, str) else None
        p2 = r.prev2 if isinstance(r.prev2, str) else None
        s1 = pg.get((r.team_id, p1), set()) if p1 else set()
        s2 = pg.get((r.team_id, p2), set()) if p2 else set()
        vm = vu = 0.0
        for p in s2 - s1:                                  # missed the last game -> assumed still out
            if xm.get((p, p2), 0) >= 12:
                vm += xm[(p, p2)]; vu += xu[(p, p2)]
        asum = sum(xm.get((p, p1), 0) for p in s1) if s1 else np.nan
        vac.append((r.team_id, r.game_id, vm, vu, asum))
    b = b.merge(pd.DataFrame(vac, columns=["team_id", "game_id", "vac_min", "vac_use", "act_sum_min"]),
                on=["team_id", "game_id"], how="left")
    b["renorm"] = (240.0 / b.act_sum_min.where(b.act_sum_min > 120)).clip(0.8, 1.35).fillna(1.0)
    b["x_min_adj"] = b.s_min * b.renorm
    for col in BASE:
        b["x_" + col] = b.x_min_adj * b["rs_" + col]
    b["month"] = pd.to_datetime(b.date).dt.month
    for col, parts in COMBO.items():
        b[col] = sum(b[x] for x in parts)
    return b


def feat_cols(b):
    pre = lambda p: [c for c in b.columns if c.startswith(p)]
    cols = (["s_min", "l_min", "s_starter", "l_starter", "last_min", "last_starter", "cum_min", "n_g", "n_gs", "days_since", "b2b",
             "same_team", "is_home", "post", "month", "pos_g", "vac_min", "vac_use", "act_sum_min", "renorm", "x_min_adj",
             "spread", "total", "implied", "tg_n", "opp_o_g_total"]
            + pre("r6_") + pre("r20_") + pre("rs_") + pre("x_pts") + pre("x_reb") + pre("x_ast") + pre("x_fg3m") + pre("x_stl")
            + pre("x_blk") + pre("x_tov") + pre("o_") + pre("od_") + pre("odr_"))
    out = []
    for col in cols:
        if col in b.columns and col not in out:
            out.append(col)
    return out


def _X(b, cols):
    import numpy as np
    return np.column_stack([b[col].astype(float).values for col in cols])


def train(b) -> dict:
    import lightgbm as lgb
    os.makedirs(D3, exist_ok=True)
    cols = feat_cols(b)
    h = b[(b.synthetic == 0) & (b.n_g >= 3) & (b.s_min.fillna(0) >= 8)]
    X = _X(h, cols)
    meta = {"trained_at": datetime.now(timezone.utc).isoformat(), "through": str(h.date.max()), "features": cols, "n": len(h)}
    for st in BASE:
        lgb.train(PARAMS, lgb.Dataset(X, h[st].values), num_boost_round=ROUNDS).save_model(os.path.join(D3, f"model_{st}.txt"))
    json.dump(meta, open(os.path.join(D3, "models.json"), "w"), indent=1)
    logger.info("nba v3: trained on %d player-games through %s", meta["n"], meta["through"])
    return meta


# ------------------------------------------------------------------ predictions + pool
def _ensure(c):
    c.execute("""CREATE TABLE IF NOT EXISTS nbav3_preds(
        game_id TEXT NOT NULL, player_id INTEGER NOT NULL, stat TEXT NOT NULL, pred REAL, pos_g INTEGER, n_gs INTEGER,
        s_min REAL, game_date TEXT, season INTEGER, built_at TEXT, actual REAL, PRIMARY KEY(game_id, player_id, stat))""")


def build(retrain: bool = False) -> dict:
    import lightgbm as lgb
    t0 = time.time()
    b, g = _box()
    b, up_ids = _upcoming(b, g)
    f = features(b, _game_lines())
    mp = os.path.join(D3, "models.json")
    if retrain or not os.path.exists(mp) or time.time() - os.path.getmtime(mp) > RETRAIN_DAYS * 86400:
        train(f)
    meta = json.load(open(mp))
    models = {st: lgb.Booster(model_file=os.path.join(D3, f"model_{st}.txt")) for st in BASE}
    c = _nba()
    _ensure(c)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    u = f[(f.synthetic == 1) & (f.n_g >= 3)]
    rows = []
    if len(u):
        X = _X(u, meta["features"])
        pr = {st: models[st].predict(X) for st in BASE}
        for col, parts in COMBO.items():
            pr[col] = sum(pr[p] for p in parts)
        for i, r in enumerate(u.itertuples(index=False)):
            for st in STATS:
                rows.append((r.game_id, int(r.player_id), st, round(float(pr[st][i]), 4), int(r.pos_g), int(r.n_gs),
                             None if r.s_min != r.s_min else round(float(r.s_min), 2), r.date, int(r.season), now))
        for gid in set(u.game_id):
            c.execute("DELETE FROM nbav3_preds WHERE game_id = ? AND actual IS NULL", (gid,))
        c.executemany("INSERT OR REPLACE INTO nbav3_preds(game_id, player_id, stat, pred, pos_g, n_gs, s_min, game_date, season, "
                      "built_at) VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
        c.commit()
    hist = f[f.synthetic == 0]
    act = {}
    for st in STATS:
        for gid, pid, v in zip(hist.game_id, hist.player_id, hist[st]):
            if v == v:
                act[(gid, int(pid), st)] = float(v)
    pend = c.execute("SELECT game_id, player_id, stat FROM nbav3_preds WHERE actual IS NULL AND game_date < ?",
                     (datetime.now(timezone.utc).strftime("%Y-%m-%d"),)).fetchall()
    done = [(act[(r[0], int(r[1]), r[2])], r[0], r[1], r[2]) for r in pend if (r[0], int(r[1]), r[2]) in act]
    if done:
        c.executemany("UPDATE nbav3_preds SET actual = ? WHERE game_id = ? AND player_id = ? AND stat = ?", done)
        c.commit()
    _POOL["t"] = 0.0
    _PRED["t"] = 0.0
    out = {"games": up_ids, "predictions": len(rows), "graded": len(done), "trained_through": meta["through"],
           "secs": round(time.time() - t0)}
    logger.info("nba v3 build: %s", out)
    return out


_POOL = {"t": 0.0, "pools": {}, "before": {}}


def _pools():
    if time.time() - _POOL["t"] < 1800 and _POOL["pools"]:
        return _POOL["pools"]
    import pandas as pd
    pools: dict = {}
    seen = set()
    p = os.path.join(D3, "oof_pool.parquet")
    if os.path.exists(p):
        o = pd.read_parquet(p)
        for st, pgr, d, pred, a, s, gid, pid in zip(o.stat, o.pos_g, o.date, o.pred, o.actual, o.season, o.game_id, o.player_id):
            pools.setdefault((st, int(pgr)), []).append((d, float(pred), float(a), int(s)))
            seen.add((str(gid), int(pid), st))
    c = _nba()
    _ensure(c)
    for gid, pid, st, pgr, d, pred, a, s in c.execute(
            "SELECT game_id, player_id, stat, pos_g, game_date, pred, actual, season FROM nbav3_preds WHERE actual IS NOT NULL"):
        if (str(gid), int(pid), st) not in seen:
            pools.setdefault((st, int(pgr)), []).append((d, float(pred), float(a), int(s)))
    for k in pools:
        pools[k].sort()
    _POOL.update(t=time.time(), pools=pools, before={})
    return pools


def probs(stat: str, pos_g: int, pred: float, line: float, game_date: str, season: int):
    """(P(actual > line), P(actual < line), n) from the neighbour pool (research eval.py), or None."""
    import numpy as np
    k = (stat, int(pos_g), game_date)
    if k not in _POOL["before"]:
        P = _pools().get((stat, int(pos_g))) or []
        i = bisect.bisect_left(P, (game_date,))
        sub = sorted((x[1], x[2], x[3]) for x in P[:i])
        _POOL["before"][k] = (np.array([x[0] for x in sub]), np.array([x[1] for x in sub]), np.array([x[2] for x in sub]))
    cs, acts, seas = _POOL["before"][k]
    if len(cs) < 300:
        return None
    rad = max(0.5, 0.05 * pred)
    while True:
        lo = bisect.bisect_left(cs, pred - rad)
        hi = bisect.bisect_right(cs, pred + rad)
        if hi - lo >= 250 or rad > 5 * pred + 20:
            break
        rad *= 1.25
    if hi - lo < 50:
        return None
    w = np.array([SW.get(int(season - s), 0.0) for s in seas[lo:hi]])
    a = acts[lo:hi]
    if w.sum() <= 0:
        return None
    return float(w[a > line].sum() / w.sum()), float(w[a < line].sum() / w.sum()), int(hi - lo)


_PRED = {"t": 0.0, "rows": {}}


def prediction(game_id, player_id, stat: str) -> dict | None:
    if time.time() - _PRED["t"] > 300:
        c = _nba()
        _ensure(c)
        rows = {}
        for gid, pid, st, pred, pgr, ngs, smin, d, s in c.execute(
                "SELECT game_id, player_id, stat, pred, pos_g, n_gs, s_min, game_date, season FROM nbav3_preds WHERE actual IS NULL"):
            rows[(str(gid), int(pid), st)] = {"pred": pred, "pos_g": pgr, "n_gs": ngs, "min": smin, "date": d, "season": s}
        _PRED.update(t=time.time(), rows=rows)
    return _PRED["rows"].get((str(game_id), int(player_id), stat))


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if len(sys.argv) > 1 and sys.argv[1] == "build":
        print(json.dumps(build(retrain="--retrain" in sys.argv)))
