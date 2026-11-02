"""NFL player props v3 (MintyBot 2026-11-02, Austin "Switch NFL") -- the walk-forward research model, live.

Research (Pi ~/pibot/data/research/nflv3, 11-01/02): per-stat LightGBM on as-of player / team / opponent / QB /
vacated-target / game-line features from nflverse play-by-play 2020-now. Walk-forward on 2025 ESPN BET lines it
beat the market's log-loss at the OPEN (.6838 vs .6863) and the close (.6895 vs .6909); the picker's rule (anchor
w .35, edge 4%, p .40, raw band .2-.8) went 116-70, +21.9% +/-7.0 on every qualifying candidate at the open, but only
+11% at the close and ~0% on the capped card. So the edge is an EARLY-WEEK edge, and v3 runs in SHADOW behind the
card gate (prop_picks.shadow_cells): every HR main line is priced and graded at its first-seen price, and a stat x
side reaches the card only once its v3 record proves out.

Pipeline (scripts/nfl_v3_build.bat, scheduled; build() is idempotent):
  1. extract   nflverse pbp + snap counts + players + games.csv -> team-game and player-game tables
  2. upcoming  the next slate (nflverse games with a spread + total, not yet played): one row per team (lines, rest,
               roof, projected starting QB) and one per expected-active skill player (active in either of the team's
               last two games and not ruled Out) -- the research used the real actives, so early-week rows carry
               some extra noise
  3. features  the research's features.py + export.py, unchanged: every feature uses only earlier games
  4. models    LightGBM per stat on every graded player-game (retrained when older than RETRAIN_DAYS)
  5. predict   upcoming rows -> nflv3_preds (props db). probs(): P(actual > L) = weighted share of the neighbour
               pool (same stat x position group, projections dated before the game, >= 250 nearest by projection)
               -- the research OOF projections, plus every live projection once its game is graded.
python -m engine.sports.nfl.prop_v3 build [--retrain]
"""
from __future__ import annotations

import bisect
import json
import logging
import os
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone

logger = logging.getLogger(__name__)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
NV = os.path.join(ROOT, "data", "football", "nflverse")
FB = os.path.join(ROOT, "data", "football")
D3 = os.path.join(FB, "nflv3")
SEASONS = range(2020, 2027)
SEASON = 2026
RETRAIN_DAYS = 6
UA = {"User-Agent": "curl/8.4.0"}
STATS = ["rec_yds", "receptions", "rush_yds", "carries", "rr_yds", "pass_yds", "pass_att", "completions"]
COUNT = {"receptions", "carries", "pass_att", "completions"}
SW = {0: 1.0, 1: 1.0, 2: 0.5}            # neighbour weight by seasons back (research eval_v3_np.py)
PARAMS = dict(learning_rate=0.03, num_leaves=15, min_data_in_leaf=60, feature_fraction=0.7, bagging_fraction=0.8,
              bagging_freq=1, lambda_l2=5.0, verbose=-1, num_threads=2, seed=11)
ROUNDS = 500


def _dl(url, path, max_age_days=None):
    import requests
    if os.path.exists(path) and (max_age_days is None or time.time() - os.path.getmtime(path) < max_age_days * 86400):
        return path
    r = requests.get(url, timeout=300, headers=UA)
    r.raise_for_status()
    tmp = path + ".tmp"
    open(tmp, "wb").write(r.content)
    os.replace(tmp, path)
    return path


# ------------------------------------------------------------------ 1. extract (research extract.py)
def _extract():
    import numpy as np
    import pandas as pd
    os.makedirs(D3, exist_ok=True)
    pl = pd.read_parquet(_dl("https://github.com/nflverse/nflverse-data/releases/download/players/players.parquet",
                             os.path.join(D3, "players.parquet"), max_age_days=3))
    pl = pl.dropna(subset=["gsis_id"])
    snaps = []
    for s in SEASONS:
        p = os.path.join(FB, f"_prop_snaps_{s}.parquet")
        if not os.path.exists(p):
            p = _dl(f"https://github.com/nflverse/nflverse-data/releases/download/snap_counts/snap_counts_{s}.parquet",
                    os.path.join(D3, f"snaps_{s}.parquet"))
        snaps.append(pd.read_parquet(p))
    sn = pd.concat(snaps, ignore_index=True)
    cols = ["play_id", "game_id", "season", "week", "season_type", "game_date", "posteam", "defteam", "home_team",
            "away_team", "passer_player_id", "rusher_player_id", "receiver_player_id", "passing_yards", "rushing_yards",
            "receiving_yards", "complete_pass", "pass_attempt", "rush_attempt", "sack", "two_point_attempt",
            "qb_dropback", "qb_scramble", "qb_kneel", "qb_spike", "air_yards", "yardline_100", "down", "wp", "qtr",
            "half_seconds_remaining", "epa", "play_type", "score_differential"]
    pbp = pd.concat([pd.read_parquet(os.path.join(NV, f"play_by_play_{s}.parquet"), columns=cols) for s in SEASONS
                     if os.path.exists(os.path.join(NV, f"play_by_play_{s}.parquet"))], ignore_index=True)
    pbp = pbp[pbp.game_id.notna() & pbp.posteam.notna()]
    for c in ["passing_yards", "rushing_yards", "receiving_yards", "complete_pass", "pass_attempt", "rush_attempt",
              "sack", "two_point_attempt", "qb_dropback", "qb_scramble", "qb_kneel", "qb_spike", "air_yards", "epa"]:
        pbp[c] = pd.to_numeric(pbp[c], errors="coerce").fillna(0.0)
    pbp = pbp[pbp.two_point_attempt == 0]
    pbp = pbp[pbp.play_type.isin(["pass", "run", "qb_kneel", "qb_spike"])]
    pbp["scrim"] = ((pbp.play_type.isin(["pass", "run"])) & (pbp.qb_kneel == 0) & (pbp.qb_spike == 0)).astype(float)
    pbp["neutral"] = ((pbp.down.isin([1, 2])) & (pbp.wp.between(0.2, 0.8)) & (pbp.qtr <= 4)
                      & ~((pbp.qtr.isin([2, 4])) & (pbp.half_seconds_remaining < 120))).astype(float) * pbp.scrim
    pbp["rz"] = (pbp.yardline_100 <= 20).astype(float)
    pbp["i10"] = (pbp.yardline_100 <= 10).astype(float)
    pbp["i5"] = (pbp.yardline_100 <= 5).astype(float)
    pbp["tgt"] = pbp.receiver_player_id.notna().astype(float) * (pbp.sack == 0)
    pbp["ay"] = np.where(pbp.tgt > 0, pbp.air_yards, 0.0)
    pbp["patt"] = pbp.pass_attempt - pbp.sack
    tg = pbp.groupby(["game_id", "posteam"]).agg(
        season=("season", "first"), week=("week", "first"), season_type=("season_type", "first"),
        date=("game_date", "first"), opp=("defteam", "first"), home_team=("home_team", "first"),
        plays=("scrim", "sum"), dropbacks=("qb_dropback", "sum"), patt=("patt", "sum"), sacks=("sack", "sum"),
        carries=("rush_attempt", "sum"), targets=("tgt", "sum"), air=("ay", "sum"),
        pass_yds=("passing_yards", "sum"), rush_yds=("rushing_yards", "sum"), comp=("complete_pass", "sum"),
        neu_plays=("neutral", "sum")).reset_index().rename(columns={"posteam": "team"})
    neu = (pbp[pbp.neutral > 0].groupby(["game_id", "posteam"]).qb_dropback.sum().rename("neu_db").reset_index()
           .rename(columns={"posteam": "team"}))
    tg = tg.merge(neu, on=["game_id", "team"], how="left").fillna({"neu_db": 0})
    ep = pbp[pbp.scrim > 0]
    e1 = (ep[ep.qb_dropback > 0].groupby(["game_id", "posteam"]).epa.agg(["sum", "count"])
          .rename(columns={"sum": "epa_db", "count": "n_db"}).reset_index().rename(columns={"posteam": "team"}))
    e2 = (ep[(ep.qb_dropback == 0) & (ep.rush_attempt > 0)].groupby(["game_id", "posteam"]).epa.agg(["sum", "count"])
          .rename(columns={"sum": "epa_ru", "count": "n_ru"}).reset_index().rename(columns={"posteam": "team"}))
    tg = tg.merge(e1, on=["game_id", "team"], how="left").merge(e2, on=["game_id", "team"], how="left").fillna(0)
    fp = (pbp[(pbp.qb_dropback > 0) & pbp.passer_player_id.notna()].sort_values(["game_id", "play_id"])
          .groupby(["game_id", "posteam"]).passer_player_id.first())
    tg = tg.merge(fp.rename("qb1").reset_index().rename(columns={"posteam": "team"}), on=["game_id", "team"], how="left")
    games = pd.read_csv(os.path.join(NV, "games.csv"))
    gm = games[["game_id", "espn", "spread_line", "total_line", "home_team", "away_team", "roof", "wind", "temp",
                "div_game", "home_rest", "away_rest", "gameday"]].rename(columns={"home_team": "h", "away_team": "a"})
    tg = tg.merge(gm, on="game_id", how="left")
    tg = _game_ctx(tg)

    def agg(df, key, spec):
        return df[df[key].notna()].groupby([key, "game_id"]).agg(**spec).reset_index().rename(columns={key: "gsis"})
    pa = agg(pbp, "passer_player_id", dict(team=("posteam", "last"), pass_yds=("passing_yards", "sum"),
                                           pass_att=("patt", "sum"), completions=("complete_pass", "sum"),
                                           p_db=("qb_dropback", "sum"), p_epa=("epa", "sum")))
    ru = agg(pbp, "rusher_player_id", dict(team_r=("posteam", "last"), rush_yds=("rushing_yards", "sum"),
                                           carries=("rush_attempt", "sum"), rz_car=("rz", "sum"),
                                           i10_car=("i10", "sum"), i5_car=("i5", "sum"), scr=("qb_scramble", "sum")))
    rz_ = pbp.assign(rzt=pbp.rz * pbp.tgt, ezt=pbp.i10 * pbp.tgt)
    re_ = agg(rz_[rz_.tgt > 0], "receiver_player_id", dict(team_c=("posteam", "last"), rec_yds=("receiving_yards", "sum"),
                                                           receptions=("complete_pass", "sum"), targets=("tgt", "sum"),
                                                           air=("ay", "sum"), rz_tgt=("rzt", "sum"),
                                                           i10_tgt=("ezt", "sum")))
    m = pa.merge(ru, on=["gsis", "game_id"], how="outer").merge(re_, on=["gsis", "game_id"], how="outer")
    m["team"] = m.team.fillna(m.team_r).fillna(m.team_c)
    m = m.drop(columns=["team_r", "team_c"])
    pfr2g = dict(zip(pl.pfr_id.astype(str), pl.gsis_id))
    sn = sn[pd.to_numeric(sn.offense_snaps, errors="coerce").fillna(0) > 0].copy()
    sn["gsis"] = sn.pfr_player_id.astype(str).map(pfr2g)
    sn = (sn.dropna(subset=["gsis"])[["gsis", "game_id", "team", "offense_snaps", "offense_pct", "position"]]
          .rename(columns={"team": "team_s", "position": "pos_s"}))
    sn = sn.groupby(["gsis", "game_id"]).first().reset_index()
    m = m.merge(sn, on=["gsis", "game_id"], how="outer")
    m["team"] = m.team.fillna(m.team_s)
    m = m.drop(columns=["team_s"])
    m[_PSTAT] = m[_PSTAT].fillna(0.0)
    m["position"] = m.gsis.map(dict(zip(pl.gsis_id, pl.position))).fillna(m.pos_s)
    esp = {}
    for gg, e in zip(pl.gsis_id, pl.espn_id):
        try:
            esp[gg] = int(float(e))
        except (TypeError, ValueError):
            pass
    m["espn_id"] = m.gsis.map(esp)
    m["name"] = m.gsis.map(dict(zip(pl.gsis_id, pl.display_name)))
    m = m.merge(tg[["game_id", "team", "season", "week", "season_type", "date", "opp", "is_home"]],
                on=["game_id", "team"], how="inner")
    m["synthetic"] = 0
    m["in_sum"] = 1
    tg["synthetic"] = 0
    return tg, m, games, pl


_PSTAT = ["pass_yds", "pass_att", "completions", "p_db", "p_epa", "rush_yds", "carries", "rz_car", "i10_car", "i5_car",
          "scr", "rec_yds", "receptions", "targets", "air", "rz_tgt", "i10_tgt"]


def _game_ctx(tg):
    import numpy as np
    tg["is_home"] = (tg.team == tg.h).astype(int)
    tg["spread"] = np.where(tg.is_home == 1, tg.spread_line, -tg.spread_line)       # + = this team favoured by N
    tg["implied"] = (tg.total_line + tg.spread) / 2
    tg["rest"] = np.where(tg.is_home == 1, tg.home_rest, tg.away_rest)
    return tg


# ------------------------------------------------------------------ 2. the upcoming slate
def _upcoming(tg, m, games, horizon_days=9):
    """Synthetic team-game + player-game rows for the next slate's games (no stats; as-of features only)."""
    import pandas as pd
    today = date.today()
    g = games[(games.season == SEASON) & games.result.isna() & games.spread_line.notna() & games.total_line.notna()]
    g = g[(g.gameday >= (today - timedelta(days=1)).isoformat()) & (g.gameday <= (today + timedelta(days=horizon_days)).isoformat())]
    g = g[~g.game_id.isin(set(tg.game_id))].sort_values(["gameday", "game_id"])   # in the pbp = already real rows
    # ONE synthetic game per team (its next): a second one would be built on the first's empty row. A team playing
    # tonight gets next week's rows from the build after tonight's pbp lands.
    seen, keep = set(), []
    for _, r in g.iterrows():
        keep.append(not ({r.home_team, r.away_team} & seen))
        seen |= {r.home_team, r.away_team}
    g = g[keep]
    if g.empty:
        return tg, m, []
    out_ids = _ruled_out(g)
    trows, prows = [], []
    hist = m[m.synthetic == 0]
    for _, r in g.iterrows():
        for team, opp, qb in ((r.home_team, r.away_team, r.get("home_qb_id")), (r.away_team, r.home_team, r.get("away_qb_id"))):
            past = tg[(tg.team == team) & (tg.date < r.gameday)].sort_values("date")
            last2 = list(past.game_id.tail(2))
            act = hist[hist.game_id.isin(last2) & (hist.team == team)
                       & hist.position.isin(["QB", "RB", "FB", "WR", "TE"])
                       & ((hist.offense_snaps.fillna(0) > 0) | (hist.targets + hist.carries + hist.pass_att > 0))]
            act = act.sort_values("date").groupby("gsis").tail(1)
            act = act[~act.gsis.isin(out_ids)]
            last1 = set(hist[(hist.game_id.isin(last2[-1:])) & (hist.team == team)].gsis)
            if not (isinstance(qb, str) and qb):
                qb = past.qb1.iloc[-1] if len(past) else None
            if qb in out_ids or (qb is not None and qb not in set(act.gsis) and qb not in set(hist.gsis)):
                qb = None
            if qb is None:       # projected starter: the active QB with the most recent dropbacks
                q = act[act.position == "QB"].sort_values(["date", "p_db"])
                qb = q.gsis.iloc[-1] if len(q) else None
            trows.append(dict(game_id=r.game_id, team=team, season=int(r.season), week=int(r.week),
                              season_type="POST" if r.game_type != "REG" else "REG", date=r.gameday, opp=opp,
                              home_team=r.home_team, qb1=qb, espn=r.espn, spread_line=r.spread_line,
                              total_line=r.total_line, h=r.home_team, a=r.away_team, roof=r.roof, wind=r.wind,
                              temp=r.temp, div_game=r.div_game, home_rest=r.home_rest, away_rest=r.away_rest,
                              gameday=r.gameday, synthetic=1))
            act = act[(act.position != "QB") | (act.gsis == qb)]      # one QB plays: the projected starter
            ids = set(act.gsis)
            if qb and qb not in ids:
                qrow = hist[hist.gsis == qb].sort_values("date").tail(1)
                if len(qrow):
                    act = pd.concat([act, qrow])
            for _, p in act.iterrows():
                d = {c: 0.0 for c in _PSTAT}
                d.update(gsis=p.gsis, game_id=r.game_id, team=team, position=p.position, espn_id=p.espn_id,
                         name=p["name"], season=int(r.season), week=int(r.week),
                         season_type="POST" if r.game_type != "REG" else "REG", date=r.gameday, opp=opp,
                         is_home=int(team == r.home_team), offense_snaps=float("nan"), offense_pct=float("nan"),
                         pos_s=p.position, synthetic=1,
                         # only last game's actives (+ the starter) fill the team's share sums; a player back from a
                         # one-game absence is projected but not assumed to play (that would shrink everyone else)
                         in_sum=1 if (p.gsis in last1 or p.gsis == qb) else 0)
                if p.gsis == qb:
                    d["p_db"] = 1.0     # keeps the starter in the QB frame (as-of only; < 10 dropbacks never feeds it)
                prows.append(d)
    ut = _game_ctx(pd.DataFrame(trows))
    for c in tg.columns:
        if c not in ut.columns:
            ut[c] = float("nan")
    tg = pd.concat([tg, ut[tg.columns]], ignore_index=True)
    up = pd.DataFrame(prows)
    m = pd.concat([m, up[[c for c in m.columns if c in up.columns]]], ignore_index=True)
    return tg, m, list(g.game_id)


def _ruled_out(g) -> set:
    """gsis ids nflverse's injury report has Out / IR / PUP for these games' weeks (empty if unavailable)."""
    import pandas as pd
    try:
        inj = pd.read_parquet(_dl(f"https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_{SEASON}.parquet",
                                  os.path.join(D3, f"injuries_{SEASON}.parquet"), max_age_days=0.25))
    except Exception as e:
        logger.info("nfl v3: injury report unavailable (%s) -- every recent active counts", e)
        return set()
    inj = inj[inj.week.isin(set(int(w) for w in g.week))]
    st = inj.report_status.fillna("").str.lower()
    return set(inj.gsis_id[st.isin(["out", "doubtful"])].dropna())


# ------------------------------------------------------------------ 3. features (research features.py + export.py)
def _features(tg, pg):
    import numpy as np
    import pandas as pd
    tg = tg.sort_values(["date", "game_id"]).reset_index(drop=True)
    tg["season"] = tg.season.astype(int)

    def ewm_asof(df, by, cols, hl, prefix):
        sh = df.groupby(by, sort=False)[cols].shift(1)
        sh[by] = df[by].values
        r = sh.groupby(by, sort=False)[cols].ewm(halflife=hl, min_periods=1).mean().reset_index(level=0, drop=True)
        for c in cols:
            df[prefix + c] = r[c].reindex(df.index)
        return df

    def gewm(df, by, col, hl, shift=True, adjust=True, fill0=False):
        x = df.groupby(by, sort=False)[col].shift(1) if shift else df[col].copy()
        if fill0:
            x = x.fillna(0)
        t = pd.DataFrame({"k": df[by].values, "x": x.values}, index=df.index)
        return t.groupby("k", sort=False)["x"].ewm(halflife=hl, adjust=adjust).mean().reset_index(level=0, drop=True).reindex(df.index)

    syn = tg.synthetic == 1
    for c in ["plays", "dropbacks", "patt", "carries", "targets", "air", "pass_yds", "rush_yds", "comp", "neu_db",
              "neu_plays", "epa_db", "n_db", "epa_ru", "n_ru", "sacks"]:
        tg.loc[syn, c] = np.nan            # an unplayed game has no stats (never feeds an as-of mean)
    tg["db_rate"] = tg.dropbacks / tg.plays.clip(lower=1)
    tg["neu_pr"] = tg.neu_db / tg.neu_plays.clip(lower=1)
    tg["epa_db_pp"] = tg.epa_db / tg.n_db.clip(lower=1)
    tg["epa_ru_pp"] = tg.epa_ru / tg.n_ru.clip(lower=1)
    tg["ypd"] = tg.pass_yds / tg.dropbacks.clip(lower=1)
    tg["ypc"] = tg.rush_yds / tg.carries.clip(lower=1)
    tg["air_pt"] = tg.air / tg.targets.clip(lower=1)
    tg["comp_rate"] = tg.comp / tg.targets.clip(lower=1)
    OFF = ["plays", "dropbacks", "patt", "carries", "targets", "db_rate", "neu_pr", "epa_db_pp", "epa_ru_pp", "ypd",
           "ypc", "air_pt", "comp_rate", "implied", "pass_yds", "rush_yds"]
    tg = tg.sort_values(["team", "date"]).reset_index(drop=True)
    tg = ewm_asof(tg, "team", OFF, 6.0, "o_")
    tg["o_n"] = tg.groupby("team").cumcount()
    DEF = ["plays", "dropbacks", "db_rate", "epa_db_pp", "epa_ru_pp", "ypd", "ypc", "comp_rate", "pass_yds", "rush_yds",
           "air_pt"]
    dfn = tg[["game_id", "opp", "date"] + DEF].copy().sort_values(["opp", "date"]).reset_index(drop=True)
    dfn = ewm_asof(dfn, "opp", DEF, 8.0, "d_")
    dfn["d_n"] = dfn.groupby("opp").cumcount()
    tg = tg.merge(dfn[["game_id", "opp"] + ["d_" + c for c in DEF] + ["d_n"]], on=["game_id", "opp"], how="left")
    oo = tg[["game_id", "team", "o_plays", "o_db_rate", "o_epa_db_pp"]].rename(
        columns={"team": "opp", "o_plays": "opp_o_plays", "o_db_rate": "opp_o_db_rate", "o_epa_db_pp": "opp_o_epa"})
    tg = tg.merge(oo, on=["game_id", "opp"], how="left")
    tg["imp_dev"] = tg.implied - tg.o_implied
    pq = pg[pg.p_db > 0].sort_values(["gsis", "date"]).copy()
    full = pq.p_db >= 10
    pq["q_epa"] = (pq.p_epa / pq.p_db).where(full)
    pq["q_ypa"] = (pq.pass_yds / pq.pass_att.clip(lower=1)).where(full)
    pq["q_cmp"] = (pq.completions / pq.pass_att.clip(lower=1)).where(full)
    pq["q_att"] = pq.pass_att.where(full)
    pq = ewm_asof(pq, "gsis", ["q_epa", "q_ypa", "q_cmp", "q_att"], 8.0, "a_")
    pq["_f"] = full.astype(float)
    pq["q_n"] = pq.groupby("gsis")["_f"].shift(1).fillna(0).groupby(pq.gsis).cumsum()
    qmap = pq.set_index(["gsis", "game_id"])[["a_q_epa", "a_q_ypa", "a_q_cmp", "a_q_att", "q_n"]]
    tg = tg.merge(qmap, left_on=["qb1", "game_id"], right_index=True, how="left")
    tg = tg.sort_values(["team", "date"]).reset_index(drop=True)
    tg["qb_prev"] = tg.groupby("team").qb1.shift(1)
    tg["qb_same"] = (tg.qb1 == tg.qb_prev).astype(float)
    for c, prior in (("a_q_epa", -0.10), ("a_q_ypa", 6.0), ("a_q_cmp", 0.60), ("a_q_att", 28.0)):
        k = tg.q_n.fillna(0) / (tg.q_n.fillna(0) + 4.0)
        tg[c + "_s"] = k * tg[c].fillna(prior) + (1 - k) * prior

    pg = pg[pg.position.isin(["QB", "RB", "FB", "WR", "TE"])].copy()
    pg["pgp"] = pg.position.replace({"FB": "RB"})
    pg = pg.merge(tg[["game_id", "team", "targets", "carries", "air", "dropbacks", "plays"]].rename(
        columns={"targets": "T_tgt", "carries": "T_car", "air": "T_air", "dropbacks": "T_db", "plays": "T_plays"}),
        on=["game_id", "team"], how="left")
    trz = pg.groupby(["game_id", "team"]).agg(T_rzt=("rz_tgt", "sum"), T_rzc=("rz_car", "sum"), T_i5=("i5_car", "sum"),
                                              T_snap=("offense_snaps", "max")).reset_index()
    pg = pg.merge(trz, on=["game_id", "team"], how="left")
    pg["active"] = ((pg.offense_snaps.fillna(0) > 0) | (pg.targets + pg.carries + pg.pass_att > 0)
                    | (pg.synthetic == 1)).astype(int)
    pg = pg[pg.active == 1].copy()
    psyn = pg.synthetic == 1
    for c in ["targets", "carries", "receptions", "rec_yds", "rush_yds", "air", "pass_att", "pass_yds", "completions",
              "rz_tgt", "rz_car", "i5_car"]:
        pg.loc[psyn, c] = np.nan            # no stats for an unplayed game (never feeds an as-of mean)
    pg["snap"] = (pg.offense_snaps / pg.T_snap).where(pg.offense_snaps.notna())
    pg["tsh"] = pg.targets / pg.T_tgt.clip(lower=1)
    pg["ash"] = pg.air / pg.T_air.where(pg.T_air.abs() > 1)
    pg["csh"] = pg.carries / pg.T_car.clip(lower=1)
    pg["rzt_sh"] = pg.rz_tgt / pg.T_rzt.clip(lower=1)
    pg["rzc_sh"] = pg.rz_car / pg.T_rzc.clip(lower=1)
    pg["i5_sh"] = pg.i5_car / pg.T_i5.clip(lower=1)
    pg["wopr"] = 1.5 * pg.tsh + 0.7 * pg.ash.fillna(0)
    pg["tprr"] = pg.targets / (pg.snap.fillna(0) * pg.T_db).clip(lower=1)
    pg["db_on"] = pg.snap.fillna(0) * pg.T_db
    pg["patt_sh"] = pg.pass_att / pg.T_db.clip(lower=1)
    pg = pg.sort_values(["gsis", "date", "game_id"]).reset_index(drop=True)
    SH = ["snap", "tsh", "ash", "csh", "rzt_sh", "rzc_sh", "i5_sh", "wopr", "tprr", "db_on", "targets", "carries",
          "receptions", "rec_yds", "rush_yds", "air", "pass_att", "pass_yds", "completions", "patt_sh"]
    for hl, pf in ((3.0, "s_"), (8.0, "l_")):
        pg = ewm_asof(pg, "gsis", SH, hl, pf)
    pg["last_snap"] = pg.groupby("gsis").snap.shift(1)
    pg["last_tsh"] = pg.groupby("gsis").tsh.shift(1)
    pg["last_csh"] = pg.groupby("gsis").csh.shift(1)
    pg["n_g"] = pg.groupby("gsis").cumcount()
    pg["n_gs"] = pg.groupby(["gsis", "season"]).cumcount()
    pg["same_team"] = (pg.groupby("gsis").team.shift(1) == pg.team).astype(float)
    pg["days_since"] = (pd.to_datetime(pg.date) - pd.to_datetime(pg.groupby("gsis").date.shift(1))).dt.days
    for num, den, nm in (("rec_yds", "targets", "ypt"), ("receptions", "targets", "cr"), ("air", "targets", "adot"),
                         ("rush_yds", "carries", "ypc"), ("pass_yds", "pass_att", "ypa"), ("completions", "pass_att", "cmp")):
        pg["_n"] = pg[num]
        pg["_d"] = pg[den]
        g2 = pg.groupby("gsis")
        sn_ = gewm(pg, "gsis", "_n", 16, adjust=False, fill0=True)
        sd_ = gewm(pg, "gsis", "_d", 16, adjust=False, fill0=True)
        cnt = g2["_d"].shift(1).fillna(0).groupby(pg.gsis).cumsum()
        pg["e_" + nm] = sn_ / sd_.where(sd_ > 0)
        pg["e_" + nm + "_n"] = cnt
    pg = pg.drop(columns=["_n", "_d"])
    PRI = {"ypt": {"WR": 8.0, "TE": 7.0, "RB": 6.0, "QB": 0.0}, "cr": {"WR": .63, "TE": .70, "RB": .76, "QB": .5},
           "adot": {"WR": 10.5, "TE": 7.0, "RB": 1.0, "QB": 0.0}, "ypc": {"RB": 4.3, "QB": 5.0, "WR": 6.0, "TE": 3.0},
           "ypa": {"QB": 6.9, "RB": 6.9, "WR": 6.9, "TE": 6.9}, "cmp": {"QB": .64, "RB": .64, "WR": .64, "TE": .64}}
    KS = {"ypt": 40, "cr": 40, "adot": 30, "ypc": 60, "ypa": 150, "cmp": 150}
    for nm, pri in PRI.items():
        p0 = pg.pgp.map(pri).astype(float)
        n = pg["e_" + nm + "_n"].clip(upper=400)
        raw = pg["e_" + nm].fillna(p0)
        pg["e_" + nm + "_s"] = (raw * n + p0 * KS[nm]) / (n + KS[nm])
    for sh, nm in (("s_tsh", "tgt"), ("s_csh", "car"), ("s_ash", "air")):
        pg["_x"] = pg[sh] * pg.in_sum
        pg["act_sum_" + nm] = pg.groupby(["game_id", "team"])["_x"].transform("sum")
    pg = pg.drop(columns=["_x"])
    tgs = tg[["team", "game_id", "date"]].sort_values(["team", "date"])
    tgs["prev_gid"] = tgs.groupby("team").game_id.shift(1)
    act = set(zip(pg.gsis, pg.game_id))
    pg["x_tsh"] = gewm(pg, "gsis", "tsh", 3.0, shift=False)
    pg["x_csh"] = gewm(pg, "gsis", "csh", 3.0, shift=False)
    pv = pg[["gsis", "team", "game_id", "x_tsh", "x_csh"]].merge(
        tgs[["team", "game_id", "prev_gid"]].rename(columns={"game_id": "next_gid", "prev_gid": "game_id"}),
        on=["team", "game_id"])
    pv["out"] = [(g, n) not in act for g, n in zip(pv.gsis, pv.next_gid)]
    vac = (pv[pv.out].groupby(["team", "next_gid"]).agg(vac_tsh=("x_tsh", "sum"), vac_csh=("x_csh", "sum"))
           .reset_index().rename(columns={"next_gid": "game_id"}))
    pg = pg.merge(vac, on=["team", "game_id"], how="left").fillna({"vac_tsh": 0.0, "vac_csh": 0.0})
    TC = (["o_" + c for c in OFF] + ["o_n"] + ["d_" + c for c in DEF]
          + ["d_n", "opp_o_plays", "opp_o_db_rate", "opp_o_epa", "imp_dev", "implied", "spread", "total_line",
             "a_q_epa_s", "a_q_ypa_s", "a_q_cmp_s", "a_q_att_s", "q_n", "qb_same", "qb1", "roof", "wind", "temp",
             "div_game", "rest", "espn"])
    pg = pg.merge(tg[["game_id", "team"] + TC], on=["game_id", "team"], how="left")
    pg["is_qb1"] = (pg.gsis == pg.qb1).astype(int)
    # export.py
    f = pg[(pg.season >= 2021) & (pg.n_g >= 1)].copy()
    f["rr_yds"] = f.rush_yds + f.rec_yds
    f["roof_dome"] = f.roof.isin(["dome", "closed"]).astype(float)
    f["wind"] = pd.to_numeric(f.wind, errors="coerce")
    f["temp"] = pd.to_numeric(f.temp, errors="coerce")
    f["pos_i"] = f.pgp.map({"QB": 0, "RB": 1, "WR": 2, "TE": 3})
    f["post"] = (f.season_type == "POST").astype(float)
    f["renorm_t"] = (1.0 / f.act_sum_tgt.where(f.act_sum_tgt > 0.3)).clip(0.7, 1.6).fillna(1.0)
    f["renorm_c"] = (1.0 / f.act_sum_car.where(f.act_sum_car > 0.3)).clip(0.7, 1.6).fillna(1.0)
    f["x_tgt"] = f.o_targets * f.s_tsh.fillna(0) * f.renorm_t
    f["x_car"] = f.o_carries * f.s_csh.fillna(0) * f.renorm_c
    f["x_recyd"] = f.x_tgt * f.e_ypt_s
    f["x_rec"] = f.x_tgt * f.e_cr_s
    f["x_rushyd"] = f.x_car * f.e_ypc_s
    f["x_patt"] = f.o_patt * f.qb_same + f.a_q_att_s * (1 - f.qb_same)
    f["x_pyd"] = f.x_patt * f.a_q_ypa_s
    return f.reset_index(drop=True)


# ------------------------------------------------------------------ 4. models (research model_lgb.py, ABL=full)
def _feat_cols(f):
    pre = lambda p: [c for c in f.columns if c.startswith(p)]
    G_LINE = ["implied", "spread", "total_line", "imp_dev"]
    G_VAC = ["vac_tsh", "vac_csh", "act_sum_tgt", "act_sum_car", "act_sum_air", "renorm_t", "renorm_c"]
    G_QB = ["a_q_epa_s", "a_q_ypa_s", "a_q_cmp_s", "a_q_att_s", "q_n", "qb_same"]
    G_DEF = pre("d_") + ["opp_o_plays", "opp_o_db_rate", "opp_o_epa"]
    G_TEAM = pre("o_")
    G_SHARE = pre("s_") + pre("l_") + ["last_snap", "last_tsh", "last_csh"]
    G_EFF = [c for c in pre("e_") if c.endswith(("_s", "_n"))]
    G_MISC = ["n_g", "n_gs", "same_team", "days_since", "is_home", "roof_dome", "wind", "temp", "div_game", "rest",
              "pos_i", "post", "week"]
    G_STRUCT = ["x_tgt", "x_car", "x_recyd", "x_rec", "x_rushyd", "x_patt", "x_pyd"]
    out = []
    for c in G_LINE + G_VAC + G_QB + G_DEF + G_TEAM + G_SHARE + G_EFF + G_MISC + G_STRUCT:
        if c in f.columns and c not in out:
            out.append(c)
    return out


def _spec(f):
    import numpy as np
    pg = f.pgp.values
    l_tg, l_car = np.nan_to_num(f.l_targets.values), np.nan_to_num(f.l_carries.values)
    qb1 = f.is_qb1.values == 1
    recv = np.isin(pg, ["WR", "TE", "RB"]) & (l_tg >= 1.0)
    rush = ((pg == "RB") & (l_car >= 2.0)) | ((pg == "QB") & qb1)
    rbr = (pg == "RB") & (l_car >= 2.0)
    qb = (pg == "QB") & qb1
    return {"rec_yds": (recv, "regression"), "receptions": (recv, "poisson"), "rush_yds": (rush, "regression"),
            "carries": (rush, "poisson"), "rr_yds": (rbr, "regression"), "pass_yds": (qb, "regression"),
            "pass_att": (qb, "poisson"), "completions": (qb, "poisson")}


def _matrix(f, cols):
    import numpy as np
    return np.column_stack([np.asarray(f[c].astype(float).values, dtype=np.float64) for c in cols])


def train(f) -> dict:
    import lightgbm as lgb
    import numpy as np
    cols = _feat_cols(f)
    hist = f[f.synthetic == 0]
    X = _matrix(hist, cols)
    spec = _spec(hist)
    meta = {"trained_at": datetime.now(timezone.utc).isoformat(), "through": str(hist.date.max()), "features": cols, "n": {}}
    for stat, (flt, obj) in spec.items():
        y = np.asarray(hist[stat].astype(float).values)
        ok = flt & ~np.isnan(y)
        m = lgb.train(dict(PARAMS, objective=obj), lgb.Dataset(X[ok], y[ok]), num_boost_round=ROUNDS)
        m.save_model(os.path.join(D3, f"model_{stat}.txt"))
        meta["n"][stat] = int(ok.sum())
    json.dump(meta, open(os.path.join(D3, "models.json"), "w"), indent=1)
    logger.info("nfl v3: trained %s through %s", meta["n"], meta["through"])
    return meta


def _models():
    import lightgbm as lgb
    meta = json.load(open(os.path.join(D3, "models.json")))
    return meta, {s: lgb.Booster(model_file=os.path.join(D3, f"model_{s}.txt")) for s in STATS}


# ------------------------------------------------------------------ 5. predictions + neighbour pool
def _conn():
    from .player_props import _conn as _pp_conn      # own connection (data/football/nfl_player.db), safe to close
    c = _pp_conn()
    c.execute("""CREATE TABLE IF NOT EXISTS nflv3_preds(
        game_id TEXT NOT NULL, espn_event TEXT, game_date TEXT, season INTEGER, week INTEGER, gsis TEXT NOT NULL,
        espn_id INTEGER, name TEXT, team TEXT, pgp TEXT, stat TEXT NOT NULL, pred REAL, n_g INTEGER, n_gs INTEGER,
        is_qb1 INTEGER, built_at TEXT, actual REAL, PRIMARY KEY(game_id, gsis, stat))""")
    return c


def build(retrain: bool = False) -> dict:
    """Extract -> upcoming rows -> features -> (re)train if stale -> predict the slate -> grade old predictions."""
    import numpy as np
    t0 = time.time()
    tg, m, games, _pl = _extract()
    tg, m, up_ids = _upcoming(tg, m, games)
    f = _features(tg, m)
    meta_p = os.path.join(D3, "models.json")
    stale = (not os.path.exists(meta_p)) or (time.time() - os.path.getmtime(meta_p) > RETRAIN_DAYS * 86400)
    if retrain or stale:
        train(f)
    meta, models = _models()
    cols = meta["features"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    c = _conn()
    n_pred = 0
    up = f[(f.synthetic == 1)]
    if len(up):
        X = _matrix(up, cols)
        spec = _spec(up)
        rows = []
        for stat in STATS:
            flt = spec[stat][0]
            if not flt.any():
                continue
            pr = models[stat].predict(X[flt])
            for (_, r), p in zip(up[flt].iterrows(), pr):
                rows.append((r.game_id, None if r.espn != r.espn else str(int(float(r.espn))), r.date, int(r.season),
                             int(r.week), r.gsis, None if r.espn_id != r.espn_id else int(r.espn_id), r["name"],
                             r.team, r.pgp, stat, round(float(p), 4), int(r.n_g), int(r.n_gs), int(r.is_qb1), now))
        c.execute("BEGIN")
        for g in set(up.game_id):          # the slate's projections are rebuilt every run (roster / QB news)
            c.execute("DELETE FROM nflv3_preds WHERE game_id = ? AND actual IS NULL", (g,))
        c.executemany("INSERT OR REPLACE INTO nflv3_preds(game_id, espn_event, game_date, season, week, gsis, espn_id, "
                      "name, team, pgp, stat, pred, n_g, n_gs, is_qb1, built_at) VALUES "
                      "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        c.execute("COMMIT")
        n_pred = len(rows)
    # grade: a live projection whose game is now in the pbp joins the neighbour pool
    hist = f[f.synthetic == 0]
    act = {}
    for stat in STATS:
        for g, gs, v in zip(hist.game_id, hist.gsis, hist[stat]):
            if v == v:
                act[(g, gs, stat)] = float(v)
    pend = c.execute("SELECT game_id, gsis, stat FROM nflv3_preds WHERE actual IS NULL AND game_date < ?",
                     (date.today().isoformat(),)).fetchall()
    done = [(act[(r[0], r[1], r[2])], r[0], r[1], r[2]) for r in pend if (r[0], r[1], r[2]) in act]
    if done:
        c.executemany("UPDATE nflv3_preds SET actual = ? WHERE game_id = ? AND gsis = ? AND stat = ?", done)
        c.commit()
    c.close()
    _POOL["t"] = 0.0
    out = {"games": up_ids, "predictions": n_pred, "graded": len(done), "trained_through": meta["through"],
           "secs": round(time.time() - t0)}
    logger.info("nfl v3 build: %s", out)
    return out


_POOL = {"t": 0.0, "pools": {}, "before": {}}


def _pools():
    """{(stat, pgp): [(date, pred, actual, season)] sorted}: research OOF + graded live projections."""
    if time.time() - _POOL["t"] < 1800 and _POOL["pools"]:
        return _POOL["pools"]
    import csv
    pools: dict = {}
    seen = set()
    p = os.path.join(D3, "oof_pool.csv")
    if os.path.exists(p):
        for r in csv.DictReader(open(p)):
            try:
                pools.setdefault((r["stat"], r["pgp"]), []).append((r["date"], float(r["pred"]), float(r["actual"]), int(r["season"])))
                seen.add((r["game_id"], r["gsis"], r["stat"]))
            except (TypeError, ValueError, KeyError):
                continue
    c = _conn()
    for g, gs, stat, pgp, d, pred, a, s in c.execute(
            "SELECT game_id, gsis, stat, pgp, game_date, pred, actual, season FROM nflv3_preds WHERE actual IS NOT NULL"):
        if (g, gs, stat) not in seen:
            pools.setdefault((stat, pgp), []).append((d, float(pred), float(a), int(s)))
    c.close()
    for k in pools:
        pools[k].sort()
    _POOL.update(t=time.time(), pools=pools, before={})
    return pools


def probs(stat: str, pgp: str, pred: float, line: float, game_date: str, season: int = SEASON):
    """(P(actual > line), P(actual < line), n neighbours) from the pool, or None (research eval_v3_np.py neigh)."""
    import numpy as np
    k = (stat, pgp, game_date)
    if k not in _POOL["before"]:
        P = _pools().get((stat, pgp)) or []
        i = bisect.bisect_left(P, (game_date,))
        sub = sorted((x[1], x[2], x[3]) for x in P[:i])
        _POOL["before"][k] = (np.array([x[0] for x in sub]), np.array([x[1] for x in sub]), np.array([x[2] for x in sub]))
    cs, acts, seas = _POOL["before"][k]
    if len(cs) < 30:
        return None
    rad = max(0.5, 0.05 * pred)
    while True:
        lo = bisect.bisect_left(cs, pred - rad)
        hi = bisect.bisect_right(cs, pred + rad)
        if hi - lo >= 250 or rad > 5 * pred + 50:
            break
        rad *= 1.25
    if hi - lo < 30:
        return None
    w = np.array([SW.get(season - s, 0.0) for s in seas[lo:hi]])
    a = acts[lo:hi]
    sw = w.sum()
    if sw <= 0:
        return None
    return float(w[a > line].sum() / sw), float(w[a < line].sum() / sw), int(hi - lo)


_PRED = {"t": 0.0, "rows": {}}


def prediction(espn_event: str, espn_id: int, stat: str) -> dict | None:
    """The slate projection for (ESPN event id, ESPN player id, stat), cached 5 min."""
    if time.time() - _PRED["t"] > 300:
        c = _conn()
        rows = {}
        for r in c.execute("SELECT espn_event, espn_id, stat, pred, pgp, n_g, n_gs, game_date, season, is_qb1, built_at "
                           "FROM nflv3_preds WHERE actual IS NULL AND espn_event IS NOT NULL AND espn_id IS NOT NULL"):
            rows[(str(r[0]), int(r[1]), r[2])] = {"pred": r[3], "pgp": r[4], "games": r[5], "games_season": r[6],
                                                  "date": r[7], "season": r[8], "is_qb1": r[9], "built_at": r[10]}
        c.close()
        _PRED.update(t=time.time(), rows=rows)
    return _PRED["rows"].get((str(espn_event), int(espn_id), stat))


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if len(sys.argv) > 1 and sys.argv[1] == "build":
        print(json.dumps(build(retrain="--retrain" in sys.argv)))
