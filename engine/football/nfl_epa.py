"""NFL EPA ratings (MintyBot 2026-10-31, Austin "lets work on our log loss for our sports").

Replaces the Elo-style NFL margin with opponent-adjusted efficiency from nflverse play-by-play:
  * per team, OFFENSE and DEFENSE EPA per play, PASS and RUSH separately -- weighted ridge over every prior game
    (half-life 10 game-weeks, prior seasons x0.3, ridge 60 plays), the same fit the walk-forward backtest used;
  * the starting QB's own EPA per dropback (shrunk 150 dropbacks toward 0);
  * margin = const + hfa + b_pass*pass_diff + b_rush*rush_diff + b_qb*qb_diff + b_rest*rest_diff (least squares),
    win prob = logistic(slope * margin) -- both fit on every completed season before the current one.
Walk-forward vs the ESPN BET / DK / nflverse closing moneyline (trained 2019-22): log-loss 2023 .645 (old .668,
close .627), 2024 .607 (.610 / .589), 2025 .645 (.662 / .607), 2026 .660 (.687 / .660).

Data: data/football/nflverse/play_by_play_<year>.parquet + games.csv (refresh(): current season + games.csv).
The fit is pickled to epa_state.pkl and refit on a background thread when the data files are newer (REFIT_S); predict() returns None when it can't price a game (caller falls back).
"""
from __future__ import annotations

import logging
import math
import os
import pickle
import threading
import time
import urllib.request

import numpy as np

logger = logging.getLogger(__name__)

D = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'football', 'nflverse')
HL, PRIOR, KAPPA, QB_SHRINK = 10.0, 0.3, 60.0, 150.0
FIRST_SEASON = 2018
REFIT_S = 3 * 3600
REFRESH_S = 12 * 3600
# ESPN / our DB abbreviations -> nflverse
ALIAS = {'WSH': 'WAS', 'LAR': 'LA', 'JAC': 'JAX', 'LVR': 'LV', 'OAK': 'LV', 'SD': 'LAC', 'STL': 'LA'}
_LOCK = threading.Lock()
_STATE: dict = {}


def nv(abbr):
    a = str(abbr or '').upper().strip()
    return ALIAS.get(a, a)


def refresh(seasons=None):
    """Download the current season's play-by-play + games.csv (both update daily on nflverse)."""
    os.makedirs(D, exist_ok=True)
    y = time.gmtime().tm_year if time.gmtime().tm_mon >= 7 else time.gmtime().tm_year - 1
    for s in (seasons or [y]):
        _fetch(f'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{s}.parquet', f'play_by_play_{s}.parquet')
    _fetch('https://github.com/nflverse/nfldata/raw/master/data/games.csv', 'games.csv')


def _fetch(url, name):
    dst = os.path.join(D, name)
    urllib.request.urlretrieve(url, dst + '.part')
    if os.path.getsize(dst + '.part') < 10000:
        raise RuntimeError(f'{name}: short download')
    os.replace(dst + '.part', dst)


def _ridge(hist, teams, t_index, cur_season):
    n = len(teams); TI = {t: i for i, t in enumerate(teams)}; P = 1 + 2 * n
    A = np.zeros((P, P)); b = np.zeros(P)
    for (gi, season, off, de, epa, cnt) in hist:
        if cnt <= 0 or off not in TI or de not in TI:
            continue
        w = cnt * 0.5 ** ((t_index - gi) / HL) * (1.0 if season == cur_season else PRIOR)
        y = epa / cnt
        idx = (0, 1 + TI[off], 1 + n + TI[de]); sg = (1.0, 1.0, -1.0)
        for a_, sa in zip(idx, sg):
            b[a_] += w * sa * y
            for c_, sc in zip(idx, sg):
                A[a_, c_] += w * sa * sc
    A[np.arange(1, P), np.arange(1, P)] += KAPPA
    sol = np.linalg.solve(A, b)
    return {t: (sol[1 + TI[t]], sol[1 + n + TI[t]]) for t in teams}


def _build():
    import pandas as pd
    cols = ['game_id', 'season', 'posteam', 'defteam', 'epa', 'pass', 'rush', 'qb_dropback', 'passer_player_id']
    files = [os.path.join(D, f'play_by_play_{y}.parquet') for y in range(FIRST_SEASON, 2100)
             if os.path.exists(os.path.join(D, f'play_by_play_{y}.parquet'))]
    pbp = pd.concat([pd.read_parquet(f, columns=cols) for f in files])
    pbp = pbp[pbp['epa'].notna() & pbp['posteam'].notna() & ((pbp['pass'] == 1) | (pbp['rush'] == 1))]
    pbp = pbp.assign(pass_epa=pbp['epa'] * (pbp['pass'] == 1), rush_epa=pbp['epa'] * (pbp['rush'] == 1))
    tgm = pbp.groupby(['game_id', 'posteam', 'defteam']).agg(pass_epa=('pass_epa', 'sum'), pass_n=('pass', 'sum'),
                                                             rush_epa=('rush_epa', 'sum'), rush_n=('rush', 'sum')).reset_index()
    tg = {(r.game_id, r.posteam): r for r in tgm.itertuples()}
    qbg = pbp[pbp['qb_dropback'] == 1].groupby(['game_id', 'passer_player_id'])['epa'].agg(['sum', 'count']).reset_index()
    qb_by_game = {k: v for k, v in qbg.groupby('game_id')}
    games = pd.read_csv(os.path.join(D, 'games.csv'))
    games['gameday'] = pd.to_datetime(games['gameday'])
    played = games[games['home_score'].notna()].sort_values(['gameday', 'game_id'])
    played = played[played['season'] >= FIRST_SEASON]
    teams = sorted(set(played['home_team']) | set(played['away_team']))
    wk_keys = sorted((played['season'] * 100 + played['week']).unique())
    wk_index = {k: i for i, k in enumerate(wk_keys)}
    hp, hr, qbh = [], [], {}
    rows = []
    qbv = lambda q: (lambda s: (s[0] / s[1]) * (s[1] / (s[1] + QB_SHRINK)) if s and s[1] > 0 else 0.0)(qbh.get(q))
    for wk, gw in played.groupby(played['season'] * 100 + played['week'], sort=True):
        ti = wk_index[wk]; cur = int(wk // 100)
        if cur >= FIRST_SEASON + 1:            # fit only once there is history
            rp = _ridge(hp, teams, ti, cur); rr = _ridge(hr, teams, ti, cur)
            for r in gw.itertuples():
                h, a = r.home_team, r.away_team
                rows.append((cur, r.home_score - r.away_score, 0 if str(r.location) == 'Neutral' else 1,
                             (rp[h][0] - rp[a][1]) - (rp[a][0] - rp[h][1]), (rr[h][0] - rr[a][1]) - (rr[a][0] - rr[h][1]),
                             qbv(r.home_qb_id) - qbv(r.away_qb_id), (r.home_rest or 7) - (r.away_rest or 7)))
        for r in gw.itertuples():
            for off, de in ((r.home_team, r.away_team), (r.away_team, r.home_team)):
                t = tg.get((r.game_id, off))
                if t is not None:
                    hp.append((ti, cur, off, de, t.pass_epa, t.pass_n)); hr.append((ti, cur, off, de, t.rush_epa, t.rush_n))
            q = qb_by_game.get(r.game_id)
            if q is not None:
                for x in q.itertuples():
                    s = qbh.setdefault(x.passer_player_id, [0.0, 0]); s[0] += x.sum; s[1] += x.count
    cur_season = int(games.loc[games['home_score'].isna(), 'season'].min()) if games['home_score'].isna().any() else int(played['season'].max())
    R = np.array([r for r in rows if r[0] < cur_season], dtype=float)
    X = np.c_[np.ones(len(R)), R[:, 2], R[:, 3:7]]
    beta = np.linalg.lstsq(X, R[:, 1], rcond=None)[0]
    m = X @ beta; k = R[:, 1] != 0; y = (R[:, 1] > 0).astype(float)
    def ll(s):
        p = 1 / (1 + np.exp(-s * m[k])); return -np.mean(y[k] * np.log(p) + (1 - y[k]) * np.log(1 - p))
    slope = min(np.arange(0.05, 0.30, 0.0025), key=ll)
    ti_next = len(wk_keys)
    rp = _ridge(hp, teams, ti_next, cur_season); rr = _ridge(hr, teams, ti_next, cur_season)
    last_qb = {}
    for r in played.itertuples():
        last_qb[r.home_team] = r.home_qb_id; last_qb[r.away_team] = r.away_qb_id
    upcoming = {(r['home_team'], r['away_team']): {k: r.get(k) for k in ('home_qb_id', 'away_qb_id', 'home_rest', 'away_rest', 'location', 'gameday')}
                for r in games[games['home_score'].isna()].to_dict('records')}
    return {'beta': beta, 'slope': slope, 'rp': rp, 'rr': rr, 'qbv': {q: qbv(q) for q in qbh}, 'last_qb': {k: v for k, v in last_qb.items() if isinstance(v, str)},
            'upcoming': upcoming, 'season': cur_season, 'n_fit': len(R), 't': time.time()}


_CACHE_F = os.path.join(D, 'epa_state.pkl')
_building = threading.Event()


def _src_mtime():
    fs = [os.path.join(D, f) for f in os.listdir(D) if f.startswith('play_by_play_') or f == 'games.csv'] if os.path.isdir(D) else []
    return max((os.path.getmtime(f) for f in fs), default=0.0)


def _bg_build():
    try:
        if time.time() - _src_mtime() > REFRESH_S:          # nflverse updates pbp + games.csv nightly
            try:
                refresh()
            except Exception as e:
                logger.warning('nfl_epa data refresh failed (fitting on the files we have): %s', e)
        st = _build(); st['src'] = _src_mtime()
        with _LOCK:
            _STATE['s'] = st
        try:
            with open(_CACHE_F + '.tmp', 'wb') as f:
                pickle.dump(st, f)
            os.replace(_CACHE_F + '.tmp', _CACHE_F)
        except Exception as e:
            logger.warning('nfl_epa cache write failed: %s', e)
        logger.info('nfl_epa: refit season %s on %d games, slope %.4f', st['season'], st['n_fit'], st['slope'])
    except Exception as e:
        logger.warning('nfl_epa build failed: %s', e)
    finally:
        _building.clear()


def state():
    """Current fit, never blocking a request: disk pickle -> memory; a stale/missing fit rebuilds on a thread
    (~100 s) and the caller falls back to the old ratings meanwhile."""
    with _LOCK:
        st = _STATE.get('s')
        if st is None and os.path.exists(_CACHE_F):
            try:
                with open(_CACHE_F, 'rb') as f:
                    st = pickle.load(f)
                _STATE['s'] = st
            except Exception as e:
                logger.warning('nfl_epa cache read failed: %s', e)
        stale = st is None or (time.time() - st['t'] > REFIT_S and (_src_mtime() > st.get('src', 0) or time.time() - _src_mtime() > REFRESH_S))
        if stale and not _building.is_set():
            _building.set()
            threading.Thread(target=_bg_build, name='nfl-epa-build', daemon=True).start()
        return st


_ABBR: dict = {}


def abbr_for(team_id):
    """ESPN NFL team id -> our abbreviation (data/football/nfl.db teams)."""
    if not _ABBR:
        import sqlite3
        try:
            c = sqlite3.connect(f"file:{os.path.join(os.path.dirname(D), 'nfl.db')}?mode=ro", uri=True, timeout=10)
            _ABBR.update({int(i): a for i, a in c.execute('SELECT id, abbreviation FROM teams')})
            c.close()
        except Exception as e:
            logger.warning('nfl_epa team map failed: %s', e)
    return _ABBR.get(int(team_id))


def predict_ids(home_id, away_id):
    h, a = abbr_for(home_id), abbr_for(away_id)
    return predict(h, a) if h and a else None


def predict(home_abbr, away_abbr, neutral=False):
    """{'margin','p_home','qb_home','qb_away'} for an upcoming game, or None."""
    st = state()
    if not st:
        return None
    h, a = nv(home_abbr), nv(away_abbr)
    if h not in st['rp'] or a not in st['rp']:
        return None
    up = st['upcoming'].get((h, a))
    up = up or {}
    qh, qa = up.get('home_qb_id'), up.get('away_qb_id')
    qh = qh if isinstance(qh, str) and qh else st['last_qb'].get(h)
    qa = qa if isinstance(qa, str) and qa else st['last_qb'].get(a)
    rest = 0.0
    if up:
        try:
            rest = float(up.get('home_rest') or 7) - float(up.get('away_rest') or 7)
        except (TypeError, ValueError):
            rest = 0.0
        neutral = neutral or str(up.get('location')) == 'Neutral'
    x = np.array([1.0, 0.0 if neutral else 1.0,
                  (st['rp'][h][0] - st['rp'][a][1]) - (st['rp'][a][0] - st['rp'][h][1]),
                  (st['rr'][h][0] - st['rr'][a][1]) - (st['rr'][a][0] - st['rr'][h][1]),
                  st['qbv'].get(qh, 0.0) - st['qbv'].get(qa, 0.0), rest])
    margin = float(x @ st['beta'])
    return {'margin': margin, 'p_home': 1.0 / (1.0 + math.exp(-st['slope'] * margin)), 'qb_home': qh, 'qb_away': qa,
            'season': st['season'], 'gameday': str(up.get('gameday') or '')[:10]}
