"""Football win-% bar calibration (data/display_calibration.json), refit nightly by the
DisplayCalRefit task (05:30) via scripts/refit_display_cal.bat; backend/server/_display_cal.py
applies it live (mtime reload) to the SERVED p_home with a no-flip clamp.

2026-10-25 rewrite (MintyBot, Austin "A B and C"): the old isotonic step map was fit on the bare
win/loss-Elo replay and showed every NFL home dog from 15-35% as 28%. Out of sample (fit 2023-24,
test 2025-26) it added nothing (Brier .2411 vs raw .2409). Now each league gets a smooth,
recency-weighted Platt curve fit on a walk-forward replay of the number the bar actually shows:
the SERVING ratings (points-based for CFB, engine.football._elo.walk_forward) blended like the
ensemble's factor + MC legs (the GBM leg can't be replayed out of sample). Written as a 101-knot
grid so _display_cal interpolates it unchanged. Other keys in the file (hockey/nhl) are kept.
"""
import json
import math
import os
import sys

os.chdir(r'C:\sports-model-bettor'); sys.path.insert(0, r'C:\sports-model-bettor')


def out(s):
    sys.stdout.buffer.write((str(s) + '\n').encode('utf-8', 'replace'))


import numpy as np
from engine.football import LEAGUE_REGISTRY, get_league_config
from engine.football import _elo
from engine.football._db import get_conn
from engine.football._ensemble import _load_weights

PATH = 'data/display_calibration.json'
GRID = [round(i / 100, 2) for i in range(101)]
MIN_N = 400            # games behind a fit, else the league's bars stay raw
BURN_IN = 3            # games each team must have played before its games count
HALF_LIFE_DAYS = 365   # recency weight: last season counts double the one before


def _lg(p):
    p = min(max(p, 1e-4), 1 - 1e-4)
    return math.log(p / (1 - p))


def _sig(z):
    return 1.0 / (1.0 + math.exp(-z)) if z >= 0 else math.exp(z) / (1.0 + math.exp(z))


def platt(xs, ys, ws):
    """Weighted logistic fit y ~ sigmoid(a + b*logit(x)) by Newton/IRLS."""
    a, b = 0.0, 1.0
    for _ in range(100):
        g0 = g1 = h00 = h01 = h11 = 0.0
        for x, y, w in zip(xs, ys, ws):
            z = _lg(x)
            p = _sig(a + b * z)
            e = w * (y - p)
            v = w * p * (1 - p)
            g0 += e; g1 += e * z; h00 += v; h01 += v * z; h11 += v * z * z
        det = h00 * h11 - h01 * h01
        if abs(det) < 1e-12:
            break
        da = (h11 * g0 - h01 * g1) / det
        db = (-h01 * g0 + h00 * g1) / det
        a += da; b += db
        if abs(da) + abs(db) < 1e-10:
            break
    return a, b


def brier(ps, ys):
    return sum((p - y) ** 2 for p, y in zip(ps, ys)) / len(ys)


def served_proxy(league, cfg):
    """[(date, x, y)] for decided games: x = the ensemble's factor+MC blend from PRE-game ratings."""
    hfa, epp = _elo.serving_params(cfg)
    w = (_load_weights(league, True).get('ml') or {})
    wf, wm = float(w.get('factor', 0.45)), float(w.get('mc', 0.35))
    margins = mean = None
    try:
        md = json.load(open(f'data/football/{league}_margin_dist.json'))
        margins, mean = np.sort(np.asarray(md['margins'], dtype=float)), float(md['mean'])
    except Exception:
        pass
    seen, rows = {}, []
    for r, rh, ra in _elo.walk_forward(league, get_conn(league)):
        h, a = int(r['home_team_id']), int(r['away_team_id'])
        hs, as_ = int(r['home_score']), int(r['away_score'])
        if hs != as_ and not (hs == 0 and as_ == 0) and seen.get(h, 0) >= BURN_IN and seen.get(a, 0) >= BURN_IN:
            f = _elo.expected_score(rh, ra, hfa)
            x = f
            if margins is not None:
                shift = (rh - ra + hfa) / epp - mean      # MC: empirical margins shifted to the expected margin
                win = len(margins) - np.searchsorted(margins, -shift, side='right')
                loss = np.searchsorted(margins, -shift, side='left')
                if win + loss > 0:
                    x = (wf * f + wm * win / (win + loss)) / (wf + wm)
            rows.append((str(r['date'])[:10], float(x), 1 if hs > as_ else 0))
        seen[h] = seen.get(h, 0) + 1
        seen[a] = seen.get(a, 0) + 1
    return rows


def _weights(dates):
    import datetime as dt
    last = max(dt.date.fromisoformat(d) for d in dates)
    return [0.5 ** ((last - dt.date.fromisoformat(d)).days / HALF_LIFE_DAYS) for d in dates]


def fit_league(league):
    cfg = get_league_config(league)
    key = f'football/{league}'
    rows = served_proxy(league, cfg)
    n = len(rows)
    if n < MIN_N:
        out(f'{key}: n={n}<{MIN_N} SKIP (raw)')
        return key, None
    dates = [d for d, _, _ in rows]
    xs = [x for _, x, _ in rows]
    ys = [y for _, _, y in rows]
    # out-of-sample check: fit on everything before the latest season, score the latest season
    cut = f'{max(dates)[:4]}-03-01' if max(dates)[5:7] >= '03' else f'{int(max(dates)[:4]) - 1}-03-01'
    tr = [i for i, d in enumerate(dates) if d < cut]
    te = [i for i, d in enumerate(dates) if d >= cut]
    oos = None
    if len(tr) >= MIN_N // 2 and len(te) >= 50:
        a0, b0 = platt([xs[i] for i in tr], [ys[i] for i in tr], _weights([dates[i] for i in tr]))
        yt = [ys[i] for i in te]
        oos = {'test_from': cut, 'n_test': len(te),
               'brier_raw': round(brier([xs[i] for i in te], yt), 4),
               'brier_cal': round(brier([_sig(a0 + b0 * _lg(xs[i])) for i in te], yt), 4)}
    a, b = platt(xs, ys, _weights(dates))
    fit = 'platt_replay'
    if oos and oos['brier_cal'] >= oos['brier_raw']:
        a, b, fit = 0.0, 1.0, 'identity_replay'   # the curve doesn't beat raw out of sample: show raw
    mapped = [round(min(max(_sig(a + b * _lg(g)), 0.02), 0.98), 4) for g in GRID]
    b_raw = brier(xs, ys)
    b_cal = brier([_sig(a + b * _lg(x)) for x in xs], ys)
    out(f'{key}: n={n} base={sum(ys) / n:.3f} {fit} a={a:+.3f} b={b:.3f}  Brier {b_raw:.4f}->{b_cal:.4f}  OOS {oos}')
    out('   ' + ', '.join(f'{int(g * 100)}->{m * 100:.0f}' for g, m in zip(GRID, mapped) if int(round(g * 100)) % 10 == 0))
    return key, {'grid': GRID, 'mapped': mapped, 'n': n, 'market': 'home_win', 'fit': fit,
                 'a': round(a, 4), 'b': round(b, 4), 'half_life_days': HALF_LIFE_DAYS,
                 'base': round(sum(ys) / n, 4), 'brier_raw': round(b_raw, 4), 'brier_cal': round(b_cal, 4),
                 'oos': oos, 'x': 'ensemble factor+MC blend, serving ratings, walk-forward'}


if __name__ == '__main__':
    try:
        res = json.load(open(PATH))
    except Exception:
        res = {}
    for lg in list(LEAGUE_REGISTRY):
        try:
            key, entry = fit_league(lg)
            if entry is None:
                res.pop(key, None)
            else:
                res[key] = entry
        except Exception as e:
            out(f'football/{lg} ERR {str(e)[:120]}')
    if '--dry-run' in sys.argv:
        out('DRY RUN: not written')
    else:
        tmp = PATH + '.tmp'
        json.dump(res, open(tmp, 'w'), indent=2)
        os.replace(tmp, PATH)
        out('WROTE keys=' + str(list(res)))
