"""Early-season recruiting-talent prior for CFB margins (Austin 2026-10-31, CFBD key).

Weeks 1-4 the points ratings are still mostly last year's regressed numbers. Walk-forward test (fit 2024-25, test
2026 weeks 1-4, n=209): adding beta * (talent_home - talent_away)/100 to the model margin cut RMSE 16.00 -> 15.60;
beta = 1.66 pts per 100 talent points. Against the closing line it adds nothing (the market already knows) -- this
makes early cards less wild, it is not an edge. Off after week 4 (untested there; the ratings have caught up).
Talent comes from data/football/cfbd.db (engine.football.cfbd_fetch); team names map to ESPN ids via CFBD games.
"""
import json, os, sqlite3
from datetime import date

BETA_PER_100 = 1.66
LAST_WEEK = 4
_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'football', 'cfbd.db')
_CACHE = {}

def _season(d):
    return d.year if d.month >= 7 else d.year - 1

def _load(season):
    if season in _CACHE: return _CACHE[season]
    out = ({}, None)
    try:
        c = sqlite3.connect(f'file:{_DB}?mode=ro', uri=True, timeout=10)
        row = c.execute('SELECT body FROM raw WHERE path=?', (f'/talent?year={season}',)).fetchone()
        grow = c.execute('SELECT body FROM raw WHERE path=?', (f'/games?seasonType=both&year={season}',)).fetchone()
        if row and grow:
            name2id = {}
            games = json.loads(grow[0])
            for g in games:
                name2id[g['homeTeam']] = g['homeId']; name2id[g['awayTeam']] = g['awayId']
            tal = {name2id[t['team']]: float(t['talent']) for t in json.loads(row[0]) if t.get('team') in name2id and t.get('talent') is not None}
            starts = sorted(g['startDate'][:10] for g in games if g.get('week') == 1 and g.get('seasonType') == 'regular')
            out = (tal, date.fromisoformat(starts[0]) if starts else None)
    except Exception:
        pass
    _CACHE[season] = out
    return out

def margin_adjustment(home_id, away_id, on=None):
    """Points to ADD to the home margin, or 0.0 outside weeks 1-4 / without talent data."""
    d = on or date.today()
    tal, wk1 = _load(_season(d))
    if not tal or not wk1: return 0.0
    week = (d - wk1).days // 7 + 1
    if week < 1 or week > LAST_WEEK: return 0.0
    th, ta = tal.get(int(home_id)), tal.get(int(away_id))
    if th is None or ta is None: return 0.0
    return BETA_PER_100 * (th - ta) / 100.0
