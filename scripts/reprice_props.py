"""
Near-game re-price -> re-sights closing_odds (CLV capture).

Each sport's slate is made ONCE (entry odds) and never looked at again, so
props / POTD never get a closing snapshot (0% CLV). This re-runs the
pickers so insert_pick's re-sight path (or the POTD resolver) stamps the board's
CURRENT price as closing_odds on any still-pending pick. The last run before
start is effectively the close.

Covers:
  * legacy O/U player props (mlb/nba/nhl/wnba)
  * Pick-of-the-Day closing capture — update_potd_closing_odds() existed but was
    DEAD CODE (never called anywhere), so every sport's POTD CLV was ~0%. Wired
    here.

Sport-agnostic, safe/idempotent (only fills closing_odds WHERE result IS NULL).
Scheduled by RepriceProps. PAPER model — no real-money / live_worker touched.
"""
import os
import sys
import importlib
from datetime import datetime  # Beelink runs on ET (verified -4 EDT)

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.getcwd())


def w(s):
    sys.stdout.buffer.write((str(s) + '\n').encode('utf-8', 'replace'))


# Legacy O/U-style player props (generate_picks(date=...)). NFL absent here —
# its player props run via the live_worker backstop, not this script.
SPORTS = [('mlb', 'engine.mlb_prop_picks'),
          ('nba', 'engine.nba_prop_picks'),
          ('nhl', 'engine.nhl_prop_picks'),
          ('wnba', 'engine.basketball._wnba_prop_picks')]


# Sports whose Pick-of-the-Day gets a closing snapshot. update_potd_closing_odds
# has real resolvers for mlb/nhl/nba + a generic fallback for the rest.
POTD_SPORTS = ['mlb', 'nba', 'nhl', 'wnba', 'nfl', 'cfb', 'tennis']


def main():
    today = datetime.now().strftime('%Y-%m-%d')

    # 1. Legacy player props.
    for sport, mod in SPORTS:
        try:
            gp = importlib.import_module(mod).generate_picks
            res = gp(date=today)
            if isinstance(res, dict):
                res = {k: res[k] for k in list(res)[:5]}
            w('%s %-8s reprice: %s' % (today, sport, res))
        except Exception as e:
            w('%s %-8s reprice ERR: %s' % (today, sport, e))

    # 3. Pick-of-the-Day closing capture (was dead code — never invoked).
    try:
        from engine.pick_of_day import update_potd_closing_odds
        for sport in POTD_SPORTS:
            try:
                res = update_potd_closing_odds(sport)
                w('%s %-8s potd-clv: %s' % (today, sport, res))
            except Exception as e:
                w('%s %-8s potd-clv ERR: %s' % (today, sport, e))
    except Exception as e:
        w('%s potd-clv import ERR: %s' % (today, e))


if __name__ == '__main__':
    main()
