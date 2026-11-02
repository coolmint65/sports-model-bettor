#!/usr/bin/env python
"""
Link captured derivative closing odds -> <sport>.db derivative-pick tables
(closing_odds) so CLV accumulates on the edge-hunt markets.

game_odds.db::game_derivative_odds holds the captured lines, keyed by a
team-NAME game_key + market + selection + line. Each sport's derivative-pick
table stores game_id + free-text pick + bet_type. This normalizer bridges them.

Idempotent (only fills NULL closing_odds). Series-safe: disambiguate by game
date, take the latest capture on that date as the close, skip if ambiguous.
PAPER model -- zero placement impact.

--- SPORT STATUS (2026-09-17) ---
  mlb : LIVE + validated against real picks+odds.
  nhl : DRAFT map (markets align: Period Total/DNB/Team Total). UNVALIDATED --
        no hockey odds captured yet (offseason). Validate when NHL season opens
        (~Oct): confirm game_key naming + the PERIOD_TOTAL selection format.
  nba : DRAFT + BLOCKED. Picker bets "Q1 Team Total" but the basketball capture
        only snapshots Q1 *game* totals (Q1_TOTAL), not per-team -> no target to
        link to. Needs extract_basketball extended (capture side) first.
  ncaab/soccer: not wired -- soccer has no derivative picks generated at all and
        no soccer capture; ncaab mirrors nba's capture gap.

Only sports flagged live=True run by default (the scheduled task). Drafts run
only with --include-draft, so unvalidated maps can never write wrong closing_odds
into production. Austin 2026-09-17 ("scaffold NBA/NCAAB/NHL").
"""
import sqlite3, os, re, sys, argparse
from collections import defaultdict
from datetime import datetime, timedelta


def w(s):
    sys.stdout.buffer.write((str(s) + '\n').encode('utf-8', 'replace'))


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ODDS_DB = os.path.join(ROOT, 'data', 'game_odds.db')


def etd(st):
    """start_time ISO(UTC) -> ET date (EDT approx, -4h)."""
    try:
        return (datetime.fromisoformat(st.replace('Z', '+00:00')) - timedelta(hours=4)).strftime('%Y-%m-%d')
    except Exception:
        return None


def _ts_now():
    """Current UTC timestamp, same format as engine.tracker_core._cc_now_iso
    (used as the closing_odds_captured_at fallback when no capture_date is
    on file for the linked row)."""
    from datetime import timezone
    return datetime.now(timezone.utc).isoformat()


def _teams(cur):
    return {a: n for a, c, n in cur.execute('select abbreviation,city,name from teams')}


# ---- per-sport parsers: (matchup, bet_type, pick, name_of) -> (game_key, market, selection, line) | None ----

MLB_ALIAS = {'CHW': 'CWS', 'ARI': 'AZ', 'OAK': 'ATH', 'WSN': 'WSH',
             'TBR': 'TB', 'KCR': 'KC', 'SFG': 'SF', 'SDP': 'SD'}
MLB_BT = {'Team Total': 'TEAM_TOTAL', 'F5 Team Total': 'F5_TEAM_TOTAL'}


def _gamekey(matchup, nameof):
    m = re.match(r'\s*([A-Za-z]+)\s*@\s*([A-Za-z]+)', matchup or '')
    if not m:
        return None, None, None
    an, hn = nameof(m.group(1)), nameof(m.group(2))
    if not an or not hn:
        return None, None, None
    return '%s@%s' % (an, hn), m.group(1), m.group(2)


def parse_mlb(matchup, bt, pick, nameof):
    gk, away_ab, home_ab = _gamekey(matchup, nameof)
    if not gk:
        return None
    toks = (pick or '').split()
    side = next((t.lower() for t in toks if t.lower() in ('over', 'under', 'odd', 'even')), None)
    if side is None:
        return None
    if bt == 'Total O/E':
        return (gk, 'TOTAL_OE', side, 0.0)
    if bt in MLB_BT:
        market = MLB_BT[bt] + ('_HOME' if nameof(toks[0]) == nameof(home_ab) else '_AWAY')
        try:
            return (gk, market, side, float(toks[-1]))
        except Exception:
            return None
    return None


# DRAFT -- validate against real captured hockey odds when the NHL season opens.
NHL_BT = {'Period Total': 'PERIOD_TOTAL', 'Period DNB': 'PERIOD_DNB', 'Team Total': 'TEAM_TOTAL'}


def parse_nhl(matchup, bt, pick, nameof):
    gk, away_ab, home_ab = _gamekey(matchup, nameof)
    if not gk:
        return None
    toks = (pick or '').split()  # e.g. "P1 Over 1.5"
    side = next((t.lower() for t in toks if t.lower() in ('over', 'under', 'odd', 'even')), None)
    period = next((t.upper() for t in toks if re.fullmatch(r'P[123]', t.upper() or '')), None)
    market = NHL_BT.get(bt)
    if not market:
        return None
    # TODO(validate Oct): confirm PERIOD_TOTAL selection encoding. MLB uses
    # "I1:under"; hockey likely "P1:over" -- guessed here, verify with a real row.
    if market in ('PERIOD_TOTAL', 'PERIOD_DNB') and period:
        sel = ('%s:%s' % (period, side)).lower() if side else period.lower()
    else:
        sel = side
    try:
        line = float(toks[-1])
    except Exception:
        line = 0.0
    if sel is None:
        return None
    return (gk, market, sel, line)


def parse_nba(matchup, bt, pick, nameof):
    # BLOCKED: "Q1 Team Total" has no captured target (capture snapshots Q1 game
    # total only). Left as a stub so the structure is ready once extract_basketball
    # captures Q1 team totals. Returns None until then.
    return None


SPORT_CONFIG = [
    dict(key='mlb', db='data/mlb.db', table='derivative_picks',
         tag='baseball', alias=MLB_ALIAS, parse=parse_mlb, live=True),
    dict(key='nhl', db='data/nhl.db', table='nhl_derivative_picks',
         tag='hockey', alias={}, parse=parse_nhl, live=False),   # DRAFT
    dict(key='nba', db='data/nba.db', table='nba_derivative_picks',
         tag='basketball', alias={}, parse=parse_nba, live=False),  # BLOCKED (capture gap)
]


def link_sport(cfg, dry=False):
    dbf = os.path.join(ROOT, cfg['db'])
    if not os.path.exists(dbf):
        return 0, 0, ['(no db)']
    mc = sqlite3.connect(dbf)
    cur = mc.cursor()
    try:
        abbr2name = _teams(cur)   # some sports (e.g. nhl) lack a teams table -> draft no-ops
    except Exception:
        mc.close()
        return 0, 0, ['(no teams table -- schema not ready)']
    alias = cfg['alias']

    # CLV needs to know a close was captured PRE-GAME to trust it -- stamp
    # when, guarded so a table without the column still works (mirrors
    # engine.tracker_core.core_capture_closing_odds).
    _has_ts = any(d[1] == 'closing_odds_captured_at'
                  for d in cur.execute('PRAGMA table_info(%s)' % cfg['table']))

    def nameof(ab):
        # game_derivative_odds keys the Athletics by their old 'OAK' code,
        # while the teams table + picks use 'ATH' -> pick-side game_keys
        # built from teams.name ('Athletics@...') never match the captured
        # 'OAK@...'. Force the captured token. Scoped to MLB and keyed off
        # the abbrev/alias (survives a teams.name change); no-op elsewhere.
        if cfg['tag'] == 'baseball' and (ab in ('ATH', 'OAK') or alias.get(ab) == 'ATH'):
            return 'OAK'
        if ab in abbr2name:
            return abbr2name[ab]
        if ab in alias:
            return abbr2name.get(alias[ab])
        return None

    gc = sqlite3.connect(ODDS_DB).cursor()
    idx = defaultdict(list)
    for gk, mk, sel, ln, od, st, cd in gc.execute(
            "select game_key,market,selection,line,odds,start_time,capture_date "
            "from game_derivative_odds where sport=?", (cfg['tag'],)):
        idx[(gk, mk, str(sel).lower(), float(ln))].append((od, st, cd))

    rows = cur.execute(
        'select id,date,matchup,bet_type,pick from %s where closing_odds is null' % cfg['table']).fetchall()
    matched = updated = 0
    ex = []
    for pid, dt, matchup, bt, pick in rows:
        parsed = cfg['parse'](matchup, bt, pick, nameof)
        if not parsed:
            continue
        gk, market, sel, line = parsed
        cands = idx.get((gk, market, str(sel).lower(), float(line)), [])
        if not cands:
            continue
        dm = [c for c in cands if etd(c[1]) == dt]
        if dm:
            best_c = max(dm, key=lambda c: c[2])
        elif len(cands) == 1:
            best_c = cands[0]
        else:
            continue
        best = best_c[0]
        matched += 1
        if not dry:
            if _has_ts:
                # best_c[2] is the capture_date of the winning snapshot --
                # the actual close, more accurate than "now" since this
                # script can run well after the real close. Fall back to
                # "now" on the rare candidate with no capture_date on file.
                stamp = best_c[2] or _ts_now()
                cur.execute(
                    'update %s set closing_odds=?, closing_odds_captured_at=? '
                    'where id=? and closing_odds is null' % cfg['table'],
                    (best, stamp, pid))
            else:
                cur.execute('update %s set closing_odds=? where id=? and closing_odds is null' % cfg['table'],
                            (best, pid))
            updated += cur.rowcount
        if len(ex) < 4:
            ex.append((pid, pick, best))
    if not dry:
        mc.commit()
    mc.close()
    return matched, updated, ex


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry', action='store_true')
    ap.add_argument('--include-draft', action='store_true',
                    help='also run UNVALIDATED draft sports (nhl/nba). Off by default so bad maps never write prod.')
    args = ap.parse_args()
    total = 0
    for cfg in SPORT_CONFIG:
        if not cfg['live'] and not args.include_draft:
            continue
        matched, updated, ex = link_sport(cfg, dry=args.dry)
        tag = cfg['key'] + ('' if cfg['live'] else ' [DRAFT]')
        w('%s %s: matched=%d updated=%d  e.g. %s' % (tag, cfg['table'], matched, updated, ex))
        total += updated
    w('TOTAL closing_odds linked: %d%s' % (total, ' (DRY)' if args.dry else ''))


if __name__ == '__main__':
    main()
