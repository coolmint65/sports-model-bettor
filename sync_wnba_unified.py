import sqlite3
leg = sqlite3.connect('data/basketball/wnba.db'); leg.row_factory = sqlite3.Row
uni = sqlite3.connect('data/picks_unified.db'); uni.row_factory = sqlite3.Row
vals = [r[0] for r in uni.execute("select distinct result from picks where sport='basketball' and result is not null")]
print('unified distinct result values:', vals)
longform = any(str(v).lower() in ('won', 'lost', 'push', 'void') for v in vals)
CONV = {'W': 'won', 'L': 'lost', 'P': 'push', 'V': 'void'}
def conv(r):
    return CONV.get(r, r) if longform else r
lg = leg.execute("select date,matchup,bet_type,result,profit,settled_at from picks where result is not null").fetchall()
print('legacy graded picks:', len(lg), '| unified uses longform:', longform)
synced = 0
for r in lg:
    cur = uni.execute(
        "update picks set result=?, profit=?, settled_at=coalesce(?, settled_at) "
        "where sport='basketball' and pick_date=? and matchup=? and bet_type=? and result is null",
        (conv(r['result']), r['profit'], r['settled_at'], r['date'], r['matchup'], r['bet_type']))
    synced += cur.rowcount
uni.commit()
print('rows synced into picks_unified:', synced)
print('unified basketball ungraded now:', uni.execute("select count(*) from picks where sport='basketball' and result is null").fetchone()[0])
print('unified basketball last graded now:', uni.execute("select max(pick_date) from picks where sport='basketball' and result is not null").fetchone()[0])
print('--- recent (>=Aug11) unified basketball now ---')
for r in uni.execute("select pick_date,matchup,pick_text,result from picks where sport='basketball' and pick_date>='2026-08-11' order by pick_date desc limit 12"):
    print('  ', r['pick_date'], str(r['matchup'])[:14], str(r['pick_text'])[:16], '| result=', repr(r['result']))
