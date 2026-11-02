import sys, os, shutil, py_compile, sqlite3
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
os.chdir(r'C:\sports-model-bettor'); sys.path.insert(0, r'C:\sports-model-bettor')
from engine.db import get_conn
conn = get_conn(); conn.row_factory = sqlite3.Row

# Blast radius: how many game_ids span >1 distinct date?
rows = conn.execute("""
  SELECT game_id, COUNT(DISTINCT date) nd, GROUP_CONCAT(DISTINCT date) ds
  FROM picks WHERE game_id IS NOT NULL GROUP BY game_id HAVING nd>1 ORDER BY game_id
""").fetchall()
print(f"game_ids spanning >1 date: {len(rows)}")
for r in rows:
    print(f"  gid={r['game_id']} dates={r['ds']}")

RM  = r'C:\sports-model-bettor\backend\server\routes_mlb.py'
BAK = RM + '.bak-authrec-dtkey-20261023'
src = open(RM, 'rb').read().decode('utf-8')
crlf = ('\r\n' in src); lf = src.replace('\r\n','\n')

repls = [
 ("""        gids = [r[0] for r in conn.execute(
            "SELECT game_id, MAX(created_at) c FROM picks "
            "WHERE game_id IS NOT NULL AND (result IS NULL OR result != 'V') "
            "GROUP BY game_id ORDER BY c DESC LIMIT ?", (limit,))]""",
  """        grp = [(r[0], r[1]) for r in conn.execute(
            "SELECT game_id, date, MAX(created_at) c FROM picks "
            "WHERE game_id IS NOT NULL AND (result IS NULL OR result != 'V') "
            "GROUP BY game_id, date ORDER BY c DESC LIMIT ?", (limit,))]"""),
 ("""        logger.warning("mlb unified tracker game list failed: %s", e)
        gids = []
    rows = []
    for gid in gids:
        best = _mlb_recorded_authority_pick(gid, conn=conn)""",
  """        logger.warning("mlb unified tracker game list failed: %s", e)
        grp = []
    rows = []
    for gid, gdate in grp:
        best = _mlb_recorded_authority_pick(gid, conn=conn, date=gdate)"""),
]
for old, new in repls:
    assert lf.count(old) == 1, f"anchor count={lf.count(old)} for:\n{old[:60]}..."
    lf = lf.replace(old, new)

assert "GROUP BY game_id, date ORDER BY c DESC" in lf
assert "for gid, gdate in grp:" in lf
assert "_mlb_recorded_authority_pick(gid, conn=conn, date=gdate)" in lf
assert "GROUP BY game_id ORDER BY c DESC" not in lf  # old form gone

out = lf.replace('\n','\r\n') if crlf else lf
shutil.copyfile(RM, BAK)
open(RM,'wb').write(out.encode('utf-8'))
print("\nbackup ->", BAK)
py_compile.compile(RM, doraise=True)
print("py_compile: OK")
