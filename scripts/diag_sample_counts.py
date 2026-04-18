"""Print total calibration samples (synthetic + real) across all three sports."""
from engine.db import get_conn as mlb_conn
from engine.nhl_db import get_conn as nhl_conn
from engine.nba_db import get_conn as nba_conn

SPORTS = [
    ("mlb", mlb_conn, "picks"),
    ("nhl", nhl_conn, "nhl_picks"),
    ("nba", nba_conn, "nba_picks"),
]

print(f"{'sport':<5}  {'synthetic':>10}  {'real':>7}  {'total':>7}")
print("-" * 40)
grand_total = 0
for name, gc, real_table in SPORTS:
    conn = gc()
    n_syn = conn.execute(
        "SELECT COUNT(*) FROM calibration_samples WHERE result IN ('W','L')"
    ).fetchone()[0]
    n_real = conn.execute(
        f"SELECT COUNT(*) FROM {real_table} WHERE result IN ('W','L')"
    ).fetchone()[0]
    total = n_syn + n_real
    grand_total += total
    print(f"{name:<5}  {n_syn:>10}  {n_real:>7}  {total:>7}")
print("-" * 40)
print(f"{'TOTAL':<5}  {'':>10}  {'':>7}  {grand_total:>7}")
