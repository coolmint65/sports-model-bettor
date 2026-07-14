"""One-shot sweep: find pending picks that share (game_id, family) for
any sport DB, void the older row, keep the newest. Uses the same family
mapping as engine._dedup_helpers so the surviving row matches what the
record-time guard would have left in place going forward.

After the shared dedup helper landed across all per-sport trackers, this
sweep cleans up the dups recorded BEFORE the helper was wired in (WNBA
SPREAD on 5/22-5/25, AHL OU on 5/19, etc).

Usage: python -m scripts.sweep_pending_dups [--apply]
   without --apply: dry-run, prints what would be voided.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime

# Make engine importable from repo root
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from engine._dedup_helpers import default_family_for  # noqa: E402


DB_TABLES = [
    # (db_path, picks_table_name, game_id_column)
    ("data/baseball/college.db", "picks", "game_id"),
    ("data/football/ufl.db", "picks", "game_id"),
    ("data/mlb.db", "picks", "game_id"),
    ("data/nhl.db", "picks", "game_id"),
    ("data/wnba.db", "picks", "game_id"),
    ("data/nba.db", "nba_picks", "game_id"),
]

# Basketball + hockey + soccer + golf + motorsports add their own
BASKETBALL_LEAGUES = [
    "afl", "argentina_lnb", "australia_nbl", "brazil_lbf_w", "brazil_nbb",
    "bulgaria_nbl", "china_cba", "czech_nbl", "denmark_basketligaen",
    "dominican_lnb", "euroleague", "fiba_champions", "finland_korisliiga",
    "france_pro_b", "germany_bbl", "greece_a1", "hungary_nb1",
    "iceland_urvalsdeild", "iceland_urvalsdeild_w", "israel_super",
    "japan_b2", "korea_kbl", "latvia_lbl", "lithuania_lkl", "ncaam",
    "ncaaw", "nz_nbl", "puerto_rico_bsn", "slovakia_extraliga",
    "slovenia_skl", "sweden_ligan",
]
for _l in BASKETBALL_LEAGUES:
    DB_TABLES.append((f"data/basketball/{_l}.db", "picks", "game_id"))

HOCKEY_LEAGUES = ["ahl", "aihl", "nzihl", "pwhl"]
for _l in HOCKEY_LEAGUES:
    DB_TABLES.append((f"data/hockey/{_l}.db", "picks", "game_id"))

SOCCER_LEAGUES = [
    "arg_lpf", "bra_seriea", "conmebol_libertadores", "eng_premier",
    "esp_laliga", "fifa_internationals", "fifa_world_cup", "fra_ligue1",
    "ger_bundesliga", "ita_seriea", "mls", "uefa_champions",
    "uefa_conference", "uefa_europa", "us_nwsl", "us_open_cup",
    "usl_championship",
]
for _l in SOCCER_LEAGUES:
    DB_TABLES.append((f"data/soccer/{_l}.db", "picks", "match_id"))


def sweep(apply: bool) -> None:
    total_voided = 0
    now = datetime.utcnow().isoformat()
    for db, table, gid_col in DB_TABLES:
        if not os.path.exists(db):
            continue
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        # Check table exists
        ok = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if not ok:
            conn.close()
            continue
        rows = conn.execute(
            f"SELECT id, {gid_col} AS gid, bet_type, pick, created_at, date "
            f"FROM {table} WHERE result IS NULL "
            f"ORDER BY {gid_col}, id"
        ).fetchall()
        if not rows:
            conn.close()
            continue
        # Group by (gid, family); when >1, keep newest (highest id), void rest
        by_key: dict[tuple, list[sqlite3.Row]] = defaultdict(list)
        for r in rows:
            fam = default_family_for(r["bet_type"])
            by_key[(r["gid"], fam)].append(r)
        voided_here = 0
        for (gid, fam), lst in by_key.items():
            if len(lst) < 2:
                continue
            # Keep highest id (most recent), void others
            lst_sorted = sorted(lst, key=lambda r: r["id"])
            keep = lst_sorted[-1]
            for r in lst_sorted[:-1]:
                action = "VOID" if apply else "DRY"
                print(f"  [{action}] {db} gid={gid} fam={fam} "
                      f"id={r['id']} {r['bet_type']} '{r['pick']}' "
                      f"(keep id={keep['id']} {keep['bet_type']} "
                      f"'{keep['pick']}')")
                if apply:
                    conn.execute(
                        f"UPDATE {table} SET result='V', profit=0, "
                        f"settled_at=? WHERE id=?",
                        (now, r["id"]),
                    )
                voided_here += 1
        if voided_here:
            print(f"{db}: {voided_here} dup pending rows "
                  f"{'voided' if apply else '(dry-run)'}")
            total_voided += voided_here
        if apply:
            conn.commit()
        conn.close()
    print(f"\nTotal: {total_voided} rows "
          f"{'voided' if apply else 'would be voided (dry-run)'}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Actually void rows (default: dry-run)")
    args = ap.parse_args()
    sweep(args.apply)
