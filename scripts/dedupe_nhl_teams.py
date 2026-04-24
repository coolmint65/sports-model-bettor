"""
One-time cleanup: collapse duplicate nhl_teams rows down to one row
per team (the canonical NHL API team_id, which is the lowest existing
id per abbreviation).

A previous bad scraper run inserted three rows per NHL team using
auto-incremented high IDs (e.g. COL had id=21, id=32964, id=100242).
None of the high-ID rows were referenced by nhl_games / nhl_players /
nhl_p1_stats — verified before deletion — so the cleanup is a pure
delete with no FK rewiring needed.

Idempotent: running again on a clean table is a no-op. Backs up
deleted rows to ``_nhl_teams_dupes_backup`` in the same DB so this
can be reverted if anything downstream surprises us.

Usage:
    python -m scripts.dedupe_nhl_teams [--dry-run]

Run on a fresh Beelink (or any environment where the bad scraper
output is still in the DB) right after restoring the data folder.
"""

from __future__ import annotations

import sys


def main(argv: list[str]) -> int:
    dry = "--dry-run" in argv
    from engine.nhl_db import get_conn

    conn = get_conn()
    before = conn.execute("SELECT COUNT(*) FROM nhl_teams").fetchone()[0]
    print(f"nhl_teams rows before: {before}")

    # Show what will be deleted
    dupes = conn.execute("""
        SELECT id, abbreviation, name FROM nhl_teams
        WHERE id NOT IN (SELECT MIN(id) FROM nhl_teams GROUP BY abbreviation)
        ORDER BY abbreviation, id
    """).fetchall()
    print(f"Duplicate rows to remove: {len(dupes)}")
    if not dupes:
        print("No duplicates found — DB is clean.")
        return 0
    for r in dupes[:10]:
        print(f"  id={r['id']:>6}  {r['abbreviation']}  {r['name']}")
    if len(dupes) > 10:
        print(f"  ... and {len(dupes) - 10} more")

    if dry:
        print("\nDRY RUN — no changes made. Re-run without --dry-run to apply.")
        return 0

    # Verify no FK references to the dupe IDs before deleting.
    dupe_ids = [r["id"] for r in dupes]
    placeholders = ",".join("?" * len(dupe_ids))
    games_refs = conn.execute(
        f"SELECT COUNT(*) FROM nhl_games "
        f"WHERE home_team_id IN ({placeholders}) "
        f"   OR away_team_id IN ({placeholders})",
        dupe_ids + dupe_ids,
    ).fetchone()[0]
    p1_refs = conn.execute(
        f"SELECT COUNT(*) FROM nhl_p1_stats WHERE team_id IN ({placeholders})",
        dupe_ids,
    ).fetchone()[0]
    players_refs = conn.execute(
        f"SELECT COUNT(*) FROM nhl_players WHERE team_id IN ({placeholders})",
        dupe_ids,
    ).fetchone()[0]
    if games_refs or p1_refs or players_refs:
        print(f"\n!! FK references found — refusing to delete. "
              f"games={games_refs} p1_stats={p1_refs} players={players_refs}")
        print("   Manual rewiring needed. Aborting.")
        return 1

    # Backup table — same schema as nhl_teams, holds the rows we're about to delete.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _nhl_teams_dupes_backup AS "
        "SELECT * FROM nhl_teams WHERE 0=1"
    )
    conn.execute("DELETE FROM _nhl_teams_dupes_backup")
    conn.execute("""
        INSERT INTO _nhl_teams_dupes_backup
        SELECT * FROM nhl_teams
        WHERE id NOT IN (SELECT MIN(id) FROM nhl_teams GROUP BY abbreviation)
    """)
    backup_count = conn.execute(
        "SELECT COUNT(*) FROM _nhl_teams_dupes_backup"
    ).fetchone()[0]
    print(f"\nBacked up {backup_count} rows to _nhl_teams_dupes_backup")

    conn.execute("""
        DELETE FROM nhl_teams
        WHERE id NOT IN (SELECT MIN(id) FROM nhl_teams GROUP BY abbreviation)
    """)
    conn.commit()

    after = conn.execute("SELECT COUNT(*) FROM nhl_teams").fetchone()[0]
    print(f"nhl_teams rows after:  {after}")
    print(f"Deleted: {before - after}")

    remaining_dupes = conn.execute(
        "SELECT abbreviation, COUNT(*) n FROM nhl_teams "
        "GROUP BY abbreviation HAVING n > 1"
    ).fetchall()
    if remaining_dupes:
        print(f"!! Still {len(remaining_dupes)} duplicate abbreviations remaining")
        return 1
    print("OK — every NHL team now has exactly one row.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
