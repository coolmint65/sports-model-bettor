"""
Nightly DB backup with retention.

Snapshots each sport's SQLite database into ``data/backups/YYYY-MM-DD/``
and prunes anything older than ``RETENTION_DAYS``. Uses SQLite's online
backup API instead of a filesystem copy so the backup is consistent
even when the backend has the DB open under WAL mode.

CLAUDE.md mandates "Test backup restores at least once" -- to verify a
snapshot, copy the .db file out of data/backups/ and point engine.db at
it via the DB_PATH env var.

Usage:
    python -m scripts.backup_dbs               # standard nightly backup
    python -m scripts.backup_dbs --dry-run     # show what would happen
    python -m scripts.backup_dbs --retention 14
"""

import argparse
import logging
import shutil
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
BACKUP_ROOT = DATA_DIR / "backups"

DB_FILES = ["mlb.db", "nhl.db", "nba.db"]
RETENTION_DAYS_DEFAULT = 30


def _backup_one(src: Path, dst: Path) -> int:
    """Use SQLite's backup API for a consistent snapshot."""
    src_conn = sqlite3.connect(str(src))
    dst_conn = sqlite3.connect(str(dst))
    try:
        src_conn.backup(dst_conn)
    finally:
        dst_conn.close()
        src_conn.close()
    return dst.stat().st_size


def _prune_old_backups(retention_days: int, dry_run: bool) -> int:
    """Remove dated backup folders older than retention_days."""
    if not BACKUP_ROOT.exists():
        return 0
    cutoff = datetime.now().date() - timedelta(days=retention_days)
    pruned = 0
    for child in sorted(BACKUP_ROOT.iterdir()):
        if not child.is_dir():
            continue
        try:
            folder_date = datetime.strptime(child.name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if folder_date < cutoff:
            logger.info("Pruning %s (older than %d days)", child, retention_days)
            if not dry_run:
                shutil.rmtree(child)
            pruned += 1
    return pruned


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retention", type=int, default=RETENTION_DAYS_DEFAULT,
                        help=f"days to keep backups (default {RETENTION_DAYS_DEFAULT})")
    parser.add_argument("--dry-run", action="store_true",
                        help="print actions without touching disk")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    today_dir = BACKUP_ROOT / datetime.now().strftime("%Y-%m-%d")
    if not args.dry_run:
        today_dir.mkdir(parents=True, exist_ok=True)

    total_bytes = 0
    snapshots = 0
    failures = 0
    for name in DB_FILES:
        src = DATA_DIR / name
        if not src.exists():
            logger.info("Skip %s (not found)", name)
            continue
        dst = today_dir / name
        if args.dry_run:
            logger.info("Would back up %s -> %s", src, dst)
            continue
        try:
            size = _backup_one(src, dst)
            total_bytes += size
            snapshots += 1
            logger.info("Backed up %s -> %s (%.1f MB)", name, dst, size / 1_000_000)
        except Exception as e:
            failures += 1
            logger.error("Backup failed for %s: %s", name, e)

    pruned = _prune_old_backups(args.retention, args.dry_run)

    logger.info(
        "Backup summary: %d snapshots (%.1f MB total), %d pruned, %d failures",
        snapshots, total_bytes / 1_000_000, pruned, failures,
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
