#!/usr/bin/env python3
"""
Scraper orchestrator.

Run manually to update all team data from ESPN.
Can target specific leagues or run everything.

Usage:
    python -m scrapers.run_all              # All leagues
    python -m scrapers.run_all NFL NBA      # Specific leagues
    python -m scrapers.run_all --soccer     # All soccer leagues
    python -m scrapers.run_all --college    # All college leagues
"""

import sys
import logging
import time
from datetime import datetime
from pathlib import Path

from .config import ESPN_LEAGUES
from .espn import scrape_league

# Set up logging
LOG_DIR = Path(__file__).resolve().parent.parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "scraper.log"),
    ],
)
logger = logging.getLogger(__name__)

SOCCER_KEYS = {"EPL", "UCL", "LALIGA", "BUNDESLIGA", "MLS", "NWSL", "LIGAMX"}
COLLEGE_KEYS = {"CFB", "NCAAB", "NCAAW"}


def main():
    args = [a.upper() for a in sys.argv[1:]]

    # Expand group flags
    target_leagues = set()
    for arg in args:
        if arg == "--SOCCER":
            target_leagues |= SOCCER_KEYS
        elif arg == "--COLLEGE":
            target_leagues |= COLLEGE_KEYS
        elif arg == "--ALL" or not args:
            target_leagues = None  # Run everything
            break
        else:
            target_leagues.add(arg)

    start = time.time()
    total_updated = 0
    errors = []

    logger.info("=" * 60)
    logger.info(f"SCRAPER RUN — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    for espn_sport, espn_league, our_key in ESPN_LEAGUES:
        if target_leagues is not None and our_key not in target_leagues:
            continue

        logger.info(f"\n{'─' * 40}")
        logger.info(f"LEAGUE: {our_key}")
        logger.info(f"{'─' * 40}")

        try:
            updated = scrape_league(espn_sport, espn_league, our_key)
            total_updated += len(updated)
            logger.info(f"✓ {our_key}: Updated {len(updated)} teams")
        except Exception as e:
            logger.error(f"✗ {our_key}: {e}")
            errors.append((our_key, str(e)))

        # Brief pause between leagues
        time.sleep(2)

    elapsed = time.time() - start
    logger.info(f"\n{'=' * 60}")
    logger.info(f"COMPLETE — {total_updated} teams updated in {elapsed:.0f}s")
    if errors:
        logger.info(f"ERRORS ({len(errors)}):")
        for league, err in errors:
            logger.info(f"  {league}: {err}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
