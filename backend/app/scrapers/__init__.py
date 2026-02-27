"""
Scrapers package for fetching external sports data.

Primary exports:
    NHLScraper  - NHL stats API scraper (schedules, standings, boxscores, rosters)
    OddsScraper - The Odds API scraper (moneyline, spread, totals)
"""

from app.scrapers.nhl_api import NHLScraper
from app.scrapers.odds_api import OddsScraper

__all__ = [
    "NHLScraper",
    "OddsScraper",
]
