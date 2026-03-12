"""
Scrapers package for fetching external sports data.

Primary exports:
    NHLScraper   - NHL stats API scraper (schedules, standings, boxscores, rosters)
    OddsScraper  - The Odds API scraper (moneyline, spread, totals)
    ESPNScraper  - ESPN public API scraper (team stats, PP%, PK%, shots, faceoffs)
"""

from app.scrapers.nhl_api import NHLScraper
from app.scrapers.odds_api import OddsScraper
from app.scrapers.espn import ESPNScraper

__all__ = [
    "NHLScraper",
    "OddsScraper",
    "ESPNScraper",
]
