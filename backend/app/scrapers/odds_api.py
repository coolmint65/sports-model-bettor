"""
Odds scraper using The Odds API (https://api.the-odds-api.com/v4).

Fetches current betting odds for NHL games and normalises them into
a standard format compatible with the Game model.

Requires an API key set via the ODDS_API_KEY environment variable.
Operates gracefully when no key is configured (logs a warning, returns empty).
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.game import Game
from app.models.team import Team
from app.scrapers.base import BaseScraper, ScraperError

logger = logging.getLogger(__name__)

# Mapping of common Odds API team names to NHL abbreviations.
# The Odds API uses full team names; we map them to the 3-letter codes
# stored in our database.
ODDS_API_TEAM_MAP: Dict[str, str] = {
    "Anaheim Ducks": "ANA",
    "Arizona Coyotes": "ARI",
    "Boston Bruins": "BOS",
    "Buffalo Sabres": "BUF",
    "Calgary Flames": "CGY",
    "Carolina Hurricanes": "CAR",
    "Chicago Blackhawks": "CHI",
    "Colorado Avalanche": "COL",
    "Columbus Blue Jackets": "CBJ",
    "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET",
    "Edmonton Oilers": "EDM",
    "Florida Panthers": "FLA",
    "Los Angeles Kings": "LAK",
    "Minnesota Wild": "MIN",
    "Montreal Canadiens": "MTL",
    "Montréal Canadiens": "MTL",
    "Nashville Predators": "NSH",
    "New Jersey Devils": "NJD",
    "New York Islanders": "NYI",
    "New York Rangers": "NYR",
    "Ottawa Senators": "OTT",
    "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT",
    "San Jose Sharks": "SJS",
    "Seattle Kraken": "SEA",
    "St. Louis Blues": "STL",
    "St Louis Blues": "STL",
    "Tampa Bay Lightning": "TBL",
    "Toronto Maple Leafs": "TOR",
    "Utah Hockey Club": "UTA",
    "Vancouver Canucks": "VAN",
    "Vegas Golden Knights": "VGK",
    "Washington Capitals": "WSH",
    "Winnipeg Jets": "WPG",
}


class OddsScraper(BaseScraper):
    """
    Scraper for The Odds API.

    Fetches live and pre-match odds for NHL games. Supports moneyline,
    spread (puck line), and totals (over/under) markets.

    The API key is loaded from `settings.odds_api_key`. If no key is
    configured, all fetch methods return empty results with a warning.
    """

    SPORT_KEY = "icehockey_nhl"
    REGIONS = "us"
    ODDS_FORMAT = "american"

    def __init__(
        self,
        base_url: str = settings.odds_api_base,
        api_key: Optional[str] = None,
        rate_limit: float = 1.0,
        **kwargs,
    ):
        super().__init__(base_url=base_url, rate_limit=rate_limit, **kwargs)
        self.api_key = api_key or settings.odds_api_key
        if not self.api_key:
            logger.warning(
                "No ODDS_API_KEY configured. Odds fetching will be disabled. "
                "Set the ODDS_API_KEY environment variable to enable."
            )

    @property
    def _has_key(self) -> bool:
        return bool(self.api_key)

    # ------------------------------------------------------------------
    # Fetch odds
    # ------------------------------------------------------------------

    async def fetch_nhl_odds(
        self,
        markets: str = "h2h,spreads,totals",
    ) -> List[Dict[str, Any]]:
        """
        Fetch current odds for all upcoming NHL games.

        Args:
            markets: Comma-separated market types. Defaults to
                     "h2h,spreads,totals" (moneyline, puck line, over/under).

        Returns:
            List of normalised odds dicts, one per game. Each dict contains:
            - commence_time: ISO datetime of game start
            - home_team: full team name from the API
            - away_team: full team name from the API
            - home_abbrev: mapped 3-letter abbreviation
            - away_abbrev: mapped 3-letter abbreviation
            - bookmakers: list of bookmaker odds snapshots
            - best_odds: dict with the best available odds across books
        """
        if not self._has_key:
            logger.warning("Odds API key not set; returning empty odds list.")
            return []

        path = f"/sports/{self.SPORT_KEY}/odds"
        params = {
            "apiKey": self.api_key,
            "regions": self.REGIONS,
            "markets": markets,
            "oddsFormat": self.ODDS_FORMAT,
        }

        try:
            data = await self.fetch_json(path, params=params)
        except ScraperError as exc:
            logger.error("Failed to fetch odds: %s", exc)
            return []

        if not isinstance(data, list):
            logger.warning("Unexpected odds response type: %s", type(data))
            return []

        results: List[Dict[str, Any]] = []
        for event in data:
            parsed = self._parse_event(event)
            if parsed:
                results.append(parsed)

        logger.info("Fetched odds for %d NHL events", len(results))
        return results

    def _parse_event(self, event: dict) -> Optional[Dict[str, Any]]:
        """Parse a single event from the Odds API response."""
        try:
            home_team = event.get("home_team", "")
            away_team = event.get("away_team", "")

            home_abbrev = ODDS_API_TEAM_MAP.get(home_team, "")
            away_abbrev = ODDS_API_TEAM_MAP.get(away_team, "")

            if not home_abbrev or not away_abbrev:
                logger.debug(
                    "Could not map team names: home='%s', away='%s'",
                    home_team,
                    away_team,
                )

            # Parse bookmaker odds
            bookmakers_raw = event.get("bookmakers", [])
            bookmakers: List[Dict[str, Any]] = []
            all_h2h: List[Dict[str, float]] = []
            all_spreads: List[Dict[str, Any]] = []
            all_totals: List[Dict[str, Any]] = []

            for bm in bookmakers_raw:
                bm_parsed = {
                    "key": bm.get("key", ""),
                    "title": bm.get("title", ""),
                    "last_update": bm.get("last_update", ""),
                    "markets": {},
                }

                for market in bm.get("markets", []):
                    market_key = market.get("key", "")
                    outcomes = market.get("outcomes", [])

                    if market_key == "h2h":
                        h2h = self._parse_h2h_market(
                            outcomes, home_team, away_team
                        )
                        bm_parsed["markets"]["h2h"] = h2h
                        all_h2h.append(h2h)

                    elif market_key == "spreads":
                        spread = self._parse_spread_market(
                            outcomes, home_team
                        )
                        bm_parsed["markets"]["spreads"] = spread
                        all_spreads.append(spread)

                    elif market_key == "totals":
                        total = self._parse_totals_market(outcomes)
                        bm_parsed["markets"]["totals"] = total
                        all_totals.append(total)

                bookmakers.append(bm_parsed)

            # Compute best available odds
            best_odds = self._compute_best_odds(
                all_h2h, all_spreads, all_totals
            )

            return {
                "event_id": event.get("id", ""),
                "commence_time": event.get("commence_time", ""),
                "home_team": home_team,
                "away_team": away_team,
                "home_abbrev": home_abbrev,
                "away_abbrev": away_abbrev,
                "bookmakers": bookmakers,
                "best_odds": best_odds,
            }

        except Exception as exc:
            logger.warning("Failed to parse odds event: %s", exc)
            return None

    @staticmethod
    def _parse_h2h_market(
        outcomes: list, home_team: str, away_team: str
    ) -> Dict[str, float]:
        """Parse head-to-head (moneyline) market outcomes."""
        result: Dict[str, float] = {
            "home": 0.0,
            "away": 0.0,
        }
        for outcome in outcomes:
            name = outcome.get("name", "")
            price = outcome.get("price", 0)
            if name == home_team:
                result["home"] = float(price)
            elif name == away_team:
                result["away"] = float(price)
        return result

    @staticmethod
    def _parse_spread_market(
        outcomes: list, home_team: str
    ) -> Dict[str, Any]:
        """Parse spread (puck line) market outcomes."""
        result: Dict[str, Any] = {
            "home_spread": 0.0,
            "home_price": 0.0,
            "away_spread": 0.0,
            "away_price": 0.0,
        }
        for outcome in outcomes:
            name = outcome.get("name", "")
            point = outcome.get("point", 0.0)
            price = outcome.get("price", 0)
            if name == home_team:
                result["home_spread"] = float(point)
                result["home_price"] = float(price)
            else:
                result["away_spread"] = float(point)
                result["away_price"] = float(price)
        return result

    @staticmethod
    def _parse_totals_market(outcomes: list) -> Dict[str, Any]:
        """Parse totals (over/under) market outcomes."""
        result: Dict[str, Any] = {
            "total": 0.0,
            "over_price": 0.0,
            "under_price": 0.0,
        }
        for outcome in outcomes:
            name = outcome.get("name", "").lower()
            point = outcome.get("point", 0.0)
            price = outcome.get("price", 0)
            if name == "over":
                result["total"] = float(point)
                result["over_price"] = float(price)
            elif name == "under":
                result["total"] = float(point)
                result["under_price"] = float(price)
        return result

    @staticmethod
    def _compute_best_odds(
        all_h2h: List[Dict[str, float]],
        all_spreads: List[Dict[str, Any]],
        all_totals: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Compute the best available odds across all bookmakers.

        For moneyline: best = highest positive or least negative price.
        For spreads: best = most favourable spread for each side.
        For totals: best = the consensus total line.
        """
        best: Dict[str, Any] = {}

        # Best moneyline
        if all_h2h:
            home_prices = [h.get("home", 0) for h in all_h2h if h.get("home")]
            away_prices = [h.get("away", 0) for h in all_h2h if h.get("away")]
            if home_prices:
                best["home_moneyline"] = max(home_prices)
            if away_prices:
                best["away_moneyline"] = max(away_prices)

        # Best spread
        if all_spreads:
            home_spreads = [
                s.get("home_spread", 0) for s in all_spreads if s.get("home_spread")
            ]
            if home_spreads:
                best["home_spread"] = max(home_spreads)

        # Consensus total
        if all_totals:
            totals = [t.get("total", 0) for t in all_totals if t.get("total")]
            if totals:
                best["over_under"] = round(sum(totals) / len(totals), 1)

        return best

    # ------------------------------------------------------------------
    # Sync odds into database
    # ------------------------------------------------------------------

    async def sync_odds(self, db: AsyncSession) -> List[Dict[str, Any]]:
        """
        Fetch current NHL odds and match them to existing Game records.

        Matches odds events to existing Game records by comparing team
        abbreviations and game dates. Returns the matched odds data
        as a list of dicts, each containing the Game id and best odds.

        The Game model does not store odds directly; downstream consumers
        (e.g., the prediction engine) should use the returned list.

        Args:
            db: Async SQLAlchemy session.

        Returns:
            List of dicts with keys: game_id, home_abbrev, away_abbrev,
            game_date, best_odds, bookmakers.
        """
        if not self._has_key:
            logger.warning("No API key; skipping odds sync.")
            return []

        odds_list = await self.fetch_nhl_odds()
        matched: List[Dict[str, Any]] = []

        for odds in odds_list:
            home_abbrev = odds.get("home_abbrev", "")
            away_abbrev = odds.get("away_abbrev", "")

            if not home_abbrev or not away_abbrev:
                continue

            # Parse commence_time to date for matching
            commence = odds.get("commence_time", "")
            game_date = None
            if commence:
                try:
                    dt = datetime.fromisoformat(
                        commence.replace("Z", "+00:00")
                    )
                    game_date = dt.date()
                except (ValueError, TypeError):
                    continue
            else:
                continue

            # Find matching teams
            home_result = await db.execute(
                select(Team).where(Team.abbreviation == home_abbrev)
            )
            home_team = home_result.scalar_one_or_none()

            away_result = await db.execute(
                select(Team).where(Team.abbreviation == away_abbrev)
            )
            away_team = away_result.scalar_one_or_none()

            if not home_team or not away_team:
                logger.debug(
                    "Could not find teams for odds: %s vs %s",
                    home_abbrev,
                    away_abbrev,
                )
                continue

            # Find the matching game
            game_result = await db.execute(
                select(Game).where(
                    Game.home_team_id == home_team.id,
                    Game.away_team_id == away_team.id,
                    Game.date == game_date,
                )
            )
            game = game_result.scalar_one_or_none()

            if game is None:
                logger.debug(
                    "No game found for %s vs %s on %s",
                    home_abbrev,
                    away_abbrev,
                    game_date,
                )
                continue

            matched.append({
                "game_id": game.id,
                "game_external_id": game.external_id,
                "home_abbrev": home_abbrev,
                "away_abbrev": away_abbrev,
                "game_date": str(game_date),
                "best_odds": odds.get("best_odds", {}),
                "bookmakers": odds.get("bookmakers", []),
            })

        logger.info("Odds sync complete: %d games matched", len(matched))
        return matched

    # ------------------------------------------------------------------
    # Usage tracking
    # ------------------------------------------------------------------

    async def get_usage(self) -> Optional[Dict[str, Any]]:
        """
        Check remaining API quota.

        The Odds API returns usage information in response headers.
        This method makes a lightweight request and extracts the
        remaining/used request counts.

        Returns:
            Dict with requests_remaining, requests_used, or None if unavailable.
        """
        if not self._has_key:
            return None

        path = f"/sports/{self.SPORT_KEY}/odds"
        params = {
            "apiKey": self.api_key,
            "regions": self.REGIONS,
            "markets": "h2h",
            "oddsFormat": self.ODDS_FORMAT,
        }

        client = self._get_client()
        await self._wait_for_rate_limit()

        try:
            response = await client.get(path, params=params)
            remaining = response.headers.get("x-requests-remaining")
            used = response.headers.get("x-requests-used")

            usage = {}
            if remaining is not None:
                usage["requests_remaining"] = int(remaining)
            if used is not None:
                usage["requests_used"] = int(used)

            logger.info("Odds API usage: %s", usage)
            return usage if usage else None

        except Exception as exc:
            logger.warning("Could not fetch API usage: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Abstract method implementation
    # ------------------------------------------------------------------

    async def sync_all(self, db: AsyncSession) -> None:
        """Run the full odds sync pipeline."""
        await self.sync_odds(db)
