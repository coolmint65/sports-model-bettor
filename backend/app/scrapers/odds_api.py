"""
Odds scraper using The Odds API (https://api.the-odds-api.com/v4).

Fetches current betting odds for NHL games and normalises them into
a standard format compatible with the Game model.

Requires an API key set via the ODDS_API_KEY environment variable.
Operates gracefully when no key is configured (logs a warning, returns empty).
"""

import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.game import Game
from app.models.team import Team
from app.scrapers.base import BaseScraper, ScraperError
from app.scrapers.team_map import NHL_TEAM_MAP, resolve_team, resolve_nba_team, resolve_team_for_sport

logger = logging.getLogger(__name__)

# Re-export for any code that imports ODDS_API_TEAM_MAP from here.
ODDS_API_TEAM_MAP = NHL_TEAM_MAP

# Sport key mapping for The Odds API
ODDS_API_SPORT_KEYS = {
    "nhl": "icehockey_nhl",
    "nba": "basketball_nba",
}


class OddsScraper(BaseScraper):
    """
    Scraper for The Odds API.

    Fetches live and pre-match odds for games. Supports moneyline,
    spread, and totals (over/under) markets for multiple sports.

    The API key is loaded from `settings.odds_api_key`. If no key is
    configured, all fetch methods return empty results with a warning.
    """

    REGIONS = "us"
    ODDS_FORMAT = "american"
    # Cache odds for 60 seconds — balances freshness vs API credits.
    DEFAULT_CACHE_TTL = 60.0

    def __init__(
        self,
        base_url: str = settings.odds_api_base,
        api_key: Optional[str] = None,
        rate_limit: float = 1.0,
        sport: str = "nhl",
        **kwargs,
    ):
        super().__init__(base_url=base_url, rate_limit=rate_limit, **kwargs)
        self.api_key = api_key or settings.odds_api_key
        self.sport = sport
        self.sport_key = ODDS_API_SPORT_KEYS.get(sport, "icehockey_nhl")
        if not self.api_key:
            logger.warning(
                "No ODDS_API_KEY configured. Odds fetching will be disabled. "
                "Set the ODDS_API_KEY environment variable to enable."
            )

    @property
    def _has_key(self) -> bool:
        return bool(self.api_key)

    def _resolve_team(self, name: str) -> str:
        """Resolve team name with direct + fuzzy matching."""
        return resolve_team_for_sport(name, self.sport)

    # ------------------------------------------------------------------
    # Fetch odds
    # ------------------------------------------------------------------

    async def fetch_odds(
        self,
        markets: str = "h2h,spreads,totals",
    ) -> List[Dict[str, Any]]:
        """
        Fetch current odds for all upcoming games of the configured sport.

        Args:
            markets: Comma-separated market types. Defaults to
                     "h2h,spreads,totals" (moneyline, spread, over/under).

        Returns:
            List of normalised odds dicts, one per game.
        """
        if not self._has_key:
            logger.warning("Odds API key not set; returning empty odds list.")
            return []

        path = f"/sports/{self.sport_key}/odds"
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

        logger.info("Fetched odds for %d %s events", len(results), self.sport.upper())
        return results

    async def fetch_nhl_odds(
        self,
        markets: str = "h2h,spreads,totals",
    ) -> List[Dict[str, Any]]:
        """Backward-compatible alias for fetch_odds (NHL)."""
        return await self.fetch_odds(markets=markets)

    def _parse_event(self, event: dict) -> Optional[Dict[str, Any]]:
        """Parse a single event from the Odds API response."""
        try:
            home_team = event.get("home_team", "")
            away_team = event.get("away_team", "")

            home_abbrev = self._resolve_team(home_team)
            away_abbrev = self._resolve_team(away_team)

            if not home_abbrev or not away_abbrev:
                logger.warning(
                    "Odds API: UNMAPPED team -- home=%r->%r, away=%r->%r",
                    home_team, home_abbrev, away_team, away_abbrev,
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
                            outcomes, home_team, away_team
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
        outcomes: list, home_team: str, away_team: str
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
            elif name == away_team:
                result["away_spread"] = float(point)
                result["away_price"] = float(price)
            else:
                logger.debug("Spread outcome name '%s' didn't match home='%s' or away='%s'", name, home_team, away_team)
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
        For totals: best = the consensus total line with prices.
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

        # Consensus spread with averaged prices
        if all_spreads:
            home_spreads = [
                s for s in all_spreads if s.get("home_spread")
            ]
            if home_spreads:
                # Find the most common absolute spread value (mode)
                spread_counts = Counter(
                    abs(s["home_spread"]) for s in home_spreads
                )
                consensus_abs = spread_counts.most_common(1)[0][0]
                # Books at this absolute spread
                consensus_books = [
                    s for s in home_spreads
                    if abs(s["home_spread"]) == consensus_abs
                ]
                # Average the signed spreads and prices from consensus books
                avg_home_spread = sum(s["home_spread"] for s in consensus_books) / len(consensus_books)
                avg_away_spread = sum(s.get("away_spread", -s["home_spread"]) for s in consensus_books) / len(consensus_books)
                hp = [s.get("home_price", -110) for s in consensus_books]
                ap = [s.get("away_price", -110) for s in consensus_books]

                best["home_spread"] = round(avg_home_spread, 1)
                best["away_spread"] = round(avg_away_spread, 1)
                best["home_spread_price"] = round(sum(hp) / len(hp))
                best["away_spread_price"] = round(sum(ap) / len(ap))

        # Consensus total with averaged prices
        if all_totals:
            totals = [t.get("total", 0) for t in all_totals if t.get("total")]
            if totals:
                # Find the most common total line
                total_counts = Counter(totals)
                consensus_total = total_counts.most_common(1)[0][0]
                best["over_under"] = consensus_total
            # Average over/under prices for books at the consensus line
            consensus_totals = [t for t in all_totals if t.get("total") == best.get("over_under")]
            if not consensus_totals:
                consensus_totals = all_totals
            over_prices = [t.get("over_price", 0) for t in consensus_totals if t.get("over_price")]
            under_prices = [t.get("under_price", 0) for t in consensus_totals if t.get("under_price")]
            if over_prices:
                best["over_price"] = round(sum(over_prices) / len(over_prices))
            if under_prices:
                best["under_price"] = round(sum(under_prices) / len(under_prices))

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

        odds_list = await self.fetch_odds()
        matched: List[Dict[str, Any]] = []

        for odds in odds_list:
            home_abbrev = odds.get("home_abbrev", "")
            away_abbrev = odds.get("away_abbrev", "")

            if not home_abbrev or not away_abbrev:
                continue

            # Parse commence_time to the LOCAL game date.
            # The DB stores Game.date as the local (ET) calendar date,
            # but commence_time from the Odds API is UTC.  A 7 PM ET
            # game is midnight UTC the next day — so we must convert to
            # ET before extracting .date() to avoid a one-day mismatch
            # for evening games.
            commence = odds.get("commence_time", "")
            game_date = None
            if commence:
                try:
                    dt = datetime.fromisoformat(
                        commence.replace("Z", "+00:00")
                    )
                    dt_et = dt.astimezone(ZoneInfo("America/New_York"))
                    game_date = dt_et.date()
                except (ValueError, TypeError):
                    continue
            else:
                continue

            # Find matching teams (scoped to sport)
            home_result = await db.execute(
                select(Team).where(
                    Team.abbreviation == home_abbrev,
                    Team.sport == self.sport,
                )
            )
            home_team = home_result.scalar_one_or_none()

            away_result = await db.execute(
                select(Team).where(
                    Team.abbreviation == away_abbrev,
                    Team.sport == self.sport,
                )
            )
            away_team = away_result.scalar_one_or_none()

            if not home_team or not away_team:
                logger.debug(
                    "Could not find teams for odds: %s vs %s",
                    home_abbrev,
                    away_abbrev,
                )
                continue

            # Find the matching game.  Try exact date first, then
            # adjacent days as a safety net for DST edge cases.
            game = None
            for candidate_date in (game_date, game_date - timedelta(days=1), game_date + timedelta(days=1)):
                game_result = await db.execute(
                    select(Game).where(
                        Game.home_team_id == home_team.id,
                        Game.away_team_id == away_team.id,
                        Game.date == candidate_date,
                    )
                )
                game = game_result.scalar_one_or_none()
                if game is not None:
                    break

            if game is None:
                logger.debug(
                    "No game found for %s vs %s on %s (±1 day)",
                    home_abbrev,
                    away_abbrev,
                    game_date,
                )
                continue

            # Persist odds to the Game record
            best_odds = odds.get("best_odds", {})
            if best_odds.get("home_moneyline") is not None:
                game.home_moneyline = best_odds["home_moneyline"]
            if best_odds.get("away_moneyline") is not None:
                game.away_moneyline = best_odds["away_moneyline"]
            if best_odds.get("over_under") is not None:
                ou_raw = float(best_odds["over_under"])
                # Normalize whole-number lines to .5 (e.g., 7 → 6.5)
                if ou_raw % 1 != 0.5:
                    ou_raw = float(int(ou_raw) - 1) + 0.5
                game.over_under_line = ou_raw
            if best_odds.get("home_spread") is not None:
                game.home_spread_line = best_odds["home_spread"]
            if best_odds.get("away_spread") is not None:
                game.away_spread_line = best_odds["away_spread"]
            if best_odds.get("home_spread_price") is not None:
                game.home_spread_price = best_odds["home_spread_price"]
            if best_odds.get("away_spread_price") is not None:
                game.away_spread_price = best_odds["away_spread_price"]
            if best_odds.get("over_price") is not None:
                game.over_price = best_odds["over_price"]
            if best_odds.get("under_price") is not None:
                game.under_price = best_odds["under_price"]
            game.odds_updated_at = datetime.now(timezone.utc)

            matched.append({
                "game_id": game.id,
                "game_external_id": game.external_id,
                "home_abbrev": home_abbrev,
                "away_abbrev": away_abbrev,
                "game_date": str(game_date),
                "best_odds": best_odds,
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

        path = f"/sports/{self.sport_key}/odds"
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
