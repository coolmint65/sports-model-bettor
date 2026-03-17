"""
Player prop odds scraper using The Odds API per-event endpoint.

Fetches NHL player prop markets (ATG, SOG, Points, Assists, Saves)
for today's games and persists them to the PlayerPropOdds table.

Uses the bulk /odds endpoint (cached) to discover event IDs, then
fetches props per-event from /events/{eventId}/odds.  Prop lines
are refreshed on a slow cadence (30 min) since they rarely move.

Credit cost: 5 markets × 1 region × N games per sync.
"""

import asyncio
import logging
import time as _time_mod
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.game import Game
from app.models.player import Player
from app.models.player_prop import PlayerPropOdds
from app.models.team import Team
from app.scrapers.http_helpers import make_request as _make_request_shared

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Markets we care about
# ---------------------------------------------------------------------------

PROP_MARKETS: List[str] = [
    "player_goal_scorer_anytime",  # ATG
    "player_shots_on_goal",        # SOG
    "player_points",               # Points (G+A)
    "player_assists",              # Assists
    "player_total_saves",          # Goalie saves
]

PROP_MARKETS_CSV = ",".join(PROP_MARKETS)

# ---------------------------------------------------------------------------
# Cache — avoids re-fetching props within the TTL window
# ---------------------------------------------------------------------------

_props_cache: Dict[str, List[Dict[str, Any]]] = {}  # event_id -> parsed props
_props_cache_ts: float = 0.0
_PROPS_CACHE_TTL: float = 3600.0  # 60 minutes (conserve Odds API credits)


def props_cache_fresh() -> bool:
    """Return True if the props cache is still within its TTL."""
    return (
        bool(_props_cache)
        and (_time_mod.monotonic() - _props_cache_ts) < _PROPS_CACHE_TTL
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _make_request(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 10.0,
    max_retries: int = 2,
) -> Optional[Any]:
    """GET with retry on 429.  Returns parsed JSON or None.

    Delegates to the shared make_request helper in http_helpers.
    """
    return await _make_request_shared(
        client, url, params=params, timeout=timeout, max_retries=max_retries,
    )


def _parse_prop_outcomes(
    market_key: str,
    outcomes: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Parse outcomes from an Odds API prop market into our internal format.

    For over/under markets (SOG, Points, Assists, Saves):
        Groups outcomes by (player_name, point) into over/under pairs.

    For anytime goal scorer:
        Each outcome is a simple yes price (no line).
    """
    props: List[Dict[str, Any]] = []

    if market_key == "player_goal_scorer_anytime":
        # Each outcome is one player with a "yes" price
        for oc in outcomes:
            player_name = oc.get("description", oc.get("name", ""))
            price = oc.get("price")
            if not player_name or price is None:
                continue
            props.append({
                "player_name": player_name,
                "market": market_key,
                "line": None,  # ATG has no line
                "over_price": float(price),
                "under_price": None,
            })
    else:
        # Over/under market — group by (player, point)
        by_key: Dict[Tuple[str, float], Dict[str, Any]] = {}
        for oc in outcomes:
            player_name = oc.get("description", "")
            point = oc.get("point")
            price = oc.get("price")
            side = oc.get("name", "").lower()  # "Over" or "Under"
            if not player_name or point is None or price is None:
                continue
            key = (player_name, float(point))
            if key not in by_key:
                by_key[key] = {
                    "player_name": player_name,
                    "market": market_key,
                    "line": float(point),
                }
            if "over" in side:
                by_key[key]["over_price"] = float(price)
            elif "under" in side:
                by_key[key]["under_price"] = float(price)

        for entry in by_key.values():
            # Only include if we have at least one side
            if "over_price" in entry or "under_price" in entry:
                entry.setdefault("over_price", None)
                entry.setdefault("under_price", None)
                props.append(entry)

    return props


async def _fetch_event_props(
    client: httpx.AsyncClient,
    event_id: str,
) -> List[Dict[str, Any]]:
    """Fetch player props for a single Odds API event.

    Returns a list of prop dicts ready for DB persistence.
    Cost: 5 credits (1 per market with data × 1 region).
    """
    api_key = settings.odds_api_key
    if not api_key:
        return []

    url = (
        f"https://api.the-odds-api.com/v4/sports/icehockey_nhl"
        f"/events/{event_id}/odds"
    )
    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": PROP_MARKETS_CSV,
        "oddsFormat": "american",
    }

    data = await _make_request(client, url, params=params)
    if not data or not isinstance(data, dict):
        return []

    all_props: List[Dict[str, Any]] = []
    home_team = data.get("home_team", "")
    away_team = data.get("away_team", "")

    # Aggregate best lines across all bookmakers.
    # For each (player, market, line) keep the best over and under prices.
    best: Dict[Tuple[str, str, Optional[float]], Dict[str, Any]] = {}

    for bm in data.get("bookmakers", []):
        bm_key = bm.get("key", "")
        for market in bm.get("markets", []):
            mkey = market.get("key", "")
            if mkey not in PROP_MARKETS:
                continue
            outcomes = market.get("outcomes", [])
            parsed = _parse_prop_outcomes(mkey, outcomes)
            for prop in parsed:
                pk = (prop["player_name"], prop["market"], prop.get("line"))
                if pk not in best:
                    best[pk] = {**prop, "bookmaker": bm_key}
                else:
                    existing = best[pk]
                    # Keep highest over_price (best for bettor)
                    op = prop.get("over_price")
                    if op is not None:
                        if existing.get("over_price") is None or op > existing["over_price"]:
                            existing["over_price"] = op
                            existing["bookmaker"] = bm_key
                    # Keep highest under_price (best for bettor)
                    up = prop.get("under_price")
                    if up is not None:
                        if existing.get("under_price") is None or up > existing["under_price"]:
                            existing["under_price"] = up

    all_props = list(best.values())
    logger.debug(
        "Props for event %s (%s vs %s): %d lines across %d markets",
        event_id, away_team, home_team, len(all_props),
        len({p["market"] for p in all_props}),
    )
    return all_props


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fetch_all_player_props(
    client: httpx.AsyncClient,
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch player props for all today's NHL events.

    Uses the cached bulk odds response to discover event IDs (free),
    then fetches props per-event (5 credits each).

    Returns {event_id: [prop_dicts]}.
    """
    global _props_cache, _props_cache_ts

    if props_cache_fresh():
        logger.debug(
            "Player props: using cache (%d events, age %.0fs)",
            len(_props_cache),
            _time_mod.monotonic() - _props_cache_ts,
        )
        return _props_cache

    # Discover event IDs from the cached bulk odds response
    from app.scrapers.odds_multi import _fetch_odds_api_raw

    raw = await _fetch_odds_api_raw(client)
    if not raw:
        logger.warning("Player props: no bulk odds data — cannot discover events")
        return _props_cache  # return stale cache if available

    event_ids = [ev.get("id") for ev in raw if ev.get("id")]
    if not event_ids:
        logger.warning("Player props: no event IDs in bulk response")
        return _props_cache

    logger.info(
        "Player props: fetching %d events × %d markets = ~%d credits",
        len(event_ids), len(PROP_MARKETS), len(event_ids) * len(PROP_MARKETS),
    )

    # Fetch props for all events concurrently
    results = await asyncio.gather(
        *(_fetch_event_props(client, eid) for eid in event_ids),
        return_exceptions=True,
    )

    new_cache: Dict[str, List[Dict[str, Any]]] = {}
    for eid, result in zip(event_ids, results):
        if isinstance(result, list):
            new_cache[eid] = result
        else:
            logger.warning("Props fetch failed for event %s: %s", eid, result)

    total_props = sum(len(v) for v in new_cache.values())
    logger.info(
        "Player props: fetched %d prop lines across %d events",
        total_props, len(new_cache),
    )

    _props_cache = new_cache
    _props_cache_ts = _time_mod.monotonic()
    return _props_cache


async def sync_player_props(db: AsyncSession) -> int:
    """Fetch player props and persist to PlayerPropOdds table.

    Matches events to Game records by team names and date,
    then upserts prop lines.

    Returns the number of prop lines synced.
    """
    from app.scrapers.odds_multi import _fetch_odds_api_raw, _map_team

    async with httpx.AsyncClient(
        follow_redirects=True,
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
    ) as client:
        props_by_event = await fetch_all_player_props(client)

    if not props_by_event:
        return 0

    # We need to map event_id -> Game.  Re-read the bulk data for team info.
    async with httpx.AsyncClient(
        follow_redirects=True,
        limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
    ) as client:
        raw = await _fetch_odds_api_raw(client)

    if not raw:
        return 0

    # Build event_id -> (home_abbr, away_abbr, commence_time) map
    event_meta: Dict[str, Dict[str, Any]] = {}
    for ev in raw:
        eid = ev.get("id", "")
        if not eid:
            continue
        home_abbr = _map_team(ev.get("home_team", ""))
        away_abbr = _map_team(ev.get("away_team", ""))
        if home_abbr and away_abbr:
            event_meta[eid] = {
                "home_abbr": home_abbr,
                "away_abbr": away_abbr,
                "commence_time": ev.get("commence_time", ""),
            }

    # Resolve team abbreviations -> Game records
    now_utc = datetime.now(timezone.utc)
    synced = 0

    for event_id, prop_list in props_by_event.items():
        if not prop_list:
            continue
        meta = event_meta.get(event_id)
        if not meta:
            logger.debug("Props sync: no metadata for event %s", event_id)
            continue

        # Find the Game record
        home_abbr = meta["home_abbr"]
        away_abbr = meta["away_abbr"]

        # Parse commence time to local date
        commence = meta.get("commence_time", "")
        game_date = None
        if commence:
            try:
                ct = commence.replace("Z", "+00:00") if isinstance(commence, str) else commence
                dt = datetime.fromisoformat(ct) if isinstance(ct, str) else ct
                dt_et = dt.astimezone(ZoneInfo("America/New_York"))
                game_date = dt_et.date()
            except (ValueError, TypeError, AttributeError):
                continue

        if not game_date:
            continue

        # Look up teams
        home_result = await db.execute(
            select(Team).where(
                Team.abbreviation == home_abbr,
                Team.sport == "nhl",
            )
        )
        home_team = home_result.scalar_one_or_none()

        away_result = await db.execute(
            select(Team).where(
                Team.abbreviation == away_abbr,
                Team.sport == "nhl",
            )
        )
        away_team = away_result.scalar_one_or_none()

        if not home_team or not away_team:
            continue

        # Find game (try exact date ± 1 day for DST edge cases)
        game = None
        for d in (game_date, game_date - timedelta(days=1), game_date + timedelta(days=1)):
            game_result = await db.execute(
                select(Game).where(
                    Game.home_team_id == home_team.id,
                    Game.away_team_id == away_team.id,
                    Game.date == d,
                )
            )
            game = game_result.scalar_one_or_none()
            if game:
                break

        if not game:
            logger.debug(
                "Props sync: no game for %s@%s on %s",
                away_abbr, home_abbr, game_date,
            )
            continue

        # Try to match player names to Player records (best-effort)
        # Build a lookup of lowercase name -> player_id for this game's teams
        players_result = await db.execute(
            select(Player).where(
                Player.team_id.in_([home_team.id, away_team.id]),
                Player.active == True,
            )
        )
        players = players_result.scalars().all()
        player_lookup: Dict[str, int] = {}
        for p in players:
            player_lookup[p.name.lower()] = p.id
            # Also store last name for fuzzy matching
            parts = p.name.split()
            if len(parts) >= 2:
                player_lookup[parts[-1].lower()] = p.id

        # Upsert props
        for prop in prop_list:
            player_name = prop["player_name"]
            market = prop["market"]

            # Try to resolve player_id
            player_id = player_lookup.get(player_name.lower())
            if player_id is None:
                # Try last name match
                name_parts = player_name.split()
                if len(name_parts) >= 2:
                    player_id = player_lookup.get(name_parts[-1].lower())

            # Check for existing record (upsert)
            existing_result = await db.execute(
                select(PlayerPropOdds).where(
                    PlayerPropOdds.game_id == game.id,
                    PlayerPropOdds.player_name == player_name,
                    PlayerPropOdds.market == market,
                )
            )
            existing = existing_result.scalar_one_or_none()

            if existing:
                existing.line = prop.get("line")
                existing.over_price = prop.get("over_price")
                existing.under_price = prop.get("under_price")
                existing.bookmaker = prop.get("bookmaker")
                existing.player_id = player_id or existing.player_id
                existing.odds_updated_at = now_utc
            else:
                new_prop = PlayerPropOdds(
                    game_id=game.id,
                    player_id=player_id,
                    player_name=player_name,
                    market=market,
                    line=prop.get("line"),
                    over_price=prop.get("over_price"),
                    under_price=prop.get("under_price"),
                    opening_over_price=prop.get("over_price"),
                    opening_under_price=prop.get("under_price"),
                    bookmaker=prop.get("bookmaker"),
                    odds_updated_at=now_utc,
                )
                db.add(new_prop)

            synced += 1

    await db.flush()
    logger.info("Player props: synced %d prop lines", synced)
    return synced
