"""
Player prop odds scraper using The Odds API per-event endpoint.

Fetches sport-specific player prop markets for today's games and
persists them to the PlayerPropOdds table.

Uses the bulk /odds endpoint (cached via gateway) to discover event
IDs, then fetches props per-event.  Prop lines are refreshed on a
slow cadence (60 min) since they rarely move.

Credit cost: N markets × 1 region × G games per sync.
"""

import asyncio
import logging
import time as _time_mod
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.game import Game
from app.models.player import Player
from app.models.player_prop import PlayerPropOdds
from app.models.team import Team

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache — avoids re-fetching props within the TTL window.
# Keyed by sport to prevent cross-sport cache collisions.
# ---------------------------------------------------------------------------

_props_cache: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}  # sport -> {event_id -> props}
_props_cache_ts: Dict[str, float] = {}  # sport -> monotonic timestamp
_PROPS_CACHE_TTL: float = 3600.0  # 60 minutes (conserve Odds API credits)

# Limit concurrent per-event requests to avoid triggering 429 rate limits.
_CONCURRENT_EVENT_LIMIT = 3


def props_cache_fresh(sport: str = "nhl") -> bool:
    """Return True if the props cache is still within its TTL for a sport."""
    ts = _props_cache_ts.get(sport, 0.0)
    return (
        bool(_props_cache.get(sport))
        and (_time_mod.monotonic() - ts) < _PROPS_CACHE_TTL
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_prop_outcomes(
    market_key: str,
    outcomes: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Parse outcomes from an Odds API prop market into our internal format.

    For over/under markets (SOG, Points, Assists, Saves, Rebounds, etc.):
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
    event_id: str,
    sport: str = "nhl",
) -> List[Dict[str, Any]]:
    """Fetch player props for a single event via the gateway.

    Returns a list of prop dicts ready for DB persistence.
    """
    from app.scrapers.odds_gateway import fetch_event_odds

    sport_cfg = settings.get_sport_config(sport)
    markets_csv = ",".join(sport_cfg.odds_api_prop_markets)
    if not markets_csv:
        return []

    data = await fetch_event_odds(
        sport, event_id,
        markets=markets_csv,
        regions=sport_cfg.odds_api_regions,
    )
    if not data or not isinstance(data, dict):
        return []

    all_props: List[Dict[str, Any]] = []
    home_team = data.get("home_team", "")
    away_team = data.get("away_team", "")

    prop_markets = set(sport_cfg.odds_api_prop_markets)

    # Aggregate best lines across all bookmakers.
    best: Dict[Tuple[str, str, Optional[float]], Dict[str, Any]] = {}

    for bm in data.get("bookmakers", []):
        bm_key = bm.get("key", "")
        for market in bm.get("markets", []):
            mkey = market.get("key", "")
            if mkey not in prop_markets:
                continue
            outcomes = market.get("outcomes", [])
            parsed = _parse_prop_outcomes(mkey, outcomes)
            for prop in parsed:
                pk = (prop["player_name"], prop["market"], prop.get("line"))
                if pk not in best:
                    best[pk] = {**prop, "bookmaker": bm_key}
                else:
                    existing = best[pk]
                    op = prop.get("over_price")
                    if op is not None:
                        if existing.get("over_price") is None or op > existing["over_price"]:
                            existing["over_price"] = op
                            existing["bookmaker"] = bm_key
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
    sport: str = "nhl",
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch player props for all today's events of a given sport.

    Uses the cached bulk odds response (via gateway) to discover event
    IDs, then fetches props per-event.

    Returns {event_id: [prop_dicts]}.
    """
    global _props_cache, _props_cache_ts

    if props_cache_fresh(sport):
        cached = _props_cache.get(sport, {})
        logger.debug(
            "Player props: using %s cache (%d events, age %.0fs)",
            sport.upper(), len(cached),
            _time_mod.monotonic() - _props_cache_ts.get(sport, 0.0),
        )
        return cached

    # Discover event IDs from the cached bulk odds response
    from app.scrapers.odds_gateway import fetch_bulk_odds

    raw = await fetch_bulk_odds(sport)
    if not raw:
        logger.warning("Player props: no bulk %s odds data -- cannot discover events", sport.upper())
        return _props_cache.get(sport, {})

    event_ids = [ev.get("id") for ev in raw if ev.get("id")]
    if not event_ids:
        logger.warning("Player props: no event IDs in %s bulk response", sport.upper())
        return _props_cache.get(sport, {})

    sport_cfg = settings.get_sport_config(sport)
    n_markets = len(sport_cfg.odds_api_prop_markets)

    logger.info(
        "Player props: fetching %d %s events × %d markets = ~%d credits",
        len(event_ids), sport.upper(), n_markets, len(event_ids) * n_markets,
    )

    # Fetch props with bounded concurrency to avoid 429 rate limits.
    sem = asyncio.Semaphore(_CONCURRENT_EVENT_LIMIT)

    async def _throttled_fetch(eid: str) -> List[Dict[str, Any]]:
        async with sem:
            return await _fetch_event_props(eid, sport=sport)

    results = await asyncio.gather(
        *(_throttled_fetch(eid) for eid in event_ids),
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
        "Player props: fetched %d %s prop lines across %d events",
        total_props, sport.upper(), len(new_cache),
    )

    _props_cache[sport] = new_cache
    _props_cache_ts[sport] = _time_mod.monotonic()
    return new_cache


async def sync_player_props(db: AsyncSession, sport: str = "nhl") -> int:
    """Fetch player props and persist to PlayerPropOdds table.

    Matches events to Game records by team names and date,
    then upserts prop lines.

    Returns the number of prop lines synced.
    """
    from app.scrapers.odds_gateway import fetch_bulk_odds
    from app.scrapers.team_map import resolve_team_for_sport

    props_by_event = await fetch_all_player_props(sport=sport)

    if not props_by_event:
        return 0

    # Re-read the bulk data for team info (uses gateway cache — free).
    raw = await fetch_bulk_odds(sport)

    if not raw:
        return 0

    # Build event_id -> (home_abbr, away_abbr, commence_time) map
    event_meta: Dict[str, Dict[str, Any]] = {}
    for ev in raw:
        eid = ev.get("id", "")
        if not eid:
            continue
        home_abbr = resolve_team_for_sport(ev.get("home_team", ""), sport)
        away_abbr = resolve_team_for_sport(ev.get("away_team", ""), sport)
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

        # Look up teams with sport filter
        home_result = await db.execute(
            select(Team).where(
                Team.abbreviation == home_abbr,
                Team.sport == sport,
            ).order_by(Team.id)
        )
        home_team = home_result.scalars().first()

        away_result = await db.execute(
            select(Team).where(
                Team.abbreviation == away_abbr,
                Team.sport == sport,
            ).order_by(Team.id)
        )
        away_team = away_result.scalars().first()

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
    logger.info("Player props: synced %d %s prop lines", synced, sport.upper())
    return synced
