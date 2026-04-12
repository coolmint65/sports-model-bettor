"""
ESPN-backed NBA odds scraper with Q1-market support.

Fallback when The Odds API plan doesn't expose Q1 markets AND DK
geo-blocks the user's IP. ESPN's public summary endpoint reliably
returns full-game odds and, for games where ESPN BET is the posted
provider, period-split odds as well.

Endpoints tried (in order):
1. /apis/site/v2/sports/basketball/nba/summary?event={eventId}
     → pickcenter array. Each provider may include `periodOdds` with
       period: 1 entries holding Q1 spread / total / ML.
2. sports.core.api.espn.com/.../events/{id}/competitions/{id}/odds
     → same provider list, sometimes with `firstQuarterOdds` or
       `periods` arrays.
3. sports.core.api.espn.com/.../events/{id}/competitions/{id}/
   odds/{providerId}/periods/1
     → last-resort per-period per-provider fetch.

Returns dict keyed by "AWAY@HOME" (same abbr convention as nba_odds.py)
with the same schema so downstream code can merge transparently.
"""

import json
import logging
import time
import urllib.request
from datetime import datetime

logger = logging.getLogger(__name__)

# Same team-name → abbr map as scrapers/nba_odds.py so keys line up
_NBA_NAME_TO_ABBR: dict[str, str] = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA", "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN", "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "LA Clippers": "LAC", "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM", "Miami Heat": "MIA", "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NO",
    "New York Knicks": "NY", "Oklahoma City Thunder": "OKC", "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX", "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC", "San Antonio Spurs": "SA", "Toronto Raptors": "TOR",
    "Utah Jazz": "UTAH", "Washington Wizards": "WSH",
}

# ESPN sometimes uses different abbreviations — normalize both directions
_ESPN_ABBR_ALT = {
    "NOP": "NO", "NYK": "NY", "SAS": "SA", "UTA": "UTAH", "WAS": "WSH",
    "BRK": "BKN", "BKN": "BKN", "CHO": "CHA",
}

SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
SUMMARY_URL = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/summary"
CORE_ODDS_URL = ("https://sports.core.api.espn.com/v2/sports/basketball/"
                 "leagues/nba/events/{event_id}/competitions/{event_id}/odds")

_cache: dict | None = None
_cache_ts: float = 0
CACHE_TTL = 600  # 10 minutes


def _fetch(url: str, timeout: int = 12) -> dict | None:
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        logger.debug("ESPN fetch failed: %s (%s)", url, e)
        return None


def _team_abbr(name: str) -> str:
    """Map ESPN team name (e.g. 'Boston Celtics') or abbr to our canonical form."""
    if not name:
        return ""
    # First try full name
    if name in _NBA_NAME_TO_ABBR:
        return _NBA_NAME_TO_ABBR[name]
    # Try normalized abbreviation
    up = name.strip().upper()
    return _ESPN_ABBR_ALT.get(up, up)


def _todays_events() -> list[dict]:
    """Return a list of {event_id, away_abbr, home_abbr} for today's NBA games."""
    date = datetime.now().strftime("%Y%m%d")
    data = _fetch(f"{SCOREBOARD_URL}?dates={date}")
    if not data:
        return []

    events = []
    for event in data.get("events", []) or []:
        eid = event.get("id")
        if not eid:
            continue
        comp = (event.get("competitions") or [{}])[0]
        # Skip finals/live — Q1 odds are irrelevant once the quarter has started
        status = (comp.get("status") or {}).get("type") or {}
        if status.get("state") in ("post", "in"):
            continue
        away_abbr = home_abbr = ""
        for c in comp.get("competitors") or []:
            team = c.get("team") or {}
            abbr_raw = team.get("abbreviation") or team.get("displayName") or ""
            abbr = _team_abbr(abbr_raw) or _team_abbr(team.get("displayName", ""))
            if c.get("homeAway") == "home":
                home_abbr = abbr
            else:
                away_abbr = abbr
        if eid and away_abbr and home_abbr:
            events.append({
                "event_id": str(eid),
                "away_abbr": away_abbr,
                "home_abbr": home_abbr,
            })
    return events


# ── Provider-response parsing ──

def _parse_provider_block(provider_entry: dict, home_abbr: str, away_abbr: str,
                          result: dict) -> None:
    """Pull full-game + Q1 data out of one pickcenter/odds provider entry.

    Defensively handles the many shapes ESPN returns across endpoints:
      - Top-level: moneyLine/overUnder/spread + *Odds fields
      - homeTeamOdds/awayTeamOdds with moneyLine, spreadOdds
      - periodOdds: list of period-scoped odds blocks
      - periods: alternate name for the same data
      - firstQuarterOdds / quarterOdds
    """
    # Full-game
    home_to = provider_entry.get("homeTeamOdds") or {}
    away_to = provider_entry.get("awayTeamOdds") or {}

    if result.get("home_ml") is None:
        ml = home_to.get("moneyLine") or home_to.get("moneyLineValue")
        if ml is not None:
            result["home_ml"] = _as_int(ml)
    if result.get("away_ml") is None:
        ml = away_to.get("moneyLine") or away_to.get("moneyLineValue")
        if ml is not None:
            result["away_ml"] = _as_int(ml)

    if result.get("over_under") is None:
        ou = provider_entry.get("overUnder")
        if ou is not None:
            result["over_under"] = _as_float(ou)
    if result.get("over_odds") is None:
        o = provider_entry.get("overOdds") or home_to.get("overOdds")
        if o is not None:
            result["over_odds"] = _as_int(o)
    if result.get("under_odds") is None:
        u = provider_entry.get("underOdds") or away_to.get("underOdds")
        if u is not None:
            result["under_odds"] = _as_int(u)

    if result.get("home_spread_point") is None:
        sp = provider_entry.get("spread")
        if sp is not None:
            sp_val = _as_float(sp)
            # ESPN's "spread" is signed for the FAVORITE, not for home.
            # Detect which side is favored via the favorite flag (when
            # present) or via the moneyline signs.
            home_fav = bool(home_to.get("favorite"))
            away_fav = bool(away_to.get("favorite"))
            if not home_fav and not away_fav:
                h_ml = _as_int(home_to.get("moneyLine") or home_to.get("moneyLineValue"))
                a_ml = _as_int(away_to.get("moneyLine") or away_to.get("moneyLineValue"))
                if h_ml is not None and a_ml is not None:
                    # Lower (more negative) ML = favorite
                    home_fav = h_ml < a_ml
                    away_fav = not home_fav
            # Spread magnitude (always positive distance)
            mag = abs(sp_val)
            if home_fav:
                result["home_spread_point"] = -mag
                result["away_spread_point"] = +mag
            elif away_fav:
                result["home_spread_point"] = +mag
                result["away_spread_point"] = -mag
            else:
                # Can't tell — leave spread unassigned rather than guess
                pass
    if result.get("home_spread_odds") is None:
        s = home_to.get("spreadOdds")
        if s is not None:
            result["home_spread_odds"] = _as_int(s)
    if result.get("away_spread_odds") is None:
        s = away_to.get("spreadOdds")
        if s is not None:
            result["away_spread_odds"] = _as_int(s)

    # ── Q1 / Period 1 ──
    period_blocks = (
        provider_entry.get("periodOdds")
        or provider_entry.get("periods")
        or []
    )
    # Some responses put Q1 data at a first-quarter-specific top-level key
    quarter_block = (provider_entry.get("firstQuarterOdds")
                     or provider_entry.get("quarterOneOdds")
                     or None)
    if quarter_block:
        period_blocks = list(period_blocks) + [
            {**quarter_block, "period": 1}
        ]

    for pb in period_blocks:
        if not isinstance(pb, dict):
            continue
        period = pb.get("period") or pb.get("number") or pb.get("periodNumber")
        try:
            period = int(period)
        except (ValueError, TypeError):
            continue
        if period != 1:
            continue

        h_to = pb.get("homeTeamOdds") or {}
        a_to = pb.get("awayTeamOdds") or {}

        h_ml = h_to.get("moneyLine") or pb.get("homeMoneyLine")
        a_ml = a_to.get("moneyLine") or pb.get("awayMoneyLine")
        if h_ml is not None and result.get("q1_home_ml") is None:
            result["q1_home_ml"] = _as_int(h_ml)
        if a_ml is not None and result.get("q1_away_ml") is None:
            result["q1_away_ml"] = _as_int(a_ml)

        sp = pb.get("spread")
        if sp is not None and result.get("q1_spread") is None:
            sp_val = _as_float(sp)
            # generate_q1_picks treats q1_spread as the HOME spread.
            # ESPN's period spread is for the favorite — detect and orient.
            q_home_fav = bool(h_to.get("favorite"))
            q_away_fav = bool(a_to.get("favorite"))
            if not q_home_fav and not q_away_fav:
                h_ml = _as_int(h_to.get("moneyLine"))
                a_ml = _as_int(a_to.get("moneyLine"))
                if h_ml is not None and a_ml is not None:
                    q_home_fav = h_ml < a_ml
                    q_away_fav = not q_home_fav
            mag = abs(sp_val)
            if q_home_fav:
                result["q1_spread"] = -mag
            elif q_away_fav:
                result["q1_spread"] = +mag
            else:
                result["q1_spread"] = sp_val
        h_sp_odds = h_to.get("spreadOdds")
        a_sp_odds = a_to.get("spreadOdds")
        if h_sp_odds is not None and result.get("q1_spread_home_odds") is None:
            result["q1_spread_home_odds"] = _as_int(h_sp_odds)
        if a_sp_odds is not None and result.get("q1_spread_away_odds") is None:
            result["q1_spread_away_odds"] = _as_int(a_sp_odds)

        ou = pb.get("overUnder") or pb.get("total")
        if ou is not None and result.get("q1_total") is None:
            result["q1_total"] = _as_float(ou)
        ov = pb.get("overOdds") or h_to.get("overOdds")
        un = pb.get("underOdds") or a_to.get("underOdds")
        if ov is not None and result.get("q1_over_odds") is None:
            result["q1_over_odds"] = _as_int(ov)
        if un is not None and result.get("q1_under_odds") is None:
            result["q1_under_odds"] = _as_int(un)


def _as_int(val) -> int | None:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            return int(val)
        s = str(val).replace("+", "").strip()
        return int(s) if s else None
    except (ValueError, TypeError):
        return None


def _as_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ── Main entry point ──

def fetch_nba_espn_odds() -> dict:
    """Fetch NBA odds (full-game + Q1) from ESPN per event.

    Returns dict keyed by "AWAY@HOME" with q1_spread, q1_total,
    q1_home_ml, q1_away_ml when ESPN exposes them; otherwise falls
    back to full-game markets only. Empty dict on total failure.
    """
    global _cache, _cache_ts
    if _cache and time.time() - _cache_ts < CACHE_TTL:
        return _cache

    events = _todays_events()
    if not events:
        logger.info("ESPN NBA odds: no scheduled events for today")
        _cache, _cache_ts = {}, time.time()
        return {}

    odds_map: dict[str, dict] = {}
    q1_hits = 0

    for ev in events:
        eid = ev["event_id"]
        home_abbr = ev["home_abbr"]
        away_abbr = ev["away_abbr"]
        key = f"{away_abbr}@{home_abbr}"
        result: dict = {"provider": "ESPN"}

        # Try summary endpoint first — it's the most complete
        summary = _fetch(f"{SUMMARY_URL}?event={eid}")
        if summary:
            pc = summary.get("pickcenter") or []
            # Prefer DraftKings / ESPN BET / any provider that has period odds
            providers_ranked = sorted(
                pc,
                key=lambda p: (
                    -1 if (p.get("periodOdds") or p.get("periods")) else 0,
                    0 if "draftkings" in (p.get("provider", {}).get("name") or "").lower() else 1,
                    1,
                ),
            )
            for prov in providers_ranked:
                _parse_provider_block(prov, home_abbr, away_abbr, result)
                # Also capture provider name for display
                if result.get("provider") == "ESPN":
                    pname = (prov.get("provider") or {}).get("name")
                    if pname:
                        result["provider"] = pname

        # If no Q1 yet, try the core API odds endpoint
        if result.get("q1_spread") is None and result.get("q1_total") is None:
            core = _fetch(CORE_ODDS_URL.format(event_id=eid))
            if core:
                for prov in core.get("items") or []:
                    _parse_provider_block(prov, home_abbr, away_abbr, result)

        # Only keep entries that yielded any usable data
        if any(result.get(k) is not None for k in
               ("home_ml", "q1_home_ml", "q1_spread", "q1_total", "over_under")):
            odds_map[key] = result
        if result.get("q1_spread") is not None or result.get("q1_total") is not None \
                or result.get("q1_home_ml") is not None:
            q1_hits += 1

        time.sleep(0.3)  # polite inter-request delay

    logger.info("ESPN NBA odds: fetched %d games (%d with Q1 markets)",
                len(odds_map), q1_hits)
    _cache = odds_map
    _cache_ts = time.time()
    return odds_map


# ── CLI ──

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    odds = fetch_nba_espn_odds()
    print(f"\n{'=' * 60}")
    print(f"  ESPN NBA Odds ({len(odds)} games)")
    print(f"{'=' * 60}")
    for key, v in sorted(odds.items()):
        print(f"\n  {key}  [{v.get('provider', '?')}]")
        for k in ("home_ml", "away_ml", "over_under", "over_odds", "under_odds",
                  "home_spread_point", "home_spread_odds",
                  "away_spread_point", "away_spread_odds",
                  "q1_home_ml", "q1_away_ml",
                  "q1_spread", "q1_spread_home_odds", "q1_spread_away_odds",
                  "q1_total", "q1_over_odds", "q1_under_odds"):
            if k in v and v[k] is not None:
                print(f"    {k:25s} {v[k]}")
