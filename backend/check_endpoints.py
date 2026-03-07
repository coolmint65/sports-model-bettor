"""Quick diagnostic: test which sportsbook endpoints are reachable."""
import asyncio
import httpx

ENDPOINTS = {
    "DraftKings (US)": "https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/42133?format=json",
    "DraftKings (NJ)": "https://sportsbook-us-nj.draftkings.com/sites/US-NJ-SB/api/v5/eventgroups/42133?format=json",
    "DraftKings (PA)": "https://sportsbook-us-pa.draftkings.com/sites/US-PA-SB/api/v5/eventgroups/42133?format=json",
    "FanDuel": "https://sbapi.nj.sportsbook.fanduel.com/api/content-managed-page?page=CUSTOM&customPageId=nhl&_ak=FhMFpcPWXMeyZxOx",
    "Kambi (BetRivers)": "https://eu-offering-api.kambicdn.com/offering/v2018/betrivers/listView/ice_hockey/nhl.json?lang=en_US&market=US",
    "Bovada": "https://www.bovada.lv/services/sports/event/coupon/events/A/description/hockey/nhl?lang=en",
    "Odds API (no key)": "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}


async def check(name: str, url: str, client: httpx.AsyncClient):
    try:
        r = await client.get(url, headers=HEADERS, timeout=15)
        size = len(r.content)
        # For DraftKings, check if we got prop data
        extra = ""
        if "draftkings" in name.lower() and r.status_code == 200:
            try:
                data = r.json()
                offers = data.get("eventGroup", {}).get("offerCategories", [])
                cat_names = [c.get("name", "") for c in offers] if isinstance(offers, list) else []
                extra = f" | categories: {cat_names[:5]}"
            except Exception:
                pass
        # For Kambi, count events
        if "kambi" in name.lower() and r.status_code == 200:
            try:
                data = r.json()
                events = data.get("events", [])
                extra = f" | {len(events)} events"
            except Exception:
                pass
        print(f"  {name:25s}  HTTP {r.status_code}  ({size:,} bytes){extra}")
    except httpx.TimeoutException:
        print(f"  {name:25s}  TIMEOUT (15s)")
    except httpx.ConnectError as e:
        print(f"  {name:25s}  CONNECTION FAILED: {e}")
    except Exception as e:
        print(f"  {name:25s}  ERROR: {e}")


async def main():
    print("\nSportsbook Endpoint Check")
    print("=" * 60)
    async with httpx.AsyncClient() as client:
        tasks = [check(name, url, client) for name, url in ENDPOINTS.items()]
        await asyncio.gather(*tasks)
    print()


if __name__ == "__main__":
    asyncio.run(main())
