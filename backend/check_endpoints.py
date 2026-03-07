"""Quick diagnostic: test which sportsbook endpoints are reachable and what markets they offer."""
import asyncio
import httpx

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}


async def check_fanduel(client: httpx.AsyncClient):
    """Check FanDuel and list available market types."""
    print("\n--- FanDuel ---")
    for state in ["nj", "il", "pa"]:
        url = f"https://sbapi.{state}.sportsbook.fanduel.com/api/content-managed-page"
        params = {"page": "CUSTOM", "customPageId": "nhl", "_ak": "FhMFpcPWXMeyZxOx"}
        try:
            r = await client.get(url, headers=HEADERS, params=params, timeout=15)
            if r.status_code != 200:
                print(f"  {state}: HTTP {r.status_code}")
                continue
            data = r.json()
            attachments = data.get("attachments", {})
            markets = attachments.get("markets", {})
            events = attachments.get("events", {})
            print(f"  {state}: HTTP 200 | {len(events)} events, {len(markets)} markets")

            # Collect unique market types and names
            market_types = {}
            btts_count = 0
            ot_count = 0
            for mid, m in markets.items() if isinstance(markets, dict) else []:
                mt = (m.get("marketType", "UNKNOWN") or "").upper()
                mn = m.get("marketName", "") or m.get("name", "")
                if mt not in market_types:
                    market_types[mt] = mn

                runners = m.get("runners", [])
                # Check BTTS
                if mt == "BOTH_TEAMS_TO_SCORE":
                    btts_count += 1
                    eid = str(m.get("eventId", ""))
                    ev_name = events.get(eid, {}).get("name", "?") if isinstance(events, dict) else "?"
                    for runner in runners if isinstance(runners, list) else []:
                        rn = runner.get("runnerName", "")
                        wo = runner.get("winRunnerOdds", {})
                        am = wo.get("americanDisplayOdds", {}).get("americanOdds", "")
                        if btts_count <= 2:  # Show first 2 games only
                            print(f"    BTTS [{ev_name}]: {rn} = {am}")

                # Check OT
                if mt in ("OVERTIME_YES_NO", "OVERTIME"):
                    ot_count += 1
                    eid = str(m.get("eventId", ""))
                    ev_name = events.get(eid, {}).get("name", "?") if isinstance(events, dict) else "?"
                    for runner in runners if isinstance(runners, list) else []:
                        rn = runner.get("runnerName", "")
                        wo = runner.get("winRunnerOdds", {})
                        am = wo.get("americanDisplayOdds", {}).get("americanOdds", "")
                        if ot_count <= 2:
                            print(f"    OT [{ev_name}]: {rn} = {am}")

            print(f"  BTTS markets: {btts_count}, OT markets: {ot_count}")
            print(f"  Market types ({len(market_types)}):")
            for mt, mn in sorted(market_types.items()):
                print(f"    {mt:35s} ({mn})")
            break  # Only need one working state
        except Exception as e:
            print(f"  {state}: ERROR: {e}")


async def check_bovada(client: httpx.AsyncClient):
    """Check Bovada and list available market types."""
    print("\n--- Bovada ---")
    url = "https://www.bovada.lv/services/sports/event/coupon/events/A/description/hockey/nhl"
    try:
        r = await client.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print(f"  HTTP {r.status_code}")
            return
        data = r.json()
        if not isinstance(data, list):
            print(f"  Unexpected response type: {type(data)}")
            return

        total_events = 0
        market_descs = set()
        btts_found = False
        ot_found = False

        for section in data:
            for ev in section.get("events", []):
                total_events += 1
                for dg in ev.get("displayGroups", []):
                    for market in dg.get("markets", []):
                        desc = (market.get("description", "") or "").lower()
                        key = market.get("key", "")
                        period = market.get("period", {})
                        p_desc = (period.get("description", "") or "") if isinstance(period, dict) else ""
                        label = f"{desc} [{key}]"
                        if p_desc:
                            label += f" (period: {p_desc})"
                        market_descs.add(label)
                        if "both" in desc and "score" in desc:
                            btts_found = True
                            # Show actual outcomes
                            for oc in market.get("outcomes", []):
                                oc_desc = oc.get("description", "")
                                price = oc.get("price", {})
                                am = price.get("american", "") if isinstance(price, dict) else ""
                                print(f"  BTTS outcome: {oc_desc} = {am}")
                        if "overtime" in desc:
                            ot_found = True
                            for oc in market.get("outcomes", []):
                                oc_desc = oc.get("description", "")
                                price = oc.get("price", {})
                                am = price.get("american", "") if isinstance(price, dict) else ""
                                print(f"  OT outcome: {oc_desc} = {am}")

        print(f"  HTTP 200 | {total_events} events")
        print(f"  BTTS market found: {btts_found}")
        print(f"  Overtime market found: {ot_found}")
        print(f"  All market types ({len(market_descs)}):")
        for desc in sorted(market_descs):
            print(f"    {desc}")
    except Exception as e:
        print(f"  ERROR: {e}")


async def check_draftkings(client: httpx.AsyncClient):
    """Check DraftKings endpoint."""
    print("\n--- DraftKings ---")
    urls = [
        ("US", "https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/42133?format=json"),
        ("NJ", "https://sportsbook-us-nj.draftkings.com/sites/US-NJ-SB/api/v5/eventgroups/42133?format=json"),
    ]
    for label, url in urls:
        try:
            r = await client.get(url, headers=HEADERS, timeout=15)
            print(f"  {label}: HTTP {r.status_code} ({len(r.content):,} bytes)")
            if r.status_code == 200:
                break
        except Exception as e:
            print(f"  {label}: ERROR: {e}")


async def check_kambi(client: httpx.AsyncClient):
    """Check Kambi endpoint."""
    print("\n--- Kambi (BetRivers) ---")
    url = "https://eu-offering-api.kambicdn.com/offering/v2018/betrivers/listView/ice_hockey/nhl.json?lang=en_US&market=US"
    try:
        r = await client.get(url, headers=HEADERS, timeout=15)
        print(f"  HTTP {r.status_code} ({len(r.content):,} bytes)")
    except Exception as e:
        print(f"  ERROR: {e}")


async def main():
    print("\nSportsbook Market Diagnostic")
    print("=" * 60)
    async with httpx.AsyncClient() as client:
        await check_draftkings(client)
        await check_kambi(client)
        await check_fanduel(client)
        await check_bovada(client)
    print()


if __name__ == "__main__":
    asyncio.run(main())
