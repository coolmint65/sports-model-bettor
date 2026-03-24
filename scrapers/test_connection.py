"""Quick connectivity test for ESPN API — run this first to verify access."""
import json
import urllib.request
import ssl

TEST_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams"

print(f"Testing: {TEST_URL}")
print()

try:
    req = urllib.request.Request(TEST_URL, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = resp.read().decode()
        data = json.loads(raw)

        print(f"Status: {resp.status}")
        print(f"Response size: {len(raw):,} bytes")
        print(f"Top-level keys: {list(data.keys())}")
        print()

        # Try to find teams
        teams_found = 0

        # Check sports[].leagues[].teams[]
        for sport in data.get("sports", []):
            for league in sport.get("leagues", []):
                t = league.get("teams", [])
                teams_found += len(t)
                if t:
                    sample = t[0].get("team", t[0])
                    print(f"Format: sports[].leagues[].teams[]")
                    print(f"Teams found: {len(t)}")
                    print(f"Sample team keys: {list(sample.keys())}")
                    print(f"Sample: {sample.get('displayName', sample.get('name', '?'))}")

        # Check flat teams[]
        if not teams_found and "teams" in data:
            t = data["teams"]
            sample = t[0].get("team", t[0]) if t else {}
            print(f"Format: teams[] (flat)")
            print(f"Teams found: {len(t)}")
            if sample:
                print(f"Sample team keys: {list(sample.keys())}")
                print(f"Sample: {sample.get('displayName', sample.get('name', '?'))}")

        if not teams_found and "teams" not in data:
            print(f"WARNING: No teams found in response!")
            print(f"Full structure preview:")
            print(json.dumps(data, indent=2)[:2000])

        print()
        print("ESPN API is reachable and returning data.")

except urllib.error.HTTPError as e:
    print(f"HTTP Error {e.code}: {e.reason}")
    print(f"This may mean the endpoint has changed or is rate-limited.")
except urllib.error.URLError as e:
    print(f"Connection Error: {e.reason}")
    if "SSL" in str(e.reason) or "CERTIFICATE" in str(e.reason).upper():
        print()
        print("SSL certificate issue detected. Try running:")
        print("  pip install certifi")
        print("Or set environment variable:")
        print("  set SSL_CERT_FILE=<path to cacert.pem>")
    else:
        print("Check your internet connection and firewall settings.")
except Exception as e:
    print(f"Unexpected error: {type(e).__name__}: {e}")
