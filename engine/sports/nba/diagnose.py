"""
NBA diagnostic dump - run this to see why Q1 picks aren't recording
and why the bulk player-stats endpoint isn't populating q1_impact.

Usage:
    python -m engine.nba_diagnose
    python -m engine.nba_diagnose --game ORL@BOS
"""

import json
import logging
import sys
import urllib.request
from ..._tz import et_today_str

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")


def dump_bulk_stats_raw():
    """Hit the bulk stats endpoint and print its real response shape."""
    url = ("https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba"
           "/statistics/byathlete?region=us&lang=en&contentorigin=espn"
           "&seasontype=2&limit=5&sort=general.avgMinutes:desc")
    print(f"\n{'=' * 72}")
    print("  BULK PLAYER-STATS ENDPOINT RAW RESPONSE (top 5 by minutes)")
    print(f"{'=' * 72}")
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        print(f"FETCH FAILED: {e}")
        return

    # Top-level keys
    print(f"\nTop-level keys: {list(data.keys())}")

    # Print the 'categories' / 'glossary' section - this is where stat
    # labels typically live when they're separated from per-athlete values.
    if "categories" in data:
        cats = data["categories"]
        print(f"\ndata['categories'] ({len(cats)} categories):")
        print(json.dumps(cats, indent=2)[:3000])
    if "glossary" in data:
        gloss = data["glossary"]
        print(f"\ndata['glossary'] ({len(gloss) if isinstance(gloss, list) else '?'} items):")
        print(json.dumps(gloss, indent=2)[:1500])

    # First athlete - focus on the non-link/bio fields (stats live under
    # sibling keys to 'athlete'). Strip 'links' since it blows up the output.
    for key in ("athletes", "items", "results"):
        if key in data and data[key]:
            first = dict(data[key][0])  # shallow copy
            if isinstance(first.get("athlete"), dict):
                ath = dict(first["athlete"])
                ath.pop("links", None)
                ath.pop("teams", None)
                first["athlete"] = ath
            print(f"\nFirst entry in data['{key}'] (links stripped):")
            print(json.dumps(first, indent=2)[:5000])
            break


def dump_pick_flow(target_game: str | None = None):
    """Walk through the pick-recording flow for today's games."""
    print(f"\n{'=' * 72}")
    print("  PICK-RECORDING FLOW DIAGNOSTIC")
    print(f"{'=' * 72}")

    from engine.sports.nba.tracker import _fetch_nba_scoreboard
    from scrapers.nba_odds import fetch_all_nba_odds
    from engine.sports.nba.picks import generate_q1_picks
    from engine.sports.nba.db import get_conn
    from datetime import datetime

    today = et_today_str()
    events = _fetch_nba_scoreboard(today)
    print(f"\nEvents returned by scoreboard for {today}: {len(events)}")

    odds = fetch_all_nba_odds()
    print(f"Odds map size: {len(odds)} games")
    print(f"Odds map keys: {sorted(odds.keys())}")

    conn = get_conn()

    for event in events:
        game_id = event.get("id", "")
        comp = event.get("competitions", [{}])[0]
        status = comp.get("status", {}).get("type", {})
        completed = status.get("completed", False)

        competitors = comp.get("competitors", [])
        h_abbr = ""
        a_abbr = ""
        for c in competitors:
            team = c.get("team", {})
            abbr = team.get("abbreviation", "")
            if c.get("homeAway") == "home":
                h_abbr = abbr
            else:
                a_abbr = abbr

        if not h_abbr or not a_abbr:
            continue

        matchup_key = f"{a_abbr}@{h_abbr}"
        if target_game and target_game != matchup_key:
            continue

        print(f"\n─── {matchup_key} (game_id {game_id}) ───")
        print(f"  Status completed: {completed}")

        existing_count = conn.execute(
            "SELECT COUNT(*) as c FROM nba_picks WHERE game_id = ?",
            (game_id,)
        ).fetchone()["c"]
        print(f"  Existing picks for this game_id: {existing_count}")

        # Odds lookup - show both the key hit and what's in the map
        market_odds = odds.get(matchup_key) or {}
        print(f"  In odds map as '{matchup_key}': "
              f"{'YES' if market_odds else 'NO'}")
        if market_odds:
            for k in ("home_ml", "away_ml", "q1_home_ml", "q1_away_ml",
                      "q1_spread", "q1_spread_home_odds", "q1_spread_away_odds",
                      "q1_total", "q1_over_odds", "q1_under_odds"):
                if k in market_odds:
                    print(f"    {k:25s} {market_odds[k]}")

        odds_dict = {
            "q1_spread": market_odds.get("q1_spread"),
            "q1_total": market_odds.get("q1_total"),
            "q1_spread_home_odds": market_odds.get("q1_spread_home_odds", -110),
            "q1_spread_away_odds": market_odds.get("q1_spread_away_odds", -110),
            "q1_over_odds": market_odds.get("q1_over_odds", -110),
            "q1_under_odds": market_odds.get("q1_under_odds", -110),
            "home_ml": market_odds.get("q1_home_ml") or market_odds.get("home_ml"),
            "away_ml": market_odds.get("q1_away_ml") or market_odds.get("away_ml"),
        }

        try:
            picks = generate_q1_picks(h_abbr, a_abbr, odds_dict)
        except Exception as e:
            print(f"  generate_q1_picks CRASHED: {e}")
            import traceback
            traceback.print_exc()
            continue

        print(f"  generate_q1_picks returned {len(picks)} picks:")
        for p in picks:
            print(f"    {p['type']:12s} {p['pick']:30s} "
                  f"prob={p['prob']:.3f} edge={p['edge']:+.1f} "
                  f"adj_ev={p.get('adjusted_ev', 0):+.2f} odds={p['odds']}")

        # Also call predict_q1 directly so we can see internal state
        # (reasoning array + home/away roster deltas). This tells us
        # whether compute_q1_adjustment fired at all inside the model.
        from engine.sports.nba.q1_predict import predict_q1
        try:
            pred = predict_q1(h_abbr, a_abbr,
                              spread=odds_dict.get("q1_spread"),
                              total=odds_dict.get("q1_total"))
        except Exception as e:
            print(f"  predict_q1 CRASHED: {e}")
            import traceback
            traceback.print_exc()
            continue

        factors = pred.get("factors", {})
        home_roster = factors.get("home_roster") or {}
        away_roster = factors.get("away_roster") or {}
        print(f"  predict_q1 internal state:")
        print(f"    home_q1_expected : {pred.get('home_q1_expected')}")
        print(f"    away_q1_expected : {pred.get('away_q1_expected')}")
        print(f"    predicted_margin : {pred.get('predicted_margin')}")
        print(f"    predicted_total  : {pred.get('predicted_total')}")
        print(f"    q1_ml_home       : {pred.get('q1_ml_home'):.3f}")
        print(f"    HOME team_id     : "
              f"(resolved via _get_team_data; see roster delta below)")
        print(f"    home_roster_adj  : q1_delta={home_roster.get('q1_delta')} "
              f"starters_out={home_roster.get('starters_out')} "
              f"load_mgmt={home_roster.get('load_management')}")
        print(f"    away_roster_adj  : q1_delta={away_roster.get('q1_delta')} "
              f"starters_out={away_roster.get('starters_out')} "
              f"load_mgmt={away_roster.get('load_management')}")
        print(f"  reasoning:")
        for r in pred.get("reasoning", []):
            print(f"    - {r}")

        # Bypass predict_q1's try/except and call the underlying
        # functions directly. If compute_q1_adjustment returns -8 here
        # but predict_q1's internal call returns 0, the bug is in the
        # predict_q1 branch logic (team_id resolution, season, etc.).
        print(f"  direct subsystem calls (bypass predict_q1):")
        try:
            from engine.sports.nba.q1_predict import _get_team_data
            from engine.sports.nba.injuries import compute_q1_adjustment, is_likely_resting_spot
            home_data = _get_team_data(h_abbr)
            away_data = _get_team_data(a_abbr)
            print(f"    _get_team_data({h_abbr!r}): team_id={home_data.get('team_id')} "
                  f"season={home_data.get('season')} games={home_data.get('games')}")
            print(f"    _get_team_data({a_abbr!r}): team_id={away_data.get('team_id')} "
                  f"season={away_data.get('season')} games={away_data.get('games')}")

            # Now call compute_q1_adjustment with same args predict_q1 would use
            hs = home_data.get("season")
            if home_data.get("team_id"):
                direct_home_adj = compute_q1_adjustment(home_data["team_id"], hs)
                print(f"    compute_q1_adjustment({home_data['team_id']}, {hs}): "
                      f"q1_delta={direct_home_adj['q1_delta']} "
                      f"starters_out={direct_home_adj['starters_out']} "
                      f"out_players={len(direct_home_adj.get('out_players') or [])}")

                rest_flag = is_likely_resting_spot(
                    home_data["team_id"],
                    et_today_str(),
                    hs)
                print(f"    is_likely_resting_spot(home): {rest_flag}")
        except Exception as e:
            print(f"    DIRECT CALL CRASHED: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()


def dump_player_sample():
    """Print what's in nba_players for BOS - are MPG/PPG actually populated?"""
    print(f"\n{'=' * 72}")
    print("  NBA_PLAYERS SAMPLE (BOS)")
    print(f"{'=' * 72}")
    from engine.sports.nba.db import get_conn, get_nba_team_by_abbr
    conn = get_conn()
    team = get_nba_team_by_abbr("BOS")
    if not team:
        print("BOS not in nba_teams")
        return
    rows = conn.execute(
        "SELECT name, position, minutes_per_game, points_per_game, "
        "starter, q1_impact FROM nba_players "
        "WHERE team_id = ? ORDER BY minutes_per_game DESC NULLS LAST",
        (team["id"],)
    ).fetchall()
    print(f"\n{len(rows)} players stored for BOS:")
    for r in rows:
        print(f"  {'*' if r['starter'] else ' '} {r['name']:28s} "
              f"{(r['position'] or '??'):4s}  "
              f"MPG: {r['minutes_per_game']}  PPG: {r['points_per_game']}  "
              f"q1_impact: {r['q1_impact']}")


if __name__ == "__main__":
    target = None
    if len(sys.argv) > 2 and sys.argv[1] == "--game":
        target = sys.argv[2]

    dump_bulk_stats_raw()
    dump_player_sample()
    dump_pick_flow(target)
    print()
