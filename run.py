#!/usr/bin/env python3
"""
Sports Matchup Engine — CLI

Pick a league, pick two teams, get a full prediction.
"""

import sys
from engine.leagues import list_leagues, LEAGUES
from engine.data import list_teams, search_teams
from engine.predict import predict_matchup
from engine.display import format_prediction


def pick_league() -> str:
    """Let user pick a league."""
    leagues = list_leagues()
    print("\n  LEAGUES:")
    print("  " + "-" * 40)

    sports_order = ["football", "basketball", "baseball", "hockey", "soccer"]
    current_sport = None

    for i, key in enumerate(leagues, 1):
        league = LEAGUES[key]
        if league["sport"] != current_sport:
            current_sport = league["sport"]
            print(f"\n  {current_sport.upper()}")
        print(f"    {i:2d}. {key:12s} {league['name']}")

    print()
    while True:
        choice = input("  Pick league (number or code): ").strip()
        if not choice:
            continue

        # By number
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(leagues):
                return leagues[idx]

        # By code
        if choice.upper() in LEAGUES:
            return choice.upper()

        print(f"  '{choice}' not found. Try again.")


def pick_team(league: str, label: str) -> str:
    """Let user pick a team via search or list."""
    teams = list_teams(league)
    if not teams:
        print(f"\n  No teams loaded for {league}. Add team data to data/teams/{league}/")
        sys.exit(1)

    while True:
        query = input(f"\n  {label} team (search or 'list'): ").strip()
        if not query:
            continue

        if query.lower() == "list":
            print(f"\n  {league} TEAMS:")
            for i, t in enumerate(teams, 1):
                rec = f"  ({t['record']})" if t.get("record") else ""
                print(f"    {i:3d}. {t['name']}{rec}")
            print()

            choice = input(f"  Pick {label.lower()} team (number): ").strip()
            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(teams):
                    return teams[idx]["key"]
            continue

        # Search
        results = search_teams(league, query)
        if len(results) == 1:
            print(f"  → {results[0]['name']}")
            return results[0]["key"]
        elif len(results) > 1:
            print(f"\n  Multiple matches:")
            for i, t in enumerate(results, 1):
                rec = f"  ({t['record']})" if t.get("record") else ""
                print(f"    {i}. {t['name']}{rec}")
            choice = input(f"  Pick (number): ").strip()
            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(results):
                    return results[idx]["key"]
        else:
            print(f"  No teams matching '{query}'. Try 'list' to see all.")


def main():
    print("\n" + "=" * 64)
    print("   SPORTS MATCHUP ENGINE")
    print("=" * 64)

    try:
        while True:
            league = pick_league()
            home = pick_team(league, "Home")
            away = pick_team(league, "Away")

            prediction = predict_matchup(league, home, away)
            print(format_prediction(prediction))

            again = input("  Run another matchup? (y/n): ").strip().lower()
            if again != "y":
                break

    except KeyboardInterrupt:
        print("\n\n  Goodbye.\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
