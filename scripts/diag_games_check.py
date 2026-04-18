"""Inspect the MLB games table to find why compute_team_stats_at_date
returns {} for the current season.

The function expects status='final' AND season=YYYY AND date<TODAY.
This script tells us which of those filters drops the rows.

Usage:  python -m scripts.diag_games_check
"""
from engine.db import get_conn

conn = get_conn()

print("=" * 60)
print("Distribution of games by season + status (MLB)")
print("=" * 60)
rows = conn.execute(
    "SELECT season, status, COUNT(*) AS n "
    "FROM games "
    "WHERE date >= '2026-01-01' "
    "GROUP BY season, status "
    "ORDER BY season, status"
).fetchall()
for r in rows:
    print(dict(r))

print("\n" + "=" * 60)
print("Sample 5 NYY (147) games in 2026 -- raw row data")
print("=" * 60)
rows = conn.execute(
    "SELECT mlb_game_id, date, home_team_id, away_team_id, "
    "       home_score, away_score, status, season "
    "FROM games "
    "WHERE (home_team_id = 147 OR away_team_id = 147) "
    "  AND date >= '2026-01-01' "
    "ORDER BY date DESC LIMIT 5"
).fetchall()
for r in rows:
    print(dict(r))

print("\n" + "=" * 60)
print("What compute_team_stats_at_date sees for NYY today")
print("=" * 60)
from datetime import datetime
today = datetime.now().strftime("%Y-%m-%d")
matching = conn.execute(
    "SELECT COUNT(*) AS n FROM games "
    "WHERE (home_team_id = 147 OR away_team_id = 147) "
    "  AND date < ? AND season = 2026 AND status = 'final' "
    "  AND home_score IS NOT NULL AND away_score IS NOT NULL",
    (today,),
).fetchone()
print(f"matching rows: {matching['n']}")

# Now relax filters one by one
for label, sql in [
    ("no status filter",
     "SELECT COUNT(*) AS n FROM games WHERE (home_team_id=147 OR away_team_id=147) AND date<? AND season=2026"),
    ("no season filter",
     "SELECT COUNT(*) AS n FROM games WHERE (home_team_id=147 OR away_team_id=147) AND date<? AND status='final'"),
    ("no date filter",
     "SELECT COUNT(*) AS n FROM games WHERE (home_team_id=147 OR away_team_id=147) AND season=2026 AND status='final'"),
    ("only team filter",
     "SELECT COUNT(*) AS n FROM games WHERE (home_team_id=147 OR away_team_id=147)"),
]:
    n = conn.execute(sql, (today,) if "?" in sql else ()).fetchone()["n"]
    print(f"{label}: {n}")
