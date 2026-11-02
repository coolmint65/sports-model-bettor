import sqlite3
conn = sqlite3.connect(r"C:\sports-model-bettor\data\nhl.db")
conn.row_factory = sqlite3.Row
for abbr in ("TOR","SJS","OTT","SJ"):
    team = conn.execute("SELECT id, abbreviation FROM nhl_teams WHERE abbreviation=?", (abbr,)).fetchone()
    if not team:
        print(abbr, "-> no team row")
        continue
    tid = team["id"]
    rows = conn.execute(
        "SELECT date, home_team_id, away_team_id, status FROM nhl_games "
        "WHERE (home_team_id=? OR away_team_id=?) AND date >= '2026-10-10' AND date <= '2026-10-23' "
        "ORDER BY date", (tid, tid)
    ).fetchall()
    print(f"--- {abbr} (id={tid}) ---")
    for r in rows:
        side = "HOME" if r["home_team_id"]==tid else "AWAY"
        print(f"  {r['date']}  {side}  status={r['status']}")
