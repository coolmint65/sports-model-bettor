import sqlite3
c = sqlite3.connect(r"file:C:\sports-model-bettor\data\nba.db?mode=ro", uri=True); c.row_factory = sqlite3.Row
print([d[1] for d in c.execute("PRAGMA table_info(pick_events)")])
for r in c.execute("SELECT * FROM pick_events WHERE date='2026-11-01' ORDER BY game_id, ts"):
    d = dict(r); print(d["game_id"], d["ts"], d["scope"], d["event_type"], d["prev_bet_type"], d["prev_pick"], "->", d["curr_bet_type"], d["curr_pick"], d["curr_odds"], d["curr_edge"])
