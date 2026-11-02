import json, urllib.request
b = json.loads(urllib.request.urlopen("http://localhost:8000/api/nba/best-bets", timeout=120).read())
b = b if isinstance(b, list) else b.get("bets") or b.get("games") or []
for g in b:
    bp = g.get("best_pick") or {}
    print(g.get("game_id"), g.get("generated_at"), bp.get("type"), bp.get("pick"), bp.get("_scope"), bp.get("held"))
