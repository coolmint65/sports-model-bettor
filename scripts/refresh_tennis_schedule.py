import os, sys, datetime
os.chdir(r"C:\sports-model-bettor"); sys.path.insert(0, r"C:\sports-model-bettor")
from engine.tennis_schedule import ingest_schedule
for d in (0, 1):
    day = (datetime.date.today() + datetime.timedelta(days=d)).isoformat()
    try: print(day, ingest_schedule(day))
    except Exception as e: print(day, "ERR", e)
