@echo off
REM NBA props v3 = Austin's "NBA props V2" (engine/sports/nba/prop_v3.py): nba.db box scores -> features -> LightGBM ->
REM projections for each team's next game in nba.db nbav3_preds, and grades finished ones into the probability pool.
REM Retrains when the models are over 6 days old. Scheduled task NbaV3Build, every 2 h (~3 min; ~9 with a retrain).
REM MintyBot 2026-11-02.
cd /d C:\sports-model-bettor
if not exist data\nba_v3\logs mkdir data\nba_v3\logs
start /belownormal /b /wait .venv\Scripts\python.exe -u -W ignore -m engine.sports.nba.prop_v3 build >> data\nba_v3\logs\build.out 2>&1
