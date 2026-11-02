@echo off
REM NFL props v3 (engine/sports/nfl/prop_v3.py): nflverse -> features -> LightGBM -> next slate's projections in
REM nfl_player.db nflv3_preds, and grades past projections into the probability pool. Retrains when the models are
REM over 6 days old. Scheduled task NflV3Build, every 3 h (~2-4 min). MintyBot 2026-11-02.
cd /d C:\sports-model-bettor
if not exist data\football\nflv3\logs mkdir data\football\nflv3\logs
start /belownormal /b /wait .venv\Scripts\python.exe -u -W ignore -m engine.sports.nfl.prop_v3 build >> data\football\nflv3\logs\build.out 2>&1
