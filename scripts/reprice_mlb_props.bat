@echo off
cd /d C:\sports-model-bettor
set PYTHONPATH=C:\sports-model-bettor;%PYTHONPATH%
if not exist "data\logs" mkdir "data\logs"
echo [%DATE% %TIME%] reprice MLB props (re-sight closing_odds for today) >> data\logs\reprice_mlb_props.log
C:\sports-model-bettor\.venv\Scripts\python.exe -c "from engine.mlb_prop_picks import generate_picks; from datetime import datetime; print(generate_picks(date=datetime.now().strftime('%%Y-%%m-%%d')))" >> data\logs\reprice_mlb_props.log 2>&1
