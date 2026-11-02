@echo off
cd /d C:\sports-model-bettor
.venv\Scripts\python.exe scripts\capture_all_odds.py >> data\game_odds_capture.log 2>&1
.venv\Scripts\python.exe scripts\capture_derivative_odds.py >> data\game_derivative_odds_capture.log 2>&1
