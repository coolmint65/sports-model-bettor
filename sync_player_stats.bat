@echo off
cd /d "%~dp0"
if not exist "data\logs" mkdir "data\logs"
echo [%date% %time%] player-stats refresh START >> data\logs\player_stats_sync.log
.venv\Scripts\python.exe -c "from scrapers.mlb_stats import sync_all_player_stats; print('rows:', sync_all_player_stats())" >> data\logs\player_stats_sync.log 2>&1
echo [%date% %time%] player-stats refresh DONE >> data\logs\player_stats_sync.log
