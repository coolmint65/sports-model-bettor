@echo off
cd /d "%~dp0"
echo ============================================
echo   NHL Data Sync
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"

if "%1"=="--full" goto :full
if "%1"=="--history" goto :history
goto :quick

:full
echo Running FULL NHL sync (teams, rosters, stats, recent games)...
python -m scrapers.nhl_api --full
echo.
goto :calibrate

:history
echo Loading NHL %2 season data...
python -m scrapers.nhl_api --history %2
echo.
goto :calibrate

:quick
echo Quick NHL sync (today's games + team stats)...
python -m scrapers.nhl_api
echo.

:calibrate
echo Calibrating NHL model...
python -m engine.nhl_calibration --days 30
echo.

echo Recording today's NHL picks...
python -m engine.nhl_tracker --record

echo.
echo Settling completed NHL picks...
python -m engine.nhl_tracker --settle

echo.
echo ============================================
echo   NHL Sync Complete
echo ============================================
