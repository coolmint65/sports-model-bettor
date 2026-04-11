@echo off
cd /d "%~dp0"
echo ============================================
echo   NHL Data Sync
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"

if "%1"=="--full" goto :full
if "%1"=="--history" goto :history

REM Auto-detect first run: if NHL DB has no games, do full sync
python -c "from engine.nhl_db import get_conn; c=get_conn(); r=c.execute('SELECT COUNT(*) FROM nhl_games').fetchone()[0]; exit(0 if r > 50 else 1)" 2>nul
if errorlevel 1 (
    echo First run detected - running full NHL sync...
    echo This fetches the full season and takes 2-3 minutes.
    echo.
    goto :full
)
goto :quick

:full
echo Running FULL NHL sync (teams, rosters, stats, season games)...
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
python -m engine.nhl_calibration
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
