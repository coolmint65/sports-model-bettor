@echo off
cd /d "%~dp0"
echo ============================================
echo   MLB Data Sync
echo ============================================
echo.
echo Working directory: %cd%
echo.

REM Check Python is available
python --version 2>&1
if errorlevel 1 (
    echo.
    echo ERROR: Python not found. Make sure Python is installed and on your PATH.
    goto :done
)

REM Create logs directory
if not exist "data\logs" mkdir "data\logs"

echo.

if "%1"=="--full" goto :full
if "%1"=="--daily" goto :daily
if "%1"=="--standings" goto :standings
if "%1"=="--history" goto :history
goto :quick

:full
echo Running FULL data sync (teams, rosters, all games, player stats)...
echo This will take 10-15 minutes.
echo.
python -m scrapers.mlb_stats --full
echo.
echo Running advanced stats (Statcast + FanGraphs)...
python -m scrapers.mlb_advanced
goto :calibrate

:daily
echo Running daily sync...
python -m scrapers.mlb_stats --daily
goto :calibrate

:standings
echo Updating standings...
python -m scrapers.mlb_stats --standings
goto :calibrate

:history
echo Loading %2 season data for backtesting...
echo This will take a few minutes.
echo.
python -m scrapers.mlb_stats --history %2
goto :calibrate

:quick
echo Running quick sync (teams, today's games, standings)...
echo.
python -m scrapers.mlb_stats
goto :calibrate

:calibrate
echo.
echo Calibrating model...
python -m engine.calibration --days 30
echo.
echo Recording today's picks...
python -m engine.tracker --record
echo.
echo Settling completed picks...
python -m engine.tracker --settle

:done
echo.
echo ============================================
echo   Finished
echo ============================================
echo.
pause
