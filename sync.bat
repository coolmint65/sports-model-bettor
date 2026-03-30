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
if "%1"=="--full" (
    echo Running FULL data sync (teams, rosters, all games, player stats)...
    echo This will take 10-15 minutes.
    echo.
    python -m scrapers.mlb_stats --full
    echo.
    echo Running advanced stats (Statcast + FanGraphs)...
    python -m scrapers.mlb_advanced
) else if "%1"=="--daily" (
    echo Running daily sync...
    python -m scrapers.mlb_stats --daily
) else if "%1"=="--standings" (
    echo Updating standings...
    python -m scrapers.mlb_stats --standings
) else (
    echo Running quick sync (teams, today's games, standings)...
    echo.
    python -m scrapers.mlb_stats
)

:done
echo.
echo ============================================
echo   Finished
echo ============================================
echo.
pause
