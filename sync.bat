@echo off
cd /d "%~dp0"
echo ============================================
echo   MLB Data Sync
echo ============================================
echo.

REM Check Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found. Make sure Python is installed and on your PATH.
    echo.
    pause
    exit /b 1
)

REM Check pybaseball is installed
python -c "import pybaseball" >nul 2>&1
if errorlevel 1 (
    echo Installing required packages...
    pip install pybaseball
    echo.
)

REM Create logs directory
if not exist "data\logs" mkdir "data\logs"

if "%1"=="" (
    echo Running full data sync...
    echo This will take a few minutes on first run.
    echo.
    python -m scrapers.mlb_stats
    if errorlevel 1 (
        echo.
        echo WARNING: MLB Stats sync had errors. Check above for details.
        echo.
    )
    echo.
    echo Running advanced stats sync...
    python -m scrapers.mlb_advanced
) else if "%1"=="--daily" (
    echo Running daily sync...
    python -m scrapers.mlb_stats --daily
) else if "%1"=="--advanced" (
    echo Running advanced stats sync...
    python -m scrapers.mlb_advanced
) else if "%1"=="--standings" (
    echo Updating standings...
    python -m scrapers.mlb_stats --standings
) else (
    echo Usage: sync.bat [--daily ^| --advanced ^| --standings]
    echo.
    echo   (no args)   Full sync: teams, rosters, games, standings, stats
    echo   --daily     Quick: today's games + standings
    echo   --advanced  Statcast + FanGraphs advanced metrics
    echo   --standings Standings only
)

echo.
echo ============================================
echo   Done!
echo ============================================
pause
