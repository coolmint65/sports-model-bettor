@echo off
echo ============================================
echo   MLB Data Sync
echo ============================================
echo.

if "%1"=="" (
    echo Running full data sync...
    echo This will take a few minutes on first run.
    echo.
    python -m scrapers.mlb_stats
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
echo Done!
pause
