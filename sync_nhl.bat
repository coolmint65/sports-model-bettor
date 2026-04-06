@echo off
cd /d "%~dp0"
echo ============================================
echo   NHL Data Sync
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"

if "%1"=="--full" goto :full
goto :quick

:full
echo Refreshing all NHL team data from ESPN (this takes ~2 minutes)...
python -c "from scrapers.espn import scrape_league; scrape_league('hockey', 'nhl', 'NHL')"
echo.
goto :picks

:quick
echo Quick sync (picks only, team data already loaded)...
echo   Run "sync_nhl.bat --full" to refresh all team stats from ESPN
echo.

:picks
echo Recording today's NHL picks...
python -m engine.nhl_tracker --record

echo.
echo Settling completed NHL picks...
python -m engine.nhl_tracker --settle

echo.
echo ============================================
echo   NHL Sync Complete
echo ============================================
