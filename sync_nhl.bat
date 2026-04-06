@echo off
cd /d "%~dp0"
echo ============================================
echo   NHL Data Sync
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"

echo Refreshing NHL team data from ESPN...
python -c "from scrapers.espn import scrape_league; scrape_league('hockey', 'nhl', 'NHL')"

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
