@echo off
cd /d "%~dp0"
echo ============================================
echo   MLB Data Sync
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"

REM Check if DB needs rebuild
python -c "from engine.db import get_conn; c=get_conn(); r=c.execute('SELECT COUNT(*) as c FROM games WHERE status=\"final\" AND home_linescore IS NOT NULL').fetchone(); exit(0 if r['c'] > 10 else 1)" 2>nul
if errorlevel 1 (
    echo Database needs rebuild - running season reload...
    python -m scrapers.mlb_stats --season
    echo.
)

echo Syncing teams, today's games, standings...
python -m scrapers.mlb_stats

echo.
echo Calibrating global model...
python -m engine.calibration --days 30

echo.
echo Calibrating per-team factors...
python -m engine.team_calibration

echo.
echo Recording today's picks...
python -m engine.tracker --record

echo.
echo Settling completed picks...
python -m engine.tracker --settle

echo.
echo ============================================
echo   MLB Sync Complete
echo ============================================
