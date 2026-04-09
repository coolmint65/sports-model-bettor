@echo off
cd /d "%~dp0"
echo ============================================
echo   MLB Data Sync
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"

REM Auto-detect: if no final games with linescores OR no player stats, do full sync
python -c "from engine.db import get_conn; c=get_conn(); g=c.execute('SELECT COUNT(*) as c FROM games WHERE status=\"final\" AND home_linescore IS NOT NULL').fetchone()['c']; p=c.execute('SELECT COUNT(*) FROM pitcher_stats').fetchone()[0]; exit(0 if g > 10 and p > 10 else 1)" 2>nul
if errorlevel 1 (
    echo First run or missing data - running full MLB sync...
    echo This fetches the full season + player stats (5-10 minutes).
    echo.
    python -m scrapers.mlb_stats --full
    echo.
    echo Running advanced stats...
    python -m scrapers.mlb_advanced 2>nul
    echo.
    goto :calibrate
)

echo Quick sync (teams, today's games, standings)...
python -m scrapers.mlb_stats

:calibrate
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
