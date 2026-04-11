@echo off
cd /d "%~dp0"
echo ============================================
echo   NBA Data Sync
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"

REM Auto-detect first run
python -c "from engine.nba_db import get_conn; c=get_conn(); r=c.execute('SELECT COUNT(*) FROM nba_games').fetchone()[0]; exit(0 if r > 50 else 1)" 2>nul
if errorlevel 1 (
    echo First run - running full NBA sync...
    echo This fetches the full season (2-3 minutes).
    echo.
    python -m scrapers.nba_espn --full
    goto :calibrate
)

echo Quick NBA sync (today's games)...
python -m scrapers.nba_espn

:calibrate
echo.
echo Calibrating NBA Q1 model...
python -m engine.nba_calibration

echo.
echo Recording today's NBA picks...
python -m engine.nba_tracker --record

echo.
echo Settling completed NBA picks...
python -m engine.nba_tracker --settle

echo.
echo ============================================
echo   NBA Sync Complete
echo ============================================
