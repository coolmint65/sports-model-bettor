@echo off
cd /d "%~dp0"
echo ============================================
echo   NBA Data Sync
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"

if "%1"=="--full" goto :full

REM Auto-detect first run
python -c "import engine.nba_db as db; c=db.get_conn(); n=c.execute('SELECT COUNT(*) FROM nba_games').fetchone(); raise SystemExit(0 if n[0]>50 else 1)" 2>nul
if errorlevel 1 goto :full
goto :quick

:full
echo Running FULL NBA sync...
python -m scrapers.nba_espn --full
goto :calibrate

:quick

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
