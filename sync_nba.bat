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
echo Checking for late-injury deltas (invalidates stale picks + NBA POTD)...
REM Snapshots confirmed-OUT player sets per game. On delta (OUT
REM set changed for either team) drops picks_cache + unsettled POTD
REM for the affected game and force-re-records the NBA tracker.
REM Only OUT / DNP / Suspended flips trigger invalidation; routine
REM questionable / day-to-day reshuffles are ignored.
python -c "from engine.nba_injury_refresh import refresh_for_date; import json; print(json.dumps(refresh_for_date(), indent=2, default=str))" 2>nul

echo.
echo Recording today's NBA picks...
python -m engine.nba_tracker --record

echo.
echo Settling completed NBA picks...
python -m engine.nba_tracker --settle

echo.
echo Refreshing empirical NBA pick-prob calibration...
python -c "from engine.empirical_calibration import refresh_calibration; print(refresh_calibration('nba'))" 2>nul

echo.
echo Refreshing adaptive baselines (LEAGUE_AVG_Q1_TOTAL etc.)...
REM Computes long+short rolling baselines from completed games and
REM writes overrides via engine.model_overrides when the trailing
REM window diverges significantly from the long-term mean. Catches
REM regime shifts (playoff pace slowdowns etc.) without source edits.
python -c "from engine.adaptive_baselines import update_all; import json; print(json.dumps(update_all(), indent=2, default=str))" 2>nul

echo.
echo Updating POTD closing odds (CLV capture)...
python -c "from engine.pick_of_day import update_potd_closing_odds; print(update_potd_closing_odds('nba'))" 2>nul

echo.
echo Settling POTD...
python -c "from engine.pick_of_day import settle_potd; print(settle_potd('nba'))" 2>nul

echo.
echo Ingesting today's player game logs (Phase 2h-i)...
python -c "from engine.nba_player_logs import ingest_today; print(ingest_today())" 2>nul

echo.
echo Settling player props...
python -c "from engine.player_props_tracker import settle_player_props; print(settle_player_props('nba'))" 2>nul

echo.
echo ============================================
echo   NBA Sync Complete    [%DATE% %TIME%]
echo ============================================
if /i not "%SESSIONNAME%"=="" if /i not "%SESSIONNAME%"=="Services" if "%~1"=="" pause
