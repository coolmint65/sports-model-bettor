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
echo Capturing NBA Q1 closing odds (CLV pre-game snapshot)...
REM Stamps current Hard Rock Q1 odds as closing_odds for any pending
REM pick that doesn't have one yet. Must run before tip-off because
REM HR drops Q1 markets the instant Q1 ends.
python -m engine.nba_tracker --capture-closing

echo.
echo Settling completed NBA picks...
python -m engine.nba_tracker --settle

echo.
echo Refreshing empirical NBA pick-prob calibration...
python -c "from engine.empirical_calibration import refresh_calibration; print(refresh_calibration('nba'))" 2>nul

echo.
echo Refitting blend calibration (sport-wide isotonic/Platt over settled picks)...
python -m engine.blend_calibration --sport nba 2>nul

echo.
echo Recomputing data-driven edge floors per (bet_type, direction)...
python -m engine.edge_floors --sport nba 2>nul

echo.
echo Refreshing prop-shrinkage table (weekly cadence)...
REM Mirrors sync_mlb.bat. Gate on prop_shrinkage.json mtime so the
REM 1-2 min replay only fires once a week per sport.
python -c "import os, time; p=r'data\prop_shrinkage.json'; exit(0 if (not os.path.exists(p)) or (time.time() - os.path.getmtime(p) > 7*86400) else 1)" >nul 2>&1
if not errorlevel 1 (
    python -m engine.player_props_calibration --sport nba 2>nul
)

echo.
echo Refreshing adaptive baselines (LEAGUE_AVG_Q1_TOTAL etc.)...
REM Computes long+short rolling baselines from completed games and
REM writes overrides via engine.model_overrides when the trailing
REM window diverges significantly from the long-term mean. Catches
REM regime shifts (playoff pace slowdowns etc.) without source edits.
python -c "from engine.adaptive_baselines import update_all; import json; print(json.dumps(update_all(), indent=2, default=str))" 2>nul

echo.
echo Refreshing POTD live line (track HR line movement)...
REM Re-stamps a pending POTD if HR has moved the line/price since lock.
REM Same selection (game / bet_type / direction) — just current numbers.
python -c "from engine.pick_of_day import refresh_potd_for_line_movement; print(refresh_potd_for_line_movement('nba'))" 2>nul

echo.
echo Updating POTD closing odds (CLV capture)...
python -c "from engine.pick_of_day import update_potd_closing_odds; print(update_potd_closing_odds('nba'))" 2>nul

echo.
echo Settling POTD...
python -c "from engine.pick_of_day import settle_potd; print(settle_potd('nba'))" 2>nul

echo.
echo Ingesting recent finalized player game logs (Phase 2h-i)...
REM 3-day lookback to backfill missed syncs / UTC-rollover games.
python -c "from engine.nba_player_logs import ingest_recent_finals; print(ingest_recent_finals(lookback_days=3))" 2>nul

echo.
echo Settling player props...
python -c "from engine.player_props_tracker import settle_player_props; print(settle_player_props('nba'))" 2>nul

echo.
echo Generating NBA player-prop picks (Phase 2h-iii)...
python -c "from engine.nba_prop_picks import generate_picks; from datetime import datetime, timedelta; print(generate_picks(date=(datetime.now()+timedelta(days=1)).strftime('%%Y-%%m-%%d')))" 2>nul

echo.
echo Refreshing prop POTD live line (track HR line movement)...
python -c "from engine.player_props_potd import refresh_potd_for_line_movement; print(refresh_potd_for_line_movement('nba'))" 2>nul

echo.
echo Auto-tuning ensemble weights (skipped when ^< 200 settled signals/market)...
python scripts\run.py engine.ensemble_auto_tune nba -v 2>nul

echo.
echo ============================================
echo   NBA Sync Complete    [%DATE% %TIME%]
echo ============================================
if /i not "%SESSIONNAME%"=="" if /i not "%SESSIONNAME%"=="Services" if "%~1"=="" pause
