@echo off
cd /d "%~dp0"
echo ============================================
echo   NHL Data Sync
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"

if "%1"=="--full" goto :full
if "%1"=="--history" goto :history

REM Auto-detect first run: if NHL DB has no games, do full sync
python -c "from engine.nhl_db import get_conn; c=get_conn(); r=c.execute('SELECT COUNT(*) FROM nhl_games').fetchone()[0]; exit(0 if r > 50 else 1)" 2>nul
if errorlevel 1 (
    echo First run detected - running full NHL sync...
    echo This fetches the full season and takes 2-3 minutes.
    echo.
    goto :full
)
goto :quick

:full
echo Running FULL NHL sync (teams, rosters, stats, season games)...
python -m scrapers.nhl_api --full
echo.
goto :calibrate

:history
echo Loading NHL %2 season data...
python -m scrapers.nhl_api --history %2
echo.
goto :calibrate

:quick
echo Quick NHL sync (today's games + team stats)...
python -m scrapers.nhl_api
echo.

:calibrate
echo Calibrating NHL model...
python -m engine.nhl_calibration
echo.

echo Checking for late-goalie deltas (invalidates stale picks + NHL POTD)...
REM Snapshots announced starting goalies. On delta (confirmed
REM starter differs from the morning snapshot) drops picks_cache +
REM unsettled POTD for the affected game and force-re-records
REM the NHL tracker with the confirmed goalie.
python -c "from engine.nhl_goalie_refresh import refresh_for_date; import json; print(json.dumps(refresh_for_date(), indent=2, default=str))" 2>nul

echo Recording today's NHL picks...
python -m engine.nhl_tracker --record

echo.
echo Settling completed NHL picks...
python -m engine.nhl_tracker --settle

echo.
echo Refreshing empirical NHL pick-prob calibration...
python -c "from engine.empirical_calibration import refresh_calibration; print(refresh_calibration('nhl'))" 2>nul

echo.
echo Capturing per-pick closing odds (CLV pre-game snapshot)...
REM Stamps current Hard Rock odds as closing_odds for any pending pick.
REM Without this the per-row CLV column stays "-" forever.
python -c "from engine.nhl_tracker import capture_closing_odds; print(capture_closing_odds())" 2>nul

echo.
echo Refreshing POTD live line (track HR line movement)...
python -c "from engine.pick_of_day import refresh_potd_for_line_movement; print(refresh_potd_for_line_movement('nhl'))" 2>nul

echo.
echo Updating POTD closing odds (CLV capture)...
python -c "from engine.pick_of_day import update_potd_closing_odds; print(update_potd_closing_odds('nhl'))" 2>nul

echo.
echo Settling POTD...
python -c "from engine.pick_of_day import settle_potd; print(settle_potd('nhl'))" 2>nul

echo.
echo Ingesting recent finalized player game logs (Phase 2i-i)...
REM 3-day lookback to backfill missed syncs / UTC-rollover games.
python -c "from engine.nhl_player_logs import ingest_recent_finals; print(ingest_recent_finals(lookback_days=3))" 2>nul

echo.
echo Settling player props...
python -c "from engine.player_props_tracker import settle_player_props; print(settle_player_props('nhl'))" 2>nul

echo.
echo Generating NHL player-prop picks (Phase 2i-iii)...
python -c "from engine.nhl_prop_picks import generate_picks; from datetime import datetime, timedelta; print(generate_picks(date=(datetime.now()+timedelta(days=1)).strftime('%%Y-%%m-%%d')))" 2>nul

echo.
echo Refreshing prop POTD live line (track HR line movement)...
python -c "from engine.player_props_potd import refresh_potd_for_line_movement; print(refresh_potd_for_line_movement('nhl'))" 2>nul

echo.
echo ============================================
echo   NHL Sync Complete    [%DATE% %TIME%]
echo ============================================
if /i not "%SESSIONNAME%"=="" if /i not "%SESSIONNAME%"=="Services" if "%~1"=="" pause
