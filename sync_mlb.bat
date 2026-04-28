@echo off
cd /d "%~dp0"
echo ============================================
echo   MLB Data Sync
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"

REM Route Python logging through the structured JSON formatter so the
REM nightly log can be tailed/grepped/jq'd. Modules that call
REM scripts.structured_log.configure_from_env() honor these vars; bare
REM logging.basicConfig calls keep their human-formatted output.
set LOG_FILE=data\logs\sync_mlb.jsonl
set LOG_LEVEL=INFO
set PYTHONPATH=%~dp0;%PYTHONPATH%

REM Auto-detect: if no final games with linescores OR no player stats, do full sync
python -c "from engine.db import get_conn; c=get_conn(); g=c.execute('SELECT COUNT(*) as c FROM games WHERE status=\"final\" AND home_linescore IS NOT NULL').fetchone()['c']; p=c.execute('SELECT COUNT(*) FROM pitcher_stats').fetchone()[0]; exit(0 if g > 10 and p > 10 else 1)" 2>nul
if errorlevel 1 (
    echo First run or missing data - running full MLB sync...
    echo This fetches the full season + player stats ^(5-10 minutes^).
    echo.
    python scripts\run.py scrapers.mlb_stats --full
    echo.
    echo Running advanced stats...
    python scripts\run.py scrapers.mlb_advanced 2>nul
    echo.
    goto :calibrate
)

echo Quick sync (teams, today's games, standings)...
python scripts\run.py scrapers.mlb_stats

echo.
echo Draining pending odds (matchups now resolvable to mlb_game_id)...
python -c "from engine.odds_history import drain_pending_mlb_odds; print(drain_pending_mlb_odds())" 2>nul

:calibrate
echo.
echo Refreshing umpire tendency table from recent games...
REM Run after both full and quick sync paths so GBM's umpire features
REM see fresh run_factor / k_pct / over_pct on every sync.
python -c "from engine.umpire import update_umpire_stats; print(update_umpire_stats())" 2>nul

echo.
echo Calibrating global model...
REM --auto picks the look-back window based on season progress; see
REM engine.calibration.adaptive_window() for the per-phase ranges.
python scripts\run.py engine.calibration --auto

echo.
echo Calibrating per-team factors...
python scripts\run.py engine.team_calibration

echo.
echo Checking for late-lineup deltas (invalidates stale picks + POTD)...
REM Snapshots today's confirmed lineups and compares vs the morning
REM snapshot. On delta: drops picks_cache + unsettled POTD for the
REM affected game and force-re-records the tracker with the fresh
REM lineup. First run of the day just stores snapshots silently.
python -c "from engine.lineup_refresh import refresh_for_date; import json; print(json.dumps(refresh_for_date(), indent=2, default=str))" 2>nul

echo.
echo Recording today's picks...
python scripts\run.py engine.tracker --record

echo.
echo Settling completed picks...
python scripts\run.py engine.tracker --settle

echo.
echo Refreshing empirical pick-prob calibration from settled picks...
REM Per-bet-type lookup tables built from picks.result. Pickled in
REM memory; refreshed nightly so the over-confidence at the upper tail
REM converges toward observed reality as more picks settle.
python -c "from engine.empirical_calibration import refresh_calibration; print(refresh_calibration())" 2>nul

echo.
echo Auto-applying train recommendations (n>=30, p<0.01, disable-only)...
python scripts\run.py engine.train mlb --apply 2>nul

echo.
echo Capturing per-pick closing odds (CLV pre-game snapshot)...
REM Stamps current real odds as closing_odds for any pending pick.
REM Without this the Pick Tracker per-row CLV column stays "-" because
REM the settle-time path only fires for a fraction of picks.
python -c "from engine.tracker import capture_closing_odds; print(capture_closing_odds())" 2>nul

echo.
echo Refreshing POTD live line (track HR line movement)...
python -c "from engine.pick_of_day import refresh_potd_for_line_movement; print(refresh_potd_for_line_movement('mlb'))" 2>nul

echo.
echo Updating POTD closing odds (CLV capture)...
python -c "from engine.pick_of_day import update_potd_closing_odds; print(update_potd_closing_odds('mlb'))" 2>nul

echo.
echo Settling POTD...
python -c "from engine.pick_of_day import settle_potd; print(settle_potd('mlb'))" 2>nul

echo.
echo Ingesting recent finalized player game logs (Phase 2g-i)...
REM 3-day lookback so a missed sync or a UTC-rollover game doesn't
REM leave logs un-ingested for >24h (DNP-void would then wrongly mark
REM real plays as Push). Idempotent — already-ingested games are no-op.
python -c "from engine.mlb_player_logs import ingest_recent_finals; print(ingest_recent_finals(lookback_days=3))" 2>nul

echo.
echo Settling player props...
python -c "from engine.player_props_tracker import settle_player_props; print(settle_player_props('mlb'))" 2>nul

echo.
echo Generating MLB player-prop picks (Phase 2g-iii)...
REM Picker runs after ingest+settle so today's just-finalized games
REM contribute to per-player rolling means before tomorrow's slate
REM is scored.
python -c "from engine.mlb_prop_picks import generate_picks; from datetime import datetime, timedelta; print(generate_picks(date=(datetime.now()+timedelta(days=1)).strftime('%%Y-%%m-%%d')))" 2>nul

echo.
echo Refreshing prop POTD live line (track HR line movement)...
python -c "from engine.player_props_potd import refresh_potd_for_line_movement; print(refresh_potd_for_line_movement('mlb'))" 2>nul

echo.
echo Auto-tuning ensemble weights (skipped when < 200 settled signals/market)...
python scripts\run.py engine.ensemble_auto_tune mlb -v 2>nul

echo.
echo Backing up DBs...
python scripts\run.py scripts.backup_dbs

echo.
echo ============================================
echo   MLB Sync Complete    [%DATE% %TIME%]
echo ============================================
REM Only pause when launched interactively (via double-click, not
REM Task Scheduler). See sync.bat for the same SESSIONNAME check.
if /i not "%SESSIONNAME%"=="" if /i not "%SESSIONNAME%"=="Services" if "%~1"=="" pause
