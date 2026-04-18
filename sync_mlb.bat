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
    echo This fetches the full season + player stats (5-10 minutes).
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
echo Updating POTD closing odds (CLV capture)...
python -c "from engine.pick_of_day import update_potd_closing_odds; print(update_potd_closing_odds('mlb'))" 2>nul

echo.
echo Settling POTD...
python -c "from engine.pick_of_day import settle_potd; print(settle_potd('mlb'))" 2>nul

echo.
echo Auto-tuning ensemble weights (skipped when < 200 settled signals/market)...
python scripts\run.py engine.ensemble_auto_tune mlb -v 2>nul

echo.
echo Backing up DBs...
python scripts\run.py scripts.backup_dbs

echo.
echo ============================================
echo   MLB Sync Complete
echo ============================================
