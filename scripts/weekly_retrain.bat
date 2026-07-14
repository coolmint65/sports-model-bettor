@echo off
REM Weekly GBM retrain - covers EVERY sport's prematch + live + props.
REM
REM History: this script only retrained MLB until 2026-05-08. NBA, NHL,
REM the live models, and props all relied on manual triggers; live GBMs
REM went 8+ days stale during playoffs. New version retrains everything
REM the inference path actually loads.
REM
REM Time-decay: 365-day half-life on team sports so recent rotations
REM (playoffs, trades, rule changes) weigh more than career averages.
REM Tennis defaults to 730 inside the train CLI; not invoked here
REM because sync_tennis.bat retrains tennis daily.
REM
REM On failure: each step is independent - a sport that errors out
REM doesn't abort the rest. Output streams to data\logs\weekly_retrain.log.

cd /d "%~dp0\.."
echo ============================================
echo   Weekly GBM Retrain  (%DATE% %TIME%)
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"
if not exist "data\models" mkdir "data\models"

set RUN_LOG=data\logs\weekly_retrain.log
set LOG_FILE=data\logs\weekly_retrain.jsonl
set LOG_LEVEL=INFO
set PYTHONPATH=%~dp0\..;%PYTHONPATH%

echo === Run start: %DATE% %TIME% === >> %RUN_LOG%

REM --- Prematch GBMs ---
echo Retraining MLB prematch GBM...
echo --- mlb prematch (%TIME%) --- >> %RUN_LOG%
python scripts\run.py engine.gbm.train mlb --days 1095 --time-decay-half-life-days 365 -v >> %RUN_LOG% 2>&1

echo Retraining NHL prematch GBM...
echo --- nhl prematch (%TIME%) --- >> %RUN_LOG%
python scripts\run.py engine.gbm.train nhl --days 1095 --time-decay-half-life-days 365 -v >> %RUN_LOG% 2>&1

echo Retraining NBA prematch GBM...
echo --- nba prematch (%TIME%) --- >> %RUN_LOG%
python scripts\run.py engine.gbm.train nba --days 1095 --time-decay-half-life-days 365 -v >> %RUN_LOG% 2>&1

REM --- Live GBMs (Phase 5e) ---
REM No time-decay: features_live emits one row per (game, period_end)
REM with empty _date col, so the harness can't compute days_old and the
REM resulting weights underflow to 0 -> xgboost rejects with
REM "Weights must be positive values". Live PBP corpus is already
REM recent (last 2 seasons), so career-decay isn't useful here anyway.
echo Retraining NBA live GBM...
echo --- nba_live (%TIME%) --- >> %RUN_LOG%
python scripts\run.py engine.gbm.train nba_live --days 730 -v >> %RUN_LOG% 2>&1

echo Retraining NHL live GBM...
echo --- nhl_live (%TIME%) --- >> %RUN_LOG%
python scripts\run.py engine.gbm.train nhl_live --days 730 -v >> %RUN_LOG% 2>&1

REM --- Props GBMs ---
echo Retraining MLB props GBMs...
echo --- mlb props (%TIME%) --- >> %RUN_LOG%
python scripts\retrain_props_gbm.py mlb >> %RUN_LOG% 2>&1

echo Retraining NBA props GBMs...
echo --- nba props (%TIME%) --- >> %RUN_LOG%
python scripts\retrain_props_gbm.py nba >> %RUN_LOG% 2>&1

echo Retraining NHL props GBMs...
echo --- nhl props (%TIME%) --- >> %RUN_LOG%
python scripts\retrain_props_gbm.py nhl >> %RUN_LOG% 2>&1

echo === Run end: %DATE% %TIME% === >> %RUN_LOG%

echo.
echo ============================================
echo   Retrain complete. Latest artifacts in data\models\
echo   Log: %RUN_LOG%
echo ============================================
