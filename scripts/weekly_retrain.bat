@echo off
cd /d "%~dp0\.."
echo ============================================
echo   Weekly GBM Retrain
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"
if not exist "data\models" mkdir "data\models"

set LOG_FILE=data\logs\weekly_retrain.jsonl
set LOG_LEVEL=INFO
set PYTHONPATH=%~dp0\..;%PYTHONPATH%

echo Retraining MLB GBM...
python scripts\run.py engine.gbm.train mlb --days 1095 -v

echo.
echo ============================================
echo   Retrain complete. Latest artifacts in data\models\
echo ============================================
