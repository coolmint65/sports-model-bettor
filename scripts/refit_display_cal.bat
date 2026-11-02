@echo off
cd /d C:\sports-model-bettor
if not exist data\logs mkdir data\logs
echo [%date% %time%] refit start >> data\logs\display_cal_refit.log
.venv\Scripts\python.exe scripts\fit_display_calibration.py >> data\logs\display_cal_refit.log 2>&1
echo [%date% %time%] refit done rc=%errorlevel% >> data\logs\display_cal_refit.log
