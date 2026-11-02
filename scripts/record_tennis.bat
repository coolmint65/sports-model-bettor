@echo off
cd /d C:\sports-model-bettor
if not exist data\logs mkdir data\logs
echo [%date% %time%] tennis schedule refresh start >> data\logs\tennis_record.log
.venv\Scripts\python.exe scripts\refresh_tennis_schedule.py >> data\logs\tennis_record.log 2>&1
echo [%date% %time%] tennis schedule refresh done rc=%errorlevel% >> data\logs\tennis_record.log
