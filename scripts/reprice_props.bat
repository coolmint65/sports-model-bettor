@echo off
cd /d C:\sports-model-bettor
set PYTHONPATH=C:\sports-model-bettor;%PYTHONPATH%
if not exist "data\logs" mkdir "data\logs"
echo [%DATE% %TIME%] reprice props >> data\logs\reprice_props.log
C:\sports-model-bettor\.venv\Scripts\python.exe C:\sports-model-bettor\scripts\reprice_props.py >> data\logs\reprice_props.log 2>&1
