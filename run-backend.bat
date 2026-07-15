@echo off
REM Backend supervisor for the Sports API (uvicorn backend.server:app).
REM Launched by start.beelink.bat in its own "Backend-API" window.
REM (1) Redirects uvicorn stdout+stderr to data\logs\backend.log so a
REM     crash traceback survives (bare window loses it on close).
REM (2) Auto-restarts uvicorn if it exits so the API/dashboard self-heal.
REM HR_RELAY_URL / HR_RELAY_TOKEN / AUTO_BET_LIVE are exported by arm.bat
REM and inherited here; fallbacks only matter for a standalone run.

cd /d "%~dp0"

if not defined HR_RELAY_URL set HR_RELAY_URL=http://127.0.0.1:7478
if not defined AUTO_BET_LIVE set AUTO_BET_LIVE=1
if not exist "data\logs" mkdir "data\logs"

:loop
echo [%date% %time%] backend-supervisor: starting uvicorn on :8000 >> data\logs\backend.log
python -m uvicorn backend.server:app --host 0.0.0.0 --port 8000 --no-access-log >> data\logs\backend.log 2>&1
echo [%date% %time%] backend-supervisor: uvicorn EXITED (errorlevel=%errorlevel%) - restarting in 5s >> data\logs\backend.log
timeout /t 5 /nobreak >nul
goto loop
