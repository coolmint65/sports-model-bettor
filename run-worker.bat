@echo off
REM Live-worker supervisor — mirror of run-backend.bat for the worker
REM process (services.live_worker.main). Same three jobs:
REM (1) Redirects stdout+stderr to data\logs\worker.log so any crash
REM     traceback survives instead of vanishing with the window.
REM (2) Auto-restarts on exit so a transient ESPN 502 / SQLite lock /
REM     network blip doesn't take the cadences down until the next
REM     logon (task scheduler retriggers arm.bat, but only on logon).
REM (3) Inherits HR_RELAY_URL / HR_RELAY_TOKEN / AUTO_BET_LIVE from
REM     arm.bat with the same fallbacks the backend supervisor uses.

cd /d "%~dp0"

if not defined HR_RELAY_URL set HR_RELAY_URL=http://127.0.0.1:7478
if not defined AUTO_BET_LIVE set AUTO_BET_LIVE=1
if not exist "data\logs" mkdir "data\logs"

:loop
echo [%date% %time%] worker-supervisor: starting live_worker >> data\logs\worker.log
python -m services.live_worker.main >> data\logs\worker.log 2>&1
echo [%date% %time%] worker-supervisor: live_worker EXITED (errorlevel=%errorlevel%) - restarting in 5s >> data\logs\worker.log
timeout /t 5 /nobreak >nul
goto loop
