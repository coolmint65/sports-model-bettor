@echo off
REM Beelink-side launcher — backend + worker + data sync only, NO
REM frontend. The frontend runs on the dev box; it just proxies /api
REM here over Tailscale. Skipping the npm boot on the Beelink saves
REM ~500 MB RAM + a chunk of startup time and avoids Node fighting
REM Chrome/GeoComply for a browser context.

cd /d "%~dp0"
echo ============================================
echo   Sports Prediction Engine (Beelink)
echo ============================================
echo   Backend + worker only; no frontend.
echo.

REM Check Python — arm.bat activates the .venv 3.12 before calling
REM us, so `python` here should already be the venv one.
python --version 2>&1
if errorlevel 1 (
    echo ERROR: Python not found. Did arm.bat activate the venv?
    goto :done
)

REM Clear stale bytecode cache so code changes take effect immediately
for /d /r %%d in (__pycache__) do @rd /s /q "%%d" 2>nul

REM Free :8000 from any orphan process. Closing the launcher CMD
REM windows doesn't cascade-kill the python children on Windows, so
REM a previous run's uvicorn often survives.
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8000.*LISTENING"') do taskkill /F /PID %%a >nul 2>&1

REM Live worker has no listening port; kill by window title so
REM restarts don't stack instances (ESPN N times per tick).
taskkill /F /FI "WINDOWTITLE eq Live-Worker*" >nul 2>&1

if not exist "data\logs" mkdir "data\logs"

echo Starting backend + worker...
echo.

REM ── Data sync (MLB, NHL, NBA, Tennis) ──
REM Chain with `&` so a failure in an earlier sport doesn't skip the
REM later ones — each sport's tracker record/settle lives inside its
REM own sync.
echo [1/3] Syncing data (auto-closes when done)...
start "Data-Sync" cmd /c "cd /d %~dp0 & call sync_mlb.bat --scheduled & call sync_nhl.bat --scheduled & call sync_nba.bat --scheduled & call sync_tennis.bat --scheduled & exit"

REM ── Backend API server ──
REM run-backend.bat is a supervisor: it redirects uvicorn stdout+stderr
REM to data\logs\backend.log and auto-restarts on exit so a crash doesn't
REM take the API + dashboard down until the next logon-triggered arm.
echo [2/3] Backend API server on :8000 (supervised)...
start /min "Backend-API" cmd /c "cd /d %~dp0 && call run-backend.bat"

REM Give backend a moment to boot
timeout /t 3 /nobreak >nul

REM ── Live worker (Phase 3) ──
REM Polls ESPN + HR odds for in-progress games + drives all the
REM lazy-on-click cadences (framework refresh, prematch sync, queue
REM placer sweep). run-worker.bat mirrors the backend supervisor —
REM redirects to data\logs\worker.log and auto-restarts on exit.
echo [3/3] Live worker (supervised)...
start /min "Live-Worker" cmd /c "cd /d %~dp0 && call run-worker.bat"

echo.
echo ============================================
echo   Backend at http://127.0.0.1:8000  (over Tailscale: http://100.80.245.68:8000)
echo ============================================
echo   3 windows launched:
echo     Data-Sync   - syncs MLB, NHL, NBA, Tennis (auto-closes)
echo     Backend-API - API server on :8000 (stays open)
echo     Live-Worker - polls live state + drives placer (stays open)
echo.
echo   Frontend runs on the dev box; nothing to launch here.
echo   To stop: close Backend-API and Live-Worker windows.

:done
