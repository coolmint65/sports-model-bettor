@echo off
cd /d "%~dp0"
echo ============================================
echo   Sports Prediction Engine
echo ============================================
echo.
echo   This is the only script you need to run.
echo   It syncs data, starts servers, and opens
echo   the app in your browser.
echo.

REM Check Python
python --version 2>&1
if errorlevel 1 (
    echo ERROR: Python not found.
    goto :done
)

REM Clear stale bytecode cache so code changes take effect immediately
for /d /r %%d in (__pycache__) do @rd /s /q "%%d" 2>nul

if not exist "data\logs" mkdir "data\logs"

echo Starting everything...
echo.

REM ── Sync: MLB, NHL, NBA (sequential to avoid DB race conditions) ──
echo [1/4] Syncing data (auto-closes when done)...
start "Data-Sync" cmd /c "cd /d %~dp0 && call sync_mlb.bat && call sync_nhl.bat && call sync_nba.bat && exit"

REM ── Backend server ──
echo [3/4] Backend API server...
start /min "Backend-API" cmd /c "cd /d %~dp0 && pip install -r backend\requirements.txt -q 2>nul && python -m uvicorn backend.server:app --host 0.0.0.0 --port 8000 --reload"

REM Give backend a moment to boot
timeout /t 3 /nobreak >nul

REM ── Frontend dev server ──
echo [4/4] Frontend dev server...
start /min "Frontend" cmd /c "cd /d %~dp0\frontend && npm install --silent 2>nul && npm run dev"

REM Wait for frontend to be ready
timeout /t 5 /nobreak >nul

echo.
echo ============================================
echo   App running at http://localhost:5173
echo ============================================
echo.
echo   4 windows launched:
echo     Data-Sync - syncs MLB, NHL, NBA data + records/settles picks (auto-closes)
echo     Backend   - API server on :8000 (stays open)
echo     Frontend  - UI server on :5173 (stays open)
echo.
echo   To stop: close Backend-API and Frontend windows,
echo   or end "node.exe" and "python.exe" in Task Manager.
echo.
start http://localhost:5173

:done
