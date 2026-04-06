@echo off
cd /d "%~dp0"
echo ============================================
echo   Sports Prediction Engine
echo ============================================
echo.

REM Check Python
python --version 2>&1
if errorlevel 1 (
    echo ERROR: Python not found.
    goto :done
)

if not exist "data\logs" mkdir "data\logs"

echo Starting syncs and servers...
echo.

REM ── Sync: MLB (auto-closes when done) ──
echo [1/4] MLB Sync...
start "MLB-Sync" cmd /c "cd /d %~dp0 && call sync_mlb.bat && exit"

REM ── Sync: NHL (auto-closes when done) ──
echo [2/4] NHL Sync...
start "NHL-Sync" cmd /c "cd /d %~dp0 && call sync_nhl.bat && exit"

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
echo   Windows opened:
echo     MLB-Sync   - syncs MLB data (auto-closes)
echo     NHL-Sync   - syncs NHL data (auto-closes)
echo     Backend    - API server on :8000 (minimized)
echo     Frontend   - Vite dev server (minimized)
echo.
echo Opening browser...
start http://localhost:5173
echo.
echo To stop servers: close Backend and Frontend windows
echo   or end "node.exe" and "python.exe" in Task Manager.
echo.

:done
