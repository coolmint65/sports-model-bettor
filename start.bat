@echo off
cd /d "%~dp0"
echo ============================================
echo   MLB Prediction Engine
echo ============================================
echo.

:: Run sync first (data + calibrate + record picks + settle)
echo Syncing data...
echo.
if not exist "data\logs" mkdir "data\logs"
python -m scrapers.mlb_stats 2>nul
python -m engine.calibration --days 30 2>nul
python -m engine.tracker --record 2>nul
python -m engine.tracker --settle 2>nul
echo.
echo Sync complete.
echo.

:: Start backend
echo Starting backend on http://localhost:8000 ...
start "Backend" cmd /k "cd /d %~dp0 && pip install -r backend\requirements.txt -q && python -m uvicorn backend.server:app --host 0.0.0.0 --port 8000 --reload"

:: Give backend a moment to boot
timeout /t 3 /nobreak >nul

:: Start frontend
echo Starting frontend on http://localhost:5173 ...
start "Frontend" cmd /k "cd /d %~dp0\frontend && npm install && npm run dev"

:: Wait for frontend
timeout /t 5 /nobreak >nul

echo.
echo ============================================
echo   Backend:  http://localhost:8000
echo   Frontend: http://localhost:5173
echo ============================================
echo.
echo Opening browser...
start http://localhost:5173
