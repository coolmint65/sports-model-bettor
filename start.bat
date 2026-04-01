@echo off
cd /d "%~dp0"
echo ============================================
echo   MLB Prediction Engine
echo ============================================
echo.

:: Run sync first (data + calibrate + record picks + settle)
echo Syncing data...
if not exist "data\logs" mkdir "data\logs"
python -m scrapers.mlb_stats 2>nul
python -m engine.calibration --days 30 2>nul
python -m engine.team_calibration 2>nul
python -m engine.tracker --record 2>nul
python -m engine.tracker --settle 2>nul
echo Sync complete.
echo.

:: Start backend (minimized)
echo Starting servers...
start /min "MLB-Backend" cmd /c "cd /d %~dp0 && pip install -r backend\requirements.txt -q && python -m uvicorn backend.server:app --host 0.0.0.0 --port 8000 --reload"

:: Give backend a moment to boot
timeout /t 3 /nobreak >nul

:: Start frontend (minimized)
start /min "MLB-Frontend" cmd /c "cd /d %~dp0\frontend && npm install --silent && npm run dev"

:: Wait for frontend
timeout /t 5 /nobreak >nul

echo.
echo ============================================
echo   App running at http://localhost:5173
echo   (servers running in background)
echo ============================================
echo.
echo Opening browser...
start http://localhost:5173
echo.
echo You can close this window.
echo To stop the servers, open Task Manager
echo and end "node.exe" and "python.exe".
echo.
pause
