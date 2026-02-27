@echo off
echo ============================================
echo   Sports Betting Model - Starting Up...
echo ============================================
echo.

:: Start the backend in the background
echo Starting backend on http://localhost:8000 ...
cd /d "%~dp0backend"
start /B python run.py > nul 2>&1

:: Wait for backend to be ready
timeout /t 3 /nobreak > nul

:: Install frontend deps if needed and start
echo Starting frontend on http://localhost:3000 ...
cd /d "%~dp0frontend"
if not exist node_modules (
    echo Installing frontend dependencies...
    call npm install > nul 2>&1
)
start /B npx vite --port 3000 > nul 2>&1

:: Wait a moment then open browser
timeout /t 3 /nobreak > nul

echo.
echo ============================================
echo   Backend:  http://localhost:8000/docs
echo   Frontend: http://localhost:3000
echo ============================================
echo.
echo Opening browser...
start http://localhost:3000

echo Press any key to stop all services...
pause > nul

:: Cleanup - kill python and node processes we started
taskkill /F /IM python.exe /FI "WINDOWTITLE eq *run.py*" > nul 2>&1
taskkill /F /IM node.exe > nul 2>&1
echo Services stopped.
