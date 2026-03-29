@echo off
cd /d "%~dp0"
echo ============================================
echo   MLB Data Sync
echo ============================================
echo.
echo Working directory: %cd%
echo.

REM Check Python is available
python --version 2>&1
if errorlevel 1 (
    echo.
    echo ERROR: Python not found. Make sure Python is installed and on your PATH.
    goto :done
)

REM Create logs directory
if not exist "data\logs" mkdir "data\logs"

echo.
echo Running quick sync (teams, today's games, standings)...
echo.
python -m scrapers.mlb_stats
if errorlevel 1 (
    echo.
    echo ERROR: Sync failed. See error above.
)

:done
echo.
echo ============================================
echo   Finished
echo ============================================
echo.
pause
