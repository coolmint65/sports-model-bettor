@echo off
cd /d "%~dp0"
echo ============================================
echo   Manual Data Sync
echo ============================================
echo.
echo   NOTE: start.bat already does a full sync
echo   automatically. Only use this for:
echo     --full       Full MLB rebuild + advanced stats
echo     --history X  Load historical season (e.g. 2025)
echo     --mlb        MLB only
echo     --nhl        NHL only
echo     --nhl --full Full NHL rebuild (rosters + stats)
echo     (no args)    Quick sync both sports
echo.

python --version 2>&1
if errorlevel 1 (
    echo ERROR: Python not found.
    goto :done
)

if not exist "data\logs" mkdir "data\logs"

if "%1"=="--mlb" goto :mlb_only
if "%1"=="--nhl" goto :nhl_only
if "%1"=="--full" goto :full
if "%1"=="--daily" goto :daily
if "%1"=="--history" goto :history
goto :quick

:full
echo Running FULL MLB data sync...
python -m scrapers.mlb_stats --full
python -m scrapers.mlb_advanced
goto :calibrate

:daily
echo Running daily MLB sync...
python -m scrapers.mlb_stats --daily
goto :calibrate

:history
echo Loading %2 season data for backtesting...
python -m scrapers.mlb_stats --history %2
goto :calibrate

:mlb_only
echo Running MLB sync only...
call sync_mlb.bat
goto :done

:nhl_only
echo Running NHL sync only...
call sync_nhl.bat %2
goto :done

:quick
echo.
echo ── MLB Sync ──
call sync_mlb.bat
echo.
echo ── NHL Sync ──
call sync_nhl.bat
goto :done

:calibrate
echo.
echo Calibrating MLB model...
python -m engine.calibration --days 30
python -m engine.team_calibration
echo.
echo Recording + settling MLB picks...
python -m engine.tracker --record
python -m engine.tracker --settle
echo.
echo Recording + settling NHL picks...
python -m engine.nhl_tracker --record
python -m engine.nhl_tracker --settle

:done
echo.
echo ============================================
echo   Sync Complete
echo ============================================
echo.
pause
