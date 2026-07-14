@echo off
cd /d "%~dp0"
if not exist "data\logs" mkdir "data\logs"

echo ============================================
echo   Manual Data Sync    [%DATE% %TIME%]
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
if "%1"=="--tennis" goto :tennis_only
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
call "%~dp0sync_mlb.bat"
goto :done

:nhl_only
echo Running NHL sync only...
call "%~dp0sync_nhl.bat" %2
goto :done

:tennis_only
echo Running Tennis sync only...
call "%~dp0sync_tennis.bat" %2
goto :done

:quick
echo.
echo -- MLB Sync --
call "%~dp0sync_mlb.bat" --scheduled
echo.
echo -- NHL Sync --
call "%~dp0sync_nhl.bat" --scheduled
echo.
echo -- NBA Sync --
call "%~dp0sync_nba.bat" --scheduled
echo.
echo -- Tennis Sync --
call "%~dp0sync_tennis.bat" --scheduled

REM Weekly GBM retrain - runs every 7 days from inside the existing
REM scheduled sync, so we don't need to register a second Task
REM Scheduler entry. Adds ~60-70 min to that day's sync once a week;
REM skipped on the other 13 of 14 scheduled runs via the marker-file
REM mtime gate (forfiles /D -7 returns errorlevel 0 only when the file
REM is OLDER than 7 days, so we want errorlevel 0 -> run).
set RETRAIN_MARKER=data\logs\weekly_retrain.lastrun
if not exist "%RETRAIN_MARKER%" goto :do_weekly_retrain
forfiles /P "data\logs" /M "weekly_retrain.lastrun" /D -7 >nul 2>&1
if errorlevel 1 goto :skip_weekly_retrain

:do_weekly_retrain
echo.
echo -- Weekly GBM Retrain (cadence: 7d) --
call "%~dp0scripts\weekly_retrain.bat"
echo %DATE% %TIME% > "%RETRAIN_MARKER%"

:skip_weekly_retrain
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
REM Only pause when launched via double-click. Task Scheduler launches
REM in the user's Console session (SESSIONNAME=Console under Logon
REM Mode "Interactive only"), so checking SESSIONNAME alone isn't
REM enough — the schedule must explicitly pass an arg (e.g. --scheduled)
REM so the third guard below catches it. Without the arg-passed guard,
REM the parent paused after the children finished, hung the task for
REM 30 min, and Task Scheduler killed it with exit 255.
if /i not "%SESSIONNAME%"=="" if /i not "%SESSIONNAME%"=="Services" if "%~1"=="" pause
