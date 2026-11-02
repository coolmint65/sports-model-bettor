@echo off
REM Daily NBA + NHL roster refresh (wired 2026-09-30). Nothing else on
REM the box refreshes rosters; without this NBA drifts to last-season's
REM roster (it sat frozen April->Sept). NFL/CFB self-refresh via logs.
cd /d "%~dp0"
if not exist "data\logs" mkdir "data\logs"
echo [%date% %time%] roster-refresh start >> data\logs\roster_refresh.log
".venv\Scripts\python.exe" -m scrapers.refresh_rosters >> data\logs\roster_refresh.log 2>&1
echo [%date% %time%] roster-refresh exit=%errorlevel% >> data\logs\roster_refresh.log
