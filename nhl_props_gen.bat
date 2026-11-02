@echo off
REM Standalone NHL player-props generation (decoupled from the best-bets
REM piggyback 2026-10-01 -- see scripts\nhl_props_gen.py docstring).
REM Registered as scheduled task NhlPropsGen, hourly at :05 around the clock (2026-10-26).
cd /d "%~dp0"
if not exist "data\logs" mkdir "data\logs"
echo [%date% %time%] nhl-props-gen start >> data\logs\nhl_props_gen.log
".venv\Scripts\python.exe" -m scripts.nhl_props_gen >> data\logs\nhl_props_gen.log 2>&1
echo [%date% %time%] nhl-props-gen exit=%errorlevel% >> data\logs\nhl_props_gen.log
