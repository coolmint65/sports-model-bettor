@echo off
REM ==============================================================
REM  restart-pickmint.bat  -  ONE-COMMAND clean restart of the
REM  PickMint backend (uvicorn on :8000, also serves the frontend).
REM
REM  Does, in order:
REM    1. Kills any running uvicorn (no double-launch)
REM    2. Relaunches via the mb-backend task (venv 3.12 = has uvicorn)
REM    3. Waits until the backend answers 200
REM    4. Warms every slate so the first page load is instant
REM
REM  Safe for START or RESTART. Does NOT touch the sports-app worker
REM  (intentionally stopped 2026-10-12 - no edge, it was starving this).
REM  Just double-click it, or run it from any cmd window.
REM ==============================================================

echo [1/4] Killing any running uvicorn...
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*uvicorn*backend.server*' } | ForEach-Object { Write-Host ('      killed PID ' + $_.ProcessId); Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
timeout /t 3 /nobreak >nul

echo [2/4] Relaunching backend...
REM Supervisor guard (MintyBot 2026-10-25): when the backend runs under run-backend.bat
REM (the auto-restart supervisor loop started by arm.bat / SportsBettorArm), killing uvicorn
REM is enough -- the supervisor relaunches it in 5s. Firing the mb-backend task as well
REM launches a SECOND backend that fights the first for :8000 (the 2026-10-06 flapping
REM outage). Only use the task when no supervisor is present (the one-shot bridge topology).
powershell -NoProfile -Command "if (Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'cmd.exe' -and $_.CommandLine -like '*run-backend.bat*' }) { exit 1 } else { exit 0 }"
if errorlevel 1 (
  echo       run-backend.bat supervisor is running -- it relaunches uvicorn itself, skipping task launch
) else (
  echo       no supervisor -- launching via mb-backend task
  schtasks /run /tn mb-backend >nul 2>&1
)

echo [3/4] Waiting for backend to answer...
powershell -NoProfile -Command "$ok=$false; for ($i=0; $i -lt 30; $i++) { try { $r = Invoke-WebRequest 'http://localhost:8000/' -TimeoutSec 5 -UseBasicParsing; Write-Host ('      UP after ~' + ($i*5) + 's  (root ' + $r.StatusCode + ')'); $ok=$true; break } catch { Start-Sleep 5 } }; if (-not $ok) { Write-Host '      STILL DOWN after 150s -- check manually (python procs / log)'; exit 1 }"
if errorlevel 1 goto :end

echo [4/4] Warming slates (sequential, avoids the cold-compute storm)...
powershell -NoProfile -Command "$urls = @('nhl/best-bets','nba/best-bets','football/nfl/today','football/cfb/today','soccer/leagues','basketball/wnba/today','tennis/scheduled?tour=atp','tennis/scheduled?tour=wta','tracker/history','tracker/summary','nhl/tracker/history','nhl/tracker/summary','nba/tracker/history','nba/tracker/summary'); foreach ($u in $urls) { try { $t = Measure-Command { Invoke-WebRequest ('http://localhost:8000/api/' + $u) -TimeoutSec 90 -UseBasicParsing | Out-Null }; Write-Host ('      ' + $u + '  ' + [int]$t.TotalSeconds + 's') } catch { Write-Host ('      ' + $u + '  (skipped/none)') } }"

echo.
echo === PickMint restart complete - backend up and warmed ===

:end
