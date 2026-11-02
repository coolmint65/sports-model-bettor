@echo off
REM Restart the backend the SAFE way: kill uvicorn ONLY. The run-backend
REM supervisor (venv-robust) auto-restarts it in ~5s; full boot ~90s.
REM Never kill the supervisor itself - a bare relaunch grabs system python.
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*uvicorn*backend.server*' } | ForEach-Object { Write-Host ('killing uvicorn ' + $_.ProcessId); Stop-Process -Id $_.ProcessId -Force }"
echo Backend uvicorn killed - supervisor relaunches it (venv 3.12) in ~5s.
