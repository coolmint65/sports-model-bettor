@echo off
REM Restart the live_worker the SAFE way: kill the worker python ONLY.
REM The run-worker.bat supervisor auto-restarts it in ~5s. Never kill
REM the supervisor (cmd.exe running run-worker.bat) itself.
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*services.live_worker.main*' } | ForEach-Object { Write-Host ('killing worker ' + $_.ProcessId); Stop-Process -Id $_.ProcessId -Force }"
echo live_worker killed - run-worker supervisor relaunches it in ~5s.
