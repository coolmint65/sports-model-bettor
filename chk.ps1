$u = @(Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*uvicorn*backend.server*' })
Write-Output ("uvicorn procs " + $u.Count)
foreach($p in $u){ Write-Output ("  PID " + $p.ProcessId) }
$rb = @(Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*run-backend*' })
Write-Output ("run-backend loop procs " + $rb.Count)
for($i=1;$i -le 6;$i++){
  try { $r=Invoke-WebRequest -Uri 'http://localhost:8000/health' -TimeoutSec 8 -UseBasicParsing; Write-Output ("poll " + $i + " HTTP " + $r.StatusCode); break } catch { Write-Output ("poll " + $i + " ERR") }
  Start-Sleep -Seconds 6
}
