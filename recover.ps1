Copy-Item 'C:/sports-model-bettor/backend/server/routes_nhl.py.bak-paperonly' 'C:/sports-model-bettor/backend/server/routes_nhl.py' -Force
Write-Output "routes_nhl reverted"
$bp = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*uvicorn*backend.server*' }
foreach ($p in $bp) { Write-Output ("kill hung backend "+$p.ProcessId); Stop-Process -Id $p.ProcessId -Force }
Start-Sleep -Seconds 22
try { $r=Invoke-WebRequest -Uri 'http://localhost:8000/health' -TimeoutSec 15 -UseBasicParsing; Write-Output ("backend HTTP "+$r.StatusCode) } catch { Write-Output ("backend "+$_.Exception.Message) }
