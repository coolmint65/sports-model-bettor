$procs = Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object { $_.CommandLine -like '*live_worker*' }
if ($procs) {
  foreach ($p in $procs) { Write-Output ("killing worker PID " + $p.ProcessId); Stop-Process -Id $p.ProcessId -Force }
} else {
  Write-Output "no live_worker python process found"
}
