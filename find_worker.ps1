Get-CimInstance Win32_Process -Filter "name='python.exe'" | ForEach-Object {
  ("PID {0}: {1}" -f $_.ProcessId, $_.CommandLine)
}
"---BATS---"
Get-ChildItem C:\sports-model-bettor -Recurse -Filter *.bat -ErrorAction SilentlyContinue | Where-Object { $_.Name -match 'worker|run' } | ForEach-Object { $_.FullName }
"---LOGS(recent worker/live)---"
Get-ChildItem C:\sports-model-bettor -Recurse -Include *worker*.log,*live*.log -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 4 | ForEach-Object { ("{0}  {1}" -f $_.LastWriteTime, $_.FullName) }
