# Update the sports-model-bettor scheduled task to:
#   - run sync.bat through scripts\run-silent.vbs so no cmd window pops
#     up and steals focus during the day
#   - pass --scheduled so the parent doesn't pause-hang the task
#   - run twice daily (8am and 5pm local time)
#
# Run from an ELEVATED PowerShell:
#     powershell -ExecutionPolicy Bypass -File E:\sports-model-bettor\scripts\update-scheduler.ps1
#
# After it finishes, trigger a test run:
#     schtasks /Run /TN sports-model-bettor.task.utf16
# Last Result should be 0 and no console window should flash.

$ErrorActionPreference = "Stop"

$repoRoot = "E:\sports-model-bettor"
$taskName = "sports-model-bettor.task.utf16"
$vbs      = Join-Path $repoRoot "scripts\run-silent.vbs"

# wscript.exe + run-silent.vbs is the focus-stealing fix. wscript has no
# console, run-silent launches cmd.exe with nShow=0, real exit code is
# preserved for Task Scheduler logging.
$action = New-ScheduledTaskAction `
    -Execute "wscript.exe" `
    -Argument "`"$vbs`" sync.bat --scheduled" `
    -WorkingDirectory $repoRoot

$trigger1 = New-ScheduledTaskTrigger -Daily -At 8am
$trigger2 = New-ScheduledTaskTrigger -Daily -At 5pm

# -Hidden hides the task itself in the Scheduled Tasks UI list (cosmetic).
# The window-suppression comes from wscript+vbs, not this flag.
$settings = New-ScheduledTaskSettingsSet -Hidden

Set-ScheduledTask -TaskName $taskName `
    -Action $action `
    -Trigger @($trigger1, $trigger2) `
    -Settings $settings

Write-Host ""
Write-Host "OK - $taskName updated"
Write-Host "  Action:  wscript.exe `"$vbs`" sync.bat --scheduled"
Write-Host "  Trigger: Daily 08:00 + 17:00 local"
Write-Host "  Window:  hidden (no console flash)"
Write-Host ""
Write-Host "Verify (no popup expected):"
Write-Host "  schtasks /Run /TN $taskName"
