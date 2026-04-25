' run-silent.vbs - invoke a sibling .bat with no visible console window.
'
' Why this exists: Task Scheduler running a .bat file pops a cmd.exe
' window in the user's session that steals focus mid-work. Routing
' the action through wscript.exe + this wrapper hides the console
' entirely (wscript has no console of its own; we launch cmd.exe with
' nShow=0). Wait=True so the real exit code propagates back to
' Task Scheduler instead of always returning 0.
'
' Usage (from Task Scheduler action):
'   Program/script:   wscript.exe
'   Arguments:        "E:\sports-model-bettor\scripts\run-silent.vbs" sync.bat --scheduled
'   Start in:         E:\sports-model-bettor

Option Explicit

Dim sh, fso, repoDir, target, cmd, i, exitCode
Set sh  = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

If WScript.Arguments.Count = 0 Then
    WScript.Quit 1
End If

' Repo root = parent of the scripts/ directory this file lives in.
repoDir = fso.GetParentFolderName(fso.GetParentFolderName(WScript.ScriptFullName))
target  = WScript.Arguments(0)

cmd = "cmd.exe /c """ & repoDir & "\" & target & """"
For i = 1 To WScript.Arguments.Count - 1
    cmd = cmd & " " & WScript.Arguments(i)
Next

sh.CurrentDirectory = repoDir
' nShow=0 hides the cmd window; bWaitOnReturn=True returns the bat's
' exit code so Task Scheduler logs accurate Last Result values.
exitCode = sh.Run(cmd, 0, True)
WScript.Quit exitCode
