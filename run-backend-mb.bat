@echo off
cd /d "%~dp0"
call "%~dp0relay-secrets.bat"
:loop
echo [mb-backend %date% %time%] starting uvicorn (venv 3.12) >> data\logs\backend.log 2>&1
".venv\Scripts\python.exe" -m uvicorn backend.server:app --host 0.0.0.0 --port 8000 --no-access-log >> data\logs\backend.log 2>&1
echo [mb-backend %date% %time%] uvicorn EXITED - restart in 5s >> data\logs\backend.log 2>&1
timeout /t 5 /nobreak >nul
goto loop
