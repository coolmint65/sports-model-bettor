@echo off
cd /d "%~dp0"
echo ============================================
echo   Sports Prediction Engine
echo ============================================
echo.
echo   This is the only script you need to run.
echo   It syncs data, starts servers, and opens
echo   the app in your browser.
echo.

REM Check Python
python --version 2>&1
if errorlevel 1 (
    echo ERROR: Python not found.
    goto :done
)

REM Clear stale bytecode cache so code changes take effect immediately
for /d /r %%d in (__pycache__) do @rd /s /q "%%d" 2>nul

REM Free :8000 / :5173 from any orphan process. Closing the launcher
REM CMD windows doesn't cascade-kill the python/node children on
REM Windows, so a previous run's uvicorn often survives. The new
REM uvicorn then silently fails to bind and exits, leaving the dash
REM hitting yesterday's stale backend. Killing the holders up front
REM means start.bat is always idempotent.
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8000.*LISTENING"') do taskkill /F /PID %%a >nul 2>&1
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":5173.*LISTENING"') do taskkill /F /PID %%a >nul 2>&1

REM Live worker has no listening port, so port-based kills don't
REM catch it. Kill by window title — start.bat below launches it
REM with title "Live-Worker" so the next run cleans up the previous
REM one. Without this, restarts stack worker instances and we'd hit
REM ESPN N times per tick.
taskkill /F /FI "WINDOWTITLE eq Live-Worker*" >nul 2>&1

if not exist "data\logs" mkdir "data\logs"

echo Starting everything...
echo.

REM ── Sync: MLB, NHL, NBA (sequential to avoid DB race conditions) ──
REM Chain with `&` (not `&&`) so a failure in an earlier sport doesn't
REM skip the later ones -- the tracker record/settle steps for each
REM sport live inside that sport's sync, so an MLB hiccup must not
REM block NBA from automating its record/settle pass.
echo [1/5] Syncing data (auto-closes when done)...
REM Pass --scheduled to each sub-script so its pause guard skips. Without
REM the arg the per-sport pause hits (SESSIONNAME=Console under start.bat)
REM and the Data-Sync window sits forever waiting for a keypress instead
REM of auto-closing.
start "Data-Sync" cmd /c "cd /d %~dp0 & call sync_mlb.bat --scheduled & call sync_nhl.bat --scheduled & call sync_nba.bat --scheduled & call sync_tennis.bat --scheduled & exit"

REM ── Backend server ──
echo [3/5] Backend API server...
start /min "Backend-API" cmd /c "cd /d %~dp0 && pip install -r backend\requirements.txt -q 2>nul && python -m uvicorn backend.server:app --host 0.0.0.0 --port 8000 --reload"

REM Give backend a moment to boot
timeout /t 3 /nobreak >nul

REM ── Frontend dev server ──
echo [4/5] Frontend dev server...
start /min "Frontend" cmd /c "cd /d %~dp0\frontend && npm install --silent 2>nul && npm run dev"

REM Wait for frontend to be ready
timeout /t 5 /nobreak >nul

REM ── Live betting worker (Phase 3) ──
REM Polls ESPN scoreboard + HR live odds for in-progress NBA/NHL games
REM every 15s/30s and writes to data/live.db. Without this running, the
REM Live tab on the dashboard goes dark. Spec'd as a separate process
REM (Phase 3 design) so it's isolated from API server restarts.
echo [5/5] Live worker (NBA/NHL polling)...
start /min "Live-Worker" cmd /c "cd /d %~dp0 && python -m services.live_worker.main"

echo.
echo ============================================
echo   App running at http://localhost:5173
echo ============================================
echo.
echo   5 windows launched:
echo     Data-Sync   - syncs MLB, NHL, NBA, Tennis data + records/settles picks (auto-closes)
echo     Backend     - API server on :8000 (stays open)
echo     Frontend    - UI server on :5173 (stays open)
echo     Live-Worker - polls live NBA/NHL state every 15s/30s (stays open)
echo.
echo   To stop: close Backend-API, Frontend, and Live-Worker windows,
echo   or end "node.exe" and "python.exe" in Task Manager.
echo.
start http://localhost:5173

:done
