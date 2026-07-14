# Deploy the sports backend to the Beelink

Goal: the backend + worker + auto-placer run on the Beelink 24/7.
The frontend stays on your local dev box; you just point it at the
Beelink's Tailscale IP.

## Prerequisites

- Beelink is on the tailnet (already is — PiBot lives there)
- Python 3.11+ installed on Beelink (`python --version` from a
  Beelink CMD)
- ~12 GB free disk (~8 GB for the data dir + ~2 GB Python deps + headroom)
- Sports app repo pulled onto the Beelink OR rsynced from local

## Colocation notes (from TT/PiBot)

Once we're on the same box as the relay + Chrome bridge:

- **Relay URL becomes `http://127.0.0.1:7478`** (localhost, no
  Tailscale hop). Lower latency; also means placement keeps working
  if Tailscale hiccups. The `:8000` we bind for the sports API is
  distinct from :7478 so no port clash.
- **Run the worker + backend at BelowNormal priority.** Chrome +
  GeoComply own the placement moment; our Python is a good
  neighbor. On Windows: prefix launches with
  `start "" /belownormal python ...` (or `Start-Process -Priority
  BelowNormal ...` from PowerShell).
- **Register at logon.** The Beelink cold-boots occasionally; a
  scheduled task at logon like the relay's `PongbotRelayRestart`
  keeps us from silently missing wake-ups.

## One-time setup on the Beelink

Everything below runs in a CMD or PowerShell on the Beelink itself
(RDP or Tailscale SSH in).

1. Clone / copy the repo:
   ```
   git clone <repo-url> C:\sports-model-bettor
   cd C:\sports-model-bettor
   ```
2. Install Python deps:
   ```
   pip install -r backend\requirements.txt
   ```
3. Copy your local `data\` directory over (this is the DBs +
   calibration JSONs + model files). Total ~11 GB, of which:
   - `data\backups\` (3.3 GB) is historical DB snapshots — SKIP for
     the first migration; nothing at runtime reads them.
   - `data\live.db` (5.7 GB) is live pick history — REQUIRED for the
     tracker to keep continuity.
   - Everything else combined ~2 GB.

   Effective migration size: ~7.7 GB. From the dev box:
   ```
   robocopy E:\sports-model-bettor\data \\<beelink>\C$\sports-model-bettor\data ^
     /E /Z /R:2 /W:5 /XD backups /XF *.db-shm *.db-wal
   ```
   The `/XF *.db-shm *.db-wal` excludes SQLite's transient
   write-ahead-log files — they only matter for a live-connected DB
   and copying them can corrupt a snapshot. Take them from a running
   copy of the DB in read-only mode if you want a fully-frozen state.
4. Copy `arm.bat` over. On the Beelink use `HR_RELAY_URL=
   http://127.0.0.1:7478` (localhost — you're on the same box as
   the relay). Keep the token as-is.

## Running

Once daily manual: `arm.bat` (starts sync + backend + worker).

Better: register as a Windows service so it survives reboots. Two
options:

**Option A — NSSM (simplest)**

Grab NSSM (https://nssm.cc/), then:

```
nssm install SportsBackend "C:\Python311\python.exe" "-m uvicorn backend.server:app --host 0.0.0.0 --port 8000"
nssm set SportsBackend AppDirectory "C:\sports-model-bettor"
nssm set SportsBackend AppEnvironmentExtra "AUTO_BET_LIVE=1" "HR_RELAY_URL=http://127.0.0.1:7478" "HR_RELAY_TOKEN=<paste-token>"
nssm set SportsBackend AppPriority BELOW_NORMAL_PRIORITY_CLASS
nssm start SportsBackend

nssm install SportsWorker "C:\Python311\python.exe" "-m services.live_worker.main"
nssm set SportsWorker AppDirectory "C:\sports-model-bettor"
nssm set SportsWorker AppEnvironmentExtra "AUTO_BET_LIVE=1" "HR_RELAY_URL=http://127.0.0.1:7478" "HR_RELAY_TOKEN=<paste-token>"
nssm set SportsWorker AppPriority BELOW_NORMAL_PRIORITY_CLASS
nssm start SportsWorker
```

Both auto-restart on crash and auto-start on Beelink reboot at
BelowNormal priority.

**Option B — Windows Task Scheduler**

Schedule a "run at logon" task pointing at `arm.bat`. Less robust
(no auto-restart on crash) but no third-party install. In the task's
Action, wrap the launch with `cmd /c start "" /belownormal
arm.bat` to keep the priority hint.

## Point the frontend at Beelink

Back on your dev box:

1. In `frontend/`, create `.env.local`:
   ```
   VITE_API_TARGET=http://100.80.245.68:8000
   ```
   (Use the Beelink's Tailscale IP — replace the `.68` if yours differs.)
2. `npm run dev`
3. Frontend loads local, all `/api/*` calls proxy over Tailscale
   to the Beelink backend. No code changes.

To flip back to local backend: delete `.env.local` and restart the
dev server.

## Sync scripts (daily data refresh)

The `sync_*.bat` scripts need to run once a day to pull the day's
schedule + odds + settle picks. Two options:

- Register each as a Task Scheduler task at 5 AM ET (or whatever)
- Have `SportsWorker` service kick them off from inside the worker
  loop (already partially wired via the framework refresh cadence)

## What still runs on your dev box (frontend host)

- The React dev server (or a built `dist/` served by any static host)
- Nothing else. The dev box can sleep, be off, whatever — bets fire
  from the Beelink.

## Rollback

- Windows service: `nssm stop SportsBackend && nssm stop SportsWorker`
- Auto-placer only: hit `/api/bet-queue/live-fire/off` from anywhere
  on the tailnet, or delete `data/queue_placer/live_fire.flag` on the
  Beelink
- Env-var un-arm: `nssm set SportsBackend AppEnvironmentExtra ""`
  and restart the service — instant dry-run
- Full revert to dev box: stop the Beelink services, delete
  `frontend/.env.local` on dev, run `arm.bat` on dev → back to the
  pre-migration state (the Beelink still has your data copy for
  when you decide to re-flip).
