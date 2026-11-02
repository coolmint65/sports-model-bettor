@echo off
REM Odds warehouse backfill (resumable; see warehouse/backfill.py). Scheduled task WarehouseBackfill
REM re-launches it hourly; a lock file makes extra launches exit immediately.
cd /d C:\sports-model-bettor
if not exist data\warehouse\logs mkdir data\warehouse\logs
.venv\Scripts\python.exe -u -m warehouse.backfill run >> data\warehouse\logs\backfill.out 2>&1
