@echo off
REM Odds warehouse: Hard Rock spool -> wh.db quotes (warehouse/hr_ingest.py). Scheduled task WarehouseHrIngest, every 10 min.
REM Raw boards move to spool\hr_done and are pruned after 3 days. MintyBot 2026-11-02.
cd /d C:\sports-model-bettor
if not exist data\warehouse\logs mkdir data\warehouse\logs
start /belownormal /b /wait .venv\Scripts\python.exe -u -m warehouse.hr_ingest >> data\warehouse\logs\hr_ingest.out 2>&1
