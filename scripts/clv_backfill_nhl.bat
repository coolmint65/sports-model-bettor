@echo off
cd /d C:\sports-model-bettor
.venv\Scripts\python.exe scripts\clv_backfill_nhl.py --apply >> data\logs\clv_backfill_nhl.log 2>&1
.venv\Scripts\python.exe scripts\clv_backfill_mlb.py --apply >> data\logs\clv_backfill_mlb.log 2>&1
