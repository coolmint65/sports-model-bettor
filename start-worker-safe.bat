@echo off
REM ==============================================================
REM  start-worker-safe.bat  -  bring the PickMint live_worker up
REM  BETTING-OFF (keeps calibration refit + ingest + settlement
REM  alive; does NOT place any bets). Launched by the mb-worker-safe
REM  scheduled task at LOW priority so it yields CPU to the backend.
REM
REM  Betting kill-switch (main.py line 1641): live = AUTO_BET_LIVE=1
REM  AND an on-disk flag. Forcing AUTO_BET_LIVE=0 => genuinely off.
REM ==============================================================
cd /d C:\sports-model-bettor
set AUTO_BET_LIVE=0
set DRY_RUN=1
call run-worker.bat
