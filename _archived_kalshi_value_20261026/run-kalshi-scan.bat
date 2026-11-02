@echo off
cd /d C:\sports-model-bettor
".venv\Scripts\python.exe" -c "from engine.kalshi_value_scanner import scan, settle; print('scan', scan()); print('settle', settle())" >> data\logs\kalshi_value.log 2>&1
