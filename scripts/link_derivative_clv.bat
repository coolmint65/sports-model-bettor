@echo off
if not exist C:\sports-model-bettor\data\logs mkdir C:\sports-model-bettor\data\logs
C:\sports-model-bettor\.venv\Scripts\python.exe C:\sports-model-bettor\scripts\link_derivative_clv.py >> C:\sports-model-bettor\data\logs\link_derivative_clv.log 2>&1
