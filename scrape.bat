@echo off
echo ============================================
echo   Sports Data Scraper
echo ============================================
echo.

cd /d %~dp0

if "%~1"=="" (
    echo Updating ALL leagues...
    echo This may take 15-20 minutes.
    echo.
    python -m scrapers.run_all
) else (
    echo Updating: %*
    echo.
    python -m scrapers.run_all %*
)

echo.
echo ============================================
echo   Scraping complete! Data saved to data/teams/
echo ============================================
echo.
pause
