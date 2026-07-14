@echo off
cd /d "%~dp0"
echo ============================================
echo   Tennis Data Sync
echo ============================================
echo.

if not exist "data\logs" mkdir "data\logs"
set LOG_FILE=data\logs\sync_tennis.jsonl
set LOG_LEVEL=INFO
set PYTHONPATH=%~dp0;%PYTHONPATH%

REM Auto-detect: if no Sackmann data ingested, do full historical pull
python -c "from engine.tennis_db import get_conn, ensure_tables; ensure_tables(); c = get_conn(); n = c.execute(\"SELECT COUNT(*) FROM tennis_matches\").fetchone()[0]; exit(0 if n > 1000 else 1)" 2>nul
if errorlevel 1 (
    echo First run - full Sackmann historical ingest ^(several minutes^)...
    python -m scrapers.tennis_sackmann --full
    echo.
    echo Training surface Elo...
    python -m engine.tennis_elo
    echo.
    goto :slate
)

echo Quick Sackmann sync ^(current year only, idempotent^)...
python -m scrapers.tennis_sackmann

:slate
echo.
echo Pulling today + tomorrow's draw from ESPN...
REM --days 2 ingests both today and tomorrow so the Tennis tab shows
REM the upcoming slate before midnight ET (without it the panel went
REM empty as soon as today's matches finished).
REM HR-supplement DROPPED 2026-05-02. HR's tennis market feed
REM publishes bracket-projection matchups that frequently never play
REM (Bolt vs Walton 2026-05-01: neither faced the other that day).
REM We accepted the tradeoff: lose Challenger draw preview ahead of
REM TE catching it, gain phantom-matchup elimination at root.
python -m engine.tennis_schedule --days 2

echo.
echo Pulling Tennis Explorer results (sub-tour result source)...
REM ESPN doesn't carry sub-tour events (Challenger / ITF / WTA125), and
REM Sackmann's CSVs ship annually — that left HR-supplement picks
REM unsettleable AND the post-2024 sub-tour corpus empty. Tennis
REM Explorer publishes daily result tables for every tour and the
REM parser feeds two consumers: the settle-path fallback resolver
REM and the corpus backfill (next step) for current-season Elo.
REM Default --date is "yesterday"; lookback 4 covers timezone slip
REM + missed sync days.
REM Lookback was 4 — most missed-sync slips are 1-2 days; cap at 2
REM to halve the network walk time without sacrificing settle coverage.
python -m scrapers.tennis_results --lookback 2 2>nul

echo.
echo Backfilling ESPN + Tennis Explorer rows into the historical corpus...
REM Sackmann ships annually so any 2025+ result has to come from
REM somewhere else. Two pipelines feed the same tennis_matches table:
REM   1. ESPN scheduled rows with status='post' — main tour
REM   2. Tennis Explorer rows — sub-tour gap (Challenger/ITF/WTA125)
REM Both are idempotent on (tour, match_id). Order matters: this runs
REM BEFORE Elo training so the trainer sees current-season rows.
python -m engine.tennis_corpus_backfill 2>nul

echo.
echo Re-training surface Elo on fresh data...
REM Moved AFTER corpus backfill so the trainer sees current-season
REM ESPN + TE rows, not just the annual Sackmann snapshot. Cold-start
REM ratings on Challenger/ITF players improve significantly with this
REM ordering — they pick up post-2024 match history that Sackmann
REM doesn't have yet.
python -m engine.tennis_elo

echo.
echo Re-training tennis GBMs with 2-year time-decay weights...
REM Retrain ATP + WTA GBMs after the corpus backfill so the models
REM see current-season data with recency weighting (half-life=730d).
REM Counters the career-long surface-specialization bias the Elo
REM shrinkage corrects — the GBM was trained on the same corpus and
REM had the same Zverev-on-clay overconfidence problem before this.
REM Each tour takes ~2-3s on a modern CPU.
python -m engine.gbm.train tennis_atp 2>nul
python -m engine.gbm.train tennis_wta 2>nul

echo.
echo Settling completed matches against tennis_matches + TE results...
REM Settle reads tennis_scheduled_matches first, falls back to
REM tennis_matches, then to tennis_match_results — so the 2-3 above
REM (TE scrape + corpus backfill) all feed the resolver chain.
python -c "from engine.tennis_tracker import settle_tennis_picks; print(settle_tennis_picks())"

echo.
echo Refreshing empirical calibration table for tennis...
python -c "from engine.empirical_calibration import refresh_calibration; print(refresh_calibration('tennis'))" 2>nul

echo.
echo Backfilling tennis player photos for any new names on the slate...
REM Cache-only resolver — only fires for players whose name isn't
REM already in tennis_player_photos. ESPN search is rate-friendly
REM (~5 req/sec) and the slate is bounded so this caps at ~100s.
python -m engine.tennis_player_photos --backfill

echo.
echo Capturing opening odds for today's HR matches (LineMovedChip)...
REM First-seen-of-day per (match, date). Idempotent — re-running
REM doesn't overwrite morning capture. Reads HR through the shared
REM cache so it doesn't pile on rate limits.
python -m engine.tennis_line_movement --capture

echo.
echo ============================================
echo   Tennis Sync Complete    [%DATE% %TIME%]
echo ============================================
if /i not "%SESSIONNAME%"=="" if /i not "%SESSIONNAME%"=="Services" if "%~1"=="" pause
