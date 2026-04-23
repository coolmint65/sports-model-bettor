#!/usr/bin/env bash
# Linux port of sync_nba.bat.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

if [ -f ".venv/bin/activate" ]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}"
export LOG_FILE=data/logs/sync_nba.jsonl
export LOG_LEVEL=INFO
mkdir -p data/logs

echo "============================================"
echo "  NBA Data Sync"
echo "============================================"

if [ "${1:-}" = "--full" ]; then
    echo "Running FULL NBA sync..."
    python -m scrapers.nba_espn --full
else
    first_run=0
    python - <<'PY' || first_run=1
import engine.nba_db as db
c = db.get_conn()
n = c.execute("SELECT COUNT(*) FROM nba_games").fetchone()[0]
import sys
sys.exit(0 if n > 50 else 1)
PY
    if [ "${first_run}" -eq 1 ]; then
        echo "First run detected — running full NBA sync..."
        python -m scrapers.nba_espn --full
    else
        echo "Quick NBA sync (today's games)..."
        python -m scrapers.nba_espn
    fi
fi

echo ""
echo "Calibrating NBA Q1 model..."
python -m engine.nba_calibration

echo ""
echo "Checking for late-injury deltas..."
python -c "from engine.nba_injury_refresh import refresh_for_date; import json; print(json.dumps(refresh_for_date(), indent=2, default=str))" || true

echo ""
echo "Recording today's NBA picks..."
python -m engine.nba_tracker --record

echo ""
echo "Settling completed NBA picks..."
python -m engine.nba_tracker --settle

echo ""
echo "Refreshing empirical NBA calibration..."
python -c "from engine.empirical_calibration import refresh_calibration; print(refresh_calibration('nba'))" || true

echo ""
echo "Updating POTD closing odds..."
python -c "from engine.pick_of_day import update_potd_closing_odds; print(update_potd_closing_odds('nba'))" || true

echo ""
echo "Settling POTD..."
python -c "from engine.pick_of_day import settle_potd; print(settle_potd('nba'))" || true

echo ""
echo "============================================"
echo "  NBA Sync Complete"
echo "============================================"
