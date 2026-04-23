#!/usr/bin/env bash
# Linux port of sync_nhl.bat.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

if [ -f ".venv/bin/activate" ]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}"
export LOG_FILE=data/logs/sync_nhl.jsonl
export LOG_LEVEL=INFO
mkdir -p data/logs

echo "============================================"
echo "  NHL Data Sync"
echo "============================================"

case "${1:-}" in
    --full)
        echo "Running FULL NHL sync..."
        python -m scrapers.nhl_api --full
        ;;
    --history)
        echo "Loading NHL ${2} season..."
        python -m scrapers.nhl_api --history "${2}"
        ;;
    *)
        first_run=0
        python - <<'PY' || first_run=1
from engine.nhl_db import get_conn
c = get_conn()
r = c.execute("SELECT COUNT(*) FROM nhl_games").fetchone()[0]
import sys
sys.exit(0 if r > 50 else 1)
PY
        if [ "${first_run}" -eq 1 ]; then
            echo "First run detected — running full NHL sync..."
            python -m scrapers.nhl_api --full
        else
            echo "Quick NHL sync (today's games + team stats)..."
            python -m scrapers.nhl_api
        fi
        ;;
esac

echo ""
echo "Calibrating NHL model..."
python -m engine.nhl_calibration

echo ""
echo "Checking for late-goalie deltas..."
python -c "from engine.nhl_goalie_refresh import refresh_for_date; import json; print(json.dumps(refresh_for_date(), indent=2, default=str))" || true

echo ""
echo "Recording today's NHL picks..."
python -m engine.nhl_tracker --record

echo ""
echo "Settling completed NHL picks..."
python -m engine.nhl_tracker --settle

echo ""
echo "Refreshing empirical NHL calibration..."
python -c "from engine.empirical_calibration import refresh_calibration; print(refresh_calibration('nhl'))" || true

echo ""
echo "Updating POTD closing odds..."
python -c "from engine.pick_of_day import update_potd_closing_odds; print(update_potd_closing_odds('nhl'))" || true

echo ""
echo "Settling POTD..."
python -c "from engine.pick_of_day import settle_potd; print(settle_potd('nhl'))" || true

echo ""
echo "============================================"
echo "  NHL Sync Complete"
echo "============================================"
