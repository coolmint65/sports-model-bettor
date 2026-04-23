#!/usr/bin/env bash
# Linux port of sync_mlb.bat. Same cadence + side-effects; systemd
# timer (see scripts/systemd/) invokes this via sync.sh.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

if [ -f ".venv/bin/activate" ]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}"
export LOG_FILE=data/logs/sync_mlb.jsonl
export LOG_LEVEL=INFO
mkdir -p data/logs

CALIBRATE_ONLY=0
for arg in "$@"; do
    [ "${arg}" = "--calibrate-only" ] && CALIBRATE_ONLY=1
done

echo "============================================"
echo "  MLB Data Sync"
echo "============================================"

if [ "${CALIBRATE_ONLY}" -eq 0 ]; then
    # Auto-detect: if no final games with linescores OR no player stats,
    # run a full sync instead of the lighter daily path.
    first_run=0
    python - <<'PY' || first_run=1
from engine.db import get_conn
c = get_conn()
g = c.execute("SELECT COUNT(*) FROM games WHERE status='final' AND home_linescore IS NOT NULL").fetchone()[0]
p = c.execute("SELECT COUNT(*) FROM pitcher_stats").fetchone()[0]
import sys
sys.exit(0 if (g > 10 and p > 10) else 1)
PY
    if [ "${first_run}" -eq 1 ]; then
        echo "First run or missing data — running full MLB sync..."
        python scripts/run.py scrapers.mlb_stats --full
        echo ""
        echo "Running advanced stats..."
        python scripts/run.py scrapers.mlb_advanced || true
    else
        echo "Quick sync (teams, today's games, standings)..."
        python scripts/run.py scrapers.mlb_stats
        echo ""
        echo "Draining pending odds..."
        python -c "from engine.odds_history import drain_pending_mlb_odds; print(drain_pending_mlb_odds())" || true
    fi
fi

echo ""
echo "Refreshing umpire tendency table..."
python -c "from engine.umpire import update_umpire_stats; print(update_umpire_stats())" || true

echo ""
echo "Calibrating global model (adaptive window)..."
python scripts/run.py engine.calibration --auto

echo ""
echo "Calibrating per-team factors..."
python scripts/run.py engine.team_calibration

echo ""
echo "Checking for late-lineup deltas..."
python -c "from engine.lineup_refresh import refresh_for_date; import json; print(json.dumps(refresh_for_date(), indent=2, default=str))" || true

echo ""
echo "Recording today's picks..."
python scripts/run.py engine.tracker --record

echo ""
echo "Settling completed picks..."
python scripts/run.py engine.tracker --settle

echo ""
echo "Refreshing empirical calibration..."
python -c "from engine.empirical_calibration import refresh_calibration; print(refresh_calibration())" || true

echo ""
echo "Auto-applying train recommendations..."
python scripts/run.py engine.train mlb --apply || true

echo ""
echo "Updating POTD closing odds..."
python -c "from engine.pick_of_day import update_potd_closing_odds; print(update_potd_closing_odds('mlb'))" || true

echo ""
echo "Settling POTD..."
python -c "from engine.pick_of_day import settle_potd; print(settle_potd('mlb'))" || true

echo ""
echo "Auto-tuning ensemble weights..."
python scripts/run.py engine.ensemble_auto_tune mlb -v || true

echo ""
echo "Backing up DBs..."
python scripts/run.py scripts.backup_dbs

echo ""
echo "============================================"
echo "  MLB Sync Complete"
echo "============================================"
