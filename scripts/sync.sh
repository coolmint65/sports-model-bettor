#!/usr/bin/env bash
# Dispatch wrapper — ports of sync.bat for Linux / WSL.
# Usage:
#   ./scripts/sync.sh             → quick sync of all three sports
#   ./scripts/sync.sh --mlb       → MLB only
#   ./scripts/sync.sh --nhl       → NHL only
#   ./scripts/sync.sh --nba       → NBA only
#   ./scripts/sync.sh --full      → full MLB rebuild
#   ./scripts/sync.sh --history Y → load historical season
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

# Activate venv if present (expected at ./.venv on the Beelink).
if [ -f ".venv/bin/activate" ]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
fi

export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}"
mkdir -p data/logs

echo "============================================"
echo "  Sync dispatch — $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "============================================"

case "${1:-}" in
    --mlb)
        "${SCRIPT_DIR}/sync_mlb.sh"
        ;;
    --nhl)
        "${SCRIPT_DIR}/sync_nhl.sh" "${@:2}"
        ;;
    --nba)
        "${SCRIPT_DIR}/sync_nba.sh" "${@:2}"
        ;;
    --full)
        echo "-- FULL MLB rebuild --"
        python -m scrapers.mlb_stats --full
        python -m scrapers.mlb_advanced
        "${SCRIPT_DIR}/sync_mlb.sh" --calibrate-only
        ;;
    --history)
        echo "-- Loading ${2} season for backtesting --"
        python -m scrapers.mlb_stats --history "${2}"
        "${SCRIPT_DIR}/sync_mlb.sh" --calibrate-only
        ;;
    *)
        echo ""
        echo "── MLB ──"
        "${SCRIPT_DIR}/sync_mlb.sh"
        echo ""
        echo "── NHL ──"
        "${SCRIPT_DIR}/sync_nhl.sh"
        echo ""
        echo "── NBA ──"
        "${SCRIPT_DIR}/sync_nba.sh"
        ;;
esac

echo ""
echo "============================================"
echo "  Sync complete — $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "============================================"
