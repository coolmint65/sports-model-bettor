"""
NHL Model Backtester.

Runs the NHL prediction model against historical games with:
- DB-backed games from nhl_games table (primary)
- On-the-fly ESPN API fetch when DB is empty (fallback)
- Per-category results (ML, O/U, PL all shown independently)
- Point-in-time rolling stats (no lookahead bias)
- Probability-based realistic odds with vig

Usage:
    python -m engine.nhl_backtest                    # Last 30 days (PIT mode)
    python -m engine.nhl_backtest --days 60           # Last 60 days
    python -m engine.nhl_backtest --season 2025       # Full season
    python -m engine.nhl_backtest --min-edge 3        # Only 3%+ edge bets
    python -m engine.nhl_backtest --no-pit            # Use live model (lookahead)
    python -m engine.nhl_backtest --thresholds        # Compare edge thresholds

Package layout (split 2026-04-28 from a 1240-line module):
    _helpers — constants + small math (Poisson, payout, win-pct accumulators)
    _io      — historical odds map + games-from-DB / games-from-ESPN
    _pit     — point-in-time predictor (the leak-free heart of the
               backtest), market baseline, B2B detection, and the
               live-model fallback
    _runner  — run_nhl_backtest, analyze_edge_thresholds, print_backtest

Public API kept identical so backend/server.py imports unchanged.
"""

from ._runner import run_nhl_backtest, analyze_edge_thresholds, print_backtest

__all__ = ["run_nhl_backtest", "analyze_edge_thresholds", "print_backtest"]
