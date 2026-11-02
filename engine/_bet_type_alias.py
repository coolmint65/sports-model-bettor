"""Canonical bet-type labels — ONE normalizer for every cell-keyed consumer.

Why (MintyBot 2026-10-25, Pick Authority walkthrough): the trackers have accumulated
two label families for the same markets — the legacy spaced/abbreviated family
("O/U", "RL", "ALT O/U", "F5 O/U", "Team Total", "1st INN") and the 2026-10-14/23
underscore family ("TOTAL", "RUN_LINE", "ALT_TOTAL", "F5_TOTAL", "TEAM_TOTAL",
"1ST_INN"). Anything that buckets by bet_type then splits ONE market across TWO cells:

  * engine.selection.edge_proven (the proven-edge gate) keyed MLB's proven
    "O/U over" (n=29, roi +16.9%) and "RL dog" (n=162, roi +8.5%) under the OLD
    labels while every new candidate arrived as "TOTAL"/"RUN_LINE" -> looked up a
    cell with n=2 -> unproven -> W=0.15 -> MLB's two proven markets were silently
    gated OFF for ~10 days.
  * engine.empirical_calibration bucketed the same way -> the new labels calibrated
    on near-empty cells (coarse fallback) instead of the hundreds of settled rows.

Canonical form is the UPPER_SNAKE underscore family (where new rows land). Callers
that prefer lower-case (the calibrator) lower() the result. Pure function, no I/O.
"""
from __future__ import annotations

# alias (already upper + spaces->underscores) -> canonical
_ALIASES = {
    "O/U": "TOTAL",
    "OU": "TOTAL",
    "ALT_O/U": "ALT_TOTAL",
    "ALT_OU": "ALT_TOTAL",
    "F5_O/U": "F5_TOTAL",
    "F5_OU": "F5_TOTAL",
    "RL": "RUN_LINE",
    "RUNLINE": "RUN_LINE",
    "ALT_RL": "ALT_RUN_LINE",
    "ALT_RUNLINE": "ALT_RUN_LINE",
    "F5_RL": "F5_RUN_LINE",
    "PL": "PUCK_LINE",
    "PUCKLINE": "PUCK_LINE",
    "ALT_PL": "ALT_PUCK_LINE",
    "SPRD": "SPREAD",
    "ALT_SPRD": "ALT_SPREAD",
    "1ST_INNING": "1ST_INN",
    "1STINN": "1ST_INN",
}


def canonical_bet_type(bt) -> str:
    """'ALT O/U' -> 'ALT_TOTAL', 'Team Total' -> 'TEAM_TOTAL', 'RL' -> 'RUN_LINE',
    'F5 Team Total' -> 'F5_TEAM_TOTAL', 'Q1_TOTAL' -> 'Q1_TOTAL'. Empty -> ''."""
    s = str(bt or "").strip().upper()
    if not s:
        return ""
    s = "_".join(s.split())          # collapse runs of whitespace into single underscores
    return _ALIASES.get(s, s)
