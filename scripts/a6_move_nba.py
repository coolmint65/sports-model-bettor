"""A6 Session 3 — physical NBA file move.

Same pattern as scripts/a6_move_nhl.py and a6_move_mlb.py.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEGACY_DIR = ROOT / "engine"
NEW_DIR = ROOT / "engine" / "sports" / "nba"

FILES = {
    "nba_predict": "predict",
    "nba_q1_predict": "q1_predict",
    "nba_db": "db",
    "nba_picks": "picks",
    "nba_calibration": "calibration",
    "nba_injuries": "injuries",
    "nba_injury_refresh": "injury_refresh",
    "nba_minutes_projector": "minutes_projector",
    "nba_player_logs": "player_logs",
    "nba_player_mc": "player_mc",
    "nba_prop_features": "prop_features",
    "nba_prop_gbm": "prop_gbm",
    "nba_prop_picks": "prop_picks",
    "nba_q1_composer": "q1_composer",
    "nba_derivative_picks": "derivative_picks",
    "nba_deriv_retrobt": "deriv_retrobt",
    "nba_full_backtest": "full_backtest",
    "nba_diagnose": "diagnose",
    # nba_nhl_props_train is misnamed (handles NBA props) — keep the
    # name unchanged this session; rename can land separately so its
    # caller updates aren't tangled with the move.
    "nba_nhl_props_train": "nhl_props_train",
}

_NBA_NEW_MARKER = "__A6NBA__"


def _translate_imports(src: str) -> str:
    src = re.sub(
        r"from \.nba_(\w+)( import|\b)",
        rf"from {_NBA_NEW_MARKER}.\1\2",
        src,
    )
    src = re.sub(
        r"from engine\.nba_(\w+)( import|\b)",
        r"from engine.sports.nba.\1\2",
        src,
    )
    src = re.sub(
        r"\bimport engine\.nba_(\w+)\b",
        r"import engine.sports.nba.\1",
        src,
    )
    src = re.sub(
        r"from \.(\w+)( import|\b)",
        r"from ...\1\2",
        src,
    )
    src = src.replace(f"from {_NBA_NEW_MARKER}.", "from .")
    src = src.replace(
        "Path(__file__).resolve().parent.parent",
        "Path(__file__).resolve().parents[3]",
    )
    src = src.replace(
        "Path(__file__).parent.parent",
        "Path(__file__).resolve().parents[3]",
    )
    return src


def _is_stub(path: Path) -> bool:
    if not path.exists():
        return False
    txt = path.read_text(encoding="utf-8")
    return ("from engine.sports.nba." in txt and "STUB" in txt.upper())


def _stub_text(legacy_stem: str, new_stem: str) -> str:
    return (
        f'"""Legacy path - moved to ``engine.sports.nba.{new_stem}`` (A6 STUB)."""\n'
        f'from engine.sports.nba.{new_stem} import *  # noqa: F401, F403\n'
        f'from engine.sports.nba import {new_stem} as _mod  # noqa: F401\n'
        f'\n'
        f'globals().update({{k: v for k, v in vars(_mod).items()\n'
        f'                   if k.startswith("_") and not k.startswith("__")}})\n'
    )


def move_file(legacy_stem: str, new_stem: str) -> str:
    legacy_path = LEGACY_DIR / f"{legacy_stem}.py"
    new_path = NEW_DIR / f"{new_stem}.py"
    if not legacy_path.exists():
        return f"SKIP {legacy_stem} - missing"
    if _is_stub(legacy_path):
        return f"SKIP {legacy_stem} - already stub"
    src = legacy_path.read_text(encoding="utf-8")
    new_src = _translate_imports(src)
    NEW_DIR.mkdir(parents=True, exist_ok=True)
    new_path.write_text(new_src, encoding="utf-8")
    legacy_path.write_text(_stub_text(legacy_stem, new_stem), encoding="utf-8")
    return f"MOVED {legacy_stem} -> engine/sports/nba/{new_stem}.py"


def main() -> int:
    print(f"Migrating NBA files: {LEGACY_DIR} -> {NEW_DIR}")
    for legacy_stem, new_stem in FILES.items():
        print("  " + move_file(legacy_stem, new_stem))
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
