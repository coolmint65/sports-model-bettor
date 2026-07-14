"""A6 Session 2 — physical MLB file move.

Moves 20 ``engine/mlb_*.py`` files into ``engine/sports/mlb/``,
translating imports for the deeper nesting and inter-MLB siblings.
Leaves a stub at each legacy path that re-exports from the new location.

``engine/db.py`` (the MLB tracker DB) intentionally stays put — many
non-MLB callers (events_backfill, sync scripts, calibration) hit it
directly. The shim ``engine/sports/mlb/db.py`` re-exports it.

After running, run the parity check::

    python scripts/a6_move_mlb_check.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEGACY_DIR = ROOT / "engine"
NEW_DIR = ROOT / "engine" / "sports" / "mlb"

FILES = {
    "mlb_predict": "predict",
    "mlb_factors": "factors",
    "mlb_scoring": "scoring",
    "mlb_team_composer": "team_composer",
    "mlb_player_logs": "player_logs",
    "mlb_player_mc": "player_mc",
    "mlb_prop_features": "prop_features",
    "mlb_prop_features_v2": "prop_features_v2",
    "mlb_prop_gbm": "prop_gbm",
    "mlb_prop_picks": "prop_picks",
    "mlb_props_train_all": "props_train_all",
    "mlb_first_inning_features": "first_inning_features",
    "mlb_first_inning_train": "first_inning_train",
    "mlb_pitcher_k_train": "pitcher_k_train",
    "mlb_derivative_picks": "derivative_picks",
    "mlb_deriv_retrobt": "deriv_retrobt",
    "mlb_retrobt": "retrobt",
    "mlb_diagnose": "diagnose",
    "mlb_game_diagnose": "game_diagnose",
    "mlb_autopsy": "autopsy",
}

_MLB_NEW_MARKER = "__A6MLB__"


def _translate_imports(src: str) -> str:
    # Phase 1: MLB siblings (relative)
    src = re.sub(
        r"from \.mlb_(\w+)( import|\b)",
        rf"from {_MLB_NEW_MARKER}.\1\2",
        src,
    )
    # Phase 1b: MLB siblings (absolute)
    src = re.sub(
        r"from engine\.mlb_(\w+)( import|\b)",
        r"from engine.sports.mlb.\1\2",
        src,
    )
    src = re.sub(
        r"\bimport engine\.mlb_(\w+)\b",
        r"import engine.sports.mlb.\1",
        src,
    )
    # Phase 2: relative-depth fix for non-MLB engine modules
    src = re.sub(
        r"from \.(\w+)( import|\b)",
        r"from ...\1\2",
        src,
    )
    # Restore MLB sibling marker
    src = src.replace(f"from {_MLB_NEW_MARKER}.", "from .")
    # Path traversal — moved file is 2 levels deeper
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
    return ("from engine.sports.mlb." in txt and "STUB" in txt.upper())


def _stub_text(legacy_stem: str, new_stem: str) -> str:
    return (
        f'"""Legacy path - moved to ``engine.sports.mlb.{new_stem}`` (A6 STUB).\n'
        f'\n'
        f'Old callers using ``engine.{legacy_stem}`` keep working via this\n'
        f're-export. Update your imports when convenient.\n'
        f'"""\n'
        f'from engine.sports.mlb.{new_stem} import *  # noqa: F401, F403\n'
        f'from engine.sports.mlb import {new_stem} as _mod  # noqa: F401\n'
        f'\n'
        f'# Re-export underscore-prefixed names that wildcard skips.\n'
        f'globals().update({{k: v for k, v in vars(_mod).items()\n'
        f'                   if k.startswith("_") and not k.startswith("__")}})\n'
    )


def move_file(legacy_stem: str, new_stem: str) -> str:
    legacy_path = LEGACY_DIR / f"{legacy_stem}.py"
    new_path = NEW_DIR / f"{new_stem}.py"
    if not legacy_path.exists():
        return f"SKIP {legacy_stem} - legacy missing"
    if _is_stub(legacy_path):
        return f"SKIP {legacy_stem} - already stub"
    src = legacy_path.read_text(encoding="utf-8")
    new_src = _translate_imports(src)
    NEW_DIR.mkdir(parents=True, exist_ok=True)
    new_path.write_text(new_src, encoding="utf-8")
    legacy_path.write_text(_stub_text(legacy_stem, new_stem), encoding="utf-8")
    return f"MOVED {legacy_stem} -> engine/sports/mlb/{new_stem}.py"


def main() -> int:
    print(f"Migrating MLB files: {LEGACY_DIR} -> {NEW_DIR}")
    for legacy_stem, new_stem in FILES.items():
        print("  " + move_file(legacy_stem, new_stem))
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
