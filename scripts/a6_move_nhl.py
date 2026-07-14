"""A6 Session 1 — physical NHL file move.

Moves 15 ``engine/nhl_*.py`` files into ``engine/sports/nhl/``,
translating imports for the deeper nesting and inter-NHL siblings.
Leaves a stub at each legacy path that re-exports from the new
location, so external callers (~128 sites) keep working unchanged.

After running this, run the parity check::

    python scripts/a6_move_nhl_check.py

Idempotent: skips files already moved (legacy is already a stub).
"""
from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEGACY_DIR = ROOT / "engine"
NEW_DIR = ROOT / "engine" / "sports" / "nhl"

# Map legacy filename stem -> new filename stem (drops the "nhl_" prefix).
# nhl_predict is a package (directory) — handled separately.
FILES = {
    "nhl_db": "db",
    "nhl_calibration": "calibration",
    "nhl_picks": "picks",
    "nhl_explain": "explain",
    "nhl_goalie_refresh": "goalie_refresh",
    "nhl_granular": "granular",
    "nhl_player_logs": "player_logs",
    "nhl_player_mc": "player_mc",
    "nhl_prop_features": "prop_features",
    "nhl_prop_gbm": "prop_gbm",
    "nhl_prop_picks": "prop_picks",
    "nhl_derivative_picks": "derivative_picks",
    "nhl_deriv_retrobt": "deriv_retrobt",
    "nhl_retrobt": "retrobt",
    "nhl_diagnose": "diagnose",
}

# nhl_predict is a package — moves wholesale.
PACKAGE_NAME = "nhl_predict"
PACKAGE_NEW = "predict"

_NHL_NEW_MARKER = "__A6NHL__"


def _translate_imports(src: str) -> str:
    """Rewrite imports in a file moving from engine/ to engine/sports/nhl/.

    Two transforms, applied in order to avoid double-translation:

    1. NHL siblings: ``from .nhl_X import`` -> ``from .X import``
       (and absolute ``from engine.nhl_X`` -> ``from engine.sports.nhl.X``).
       Marker pass keeps these out of the relative-depth pass below.
    2. Relative engine imports: ``from .X import`` -> ``from ...X import``
       (the moved file is now 2 packages deeper).

    Path traversals using ``Path(__file__).resolve().parent.parent`` to
    reach the project root get +2 ``.parent`` calls.
    """
    # Phase 1a: NHL siblings (relative)
    src = re.sub(
        r"from \.nhl_(\w+)( import|\b)",
        rf"from {_NHL_NEW_MARKER}.\1\2",
        src,
    )
    # Phase 1b: NHL siblings (absolute) — both engine.nhl_X and engine.nhl_predict
    src = re.sub(
        r"from engine\.nhl_(\w+)( import|\b)",
        r"from engine.sports.nhl.\1\2",
        src,
    )
    src = re.sub(
        r"\bimport engine\.nhl_(\w+)\b",
        r"import engine.sports.nhl.\1",
        src,
    )
    # Phase 2: relative-depth fix for non-NHL engine modules
    # `from .X import` -> `from ...X import` (X is anything except NHL siblings,
    # which we marked above and will restore in phase 3)
    src = re.sub(
        r"from \.(\w+)( import|\b)",
        r"from ...\1\2",
        src,
    )
    # Restore NHL sibling marker -> relative import inside the new package
    src = src.replace(f"from {_NHL_NEW_MARKER}.", "from .")
    # Path traversal — add two more `.parent` calls
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
    """A stub is a file we previously left behind that just re-exports."""
    if not path.exists():
        return False
    txt = path.read_text(encoding="utf-8")
    return ("from engine.sports.nhl." in txt
              and "STUB" in txt.upper())


def _stub_text(legacy_stem: str, new_stem: str) -> str:
    """Generate the legacy-path stub. Wildcard imports cover public
    names; private names (underscore) get explicit re-exports per file."""
    return (
        f'"""Legacy path — moved to ``engine.sports.nhl.{new_stem}`` (A6 STUB).\n'
        f'\n'
        f'Old callers using ``engine.{legacy_stem}`` keep working via this\n'
        f're-export. Update your imports when convenient — the legacy path\n'
        f'will be removed once no in-tree callers reference it.\n'
        f'"""\n'
        f'from engine.sports.nhl.{new_stem} import *  # noqa: F401, F403\n'
        f'from engine.sports.nhl import {new_stem} as _mod  # noqa: F401\n'
        f'\n'
        f'# Explicit private re-exports (wildcard import skips _-prefixed names).\n'
        f'# Add specific names below if external callers reference them.\n'
        f'globals().update({{k: v for k, v in vars(_mod).items()\n'
        f'                   if k.startswith("_") and not k.startswith("__")}})\n'
    )


def move_file(legacy_stem: str, new_stem: str) -> str:
    legacy_path = LEGACY_DIR / f"{legacy_stem}.py"
    new_path = NEW_DIR / f"{new_stem}.py"
    if not legacy_path.exists():
        return f"SKIP {legacy_stem} — legacy path missing"
    if _is_stub(legacy_path):
        return f"SKIP {legacy_stem} — already a stub"
    src = legacy_path.read_text(encoding="utf-8")
    new_src = _translate_imports(src)
    NEW_DIR.mkdir(parents=True, exist_ok=True)
    new_path.write_text(new_src, encoding="utf-8")
    legacy_path.write_text(_stub_text(legacy_stem, new_stem), encoding="utf-8")
    return f"MOVED {legacy_stem} -> engine/sports/nhl/{new_stem}.py"


def move_package() -> str:
    """nhl_predict is a directory package — move it wholesale."""
    legacy_dir = LEGACY_DIR / PACKAGE_NAME
    new_dir = NEW_DIR / PACKAGE_NEW
    if not legacy_dir.exists() or not legacy_dir.is_dir():
        return f"SKIP {PACKAGE_NAME} — legacy package missing"
    legacy_init = legacy_dir / "__init__.py"
    if not legacy_init.exists():
        return f"SKIP {PACKAGE_NAME} — no __init__.py"
    if _is_stub(legacy_init):
        return f"SKIP {PACKAGE_NAME} — already a stub"
    # An earlier namespace-shim pass left engine/sports/nhl/predict.py as
    # a single file; remove it before copying the package directory in.
    new_file = NEW_DIR / f"{PACKAGE_NEW}.py"
    if new_file.exists():
        new_file.unlink()
    # Copy directory contents
    if new_dir.exists():
        shutil.rmtree(new_dir)
    shutil.copytree(legacy_dir, new_dir)
    # Translate imports in every .py file under the moved package
    for py in new_dir.rglob("*.py"):
        py.write_text(_translate_imports(py.read_text(encoding="utf-8")),
                       encoding="utf-8")
    # Replace legacy package with a stub package (single __init__.py
    # re-exporting from the new location). Keep the directory so that
    # `import engine.nhl_predict` still resolves; remove other files.
    for child in legacy_dir.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()
    legacy_init.write_text(_stub_text(PACKAGE_NAME, PACKAGE_NEW),
                            encoding="utf-8")
    return f"MOVED {PACKAGE_NAME}/ -> engine/sports/nhl/{PACKAGE_NEW}/"


def main() -> int:
    print(f"Migrating NHL files from {LEGACY_DIR} -> {NEW_DIR}")
    # Move single-file modules first (no inter-NHL deps among the leaf
    # files most of them are; nhl_calibration imports nhl_db, etc, but
    # the imports get translated either way).
    for legacy_stem, new_stem in FILES.items():
        print("  " + move_file(legacy_stem, new_stem))
    # Move the predict package
    print("  " + move_package())
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
