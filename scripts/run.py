"""
Entry-point shim that installs structured JSON logging before running a
module as ``python -m``.

The legacy entry points (engine.calibration, engine.tracker, scrapers.*)
each call ``logging.basicConfig(...)`` at import time, which makes it
hard to switch the whole batch of nightly tools to JSON output without
editing every file. Wrapping them via this shim lets the cron / Task
Scheduler invocation pick up ``LOG_FILE`` and ``LOG_LEVEL`` env vars
without touching the modules themselves.

Usage:
    python scripts/run.py <module.path> [args...]

Equivalent to ``python -m <module.path> [args...]`` except the root
logger is reconfigured to JSON format first (via
``scripts.structured_log.configure_from_env``).
"""

import runpy
import sys
from pathlib import Path

# Make sure the project root is importable when this script is invoked
# directly (``python scripts/run.py ...``).
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts.structured_log import configure_from_env  # noqa: E402


def main() -> int:
    if len(sys.argv) < 2:
        print(
            "Usage: python scripts/run.py <module.path> [args...]",
            file=sys.stderr,
        )
        return 2

    configure_from_env()

    module = sys.argv[1]
    # Shift argv so the wrapped module sees its own argv0.
    sys.argv = [module, *sys.argv[2:]]
    try:
        runpy.run_module(module, run_name="__main__", alter_sys=True)
    except SystemExit as e:
        return int(e.code or 0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
