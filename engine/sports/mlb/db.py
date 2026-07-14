"""Re-export of ``engine.db`` — the MLB tracker DB (A6).

MLB's tracker DB lives at ``engine.db`` historically (predates the
per-sport split). Re-exporting it under engine.sports.mlb.db gives
new callers the consistent namespace.
"""
from __future__ import annotations

from ...db import *  # noqa: F401, F403
