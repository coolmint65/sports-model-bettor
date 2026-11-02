"""CFB FBS / FCS segmentation (MintyBot 2026-10-25, Austin PA CFB walkthrough).

HR prices FCS-vs-FCS games next to the FBS slate, and the two behave differently:
shadow FBS games ran -22% on 318 deduped games while FCS-only games ran +5% on 77
(headline lane: FBS -29% on 137, FCS-only +36% on 39). Blending them in one
proven-edge cell lets the bigger FBS sample decide the FCS games' verdict. So for
the Pick Authority gate a game where NEITHER team is FBS is scored under its own
league key, ``cfb_fcs``; everything else stays ``cfb``. Recording, settlement,
calibration and predict_match keep using the real league (``cfb``) -- only the
edge_proven lookups (market_weight / is_eligible) see the segment.

The FBS team list (data/football/cfb_fbs_teams.json) is static per season: refresh it
each August from the ESPN FBS (groups=80) scoreboards. The CFB rating model also uses
it for its new-team / season-regression priors (engine.football._elo).
"""
from __future__ import annotations

import json
import time
from pathlib import Path

_FBS_PATH = Path(__file__).resolve().parents[2] / "data" / "football" / "cfb_fbs_teams.json"
_FBS: frozenset | None = None
_FCS_GAMES = {"ts": 0.0, "n": -1, "ids": frozenset()}
_FCS_TTL = 300.0
FCS_LEAGUE = "cfb_fcs"


def fbs_team_ids() -> frozenset:
    global _FBS
    if _FBS is None:
        try:
            _FBS = frozenset(int(t) for t in json.loads(_FBS_PATH.read_text(encoding="utf-8"))["fbs_team_ids"])
        except Exception:
            _FBS = frozenset()
    return _FBS


def is_fcs_only(home_team_id, away_team_id) -> bool:
    """True when neither team is FBS. False when the FBS list is unavailable (no split)."""
    fbs = fbs_team_ids()
    if not fbs:
        return False
    try:
        return int(home_team_id) not in fbs and int(away_team_id) not in fbs
    except (TypeError, ValueError):
        return False


def edge_league(league: str, home_team_id, away_team_id) -> str:
    """The league key edge_proven should score this game under."""
    if league == "cfb" and is_fcs_only(home_team_id, away_team_id):
        return FCS_LEAGUE
    return league


def fcs_game_ids() -> frozenset:
    """ESPN game ids (str) of every CFB game with no FBS team -- edge_proven re-keys their
    unified rows to ``cfb_fcs``. Cached; refreshed when cfb.db's game count changes."""
    now = time.time()
    if now - _FCS_GAMES["ts"] < _FCS_TTL and _FCS_GAMES["n"] >= 0:
        return _FCS_GAMES["ids"]
    fbs = fbs_team_ids()
    if not fbs:
        return frozenset()
    try:
        from ._db import get_conn
        rows = get_conn("cfb").execute("SELECT game_id, home_team_id, away_team_id FROM games").fetchall()
        ids = frozenset(str(r[0]) for r in rows if int(r[1]) not in fbs and int(r[2]) not in fbs)
        _FCS_GAMES.update(ts=now, n=len(rows), ids=ids)
    except Exception:
        return _FCS_GAMES["ids"]
    return _FCS_GAMES["ids"]
