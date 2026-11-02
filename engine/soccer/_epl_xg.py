"""engine/soccer/_epl_xg.py — EPL per-team xG attack/defense goal-rate model.

The legacy soccer predictor forms λ from a SINGLE Elo skill scalar per team
(results-only). That chases luck and can't tell "created 2.5 xG, lost to a
screamer" from "parked the bus, nicked a 1-0" — exactly the signal the PL model
was missing (Austin: soccer has never had a green day). This replaces the
Elo-tilt λ for the Premier League with a data-driven goal-rate model:

    λ_home = avg_home_xg · attack_home(H) · defense_away(A)
    λ_away = avg_away_xg · attack_away(A) · defense_home(H)

where each multiplier is league-average-normalized (1.0 = average), built from
SofaScore match xG with exponential time-decay + sample-size shrinkage
(scanners/epl-ratings-build.js on the Pi → data/epl/epl-xg-ratings.json).

Home advantage is already encoded (avg_home_xg > avg_away_xg + venue-split
ratings), so NO extra Elo home-adv is layered — matches predict_match's own
double-count guard.

Team join: the ratings are keyed by SofaScore team name; predict_match passes
ESPN team ids. We resolve the id → name via the league's own `teams` table and
normalize both sides (lowercase, &→and, strip fc/afc/wanderers). Verified 1:1
across all 25 PL teams in the 24/25–26/27 pool, zero collisions. We deliberately
do NOT strip city/united (would collide Man City / Man United)."""

from __future__ import annotations
import json
import re
import threading
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# project-root-relative, same convention as _db.get_conn
_RATINGS_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "epl" / "epl-ratings.json"

_lock = threading.Lock()
_cache: dict | None = None
_cache_mtime: float | None = None


def _norm(s: str) -> str:
    s = (s or "").lower().strip().replace("&", "and")
    toks = [t for t in re.split(r"[\s\.]+", s) if t and t not in ("fc", "afc", "wanderers")]
    return " ".join(toks)


def _load() -> dict | None:
    """Load + cache the ratings json, refreshing on file mtime change."""
    global _cache, _cache_mtime
    try:
        mt = _RATINGS_PATH.stat().st_mtime
    except OSError:
        return None
    with _lock:
        if _cache is not None and _cache_mtime == mt:
            return _cache
        try:
            raw = json.loads(_RATINGS_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            logger.warning("[epl-xg] ratings load failed: %s", e)
            return None
        by_norm = {}
        for _tid, t in (raw.get("teams") or {}).items():
            by_norm[_norm(t.get("name", ""))] = t
        raw["_by_norm"] = by_norm
        _cache, _cache_mtime = raw, mt
        logger.info("[epl-xg] loaded %d team ratings (%d matches, built %s)",
                    len(by_norm), raw.get("matches"), raw.get("built"))
        return raw


def _team_name(conn, team_id: int) -> str | None:
    try:
        row = conn.execute("SELECT name FROM teams WHERE id = ?", (int(team_id),)).fetchone()
    except Exception:
        return None
    if not row:
        return None
    # sqlite3.Row or tuple
    return row["name"] if hasattr(row, "keys") else row[0]


def epl_xg_lambdas(conn, home_team_id: int, away_team_id: int):
    """Return (lam_h, lam_a, meta) from the xG ratings, or None when either
    team has no rating (e.g. a just-promoted side not yet synced) — caller
    then keeps the Elo λ as a graceful fallback."""
    rat = _load()
    if not rat:
        return None
    idx = rat["_by_norm"]
    hn, an = _team_name(conn, home_team_id), _team_name(conn, away_team_id)
    H = idx.get(_norm(hn)) if hn else None
    A = idx.get(_norm(an)) if an else None
    if not H or not A:
        return None
    avg_h = float(rat.get("avg_home_xg") or 1.53)
    avg_a = float(rat.get("avg_away_xg") or 1.27)
    lam_h = avg_h * float(H["attack_home"]) * float(A["defense_away"])
    lam_a = avg_a * float(A["attack_away"]) * float(H["defense_home"])
    meta = {"home_rating": H, "away_rating": A, "avg_home_xg": avg_h, "avg_away_xg": avg_a}
    return (lam_h, lam_a, meta)
