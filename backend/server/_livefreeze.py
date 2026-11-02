"""In-memory live/final paper-pick freeze (Austin 2026-09-29).

The framework boards (basketball / football / hockey) keep live AND final
games on the card and recompute the PAPER lean from the CURRENT odds on every
request, so the pick visibly CHANGES on refresh during a live game. The
kickoff-locked forward-test record can't rescue a game first seen live (it
refuses, by design, to record a pick after the game has started), so the
per-game `_lock_*_paper_pick` helpers find no recorded row and let the fresh,
drifting recompute leak through.

This freezes the DISPLAY: the first paper pick(s) shown once a game is no
longer scheduled are cached per (league, game_id) and served on every later
refresh, so the card is stable for the rest of the game. It is display-only —
it never writes to the is_shadow forward-test record, the tracker, the queue,
or any real (is_shadow=0) row. Pre-tip (scheduled) games are untouched, so
their picks still update with the line right up to puck-drop / tip-off.

In-memory by design: a backend restart re-establishes the freeze from the
then-current pick (a one-time re-anchor), which is acceptable and avoids
polluting any persistent store.
"""
from __future__ import annotations

# (league, game_id) -> {paper-key: [pick, ...]}
_FREEZE: dict = {}
_CAP = 4000  # bound memory; a full clear on overflow just re-anchors live games
_PAPER_KEYS = ("paper_picks", "paper_picks_q1", "paper_picks_p1")


def freeze_live_paper(d: dict, league: str) -> None:
    """Freeze a live/final game's paper pick(s) in ``d`` in place.

    No-op while the game is still scheduled — pre-tip picks stay fresh so they
    track the line. On the first non-scheduled sight the current paper pick(s)
    are snapshotted; every later call is overwritten with that snapshot.
    """
    status = str(d.get("status") or "scheduled").lower()
    if status in ("scheduled", "pre_game", "pregame", "", "final", "complete", "completed"):
        return
    gid = d.get("game_id")
    if not gid:
        return
    key = (str(league), str(gid))
    cached = _FREEZE.get(key)
    if cached is not None:
        for k in _PAPER_KEYS:
            if k in cached:
                d[k] = cached[k]
        return
    # First non-scheduled sight: snapshot whatever paper pick(s) are present
    # (these already reflect the recorded pre-tip pick if the lock helper
    # found one, else the fresh live pick — either way it's now frozen).
    snap = {k: d[k] for k in _PAPER_KEYS if d.get(k)}
    if not snap:
        return  # nothing to freeze yet; try again next refresh
    if len(_FREEZE) >= _CAP:
        _FREEZE.clear()
    _FREEZE[key] = snap


def freeze_size() -> int:
    """Number of games currently frozen (diagnostics)."""
    return len(_FREEZE)
