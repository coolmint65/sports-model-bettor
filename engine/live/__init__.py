"""
Live betting engine — Phase 3.

Locked 2026-04-28 (see `project_sports_model_roadmap.md` Phase 3 spec).

Sports + markets in scope:
    NBA  — live full-game ML / SPREAD / TOTAL, Q2 / Q3 / Q4, 1H, 2H
    NHL  — live full-game ML / PL / TOTAL, P2 / P3 (Period Total /
           Period BTS / Period Winner)

Out of scope for Phase 3: live player props (Phase 4), parlays, MLB live.

Cadence: 15s NBA, 30s NHL. Hard constraint: all lines settled + new
bets available within ~2-min NBA quarter intermission.

Architecture:
    _state    — ESPN scoreboard poller (cheap, ~2KB/poll, 30s-2min lag)
    _odds     — HR live-markets fetch (different GraphQL path than the
                prematch scrapers/hardrock_odds; live odds shape differs)
    _predict  — conditional predictor: current_state → remaining-game
                distribution. Per-quarter totals, half markets, live
                ML/spread via time-adjusted margin CDF, NHL period
                xG conditional on score state + time remaining
    _picks    — per-sport generate_live_picks; same edge math as
                pre-game pickers but conditioned on live state
    _store    — _LIVE_STORE in-memory cache keyed by (sport, game_id),
                fed by the separate worker process (services/live_worker)

Public API (will populate as 3a → 3d ship):
    get_live_picks(sport, game_id)   — returns the live pick list for a
                                       single in-progress game
    get_live_state(sport, game_id)   — debug accessor for current state

Pick lifecycle = snapshot. A pick locked at "Q3 Under 28.5 with NY
leading 50-44 at 8:42" is settled when Q3 ends — not mutated as the
quarter unfolds. The breadcrumb popover (pick_events) shows what the
model would have picked in between, but the tracker row is frozen.

Polling lives in services/live_worker/ (separate process), NOT in the
uvicorn app. The user's directive: "want this done right." Trade-off:
slightly more infra to manage; isolated from API server restarts.
"""

from ._picks import generate_live_picks
from ._predict import predict_live_nba_full
from ._store import get_state, list_active


def get_live_picks(sport: str, game_id: str) -> list[dict]:
    """Return live pick candidates for one in-progress game.

    Reads the latest game state (with HR live odds attached, if the
    worker fetched them) from the shared store, runs the live
    predictor, scores edges, and returns the picks that clear the
    edge floor + odds cap. Empty list when the game is not in the
    store, the state is stale (>5 min since last worker write), or
    the sport's live engine isn't implemented yet.
    """
    state = get_state(sport, game_id)
    if not state:
        return []
    return generate_live_picks(state)


def get_live_state(sport: str, game_id: str) -> dict | None:
    """Debug accessor — returns the raw state blob the worker last
    wrote, or None if the row is missing/stale. The /api/{sport}/
    live-state endpoint surfaces this directly."""
    return get_state(sport, game_id)


__all__ = [
    "get_live_picks",
    "get_live_state",
    "list_active",
    "generate_live_picks",
    "predict_live_nba_full",
]
