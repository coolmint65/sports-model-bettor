"""Auto-placement config — every safety bound the user authorized.

Values are constants. Overrides go through env vars only (never a UI
toggle) so a stray click can't raise the cap.

Kill switch:
  AUTO_BET_LIVE=1    Required to fire real bets. Without it, everything
                     runs in dry-run mode (logs would-place decisions
                     but never touches the relay).

Relay client:
  HR_RELAY_URL       e.g. http://100.x.x.x:7478
  HR_RELAY_SECRET    optional shared secret sent as X-Relay-Secret

All monetary values are dollars. The queue's tracker uses `unit` as its
native denomination, but the placer speaks in $ because that's what
HR takes.
"""
from __future__ import annotations
import os
from pathlib import Path


# ── Unit sizing ─────────────────────────────────────────────
UNIT_DOLLARS = 1.0

# ── Stake caps ──────────────────────────────────────────────
# These are OUR OWN envelope — the sports app's per-day / per-week
# budget. Independent of what the Table Tennis project stakes on the
# shared HR account. Never subtract another app's stake from this
# cap — that would let a chatty TT session starve us to zero.
MAX_STAKE_PER_BET_DOLLARS = 1.0      # 1u
MAX_STAKED_PER_DAY_DOLLARS = 10.0    # 10u
MAX_STAKED_PER_WEEK_DOLLARS = 50.0   # ~5 days worth at the daily cap

# Optional shared-account ceiling. Only enforced when set to a positive
# number. Meant for the case where sports-app + TT combined stake could
# drain the shared HR balance — think total-account safety, not our
# own budget. Default is `None` (no shared ceiling) because at flat $1
# units contention is impossible.
TOTAL_ACCOUNT_DAILY_CEILING_DOLLARS: float | None = None

# ── Circuit breakers ────────────────────────────────────────
CONSECUTIVE_LOSS_HALT = 4
DAILY_DRAWDOWN_HALT_DOLLARS = -3.0

# ── Line-drift tolerance ────────────────────────────────────
# When HR's live line differs from the queue's captured line, accept
# the placement as long as the implied-probability delta is within
# this tolerance. Anything beyond bails ("line moved"). At 3pp: ~15¢
# at pick'em, ~20¢ at ±150, ~25¢ at ±200. Catches modest steam without
# chasing genuine line moves.
LINE_DRIFT_TOLERANCE_PP = 3.0

# ── Kill switch ─────────────────────────────────────────────
# On-disk flag PLUS env var — either being off blocks placement.
# The on-disk flag lets a runtime toggle (via the /live-fire/off
# endpoint) survive a restart without needing shell env access.
_KILL_FLAG_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "queue_placer" / "live_fire.flag"
)


def kill_flag_path() -> Path:
    return _KILL_FLAG_PATH


def is_live_fire_enabled() -> bool:
    env_on = os.environ.get("AUTO_BET_LIVE") == "1"
    flag_on = _KILL_FLAG_PATH.exists()
    return env_on and flag_on


def set_live_fire(on: bool) -> None:
    """Toggle the on-disk flag. Requires the env var separately to
    actually fire — two-key launch."""
    _KILL_FLAG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if on:
        _KILL_FLAG_PATH.write_text("on\n")
    elif _KILL_FLAG_PATH.exists():
        _KILL_FLAG_PATH.unlink()


# ── Relay client ────────────────────────────────────────────
# We're a co-tenant on the same Beelink relay that PiBot (Table Tennis
# Claude project) uses. Same HR session, same account, same bankroll.
# PiBot owns session management: keepalive, pre-bet relogin, and
# auto-login/2FA. We must NEVER run our own keepalive or relogin loop
# against this relay — doing so would fight PiBot's session and cause
# mid-bet drift. Thin placement client: POST /place, read
# /session-pull if we need the token, that's it.
def relay_url() -> str | None:
    # Default: PiBot's Beelink over Tailscale (unset for legacy
    # HR_RELAY_URL is respected first for override).
    return os.environ.get("HR_RELAY_URL") or None


def relay_token() -> str | None:
    """X-Relay-Token header value. Supports HR_RELAY_TOKEN (matches
    PiBot's convention) with HR_RELAY_SECRET as a legacy alias."""
    return (os.environ.get("HR_RELAY_TOKEN")
            or os.environ.get("HR_RELAY_SECRET")
            or None)


# Kept as an alias for the previously-shipped API surface. New callers
# should use relay_token().
relay_secret = relay_token


# ── Placer cadence ──────────────────────────────────────────
# How often the worker sweeps the queue for placeable picks. The queue
# itself refreshes every 2 min, so anything faster wastes cycles.
PLACER_INTERVAL_S = 60

# How long before game start we stop attempting to place. HR pulls the
# prematch market at kickoff and the placer would just retry until
# retry budget is exhausted — cheaper to skip.
PLACE_LATEST_MINUTES_BEFORE_START = 2
