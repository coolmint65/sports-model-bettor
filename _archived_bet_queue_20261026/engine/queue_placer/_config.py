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
UNIT_DOLLARS = 5.0

# ── Stake caps ──────────────────────────────────────────────
# These are OUR OWN envelope — the sports app's per-day / per-week
# budget. Independent of what the Table Tennis project stakes on the
# shared HR account. Never subtract another app's stake from this
# cap — that would let a chatty TT session starve us to zero.
MAX_STAKE_PER_BET_DOLLARS = 10.0      # 1u
MAX_STAKED_PER_DAY_DOLLARS = 1000000.0    # 10u

# Optional shared-account ceiling. Only enforced when set to a positive
# number. Meant for the case where sports-app + TT combined stake could
# drain the shared HR balance — think total-account safety, not our
# own budget. Default is `None` (no shared ceiling) because at flat $1
# units contention is impossible.
TOTAL_ACCOUNT_DAILY_CEILING_DOLLARS: float | None = None

# ── Circuit breakers ────────────────────────────────────────
CONSECUTIVE_LOSS_HALT = 1000000
DAILY_DRAWDOWN_HALT_DOLLARS = -1000000.0

# Per-book cap sets. Each book (hr, dk) tracks its OWN daily/weekly staked
# total, drawdown, and loss streak INDEPENDENTLY -- an HR bet never counts
# against DK's caps and vice-versa. Values default to the module constants
# above; edit a single book here to give it a different envelope. Units were
# bumped to $2 and caps made per-book on 2026-07-16 as DK came online.
BOOK_CAPS: dict = {
    "hr": {
        "max_stake_per_bet": MAX_STAKE_PER_BET_DOLLARS,
        "max_staked_per_day": MAX_STAKED_PER_DAY_DOLLARS,
        "daily_drawdown_halt": DAILY_DRAWDOWN_HALT_DOLLARS,
        "consecutive_loss_halt": CONSECUTIVE_LOSS_HALT,
    },
    "dk": {
        "max_stake_per_bet": MAX_STAKE_PER_BET_DOLLARS,
        "max_staked_per_day": MAX_STAKED_PER_DAY_DOLLARS,
        "daily_drawdown_halt": DAILY_DRAWDOWN_HALT_DOLLARS,
        "consecutive_loss_halt": CONSECUTIVE_LOSS_HALT,
    },
    "zotabet": {
        "max_stake_per_bet": MAX_STAKE_PER_BET_DOLLARS,
        "max_staked_per_day": MAX_STAKED_PER_DAY_DOLLARS,
        "daily_drawdown_halt": DAILY_DRAWDOWN_HALT_DOLLARS,
        "consecutive_loss_halt": CONSECUTIVE_LOSS_HALT,
    },
    "betandplay": {
        "max_stake_per_bet": MAX_STAKE_PER_BET_DOLLARS,
        "max_staked_per_day": MAX_STAKED_PER_DAY_DOLLARS,
        "daily_drawdown_halt": DAILY_DRAWDOWN_HALT_DOLLARS,
        "consecutive_loss_halt": CONSECUTIVE_LOSS_HALT,
    },
    "kalshi": {
        "max_stake_per_bet": MAX_STAKE_PER_BET_DOLLARS,
        "max_staked_per_day": MAX_STAKED_PER_DAY_DOLLARS,
        "daily_drawdown_halt": DAILY_DRAWDOWN_HALT_DOLLARS,
        "consecutive_loss_halt": CONSECUTIVE_LOSS_HALT,
    },
}

_ALL_BOOKS = tuple(BOOK_CAPS.keys())

# Hard authorized per-bet ceiling. The balance ladder (2x unit) may raise
# the per-bet stake with bankroll, but NEVER above this without an explicit
# config change here -- a balance-read glitch can't float the cap away
# (audit T1). Current ladder tops out at $5 unit -> $10/bet; raise this
# deliberately when scaling caps (see hr_1k_deposit plan).
MAX_STAKE_PER_BET_ABS = 10.0
# Per-book overrides to the abs ceiling. Kalshi sizes at a flat 10% of its
# own bankroll (Austin 2026-08-10 msg 24781) — _sizing.py computes the stake;
# this is only the GLITCH GUARD (a bad balance read can't float the stake
# away). $100 is non-binding under a ~$1000 Kalshi bankroll (10% = $100) and
# hard-bounds anything above. HR/DK stay hard-capped at $10 (flat-$10 policy).
MAX_STAKE_PER_BET_ABS_BY_BOOK = {"kalshi": 100.0}


def caps_for(book) -> dict:
    """Per-book cap set. Unknown / None book falls back to hr's envelope.
    max_stake_per_bet scales with the balance-laddered unit (2x tier unit)."""
    caps = dict(BOOK_CAPS.get((book or "hr").lower(), BOOK_CAPS["hr"]))
    try:
        from . import _sizing
        abs_cap = MAX_STAKE_PER_BET_ABS_BY_BOOK.get(
            (book or "").lower(), MAX_STAKE_PER_BET_ABS)
        caps["max_stake_per_bet"] = min(
            abs_cap,
            round(2.0 * _sizing.unit_for_book(book), 2),
        )
    except Exception:
        pass
    return caps

# ── Line-drift tolerance ────────────────────────────────────
# When HR's live line differs from the queue's captured line, accept
# the placement as long as the implied-probability delta is within
# this tolerance. Anything beyond bails ("line moved"). At 3pp: ~15¢
# at pick'em, ~20¢ at ±150, ~25¢ at ±200. Catches modest steam without
# chasing genuine line moves.
LINE_DRIFT_TOLERANCE_PP = 999.0

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


# --- DK placement service (Pi): a SECOND book alongside HR -------------------
# DK is a SECOND book alongside HR. Resolution + placement live on the Pi
# (dk-place-server.js -> dk.sh -> KY-box keyboard). We POST the pick; the
# Pi owns the DK board + placement. Placed only when the shared live-fire
# switch is on AND DK is explicitly enabled, so it toggles independent of HR.

# Max DK placements attempted per sweep - bounds the first post-enable sweep
# so it can't drain a whole backlog in one blocking burst (DK's keyboard
# placement is slower than HR's API).
MAX_DK_PER_SWEEP = 4

_DK_ENABLED_FLAG = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "queue_placer" / "dk_enabled.flag"
)
_DK_TOKEN_FILE = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "queue_placer" / "dk_place_token"
)

_POTD_ENABLED_FLAG = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "queue_placer" / "potd_enabled.flag"
)


def dk_place_url() -> str | None:
    return os.environ.get("DK_PLACE_URL") or "http://100.118.96.46:9097"


def dk_place_token() -> str | None:
    v = os.environ.get("DK_PLACE_TOKEN")
    if v:
        return v
    try:
        return _DK_TOKEN_FILE.read_text().strip() or None
    except Exception:
        return None


def dk_enabled() -> bool:
    """DK is placed alongside HR only when this flag is on (or env
    DK_PLACE_ENABLED=1). Independent toggle so DK pauses without touching HR."""
    return os.environ.get("DK_PLACE_ENABLED") == "1" or _DK_ENABLED_FLAG.exists()


def set_dk_enabled(on: bool) -> None:
    _DK_ENABLED_FLAG.parent.mkdir(parents=True, exist_ok=True)
    if on:
        _DK_ENABLED_FLAG.write_text("on\n")
    elif _DK_ENABLED_FLAG.exists():
        _DK_ENABLED_FLAG.unlink()


# --- Per-sport real-money arming --------------------------------------------
# wnba is the established live book and is ALWAYS armed. Any OTHER sport must
# be explicitly armed with an on-disk flag (data/queue_placer/arm_<sport>.flag)
# or env ARM_<SPORT>=1 before the placer will send its picks to a book. This
# decouples "the HR resolver can price this sport" from "we authorized real
# money on it" -- so widening _hr_resolver.SUPPORTED_SPORTS to a new sport
# can NOT auto-fire real bets without a deliberate opt-in.
_ALWAYS_ARMED_SPORTS = frozenset({"wnba"})


def sport_arm_flag_path(sport: str) -> Path:
    return (
        Path(__file__).resolve().parent.parent.parent
        / "data" / "queue_placer" / ("arm_" + (sport or "").lower() + ".flag")
    )


def sport_armed(sport: str) -> bool:
    """True if real-money placement is authorized for this sport. wnba is
    always armed; every other sport needs an explicit flag/env opt-in so a
    resolver widening cannot auto-fire it."""
    s = (sport or "").lower()
    if s in _ALWAYS_ARMED_SPORTS:
        return True
    if os.environ.get("ARM_" + s.upper()) == "1":
        return True
    return sport_arm_flag_path(s).exists()


def set_sport_armed(sport: str, on: bool) -> None:
    """Arm/disarm real-money placement for a non-wnba sport."""
    p = sport_arm_flag_path(sport)
    p.parent.mkdir(parents=True, exist_ok=True)
    if on:
        p.write_text("on\n")
    elif p.exists():
        p.unlink()


# --- Per-market real-money arming -------------------------------------------
# Finer than sport_armed: pick exactly which (sport, market) cells auto-bet,
# from the ones that qualify. A cell is armed if explicitly set in
# data/queue_placer/market_arming.json; otherwise wnba cells default ON
# (legacy behaviour preserved) and every other sport/market defaults OFF
# (opt-in). The placer checks this AND sport_armed. Added 2026-09-02.
_MARKET_ARMING_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "queue_placer" / "market_arming.json"
)


def _market_key(sport: str, bet_type: str) -> str:
    return (sport or "").lower() + ":" + (bet_type or "").strip()


def _load_market_arming() -> dict:
    try:
        import json
        return json.loads(_MARKET_ARMING_PATH.read_text())
    except Exception:
        return {}


def market_armed(sport: str, bet_type: str) -> bool:
    """True if this specific (sport, market) cell may auto-bet. Explicit entry
    in market_arming.json wins; else wnba defaults ON (legacy), every other
    sport/market defaults OFF (opt-in)."""
    cfg = _load_market_arming()
    key = _market_key(sport, bet_type)
    if key in cfg:
        return bool(cfg[key])
    return (sport or "").lower() in _ALWAYS_ARMED_SPORTS


def set_market_armed(sport: str, bet_type: str, on: bool) -> dict:
    """Arm/disarm one (sport, market) cell. Returns the full arming map."""
    import json
    cfg = _load_market_arming()
    cfg[_market_key(sport, bet_type)] = bool(on)
    _MARKET_ARMING_PATH.parent.mkdir(parents=True, exist_ok=True)
    _MARKET_ARMING_PATH.write_text(json.dumps(cfg, indent=2))
    if on:  # arming a cell also arms its sport (market gate still per-cell)
        set_sport_armed(sport, True)
    return cfg


def list_market_arming() -> dict:
    """The raw arming map (explicit overrides only), for the UI/report."""
    return _load_market_arming()


def potd_enabled() -> bool:
    """WNBA POTD auto-bet pass runs only when this flag is on (or env
    POTD_PLACE_ENABLED=1). Independent toggle -- regular queue placement is untouched."""
    return os.environ.get("POTD_PLACE_ENABLED") == "1" or _POTD_ENABLED_FLAG.exists()


def set_potd_enabled(on: bool) -> None:
    _POTD_ENABLED_FLAG.parent.mkdir(parents=True, exist_ok=True)
    if on:
        _POTD_ENABLED_FLAG.write_text("on\n")
    elif _POTD_ENABLED_FLAG.exists():
        _POTD_ENABLED_FLAG.unlink()


# ── Placer cadence ──────────────────────────────────────────
# How often the worker sweeps the queue for placeable picks. The queue
# itself refreshes every 2 min, so anything faster wastes cycles.
PLACER_INTERVAL_S = 60

# How long before game start we stop attempting to place. HR pulls the
# prematch market at kickoff and the placer would just retry until
# retry budget is exhausted — cheaper to skip.
PLACE_LATEST_MINUTES_BEFORE_START = 2


# -- BetConstruct (Zotabet + Betandplay) books - WNBA ----------------------
MAX_BC_PER_SWEEP = 6

_BC_ENABLED_FLAG = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "queue_placer" / "betconstruct_enabled.flag"
)
_BC_TOKEN_FILE = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "queue_placer" / "betconstruct_place_token"
)


def betconstruct_place_url() -> str | None:
    return os.environ.get("BETCONSTRUCT_PLACE_URL") or "http://100.118.96.46:9100"


def betconstruct_place_token() -> str | None:
    v = os.environ.get("BETCONSTRUCT_PLACE_TOKEN")
    if v:
        return v
    try:
        return _BC_TOKEN_FILE.read_text().strip() or None
    except Exception:
        return None


# -- Kalshi (US-legal CFTC exchange) - WNBA Q1 totals ----------------------
MAX_KALSHI_PER_SWEEP = 6

_KALSHI_ENABLED_FLAG = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "queue_placer" / "kalshi_enabled.flag"
)


def kalshi_enabled() -> bool:
    """Kalshi placed alongside HR/DK only when this flag is on (or env
    KALSHI_PLACE_ENABLED=1). WNBA Q1 totals only; the relay also self-gates
    on the key id + private key being present."""
    return os.environ.get("KALSHI_PLACE_ENABLED") == "1" or _KALSHI_ENABLED_FLAG.exists()


def set_kalshi_enabled(on: bool) -> None:
    _KALSHI_ENABLED_FLAG.parent.mkdir(parents=True, exist_ok=True)
    if on:
        _KALSHI_ENABLED_FLAG.write_text("on")
    elif _KALSHI_ENABLED_FLAG.exists():
        _KALSHI_ENABLED_FLAG.unlink()


def betconstruct_enabled() -> bool:
    """Zotabet + Betandplay placed alongside HR/DK only when this flag is
    on (or env BETCONSTRUCT_PLACE_ENABLED=1). Independent toggle."""
    return os.environ.get("BETCONSTRUCT_PLACE_ENABLED") == "1" or _BC_ENABLED_FLAG.exists()


def set_betconstruct_enabled(on: bool) -> None:
    _BC_ENABLED_FLAG.parent.mkdir(parents=True, exist_ok=True)
    if on:
        _BC_ENABLED_FLAG.write_text("on\n")
    elif _BC_ENABLED_FLAG.exists():
        _BC_ENABLED_FLAG.unlink()
