"""Balance-aware unit ladder for per-book stake sizing.

Each book's unit steps up with its OWN live balance:
    < $150  -> $2 unit
    >= $150 -> $3 unit
    >= $250 -> $5 unit
Balance is fetched live (HR: relay /balance, DK: dk-place-server /balance),
cached 60s. It FAILS SAFE: any missing/failed balance -> base $2 unit (never
sizes UP on a bad read). Per-bet stake = the model's fraction x the tier unit,
hard-clamped to 2x the unit so a single bet can never blow past the envelope.
"""
import time
import json
import urllib.request

from . import _config

# (min_balance, unit) ordered high -> low, PER BOOK. Default = flat $5
# override (Austin "$5 units for now" 2026-08-01). Kalshi gets a REAL ladder
# (Austin 2026-08-05: the venue can't limit us, so its unit scales with its
# own balance) -- top tier $10/bet = MAX_STAKE_PER_BET_ABS (audit T1); going
# past $10 is a deliberate _config change, not a ladder edit.
_DEFAULT_LADDER = [(0.0, 5.0)]
_LADDERS = {
    # hr/dk flat $10 (Austin 2026-08-07 msg 24179: "bump WNBA to full $10 on hr and dk")
    "hr": [(0.0, 10.0)],
    "dk": [(0.0, 10.0)],
    # Kalshi: a LADDER that roughly tracks 10% of bankroll as it grows,
    # FLOORED at $5 (Austin 2026-08-10 msg 24784: "I want the ladder, just
    # roughly 10% as the ladder grows. We keep it at $5 for now — anything
    # lower and it becomes a slog to improve the bankroll"). Flat at the rung
    # per bet (apply_unit special-case), stepping up with the balance. Each
    # rung ~= 10% of its threshold; $5 floor holds until 10% clears $5 (~$100).
    "kalshi": [(1000.0, 100.0), (750.0, 75.0), (500.0, 50.0), (400.0, 40.0),
               (300.0, 30.0), (250.0, 25.0), (200.0, 20.0), (150.0, 15.0),
               (100.0, 10.0), (0.0, 5.0)],
}
_BASE_UNIT = 5.0          # must match _config.UNIT_DOLLARS (what build_queue sized at)
_CACHE_TTL_S = 60.0
_cache = {}               # book -> (ts, unit)


def _unit_for_balance(book, bal):
    for lo, u in _LADDERS.get(book, _DEFAULT_LADDER):
        if bal >= lo:
            return u
    return _BASE_UNIT


def _balance_endpoint(book):
    """(url, json_key) for the book's balance, or (None, None)."""
    if book == "dk":
        base = _config.dk_place_url()
        return ((base.rstrip("/") + "/balance"), "amount") if base else (None, None)
    base = _config.relay_url() or "http://127.0.0.1:7478"
    return (base.rstrip("/") + "/balance"), "balance"


def _fetch_balance(book):
    # Kalshi: direct signed API (no local relay/proxy), fail-safe None.
    if book == "kalshi":
        from . import _kalshi
        return _kalshi.get_balance_dollars()
    url, key = _balance_endpoint(book)
    if not url:
        return None
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            d = json.loads(r.read().decode())
        v = d.get(key)
        return float(v) if v is not None else None
    except Exception:
        return None


def unit_for_book(book):
    """Current ladder unit for a book, cached 60s, fail-safe to $2."""
    book = (book or "hr").lower()
    now = time.time()
    c = _cache.get(book)
    if c and now - c[0] < _CACHE_TTL_S:
        return c[1]
    bal = _fetch_balance(book)
    unit = _unit_for_balance(book, bal) if bal is not None else _BASE_UNIT
    _cache[book] = (now, unit)
    return unit


def apply_unit(pick):
    """Return a copy of pick with stake_dollars re-sized to the book's ladder
    unit (scaled from the base unit build_queue used), hard-clamped to 2x the
    tier unit. No-op if it can't compute or nothing changes."""
    book = (pick.get("book") or "hr").lower()
    unit = unit_for_book(book)
    if book == "kalshi":
        # Flat at the ladder rung (Austin msg 24784) — the rung roughly tracks
        # 10% of bankroll as it grows, floored at $5. Stake the rung itself,
        # ignoring the model fraction, so "keep it at $5 for now" is a flat
        # $5/bet. Contract rounding happens downstream in _kalshi.
        if unit <= 0:
            return pick
        return dict(pick, stake_dollars=round(unit, 2))
    base = _config.UNIT_DOLLARS or _BASE_UNIT
    raw = float(pick.get("stake_dollars") or 0.0)
    if base <= 0 or raw <= 0:
        return pick
    scaled = round(raw * (unit / base), 2)
    scaled = min(scaled, 2.0 * unit)      # never exceed 2x the tier's unit
    if scaled == raw:
        return pick
    return dict(pick, stake_dollars=scaled)
