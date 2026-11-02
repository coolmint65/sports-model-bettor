"""Per-sport /best-bets progress + single-flight infrastructure.

Lifted out of ``__init__.py`` so per-sport router modules can import
``_bb_reset_on_exit`` at load time (the decorator is applied when the
route function is being defined, not at request time, so a lazy proxy
won't work).

Three pieces:

  - ``_BB_PROGRESS``: per-sport progress bucket polled by the spinner.
  - ``_bb_single_flight``: dedups concurrent /best-bets compute per sport.
  - ``_bb_reset_on_exit``: decorator combining the two; what every
    /api/{sport}/best-bets endpoint wraps with.
"""

from __future__ import annotations

import concurrent.futures as _futures
import functools as _functools
import threading as _threading
import time as _time

# Per-sport /best-bets progress state for the spinner. Frontend polls
# /api/<sport>/best-bets/progress to render a percentage instead of an
# indeterminate spinner during the multi-minute cold load. Each sport
# has its own counter so parallel dashboard mounts don't stomp on each
# other's totals.
_BB_PROGRESS: dict = {
    "mlb": {"total": 0, "done": 0, "phase": "idle",
            "started_at": None, "finished_at": None},
    "nhl": {"total": 0, "done": 0, "phase": "idle",
            "started_at": None, "finished_at": None},
    "nba": {"total": 0, "done": 0, "phase": "idle",
            "started_at": None, "finished_at": None},
}
_BB_PROGRESS_LOCK = _threading.Lock()


def _bb_progress_set(sport: str = "mlb", **fields) -> None:
    with _BB_PROGRESS_LOCK:
        _BB_PROGRESS.setdefault(sport, {}).update(fields)


def _bb_progress_increment(sport: str = "mlb") -> None:
    with _BB_PROGRESS_LOCK:
        bucket = _BB_PROGRESS.setdefault(sport, {})
        bucket["done"] = bucket.get("done", 0) + 1


def _bb_progress_snapshot(sport: str = "mlb") -> dict:
    with _BB_PROGRESS_LOCK:
        return dict(_BB_PROGRESS.get(sport, {}))


# Single-flight registry. Concurrent /api/<sport>/best-bets calls (mount
# load + 5-min refresh interval racing each other, or two tabs open)
# would otherwise both run the full per-game prediction chain AND both
# bump the same shared progress counter -- producing "32/15 (100%)" in
# the spinner and doubling the workload. _BB_INFLIGHT holds the running
# request's Future so latecomers wait for its result instead of racing.
_BB_INFLIGHT: dict[str, _futures.Future] = {}
_BB_INFLIGHT_LOCK = _threading.Lock()


def _bb_single_flight(sport: str, runner):
    """Run `runner()` once per sport at a time. If another request is
    already executing the same sport's work, wait for and return its
    result. Used by all three /best-bets endpoints."""
    with _BB_INFLIGHT_LOCK:
        existing = _BB_INFLIGHT.get(sport)
        if existing is not None and not existing.done():
            fut = existing
            owner = False
        else:
            fut = _futures.Future()
            _BB_INFLIGHT[sport] = fut
            owner = True

    if not owner:
        return fut.result()

    try:
        result = runner()
        fut.set_result(result)
        return result
    except BaseException as e:
        fut.set_exception(e)
        raise
    finally:
        with _BB_INFLIGHT_LOCK:
            if _BB_INFLIGHT.get(sport) is fut:
                _BB_INFLIGHT.pop(sport, None)


def _bb_reset_on_exit(sport: str):
    """Decorator wrapping a /best-bets endpoint with two guarantees:

    1. Single-flight: only one underlying compute runs per sport at a
       time. Concurrent callers (mount + 5-min refresh racing, two open
       tabs) share the in-flight result instead of duplicating the
       full per-game prediction chain. Without this the shared progress
       counter overshoots ("32/15 (100%)" in the spinner) because two
       runners both increment the same `done`.
    2. Progress reset: the sport's progress bucket is forced back to
       phase=idle on exit -- even when the inner function raises -- so
       a mid-loop crash can never leave the spinner stuck at
       "Computing 4/15 games" forever.
    """
    def decorator(fn):
        @_functools.wraps(fn)
        def wrapper(*args, **kwargs):
            def _runner():
                try:
                    return fn(*args, **kwargs)
                finally:
                    _bb_progress_set(sport, phase="idle",
                                     finished_at=_time.time())
            return _bb_single_flight(sport, _runner)
        return wrapper
    return decorator


# --- Persisted best-bets response cache (survives process restart) ----------
# The in-process _*_BB_CACHE is ephemeral: after a restart the first /best-bets
# request recomputes the whole slate synchronously on the request thread (the
# "Computing predictions 0/N" multi-minute stall). Mirror each assembled response
# to disk so a cold process serves the last-known slate INSTANTLY and lets the
# existing stale-while-revalidate refresh update it in the background.
# MintyBot 2026-10-15.
import json as _json
import os as _os

_BB_PERSIST_DIR = _os.path.join(
    _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))),
    "data", "bb_cache")
_BB_PERSIST_LOCK = _threading.Lock()


def persist_bb(sport, resp):
    """Mirror an assembled best-bets response to disk. Best-effort; never raises."""
    try:
        from fastapi.encoders import jsonable_encoder
        path = _os.path.join(_BB_PERSIST_DIR, sport + ".json")
        tmp = path + ".tmp"
        with _BB_PERSIST_LOCK:
            _os.makedirs(_BB_PERSIST_DIR, exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as f:
                _json.dump({"saved_at": _time.time(), "resp": jsonable_encoder(resp)}, f)
            _os.replace(tmp, path)
    except Exception:
        pass


def load_bb(sport):
    """Return (resp, saved_at_epoch) persisted for `sport`, or (None, None)."""
    try:
        path = _os.path.join(_BB_PERSIST_DIR, sport + ".json")
        with open(path, "r", encoding="utf-8") as f:
            d = _json.load(f)
        return d.get("resp"), d.get("saved_at")
    except Exception:
        return None, None


# --- Backtest response cache (speed pass 2026-10-28) -------------------------
# Full-season backtests replay every game (MLB ~3 min, NHL ~40 s), so the Backtest tab timed
# out or crawled on every visit. Serve the last result for a param set (persisted, survives
# restarts) and re-run it in the background at most every 6 h. Only the very first run of a
# param set blocks.
_BT_CACHE: dict = {}
_BT_REFRESHING: set = set()
_BT_TTL_S = 6 * 3600


def cached_backtest(ck, run):
    cached = _BT_CACHE.get(ck)
    if cached is None:
        resp, saved = load_bb(ck)
        if resp is not None:
            cached = _BT_CACHE[ck] = (saved or 0.0, resp)
    if cached is not None:
        built, resp = cached
        if _time.time() - built >= _BT_TTL_S and ck not in _BT_REFRESHING:
            _BT_REFRESHING.add(ck)

            def _bg():
                try:
                    r = run()
                    if not (isinstance(r, dict) and r.get("error")):
                        _BT_CACHE[ck] = (_time.time(), r)
                        persist_bb(ck, r)
                finally:
                    _BT_REFRESHING.discard(ck)

            _threading.Thread(target=_bg, daemon=True).start()
        return resp
    r = run()
    if not (isinstance(r, dict) and r.get("error")):
        _BT_CACHE[ck] = (_time.time(), r)
        persist_bb(ck, r)
    return r
