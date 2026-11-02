"""Shared stale-while-revalidate slate cache (Austin 2026-09-19).

Any heavy sport-slate endpoint wraps its compute with this so:
  - warm hits serve the cached response instantly,
  - a stale entry is refreshed in a guarded background thread,
  - a cold-cache hit returns a loading placeholder + kicks ONE guarded compute
    (never blocks the request, never lets two computes race the same key).

First proven on football (routes_football): 203s cold HANG -> 0.15s + a
non-hanging loading state that fills in when the compute lands.

Usage in a route::

    from backend.server._slate_cache import SlateCache
    _CACHE = SlateCache("football", ttl=60.0)

    @router.get("/api/football/{league}/today")
    def api_football_today(league, days=None):
        ...
        return _CACHE.serve(
            key=(league, days),
            compute=lambda: _compute_slate(league, days),
            loading={"league": league, "games": [], "loading": True},
        )

And in a startup pre-warm::

    _CACHE.warm((league, None), lambda: _compute_slate(league, None))
"""
from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)


class SlateCache:
    """Per-endpoint SWR cache. One instance per sport/route; keys namespace the
    per-league/per-tour entries within it."""

    # A compute that hasn't landed in HUNG_AFTER seconds no longer blocks a fresh
    # one (a hung HR/Sofa fetch used to pin `_refreshing` and serve the same stale
    # slate for hours: CBA/NBL finals stuck "prematch" 10-30 + 10-31).
    HUNG_AFTER = 240.0
    # An entry older than MAX_STALE is not served blindly: the request waits up to
    # MAX_WAIT for the refresh, then falls back to the stale copy.
    MAX_STALE = 600.0
    MAX_WAIT = 12.0

    def __init__(self, name: str, ttl: float = 60.0):
        self.name = name
        self.ttl = ttl
        self._cache: dict = {}
        self._refreshing: dict = {}      # key -> (started monotonic, done Event)
        self._lock = threading.Lock()

    def _spawn(self, key, compute):
        """Start ONE guarded compute for key; returns its done-Event. Kills the
        pre-warm-vs-cold-hit thundering herd."""
        with self._lock:
            cur = self._refreshing.get(key)
            if cur is not None:
                started, ev = cur
                if time.monotonic() - started < self.HUNG_AFTER:
                    return ev
                logger.warning("[%s cache] compute for %r running %.0fs, starting a fresh one",
                               self.name, key, time.monotonic() - started)
            ev = threading.Event()
            token = (time.monotonic(), ev)
            self._refreshing[key] = token

        def _bg():
            t0 = time.monotonic()
            try:
                r = compute()
                prev = self._cache.get(key)
                if prev is None or prev[0] <= t0:      # a newer compute already landed: drop ours
                    self._cache[key] = (t0, r)
                secs = time.monotonic() - t0
                if secs > 30:
                    logger.warning("[%s cache] slow compute %r: %.1fs", self.name, key, secs)
            except Exception as e:
                logger.warning("[%s cache] compute failed for %r: %s",
                               self.name, key, e)
            finally:
                with self._lock:
                    if self._refreshing.get(key) is token:
                        del self._refreshing[key]
                ev.set()

        threading.Thread(target=_bg, daemon=True).start()
        return ev

    def status(self) -> dict:
        now = time.monotonic()
        return {
            "name": self.name, "ttl": self.ttl,
            "entries": {repr(k): round(now - v[0], 1) for k, v in self._cache.items()},
            "refreshing": {repr(k): round(now - v[0], 1) for k, v in self._refreshing.items()},
        }

    def serve(self, key, compute, loading):
        """SWR: serve the cached response (refreshing in the background when
        stale), or — on a cold cache — return ``loading`` immediately and kick a
        single guarded compute. An entry older than MAX_STALE waits up to MAX_WAIT
        for its refresh instead of serving an hours-old slate."""
        cached = self._cache.get(key)
        if cached is not None:
            built, resp = cached
            age = time.monotonic() - built
            if age >= self.ttl:
                ev = self._spawn(key, compute)
                if age >= self.MAX_STALE and ev.wait(self.MAX_WAIT):
                    return self._cache.get(key, cached)[1]
            return resp
        self._spawn(key, compute)
        return loading

    def serve_blocking(self, key, compute):
        """SWR with a BLOCKING cold path — compute inline on a cache miss (fine
        when the compute is fast, e.g. small slates). Warm hits still serve
        instantly and refresh in the background when stale. No loading
        placeholder, so callers need no frontend change."""
        cached = self._cache.get(key)
        if cached is not None:
            built, resp = cached
            if (time.monotonic() - built) >= self.ttl:
                self._spawn(key, compute)
            return resp
        r = compute()
        self._cache[key] = (time.monotonic(), r)
        return r

    def warm(self, key, compute):
        """Startup pre-warm — guarded synchronous compute + cache. Skips if a
        request already kicked a compute for this key (so it never herds)."""
        with self._lock:
            if key in self._refreshing:
                return None
            token = (time.monotonic(), threading.Event())
            self._refreshing[key] = token
        try:
            t0 = time.monotonic()
            r = compute()
            self._cache[key] = (t0, r)
            return r
        except Exception as e:
            logger.warning("[%s cache] warm failed for %r: %s",
                           self.name, key, e)
            return None
        finally:
            with self._lock:
                if self._refreshing.get(key) is token:
                    del self._refreshing[key]
            token[1].set()

    def peek(self, key):
        """Return the cached response for ``key`` without triggering a compute
        (or None). For diagnostics / conditional pre-warm."""
        cached = self._cache.get(key)
        return cached[1] if cached is not None else None
