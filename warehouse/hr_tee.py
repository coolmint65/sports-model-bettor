"""Fail-safe tee: record every Hard Rock board PickMint ALREADY fetches.

ZERO extra HR requests. Hooked into ``scrapers.hardrock_odds._fetch_events_for_comp``
right after a successful (cache-miss) fetch + normalization:

    try:
        from warehouse import hr_tee as _wh_tee
        _wh_tee.offer(comp_id, events)
    except Exception:
        pass

Contract (the worker / backend must never notice this module):
  * ``offer()`` is O(1): one ``put_nowait`` on a bounded queue. Full queue -> the board is
    dropped (counted), never blocks, never raises.
  * A daemon thread (started lazily, one per process) projects the board to a compact JSON
    (scalar fields of event / market / selection only) and writes it atomically to
    ``data/warehouse/spool/hr/<YYYYMMDD>/<HHMMSS>_<ms>_<pid>_<comp>.json.gz`` (tmp + os.replace).
  * ``warehouse.hr_ingest`` (scheduled, separate process) parses the spool into wh.db quotes.
  * Kill switch: create ``data/warehouse/hr_tee.off`` (re-checked every 60 s by the thread).
    Also stops writing when the disk has < 20 GB free or today's spool dir has > 40k files.
  * Stats per process: ``data/warehouse/spool/stats/hr_tee_<pid>.json`` (every ~2 min).

stdlib only; no sqlite, no imports of the app.  (MintyBot 2026-11-01, odds warehouse.)
"""
from __future__ import annotations

import gzip
import json
import os
import queue
import shutil
import sys
import threading
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_WH = os.path.join(_ROOT, "data", "warehouse")
_SPOOL = os.path.join(_WH, "spool", "hr")
_STATS_DIR = os.path.join(_WH, "spool", "stats")
_OFF_FLAG = os.path.join(_WH, "hr_tee.off")

_QMAX = 16
_MIN_FREE = 20 * 1024 ** 3
_MAX_FILES_PER_DAY = 40000

_Q: "queue.Queue" = queue.Queue(maxsize=_QMAX)
_start_lock = threading.Lock()
_thread = None
_S = {"enabled": True, "offered": 0, "queued": 0, "dropped_full": 0, "dropped_off": 0,
      "written": 0, "bytes": 0, "selections": 0, "errors": 0, "last_err": None,
      "started": None, "last_write": None, "pid": os.getpid()}

# verbose / nested keys we never need (dropped from the projection)
_SKIP = {"fullPath", "displayedGroups", "links", "media", "tags", "templates", "metadata",
         "translations", "streaming", "statistics", "scoreboard", "betBuilder"}


def offer(comp_id, events, ts=None):
    """Hand an already-fetched board to the background writer. Never raises / blocks."""
    try:
        if not _S["enabled"]:
            _S["dropped_off"] += 1
            return
        if not events:
            return
        _S["offered"] += 1
        try:
            _Q.put_nowait((ts or time.time(), str(comp_id), events))
            _S["queued"] += 1
        except queue.Full:
            _S["dropped_full"] += 1
            return
        if _thread is None:
            _ensure_thread()
    except Exception:
        pass


def stats():
    return dict(_S, qsize=_Q.qsize())


def _ensure_thread():
    global _thread
    with _start_lock:
        if _thread is not None:
            return
        t = threading.Thread(target=_run, name="wh-hr-tee", daemon=True)
        _thread = t
        _S["started"] = time.time()
        t.start()


def _scal(d):
    """Scalar fields of a dict (str/int/float/bool), via an atomic C-level copy."""
    if not isinstance(d, dict):
        return {}
    c = d.copy()
    out = {}
    for k, v in c.items():
        if v is None or k in _SKIP:
            continue
        if isinstance(v, (str, int, float, bool)):
            out[k] = v
    return out


def _project(events):
    evs = []
    nsel = 0
    for ev in list(events):
        if not isinstance(ev, dict):
            continue
        e = _scal(ev)
        parts = ev.get("participants")
        if isinstance(parts, list):
            e["participants"] = [_scal(p) for p in list(parts) if isinstance(p, dict)]
        mk = []
        for m in list(ev.get("markets") or []):
            if not isinstance(m, dict):
                continue
            mm = _scal(m)
            sels = m.get("selection")
            if sels is None:
                sels = m.get("selections")
            ss = []
            for s in list(sels or []):
                if isinstance(s, dict):
                    ss.append(_scal(s))
            mm["sel"] = ss
            nsel += len(ss)
            mk.append(mm)
        e["markets"] = mk
        evs.append(e)
    return evs, nsel


def _proc_tag():
    a = " ".join(sys.argv)[:200].lower()
    if "uvicorn" in a:
        return "backend"
    if "live_worker" in a:
        return "worker"
    if "capture_all_odds" in a:
        return "capture"
    if "nhl_props_gen" in a:
        return "nhlprops"
    return os.path.basename(sys.argv[0] or "py")[:24] or "py"


def _check(now, day_dir):
    try:
        if os.path.exists(_OFF_FLAG):
            return False
        if shutil.disk_usage(_WH if os.path.isdir(_WH) else _ROOT).free < _MIN_FREE:
            return False
        if os.path.isdir(day_dir):
            n = 0
            with os.scandir(day_dir) as it:
                for _ in it:
                    n += 1
                    if n > _MAX_FILES_PER_DAY:
                        return False
        return True
    except Exception:
        return True


def _write_stats():
    try:
        os.makedirs(_STATS_DIR, exist_ok=True)
        p = os.path.join(_STATS_DIR, "hr_tee_%d.json" % os.getpid())
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(dict(stats(), proc=_proc_tag(), argv=" ".join(sys.argv)[:200], at=time.time()), fh)
        os.replace(tmp, p)
    except Exception:
        pass


def _run():
    proc = _proc_tag()
    last_check = 0.0
    last_stats = 0.0
    seq = 0
    while True:
        try:
            item = _Q.get()
        except Exception:
            time.sleep(1.0)
            continue
        try:
            ts, comp, events = item
            item = None
            now = time.time()
            day = time.strftime("%Y%m%d", time.gmtime(ts))
            day_dir = os.path.join(_SPOOL, day)
            if now - last_check > 60:
                _S["enabled"] = _check(now, day_dir)
                last_check = now
            if not _S["enabled"]:
                _S["dropped_off"] += 1
                continue
            evs, nsel = _project(events)
            events = None
            if not evs:
                continue
            payload = {"v": 1, "ts": round(ts, 3), "comp": comp, "pid": os.getpid(), "proc": proc,
                       "n_sel": nsel, "events": evs}
            raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            evs = payload = None
            os.makedirs(day_dir, exist_ok=True)
            seq += 1
            name = "%s_%03d_%d_%s_%d.json.gz" % (time.strftime("%H%M%S", time.gmtime(ts)),
                                                int((ts % 1) * 1000), os.getpid(), comp[-12:], seq)
            path = os.path.join(day_dir, name)
            tmp = path + ".tmp"
            with open(tmp, "wb") as fh:
                fh.write(gzip.compress(raw, compresslevel=5))
            os.replace(tmp, path)
            _S["written"] += 1
            _S["bytes"] += os.path.getsize(path)
            _S["selections"] += nsel
            _S["last_write"] = now
        except Exception as e:  # never propagate
            _S["errors"] += 1
            _S["last_err"] = repr(e)[:300]
        finally:
            item = events = None
            try:
                if time.time() - last_stats > 120:
                    last_stats = time.time()
                    _write_stats()
            except Exception:
                pass
