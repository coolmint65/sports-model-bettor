"""Reproduce the parallel _predict_mlb_full hang and dump the call stack
of every running thread once the deadlock is observed.

The point isn't to fix anything -- it's to point at the exact line
each worker is blocked on so we can see the actual root cause instead
of guessing.

Usage:
    python -m scripts.diag_thread_dump

What it does:
  - Starts faulthandler so a deadlock can be force-traced.
  - Submits 3 _predict_mlb_full jobs to a ThreadPoolExecutor.
  - Waits 45 s; if the futures haven't completed, dumps every Python
    thread's current frame to stderr. The output names every C-extension
    boundary (xgboost, sqlite3, numpy) so you can see which one each
    thread is blocked inside.
"""
from __future__ import annotations

import faulthandler
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

JOBS = [
    (147, 118, 5132011, 4417208, "Yankee Stadium"),
    (142, 113, 42480,   4414528, "Target Field"),
    (112, 121, 31258,   39825,   "Wrigley Field"),
]


def _run_one(args):
    from backend.server import _predict_mlb_full
    return _predict_mlb_full(*args, use_cache=False)


def _dump_all_threads(label: str) -> None:
    """Print the Python frame each thread is currently executing."""
    print(f"\n=== {label} ===", flush=True)
    frames = sys._current_frames()
    for thread in threading.enumerate():
        tid = thread.ident
        frame = frames.get(tid)
        print(f"\n--- Thread {thread.name} (id={tid}, daemon={thread.daemon}) ---",
              flush=True)
        if frame is None:
            print("  (no frame -- finished or not started)", flush=True)
            continue
        traceback.print_stack(frame, file=sys.stdout)
        sys.stdout.flush()


def main() -> int:
    # faulthandler is the nuclear option -- sends SIGABRT-style traceback
    # of every thread to stderr if the process completely freezes.
    faulthandler.enable()
    faulthandler.dump_traceback_later(60, repeat=False)

    print(f"[{time.strftime('%H:%M:%S')}] Submitting {len(JOBS)} jobs to "
          f"ThreadPoolExecutor(max_workers=3)...", flush=True)
    started = time.time()
    pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix="diag")
    futures = [pool.submit(_run_one, args) for args in JOBS]

    # Watch for completion every 5 s. After 45 s we assume deadlock and
    # dump every thread's stack, then exit non-zero.
    while time.time() - started < 45:
        time.sleep(5)
        done = sum(1 for f in futures if f.done())
        elapsed = round(time.time() - started, 1)
        print(f"[{time.strftime('%H:%M:%S')}] {done}/{len(futures)} done "
              f"after {elapsed}s", flush=True)
        if done == len(futures):
            for i, f in enumerate(futures):
                try:
                    r = f.result(timeout=1)
                    wp = (r.get("win_prob") or {}).get("home")
                    print(f"  job {i}: home_wp={wp}", flush=True)
                except Exception as e:
                    print(f"  job {i}: ERROR {e}", flush=True)
            pool.shutdown(wait=True)
            return 0

    # Hung. Snapshot every thread's frame so we can see what each is
    # blocked on. The C-extension layers (sqlite3, xgboost, numpy) all
    # show up as actual frames in this dump.
    _dump_all_threads("THREAD STACKS @ 45 s")

    # Wait a bit more in case anything moves; faulthandler will also fire
    # at 60 s with its own dump.
    time.sleep(20)
    _dump_all_threads("THREAD STACKS @ 65 s (post-faulthandler)")
    print("\nNot waiting further. Workers are hung. Above stacks show why.",
          flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
