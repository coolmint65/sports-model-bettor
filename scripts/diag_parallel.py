"""Diagnostic: run _predict_mlb_full across 3 threads to see if the
parallel path inside one process genuinely works, or deadlocks.

Usage:  python -m scripts.diag_parallel
"""
import time
from concurrent.futures import ThreadPoolExecutor
from backend.server import _predict_mlb_full

JOBS = [
    (147, 118, 5132011, 4417208, "Yankee Stadium"),
    (142, 113, 42480,   4414528, "Target Field"),
    (112, 121, 31258,   39825,   "Wrigley Field"),
]


def one(args):
    return _predict_mlb_full(*args, use_cache=False)


def main():
    t = time.time()
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(one, JOBS))
    elapsed = round(time.time() - t, 2)
    n_err = sum(1 for r in results if isinstance(r, dict) and "error" in r)
    print(f"3 parallel threads took {elapsed} s")
    print(f"got {len(results)} results, errors: {n_err}")
    for i, r in enumerate(results):
        if isinstance(r, dict):
            wp = (r.get("win_prob") or {}).get("home")
            print(f"  job {i}: home_wp={wp}")


if __name__ == "__main__":
    main()
