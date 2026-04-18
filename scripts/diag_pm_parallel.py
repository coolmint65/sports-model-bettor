"""Run predict_matchup directly in parallel for 3 different MLB games.

Bypasses _predict_mlb_full and the helper layer entirely. If the 3 calls
still return the same value or take pathologically long, the bug lives
inside predict_matchup or one of its sub-calls (point-in-time stats,
team data fetch, etc.) -- nothing the wrapper added.

Usage:  python -m scripts.diag_pm_parallel
"""
import time
from concurrent.futures import ThreadPoolExecutor

from engine.mlb_predict import predict_matchup

JOBS = [
    (147, 118, 5132011, 4417208, "Yankee Stadium"),  # KC @ NYY
    (142, 113, 42480,   4414528, "Target Field"),    # CIN @ MIN
    (112, 121, 31258,   39825,   "Wrigley Field"),   # NYM @ CHC
]


def _one(args):
    h_id, a_id, h_pid, a_pid, venue = args
    t = time.time()
    r = predict_matchup(h_id, a_id, h_pid, a_pid, venue)
    return {
        "home": h_id, "away": a_id,
        "took": round(time.time() - t, 2),
        "win_prob_home": (r.get("win_prob") or {}).get("home") if r else None,
    }


def serial():
    print("\n--- SERIAL ---")
    t0 = time.time()
    for j in JOBS:
        r = _one(j)
        print(r)
    print(f"serial total: {round(time.time() - t0, 2)}s")


def parallel():
    print("\n--- PARALLEL (3 threads) ---")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=3, thread_name_prefix="diag") as pool:
        results = list(pool.map(_one, JOBS))
    for r in results:
        print(r)
    print(f"parallel total: {round(time.time() - t0, 2)}s")


if __name__ == "__main__":
    serial()
    parallel()
