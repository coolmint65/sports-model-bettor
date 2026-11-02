"""NHL display-time win-probability calibration fit (MintyBot 2026-10-22).

The NHL card's H2H win-% BARS (win_prob{home,away}, routes_nhl._compute_nhl_
best_bets) are the RAW model prob and run ~8pp hot in the 60-70% band (model
says 60-70%, teams actually win ~54-58%; overall +2.1pp, Brier ~0.249). Football
already has a display-calibration map (scripts/fit_display_calibration.py, via an
Elo replay); the hockey engine is GBM-based and exposes NO Elo replay interface,
so we fit the DISPLAY map directly from the model's own settled ML track record.

Source: data/nhl.db table `calibration_samples` WHERE bet_type='ML' AND
result IN ('W','L'). model_prob is the model's PICK-confidence (always >= 0.5 --
the probability of whichever side the model favored), result='W' iff that pick won.
~4061 rows incl. representative backtest. X = model_prob, y = (result == 'W').

Fit: sklearn IsotonicRegression (monotonic). Because the samples are one-sided
(pick-confidence, all >= 0.5) but the card's win_prob.home is TWO-sided (an away
favorite has p_home < 0.5), the map is built SYMMETRIC about 0.5:
  * top half (g >= 0.5): the isotonic calibrated win-rate, with the sparse upper
    tail clipped to the 99th-percentile X (the data is a dense discrete cluster at
    ~0.65; only ~0.1% of samples exceed it and those few happened to all win,
    which would otherwise spike the isotonic to ~0.98 and INFLATE a 0.68 bar --
    the opposite of the fix). So every raw bar >= p99 maps to p99's well-supported
    calibrated rate.
  * bottom half (g < 0.5): the MIRROR, mapped[g] = 1 - mapped[1-g], so an away
    favorite at win_prob.home = 0.35 calibrates exactly like a home favorite at
    0.65 (cal_winprob calibrates the home field and sets away = complement).
  * NO-FLIP: a favorite (g >= 0.5) never maps below 0.5 and vice-versa; 0.5 is a
    fixpoint. (_display_cal._apply re-applies this clamp as a backstop.)

Data-gated: writes the map only with n >= MIN_N (we have ~4061). MERGES into
data/display_calibration.json under key 'hockey/nhl' (the loader also aliases it
'hockey_nhl'); every existing key (football/*, baseball/*) is preserved untouched.
_display_cal live-reloads the json by mtime, so no backend restart is needed for
the MAP itself (the routes_nhl wiring that READS it does need the one restart).

Re-run any time to refresh as the settled record grows:
    .venv\\Scripts\\python.exe scripts\\fit_nhl_display_calibration.py
"""
import os, sys, json, sqlite3

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.getcwd())

from sklearn.isotonic import IsotonicRegression

GRID = [round(x / 100, 2) for x in range(0, 101, 5)]  # 0.00 .. 1.00 step 0.05
MIN_N = 400
KEY = "hockey/nhl"
DB = os.path.join("data", "nhl.db")
PATH = os.path.join("data", "display_calibration.json")


def out(s):
    sys.stdout.buffer.write((str(s) + "\n").encode("utf-8", "replace"))


def _interp(mapped, p):
    if p <= GRID[0]:
        return mapped[0]
    if p >= GRID[-1]:
        return mapped[-1]
    for k in range(1, len(GRID)):
        if p <= GRID[k]:
            t = (p - GRID[k - 1]) / (GRID[k] - GRID[k - 1])
            return mapped[k - 1] + t * (mapped[k] - mapped[k - 1])
    return mapped[-1]


def _percentile(sorted_vals, q):
    """Linear-interpolated percentile (q in 0-100); no numpy dependency."""
    if not sorted_vals:
        return None
    if q <= 0:
        return sorted_vals[0]
    if q >= 100:
        return sorted_vals[-1]
    pos = (len(sorted_vals) - 1) * (q / 100.0)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


def main():
    # MERGE: load the existing maps so football/* + baseball/* are preserved.
    try:
        res = json.load(open(PATH, encoding="utf-8"))
    except Exception:
        res = {}
    before_keys = sorted(res)

    conn = sqlite3.connect(DB)
    rows = conn.execute(
        "SELECT model_prob, result FROM calibration_samples "
        "WHERE bet_type='ML' AND result IN ('W','L')"
    ).fetchall()
    conn.close()

    X = [float(p) for p, _ in rows if p is not None]
    Y = [1 if r == "W" else 0 for p, r in rows if p is not None]
    n = len(X)
    if n < MIN_N:
        out(f"{KEY}: n={n} < {MIN_N} -- NOT fitted (raw passthrough kept)")
        res.pop(KEY, None)
        _write(res)
        return

    xs = sorted(X)
    x_min = xs[0]
    x_hi = _percentile(xs, 99)          # robust ceiling (sparse tail guard)
    x_hi = max(x_hi, 0.55)              # always keep a usable favorite range
    base = sum(Y) / n

    ir = IsotonicRegression(y_min=0.02, y_max=0.98, out_of_bounds="clip")
    ir.fit(X, Y)

    def _top(g):
        """Calibrated win-rate for a favorite grid knot g (>= 0.5): isotonic at
        g clipped into [x_min, x_hi], NO-FLIP clamped to >= 0.5."""
        xe = min(max(g, x_min), x_hi)
        return max(0.5, float(ir.predict([xe])[0]))

    mapped = []
    for g in GRID:
        if g >= 0.5:
            mapped.append(round(_top(g), 4))
        else:
            mapped.append(round(1.0 - _top(round(1.0 - g, 2)), 4))  # mirror

    # Diagnostics.
    b_raw = sum((p - y) ** 2 for p, y in zip(X, Y)) / n
    b_cal = sum((_interp(mapped, p) - y) ** 2 for p, y in zip(X, Y)) / n
    mono = all(mapped[i] <= mapped[i + 1] + 1e-9 for i in range(len(mapped) - 1))
    flips = sum(
        1 for i in range(len(GRID))
        if (GRID[i] > 0.5 and mapped[i] < 0.5) or (GRID[i] < 0.5 and mapped[i] > 0.5)
    )

    res[KEY] = {
        "grid": GRID,
        "mapped": mapped,
        "n": n,
        "market": "home_win",
        "fit": "isotonic_calibration_samples_mirrored",
        "source": "nhl.db:calibration_samples(ML)",
        "x_min": round(x_min, 4),
        "x_hi": round(x_hi, 4),
        "base": round(base, 4),
        "brier_raw": round(b_raw, 4),
        "brier_cal": round(b_cal, 4),
    }

    out(f"{KEY}: n={n} base={base*100:.1f}% x_min={x_min:.4f} x_hi={x_hi:.4f} "
        f"Brier {b_raw:.4f}->{b_cal:.4f} monotonic={mono} flips={flips}")
    out("   GRID   " + " ".join(f"{g:.2f}" for g in GRID))
    out("   MAPPED " + " ".join(f"{m:.3f}" for m in mapped))
    for t in (0.55, 0.60, 0.625, 0.65, 0.675, 0.70):
        out(f"   cal({t:.3f}) = {_interp(mapped, t):.4f}")
    if not mono or flips:
        out("!! REFUSING TO WRITE: map not monotonic / has a flip")
        return
    _write(res)
    out(f"WROTE {PATH}  keys before={before_keys}  after={sorted(res)}")


def _write(res):
    # CRLF, to match the existing file's line endings on Windows.
    s = json.dumps(res, indent=2).replace("\r\n", "\n").replace("\n", "\r\n")
    with open(PATH, "wb") as fh:
        fh.write(s.encode("utf-8"))


if __name__ == "__main__":
    main()
