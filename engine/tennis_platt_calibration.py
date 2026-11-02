"""Tennis POTD calibration — per-bet-type Platt (logistic).

The hierarchical empirical cells are too thin for tennis's high-prob buckets
(ML 0.8+ = n=2, both wins -> a fake 0.93 posterior). Tennis is overconfident
across EVERY market (model prob >> realized win rate), so a smooth 2-parameter
logistic per bet_type de-biases it without the thin-cell noise. Coefficients are
fit from tennis_picks and cached in data/tennis_platt.json (refit via
fit_tennis_platt()). MintyBot 2026-10-06.
"""
import os, json, sqlite3, math

_COEF_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "tennis_platt.json")
_DB = os.path.join(os.path.dirname(__file__), "..", "data", "tennis.db")
_MIN_N = 40        # need enough settled picks to fit a bet_type, else passthrough
_ITERS = 4000
_CACHE = None


def _logit(p):
    p = min(max(float(p), 1e-6), 1 - 1e-6)
    return math.log(p / (1 - p))


def _sig(z):
    z = min(max(z, -30), 30)
    return 1.0 / (1.0 + math.exp(-z))


def fit_tennis_platt(write=True):
    """Fit sigmoid(A + B*logit(model_prob)) ~ win, per bet_type."""
    coef = {}
    try:
        c = sqlite3.connect("file:" + _DB + "?mode=ro", uri=True)
    except Exception:
        return coef
    try:
        bts = [r[0] for r in c.execute(
            "SELECT DISTINCT bet_type FROM tennis_picks WHERE result IN ('W','L')").fetchall()]
        for bt in bts:
            rows = c.execute(
                "SELECT model_prob, result FROM tennis_picks WHERE bet_type=? "
                "AND result IN ('W','L') AND model_prob>0 AND model_prob<1", (bt,)).fetchall()
            data = [(_logit(mp), 1.0 if r == 'W' else 0.0) for mp, r in rows]
            if len(data) < _MIN_N:
                continue
            A, B = 0.0, 1.0
            n = len(data)
            lr = 0.1
            for _ in range(_ITERS):
                gA = gB = 0.0
                for lm, y in data:
                    e = _sig(A + B * lm) - y
                    gA += e
                    gB += e * lm
                A -= lr * gA / n
                B -= lr * gB / n
                # Projected GD (MintyBot 2026-10-15): tennis is overconfident across
                # every market, so the calibrated slope must only FLATTEN -- clip B to
                # [0,1] so the fit can never invert the model (negative B, the old
                # SET_SPREAD artifact) or amplify overconfidence (B>1, the old
                # TOTAL_SETS artifact). A (the downward shift) stays free.
                B = min(max(B, 0.0), 1.0)
            coef[bt] = [round(A, 4), round(B, 4), n]
    finally:
        c.close()
    if write and coef:
        try:
            os.makedirs(os.path.dirname(_COEF_PATH), exist_ok=True)
            json.dump(coef, open(_COEF_PATH, "w"))
        except Exception:
            pass
    global _CACHE
    _CACHE = coef
    return coef


def _load():
    global _CACHE
    if _CACHE is None:
        try:
            _CACHE = json.load(open(_COEF_PATH))
        except Exception:
            _CACHE = {}
    return _CACHE


def tennis_calibrate(bet_type, raw_prob):
    """Calibrated win prob for a tennis pick; passthrough if no fit for this bet_type."""
    if raw_prob is None:
        return raw_prob
    coef = _load().get(bet_type)
    if not coef:
        return raw_prob
    return _sig(coef[0] + coef[1] * _logit(raw_prob))


def has_fit(bet_type):
    """True if a Platt fit exists for this bet_type (else callers fall back)."""
    return bool(_load().get(bet_type))
