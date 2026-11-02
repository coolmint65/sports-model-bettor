import sys
PATH = r"C:\sports-model-bettor\engine\sports\mlb\prop_picks.py"

def b(s: str) -> bytes:
    return s.replace("\n", "\r\n").encode("utf-8")

with open(PATH, "rb") as f:
    data = f.read()

edits = []

# ---- 1: insert _continuous_over_under helper + add kind= to signature ----
OLD_1 = """    return 100.0 / (n + 100.0) if n > 0 else abs(n) / (abs(n) + 100.0)


def score_player_prop(samples: dict, prop: dict) -> dict | None:
"""
NEW_1 = '''    return 100.0 / (n + 100.0) if n > 0 else abs(n) / (abs(n) + 100.0)


def _continuous_over_under(samp, line) -> tuple[float, float]:
    """P(over)=P(X>=line), P(under)=P(X<=line) for a CONTINUOUS stat,
    read from the best-AIC continuous fit of the player's samples.

    Parallel to the count path (``prob_over`` / ``prob_under`` use the
    empirical CDF of the samples); here we fit a continuous family
    (``engine.distribution_fit.fit_all_continuous``) and read its
    survival / CDF. Continuous => P(X == line) = 0, so the two sides
    sum to ~1. Falls back to the empirical survival if no fit yields a
    CDF, so the scorer never returns a silently-wrong probability."""
    import numpy as _np
    from ...distribution_fit import fit_all_continuous
    arr = _np.asarray(samp, dtype=float)
    fits = fit_all_continuous(arr)
    best = fits[0] if fits else None
    if best is not None and best.get("cdf") is not None:
        cdf = best["cdf"]
        sf = best.get("sf")
        p_under = min(1.0, max(0.0, float(cdf(line))))
        p_over = float(sf(line)) if sf is not None else (1.0 - p_under)
        p_over = min(1.0, max(0.0, p_over))
        return p_over, p_under
    return prob_over(arr, line), prob_under(arr, line)


def score_player_prop(samples: dict, prop: dict,
                      *, kind: str | None = None) -> dict | None:
'''
edits.append(("1", OLD_1, NEW_1))

# ---- 2: kind dispatch for p_over/p_under ----
OLD_2 = """    over_odds = prop.get("over_odds")
    under_odds = prop.get("under_odds")
    p_over = prob_over(samp, line)
    p_under = prob_under(samp, line)
    # Shrink toward 50% per the per-stat calibration findings. Only
"""
NEW_2 = """    over_odds = prop.get("over_odds")
    under_odds = prop.get("under_odds")
    # Dispatch by stat KIND. COUNT (default) keeps the empirical
    # survival on the MC samples, exactly as before. CONTINUOUS fits a
    # continuous family to the samples and reads P(X>=line)/P(X<=line)
    # from its survival/CDF. Resolving kind via the registry keeps every
    # current (count) caller on the identical count path.
    from ...distribution_fit import get_stat_kind as _gsk
    eff_kind = kind if kind is not None else _gsk("mlb", stat_key)
    if eff_kind == "continuous":
        p_over, p_under = _continuous_over_under(samp, line)
    else:
        p_over = prob_over(samp, line)
        p_under = prob_under(samp, line)
    # Shrink toward 50% per the per-stat calibration findings. Only
"""
edits.append(("2", OLD_2, NEW_2))

for tag, old, new in edits:
    ob, nb = b(old), b(new)
    cnt = data.count(ob)
    if cnt != 1:
        print(f"ABORT edit {tag}: expected 1 match, found {cnt}")
        sys.exit(1)
    data = data.replace(ob, nb)
    print(f"edit {tag}: applied")

with open(PATH, "wb") as f:
    f.write(data)
print("WROTE", PATH)
raw = open(PATH, "rb").read()
lf = raw.count(b"\n"); crlf = raw.count(b"\r\n")
print(f"CRLF={crlf} LF={lf} bareLF={lf-crlf}")
