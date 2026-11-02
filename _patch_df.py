import sys
PATH = r"C:\sports-model-bettor\engine\distribution_fit.py"

def b(s: str) -> bytes:
    # file is uniformly CRLF; anchors/new text written with \n here
    return s.replace("\n", "\r\n").encode("utf-8")

with open(PATH, "rb") as f:
    data = f.read()

edits = []

# ---- A: insert continuous block + extend summarize_stat signature ----
OLD_A = """    return sorted(results, key=lambda r: r["aic"])


def summarize_stat(sport: str, stat_key: str,
                   min_games_per_player: int = 5,
                   since_date: str | None = None) -> dict:
"""
NEW_A = '''    return sorted(results, key=lambda r: r["aic"])


# \u2500\u2500 Continuous families (parallel path, Phase 1) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
# COUNT stats (above) use pmf-based fitters + the discrete K-S in
# ``_ks_test``. CONTINUOUS stats (e.g. NFL passing / rushing /
# receiving YARDS) need PDF/CDF-based fitters and a continuous K-S.
# This is a PARALLEL path: it never runs or mutates the count fitters,
# ``_CANDIDATES``, ``fit_all`` or ``_ks_test``, so every existing count
# caller is behavior-identical. Continuous fit dicts mirror the count
# fit dict shape (name, ll, aic, bic, params) and additionally carry
# ``cdf`` / ``sf`` callables (the continuous analogue of the count
# ``pmf``) so scoring can read a survival / CDF directly. ``pmf`` is
# kept as None on continuous dicts for shape parity.


def _fit_normal(data) -> dict:
    """Gaussian MLE fit \u2014 symmetric baseline for a continuous stat."""
    n = len(data)
    if n == 0:
        return {"name": "Normal", "ll": -float("inf"), "aic": float("inf"),
                "bic": float("inf"), "params": {}, "pmf": None,
                "cdf": None, "sf": None}
    mu, sigma = _stats.norm.fit(data)
    if not (sigma > 0) or not math.isfinite(sigma):
        return {"name": "Normal", "ll": -float("inf"), "aic": float("inf"),
                "bic": float("inf"), "params": {}, "pmf": None,
                "cdf": None, "sf": None}
    ll = float(np.sum(_stats.norm.logpdf(data, mu, sigma)))
    k = 2
    return {
        "name": "Normal",
        "ll": ll,
        "aic": 2 * k - 2 * ll,
        "bic": k * math.log(n) - 2 * ll,
        "params": {"mu": float(mu), "sigma": float(sigma)},
        "pmf": None,
        "cdf": lambda x, _m=mu, _s=sigma: _stats.norm.cdf(x, _m, _s),
        "sf": lambda x, _m=mu, _s=sigma: _stats.norm.sf(x, _m, _s),
    }


def _fit_lognormal(data) -> dict:
    """Log-normal MLE fit (loc fixed at 0). Right-skewed and strictly
    positive \u2014 the natural shape for yards-type stats. Only valid when
    every observation is > 0; otherwise returns a non-candidate (inf
    AIC) so it drops out of the leaderboard."""
    n = len(data)
    if n == 0 or float(np.min(data)) <= 0:
        return {"name": "LogNormal", "ll": -float("inf"), "aic": float("inf"),
                "bic": float("inf"), "params": {}, "pmf": None,
                "cdf": None, "sf": None}
    shape, loc, scale = _stats.lognorm.fit(data, floc=0)
    if not (shape > 0 and scale > 0) or not (math.isfinite(shape) and math.isfinite(scale)):
        return {"name": "LogNormal", "ll": -float("inf"), "aic": float("inf"),
                "bic": float("inf"), "params": {}, "pmf": None,
                "cdf": None, "sf": None}
    ll = float(np.sum(_stats.lognorm.logpdf(data, shape, loc=0, scale=scale)))
    k = 2
    return {
        "name": "LogNormal",
        "ll": ll,
        "aic": 2 * k - 2 * ll,
        "bic": k * math.log(n) - 2 * ll,
        "params": {"sigma_log": float(shape), "scale": float(scale),
                   "mu_log": float(math.log(scale))},
        "pmf": None,
        "cdf": lambda x, _sh=shape, _sc=scale: _stats.lognorm.cdf(x, _sh, loc=0, scale=_sc),
        "sf": lambda x, _sh=shape, _sc=scale: _stats.lognorm.sf(x, _sh, loc=0, scale=_sc),
    }


def _ks_test_continuous(data, cdf) -> float:
    """One-sample K-S statistic against a fitted CONTINUOUS CDF via
    ``scipy.stats.kstest``. Returns the D statistic (lower = better),
    parallel to the discrete ``_ks_test``. Kept separate from
    ``_ks_test`` \u2014 that one enumerates integer k, which is wrong for
    continuous data. ``cdf`` must accept a numpy array (scipy passes
    the sorted sample in)."""
    if cdf is None:
        return float("inf")
    try:
        res = _stats.kstest(np.asarray(data, dtype=float), cdf)
        return float(res.statistic)
    except Exception as e:
        logger.warning("continuous K-S failed: %s", e)
        return float("inf")


_CONTINUOUS_CANDIDATES = [_fit_normal, _fit_lognormal]


def fit_all_continuous(data) -> list[dict]:
    """Continuous analogue of ``fit_all``: fit every continuous
    candidate, score goodness-of-fit with the continuous K-S, and
    return the list sorted by AIC (lowest first). Does NOT run or
    alter the count candidates."""
    results = []
    for fitter in _CONTINUOUS_CANDIDATES:
        try:
            r = fitter(data)
        except Exception as e:
            logger.warning("%s failed: %s", fitter.__name__, e)
            continue
        if r.get("cdf") is not None:
            r["ks"] = _ks_test_continuous(data, r["cdf"])
        else:
            r["ks"] = float("inf")
        results.append(r)
    return sorted(results, key=lambda r: r["aic"])


# \u2500\u2500 Stat KIND registry \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
# Marks a (sport, stat_key) as "continuous" vs "count". DEFAULT is
# "count", so every existing stat keeps the discrete path untouched \u2014
# only continuous stats need an entry. NFL yards are registered now so
# the dispatch is exercisable, but NFL is NOT migrated in Phase 1: no
# live scorer routes through the continuous branch yet (NFL's own
# picker still owns its scoring).
_STAT_KIND: dict[tuple[str, str], str] = {
    ("nfl", "pass_yds"): "continuous",
    ("nfl", "rush_yds"): "continuous",
    ("nfl", "rec_yds"): "continuous",
}


def get_stat_kind(sport: str, stat_key: str) -> str:
    """Return "continuous" or "count" (the default) for a stat."""
    return _STAT_KIND.get((sport, stat_key), "count")


def register_stat_kind(sport: str, stat_key: str, kind: str) -> None:
    """Register / override a stat's kind at runtime (Phase 2 NFL)."""
    _STAT_KIND[(sport, stat_key)] = kind


# Locked continuous family choice per (sport, stat), the continuous
# analogue of the count ``_*_STAT_DISTRIBUTIONS`` tables. Yards are
# right-skewed and positive so log-normal is the default shape. Inert
# until a continuous stat is actually consumed (Phase 2).
_CONTINUOUS_STAT_DISTRIBUTIONS: dict[tuple[str, str], dict] = {
    ("nfl", "pass_yds"): {"family": "lognormal", "kind": "continuous"},
    ("nfl", "rush_yds"): {"family": "lognormal", "kind": "continuous"},
    ("nfl", "rec_yds"): {"family": "lognormal", "kind": "continuous"},
}


def summarize_stat(sport: str, stat_key: str,
                   min_games_per_player: int = 5,
                   since_date: str | None = None,
                   kind: str | None = None) -> dict:
'''
edits.append(("A", OLD_A, NEW_A))

# ---- B: fit dispatch inside summarize_stat ----
OLD_B = """    desc["overdispersion"] = round(desc["var"] / desc["mean"], 2) if desc["mean"] > 0 else float("inf")
    fits = fit_all(data)
"""
NEW_B = """    desc["overdispersion"] = round(desc["var"] / desc["mean"], 2) if desc["mean"] > 0 else float("inf")
    eff_kind = kind if kind is not None else get_stat_kind(sport, stat_key)
    fits = fit_all_continuous(data) if eff_kind == "continuous" else fit_all(data)
"""
edits.append(("B", OLD_B, NEW_B))

# ---- C: get_distribution signature + continuous branch ----
OLD_C = '''def get_distribution(sport: str, stat_key: str) -> dict | None:
    """Locked per-stat distribution choice for the MC sampler. Returns
    ``{family, dispersion_k?, note?}`` or None when stat isn't known."""
    if sport == "mlb":
'''
NEW_C = '''def get_distribution(sport: str, stat_key: str,
                     kind: str | None = None) -> dict | None:
    """Locked per-stat distribution choice for the MC sampler. Returns
    ``{family, dispersion_k?, note?}`` or None when stat isn't known.

    Dispatches by stat KIND: a (sport, stat_key) registered continuous
    (or an explicit ``kind="continuous"``) returns a continuous family
    choice from ``_CONTINUOUS_STAT_DISTRIBUTIONS``; everything else
    takes the existing COUNT lookup below, unchanged."""
    eff_kind = kind if kind is not None else get_stat_kind(sport, stat_key)
    if eff_kind == "continuous":
        return _CONTINUOUS_STAT_DISTRIBUTIONS.get(
            (sport, stat_key), {"family": "lognormal", "kind": "continuous"})
    if sport == "mlb":
'''
edits.append(("C", OLD_C, NEW_C))

# ---- D: __all__ exports ----
OLD_D = '''    "fit_all", "pull_observations",
    "get_distribution", "get_prob_shrink",
    "save_learned_shrinkage", "reload_learned_shrinkage",
    "_MLB_STAT_DISTRIBUTIONS", "_NBA_STAT_DISTRIBUTIONS", "_NHL_STAT_DISTRIBUTIONS",
    "_PROB_SHRINK",
]
'''
NEW_D = '''    "fit_all", "fit_all_continuous", "pull_observations",
    "get_distribution", "get_prob_shrink",
    "get_stat_kind", "register_stat_kind",
    "save_learned_shrinkage", "reload_learned_shrinkage",
    "_MLB_STAT_DISTRIBUTIONS", "_NBA_STAT_DISTRIBUTIONS", "_NHL_STAT_DISTRIBUTIONS",
    "_CONTINUOUS_STAT_DISTRIBUTIONS", "_STAT_KIND",
    "_PROB_SHRINK",
]
'''
edits.append(("D", OLD_D, NEW_D))

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
# sanity: no bare LF introduced
raw = open(PATH, "rb").read()
lf = raw.count(b"\n")
crlf = raw.count(b"\r\n")
print(f"CRLF={crlf} LF={lf} bareLF={lf-crlf}")
