import sys, json, math
import numpy as np
sys.path.insert(0, r"C:\sports-model-bettor")
from engine import distribution_fit as DF
from engine.sports.mlb import prop_picks as PP
from engine.sports.mlb import player_mc as MC

def clean(o):
    if isinstance(o, dict):
        return {k: clean(v) for k, v in o.items() if k not in ("pmf", "cdf", "sf")}
    if isinstance(o, (list, tuple)):
        return [clean(x) for x in o]
    if isinstance(o, bool):
        return o
    if isinstance(o, (np.floating, float)):
        f = float(o)
        if math.isinf(f):
            return "inf" if f > 0 else "-inf"
        if math.isnan(f):
            return "nan"
        return round(f, 9)
    if isinstance(o, (np.integer,)):
        return int(o)
    return o

out = {}

FIT = [("nhl", "sog", "2026-10-01"), ("mlb", "tb", "2026-09-15")]
for sport, stat, sd in FIT:
    try:
        data = DF.pull_observations(sport, stat, since_date=sd)
        fits = DF.fit_all(data)
        out[f"fit_all:{sport}:{stat}"] = {
            "n": int(len(data)),
            "leaderboard": [clean({"name": f["name"], "ll": f["ll"], "aic": f["aic"],
                                   "bic": f["bic"], "ks": f.get("ks"), "params": f["params"]})
                            for f in fits],
            "best": fits[0]["name"] if fits else None,
        }
    except Exception as e:
        out[f"fit_all:{sport}:{stat}"] = {"error": repr(e)}

SUMM = [("nhl", "sog", "2026-10-01"), ("nhl", "g", "2026-10-01"), ("mlb", "tb", "2026-09-15")]
for sport, stat, sd in SUMM:
    try:
        out[f"summarize:{sport}:{stat}"] = clean(DF.summarize_stat(sport, stat, since_date=sd))
    except Exception as e:
        out[f"summarize:{sport}:{stat}"] = {"error": repr(e)}

for sport, stat in [("mlb", "tb"), ("mlb", "hr"), ("nba", "pts"), ("nba", "ftm"),
                    ("nhl", "sog"), ("nhl", "ga"), ("wnba", "pts")]:
    out[f"get_dist:{sport}:{stat}"] = clean(DF.get_distribution(sport, stat))

# COUNT-path probability (what score_player_prop's count branch computes)
rng = np.random.default_rng(42)
samp = np.sort(rng.poisson(1.3, 4000).astype(float))
samples = {"tb": samp}
for L in (0.5, 1.5, 2.5):
    out[f"prob:tb:{L}"] = {"p_over": clean(MC.prob_over(samp, L)),
                           "p_under": clean(MC.prob_under(samp, L))}

# end-to-end real path (deriv-cal ON; deterministic)
for line, oo, uo in [(1.5, -120, 100), (2.5, 160, -200)]:
    prop = {"bet_type": "Batter TB", "line": line, "over_odds": oo, "under_odds": uo}
    out[f"score:tb:{line}"] = clean(PP.score_player_prop(samples, prop))

# end-to-end with deriv-cal forced OFF -> non-null pick w/ real model_prob
import engine.config as _cfg
_orig = _cfg.get_flag
_cfg.get_flag = lambda name, default=None, **kw: (False if name == "ENABLE_DERIV_CALIBRATION" else _orig(name, default, **kw))
try:
    for line, oo, uo in [(1.5, -110, -110), (2.5, 150, -180)]:
        prop = {"bet_type": "Batter TB", "line": line, "over_odds": oo, "under_odds": uo}
        out[f"score_nocal:tb:{line}"] = clean(PP.score_player_prop(samples, prop))
finally:
    _cfg.get_flag = _orig

with open(sys.argv[1], "w", encoding="utf-8") as f:
    json.dump(out, f, indent=2, sort_keys=True)
print("WROTE", sys.argv[1], "n_keys", len(out))
