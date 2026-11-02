import sys, numpy as np
sys.path.insert(0, r"C:\sports-model-bettor")
from engine import distribution_fit as DF
from engine.sports.mlb import prop_picks as PP

print("=== 1) KIND registry / dispatch ===")
print("get_stat_kind nfl pass_yds :", DF.get_stat_kind("nfl", "pass_yds"))
print("get_stat_kind nfl rush_yds :", DF.get_stat_kind("nfl", "rush_yds"))
print("get_stat_kind mlb tb       :", DF.get_stat_kind("mlb", "tb"))
print("get_distribution nfl pass_yds (registry->continuous):", DF.get_distribution("nfl", "pass_yds"))
print("get_distribution mlb tb     (count, unchanged)      :", DF.get_distribution("mlb", "tb"))
print("get_distribution nba pts kind='continuous' (override):", DF.get_distribution("nba", "pts", kind="continuous"))
DF.register_stat_kind("nfl", "fake_yds", "continuous")
print("register_stat_kind -> get_stat_kind nfl fake_yds:", DF.get_stat_kind("nfl", "fake_yds"))

print("\n=== 2) _fit_normal / _fit_lognormal on a Normal(250,45) sample ===")
rng = np.random.default_rng(7)
norm_samp = rng.normal(250.0, 45.0, 6000)
print("sample mean/std/min:", round(float(norm_samp.mean()),2), round(float(norm_samp.std()),2), round(float(norm_samp.min()),2))
fits = DF.fit_all_continuous(norm_samp)
for f in fits:
    print(f"  {f['name']:9s} aic={f['aic']:.1f} ks={f['ks']:.4f} params={ {k:round(v,4) for k,v in f['params'].items()} }")
print("best (lowest AIC):", fits[0]["name"])

print("\n=== 3) right-skewed LogNormal sample -> LogNormal should win ===")
ln_samp = rng.lognormal(mean=4.0, sigma=0.5, size=6000)   # positive, skewed
print("sample mean/median/min:", round(float(ln_samp.mean()),2), round(float(np.median(ln_samp)),2), round(float(ln_samp.min()),3))
fits2 = DF.fit_all_continuous(ln_samp)
for f in fits2:
    print(f"  {f['name']:9s} aic={f['aic']:.1f} ks={f['ks']:.4f}")
print("best:", fits2[0]["name"])

print("\n=== 4) sample WITH zeros -> LogNormal must drop out (inf aic), Normal still fits ===")
zero_samp = np.concatenate([rng.normal(60, 20, 3000).clip(min=0), np.zeros(400)])
fits3 = DF.fit_all_continuous(zero_samp)
for f in fits3:
    print(f"  {f['name']:9s} aic={f['aic'] if np.isfinite(f['aic']) else 'inf'} ks={f['ks'] if np.isfinite(f['ks']) else 'inf'}")

print("\n=== 5) monotonic P(over) via _continuous_over_under (Normal mean=250) ===")
for line in [150, 200, 250, 300, 350]:
    po, pu = PP._continuous_over_under(norm_samp, line)
    print(f"  line={line:3d}  P(over)={po:.4f}  P(under)={pu:.4f}  sum={po+pu:.4f}")

print("\n=== 6) score_player_prop(kind='continuous') end-to-end (deriv-cal OFF for clean prob) ===")
import engine.config as cfg
_orig = cfg.get_flag
cfg.get_flag = lambda name, default=None, **kw: (False if name == "ENABLE_DERIV_CALIBRATION" else _orig(name, default, **kw))
try:
    samples = {"tb": norm_samp}
    for line in [200, 250, 300]:
        prop = {"bet_type": "Batter TB", "line": line, "over_odds": -110, "under_odds": -110}
        r = PP.score_player_prop(samples, prop, kind="continuous")
        print(f"  line={line}: {r}")
    # confirm count path still available on same call without kind=
    r_count = PP.score_player_prop({"tb": np.sort(rng.poisson(1.3,4000).astype(float))},
                                   {"bet_type":"Batter TB","line":1.5,"over_odds":-110,"under_odds":-110})
    print("  (same fn, no kind -> COUNT path):", r_count)
finally:
    cfg.get_flag = _orig
