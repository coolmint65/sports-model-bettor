"""Weekly tennis calibration refit (walk-forward Elo replay + Platt). MintyBot 2026-10-06.
Scheduled via Windows Task Scheduler (TennisCalibRefit). Keeps tennis_calibration.json +
tennis_platt.json fresh so calibration never silently goes stale again (it had sat 142 days)."""
import subprocess, sys, os
R = r"C:\sports-model-bettor"
try:
    subprocess.run([sys.executable, "-m", "engine.tennis_calibrate"], cwd=R, timeout=900)
    print("walk-forward refit done")
except Exception as e:
    print("walk-forward refit err:", e)
sys.path.insert(0, R)
try:
    from engine.tennis_platt_calibration import fit_tennis_platt
    fit_tennis_platt(write=True)
    print("platt refit done")
except Exception as e:
    print("platt refit err:", e)
