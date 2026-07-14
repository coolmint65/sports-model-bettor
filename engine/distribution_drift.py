"""
MC-distribution drift detector.

The hardcoded ``_*_STAT_DISTRIBUTIONS`` in ``engine.distribution_fit``
was tuned at a specific point in time (notes in the dict mark each
locked decision). As the player_game_logs table grows, the AIC-best
family or NegBin dispersion can shift. This module detects and
reports that drift without auto-overwriting — the locked dict carries
operator context (bimodal SP/RP notes, "promoted from poisson"
rationale, etc.) that the auto-fit cannot regenerate, so we flag and
let the operator decide.

CLI::

    python -m engine.distribution_drift [--sport mlb|nba|nhl] [--aic-threshold N]

For each (sport, stat) the script:
  1. Pulls fresh observations and runs ``fit_all``.
  2. Compares current AIC-best family + parameters to the locked
     entry's family.
  3. Flags drift when:
       - family changed AND new family beats locked by ≥ aic_threshold
         (default 100 — gives the locked choice meaningful inertia)
       - OR family stayed but NegBin dispersion ``k`` shifted by
         more than ``k_drift_pct`` (default 25%).

Drift entries are printed with the rationale so a operator can update
the locked dict in one pass instead of running per-stat introspection.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


_SPORTS = ("mlb", "nba", "nhl")


def _locked_for(sport: str) -> dict:
    """Pull the hardcoded distribution dict for ``sport``."""
    from .distribution_fit import (
        _MLB_STAT_DISTRIBUTIONS, _NBA_STAT_DISTRIBUTIONS,
        _NHL_STAT_DISTRIBUTIONS,
    )
    return {
        "mlb": _MLB_STAT_DISTRIBUTIONS,
        "nba": _NBA_STAT_DISTRIBUTIONS,
        "nhl": _NHL_STAT_DISTRIBUTIONS,
    }[sport]


def _summarize_all(sport: str) -> list[dict]:
    from .distribution_fit import (
        summarize_all_mlb_stats, summarize_all_nba_stats,
        summarize_all_nhl_stats,
    )
    return {
        "mlb": summarize_all_mlb_stats,
        "nba": summarize_all_nba_stats,
        "nhl": summarize_all_nhl_stats,
    }[sport]()


def _negbin_k(params: dict | None) -> float | None:
    """Pull NegBin dispersion k from a fit's params dict. Different
    fitters label it 'k', 'r', or 'dispersion_k'; check all."""
    if not params:
        return None
    for key in ("dispersion_k", "k", "r"):
        if key in params:
            try:
                return float(params[key])
            except (TypeError, ValueError):
                continue
    return None


_FAMILY_ALIASES = {
    "negbin": "negbin", "negative binomial": "negbin",
    "poisson": "poisson",
    "geometric": "geometric", "geom": "geometric",
    "zip": "zip", "zero-inflated poisson": "zip",
}


def _norm_family(name: str | None) -> str:
    """Lowercase + alias-map family names so the fitter's
    'NegBin'/'Geometric' labels reconcile with the locked dict's
    lowercase keys ('negbin'/'geometric')."""
    if not name:
        return ""
    key = str(name).strip().lower()
    return _FAMILY_ALIASES.get(key, key)


def detect_drift(sport: str, *, aic_threshold: float = 100.0,
                 k_drift_pct: float = 25.0) -> list[dict]:
    """Run a fresh fit and compare against the locked dict. Returns one
    row per stat with a ``drift`` flag and explanation."""
    locked_all = _locked_for(sport)
    summaries = _summarize_all(sport)
    out: list[dict] = []
    for s in summaries:
        stat = s.get("stat")
        if not stat:
            continue
        if "warning" in s:
            out.append({
                "stat": stat, "n": s.get("n", 0),
                "drift": False, "note": s["warning"],
            })
            continue
        leaderboard = s.get("leaderboard") or []
        if not leaderboard:
            out.append({"stat": stat, "n": s["descriptive"]["n"],
                        "drift": False, "note": "no fits"})
            continue
        best = leaderboard[0]
        best_family = _norm_family(best["family"])
        locked = locked_all.get(stat)
        if not locked:
            # Stat not yet in the locked dict — surface so operator
            # can decide whether to add it.
            out.append({
                "stat": stat, "n": s["descriptive"]["n"],
                "drift": True,
                "current_best": best_family,
                "current_aic": best["aic"],
                "locked_family": None,
                "note": "stat not in locked dict",
            })
            continue
        locked_family = _norm_family(locked.get("family"))
        # Find the locked family's row in the leaderboard.
        locked_row = next((r for r in leaderboard
                           if _norm_family(r["family"]) == locked_family), None)
        if locked_row is None:
            out.append({
                "stat": stat, "n": s["descriptive"]["n"],
                "drift": True,
                "current_best": best["family"],
                "current_aic": best["aic"],
                "locked_family": locked_family,
                "locked_aic": None,
                "note": f"locked family {locked_family!r} no longer "
                        f"in leaderboard",
            })
            continue
        delta_aic = locked_row["aic"] - best["aic"]
        # Family-change drift: new winner beats locked by enough AIC.
        family_drift = (best_family != locked_family
                        and delta_aic >= aic_threshold)
        # Dispersion-k drift: only meaningful for NegBin.
        k_drift = False
        k_note = ""
        if locked_family == "negbin" and best_family == "negbin":
            k_locked = locked.get("dispersion_k")
            k_now = _negbin_k(best.get("params") or {})
            if k_locked and k_now:
                pct = abs(k_now - k_locked) / max(0.01, k_locked) * 100.0
                if pct >= k_drift_pct:
                    k_drift = True
                    k_note = (f"k drifted {k_locked:.2f} -> {k_now:.2f} "
                              f"({pct:.0f}%)")
        drift = family_drift or k_drift
        note_parts = []
        if family_drift:
            note_parts.append(f"family {locked_family} -> {best_family} "
                              f"(AIC -{delta_aic:.0f})")
        if k_note:
            note_parts.append(k_note)
        if not drift:
            note_parts.append(
                f"locked {locked_family} OK (AIC gap {delta_aic:+.1f})")
        out.append({
            "stat": stat, "n": s["descriptive"]["n"],
            "drift": drift,
            "current_best": best_family,
            "current_aic": round(best["aic"], 1),
            "locked_family": locked_family,
            "locked_aic": round(locked_row["aic"], 1),
            "delta_aic": round(delta_aic, 1),
            "note": "; ".join(note_parts) if note_parts else "",
        })
    return out


def _print_report(sport: str, rows: list[dict]) -> None:
    print(f"\n=== {sport.upper()} distribution drift ===")
    if not rows:
        print("  (no stats checked)")
        return
    print(f"  {'stat':14s}  {'n':>6s}  {'locked':>8s}  "
          f"{'now':>8s}  {'dAIC':>7s}  flag  note")
    for r in rows:
        flag = "DRIFT" if r["drift"] else "ok"
        locked = r.get("locked_family") or "-"
        nowf = r.get("current_best") or "-"
        delta = r.get("delta_aic", 0)
        delta_s = f"{delta:+7.1f}" if isinstance(delta, (int, float)) else "    -"
        n = r.get("n", 0)
        print(f"  {r['stat']:14s}  {n:>6d}  {locked:>8s}  "
              f"{nowf:>8s}  {delta_s}  {flag:>5s}  {r['note']}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="engine.distribution_drift")
    ap.add_argument("--sport", choices=_SPORTS,
                    help="Limit to one sport. Default: all three.")
    ap.add_argument("--aic-threshold", type=float, default=100.0,
                    help="Family-change drift requires the new family "
                         "to beat the locked family's AIC by this margin "
                         "(default 100). Lower = noisier flags.")
    ap.add_argument("--k-drift-pct", type=float, default=25.0,
                    help="NegBin dispersion drift threshold in percent "
                         "(default 25).")
    args = ap.parse_args(argv)
    sports = (args.sport,) if args.sport else _SPORTS

    print("=" * 78)
    print("  DISTRIBUTION DRIFT -- compare locked _*_STAT_DISTRIBUTIONS to fresh fits")
    print("=" * 78)
    print(f"  Drift threshold: AIC gap >= {args.aic_threshold:.0f} for "
          f"family change, k shift >= {args.k_drift_pct:.0f}% for NegBin")
    print("  Reports DRIFT only -- no auto-update. Locked entries carry")
    print("  operator notes (bimodal pools, ZIP rationale) the auto-fit can't")
    print("  reproduce, so the operator updates the dict by hand.")

    rc = 0
    for sport in sports:
        try:
            rows = detect_drift(
                sport,
                aic_threshold=args.aic_threshold,
                k_drift_pct=args.k_drift_pct,
            )
            _print_report(sport, rows)
            drift_rows = [r for r in rows if r.get("drift")]
            if drift_rows:
                # Persist a structured alert to data/logs/drift_alerts.jsonl
                # so a stale stdout report doesn't bury the signal. Each
                # alert is one line of JSON the operator can grep / wire
                # into a notification later.
                _emit_drift_alerts(sport, drift_rows)
        except Exception as e:
            print(f"  [{sport}] drift check FAILED: {e}")
            rc = 1
    print()
    return rc


def _emit_drift_alerts(sport: str, drift_rows: list) -> None:
    """Append per-stat drift records to data/logs/drift_alerts.jsonl.
    Format: one JSON object per line. Operators can `tail -f` to watch
    or grep for sport-specific alerts."""
    import json as _json
    from pathlib import Path
    log_dir = Path(__file__).resolve().parent.parent / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "drift_alerts.jsonl"
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    with log_path.open("a", encoding="utf-8") as fh:
        for r in drift_rows:
            fh.write(_json.dumps({
                "ts": ts, "sport": sport,
                "stat": r.get("stat"),
                "locked_family": r.get("locked_family"),
                "current_best": r.get("current_best"),
                "delta_aic": r.get("delta_aic"),
                "n": r.get("n"),
                "note": r.get("note"),
            }) + "\n")
    print(f"  -> {len(drift_rows)} alert(s) appended to {log_path}")


if __name__ == "__main__":
    sys.exit(main())
