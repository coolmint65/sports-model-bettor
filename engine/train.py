"""
Consolidated training / calibration report.

One command that runs every calibration tool in sequence and produces
a single scannable report with auto-suggested config changes.

Usage:
    python -m engine.train              # all sports
    python -m engine.train mlb          # MLB only
    python -m engine.train nhl
    python -m engine.train nba

What it runs:
    1. Overall tracker W/L + flip-diagnostic (sign-error check)
    2. Per-bet-type WR
    3. Per-edge-bucket WR (is high conviction paying off?)
    4. Direction splits (home/away, fav/dog, over/under, NRFI/YRFI)
    5. Probability-bucket calibration (is 70% predicted ~70% actual?)
    6. Factor ablation highlights (NBA/MLB)
    7. Actionable RECOMMENDATIONS section at the end listing exactly
       which config.py constants the data suggests changing.

The recommendations are thresholded: only printed when the bias is
clear enough to justify a change on this sample. Small-sample noise
is suppressed with minimum-N gates.
"""

import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from statistics import mean, pstdev

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent / "data"
DB_PATHS = {
    "mlb": _ROOT / "mlb.db",
    "nhl": _ROOT / "nhl.db",
    "nba": _ROOT / "nba.db",
}
PICKS_TABLE = {
    "mlb": "picks",
    "nhl": "nhl_picks",
    "nba": "nba_picks",
}

MIN_N_BUCKET = 8   # minimum samples before calling a bucket a signal
MIN_N_FLIP = 30    # min picks before we diagnose a sign error


def _canon(v) -> str:
    if v is None:
        return ""
    s = str(v).strip().lower()
    if s in ("win", "w", "hit", "1", "true", "yes"):
        return "win"
    if s in ("loss", "lose", "l", "miss", "0", "false", "no"):
        return "loss"
    if s in ("push", "p", "tie", "draw"):
        return "push"
    return s


def _american_to_profit(odds, won: bool) -> float:
    try:
        o = int(odds)
    except (TypeError, ValueError):
        return 0.0
    if won:
        return float(o) if o > 0 else 100 / abs(o) * 100
    return -100.0


def _wr(w: int, l: int) -> float:
    return 100.0 * w / (w + l) if (w + l) else 0.0


def _roi(w_records: list[tuple[int, bool]]) -> float:
    """w_records is list of (odds, won_bool). Returns ROI % on $100 flat bets."""
    if not w_records:
        return 0.0
    total = 0.0
    for odds, won in w_records:
        total += _american_to_profit(odds, won)
    return total / len(w_records)


# ── Report sections ──

def _load_settled(sport: str) -> list[sqlite3.Row]:
    db = DB_PATHS[sport]
    if not db.exists():
        return []
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    try:
        cols = "bet_type, pick, model_prob, edge, odds, result, matchup, profit"
        try:
            rows = conn.execute(
                f"SELECT {cols} FROM {PICKS_TABLE[sport]} WHERE result IS NOT NULL"
            ).fetchall()
        except sqlite3.Error:
            # profit column may not exist on older schemas
            cols = "bet_type, pick, model_prob, edge, odds, result, matchup"
            rows = conn.execute(
                f"SELECT {cols} FROM {PICKS_TABLE[sport]} WHERE result IS NOT NULL"
            ).fetchall()
        return list(rows)
    finally:
        conn.close()


def section_overall(settled: list, out: list, recs: list) -> None:
    w = l = p = 0
    profit_total = 0.0
    odds_pnl = []
    for r in settled:
        c = _canon(r["result"])
        if c == "win":
            w += 1
        elif c == "loss":
            l += 1
        elif c == "push":
            p += 1
        # Prefer stored profit when available
        try:
            profit_total += float(r["profit"] or 0.0)
        except (IndexError, KeyError, TypeError):
            odds_pnl.append((r["odds"], c == "win"))

    total = w + l + p
    wr = _wr(w, l)
    flipped_wr = _wr(l, w)
    if profit_total == 0.0 and odds_pnl:
        profit_total = sum(_american_to_profit(o, won) for o, won in odds_pnl)
    roi = (profit_total / max(1, w + l)) if (w + l) else 0.0

    out.append("## 1. OVERALL TRACKER")
    out.append(f"   Settled: {total}    Actual: {w}W-{l}L-{p}P   WR={wr:.1f}%   ROI={roi:+.1f}%")
    out.append(f"   Flipped scenario:   {l}W-{w}L-{p}P   WR={flipped_wr:.1f}%")
    if total >= MIN_N_FLIP and flipped_wr >= 58:
        out.append(f"   >> SIGN-ERROR ALERT: flipping every pick would produce {flipped_wr:.1f}% WR.")
        recs.append("CRITICAL: investigate a potential inverted factor (flip WR >= 58%). "
                    "Run mlb_game_diagnose / nba_diagnose on a low-WR matchup to localize.")
    elif total >= MIN_N_FLIP and wr <= 42:
        out.append(f"   >> POSSIBLE SIGN ERROR: WR {wr:.1f}% is significantly below 50%.")
        recs.append(f"Investigate overall WR {wr:.1f}% — may indicate factor miscalibration.")
    out.append("")


def section_by_type(settled: list, out: list, recs: list) -> None:
    try:
        from ._analysis_common import canon_bet_type
    except Exception:
        canon_bet_type = lambda x: x

    by_type: dict[str, list] = {}
    for r in settled:
        c = _canon(r["result"])
        if c not in ("win", "loss"):
            continue
        bt = canon_bet_type(r["bet_type"]) or "?"
        row = by_type.setdefault(bt, [0, 0, []])
        if c == "win":
            row[0] += 1
        else:
            row[1] += 1
        row[2].append((r["odds"], c == "win"))

    out.append("## 2. BY BET TYPE")
    out.append(f"   {'Type':<10} {'W-L':>8} {'WR':>7} {'ROI':>7}")
    for bt, (w, l, pnl) in sorted(by_type.items()):
        if w + l < 3:
            continue
        out.append(f"   {bt:<10} {f'{w}-{l}':>8} {_wr(w, l):>6.1f}% {_roi(pnl):>+6.1f}%")
        # Recommendations
        if w + l >= MIN_N_BUCKET and _wr(w, l) < 42:
            recs.append(
                f"{bt} has {w}-{l} ({_wr(w, l):.1f}% WR). Lower its BET_RELIABILITY "
                f"weight or disable via config if sustained for another {MIN_N_BUCKET} picks."
            )
    out.append("")


def section_edge_buckets(settled: list, out: list, recs: list) -> None:
    buckets = [("0-2%", 0, 2), ("2-4%", 2, 4), ("4-6%", 4, 6),
               ("6-10%", 6, 10), ("10%+", 10, 9999)]
    out.append("## 3. EDGE BUCKETS")
    out.append(f"   {'Edge':<7} {'W-L':>8} {'WR':>7} {'ROI':>7}")
    low_edge_data = None
    for name, lo, hi in buckets:
        w = l = 0
        pnl = []
        for r in settled:
            c = _canon(r["result"])
            if c not in ("win", "loss"):
                continue
            e = r["edge"] or 0
            if not (lo <= e < hi):
                continue
            if c == "win":
                w += 1
            else:
                l += 1
            pnl.append((r["odds"], c == "win"))
        if w + l == 0:
            continue
        out.append(f"   {name:<7} {f'{w}-{l}':>8} {_wr(w, l):>6.1f}% {_roi(pnl):>+6.1f}%")
        if name in ("0-2%", "2-4%") and w + l >= MIN_N_BUCKET:
            low_edge_data = (name, w, l, _roi(pnl))

    if low_edge_data:
        name, w, l, roi = low_edge_data
        if roi < -5 and _wr(w, l) < 45:
            recs.append(
                f"EDGE_SKIP: sub-4% bucket went {w}-{l} ({_wr(w, l):.1f}% WR, {roi:+.1f}% ROI). "
                f"Raise EDGE_SKIP from 4.0 to 5.0 in engine/config.py."
            )
    out.append("")


def section_direction_splits(settled: list, out: list, recs: list,
                              sport: str) -> None:
    try:
        from ._analysis_common import canon_bet_type
    except Exception:
        canon_bet_type = lambda x: x

    out.append("## 4. DIRECTION SPLITS")

    # ML home/away
    ml_h_w = ml_h_l = ml_a_w = ml_a_l = 0
    for r in settled:
        if canon_bet_type(r["bet_type"]) != "ML":
            continue
        c = _canon(r["result"])
        if c not in ("win", "loss"):
            continue
        parts = (r["matchup"] or "").split(" @ ")
        if len(parts) != 2:
            continue
        away, home = parts[0].strip(), parts[1].strip()
        pick = (r["pick"] or "").strip()
        if pick == home:
            ml_h_w += 1 if c == "win" else 0
            ml_h_l += 1 if c == "loss" else 0
        elif pick == away:
            ml_a_w += 1 if c == "win" else 0
            ml_a_l += 1 if c == "loss" else 0

    if ml_h_w + ml_h_l + ml_a_w + ml_a_l:
        out.append(f"   ML home: {ml_h_w}-{ml_h_l}  WR={_wr(ml_h_w, ml_h_l):.1f}%   "
                   f"ML away: {ml_a_w}-{ml_a_l}  WR={_wr(ml_a_w, ml_a_l):.1f}%")
        gap = _wr(ml_a_w, ml_a_l) - _wr(ml_h_w, ml_h_l)
        if (ml_h_w + ml_h_l) >= MIN_N_BUCKET and (ml_a_w + ml_a_l) >= MIN_N_BUCKET \
                and abs(gap) > 10:
            which = "home" if _wr(ml_h_w, ml_h_l) < _wr(ml_a_w, ml_a_l) else "away"
            recs.append(
                f"ML {which} picks underperforming by {abs(gap):.1f}pp. "
                f"{'Lower MLB_HOME_EDGE.' if sport == 'mlb' else 'Investigate home-edge tuning.'}"
            )

    # RL favorite/underdog (MLB only + NHL PL)
    rl_f_w = rl_f_l = rl_d_w = rl_d_l = 0
    rl_types = {"mlb": "RL", "nhl": "PL", "nba": "Q1_SPREAD"}
    target_type = rl_types.get(sport, "RL")
    for r in settled:
        if canon_bet_type(r["bet_type"]) != target_type:
            continue
        c = _canon(r["result"])
        if c not in ("win", "loss"):
            continue
        pick = (r["pick"] or "").strip()
        if "-1.5" in pick or " -1.5" in pick:
            rl_f_w += 1 if c == "win" else 0
            rl_f_l += 1 if c == "loss" else 0
        elif "+1.5" in pick or " +1.5" in pick:
            rl_d_w += 1 if c == "win" else 0
            rl_d_l += 1 if c == "loss" else 0

    if rl_f_w + rl_f_l + rl_d_w + rl_d_l:
        out.append(f"   {target_type} fav:  {rl_f_w}-{rl_f_l}  WR={_wr(rl_f_w, rl_f_l):.1f}%   "
                   f"{target_type} dog:  {rl_d_w}-{rl_d_l}  WR={_wr(rl_d_w, rl_d_l):.1f}%")
        if (rl_f_w + rl_f_l) >= MIN_N_BUCKET and _wr(rl_f_w, rl_f_l) < 40 and sport == "mlb":
            recs.append(
                f"MLB_ALLOW_RL_FAVORITE: -1.5 favs {rl_f_w}-{rl_f_l} "
                f"({_wr(rl_f_w, rl_f_l):.1f}% WR). Keep flag OFF."
            )

    # Over/Under
    ou_o_w = ou_o_l = ou_u_w = ou_u_l = 0
    ou_types = {"mlb": "O/U", "nhl": "O/U", "nba": "Q1_TOTAL"}
    ou_type = ou_types.get(sport, "O/U")
    for r in settled:
        if canon_bet_type(r["bet_type"]) != ou_type:
            continue
        c = _canon(r["result"])
        if c not in ("win", "loss"):
            continue
        pick = (r["pick"] or "").lower()
        if "over" in pick:
            ou_o_w += 1 if c == "win" else 0
            ou_o_l += 1 if c == "loss" else 0
        elif "under" in pick:
            ou_u_w += 1 if c == "win" else 0
            ou_u_l += 1 if c == "loss" else 0

    if ou_o_w + ou_o_l + ou_u_w + ou_u_l:
        out.append(f"   {ou_type} Over:  {ou_o_w}-{ou_o_l}  WR={_wr(ou_o_w, ou_o_l):.1f}%   "
                   f"{ou_type} Under: {ou_u_w}-{ou_u_l}  WR={_wr(ou_u_w, ou_u_l):.1f}%")
    out.append("")


def section_calibration(settled: list, out: list, recs: list,
                         sport: str) -> None:
    out.append("## 5. PROBABILITY CALIBRATION")
    out.append(f"   {'Bucket':<12} {'N':>5} {'Avg predicted':>14} {'Actual WR':>11}")
    buckets = [(0.50, 0.55), (0.55, 0.60), (0.60, 0.65), (0.65, 0.70), (0.70, 1.00)]
    miscal = None
    for lo, hi in buckets:
        w = l = 0
        probs = []
        for r in settled:
            c = _canon(r["result"])
            if c not in ("win", "loss"):
                continue
            p = r["model_prob"] or 0
            if not (lo <= p < hi):
                continue
            probs.append(p)
            if c == "win":
                w += 1
            else:
                l += 1
        if w + l < 5:
            continue
        avg_pred = mean(probs) * 100 if probs else 0
        actual = _wr(w, l)
        label = f"{int(lo * 100)}-{int(hi * 100)}%"
        out.append(f"   {label:<12} {w + l:>5d} {avg_pred:>13.1f}% {actual:>10.1f}%")
        if lo >= 0.65 and w + l >= MIN_N_BUCKET and avg_pred - actual > 10:
            miscal = (label, avg_pred, actual)

    if miscal:
        label, pred, actual = miscal
        gap = pred - actual
        if sport == "mlb":
            recs.append(
                f"MLB_WIN_PROB_CAP: bucket {label} predicted {pred:.1f}% / actual "
                f"{actual:.1f}% (off by {gap:.1f}pp). Tighten cap."
            )
        elif sport == "nba":
            recs.append(
                f"NBA Q1 overconfidence: bucket {label} predicted {pred:.1f}% / actual "
                f"{actual:.1f}%. Widen Q1_STD_DEV in engine/nba_q1_predict.py."
            )
    out.append("")


def section_factor_hints(sport: str, out: list, recs: list) -> None:
    """Quick per-sport factor sanity summary (not the full ablation;
    that takes minutes for large pick histories)."""
    out.append("## 6. FACTOR STATE")
    try:
        from . import config as cfg
    except Exception:
        return

    if sport == "mlb":
        flags = [
            "MLB_ENABLE_SITUATIONAL_FACTORS", "MLB_ENABLE_TEAM_CAL",
            "MLB_ENABLE_BULLPEN_FATIGUE", "MLB_ENABLE_SITUATIONAL_AGG",
            "MLB_ENABLE_UMPIRE_FACTOR", "MLB_ENABLE_WEATHER_ADJ",
            "MLB_ENABLE_TRAVEL_FATIGUE", "MLB_ENABLE_MATCHUP_INTERACTION",
            "MLB_ENABLE_COORS_BOOST", "MLB_ENABLE_PLATOON_DUP",
            "MLB_ENABLE_H2H_VS_PITCHER", "MLB_ENABLE_LINEUP_STRENGTH",
        ]
    elif sport == "nhl":
        flags = ["NHL_ENABLE_GRANULAR_FACTORS"]
    elif sport == "nba":
        flags = ["NBA_ENABLE_ROSTER_ADJUSTMENT"]
    else:
        flags = []

    for flag in flags:
        if hasattr(cfg, flag):
            state = "ON " if getattr(cfg, flag) else "OFF"
            short = flag.replace(f"{sport.upper()}_ENABLE_", "").replace("MLB_ENABLE_", "")
            out.append(f"   [{state}] {short}")

    out.append(
        f"   Run 'python -m engine.factor_backtest {sport}' for the full per-factor\n"
        f"   ablation study with WR deltas."
    )
    out.append("")


def section_baselines(sport: str, out: list, recs: list) -> None:
    """Sport-specific baseline calibration (NBA Q1 numbers, etc)."""
    if sport == "nba":
        try:
            from .nba_calibration import calibrate
            result = calibrate()
            if result and "error" not in result:
                out.append("## 7. NBA Q1 BASELINES")
                ha = result.get("home_q1_advantage", {})
                qt = result.get("q1_totals", {})
                qv = result.get("q1_variance", {})
                out.append(f"   Home Q1 margin: actual {ha.get('actual_avg_margin', 0):+.2f} "
                           f"(model {ha.get('model_assumes', 0):+.2f}, err {ha.get('error', 0):+.2f})")
                out.append(f"   Q1 total:       actual {qt.get('actual_avg_total', 0):.1f} "
                           f"(model {qt.get('model_baseline', 0):.1f}, err {qt.get('bias', 0):+.1f})")
                out.append(f"   Q1 margin std:  actual {qv.get('actual_margin_std_dev', 0):.2f} "
                           f"(model {qv.get('model_assumes', 0):.2f}, err {qv.get('error', 0):+.2f})")
                for rec in result.get("recommendations", []):
                    if "calibration looks good" not in rec.lower():
                        recs.append(f"NBA Q1: {rec}")
                out.append("")
        except Exception as e:
            logger.debug("NBA baseline calc failed: %s", e)


# ── Main ──

def run_sport(sport: str, structured_recs: list | None = None) -> list[str]:
    """Build the report sections for one sport.

    When ``structured_recs`` is passed (a list), each direction-split
    bucket below the binomial threshold gets appended as a dict so the
    --apply path can flip flags via engine.model_overrides without
    reparsing the human-readable strings.
    """
    out = [f"\n{'=' * 72}",
           f"  TRAINING REPORT - {sport.upper()}",
           f"{'=' * 72}\n"]
    recs: list[str] = []

    settled = _load_settled(sport)
    if not settled:
        out.append(f"  No settled picks yet for {sport.upper()}.")
        out.append(f"  Run sync_{sport}.bat and let the tracker accumulate data first.")
        return out

    section_overall(settled, out, recs)
    section_by_type(settled, out, recs)
    section_edge_buckets(settled, out, recs)
    section_direction_splits(settled, out, recs, sport)
    section_calibration(settled, out, recs, sport)
    section_factor_hints(sport, out, recs)
    section_baselines(sport, out, recs)

    if structured_recs is not None and sport == "mlb":
        # Walk the settled picks again to build {flag, value, n, p} dicts
        # for the direction allow-flags; auto-apply only handles MLB
        # direction filters in this iteration.
        structured_recs.extend(_structured_direction_recs(settled, sport))

    out.append(f"{'=' * 72}")
    out.append(f"  RECOMMENDATIONS ({sport.upper()})")
    out.append(f"{'=' * 72}")
    if recs:
        for i, r in enumerate(recs, 1):
            # Word-wrap long lines at 70 chars for readability
            out.append(f"  {i}. {r}")
    else:
        out.append("  No config changes recommended. Model calibration within tolerance.")
    out.append("")
    return out


def _structured_direction_recs(settled: list, sport: str) -> list[dict]:
    """Compute auto-apply candidates for the direction allow-flags.

    Returns a list of dicts with shape:
      {flag: 'MLB_ALLOW_OU_UNDER', value: False,
       n: 42, wins: 13, p_value: 0.004,
       reason: 'Under WR 31.0% over 42 picks, p=0.004'}

    Only emits dicts for buckets where:
      - N >= MIN_SAMPLES (engine.model_overrides.MIN_SAMPLES)
      - WR < the implied break-even (52.4% for -110)
      - p_value < MAX_P_VALUE
    The actual write happens in main() under --apply with all guard
    rails (max-per-run cap, audit log) intact.
    """
    if sport != "mlb":
        return []
    try:
        from .model_overrides import binomial_p_value, MIN_SAMPLES, MAX_P_VALUE
        from ._analysis_common import canon_bet_type
    except Exception:
        return []

    # Tally each (bet-type, side) bucket. Keyed by the flag we'd flip.
    buckets: dict[str, dict[str, int]] = {}

    def _bump(flag: str, won: bool):
        b = buckets.setdefault(flag, {"wins": 0, "losses": 0})
        if won:
            b["wins"] += 1
        else:
            b["losses"] += 1

    for r in settled:
        bt = canon_bet_type(r.get("bet_type", ""))
        c = _canon(r.get("result"))
        if c not in ("win", "loss"):
            continue
        won = (c == "win")
        pick = (r.get("pick") or "").strip()
        if bt == "RL":
            if "-1.5" in pick or " -1.5" in pick:
                _bump("MLB_ALLOW_RL_FAVORITE", won)
            elif "+1.5" in pick or " +1.5" in pick:
                _bump("MLB_ALLOW_RL_UNDERDOG", won)
        elif bt == "O/U":
            pl = pick.lower()
            if "over" in pl:
                _bump("MLB_ALLOW_OU_OVER", won)
            elif "under" in pl:
                _bump("MLB_ALLOW_OU_UNDER", won)
        elif bt == "1st INN":
            if pick == "NRFI":
                _bump("MLB_ALLOW_NRFI", won)
            elif pick == "YRFI":
                _bump("MLB_ALLOW_YRFI", won)

    out = []
    for flag, b in buckets.items():
        n = b["wins"] + b["losses"]
        if n < MIN_SAMPLES:
            continue
        wr = b["wins"] / n
        if wr >= 0.524:
            # At or above break-even: don't auto-disable.
            continue
        p = binomial_p_value(b["wins"], n, p_null=0.524)
        if p >= MAX_P_VALUE:
            continue
        out.append({
            "flag": flag,
            "value": False,
            "n": n,
            "wins": b["wins"],
            "losses": b["losses"],
            "p_value": round(p, 4),
            "reason": f"WR {wr * 100:.1f}% over {n} picks, p={p:.4f}",
        })
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sport", nargs="?", default="all",
                        choices=["mlb", "nhl", "nba", "all"])
    parser.add_argument(
        "--apply", action="store_true",
        help=("Auto-apply qualifying recommendations to model_overrides. "
              "Guard rails: N >= MIN_SAMPLES, p < MAX_P_VALUE, direction "
              "= disable only, max " "MAX_OVERRIDES_PER_RUN per call. "
              "Overrides expire after EXPIRY_DAYS so a one-off cold "
              "streak doesn't lock in forever."),
    )
    parser.add_argument(
        "--revert", metavar="FLAG",
        help="Manually clear an active override (e.g. MLB_ALLOW_OU_UNDER).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.apply else logging.WARNING,
                        format="%(levelname)-7s %(message)s")

    if args.revert:
        from .model_overrides import revert_override
        # Try each sport until one matches; the same flag name shouldn't
        # collide across sports because we prefix with MLB_ / NHL_ / NBA_.
        for s in ("mlb", "nhl", "nba"):
            if revert_override(s, args.revert):
                print(f"Reverted {s}.{args.revert}")
                return
        print(f"No active override found for {args.revert}")
        return

    sports = ["mlb", "nhl", "nba"] if args.sport == "all" else [args.sport]
    all_out: list[str] = []
    structured: list[dict] = []
    for sport in sports:
        all_out.extend(run_sport(sport, structured_recs=structured))

    print("\n".join(all_out))

    if args.apply and structured:
        from .model_overrides import (
            apply_override, expire_stale, MAX_OVERRIDES_PER_RUN,
        )
        # Always expire stale overrides regardless of whether anything
        # new gets applied -- otherwise a fresh streak gets buried under
        # an old one that should've rolled off.
        for sport in sports:
            expire_stale(sport)

        print(f"\n{'=' * 72}")
        print(f"  AUTO-APPLY: {len(structured)} candidate(s) passed guard rails")
        print(f"{'=' * 72}")
        applied = 0
        for rec in structured:
            if applied >= MAX_OVERRIDES_PER_RUN:
                print(f"  SKIPPED {rec['flag']}: per-run cap "
                      f"({MAX_OVERRIDES_PER_RUN}) reached")
                continue
            apply_override(
                "mlb", rec["flag"], rec["value"],
                reason=rec["reason"],
                n_samples=rec["n"],
                p_value=rec["p_value"],
            )
            print(f"  APPLIED {rec['flag']} = {rec['value']}  ({rec['reason']})")
            applied += 1
        print(f"  {applied} override(s) applied; expires in 14d unless re-confirmed.")
        return

    if args.apply:
        # No qualifying recs -- still expire stale ones.
        from .model_overrides import expire_stale
        for sport in sports:
            n = expire_stale(sport)
            if n:
                print(f"\nExpired {n} stale {sport.upper()} override(s).")
        print("\nNo new overrides to apply (no buckets passed the guard rails).")
        return

    print("\nNext steps:")
    print("  1. Apply suggested config changes in engine/config.py,")
    print("     OR re-run with --apply to auto-write qualifying overrides.")
    print("  2. Run 'python -m engine.backtest' (MLB) / engine.nhl_retrobt / ")
    print("     engine.nba_q1_predict retrobt to verify.")
    print("  3. Commit the change with a message citing the report numbers.")
    print("  4. Let the tracker accumulate another week, then re-run this.\n")


if __name__ == "__main__":
    main()
