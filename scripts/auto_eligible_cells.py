"""Which (sport, bet_type, direction) cells clear the +5% ROI bar with
at least 50 stake-weighted settled picks?

Reads every per-sport picks table, groups by cell, computes
stake-weighted ROI, and prints the eligibility ladder. This IS the
"which markets can we auto-fire on" question — the queue endpoint
consumes the same output.

Usage:
    python -m scripts.auto_eligible_cells [--min-n 50] [--min-roi 5]
"""
from __future__ import annotations

import argparse
import importlib
import sys
from collections import defaultdict


# Same lookup edge_floors uses — reuse so a new sport onboarded there
# is auto-picked up here too.
_SOURCES = {
    "mlb":   ("engine.db",     "picks"),
    "nhl":   ("engine.nhl_db", "nhl_picks"),
    "nba":   ("engine.nba_db", "nba_picks"),
    "tennis": ("engine.tennis_db", "tennis_picks"),
    # Basketball framework
    "afl":       ("engine.basketball._db", "picks", "afl"),
    "wnba":      ("engine.basketball._db", "picks", "wnba"),
    "ncaam":     ("engine.basketball._db", "picks", "ncaam"),
    "ncaaw":     ("engine.basketball._db", "picks", "ncaaw"),
    "euroleague": ("engine.basketball._db", "picks", "euroleague"),
    "nba_summer_league": ("engine.basketball._db", "picks",
                            "nba_summer_league"),
    # Soccer framework
    "fifa_world_cup":    ("engine.soccer._db", "picks", "fifa_world_cup"),
    "eng_premier":       ("engine.soccer._db", "picks", "eng_premier"),
    "esp_laliga":        ("engine.soccer._db", "picks", "esp_laliga"),
    "ita_seriea":        ("engine.soccer._db", "picks", "ita_seriea"),
    "ger_bundesliga":    ("engine.soccer._db", "picks", "ger_bundesliga"),
    "fra_ligue1":        ("engine.soccer._db", "picks", "fra_ligue1"),
    "usa_mls":           ("engine.soccer._db", "picks", "usa_mls"),
    "bra_seriea":        ("engine.soccer._db", "picks", "bra_seriea"),
    "arg_lpf":           ("engine.soccer._db", "picks", "arg_lpf"),
    "conmebol_libertadores": ("engine.soccer._db", "picks",
                                "conmebol_libertadores"),
    "uefa_champions":    ("engine.soccer._db", "picks", "uefa_champions"),
    "uefa_europa":       ("engine.soccer._db", "picks", "uefa_europa"),
    "uefa_conference":   ("engine.soccer._db", "picks", "uefa_conference"),
    "nwsl":              ("engine.soccer._db", "picks", "nwsl"),
    "usl_championship":  ("engine.soccer._db", "picks", "usl_championship"),
    "usa_open_cup":      ("engine.soccer._db", "picks", "usa_open_cup"),
    "f1":                ("engine.motorsports._db", "picks", "f1"),
    "nascar":            ("engine.motorsports._db", "picks", "nascar"),
    "indycar":           ("engine.motorsports._db", "picks", "indycar"),
}


def _direction_label(bet_type: str, pick: str | None) -> str:
    """Same helper edge_floors uses — over/under/nrfi/yrfi/fav/dog
    for markets where the direction meaningfully changes the ROI."""
    if not pick:
        return ""
    pk = pick.strip().lower()
    bt = (bet_type or "").strip().lower()
    if pk.startswith("over"):
        return "over"
    if pk.startswith("under"):
        return "under"
    if bt in ("1st inn", "1stinn", "nrfi"):
        if "nrfi" in pk:
            return "nrfi"
        if "yrfi" in pk:
            return "yrfi"
    if bt in ("rl", "pl", "spread", "alt rl", "alt pl", "alt spread"):
        import re
        m = re.search(r"([+-])\d", pk)
        if m:
            return "fav" if m.group(1) == "-" else "dog"
    return ""


def _load_settled(sport: str, since: str | None = None) -> list[dict]:
    src = _SOURCES.get(sport)
    if not src:
        return []
    db_module_name, table = src[0], src[1]
    league_arg = src[2] if len(src) >= 3 else None
    try:
        mod = importlib.import_module(db_module_name)
        conn = (mod.get_conn(league_arg) if league_arg is not None
                else mod.get_conn())
    except Exception:
        return []
    where = ["result IN ('W', 'L', 'P')", "stake_units IS NOT NULL",
              "stake_units > 0"]
    params: list = []
    if since:
        where.append("date >= ?")
        params.append(since)
    sql = (f"SELECT bet_type, pick, stake_units, result, profit "
           f"FROM {table} WHERE {' AND '.join(where)}")
    try:
        rows = conn.execute(sql, params).fetchall()
    except Exception:
        return []
    out = [dict(r) for r in rows]
    # Also read player_props_picks when the table exists.
    try:
        pp_sql = (f"SELECT bet_type, pick, stake_units, result, profit "
                   f"FROM player_props_picks "
                   f"WHERE {' AND '.join(where)}")
        for r in conn.execute(pp_sql, params).fetchall():
            out.append(dict(r))
    except Exception:
        pass
    return out


def collect_cells(min_n: int = 50,
                    min_roi: float = 5.0) -> list[dict]:
    """Return every (sport, bet_type, direction) cell that clears
    the eligibility bar, sorted by ROI descending."""
    cells: dict[tuple, dict] = defaultdict(
        lambda: {"n": 0, "wins": 0, "losses": 0,
                 "staked_dollars": 0.0, "profit_dollars": 0.0})
    for sport in _SOURCES.keys():
        for r in _load_settled(sport):
            bt = (r.get("bet_type") or "").strip()
            if not bt:
                continue
            direction = _direction_label(bt, r.get("pick"))
            stake = float(r.get("stake_units") or 0.0)
            if stake <= 0:
                continue
            key = (sport, bt, direction)
            c = cells[key]
            c["n"] += 1
            c["staked_dollars"] += stake * 100.0
            # profit column is 1u-basis (per today's audit); scale by
            # stake so the ROI number reflects real bankroll impact.
            profit_1u = float(r.get("profit") or 0.0)
            c["profit_dollars"] += profit_1u * stake
            if r.get("result") == "W":
                c["wins"] += 1
            elif r.get("result") == "L":
                c["losses"] += 1

    eligible: list[dict] = []
    for (sport, bt, direction), c in cells.items():
        if c["n"] < min_n:
            continue
        if c["staked_dollars"] <= 0:
            continue
        roi = (c["profit_dollars"] / c["staked_dollars"]) * 100.0
        if roi < min_roi:
            continue
        decided = c["wins"] + c["losses"]
        wr = (c["wins"] / decided * 100.0) if decided else 0.0
        eligible.append({
            "sport": sport,
            "bet_type": bt,
            "direction": direction,
            "n": c["n"],
            "wins": c["wins"],
            "losses": c["losses"],
            "wr": wr,
            "roi": roi,
            "staked_dollars": c["staked_dollars"],
            "profit_dollars": c["profit_dollars"],
        })
    eligible.sort(key=lambda r: -r["roi"])
    return eligible


def _print_report(rows: list[dict], min_n: int, min_roi: float) -> None:
    print(f"\n=== Auto-eligible cells (n>={min_n}, ROI>={min_roi}%, "
          f"stake-weighted) ===\n")
    if not rows:
        print("  no cells clear the bar")
        return
    print(f"  {'sport':<20}{'bet_type':<22}{'dir':>6}"
          f"{'n':>5}{'WR':>6}{'ROI':>8}{'staked':>10}{'profit':>10}")
    for r in rows:
        d = r["direction"] or "-"
        print(f"  {r['sport']:<20}{r['bet_type']:<22}{d:>6}"
              f"{r['n']:>5}{r['wr']:>5.1f}%{r['roi']:>+7.2f}%"
              f"{r['staked_dollars']:>10.0f}{r['profit_dollars']:>+10.0f}")
    total_staked = sum(r["staked_dollars"] for r in rows)
    total_profit = sum(r["profit_dollars"] for r in rows)
    total_roi = (total_profit / total_staked * 100.0
                  if total_staked else 0.0)
    print(f"\n  TOTAL: n={sum(r['n'] for r in rows)}, "
          f"staked=${total_staked:.0f}, profit=${total_profit:+.0f}, "
          f"blended ROI={total_roi:+.2f}%")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-n", type=int, default=50)
    ap.add_argument("--min-roi", type=float, default=5.0)
    args = ap.parse_args()
    rows = collect_cells(min_n=args.min_n, min_roi=args.min_roi)
    _print_report(rows, args.min_n, args.min_roi)
    return 0


if __name__ == "__main__":
    sys.exit(main())
