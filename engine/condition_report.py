"""
Condition report — performance by context (side, role, bet type, line movement).

Splits settled picks along several dimensions and reports WR + ROI per side
so we can find systematic biases to filter out:

  - Side:          home pick vs away pick (using matchup "AWAY @ HOME")
  - Role:          favorite (odds < 0) vs underdog (odds > 0)
  - Bet type:      already recorded per sport
  - Line movement: got-better-price vs got-worse-price (requires closing_odds)

A "strong bias" (20+ pt WR delta with N>=10 on BOTH sides) triggers a
recommendation to filter or reweight that dimension.

Usage:
    python -m engine.condition_report              # all sports
    python -m engine.condition_report --sport nhl
    python -m engine.condition_report --sport all
"""

from __future__ import annotations

import argparse

from engine._analysis_common import (
    SPORTS,
    canon_result,
    fetch_settled,
    label,
    open_conn,
    pick_profit,
    pick_role,
    pick_side,
    resolve_sports,
)

STRONG_DELTA_PTS = 20.0
STRONG_MIN_N = 10


def _implied_prob(odds) -> float | None:
    """American odds -> implied probability in [0,1]."""
    if odds is None:
        return None
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    if o < 0:
        return abs(o) / (abs(o) + 100.0)
    return 100.0 / (o + 100.0)


def _line_movement_bucket(row: dict) -> str | None:
    """'better' if bet_odds gave higher payout than closing (closing shifted against
    the side, i.e. closing implied prob < bet implied prob? no -- we want CLV logic).

    CLV positive = we got a BETTER price than closing. Implied prob at close
    > implied prob at bet  =>  closing moved toward our side  =>  positive CLV.
    """
    bet = row.get("odds")
    close = row.get("closing_odds")
    if bet is None or close is None:
        return None
    bi = _implied_prob(bet)
    ci = _implied_prob(close)
    if bi is None or ci is None:
        return None
    if ci > bi:
        return "better_price"    # closing implied > bet implied = we beat closing
    if ci < bi:
        return "worse_price"
    return "flat"


def _tally(rows: list[dict], classifier) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for r in rows:
        key = classifier(r)
        if key is None:
            continue
        b = out.setdefault(key, {"w": 0, "l": 0, "p": 0, "profit": 0.0})
        c = canon_result(r.get("result"))
        if c == "win":
            b["w"] += 1
        elif c == "loss":
            b["l"] += 1
        elif c == "push":
            b["p"] += 1
        else:
            continue
        b["profit"] += pick_profit(r)
    return out


def _fmt_row(key: str, b: dict, width: int = 14) -> str:
    n = b["w"] + b["l"] + b["p"]
    decided = b["w"] + b["l"]
    wr = 100.0 * b["w"] / decided if decided else 0.0
    roi = 100.0 * b["profit"] / (n * 100.0) if n else 0.0
    return (
        f"    {key:<{width}s}: N={n:3d}  {b['w']:3d}-{b['l']:3d}-{b['p']}  "
        f"WR={wr:5.1f}%  profit=${b['profit']:+8.2f}  ROI={roi:+6.2f}%"
    )


def _wr(b: dict) -> tuple[int, float]:
    n = b["w"] + b["l"]
    return n, (100.0 * b["w"] / n if n else 0.0)


def _check_binary_bias(agg: dict[str, dict], key_a: str, key_b: str, dim: str,
                       recs: list[str]) -> None:
    """Flag a strong bias if both sides have N>=STRONG_MIN_N and WR delta > STRONG_DELTA_PTS."""
    a = agg.get(key_a)
    b = agg.get(key_b)
    if not a or not b:
        return
    na, wra = _wr(a)
    nb, wrb = _wr(b)
    if na < STRONG_MIN_N or nb < STRONG_MIN_N:
        return
    delta = wra - wrb
    if abs(delta) < STRONG_DELTA_PTS:
        return
    winner = key_a if delta > 0 else key_b
    loser = key_b if delta > 0 else key_a
    recs.append(
        f"{dim}: {winner} WR {max(wra, wrb):.1f}% vs {loser} WR {min(wra, wrb):.1f}% "
        f"(delta {abs(delta):.1f}pt, N={na}/{nb}) -- consider filtering out {loser} picks."
    )


def report_sport(sport: str) -> None:
    conn = open_conn(sport)
    if conn is None:
        print(f"\nNo picks yet for {label(sport)} (DB missing).")
        return
    try:
        rows = fetch_settled(conn, sport)
    finally:
        conn.close()

    if not rows:
        print(f"\nNo picks yet for {label(sport)}.")
        return

    print(f"\n{'='*60}")
    print(f"  {label(sport)} condition report (N={len(rows)})")
    print(f"{'='*60}")

    recs: list[str] = []

    # ── Side ────────────────────────────────────────────────
    side_agg = _tally(rows, pick_side)
    print("\n  By side (home vs away pick):")
    if side_agg:
        for key in ("home", "away"):
            if key in side_agg:
                print(_fmt_row(key, side_agg[key]))
        _check_binary_bias(side_agg, "home", "away", "Side", recs)
    else:
        print("    (no classifiable rows)")

    # ── Role ────────────────────────────────────────────────
    role_agg = _tally(rows, pick_role)
    print("\n  By role (favorite vs underdog):")
    if role_agg:
        for key in ("favorite", "underdog"):
            if key in role_agg:
                print(_fmt_row(key, role_agg[key]))
        _check_binary_bias(role_agg, "favorite", "underdog", "Role", recs)
    else:
        print("    (no classifiable rows)")

    # ── Bet type ────────────────────────────────────────────
    from ._analysis_common import canon_bet_type
    bt_agg = _tally(rows, lambda r: (canon_bet_type(r.get("bet_type")) or "?"))
    print("\n  By bet type:")
    if bt_agg:
        for key in sorted(bt_agg.keys()):
            print(_fmt_row(key, bt_agg[key]))
        # Flag bet types where WR < 40% with N>=10 as a drag
        for key, b in bt_agg.items():
            n, wr = _wr(b)
            if n >= STRONG_MIN_N and wr < 40.0:
                recs.append(
                    f"Bet type {key}: WR only {wr:.1f}% over {n} picks -- "
                    f"consider suspending this market."
                )
    else:
        print("    (none)")

    # ── Line movement (if closing_odds data exists) ─────────
    lm_agg = _tally(rows, _line_movement_bucket)
    print("\n  By line movement (CLV sign):")
    if lm_agg:
        for key in ("better_price", "flat", "worse_price"):
            if key in lm_agg:
                print(_fmt_row(key, lm_agg[key]))
        _check_binary_bias(lm_agg, "better_price", "worse_price",
                           "Line movement", recs)
    else:
        print("    (no picks with closing_odds data yet)")

    # ── Recommendations ─────────────────────────────────────
    if recs:
        print("\n  Recommendations:")
        for r in recs:
            print(f"    - {r}")
    else:
        print("\n  No strong biases detected (need 20+pt WR delta, N>=10 per side).")


def main() -> None:
    ap = argparse.ArgumentParser(description="Per-condition ROI analysis across tracked picks.")
    ap.add_argument("--sport", default="all",
                    help=f"One of: {', '.join(SPORTS)}, or 'all' (default).")
    args = ap.parse_args()

    for sport in resolve_sports(args.sport):
        report_sport(sport)
    print()


if __name__ == "__main__":
    main()
