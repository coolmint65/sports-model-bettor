"""
MLB pick diagnostic — checks for a systematic sign error.

If the model had an inverted factor, flipping every pick would turn a
sub-50% WR into ~50%+. This script reports:

  1. Overall W/L and what flipping would produce
  2. Breakdown by bet type (ML / O/U / RL / 1st INN)
  3. Breakdown by edge bucket (is high-conviction worse than low?)
  4. Home/away pick distribution for ML
  5. Favorite/dog pick distribution for RL
  6. Over/under distribution for totals

Usage:
    python -m engine.mlb_diagnose

If a specific bet type or edge bucket is far below 50%, that's where
the bug is.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "mlb.db"


def main() -> None:
    if not DB_PATH.exists():
        print(f"No MLB DB at {DB_PATH}. Run sync first.")
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # First: what are the actual result values? Different trackers use
    # different strings (win/loss, W/L, hit/miss, 1/0, etc.)
    raw = conn.execute("""
        SELECT result, COUNT(*) as n FROM picks
        WHERE result IS NOT NULL
        GROUP BY result
    """).fetchall()
    result_map: dict[str, int] = {(r["result"] or ""): r["n"] for r in raw}
    print(f"\nResult column values in DB: {result_map}")

    # Canonicalize: accept win/W/hit/1 as win; loss/L/miss/0 as loss; push/P/tie as push.
    def _canon(v: str) -> str:
        if v is None:
            return ""
        s = str(v).strip().lower()
        if s in ("win", "w", "hit", "1", "true", "yes"): return "win"
        if s in ("loss", "lose", "l", "miss", "0", "false", "no"): return "loss"
        if s in ("push", "p", "tie", "draw"): return "push"
        return s

    # Overall
    total = conn.execute("SELECT COUNT(*) FROM picks WHERE result IS NOT NULL").fetchone()[0]
    if total == 0:
        print("No settled picks yet.")
        return

    wins = sum(n for v, n in result_map.items() if _canon(v) == "win")
    losses = sum(n for v, n in result_map.items() if _canon(v) == "loss")
    pushes = sum(n for v, n in result_map.items() if _canon(v) == "push")
    wr = 100 * wins / (wins + losses) if (wins + losses) else 0
    flipped_wr = 100 * losses / (wins + losses) if (wins + losses) else 0

    print(f"\n{'='*60}")
    print(f"  MLB Pick Diagnostic ({total} settled picks)")
    print(f"{'='*60}")
    print(f"\nActual:  {wins}W-{losses}L-{pushes}P  WR={wr:.1f}%")
    print(f"Flipped: {losses}W-{wins}L-{pushes}P  WR={flipped_wr:.1f}%")
    if flipped_wr >= 58 and total >= 30:
        print(f"\n  >> FLIP DIAGNOSTIC: likely sign error. Flipping all picks")
        print(f"     would produce {flipped_wr:.1f}% WR on {total} samples.")
    elif wr <= 42 and total >= 30:
        print(f"\n  >> POSSIBLE SIGN ERROR: WR {wr:.1f}% is significantly")
        print(f"     below 50% across {total} picks.")
    elif wr >= 45 and wr <= 55:
        print(f"\n  >> WR near 50%: model has no edge, not a sign error.")

    # Load all settled picks once, canonicalize result in Python
    all_settled = conn.execute("""
        SELECT bet_type, pick, model_prob, edge, odds, result, matchup
        FROM picks WHERE result IS NOT NULL
    """).fetchall()

    def res(r):
        return _canon(r["result"])

    # By bet type
    print(f"\n{'─'*60}\nBy bet type:")
    by_type: dict[str, list[int]] = {}
    for r in all_settled:
        bt = r["bet_type"] or "?"
        row = by_type.setdefault(bt, [0, 0, 0])
        c = res(r)
        if c == "win": row[0] += 1
        elif c == "loss": row[1] += 1
        elif c == "push": row[2] += 1
    for bt, (w, l, p) in sorted(by_type.items()):
        if w + l > 0:
            print(f"  {bt:8s}: {w}-{l}-{p}  WR={100*w/(w+l):5.1f}%  "
                  f"(flipped {100*l/(w+l):5.1f}%)")

    # By edge bucket
    print(f"\n{'─'*60}\nBy edge bucket (high edge = high conviction):")
    buckets = [("0-2%", 0, 2), ("2-4%", 2, 4), ("4-6%", 4, 6),
               ("6-10%", 6, 10), ("10%+", 10, 9999)]
    for name, lo, hi in buckets:
        w = l = 0
        for r in all_settled:
            e = r["edge"] or 0
            if not (lo <= e < hi):
                continue
            c = res(r)
            if c == "win": w += 1
            elif c == "loss": l += 1
        if w + l > 0:
            print(f"  edge {name:6s}: {w}-{l}  WR={100*w/(w+l):5.1f}%")

    # ML pick direction
    ml_home_w = ml_home_l = ml_away_w = ml_away_l = 0
    for r in all_settled:
        if r["bet_type"] != "ML":
            continue
        c = res(r)
        if c not in ("win", "loss"):
            continue
        parts = (r["matchup"] or "").split(" @ ")
        if len(parts) != 2:
            continue
        away, home = parts[0].strip(), parts[1].strip()
        pick = (r["pick"] or "").strip()
        if pick == home:
            if c == "win": ml_home_w += 1
            else: ml_home_l += 1
        elif pick == away:
            if c == "win": ml_away_w += 1
            else: ml_away_l += 1

    print(f"\n{'─'*60}\nML picks by side:")
    if ml_home_w + ml_home_l:
        print(f"  Home picks: {ml_home_w}-{ml_home_l}  "
              f"WR={100*ml_home_w/(ml_home_w+ml_home_l):.1f}%")
    if ml_away_w + ml_away_l:
        print(f"  Away picks: {ml_away_w}-{ml_away_l}  "
              f"WR={100*ml_away_w/(ml_away_w+ml_away_l):.1f}%")

    # RL direction (MLB uses "RL" instead of "PL")
    rl_fav_w = rl_fav_l = rl_dog_w = rl_dog_l = 0
    for r in all_settled:
        if r["bet_type"] != "RL":
            continue
        c = res(r)
        if c not in ("win", "loss"):
            continue
        pick = (r["pick"] or "").strip()
        if "-1.5" in pick:
            if c == "win": rl_fav_w += 1
            else: rl_fav_l += 1
        elif "+1.5" in pick:
            if c == "win": rl_dog_w += 1
            else: rl_dog_l += 1
    print(f"\nRL picks by side:")
    if rl_fav_w + rl_fav_l:
        print(f"  -1.5 (favorite): {rl_fav_w}-{rl_fav_l}  "
              f"WR={100*rl_fav_w/(rl_fav_w+rl_fav_l):.1f}%")
    if rl_dog_w + rl_dog_l:
        print(f"  +1.5 (dog):      {rl_dog_w}-{rl_dog_l}  "
              f"WR={100*rl_dog_w/(rl_dog_w+rl_dog_l):.1f}%")

    # O/U direction
    ou_over_w = ou_over_l = ou_under_w = ou_under_l = 0
    for r in all_settled:
        if r["bet_type"] != "O/U":
            continue
        c = res(r)
        if c not in ("win", "loss"):
            continue
        pick = (r["pick"] or "").lower()
        if "over" in pick:
            if c == "win": ou_over_w += 1
            else: ou_over_l += 1
        elif "under" in pick:
            if c == "win": ou_under_w += 1
            else: ou_under_l += 1
    print(f"\nO/U picks by side:")
    if ou_over_w + ou_over_l:
        print(f"  Over:  {ou_over_w}-{ou_over_l}  "
              f"WR={100*ou_over_w/(ou_over_w+ou_over_l):.1f}%")
    if ou_under_w + ou_under_l:
        print(f"  Under: {ou_under_w}-{ou_under_l}  "
              f"WR={100*ou_under_w/(ou_under_w+ou_under_l):.1f}%")

    # 1st INN (NRFI/YRFI)
    nrfi_w = nrfi_l = yrfi_w = yrfi_l = 0
    for r in all_settled:
        if r["bet_type"] != "1st INN":
            continue
        c = res(r)
        if c not in ("win", "loss"):
            continue
        pick = (r["pick"] or "").upper()
        if "NRFI" in pick:
            if c == "win": nrfi_w += 1
            else: nrfi_l += 1
        elif "YRFI" in pick:
            if c == "win": yrfi_w += 1
            else: yrfi_l += 1
    print(f"\n1st INN picks by side:")
    if nrfi_w + nrfi_l:
        print(f"  NRFI: {nrfi_w}-{nrfi_l}  "
              f"WR={100*nrfi_w/(nrfi_w+nrfi_l):.1f}%")
    if yrfi_w + yrfi_l:
        print(f"  YRFI: {yrfi_w}-{yrfi_l}  "
              f"WR={100*yrfi_w/(yrfi_w+yrfi_l):.1f}%")

    # Last 10 losses for manual inspection
    print(f"\n{'─'*60}\nLast 10 losses for inspection:")
    losses_rows = [r for r in all_settled if res(r) == "loss"]
    for r in losses_rows[-10:]:
        mp = r["model_prob"] if r["model_prob"] is not None else 0
        ed = r["edge"] if r["edge"] is not None else 0
        od = r["odds"] if r["odds"] is not None else 0
        print(f"  {(r['matchup'] or ''):30s} {(r['bet_type'] or ''):8s} {(r['pick'] or ''):14s} "
              f"p={mp:.3f} ed={ed:+.1f}% @{od:+d}")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
