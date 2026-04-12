"""
NHL pick diagnostic — checks for a systematic sign error.

If the model had an inverted factor (e.g. multiplied the wrong team's xG),
flipping every pick would turn a 34% WR into ~66%. This script reports:

  1. Overall W/L and what flipping would produce
  2. Breakdown by bet type (ML / PL / O/U)
  3. Breakdown by edge bucket (is high-conviction worse than low?)
  4. Home/away pick distribution
  5. Favorite/dog pick distribution for PL
  6. Over/under distribution for totals

Usage:
    python -m engine.nhl_diagnose

If a specific bet type or edge bucket is far below 50%, that's where the bug is.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "nhl.db"


def main() -> None:
    if not DB_PATH.exists():
        print(f"No NHL DB at {DB_PATH}. Run sync first.")
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Overall
    total = conn.execute("SELECT COUNT(*) FROM nhl_picks WHERE result IS NOT NULL").fetchone()[0]
    if total == 0:
        print("No settled picks yet.")
        return

    wins = conn.execute("SELECT COUNT(*) FROM nhl_picks WHERE result='win'").fetchone()[0]
    losses = conn.execute("SELECT COUNT(*) FROM nhl_picks WHERE result='loss'").fetchone()[0]
    pushes = conn.execute("SELECT COUNT(*) FROM nhl_picks WHERE result='push'").fetchone()[0]
    wr = 100 * wins / (wins + losses) if (wins + losses) else 0
    flipped_wr = 100 * losses / (wins + losses) if (wins + losses) else 0

    print(f"\n{'='*60}")
    print(f"  NHL Pick Diagnostic ({total} settled picks)")
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

    # By bet type
    print(f"\n{'─'*60}\nBy bet type:")
    rows = conn.execute("""
        SELECT bet_type,
               SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as w,
               SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) as l,
               SUM(CASE WHEN result='push' THEN 1 ELSE 0 END) as p
        FROM nhl_picks WHERE result IS NOT NULL
        GROUP BY bet_type
    """).fetchall()
    for r in rows:
        w, l, p = r["w"] or 0, r["l"] or 0, r["p"] or 0
        if w + l > 0:
            print(f"  {r['bet_type']:6s}: {w}-{l}-{p}  WR={100*w/(w+l):5.1f}%  "
                  f"(flipped {100*l/(w+l):5.1f}%)")

    # By edge bucket
    print(f"\n{'─'*60}\nBy edge bucket (high edge = high conviction):")
    rows = conn.execute("""
        SELECT
            CASE
                WHEN edge < 2 THEN '0-2%'
                WHEN edge < 4 THEN '2-4%'
                WHEN edge < 6 THEN '4-6%'
                WHEN edge < 10 THEN '6-10%'
                ELSE '10%+'
            END as bucket,
            SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as w,
            SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) as l,
            MIN(edge) as min_edge
        FROM nhl_picks WHERE result IS NOT NULL
        GROUP BY bucket
        ORDER BY min_edge
    """).fetchall()
    for r in rows:
        w, l = r["w"] or 0, r["l"] or 0
        if w + l > 0:
            print(f"  edge {r['bucket']:6s}: {w}-{l}  WR={100*w/(w+l):5.1f}%")

    # ML pick direction
    ml_home_w = ml_home_l = ml_away_w = ml_away_l = 0
    for r in conn.execute("""
        SELECT matchup, pick, result FROM nhl_picks
        WHERE bet_type='ML' AND result IN ('win','loss')
    """):
        parts = (r["matchup"] or "").split(" @ ")
        if len(parts) != 2:
            continue
        away, home = parts[0].strip(), parts[1].strip()
        pick = (r["pick"] or "").strip()
        if pick == home:
            if r["result"] == "win": ml_home_w += 1
            else: ml_home_l += 1
        elif pick == away:
            if r["result"] == "win": ml_away_w += 1
            else: ml_away_l += 1

    print(f"\n{'─'*60}\nML picks by side:")
    if ml_home_w + ml_home_l:
        print(f"  Home picks: {ml_home_w}-{ml_home_l}  "
              f"WR={100*ml_home_w/(ml_home_w+ml_home_l):.1f}%")
    if ml_away_w + ml_away_l:
        print(f"  Away picks: {ml_away_w}-{ml_away_l}  "
              f"WR={100*ml_away_w/(ml_away_w+ml_away_l):.1f}%")

    # PL direction
    pl_fav_w = pl_fav_l = pl_dog_w = pl_dog_l = 0
    for r in conn.execute("""
        SELECT pick, result FROM nhl_picks
        WHERE bet_type='PL' AND result IN ('win','loss')
    """):
        pick = (r["pick"] or "").strip()
        if "-1.5" in pick:
            if r["result"] == "win": pl_fav_w += 1
            else: pl_fav_l += 1
        elif "+1.5" in pick:
            if r["result"] == "win": pl_dog_w += 1
            else: pl_dog_l += 1
    print(f"\nPL picks by side:")
    if pl_fav_w + pl_fav_l:
        print(f"  -1.5 (favorite): {pl_fav_w}-{pl_fav_l}  "
              f"WR={100*pl_fav_w/(pl_fav_w+pl_fav_l):.1f}%")
    if pl_dog_w + pl_dog_l:
        print(f"  +1.5 (dog):      {pl_dog_w}-{pl_dog_l}  "
              f"WR={100*pl_dog_w/(pl_dog_w+pl_dog_l):.1f}%")

    # O/U direction
    ou_over_w = ou_over_l = ou_under_w = ou_under_l = 0
    for r in conn.execute("""
        SELECT pick, result FROM nhl_picks
        WHERE bet_type='O/U' AND result IN ('win','loss')
    """):
        pick = (r["pick"] or "").lower()
        if "over" in pick:
            if r["result"] == "win": ou_over_w += 1
            else: ou_over_l += 1
        elif "under" in pick:
            if r["result"] == "win": ou_under_w += 1
            else: ou_under_l += 1
    print(f"\nO/U picks by side:")
    if ou_over_w + ou_over_l:
        print(f"  Over:  {ou_over_w}-{ou_over_l}  "
              f"WR={100*ou_over_w/(ou_over_w+ou_over_l):.1f}%")
    if ou_under_w + ou_under_l:
        print(f"  Under: {ou_under_w}-{ou_under_l}  "
              f"WR={100*ou_under_w/(ou_under_w+ou_under_l):.1f}%")

    # Last 10 losses for manual inspection
    print(f"\n{'─'*60}\nLast 10 losses for inspection:")
    rows = conn.execute("""
        SELECT matchup, bet_type, pick, model_prob, edge, odds
        FROM nhl_picks WHERE result='loss'
        ORDER BY created_at DESC LIMIT 10
    """).fetchall()
    for r in rows:
        print(f"  {r['matchup']:30s} {r['bet_type']:4s} {r['pick']:12s} "
              f"p={r['model_prob']:.3f} ed={r['edge']:+.1f}% @{r['odds']:+d}")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
