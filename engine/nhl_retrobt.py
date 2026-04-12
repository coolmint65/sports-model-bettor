"""
NHL retrospective sweep.

Takes every settled pick in the tracker, re-runs the prediction through
the CURRENT model (granular factors off), and reports whether the pick
would still be the same or would flip.

Hypothetical result:
  - Same side as old pick  -> old result applies (W stays W, L stays L)
  - Flipped side           -> old result flips (W becomes L, L becomes W)
  - No longer picked       -> neutral (discarded)

This tells us how much the granular-factors-off model differs from the
historical picks, and what the hypothetical WR would have been.

Caveat: live stats (standings, records, etc.) have CHANGED since the
original picks were made. This test isn't a perfect historical replay —
it re-predicts WITH today's stats but WITHOUT the granular factors.
It's a fast approximation to see if the architectural change matters.

Usage:
    python -m engine.nhl_retrobt
"""

import logging
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "nhl.db"


def _canon(v):
    if v is None:
        return ""
    s = str(v).strip().lower()
    if s in ("win", "w", "hit", "1", "true", "yes"): return "win"
    if s in ("loss", "lose", "l", "miss", "0", "false", "no"): return "loss"
    if s in ("push", "p", "tie", "draw"): return "push"
    return s


def _find_key(abbr: str):
    from engine.data import list_teams, load_team
    ALIAS = {"SJ": "SJS", "TB": "TBL", "NJ": "NJD", "LA": "LAK",
             "WAS": "WSH", "MON": "MTL", "NAS": "NSH", "CLB": "CBJ"}
    target = ALIAS.get(abbr.upper(), abbr.upper())
    for t in list_teams("NHL"):
        try:
            data = load_team("NHL", t["key"])
            stored = (data.get("abbreviation", "") or "").upper() if data else ""
            if stored == target or stored == abbr.upper():
                return t["key"]
        except Exception:
            continue
    return None


def _evaluate(bet_type: str, old_pick: str, pred: dict, home_abbr: str, away_abbr: str):
    """Given the current prediction, would it still pick the same side as old_pick?

    Returns: "same", "flipped", or "no_longer"
    """
    if not pred:
        return "no_longer"

    wp = pred.get("win_prob", {})
    pl = pred.get("puck_line", {})
    ou = pred.get("over_under", {})

    if bet_type == "ML":
        model_leans_home = wp.get("home", 0.5) > wp.get("away", 0.5)
        old_side_home = old_pick.strip() == home_abbr
        return "same" if model_leans_home == old_side_home else "flipped"

    if bet_type == "PL":
        # Old pick looks like "SJS +1.5" or "LAK -1.5"
        parts = old_pick.strip().split()
        if len(parts) < 2:
            return "no_longer"
        side_abbr = parts[0]
        spread = parts[1]
        old_is_home = side_abbr == home_abbr
        is_dog = "+1.5" in spread

        # Model's prob for the same bet
        if old_is_home:
            model_prob = pl.get("home_plus_1_5" if is_dog else "home_minus_1_5", 0.5)
        else:
            model_prob = pl.get("away_plus_1_5" if is_dog else "away_minus_1_5", 0.5)

        # Would the model STILL recommend this bet? (prob > 0.50 is the threshold)
        return "same" if model_prob > 0.50 else "flipped"

    if bet_type == "O/U":
        # Old pick looks like "Over 5.5" or "Under 6.5"
        parts = old_pick.strip().split()
        if len(parts) < 2:
            return "no_longer"
        old_over = parts[0].lower() == "over"
        try:
            line = float(parts[1])
        except ValueError:
            return "no_longer"

        # Find closest line in model's over_under dict
        best_key = None
        best_diff = 999.0
        for k in ou:
            try:
                diff = abs(float(k) - line)
                if diff < best_diff:
                    best_diff = diff
                    best_key = k
            except ValueError:
                continue
        if not best_key:
            return "no_longer"

        entry = ou[best_key]
        model_leans_over = entry.get("over", 0.5) > entry.get("under", 0.5)
        return "same" if model_leans_over == old_over else "flipped"

    return "no_longer"


def main() -> None:
    logging.basicConfig(level=logging.WARNING)

    if not DB_PATH.exists():
        print(f"No NHL DB at {DB_PATH}. Run sync first.")
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    picks = conn.execute("""
        SELECT bet_type, pick, matchup, result
        FROM nhl_picks WHERE result IS NOT NULL
        ORDER BY created_at ASC
    """).fetchall()

    if not picks:
        print("No settled picks.")
        return

    from engine.nhl_predict import predict_matchup

    print(f"\n{'='*70}")
    print(f"  NHL Retrospective Sweep ({len(picks)} settled picks)")
    print(f"  Re-running through current model (granular factors OFF)")
    print(f"{'='*70}\n")

    # Cache predictions per matchup (avoid running the full model 41 times
    # if the same matchup appears multiple times)
    pred_cache: dict[tuple[str, str], dict] = {}

    actual_w = actual_l = 0
    hypo_w = hypo_l = 0
    same_cnt = flipped_cnt = dropped_cnt = 0
    details = []

    for row in picks:
        actual = _canon(row["result"])
        if actual == "win": actual_w += 1
        elif actual == "loss": actual_l += 1
        else:
            continue  # ignore pushes

        matchup = (row["matchup"] or "").strip()
        parts = matchup.split(" @ ")
        if len(parts) != 2:
            continue
        away_abbr = parts[0].strip()
        home_abbr = parts[1].strip()

        # Re-run prediction (cache key by matchup)
        key = (home_abbr, away_abbr)
        if key not in pred_cache:
            h_key = _find_key(home_abbr)
            a_key = _find_key(away_abbr)
            pred_cache[key] = None
            if h_key and a_key:
                try:
                    pred_cache[key] = predict_matchup(h_key, a_key)
                except Exception as e:
                    logging.warning("predict failed for %s @ %s: %s", away_abbr, home_abbr, e)
        pred = pred_cache[key]

        verdict = _evaluate(row["bet_type"], row["pick"], pred or {}, home_abbr, away_abbr)
        if verdict == "same":
            same_cnt += 1
            # Same side => same result
            if actual == "win": hypo_w += 1
            else: hypo_l += 1
        elif verdict == "flipped":
            flipped_cnt += 1
            # Flipped => result flips (only if we'd realistically have taken the opposite side)
            if actual == "win": hypo_l += 1
            else: hypo_w += 1
        else:
            dropped_cnt += 1
            # If model no longer recommends this bet at all, we wouldn't have played it.
            # Treat as neutral (excluded from hypo).
            continue

        details.append((row["bet_type"], row["pick"], matchup, actual, verdict))

    def _wr(w, l):
        return 100 * w / (w + l) if (w + l) else 0.0

    print(f"Historical record:    {actual_w}W-{actual_l}L  WR={_wr(actual_w, actual_l):.1f}%")
    print(f"Hypothetical record:  {hypo_w}W-{hypo_l}L  WR={_wr(hypo_w, hypo_l):.1f}%")
    print(f"\nPick-direction changes:")
    print(f"  Same side as old pick:  {same_cnt}")
    print(f"  Flipped side:           {flipped_cnt}")
    print(f"  Model drops pick:       {dropped_cnt}")

    delta = _wr(hypo_w, hypo_l) - _wr(actual_w, actual_l)
    if abs(delta) >= 10 and (hypo_w + hypo_l) >= 20:
        direction = "improves" if delta > 0 else "worsens"
        print(f"\n  >> Current model would have {direction} WR by {abs(delta):.1f} points.")
        if delta > 0:
            print(f"     Suggests disabling granular factors was correct.")
        else:
            print(f"     Disabling granular factors didn't help — deeper issue.")
    elif dropped_cnt >= len(picks) * 0.4:
        print(f"\n  >> Model drops >40% of past picks. Conviction has dropped,")
        print(f"     which may reduce both wins and losses. Worth watching live.")
    else:
        print(f"\n  >> Change is within noise ({delta:+.1f}% WR). Need more live data.")

    # Show the flipped picks specifically
    print(f"\n── Picks that would have flipped ──")
    flipped_details = [d for d in details if d[4] == "flipped"]
    for bt, pick, mu, act, _ in flipped_details[:20]:
        new_res = "W" if act == "loss" else "L"
        print(f"  {mu:25s} {bt:4s} {pick:14s} was {act.upper()} -> would be {new_res}")

    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    main()
