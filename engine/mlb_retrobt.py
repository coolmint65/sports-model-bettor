"""
MLB retrospective sweep.

Takes every settled pick in the tracker, re-runs the prediction through
the CURRENT model (with whatever toggles are set in config.py), and
reports whether the pick would still be the same or would flip.

Hypothetical result:
  - Same side as old pick  -> old result applies (W stays W, L stays L)
  - Flipped side           -> old result flips (W becomes L, L becomes W)
  - No longer picked       -> neutral (discarded)

CAVEAT: MLB predictions depend heavily on real-time state that has
CHANGED since the original picks were made:
  - Starting pitcher (could be a different SP now)
  - Weather, temperature, wind
  - Umpire assignment
  - Lineup / platoon state
  - Bullpen fatigue / recent usage
  - Injuries
  - Team records, recent form, travel state

So this replay is APPROXIMATE — it tells us how the model architecture
has changed, not a perfect historical re-prediction. Directionally
useful, not a literal backtest.

Usage:
    python -m engine.mlb_retrobt
"""

import logging
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "mlb.db"


def _canon(v):
    if v is None:
        return ""
    s = str(v).strip().lower()
    if s in ("win", "w", "hit", "1", "true", "yes"): return "win"
    if s in ("loss", "lose", "l", "miss", "0", "false", "no"): return "loss"
    if s in ("push", "p", "tie", "draw"): return "push"
    return s


# MLB abbreviation aliases — trackers/ESPN/OddsAPI disagree sometimes.
_MLB_ALIAS = {
    "AZ": "ARI", "CHW": "CWS", "WAS": "WSH", "SFG": "SF",
    "SDP": "SD", "TBR": "TB", "KCR": "KC", "OAK": "ATH",
}


def _find_team_id(abbr: str) -> int | None:
    """Resolve an abbreviation to an MLB team_id, tolerant of aliases."""
    from .db import get_team_by_abbr
    target = abbr.upper()
    row = get_team_by_abbr(target)
    if row:
        return row.get("mlb_id")
    alt = _MLB_ALIAS.get(target)
    if alt:
        row = get_team_by_abbr(alt)
        if row:
            return row.get("mlb_id")
    # Also try the reverse mapping (ARI -> AZ, CWS -> CHW, etc.)
    for a, b in _MLB_ALIAS.items():
        if b == target:
            row = get_team_by_abbr(a)
            if row:
                return row.get("mlb_id")
    return None


def _top_pick_for_bet_type(picks: list[dict], bet_type: str) -> dict | None:
    """Return the highest adjusted_ev playable pick for a given bet type."""
    matching = [p for p in picks
                if p.get("type") == bet_type
                and p.get("confidence") != "skip"]
    if not matching:
        return None
    return sorted(matching, key=lambda p: -p.get("adjusted_ev", 0))[0]


def _evaluate(bet_type: str, old_pick: str, picks: list[dict],
              home_abbr: str, away_abbr: str) -> str:
    """Given the current list of generated picks, would the model still take
    the same side as old_pick?

    Returns: "same", "flipped", or "no_longer"
    """
    if not picks:
        return "no_longer"

    current = _top_pick_for_bet_type(picks, bet_type)
    if not current:
        return "no_longer"

    cur_label = (current.get("pick") or "").strip()
    old_label = (old_pick or "").strip()

    if bet_type == "ML":
        # Labels are bare abbreviations (e.g. "NYY")
        if cur_label == old_label:
            return "same"
        # Check alias equivalence
        if _MLB_ALIAS.get(cur_label) == old_label or _MLB_ALIAS.get(old_label) == cur_label:
            return "same"
        return "flipped"

    if bet_type == "RL":
        # Labels look like "NYY -1.5" or "BOS +1.5"
        cur_parts = cur_label.split()
        old_parts = old_label.split()
        if len(cur_parts) < 2 or len(old_parts) < 2:
            return "no_longer"
        cur_side, cur_spread = cur_parts[0], cur_parts[1]
        old_side, old_spread = old_parts[0], old_parts[1]
        same_side = (cur_side == old_side
                     or _MLB_ALIAS.get(cur_side) == old_side
                     or _MLB_ALIAS.get(old_side) == cur_side)
        same_spread = cur_spread == old_spread
        return "same" if (same_side and same_spread) else "flipped"

    if bet_type == "O/U":
        # Labels look like "Over 8.5" or "Under 9.0"
        cur_parts = cur_label.split()
        old_parts = old_label.split()
        if len(cur_parts) < 2 or len(old_parts) < 2:
            return "no_longer"
        cur_over = cur_parts[0].lower() == "over"
        old_over = old_parts[0].lower() == "over"
        return "same" if cur_over == old_over else "flipped"

    if bet_type == "1st INN":
        cur_up = cur_label.upper()
        old_up = old_label.upper()
        cur_nrfi = "NRFI" in cur_up
        old_nrfi = "NRFI" in old_up
        return "same" if cur_nrfi == old_nrfi else "flipped"

    return "no_longer"


def main() -> None:
    logging.basicConfig(level=logging.WARNING)

    if not DB_PATH.exists():
        print(f"No MLB DB at {DB_PATH}. Run sync first.")
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    picks_rows = conn.execute("""
        SELECT bet_type, pick, matchup, result, game_id
        FROM picks WHERE result IS NOT NULL
        ORDER BY created_at ASC
    """).fetchall()

    if not picks_rows:
        print("No settled picks.")
        return

    from engine.picks import generate_picks, fetch_real_odds_for_games, match_odds

    # Read situational-factor toggle state for reporting
    try:
        from engine.config import MLB_ENABLE_SITUATIONAL_FACTORS as _SITU_ON
    except Exception:
        _SITU_ON = True

    print(f"\n{'='*70}")
    print(f"  MLB Retrospective Sweep ({len(picks_rows)} settled picks)")
    print(f"  Re-running through current model")
    print(f"  MLB_ENABLE_SITUATIONAL_FACTORS = {_SITU_ON}")
    print(f"{'='*70}")
    print(f"\n  CAVEAT: MLB depends on SP, weather, umpire, lineup, bullpen")
    print(f"  fatigue, and injuries — all of which have shifted since the")
    print(f"  original picks. This replay is APPROXIMATE. It shows how the")
    print(f"  CURRENT architecture (today's inputs) lines up vs the logged")
    print(f"  historical picks, not a literal historical re-prediction.\n")

    # Fetch real odds once; we need them for edge/pick generation
    try:
        all_odds = fetch_real_odds_for_games()
    except Exception as e:
        logging.warning("Could not fetch odds: %s", e)
        all_odds = {}

    # Cache predictions per matchup — MLB predict is expensive
    picks_cache: dict[tuple[str, str], list[dict]] = {}

    actual_w = actual_l = 0
    hypo_w = hypo_l = 0
    same_cnt = flipped_cnt = dropped_cnt = 0
    details = []

    for row in picks_rows:
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

        key = (home_abbr, away_abbr)
        if key not in picks_cache:
            h_id = _find_team_id(home_abbr)
            a_id = _find_team_id(away_abbr)
            picks_cache[key] = []
            if h_id and a_id:
                # Pull the starting pitchers & venue from today's game row
                # if available; otherwise fall back to None.
                game_row = conn.execute("""
                    SELECT home_pitcher_id, away_pitcher_id, venue
                    FROM games
                    WHERE home_team_id = ? AND away_team_id = ?
                    ORDER BY date DESC LIMIT 1
                """, (h_id, a_id)).fetchone()
                home_sp = game_row["home_pitcher_id"] if game_row else None
                away_sp = game_row["away_pitcher_id"] if game_row else None
                venue = game_row["venue"] if game_row else None

                odds = match_odds(home_abbr, away_abbr, all_odds)
                try:
                    picks_cache[key] = generate_picks(
                        home_team_id=h_id,
                        away_team_id=a_id,
                        home_pitcher_id=home_sp,
                        away_pitcher_id=away_sp,
                        venue=venue,
                        odds=odds,
                    )
                except Exception as e:
                    logging.warning("generate_picks failed for %s @ %s: %s",
                                    away_abbr, home_abbr, e)
        current_picks = picks_cache[key]

        from ._analysis_common import canon_bet_type
        bt = canon_bet_type(row["bet_type"])
        verdict = _evaluate(bt, row["pick"],
                            current_picks, home_abbr, away_abbr)
        if verdict == "same":
            same_cnt += 1
            if actual == "win": hypo_w += 1
            else: hypo_l += 1
        elif verdict == "flipped":
            flipped_cnt += 1
            if actual == "win": hypo_l += 1
            else: hypo_w += 1
        else:
            dropped_cnt += 1
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
            if not _SITU_ON:
                print(f"     Suggests disabling situational factors was correct.")
            else:
                print(f"     Current architecture is aligning better than logged picks.")
        else:
            if _SITU_ON:
                print(f"     Try flipping MLB_ENABLE_SITUATIONAL_FACTORS=False and re-running.")
            else:
                print(f"     Disabling situational factors didn't help — deeper issue.")
    elif dropped_cnt >= len(picks_rows) * 0.4:
        print(f"\n  >> Model drops >40% of past picks. Conviction has dropped,")
        print(f"     which may reduce both wins and losses. Worth watching live.")
    else:
        print(f"\n  >> Change is within noise ({delta:+.1f}% WR). Need more live data.")

    # Break down by bet type — MLB's main signal is that RL is the only
    # profitable market; the retro sweep should tell us whether toggling
    # factors off shifts ML/OU/1st INN toward 50%+.
    print(f"\n── Hypothetical WR by bet type ──")
    by_bt: dict[str, list[int]] = {}
    for bt, _pk, _mu, act, verdict in details:
        row_acc = by_bt.setdefault(bt, [0, 0])  # [hypo_w, hypo_l]
        if verdict == "same":
            if act == "win": row_acc[0] += 1
            else: row_acc[1] += 1
        elif verdict == "flipped":
            if act == "win": row_acc[1] += 1
            else: row_acc[0] += 1
    for bt, (w, l) in sorted(by_bt.items()):
        if w + l > 0:
            print(f"  {bt:8s}: {w}-{l}  WR={_wr(w, l):5.1f}%")

    # Show the flipped picks specifically
    print(f"\n── Picks that would have flipped ──")
    flipped_details = [d for d in details if d[4] == "flipped"]
    for bt, pick, mu, act, _ in flipped_details[:20]:
        new_res = "W" if act == "loss" else "L"
        print(f"  {mu:25s} {bt:8s} {pick:14s} was {act.upper()} -> would be {new_res}")

    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    main()
