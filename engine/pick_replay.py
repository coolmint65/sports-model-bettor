"""Replay settled picks through the CURRENT picks_core pipeline.

Counterfactual: "if we had today's pipeline 30 days ago, would we have
made these picks?" For every settled pick (post model-stack cutoff),
re-score with the current calibration + closing-line prior + edge
floors and report:

  - kept: pipeline would still fire this pick
  - rejected: pipeline would now reject; report the gate that caught it
  - kept-but-changed: still fires but with different prob/edge

Output also rolls up the win/loss/profit for the kept-vs-rejected
buckets so you can see whether the new pipeline preserves winners
and rejects losers (the ideal outcome) or rejects both indiscriminately.

CLI::

    python -m engine.pick_replay              # all sports
    python -m engine.pick_replay --sport mlb  # one sport
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from datetime import datetime
from typing import Iterable

logger = logging.getLogger(__name__)


_SPORT_SOURCES = {
    "mlb": ("engine.db", "picks", -200),
    "nhl": ("engine.nhl_tracker._helpers", "nhl_picks", -200),
    "nba": ("engine.nba_db", "nba_picks", -200),
    "tennis": ("engine.tennis_db", "tennis_picks", -200),
}


def _get_conn(sport: str):
    if sport == "nhl":
        from .nhl_tracker._helpers import _get_nhl_db
        return _get_nhl_db()
    mod_path = _SPORT_SOURCES[sport][0]
    module = __import__(mod_path, fromlist=["get_conn"])
    return module.get_conn()


def _load_settled(sport: str) -> list[dict]:
    """Pull settled picks with the inputs needed to re-score."""
    from .model_cutoffs import get_cutoff
    cutoff = get_cutoff(sport) or "1900-01-01"
    table = _SPORT_SOURCES[sport][1]
    conn = _get_conn(sport)
    # Tennis uses `match_id` not `game_id`; alias for uniform handling.
    id_col = "match_id AS game_id" if sport == "tennis" else "game_id"
    rows = conn.execute(
        f"SELECT id, date, matchup, bet_type, pick, model_prob, "
        f"       odds, result, profit, {id_col} "
        f"FROM {table} "
        f"WHERE result IN ('W', 'L') "
        f"  AND model_prob IS NOT NULL "
        f"  AND date >= ?",
        (cutoff,),
    ).fetchall()
    return [dict(r) for r in rows]


def _replay_pick(pick: dict, sport: str) -> dict:
    """Replay one pick through current picks_core. Uses model_prob as
    raw_prob (the original model output BEFORE today's calibration).
    Returns {kept, reject_reason, new_prob, new_edge}."""
    from .picks_core import score_pick
    juice_wall = _SPORT_SOURCES[sport][2]
    # We need the RAW model prob, not the calibrated one. The picks
    # table stores `model_prob` which is the calibrated value at
    # record-time. Best approximation: use it as raw_prob and accept
    # that the second-pass calibration may shrink it further. This
    # gives a CONSERVATIVE estimate (more rejections than the truth)
    # because we're double-calibrating.
    candidate = {
        "type": pick["bet_type"],
        "pick": pick["pick"],
        "raw_prob": float(pick["model_prob"]),
        "odds": int(pick["odds"]) if pick.get("odds") else 0,
        "game_id": str(pick.get("game_id") or ""),
    }
    if not candidate["odds"]:
        return {"kept": False, "reject_reason": "no_odds_recorded",
                "new_prob": None, "new_edge": None}
    try:
        scored = score_pick(candidate, sport=sport, juice_wall=juice_wall)
    except Exception as exc:
        return {"kept": False, "reject_reason": f"error:{exc}",
                "new_prob": None, "new_edge": None}
    if scored is None:
        # Look up the rejection reason from provenance (if recorded
        # during this replay). Pipe through pick_provenance.get for
        # the trace.
        from .pick_provenance import get as _prov_get
        import hashlib
        from datetime import datetime as _dt
        today = _dt.now().strftime("%Y-%m-%d")
        key = hashlib.md5(
            f"{candidate['game_id']}|{candidate['type']}|{candidate['pick']}".encode()
        ).hexdigest()[:16]
        trace = _prov_get(sport, today, key)
        reason = (trace["trace"].get("reject_reason")
                   if trace and trace.get("trace") else "unknown")
        return {"kept": False, "reject_reason": reason or "unknown",
                "new_prob": None, "new_edge": None}
    return {"kept": True, "reject_reason": None,
            "new_prob": scored.get("prob"),
            "new_edge": scored.get("edge")}


def replay(sport: str) -> dict:
    """Replay every settled pick for ``sport``, return summary."""
    picks = _load_settled(sport)
    if not picks:
        return {"sport": sport, "n": 0}

    kept_total = 0
    rejected_total = 0
    kept_wins = 0
    kept_losses = 0
    kept_profit = 0.0
    rejected_wins = 0
    rejected_losses = 0
    rejected_profit = 0.0
    rejection_reasons: dict[str, int] = defaultdict(int)
    by_bet_type: dict[str, dict] = defaultdict(
        lambda: {"n": 0, "kept": 0, "rejected": 0,
                  "kept_profit": 0.0, "rejected_profit": 0.0}
    )

    for p in picks:
        result = _replay_pick(p, sport)
        bt = p["bet_type"]
        by_bet_type[bt]["n"] += 1
        profit = float(p.get("profit") or 0)
        won = p["result"] == "W"
        if result["kept"]:
            kept_total += 1
            by_bet_type[bt]["kept"] += 1
            by_bet_type[bt]["kept_profit"] += profit
            kept_profit += profit
            if won: kept_wins += 1
            else:   kept_losses += 1
        else:
            rejected_total += 1
            by_bet_type[bt]["rejected"] += 1
            by_bet_type[bt]["rejected_profit"] += profit
            rejected_profit += profit
            rejection_reasons[result["reject_reason"]] += 1
            if won: rejected_wins += 1
            else:   rejected_losses += 1

    return {
        "sport": sport,
        "n": len(picks),
        "kept": kept_total,
        "rejected": rejected_total,
        "kept_pct": round(kept_total / len(picks) * 100, 1),
        "kept_record": f"{kept_wins}-{kept_losses}",
        "kept_profit": round(kept_profit, 2),
        "kept_wr": round(kept_wins / max(1, kept_wins + kept_losses) * 100, 1),
        "rejected_record": f"{rejected_wins}-{rejected_losses}",
        "rejected_profit": round(rejected_profit, 2),
        "rejected_wr": round(rejected_wins / max(1, rejected_wins + rejected_losses) * 100, 1),
        "rejection_reasons": dict(rejection_reasons),
        "by_bet_type": {bt: {
            **info,
            "kept_profit": round(info["kept_profit"], 2),
            "rejected_profit": round(info["rejected_profit"], 2),
        } for bt, info in by_bet_type.items()},
    }


def main():
    p = argparse.ArgumentParser(description="Replay settled picks through current pipeline")
    p.add_argument("--sport", choices=("mlb", "nhl", "nba", "tennis"),
                   help="One sport (default: all)")
    args = p.parse_args()
    logging.basicConfig(level=logging.WARNING,
                        format="%(asctime)s %(levelname)s: %(message)s")

    sports = (args.sport,) if args.sport else ("mlb", "nhl", "nba", "tennis")
    for sport in sports:
        r = replay(sport)
        print(f"\n=== {sport.upper()} replay (n={r['n']}) ===")
        if r["n"] == 0:
            print("  (no settled picks post-cutoff)")
            continue
        print(f"  KEPT     {r['kept']:>3d} ({r['kept_pct']}%)  W-L={r['kept_record']}  "
              f"WR={r['kept_wr']}%  profit=${r['kept_profit']:+.0f}")
        print(f"  REJECTED {r['rejected']:>3d}              W-L={r['rejected_record']}  "
              f"WR={r['rejected_wr']}%  profit=${r['rejected_profit']:+.0f}")
        if r["rejection_reasons"]:
            print(f"  Reject reasons: {r['rejection_reasons']}")
        print(f"  By bet_type:")
        for bt, info in sorted(r["by_bet_type"].items(),
                                key=lambda kv: -kv[1]["n"]):
            net_change = info["kept_profit"] - info["rejected_profit"]
            verdict = ("BETTER" if net_change > 50
                       else ("WORSE" if net_change < -50 else "neutral"))
            print(f"    {bt:18s} n={info['n']:>3d}  kept={info['kept']:>3d}  "
                  f"rej={info['rejected']:>3d}  kept_p=${info['kept_profit']:+.0f}  "
                  f"rej_p=${info['rejected_profit']:+.0f}  {verdict}")


if __name__ == "__main__":
    main()
