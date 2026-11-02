"""Suggested stake per player-prop pick -- the stake chip on the prop cards.

Moved here from backend/server/routes_props.py (2026-11-01) so the props POTD
(engine.player_props_potd) can show and grade the SAME stake as the card chip
without the engine importing the server. routes_props imports it back as
``_stake_tier``.
"""

from __future__ import annotations


# Suggested stake per pick (Austin 2026-11-01 "Sure" -- he bets the card for real). Tiers by how much the
# evidence backs the KIND of pick (graded HR lines 10-23..10-31):
#   NHL Skater SOG Under  -> 1u   HR shot Overs hit .384 vs .466 priced (n=463): a market Over bias
#   other NHL Unders      -> 1/2u Points/Assists/Saves win, but our model's own Under lean (-3..-5pp) there
#   NHL Overs             -> 1/4u against the shots bias
#   NBA / NFL props       -> 1/4u model ~ DraftKings; thin edges
# Heavy juice (<= -140) drops a tier.
def stake_tier(sport, p):
    bt, side = p.get("bet_type") or "", (p.get("side") or str(p.get("pick") or "").split(" ")[0]).title()
    if sport == "nhl" and bt == "Skater SOG" and side == "Under":
        u, why = 1.0, "HR shot Overs are overpriced (hit 38% vs 47% priced)"
    elif sport == "nhl" and side == "Under":
        u, why = 0.5, "winning, but our model leans Under on this stat"
    elif sport == "nhl":
        u, why = 0.25, "Over: against the shots/market bias"
    else:
        u, why = 0.25, "model about level with DraftKings; thin edge"
    try:
        if int(p.get("odds")) <= -140 and u > 0.25:
            u, why = u / 2, why + "; heavy juice"
    except (TypeError, ValueError):
        pass
    return {"units": u, "label": {1.0: "1u", 0.5: "\u00bdu", 0.25: "\u00bcu"}.get(u, f"{u:g}u"), "why": why}


__all__ = ["stake_tier"]
