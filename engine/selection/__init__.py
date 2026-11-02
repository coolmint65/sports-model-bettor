"""engine.selection -- the single pick-selection authority (Workstream #1).

One place decides "the selected pick" for a game, calibrated through the same
gate the paper-POTD uses, so the card, hero, POTD, scoreboard and tracker all
show one answer instead of each re-deciding. See pick_authority_plan.
"""
from .authority import apply_selected
from .select_pick import select_pick, select_lean, card_lean
from . import edge_proven

__all__ = ["apply_selected", "select_pick", "select_lean", "card_lean", "edge_proven"]
