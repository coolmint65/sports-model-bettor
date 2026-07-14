"""
Tennis GBM feature extraction.

Builds per-match feature dicts for training + inference. Walk-forward
point-in-time stats only — every aggregate is computed from matches
strictly BEFORE the target date so training has no leakage.

Feature groups (per ``factors=noise`` directive):

  Per-player rolling (last 20 matches, surface-aware):
    - matches played (volume proxy)
    - W% overall + W% on surface
    - serve % (1stIn / svpt), 1st-serve win %, 2nd-serve win %, BP saved %

  Per-player static:
    - age, hand (R/L coded as numeric), height_cm

  Per-player dynamic:
    - surface Elo, 'all' Elo
    - days since last match (rest / fatigue proxy)

  Match-level:
    - surface (one-hot)
    - best_of (3 or 5)
    - tournament level encoded

  Derived deltas (the ones that carry real signal — gap features
  let the GBM split cleanly on relative strength rather than absolute):
    - elo_gap_surface, elo_gap_all
    - age_gap, height_gap
    - serve_pct_gap, win_pct_gap

What's NOT in (factors=noise):
  - Country / nationality (no documented effect at this granularity)
  - Match round (collinear with tournament prestige already encoded)
  - Week-of-year / Slam-vs-tour (encoded via tournament level)
  - Head-to-head records (small-sample noise on most pairs)
  - Hand-vs-hand interaction (Elo absorbs over time)

Targets (one classifier + two regressors per (tour, target)):
  - p1_win        — classification, did the FIRST player listed win?
  - total_games   — regression, sum of games across all sets
  - straight_sets — classification, did the winner take it in straight
                     sets (BO3 = 2-0, BO5 = 3-0)?

Important convention
--------------------
Sackmann row stores winner first ('w_*' columns); we flip in feature
extraction so 'p1' / 'p2' is randomised per match by hash. Without
randomisation a classifier learns "p1 always wins" which is useless.
The target gets flipped to match. See ``_assign_p1`` below.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)


# League-average defaults. Used when a player has no prior history
# (debut match) so the model sees a neutral prior instead of NaN.
_TENNIS_DEFAULTS = {
    "p1_matches_l20": 0,
    "p2_matches_l20": 0,
    "p1_matches_surface_l20": 0,
    "p2_matches_surface_l20": 0,
    "p1_win_pct_l20": 0.500,
    "p2_win_pct_l20": 0.500,
    "p1_win_pct_surface_l20": 0.500,
    "p2_win_pct_surface_l20": 0.500,
    "p1_serve_pct": 0.62,           # 1stIn / svpt approximate league avg
    "p2_serve_pct": 0.62,
    "p1_first_serve_won_pct": 0.72,
    "p2_first_serve_won_pct": 0.72,
    "p1_second_serve_won_pct": 0.50,
    "p2_second_serve_won_pct": 0.50,
    "p1_bp_saved_pct": 0.62,
    "p2_bp_saved_pct": 0.62,
    "p1_age": 26.0,
    "p2_age": 26.0,
    "p1_hand_r": 1, "p1_hand_l": 0,
    "p2_hand_r": 1, "p2_hand_l": 0,
    "p1_height_cm": 185,
    "p2_height_cm": 185,
    "p1_rest_days": 7,
    "p2_rest_days": 7,
    "p1_elo_surface": 1500.0,
    "p2_elo_surface": 1500.0,
    "p1_elo_all": 1500.0,
    "p2_elo_all": 1500.0,
    # Match-level
    "best_of": 3,
    "is_slam": 0,
    "is_masters": 0,
    "surface_hard": 1, "surface_clay": 0,
    "surface_grass": 0, "surface_carpet": 0,
    # Deltas
    "elo_gap_surface": 0.0,
    "elo_gap_all": 0.0,
    "age_gap": 0.0,
    "height_gap": 0,
    "serve_pct_gap": 0.0,
    "win_pct_gap": 0.0,
}

FEATURE_NAMES = list(_TENNIS_DEFAULTS.keys())

_TOURNEY_LEVEL_FLAGS = {
    "G":  ("is_slam",     1),
    "M":  ("is_masters",  1),
    "P":  ("is_masters",  1),  # WTA Premier Mandatory ~ Masters tier
    "PM": ("is_masters",  1),
    "F":  ("is_masters",  1),  # Tour Finals — top-tier
}


# ── Walking history (for training-time efficiency) ───────────────

class PlayerHistory:
    """Maintains a rolling per-player buffer of recent match outcomes
    + serve stats, partitioned by surface. Designed to be walked
    chronologically through Sackmann matches: ``snapshot()`` reads
    pre-match state, ``record()`` updates state with the match result.

    Storage strategy: keep last 20 matches per (player, surface) and
    last 20 overall. Older matches drop out. Memory cost ~constant
    per player (40 dicts × ~10 floats).
    """

    def __init__(self, window: int = 20) -> None:
        self.window = window
        # buf: tour -> player_id -> {'all': deque, surface: deque}
        self.buf: dict[str, dict[int, dict[str, list]]] = {}
        # last_match: tour -> player_id -> 'YYYY-MM-DD'
        self.last_match: dict[str, dict[int, str]] = {}

    def _player_buckets(self, tour: str, pid: int) -> dict[str, list]:
        return self.buf.setdefault(tour, {}).setdefault(
            pid, {"all": [], "Hard": [], "Clay": [], "Grass": [],
                   "Carpet": []})

    def snapshot(self, tour: str, pid: int, surface: str,
                  match_date: str) -> dict:
        """Return rolling stats AS OF the start of the given match,
        i.e. before applying its result."""
        buckets = self._player_buckets(tour, pid)
        all_buf = buckets["all"]
        surf_buf = buckets.get(surface) or []

        def _wp(buf):
            n = len(buf)
            if n == 0:
                return 0.5
            return sum(1 for b in buf if b["win"]) / n

        # Aggregate serve stats from the recent overall buffer
        svpt = sum(b["svpt"] or 0 for b in all_buf)
        first_in = sum(b["first_in"] or 0 for b in all_buf)
        first_won = sum(b["first_won"] or 0 for b in all_buf)
        second_won = sum(b["second_won"] or 0 for b in all_buf)
        second_pts = max(0, svpt - first_in)
        bp_saved = sum(b["bp_saved"] or 0 for b in all_buf)
        bp_faced = sum(b["bp_faced"] or 0 for b in all_buf)

        serve_pct = first_in / svpt if svpt > 0 else 0.62
        first_won_pct = first_won / first_in if first_in > 0 else 0.72
        second_won_pct = (second_won / second_pts
                           if second_pts > 0 else 0.50)
        bp_pct = bp_saved / bp_faced if bp_faced > 0 else 0.62

        last = self.last_match.get(tour, {}).get(pid)
        rest_days = 7
        if last:
            try:
                rest_days = max(0, (datetime.strptime(match_date, "%Y-%m-%d")
                                     - datetime.strptime(last, "%Y-%m-%d")).days)
            except ValueError:
                pass

        return {
            "matches_l20": len(all_buf),
            "matches_surface_l20": len(surf_buf),
            "win_pct_l20": _wp(all_buf),
            "win_pct_surface_l20": _wp(surf_buf),
            "serve_pct": serve_pct,
            "first_serve_won_pct": first_won_pct,
            "second_serve_won_pct": second_won_pct,
            "bp_saved_pct": bp_pct,
            "rest_days": min(rest_days, 90),
        }

    def record(self, tour: str, pid: int, surface: str, match_date: str,
                won: bool, svpt: int | None, first_in: int | None,
                first_won: int | None, second_won: int | None,
                bp_saved: int | None, bp_faced: int | None) -> None:
        buckets = self._player_buckets(tour, pid)
        entry = {"win": bool(won), "date": match_date,
                  "svpt": svpt or 0, "first_in": first_in or 0,
                  "first_won": first_won or 0,
                  "second_won": second_won or 0,
                  "bp_saved": bp_saved or 0, "bp_faced": bp_faced or 0}
        buckets["all"].append(entry)
        if len(buckets["all"]) > self.window:
            buckets["all"].pop(0)
        if surface in buckets:
            buckets[surface].append(entry)
            if len(buckets[surface]) > self.window:
                buckets[surface].pop(0)
        self.last_match.setdefault(tour, {})[pid] = match_date


# ── Per-match feature builder ─────────────────────────────────

def _hand_flags(hand: str | None) -> tuple[int, int]:
    h = (hand or "").upper().strip()
    return (1 if h == "R" else 0, 1 if h == "L" else 0)


def _surface_flags(surface: str | None) -> dict[str, int]:
    s = (surface or "Hard").strip().lower()
    return {
        "surface_hard":   1 if s == "hard" else 0,
        "surface_clay":   1 if s == "clay" else 0,
        "surface_grass":  1 if s == "grass" else 0,
        "surface_carpet": 1 if s == "carpet" else 0,
    }


def _level_flags(level: str | None) -> dict[str, int]:
    out = {"is_slam": 0, "is_masters": 0}
    if not level:
        return out
    flag, val = _TOURNEY_LEVEL_FLAGS.get(level.strip().upper(), (None, 0))
    if flag:
        out[flag] = val
    return out


def _assign_p1(match: dict) -> tuple[int, int]:
    """Decide which player is p1 vs p2. Hash on match_id so the
    assignment is deterministic but uniform — half the matches have
    the actual winner as p1, half as p2. Without this the classifier
    learns the trivial 'always pick p1' rule.
    """
    winner_id = int(match.get("winner_id") or 0)
    loser_id = int(match.get("loser_id") or 0)
    h = abs(hash(match.get("match_id") or "")) & 1
    if h == 0:
        return winner_id, loser_id   # winner is p1
    return loser_id, winner_id        # loser is p1


def extract_match_features(history: PlayerHistory, match: dict,
                            elo_lookup) -> dict | None:
    """Return a feature dict for one historical match. ``elo_lookup``
    is a callable ``(tour, pid, surface) -> {rating, ...}`` so the
    caller can reuse a cached rating table or pull live."""
    if not match:
        return None
    tour = (match.get("tour") or "").lower()
    if tour not in ("atp", "wta"):
        return None

    p1_id, p2_id = _assign_p1(match)
    if p1_id == 0 or p2_id == 0:
        return None
    p1_is_winner = (p1_id == int(match.get("winner_id") or 0))

    surface = match.get("surface") or "Hard"
    match_date = match.get("tourney_date")
    if not match_date:
        return None

    # Rolling stats
    p1_snap = history.snapshot(tour, p1_id, surface, match_date)
    p2_snap = history.snapshot(tour, p2_id, surface, match_date)

    # Static per-player from the match row itself (Sackmann ships
    # age + hand + height inline so we don't need a join)
    if p1_is_winner:
        p1_hand = match.get("winner_hand")
        p2_hand = match.get("loser_hand")
        p1_age = match.get("winner_age")
        p2_age = match.get("loser_age")
    else:
        p1_hand = match.get("loser_hand")
        p2_hand = match.get("winner_hand")
        p1_age = match.get("loser_age")
        p2_age = match.get("winner_age")

    p1_hr, p1_hl = _hand_flags(p1_hand)
    p2_hr, p2_hl = _hand_flags(p2_hand)

    # Elo lookups
    e_p1_surf = elo_lookup(tour, p1_id, surface) or {}
    e_p2_surf = elo_lookup(tour, p2_id, surface) or {}
    e_p1_all  = elo_lookup(tour, p1_id, "all") or {}
    e_p2_all  = elo_lookup(tour, p2_id, "all") or {}
    p1_elo_surf = float(e_p1_surf.get("rating") or 1500.0)
    p2_elo_surf = float(e_p2_surf.get("rating") or 1500.0)
    p1_elo_all  = float(e_p1_all.get("rating") or 1500.0)
    p2_elo_all  = float(e_p2_all.get("rating") or 1500.0)

    out = dict(_TENNIS_DEFAULTS)
    out["p1_matches_l20"] = p1_snap["matches_l20"]
    out["p2_matches_l20"] = p2_snap["matches_l20"]
    out["p1_matches_surface_l20"] = p1_snap["matches_surface_l20"]
    out["p2_matches_surface_l20"] = p2_snap["matches_surface_l20"]
    out["p1_win_pct_l20"] = p1_snap["win_pct_l20"]
    out["p2_win_pct_l20"] = p2_snap["win_pct_l20"]
    out["p1_win_pct_surface_l20"] = p1_snap["win_pct_surface_l20"]
    out["p2_win_pct_surface_l20"] = p2_snap["win_pct_surface_l20"]
    out["p1_serve_pct"] = p1_snap["serve_pct"]
    out["p2_serve_pct"] = p2_snap["serve_pct"]
    out["p1_first_serve_won_pct"] = p1_snap["first_serve_won_pct"]
    out["p2_first_serve_won_pct"] = p2_snap["first_serve_won_pct"]
    out["p1_second_serve_won_pct"] = p1_snap["second_serve_won_pct"]
    out["p2_second_serve_won_pct"] = p2_snap["second_serve_won_pct"]
    out["p1_bp_saved_pct"] = p1_snap["bp_saved_pct"]
    out["p2_bp_saved_pct"] = p2_snap["bp_saved_pct"]
    out["p1_age"] = float(p1_age or 26.0)
    out["p2_age"] = float(p2_age or 26.0)
    out["p1_hand_r"] = p1_hr; out["p1_hand_l"] = p1_hl
    out["p2_hand_r"] = p2_hr; out["p2_hand_l"] = p2_hl
    out["p1_rest_days"] = p1_snap["rest_days"]
    out["p2_rest_days"] = p2_snap["rest_days"]
    out["p1_elo_surface"] = p1_elo_surf
    out["p2_elo_surface"] = p2_elo_surf
    out["p1_elo_all"] = p1_elo_all
    out["p2_elo_all"] = p2_elo_all
    out["best_of"] = int(match.get("best_of") or 3)
    out.update(_surface_flags(surface))
    out.update(_level_flags(match.get("tourney_level")))

    # Deltas
    out["elo_gap_surface"] = p1_elo_surf - p2_elo_surf
    out["elo_gap_all"]     = p1_elo_all - p2_elo_all
    out["age_gap"]         = out["p1_age"] - out["p2_age"]
    out["serve_pct_gap"]   = (p1_snap["serve_pct"]
                               - p2_snap["serve_pct"])
    out["win_pct_gap"]     = (p1_snap["win_pct_l20"]
                               - p2_snap["win_pct_l20"])

    # Update history with this match's stats so subsequent matches see
    # the post-result state. Records use match_id-derived assignment.
    if p1_is_winner:
        history.record(tour, p1_id, surface, match_date, won=True,
                        svpt=match.get("w_svpt"),
                        first_in=match.get("w_1stIn"),
                        first_won=match.get("w_1stWon"),
                        second_won=match.get("w_2ndWon"),
                        bp_saved=match.get("w_bpSaved"),
                        bp_faced=match.get("w_bpFaced"))
        history.record(tour, p2_id, surface, match_date, won=False,
                        svpt=match.get("l_svpt"),
                        first_in=match.get("l_1stIn"),
                        first_won=match.get("l_1stWon"),
                        second_won=match.get("l_2ndWon"),
                        bp_saved=match.get("l_bpSaved"),
                        bp_faced=match.get("l_bpFaced"))
    else:
        history.record(tour, p1_id, surface, match_date, won=False,
                        svpt=match.get("l_svpt"),
                        first_in=match.get("l_1stIn"),
                        first_won=match.get("l_1stWon"),
                        second_won=match.get("l_2ndWon"),
                        bp_saved=match.get("l_bpSaved"),
                        bp_faced=match.get("l_bpFaced"))
        history.record(tour, p2_id, surface, match_date, won=True,
                        svpt=match.get("w_svpt"),
                        first_in=match.get("w_1stIn"),
                        first_won=match.get("w_1stWon"),
                        second_won=match.get("w_2ndWon"),
                        bp_saved=match.get("w_bpSaved"),
                        bp_faced=match.get("w_bpFaced"))

    return out


def extract_match_target(match: dict) -> dict | None:
    """Build the target row for a match."""
    if not match:
        return None
    p1_id, p2_id = _assign_p1(match)
    if p1_id == 0 or p2_id == 0:
        return None
    p1_is_winner = (p1_id == int(match.get("winner_id") or 0))

    # Parse score for total games + straight-sets
    from ..tennis_tracker import _parse_score
    parsed = _parse_score(match.get("score"))
    if parsed is None:
        return None
    total_games = parsed["p1_games"] + parsed["p2_games"]
    if p1_is_winner:
        winner_sets = parsed["p1_sets"]
        loser_sets = parsed["p2_sets"]
    else:
        winner_sets = parsed["p2_sets"]
        loser_sets = parsed["p1_sets"]
    best_of = int(match.get("best_of") or 3)
    needed = 2 if best_of == 3 else 3
    straight = (winner_sets == needed and loser_sets == 0)

    return {
        "p1_win": 1 if p1_is_winner else 0,
        "total_games": int(total_games),
        "straight_sets": 1 if straight else 0,
    }


__all__ = [
    "FEATURE_NAMES", "PlayerHistory",
    "extract_match_features", "extract_match_target",
]
