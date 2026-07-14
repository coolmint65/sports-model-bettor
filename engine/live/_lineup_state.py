"""
NBA players-on-floor + foul state derivation from PBP.

Phase 5c. Walks the play-by-play in order to maintain two pieces of
state per team:

  - ``on_floor``: set of player IDs currently on the court
  - ``fouls``:    dict ``{player_id: foul_count}``

Starters aren't shipped explicitly in ESPN's PBP, but they're implied
by "any player who acts before they're subbed out" — i.e., a player
who appears as the participant of a non-substitution play before
ever being the subbed-IN side of a substitution must have been on
the floor at tip-off. We backfill the on-floor set this way as plays
unfold; by the time the first sub for a team has happened, we have
all 5 starters captured.

After that, on_floor evolves naturally:

  - Substitution: ``out`` leaves the set, ``in`` joins
  - Player A acts (rebound, shot, foul) while A is not in on_floor:
    A is implicitly added (he must have been on the floor to do it)

Foul state is the running count of fouls assessed AGAINST a player.
NBA fouls out at 6, but the actionable thresholds are:

  - 4 fouls in H1 / start of Q3 ⇒ likely sit early in Q3
  - 5 fouls late ⇒ coach plays them carefully

Snapshots
---------
``snapshot_at_period_end(plays, period)`` returns the on-floor + foul
state immediately after the period-end play. The intermission
predictor (5i / 5j) calls this so it knows which players are on the
floor heading into the next period and how many fouls they're
carrying.

Edge cases handled
------------------
- Two-player swaps where ESPN emits a single Substitution play with
  both participants — first participant is IN, second is OUT.
- "Player X enters the game for Y" text format double-checked when
  participant order is ambiguous.
- Free-throw shooters who get fouled (the FOULER is the player on the
  defending team; the SHOOTER on the offensive team).
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


_FOUL_TYPES = {
    "shooting foul", "personal foul", "offensive foul",
    "loose ball foul", "technical foul", "flagrant foul",
    "personal take foul", "personal block foul",
    "transition take foul",
}

# "Austin Reaves enters the game for Marcus Smart"
_SUB_TEXT_RE = re.compile(
    r"^(?P<inn>.+?)\s+enters\s+the\s+game\s+for\s+(?P<out>.+?)\s*$",
    re.IGNORECASE,
)


def _is_substitution(play: dict) -> bool:
    return (play.get("type_text") or "").strip().lower() == "substitution"


def _sub_participants(play: dict) -> tuple[str | None, str | None]:
    """Return (player_in_id, player_out_id) for a Substitution play.

    Two paths:
      1. participants array — ESPN convention is [in, out] per the
         play text "X enters the game for Y"
      2. parse the text directly when participants don't have IDs

    When both fail we return (None, None) so the caller can ignore
    the malformed sub instead of corrupting the on-floor set.
    """
    parts = play.get("participants") or []
    in_id = parts[0].get("athlete_id") if len(parts) >= 1 else None
    out_id = parts[1].get("athlete_id") if len(parts) >= 2 else None
    return (in_id, out_id)


def _foul_player(play: dict) -> str | None:
    """The first participant on a foul play is the fouler. ESPN's
    convention is consistent across NBA fouls (Shooting/Personal/
    Offensive)."""
    parts = play.get("participants") or []
    if not parts:
        return None
    return parts[0].get("athlete_id")


def derive_lineup_state(plays: list[dict],
                         home_team_id: str | None,
                         away_team_id: str | None,
                         through_period: int | None = None) -> dict:
    """Walk plays in order and return final lineup + foul state for
    both teams as of ``through_period`` (or the entire input when
    None).

    Two-pass implementation. ESPN's PBP doesn't tag each participant
    with a team_id, but most plays include participants from BOTH
    teams (shooter + defender, passer + stealer). Pass 1 builds an
    authoritative ``player_to_team`` mapping using only plays where
    the team attribution is unambiguous:

      - Substitution: both participants belong to play.team_id (the
        sub-in and sub-out are both that team's roster)
      - Foul: participants[0] is the fouler, on play.team_id
      - Single-participant non-sub plays: participants[0] is the
        actor, on play.team_id

    Pass 2 uses that mapping to attribute every participant to the
    correct team's on-floor set, regardless of which team's offense
    triggered the play.

    Returns::

        {
          "home": {"on_floor": set[str], "fouls": {pid: count}},
          "away": {...},
          "subs_processed": int,
          "fouls_processed": int,
          "player_to_team": dict[pid, "home" | "away"],
        }
    """
    home_id = str(home_team_id or "")
    away_id = str(away_team_id or "")

    # ── Pass 1: build player -> team mapping from unambiguous plays ──
    player_to_team: dict[str, str] = {}
    for p in plays or []:
        try:
            period = int(p.get("period") or 0)
        except (TypeError, ValueError):
            period = 0
        if through_period is not None and period > through_period:
            break
        team_id = str(p.get("team_id") or "")
        if not team_id:
            continue
        if team_id == home_id:
            label = "home"
        elif team_id == away_id:
            label = "away"
        else:
            continue
        type_text = (p.get("type_text") or "").strip().lower()
        parts = p.get("participants") or []
        if not parts:
            continue
        if _is_substitution(p):
            # Both participants belong to play.team_id.
            for pp in parts:
                pid = pp.get("athlete_id")
                if pid:
                    player_to_team.setdefault(pid, label)
        elif type_text in _FOUL_TYPES:
            pid = parts[0].get("athlete_id")
            if pid:
                player_to_team.setdefault(pid, label)
        elif len(parts) == 1:
            pid = parts[0].get("athlete_id")
            if pid:
                player_to_team.setdefault(pid, label)
        else:
            # Multi-participant non-sub: only participants[0] is
            # guaranteed on play.team_id (the actor). Subsequent
            # participants (assister, defender, blocker) may be
            # opposite team; skip them for the mapping pass.
            pid = parts[0].get("athlete_id")
            if pid:
                player_to_team.setdefault(pid, label)

    # ── Pass 2: walk plays applying the mapping ─────────────────────
    home_state = {"on_floor": set(), "fouls": {}}
    away_state = {"on_floor": set(), "fouls": {}}
    subs_processed = 0
    fouls_processed = 0

    def _bucket_for_pid(pid: str | None):
        if not pid:
            return None
        label = player_to_team.get(pid)
        if label == "home":
            return home_state
        if label == "away":
            return away_state
        return None

    for p in plays or []:
        try:
            period = int(p.get("period") or 0)
        except (TypeError, ValueError):
            period = 0
        if through_period is not None and period > through_period:
            break

        team_id = str(p.get("team_id") or "")
        if not team_id or team_id not in (home_id, away_id):
            continue

        type_text = (p.get("type_text") or "").strip().lower()

        if _is_substitution(p):
            in_id, out_id = _sub_participants(p)
            bucket = home_state if team_id == home_id else away_state
            if out_id:
                bucket["on_floor"].discard(out_id)
            if in_id:
                bucket["on_floor"].add(in_id)
            subs_processed += 1
            continue

        if type_text in _FOUL_TYPES:
            pid = _foul_player(p)
            bucket = _bucket_for_pid(pid)
            if pid and bucket is not None:
                bucket["fouls"][pid] = bucket["fouls"].get(pid, 0) + 1
                bucket["on_floor"].add(pid)
                fouls_processed += 1
            continue

        # Any other play — attribute each participant to its OWN team.
        for participant in (p.get("participants") or []):
            pid = participant.get("athlete_id")
            bucket = _bucket_for_pid(pid)
            if bucket is not None:
                bucket["on_floor"].add(pid)

    return {
        "home": home_state,
        "away": away_state,
        "subs_processed": subs_processed,
        "fouls_processed": fouls_processed,
        "player_to_team": player_to_team,
    }


def snapshot_at_period_end(plays: list[dict],
                            home_team_id: str | None,
                            away_team_id: str | None,
                            period: int) -> dict:
    """Lineup + foul snapshot at the END of ``period``. Convenience
    wrapper around derive_lineup_state with the natural period bound.

    Caller use case: 5i's halftime predictor calls
    ``snapshot_at_period_end(..., period=2)`` to know who's on the
    floor coming out of halftime and which players are carrying foul
    trouble into Q3.
    """
    return derive_lineup_state(plays, home_team_id, away_team_id,
                                through_period=period)


# ── Foul-trouble adjustment helpers ──────────────────────────

def in_foul_trouble(fouls: int, period_just_ended: int) -> bool:
    """Heuristic: a player carrying enough fouls to materially limit
    their next-period minutes.

    Thresholds match standard NBA coaching tendencies:
      - 3+ fouls leaving Q1 (rare, but very limiting)
      - 4+ fouls leaving H1 (very common Q3 sit)
      - 5+ fouls leaving Q3 (one foul from out)
    """
    if period_just_ended <= 1:
        return fouls >= 3
    if period_just_ended == 2:
        return fouls >= 4
    if period_just_ended == 3:
        return fouls >= 5
    return False


def team_foul_trouble_count(fouls_by_player: dict[str, int],
                             period_just_ended: int) -> int:
    """Number of players on a team in foul trouble at the boundary."""
    return sum(1 for f in fouls_by_player.values()
               if in_foul_trouble(f, period_just_ended))


__all__ = [
    "derive_lineup_state",
    "snapshot_at_period_end",
    "in_foul_trouble",
    "team_foul_trouble_count",
]
