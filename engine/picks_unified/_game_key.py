"""Universal game identifier.

Every pick stores its game as a string `"{sport}:{league}:{native_id}"`.
The native_id is whatever the sport's authoritative system uses:
ESPN event id for NBA/NHL/WNBA/football/baseball/soccer, MLB Stats
game_pk for MLB, theScore id for hockey-framework, soccer match.id, etc.

This module is the only place that knows how to parse/format game_keys.
Settlers ask GameKey.dispatch() to resolve a pick to its game state in
the per-sport DB.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GameKey:
    sport: str
    league: str
    native_id: str

    def __str__(self) -> str:
        return f"{self.sport}:{self.league}:{self.native_id}"

    @classmethod
    def parse(cls, key: str) -> "GameKey":
        parts = key.split(":", 2)
        if len(parts) != 3:
            raise ValueError(f"malformed game_key {key!r}")
        sport, league, native_id = parts
        if not (sport and league and native_id):
            raise ValueError(f"empty component in game_key {key!r}")
        return cls(sport=sport, league=league, native_id=native_id)

    @classmethod
    def for_mlb(cls, game_pk: int | str) -> "GameKey":
        return cls("mlb", "mlb", str(game_pk))

    @classmethod
    def for_nba(cls, espn_id: str) -> "GameKey":
        return cls("nba", "nba", str(espn_id))

    @classmethod
    def for_nhl(cls, game_id: str | int) -> "GameKey":
        return cls("nhl", "nhl", str(game_id))

    @classmethod
    def for_basketball(cls, league: str, espn_id: str) -> "GameKey":
        """WNBA, NCAAM, NCAAW, Euroleague, AFL, RealGM leagues."""
        return cls("basketball", league, str(espn_id))

    @classmethod
    def for_soccer(cls, league: str, match_id: int | str) -> "GameKey":
        return cls("soccer", league, str(match_id))

    @classmethod
    def for_hockey(cls, league: str, thescore_id: str | int) -> "GameKey":
        """AHL, PWHL, AIHL, NZIHL."""
        return cls("hockey", league, str(thescore_id))

    @classmethod
    def for_baseball(cls, league: str, espn_id: str) -> "GameKey":
        """College baseball, future KBO/NPB."""
        return cls("baseball", league, str(espn_id))

    @classmethod
    def for_football(cls, league: str, espn_id: str) -> "GameKey":
        """UFL, future NFL/NCAAF."""
        return cls("football", league, str(espn_id))

    @classmethod
    def for_tennis(cls, tour: str, match_id: str) -> "GameKey":
        """ATP, WTA. `tour` is the league slot."""
        return cls("tennis", tour, str(match_id))

    @classmethod
    def for_golf(cls, tour: str, tournament_id: str | int) -> "GameKey":
        """PGA, LPGA, Korn Ferry. `tour` is the league slot."""
        return cls("golf", tour, str(tournament_id))

    @classmethod
    def for_motorsports(cls, series: str, race_id: str | int) -> "GameKey":
        return cls("motorsports", series, str(race_id))

    def to_legacy_game_id(self) -> str:
        """Compatibility shim — return the bare native_id for code
        paths that still query the old per-sport games tables."""
        return self.native_id
