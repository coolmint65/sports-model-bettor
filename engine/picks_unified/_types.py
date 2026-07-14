"""Canonical pick types.

Every sport produces these. Every reader consumes these. The shape is
deliberately flat — no nested config, no per-sport extensions buried in
JSON columns. If a sport needs something not here, we add a column and
NULL it for sports that don't care.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


class Scope(str, Enum):
    """When in the game does this market resolve?

    Flat enum chosen over structured `{period_type, index}` because
    99% of callers want a string match, and the structured form was
    just three extra lookups. Live picks get `LIVE_*` variants so a
    Q3 live-in-progress pick is distinguishable from a Q3 pre-game
    pick.
    """
    FULL = "full"
    # Halves (NCAAM, soccer)
    H1 = "h1"
    H2 = "h2"
    # Quarters (NBA / WNBA / NCAAM / AFL)
    Q1 = "q1"
    Q2 = "q2"
    Q3 = "q3"
    Q4 = "q4"
    # Periods (NHL / framework hockey)
    P1 = "p1"
    P2 = "p2"
    P3 = "p3"
    # Live in-game (single bucket — the period it fired against is
    # encoded in `pick_at_period` column for analysis)
    LIVE = "live"


class Variant(str, Enum):
    """Market variant — orthogonal to scope. An ALT spread on Q1 is
    `(scope=Q1, variant=ALT)`; a player prop is `(scope=FULL, variant=PROP)`.
    """
    MAIN = "main"             # headline ML / SPREAD / TOTAL / etc.
    ALT = "alt"               # alt spread / alt total / alt RL
    PROP = "prop"             # player props
    DERIVATIVE = "derivative" # NRFI/YRFI, BTS, Period DNB, Team Total


class Result(str, Enum):
    """Lifecycle outcome. Picks start with `result=None` (pending),
    transition through one of these."""
    WIN = "W"
    LOSS = "L"
    PUSH = "P"     # line landed on the spread/total exactly
    VOID = "V"     # match cancelled, postponed, or superseded by recorder


@dataclass
class Pick:
    """The canonical pick row. ``id`` is None pre-insert; recorder fills
    it in. Optional fields default to None so a soccer pick doesn't have
    to think about NBA's `pick_at_period`."""

    # ── Identity ──────────────────────────────────────────────
    sport: str             # "mlb" | "nba" | "nhl" | "wnba" | "ufl" | etc.
    league: str            # "nba" | "wnba" | "ahl" | "eng_premier" | etc.
    game_key: str          # "{sport}:{league}:{native_id}" — see _game_key.py
    pick_date: str         # YYYY-MM-DD — the GAME date, not the pick creation date
    matchup: str           # "MIN @ WAS" — display string, away first

    # ── Market ────────────────────────────────────────────────
    scope: Scope
    bet_type: str          # "ML" | "SPREAD" | "TOTAL" | "AH" | "DC" | "DNB" | etc.
    variant: Variant
    pick_text: str         # "MIN -13.5" | "Over 167.5" | "Haaland 2+ Goals"
    side: str | None = None  # "home" | "away" | "over" | "under" | "draw" | etc.
    line: float | None = None  # spread/total number; None for ML

    # ── Pricing ───────────────────────────────────────────────
    odds: int = 0          # American odds at lock time
    closing_odds: int | None = None
    closing_captured_at: str | None = None  # ISO timestamp

    # ── Probabilities ─────────────────────────────────────────
    prob: float = 0.0      # post-calibration, post-blend probability
    prob_raw: float | None = None  # uncalibrated model output (debug aid)

    # ── Edge + sizing ─────────────────────────────────────────
    edge_pct: float = 0.0  # (prob - market_implied) * 100
    stake_units: float = 0.0  # Quarter-Kelly sized; 0 = shadow or sub-floor
    confidence: str | None = None  # "probation" | "over_confident" | "shadow" | None
    is_shadow: bool = False  # convenience flag

    # ── Lifecycle ─────────────────────────────────────────────
    id: int | None = None
    created_at: str | None = None  # ISO; recorder fills this
    settled_at: str | None = None  # ISO
    result: Result | None = None
    profit: float | None = None  # $100-unit basis; tracker multiplies by stake

    # ── Live-pick metadata (NBA/NHL/WNBA derivative picks) ────
    pick_at_period: int | None = None
    pick_at_clock_secs: int | None = None
    pick_at_home_score: int | None = None
    pick_at_away_score: int | None = None

    # ── Provenance (helps debug calibration regressions) ──────
    model_version: str | None = None
    pipeline_version: str | None = None

    def to_row(self) -> dict[str, Any]:
        """Convert to the dict shape the recorder INSERTs. Enums become
        their string values; ``None`` columns stay None."""
        out = asdict(self)
        # Enum → string
        if isinstance(out.get("scope"), Scope):
            out["scope"] = out["scope"].value
        if isinstance(out.get("variant"), Variant):
            out["variant"] = out["variant"].value
        if isinstance(out.get("result"), Result):
            out["result"] = out["result"].value
        # SQLite stores bool as int
        out["is_shadow"] = 1 if out.get("is_shadow") else 0
        return out

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Pick":
        """Rehydrate from a DB row dict. Inverse of ``to_row``."""
        d = dict(row)
        # String → enum (tolerant of NULL)
        if d.get("scope"):
            d["scope"] = Scope(d["scope"])
        if d.get("variant"):
            d["variant"] = Variant(d["variant"])
        if d.get("result"):
            d["result"] = Result(d["result"])
        # Bool from int
        d["is_shadow"] = bool(d.get("is_shadow"))
        # Drop any extra columns we don't know about
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in d.items() if k in known})
