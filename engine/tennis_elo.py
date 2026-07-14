"""
Surface-weighted Elo trainer for tennis.

Builds per-(tour, player, surface) Elo ratings by walking the
``tennis_matches`` table chronologically and applying a per-match
update. Stores results in ``tennis_elo`` for the predictor to read.

Design choices (per ``factors=noise`` directive — see the same-named
memory file): only the levers that actually move tennis prediction
RMSE are in here. Anything more speculative gets cut.

Levers in the trainer
---------------------

1. **Surface specialisation**: a player has SEPARATE ratings for
   Hard / Clay / Grass / Carpet plus an "all" overall rating. Surface
   specificity is the single biggest tennis Elo improvement vs a
   single rating.

2. **Time decay (RD inflation)**: rating deviation grows when a
   player is inactive — a player coming back after 12 months is more
   uncertain. Standard Glicko-style: ``rd = sqrt(rd0² + c² × t_days)``.

3. **K-factor scaling by tournament level**: Slams (G) get K=32,
   Masters/WTA1000 (M / Premier Mandatory) get K=24, everything else
   K=16. Higher-stakes matches carry more rating signal because the
   selection bias is smaller (top players show up).

4. **Underdog match bonus**: a win as the underdog gets a small
   multiplier on the rating delta (marginal effect but documented).

What's NOT in
--------------

- Round bonus — round-of-tournament effect is collinear with K-factor
  and tournament level
- Margin-of-victory — sets won is helpful but adds variance from
  blowouts; standard Elo pretends a win is a win
- Head-to-head priors — kept as a separate read-time adjustment in
  the predictor, NOT folded into the rating
- Age curve — the rating itself absorbs age via natural drift
- Country / hand — too small effect, fits noise

Public API::

    train(tour=None, since_year=None) -> dict       # writes tennis_elo
    rating_for(tour, player_id, surface) -> dict | None
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Iterable

logger = logging.getLogger(__name__)


# ── Constants ──────────────────────────────────────────────────

# Glicko-style starting rating + RD. Players debut at rating=1500
# with a high RD that compresses as they play matches.
INIT_RATING = 1500.0
INIT_RD = 350.0
RD_FLOOR = 30.0     # minimum RD even for a hyper-active top player

# RD inflation rate per day of inactivity, in rating points² / day.
# Glicko's published c value tuned for chess (~30 rating-pts/day).
# Tennis tournaments are spaced 1-2 weeks apart, so we want a ratio
# that gives ~+50 RD per year of inactivity. c² × 365 = 2500 → c² ~ 7.
RD_DRIFT_PER_DAY_SQ = 7.0

# K-factor by tournament level. Sackmann's tourney_level codes:
# ATP: G=Slam, M=Masters 1000, A=ATP 250/500, F=Tour Finals, D=Davis Cup
# WTA: G=Slam, P (Premier) split into PM/P, T1/T2/...
# Conservative defaults; refine after first autopsy.
_K_BY_LEVEL = {
    "G": 32,   # Slam (BO5 ATP, BO3 WTA)
    "M": 24,   # Masters / WTA Premier Mandatory
    "F": 24,   # Year-end finals
    "A": 16,   # 250 / 500
    "P": 24,   # Older WTA Premier
    "PM": 24,
    "P5": 24,
    "P4": 20,
    "T1": 16,
    "T2": 14,
    # Sub-tour tiers — added 2026-05-01 along with Sackmann
    # Challenger / Futures / ITF ingest. Lower K because the field
    # is more variable round-to-round (tour pros parachuting in,
    # ranking volatility) so each match carries less long-term
    # signal than a tour-level result.
    "C":  10,  # Challenger
    "CH": 10,  # alt label some Sackmann rows use
    "S":  6,   # Satellite (legacy circuit)
    "I":  6,   # ITF women's circuit
    "15": 5,   # ITF $15K
    "25": 6,
    "35": 7,
    "50": 8,
    "60": 8,
    "75": 9,
    "100": 10, # ITF $100K (top of futures)
    "T3": 12,
    "T4": 10,
}
DEFAULT_K = 10  # was 16 — biased toward sub-tour matches now

# Underdog bonus: when a winner had lower pre-match rating, multiply
# the delta by this. Encodes "upsets are extra signal" lightly.
UPSET_BONUS = 1.10


# ── Surface routing ───────────────────────────────────────────

# Sackmann surface values: "Hard", "Clay", "Grass", "Carpet"; some
# rows have empty surface (Davis Cup oddities). Treat empty as Hard
# (most common globally) but log a sample so the operator notices if
# the unknown share spikes.
_KNOWN_SURFACES = ("Hard", "Clay", "Grass", "Carpet")


def _normalize_surface(surface: str | None) -> str:
    if not surface:
        return "Hard"
    s = surface.strip().title()
    return s if s in _KNOWN_SURFACES else "Hard"


# ── Rating math ───────────────────────────────────────────────

def _expected(rating_a: float, rating_b: float) -> float:
    """Standard Elo win-prob formula: 1 / (1 + 10 ** ((R_B - R_A) / 400))."""
    return 1.0 / (1.0 + 10.0 ** ((rating_b - rating_a) / 400.0))


def _k_for(level: str | None) -> float:
    if not level:
        return DEFAULT_K
    return _K_BY_LEVEL.get(level.strip().upper(), DEFAULT_K)


def _decay_rd(rd: float, last_match: str | None,
              match_date: str) -> float:
    """Inflate RD based on days since last match. Capped at INIT_RD
    so a years-out player doesn't end up at RD=1000."""
    if not last_match:
        return rd
    try:
        last = datetime.strptime(last_match, "%Y-%m-%d")
        cur = datetime.strptime(match_date, "%Y-%m-%d")
    except ValueError:
        return rd
    days = max(0, (cur - last).days)
    if days == 0:
        return rd
    new_rd_sq = rd * rd + RD_DRIFT_PER_DAY_SQ * days
    return min(INIT_RD, math.sqrt(new_rd_sq))


def _rd_after_match(rd: float) -> float:
    """RD compresses on every played match. Simple decay toward floor."""
    return max(RD_FLOOR, rd * 0.96)


# ── Trainer ───────────────────────────────────────────────────

class _RatingState:
    """In-memory rating book during a training pass.

    Keyed by ``(tour, player_id, surface)``. Surface 'all' tracks the
    overall rating that ignores surface specialisation; downstream
    code can blend surface-specific + 'all' for cold-start surfaces.
    """

    def __init__(self) -> None:
        self.book: dict[tuple, dict] = {}

    def get(self, tour: str, player_id: int,
            surface: str, match_date: str) -> dict:
        key = (tour, int(player_id), surface)
        entry = self.book.get(key)
        if entry is None:
            entry = {
                "rating": INIT_RATING, "rd": INIT_RD,
                "matches": 0, "last_match": None,
            }
            self.book[key] = entry
        # Apply RD decay for inactivity since last match
        entry["rd"] = _decay_rd(entry["rd"], entry["last_match"],
                                  match_date)
        return entry

    def update(self, tour: str, player_id: int, surface: str,
               new_rating: float, new_rd: float,
               match_date: str) -> None:
        key = (tour, int(player_id), surface)
        entry = self.book.setdefault(key, {
            "rating": INIT_RATING, "rd": INIT_RD,
            "matches": 0, "last_match": None,
        })
        entry["rating"] = new_rating
        entry["rd"] = new_rd
        entry["matches"] += 1
        entry["last_match"] = match_date


def train(tours: Iterable[str] | None = None,
          since_year: int | None = None) -> dict:
    """Walk tennis_matches chronologically and emit per-surface Elo
    ratings to tennis_elo.

    ``tours`` defaults to both ATP and WTA. ``since_year`` lower-bounds
    the training set; pass None to use everything in the DB.
    """
    from .tennis_db import get_conn, ensure_tables
    ensure_tables()

    tours = list(tours) if tours else ("atp", "wta")
    summary = {"tours": list(tours), "matches_processed": 0,
               "ratings_written": 0, "skipped_unknown_player": 0,
               "skipped_no_date": 0}

    state = _RatingState()
    conn = get_conn()
    where = ["winner_id IS NOT NULL", "loser_id IS NOT NULL",
             "tourney_date IS NOT NULL"]
    params: list = []
    if since_year is not None:
        where.append("substr(tourney_date, 1, 4) >= ?")
        params.append(str(since_year))
    in_clause = ",".join("?" for _ in tours)
    where.append(f"tour IN ({in_clause})")
    params.extend(tours)
    sql = ("SELECT tour, tourney_date, tourney_level, surface, "
           "winner_id, loser_id "
           f"FROM tennis_matches WHERE {' AND '.join(where)} "
           "ORDER BY tourney_date, match_id")
    rows = conn.execute(sql, params).fetchall()

    for r in rows:
        tour = r["tour"]
        winner = r["winner_id"]
        loser = r["loser_id"]
        date = r["tourney_date"]
        if not date:
            summary["skipped_no_date"] += 1
            continue
        if winner is None or loser is None:
            summary["skipped_unknown_player"] += 1
            continue
        surface = _normalize_surface(r["surface"])
        k = _k_for(r["tourney_level"])

        for surf in (surface, "all"):
            w_state = state.get(tour, winner, surf, date)
            l_state = state.get(tour, loser, surf, date)
            r_w = w_state["rating"]
            r_l = l_state["rating"]
            exp_w = _expected(r_w, r_l)
            actual = 1.0
            delta_w = k * (actual - exp_w)
            # Underdog bonus
            if r_w < r_l:
                delta_w *= UPSET_BONUS
            new_rw = r_w + delta_w
            new_rl = r_l - delta_w
            state.update(tour, winner, surf, new_rw,
                         _rd_after_match(w_state["rd"]), date)
            state.update(tour, loser, surf, new_rl,
                         _rd_after_match(l_state["rd"]), date)
        summary["matches_processed"] += 1

    # Persist book
    now_iso = datetime.now().isoformat(timespec="seconds")
    conn.execute("DELETE FROM tennis_elo")
    written = 0
    for (tour, pid, surf), entry in state.book.items():
        try:
            conn.execute(
                "INSERT INTO tennis_elo "
                "(tour, player_id, surface, rating, rd, matches, "
                " last_match, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (tour, int(pid), surf, float(entry["rating"]),
                 float(entry["rd"]), int(entry["matches"]),
                 entry["last_match"], now_iso),
            )
            written += 1
        except Exception as e:
            logger.warning("elo write failed for %s/%s/%s: %s",
                           tour, pid, surf, e)
    conn.commit()
    summary["ratings_written"] = written
    logger.info("elo training done: %d matches, %d ratings written",
                summary["matches_processed"], written)
    return summary


# ── Reads ─────────────────────────────────────────────────────

def rating_for(tour: str, player_id: int, surface: str = "all",
                fallback_to_all: bool = True) -> dict | None:
    """Lookup a player's rating + RD on a surface. When the
    surface-specific entry is missing and ``fallback_to_all`` is True,
    return the player's overall 'all' rating (cold-start handling)."""
    from .tennis_db import get_conn, ensure_tables
    ensure_tables()
    surf = _normalize_surface(surface) if surface != "all" else "all"
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM tennis_elo WHERE tour = ? AND player_id = ? "
        "AND surface = ?",
        (tour, int(player_id), surf),
    ).fetchone()
    if row:
        return dict(row)
    if fallback_to_all and surf != "all":
        row = conn.execute(
            "SELECT * FROM tennis_elo WHERE tour = ? AND player_id = ? "
            "AND surface = 'all'",
            (tour, int(player_id)),
        ).fetchone()
        if row:
            return dict(row)
    return None


def top_players(tour: str, surface: str = "all",
                limit: int = 25, min_matches: int = 30) -> list[dict]:
    """Read top-N players by rating on a surface. ``min_matches``
    filters out small-sample noise (a 2-match player at 1750 isn't
    actually #1)."""
    from .tennis_db import get_conn, ensure_tables
    ensure_tables()
    surf = _normalize_surface(surface) if surface != "all" else "all"
    conn = get_conn()
    rows = conn.execute(
        "SELECT e.*, p.name FROM tennis_elo e "
        "LEFT JOIN tennis_players p "
        "  ON p.tour = e.tour AND p.player_id = e.player_id "
        "WHERE e.tour = ? AND e.surface = ? AND e.matches >= ? "
        "ORDER BY e.rating DESC LIMIT ?",
        (tour, surf, int(min_matches), int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


# ── CLI ────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="engine.tennis_elo")
    ap.add_argument("--tour", choices=("atp", "wta", "all"),
                    default="all")
    ap.add_argument("--since-year", type=int, default=None)
    ap.add_argument("--show-top", type=int, default=10,
                    help="After training, print the top N hard-court "
                         "players per tour (sanity check). 0 to skip.")
    args = ap.parse_args(argv)

    tours = ("atp", "wta") if args.tour == "all" else (args.tour,)
    res = train(tours=tours, since_year=args.since_year)
    print(f"\n  trained: {res}")
    if args.show_top > 0:
        for t in tours:
            print(f"\n  Top {args.show_top} {t.upper()} hard-court:")
            for r in top_players(t, surface="Hard",
                                  limit=args.show_top, min_matches=30):
                print(f"    {r['rating']:>6.0f}  RD={r['rd']:>4.0f}  "
                      f"{r['matches']:>3} m   {r['name']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["train", "rating_for", "top_players"]
