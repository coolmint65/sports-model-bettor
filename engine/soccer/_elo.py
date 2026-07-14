"""Team Elo for the soccer framework.

Elo gives us a single skill scalar per team that we update after every
match. It feeds the Dixon-Coles predictor as the prior on each team's
attacking + defending strength — Poisson goal rates are anchored on
``f(elo_diff)`` so a 50-Elo gap maps to a ~0.15 goal-rate edge.

Why Elo (not raw points / xG / market price):

  * Self-calibrating — no need to scrape another data source. Each
    match settles its own update.
  * Decoupled from match volume — a team that plays 20 cup ties + 38
    league games still gets one rating, vs points-per-game which
    silos by competition.
  * Goal-margin weighting (the FIFA / ClubElo trick): bigger blowouts
    move the rating more than 1-0 squeakers, capturing strength
    information that pure W/D/L would discard.

Update rule (per match):

    R_a' = R_a + K · G · (S_a − E_a)

    E_a = 1 / (1 + 10 ^ ((R_b − R_a + H) / 400))   # H = home boost
    S_a ∈ {1.0 home win, 0.5 draw, 0.0 home loss}
    G   = goal-margin multiplier (1 for 1-goal, 1.5 for 2, log-scaled
          above that)

Persistence: the per-league DB carries a ``team_elo`` table — one row
per team, updated in-place. Backfill walks the matches table in date
order, applying the update for each final. Re-running backfill against
a fresh team_elo table reproduces the exact same final ratings.

Status: anchored at INIT_ELO=1500 until backfilled. The predictor
gracefully falls back to a uniform prior when a team's Elo is at the
init value (a pre-season call).
"""
from __future__ import annotations

import logging
import math
import sqlite3
from typing import Iterable

from ._db import get_conn

logger = logging.getLogger(__name__)


# ── Tunables ────────────────────────────────────────────────

# Starting Elo for a team we've never seen. 1500 is the universal Elo
# convention; ClubElo / FIFA use higher anchors but only because they
# scale K accordingly. Internal scaling is what matters, not absolute.
INIT_ELO: float = 1500.0

# K-factor — how much one match can shift a team's rating. ClubElo uses
# K=20 for league fixtures; we follow suit. Cup matches (especially WC
# qualifiers) get a 1.5× bump because the result carries more
# information about national-team form, where the Elo update cadence
# is slow.
K_LEAGUE: float = 20.0
K_CUP:    float = 30.0
K_INTERNATIONAL: float = 35.0   # FIFA / friendlies — sample is sparse so each match matters more

# Home advantage in Elo points. Empirical league averages cluster
# around 60-100 Elo (~0.4 goals worth of lift). 65 is a reasonable
# global prior; per-league tuning can override via the calibrate path.
HOME_ADVANTAGE_ELO: float = 65.0

# Neutral-site override — knockout finals, WC group stage. Reduces home
# advantage to zero.
NEUTRAL_HOME_ADVANTAGE: float = 0.0


# ── Math primitives ─────────────────────────────────────────

def expected_score(rating_a: float, rating_b: float,
                   *, home_advantage: float = HOME_ADVANTAGE_ELO
                   ) -> float:
    """Logistic expected score for team A vs team B. ``home_advantage``
    is added to team A's effective rating — pass 0 for neutral venues
    or away matches (caller flips the sign when computing B's expected
    score for an away fixture).

    Returns the probability that A wins outright (draws split 50/50)."""
    diff = (rating_a + home_advantage) - rating_b
    return 1.0 / (1.0 + 10 ** (-diff / 400.0))


def goal_margin_multiplier(home_score: int, away_score: int) -> float:
    """ClubElo's goal-margin multiplier. 1-goal margin = 1.0; 2-goal =
    1.5; higher margins compress via ``ln(1 + margin) / ln(2)`` so an
    8-1 thrashing doesn't blow up the update like a strict linear
    formula would."""
    diff = abs(home_score - away_score)
    if diff <= 1:
        return 1.0
    if diff == 2:
        return 1.5
    # 3+: log-scale to dampen blowouts.
    return (11.0 + diff) / 8.0


def _k_for_competition(competition_type: str | None,
                       confederation: str | None) -> float:
    """K-factor by match type. Internationals get the largest bump
    because a national-team Elo sees ~10 matches/year vs a club's ~40-
    60; each result has to carry more weight to keep the rating
    responsive."""
    if confederation == "FIFA":
        return K_INTERNATIONAL
    if competition_type == "cup":
        return K_CUP
    return K_LEAGUE


def _outcome(home_score: int, away_score: int) -> tuple[float, float]:
    """Translate a final score to (S_home, S_away) — the conventional
    1.0 / 0.5 / 0.0 trio. Goal margin doesn't matter here; it enters
    via the multiplier."""
    if home_score > away_score:
        return 1.0, 0.0
    if home_score < away_score:
        return 0.0, 1.0
    return 0.5, 0.5


# ── Persistence ─────────────────────────────────────────────

def get_rating(conn: sqlite3.Connection, team_id: int) -> float:
    """Current Elo for ``team_id``. Returns INIT_ELO when no row exists
    so callers can use it without a defensive check."""
    row = conn.execute(
        "SELECT elo FROM team_elo WHERE team_id = ?", (int(team_id),)
    ).fetchone()
    if row is None or row["elo"] is None:
        return INIT_ELO
    return float(row["elo"])


def _upsert_rating(conn: sqlite3.Connection, team_id: int, elo: float,
                    match_id: int) -> None:
    conn.execute(
        "INSERT INTO team_elo (team_id, elo, matches_played, last_match_id) "
        "VALUES (?, ?, 1, ?) "
        "ON CONFLICT(team_id) DO UPDATE SET "
        "  elo = excluded.elo, "
        "  matches_played = matches_played + 1, "
        "  last_match_id = excluded.last_match_id, "
        "  updated_at = datetime('now')",
        (int(team_id), float(elo), int(match_id)),
    )


# ── Update one match ────────────────────────────────────────

def update_for_match(conn: sqlite3.Connection, match: dict,
                     *, competition_type: str | None = None,
                     confederation: str | None = None) -> dict:
    """Apply the Elo delta for one finalized match. Mutates team_elo in
    place; returns ``{home_old, away_old, home_new, away_new, delta}``."""
    home_id = int(match["home_team_id"])
    away_id = int(match["away_team_id"])
    home_score = int(match.get("home_score") or 0)
    away_score = int(match.get("away_score") or 0)
    match_id = int(match["id"])
    # home_side ∈ {'home', 'neutral', 'away'} — 'away' means the away-
    # labeled side had the true home ground (host nation as the visitor
    # of record). Fall through to the legacy neutral_site bool when
    # home_side is unpopulated (older leagues, pre-migration rows).
    home_side = match.get("home_side")
    if home_side is None:
        home_side = "neutral" if match.get("neutral_site") else "home"

    ra = get_rating(conn, home_id)
    rb = get_rating(conn, away_id)
    if home_side == "home":
        home_adv = HOME_ADVANTAGE_ELO
    elif home_side == "away":
        home_adv = -HOME_ADVANTAGE_ELO
    else:
        home_adv = NEUTRAL_HOME_ADVANTAGE
    e_home = expected_score(ra, rb, home_advantage=home_adv)
    e_away = 1.0 - e_home
    s_home, s_away = _outcome(home_score, away_score)
    k = _k_for_competition(competition_type, confederation)
    g = goal_margin_multiplier(home_score, away_score)
    delta = k * g * (s_home - e_home)
    ra_new = ra + delta
    rb_new = rb - delta  # zero-sum: A's gain == B's loss
    _upsert_rating(conn, home_id, ra_new, match_id)
    _upsert_rating(conn, away_id, rb_new, match_id)
    return {
        "home_old": ra, "away_old": rb,
        "home_new": ra_new, "away_new": rb_new,
        "delta": delta, "k": k, "g": g, "expected_home": e_home,
    }


# ── Bulk backfill ───────────────────────────────────────────

def backfill(league: str, *, since: str | None = None) -> dict:
    """Walk every finalized match for ``league`` in chronological order
    and apply the Elo update. Returns ``{matches, teams, …}``.

    ``since`` is an ISO date (inclusive) — restrict to matches from
    that date onward. Useful for incremental re-runs when a previously
    pending match settles. Re-running on a fresh ``team_elo`` table
    starting from a far-past date reproduces the rating history
    deterministically.
    """
    from ._config import get_league_config
    cfg = get_league_config(league)
    conn = get_conn(league)
    sql = (
        "SELECT id, date, start_time, home_team_id, away_team_id, "
        "       home_score, away_score, neutral_site, home_side "
        "FROM matches WHERE status = 'final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
    )
    params: list = []
    if since:
        sql += "  AND date >= ? "
        params.append(since)
    sql += "ORDER BY start_time ASC, id ASC"
    rows = conn.execute(sql, params).fetchall()
    matches = 0
    teams = set()
    for r in rows:
        try:
            res = update_for_match(
                conn, dict(r),
                competition_type=cfg.get("competition_type"),
                confederation=cfg.get("confederation"),
            )
            matches += 1
            teams.add(int(r["home_team_id"]))
            teams.add(int(r["away_team_id"]))
        except Exception as e:
            logger.warning("[%s] elo update %s failed: %s",
                            league, r["id"], e)
    conn.commit()
    logger.info("[%s] elo backfill: matches=%d teams=%d", league, matches, len(teams))
    return {"matches": matches, "teams": len(teams)}


def reset(league: str) -> int:
    """Wipe the team_elo table for ``league``. Required before a full
    re-backfill so old ratings don't compound with the replay."""
    conn = get_conn(league)
    n = conn.execute("DELETE FROM team_elo").rowcount
    conn.commit()
    return n


# ── Read API ────────────────────────────────────────────────

def top_n(league: str, n: int = 20) -> list[dict]:
    """Top-N Elo for ``league``. Used by the predictor for league-mean
    sanity checks and by the UI to display power rankings."""
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT te.team_id, te.elo, te.matches_played, t.name, t.abbreviation "
        "FROM team_elo te JOIN teams t ON t.id = te.team_id "
        "ORDER BY te.elo DESC LIMIT ?", (int(n),),
    ).fetchall()
    return [dict(r) for r in rows]


# ── CLI ─────────────────────────────────────────────────────

def _cli() -> int:
    import argparse, logging as _l
    ap = argparse.ArgumentParser(prog="engine.soccer._elo")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_back = sub.add_parser("backfill")
    p_back.add_argument("league")
    p_back.add_argument("--since", default=None)
    p_back.add_argument("--reset", action="store_true",
                         help="wipe team_elo before backfilling")
    p_top = sub.add_parser("top")
    p_top.add_argument("league")
    p_top.add_argument("--n", type=int, default=10)
    args = ap.parse_args()
    _l.basicConfig(level=_l.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.cmd == "backfill":
        if args.reset:
            print("reset rows:", reset(args.league))
        print(backfill(args.league, since=args.since))
    elif args.cmd == "top":
        for r in top_n(args.league, n=args.n):
            print(f"  {r['elo']:7.1f}  {r['name']}  ({r['matches_played']} GP)")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
