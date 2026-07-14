"""
Game-state Monte Carlo (Phase 5f).

The analytical predictor in ``engine.live._nba_intermission_predict``
and ``_nhl_period_predict`` uses Gaussian variance approximations for
the rest-of-game distribution. That's tractable and fast but it
flattens the actual shape of NBA / NHL scoring — real distributions
have heavier upside tails (NBA blowouts, NHL OT goals) that a
Gaussian under-prices.

This module fits **empirical** rest-of-game scoring distributions
from ``historical_pbp`` and exposes a sampler the predictors can
use as a drop-in replacement for the Gaussian.

Approach
--------

For each (sport, period_just_ended), we accumulate every historical
game's:

  - Rest-of-game home points scored
  - Rest-of-game away points scored

Bucketed by score-state at the intermission moment so the sampler
can condition on context (e.g. "after a 5-point H1 lead, what does
H2 home scoring usually look like?"). For NBA we bucket margin
into 5-point bins; for NHL 1-goal bins.

Sampler reads the empirical bucket and draws (home_rem, away_rem)
pairs from the JOINT distribution (preserving correlation: when one
team scores a lot, the other usually does too — pace effect).

Storage
-------

Empirical samples land in JSON at ``data/state_mc_<sport>.json``::

    {
      "nba": {
        "period_1": {"margin_bucket_-5_to_-2": [[h_rem, a_rem], ...]},
        "period_2": {...},
        ...
      }
    }

Caching that as JSON keeps inference fast (load once, sample many).
Sample arrays cap at 5000 per bucket — enough for stable empirical
quantiles, small enough to load in <100ms.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


_LIVE_DB = Path(__file__).resolve().parent.parent.parent / "data" / "live.db"
_OUT_DIR = Path(__file__).resolve().parent.parent.parent / "data"


# Bucket boundaries — keep these tight enough to capture context but
# coarse enough that every bucket has 100+ samples.
_NBA_MARGIN_BUCKETS = [(-100, -10), (-10, -5), (-5, -2),
                        (-2, 2), (2, 5), (5, 10), (10, 100)]
_NHL_MARGIN_BUCKETS = [(-100, -2), (-2, -1), (-1, 0),
                        (0, 1), (1, 2), (2, 100)]
# AFL margins are ~2× wider than NBA (stddev 38.8 vs ~15) so buckets
# 2× the size keep sample density similar per cell.
_AFL_MARGIN_BUCKETS = [(-200, -20), (-20, -10), (-10, -3),
                        (-3, 3), (3, 10), (10, 20), (20, 200)]


def _live_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_LIVE_DB))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _bucket_for(margin: int, buckets: list[tuple[int, int]]) -> str:
    for lo, hi in buckets:
        if lo <= margin < hi:
            return f"{lo}_to_{hi}"
    return f"{buckets[-1][0]}_to_{buckets[-1][1]}"


_PERIOD_END_TYPES = {"period end", "end period", "end of period",
                      "period-end"}


def _walk_game_intermissions(sport: str, game_id: str,
                               final_home: int, final_away: int
                               ) -> list[dict]:
    """Return one row per intermission boundary with score-at-time-T
    and rest-of-game scoring totals."""
    conn = _live_conn()
    plays = conn.execute(
        "SELECT period, sequence, type_text, home_score, away_score "
        "FROM historical_pbp "
        "WHERE sport = ? AND game_id = ? "
        "ORDER BY period, sequence",
        (sport, str(game_id)),
    ).fetchall()
    if not plays:
        return []
    max_period = max(int(p["period"] or 0) for p in plays)
    out: list[dict] = []
    for p in plays:
        type_text = (p["type_text"] or "").strip().lower()
        if type_text not in _PERIOD_END_TYPES:
            continue
        period = int(p["period"] or 0)
        if period <= 0 or period >= max_period:
            continue
        home_so_far = int(p["home_score"] or 0)
        away_so_far = int(p["away_score"] or 0)
        out.append({
            "period_ended": period,
            "home_so_far":  home_so_far,
            "away_so_far":  away_so_far,
            "margin_so_far": home_so_far - away_so_far,
            "home_rem": final_home - home_so_far,
            "away_rem": final_away - away_so_far,
        })
    return out


def fit_sport(sport: str, *, max_per_bucket: int = 5000) -> dict:
    """Walk every backfilled game, accumulate (period, margin_bucket)
    → list of (home_rem, away_rem) samples. Persist to JSON.

    ``max_per_bucket`` caps each bucket's sample list so a busy bucket
    doesn't bloat the artifact. Reservoir-sampled when the cap hits.

    NBA/NHL read intermission states from ``historical_pbp`` (requires
    PBP backfill); WNBA reads from per-quarter splits in the basketball
    framework's games table (no PBP needed — quarter scores carry
    enough state to derive the same (home_so_far, away_so_far) at every
    Q-end boundary)."""
    if sport == "nba":
        from ..nba_db import get_conn
        finals_q = ("SELECT game_id, home_score, away_score FROM nba_games "
                    "WHERE status='final' AND home_score IS NOT NULL")
        margin_buckets = _NBA_MARGIN_BUCKETS
    elif sport == "nhl":
        from ..nhl_db import get_conn
        finals_q = ("SELECT game_id, home_score, away_score FROM nhl_games "
                    "WHERE status='final' AND home_score IS NOT NULL")
        margin_buckets = _NHL_MARGIN_BUCKETS
    elif sport in ("wnba", "ncaam", "afl",
                    "nz_nbl", "brazil_nbb", "argentina_lnb",
                    "australia_nbl", "china_cba"):
        # Quarter-splits path — no PBP needed. WNBA/NBL/NBB/LNB/CBA + AFL
        # use 4-period; NCAAM is 2-period (q1=H1, q2=H2). The shared
        # fitter adapts the period count by sport.
        return _fit_from_quarter_splits(sport, max_per_bucket=max_per_bucket)
    elif sport in ("ahl", "pwhl"):
        # Hockey: 3 × 20-min periods. theScore-sourced; period scores
        # backfilled via engine.sports._thescore_periods into
        # home_p1/p2/p3 + away_p1/p2/p3 columns.
        return _fit_from_period_splits(sport, max_per_bucket=max_per_bucket)
    else:
        raise ValueError(f"unsupported sport: {sport!r}")

    finals = {str(r["game_id"]): dict(r)
              for r in get_conn().execute(finals_q).fetchall()}

    # period -> bucket_label -> list of [h_rem, a_rem]
    buckets: dict[str, dict[str, list]] = defaultdict(
        lambda: defaultdict(list))

    import random
    rng = random.Random(20260430)
    n_games = 0
    n_intermissions = 0
    for gid, final in finals.items():
        rows = _walk_game_intermissions(
            sport, gid,
            int(final["home_score"]),
            int(final["away_score"]),
        )
        if rows:
            n_games += 1
        for r in rows:
            bucket_label = _bucket_for(r["margin_so_far"], margin_buckets)
            period_key = f"period_{r['period_ended']}"
            samples = buckets[period_key][bucket_label]
            if len(samples) < max_per_bucket:
                samples.append([r["home_rem"], r["away_rem"]])
            else:
                # Reservoir sampling — replace a random slot so older
                # buckets don't get over-weighted vs newer.
                idx = rng.randrange(len(samples) + 1)
                if idx < len(samples):
                    samples[idx] = [r["home_rem"], r["away_rem"]]
            n_intermissions += 1

    # Persist
    out_path = _OUT_DIR / f"state_mc_{sport}.json"
    payload = {p: {b: s for b, s in pb.items()}
               for p, pb in buckets.items()}
    out_path.write_text(json.dumps(payload))
    logger.info("state_mc[%s]: %d intermissions across %d games -> %s",
                sport, n_intermissions, n_games, out_path)
    return {
        "sport": sport,
        "intermissions": n_intermissions,
        "games": n_games,
        "buckets": {p: {b: len(s) for b, s in pb.items()}
                    for p, pb in payload.items()},
        "path": str(out_path),
    }


# ── Sampler (inference-time API) ─────────────────────────────

_BOOK_CACHE: dict[str, dict] = {}


def _load_book(sport: str) -> dict:
    """Lazy-load the empirical bucket dict for ``sport``. Cached."""
    if sport in _BOOK_CACHE:
        return _BOOK_CACHE[sport]
    path = _OUT_DIR / f"state_mc_{sport}.json"
    if not path.exists():
        _BOOK_CACHE[sport] = {}
        return {}
    try:
        _BOOK_CACHE[sport] = json.loads(path.read_text())
    except Exception:
        _BOOK_CACHE[sport] = {}
    return _BOOK_CACHE[sport]


def sample_remaining(sport: str, period_ended: int,
                      margin_so_far: int, n: int = 10_000) -> list[tuple]:
    """Return a list of (home_rem, away_rem) draws from the empirical
    distribution conditional on (sport, period_ended, margin_bucket).

    Falls back to:
      1. Same period, adjacent margin bucket (more common)
      2. Same period, all buckets pooled (less common)
      3. Empty list (caller should fall back to analytical)
    """
    import random
    book = _load_book(sport)
    period_key = f"period_{period_ended}"
    period_block = book.get(period_key) or {}
    # WNBA + NCAAM share NBA's 5-pt bucket grid. AFL margins are
    # roughly 2× wider so use a dedicated coarser grid. AHL/PWHL share
    # NHL's tight 1-goal grid since hockey margins cluster the same way.
    if sport in ("nhl", "ahl", "pwhl"):
        margin_buckets = _NHL_MARGIN_BUCKETS
    elif sport == "afl":
        margin_buckets = _AFL_MARGIN_BUCKETS
    else:
        margin_buckets = _NBA_MARGIN_BUCKETS
    bucket_label = _bucket_for(margin_so_far, margin_buckets)
    samples = period_block.get(bucket_label) or []
    if len(samples) < 50:
        # Pool all buckets in same period
        pooled: list = []
        for s_list in period_block.values():
            pooled.extend(s_list)
        samples = pooled
    if len(samples) < 50:
        return []
    rng = random.Random()
    return [tuple(rng.choice(samples)) for _ in range(n)]


# ── Quarter-splits fitter (WNBA path, no PBP needed) ─────────

def _fit_from_period_splits(sport: str, *,
                             max_per_bucket: int = 5000) -> dict:
    """Hockey state-MC fitter — 3 × 20-min periods, NHL margin grid.
    Reads ``home_p1/p2/p3`` + ``away_p1/p2/p3`` from the per-league
    theScore DB (engine.sports.{sport}.db). Single intermission at
    end of P1 and P2; P3 is the final."""
    import random
    if sport not in ("ahl", "pwhl"):
        raise ValueError(f"period-splits fitter doesn't cover {sport!r}")
    mod = __import__(f"engine.sports.{sport}.db", fromlist=["get_conn"])
    conn = mod.get_conn()
    rows = conn.execute(
        "SELECT id, home_score, away_score, "
        "       home_p1, away_p1, home_p2, away_p2, home_p3, away_p3 "
        "FROM games WHERE status='final' "
        "  AND home_score IS NOT NULL AND home_p1 IS NOT NULL"
    ).fetchall()
    margin_buckets = _NHL_MARGIN_BUCKETS
    buckets: dict[str, dict[str, list]] = defaultdict(
        lambda: defaultdict(list))
    rng = random.Random(20260515)
    n_games = 0
    n_intermissions = 0
    for r in rows:
        final_home = int(r["home_score"] or 0)
        final_away = int(r["away_score"] or 0)
        cum_h = cum_a = 0
        n_games += 1
        # Intermissions at end of P1 and P2 (P3 = final = no rem sample)
        for p_idx in (1, 2):
            cum_h += int(r[f"home_p{p_idx}"] or 0)
            cum_a += int(r[f"away_p{p_idx}"] or 0)
            home_rem = final_home - cum_h
            away_rem = final_away - cum_a
            if home_rem < 0 or away_rem < 0:
                continue
            margin = cum_h - cum_a
            bucket_label = _bucket_for(margin, margin_buckets)
            period_key = f"period_{p_idx}"
            samples = buckets[period_key][bucket_label]
            if len(samples) < max_per_bucket:
                samples.append([home_rem, away_rem])
            else:
                idx = rng.randrange(len(samples) + 1)
                if idx < len(samples):
                    samples[idx] = [home_rem, away_rem]
            n_intermissions += 1

    out_path = _OUT_DIR / f"state_mc_{sport}.json"
    payload = {p: {b: s for b, s in pb.items()}
               for p, pb in buckets.items()}
    out_path.write_text(json.dumps(payload))
    logger.info("state_mc[%s]: %d intermissions across %d games -> %s",
                sport, n_intermissions, n_games, out_path)
    return {
        "sport": sport,
        "intermissions": n_intermissions,
        "games": n_games,
        "buckets": {p: {b: len(s) for b, s in pb.items()}
                    for p, pb in payload.items()},
        "path": str(out_path),
    }


def _fit_from_quarter_splits(sport: str, *,
                              max_per_bucket: int = 5000) -> dict:
    """Build the same (period_ended, margin_bucket) → samples dict from
    a games table that carries per-quarter splits. WNBA + NCAAM use
    this path. WNBA is 4-period (q1-q4), NCAAM is 2-period (q1=H1,
    q2=H2); we iterate up to ``periods_before_final`` intermissions.
    No PBP needed."""
    import random
    _BB_FRAMEWORK_QUARTERS = (
        "wnba", "ncaam", "afl",
        "nz_nbl", "brazil_nbb", "argentina_lnb",
        "australia_nbl", "china_cba",
    )
    if sport not in _BB_FRAMEWORK_QUARTERS:
        raise ValueError(f"quarter-splits fitter doesn't cover {sport!r}")
    # NCAAM = 2 halves; everything else uses 4 quarters with 3 intermissions
    periods_before_final = 1 if sport == "ncaam" else 3
    from ..basketball._db import get_conn as _bb_conn
    conn = _bb_conn(sport)
    rows = conn.execute(
        "SELECT game_id, home_score, away_score, "
        "       home_q1, away_q1, home_q2, away_q2, "
        "       home_q3, away_q3, home_q4, away_q4 "
        "FROM games WHERE status='final' "
        "  AND home_score IS NOT NULL AND home_q1 IS NOT NULL"
    ).fetchall()
    margin_buckets = _AFL_MARGIN_BUCKETS if sport == "afl" else _NBA_MARGIN_BUCKETS
    buckets: dict[str, dict[str, list]] = defaultdict(
        lambda: defaultdict(list))
    rng = random.Random(20260514)
    n_games = 0
    n_intermissions = 0
    for r in rows:
        final_home = int(r["home_score"] or 0)
        final_away = int(r["away_score"] or 0)
        # Cumulative score at each intermission boundary. NCAAM only
        # has one (halftime); WNBA has three (Q1/Q2/Q3 ends).
        cum_h: list[int] = []
        cum_a: list[int] = []
        h_running = a_running = 0
        for q_idx in range(1, periods_before_final + 1):
            h_inc = int(r[f"home_q{q_idx}"] or 0)
            a_inc = int(r[f"away_q{q_idx}"] or 0)
            h_running += h_inc
            a_running += a_inc
            cum_h.append(h_running)
            cum_a.append(a_running)
        n_games += 1
        for period_idx, (h_so_far, a_so_far) in enumerate(
            zip(cum_h, cum_a), start=1,
        ):
            home_rem = final_home - h_so_far
            away_rem = final_away - a_so_far
            if home_rem < 0 or away_rem < 0:
                continue
            margin = h_so_far - a_so_far
            bucket_label = _bucket_for(margin, margin_buckets)
            period_key = f"period_{period_idx}"
            samples = buckets[period_key][bucket_label]
            if len(samples) < max_per_bucket:
                samples.append([home_rem, away_rem])
            else:
                idx = rng.randrange(len(samples) + 1)
                if idx < len(samples):
                    samples[idx] = [home_rem, away_rem]
            n_intermissions += 1

    out_path = _OUT_DIR / f"state_mc_{sport}.json"
    payload = {p: {b: s for b, s in pb.items()}
               for p, pb in buckets.items()}
    out_path.write_text(json.dumps(payload))
    logger.info("state_mc[%s]: %d intermissions across %d games -> %s",
                sport, n_intermissions, n_games, out_path)
    return {
        "sport": sport,
        "intermissions": n_intermissions,
        "games": n_games,
        "buckets": {p: {b: len(s) for b, s in pb.items()}
                    for p, pb in payload.items()},
        "path": str(out_path),
    }


# ── CLI ───────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="engine.live._state_mc")
    ap.add_argument("--sport", choices=("nba", "nhl"), required=True)
    ap.add_argument("--max-per-bucket", type=int, default=5000)
    args = ap.parse_args(argv)
    res = fit_sport(args.sport, max_per_bucket=args.max_per_bucket)
    print(f"\n  state_mc[{args.sport}]")
    print(f"  games: {res['games']}, intermissions: {res['intermissions']}")
    print(f"  output: {res['path']}\n")
    print("  bucket sample counts:")
    for period, buckets in sorted(res["buckets"].items()):
        print(f"    {period}:")
        for label, n in sorted(buckets.items()):
            print(f"      {label:>20s}  {n:>5d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["fit_sport", "sample_remaining"]
