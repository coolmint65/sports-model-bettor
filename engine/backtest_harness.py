"""Universal backtest harness — accuracy-first measurement infrastructure.

Replays N completed games using two prediction stacks (e.g. current
ensemble vs experimental player-MC composer) and reports per-market
calibration / Brier / RMSE side-by-side.

Designed as the load-bearing measurement tool for every "is this change
actually helping" question. Stage 0 player-MC verdicts (#171/172/173/187)
gate off this harness's output. Pays for itself even if every Stage 0
returns a wash.

Architecture
------------

Two abstractions:

1. **Sport adapter** — knows how to load completed games for a sport,
   knows the markets that sport trades, knows the per-game outcome
   resolver. Registered once per sport.

2. **Prediction stack** — a callable that takes a ``game_meta`` dict
   and returns a ``{market: value}`` dict (probabilities for binary
   markets, expected values for regression markets). One per stack
   variant (current ensemble, player-MC alpha, etc.).

The harness orchestrates: pull N games → for each game run both stacks
→ join to outcome → aggregate metrics → report.

Point-in-time leak avoidance
----------------------------

Every per-sport predictor exposes a ``backtest=True`` (or ``date=``)
parameter that suppresses live HTTP fetches and time-traveling stat
lookups. The harness sets it on every stack invocation. Without this
the comparison is meaningless because the "model" sees future stats.

Usage
-----

::

    python -m engine.backtest_harness mlb --games 300
    python -m engine.backtest_harness nba --games 200 --stack player_mc
    python -m engine.backtest_harness tennis --games 500 --markets p1_win,total_games

Stack variants are registered via ``register_stack(sport, name, fn)``.
The default ``current`` stack uses the existing ensemble for each sport.

Output is a side-by-side report with per-market metrics + per-bucket
breakdowns (favs vs dogs, high vs low totals, B2B vs rested, surface).
"""

from __future__ import annotations

import argparse
import json
import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Iterable

logger = logging.getLogger(__name__)


# ── Metrics ────────────────────────────────────────────────────

def _brier(samples: Iterable[tuple[float, float]]) -> float:
    """Mean (predicted_prob - outcome)^2. Outcome is 0 or 1."""
    n = 0
    s = 0.0
    for p, y in samples:
        s += (p - y) ** 2
        n += 1
    return s / n if n else float("nan")


def _log_loss(samples: Iterable[tuple[float, float]]) -> float:
    """-mean[ y log p + (1-y) log (1-p) ]. Penalizes confident wrong picks
    much harder than Brier — small extra signal worth tracking alongside."""
    n = 0
    s = 0.0
    for p, y in samples:
        p = max(1e-6, min(1 - 1e-6, p))
        s += -(y * math.log(p) + (1 - y) * math.log(1 - p))
        n += 1
    return s / n if n else float("nan")


def _rmse(samples: Iterable[tuple[float, float]]) -> float:
    """sqrt(mean((predicted - actual)^2)). For regression markets."""
    n = 0
    s = 0.0
    for p, y in samples:
        s += (p - y) ** 2
        n += 1
    return math.sqrt(s / n) if n else float("nan")


def _mae(samples: Iterable[tuple[float, float]]) -> float:
    """Mean absolute error — robust complement to RMSE."""
    n = 0
    s = 0.0
    for p, y in samples:
        s += abs(p - y)
        n += 1
    return s / n if n else float("nan")


def _calibration_buckets(samples: list[tuple[float, float]],
                          n_buckets: int = 10) -> list[dict]:
    """Bin predicted probabilities into n_buckets, report mean predicted
    vs mean realized per bucket. Useful for reliability diagrams."""
    if not samples:
        return []
    buckets: dict[int, list[tuple[float, float]]] = defaultdict(list)
    for p, y in samples:
        idx = min(n_buckets - 1, max(0, int(p * n_buckets)))
        buckets[idx].append((p, y))
    out = []
    for idx in range(n_buckets):
        rows = buckets.get(idx) or []
        if not rows:
            out.append({"bucket": idx, "n": 0,
                        "mean_pred": None, "mean_actual": None})
            continue
        mp = sum(p for p, _ in rows) / len(rows)
        ma = sum(y for _, y in rows) / len(rows)
        out.append({"bucket": idx, "n": len(rows),
                    "mean_pred": round(mp, 4),
                    "mean_actual": round(ma, 4)})
    return out


# ── Data structures ────────────────────────────────────────────

@dataclass
class GameRow:
    """One historical game's metadata + outcomes per market.

    ``meta`` is whatever the sport adapter needs to invoke its stacks
    (team IDs, abbrs, pitcher IDs, surface, etc.). ``outcomes`` is the
    realized truth per market — None for markets the game doesn't have
    (e.g. NBA Q1 markets when q1 score not recorded)."""
    sport: str
    game_id: str
    date: str
    meta: dict[str, Any]
    outcomes: dict[str, float | None]


@dataclass
class Prediction:
    """One stack's output for one game. Keys match the markets the
    sport adapter publishes; values are probabilities (binary) or
    expected values (regression)."""
    stack_name: str
    game_id: str
    values: dict[str, float | None]


@dataclass
class StackReport:
    """Per-stack aggregated metrics across all replayed games."""
    stack_name: str
    per_market: dict[str, dict[str, Any]] = field(default_factory=dict)


# ── Sport adapter registry ─────────────────────────────────────

@dataclass
class SportAdapter:
    name: str
    load_games: Callable[[int], list[GameRow]]
    market_kinds: dict[str, str]  # market → 'binary' | 'regression'
    bucket_keys: dict[str, Callable[[GameRow], str]] = field(default_factory=dict)


_SPORTS: dict[str, SportAdapter] = {}
_STACKS: dict[str, dict[str, Callable[[GameRow], dict]]] = defaultdict(dict)


def register_sport(adapter: SportAdapter) -> None:
    _SPORTS[adapter.name] = adapter


def register_stack(sport: str, name: str,
                    fn: Callable[[GameRow], dict[str, float | None]]) -> None:
    """Register a prediction stack for a sport.

    ``fn(game_row)`` returns a dict mapping market name → predicted
    probability or expected value. Returning None for any market means
    "this stack can't predict this market for this game" — the harness
    skips it from that market's metrics for that stack only."""
    _STACKS[sport][name] = fn


def list_stacks(sport: str) -> list[str]:
    return sorted(_STACKS.get(sport, {}).keys())


# ── Orchestrator ───────────────────────────────────────────────

def replay(sport: str, *, games: int = 300,
            stacks: list[str] | None = None,
            markets: list[str] | None = None) -> dict:
    """Replay last N games of ``sport`` with the named stacks. Returns
    a structured report with per-stack per-market metrics + per-bucket
    breakdowns + calibration."""
    adapter = _SPORTS.get(sport)
    if not adapter:
        raise ValueError(f"no adapter registered for sport={sport!r}")
    available_stacks = _STACKS.get(sport) or {}
    if not available_stacks:
        raise ValueError(f"no stacks registered for sport={sport!r}")
    chosen = stacks or list(available_stacks.keys())
    chosen_markets = markets or list(adapter.market_kinds.keys())

    rows = adapter.load_games(games)
    logger.info("Loaded %d games for sport=%s", len(rows), sport)

    # Run each stack on each game.
    per_stack_preds: dict[str, list[Prediction]] = {s: [] for s in chosen}
    for row in rows:
        for stack_name in chosen:
            fn = available_stacks.get(stack_name)
            if not fn:
                continue
            try:
                values = fn(row) or {}
            except Exception as exc:
                logger.debug("stack %s failed on %s: %s",
                             stack_name, row.game_id, exc)
                values = {}
            per_stack_preds[stack_name].append(
                Prediction(stack_name=stack_name, game_id=row.game_id,
                           values=values))

    # Aggregate metrics per stack per market.
    report: dict[str, StackReport] = {}
    by_game = {row.game_id: row for row in rows}
    for stack_name in chosen:
        sr = StackReport(stack_name=stack_name)
        for market in chosen_markets:
            kind = adapter.market_kinds.get(market)
            if not kind:
                continue
            samples: list[tuple[float, float]] = []
            buckets: dict[str, list[tuple[float, float]]] = defaultdict(list)
            for pred in per_stack_preds[stack_name]:
                row = by_game.get(pred.game_id)
                if not row:
                    continue
                actual = row.outcomes.get(market)
                predicted = pred.values.get(market)
                if actual is None or predicted is None:
                    continue
                samples.append((float(predicted), float(actual)))
                for bucket_key, fn_b in adapter.bucket_keys.items():
                    try:
                        b = fn_b(row)
                    except Exception:
                        continue
                    if b is None:
                        continue
                    buckets[f"{bucket_key}={b}"].append(
                        (float(predicted), float(actual))
                    )
            metrics: dict[str, Any] = {"n": len(samples)}
            if samples:
                if kind == "binary":
                    metrics["brier"] = round(_brier(samples), 4)
                    metrics["log_loss"] = round(_log_loss(samples), 4)
                    metrics["calibration"] = _calibration_buckets(samples)
                else:
                    metrics["rmse"] = round(_rmse(samples), 4)
                    metrics["mae"] = round(_mae(samples), 4)
                if buckets:
                    metrics["by_bucket"] = {}
                    for label, rows_b in buckets.items():
                        if not rows_b:
                            continue
                        m: dict[str, Any] = {"n": len(rows_b)}
                        if kind == "binary":
                            m["brier"] = round(_brier(rows_b), 4)
                        else:
                            m["rmse"] = round(_rmse(rows_b), 4)
                        metrics["by_bucket"][label] = m
            sr.per_market[market] = metrics
        report[stack_name] = sr

    return {
        "sport": sport,
        "games_loaded": len(rows),
        "stacks": {name: sr.per_market for name, sr in report.items()},
    }


# ── Reporting (text) ───────────────────────────────────────────

def render_text(report: dict) -> str:
    sport = report["sport"]
    n = report["games_loaded"]
    lines = [
        f"\n{'=' * 64}",
        f"  Backtest harness — sport={sport.upper()}  games={n}",
        f"{'=' * 64}",
    ]
    stacks = list(report["stacks"].keys())
    if not stacks:
        lines.append("  (no stacks ran)")
        return "\n".join(lines)

    # Collect all markets across all stacks (union).
    all_markets: list[str] = []
    seen = set()
    for stack_name, per_market in report["stacks"].items():
        for m in per_market:
            if m not in seen:
                all_markets.append(m)
                seen.add(m)

    for market in all_markets:
        lines.append(f"\n  Market: {market}")
        lines.append(f"  {'-' * 60}")
        for stack_name in stacks:
            metrics = report["stacks"][stack_name].get(market)
            if not metrics or metrics.get("n", 0) == 0:
                lines.append(f"    {stack_name:18s} (no data)")
                continue
            n_m = metrics["n"]
            if "brier" in metrics:
                lines.append(f"    {stack_name:18s} n={n_m:>4d}  "
                             f"brier={metrics['brier']:.4f}  "
                             f"log_loss={metrics['log_loss']:.4f}")
            elif "rmse" in metrics:
                lines.append(f"    {stack_name:18s} n={n_m:>4d}  "
                             f"rmse={metrics['rmse']:.4f}  "
                             f"mae={metrics['mae']:.4f}")
        # Side-by-side delta when ≥2 stacks present.
        if len(stacks) >= 2:
            base = report["stacks"][stacks[0]].get(market) or {}
            for other in stacks[1:]:
                comp = report["stacks"][other].get(market) or {}
                if not base or not comp:
                    continue
                metric_name = "brier" if "brier" in base else "rmse"
                if metric_name not in base or metric_name not in comp:
                    continue
                delta = comp[metric_name] - base[metric_name]
                rel = (delta / base[metric_name] * 100.0
                       if base[metric_name] else 0.0)
                tag = ("BETTER" if delta < -0.001 else
                       ("WORSE"  if delta >  0.001 else "neutral"))
                lines.append(f"    {other} vs {stacks[0]}: "
                             f"delta={delta:+.4f}  rel={rel:+.2f}%  {tag}")

    return "\n".join(lines)


# ── Default stack: current ensemble per sport ──────────────────

def _current_mlb_stack(row: GameRow) -> dict[str, float | None]:
    from .mlb_predict import predict_matchup
    from .ensemble import ensemble_mlb
    meta = row.meta
    pred = predict_matchup(
        home_team_id=meta["home_team_id"],
        away_team_id=meta["away_team_id"],
        home_pitcher_id=meta.get("home_pitcher_id"),
        away_pitcher_id=meta.get("away_pitcher_id"),
        venue=meta.get("venue"),
        backtest=True,
    )
    if "error" in pred:
        return {}
    ens = ensemble_mlb(pred) or {}
    return {
        "home_win": (ens.get("home_win")
                     or (pred.get("win_prob") or {}).get("home")),
        "total":    (ens.get("total_expected")
                     or pred.get("total")),
    }


def _player_mc_mlb_stack(row: GameRow) -> dict[str, float | None]:
    """MLB Stage 0 minimal player composer — per-batter recent runs +
    per-starter recent ERA, composed to team totals. PIT-correct."""
    from .mlb_team_composer import predict_team_composer
    meta = row.meta
    try:
        pred = predict_team_composer(
            home_team_id=meta["home_team_id"],
            away_team_id=meta["away_team_id"],
            home_pitcher_id=meta.get("home_pitcher_id"),
            away_pitcher_id=meta.get("away_pitcher_id"),
            cutoff_date=row.date,
        )
    except Exception as exc:
        logger.debug("player_mc mlb failed for %s: %s", row.game_id, exc)
        return {}
    return {
        "home_win": pred.get("home_win"),
        "total":    pred.get("total"),
    }


def _current_nba_q1_stack(row: GameRow) -> dict[str, float | None]:
    from .nba_q1_predict import predict_q1_matchup
    meta = row.meta
    pred = predict_q1_matchup(meta["home_abbr"], meta["away_abbr"])
    if not pred or "error" in pred:
        return {}
    return {
        "q1_home_win":     pred.get("q1_ml_home"),
        "q1_total_points": pred.get("predicted_total"),
    }


def _player_mc_nba_stack(row: GameRow) -> dict[str, float | None]:
    """NBA Stage 0 — per-starter Q1 composer. Per locked plan, Q1
    is the cleanest player-driven NBA market (only 5 starters play
    full quarter, no rotation/foul issues)."""
    from .nba_q1_composer import predict_q1_composer
    meta = row.meta
    try:
        pred = predict_q1_composer(
            home_team_id=meta["home_team_id"],
            away_team_id=meta["away_team_id"],
            cutoff_date=row.date,
        )
    except Exception as exc:
        logger.debug("player_mc nba failed for %s: %s", row.game_id, exc)
        return {}
    return {
        "q1_home_win":     pred.get("q1_home_win"),
        "q1_total_points": pred.get("q1_total_points"),
    }


def _current_nhl_stack(row: GameRow) -> dict[str, float | None]:
    from .nhl_predict import predict_matchup
    meta = row.meta
    pred = predict_matchup(meta["home_key"], meta["away_key"], backtest=True)
    if not pred or "error" in pred:
        return {}
    return {
        "home_win":    (pred.get("win_prob") or {}).get("home"),
        "total_goals": pred.get("total"),
    }


def _current_tennis_stack(row: GameRow) -> dict[str, float | None]:
    from .tennis_predict import predict_match
    meta = row.meta
    try:
        pred = predict_match(
            meta["tour"], meta["p1_id"], meta["p2_id"],
            surface=meta["surface"], best_of=meta["best_of"],
            date=meta.get("date"),
        )
    except Exception:
        return {}
    return {"p1_win": pred.get("p1_win_prob")}


def _player_mc_tennis_stack(row: GameRow) -> dict[str, float | None]:
    """Stage 0 tennis player-MC: per-player serve-point Monte Carlo.
    PIT-correct via cutoff_date. See engine/tennis_player_mc.py."""
    from .tennis_player_mc import predict_match_mc
    meta = row.meta
    try:
        pred = predict_match_mc(
            tour=meta["tour"],
            p1_id=meta["p1_id"],
            p2_id=meta["p2_id"],
            surface=meta["surface"],
            best_of=meta["best_of"],
            cutoff_date=meta.get("date"),
            n_sims=5_000,
            seed=42,
        )
    except Exception as exc:
        logger.debug("player_mc tennis failed for %s: %s",
                     row.game_id, exc)
        return {}
    return {
        "p1_win":      pred.get("p1_win"),
        "total_games": pred.get("total_games"),
    }


def _hybrid_tennis_stack(row: GameRow) -> dict[str, float | None]:
    """Stage 1 hybrid: Elo owns p1_win (Brier 0.221 vs serve-MC 0.255),
    GBM regression owns total_games (RMSE 6.07 vs serve-MC 9.68 vs
    naive 6.33). Each tool used for what it's structurally good at —
    serve-MC dropped after Stage 0 + 0.5 + 0.7 all underperformed."""
    from .tennis_predict import predict_match
    from .tennis_dist_gbm import predict_total_games
    meta = row.meta
    out: dict[str, float | None] = {}
    try:
        elo_pred = predict_match(
            meta["tour"], meta["p1_id"], meta["p2_id"],
            surface=meta["surface"], best_of=meta["best_of"],
            date=meta.get("date"),
        )
        out["p1_win"] = elo_pred.get("p1_win_prob")
    except Exception:
        pass
    try:
        out["total_games"] = predict_total_games(
            tour=meta["tour"],
            p1_id=meta["p1_id"], p2_id=meta["p2_id"],
            surface=meta["surface"], best_of=meta["best_of"],
            cutoff_date=meta.get("date"),
        )
    except Exception:
        pass
    return out


# ── Sport adapters ─────────────────────────────────────────────

def _load_mlb_games(n: int) -> list[GameRow]:
    from .db import get_conn
    rows = get_conn().execute(
        "SELECT mlb_game_id, date, home_team_id, away_team_id, "
        "       home_pitcher_id, away_pitcher_id, venue, "
        "       home_score, away_score "
        "FROM games "
        "WHERE status='final' AND home_score IS NOT NULL "
        "  AND away_score IS NOT NULL "
        "ORDER BY date DESC LIMIT ?",
        (n,),
    ).fetchall()
    return [
        GameRow(
            sport="mlb",
            game_id=str(r["mlb_game_id"]),
            date=r["date"],
            meta={
                "home_team_id": r["home_team_id"],
                "away_team_id": r["away_team_id"],
                "home_pitcher_id": r["home_pitcher_id"],
                "away_pitcher_id": r["away_pitcher_id"],
                "venue": r["venue"],
            },
            outcomes={
                "home_win": 1.0 if r["home_score"] > r["away_score"] else 0.0,
                "total":    float(r["home_score"] + r["away_score"]),
            },
        )
        for r in rows
    ]


def _load_nba_games(n: int) -> list[GameRow]:
    from .nba_db import get_conn
    rows = get_conn().execute(
        "SELECT g.game_id, g.date, g.home_team_id, g.away_team_id, "
        "       g.home_score, g.away_score, g.home_q1, g.away_q1, "
        "       ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr "
        "FROM nba_games g "
        "LEFT JOIN nba_teams ht ON g.home_team_id = ht.id "
        "LEFT JOIN nba_teams at ON g.away_team_id = at.id "
        "WHERE g.status='final' AND g.home_score IS NOT NULL "
        "  AND g.home_q1 IS NOT NULL "
        "ORDER BY g.date DESC LIMIT ?",
        (n,),
    ).fetchall()
    out = []
    for r in rows:
        out.append(GameRow(
            sport="nba",
            game_id=str(r["game_id"]),
            date=r["date"],
            meta={
                "home_team_id": r["home_team_id"],
                "away_team_id": r["away_team_id"],
                "home_abbr": r["home_abbr"],
                "away_abbr": r["away_abbr"],
            },
            outcomes={
                "home_win": 1.0 if r["home_score"] > r["away_score"] else 0.0,
                "total_points": float(r["home_score"] + r["away_score"]),
                "q1_home_win": (1.0 if r["home_q1"] > r["away_q1"]
                                else (0.0 if r["home_q1"] < r["away_q1"] else None)),
                "q1_total_points": float(r["home_q1"] + r["away_q1"]),
            },
        ))
    return out


def _load_nhl_games(n: int) -> list[GameRow]:
    from .nhl_db import get_conn
    rows = get_conn().execute(
        "SELECT game_id, date, home_team_id, away_team_id, "
        "       home_score, away_score "
        "FROM nhl_games WHERE status='final' AND home_score IS NOT NULL "
        "ORDER BY date DESC LIMIT ?",
        (n,),
    ).fetchall()
    # NHL needs the team-key string the predictor uses (e.g. 'bruins') —
    # try teams table for abbr→key, else best-effort lower-case fallback.
    return [
        GameRow(
            sport="nhl",
            game_id=str(r["game_id"]),
            date=r["date"],
            meta={
                "home_team_id": r["home_team_id"],
                "away_team_id": r["away_team_id"],
                # nhl_predict uses team keys (slug-like). Adapter caller
                # should resolve; for now pass team_id and let the stack
                # resolve via internal lookup.
                "home_key": str(r["home_team_id"]),
                "away_key": str(r["away_team_id"]),
            },
            outcomes={
                "home_win":    1.0 if r["home_score"] > r["away_score"] else 0.0,
                "total_goals": float(r["home_score"] + r["away_score"]),
            },
        )
        for r in rows
    ]


def _parse_score_to_games(score: str | None) -> tuple[int, int] | None:
    """'7-5 6-4' → (13, 9). Tennis games-per-match, not sets."""
    if not score:
        return None
    p1_total = p2_total = 0
    import re as _re
    for m in _re.finditer(r"(\d+)-(\d+)", score):
        p1_total += int(m.group(1))
        p2_total += int(m.group(2))
    if p1_total == 0 and p2_total == 0:
        return None
    return p1_total, p2_total


def _load_tennis_games(n: int) -> list[GameRow]:
    from .tennis_db import get_conn
    rows = get_conn().execute(
        "SELECT tour, match_id, tourney_date AS date, surface, best_of, "
        "       winner_id, loser_id, score, minutes "
        "FROM tennis_matches "
        "WHERE score IS NOT NULL AND score != '' "
        "  AND surface IN ('Hard','Clay','Grass','Carpet') "
        "  AND best_of IN (3, 5) "
        "ORDER BY tourney_date DESC LIMIT ?",
        (n,),
    ).fetchall()
    out = []
    for r in rows:
        # Map (winner, loser) → (p1, p2) deterministically by id so
        # that p1_win is sometimes 0 sometimes 1 — otherwise outcomes
        # are perfectly correlated with the side label and Brier is meaningless.
        if r["winner_id"] is None or r["loser_id"] is None:
            continue
        if int(r["winner_id"]) < int(r["loser_id"]):
            p1_id, p2_id = r["winner_id"], r["loser_id"]
            p1_win = 1.0
        else:
            p1_id, p2_id = r["loser_id"], r["winner_id"]
            p1_win = 0.0
        games = _parse_score_to_games(r["score"])
        if games is None:
            continue
        if int(r["winner_id"]) < int(r["loser_id"]):
            p1_games, p2_games = games[0], games[1]
        else:
            p1_games, p2_games = games[1], games[0]
        out.append(GameRow(
            sport="tennis",
            game_id=str(r["match_id"]),
            date=r["date"],
            meta={
                "tour": r["tour"],
                "p1_id": p1_id,
                "p2_id": p2_id,
                "surface": r["surface"],
                "best_of": int(r["best_of"]),
                "date": r["date"],
            },
            outcomes={
                "p1_win": p1_win,
                "total_games": float(p1_games + p2_games),
            },
        ))
    return out


# ── Bucket helpers ─────────────────────────────────────────────

def _mlb_total_bucket(row: GameRow) -> str:
    t = row.outcomes.get("total")
    if t is None: return "unknown"
    return "high" if t > 9 else "low"


def _nba_total_bucket(row: GameRow) -> str:
    t = row.outcomes.get("total_points")
    if t is None: return "unknown"
    return "high" if t > 220 else "low"


def _tennis_surface_bucket(row: GameRow) -> str:
    return row.meta.get("surface", "unknown")


def _tennis_format_bucket(row: GameRow) -> str:
    return f"BO{row.meta.get('best_of', 3)}"


# ── Registration (run at import) ───────────────────────────────

register_sport(SportAdapter(
    name="mlb",
    load_games=_load_mlb_games,
    market_kinds={"home_win": "binary", "total": "regression"},
    bucket_keys={"total_bucket": _mlb_total_bucket},
))
register_sport(SportAdapter(
    name="nba",
    load_games=_load_nba_games,
    market_kinds={"home_win": "binary", "total_points": "regression",
                  "q1_home_win": "binary", "q1_total_points": "regression"},
    bucket_keys={"total_bucket": _nba_total_bucket},
))
register_sport(SportAdapter(
    name="nhl",
    load_games=_load_nhl_games,
    market_kinds={"home_win": "binary", "total_goals": "regression"},
))
register_sport(SportAdapter(
    name="tennis",
    load_games=_load_tennis_games,
    market_kinds={"p1_win": "binary", "total_games": "regression"},
    bucket_keys={"surface": _tennis_surface_bucket,
                 "format": _tennis_format_bucket},
))

register_stack("mlb",    "current", _current_mlb_stack)
register_stack("mlb",    "player_mc", _player_mc_mlb_stack)
register_stack("nba",    "current", _current_nba_q1_stack)
register_stack("nba",    "player_mc", _player_mc_nba_stack)
register_stack("nhl",    "current", _current_nhl_stack)
register_stack("tennis", "current", _current_tennis_stack)
register_stack("tennis", "player_mc", _player_mc_tennis_stack)
register_stack("tennis", "hybrid",   _hybrid_tennis_stack)


# ── CLI ────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="Universal backtest harness")
    p.add_argument("sport", choices=sorted(_SPORTS.keys()),
                   help="Sport to replay")
    p.add_argument("--games", type=int, default=300,
                   help="Number of recent completed games to replay")
    p.add_argument("--stack", action="append",
                   help="Stack name(s) to evaluate (default: all registered)")
    p.add_argument("--markets", type=str,
                   help="Comma-separated markets (default: all sport markets)")
    p.add_argument("--json", action="store_true",
                   help="Emit JSON instead of formatted text")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    markets = args.markets.split(",") if args.markets else None
    report = replay(args.sport, games=args.games,
                    stacks=args.stack, markets=markets)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(render_text(report))


if __name__ == "__main__":
    main()
