"""
NBA player-prop picker (Phase 2h-iii).

Mirrors ``engine.mlb_prop_picks`` for basketball — same scraper →
MC → score → tracker loop, swapping the per-sport modules.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime

from .player_props_db import insert_pick
from .player_props_tracker import _NBA_STAT_KEY
from .nba_player_mc import build_player_mc
from .mlb_prop_picks import (
    score_player_prop, _normalize_name, _confidence_for,
    PROP_MIN_EDGE_PCT, PROP_MAX_ODDS,
)
from .player_props_db import _conn_for

logger = logging.getLogger(__name__)


def _build_name_index() -> dict[str, int]:
    conn = _conn_for("nba")
    rows = conn.execute(
        "SELECT player_id, player_name, MAX(date) AS last_date "
        "FROM player_game_logs GROUP BY player_id ORDER BY last_date"
    ).fetchall()
    out: dict[str, int] = {}
    for r in rows:
        norm = _normalize_name(r["player_name"])
        if norm:
            out[norm] = int(r["player_id"])
    return out


def _resolve_game_id(games_conn: sqlite3.Connection,
                     away_abbr: str, home_abbr: str,
                     date: str) -> str | None:
    """Resolve to the NBA games table game_id (ESPN event id) for
    today's matchup. Direction-agnostic — HR sometimes flips
    away/home vs ESPN."""
    abbrs = {away_abbr.strip(), home_abbr.strip()}
    placeholders = ",".join("?" * len(abbrs))
    row = games_conn.execute(
        f"SELECT g.game_id FROM nba_games g "
        f"JOIN nba_teams ht ON ht.id = g.home_team_id AND ht.abbreviation IN ({placeholders}) "
        f"JOIN nba_teams at ON at.id = g.away_team_id AND at.abbreviation IN ({placeholders}) "
        f"WHERE g.date = ? LIMIT 1",
        (*abbrs, *abbrs, date),
    ).fetchone()
    return str(row["game_id"]) if row else None


def generate_picks(date: str | None = None,
                   props: dict | None = None,
                   *,
                   n_sims: int = 10_000,
                   lookback_days: int = 60) -> dict:
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    if props is None:
        from scrapers.hardrock_props import fetch_nba_props
        props = fetch_nba_props()

    name_index = _build_name_index()
    if not name_index:
        return {"evaluated": 0, "picked": 0,
                "skipped_no_player": 0, "skipped_no_mc": 0,
                "skipped_low_edge": 0,
                "warning": "name index empty"}

    mc_cache: dict[int, dict] = {}
    counts = {"evaluated": 0, "picked": 0,
              "skipped_no_player": 0, "skipped_no_mc": 0,
              "skipped_low_edge": 0}

    from .nba_db import get_conn as _nba_conn
    games_conn = _nba_conn()

    for matchup_key, prop_rows in props.items():
        if "@" not in matchup_key:
            continue
        away_abbr, home_abbr = matchup_key.split("@", 1)
        game_id = _resolve_game_id(games_conn, away_abbr, home_abbr,
                                    target_date)
        if not game_id:
            counts["skipped_no_player"] += len(prop_rows)
            continue

        # Dedup at (player_id, bet_type) — see mlb_prop_picks for the
        # full rationale. HR ships ~8 alt lines per stat per player;
        # without this filter the UI would show one player with 6+
        # picks for the same stat (e.g., Castle Player Rebounds with
        # 6 different lines). Keep highest-edge (line, side) per pair.
        best_per_pair: dict[tuple, dict] = {}
        for prop in prop_rows:
            counts["evaluated"] += 1
            player_name = prop.get("player_name") or ""
            norm = _normalize_name(player_name)
            player_id = name_index.get(norm)
            if not player_id:
                counts["skipped_no_player"] += 1
                continue
            cache_key = (player_id, str(game_id))
            if cache_key not in mc_cache:
                mc_cache[cache_key] = build_player_mc(
                    player_id, n_sims=n_sims,
                    lookback_days=lookback_days,
                    game_id=str(game_id),
                )
            samples = mc_cache[cache_key]
            scored = _score(samples, prop)
            if scored is None:
                if not samples:
                    counts["skipped_no_mc"] += 1
                else:
                    counts["skipped_low_edge"] += 1
                continue
            pair_key = (player_id, prop.get("bet_type", ""))
            if pair_key in best_per_pair and \
                    scored["edge"] <= best_per_pair[pair_key]["scored"]["edge"]:
                continue
            best_per_pair[pair_key] = {
                "player_id": player_id, "player_name": player_name,
                "prop": prop, "scored": scored,
            }

        for entry in best_per_pair.values():
            best = entry["scored"]
            confidence = _confidence_for(best["edge"])
            pick_text = f"{best['side']} {best['line']:g}"
            insert_pick(
                "nba",
                game_id=game_id, date=target_date,
                matchup=f"{away_abbr} @ {home_abbr}",
                player_id=entry["player_id"],
                player_name=entry["player_name"],
                bet_type=entry["prop"].get("bet_type", ""),
                pick=pick_text,
                line=best["line"], side=best["side"],
                model_prob=best["model_prob"],
                edge=best["edge"], odds=best["odds"],
                confidence=confidence,
            )
            counts["picked"] += 1

    logger.info("nba_prop_picks: %s", counts)
    return counts


def _score(samples: dict, prop: dict) -> dict | None:
    """Local wrapper that swaps in the NBA stat-key map before
    delegating to the shared scorer."""
    from .mlb_prop_picks import score_player_prop as _sc
    # The shared scorer reads _stat_for_bet_type which itself uses
    # _MLB_STAT_KEY. Patch by passing a custom resolver via
    # monkey-bypass: we'll inline a small replica here so we don't
    # mutate module globals.
    bet_type = prop.get("bet_type") or ""
    line = prop.get("line")
    if line is None:
        return None
    stat_key = _NBA_STAT_KEY.get(bet_type)
    if stat_key is None:
        return None
    samp = samples.get(stat_key)
    if samp is None or len(samp) == 0:
        return None
    over_odds = prop.get("over_odds")
    under_odds = prop.get("under_odds")
    from .mlb_player_mc import prob_over, prob_under
    from .distribution_fit import get_prob_shrink
    p_over = prob_over(samp, line)
    p_under = prob_under(samp, line)
    shrink = get_prob_shrink("nba", stat_key)
    if shrink < 1.0:
        p_over = 0.5 + (p_over - 0.5) * shrink
        p_under = 0.5 + (p_under - 0.5) * shrink
    cands: list[dict] = []
    for side, odds, p in [("Over", over_odds, p_over), ("Under", under_odds, p_under)]:
        if odds is None:
            continue
        if odds > PROP_MAX_ODDS:
            continue
        n = float(odds)
        implied = 100.0/(n+100.0) if n > 0 else abs(n)/(abs(n)+100.0)
        edge = (p - implied) * 100.0
        cands.append({"side": side, "line": float(line), "odds": int(odds),
                      "model_prob": p, "implied_prob": implied, "edge": edge})
    if not cands:
        return None
    best = max(cands, key=lambda c: c["edge"])
    if best["edge"] < PROP_MIN_EDGE_PCT:
        return None
    return best


__all__ = ["generate_picks"]
