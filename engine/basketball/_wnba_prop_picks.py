"""WNBA player-prop picker.

Mirrors ``engine.sports.nba.prop_picks`` — same scraper → MC → score
→ tracker loop. Swaps the per-sport modules:

    name index + game lookup  → wnba.db (basketball framework)
    distributions             → distribution_fit.get_distribution('wnba')
    stat-key map              → _NBA_STAT_KEY (WNBA shares NBA's bet-types)
    HR scraper                → fetch ``wnba`` from hardrock_props
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta

from ..player_props_db import insert_pick, _conn_for
from ..player_props_tracker import _NBA_STAT_KEY
from ._wnba_player_mc import build_player_mc
from ..mlb_prop_picks import (
    _normalize_name, _confidence_for,
    PROP_MIN_EDGE_PCT, PROP_MAX_ODDS, PROP_JUICE_WALL,
    _collapse_and_cap, _apply_slate_top_n,
)
from .._tz import et_today_str

logger = logging.getLogger(__name__)


def _build_name_index() -> dict[str, int]:
    conn = _conn_for("wnba")
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
                     date: str) -> tuple[str | None, str | None]:
    """Resolve a (away_abbr, home_abbr) pair to the next upcoming WNBA
    game's (game_id, actual_date). Skips already-played games.

    The basketball framework uses the same ``games`` + ``teams`` table
    shape for every league, so the join is straightforward."""
    abbrs = {away_abbr.strip(), home_abbr.strip()}
    if not abbrs:
        return None, None
    placeholders = ",".join("?" * len(abbrs))
    try:
        anchor = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        anchor = datetime.now()
    lo = (anchor - timedelta(days=1)).strftime("%Y-%m-%d")
    hi = (anchor + timedelta(days=2)).strftime("%Y-%m-%d")
    row = games_conn.execute(
        f"SELECT g.game_id, g.date FROM games g "
        f"JOIN teams ht ON ht.id = g.home_team_id AND ht.abbreviation IN ({placeholders}) "
        f"JOIN teams at ON at.id = g.away_team_id AND at.abbreviation IN ({placeholders}) "
        f"WHERE g.date BETWEEN ? AND ? "
        f"  AND COALESCE(g.status, '') NOT IN ('live', 'final', 'postponed') "
        f"ORDER BY g.date ASC LIMIT 1",
        (*abbrs, *abbrs, lo, hi),
    ).fetchone()
    if not row:
        return None, None
    return str(row["game_id"]), str(row["date"])


def _score(samples: dict, prop: dict) -> dict | None:
    """Score one HR prop against the MC samples. Returns the best
    (Over/Under) candidate dict, or None when no side clears the
    juice/cap/edge filters."""
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
    from ..mlb_player_mc import prob_over, prob_under
    from ..distribution_fit import get_prob_shrink
    p_over = prob_over(samp, line)
    p_under = prob_under(samp, line)
    # Shrinkage map lives under the 'wnba' sport key — falls through to
    # 1.0 (no shrink) until calibration accumulates.
    shrink = get_prob_shrink("wnba", stat_key)
    if shrink < 1.0:
        p_over = 0.5 + (p_over - 0.5) * shrink
        p_under = 0.5 + (p_under - 0.5) * shrink
    cands: list[dict] = []
    for side, odds, p in [("Over", over_odds, p_over),
                            ("Under", under_odds, p_under)]:
        if odds is None:
            continue
        if odds > PROP_MAX_ODDS:
            continue
        if odds < PROP_JUICE_WALL:
            continue
        n = float(odds)
        implied = 100.0 / (n + 100.0) if n > 0 else abs(n) / (abs(n) + 100.0)
        edge = (p - implied) * 100.0
        cands.append({"side": side, "line": float(line), "odds": int(odds),
                      "model_prob": p, "implied_prob": implied, "edge": edge})
    if not cands:
        return None
    best = max(cands, key=lambda c: c["edge"])
    if best["edge"] < PROP_MIN_EDGE_PCT:
        return None
    return best


def generate_picks(date: str | None = None,
                   props: dict | None = None,
                   *,
                   n_sims: int = 10_000,
                   lookback_days: int = 365) -> dict:
    # WNBA's season runs May-October — a 60-day lookback at season open
    # (mid-May) finds zero games for every player because the prior
    # season finished 7 months ago. 365 covers the full prior season so
    # the MC has a real distribution to sample. Tighten this after we've
    # accumulated 2-3 weeks of current-season games (which capture the
    # latest team/role context better than last year's data).
    target_date = date or et_today_str()
    if props is None:
        from scrapers.hardrock_props import _fetch_props
        props = _fetch_props("wnba")

    name_index = _build_name_index()
    if not name_index:
        return {"evaluated": 0, "picked": 0,
                "skipped_no_player": 0, "skipped_no_mc": 0,
                "skipped_low_edge": 0,
                "warning": "name index empty — backfill player_game_logs first"}

    mc_cache: dict[tuple, dict] = {}
    counts = {"evaluated": 0, "picked": 0,
              "skipped_no_player": 0, "skipped_no_mc": 0,
              "skipped_low_edge": 0}

    from ._db import get_conn as _bb_conn
    games_conn = _bb_conn("wnba")
    slate_winners: list[tuple[dict, str, str, str]] = []
    per_entry_date: dict[int, str] = {}

    for matchup_key, prop_rows in props.items():
        if "@" not in matchup_key:
            continue
        away_abbr, home_abbr = matchup_key.split("@", 1)
        game_id, game_date = _resolve_game_id(games_conn, away_abbr, home_abbr,
                                               target_date)
        if not game_id:
            counts["skipped_no_player"] += len(prop_rows)
            continue
        pick_date = game_date or target_date
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

        for entry in _collapse_and_cap(best_per_pair):
            slate_winners.append(
                (entry, str(game_id), pick_date,
                 f"{away_abbr} @ {home_abbr}"))
            per_entry_date[id(entry)] = pick_date

    top, deltas = _apply_slate_top_n("wnba", target_date, slate_winners)
    for entry, game_id, _date, matchup in top:
        best = entry["scored"]
        confidence = _confidence_for(best["edge"])
        pick_text = f"{best['side']} {best['line']:g}"
        pick_date = per_entry_date.get(id(entry), target_date)
        insert_pick(
            "wnba",
            game_id=game_id, date=pick_date,
            matchup=matchup,
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
    if deltas.get("slate_dropped"):
        counts["slate_dropped"] = deltas["slate_dropped"]
    if deltas.get("voided_stale"):
        counts["voided_stale"] = deltas["voided_stale"]

    logger.info("wnba_prop_picks: %s", counts)
    return counts


__all__ = ["generate_picks"]
