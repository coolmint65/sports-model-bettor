"""
NHL player-prop picker (Phase 2i-iii).

Mirrors ``engine.mlb_prop_picks`` for hockey. Skater + goalie props
share one code path; the bet_type → stat_key map decides which
sample array each prop reads from.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime

from .player_props_db import insert_pick, _conn_for
from .player_props_tracker import _NHL_STAT_KEY
from .nhl_player_mc import build_player_mc
from .mlb_prop_picks import (
    _normalize_name, _confidence_for,
    PROP_MIN_EDGE_PCT, PROP_MAX_ODDS,
)
from .mlb_player_mc import prob_over, prob_under

logger = logging.getLogger(__name__)


def _build_name_index() -> dict[str, list[dict]]:
    """Returns ``{normalized_lookup_key: [candidate_dict, ...]}``.

    Each candidate carries ``player_id, team_id, position, last_date``
    so the resolver can disambiguate same-name players using team
    context (NHL has Elias Pettersson the forward AND Elias Pettersson
    the defenseman on the same Vancouver roster) and bet-type's
    implied role (Goalie Saves only matches G; skater stats only
    match F/D).

    Each player gets multiple keys: full name AND ``initial lastname``
    so HR's "Mattias Ekholm" hits the boxscore's "M. Ekholm" entry.
    Per-key list preserves all candidates -- single-name lookups still
    work, but ambiguous ones surface every option for downstream
    filtering.
    """
    conn = _conn_for("nhl")
    rows = conn.execute(
        "SELECT player_id, player_name, team_id, "
        "       json_extract(stats_json, '$.position') AS position, "
        "       MAX(date) AS last_date "
        "FROM player_game_logs GROUP BY player_id ORDER BY last_date"
    ).fetchall()
    out: dict[str, list[dict]] = {}
    for r in rows:
        norm = _normalize_name(r["player_name"])
        if not norm:
            continue
        cand = {
            "player_id": int(r["player_id"]),
            "team_id":   int(r["team_id"]) if r["team_id"] is not None else 0,
            "position":  (r["position"] or "").upper() or "F",
            "last_date": r["last_date"] or "",
        }
        out.setdefault(norm, []).append(cand)
        parts = norm.split()
        if len(parts) >= 2:
            li_key = f"{parts[0][0]} {' '.join(parts[1:])}"
            if li_key != norm:
                out.setdefault(li_key, []).append(cand)
    return out


# bet_type → required position. Empty string means "anything but goalie".
_BET_TYPE_POSITION = {
    "Goalie Saves":          "G",
    "Goalie Goals Against":  "G",
    # All skater stats accept F or D, never G.
    "Skater Goals":   "skater",
    "Skater Assists": "skater",
    "Skater Points":  "skater",
    "Skater SOG":     "skater",
    "Skater Hits":    "skater",
    "Skater Blocks":  "skater",
}


def _resolve_player_id(name: str, idx: dict[str, list[dict]],
                        bet_type: str = "",
                        away_team_id: int = 0,
                        home_team_id: int = 0) -> int | None:
    """Resolve a player name to an id, using team + position context
    to disambiguate same-name candidates."""
    norm = _normalize_name(name)
    candidates: list[dict] = []
    if norm in idx:
        candidates.extend(idx[norm])
    parts = norm.split()
    if not candidates and len(parts) >= 2:
        li = f"{parts[0][0]} {' '.join(parts[1:])}"
        candidates.extend(idx.get(li, []))
    if not candidates:
        return None
    # Position filter: keep candidates whose role matches bet_type.
    required = _BET_TYPE_POSITION.get(bet_type, "")
    if required == "G":
        candidates = [c for c in candidates if c["position"] == "G"] or candidates
    elif required == "skater":
        candidates = [c for c in candidates if c["position"] != "G"] or candidates
    # Team filter: candidates on one of the two playing teams.
    if away_team_id or home_team_id:
        team_filtered = [c for c in candidates
                         if c["team_id"] in (away_team_id, home_team_id)]
        if team_filtered:
            candidates = team_filtered
    # Most recently active wins remaining ties.
    candidates.sort(key=lambda c: c["last_date"], reverse=True)
    return candidates[0]["player_id"] if candidates else None


def _resolve_game_id(games_conn: sqlite3.Connection,
                     away_abbr: str, home_abbr: str,
                     date: str) -> str | None:
    """Resolve to NHL games table game_id (NHL API gamePk). HR
    sometimes flips away/home vs the API — match either ordering."""
    try:
        from .abbr import aliases_for
        a_alts = aliases_for(away_abbr.strip(), sport="nhl") or [away_abbr.strip()]
        h_alts = aliases_for(home_abbr.strip(), sport="nhl") or [home_abbr.strip()]
    except Exception:
        a_alts = [away_abbr.strip()]
        h_alts = [home_abbr.strip()]
    abbrs = set(a_alts) | set(h_alts)
    placeholders = ",".join("?" * len(abbrs))
    row = games_conn.execute(
        f"SELECT g.game_id FROM nhl_games g "
        f"JOIN nhl_teams ht ON ht.id = g.home_team_id AND ht.abbreviation IN ({placeholders}) "
        f"JOIN nhl_teams at ON at.id = g.away_team_id AND at.abbreviation IN ({placeholders}) "
        f"WHERE g.date = ? LIMIT 1",
        (*abbrs, *abbrs, date),
    ).fetchone()
    return str(row["game_id"]) if row else None


def _score(samples: dict, prop: dict) -> dict | None:
    bet_type = prop.get("bet_type") or ""
    line = prop.get("line")
    if line is None:
        return None
    stat_key = _NHL_STAT_KEY.get(bet_type)
    if stat_key is None:
        return None
    samp = samples.get(stat_key)
    if samp is None or len(samp) == 0:
        return None
    over_odds = prop.get("over_odds")
    under_odds = prop.get("under_odds")
    p_over = prob_over(samp, line)
    p_under = prob_under(samp, line)
    from .distribution_fit import get_prob_shrink
    shrink = get_prob_shrink("nhl", stat_key)
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


def generate_picks(date: str | None = None,
                   props: dict | None = None,
                   *,
                   n_sims: int = 10_000,
                   lookback_days: int = 60) -> dict:
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    if props is None:
        from scrapers.hardrock_props import fetch_nhl_props
        props = fetch_nhl_props()

    name_idx = _build_name_index()
    if not name_idx:
        return {"evaluated": 0, "picked": 0,
                "skipped_no_player": 0, "skipped_no_mc": 0,
                "skipped_low_edge": 0,
                "warning": "name index empty"}

    mc_cache: dict[int, dict] = {}
    counts = {"evaluated": 0, "picked": 0,
              "skipped_no_player": 0, "skipped_no_mc": 0,
              "skipped_low_edge": 0}

    from .nhl_db import get_conn as _nhl_conn
    games_conn = _nhl_conn()

    for matchup_key, prop_rows in props.items():
        if "@" not in matchup_key:
            continue
        away_abbr, home_abbr = matchup_key.split("@", 1)
        game_id = _resolve_game_id(games_conn, away_abbr, home_abbr,
                                    target_date)
        if not game_id:
            counts["skipped_no_player"] += len(prop_rows)
            continue

        # Resolve game's team_ids once for use in name disambiguation.
        gt_row = games_conn.execute(
            "SELECT home_team_id, away_team_id FROM nhl_games WHERE game_id = ?",
            (game_id,),
        ).fetchone()
        away_tid = gt_row["away_team_id"] if gt_row else 0
        home_tid = gt_row["home_team_id"] if gt_row else 0

        for prop in prop_rows:
            counts["evaluated"] += 1
            player_name = prop.get("player_name") or ""
            bet_type = prop.get("bet_type", "")
            player_id = _resolve_player_id(player_name, name_idx,
                                            bet_type=bet_type,
                                            away_team_id=away_tid,
                                            home_team_id=home_tid)
            if not player_id:
                counts["skipped_no_player"] += 1
                continue

            if player_id not in mc_cache:
                mc_cache[player_id] = build_player_mc(
                    player_id, n_sims=n_sims,
                    lookback_days=lookback_days,
                )
            samples = mc_cache[player_id]
            best = _score(samples, prop)
            if best is None:
                if not samples:
                    counts["skipped_no_mc"] += 1
                else:
                    counts["skipped_low_edge"] += 1
                continue

            confidence = _confidence_for(best["edge"])
            pick_text = f"{best['side']} {best['line']:g}"
            insert_pick(
                "nhl",
                game_id=game_id,
                date=target_date,
                matchup=f"{away_abbr} @ {home_abbr}",
                player_id=player_id,
                player_name=player_name,
                bet_type=prop.get("bet_type", ""),
                pick=pick_text,
                line=best["line"],
                side=best["side"],
                model_prob=best["model_prob"],
                edge=best["edge"],
                odds=best["odds"],
                confidence=confidence,
            )
            counts["picked"] += 1

    logger.info("nhl_prop_picks: %s", counts)
    return counts


__all__ = ["generate_picks"]
