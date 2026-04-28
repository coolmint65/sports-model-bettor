"""
MLB player-prop picker (Phase 2g-iii).

Closes the loop scraper → MC → picker → tracker:

    fetch_mlb_props()              # HR feed (scrapers.hardrock_props)
        ↓
    build_player_mc(player_id)     # CDFs (engine.mlb_player_mc)
        ↓
    score_player_prop(...)         # this module
        ↓
    insert_pick()                  # player_props_picks table

For each HR prop row (player, bet_type, line, side, odds), looks up
the player's MC samples for the corresponding stat, computes the
empirical model_prob, derives edge vs HR's implied, applies
``engine.dynamic_reliability`` per bet_type, and persists the
qualifying picks.

Player-name → player_id resolution uses the most recent
``player_game_logs`` row matching a normalized name (lowercase,
stripped of punctuation + accents). Multi-player name collisions
are resolved by recency (last-played wins) and by team-context when
HR's team_side is set ("A"/"B" → away/home). Unresolved players
are skipped — a prop without a player_id can't be settled, so
rather than insert orphans we'd rather miss the pick.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import unicodedata
from datetime import datetime
from typing import Any

from .player_props_db import _conn_for, insert_pick
from .player_props_tracker import _MLB_STAT_KEY
from .mlb_player_mc import build_player_mc, prob_over, prob_under
from .distribution_fit import get_prob_shrink

logger = logging.getLogger(__name__)


# Min edge to record a pick. Mirrors EDGE_LEAN from engine.config but
# kept local so prop edges can move independently of game-line edges.
PROP_MIN_EDGE_PCT = 4.0

# Max American odds for a pick. NegBin tail probabilities inflate fast
# when a player has few games, generating spurious "edge" on extreme
# alt lines (Over 9.5 Ks at +5000 with the model giving 14% when HR
# implies 2%). Until Phase 2j backtest validates the tail, cap odds
# at +400 (~20% implied) so we don't ship Yes-on-longshot picks.
PROP_MAX_ODDS = 400

# Juice wall — minimum acceptable American odds for a prop pick.
# Mirrors the game-line MLB_JUICE_WALL (-200): below this the price
# eats too much of the variance for a model edge to be real. Surfaced
# 2026-04-28 when the user reported seeing -250s in the prop slate
# (e.g. Over 4.5 K with a heavy fav price). At -250 you risk $250 to
# win $100 — a 2pp model edge needs to be near-perfect to be +EV.
PROP_JUICE_WALL = -200


def _normalize_name(name: str) -> str:
    """Lowercase, strip accents and punctuation. Handles HR's
    "Suárez" vs MLB API's "Suarez" plus generic suffix cleanup."""
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    cleaned = re.sub(r"[^a-zA-Z\s]", " ", ascii_only).lower()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    # Strip common suffixes that drift between sources
    cleaned = re.sub(r"\s+(jr|sr|ii|iii|iv)\b", "", cleaned)
    return cleaned


def _build_name_index(sport: str = "mlb",
                       since_date: str | None = None) -> dict[str, int]:
    """One-shot index ``{normalized_name: player_id}`` from recent
    game-log rows. Last-played wins on collisions — if two players
    share a name (rare in MLB, but happens), the more recently active
    one is the one most likely to appear in today's HR feed."""
    conn = _conn_for(sport)
    sql = (
        "SELECT player_id, player_name, MAX(date) AS last_date "
        "FROM player_game_logs "
    )
    params: tuple = ()
    if since_date:
        sql += "WHERE date >= ? "
        params = (since_date,)
    sql += "GROUP BY player_id ORDER BY last_date"  # ascending = newer overwrites older
    rows = conn.execute(sql, params).fetchall()
    out: dict[str, int] = {}
    for r in rows:
        norm = _normalize_name(r["player_name"])
        if not norm:
            continue
        out[norm] = int(r["player_id"])
    return out


def _stat_for_bet_type(bet_type: str) -> str | None:
    return _MLB_STAT_KEY.get(bet_type)


def _implied_prob(odds: int | None) -> float | None:
    """American → implied probability. Devigging is intentionally NOT
    applied here — the prop scraper returns single-sided yes-only
    markets for many alts, and the over/under devig assumption breaks
    on those. Edge calculations against this implied are fine because
    the model_prob already includes our priors and uncertainty; the
    implied is just "what HR's price means at face value." """
    if odds is None:
        return None
    n = float(odds)
    if n == 0:
        return None
    return 100.0 / (n + 100.0) if n > 0 else abs(n) / (abs(n) + 100.0)


def score_player_prop(samples: dict, prop: dict) -> dict | None:
    """For one HR prop row, return the best-side pick dict or None
    when neither side clears ``PROP_MIN_EDGE_PCT``.

    ``samples`` is the player's CDF dict (``build_player_mc()``).
    ``prop`` is one row from ``fetch_mlb_props()`` (player_name,
    bet_type, line, over_odds, under_odds, is_alt, hr_market_type).
    """
    bet_type = prop.get("bet_type") or ""
    line = prop.get("line")
    if line is None:
        return None
    stat_key = _stat_for_bet_type(bet_type)
    if stat_key is None:
        return None
    samp = samples.get(stat_key)
    if samp is None or len(samp) == 0:
        return None

    over_odds = prop.get("over_odds")
    under_odds = prop.get("under_odds")
    p_over = prob_over(samp, line)
    p_under = prob_under(samp, line)
    # Shrink toward 50% per the per-stat calibration findings. Only
    # the "winning side" gets pulled in (whichever has p > 0.5); the
    # losing side gets the complement so we don't over-correct.
    shrink = get_prob_shrink("mlb", stat_key)
    if shrink < 1.0:
        p_over = 0.5 + (p_over - 0.5) * shrink
        p_under = 0.5 + (p_under - 0.5) * shrink

    candidates: list[dict] = []
    if over_odds is not None:
        implied = _implied_prob(over_odds)
        if implied is not None:
            edge = (p_over - implied) * 100.0
            candidates.append({
                "side": "Over", "line": float(line),
                "odds": int(over_odds), "model_prob": p_over,
                "implied_prob": implied, "edge": edge,
            })
    if under_odds is not None:
        implied = _implied_prob(under_odds)
        if implied is not None:
            edge = (p_under - implied) * 100.0
            candidates.append({
                "side": "Under", "line": float(line),
                "odds": int(under_odds), "model_prob": p_under,
                "implied_prob": implied, "edge": edge,
            })
    # Drop longshot lines before ranking — see PROP_MAX_ODDS comment.
    candidates = [c for c in candidates if c["odds"] <= PROP_MAX_ODDS]
    # Drop heavy-juice lines — see PROP_JUICE_WALL comment.
    candidates = [c for c in candidates if c["odds"] >= PROP_JUICE_WALL]
    if not candidates:
        return None

    best = max(candidates, key=lambda c: c["edge"])
    if best["edge"] < PROP_MIN_EDGE_PCT:
        return None
    return best


def _confidence_for(edge: float) -> str:
    """Thresholds match the core picks engine — consistent badge
    semantics across game lines / derivatives / props."""
    if edge >= 12.0:
        return "strong"
    if edge >= 7.0:
        return "moderate"
    if edge >= PROP_MIN_EDGE_PCT:
        return "lean"
    return "skip"


# Volume cap per game. The user explicitly asked: "instead of having
# a prop for basically every player, we have one or two per game that
# are the highest conviction." Two picks per game × ~10 games/night
# yields ~20 picks/night vs the previous ~200, which made the slate
# unscannable. Headline-only.
TOP_N_PER_GAME = 2


def _collapse_and_cap(best_per_pair: dict[tuple, dict]) -> list[dict]:
    """Reduce the per-game candidate set to the top-N highest-conviction
    picks, with at most one pick per player. Used by every prop picker
    (mlb / nhl / nba) so the volume cap is consistent.

    Selection order:
      1. Per player, keep the (bet_type) with the largest edge.
      2. Across the remaining players, sort by edge descending.
      3. Take the first TOP_N_PER_GAME.
    """
    best_per_player: dict[int, dict] = {}
    for entry in best_per_pair.values():
        pid = int(entry["player_id"])
        if (pid not in best_per_player
                or entry["scored"]["edge"] > best_per_player[pid]["scored"]["edge"]):
            best_per_player[pid] = entry
    ranked = sorted(best_per_player.values(),
                    key=lambda e: e["scored"]["edge"], reverse=True)
    return ranked[:TOP_N_PER_GAME]


def _void_stale_pending(sport: str, game_id: str, target_date: str,
                        keep: list[tuple]) -> int:
    """Delete pending prop rows for this (game_id, date) that aren't in
    the current top-N keep set. Mirrors the void-on-line-move pattern
    in derivative_tracker — without it, yesterday's picker run leaves
    stale low-edge / off-line picks pending forever (e.g. Julius Randle
    Player Points Under 21.5 lingering after today's run swapped to
    Under 24.5). Only touches result IS NULL so settled history stays.

    ``keep`` is a list of ``(player_id, bet_type, pick)`` tuples we want
    to retain. Anything else for the same game/date that's still
    pending gets dropped.

    Picks referenced by a locked prop POTD are also retained, so a line
    swap on the POTD's bet doesn't leave the POTD dangling. The POTD's
    own line refresh repoints to the new line afterward.
    """
    from .player_props_db import _conn_for
    conn = _conn_for(sport)
    rows = conn.execute(
        "SELECT id, player_id, bet_type, pick FROM player_props_picks "
        "WHERE game_id = ? AND date = ? AND result IS NULL",
        (str(game_id), target_date),
    ).fetchall()
    keep_set = {(int(p), str(bt), str(pk)) for p, bt, pk in keep}
    # Pick IDs currently referenced by any prop POTD lock — never void
    # these even if they fell out of the top-N keep set.
    locked_potd_ids = {
        int(r["pick_id"]) for r in conn.execute(
            "SELECT pick_id FROM player_props_picks_pot_day"
        ).fetchall() if r["pick_id"] is not None
    }
    drop_ids: list[int] = []
    for r in rows:
        if int(r["id"]) in locked_potd_ids:
            continue
        key = (int(r["player_id"]), str(r["bet_type"]), str(r["pick"]))
        if key not in keep_set:
            drop_ids.append(int(r["id"]))
    if drop_ids:
        placeholders = ",".join("?" * len(drop_ids))
        conn.execute(
            f"DELETE FROM player_props_picks WHERE id IN ({placeholders})",
            drop_ids,
        )
        conn.commit()
    return len(drop_ids)


def generate_picks(date: str | None = None,
                   props: dict | None = None,
                   *,
                   n_sims: int = 10_000,
                   lookback_days: int = 60) -> dict:
    """Pull today's MLB props, score every line, and persist
    qualifying picks to ``player_props_picks``.

    Returns ``{evaluated, picked, skipped_no_player, skipped_no_mc,
    skipped_low_edge}``.
    """
    target_date = date or datetime.now().strftime("%Y-%m-%d")

    # Lazy fetch the prop feed if caller didn't preload it.
    if props is None:
        from scrapers.hardrock_props import fetch_mlb_props
        props = fetch_mlb_props()

    name_index = _build_name_index("mlb")
    if not name_index:
        return {"evaluated": 0, "picked": 0,
                "skipped_no_player": 0, "skipped_no_mc": 0,
                "skipped_low_edge": 0,
                "warning": "name index empty (no player_game_logs)"}

    # Cache MC builds per player so multiple props for the same player
    # share one sample run.
    mc_cache: dict[int, dict] = {}

    counts = {"evaluated": 0, "picked": 0,
              "skipped_no_player": 0, "skipped_no_mc": 0,
              "skipped_low_edge": 0}

    # Need MLB Stats API game_pk for each matchup so the settler can
    # join player_game_logs cleanly. Look up via games table on the
    # canonical matchup string.
    from .db import get_conn as _mlb_conn
    games_conn = _mlb_conn()

    for matchup_key, prop_rows in props.items():
        # matchup_key shape: "AWAY@HOME" (per scraper)
        if "@" not in matchup_key:
            continue
        away_abbr, home_abbr = matchup_key.split("@", 1)[0:2]
        # Resolve to mlb_game_id for today's game between these teams
        game_pk = _resolve_game_pk(games_conn, away_abbr, home_abbr,
                                    target_date)
        if not game_pk:
            counts["skipped_no_player"] += len(prop_rows)
            continue

        # Dedup at (player_id, bet_type): HR ships ~10 alt lines per
        # player per stat (Over 0.5 / 1.5 / 2.5 etc.) and the picker
        # would otherwise insert one row per qualifying line. Surfaced
        # as "the player has 6 picks for the same stat" in the UI.
        # Keep one row per (player, bet_type) — the highest-edge
        # (line, side) combo — so each player shows at most one pick
        # per stat per game.
        best_per_pair: dict[tuple, dict] = {}
        for prop in prop_rows:
            counts["evaluated"] += 1
            player_name = prop.get("player_name") or ""
            norm = _normalize_name(player_name)
            player_id = name_index.get(norm)
            if not player_id:
                counts["skipped_no_player"] += 1
                continue
            cache_key = (player_id, str(game_pk))
            if cache_key not in mc_cache:
                mc_cache[cache_key] = build_player_mc(
                    player_id, n_sims=n_sims,
                    lookback_days=lookback_days,
                    game_pk=str(game_pk),
                )
            samples = mc_cache[cache_key]
            scored = score_player_prop(samples, prop)
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
                "player_id": player_id,
                "player_name": player_name,
                "prop": prop,
                "scored": scored,
            }

        top = _collapse_and_cap(best_per_pair)
        keep = []
        for entry in top:
            best = entry["scored"]
            confidence = _confidence_for(best["edge"])
            pick_text = f"{best['side']} {best['line']:g}"
            keep.append((entry["player_id"],
                         entry["prop"].get("bet_type", ""),
                         pick_text))
            insert_pick(
                "mlb",
                game_id=str(game_pk),
                date=target_date,
                matchup=f"{away_abbr} @ {home_abbr}",
                player_id=entry["player_id"],
                player_name=entry["player_name"],
                bet_type=entry["prop"].get("bet_type", ""),
                pick=pick_text,
                line=best["line"],
                side=best["side"],
                model_prob=best["model_prob"],
                edge=best["edge"],
                odds=best["odds"],
                confidence=confidence,
            )
            counts["picked"] += 1
        voided = _void_stale_pending("mlb", str(game_pk), target_date, keep)
        if voided:
            counts.setdefault("voided_stale", 0)
            counts["voided_stale"] += voided

    logger.info("mlb_prop_picks: %s", counts)
    return counts


def _resolve_game_pk(games_conn: sqlite3.Connection,
                     away_abbr: str, home_abbr: str,
                     date: str) -> int | None:
    """Translate an (away, home) abbr pair to the MLB Stats game_pk.

    Match is direction-agnostic — Hard Rock and the MLB Stats API
    sometimes flip away/home for the same physical game (today's HR
    feed has CLE@TB while MLB has TB@CLE). We accept either ordering
    and let the per-day match handle the rest. Two MLB games between
    the same pair on the same date would only happen for a
    doubleheader; the LIMIT 1 picks one and the picker still produces
    valid prop output (the player will appear in both anyway)."""
    try:
        from .abbr import aliases_for
        h_alts = aliases_for(home_abbr.strip(), sport="mlb") or [home_abbr.strip()]
        a_alts = aliases_for(away_abbr.strip(), sport="mlb") or [away_abbr.strip()]
    except Exception:
        h_alts = [home_abbr.strip()]
        a_alts = [away_abbr.strip()]
    abbr_set = set(h_alts) | set(a_alts)
    placeholders = ",".join("?" * len(abbr_set))
    row = games_conn.execute(
        f"SELECT g.mlb_game_id FROM games g "
        f"JOIN teams ht ON ht.mlb_id = g.home_team_id AND ht.abbreviation IN ({placeholders}) "
        f"JOIN teams at ON at.mlb_id = g.away_team_id AND at.abbreviation IN ({placeholders}) "
        f"WHERE g.date = ? LIMIT 1",
        (*abbr_set, *abbr_set, date),
    ).fetchone()
    return int(row["mlb_game_id"]) if row else None


__all__ = [
    "generate_picks", "score_player_prop",
    "PROP_MIN_EDGE_PCT", "PROP_MAX_ODDS", "PROP_JUICE_WALL",
]
