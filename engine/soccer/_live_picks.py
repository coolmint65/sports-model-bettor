"""Soccer halftime live picks — fires at HT against rest-of-match
markets HR keeps live during the break (1X2 full-time, OU full-game,
BTTS, DC, DNB, AH).

H1-scoped markets are skipped here — H1 is settled facts by halftime,
so re-emitting H1 picks would be pointless. The picks engine's full-
game half is reused as-is: same `_ml_picks` / `_ou_picks` / etc, fed
the halftime-adjusted prediction dict from ``_live_predict``.

Picks land in a per-league ``live_picks_soccer`` table (mirrors how
NHL/NBA keep prematch and live picks in separate tables) so the
prematch tracker stays clean.

Dedup: one live pick per family per match. If the picker re-fires
during the same break (worker tick, manual route hit, etc.), the
existing row gets updated rather than duplicated — same shape as the
prematch tracker dedup, just keyed on ``stage='ht'`` to scope the
uniqueness to "this halftime emission" vs any future live emissions.
"""
from __future__ import annotations

import logging
from datetime import datetime

from ._db import get_conn
from ._live_predict import predict_at_halftime
from ._picks import (
    _format_matchup, _ml_picks, _ou_picks, _btts_picks,
    _dnb_picks, _dc_picks, _ah_picks,
)

logger = logging.getLogger(__name__)


_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS live_picks_soccer (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    date          TEXT,
    match_id      INTEGER,
    matchup       TEXT,
    stage         TEXT,                       -- 'ht' for now; future: 'p70' etc.
    bet_type      TEXT,
    pick          TEXT,
    side          TEXT,
    line          REAL,
    model_prob    REAL,
    edge          REAL,
    odds          INTEGER,
    h1_home       INTEGER,
    h1_away       INTEGER,
    stake_units   REAL,
    result        TEXT,
    profit        REAL,
    closing_odds  INTEGER,
    created_at    TEXT DEFAULT (datetime('now')),
    settled_at    TEXT,
    UNIQUE(match_id, stage, bet_type)
);
"""


def _ensure_table(conn) -> None:
    conn.executescript(_TABLE_SCHEMA)
    conn.commit()


def generate_live_picks_at_halftime(
        league: str, match_id: int, *,
        h1_home: int | None = None, h1_away: int | None = None,
        odds: dict | None = None,
        write_to_db: bool = True) -> dict:
    """Compute halftime-adjusted picks for one match.

    Args:
        league:    soccer league key
        match_id:  ESPN-style match id, present in the matches table
        h1_home/h1_away: caller can override the DB-stored HT scores
                   (useful for backtest/replay); otherwise pulled from
                   the matches table.
        odds:      live HR odds dict for this match. If None, the
                   caller is expected to fetch it — we don't refresh
                   inline because halftime markets are time-sensitive
                   and the picks loop usually pulls one bulk live-odds
                   snapshot then walks games.
        write_to_db: persist picks. False for analysis/backtest.

    Returns ``{match_id, prediction, picks, stored}``.
    """
    conn = get_conn(league)
    row = conn.execute(
        "SELECT m.id, m.date, m.status, m.home_team_id, m.away_team_id, "
        "       m.home_score_ht, m.away_score_ht, m.neutral_site, "
        "       m.home_side, "
        "       ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr, "
        "       ht.name AS home_name, at.name AS away_name "
        "FROM matches m "
        "JOIN teams ht ON ht.id = m.home_team_id "
        "JOIN teams at ON at.id = m.away_team_id "
        "WHERE m.id = ?", (int(match_id),),
    ).fetchone()
    if not row:
        return {"error": f"match {match_id} not found in {league}"}

    if h1_home is None:
        h1_home = row["home_score_ht"]
    if h1_away is None:
        h1_away = row["away_score_ht"]
    if h1_home is None or h1_away is None:
        return {"error": f"match {match_id} has no HT scores yet"}

    pred = predict_at_halftime(
        league,
        int(row["home_team_id"]), int(row["away_team_id"]),
        h1_home=int(h1_home), h1_away=int(h1_away),
        neutral_site=bool(row["neutral_site"]),
        home_side=row["home_side"],
    )
    pred["match_id"] = int(row["id"])
    pred["home_abbr"] = row["home_abbr"]
    pred["away_abbr"] = row["away_abbr"]
    pred["home_name"] = row["home_name"]
    pred["away_name"] = row["away_name"]

    matchup = _format_matchup(pred)
    pred_match_id = pred["match_id"]
    odds = odds or {}

    full: list[dict] = []
    full += _ml_picks(league, matchup, pred_match_id, pred, odds)
    full += _ou_picks(league, matchup, pred_match_id, pred, odds)
    full += _btts_picks(league, matchup, pred_match_id, pred, odds)
    full += _dnb_picks(league, matchup, pred_match_id, pred, odds)
    full += _dc_picks(league, matchup, pred_match_id, pred, odds)
    full += _ah_picks(league, matchup, pred_match_id, pred, odds)
    # Positive-edge only, then take the headline (top by edge).
    full = [p for p in full if (p.get("edge") or 0) > 0]
    full.sort(key=lambda p: (-(p.get("edge") or 0),
                              -(p.get("raw_prob") or 0)))
    headline = full[:1]

    stored = 0
    if write_to_db and headline:
        _ensure_table(conn)
        date = row["date"] or datetime.utcnow().strftime("%Y-%m-%d")
        for p in headline:
            try:
                conn.execute(
                    "INSERT INTO live_picks_soccer "
                    "(date, match_id, matchup, stage, bet_type, pick, "
                    " side, line, model_prob, edge, odds, h1_home, "
                    " h1_away, stake_units) "
                    "VALUES (?, ?, ?, 'ht', ?, ?, ?, ?, ?, ?, ?, ?, "
                    "        ?, ?) "
                    "ON CONFLICT(match_id, stage, bet_type) DO UPDATE "
                    "SET pick = excluded.pick, side = excluded.side, "
                    "    line = excluded.line, "
                    "    model_prob = excluded.model_prob, "
                    "    edge = excluded.edge, odds = excluded.odds, "
                    "    h1_home = excluded.h1_home, "
                    "    h1_away = excluded.h1_away, "
                    "    stake_units = excluded.stake_units, "
                    "    created_at = datetime('now')",
                    (date, pred_match_id, matchup, p.get("type"),
                     p.get("pick"), p.get("side"), p.get("line"),
                     float(p.get("prob") or 0.0),
                     float(p.get("edge") or 0.0),
                     int(p.get("odds") or 0),
                     int(h1_home), int(h1_away),
                     p.get("stake_units")),
                )
                stored += 1
            except Exception as e:
                logger.warning("[soccer:%s] live pick insert failed: %s",
                                league, e)
        conn.commit()
    return {
        "match_id": pred_match_id,
        "matchup": matchup,
        "h1_home": int(h1_home),
        "h1_away": int(h1_away),
        "prediction": {
            "p_home": pred["p_home"],
            "p_draw": pred["p_draw"],
            "p_away": pred["p_away"],
            "p_over_25": pred["p_over_25"],
            "p_btts_yes": pred["p_btts_yes"],
            "lambda_2h_home": pred["lambda_2h_home"],
            "lambda_2h_away": pred["lambda_2h_away"],
        },
        "candidates": full,
        "picks": headline,
        "stored": stored,
    }


def fire_halftime_picks(league: str, *,
                          odds_map: dict | None = None) -> dict:
    """Walk every match in ``league`` that's currently in halftime state
    (status='live', HT scores recorded, no FT score) and emit live picks
    for each. Pulls the league's HR odds map once and dispatches per-
    match. Designed to be called from a worker tick or an on-demand
    route — safe to re-run (uses UPSERT on (match_id, stage, bet_type)).
    """
    conn = get_conn(league)
    # Backfill any HT scores still missing on live matches before the
    # picker walks — ESPN scoreboard sometimes drops HT detail on the
    # first call and the per-game summary fills it in. Without this
    # the picker silently emits zero picks on a match that's actually
    # at HT but DB still has NULL HT scores.
    try:
        from ._ht_backfill import backfill as _ht_backfill
        _ht_backfill(league, limit=20)
    except Exception as e:
        logger.debug("[soccer:%s] HT backfill skipped: %s", league, e)
    # Live + HT scores populated + match not final yet. Final-game
    # markets stay open through stoppage time, so we keep firing
    # updates until the match flips to status='final'.
    rows = conn.execute(
        "SELECT m.id, ht.abbreviation AS home_abbr, "
        "       at.abbreviation AS away_abbr "
        "FROM matches m "
        "JOIN teams ht ON ht.id = m.home_team_id "
        "JOIN teams at ON at.id = m.away_team_id "
        "WHERE m.status IN ('live', 'halftime') "
        "  AND m.home_score_ht IS NOT NULL "
        "  AND m.away_score_ht IS NOT NULL"
    ).fetchall()
    if not rows:
        return {"league": league, "candidates": 0, "stored": 0}
    if odds_map is None:
        try:
            from ._odds import fetch_league_odds
            odds_map = fetch_league_odds(league)
        except Exception as e:
            logger.warning("[soccer:%s] live odds fetch failed: %s",
                            league, e)
            odds_map = {}
    out = {"league": league, "candidates": len(rows), "stored": 0,
            "results": []}
    for r in rows:
        key = f"{r['away_abbr']}@{r['home_abbr']}"
        match_odds = (odds_map or {}).get(key) or {}
        res = generate_live_picks_at_halftime(
            league, int(r["id"]), odds=match_odds, write_to_db=True,
        )
        out["stored"] += int(res.get("stored") or 0)
        out["results"].append({
            "match_id": int(r["id"]),
            "matchup": res.get("matchup"),
            "stored": res.get("stored") or 0,
            "headline": (res.get("picks") or [{}])[0] if res.get("picks") else None,
        })
    return out


def settle_live_picks(league: str) -> dict:
    """Grade pending halftime live picks once their match goes final.
    Reuses the prematch resolver — the picks are scoped to full-match
    markets (1X2 / OU / BTTS / DC / DNB / AH), same resolution logic.
    """
    from ._tracker import _resolve_result
    conn = get_conn(league)
    _ensure_table(conn)
    pending = conn.execute(
        "SELECT id, bet_type, pick, side, line, odds, match_id "
        "FROM live_picks_soccer WHERE result IS NULL"
    ).fetchall()
    out = {"checked": len(pending), "settled": 0,
            "wins": 0, "losses": 0, "pushes": 0}
    if not pending:
        return out
    cache: dict[int, dict] = {}
    for r in pending:
        mid = int(r["match_id"])
        if mid not in cache:
            mrow = conn.execute(
                "SELECT m.id, m.status, m.home_score, m.away_score, "
                "       m.home_score_ht, m.away_score_ht, "
                "       ht.abbreviation AS home_abbr, "
                "       at.abbreviation AS away_abbr "
                "FROM matches m "
                "JOIN teams ht ON ht.id = m.home_team_id "
                "JOIN teams at ON at.id = m.away_team_id "
                "WHERE m.id = ?", (mid,),
            ).fetchone()
            cache[mid] = dict(mrow) if mrow else {}
        match = cache[mid]
        if (match.get("status") or "") != "final":
            continue
        if match.get("home_score") is None or match.get("away_score") is None:
            continue
        verdict, profit = _resolve_result(dict(r), match)
        if verdict is None:
            continue
        conn.execute(
            "UPDATE live_picks_soccer SET result=?, profit=?, "
            "  settled_at=? WHERE id=?",
            (verdict, profit, datetime.utcnow().isoformat(), r["id"]),
        )
        out["settled"] += 1
        if   verdict == "W": out["wins"] += 1
        elif verdict == "L": out["losses"] += 1
        else:                out["pushes"] += 1
    conn.commit()
    return out


def list_live_picks(league: str, limit: int = 100) -> list[dict]:
    """Pending + settled live picks for ``league``, newest first."""
    conn = get_conn(league)
    _ensure_table(conn)
    rows = conn.execute(
        "SELECT * FROM live_picks_soccer "
        "ORDER BY created_at DESC LIMIT ?", (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]


__all__ = [
    "generate_live_picks_at_halftime",
    "fire_halftime_picks",
    "settle_live_picks",
    "list_live_picks",
]
