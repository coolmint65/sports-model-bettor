"""NHL props CARD GATE (MintyBot 2026-11-01, Austin "work on calibration" after the card went 0-6).

The card rule (max-edge main line per game, <=1/game, <=3/date, 40% floor, 4% edge) keeps running,
but a pick only reaches the REAL card (player_props_picks -- Austin bets these on Hard Rock) when its
cell (bet_type x side) has PROVEN itself. Every other pick the rule makes is written to the shadow
table ``prop_card_shadow`` (same caps, same lock-once semantics, graded from the box scores) so the
evidence keeps accumulating without real money on it.

Why (HR eval log 10-26..11-01, 2,595 graded main lines; walk-forward replay of the card rule, 18/22
real picks reproduced):
  * the card claimed ~62% and hit ~48% (sim 10-11, real since 10-27 8-9); H1 +45%, H2 -80% (n=12/9)
  * no variant -- refit weights, Platt / isotonic per stat, edge floors 6-12, half model weight,
    SOG-Under-only -- was positive in both halves on the CARD; every capped variant did worse than
    its uncapped pool (winner's curse on the max-edge pick)
  * SOG Unders won in every model-gap bucket, including lines where the model leaned OVER: the SOG
    profit was a market-wide Under week, not model selection
  * the POTD bar (n>=20, win% - BE > 0.5 SE) admitted SOG Under walk-forward and lost (-25%), so the
    card bar is stricter.

PROVEN = n >= MIN_N graded W/L rule picks (real card since SINCE + shadow) AND flat-1u ROI at the
logged odds > SE_MULT x its standard error. Fail-closed: any error -> not proven -> no real pick.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

MIN_N = 30
SE_MULT = 1.0
SINCE = "2026-10-27"          # = engine.player_props_potd.MODEL_SINCE["nhl"] (current model)
RULE = (f"card shows a pick only from a proven stat x side: n>={MIN_N} graded rule picks since "
        f"{SINCE} (real + shadow) and ROI > {SE_MULT:g} SE")


def _conn():
    from ...player_props_db import _conn_for
    return _conn_for("nhl")


def _ensure(c) -> None:
    c.execute("""CREATE TABLE IF NOT EXISTS prop_card_shadow(
        id INTEGER PRIMARY KEY AUTOINCREMENT, game_id TEXT NOT NULL, date TEXT NOT NULL,
        matchup TEXT, player_id INTEGER NOT NULL, player_name TEXT, bet_type TEXT NOT NULL,
        side TEXT, line REAL, odds INTEGER, model_prob REAL, edge REAL, created_at TEXT,
        actual_value REAL, result TEXT, graded_at TEXT,
        UNIQUE(date, player_id, bet_type))""")


def _dec(odds) -> float:
    o = float(odds)
    return 1.0 + (o / 100.0 if o > 0 else 100.0 / -o)


def shadow_counts(day: str) -> dict:
    """{game_id: n} shadow picks already locked for ``day`` (they count against the card caps)."""
    c = _conn()
    _ensure(c)
    return {str(r[0]): int(r[1]) for r in c.execute(
        "SELECT game_id, COUNT(*) FROM prop_card_shadow WHERE date = ? GROUP BY game_id", (day,))}


def record_shadow(game_id, date, matchup, player_id, player_name, bet_type, best) -> bool:
    c = _conn()
    _ensure(c)
    cur = c.execute(
        "INSERT OR IGNORE INTO prop_card_shadow(game_id, date, matchup, player_id, player_name, bet_type, "
        "side, line, odds, model_prob, edge, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (str(game_id), date, matchup, int(player_id), player_name, bet_type, best["side"],
         float(best["line"]), int(best["odds"]), float(best["model_prob"]), float(best["edge"]),
         datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")))
    c.commit()
    return cur.rowcount > 0


def grade_shadow() -> int:
    """Grade shadow picks whose game is final (same rules as the card tracker; DNP -> 'P')."""
    from ...player_props_tracker import (_NHL_STAT_KEY, _lookup_actual, _game_is_final,
                                         _determine_outcome)
    c = _conn()
    _ensure(c)
    rows = c.execute("SELECT s.id, s.game_id, s.player_id, s.bet_type, s.side, s.line, g.start_time "
                     "FROM prop_card_shadow s LEFT JOIN nhl_games g ON CAST(g.game_id AS TEXT) = s.game_id "
                     "WHERE s.result IS NULL").fetchall()
    n, now = 0, datetime.now(timezone.utc)
    for r in rows:
        stat = _NHL_STAT_KEY.get(r["bet_type"])
        if not stat or not _game_is_final("nhl", r["game_id"]):
            continue
        actual = _lookup_actual("nhl", int(r["player_id"]), r["game_id"], stat)
        if actual is None:
            try:
                st = datetime.fromisoformat(str(r["start_time"]).replace("Z", "+00:00"))
            except Exception:
                continue
            if now - st < timedelta(hours=12):
                continue                       # box score may not be ingested yet
            res = "P"
        else:
            res = _determine_outcome(r["line"], r["side"], actual)
            if res is None:
                continue
        c.execute("UPDATE prop_card_shadow SET actual_value = ?, result = ?, graded_at = ? WHERE id = ?",
                  (actual, res, now.strftime("%Y-%m-%d %H:%M:%S"), r["id"]))
        n += 1
    if n:
        c.commit()
    return n


def cell_records() -> dict:
    """{(bet_type, side): record} over the rule's graded picks since SINCE: the real card picks
    (the rule ran ungated until 2026-11-01) + shadow picks. W/L only; pushes / DNPs ignored."""
    c = _conn()
    _ensure(c)
    rows = [("real",) + tuple(r) for r in c.execute(
        "SELECT bet_type, side, odds, result FROM player_props_picks WHERE date >= ? "
        "AND result IN ('W','L')", (SINCE,))]
    rows += [("shadow",) + tuple(r) for r in c.execute(
        "SELECT bet_type, side, odds, result FROM prop_card_shadow WHERE date >= ? "
        "AND result IN ('W','L')", (SINCE,))]
    by: dict = {}
    for src, bt, side, odds, res in rows:
        if odds is None or not side:
            continue
        by.setdefault((bt, str(side).title()), []).append((src, _dec(odds) - 1.0 if res == "W" else -1.0,
                                                          1.0 / _dec(odds), res == "W"))
    out = {}
    for k, v in by.items():
        n = len(v)
        pl = [x[1] for x in v]
        m = sum(pl) / n
        se = math.sqrt(sum((x - m) ** 2 for x in pl) / (n - 1) / n) if n > 1 else float("inf")
        w = sum(1 for x in v if x[3])
        out[k] = {"cell": f"{k[0]} {k[1]}", "n": n, "wins": w, "losses": n - w,
                  "real": sum(1 for x in v if x[0] == "real"), "shadow": sum(1 for x in v if x[0] == "shadow"),
                  "win_pct": round(100.0 * w / n, 1), "be_pct": round(100.0 * sum(x[2] for x in v) / n, 1),
                  "roi_pct": round(100.0 * m, 1), "se_pct": round(100.0 * se, 1) if n > 1 else None,
                  "proven": bool(n >= MIN_N and m > SE_MULT * se)}
    return out


def is_proven(bet_type: str, side: str, recs: dict | None = None) -> bool:
    try:
        recs = cell_records() if recs is None else recs
        return bool((recs.get((bet_type, str(side).title())) or {}).get("proven"))
    except Exception as e:
        logger.warning("nhl card gate: record check failed (fail-closed): %s", e)
        return False


def summary() -> dict:
    """For /api/nhl/props-tracker/summary: the rule, every cell's record and the shadow slate."""
    c = _conn()
    _ensure(c)
    recs = cell_records()
    pend = [dict(r) for r in c.execute(
        "SELECT date, matchup, player_name, bet_type, side, line, odds, model_prob, edge "
        "FROM prop_card_shadow WHERE result IS NULL ORDER BY date, id")]
    return {"rule": RULE, "min_n": MIN_N, "se_mult": SE_MULT, "since": SINCE,
            "proven_cells": [r["cell"] for r in recs.values() if r["proven"]],
            "cells": sorted(recs.values(), key=lambda r: -r["n"]),
            "shadow_pending": len(pend), "shadow_pending_picks": pend}


__all__ = ["MIN_N", "SE_MULT", "SINCE", "RULE", "shadow_counts", "record_shadow", "grade_shadow",
           "cell_records", "is_proven", "summary"]
