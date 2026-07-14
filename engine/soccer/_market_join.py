"""V3.1 market-data join layer.

Resolves football-data.co.uk team names → soccer DB matches by
reusing the existing ``_build_name_lookup`` resolver (the same one
that resolves HR odds names). The historical_odds DB stores the
football-data names verbatim; this module's job is to turn those into
canonical (date, home_abbr, away_abbr) keys that the predictor's
match table can join against.

Used by ``engine/gbm/market_features_soccer.py`` (downstream
extractor) and the calibration backtest. Keep this module thin — the
heavy lifting is delegated to ``_odds._resolve_team``.
"""
from __future__ import annotations

import logging
import sqlite3
from functools import lru_cache
from pathlib import Path

from ._db import get_conn
from ._odds import _build_name_lookup, _resolve_team

logger = logging.getLogger(__name__)


def _historical_db_path(league: str) -> Path:
    return (Path(__file__).resolve().parent.parent.parent
            / "data" / "soccer" / league / "historical_odds.db")


def historical_db_exists(league: str) -> bool:
    return _historical_db_path(league).exists()


@lru_cache(maxsize=32)
def _name_lookup(league: str) -> dict[str, str]:
    """Cached fuzzy-resolver dict for ``league``."""
    return _build_name_lookup(league)


def _resolve(league: str, name: str) -> str | None:
    """Football-data name → soccer DB abbreviation, via shared resolver."""
    if not name:
        return None
    return _resolve_team(name, _name_lookup(league))


def match_rate(league: str) -> dict:
    """Audit how many historical_odds rows resolve to a real match in
    the soccer DB. Returns counts + sample misses for debugging.
    """
    if not historical_db_exists(league):
        return {"league": league, "error": "no historical_odds.db"}

    h = sqlite3.connect(str(_historical_db_path(league)))
    h.row_factory = sqlite3.Row
    rows = h.execute(
        "SELECT match_date, home_name, away_name, psch, pscd, psca "
        "FROM historical_odds WHERE psch IS NOT NULL"
    ).fetchall()
    h.close()

    soc = get_conn(league)
    resolved = 0
    matched = 0
    sample_misses: list[dict] = []
    sample_unresolved: list[dict] = []
    for r in rows:
        h_abbr = _resolve(league, r["home_name"])
        a_abbr = _resolve(league, r["away_name"])
        if not (h_abbr and a_abbr):
            if len(sample_unresolved) < 5:
                sample_unresolved.append(dict(r))
            continue
        resolved += 1
        # football-data uses UTC match dates; our matches.date is ET.
        # A 22:00 ET kickoff stamps as UTC+1 (next day) in football-
        # data, so widen to ±1 day to catch the timezone drift. Same
        # orientation only — flipped home/away would be a different
        # matchup, not a tz artifact.
        q = soc.execute(
            "SELECT m.id FROM matches m "
            "JOIN teams ht ON ht.id = m.home_team_id "
            "JOIN teams at ON at.id = m.away_team_id "
            "WHERE m.date BETWEEN date(?, '-1 day') AND date(?, '+1 day') "
            "  AND ht.abbreviation = ? AND at.abbreviation = ? LIMIT 1",
            (r["match_date"], r["match_date"], h_abbr, a_abbr),
        ).fetchone()
        if q:
            matched += 1
        elif len(sample_misses) < 5:
            sample_misses.append({**dict(r), "h_abbr": h_abbr,
                                   "a_abbr": a_abbr})

    return {
        "league":     league,
        "total":      len(rows),
        "resolved":   resolved,
        "matched":    matched,
        "resolve_pct": round(100.0 * resolved / len(rows), 1) if rows else 0,
        "match_pct":  round(100.0 * matched / len(rows), 1) if rows else 0,
        "sample_unresolved": sample_unresolved,
        "sample_misses":     sample_misses,
    }


def market_features_for_match(league: str, *, match_date: str,
                                home_abbr: str, away_abbr: str
                                ) -> dict | None:
    """Return the closing-line market features for one match, or None
    when the historical_odds table has nothing for it.

    Output shape matches ``engine.gbm.market_features_nba`` so the
    downstream V3.1 blend logic is sport-uniform.
    """
    if not historical_db_exists(league):
        return None
    h = sqlite3.connect(str(_historical_db_path(league)))
    h.row_factory = sqlite3.Row
    # Widen by ±1 day to absorb the UTC↔ET drift on late-evening
    # kickoffs (football-data uses match-day UTC; our matches table
    # stores ET dates).
    rows = h.execute(
        "SELECT home_name, away_name, psch, pscd, psca, "
        "       pc_over25, pc_under25, pcahh, pcaha "
        "FROM historical_odds "
        "WHERE match_date BETWEEN date(?, '-1 day') AND date(?, '+1 day')",
        (match_date, match_date),
    ).fetchall()
    h.close()
    for r in rows:
        if (_resolve(league, r["home_name"]) == home_abbr
                and _resolve(league, r["away_name"]) == away_abbr):
            return _to_features(r)
    return None


def _to_features(r: sqlite3.Row) -> dict:
    """Convert raw decimal odds into normalized model-feature shape."""
    psch = r["psch"]; pscd = r["pscd"]; psca = r["psca"]
    if not (psch and pscd and psca):
        return {
            "has_market_data":    0,
            "market_home_implied": None,
            "market_draw_implied": None,
            "market_away_implied": None,
            "market_total_line":   r["pc_over25"] and 2.5,
            "market_ah_line":      r["pcahh"] is not None and 0.0,
        }
    # Vig-stripped implied probabilities. Divide by overround so the
    # three legs sum to 1.0 instead of ~1.05 (typical book hold).
    raw = (1.0 / psch, 1.0 / pscd, 1.0 / psca)
    total = sum(raw)
    return {
        "has_market_data":     1,
        "market_home_implied": round(raw[0] / total, 4),
        "market_draw_implied": round(raw[1] / total, 4),
        "market_away_implied": round(raw[2] / total, 4),
        # Totals + AH for downstream OU + spread blends. Pinnacle's
        # over/under feed is keyed to the 2.5 main line — the line
        # field is implicit (always 2.5 when both PC>2.5 and PC<2.5
        # are present).
        "market_total_line":   2.5 if r["pc_over25"] else None,
        "market_total_over":   r["pc_over25"],
        "market_total_under":  r["pc_under25"],
        "market_ah_home":      r["pcahh"],
        "market_ah_away":      r["pcaha"],
    }


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="engine.soccer._market_join")
    ap.add_argument("--league", required=True)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    import json
    res = match_rate(args.league)
    print(json.dumps(res, indent=2, default=str))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
