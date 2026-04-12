"""
Shared helpers for tracker analysis tools (calibration / edge / condition).

Each sport stores picks in its own SQLite DB with a slightly different table
name but the same canonical columns:
    bet_type, pick, model_prob, edge, odds, result, profit, matchup

Result formats differ per sport (historically "W"/"L"/"P", but older picks
may use "win"/"loss"/"push" or other variants), so we canonicalize in Python
rather than relying on SQL equality.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

# Sport -> (db relative path, picks table name, display label)
SPORTS: dict[str, tuple[str, str, str]] = {
    "mlb": ("data/mlb.db", "picks", "MLB"),
    "nhl": ("data/nhl.db", "nhl_picks", "NHL"),
    "nba": ("data/nba.db", "nba_picks", "NBA"),
}

ROOT = Path(__file__).resolve().parent.parent


def canon_result(v) -> str:
    """Canonicalize a result value to 'win' / 'loss' / 'push' / ''."""
    if v is None:
        return ""
    s = str(v).strip().lower()
    if s in ("win", "w", "hit", "1", "true", "yes"):
        return "win"
    if s in ("loss", "lose", "l", "miss", "0", "false", "no"):
        return "loss"
    if s in ("push", "p", "tie", "draw"):
        return "push"
    return ""


# Map historical lowercase bet_type values to the canonical uppercase form
# used by engine/picks.py today. This is why the diagnostic was showing
# "rl" and "RL" as two separate buckets — older tracker writes used lower
# case. Always canonicalize on read so the aggregations are correct.
_BET_TYPE_MAP = {
    "ml": "ML", "ML": "ML",
    "rl": "RL", "RL": "RL",
    "ou": "O/U", "o/u": "O/U", "O/U": "O/U",
    "nrfi": "1st INN", "NRFI": "1st INN", "1st INN": "1st INN", "1st inn": "1st INN",
    "yrfi": "1st INN", "YRFI": "1st INN",
    "pl": "PL", "PL": "PL",
    "q1_spread": "Q1_SPREAD", "Q1_SPREAD": "Q1_SPREAD",
    "q1_total": "Q1_TOTAL", "Q1_TOTAL": "Q1_TOTAL",
    "q1_ml": "Q1_ML", "Q1_ML": "Q1_ML",
}


def canon_bet_type(v) -> str:
    """Canonicalize bet_type: merges 'rl'/'RL', 'ml'/'ML', 'nrfi'/'1st INN', etc."""
    if v is None:
        return ""
    s = str(v).strip()
    return _BET_TYPE_MAP.get(s, _BET_TYPE_MAP.get(s.lower(), s))


def db_path(sport: str) -> Path:
    return ROOT / SPORTS[sport][0]


def table_name(sport: str) -> str:
    return SPORTS[sport][1]


def label(sport: str) -> str:
    return SPORTS[sport][2]


def open_conn(sport: str) -> sqlite3.Connection | None:
    """Open a read-only connection; return None if DB missing."""
    path = db_path(sport)
    if not path.exists():
        return None
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error:
        return False
    return any(r[1] == col for r in rows)


def fetch_settled(conn: sqlite3.Connection, sport: str) -> list[dict]:
    """Return all settled picks (result IS NOT NULL) as list of dicts.

    Always includes canonical columns; `closing_odds` is included when the
    column exists on that sport's table, otherwise it is set to None.
    """
    tbl = table_name(sport)
    cols = "bet_type, pick, model_prob, edge, odds, result, profit, matchup"
    if has_column(conn, tbl, "closing_odds"):
        cols += ", closing_odds"
    rows = conn.execute(
        f"SELECT {cols} FROM {tbl} WHERE result IS NOT NULL"
    ).fetchall()
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        if "closing_odds" not in d:
            d["closing_odds"] = None
        out.append(d)
    return out


def resolve_sports(arg: str | None) -> list[str]:
    """'all' / None -> all three sports, else just the named one."""
    if not arg or arg.lower() == "all":
        return ["mlb", "nhl", "nba"]
    s = arg.lower()
    if s not in SPORTS:
        raise SystemExit(f"Unknown sport: {arg!r}. Use one of: mlb, nhl, nba, all.")
    return [s]


def american_to_profit(odds: int | float | None) -> float:
    """Profit on a $100 stake given American odds (win case)."""
    if not odds:
        return 0.0
    o = float(odds)
    return o if o > 0 else 100.0 / abs(o) * 100.0


def pick_profit(row: dict) -> float:
    """Return the profit contribution of a settled pick on a $100 base.

    Prefer the stored `profit` column when present; otherwise derive from
    the canonical result + American odds (win = american_to_profit, loss = -100,
    push = 0).
    """
    p = row.get("profit")
    if p is not None:
        try:
            return float(p)
        except (TypeError, ValueError):
            pass
    c = canon_result(row.get("result"))
    if c == "win":
        return american_to_profit(row.get("odds"))
    if c == "loss":
        return -100.0
    return 0.0


def home_away_from_matchup(matchup: str | None) -> tuple[str, str] | None:
    """Split 'AWAY @ HOME' into (away, home) tokens, else None."""
    if not matchup:
        return None
    parts = matchup.split(" @ ")
    if len(parts) != 2:
        return None
    return parts[0].strip(), parts[1].strip()


def first_token(s: str | None) -> str:
    if not s:
        return ""
    toks = s.strip().split()
    return toks[0] if toks else ""


def pick_side(row: dict) -> str | None:
    """Classify a pick as 'home' or 'away' when determinable.

    Uses the matchup 'AWAY @ HOME' format plus the pick's first token
    (which is a team abbreviation for ML, PL/RL/spread, and Q1_ML/SPREAD).
    Totals (O/U, Q1_TOTAL) return None.
    """
    bt = (row.get("bet_type") or "").upper()
    if bt in ("O/U", "OU", "Q1_TOTAL", "1ST INN", "NRFI"):
        return None
    pair = home_away_from_matchup(row.get("matchup"))
    if not pair:
        return None
    away, home = pair
    pick = (row.get("pick") or "").strip()
    if not pick:
        return None
    # Exact-ML picks may be just an abbreviation; spread/PL picks put abbr first.
    tok = pick.split()[0] if " " in pick else pick
    if tok == home:
        return "home"
    if tok == away:
        return "away"
    # NRFI/YRFI fall through; defensive fallback.
    return None


def pick_role(row: dict) -> str | None:
    """Favorite (odds < 0) vs underdog (odds > 0). Totals excluded by sign only.

    Applied to any bet type since the odds always reflect the priced side.
    """
    odds = row.get("odds")
    if odds is None:
        return None
    try:
        o = int(odds)
    except (TypeError, ValueError):
        return None
    if o < 0:
        return "favorite"
    if o > 0:
        return "underdog"
    return None  # pick'em / 0
