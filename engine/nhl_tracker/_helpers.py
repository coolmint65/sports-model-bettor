"""NHL tracker helpers + DB connection."""

from __future__ import annotations
import logging
import sqlite3
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

_local = threading.local()

# Phase 1 derivative bet types — excluded from main NHL tracker
# recording at sync time. Derivatives flow through
# engine.derivative_tracker for isolated paper-bet evaluation.
_NHL_DERIVATIVE_TYPES: set[str] = {
    "Team Total", "Period Total", "Period BTS", "Period DNB",
    "Total O/E", "Overtime", "BTS",
}


def _core_picks(picks: list[dict]) -> list[dict]:
    return [p for p in picks if p.get("type") not in _NHL_DERIVATIVE_TYPES]


def _compute_clv(bet_odds, closing_odds):
    """Compute closing line value.
    Positive CLV = got better price than closing line = sharp.
    """
    if not bet_odds or not closing_odds:
        return None
    bet_implied = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 else 100 / (bet_odds + 100)
    close_implied = abs(closing_odds) / (abs(closing_odds) + 100) if closing_odds < 0 else 100 / (closing_odds + 100)
    return round((close_implied - bet_implied) * 100, 2)


_NHL_ABBR_MAP = None


def _nhl_abbr(name):
    """Resolve a team identifier to its NHL abbreviation. Pick text is sometimes
    the abbr (nhl_picks: 'SEA') and sometimes the full display name or city
    (POTD: 'Seattle Kraken'); normalize both so the closing-odds side-match hits
    OUR side. CLV audit 2026-10-06: full-name PL picks matched the OPPONENT's
    spread -> bogus CLV (Seattle Kraken +1.5 at -185 captured VGK -1.5's +140)."""
    global _NHL_ABBR_MAP
    if not name:
        return None
    n = str(name).strip()
    if _NHL_ABBR_MAP is None:
        _NHL_ABBR_MAP = {}
        try:
            conn = _get_nhl_db()
            for r in conn.execute("SELECT name, abbreviation, city FROM nhl_teams"):
                ab = (r["abbreviation"] or "").strip()
                if not ab:
                    continue
                _NHL_ABBR_MAP[ab.upper()] = ab
                if r["name"]:
                    _NHL_ABBR_MAP[str(r["name"]).strip().lower()] = ab
                if r["city"]:
                    _NHL_ABBR_MAP[str(r["city"]).strip().lower()] = ab
        except Exception:
            _NHL_ABBR_MAP = {}
    if len(n) <= 4 and n.upper() in _NHL_ABBR_MAP:
        return _NHL_ABBR_MAP[n.upper()]
    hit = _NHL_ABBR_MAP.get(n.lower())
    if hit:
        return hit
    return n.upper() if len(n) <= 4 else None


def _extract_nhl_closing_for_pick(bet_type: str, pick_text: str,
                                  home_abbr: str, game_odds: dict) -> int | None:
    """Pick the right closing-odds field for an NHL pick out of a Hard Rock odds
    bucket (ML / PL / O/U / ALT PL). Rewritten 2026-10-06 (CLV audit): the line is
    parsed off the END of the pick text (handles 'SEA +1.5' AND 'Seattle Kraken
    +1.5'), the team is resolved to its abbr via _nhl_abbr so we match OUR side,
    and the HR field names are correct (home_spread_point / away_spread_point /
    over_under -- the old home_spread_pt / total_line never matched, silently
    falling through to alt lines)."""
    if not game_odds:
        return None
    import re as _re
    pk = pick_text or ""

    def _line_matches(a, b, tol: float = 0.001) -> bool:
        if a is None or b is None:
            return False
        try:
            return abs(float(a) - float(b)) < tol
        except (TypeError, ValueError):
            return False

    def _team_and_line(text):
        """'SEA +1.5' / 'Seattle Kraken +1.5' -> (team, line)."""
        m = _re.search(r"([+-]?\d*\.?\d+)\s*$", text or "")
        if not m:
            return (text or "").strip(), None
        tok = m.group(1)
        try:
            ln = float(tok)
        except (TypeError, ValueError):
            ln = None
        return text[:m.start()].strip(), ln

    if bet_type == "ML":
        return (game_odds.get("home_ml") if _nhl_abbr(pk) == home_abbr
                else game_odds.get("away_ml"))
    if bet_type in ("O/U", "OU"):
        is_over = "Over" in pk
        _m = _re.search(r"([+-]?\d*\.?\d+)\s*$", pk)
        bet_line = None
        if _m:
            try:
                bet_line = float(_m.group(1))
            except (TypeError, ValueError):
                bet_line = None
        primary_line = game_odds.get("over_under")
        if bet_line is None or _line_matches(primary_line, bet_line):
            return (game_odds.get("over_odds") if is_over
                    else game_odds.get("under_odds"))
        for alt in game_odds.get("alt_totals") or []:
            if _line_matches(alt.get("line"), bet_line):
                return (alt.get("over_odds") if is_over
                        else alt.get("under_odds"))
        return None
    if bet_type == "PL":
        _team, bet_line = _team_and_line(pk)
        is_home = _nhl_abbr(_team) == home_abbr
        primary_home = game_odds.get("home_spread_point")
        primary_away = game_odds.get("away_spread_point")
        if bet_line is None:
            return (game_odds.get("home_spread_odds") if is_home
                    else game_odds.get("away_spread_odds"))
        if is_home and _line_matches(primary_home, bet_line):
            return game_odds.get("home_spread_odds")
        if (not is_home) and _line_matches(primary_away, bet_line):
            return game_odds.get("away_spread_odds")
        for alt in game_odds.get("alt_spreads") or []:
            pt = alt.get("point")
            if is_home and _line_matches(pt, bet_line):
                return alt.get("home_odds")
            if (not is_home) and _line_matches(pt, -bet_line):
                return alt.get("away_odds")
        return None
    if bet_type == "ALT PL":
        _team, line = _team_and_line(pk)
        is_home = _nhl_abbr(_team) == home_abbr
        if line is not None:
            for alt in game_odds.get("alt_spreads", []) or []:
                if _line_matches(alt.get("point"), line) or _line_matches(alt.get("point"), -line):
                    return (alt.get("home_odds") if is_home
                            else alt.get("away_odds"))
        return (game_odds.get("home_spread_odds") if is_home
                else game_odds.get("away_spread_odds"))
    return None


def _get_nhl_db():
    """Get NHL picks DB connection (SQLite, separate from MLB)."""
    db_path = Path(__file__).resolve().parent.parent.parent / "data" / "nhl.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        _local.conn = conn

    conn = _local.conn

    conn.execute("""
        CREATE TABLE IF NOT EXISTS nhl_picks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT,
            date TEXT NOT NULL,
            matchup TEXT,
            bet_type TEXT NOT NULL,
            pick TEXT NOT NULL,
            model_prob REAL,
            edge REAL,
            odds INTEGER,
            closing_odds INTEGER,
            result TEXT,
            profit REAL,
            stake_units REAL,
            is_shadow INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            settled_at TEXT
        )
    """)

    # Migration: add closing_odds column to existing databases
    try:
        existing = conn.execute("PRAGMA table_info(nhl_picks)").fetchall()
        col_names = [r[1] for r in existing]
        if "closing_odds" not in col_names:
            conn.execute("ALTER TABLE nhl_picks ADD COLUMN closing_odds INTEGER")
    except Exception as e:
        # Column already exists or table just created with it.
        logger.debug("nhl_picks closing_odds migration skipped: %s", e)

    # Migration: add is_shadow column for the calibrated PAPER layer
    # (Austin 2026-09-22). is_shadow=1 rows are the model's calibrated
    # leans, tracked as a forward test — settled + CLV'd by the SAME
    # machinery (settle_picks / capture_closing_odds select WHERE
    # result IS NULL with no is_shadow filter) but NEVER counted in the
    # real P/L (get_pick_summary + the /tracker/history real rows both
    # filter COALESCE(is_shadow,0)=0). Non-destructive ADD COLUMN —
    # existing 131 real rows default to is_shadow=0 untouched.
    try:
        existing = conn.execute("PRAGMA table_info(nhl_picks)").fetchall()
        col_names = [r[1] for r in existing]
        if "is_shadow" not in col_names:
            conn.execute(
                "ALTER TABLE nhl_picks ADD COLUMN is_shadow INTEGER DEFAULT 0"
            )
    except Exception as e:
        logger.debug("nhl_picks is_shadow migration skipped: %s", e)

    # Dedup index on (date, game_id, bet_type, pick) for pending rows.
    # Mirror of the MLB/NBA dedup so record_picks can iterate every
    # pick above min_edge and INSERT OR IGNORE without piling up
    # duplicates on re-runs. Pre-fix NHL had no per-family recording
    # AT ALL — only the single highest-edge pick per game landed,
    # which is why the cards showed picks the tracker missed.
    # FULL UNIQUE — no `WHERE result IS NULL`. The partial released
    # post-settle and let the recorder spam dupes (NBA Q1 hit 25+
    # copies per game before the 2026-06-10 cross-sport hardening).
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_nhl_picks_family
            ON nhl_picks(date, game_id, bet_type, pick, is_shadow)
    """)

    # Paper-first migration (Austin 2026-09-22): real+paper coexistence needs
    # is_shadow IN the dedup keys, so a per-game PAPER best pick records even
    # when a REAL row already occupies (date,game_id,bet_type,pick). Rebuild
    # any legacy UNIQUE index that still lacks is_shadow. Non-destructive —
    # DROP/CREATE INDEX preserves all table rows. Idempotent: once rebuilt the
    # index sql contains "is_shadow" so this is a no-op on every later call.
    try:
        for _ix in ("uq_nhl_picks_family", "idx_nhl_picks_unique"):
            _row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
                (_ix,),
            ).fetchone()
            if _row and _row[0] and "is_shadow" not in _row[0]:
                conn.execute(f"DROP INDEX {_ix}")
                if _ix == "idx_nhl_picks_unique":
                    conn.execute(
                        "CREATE UNIQUE INDEX idx_nhl_picks_unique ON nhl_picks"
                        "(date, game_id, bet_type, pick, is_shadow) "
                        "WHERE result IS NULL")
                else:
                    conn.execute(
                        "CREATE UNIQUE INDEX uq_nhl_picks_family ON nhl_picks"
                        "(date, game_id, bet_type, pick, is_shadow)")
                logger.info("nhl_picks: rebuilt %s with is_shadow", _ix)
    except Exception as e:
        logger.debug("nhl_picks is_shadow index rebuild skipped: %s", e)

    conn.commit()
    return conn
