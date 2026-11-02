"""POTD read APIs — never write to the DB.

Used by the dashboard to render the POTD card and the running summary.
get_today_potd is also called from get_or_create_potd inside _select to
return the canonical DB row shape after locking — that's why this lives
in its own module (avoids a circular import with _select).
"""

from __future__ import annotations
from datetime import datetime

from ._storage import _get_conn, _ensure_potd_table, _potd_table
from .._tz import et_today_str

# Mirrors engine.pick_of_day._refresh._SPORT_GAMES_TABLE. Duplicated
# (not imported) deliberately: _read.py's own docstring explains it
# exists specifically to avoid a circular import with _select (which
# _refresh.py pulls in), so this stays a tiny, dependency-free
# constant instead of reaching into _refresh.
_SPORT_GAMES_TABLE = {
    "mlb": ("games", "mlb_game_id"),
    "nhl": ("nhl_games", "game_id"),
    "nba": ("nba_games", "game_id"),
    "nfl": ("games", "game_id"),
    "cfb": ("games", "game_id"),
}
_LOCKED_STATUSES = {"live", "final", "postponed", "suspended",
                    "in", "post", "completed", "closed", "in_progress"}


def _game_confirmed_locked(conn, sport: str, game_id) -> bool:
    """True only when we can POSITIVELY confirm `game_id`'s game has
    gone live/final/postponed -- i.e. a captured closing_odds can
    actually BE the close (no more pre-game writes are coming). False
    for a confirmed-still-scheduled game AND for a game_id we can't
    resolve at all (NHL/MLB key their games table off a different
    native id than the ESPN id POTD stores -- same gap as the
    freeze-on-live fix in _refresh.py). Fails closed: an unresolvable
    status is treated as "not a confirmed close", matching the
    "close pending" chip rather than claiming a close we haven't
    verified."""
    if not game_id:
        return False
    spec = _SPORT_GAMES_TABLE.get(sport)
    if not spec:
        return False
    table, col = spec
    try:
        row = conn.execute(
            f"SELECT status FROM {table} WHERE {col} = ?", (game_id,)
        ).fetchone()
    except Exception:
        return False
    if not row:
        return False
    return (row["status"] or "").lower() in _LOCKED_STATUSES


def get_potd_summary(sport: str, limit: int = 30, scope: str | None = None) -> dict:
    """Return running POTD totals + recent history for the dashboard."""
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    # Optional scope filter (Austin 2026-09-19): 'q1' -> Q1_* bet types,
    # 'full' -> everything else, so full-game vs Q1 POTDs report their OWN
    # record instead of the combined tally. None = all.
    _where = ("WHERE bet_type LIKE 'Q1%'" if scope == "q1"
              else "WHERE bet_type NOT LIKE 'Q1%'" if scope == "full" else "")

    # Stake-weighted profit + ROI (mirrors per-sport tracker math).
    # Sport-framework POTD tables that pre-date the stake_units column
    # silently fall through the COALESCE to 1.0u so legacy 1u-basis
    # totals stay consistent.
    has_stake = bool(
        conn.execute(
            "SELECT 1 FROM pragma_table_info('pick_of_day') "
            " WHERE name = 'stake_units'"
        ).fetchone()
    )
    if has_stake:
        overall = conn.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
                COALESCE(SUM(profit * COALESCE(stake_units, 1.0)), 0) as profit,
                COALESCE(SUM(CASE WHEN result IN ('W','L')
                                    THEN COALESCE(stake_units, 1.0)
                                    ELSE 0 END), 0) as stake_units_settled
            FROM pick_of_day {_where}
        """).fetchone()
    else:
        overall = conn.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
                COALESCE(SUM(profit), 0) as profit,
                COALESCE(SUM(CASE WHEN result IN ('W','L') THEN 1.0
                                    ELSE 0 END), 0) as stake_units_settled
            FROM pick_of_day {_where}
        """).fetchone()

    overall = dict(overall)
    w = overall.get("wins") or 0
    l = overall.get("losses") or 0
    # ROI denominator is stake-units of settled picks (W+L). Pushes
    # don't move ROI either direction so they're excluded from both
    # sides of the ratio.
    settled_total = w + l
    staked_u = overall.get("stake_units_settled") or 0

    recent = conn.execute(
        f"SELECT * FROM pick_of_day {_where} ORDER BY date DESC LIMIT ?", (limit,)
    ).fetchall()
    # Project stake-weighted profit onto each recent row.
    recent_out = []
    for r in recent:
        d = dict(r)
        if has_stake and d.get("profit") is not None:
            stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
            d["profit"] = round(d["profit"] * stake_u, 2)
        recent_out.append(d)

    # Previous settled POTD result (Austin 2026-09-19): most recent W/L in
    # history (skips today's still-pending pick) — each POTD hero shows it.
    last_result = next(
        (d["result"] for d in recent_out if d.get("result") in ("W", "L")), None)

    return {
        "total": overall.get("total") or 0,
        "wins": w,
        "losses": l,
        "pushes": overall.get("pushes") or 0,
        "pending": overall.get("pending") or 0,
        "profit": round(overall.get("profit") or 0, 2),
        "win_pct": round(w / settled_total * 100, 1) if settled_total > 0 else 0,
        "roi": round((overall.get("profit") or 0) / staked_u, 1) if staked_u > 0 else 0,
        "recent": recent_out,
        "last_result": last_result,
    }


def get_today_potd(sport: str, date: str | None = None,
                   view: str = "q1",
                   tour: str | None = None) -> dict | None:
    """Fetch just today's POTD (doesn't create one).

    Annotates the response with a computed `clv` field when both odds
    and closing_odds are present, so the UI doesn't have to redo the
    arithmetic. Positive CLV = we got a better price than the close.

    ``view`` only matters for NBA. 'q1' reads pick_of_day, 'full' reads
    pick_of_day_full.

    ``tour`` only matters for tennis. 'atp' or 'wta' selects the
    per-tour POTD; ATP and WTA can coexist for the same date.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    target_date = date or et_today_str()
    table = _potd_table(sport, view)
    if sport == "tennis" and tour:
        row = conn.execute(
            f"SELECT * FROM {table} WHERE date = ? AND tour = ?",
            (target_date, tour),
        ).fetchone()
    else:
        row = conn.execute(
            f"SELECT * FROM {table} WHERE date = ?", (target_date,)
        ).fetchone()
    if not row:
        return None
    out = dict(row)
    bet_odds = out.get("odds")
    close = out.get("closing_odds")
    if bet_odds and close:
        # "Real close" gate (2026-10-10): closing_odds being non-null
        # isn't proof it's actually the close -- it could be a pre-game
        # snapshot the line will still move past (today's price, not
        # yet frozen) or, for an older row, a known-contaminated
        # in-play capture from before the freeze-on-live fix landed
        # (_refresh.py). Only compute/show clv once the game itself has
        # confirmably gone live/final/postponed; otherwise strip
        # closing_odds from the response entirely so CLVBadge (which
        # keys off closing_odds, not clv) falls back to its existing
        # "close pending" chip instead of a premature/wrong number.
        if _game_confirmed_locked(conn, sport, out.get("game_id")):
            bet_imp = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 \
                      else 100 / (bet_odds + 100)
            close_imp = abs(close) / (abs(close) + 100) if close < 0 \
                        else 100 / (close + 100)
            out["clv"] = round((close_imp - bet_imp) * 100, 2)
        else:
            out["closing_odds"] = None
    return out
