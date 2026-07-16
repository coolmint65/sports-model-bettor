"""Auto-eligible bet queue.

Walks today's slate across every sport, matches each pick against the
historical (sport, bet_type, direction) cell performance, and returns
the subset that clears the eligibility bar. This is the "recommendation
queue" backing the future one-tap-to-HR frontend view.

Eligibility rule (defaults; tunable per call):
  * Cell has n >= MIN_SETTLED settled picks in the stake-weighted history
  * Cell's stake-weighted ROI >= MIN_ROI (default +5%)
  * Pick's own stake_units > 0 (excludes bleed-cell shadows)

Bankroll cap:
  * Total staked units per day capped at MAX_DAILY_UNITS. The queue
    trims low-priority picks (lowest cell ROI first) when the total
    would exceed the cap.

Pre-match only — live picks are excluded by design. Every sport keeps
its live/in-progress picks in a dedicated ``live_picks_<sport>`` table
(see engine/live_tracker/_schema.py, engine/soccer/_live_picks.py).
The queue's ``_SOURCES`` map is a strict whitelist of PRE-MATCH picks
tables — the ``_is_prematch_table`` guard below refuses anything with a
``live`` prefix so a future accidental entry can't leak in-progress
picks into a manual-placement recommendation surface.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)

# Same lookup edge_floors uses.
_SOURCES: dict[str, tuple] = {
    "mlb":   ("engine.db",     "picks"),
    "nhl":   ("engine.nhl_db", "nhl_picks"),
    "nba":   ("engine.nba_db", "nba_picks"),
    "tennis": ("engine.tennis_db", "tennis_picks"),
    "afl":       ("engine.basketball._db", "picks", "afl"),
    "wnba":      ("engine.basketball._db", "picks", "wnba"),
    "ncaam":     ("engine.basketball._db", "picks", "ncaam"),
    "ncaaw":     ("engine.basketball._db", "picks", "ncaaw"),
    "euroleague": ("engine.basketball._db", "picks", "euroleague"),
    "nba_summer_league": ("engine.basketball._db", "picks",
                            "nba_summer_league"),
    "fifa_world_cup":    ("engine.soccer._db", "picks", "fifa_world_cup"),
    "eng_premier":       ("engine.soccer._db", "picks", "eng_premier"),
    "esp_laliga":        ("engine.soccer._db", "picks", "esp_laliga"),
    "ita_seriea":        ("engine.soccer._db", "picks", "ita_seriea"),
    "ger_bundesliga":    ("engine.soccer._db", "picks", "ger_bundesliga"),
    "fra_ligue1":        ("engine.soccer._db", "picks", "fra_ligue1"),
    "mls":               ("engine.soccer._db", "picks", "mls"),
    "bra_seriea":        ("engine.soccer._db", "picks", "bra_seriea"),
    "arg_lpf":           ("engine.soccer._db", "picks", "arg_lpf"),
    "conmebol_libertadores": ("engine.soccer._db", "picks",
                                "conmebol_libertadores"),
    "uefa_champions":    ("engine.soccer._db", "picks", "uefa_champions"),
    "uefa_europa":       ("engine.soccer._db", "picks", "uefa_europa"),
    "uefa_conference":   ("engine.soccer._db", "picks", "uefa_conference"),
    "us_nwsl":           ("engine.soccer._db", "picks", "us_nwsl"),
    "usl_championship":  ("engine.soccer._db", "picks", "usl_championship"),
    "us_open_cup":       ("engine.soccer._db", "picks", "us_open_cup"),
    "f1":                ("engine.motorsports._db", "picks", "f1"),
    "nascar":            ("engine.motorsports._db", "picks", "nascar"),
    "indycar":           ("engine.motorsports._db", "picks", "indycar"),
}


def _is_prematch_table(table_name: str) -> bool:
    """Refuse any table whose name flags it as a live/in-progress
    picks store. Defensive guard so a future contributor can't wire
    ``live_picks_nba`` into ``_SOURCES`` without noticing — every
    write path in the codebase names its live table with a ``live``
    prefix (``live_picks_nba``, ``live_picks_nhl``, ``live_picks_soccer``,
    ``live_picks_wnba``, ...), so a substring check catches all of them.
    """
    if not table_name:
        return False
    return "live" not in table_name.lower()


# Sanity check at import time — future me will thank present me.
for _sport, _src in _SOURCES.items():
    if not _is_prematch_table(_src[1]):
        raise RuntimeError(
            f"bet_queue: _SOURCES[{_sport!r}] points at a live table "
            f"{_src[1]!r}. The queue is pre-match only."
        )


# Tuneable defaults. All conservative — the queue's whole reason for
# existing is to only surface picks with earned historical evidence.
# Daily units MUST stay in sync with
# engine.queue_placer._config.MAX_STAKED_PER_DAY_DOLLARS so the picker
# and the auto-placer agree on the daily envelope. Current envelope:
# 10u/day = $10 at $1/u.
MIN_SETTLED = 50
MIN_ROI = 5.0
MAX_DAILY_UNITS = 10.0
# Unit size in dollars — display-only; doesn't affect selection logic.
DEFAULT_UNIT_DOLLARS = 1.0


def _direction_label(bet_type: str, pick: str | None) -> str:
    if not pick:
        return ""
    pk = pick.strip().lower()
    bt = (bet_type or "").strip().lower()
    if pk.startswith("over"):
        return "over"
    if pk.startswith("under"):
        return "under"
    if bt in ("1st inn", "1stinn", "nrfi"):
        if "nrfi" in pk:
            return "nrfi"
        if "yrfi" in pk:
            return "yrfi"
    if bt in ("rl", "pl", "spread", "alt rl", "alt pl", "alt spread"):
        import re
        m = re.search(r"([+-])\d", pk)
        if m:
            return "fav" if m.group(1) == "-" else "dog"
    return ""


def _load_cell_stats(sport: str) -> dict[tuple, dict]:
    """Historical (bet_type, direction) → {n, staked, profit, roi, wr}
    for one sport, stake-weighted."""
    src = _SOURCES.get(sport)
    if not src:
        return {}
    import importlib
    try:
        mod = importlib.import_module(src[0])
        conn = (mod.get_conn(src[2]) if len(src) >= 3 else mod.get_conn())
    except Exception as e:
        logger.debug("bet_queue: cannot open %s: %s", src[0], e)
        return {}
    tables = [src[1]]
    # Prop picks live in their own table on some sports — read both.
    tables.append("player_props_picks")
    cells: dict[tuple, dict] = defaultdict(
        lambda: {"n": 0, "wins": 0, "losses": 0,
                  "staked": 0.0, "profit": 0.0})
    for tbl in tables:
        try:
            rows = conn.execute(
                f"SELECT bet_type, pick, stake_units, result, profit "
                f"FROM {tbl} "
                f"WHERE result IN ('W','L','P') AND stake_units > 0"
            ).fetchall()
        except Exception:
            continue
        for r in rows:
            d = dict(r)
            bt = (d.get("bet_type") or "").strip()
            if not bt:
                continue
            direction = _direction_label(bt, d.get("pick"))
            stake = float(d.get("stake_units") or 0.0)
            if stake <= 0:
                continue
            key = (bt, direction)
            c = cells[key]
            c["n"] += 1
            c["staked"] += stake
            c["profit"] += float(d.get("profit") or 0.0) * stake
            if d.get("result") == "W":
                c["wins"] += 1
            elif d.get("result") == "L":
                c["losses"] += 1
    # Finalize derived fields.
    for key, c in cells.items():
        c["roi"] = ((c["profit"] / (c["staked"] * 100.0)) * 100.0
                     if c["staked"] > 0 else 0.0)
        decided = c["wins"] + c["losses"]
        c["wr"] = (c["wins"] / decided * 100.0) if decided else 0.0
    return dict(cells)


def _load_all_stats() -> dict[str, dict[tuple, dict]]:
    return {sport: _load_cell_stats(sport) for sport in _SOURCES}


def _cell_qualifies(cell: dict, min_n: int, min_roi: float) -> bool:
    return cell.get("n", 0) >= min_n and cell.get("roi", 0.0) >= min_roi


# Extra columns worth pulling per sport for display/context. Tennis
# uses `tour` (atp/wta) so the card can render "WTA Tennis" instead
# of the generic "Tennis" that flattens the two tours.
_EXTRA_COLS: dict[str, tuple[str, ...]] = {
    "tennis": ("tour",),
}


# Per-sport id column on the picks table. The queue joins to each
# sport's own scheduled endpoint on the frontend using this — that's
# how the queue reuses the same MatchCard / GameCard the sport's own
# panel renders instead of maintaining a parallel card in the queue.
_EVENT_ID_COL: dict[str, str] = {
    "mlb": "game_id", "nhl": "game_id", "nba": "game_id",
    "tennis": "match_id",
    "afl": "game_id", "wnba": "game_id", "ncaam": "game_id",
    "ncaaw": "game_id", "euroleague": "game_id",
    "nba_summer_league": "game_id",
    "fifa_world_cup": "match_id",
    "eng_premier": "match_id", "esp_laliga": "match_id",
    "ita_seriea": "match_id", "ger_bundesliga": "match_id",
    "fra_ligue1": "match_id", "mls": "match_id",
    "bra_seriea": "match_id", "arg_lpf": "match_id",
    "conmebol_libertadores": "match_id", "uefa_champions": "match_id",
    "uefa_europa": "match_id", "uefa_conference": "match_id",
    "us_nwsl": "match_id", "usl_championship": "match_id",
    "us_open_cup": "match_id",
    "f1": "race_id", "nascar": "race_id", "indycar": "race_id",
}


def _display_label(sport: str, extra: dict) -> str:
    """Human-facing sport / league label. Adds tour disambiguation for
    tennis (ATP vs WTA) and uses SPORT_LABELS-friendly names for
    everything else."""
    if sport == "tennis":
        tour = (extra.get("tour") or "").upper()
        return f"{tour} Tennis" if tour in ("ATP", "WTA") else "Tennis"
    return sport


# Sports whose "matchup" splits on " vs " (players) vs " @ " (teams).
_VS_SPORTS = {"tennis", "f1", "nascar", "indycar"}


def _card_context(sport: str, extra: dict, matchup: str) -> dict:
    """Sport-specific enrichment for the queue's card renderer. Returns
    the fields the frontend's per-sport PickCard needs to look like the
    matching sport panel — player photos for tennis, team-abbr strings
    for team sports (frontend resolves logos via resolveTeamLogo), etc.

    Matchup shape:
      * team sports (mlb/nhl/nba/basketball framework/soccer) use
        ``"AWAY @ HOME"``
      * outrights (tennis) use ``"Player A vs Player B"``

    Malformed matchups fall through to an empty context — the row still
    renders using the plain matchup string.
    """
    if not matchup:
        return {}
    ctx: dict = {}
    if sport == "tennis":
        # Player-photo enrichment. Small enough per-slate that a live
        # lookup per pick is cheap; the photo cache backs it anyway.
        parts = [p.strip() for p in matchup.split(" vs ")]
        if len(parts) == 2:
            p1_name, p2_name = parts
            ctx["p1_name"] = p1_name
            ctx["p2_name"] = p2_name
            try:
                from .tennis_player_photos import lookup_image
                ctx["p1_photo"] = lookup_image(p1_name)
                ctx["p2_photo"] = lookup_image(p2_name)
            except Exception:
                pass
            tour = (extra.get("tour") or "").upper()
            if tour in ("ATP", "WTA"):
                ctx["tour"] = tour
    elif " @ " in matchup:
        # Team-sport style. Split away@home, hand back both abbrevs;
        # frontend already has resolveTeamLogo keyed by sport + abbr.
        parts = [p.strip() for p in matchup.split(" @ ")]
        if len(parts) == 2:
            ctx["away_abbr"] = parts[0]
            ctx["home_abbr"] = parts[1]
    elif sport in _VS_SPORTS:
        parts = [p.strip() for p in matchup.split(" vs ")]
        if len(parts) == 2:
            ctx["p1_name"], ctx["p2_name"] = parts
    return ctx


# Schedule statuses (per data source) that mean a match is no longer a
# clean prematch bet: already underway, finished, or pulled. The queue
# only ever fires prematch, so a pick whose game carries one of these is
# skipped instead of sent to the relay -- a live line is either stale or
# a different market than the model priced.
_STARTED_TENNIS = {"in", "post", "retired", "walkover", "canceled",
                   "cancelled"}
_STARTED_SOCCER = {"final", "live", "post", "ft", "ht", "inprogress",
                   "in_progress", "canceled", "cancelled", "abandoned"}


def _start_time_passed(start_time) -> bool:
    """True when an ISO start_time (UTC; trailing Z optional) is at or past
    now. Feed status can lag -- a live match often still reads 'pre' /
    'scheduled' -- so the clock is the authoritative 'has it started'
    signal. Parse miss -> False (fall back to status only)."""
    if not start_time:
        return False
    try:
        from datetime import datetime, timezone
        txt = str(start_time).strip()
        if txt[-1:] in ("Z", "z"):
            txt = txt[:-1]
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt <= datetime.utcnow()
    except Exception:
        return False


def _match_started(sport: str, match_id) -> bool:
    """True when the schedule CONFIRMS the match has started, ended, or was
    pulled -- by status OR by a start_time that has already passed. Status
    lags (a live match often still reads 'pre'), so start_time is checked
    too. Defaults to False (keep the pick) on any lookup miss or error:
    dropping a valid prematch edge on a data hiccup is worse than the
    occasional live attempt the relay line-drift guard already catches."""
    if match_id in (None, ""):
        return False
    try:
        if sport == "tennis":
            import engine.tennis_db as _tdb
            row = _tdb.get_conn().execute(
                "SELECT status, start_time FROM tennis_scheduled_matches "
                "WHERE match_id = ? LIMIT 1", (match_id,)).fetchone()
            if not row:
                return False
            if (row[0] or "").strip().lower() in _STARTED_TENNIS:
                return True
            return _start_time_passed(row[1])
        src = _SOURCES.get(sport)
        if src and src[0] == "engine.soccer._db":
            import importlib
            conn = importlib.import_module(
                "engine.soccer._db").get_conn(src[2])
            row = conn.execute(
                "SELECT status, start_time FROM matches WHERE id = ? LIMIT 1",
                (match_id,)).fetchone()
            if not row:
                return False
            if (row[0] or "").strip().lower() in _STARTED_SOCCER:
                return True
            return _start_time_passed(row[1])
    except Exception:
        return False
    return False


def _iter_todays_picks() -> list[dict]:
    """Walk every sport's picks table for open (result IS NULL) picks
    dated today or tomorrow. Returns a flat list with sport tagged +
    extra per-sport context columns (tennis carries `tour`)."""
    from datetime import datetime, timedelta
    today = datetime.now().strftime("%Y-%m-%d")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    import importlib
    out: list[dict] = []
    for sport, src in _SOURCES.items():
        try:
            mod = importlib.import_module(src[0])
            conn = (mod.get_conn(src[2]) if len(src) >= 3 else mod.get_conn())
        except Exception:
            continue
        base_cols = ["id", "date", "matchup", "bet_type", "pick",
                      "model_prob", "edge", "odds", "stake_units"]
        extras = _EXTRA_COLS.get(sport, ())
        id_col = _EVENT_ID_COL.get(sport)
        select_cols = base_cols + list(extras)
        if id_col:
            select_cols.append(id_col)
        cols = ", ".join(select_cols)
        try:
            rows = conn.execute(
                f"SELECT {cols} FROM {src[1]} "
                f"WHERE result IS NULL AND stake_units > 0 "
                f"  AND date IN (?, ?)",
                (today, tomorrow),
            ).fetchall()
        except Exception as e:
            logger.debug("bet_queue: %s pick query failed: %s", sport, e)
            continue
        for r in rows:
            d = dict(r)
            d["sport"] = sport
            d["display_label"] = _display_label(sport, d)
            d["card"] = _card_context(sport, d, d.get("matchup") or "")
            # Normalize the sport-specific id column into a shared
            # `event_id` string so the frontend can look up the game
            # in the sport's scheduled endpoint without knowing
            # which id column each sport uses.
            if id_col and d.get(id_col) is not None:
                d["event_id"] = str(d[id_col])
            # Prematch-only: drop picks whose game is already underway,
            # finished, or pulled (see _match_started). Stops the queue
            # firing stale-line bets on live games -- the symptom behind
            # live/ended matches lingering in the placement log.
            if id_col and _match_started(sport, d.get(id_col)):
                continue
            out.append(d)
    return out


def build_queue(*,
                min_n: int = MIN_SETTLED,
                min_roi: float = MIN_ROI,
                max_daily_units: float = MAX_DAILY_UNITS,
                unit_dollars: float = DEFAULT_UNIT_DOLLARS,
                ) -> dict:
    """Return the auto-eligible queue for right now.

    Shape::

        {
          "generated_at": "2026-07-03T20:00:00Z",
          "cap_units": 5.0,
          "used_units": 3.75,
          "picks": [
              {"sport": "tennis", "matchup": "...", "bet_type": "...",
               "pick": "...", "odds": -145, "stake_units": 0.75,
               "stake_dollars": 75.0,
               "cell_n": 109, "cell_roi": 39.2, "cell_wr": 66.1,
               "priority": 0},
              ...
          ],
          "trimmed": [ ...picks the cap dropped ]
        }
    """
    from datetime import datetime, timezone
    cell_stats = _load_all_stats()
    picks = _iter_todays_picks()

    eligible: list[dict] = []
    for p in picks:
        sport = p["sport"]
        bt = p.get("bet_type") or ""
        direction = _direction_label(bt, p.get("pick"))
        cell = cell_stats.get(sport, {}).get((bt, direction))
        if not cell:
            # Fall through to direction-agnostic cell — some bet_types
            # (ML, DNB) don't have a natural direction axis.
            cell = cell_stats.get(sport, {}).get((bt, ""))
        if not cell:
            continue
        if not _cell_qualifies(cell, min_n, min_roi):
            continue
        stake = float(p.get("stake_units") or 0.0)
        if stake <= 0:
            continue
        eligible.append({
            "sport": sport,
            "display_label": p.get("display_label") or sport,
            "card": p.get("card") or {},
            "event_id": p.get("event_id"),
            "tour": p.get("tour"),
            "matchup": p.get("matchup") or "",
            "bet_type": bt,
            "direction": direction,
            "pick": p.get("pick") or "",
            "odds": int(p.get("odds") or 0),
            "model_prob": float(p.get("model_prob") or 0.0),
            "edge": float(p.get("edge") or 0.0),
            "stake_units": stake,
            "stake_dollars": round(stake * unit_dollars, 2),
            "date": p.get("date"),
            "cell_n": cell["n"],
            "cell_wr": round(cell["wr"], 1),
            "cell_roi": round(cell["roi"], 2),
        })

    # Prioritize by cell historical ROI descending — bankroll cap
    # keeps the strongest signals when it has to trim.
    eligible.sort(key=lambda p: -p["cell_roi"])
    for i, p in enumerate(eligible):
        p["priority"] = i

    kept: list[dict] = []
    trimmed: list[dict] = []
    used_units = 0.0
    for p in eligible:
        if used_units + p["stake_units"] <= max_daily_units:
            kept.append(p)
            used_units += p["stake_units"]
        else:
            trimmed.append(p)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cap_units": max_daily_units,
        "used_units": round(used_units, 3),
        "unit_dollars": unit_dollars,
        "min_settled_n": min_n,
        "min_roi_pct": min_roi,
        "picks": kept,
        "trimmed": trimmed,
        "cell_universe": [
            {"sport": sport, "bet_type": bt, "direction": d,
             "n": c["n"], "roi": round(c["roi"], 2),
             "wr": round(c["wr"], 1)}
            for sport, cells in cell_stats.items()
            for (bt, d), c in cells.items()
            if _cell_qualifies(c, min_n, min_roi)
        ],
    }


def _iter_settled_picks(sport: str) -> list[dict]:
    """Every settled (non-shadow) pick in the sport's picks table with
    the fields the tracker needs. Same shape across sports so the
    tracker aggregator can consume them uniformly. Pulls per-sport
    extras (tennis tour) when available."""
    src = _SOURCES.get(sport)
    if not src:
        return []
    import importlib
    try:
        mod = importlib.import_module(src[0])
        conn = (mod.get_conn(src[2]) if len(src) >= 3 else mod.get_conn())
    except Exception:
        return []
    base_cols = ["date", "matchup", "bet_type", "pick", "odds",
                  "stake_units", "result", "profit", "settled_at"]
    extras = _EXTRA_COLS.get(sport, ())
    out: list[dict] = []
    for tbl in (src[1], "player_props_picks"):
        # player_props_picks may not carry the same extras — silently
        # fall back to base columns if the extra select fails.
        for cols in (base_cols + list(extras), base_cols):
            try:
                rows = conn.execute(
                    f"SELECT {', '.join(cols)} FROM {tbl} "
                    f"WHERE result IN ('W','L','P') AND stake_units > 0"
                ).fetchall()
                break
            except Exception:
                rows = None
                continue
        if not rows:
            continue
        for r in rows:
            d = dict(r)
            d["sport"] = sport
            d["display_label"] = _display_label(sport, d)
            d["card"] = _card_context(sport, d, d.get("matchup") or "")
            out.append(d)
    return out


def build_tracker(*,
                    min_n: int = MIN_SETTLED,
                    min_roi: float = MIN_ROI,
                    unit_dollars: float = DEFAULT_UNIT_DOLLARS,
                    limit_recent: int = 25,
                    ) -> dict:
    """Historical performance for picks that fall in currently-
    qualifying cells (the same universe ``build_queue`` uses for
    today). Answers: "how well are the queue-eligible markets
    actually doing?"

    Includes an overall summary + a recent-picks list so the panel
    can render both a KPI header and a settled-picks stream without
    a second endpoint.
    """
    cell_stats = _load_all_stats()
    qualifying: set[tuple] = set()
    for sport, cells in cell_stats.items():
        for (bt, direction), c in cells.items():
            if _cell_qualifies(c, min_n, min_roi):
                qualifying.add((sport, bt, direction))

    if not qualifying:
        return {
            "generated_at": None, "min_settled_n": min_n,
            "min_roi_pct": min_roi, "unit_dollars": unit_dollars,
            "summary": {
                "n": 0, "wins": 0, "losses": 0, "pushes": 0,
                "wr": 0.0, "roi": 0.0,
                "staked_dollars": 0.0, "profit_dollars": 0.0,
            },
            "recent": [], "by_cell": [],
        }

    all_picks: list[dict] = []
    for sport in {q[0] for q in qualifying}:
        for r in _iter_settled_picks(sport):
            bt = (r.get("bet_type") or "").strip()
            if not bt:
                continue
            direction = _direction_label(bt, r.get("pick"))
            if (sport, bt, direction) not in qualifying:
                # Also allow direction-agnostic match (some bet_types
                # like ML don't have a natural direction axis)
                if (sport, bt, "") not in qualifying:
                    continue
            all_picks.append(r)

    n = wins = losses = pushes = 0
    staked_units = 0.0
    profit_units = 0.0
    for r in all_picks:
        n += 1
        stake = float(r.get("stake_units") or 0.0)
        staked_units += stake
        profit_units += float(r.get("profit") or 0.0) * stake
        if r.get("result") == "W":
            wins += 1
        elif r.get("result") == "L":
            losses += 1
        elif r.get("result") == "P":
            pushes += 1

    decided = wins + losses
    wr = (wins / decided * 100.0) if decided else 0.0
    staked_dollars = staked_units * unit_dollars
    profit_dollars = profit_units * (unit_dollars / 100.0)
    roi = (profit_dollars / staked_dollars * 100.0) if staked_dollars else 0.0

    # Sort for recent-picks display — settled_at desc, fall back to date.
    def _sort_key(r):
        return (r.get("settled_at") or r.get("date") or "")
    recent = sorted(all_picks, key=_sort_key, reverse=True)[:limit_recent]
    recent_out = []
    for r in recent:
        stake = float(r.get("stake_units") or 0.0)
        row_profit = float(r.get("profit") or 0.0) * stake * (
            unit_dollars / 100.0)
        recent_out.append({
            "sport": r.get("sport"),
            "display_label": r.get("display_label") or r.get("sport"),
            "card": r.get("card") or {},
            "date": r.get("date"),
            "matchup": r.get("matchup"),
            "bet_type": r.get("bet_type"),
            "pick": r.get("pick"),
            "odds": int(r.get("odds") or 0),
            "stake_units": stake,
            "stake_dollars": round(stake * unit_dollars, 2),
            "result": r.get("result"),
            "profit_dollars": round(row_profit, 2),
        })

    # Per-cell breakdown so the panel can show which markets are
    # carrying the bag / dragging on it.
    by_cell: list[dict] = []
    for (sport, bt, direction) in qualifying:
        c = cell_stats.get(sport, {}).get((bt, direction))
        if not c:
            continue
        cell_stake_dollars = c["staked"] * unit_dollars
        cell_profit_dollars = c["profit"] * (unit_dollars / 100.0)
        by_cell.append({
            "sport": sport, "bet_type": bt, "direction": direction,
            "n": c["n"], "wr": round(c["wr"], 1),
            "roi": round(c["roi"], 2),
            "staked_dollars": round(cell_stake_dollars, 2),
            "profit_dollars": round(cell_profit_dollars, 2),
        })
    by_cell.sort(key=lambda r: -r["profit_dollars"])

    from datetime import datetime, timezone
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "min_settled_n": min_n,
        "min_roi_pct": min_roi,
        "unit_dollars": unit_dollars,
        "summary": {
            "n": n, "wins": wins, "losses": losses, "pushes": pushes,
            "wr": round(wr, 1), "roi": round(roi, 2),
            "staked_dollars": round(staked_dollars, 2),
            "profit_dollars": round(profit_dollars, 2),
        },
        "recent": recent_out,
        "by_cell": by_cell,
    }


__all__ = [
    "build_queue", "build_tracker",
    "MIN_SETTLED", "MIN_ROI", "MAX_DAILY_UNITS",
    "DEFAULT_UNIT_DOLLARS",
]
