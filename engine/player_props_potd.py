"""
Player-prop Pick-of-the-Day selector (Phase 2j).

Selects the best prop pick from today's slate per sport and locks
it into ``player_props_picks_pot_day``. Lock-once-per-day mirrors
the main POTD pattern: subsequent calls return the locked POTD
regardless of how the slate develops, so the UI shows a stable
"as of locking time" pick.

Selection = PROVEN ACCURACY (Austin 2026-11-01, "we want to win, not lose
with better odds"; replaces the old edge x confidence ranking):
  * cell = (bet_type, Over/Under). Each cell's track record is the graded
    card picks in ``player_props_picks`` dated on/after the sport's new-model
    start (``MODEL_SINCE``): n, W-L, win%, break-even% (mean 1/decimal odds), ROI.
  * PROVEN = n >= 20 AND win% - break-even% > 0.5 x SE, SE = sqrt(be(1-be)/n).
    (Conservative: a +1pp run over 20 picks is noise, it has to clear half a
    standard error too.)
  * POTD = the eligible pick from the proven cell with the best margin;
    edge only breaks ties inside the cell.
  * No proven cell on the slate -> the cell with the best margin among cells
    with n >= 10, flagged ``proven: false``; nothing with n >= 10 -> the old
    edge x confidence rule, also ``proven: false``.
Eligibility filters are unchanged (pending, game not started, edge floor,
NFL latest-slate recency). Stake = the card's stake chip
(``engine.player_props_stake.stake_tier``) and is what the POTD is graded at.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime
from typing import Any

from .player_props_db import (
    list_picks, set_potd, get_potd, _conn_for,
)
from ._tz import et_today_str
from .player_props_stake import stake_tier

logger = logging.getLogger(__name__)


# Confidence-tier reliability multipliers. Strong > Moderate > Lean
# so a +5% strong pick scores above a +8% lean. Conservative starting
# values; tune from settled history once 2j-backtest accumulates.
_CONF_MULT = {"strong": 1.20, "moderate": 1.05, "lean": 0.90}


def _selection_score(pick: dict) -> float:
    """Score = edge × confidence multiplier. Picks with no confidence
    fall to lean's multiplier."""
    edge = float(pick.get("edge") or 0.0)
    conf = (pick.get("confidence") or "lean").lower()
    return edge * _CONF_MULT.get(conf, _CONF_MULT["lean"])


# New-model start per sport (first card pick of the current props model). Track records,
# proven-cell checks and the POTD headline record all count from here. Verified 2026-11-01:
#   NHL 10-27: the tracker record reset Austin asked for 10-29 (routes_props._PROPS_SINCE) --
#              the 10-26 evening rework (edge fix + early card lock); 10-20..10-26 rows are the
#              pre-matchup / old re-rank picker (up to 5 picks/day, saves + points mixed in).
#   NBA 10-27: props v2 shipped 10-26 (worker), first v2 card picks dated 10-27.
#   NFL 10-26: props v2 live 23:36 ET 10-25; everything dated <= 10-25 is v1 (65-166 wk8).
# MLB is offseason and has no v2 -- no cutoff (full history).
MODEL_SINCE = {"nhl": "2026-10-27", "nba": "2026-10-27", "nfl": "2026-10-26"}

PROVEN_MIN_N = 20        # graded W/L picks a cell needs before it can be "proven"
PROVEN_SE_MULT = 0.5     # ...and its margin over break-even must exceed this many SEs
FALLBACK_MIN_N = 10      # no proven cell -> best margin among cells with this many


def model_since(sport: str) -> str:
    return MODEL_SINCE.get(sport, "")


def _side_of(p: dict) -> str:
    return (p.get("side") or str(p.get("pick") or "").split(" ")[0]).title()


def _cell_key(p: dict) -> tuple:
    return (p.get("bet_type") or "", _side_of(p))


def _cell_name(key: tuple) -> str:
    return f"{key[0]} {key[1]}".strip()


def _decimal(odds) -> float | None:
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    return 1.0 + (o / 100.0 if o > 0 else 100.0 / abs(o))


def _record_of(rows: list[dict]) -> dict:
    """W-L-P, win%, break-even% (mean 1/decimal over W/L with a price) and
    flat 1u ROI for a set of graded rows."""
    w = sum(1 for r in rows if r.get("result") == "W")
    l = sum(1 for r in rows if r.get("result") == "L")
    pu = sum(1 for r in rows if r.get("result") == "P")
    wl = [r for r in rows if r.get("result") in ("W", "L")]
    decs = [_decimal(r.get("odds")) for r in wl]
    decs_ok = [d for d in decs if d]
    be = (sum(1.0 / d for d in decs_ok) / len(decs_ok)) if decs_ok else None
    flat = 0.0
    for r, d in zip(wl, decs):
        if r["result"] == "W":
            flat += (d - 1.0) if d else ((r.get("profit") or 0) / 100.0)
        else:
            flat -= 1.0
    n = w + l
    return {
        "n": n, "wins": w, "losses": l, "pushes": pu,
        "win_pct": round(w / n * 100, 1) if n else None,
        "be_pct": round(be * 100, 1) if be is not None else None,
        "roi": round(flat / n * 100, 1) if n else None,
    }


def _grade_cell(rec: dict) -> dict:
    """Add margin / SE / proven to a cell record."""
    n, wp, be = rec.get("n") or 0, rec.get("win_pct"), rec.get("be_pct")
    if n and wp is not None and be is not None:
        b = be / 100.0
        se = math.sqrt(max(b * (1 - b), 1e-9) / n) * 100
        margin = wp - be
        rec["margin_pp"] = round(margin, 1)
        rec["se_pp"] = round(se, 1)
        rec["proven"] = bool(n >= PROVEN_MIN_N and margin > PROVEN_SE_MULT * se)
    else:
        rec["margin_pp"] = None
        rec["se_pp"] = None
        rec["proven"] = False
    rec["rule"] = (f"proven = n>={PROVEN_MIN_N} and win% - break-even% > "
                   f"{PROVEN_SE_MULT:g} SE")
    return rec


def cell_records(sport: str, since: str | None = None) -> dict:
    """Track record per (bet_type, side) over graded card picks dated on/after
    the sport's new-model start. Returns {(bet_type, side): record}."""
    since = model_since(sport) if since is None else since
    conn = _conn_for(sport)
    rows = conn.execute(
        "SELECT bet_type, side, pick, odds, result, profit FROM player_props_picks "
        "WHERE result IN ('W','L','P') AND date >= ?", (since,),
    ).fetchall()
    groups: dict = {}
    for r in rows:
        d = dict(r)
        groups.setdefault(_cell_key(d), []).append(d)
    out = {}
    for k, v in groups.items():
        rec = _grade_cell(_record_of(v))
        rec["cell"] = _cell_name(k)
        rec["since"] = since
        out[k] = rec
    return out


def _empty_cell(key: tuple, since: str) -> dict:
    rec = _grade_cell({"n": 0, "wins": 0, "losses": 0, "pushes": 0,
                       "win_pct": None, "be_pct": None, "roi": None})
    rec["cell"] = _cell_name(key)
    rec["since"] = since
    return rec


def _fmt_rec(rec: dict) -> str:
    if not rec.get("n"):
        return f"{rec.get('cell')} no graded picks yet"
    wp = "-" if rec.get("win_pct") is None else f"{rec['win_pct']:.1f}%"
    be = "-" if rec.get("be_pct") is None else f"{rec['be_pct']:.1f}%"
    return f"{rec.get('cell')} {rec.get('wins', 0)}-{rec.get('losses', 0)} {wp} vs {be} need"


def _proven_note(rec: dict) -> str | None:
    if rec.get("proven"):
        return None
    return "not proven yet: " + _fmt_rec(rec)


def _eligible_pool(sport: str, anchor_str: str, min_edge: float) -> list[dict]:
    """The same eligibility the POTD always used: pending, game not started,
    edge >= floor; NFL = latest slate within the 3-day recency window, daily
    sports = the first of today..today+2 with any eligible pick."""
    from datetime import timedelta as _td
    try:
        anchor = datetime.strptime(anchor_str, "%Y-%m-%d")
    except ValueError:
        anchor = datetime.now()
    if sport == "nfl":
        # Recency guard (MintyBot 2026-10-12): NFL prop game_id may be the slate date string, so
        # _game_already_started can't see the football DB; the date field bounds the pool to the
        # current football week so a stale unsettled pick can't win forever.
        _cutoff = (anchor - _td(days=3)).strftime("%Y-%m-%d")
        elig = [p for p in list_picks(sport, pending_only=True, limit=5000)
                if (p.get("edge") or 0.0) >= min_edge
                and (p.get("date") or "") >= _cutoff
                and not _game_already_started(sport, p.get("game_id"))]
        if not elig:
            return []
        latest = max((p.get("date") or "") for p in elig)
        return [p for p in elig if (p.get("date") or "") == latest]
    for offset in (0, 1, 2):
        d = (anchor + _td(days=offset)).strftime("%Y-%m-%d")
        candidates = list_picks(sport, date=d, limit=1000)
        eligible = [p for p in candidates
                    if (p.get("edge") or 0.0) >= min_edge
                    and p.get("result") is None
                    and not _game_already_started(sport, p.get("game_id"))]
        if eligible:
            return eligible
    return []


def _pick_line(p: dict) -> str:
    return (f"{p.get('player_name')} {p.get('pick')}: model "
            f"{(p.get('model_prob') or 0)*100:.0f}% vs market "
            f"{(_implied(p.get('odds')) or 0)*100:.0f}%, edge "
            f"+{float(p.get('edge') or 0):.1f}%.")


def _reasoning(top: dict, rec: dict, mode: str, since: str) -> str:
    since_s = f" since {since[5:]}" if since else ""
    roi = "" if rec.get("roi") is None else f", ROI {rec['roi']:+.1f}%"
    core = (f"{rec['cell']} is {rec.get('wins', 0)}-{rec.get('losses', 0)}"
            f"{since_s} ({rec.get('win_pct') if rec.get('win_pct') is not None else '-'}%"
            f" vs {rec.get('be_pct') if rec.get('be_pct') is not None else '-'}% break-even{roi})")
    if mode == "proven":
        head = f"Proven cell: {core}, the best record on the slate."
    elif mode == "best_unproven":
        head = (f"Not proven yet: {core}; needs n>={PROVEN_MIN_N} and a margin over "
                f"{PROVEN_SE_MULT:g} SE. Best record among cells with {FALLBACK_MIN_N}+ graded.")
    else:
        head = (f"Not proven yet: untested cell ({core}; under {FALLBACK_MIN_N} graded{since_s}). "
                f"Cells below break-even are excluded; best edge x confidence among untested cells.")
    return f"{head} Top edge in the cell: {_pick_line(top)}"


def _choose(sport: str, pool: list[dict], recs: dict, since: str) -> dict | None:
    if not pool:
        return None
    by_cell: dict = {}
    for p in pool:
        by_cell.setdefault(_cell_key(p), []).append(p)
    keys = list(by_cell)

    def _best_in(k):
        return max(by_cell[k], key=lambda p: (float(p.get("edge") or 0.0), _selection_score(p)))

    proven = [k for k in keys if recs.get(k, {}).get("proven")]
    if proven:
        mode = "proven"
        k = max(proven, key=lambda k: (recs[k]["margin_pp"], recs[k]["n"]))
        top = _best_in(k)
    else:
        # Never pick from a cell that is BELOW its break-even (Austin 2026-11-01 "Yeah I like that
        # change"): a seasoned losing cell is excluded outright; untested cells (< FALLBACK_MIN_N
        # graded) beat it. Order: seasoned cell at/above break-even (best margin) -> best edge x
        # confidence among UNTESTED cells -> no POTD (every cell on the slate is a known loser).
        _n = lambda k: (recs.get(k, {}).get("n") or 0)
        seasoned = [k for k in keys if _n(k) >= FALLBACK_MIN_N
                    and recs[k].get("margin_pp") is not None and recs[k]["margin_pp"] >= 0]
        untested = [k for k in keys if _n(k) < FALLBACK_MIN_N]
        if seasoned:
            mode = "best_unproven"
            k = max(seasoned, key=lambda k: (recs[k]["margin_pp"], recs[k]["n"]))
            top = _best_in(k)
        elif untested:
            mode = "legacy"
            top = max((p for k in untested for p in by_cell[k]), key=_selection_score)
            k = _cell_key(top)
        else:
            logger.info("prop POTD %s: every cell on the slate is below break-even -> no POTD", sport)
            return None
    rec = dict(recs.get(k) or _empty_cell(k, since))
    top = dict(top)
    top["selection_score"] = _selection_score(top)
    top["selection_mode"] = mode
    top["cell"] = rec["cell"]
    top["cell_record"] = rec
    top["proven"] = bool(rec.get("proven")) and mode == "proven"
    top["proven_note"] = None if top["proven"] else _proven_note(rec)
    top["stake"] = stake_tier(sport, top)
    top["reasoning"] = _reasoning(top, rec, mode, since)
    top["candidate_cells"] = [
        dict(recs.get(c) or _empty_cell(c, since), eligible=len(by_cell[c]))
        for c in keys]
    return top


def select_potd(sport: str, date: str | None = None,
                min_edge: float = 6.0) -> dict | None:
    """Pick the POTD from the eligible slate by PROVEN ACCURACY (see module
    docstring). Read-only -- never locks. Returns the pick dict plus
    selection_mode / cell / cell_record / proven / proven_note / stake /
    reasoning / candidate_cells, or None when nothing is eligible.

    ``min_edge`` floor (unchanged) keeps marginal picks out of the headline.
    Search window (unchanged): NFL = latest weekly slate in the recency
    window; daily sports = today, then today+1, today+2.
    """
    anchor_str = date or et_today_str()
    pool = _eligible_pool(sport, anchor_str, min_edge)
    if not pool:
        return None
    since = model_since(sport)
    return _choose(sport, pool, cell_records(sport, since), since)


def _game_already_started(sport: str, game_id) -> bool:
    """Returns True if the underlying game has 'live' / 'final' /
    'postponed' status. POTD shouldn't surface picks on games the
    user can no longer place."""
    if not game_id:
        return False
    try:
        if sport == "mlb":
            from .db import get_conn as _gc
            row = _gc().execute(
                "SELECT status FROM games WHERE mlb_game_id = ? LIMIT 1",
                (str(game_id),),
            ).fetchone()
        elif sport == "nba":
            from .nba_db import get_conn as _gc
            row = _gc().execute(
                "SELECT status FROM nba_games WHERE game_id = ? LIMIT 1",
                (str(game_id),),
            ).fetchone()
        elif sport == "nhl":
            from .nhl_db import get_conn as _gc
            row = _gc().execute(
                "SELECT status FROM nhl_games WHERE game_id = ? LIMIT 1",
                (str(game_id),),
            ).fetchone()
        elif sport == "nfl":
            # Football game-model nfl.db, READ-ONLY.
            from engine.football._db import get_conn as _gc
            row = _gc("nfl").execute(
                "SELECT status FROM games WHERE game_id = ? LIMIT 1",
                (str(game_id),),
            ).fetchone()
        else:
            return False
    except Exception:
        return False
    if not row:
        return False
    return row["status"] in ("live", "final", "postponed")


def get_or_create_potd(sport: str, date: str | None = None,
                      min_edge: float = 6.0) -> dict | None:
    """Get today's prop POTD, locking it on first call.

    Once selected, the POTD stays frozen for the day -- the same
    semantics as ``engine.pick_of_day.get_or_create_potd``. A locked,
    still-pending POTD gets its stake re-synced to the card's stake chip
    (the line can move) and, if it predates the proven-accuracy rule, its
    cell + record annotated -- the pick itself never changes.
    """
    target_date = date or et_today_str()
    existing = get_potd(sport, target_date)
    if existing:
        return _decorate(sport, _sync_locked(sport, existing))
    selected = select_potd(sport, target_date, min_edge=min_edge)
    if not selected:
        return None
    pick_id = selected.get("id")
    if not pick_id:
        return None
    set_potd(sport, int(pick_id),
             date=target_date,
             selection_score=selected["selection_score"],
             reasoning=selected["reasoning"],
             cell=selected["cell"],
             proven=selected["proven"],
             cell_record=json.dumps(selected["cell_record"]),
             stake_units=selected["stake"]["units"])
    logger.info("prop POTD %s locked (%s): %s %s | %s", sport, selected["selection_mode"],
                selected.get("player_name"), selected.get("pick"), selected["reasoning"])
    out = get_potd(sport, target_date)
    return _decorate(sport, out) if out else None


def _sync_locked(sport: str, potd: dict) -> dict:
    """Pending lock: stake = card chip now; missing cell info -> annotate."""
    if potd.get("result") is not None or not potd.get("pot_id"):
        return potd
    conn = _conn_for(sport)
    sets, vals = [], []
    st = stake_tier(sport, potd)
    if potd.get("potd_stake_units") != st["units"]:
        sets.append("stake_units = ?"); vals.append(st["units"])
        potd["potd_stake_units"] = st["units"]
    if not potd.get("cell"):
        since = model_since(sport)
        k = _cell_key(potd)
        rec = cell_records(sport, since).get(k) or _empty_cell(k, since)
        rec = dict(rec, annotated_after_lock=True)
        sets += ["cell = ?", "proven = ?", "cell_record = ?"]
        vals += [rec["cell"], int(bool(rec.get("proven"))), json.dumps(rec)]
        potd.update(cell=rec["cell"], proven=int(bool(rec.get("proven"))),
                    cell_record=json.dumps(rec))
    if sets:
        try:
            conn.execute(f"UPDATE player_props_picks_pot_day SET {', '.join(sets)} WHERE id = ?",
                         (*vals, int(potd["pot_id"])))
            conn.commit()
        except Exception as e:
            logger.warning("prop POTD %s lock sync failed: %s", sport, e)
    return potd


_LABEL = {1.0: "1u", 0.5: "\u00bdu", 0.25: "\u00bcu"}


def _decorate(sport: str, potd: dict) -> dict:
    """API shape: parsed cell_record, proven bool + note, stake dict = what the
    POTD is shown and graded at."""
    rec = potd.get("cell_record")
    if isinstance(rec, str):
        try:
            rec = json.loads(rec)
        except ValueError:
            rec = None
    potd["cell_record"] = rec
    potd["proven"] = None if potd.get("proven") is None else bool(potd["proven"])
    potd["proven_note"] = (_proven_note(rec) if (rec and potd["proven"] is False) else None)
    potd["pick_stake_units"] = potd.get("stake_units")
    if potd.get("result") is None:
        st = stake_tier(sport, potd)
    elif potd.get("potd_stake_units") is not None:
        u = float(potd["potd_stake_units"])
        st = {"units": u, "label": _LABEL.get(u, f"{u:g}u"), "why": "stake at grading"}
    else:
        u = float(potd["stake_units"]) if potd.get("stake_units") is not None else 1.0
        st = {"units": u, "label": _LABEL.get(u, f"{u:g}u"),
              "why": "legacy POTD: graded at the pick's own stake"}
    potd["stake"] = st
    potd["stake_units"] = st["units"]
    potd["model_since"] = model_since(sport)
    return potd


def _implied(odds: int | None) -> float | None:
    if odds is None:
        return None
    n = float(odds)
    if n == 0:
        return None
    return 100.0 / (n + 100.0) if n > 0 else abs(n) / (abs(n) + 100.0)


def refresh_potd_for_line_movement(sport: str,
                                   date: str | None = None) -> dict:
    """Repoint a locked prop POTD at the current pending pick for the
    same player + bet_type + side, when HR has moved the line since
    the lock.

    Selection itself stays frozen (same player, same stat, same Over/
    Under direction) — only the line / odds / edge update. The
    underlying pick row that the POTD points at gets swapped to the
    fresher line; the now-orphaned old row is deleted in the same
    pass so the picker's keep-set logic stays clean.

    Returns ``{updated, reason}`` per call.
    """
    if sport not in ("mlb", "nhl", "nba", "nfl"):
        return {"updated": 0, "reason": f"unsupported sport {sport!r}"}
    target = date or et_today_str()
    conn = _conn_for(sport)

    pot = conn.execute(
        "SELECT pot.id AS pot_id, pot.pick_id, pot.reasoning, "
        "       p.game_id, p.player_id, p.player_name, p.bet_type, "
        "       p.side, p.pick, p.line, p.odds, p.edge, p.model_prob "
        "FROM player_props_picks_pot_day pot "
        "JOIN player_props_picks p ON p.id = pot.pick_id "
        "WHERE pot.date = ? AND p.result IS NULL",
        (target,),
    ).fetchone()
    if not pot:
        return {"updated": 0, "reason": "no live POTD"}
    pot = dict(pot)

    # Find a sibling pending pick for the same (game, player, bet_type,
    # side) at a different line/odds. Order DESC so the most recent
    # insert wins on ties — that's the picker's current top.
    sibling = conn.execute(
        "SELECT id, pick, line, odds, edge, model_prob "
        "FROM player_props_picks "
        "WHERE game_id = ? AND player_id = ? AND bet_type = ? "
        "  AND side = ? AND id != ? AND result IS NULL "
        "ORDER BY id DESC LIMIT 1",
        (pot["game_id"], int(pot["player_id"]), pot["bet_type"],
         pot["side"], int(pot["pick_id"])),
    ).fetchone()
    if not sibling:
        return {"updated": 0, "reason": "no sibling line"}
    sibling = dict(sibling)

    # No-op if line + price match (common case — picker just re-ran
    # without HR moving anything).
    if (sibling["pick"] == pot["pick"]
            and sibling["odds"] == pot["odds"]
            and sibling["edge"] == pot["edge"]):
        return {"updated": 0, "reason": "no change"}

    # Repoint POTD at the new pick row, drop the old (now orphaned).
    new_reasoning = (
        f"{pot['player_name']} {sibling['pick']}"
        f" — model gives {(sibling['model_prob'] or 0) * 100:.0f}%"
        f" vs market {(_implied(sibling['odds']) or 0) * 100:.0f}%."
        f" Edge +{sibling['edge']:.1f}%."
    )
    # Re-score using the same helper as selection so the badge stays
    # consistent. Confidence pulled from the new line's pick row.
    sibling_for_score = conn.execute(
        "SELECT edge, confidence FROM player_props_picks WHERE id = ?",
        (int(sibling["id"]),),
    ).fetchone()
    new_score = _selection_score(dict(sibling_for_score) if sibling_for_score
                                 else {"edge": sibling["edge"]})
    conn.execute(
        "UPDATE player_props_picks_pot_day "
        "SET pick_id = ?, reasoning = ?, selection_score = ? "
        "WHERE id = ?",
        (int(sibling["id"]), new_reasoning, new_score,
         int(pot["pot_id"])),
    )
    conn.execute(
        "DELETE FROM player_props_picks WHERE id = ?",
        (int(pot["pick_id"]),),
    )
    conn.commit()
    logger.info(
        "prop POTD %s line-refresh: %r %+d edge=%.1f -> %r %+d edge=%.1f",
        sport, pot["pick"], int(pot["odds"] or 0), float(pot["edge"] or 0),
        sibling["pick"], int(sibling["odds"] or 0),
        float(sibling["edge"] or 0),
    )
    return {"updated": 1,
            "old_pick": pot["pick"], "old_odds": pot["odds"],
            "new_pick": sibling["pick"], "new_odds": sibling["odds"]}


def get_potd_summary(sport: str) -> dict:
    """Prop POTD record. Headline = ``since_model`` (POTDs dated on/after the
    sport's new-model start); ``legacy`` = before it. The old top-level keys
    (total/wins/losses/pushes/profit/win_pct/roi) are kept for PotdHero and
    now equal ``since_model``.

    Stake per POTD = the lock row's stake_units (card chip, set from
    2026-11-01 on); older locks fall back to the pick's own stake_units (what
    they were always graded at -- history isn't rewritten), then 1u.
    profit is in $ at $100/u; roi = profit / units staked (%); flat_roi = 1u each.
    Duplicate lock rows that point at the same pick (NFL weekly slates were
    re-locked daily) count once.
    """
    from .player_props_db import _conn_for, ensure_tables
    ensure_tables(sport)
    conn = _conn_for(sport)
    rows = conn.execute("""
        SELECT MIN(pd.date) AS potd_date, p.id, p.result, p.odds, p.profit,
               COALESCE(MAX(pd.stake_units), p.stake_units, 1.0) AS stake
        FROM player_props_picks_pot_day pd
        JOIN player_props_picks p ON p.id = pd.pick_id
        GROUP BY p.id
        ORDER BY potd_date
    """).fetchall()
    rows = [dict(r) for r in rows]
    since = model_since(sport)

    def _block(rs: list[dict]) -> dict:
        settled = [r for r in rs if r.get("result") in ("W", "L", "P")]
        rec = _record_of(settled)
        profit = sum((r.get("profit") or 0) * (r.get("stake") or 0) for r in settled)
        staked = sum((r.get("stake") or 0) for r in settled if r["result"] in ("W", "L"))
        last = next((r["result"] for r in reversed(settled) if r["result"] in ("W", "L")), None)
        return {
            "total": len(settled),
            "wins": rec["wins"], "losses": rec["losses"], "pushes": rec["pushes"],
            "record": f"{rec['wins']}-{rec['losses']}-{rec['pushes']}",
            "win_pct": rec["win_pct"] if rec["win_pct"] is not None else 0,
            "be_pct": rec["be_pct"],
            "profit": round(profit, 2),
            "units_staked": round(staked, 2),
            "roi": round(profit / staked, 1) if staked > 0 else 0,
            "flat_roi": rec["roi"],
            "pending": sum(1 for r in rs if r.get("result") is None),
            "last_result": last,
            "first_date": rs[0]["potd_date"] if rs else None,
        }

    since_model = _block([r for r in rows if (r["potd_date"] or "") >= since])
    since_model["since"] = since
    legacy = _block([r for r in rows if (r["potd_date"] or "") < since])
    legacy["before"] = since
    out = {k: since_model[k] for k in ("total", "wins", "losses", "pushes", "profit",
                                       "win_pct", "roi", "be_pct", "pending", "last_result")}
    out.update(since=since, since_model=since_model, legacy=legacy)
    return out


__all__ = ["select_potd", "get_or_create_potd",
           "refresh_potd_for_line_movement",
           "get_potd_summary", "cell_records", "MODEL_SINCE"]
