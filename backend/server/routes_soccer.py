"""Soccer backend routes.

Per-league read endpoints. Pattern mirrors routes_golf — one ingest +
predict + odds + picks call per slate hit, with debounced ESPN +
debounced HR odds caching at the layer below.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from fastapi import APIRouter

router = APIRouter()
logger = logging.getLogger(__name__)


try:
    from zoneinfo import ZoneInfo
    _US_EASTERN = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    _US_EASTERN = None


def _today_et() -> str:
    """US-Eastern calendar date. Matches engine.soccer._espn_ingest._utc_date
    which now writes ET dates on the matches.date column — using UTC
    here would misalign the slate at the late-evening ET boundary
    (10pm ET = 2am UTC tomorrow), dropping live games off "today"."""
    if _US_EASTERN is not None:
        return datetime.now(_US_EASTERN).strftime("%Y-%m-%d")
    # Fallback: hour-offset by EDT/EST month
    now = datetime.now(timezone.utc)
    offset = 4 if 3 <= now.month <= 10 else 5
    return datetime.fromtimestamp(now.timestamp() - offset * 3600).strftime("%Y-%m-%d")


# Per-league ingest debounce — ESPN's scoreboard moves a few times an
# hour even mid-match, so 90s is a reasonable balance between fresh
# scores and not hammering ESPN on every tab focus.
_INGEST_TTL_S = 90
_INGEST_TS: dict[str, float] = {}


def _lock_live_picks(conn, slate: dict) -> None:
    """For every match in ``slate`` that already has open recorded
    picks in the per-league picks table, replace the freshly-generated
    ``picks`` list with those recorded picks. Mutates the slate in
    place and tags each locked match with ``_locked=True`` so the
    caller can skip re-recording.

    Lock-on-record semantics: once a pick has been persisted (via a
    prior slate hit), that IS the pick we're betting on and grading
    against. Re-showing a different live-odds pick on the card because
    HR moved lines or the ensemble changed its mind between ticks
    diverges the card from the tracker — which is exactly the drift
    the user flagged on WC 2026-07-03 (card said BTTS Yes, tracker had
    H1_ML + OU from earlier that morning). The record_picks path
    itself already drops later same-family picks (see
    engine.soccer._tracker); this method makes the display honor that
    same lock. Live + past-start-time matches are always locked; a
    scheduled match with recorded picks is locked too.
    """
    matches = slate.get("matches") or []
    if not matches:
        return
    match_ids = [m.get("match_id") for m in matches if m.get("match_id")]
    if not match_ids:
        return
    placeholders = ",".join("?" * len(match_ids))
    try:
        status_rows = conn.execute(
            f"SELECT id, status, start_time FROM matches "
            f"WHERE id IN ({placeholders})",
            match_ids,
        ).fetchall()
    except Exception:
        return
    for m in matches:
        mid = m.get("match_id")
        if mid is None:
            continue
        try:
            recorded = conn.execute(
                "SELECT bet_type, pick, side, line, model_prob, edge, odds, "
                "       stake_units "
                "FROM picks "
                "WHERE match_id = ? AND result IS NULL "
                "ORDER BY edge DESC",
                (int(mid),),
            ).fetchall()
        except Exception:
            continue
        if not recorded:
            # No pre-kickoff pick recorded (predictor produced nothing
            # when scheduled). Leave the slate-generated picks alone —
            # better than blanking the card.
            continue
        m["picks"] = [{
            "type":      r["bet_type"],
            "pick":      r["pick"],
            "side":      r["side"],
            "line":      r["line"],
            "model_prob": r["model_prob"],
            "raw_prob":  r["model_prob"],
            "edge":      r["edge"],
            "odds":      r["odds"],
            "stake_units": r["stake_units"] if "stake_units" in r.keys() else None,
            "locked":    True,
        } for r in recorded]
        m["_locked"] = True
    # Recompute top_pick across the (now-locked-for-live) slate so the
    # hero matches what's on the cards. Prefer picks with a real stake
    # over shadow (stake_units=0) — same fix basketball landed 2026-05-29
    # (see project_basketball_card_headline_0u_fix.md). Shadow picks
    # can carry huge stated edges because bleed-cell floors keep them
    # alive for calibration data collection, but they aren't picks
    # we're actually betting — surfacing them as the hero misrepresents
    # what the tracker is doing.
    all_picks: list[dict] = []
    for m in matches:
        for p in m.get("picks") or []:
            all_picks.append(p)
    all_picks.sort(key=lambda p: (
        0 if (p.get("stake_units") or 0) > 0 else 1,  # staked first
        -(p.get("edge") or 0),
    ))
    slate["picks"] = all_picks
    slate["top_pick"] = all_picks[0] if all_picks else None


@router.get("/api/soccer/leagues")
def api_soccer_leagues() -> dict:
    """Sidebar discovery endpoint — every configured competition.
    Includes today's match count per league so the sidebar can render
    the per-league badge same as basketball + hockey."""
    from engine.soccer import LEAGUE_REGISTRY, active_leagues
    from engine.soccer._db import get_conn
    in_season = set(active_leagues())
    today = _today_et()

    def _count_today(league: str) -> int:
        # Three-stage count, mirroring basketball:
        #   1. Local DB for today (catches everything when ingest is fresh).
        #   2. Fall-forward to the next upcoming date in the DB when
        #      today is empty — keeps the badge non-zero across off-days
        #      so the user sees "next match" instead of a dead-looking 0.
        #   3. ESPN scoreboard live fallback when the worker poll lags.
        #   4. HR numEvents fallback for leagues with no future schedule
        #      in the DB (UEFA cup ties land in HR before they reach our
        #      ESPN ingest because we only pull league-paths daily).
        from engine.soccer import get_league_config
        cfg = get_league_config(league)
        n = 0
        try:
            c = get_conn(league)
            row = c.execute(
                "SELECT COUNT(*) AS n FROM matches "
                "WHERE date = ? "
                "  AND status IN ('scheduled', 'live', 'final')",
                (today,),
            ).fetchone()
            n = int(row["n"] or 0)
            if n == 0:
                nxt = c.execute(
                    "SELECT date, COUNT(*) AS n FROM matches "
                    "WHERE date > ? AND status = 'scheduled' "
                    "GROUP BY date ORDER BY date ASC LIMIT 1",
                    (today,),
                ).fetchone()
                if nxt:
                    n = int(nxt["n"] or 0)
        except Exception:
            return 0
        if n == 0:
            espn_n = _live_espn_soccer_count(cfg.get("espn_league_path"),
                                              today)
            if espn_n is not None:
                n = espn_n
        if n == 0 and cfg.get("hr_comp_id"):
            hr_n = _live_hr_soccer_count(cfg.get("hr_comp_id"))
            if hr_n is not None:
                n = hr_n
        return n

    return {
        "leagues": [
            {
                "key": k,
                "display_name": v.get("display_name") or k,
                "country": v.get("country"),
                "confederation": v.get("confederation"),
                "tier": v.get("tier"),
                "competition_type": v.get("competition_type"),
                "status": v.get("status"),
                "in_season": k in in_season,
                "game_count_today": _count_today(k) if k in in_season else 0,
            }
            for k, v in LEAGUE_REGISTRY.items()
        ],
    }


_live_espn_soccer_cache: dict[tuple[str, str], tuple[float, int | None]] = {}
_live_hr_soccer_cache: dict[str, tuple[float, int | None]] = {}


def _live_espn_soccer_count(espn_path: str | None, date_str: str
                              ) -> int | None:
    """Sidebar-badge fallback: count ESPN scoreboard events for
    ``date_str``. Caches 60s per (path, date)."""
    if not espn_path:
        return None
    import time as _t
    import urllib.request as _ur
    import json as _json
    key = (espn_path, date_str)
    cached = _live_espn_soccer_cache.get(key)
    now = _t.time()
    if cached and (now - cached[0]) < 60:
        return cached[1]
    try:
        url = (f"https://site.api.espn.com/apis/site/v2/sports/soccer/"
                f"{espn_path}/scoreboard?dates={date_str.replace('-', '')}")
        req = _ur.Request(url, headers={
            "User-Agent": "SportsBettor/1.0 (soccer-leagues-count)",
        })
        with _ur.urlopen(req, timeout=4) as r:
            data = _json.loads(r.read())
        n = len(data.get("events") or [])
    except Exception:
        n = None
    _live_espn_soccer_cache[key] = (now, n)
    return n


def _live_hr_soccer_count(comp_id: str | None) -> int | None:
    """Sidebar-badge fallback: pull HR's ``numEvents`` for the
    competition. Catches leagues whose ESPN ingest hasn't seen the
    fixture yet (cup ties especially). Cached 60s per comp_id."""
    if not comp_id:
        return None
    import time as _t
    now = _t.time()
    cached = _live_hr_soccer_cache.get(comp_id)
    if cached and (now - cached[0]) < 60:
        return cached[1]
    try:
        from scrapers.hardrock_odds import _fetch_sports_tree
        tree = _fetch_sports_tree() or {}
        for sport in tree.get("sports") or []:
            for group in sport.get("competitions") or []:
                for comp in group.get("competitions") or []:
                    if str(comp.get("id")) == str(comp_id):
                        n = int(comp.get("numEvents") or 0)
                        _live_hr_soccer_cache[comp_id] = (now, n)
                        return n
        _live_hr_soccer_cache[comp_id] = (now, 0)
        return 0
    except Exception:
        _live_hr_soccer_cache[comp_id] = (now, None)
        return None


@router.get("/api/soccer/{league}/today")
def api_soccer_today(league: str) -> dict:
    """Slate for ``league`` on the current UTC day. Returns matches with
    prediction + odds + picks per match plus the day's top pick across
    the slate."""
    from engine.soccer import LEAGUE_REGISTRY, get_league_config
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    cfg = get_league_config(league)

    from engine.soccer._espn_ingest import ingest_today
    now = time.monotonic()
    if (now - _INGEST_TS.get(league, 0.0)) >= _INGEST_TTL_S:
        try:
            ingest_today(league)
            # 2-day backsweep — late-finalized matches (ESPN drops the
            # FT status update sometimes) get re-ingested so settler
            # sees the final score on the next tick. Same defense pattern
            # as the football route added on 2026-05-31.
            from datetime import datetime as _dt0, timedelta as _td0
            today_dt = _dt0.strptime(_today_et(), "%Y-%m-%d")
            for back in (1, 2):
                d = (today_dt - _td0(days=back)).strftime("%Y-%m-%d")
                try:
                    ingest_today(league, date=d)
                except Exception as e:
                    logger.debug("[soccer:%s] backsweep %s failed: %s",
                                  league, d, e)
            _INGEST_TS[league] = now
        except Exception as e:
            logger.warning("[soccer:%s] ingest_today failed: %s", league, e)

    from engine.soccer._picks import generate_picks_for_slate
    today = _today_et()
    slate = generate_picks_for_slate(league, today)
    # Fall-forward: when today has no matches for this league, walk
    # forward up to 7 days to the next scheduled date. Mirrors the
    # pattern basketball / hockey / football routes already use so the
    # panel always shows the *next* upcoming slate rather than going
    # blank on off-days.
    if not (slate.get("matches") or []):
        from datetime import datetime as _dt, timedelta as _td
        from engine.soccer._db import get_conn as _gc
        c = _gc(league)
        nxt = c.execute(
            "SELECT date FROM matches WHERE date > ? "
            "  AND status IN ('scheduled', 'live') "
            "ORDER BY date ASC LIMIT 1",
            (today,),
        ).fetchone()
        if nxt:
            slate = generate_picks_for_slate(league, nxt["date"])
            slate["date"] = nxt["date"]
            slate["fell_forward"] = True

    # Lock picks for matches that have already kicked off. Same pattern
    # as MLB/NHL/NBA `_get_recorded_pick`: once a match goes live, the
    # card surfaces the pick that was recorded pre-kickoff rather than
    # whatever the model + live odds spit out now. Soccer cards were
    # silently swapping picks mid-match as HR's lines moved, which
    # diverged from what the tracker had on file.
    from engine.soccer._db import get_conn as _soc_conn
    _lock_live_picks(_soc_conn(league), slate)

    # Piggyback record + settle on every slate hit so the tracker stays
    # fresh without a separate cron. Same pattern as golf. Live matches
    # skip record_picks — their picks were already locked in when the
    # match was scheduled, and re-recording would be a no-op against
    # the open-pick unique index anyway.
    try:
        from engine.soccer._tracker import record_picks, settle_picks
        for m in slate["matches"]:
            if m.get("_locked"):
                continue
            if m["picks"]:
                record_picks(league, m["match_id"], m["picks"])
        settle_picks(league)
    except Exception as e:
        logger.warning("[soccer:%s] record/settle piggyback failed: %s",
                       league, e)

    # Mark the POTD's match with is_potd=True so the card surfaces the
    # gold ★ badge that links the hero to its source match. Mirrors the
    # basketball-framework pattern. Read-only here — POTD creation /
    # locking happens on the dedicated /potd endpoint.
    try:
        from engine.pick_of_day import get_today_potd as _potd_read
        potd_row = _potd_read(league)
        if potd_row and potd_row.get("game_id"):
            potd_gid = str(potd_row["game_id"])
            for m in slate.get("matches") or []:
                if str(m.get("match_id")) == potd_gid:
                    m["is_potd"] = True
                    break
    except Exception:
        pass

    # Lookahead — tomorrow's slate (next calendar day after today's
    # slate date) generated full-fat: predictions + HR odds + picks,
    # rendered as the same SoccerGameCard primitive the active slate
    # uses. Scoped to ONE day to keep the route from blowing up on
    # tournament leagues like the World Cup with 4-6 matches per day
    # for two weeks. User asked for it 2026-06-20.
    upcoming: list[dict] = []
    try:
        from datetime import datetime as _dt0, timedelta as _td0
        base = _dt0.strptime(slate.get("date") or today, "%Y-%m-%d")
        tomorrow = (base + _td0(days=1)).strftime("%Y-%m-%d")
        tomorrow_slate = generate_picks_for_slate(league, tomorrow)
        upcoming = tomorrow_slate.get("matches") or []
        # Apply the same live-lock pass so already-tipped matches in
        # the tomorrow window don't pick up new picks mid-game.
        _lock_live_picks(_soc_conn(league), {"matches": upcoming})
    except Exception as e:
        logger.debug("[soccer:%s] tomorrow-lookahead failed: %s", league, e)

    return {
        "league": league,
        "display_name": cfg.get("display_name") or league,
        "country": cfg.get("country"),
        "confederation": cfg.get("confederation"),
        **slate,
        "upcoming": upcoming,
    }


@router.get("/api/soccer/{league}/calibration")
def api_soccer_calibration(league: str) -> dict:
    """League constants + ensemble weights for the Calibration tab.
    Mirrors the baseball/basketball framework shape so PickHistory's
    sibling calibration view renders identically across sports."""
    from engine.soccer import LEAGUE_REGISTRY, get_league_config
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    cfg = get_league_config(league)
    from engine.soccer._db import get_conn
    conn = get_conn(league)
    settled_n = conn.execute(
        "SELECT COUNT(*) FROM picks WHERE result IN ('W','L','P')"
    ).fetchone()[0]
    constants = {
        "avg_home_goals":   cfg.get("avg_home_goals"),
        "avg_away_goals":   cfg.get("avg_away_goals"),
        "home_advantage":   cfg.get("home_advantage"),
        "dc_rho":           cfg.get("dc_rho"),
        "fitted_n":         cfg.get("fitted_n"),
        "fitted_at":        cfg.get("fitted_at"),
        "status":           cfg.get("status"),
    }
    return {
        "league":    league,
        "constants": constants,
        "n_settled": settled_n,
        "buckets":   [],
    }


@router.get("/api/soccer/{league}/tracker/history")
def api_soccer_tracker_history(league: str, limit: int = 200) -> dict:
    """Tracker view — pending + settled with summary."""
    from engine.soccer import LEAGUE_REGISTRY, list_history, settle_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    try:
        settle_picks(league)
    except Exception as e:
        logger.debug("[soccer:%s] settle on history failed: %s", league, e)
    rows = list_history(league, limit=limit)
    rows_out = []
    for r in rows:
        d = dict(r) if not isinstance(r, dict) else dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        rows_out.append(d)
    # Summary scans every pick on file (not just the limit-200 slice)
    # so the hero P/L reflects the full history. Stake-weighted: per
    # row contributes profit * stake_units; ROI denom is sum(stake_units).
    from engine.soccer._db import get_conn as _soc_conn
    conn = _soc_conn(league)
    all_picks = conn.execute(
        "SELECT bet_type, result, profit, stake_units FROM picks"
    ).fetchall()
    wins = losses = pushes = pending = 0
    profit = 0.0
    stake_settled = 0.0
    by_type: dict[str, dict] = {}
    for r in all_picks:
        bt = r["bet_type"] or "?"
        b = by_type.setdefault(bt, {"total": 0, "wins": 0, "losses": 0,
                                     "pushes": 0, "pending": 0,
                                     "profit": 0.0, "stake_settled": 0.0})
        b["total"] += 1
        res = r["result"]
        stake_u = float(r["stake_units"] if r["stake_units"] is not None else 1.0)
        pr = float(r["profit"] or 0) * stake_u
        if res == "W":
            b["wins"] += 1; wins += 1
            b["profit"] += pr; profit += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        elif res == "L":
            b["losses"] += 1; losses += 1
            b["profit"] += pr; profit += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        elif res == "P":
            b["pushes"] += 1; pushes += 1
        else:
            b["pending"] += 1; pending += 1
    for b in by_type.values():
        d = b["wins"] + b["losses"]
        st = b.pop("stake_settled", 0)
        b["win_pct"] = round(b["wins"] / d * 100, 1) if d else 0.0
        b["roi"] = round(b["profit"] / st, 1) if st else 0.0
        b["profit"] = round(b["profit"], 2)
    decided = wins + losses
    summary = {
        "overall": {
            "total": wins + losses + pushes,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "pending": pending,
            "profit": round(profit, 2),
            "win_pct": round(wins / decided * 100, 1) if decided else 0.0,
            "roi": round(profit / stake_settled, 1) if stake_settled else 0.0,
        },
        "by_type": by_type,
    }
    return {"league": league, "rows": rows_out, "summary": summary}


def _build_soccer_potd_bets(matches: list[dict]) -> list[dict]:
    """Reshape soccer slate matches into the bet-list shape select_potd
    expects. Mirrors the basketball/baseball pattern. Considers only
    full-game picks (skips H1_* halftime markets) since POTD locks
    pre-kickoff and H1 markets settle at HT, not FT."""
    bets: list[dict] = []
    for m in matches:
        full = [p for p in (m.get("picks") or [])
                  if not str(p.get("type") or "").upper().startswith("H1_")]
        if not full:
            continue
        bp = max(full, key=lambda p: float(p.get("edge") or 0))
        bp_norm = dict(bp)
        # POTD selector reads `prob`; soccer picks emit `raw_prob` +
        # `prob` already (engine.soccer._picks). Make sure both keys
        # are populated.
        if "prob" not in bp_norm and "raw_prob" in bp_norm:
            bp_norm["prob"] = bp_norm["raw_prob"]
        home = m.get("home") or {}
        away = m.get("away") or {}
        bets.append({
            "game_id": m.get("match_id"),
            "matchup": m.get("matchup"),
            "best_pick": bp_norm,
            "home": {"name": home.get("name"),
                      "abbreviation": home.get("abbr")},
            "away": {"name": away.get("name"),
                      "abbreviation": away.get("abbr")},
            "time": m.get("start_time") or "",
            "venue": "",
            "status": {"state": "pre"},
        })
    return bets


@router.get("/api/soccer/{league}/potd")
def api_soccer_potd(league: str) -> dict:
    """Pick of the Day for a soccer-framework league. Locks at first
    creation per (league, date). Mirrors the basketball-framework POTD
    contract so PickOfDayHero hits `/soccer/{league}/potd` the same
    way it hits `/basketball/{league}/potd`."""
    from engine.soccer import LEAGUE_REGISTRY
    from engine.pick_of_day import get_or_create_potd, get_today_potd
    from engine.pick_of_day._storage import _ensure_potd_table
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    _ensure_potd_table(league)
    existing = get_today_potd(league)
    if existing:
        return existing
    slate = api_soccer_today(league)
    bets = _build_soccer_potd_bets(slate.get("matches") or [])
    if not bets:
        return {"message": "No qualifying picks today", "sport": league}
    potd = get_or_create_potd(league, bets)
    return potd or {"message": "No qualifying picks today", "sport": league}


@router.get("/api/soccer/{league}/potd/summary")
def api_soccer_potd_summary(league: str) -> dict:
    """POTD running totals for the league."""
    from engine.soccer import LEAGUE_REGISTRY
    from engine.pick_of_day import get_potd_summary, settle_potd
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    try:
        settle_potd(league)
    except Exception as e:
        logger.debug("POTD auto-settle failed for %s: %s", league, e)
    return get_potd_summary(league)


@router.post("/api/soccer/{league}/tracker/settle")
def api_soccer_tracker_settle(league: str) -> dict:
    from engine.soccer import LEAGUE_REGISTRY, settle_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    return settle_picks(league)


@router.post("/api/soccer/{league}/tracker/record")
def api_soccer_tracker_record(league: str) -> dict:
    """Force-regenerate today's picks and persist them."""
    from engine.soccer import LEAGUE_REGISTRY
    from engine.soccer._picks import generate_picks_for_slate
    from engine.soccer._tracker import record_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    today = _today_et()
    slate = generate_picks_for_slate(league, today, force_odds_refresh=True)
    totals = {"recorded": 0, "duplicate": 0, "errors": 0}
    for m in slate["matches"]:
        if m["picks"]:
            r = record_picks(league, m["match_id"], m["picks"])
            for k in totals:
                totals[k] += r.get(k, 0)
    return totals


# ── Live (halftime) picks ──────────────────────────────────────

@router.post("/api/soccer/{league}/live/halftime")
def api_soccer_live_halftime(league: str) -> dict:
    """Fire halftime live picks for every match currently in HT state
    (status='live' AND HT scores recorded AND no FT score). Idempotent
    via UPSERT on (match_id, stage, bet_type) — safe to re-call."""
    from engine.soccer import LEAGUE_REGISTRY
    from engine.soccer._live_picks import fire_halftime_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    return fire_halftime_picks(league)


@router.get("/api/soccer/{league}/live/history")
def api_soccer_live_history(league: str, limit: int = 100) -> dict:
    """Pending + settled halftime live picks. Mirrors the prematch
    /tracker/history endpoint shape so the same UI primitive renders
    both panes."""
    from engine.soccer import LEAGUE_REGISTRY
    from engine.soccer._live_picks import (
        list_live_picks, settle_live_picks,
    )
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    # Settle anything graded before returning so the UI shows fresh state.
    settle_live_picks(league)
    return {"league": league, "rows": list_live_picks(league, limit=limit)}


@router.post("/api/soccer/{league}/live/settle")
def api_soccer_live_settle(league: str) -> dict:
    from engine.soccer import LEAGUE_REGISTRY
    from engine.soccer._live_picks import settle_live_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    return settle_live_picks(league)
