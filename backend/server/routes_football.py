"""Football backend routes.

Per-league read endpoints. Pattern mirrors routes_hockey:
one ingest + predict + odds + picks call per slate hit, with debounced
ESPN ingest at the layer below.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

from fastapi import APIRouter
from engine.picks import get_best_pick

router = APIRouter()
logger = logging.getLogger(__name__)


try:
    from zoneinfo import ZoneInfo
    _US_EASTERN = ZoneInfo("America/New_York")
except Exception:
    _US_EASTERN = None


def _today_et() -> str:
    if _US_EASTERN is not None:
        return datetime.now(_US_EASTERN).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc)
    offset = 4 if 3 <= now.month <= 10 else 5
    return (datetime.fromtimestamp(now.timestamp() - offset * 3600)
             .strftime("%Y-%m-%d"))


_INGEST_TTL_S = 90
_INGEST_TS: dict[str, float] = {}
# Slate response cache (Austin 2026-09-19 perf): the display compute
# (predict + generate_picks ~0.5s/game -> ~42s for CFB's 71-game board) PLUS the
# write-side (record/settle/capture_closing_odds) ran on EVERY hit -> ~200s cold
# loads. Serve a cached response and refresh it in a background thread
# (stale-while-revalidate) so no user request waits on the recompute after the
# first. Keyed by (league, days).
_SLATE_CACHE: dict = {}
_SLATE_TTL_S = 60.0

# Per-game compute cache (MintyBot 2026-10-17 perf). (league, game_id) ->
# (monotonic_ts, status, full_game_dict). A game's prediction + pick are frozen
# once it locks (live/final, or past its lock_schedule time), so the ~5s/game of
# predict+picks+calibration+card-lock is reused WHOLESALE; only the live fields
# (status/score/updated_at) are refreshed from the re-ingested row. Unlocked
# games recompute, but at most every _GAME_TTL_S. A status change busts the entry
# so lock-on-go-live + final transitions run once. NOTE: reused games skip the
# per-hit signal-log write (redundant for a frozen prediction).
_GAME_CACHE: dict = {}
_GAME_CACHE_LOCK = threading.Lock()
_GAME_TTL_S = 900.0


def _game_locked_for_cache(d: dict) -> bool:
    """True if a game's pick/prediction are frozen (cached compute reusable):
    live/final/postponed/cancelled, or past its lock_schedule time."""
    if d.get("status") in ("live", "final", "postponed", "cancelled"):
        return True
    start = d.get("start_time")
    if not start:
        return False
    try:
        from datetime import datetime as _dt
        from engine.lock_schedule import is_locked as _ls_is_locked
        return _ls_is_locked(_dt.fromisoformat(str(start).replace("Z", "+00:00")))
    except Exception:
        return False


def _game_cached(league, gid, status, now_mono, locked):
    """Cached full game dict for (league, gid) if valid, else None.
    Valid = cached status matches AND (locked OR within _GAME_TTL_S)."""
    with _GAME_CACHE_LOCK:
        c = _GAME_CACHE.get((league, gid))
    if c is None:
        return None
    c_ts, c_status, c_dict = c
    if c_status != status:
        return None
    if locked or (now_mono - c_ts) < _GAME_TTL_S:
        return c_dict
    return None


# Legacy re-entrancy guards — no longer read by _spawn_refresh/_spawn_writeside
# (see _named_lock below), kept only so the long-dead
# @app.on_event("startup") _prewarm_football_slates_on_startup in
# backend/server/__init__.py (its import of _SLATE_REFRESHING never actually
# runs — on_event hooks don't fire in this app — but would ImportError at
# call time if the name vanished) doesn't break if it's ever resurrected.
_SLATE_REFRESHING: set = set()
# Write-side (record/settle/closing-odds) runs OFF the display compute in this
# guarded thread — one per league at a time. Austin 2026-09-19.
_WRITESIDE_RUNNING: set = set()
_INJ_TS: dict = {}          # league -> last injury refresh (epoch)
_INJ_TTL_S = 1800          # refresh ESPN injuries at most every 30 min

# Named-lock registry (Austin 2026-10-10 race fix): _SLATE_REFRESHING /
# _WRITESIDE_RUNNING above are plain sets that were checked-then-added in two
# separate statements (`if key in s: return` then `s.add(key)`) — not atomic,
# so two threads can both see "not running" in the gap between those lines
# and both proceed. Confirmed in production: this let two overlapping CFB
# write-side passes both pass record_paper()'s own pending-row check and both
# insert a paper pick for the same game (different markets) — corrupted 43
# games' worth of pending picks on 2026-10-10 (cleaned up separately via the
# existing void-superseded mechanism). threading.Lock.acquire(blocking=False)
# is a true atomic test-and-set, so replace the sets with per-key locks.
_LOCKS_GUARD = threading.Lock()
_NAMED_LOCKS: dict = {}


def _named_lock(key) -> threading.Lock:
    """Process-wide lock for ``key``, created on first use. Lock creation
    itself is guarded by one small meta-lock so two threads racing to create
    the SAME key's lock can't each get their own (separate) Lock object."""
    lock = _NAMED_LOCKS.get(key)
    if lock is None:
        with _LOCKS_GUARD:
            lock = _NAMED_LOCKS.get(key)
            if lock is None:
                lock = threading.Lock()
                _NAMED_LOCKS[key] = lock
    return lock


def _spawn_writeside(league: str, games: list, today: str, did_ingest: bool) -> None:
    lock = _named_lock(("writeside", league))
    if not lock.acquire(blocking=False):
        return  # a write-side pass for this league is already in flight

    def _ws() -> None:
        try:
            from datetime import datetime as _dt, timedelta as _td
            from engine.football._tracker import (
                record_picks, settle_picks, capture_closing_odds,
            )
            from engine.football._tracker import record_paper, record_shadow_slate
            from engine.football._injuries import snapshot_injury
            today_dt = _dt.strptime(today, "%Y-%m-%d")
            max_record_date = (today_dt + _td(days=3)).strftime("%Y-%m-%d")
            for g in games:
                if g.get("pick_locked"):
                    continue
                if (did_ingest and g.get("status") == "scheduled" and g.get("picks")
                        and (g.get("date") or "9999") <= max_record_date):
                    record_picks(league, g["game_id"], g["picks"])
                if (did_ingest and g.get("status") == "scheduled" and g.get("_paper_leans")
                        and (g.get("date") or "9999") <= max_record_date):
                    _mu = "%s @ %s" % (g.get("away_abbr") or "", g.get("home_abbr") or "")
                    record_paper(league, g["game_id"], _mu, g["_paper_leans"])
                if (did_ingest and g.get("status") == "scheduled" and g.get("_authority_candidates")
                        and (g.get("date") or "9999") <= max_record_date):
                    _mus = "%s @ %s" % (g.get("away_abbr") or "", g.get("home_abbr") or "")
                    record_shadow_slate(league, g["game_id"], _mus, g["_authority_candidates"])
                # Injury calibration snapshot (Austin 2026-09-19): capture each
                # scheduled game's pre-kickoff injury state so we can fit the
                # QB-out penalty against outcomes as games settle.
                if g.get("status") == "scheduled" and g.get("home_team_id") and g.get("away_team_id"):
                    snapshot_injury(league, g["game_id"], g["home_team_id"], g["away_team_id"])
            settle_picks(league)  # grades is_shadow 0 (real) + 1 (paper)
            # Lock + settle the day's POTD (Austin 2026-09-19) so it has its OWN
            # record instead of the all-plays aggregate. _pick_potd = the stable
            # max-edge<=15 pick (stable now that live/final picks persist).
            try:
                from engine.football._tracker import _pick_potd, record_potd
                from engine.pick_of_day import settle_potd
                _potd = _pick_potd(games, league)
                if _potd:
                    record_potd(league, today, _potd)
                settle_potd(league)
            except Exception as _pe:
                logger.debug("[football:%s] POTD lock/settle failed: %s", league, _pe)
            if did_ingest:
                capture_closing_odds(league)
                # Pick Authority CLV (2026-10-25): same-line closes for the unified rows too
                try:
                    from engine.football._tracker import capture_unified_closing
                    capture_unified_closing(league)
                except Exception as _ce:
                    logger.debug("[football:%s] unified closing capture failed: %s", league, _ce)
            # Injury signal refresh (Austin 2026-09-19): keep ESPN injuries fresh,
            # throttled ~30min so we don't hammer ESPN on every compute.
            import time as _t3
            if (_t3.time() - _INJ_TS.get(league, 0.0)) >= _INJ_TTL_S:
                try:
                    from engine.football._injuries import refresh_injuries
                    refresh_injuries(league)
                    _INJ_TS[league] = _t3.time()
                except Exception as _e:
                    logger.debug("[football:%s] injury refresh failed: %s", league, _e)
        except Exception as e:
            logger.warning("[football:%s] async write-side failed: %s", league, e)
        finally:
            lock.release()

    threading.Thread(target=_ws, daemon=True).start()


_NOT_LIVE_STATUSES = frozenset({
    "scheduled", "pre_game", "pregame",
    "final", "complete", "completed",
    "cancelled", "postponed", "voided",
})


def _lock_football_live_pick(conn, d: dict) -> None:
    """Lock-on-go-live: once a football game flips to live, the card
    surfaces the recorded pre-kickoff pick instead of regenerating
    from current odds. Same pattern as soccer/basketball/hockey
    framework routes."""
    status = (d.get("status") or "scheduled").lower()
    if status in _NOT_LIVE_STATUSES:
        return
    gid = d.get("game_id")
    if not gid:
        return
    try:
        rows = conn.execute(
            "SELECT bet_type, pick, model_prob, edge, odds, stake_units "
            "FROM picks "
            "WHERE game_id = ? AND result IS NULL AND COALESCE(is_shadow,0)=0 "
            "ORDER BY edge DESC",
            (str(gid),),
        ).fetchall()
    except Exception:
        return
    if not rows:
        return
    locked = {
        "type":       rows[0]["bet_type"],
        "pick":       rows[0]["pick"],
        "model_prob": rows[0]["model_prob"],
        "edge":       rows[0]["edge"],
        "odds":       rows[0]["odds"],
        "stake_units": rows[0]["stake_units"] if "stake_units" in rows[0].keys() else None,
        "locked":     True,
    }
    d["best_pick"] = locked
    d["picks"] = [locked]
    d["pick_locked"] = True


@router.get("/api/football/leagues")
def api_football_leagues() -> dict:
    """Sidebar discovery — every football league with status/in-season
    + today's game count."""
    from engine.football import LEAGUE_REGISTRY, active_leagues
    from engine.football._db import get_conn
    in_season = set(active_leagues())
    today = _today_et()
    out = []
    for k, v in LEAGUE_REGISTRY.items():
        n = 0
        if k in in_season:
            try:
                conn = get_conn(k)
                row = conn.execute(
                    "SELECT COUNT(*) FROM games "
                    "WHERE date = ? AND status IN "
                    "  ('scheduled', 'live', 'final')",
                    (today,),
                ).fetchone()
                n = int(row[0] or 0)
                if n == 0:
                    nxt = conn.execute(
                        "SELECT COUNT(*) FROM games "
                        "WHERE date > ? AND status = 'scheduled' "
                        "  ORDER BY date ASC LIMIT 7",
                        (today,),
                    ).fetchone()
                    n = int(nxt[0] or 0)
            except Exception:
                n = 0
        out.append({
            "key":              k,
            "display_name":     v.get("display_name") or k,
            "country":          v.get("country"),
            "region":           v.get("region"),
            "status":           v.get("status"),
            "in_season":        k in in_season,
            "game_count_today": n,
        })
    return {"leagues": out}



def _cfb_card_w(league, d):
    """CFB card-% model weight by part of the season (MintyBot 2026-10-31, Austin "all four"). Walk-forward
    2023-26 vs the close: the best blend is 0% model in weeks 1-4 and bowls (the model only made the close worse:
    log-loss .435 -> .439 / .621 -> .628 at 15%) and ~10% the rest of the season. None = default weight."""
    if league != "cfb":
        return None
    try:
        dt = str(d.get("date") or d.get("start_time") or "")[:10]
        m, day = int(dt[5:7]), int(dt[8:10])
        wk = int(d.get("week") or 0)
    except (TypeError, ValueError):
        return None
    if m == 1 or (m == 12 and day >= 13):        # bowls / CFP
        return 0.0
    if m in (8, 9) and 0 < wk <= 4:
        return 0.0
    return 0.10


def _football_paper_picks(pred, odds, home_abbr, away_abbr, *,
                           league: str | None = None, game_id=None):
    """PAPER best-of ML/SPREAD/TOTAL from the model's margin/total + sigma,
    routed through the SAME score_pick gates (juice wall, belief gate,
    calibration, edge floor/ceiling) as the real pick path
    (engine.football._picks.generate_picks) — NOT bypassing them.

    Was: "bypassing the bet gates so the model's edge SHOWS on the card"
    (Austin 2026-09-18), filtered only by a hand-rolled |edge|<=20 ceiling
    with no juice wall, no belief gate, no calibration, and no edge FLOOR
    (negative edges passed the ceiling check trivially, since abs(-13.8)
    <= 20). That let heavy-favorite MLs at bad juice — and flat negative-
    edge picks — get locked/shown on the card: PHI@JAX ML JAX -375 at
    -13.8pp, HOU@TEN ML HOU -375 at -0.5pp, CIN@MIA ML CIN -325 at -17.2pp
    (Austin, 2026-10-11). A pick that wouldn't clear the gates for real
    money shouldn't be the one locked/shown as the paper lean either —
    score_pick's juice wall (NHL_JUICE_WALL=-200, i.e. odds must be >=
    -200) alone rejects all three of those (-375/-375/-325 < -200); its
    edge<=0 check rejects them a second, independent way. Mirrors
    _basketball_paper_picks's SHAPE; football odds are FLAT (home_ml /
    home_spread_point / over_under). Austin 2026-09-18, gated 2026-10-11."""
    import math
    if not pred or not odds:
        return []
    from engine.picks_core import score_pick
    from engine.config import NHL_JUICE_WALL as _JUICE_WALL
    matchup = f"{away_abbr or ''}@{home_abbr or ''}"
    sport = f"football_{league}" if league else "football"

    def ncdf(x):
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    def _score(bet_type, gate_pick_text, bare_pick, raw_prob, am_odds,
               line_val, model_value):
        """Run one candidate through score_pick; None if any gate rejects.
        ``gate_pick_text`` (line embedded, e.g. "UK +9.5") matches
        generate_picks' convention for edge_floor/calibration cell
        keying; the returned dict keeps the established paper-pick
        shape (bare ``pick`` + separate ``line``) that record_paper /
        recorded_paper_pick / the frontend already expect."""
        if am_odds is None or raw_prob is None:
            return None
        scored = score_pick(
            {"type": bet_type, "pick": gate_pick_text, "raw_prob": raw_prob,
             "odds": am_odds, "matchup": matchup, "game_id": game_id},
            sport=sport, juice_wall=_JUICE_WALL)
        if not scored:
            return None
        return dict(type=bet_type, market=bet_type, pick=bare_pick,
                    line=line_val, prob=scored["prob"], odds=scored["odds"],
                    edge=scored["edge"], paper=True, model_value=model_value)

    pmargin = pred.get("expected_margin")
    if pmargin is None:
        pmargin = pred.get("mc_expected_margin")
    ptotal = pred.get("expected_total")
    if ptotal is None:
        ptotal = pred.get("mc_expected_total")
    sm, st = 13.0, 10.0  # last-resort Normal only (see `line_pred` below)
    # Cover / over probabilities at the POSTED main line come from the model itself --
    # predict_match's ensemble (empirical key-number margins + the league's fitted total
    # sigma), the same numbers generate_picks_both's shadow lane scores. The hand-rolled
    # Normal above (margin sd 13, total sd 10 for NFL and CFB alike) is far too sharp (CFB
    # margins spread ~20 pts) and the stacked football calibration used to hide that; with
    # football calibrating once (Austin 2026-10-25 "A B and C") it put +13-15pp leans on the
    # card (DEL@WKU WKU +2.5 at .685 vs the model's .607).
    line_pred = {}
    if league and pred.get("home_team_id") is not None and (
            odds.get("home_spread_point") is not None or odds.get("over_under") is not None):
        try:
            from engine.football._predict import predict_match as _pm
            line_pred = _pm(league, int(pred["home_team_id"]), int(pred["away_team_id"]),
                            spread=odds.get("home_spread_point"),
                            total_line=odds.get("over_under")) or {}
        except Exception as e:
            logger.debug("[football:%s] paper line probs failed: %s", league, e)
            line_pred = {}
    out = []
    p_home_ml = pred.get("p_home")
    if p_home_ml is None:
        p_home_ml = pred.get("mc_p_home")
    if p_home_ml is not None and (odds.get("home_ml") is not None or odds.get("away_ml") is not None):
        if p_home_ml >= 0.5:
            side, prob, o = (home_abbr or "HOME"), p_home_ml, odds.get("home_ml")
        else:
            side, prob, o = (away_abbr or "AWAY"), 1 - p_home_ml, odds.get("away_ml")
        p = _score("ML", side, side, prob, o, None, round(p_home_ml, 3))
        if p:
            out.append(p)
    line = odds.get("over_under")
    if ptotal is not None and line is not None and st > 0:
        if line_pred.get("p_over") is not None:
            p_over, p_under = line_pred["p_over"], line_pred["p_under"]
        else:
            p_over = ncdf((ptotal - line) / st); p_under = 1 - p_over
        side, prob, o = ("Over", p_over, odds.get("over_odds")) if p_over >= p_under else ("Under", p_under, odds.get("under_odds"))
        p = _score("TOTAL", f"{side} {line}", side, prob, o, line, round(ptotal, 1))
        if p:
            out.append(p)
    hpt = odds.get("home_spread_point")
    if pmargin is not None and hpt is not None and sm > 0:
        if line_pred.get("p_home_cover") is not None:
            p_home, p_away = line_pred["p_home_cover"], line_pred["p_away_cover"]
        else:
            p_home = ncdf((pmargin - (-hpt)) / sm); p_away = 1 - p_home
        if p_home >= p_away:
            side, prob, ln, o = (home_abbr or "HOME"), p_home, hpt, odds.get("home_spread_odds")
        else:
            side, prob, ln, o = (away_abbr or "AWAY"), p_away, odds.get("away_spread_point"), odds.get("away_spread_odds")
        if ln is not None:
            sign = "+" if ln > 0 else ""
            p = _score("SPREAD", f"{side} {sign}{ln}", side, prob, o, ln, round(pmargin, 1))
            if p:
                out.append(p)
    return out


@router.get("/api/football/{league}/today")
def api_football_today(league: str, days: int | None = None) -> dict:
    """Slate for ``league`` — served from a short-TTL cache and refreshed in a
    background thread so the heavy compute never blocks a user request after the
    first (Austin 2026-09-19). First hit on a cold cache still computes inline."""
    from engine.football import LEAGUE_REGISTRY, get_league_config
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    ck = (league, days)

    def _spawn_refresh() -> None:
        # Exactly ONE compute per key at a time — kills the pre-warm-vs-cold-hit
        # thundering herd that made both crawl. (Lock-based per _named_lock
        # above — a plain set here could let two overlapping refreshes both
        # pass the check; see the 2026-10-10 comment on _NAMED_LOCKS.)
        lock = _named_lock(("slate", ck))
        if not lock.acquire(blocking=False):
            return

        def _bg():
            try:
                r = _compute_slate(league, days)
                _SLATE_CACHE[ck] = (time.monotonic(), r)
            except Exception as e:
                # exc_info: on 2026-10-25 eighteen identical "SQLite objects created in a
                # thread can only be used in that same thread" failures could not be traced
                # to the offending connection from the message alone (every per-thread conn
                # in the compute path checked clean) — log the traceback so the next one
                # names it instead of silently freezing the slate.
                logger.warning("[football:%s] slate compute failed: %s", league, e,
                               exc_info=True)
            finally:
                lock.release()

        threading.Thread(target=_bg, daemon=True).start()

    cached = _SLATE_CACHE.get(ck)
    if cached is not None:
        built, resp = cached
        if (time.monotonic() - built) >= _SLATE_TTL_S:
            _spawn_refresh()  # stale -> refresh in bg, serve stale instantly
        return resp
    # Cold cache: NEVER block the request on the heavy compute. Kick off one
    # guarded compute + return a loading placeholder; the frontend polls and
    # fills in the moment the compute lands (no 3-min browser hang, no herd).
    _spawn_refresh()
    return {
        "league": league,
        "display_name": get_league_config(league).get("display_name") or league,
        "date": _today_et(),
        "games": [],
        "loading": True,
    }


def _compute_slate(league: str, days: int | None = None) -> dict:
    """Slate for ``league``. Fall-forward to the nearest upcoming day
    when today has no scheduled or finalized games."""
    from engine.football import LEAGUE_REGISTRY, get_league_config
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    cfg = get_league_config(league)

    from engine.football._espn_ingest import ingest_today
    now = time.monotonic()
    # Gate the heavy write-side (signal log, record/settle, closing-odds) on the
    # same 90s debounce as the ingest: it adds ~15s to a 124-game CFB slate, but 90s
    # freshness is fine. Display hits between just predict + price (Austin 2026-09-19).
    _did_ingest = (now - _INGEST_TS.get(league, 0.0)) >= _INGEST_TTL_S
    if _did_ingest:
        try:
            ingest_today(league)
            # Re-ingest the prior 2 days too. ESPN sometimes finalizes
            # a late-night game hours after kickoff; without this sweep,
            # games like DAL@STL 2026-05-29 stay stuck at status='
            # scheduled' and their picks sit pending forever. Cheap —
            # the scoreboard endpoint is one HTTP call per day.
            from datetime import datetime as _dt, timedelta as _td
            today_dt = _dt.strptime(_today_et(), "%Y-%m-%d")
            for back in (1, 2):
                d = (today_dt - _td(days=back)).strftime("%Y-%m-%d")
                try:
                    ingest_today(league, date=d)
                except Exception as e:
                    logger.debug("[football:%s] backsweep %s failed: %s",
                                  league, d, e)
            # 2026-09-03 (Austin: lookahead): also ingest the upcoming window so the
            # slate can show next week's games (HR prices them days out). The old
            # fall-forward could only find games already in the DB = game day only.
            # Ingest a forward window even when the DISPLAY lookahead is 0
            # (CFB=0): otherwise upcoming games never land in the games
            # table and the fall-forward below cannot surface the next
            # game day, leaving an empty slate mid-season. Display window
            # is unchanged (still cfg.lookahead_days).
            # Austin 2026-09-19 "the look-forward isn't helping [the load]":
            # for DISPLAY-today sports (CFB, lookahead_days=0) the forward days
            # never render, so a 7-day-ahead ingest is pure wasted ESPN calls on
            # any day that already has a slate. Only forward-ingest when today is
            # empty (then it's needed to find the next game day). Lookahead sports
            # (nfl/ufl=7) still ingest their display window.
            _look = int(cfg.get("lookahead_days") or 0)
            _need_fwd = _look > 0
            if not _need_fwd:
                from engine.football._db import get_conn as _gc
                _n_today = _gc(league).execute(
                    "SELECT COUNT(*) FROM games WHERE date = ? AND status IN "
                    "('scheduled', 'live', 'final')", (_today_et(),)).fetchone()[0]
                _need_fwd = not _n_today
            if _need_fwd:
                _ingest_ahead = max(_look, 7)
                for fwd in range(1, _ingest_ahead + 1):
                    d = (today_dt + _td(days=fwd)).strftime("%Y-%m-%d")
                    try:
                        ingest_today(league, date=d)
                    except Exception as e:
                        logger.debug("[football:%s] lookahead %s failed: %s",
                                      league, d, e)
            # 2026-10-25 (MintyBot): cache ESPN box scores for recent finals (engine.football._boxscore
            # -> game_stats). Team pace / pass rate / drives / red-zone trips for the props work. Capped
            # per tick so a CFB Saturday (60+ finals) drains over a few ingest ticks, not one slate call.
            try:
                from engine.football._boxscore import ingest_boxscores
                ingest_boxscores(league, limit=12)
            except Exception as e:
                logger.debug("[football:%s] boxscore ingest failed: %s", league, e)
            _INGEST_TS[league] = now
        except Exception as e:
            logger.warning("[football:%s] ingest failed: %s", league, e)

    from engine.football._db import get_conn
    from engine.football._predict import predict_match
    from engine.football._odds import fetch_league_odds
    from engine.football._picks import generate_picks
    from engine.football._tracker import record_picks, settle_picks, capture_closing_odds
    from engine.football._elo import replay
    conn = get_conn(league)

    today = _today_et()
    from datetime import datetime as _dt, timedelta as _td
    _SEL = ("SELECT g.*, ht.abbreviation AS home_abbr, "
            "       ht.name AS home_name, ht.logo_url AS home_logo, "
            "       at.abbreviation AS away_abbr, "
            "       at.name AS away_name, at.logo_url AS away_logo "
            "FROM games g "
            "JOIN teams ht ON ht.id = g.home_team_id "
            "JOIN teams at ON at.id = g.away_team_id ")
    # 2026-09-03 lookahead window (Austin): `days` query param, else the registry's
    # lookahead_days (nfl/ufl 7 = the coming week; cfb 0 = today only — Saturday
    # slates are 70+ games). Window mode returns every game from today through
    # today+days, each row carrying its own date; `date` becomes the range label.
    win = int(days) if days is not None else int(cfg.get("lookahead_days") or 0)
    target = today
    rows = []
    if win > 0:
        end = (_dt.strptime(today, "%Y-%m-%d") + _td(days=win)).strftime("%Y-%m-%d")
        rows = conn.execute(
            _SEL + "WHERE g.date BETWEEN ? AND ? "
                   "  AND g.status IN ('scheduled', 'live', 'final') "
                   "ORDER BY g.date ASC, g.start_time ASC",
            (today, end),
        ).fetchall()
        if rows:
            dates = sorted({r["date"] for r in rows})
            target = dates[0] if len(dates) == 1 else f"{dates[0]} → {dates[-1]}"
    if not rows:
        # Try today first; fall forward up to 7 days if empty.
        for delta in range(0, 8):
            cand = (_dt.strptime(today, "%Y-%m-%d") + _td(days=delta)
                     ).strftime("%Y-%m-%d")
            rows = conn.execute(
                _SEL + "WHERE g.date = ? "
                       "  AND g.status IN ('scheduled', 'live', 'final') "
                       "ORDER BY g.start_time ASC",
                (cand,),
            ).fetchall()
            if rows:
                target = cand
                break

    # CFB FBS-only display (Austin 2026-09-19): filter to ESPN's FBS board (group 80),
    # cached at ingest in _FBS_BOARD (our tables carry no FBS/FCS flag). Kills the
    # FCS-vs-FCS noise + the overconfident FCS-underdog picks + ~halves the load. Elo
    # still ingests FCS (group 81) for ratings. Safe: empty cache -> keep all rows.
    if league == "cfb" and rows:
        from engine.football._espn_ingest import _FBS_BOARD as _fbs_cache
        _fbs = set()
        for _d in {r["date"] for r in rows}:
            _fbs |= _fbs_cache.get((league, _d), set())
        if _fbs:
            rows = [r for r in rows if str(r["game_id"]) in _fbs]

    odds_by_key = fetch_league_odds(league)
    if rows and not odds_by_key:
        # Empty board on a slate that HAS games = a transient HR miss (CF 403 / empty
        # payload). fetch_league_odds keeps last-good odds only once this PROCESS has had
        # a good fetch — right after a restart there is none, so the prewarm's first miss
        # cached {} for 2 min and both football slates computed every locked game
        # pick-less (generate_picks: no odds -> []). Seen at both 2026-10-25 restarts
        # (09:10 + 11:15): 13/13 NFL cards NO PICK with the board blank. One forced retry
        # before computing a whole slate without odds; if still empty, say so loudly
        # (the per-hit board refresh + no-odds TTL fallback in the loop then heal it).
        odds_by_key = fetch_league_odds(league, force=True)
        if not odds_by_key:
            logger.warning("[football:%s] odds board EMPTY for %d games after retry — "
                           "picks skipped this compute; locked games recompute within %ds",
                           league, len(rows), int(_GAME_TTL_S))
    ratings = replay(league)
    from engine.football._predict import log_signals
    games = []
    _now_mono = time.monotonic()
    for r in rows:
        d = dict(r)
        # Per-game compute cache (MintyBot 2026-10-17 perf): reuse a game's ~5s
        # of predict+picks+calibration+card-lock while its pick is frozen
        # (live/final, or past its lock_schedule time); only refresh the live
        # fields from the re-ingested row. Unlocked (provisional) games recompute
        # at most every _GAME_TTL_S; a status change busts the entry so
        # lock-on-go-live + final transitions still run once.
        _gid = str(d["game_id"])
        _locked = _game_locked_for_cache(d)
        _cd = _game_cached(league, _gid, d.get("status"), _now_mono, _locked)
        # A LOCKED scheduled game cached with NO odds board (computed inside a transient
        # HR miss — fetch_league_odds caches {} for 2 min on failure) must not freeze that
        # way: it has no picks either. Fall back to the TTL rule so it recomputes within
        # _GAME_TTL_S of HR posting lines. (Austin 2026-10-25 "NFL missing odds": after
        # the 09:10 restart 23/26 cards sat odds-less + NO PICK while HR had all 13 games.)
        if _cd is not None and _locked and _cd.get("odds") is None \
                and d.get("status") == "scheduled":
            _cd = _game_cached(league, _gid, d.get("status"), _now_mono, False)
        if _cd is not None:
            _rd = dict(_cd)
            # live fields always come from this hit's row, not the 15-min game cache (clock froze 10-31)
            for _lf in ("status", "home_score", "away_score", "updated_at", "status_detail", "periods", "home_linescore", "away_linescore"):
                _rd[_lf] = d.get(_lf)
            # The odds BOARD is a live field too (line moves): refresh it from this hit's
            # fetch while HR still lists the game; keep the cached board once HR pulls it
            # (live/final lose their pre-game markets). The PICK stays frozen — only the
            # displayed both-sides board moves.
            _fo = odds_by_key.get(f"{d['away_abbr']}@{d['home_abbr']}")
            if _fo:
                _rd["odds"] = _fo
            games.append(_rd)
            continue
        pred = predict_match(league, int(d["home_team_id"]),
                              int(d["away_team_id"]), ratings=ratings)
        # Stamp IDs onto the prediction so log_signals can attribute
        # the row. The bare predict_match doesn't know which game it
        # came from.
        pred["game_id"] = d["game_id"]
        pred["home_team_id"] = int(d["home_team_id"])
        pred["away_team_id"] = int(d["away_team_id"])
        key = f"{d['away_abbr']}@{d['home_abbr']}"
        odds = odds_by_key.get(key)
        d["odds"] = odds
        d["prediction"] = pred
        picks = []
        shadow_cands: list = []
        if d["status"] == "scheduled" and odds:
            # ONE pass, two lists (engine.football._picks.generate_picks_both): the
            # anchored HEADLINE candidates + the un-anchored SHADOW candidates for the
            # graduation track. Shared MC sampling — a second generate_picks call
            # doubled the cold slate compute on 2026-10-25.
            try:
                from engine.football._picks import generate_picks_both as _gp_both
                picks, shadow_cands = _gp_both(league, pred, odds, game_id=d["game_id"])
            except Exception as e:
                logger.warning("[football:%s] picks failed for %s: %s",
                                league, d["game_id"], e)
        # Pick Authority (Austin 2026-10-18 "one system for every pick, period"):
        # ONE proven-edge-gated selector (edge x prob), read by card + POTD +
        # scoreboard alike so they stop disagreeing. Markets we don't headline are
        # still paper-tracked below (record_shadow_slate) so they can graduate.
        from engine.selection import select_pick as _authority_select
        from engine.football._segments import edge_league as _edge_league
        _ep_league = _edge_league(league, pred.get("home_team_id"), pred.get("away_team_id"))  # FCS-only CFB -> "cfb_fcs" cells
        best = _authority_select("football", _ep_league, picks)
        # Card == tracker (Austin 2026-11-01 "one pick, read everywhere"): once the game has a RECORDED
        # authority pick (the FIRST non-void is_shadow=0 row = the best_pick this card headlined when the
        # write-side recorded it), the card HOLDS it at its entry price -- pre-kickoff, live and final --
        # instead of re-deriving a live pick as lines move. /tracker/unified reads the SAME function.
        # Until one is recorded the live selection headlines (and the write-side records it, once).
        try:
            _auth_rec = _football_recorded_authority_pick(league, d["game_id"], conn)
        except Exception:
            _auth_rec = None
        if _auth_rec:
            from engine.config import EDGE_STRONG, EDGE_MODERATE
            _re = float(_auth_rec.get("edge") or 0)
            best = {**_auth_rec, "model_prob": _auth_rec.get("prob"), "held": True, "locked": True,
                    "confidence": ("strong" if _re >= EDGE_STRONG else "moderate" if _re >= EDGE_MODERATE else "lean")}
        try:
            from engine.selection import card_lean as _card_lean
            d["card_lean"] = _card_lean("football", _ep_league, [] if _auth_rec else (shadow_cands or picks), best)  # card's best lean (Austin 2026-10-26): graduated pick, else the ungraduated lean -- display only
        except Exception:
            d["card_lean"] = None
        if d.get("status") == "scheduled":
            try:   # held per game (Austin 2026-10-27): the first lean is kept, and carried through live + final
                from engine.selection import lean_lock as _lean_lock
                d["card_lean"] = _lean_lock.hold(f"football:{league}", d.get("game_id"), d.get("card_lean"))
            except Exception:
                pass
        d["picks"] = [best] if best else []
        d["best_pick"] = best
        # Graduation track = the UN-ANCHORED slate (Pick Authority, Austin 2026-10-25
        # "NFL — fix it up"). Feeding the anchored list here meant a league with zero
        # proven cells (NFL: W=0.15 on every market) contributed NOTHING to
        # record_shadow_slate -> edge_proven never accumulated -> nothing could ever
        # graduate. The headline above still selects ONLY from the anchored `picks`.
        d["_authority_candidates"] = shadow_cands
        # Lock-on-go-live mirror of the other frameworks (re-selects max-edge pending real row; the
        # held authority pick below overrides it so card == tracker).
        _lock_football_live_pick(conn, d)
        if _auth_rec:
            d["best_pick"] = best
            d["picks"] = [best]
        # V3.2 signal log — fire-and-forget; opens a short-lived
        # writer conn so concurrent slate hits don't race the cached
        # one.
        _did_ingest and log_signals(league, pred)
        # Display-only win-prob calibration (2026-09-17): raw bar probs
        # mapped through the model replay track record. Applied to a COPY
        # so the pick objects (already score_pick-calibrated) and the raw
        # signal log above stay untouched. Passthrough for un-fitted leagues.
        try:
            from ._display_cal import cal_pair
            # the NFL replay calibration was fit on the old Elo model; the EPA prob is already calibrated (2026-10-31)
            d["prediction"] = dict(pred) if pred.get("margin_source") == "nfl_epa" else cal_pair(f"football_{league}", dict(pred),
                                       "p_home", "p_away")
        except Exception:
            pass
        # Card % = what the Pick Authority bets with (model blended with the de-vigged ML; Austin
        # 2026-10-26 "Any percentages for any card in any sport, should be that way").
        try:
            from ._display_cal import market_card_wp
            d["card_win_prob"] = market_card_wp("football", league, (d.get("prediction") or {}).get("p_home"),
                                                (odds or {}).get("home_ml"), (odds or {}).get("away_ml"),
                                                w_unproven=_cfb_card_w(league, d))
        except Exception:
            pass
        d["paper_picks"] = _football_paper_picks(
            pred, odds, d.get("home_abbr"), d.get("away_abbr"),
            league=league, game_id=d.get("game_id"))  # Austin 2026-09-18, gated 2026-10-11
        d["_paper_leans"] = d.get("paper_picks")  # raw per-market leans -> record_paper (tracker/finished-card fallback)
        # Persist the pick after kickoff/final (Austin 2026-09-19): finished games
        # lose their odds so the live paper pick can't be recomputed — fall back to
        # the RECORDED pre-game pick (with its W/L) so the card keeps showing it.
        # LOCK the card's paper pick to the first pre-kickoff lean (Austin 2026-10-06):
        # surface the recorded (held) pick instead of the live recompute so the card
        # stops flipping as the line moves. First compute (none recorded yet) falls
        # back to the live pick, which _ws then records as the locked lean. Post-kickoff
        # the odds are gone so the live pick can't recompute anyway -> same recorded pick.
        from engine.football._tracker import recorded_paper_pick as _rpp
        _rec = _rpp(league, d["game_id"])
        # Pick Authority (Austin 2026-10-18): NO re-cal of the held lean (one
        # calibration only). The recorded pick stands as-is for live/final display
        # (keeps its W/L); scheduled games use the live proven select_pick below.
        # Don't hold a STALE pre-injury lean: if a QB is now Out for this
        # SCHEDULED game, a recorded pick may predate the ruling, so fall
        # through to the live recompute (injury-aware: factor -175 Elo + GBM
        # dropped) instead of locking the old lean (Austin 2026-10-11: Lamar
        # ruled out mid-day -> held BAL +150 was stale). Post-kickoff still
        # holds the recorded pick for its W/L record.
        _qb_out_now = bool((pred.get("injury_home") or {}).get("qb_out")
                           or (pred.get("injury_away") or {}).get("qb_out"))
        _hold_stale = _qb_out_now and d.get("status") == "scheduled"
        if d.get("status") == "scheduled":
            # Pre-kickoff: the ONE proven-gated LIVE selector, read identically by
            # the card (best_pick) and the POTD hero (paper_picks[0]) -> no drift.
            # pred is injury-aware so a mid-day QB-Out is already handled (no
            # stale-held-lean special case needed; _qb_out_now/_hold_stale unused).
            d["paper_picks"] = [best] if best else []
            d["best_pick"] = best
        elif _auth_rec:
            # Live/final with a recorded authority pick: hold it (+ its W/L) -- the same row the tracker grades.
            d["paper_picks"] = [best]
            d["best_pick"] = best
            d["picks"] = [best]
            try:
                from engine.selection import card_lean as _card_lean
                d["card_lean"] = _card_lean("football", _ep_league, [], best)
            except Exception:
                d["card_lean"] = dict(best, graduated=True, lean=False)
        else:
            # Live/final: odds are gone -> hold the RECORDED pick, but run it through
            # the SAME proven+positive gate as pre-kickoff so NOPLAY / negative-edge
            # shadow leans never surface as a live headline (the known BAL@CLE /
            # PIT@TB -10.6% TOTAL Over calibration rows). A real held pick still shows
            # with its W/L. best_pick == paper_picks[0] in every case (both empty on
            # NO-PLAY -> never a half-set card).
            # Card == tracker (2026-11-01): a started game with NO recorded authority pick has no
            # headline -- the paper lean (_rec) is never promoted to one (the tracker never grades it);
            # it can still show as the ungraduated card_lean below.
            _held = None
            d["paper_picks"] = []
            d["best_pick"] = None
            d["picks"] = []
            try:
                from engine.selection import card_lean as _card_lean
                from engine.selection import lean_lock as _lean_lock
                # the lean this card held pre-kickoff (Austin 2026-10-27 "they should stay on the cards when the
                # game is live"); a game with nothing held keeps the recorded-pick lean as before
                d["card_lean"] = (_lean_lock.held_with_result(f"football:{league}", league, d.get("game_id"),
                                                              game_key=f"football:{league}:{d.get('game_id')}")
                                  or _card_lean("football", _ep_league, [_rec] if _rec else [], _held))
            except Exception:
                d["card_lean"] = None
        from ._livefreeze import freeze_live_paper
        freeze_live_paper(d, league)
        games.append(d)
        with _GAME_CACHE_LOCK:
            _GAME_CACHE[(league, _gid)] = (_now_mono, d.get("status"), dict(d))

    # Write-side (record picks + paper shadow, settle, snapshot closing lines)
    # is pure bookkeeping — split OFF the compute (Austin 2026-09-19) so a cold
    # slate fills in on predict+price alone (~4x faster). Runs in its own guarded
    # thread: never blocks the display, never overlaps itself.
    _spawn_writeside(league, games, today, _did_ingest)

    # Lock + fetch today's paper POTD synchronously (Austin 2026-09-19) so the
    # DISPLAYED pick-of-the-day is stable from the first load, not only after the
    # async write-side runs. record_potd is idempotent (INSERT OR IGNORE).
    _locked_potd = None
    try:
        from engine.football._tracker import _pick_potd, record_potd, get_locked_potd
        _potd = _pick_potd(games, league)
        if _potd:
            record_potd(league, today, _potd)
        _locked_potd = get_locked_potd(league, today)
    except Exception as _pe:
        logger.debug("[football:%s] POTD display-lock failed: %s", league, _pe)

    return {
        "league":       league,
        "display_name": cfg.get("display_name") or league,
        "date":         target,
        "games":        games,
        "locked_potd":  _locked_potd,
    }


@router.post("/api/football/{league}/tracker/settle")
def api_football_tracker_settle(league: str) -> dict:
    from engine.football import LEAGUE_REGISTRY
    from engine.football._tracker import settle_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    return settle_picks(league)


@router.get("/api/football/{league}/standings")
def api_football_standings(league: str) -> list[dict]:
    """Standings — W-L record + points-for/against + L10 + streak.
    Mirrors the baseball / basketball framework shape so the shared
    StandingsView UI primitive renders football identically."""
    from engine.football import LEAGUE_REGISTRY, get_league_config
    if league not in LEAGUE_REGISTRY:
        return []
    from engine.football._db import get_conn
    conn = get_conn(league)
    # CURRENT-SEASON ONLY (2026-09-08 fix): the games table's `season` column is unpopulated (NULL for
    # every row), so this was summing EVERY season in the DB -> absurd records (NFL Eagles 40-17 over
    # ~3 seasons, cross-season CFB entries). Anchor a season window on the most-recent final game and
    # the league's first season month, then filter by DATE. (The Elo/model path still uses ALL games —
    # only the standings VIEW is scoped to the current season.)
    _mx = conn.execute(
        "SELECT MAX(date) AS d FROM games "
        "WHERE status = 'final' AND home_score IS NOT NULL AND away_score IS NOT NULL"
    ).fetchone()
    season_start = None
    if _mx and _mx["d"]:
        try:
            from datetime import datetime as _dt
            _ld = _dt.fromisoformat(str(_mx["d"])[:10]).date()
            _sm = (get_league_config(league).get("season_months") or (8,))[0]
            _sy = _ld.year if _ld.month >= _sm else _ld.year - 1
            season_start = "%04d-%02d-01" % (_sy, _sm)
        except Exception:
            season_start = None
    rows = conn.execute(
        "SELECT g.home_team_id, g.away_team_id, g.home_score, g.away_score, "
        "       g.date "
        "FROM games g "
        "WHERE g.status = 'final' "
        "  AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL "
        "  AND (? IS NULL OR g.date >= ?) "
        "ORDER BY g.date ASC, g.game_id ASC",
        (season_start, season_start)
    ).fetchall()
    teams_meta = {
        r["id"]: dict(r) for r in conn.execute(
            "SELECT id, name, abbreviation, logo_url FROM teams"
        ).fetchall()
    }
    state: dict[int, dict] = {}
    for r in rows:
        h = int(r["home_team_id"]); a = int(r["away_team_id"])
        hs = int(r["home_score"]); as_ = int(r["away_score"])
        for tid, scored, allowed, won, tied in (
            (h, hs, as_, hs > as_, hs == as_),
            (a, as_, hs, as_ > hs, hs == as_),
        ):
            s = state.setdefault(tid, {
                "team_id": tid, "wins": 0, "losses": 0, "ties": 0,
                "points_for": 0, "points_against": 0,
                "recent": [], "streak_type": None, "streak_len": 0,
            })
            s["points_for"] += scored
            s["points_against"] += allowed
            if tied:
                s["ties"] += 1
                result = "T"
            elif won:
                s["wins"] += 1
                result = "W"
            else:
                s["losses"] += 1
                result = "L"
            s["recent"].append(result)
            if s["streak_type"] == result:
                s["streak_len"] += 1
            else:
                s["streak_type"] = result
                s["streak_len"] = 1
    # Skip conference/all-star pseudo-teams (NFL Pro Bowl "AFC"/"NFC" ingest as teams; not real standings).
    _PSEUDO = {"AFC", "NFC"}
    out = []
    for tid, s in state.items():
        team = teams_meta.get(tid, {})
        if team.get("abbreviation") in _PSEUDO or team.get("name") in _PSEUDO:
            continue
        wins = s["wins"]; losses = s["losses"]; ties = s["ties"]
        played = wins + losses + ties
        last10 = s["recent"][-10:]
        l10_w = sum(1 for r in last10 if r == "W")
        l10_l = sum(1 for r in last10 if r == "L")
        out.append({
            "team_id":        tid,
            "name":           team.get("name"),
            "abbreviation":   team.get("abbreviation"),
            "logo_url":       team.get("logo_url"),
            "wins":           wins,
            "losses":         losses,
            "ties":           ties,
            "pct":            round((wins + 0.5 * ties) / played, 3) if played else 0.0,
            "points_for":     s["points_for"],
            "points_against": s["points_against"],
            "point_diff":     s["points_for"] - s["points_against"],
            "l10":            f"{l10_w}-{l10_l}",
            "streak":         (f"{s['streak_type']}{s['streak_len']}"
                                  if s["streak_type"] else "-"),
        })
    out.sort(key=lambda x: (-x["pct"], -x["point_diff"]))
    return out


@router.get("/api/football/{league}/calibration")
def api_football_calibration(league: str) -> dict:
    """Calibration tab payload: fitted Elo/Normal constants, ensemble
    weights, and per-bucket Brier rolls once enough picks settle.
    Mirrors /api/hockey/{league}/calibration shape so the React panel
    can reuse the layout."""
    from engine.football import LEAGUE_REGISTRY, get_league_config
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    cfg = get_league_config(league)
    from engine.football._db import get_conn
    conn = get_conn(league)
    settled_n = conn.execute(
        "SELECT COUNT(*) FROM picks WHERE result IN ('W','L','P') AND COALESCE(is_shadow,0)=0"
    ).fetchone()[0]
    # Ensemble weights + GBM-trained flag — pulled directly from the
    # ensemble module so the panel reports what predictions actually
    # use, not a hard-coded constant.
    from engine.football._ensemble import _load_weights
    from engine.football._gbm import _is_trained
    gbm_trained = _is_trained(league)
    weights = _load_weights(league, gbm_trained)

    constants = {
        "home_advantage":   cfg.get("home_advantage"),
        "home_advantage_pts": cfg.get("home_advantage_pts"),
        "league_avg_total": cfg.get("league_avg_total"),
        "margin_sigma":     cfg.get("margin_sigma"),
        "total_sigma":      cfg.get("total_sigma"),
        "fitted_n":         cfg.get("fitted_n"),
        "status":           cfg.get("status"),
    }
    return {
        "league":    league,
        "constants": constants,
        "ensemble":  {**weights, "gbm_trained": gbm_trained},
        "n_settled": settled_n,
        "buckets":   [],
    }


# -- football unified tracker (MintyBot 2026-10-22, NBA "tracker merge" -> football) --
# ONE per-league tracker = the card's single BEST per game = the merged CALIBRATED
# best across all game markets (Full Game core: ML/SPREAD/TOTAL + the EXTRA
# period / alt-line / team-total markets), GATED like NBA/NHL: core full-game always
# eligible; an EXTRA market may only HEADLINE once it has EARNED a track record
# (>=60 settled / >=30 days, calibration_gate.headline_eligible) AND clears the +EV
# floor. PAPER / display only -- reads the league's is_shadow=1 rows in its picks
# table; NEVER the real (is_shadow=0) picks. League-scoped (nfl/cfb/ufl). cal_edge is
# keyed on the LEAGUE (nfl/cfb/ufl -> the shared football calibrator). PURELY ADDITIVE:
# only new fns + one route; the market_weight anchor wiring / _picks / _derivatives are
# untouched. Forked from routes_nba.

_FB_EV_CACHE = {}  # {league: {"ts": float, "map": {bet_type: (n, days)}}}

def _football_extra_evidence_map(conn, league):
    """{bet_type: (settled_n, days_tracked)} across this league's is_shadow=1 picks.
    Cached 300s per league so the gate costs one GROUP BY per window. Mirror of
    _nba_extra_evidence_map (core bet_types are counted too but never looked up --
    only EXTRA picks consult the map)."""
    import time as _t
    c = _FB_EV_CACHE.get(league)
    if c and c["map"] and (_t.monotonic() - c["ts"]) < 300:
        return c["map"]
    m = {}
    try:
        for row in conn.execute(
                "SELECT bet_type, COUNT(*) n, MIN(date) mn, MAX(date) mx FROM picks "
                "WHERE COALESCE(is_shadow,0)=1 AND result IS NOT NULL AND result NOT IN ('V','') "
                "GROUP BY bet_type"):
            n = (row["n"] or 0)
            days = 0
            if row["mn"] and row["mx"]:
                try:
                    days = int((conn.execute("SELECT julianday(?)-julianday(?)",
                                (row["mx"], row["mn"])).fetchone()[0]) or 0)
                except Exception:
                    days = 0
            m[row["bet_type"]] = (n, days)
    except Exception:
        return {}
    _FB_EV_CACHE[league] = {"ts": _t.monotonic(), "map": m}
    return m


def _football_market_scope(bet_type, pick=None):
    """Market bucket for the tracker chip, keyed on the BET: Full Game
    (ML/SPREAD/TOTAL) vs the EXTRA markets -- Alt Line (ALT_SPREAD/ALT_TOTAL),
    Team Total (TEAM_TOTAL), Period (H1_/H2_/Q1_/1H/2H/half). Mirror of
    _nba_market_scope; the EXTRA set matches engine.football._tracker's
    _EXTRA_DERIV_TYPES + the period markets."""
    bt = (bet_type or "").strip().upper()
    if bt in ("ML", "SPREAD", "TOTAL"):
        return "Full Game"
    if bt in ("ALT_SPREAD", "ALT SPREAD", "ALT_TOTAL", "ALT TOTAL"):
        return "Alt Line"
    if bt in ("TEAM_TOTAL", "TEAM TOTAL"):
        return "Team Total"
    if (bt.startswith("H1_") or bt.startswith("H2_") or bt.startswith("Q1_")
            or bt.startswith("Q2_") or bt.startswith("1H") or bt.startswith("2H")
            or "PERIOD" in bt or "HALF" in bt):
        return "Period"
    return "Other"


def _football_flat_profit(result, odds):
    """Flat $100-stake P/L from (result, American odds) -- win: +odds if positive
    else 100/abs(odds)*100; loss: -$100; push/void: $0. Paper rows record
    stake_units~=0 so profit*stake_units would read a useless $0 -- grade every row
    flat $100 instead. Mirror of _nba_flat_profit (Austin 2026-10-22)."""
    r = (result or "").strip().upper()
    if not r:
        return None  # pending -- excluded from the P/L sum
    if r == "W":
        try:
            o = float(odds if odds is not None else -110)
        except (TypeError, ValueError):
            o = -110.0
        if o == 0:
            return 0.0
        return round(o, 2) if o > 0 else round(100.0 / abs(o) * 100.0, 2)
    if r == "L":
        return -100.0
    return 0.0  # P / PUSH / V


_LAST_FB_AUTO_SETTLE = {}  # {league: monotonic ts}
def _maybe_auto_settle_football(league):
    """Throttled, non-blocking settle so a league's picks grade shortly after games
    go final. settle_picks(league) grades BOTH real (is_shadow=0) and paper
    (is_shadow=1) rows. Fires on a tracker view, at most once per 10 min/league, in
    a daemon thread. Mirror of _maybe_auto_settle_nba."""
    import time as _t, threading as _th
    now = _t.monotonic()
    if now - _LAST_FB_AUTO_SETTLE.get(league, 0.0) < 600.0:
        return
    _LAST_FB_AUTO_SETTLE[league] = now
    def _run():
        try:
            from engine.football._tracker import settle_picks as _sp
            _sp(league)
        except Exception as _e:
            logger.warning("auto-settle football %s failed: %s", league, _e)
    _th.Thread(target=_run, daemon=True).start()


def _disp_pick(bet_type, pick, line):
    """Combine a bare side/team pick with its line for display ("Under 53.5",
    "ELON +10.5", "CHW -1.5"); ML-type picks pass through, and a pick that already
    carries its number is left alone. (MintyBot 2026-10-22: football/mlb store
    pick + line separately, so the unified tracker showed just "Over"/"ELON".)"""
    p = (pick or "").strip()
    if line is None or line == "":
        return p
    try:
        lf = float(line)
    except (TypeError, ValueError):
        return p
    toks = p.split()
    if toks and any(c.isdigit() for c in toks[-1]):
        return p
    ls = ("%g" % lf)
    bt = (bet_type or "").upper()
    if "TOTAL" in bt or bt in ("O/U", "OU"):
        return "%s %s" % (p, ls)
    if "SPREAD" in bt or bt in ("RL", "RUN_LINE", "PL", "PUCK_LINE", "SPRD"):
        return "%s %s%s" % (p, "+" if lf > 0 else "", ls)
    return p


def _football_merged_recorded_best(league, game_id, date=None, conn=None, ev_map=None):
    """The game's single BEST recorded PAPER pick = the merged CALIBRATED best
    across all game markets (Full Game core + Period / Alt / Team-Total extras),
    GATED: core full-game always eligible; an EXTRA market only once it has EARNED a
    track record (headline_eligible) AND clears the +EV floor. Reads the league's
    is_shadow=1 picks ONLY; NEVER the real (is_shadow=0) picks. Chip-shaped +
    flat-$100 profit + _scope. Pass conn/ev_map to share them across many games.
    Mirror of _nba_merged_recorded_best; cal_edge keyed on the league."""
    import sqlite3 as _sq
    from engine.football._db import get_conn
    from engine.calibration_gate import cal_edge as _ce, headline_eligible as _he
    if conn is None:
        try:
            conn = get_conn(league); conn.row_factory = _sq.Row
        except Exception:
            return None
    if ev_map is None:
        ev_map = _football_extra_evidence_map(conn, league)
    dq = " AND date = ?" if date else ""
    args = (str(game_id), date) if date else (str(game_id),)
    cands = []
    try:
        # The ABS(edge)<=20 guard drops a blown-up calibration edge (mirror NBA/NHL).
        for r in conn.execute(
                "SELECT bet_type,pick,line,odds,edge,model_prob,result,profit,stake_units,closing_odds "
                "FROM picks WHERE game_id=?" + dq + " "
                "AND COALESCE(is_shadow,0)=1 AND (result IS NULL OR result != 'V') "
                "AND ABS(COALESCE(edge,0))<=20.0", args):
            d = dict(r); d["_scope"] = _football_market_scope(d.get("bet_type"), d.get("pick"))
            d["_extra"] = (d["_scope"] != "Full Game"); cands.append(d)
    except Exception:
        return None
    if not cands:
        return None
    scored = []
    for p in cands:
        ce = _ce(league, p.get("bet_type"), p.get("model_prob"), p.get("odds"),
                 pick_text=p.get("pick"), raw_edge=p.get("edge"))
        n = days = None
        if p["_extra"]:
            n, days = ev_map.get(p["bet_type"], (0, 0))
        eligible = _he(ce, p["_extra"], n, days)
        scored.append((eligible, ce if ce is not None else -999.0, p))
    pool = [sc for sc in scored if sc[0]]
    if not pool:
        return None
    b = max(pool, key=lambda sc: sc[1])[2]
    return {"type": b["bet_type"], "market": b["bet_type"], "pick": _disp_pick(b["bet_type"], b["pick"], b.get("line")),
            "odds": b["odds"], "edge": b["edge"], "prob": b["model_prob"],
            "result": b["result"], "paper": True, "_scope": b["_scope"],
            "closing_odds": b.get("closing_odds"),
            "profit": _football_flat_profit(b["result"], b["odds"]),
            "stake_units": 1.0}


def _football_recorded_authority_pick(league, game_id, conn, date=None):
    """The game's recorded AUTHORITY pick = the FIRST non-void is_shadow=0 row = the
    select_pick headline the card showed when the write-side recorded it. FIRST PICK WINS
    (Austin 2026-11-01 "one pick, read everywhere"): record_picks never rewrites it, and the
    card holds THIS function's result pre-kickoff, live and final, so tracker == card.
    (Was latest-created, which followed the old void-and-replace recorder's last swap.)
    Replaces the shadow-pool re-derivation (_football_merged_recorded_best)."""
    dq = " AND date = ?" if date else ""
    args = (str(game_id), date) if date else (str(game_id),)
    try:
        r = conn.execute(
            "SELECT bet_type,pick,line,odds,edge,model_prob,result,closing_odds "
            "FROM picks WHERE game_id=?" + dq + " AND COALESCE(is_shadow,0)=0 "
            "AND (result IS NULL OR result != 'V') "
            "ORDER BY created_at ASC, id ASC LIMIT 1", args).fetchone()
    except Exception:
        return None
    if not r:
        return None
    d = dict(r)
    scope = _football_market_scope(d.get("bet_type"), d.get("pick"))
    return {"type": d["bet_type"], "market": d["bet_type"],
            "pick": _disp_pick(d["bet_type"], d["pick"], d.get("line")),
            "odds": d["odds"], "edge": d["edge"], "prob": d["model_prob"],
            "result": d["result"], "paper": True, "_scope": scope,
            "closing_odds": d.get("closing_odds"),
            "profit": _football_flat_profit(d["result"], d["odds"]),
            "stake_units": 1.0}


@router.get("/api/football/{league}/tracker/unified")
def api_football_tracker_unified(league: str, limit: int = 300):
    """ONE football tracker (per league) = the card's 'Best' per game (the merged
    CALIBRATED best across Full Game + Period / Alt / Team-Total). Mirrors the NBA/NHL
    tracker merge. Reads the league's PAPER (is_shadow=1) picks ONLY; NEVER the real
    picks. One row per game (the merged best), via _football_merged_recorded_best; P/L
    graded FLAT $100. Read-only; per-market endpoints untouched."""
    from engine.football import LEAGUE_REGISTRY
    empty = {"rows": [], "summary": {"all": {"n": 0, "wins": 0, "losses": 0,
             "pushes": 0, "pending": 0, "profit": 0.0, "win_pct": 0.0}}, "by_market": {}}
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}", **empty}
    _maybe_auto_settle_football(league)
    import sqlite3 as _sq
    from engine.football._db import get_conn
    conn = get_conn(league); conn.row_factory = _sq.Row
    # {abbr: logo_url} built ONCE per request. Football unified rows didn't carry
    # logos, so CFB (ESPN CFB logos are team-id keyed -> excluded from the abbr CDN
    # path; "CFB relies on the payload") rendered abbr-boxes. teams.logo_url already
    # holds the ncaa/500/<id>.png URL per abbreviation. Only store non-empty URLs so
    # a colliding CFB abbr (e.g. OSU) keeps a real logo. (MintyBot 2026-10-22)
    _logo_by_abbr = {}
    try:
        for _lr in conn.execute("SELECT abbreviation, logo_url FROM teams"):
            if _lr[0] and _lr[1]:
                _logo_by_abbr[str(_lr[0]).strip().upper()] = _lr[1]
    except Exception:
        _logo_by_abbr = {}
    try:
        gids = [r[0] for r in conn.execute(
            "SELECT game_id, MAX(created_at) c FROM picks "
            "WHERE COALESCE(is_shadow,0)=0 AND (result IS NULL OR result != 'V') "
            "GROUP BY game_id ORDER BY c DESC LIMIT ?", (limit,))]
    except Exception as e:
        logger.warning("football %s unified tracker game list failed: %s", league, e)
        gids = []
    evm = _football_extra_evidence_map(conn, league)
    rows = []
    for gid in gids:
        best = _football_recorded_authority_pick(league, gid, conn=conn)
        if not best:
            continue
        meta = conn.execute("SELECT matchup,date FROM picks WHERE game_id=? AND COALESCE(is_shadow,0)=0 AND (result IS NULL OR result != 'V') ORDER BY created_at ASC, id ASC LIMIT 1", (str(gid),)).fetchone()
        _mu = (meta["matchup"] if meta else None) or ""
        # matchup is "AWAY@HOME" (abbreviations); split + resolve each side's logo from
        # teams.logo_url so CFB rows carry ESPN ncaa URLs (MatchupCell uses espnLogo).
        _away_ab, _home_ab = ((_mu.split("@", 1) + [""])[:2]) if "@" in _mu else ("", "")
        rows.append({
            "game_id": gid,
            "date": meta["date"] if meta else None,
            "matchup": meta["matchup"] if meta else None,
            "home_logo": _logo_by_abbr.get(_home_ab.strip().upper()),
            "away_logo": _logo_by_abbr.get(_away_ab.strip().upper()),
            "market": best.get("_scope"),
            "bet_type": best["type"], "pick": best["pick"], "odds": best["odds"],
            "edge": best["edge"], "result": best["result"], "profit": best.get("profit"),
            "model_prob": best.get("prob"), "closing_odds": best.get("closing_odds"),
            "stake_units": best.get("stake_units"),
        })

    def _sum(rs):
        w = sum(1 for r in rs if r["result"] == "W")
        l = sum(1 for r in rs if r["result"] == "L")
        pu = sum(1 for r in rs if r["result"] in ("P", "PUSH"))
        pend = sum(1 for r in rs if not r["result"])
        prof = round(sum((r["profit"] or 0) for r in rs if r["result"]), 2)
        dec = w + l
        return {"n": len(rs), "wins": w, "losses": l, "pushes": pu, "pending": pend,
                "profit": prof, "win_pct": round(100.0 * w / dec, 1) if dec else 0.0}

    by_market = {}
    for r in rows:
        by_market.setdefault(r["market"] or "?", []).append(r)
    return {
        "rows": rows,
        "summary": {"all": _sum(rows)},
        "by_market": {m: _sum(rs) for m, rs in by_market.items()},
    }


@router.get("/api/football/{league}/tracker/history")
def api_football_tracker_history(league: str, limit: int = 200) -> dict:
    from engine.football import LEAGUE_REGISTRY
    from engine.football._tracker import list_history, settle_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    try:
        settle_picks(league)
    except Exception as e:
        logger.debug("[football:%s] settle on history failed: %s",
                      league, e)
    try:
        from engine.football._tracker import paper_summary, list_history as _paper_list
        from engine.pick_of_day import settle_potd, get_potd_summary
        settle_potd(league)
        paper = {"summary": paper_summary(league),
                 "rows": _paper_list(league, limit=limit, shadow=1),
                 "potd_record": get_potd_summary(league)}
    except Exception as e:
        logger.debug("[football:%s] paper shadow on history failed: %s",
                      league, e)
        paper = {"summary": {}, "rows": []}
    rows = list_history(league, limit=limit, shadow=0)
    # Project stake-weighted P/L onto each returned row so the table
    # column matches the summed totals below.
    rows_out = []
    for r in rows:
        d = dict(r) if not isinstance(r, dict) else dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        rows_out.append(d)

    from engine.football._db import get_conn
    conn = get_conn(league)
    all_picks = conn.execute(
        "SELECT bet_type, result, profit, stake_units FROM picks "
        "WHERE COALESCE(is_shadow,0)=0"
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
            b["wins"] += 1; wins += 1; b["profit"] += pr; profit += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        elif res == "L":
            b["losses"] += 1; losses += 1; b["profit"] += pr; profit += pr
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
            "wins": wins, "losses": losses, "pushes": pushes,
            "pending": pending,
            "profit": round(profit, 2),
            "win_pct": round(wins / decided * 100, 1) if decided else 0.0,
            "roi": round(profit / stake_settled, 1) if stake_settled else 0.0,
        },
        "by_type": by_type,
    }
    return {"league": league, "rows": rows_out, "summary": summary, "paper": paper}
