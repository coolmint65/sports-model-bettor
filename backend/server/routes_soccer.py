"""Soccer backend routes.

Per-league read endpoints. Pattern mirrors the per-sport routes — one ingest +
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
        # NAV PERF (2026-10-11, MintyBot): the sidebar fetches /soccer/leagues on
        # every page load via a one-shot useOnce — a COLD call doing these live
        # ESPN + HR per-league count lookups ran ~8s and timed out the nav fetch,
        # so soccer (EPL) silently vanished from the sidebar. The badge only needs
        # a game count, and the DB (fresh from the worker ingest) has it; dropping
        # the synchronous network fallbacks from the nav path makes it return
        # instantly. An empty-DB league just shows 0 until the next ingest — fine
        # for a nav badge (opening the slate still hits ESPN/HR live elsewhere).
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
            # Austin 2026-09-19: World Cup over + FIFA internationals no edge — off the sidebar.
            if k not in ("fifa_internationals", "fifa_world_cup")
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


from backend.server._slate_cache import SlateCache
_SOCCER_CACHE = SlateCache("soccer")


# Pick Authority shadow lane for PA soccer leagues (EPL, 2026-10-28 -- the NBA / NHL / college design):
# every market re-priced on the MODEL's own probability (W=1) and mirrored to picks_unified as
# is_shadow=1, so unproven markets build a settled record and can graduate. Pre-kickoff matches
# only; at most once per _SOC_SHADOW_EVERY_S per match (record_shadow_slate keeps the FIRST row
# per match + market, so this is about cost, not double counting).
_SOC_SHADOW_EVERY_S = 1800
_SOC_SHADOW_LAST: dict = {}
_SOC_SHADOW_CANDS: dict = {}   # (league, match_id) -> last W=1 candidates: the card's model lean (NBA design)


def _soccer_pa_shadow(league, matches):
    import time as _t
    from datetime import datetime as _dt, timezone as _tz
    try:
        from engine.soccer import _market_anchor as _MA
        from engine.soccer._picks import generate_picks_for_match
        from engine.soccer._predict import predict_match
        from engine.picks_unified import record_shadow_slate
    except Exception as e:
        logger.debug("[soccer:%s] PA shadow import failed: %s", league, e)
        return
    now = _dt.now(_tz.utc)
    for m in matches or []:
        mid = m.get("match_id")
        st = m.get("start_time")
        try:
            if not mid or not st or _dt.fromisoformat(str(st).replace("Z", "+00:00")) <= now:
                continue
        except Exception:
            continue
        if _t.time() - _SOC_SHADOW_LAST.get((league, mid), 0.0) < _SOC_SHADOW_EVERY_S:
            continue
        if not (m.get("odds") or {}):
            continue
        try:
            from engine.soccer._db import get_conn as _gc
            row = _gc(league).execute(
                "SELECT home_team_id, away_team_id, neutral_site, home_side, date FROM matches WHERE id = ?",
                (int(mid),)).fetchone()
            if not row:
                continue
            pred = predict_match(league, int(row["home_team_id"]), int(row["away_team_id"]),
                                 neutral_site=bool(row["neutral_site"]), home_side=row["home_side"])
            pred.update(match_id=int(mid), home_abbr=(m.get("home") or {}).get("abbr"),
                        away_abbr=(m.get("away") or {}).get("abbr"))
            with _MA.shadow_mode():
                picks = generate_picks_for_match(league, pred, m.get("odds") or {})
            cands = []
            for p in picks:
                cands.extend(p.pop("_candidates", None) or [])
            record_shadow_slate("soccer", league, mid, m.get("matchup"), cands or picks, pick_date=row["date"])
            _SOC_SHADOW_CANDS[(league, mid)] = cands or picks
            _SOC_SHADOW_LAST[(league, mid)] = _t.time()
        except Exception as e:
            logger.debug("[soccer:%s] PA shadow %s failed: %s", league, mid, e)


@router.get("/api/soccer/{league}/today")
def api_soccer_today(league: str) -> dict:
    from engine.soccer import LEAGUE_REGISTRY
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    return _SOCCER_CACHE.serve((league,), lambda: _compute_api_soccer_today(league),
                               {"league": league, "matches": [], "loading": True})


def _compute_api_soccer_today(league: str) -> dict:
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
            # Forward look-ahead (Austin 2026-09-19): European leagues play tonight +
            # this weekend, but ingest_today only pulled the current day, so the
            # fall-forward below found nothing (upcoming games were never ingested).
            # Pull the next 6 days so tonight's + weekend fixtures surface. Mirrors football.
            for fwd in range(1, 7):
                d = (today_dt + _td0(days=fwd)).strftime("%Y-%m-%d")
                try:
                    ingest_today(league, date=d)
                except Exception as e:
                    logger.debug("[soccer:%s] lookahead %s failed: %s",
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
    # fresh without a separate cron. Live matches
    # skip record_picks — their picks were already locked in when the
    # match was scheduled, and re-recording would be a no-op against
    # the open-pick unique index anyway.
    try:
        from engine.soccer._tracker import record_picks, settle_picks
        from engine.soccer import _market_anchor as _soc_ma0
        _pa_rec = _soc_ma0.is_pa(league)
        for m in slate["matches"]:
            if m.get("_locked"):
                continue
            if _pa_rec:
                continue   # PA: records the card's held authority best_pick after selection (2026-11-01)
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

    # Step 3 (Pick Authority, Austin 2026-10-17): route each match's selected
    # pick through the ONE authority (apply_selected) so the BACKEND sets the
    # authoritative best_pick and the card reads THAT -- no more frontend
    # re-selection (the adaptBetShape max-edge reduce) that let the card diverge
    # from the tracker (AVL: card showed full-game DNB, tracker had H1 Double
    # Chance). picks[0] is the slate's top pick across ALL markets (incl H1);
    # apply_selected keeps the pick's edge when cal_edge has no soccer fit, so
    # this never corrupts the number.
    # Pick Authority (Austin 2026-10-18 "apply this to EPL/soccer"): ONE proven-
    # edge-gated selector (edge x prob). Soccer proven markets = ML (+8.3%) +
    # Double Chance (+7.6%); the rest (OU/DNB/AH/BTTS/H1_*) lose -> NO-PLAY as a
    # headline. record_picks below still records ALL candidates (is_shadow=0) so
    # every market keeps its W-map track record + can graduate.
    try:
        from engine.selection import select_pick as _authority_select
        from engine.soccer import _market_anchor as _soc_ma
        _pa = _soc_ma.is_pa(league)
        if _pa:
            _soccer_pa_shadow(league, (slate.get("matches") or []) + (upcoming or []))
        for _m in (slate.get("matches") or []) + (upcoming or []):
            # PA leagues select over EVERY gated candidate, not each family's top-1 (2026-10-28)
            _cands = [p for p in ((_m.get("candidates") if _pa else None) or _m.get("picks") or []) if p]
            _m["best_pick"] = _authority_select("soccer", league, _cands)
            # Card == tracker (Austin 2026-11-01 "one pick, read everywhere"): once the match has a
            # RECORDED authority pick, the headline HOLDS it at its entry price (pre-game, live, final)
            # instead of re-selecting as lines move. /tracker/unified reads the SAME function.
            if _pa:
                try:
                    _rec = _soccer_recorded_authority_best(league, _m.get("match_id"))
                except Exception:
                    _rec = None
                if _rec:
                    from engine.config import EDGE_STRONG, EDGE_MODERATE
                    _re = float(_rec.get("edge") or 0)
                    _m["best_pick"] = {**_rec, "model_prob": _rec.get("prob"), "raw_prob": _rec.get("prob"),
                                       "stake_units": _rec.get("recorded_stake_units"),
                                       "held": True, "locked": True,
                                       "confidence": ("strong" if _re >= EDGE_STRONG else "moderate" if _re >= EDGE_MODERATE else "lean")}
            try:
                from engine.selection import card_lean as _card_lean
                _lean_src = (_SOC_SHADOW_CANDS.get((league, _m.get("match_id"))) if _pa else None) or _cands
                _m["card_lean"] = _card_lean("soccer", league, _lean_src, _m["best_pick"])  # card's best lean (Austin 2026-10-26): graduated pick, else the ungraduated lean -- display only
                from engine.selection import lean_lock as _lean_lock
                _m["card_lean"] = _lean_lock.hold(f"soccer:{league}", _m.get("match_id"), _m["card_lean"])   # first lean held through live + final (Austin 2026-10-27)
            except Exception:
                _m["card_lean"] = None
    except Exception as _e:
        logger.debug("[soccer:%s] select_pick fold-in failed: %s", league, _e)

    # Live / final state + score per match, and the held lean's grade once it's over (Austin 2026-10-31:
    # "these games aren't grading, and prematch also"). The slate rows never carried status or score, so
    # every soccer card rendered as pre-game forever and the lean never showed W/L.
    try:
        from engine.soccer import _market_anchor as _soc_ma2
        _soc_pa_card = _soc_ma2.is_pa(league)
    except Exception:
        _soc_pa_card = False
    try:
        _sc = _soc_conn(league)
        for _m in (slate.get("matches") or []) + (upcoming or []):
            _r = _sc.execute("SELECT status, status_detail, home_score, away_score, home_score_ht, away_score_ht "
                             "FROM matches WHERE id = ?", (_m.get("match_id"),)).fetchone()
            if not _r:
                continue
            _st = (_r["status"] or "scheduled").lower()
            _m["status"] = _st
            _m["status_detail"] = _r["status_detail"]
            if _st in ("live", "halftime", "final"):
                _m["home_score"], _m["away_score"] = _r["home_score"], _r["away_score"]
                _m["home_score_ht"], _m["away_score_ht"] = _r["home_score_ht"], _r["away_score_ht"]
                try:
                    from engine.selection import lean_lock as _lean_lock
                    _held = _lean_lock.held_with_result(f"soccer:{league}", league, _m.get("match_id"),
                                                        game_key=f"soccer:{league}:{_m.get('match_id')}")
                    if _held and not (_m.get("best_pick") or {}).get("held"):
                        _m["card_lean"] = _held
                except Exception:
                    pass
                if _soc_pa_card and not (_m.get("best_pick") or {}).get("held"):
                    _m["best_pick"] = None   # never a live re-selection as the headline (2026-11-01)
    except Exception as _e:
        logger.debug("[soccer:%s] status/score fold-in failed: %s", league, _e)

    # PA recorder (2026-11-01): record the card's authority headline (best_pick) -- first pick wins --
    # for today's pre-kickoff matches; once recorded the card holds it, so a re-hit is a no-op.
    try:
        from engine.soccer import _market_anchor as _soc_ma1
        if _soc_ma1.is_pa(league):
            from datetime import datetime as _dtr, timezone as _tzr
            from engine.soccer._tracker import record_picks as _rp
            _nowr = _dtr.now(_tzr.utc)
            for _m in (slate.get("matches") or []) + (upcoming or []):   # every card that can headline a pick
                _bp = _m.get("best_pick")
                if not _bp or _bp.get("held") or (_m.get("status") or "scheduled") in ("live", "halftime", "final", "postponed", "cancelled"):
                    continue
                try:
                    if _dtr.fromisoformat(str(_m.get("start_time")).replace("Z", "+00:00")) <= _nowr:
                        continue
                except Exception:
                    continue
                _rp(league, _m["match_id"], [dict(_bp, matchup=_bp.get("matchup") or _m.get("matchup"))])
    except Exception as _e:
        logger.warning("[soccer:%s] PA authority record failed: %s", league, _e)

    # Card % = the Pick Authority's market blend of the 1X2 model with the de-vigged 1X2 board (Austin
    # 2026-10-26 "Any percentages for any card in any sport, should be that way").
    try:
        from ._display_cal import market_card_wp3
        for _m in (slate.get("matches") or []) + (upcoming or []):
            _p = _m.get("prediction") or {}
            _ml = (_m.get("odds") or {}).get("ml") or {}
            _m["card_win_prob"] = market_card_wp3("soccer", league, _p.get("p_home"), _p.get("p_draw"),
                                                  _p.get("p_away"), _ml.get("home"), _ml.get("draw"), _ml.get("away"))
    except Exception as _e:
        logger.debug("[soccer:%s] card blend failed: %s", league, _e)

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


# -- soccer unified tracker (MintyBot 2026-10-22, NBA "tracker merge" -> soccer) --
# ONE per-league tracker = the card's single BEST per match = the merged CALIBRATED
# best across all markets (Full Game core: ML/OU/DNB/AH/DC/BTTS + the First-Half H1_*
# extras), GATED like NBA/NHL: core full-game always eligible; an EXTRA (H1_*) market
# may only HEADLINE once it has EARNED a track record (>=60 settled / >=30 days,
# calibration_gate.headline_eligible) AND clears the +EV floor. PAPER / display only:
# soccer is a paper-only model, so EVERY pick in the league's `picks` table IS a paper
# pick (no is_shadow split, no real money). League-scoped (eng_premier etc.). cal_edge
# is keyed on the LEAGUE; soccer has no calibrator fit, so cal_edge falls back to the
# stored raw edge (gate still enforces the +EV floor). Forked from routes_nba.

_SOC_EV_CACHE = {}  # {league: {"ts": float, "map": {bet_type: (n, days)}}}

def _soccer_extra_evidence_map(conn, league):
    """{bet_type: (settled_n, days_tracked)} for the EXTRA (H1_*) markets in this
    league's picks table. Cached 300s per league so the gate costs one GROUP BY per
    window. Mirror of _nba_extra_evidence_map (soccer has one picks table)."""
    import time as _t
    c = _SOC_EV_CACHE.get(league)
    if c and c["map"] and (_t.monotonic() - c["ts"]) < 300:
        return c["map"]
    m = {}
    try:
        for row in conn.execute(
                "SELECT bet_type, COUNT(*) n, MIN(date) mn, MAX(date) mx FROM picks "
                "WHERE result IS NOT NULL AND result != 'V' AND UPPER(bet_type) LIKE 'H1\\_%' ESCAPE '\\' "
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
    _SOC_EV_CACHE[league] = {"ts": _t.monotonic(), "map": m}
    return m


def _soccer_market_scope(bet_type, pick=None):
    """Market bucket for the tracker chip, keyed on the BET: First Half (H1_*) vs
    Full Game (ML/OU/DNB/AH/DC/BTTS/...). Mirror of _nba_market_scope."""
    bt = (bet_type or "").strip().upper()
    if bt.startswith("H1_") or bt.startswith("H1 "):
        return "First Half"
    return "Full Game"


def _soccer_flat_profit(result, odds):
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


_LAST_SOC_AUTO_SETTLE = {}  # {league: monotonic ts}
def _maybe_auto_settle_soccer(league):
    """Throttled, non-blocking settle so a league's paper picks grade shortly after
    matches go final. Fires on a tracker view, at most once per 10 min/league, in a
    daemon thread. Mirror of _maybe_auto_settle_nba."""
    import time as _t, threading as _th
    now = _t.monotonic()
    if now - _LAST_SOC_AUTO_SETTLE.get(league, 0.0) < 600.0:
        return
    _LAST_SOC_AUTO_SETTLE[league] = now
    def _run():
        try:
            from engine.soccer import settle_picks as _sp
            _sp(league)
        except Exception as _e:
            logger.warning("auto-settle soccer %s failed: %s", league, _e)
    _th.Thread(target=_run, daemon=True).start()


def _soccer_merged_recorded_best(league, match_id, date=None, conn=None, ev_map=None):
    """The match's single BEST recorded PAPER pick = the merged CALIBRATED best
    across all markets (Full Game core + First-Half H1_*), GATED: core full-game
    always eligible; an EXTRA (H1_*) market only once it has EARNED a track record
    (headline_eligible) AND clears the +EV floor. soccer is paper-only, so the
    league's `picks` table IS the paper store. Chip-shaped + flat-$100 profit +
    _scope. Pass conn/ev_map to share them across many matches. Mirror of
    _nba_merged_recorded_best; cal_edge keyed on the league."""
    import sqlite3 as _sq
    from engine.soccer._db import get_conn
    from engine.calibration_gate import cal_edge as _ce, headline_eligible as _he
    if conn is None:
        try:
            conn = get_conn(league); conn.row_factory = _sq.Row
        except Exception:
            return None
    if ev_map is None:
        ev_map = _soccer_extra_evidence_map(conn, league)
    dq = " AND date = ?" if date else ""
    args = (str(match_id), date) if date else (str(match_id),)
    cands = []
    try:
        # The ABS(edge)<=20 guard drops a blown-up calibration edge (mirror NBA/NHL).
        for r in conn.execute(
                "SELECT bet_type,pick,odds,edge,model_prob,result,profit,stake_units,closing_odds "
                "FROM picks WHERE match_id=?" + dq + " "
                "AND (result IS NULL OR result != 'V') AND ABS(COALESCE(edge,0))<=20.0", args):
            d = dict(r); d["_scope"] = _soccer_market_scope(d.get("bet_type"), d.get("pick"))
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
    return {"type": b["bet_type"], "market": b["bet_type"], "pick": b["pick"],
            "odds": b["odds"], "edge": b["edge"], "prob": b["model_prob"],
            "result": b["result"], "paper": True, "_scope": b["_scope"],
            "closing_odds": b.get("closing_odds"),
            "profit": _soccer_flat_profit(b["result"], b["odds"]),
            "stake_units": 1.0}


def _soccer_recorded_authority_best(league, match_id, date=None, conn=None):
    """The match's RECORDED AUTHORITY pick = EXACTLY what the card headlined, read back from the
    league's `picks` table (Austin 2026-11-01 "one pick, read everywhere"; mirror of
    _nba_recorded_authority_best). Shared by the card hold (_compute_api_soccer_today) and
    /api/soccer/{league}/tracker/unified, so the two can never disagree.

    PA leagues (EPL): the /today recorder writes the card's select_pick headline (best_pick) only,
    first pick wins (engine.soccer._tracker.record_picks family lock + the card hold) -> the FIRST
    recorded non-void row IS the headline. (Rows before 2026-11-01 were the per-family max-edge heads;
    the first one still wins.) Other leagues: the per-family heads the card selected from -> max by
    edge x prob (the card's ranking). NO cal_edge / headline_eligible regate, NO edge guard.
    Chip-shaped + flat-$100 profit + _scope (tracker row shape unchanged)."""
    import sqlite3 as _sq
    from engine.soccer._db import get_conn
    from engine.soccer import _market_anchor as _ma
    if conn is None:
        try:
            conn = get_conn(league); conn.row_factory = _sq.Row
        except Exception:
            return None
    if match_id is None or match_id == "":
        return None
    dq = " AND date = ?" if date else ""
    args = (str(match_id), date) if date else (str(match_id),)
    rows = []
    try:
        for r in conn.execute(
                "SELECT id,bet_type,pick,odds,edge,model_prob,result,stake_units,closing_odds "
                "FROM picks WHERE match_id=?" + dq + " "
                "AND (result IS NULL OR result != 'V') ORDER BY id", args):
            d = dict(r); d["_scope"] = _soccer_market_scope(d.get("bet_type"), d.get("pick"))
            rows.append(d)
    except Exception:
        return None
    if not rows:
        return None
    if _ma.is_pa(league):
        b = rows[0]   # first recorded headline wins
    else:
        b = max(rows, key=lambda p: float(p.get("edge") or 0) * float(p.get("model_prob") or 0))
    return {"type": b["bet_type"], "market": b["bet_type"], "pick": b["pick"],
            "odds": b["odds"], "edge": b["edge"], "prob": b["model_prob"],
            "result": b["result"], "paper": True, "_scope": b["_scope"],
            "closing_odds": b.get("closing_odds"),
            "profit": _soccer_flat_profit(b["result"], b["odds"]),
            "stake_units": 1.0, "recorded_stake_units": b.get("stake_units")}


@router.get("/api/soccer/{league}/tracker/unified")
def api_soccer_tracker_unified(league: str, limit: int = 300):
    """ONE soccer tracker (per league) = the card's 'Best' per match (the merged
    CALIBRATED best across Full Game + First-Half H1_*). Mirrors the NBA/NHL tracker
    merge. soccer is paper-only, so the league's `picks` table IS the paper store.
    One row per match (the merged best), via _soccer_merged_recorded_best; P/L graded
    FLAT $100. Read-only; per-market endpoints untouched."""
    from engine.soccer import LEAGUE_REGISTRY
    empty = {"rows": [], "summary": {"all": {"n": 0, "wins": 0, "losses": 0,
             "pushes": 0, "pending": 0, "profit": 0.0, "win_pct": 0.0}}, "by_market": {}}
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}", **empty}
    _maybe_auto_settle_soccer(league)
    import sqlite3 as _sq
    from engine.soccer._db import get_conn
    conn = get_conn(league); conn.row_factory = _sq.Row
    # Team logos (MintyBot 2026-10-23): unified tracker rows lacked crests
    # (only the history endpoint built them). Mirror it: abbr -> teams.logo_url.
    _logo_by_abbr = {}
    try:
        for _ab, _lg in conn.execute("SELECT abbreviation, logo_url FROM teams").fetchall():
            if _ab:
                _logo_by_abbr[str(_ab)] = _lg
    except Exception:
        _logo_by_abbr = {}
    try:
        mids = [r[0] for r in conn.execute(
            "SELECT match_id, MAX(created_at) c FROM picks "
            "GROUP BY match_id ORDER BY c DESC LIMIT ?", (limit,))]
    except Exception as e:
        logger.warning("soccer %s unified tracker match list failed: %s", league, e)
        mids = []
    evm = _soccer_extra_evidence_map(conn, league)
    rows = []
    for mid in mids:
        best = _soccer_recorded_authority_best(league, mid, conn=conn)  # == the card's held headline (2026-11-01)
        if not best:
            continue
        meta = conn.execute("SELECT matchup,date FROM picks WHERE match_id=? LIMIT 1", (str(mid),)).fetchone()
        _mu = meta["matchup"] if meta else None
        _aa = _ha = None
        if _mu and " @ " in _mu:
            _aa, _ha = [x.strip() for x in _mu.split(" @ ", 1)]
        rows.append({
            "game_id": mid,
            "date": meta["date"] if meta else None,
            "matchup": _mu,
            "a_abbr": _aa, "h_abbr": _ha,
            "away_logo": _logo_by_abbr.get(_aa) if _aa else None,
            "home_logo": _logo_by_abbr.get(_ha) if _ha else None,
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
    # Team logos for tracker rows (Austin 2026-10-17: EPL tracker showed abbr
    # badges, no crests). Mirror football h_abbr/a_abbr/home_logo/away_logo so
    # PickHistory renders the ESPN crests. abbr -> logo_url from the teams table.
    from engine.soccer._db import get_conn as _soc_conn0
    _logo_by_abbr: dict = {}
    try:
        for _ab, _lg in _soc_conn0(league).execute(
                "SELECT abbreviation, logo_url FROM teams").fetchall():
            if _ab:
                _logo_by_abbr[str(_ab)] = _lg
    except Exception as _e:
        logger.debug("[soccer:%s] tracker logo map failed: %s", league, _e)
    rows_out = []
    for r in rows:
        d = dict(r) if not isinstance(r, dict) else dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        _mu = str(d.get("matchup") or "")
        if " @ " in _mu:
            _aa, _ha = [x.strip() for x in _mu.split(" @ ", 1)]
            d["a_abbr"], d["h_abbr"] = _aa, _ha
            d["away_logo"] = _logo_by_abbr.get(_aa)
            d["home_logo"] = _logo_by_abbr.get(_ha)
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
    # Attach game_date (= slate date, which is a fell-forward FUTURE date when the
    # league has no games today) so the cross-sport hero filters it out instead of
    # headlining e.g. Saturday's EPL game as today's pick. MintyBot 2026-10-13.
    slate = api_soccer_today(league)
    _slate_date = slate.get("date")
    existing = get_today_potd(league)
    if existing:
        return {**existing, "game_date": _slate_date} if _slate_date else existing
    bets = _build_soccer_potd_bets(slate.get("matches") or [])
    if not bets:
        return {"message": "No qualifying picks today", "sport": league}
    potd = get_or_create_potd(league, bets, db_sport="soccer", league=league)
    if potd and _slate_date:
        potd = {**potd, "game_date": _slate_date}
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
