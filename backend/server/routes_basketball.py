"""Basketball routes — exposes LEAGUE_REGISTRY to the frontend so the
nested sidebar can render every league HR offers (NBA + WNBA + NCAAM +
27 others) without hardcoding the list.

NBA's existing routes (``/api/nba/best-bets`` etc.) stay untouched.
This module is for the framework-managed leagues + cross-league reads.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime

from fastapi import APIRouter

from ._tz import et_today_str, et_now, et_month, _US_EASTERN

logger = logging.getLogger(__name__)

# Per-league monotonic timestamp of last upstream ingest. Acts as a
# request-scoped debounce: repeat slate hits inside _SLATE_INGEST_TTL_S
# skip the ingest call so we don't hammer RealGM/ESPN with one
# scrape per user click.
_SLATE_INGEST_TS: dict[str, float] = {}
_SLATE_INGEST_TTL_S = 60.0

router = APIRouter()


# Statuses the slate considers "settled" or "not yet started." Anything
# outside this set is treated as in-flight (live, intermission, between
# periods) and triggers the lock-on-live behavior below.
_NOT_LIVE_STATUSES = frozenset({
    "scheduled", "pre_game", "pregame",
    "final", "complete", "completed",
    "cancelled", "postponed", "voided",
})


def _lock_basketball_live_pick(conn, d: dict, league: str) -> None:
    """When a basketball framework game is live, surface the open pick
    that was recorded pre-tipoff instead of whatever the fresh predict
    + odds run produces (or in the gated case, nothing at all). The
    tracker grades the pre-tipoff pick; the card has to match.

    Mutates ``d`` in place — ``best_pick`` / ``best_pick_full`` /
    ``best_pick_q1`` get replaced with the locked rows, and ``picks``
    is rebuilt from them.
    """
    from engine.basketball._db import picks_table
    status = (d.get("status") or "scheduled").lower()
    if status in _NOT_LIVE_STATUSES:
        return
    gid = d.get("game_id")
    if not gid:
        return
    p_tbl = picks_table(league)
    try:
        rows = conn.execute(
            f"SELECT bet_type, pick, model_prob, edge, odds "
            f"FROM {p_tbl} "
            f"WHERE game_id = ? AND result IS NULL "
            f"ORDER BY edge DESC",
            (str(gid),),
        ).fetchall()
    except Exception:
        return
    if not rows:
        return
    q1_types = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
    def _to_pick(r):
        return {
            "type":       r["bet_type"],
            "pick":       r["pick"],
            "model_prob": r["model_prob"],
            "edge":       r["edge"],
            "odds":       r["odds"],
            "locked":     True,
        }
    full_locked = next((_to_pick(r) for r in rows
                        if (r["bet_type"] or "") not in q1_types), None)
    q1_locked = next((_to_pick(r) for r in rows
                      if (r["bet_type"] or "") in q1_types), None)
    all_locked = max(
        (p for p in (full_locked, q1_locked) if p),
        key=lambda p: p.get("edge") or 0,
        default=None,
    )
    if all_locked is None:
        return
    d["best_pick_full"] = full_locked
    d["best_pick_q1"] = q1_locked
    d["best_pick"] = all_locked
    d["picks"] = [all_locked]
    d["pick_locked"] = True


def _build_potd_bets(games: list[dict], today: str) -> tuple[list[dict], dict]:
    """Reshape slate games into the bet-list shape select_potd expects.
    Returns (bets, game_date_by_id) so callers can look up the chosen
    pick's actual ET date when locking the POTD for far-east leagues."""
    bets: list[dict] = []
    game_date_by_id: dict[str, str] = {}
    for g in games:
        if g.get("status") != "scheduled":
            continue
        bp = g.get("best_pick")
        if not bp:
            continue
        bp_norm = dict(bp)
        if "prob" not in bp_norm and "model_prob" in bp_norm:
            bp_norm["prob"] = bp_norm["model_prob"]
        home_abbr = g.get("home_abbr") or "?"
        away_abbr = g.get("away_abbr") or "?"
        gid = g.get("game_id")
        if gid is not None:
            game_date_by_id[str(gid)] = g.get("date") or today
        bets.append({
            "game_id": gid,
            "matchup": f"{away_abbr} @ {home_abbr}",
            "best_pick": bp_norm,
            "home": {"name": g.get("home_name") or home_abbr,
                      "abbreviation": home_abbr},
            "away": {"name": g.get("away_name") or away_abbr,
                      "abbreviation": away_abbr},
            "time": g.get("start_time") or "",
            "venue": "",
            "status": {"state": "pre"},
        })
    return bets, game_date_by_id


def _resolve_potd_lock_date(league: str, today: str,
                              bets: list[dict],
                              game_date_by_id: dict) -> str:
    """For far-east leagues, lock the POTD under the chosen pick's
    actual ET date so today and tomorrow lookups return the same row.
    Other leagues lock under today. Centralizing this prevents the
    /potd endpoint and the slate route's lazy-create from forking."""
    from engine.basketball._config import is_far_east
    if not is_far_east(league):
        return today
    from engine.pick_of_day._select import select_potd as _sel
    chosen = _sel(league, bets, view="q1")
    if chosen and str(chosen.get("game_id")) in game_date_by_id:
        return game_date_by_id[str(chosen["game_id"])]
    return today


def _read_active_potd(league: str, today: str):
    """Today's POTD with a tomorrow-fallback for far-east leagues
    (locked under game_date which can be tomorrow ET). Single source
    of truth so /potd and the slate route can't drift."""
    from engine.basketball._config import is_far_east
    from engine.pick_of_day import get_today_potd as _potd_read
    potd = _potd_read(league, view="q1")
    if not potd and is_far_east(league):
        tmrw = (_dt.strptime(today, "%Y-%m-%d") + _td(days=1)
                ).strftime("%Y-%m-%d")
        potd = _potd_read(league, date=tmrw, view="q1")
    return potd


@router.get("/api/basketball/{league}/today")
def api_basketball_today(league: str) -> dict:
    """Return today's slate for ``league`` with per-game predictions,
    HR odds, and picks (when both prediction + odds are available).

    NBA falls through to its existing /api/nba/best-bets endpoint —
    callers should special-case the route on the frontend rather than
    routing NBA through here.
    """
    from datetime import datetime
    from engine.basketball import (
        LEAGUE_REGISTRY, predict_full, generate_picks,
    )
    from engine.basketball._db import get_conn, games_table, teams_table
    from engine.basketball._odds import fetch_league_odds
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA uses /api/nba/best-bets — don't route through here"}

    today = et_today_str()
    real_today = today  # preserved across fall-forward for stub-gate window

    # Refresh upstream BEFORE we query — but debounce per-league so
    # repeat hits inside a minute don't re-hammer RealGM/ESPN. First
    # hit pays the ingest latency (2-12s); subsequent hits inside the
    # window serve fresh-enough state immediately.
    cfg = LEAGUE_REGISTRY[league]
    _now_ts = time.monotonic()
    last_ingest = _SLATE_INGEST_TS.get(league, 0.0)
    if (_now_ts - last_ingest) >= _SLATE_INGEST_TTL_S:
        try:
            ds = cfg.get("data_source") or ""
            # `data_source: "realgm"` is historical — many leagues
            # tagged that way actually backfill via SofaScore (the
            # primary ingest path superseded RealGM for most minor
            # Euro/Asia leagues 2026-05). Route by what's actually
            # mapped: if the league has a sofascore_tournament_id
            # AND isn't in the RealGM map, take the SofaScore path so
            # results refresh and pending picks can settle.
            from engine.basketball._config import sofascore_tournament_id
            from engine.basketball._realgm_ingest import _REALGM_LEAGUES
            has_realgm = league in _REALGM_LEAGUES
            has_sofa = bool(sofascore_tournament_id(league))
            if ds == "espn":
                from engine.basketball._espn_ingest import ingest_today as _esp
                _esp(league)
                # 2-day backsweep — late-finalized games (WNBA CON@POR
                # 2026-05-18 was stuck at status='in' for two weeks
                # because the ingest only ever pulled today's scoreboard)
                # now get re-fetched so the settler sees the final.
                # Same defense pattern as soccer/baseball/football routes.
                from datetime import datetime as _dt0, timedelta as _td0
                today_dt = _dt0.now().date()
                for back in (1, 2):
                    d = (today_dt - _td0(days=back)).strftime("%Y-%m-%d")
                    try:
                        _esp(league, date=d)
                    except Exception as e:
                        logger.debug("[basketball:%s] backsweep %s failed: %s",
                                      league, d, e)
            elif ds == "euroleague_api":
                from engine.basketball._euroleague_ingest import (
                    ingest_today as _eu,
                )
                _eu()
                from datetime import datetime as _dt0, timedelta as _td0
                today_dt = _dt0.now().date()
                for back in (1, 2):
                    d = (today_dt - _td0(days=back)).strftime("%Y-%m-%d")
                    try:
                        _eu(date=d)
                    except Exception as e:
                        logger.debug("[euroleague] backsweep %s failed: %s",
                                      d, e)
            elif has_realgm:
                from engine.basketball._realgm_ingest import ingest_today as _rg
                _rg(league)
            elif has_sofa:
                from engine.basketball._sofascore_primary_ingest import (
                    ingest_today as _sofa_today,
                )
                # Sofascore ingest already takes days_back; bump to 2.
                _sofa_today(league, days_back=2)
            _SLATE_INGEST_TS[league] = _now_ts
        except Exception as e:
            logger.debug("slate refresh %s failed: %s", league, e)

    conn = get_conn(league)
    g_tbl = games_table(league)
    t_tbl = teams_table(league)
    # Stale-game sweep — flip any 'scheduled' row older than 2 days
    # with no score into 'cancelled'. ESPN / RealGM / SofaScore each
    # silently drop postponed events, and the unswept rows pollute
    # the slate + leave picks dangling. Runs cheap (single UPDATE)
    # so we eat the cost per route hit rather than scattering it
    # across four ingest modules.
    try:
        from datetime import datetime as _stale_dt, timedelta as _stale_td
        cutoff = (_stale_dt.now() - _stale_td(days=2)
                   ).strftime("%Y-%m-%d")
        conn.execute(
            f"UPDATE {g_tbl} SET status = 'cancelled' "
            f"WHERE date < ? "
            f"  AND status NOT IN ('final', 'complete', 'completed', "
            f"                       'cancelled', 'postponed', 'voided') "
            f"  AND home_score IS NULL",
            (cutoff,),
        )
        # Force-finalize games stuck at status='in' for >2 days with a
        # populated home_score (i.e. ESPN told us the game tipped off
        # but never sent the final-status update). If linescore quarters
        # are populated, the current home_score/away_score are the
        # finals — flip status so the settler can grade pending picks.
        # WNBA CON@POR 2026-05-18 was the trigger: stuck at status='in'
        # with Q1-Q3 populated but no Q4 for two weeks until the user
        # flagged it.
        conn.execute(
            f"UPDATE {g_tbl} SET status = 'final' "
            f"WHERE date < ? "
            f"  AND status IN ('in', 'live', 'in_progress') "
            f"  AND home_score IS NOT NULL AND away_score IS NOT NULL",
            (cutoff,),
        )
        conn.commit()
    except Exception as _e:
        logger.debug("[basketball:%s] stale-sweep failed: %s", league, _e)
    select_cols = (
        f"g.game_id, g.date, g.start_time, g.status, "
        f"g.home_team_id, h.abbreviation AS home_abbr, h.name AS home_name, "
        f"h.logo_url AS home_logo, "
        f"g.away_team_id, a.abbreviation AS away_abbr, a.name AS away_name, "
        f"a.logo_url AS away_logo, "
        f"g.home_score, g.away_score "
    )
    join_clause = (
        f"FROM {g_tbl} g "
        f"LEFT JOIN {t_tbl} h ON g.home_team_id = h.id "
        f"LEFT JOIN {t_tbl} a ON g.away_team_id = a.id "
    )
    # Far-east leagues (China/Japan/Korea/Australia/NZ) include tomorrow
    # ET in the same window because their local "tonight" tipoff (7 PM
    # AEDT/JST/KST/CST) maps to 3-7 AM ET the next day. Without this the
    # panel is permanently 24h behind — by the time a game's date stamp
    # rolls into the user's "today", the game has already finished.
    from engine.basketball._config import is_far_east
    from datetime import datetime as _dt, timedelta as _td
    if is_far_east(league):
        tmrw = (_dt.strptime(today, "%Y-%m-%d") + _td(days=1)
                ).strftime("%Y-%m-%d")
        rows = conn.execute(
            f"SELECT {select_cols}{join_clause}WHERE g.date IN (?, ?) "
            f"  AND (g.status IS NULL "
            f"    OR g.status NOT IN ('cancelled','postponed','voided')) "
            f"ORDER BY g.date ASC, COALESCE(g.start_time, '9999') ASC, "
            f"  g.game_id",
            (today, tmrw),
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT {select_cols}{join_clause}WHERE g.date = ? "
            f"  AND (g.status IS NULL "
            f"    OR g.status NOT IN ('cancelled','postponed','voided')) "
            f"ORDER BY COALESCE(g.start_time, '9999') ASC, g.game_id",
            (today,),
        ).fetchall()
    # Empty today *or* all-final today → fall forward to TOMORROW only
    # (max 1 day ahead). All-final triggers when the day's slate has
    # already wrapped — AFL had this on 2026-05-10 (single 01:15 ET game,
    # final by morning) and the panel showed a finished game with no HR
    # odds even though HR's next-round slate (May 14-17) was on the
    # board. Capping at +1 day means the panel shows nothing rather than
    # a phantom "tonight" from days out. (Far-east leagues already
    # include tomorrow, so this only fires for leagues without overnight
    # ET coverage.)
    has_scheduled = any((r["status"] or "scheduled") not in
                         ("final", "complete", "completed", "cancelled",
                          "postponed", "voided")
                         for r in rows)
    fell_forward = False
    # Walk forward up to 7 days. Earlier this was capped at +1 day to
    # avoid surfacing phantom-tonight cards with no date pill, but the
    # response now exposes `fell_forward` + the actual `date`, and the
    # frontend renders an explicit "no games today, showing next slate
    # (DATE)" banner — so a 4-day jump is unambiguous to the user.
    # Without a wider walk, AFL on 2026-05-10 (one finished game today,
    # next round May 14-17) would still show empty.
    if not has_scheduled and not is_far_east(league):
        for delta in range(1, 8):
            cand = (_dt.strptime(today, "%Y-%m-%d") + _td(days=delta)
                    ).strftime("%Y-%m-%d")
            next_rows = conn.execute(
                f"SELECT {select_cols}{join_clause}WHERE g.date = ? "
                f"  AND (g.status IS NULL "
                f"    OR g.status NOT IN ('cancelled','postponed','voided')) "
                f"ORDER BY COALESCE(g.start_time, '9999') ASC, g.game_id",
                (cand,),
            ).fetchall()
            if any((r["status"] or "scheduled") not in
                    ("final", "complete", "completed", "cancelled",
                     "postponed", "voided") for r in next_rows):
                rows = next_rows
                today = cand
                fell_forward = True
                break
    # HR odds — single fetch per request, keyed off LEAGUE_REGISTRY's
    # hr_comp_id. Empty dict on any failure; downstream just gets
    # no-odds-no-picks.
    try:
        odds_by_key = fetch_league_odds(league)
    except Exception as e:
        logger.warning("HR odds %s failed: %s", league, e)
        odds_by_key = {}

    games = []
    for r in rows:
        d = dict(r)
        odds = None
        picks: list = []
        if d["status"] == "scheduled" and d["home_abbr"] and d["away_abbr"]:
            try:
                pred = predict_full(league, d["home_abbr"], d["away_abbr"])
            except Exception as e:
                logger.debug("predict %s/%s@%s failed: %s",
                             league, d["away_abbr"], d["home_abbr"], e)
                pred = {"error": str(e)}
            # Attach odds + run picks pipeline if odds matched.
            key = f"{d['away_abbr']}@{d['home_abbr']}"
            odds = odds_by_key.get(key)
            # Fallback: HR's abbreviation may differ from our canonical
            # one when their team name is generic (CBA: HR ships
            # "Zhejiang" → ZHE; our DB has Zhejiang Chouzhou → ZHEC).
            # Fuzzy-match against HR odds by team full-name first word
            # so the canonical row picks up the live market instead of
            # a side-by-side HR-stub card.
            if not odds and (d.get('home_name') or d.get('away_name')):
                hn_first = (d.get('home_name') or '').split()[:1]
                an_first = (d.get('away_name') or '').split()[:1]
                for hr_key, hr_v in (odds_by_key or {}).items():
                    h_full = (hr_v.get('home_full_name') or '').lower()
                    a_full = (hr_v.get('away_full_name') or '').lower()
                    if (hn_first and h_full.startswith(hn_first[0].lower())
                            and an_first and a_full.startswith(an_first[0].lower())):
                        odds = hr_v
                        break
            if pred and not pred.get("error") and odds:
                # Predictor already has home_abbr/away_abbr in output
                pred.setdefault("home_abbr", d["home_abbr"])
                pred.setdefault("away_abbr", d["away_abbr"])
                try:
                    picks = generate_picks(league, pred, odds,
                                            game_id=d["game_id"])
                except Exception as e:
                    logger.warning("picks %s/%s failed: %s",
                                   league, d["game_id"], e)
            # HR's start_time is more reliable than RealGM's parsed time
            # (RealGM ships rounded HH:MM ET that can be 1-2h off the
            # actual tipoff — Nelson @ Franklin on 2026-05-08 was 5:00 AM
            # in RealGM but 3:30 AM per HR / SofaScore). Prefer HR when
            # available — BUT only when the swap is a minor refinement,
            # not a wholesale day-of-week move. Toronto Tempo @ LA on
            # 2026-05-15 had ESPN at Fri 22:00 ET (correct) and HR at
            # Sun 19:00 ET (wrong — HR had the matchup listed twice
            # under different events). Cap the swap window at ±12h vs
            # the DB time so a wildly-wrong HR row can't relabel the
            # whole card to a different day.
            if odds and odds.get("start_time") and d.get("start_time"):
                try:
                    from datetime import datetime as _dt
                    _hr_iso = odds["start_time"].replace("Z", "+00:00")
                    _db_iso = d["start_time"].replace("Z", "+00:00")
                    _hr_dt = _dt.fromisoformat(_hr_iso)
                    _db_dt = _dt.fromisoformat(_db_iso)
                    if abs((_hr_dt - _db_dt).total_seconds()) <= 12 * 3600:
                        d["start_time"] = odds["start_time"]
                except (ValueError, TypeError, AttributeError):
                    # Fall through to DB time on any parse failure.
                    pass
            elif odds and odds.get("start_time") and not d.get("start_time"):
                # DB has no time — accept HR's unconditionally.
                d["start_time"] = odds["start_time"]
        else:
            pred = None
        # Standard layout: ONE pick per game card. Pick the highest-edge
        # non-skip candidate; the rest stay reachable via the game-detail
        # pane but never bloat the card or the slate-header pick count.
        # See feedback_standard_layout.md #2.
        #
        # For leagues that emit Q1 markets (WNBA from 2026-05-13),
        # split the best pick across two views — best_pick_full picks
        # from the full-game ML/SPREAD/TOTAL candidates only, and
        # best_pick_q1 picks from Q1_ML/Q1_SPREAD/Q1_TOTAL only. NBA's
        # frontend already keys on these per-view fields; WNBA gets
        # parity once the toggle is wired.
        Q1_TYPES = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
        full_candidates = [p for p in picks if p.get("type") not in Q1_TYPES]
        q1_candidates = [p for p in picks if p.get("type") in Q1_TYPES]
        # Prefer a staked pick (stake_units>0) over any 0u pick when
        # picking the card headline. 0u happens for two reasons:
        # (a) NOPLAY shadow — banned cell emitted only for learning;
        # (b) calibrated prob below the stake floor (e.g. < 0.52) — the
        # model isn't confident enough to bet. Either way it should
        # never headline the card over a real recommended bet. Same
        # pattern as engine.picks.get_best_pick.
        def _pick_best(cands):
            if not cands:
                return None
            staked = [p for p in cands if (p.get("stake_units") or 0) > 0]
            pool = staked if staked else cands
            return max(pool, key=lambda p: p.get("edge", 0))
        best_full = _pick_best(full_candidates)
        if best_full and (best_full.get("confidence") or "lean") == "skip":
            best_full = None
        best_q1 = _pick_best(q1_candidates)
        if best_q1 and (best_q1.get("confidence") or "lean") == "skip":
            best_q1 = None
        # `best_pick` stays as the headline across all markets so existing
        # callers (POTD selector, single-pick cards) keep working. Same
        # staked-over-shadow preference applies here.
        all_best = _pick_best([p for p in (best_full, best_q1) if p])
        d["prediction"] = pred
        d["odds"] = odds
        d["picks"] = [all_best] if all_best else []
        d["best_pick"] = all_best
        d["best_pick_full"] = best_full
        d["best_pick_q1"] = best_q1
        # Lock the pre-tipoff pick once the game goes live. Mutates d
        # in place; no-op for scheduled/final games.
        _lock_basketball_live_pick(conn, d, league)
        games.append(d)

    # HR-stub backfill — same pattern as routes_hockey.py. Some leagues
    # in the registry have no canonical schedule source (china_cba uses
    # RealGM but no scraper exists yet; brazil_nbb / other RealGM tiers
    # similarly lack ingest). When HR posts an odds market we still want
    # the user to see the matchup; otherwise CBA looks like an empty
    # tab even though HR is trading 2 games tonight.
    seen_keys = {f"{g.get('away_abbr')}@{g.get('home_abbr')}" for g in games
                  if g.get('away_abbr') and g.get('home_abbr')}
    # Track game_ids already on the slate so a stub matched to an
    # existing canonical game (CBA: HR's "Zhejiang" → ZHEC@SHE) doesn't
    # render twice — once from the games-table iteration and again
    # from the HR-stub backfill.
    seen_game_ids = {str(g.get('game_id')) for g in games
                      if g.get('game_id') is not None}
    for hr_key, hr_odds in (odds_by_key or {}).items():
        if hr_key in seen_keys:
            continue
        away_abbr = (hr_odds.get("away_abbr")
                     or hr_key.split("@")[0])
        home_abbr = (hr_odds.get("home_abbr")
                     or hr_key.split("@")[1])
        # Resolve full team names + logos when present in the per-league
        # teams table; fall back to abbreviation otherwise.
        h_row = conn.execute(
            f"SELECT * FROM {t_tbl} WHERE abbreviation = ?",
            (home_abbr,)).fetchone()
        a_row = conn.execute(
            f"SELECT * FROM {t_tbl} WHERE abbreviation = ?",
            (away_abbr,)).fetchone()
        # Fuzzy-match against full team names when abbreviation lookup
        # fails. HR ships "Zhejiang" generically; our DB has the more
        # specific "Zhejiang Chouzhou" / "Zhejiang Guangsha" — without
        # this, the stub minted a fresh ZHE@SHE card alongside the
        # canonical RealGM ZHEC@SHE row and the user saw the same
        # game twice. We trust the match only when ONE row matches
        # (avoids picking the wrong Zhejiang when both options exist).
        if not h_row and (hr_odds.get("home_full_name") or "").strip():
            hn = hr_odds["home_full_name"].split()[0]
            cands = conn.execute(
                f"SELECT * FROM {t_tbl} WHERE name LIKE ?",
                (f"{hn}%",)).fetchall()
            if len(cands) == 1:
                h_row = cands[0]
        if not a_row and (hr_odds.get("away_full_name") or "").strip():
            an = hr_odds["away_full_name"].split()[0]
            cands = conn.execute(
                f"SELECT * FROM {t_tbl} WHERE name LIKE ?",
                (f"{an}%",)).fetchall()
            if len(cands) == 1:
                a_row = cands[0]
        h_team = dict(h_row) if h_row else {}
        a_team = dict(a_row) if a_row else {}
        # Fall back to HR's full participant name when our teams table
        # doesn't have the team yet — for pre-launch leagues like CBA,
        # Brazil NBB, etc. without a canonical schedule scraper.
        h_full = h_team.get("name") or hr_odds.get("home_full_name") or home_abbr
        a_full = a_team.get("name") or hr_odds.get("away_full_name") or away_abbr
        # Stable stub-id derived from the matchup (deterministic across
        # restarts — see hockey route's MD5 fix for the rationale).
        import hashlib as _hashlib
        _stub_id = int(_hashlib.md5(hr_key.encode("utf-8"))
                        .hexdigest()[:8], 16) % (10 ** 9)
        start_iso = hr_odds.get("start_time") or ""
        # HR's start_time is UTC. A "tonight 8 PM ET" game stamps as
        # midnight UTC tomorrow — without an ET conversion the stub
        # filter rejects it as a future game. Convert to ET-day so
        # the matching aligns with the user's mental model.
        stub_date_utc = start_iso[:10] if len(start_iso) >= 10 else today
        stub_date = stub_date_utc
        if start_iso:
            try:
                from datetime import datetime as _dt, timezone as _tz, timedelta as _td
                if start_iso.endswith("Z"):
                    dt = _dt.strptime(start_iso, "%Y-%m-%dT%H:%M:%SZ")
                    dt = dt.replace(tzinfo=_tz.utc)
                else:
                    dt = _dt.fromisoformat(start_iso)
                # Convert UTC -> ET using zoneinfo so the offset is
                # right whether we're in EDT (mid-Mar to early-Nov) or
                # EST (rest of year). Earlier this was hardcoded -4
                # which silently rounded Nov-Mar games into the wrong
                # ET date.
                if _US_EASTERN is not None:
                    et = dt.astimezone(_US_EASTERN)
                else:
                    # Fallback: month-keyed offset, EDT roughly Mar-Oct.
                    offset_hours = -4 if 3 <= dt.month <= 10 else -5
                    et = dt.astimezone(_tz(_td(hours=offset_hours)))
                stub_date = et.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass
        # Stubs must match the slate's effective date in ET. Strict
        # match prevents the Euroleague case (5-08 canonical leaking
        # onto 5-07 slate) while still surfacing late-night ET games
        # whose UTC stamp rolls into tomorrow's date.
        # Far-east leagues additionally accept tomorrow ET so HR's
        # markets for their local-tonight games (3-7 AM ET tipoff)
        # mint stubs even before the canonical schedule scraper sees
        # them.
        if is_far_east(league):
            tmrw_stub = (_dt.strptime(real_today, "%Y-%m-%d") + _td(days=1)
                          ).strftime("%Y-%m-%d")
            if stub_date not in (real_today, tmrw_stub):
                continue
        elif stub_date != today:
            continue
        # Prefer an existing games-table row on the stub_date so the
        # stub inherits the canonical game_id (and picks can settle
        # against final scores later). Without this, MAD@HTA on
        # 2026-05-07 generated a hr-* stub even though E2025_398 was
        # already in our games table — so picks recorded against the
        # synthetic id would never settle.
        gid_real = None
        if h_team.get("id") and a_team.get("id"):
            row = conn.execute(
                f"SELECT game_id FROM {g_tbl} WHERE date = ? "
                f"  AND home_team_id = ? AND away_team_id = ? LIMIT 1",
                (stub_date, h_team.get("id"), a_team.get("id"))
            ).fetchone()
            if row:
                gid_real = row["game_id"]
        # When fuzzy team-resolution couldn't pick exactly one team
        # (CBA: HR's "Zhejiang" matches both Zhejiang Chouzhou and
        # Zhejiang Guangsha), still try to link by name-substring on
        # the games table for the exact date. If only one game on that
        # date matches the partial names from BOTH sides, link it.
        if not gid_real:
            h_first = (hr_odds.get("home_full_name") or "").split()[:1]
            a_first = (hr_odds.get("away_full_name") or "").split()[:1]
            if h_first and a_first:
                row = conn.execute(
                    f"SELECT g.game_id FROM {g_tbl} g "
                    f"JOIN {t_tbl} h ON g.home_team_id = h.id "
                    f"JOIN {t_tbl} a ON g.away_team_id = a.id "
                    f"WHERE g.date = ? AND h.name LIKE ? AND a.name LIKE ? "
                    f"LIMIT 1",
                    (stub_date, f"{h_first[0]}%", f"{a_first[0]}%")
                ).fetchone()
                if row:
                    gid_real = row["game_id"]
        # If we resolved to a canonical game already in `games`, skip
        # the stub entirely — the canonical row is being shown.
        if gid_real and str(gid_real) in seen_game_ids:
            continue

        # When fuzzy-matched, fill in canonical team data so the stub
        # renders with proper names + logos instead of synthetic abbrevs.
        if gid_real:
            if not h_team.get("id"):
                hr_full = conn.execute(
                    f"SELECT t.* FROM {t_tbl} t "
                    f"JOIN {g_tbl} g ON g.home_team_id = t.id "
                    f"WHERE g.game_id = ?", (gid_real,)).fetchone()
                if hr_full:
                    h_team = dict(hr_full)
                    h_full = h_team.get("name") or h_full
            if not a_team.get("id"):
                ar_full = conn.execute(
                    f"SELECT t.* FROM {t_tbl} t "
                    f"JOIN {g_tbl} g ON g.away_team_id = t.id "
                    f"WHERE g.game_id = ?", (gid_real,)).fetchone()
                if ar_full:
                    a_team = dict(ar_full)
                    a_full = a_team.get("name") or a_full

        # Run prediction + picks when both teams resolve (most leagues
        # populate the teams table from canonical-source ingest, so we
        # can model Euroleague's tomorrow-stub MAD@HTA the same as a
        # row from the games table). For pre-launch leagues with empty
        # teams tables, this falls through silently and the card just
        # shows odds without a model lean.
        stub_pred = None
        stub_picks: list = []
        stub_best = None
        if home_abbr and away_abbr and h_team.get("id") and a_team.get("id"):
            try:
                stub_pred = predict_full(league, home_abbr, away_abbr)
                if stub_pred and not stub_pred.get("error"):
                    stub_pred.setdefault("home_abbr", home_abbr)
                    stub_pred.setdefault("away_abbr", away_abbr)
                    stub_picks = generate_picks(
                        league, stub_pred, hr_odds,
                        game_id=gid_real or f"hr-{_stub_id}")
            except Exception as e:
                logger.debug("stub predict %s/%s failed: %s",
                              league, hr_key, e)
        # Same Q1-vs-Full split + stake-aware "best" selection as the
        # canonical path above. Without this the stub card sets only
        # `best_pick` and BasketballPanel's `best_pick_full`/`q1` view
        # key reads null — the card renders without a pick badge even
        # when the model has a strong recommendation. WNBA 2026-06-21
        # GS@LV, WSH@MIN, NY@LA all hit this (HR scoreboard had today's
        # games before ESPN did).
        STUB_Q1 = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
        stub_full_cands = [p for p in stub_picks if p.get("type") not in STUB_Q1]
        stub_q1_cands   = [p for p in stub_picks if p.get("type") in STUB_Q1]
        def _stub_pick_best(cands):
            if not cands:
                return None
            staked = [p for p in cands if (p.get("stake_units") or 0) > 0]
            chosen = max(staked or cands, key=lambda p: p.get("edge", 0))
            if (chosen.get("confidence") or "lean") == "skip":
                return None
            return chosen
        stub_best_full = _stub_pick_best(stub_full_cands)
        stub_best_q1   = _stub_pick_best(stub_q1_cands)
        stub_best = _stub_pick_best(
            [p for p in (stub_best_full, stub_best_q1) if p]
        )
        stub = {
            "game_id": gid_real or f"hr-{_stub_id}",
            "date": stub_date,
            "start_time": start_iso or None,
            "status": "scheduled",
            "home_team_id": h_team.get("id"),
            "home_abbr": home_abbr,
            "home_name": h_full,
            "home_logo": h_team.get("logo_url"),
            "away_team_id": a_team.get("id"),
            "away_abbr": away_abbr,
            "away_name": a_full,
            "away_logo": a_team.get("logo_url"),
            "home_score": None, "away_score": None,
            "prediction": stub_pred,
            "odds": hr_odds,
            "picks": [stub_best] if stub_best else [],
            "best_pick": stub_best,
            "best_pick_full": stub_best_full,
            "best_pick_q1": stub_best_q1,
        }
        games.append(stub)

    # Piggyback record + settle on every slate hit. Mirrors NHL/NBA so
    # picks land in the tracker the moment they're emitted, and games
    # that just went final settle without waiting for the worker tick.
    # record_pick is idempotent on (date, game_id, bet_type, pick) for
    # pending rows, so re-hitting this route is a no-op.
    try:
        from engine.basketball._tracker import (record_pick as _bb_record,
                                                  settle_picks as _bb_settle)
        recorded = 0
        for g in games:
            best = g.get("best_pick")
            if not best:
                continue
            try:
                pid = _bb_record(league, {
                    "date": g.get("date") or today,
                    "game_id": g.get("game_id"),
                    "matchup": f"{g.get('away_abbr')} @ {g.get('home_abbr')}",
                    "bet_type": best.get("type"),
                    "pick": best.get("pick"),
                    "model_prob": best.get("prob"),
                    "edge": best.get("edge"),
                    "odds": best.get("odds"),
                    "stake_units": best.get("stake_units"),
                })
                if pid:
                    recorded += 1
            except Exception as e:
                logger.debug("basketball record %s/%s failed: %s",
                              league, g.get("game_id"), e)
        if recorded:
            logger.info("basketball tracker (api hit) %s: recorded=%d",
                        league, recorded)
        sett = _bb_settle(league)
        if sett.get("settled"):
            logger.info("basketball settle (api hit) %s: %s", league, sett)
    except Exception as e:
        logger.warning("basketball tracker piggyback %s failed: %s", league, e)

    # Mark which slate card holds the active POTD so the frontend can
    # render a star badge. For far-east leagues the POTD is locked under
    # the GAME's date (which may be tomorrow ET), so check today AND
    # tomorrow when looking up the active row.
    # When no POTD exists yet, lazily create one from the slate's bets
    # so the star appears the first time the slate is viewed — without
    # this, only leagues whose /potd endpoint had been hit show the star.
    try:
        from engine.pick_of_day import get_or_create_potd as _potd_lock
        active_potd = _read_active_potd(league, real_today)
        if not active_potd:
            bets, game_date_by_id = _build_potd_bets(games, real_today)
            if bets:
                lock_date = _resolve_potd_lock_date(
                    league, real_today, bets, game_date_by_id)
                active_potd = _potd_lock(league, bets, date=lock_date, view="q1")
        if active_potd and active_potd.get("game_id"):
            potd_gid = str(active_potd["game_id"])
            for g in games:
                if str(g.get("game_id")) == potd_gid:
                    g["is_potd"] = True
                    break
    except Exception as e:
        logger.debug("POTD slate-flag lookup %s failed: %s", league, e)

    return {
        "league": league,
        "display_name": cfg.get("display_name") or league,
        "date": today,
        "real_today": real_today,
        "fell_forward": fell_forward,
        "status": cfg.get("status"),
        "in_season": et_month() in (cfg.get("season_months") or ()),
        "games": games,
    }


@router.get("/api/basketball/{league}/tracker/history")
def api_basketball_tracker_history(league: str, limit: int = 200) -> dict:
    """Pending + settled picks for ``league`` plus the summary shape
    PickHistory expects (overall + per-bet-type tiles). Settles any
    newly-final games before returning so the user sees fresh results
    without clicking Settle."""
    from engine.basketball import (
        LEAGUE_REGISTRY, list_history, settle_picks,
    )
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA uses its existing tracker route"}
    try:
        settle_picks(league)
    except Exception as e:
        logger.debug("settle %s failed: %s", league, e)
    rows = list_history(league, limit=limit)
    rows_out = []
    for r in rows:
        d = dict(r) if not isinstance(r, dict) else dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        rows_out.append(d)

    # Build the summary shape PickHistory consumes — mirrors what
    # tennis/MLB/NHL routes already return so the frontend renders
    # identically across sports. Summary scans the FULL picks table
    # (not the LIMIT-capped history slice) so the hero P/L reflects
    # every settled pick on file. Stake-weighted profit/ROI: each row
    # contributes profit*stake_units; ROI denom is sum(stake_units).
    from engine.basketball._db import get_conn as _bb_conn
    conn = _bb_conn(league)
    all_picks = conn.execute(
        "SELECT bet_type, result, profit, stake_units FROM picks"
    ).fetchall()
    wins = losses = pushes = pending = 0
    profit_total = 0.0
    stake_settled = 0.0
    by_type: dict[str, dict] = {}
    for r in all_picks:
        bt = r["bet_type"] or "?"
        b = by_type.setdefault(bt, {"total": 0, "wins": 0, "losses": 0,
                                      "pushes": 0, "pending": 0,
                                      "profit": 0.0, "win_pct": 0.0,
                                      "roi": 0.0, "stake_settled": 0.0})
        b["total"] += 1
        res = r["result"]
        stake_u = float(r["stake_units"] if r["stake_units"] is not None else 1.0)
        pr = float(r["profit"] or 0) * stake_u
        if res == "W":
            b["wins"] += 1; wins += 1
            b["profit"] += pr; profit_total += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        elif res == "L":
            b["losses"] += 1; losses += 1
            b["profit"] += pr; profit_total += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        elif res == "P":
            b["pushes"] += 1; pushes += 1
        else:
            b["pending"] += 1; pending += 1
    decided = wins + losses
    for bt, b in by_type.items():
        d = b["wins"] + b["losses"]
        st = b.pop("stake_settled", 0)
        b["win_pct"] = round(b["wins"] / d * 100, 1) if d else 0.0
        b["roi"] = round(b["profit"] / st, 1) if st else 0.0
        b["profit"] = round(b["profit"], 2)

    summary = {
        "overall": {
            "total": wins + losses + pushes,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "pending": pending,
            "profit": round(profit_total, 2),
            "win_pct": round(wins / decided * 100, 1) if decided else 0.0,
            "roi": round(profit_total / stake_settled, 1) if stake_settled else 0.0,
            "avg_clv": None,
            "clv_sample": 0,
        },
        "by_type": by_type,
    }
    return {"league": league, "rows": rows_out, "summary": summary}


@router.post("/api/basketball/{league}/tracker/record")
def api_basketball_tracker_record(league: str) -> dict:
    """Walk today's slate, record any picks above the framework gates
    into the per-league picks table. Idempotent on
    (date, game_id, bet_type, pick) so re-running is safe."""
    from engine.basketball import LEAGUE_REGISTRY, record_pick
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA uses its existing tracker route"}
    slate = api_basketball_today(league)
    games = slate.get("games") or []
    out = {"recorded": 0, "skipped": 0, "errors": 0}
    # Leagues with a Q1 tracker tab (currently wnba/afl) record BOTH
    # best_pick_full AND best_pick_q1 — they're independent markets
    # with separate P/L. Everything else records best_pick only (the
    # combined highest-edge pick from the slate). NBA has its own
    # tracker route and isn't routed here.
    Q1_TRACKER_LEAGUES = {"wnba", "afl", "ncaam", "euroleague", "ncaaw",
                          "china_cba", "bulgaria_nbl", "czech_nbl",
                          "germany_bbl", "denmark_basketligaen",
                          "finland_korisliiga", "france_pro_b",
                          "greece_a1", "hungary_nb1",
                          "iceland_urvalsdeild",
                          "iceland_urvalsdeild_w",
                          "israel_super", "latvia_lbl",
                          "lithuania_lkl", "slovakia_extraliga",
                          "slovenia_skl", "sweden_ligan",
                          "argentina_lnb", "brazil_lbf_w",
                          "brazil_nbb", "dominican_lnb",
                          "puerto_rico_bsn", "japan_b2",
                          "nz_nbl", "australia_nbl", "korea_kbl"}
    for g in games:
        if league in Q1_TRACKER_LEAGUES:
            candidates = [g.get("best_pick_full"), g.get("best_pick_q1")]
        else:
            candidates = [g.get("best_pick")]
        for best in candidates:
            if not best:
                continue
            try:
                pid = record_pick(league, {
                    "date": g.get("date"),
                    "game_id": g.get("game_id"),
                    "matchup": f"{g.get('away_abbr')} @ {g.get('home_abbr')}",
                    "bet_type": best.get("type"),
                    "pick": best.get("pick"),
                    "model_prob": best.get("prob"),
                    "edge": best.get("edge"),
                    "odds": best.get("odds"),
                    "stake_units": best.get("stake_units"),
                })
                if pid:
                    out["recorded"] += 1
                else:
                    out["skipped"] += 1
            except Exception as e:
                logger.warning("record %s/%s failed: %s",
                               league, g.get("game_id"), e)
                out["errors"] += 1
    return out


@router.post("/api/basketball/{league}/tracker/settle")
def api_basketball_tracker_settle(league: str) -> dict:
    """Manual settle trigger for ``league``."""
    from engine.basketball import LEAGUE_REGISTRY, settle_picks
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA uses its existing tracker route"}
    return settle_picks(league)


@router.get("/api/basketball/{league}/standings")
def api_basketball_standings(league: str) -> list[dict]:
    """NBA-shape standings: W-L record, PCT, GB, PF/PA/Diff per game,
    L10, streak, conference/division grouping when present.
    Mirrors /api/nba/standings so the same StandingsView can render
    basketball-framework leagues without per-league branching."""
    from engine.basketball import LEAGUE_REGISTRY
    from engine.basketball._db import get_conn, games_table, teams_table
    if league not in LEAGUE_REGISTRY:
        return []
    if league == "nba":
        return []
    conn = get_conn(league)
    g_tbl = games_table(league)
    t_tbl = teams_table(league)
    season = _current_season(league)

    # Defensive SELECT — basketball-framework team tables don't all
    # have division/conference columns (international leagues skip them).
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({t_tbl})").fetchall()}
    div_col = "COALESCE(division, '')" if "division" in cols else "''"
    conf_col = "COALESCE(conference, '')" if "conference" in cols else "''"
    logo_col = "logo_url" if "logo_url" in cols else "''"
    teams_map = {r["id"]: dict(r) for r in conn.execute(
        f"SELECT id, abbreviation, name, "
        f"       {div_col} AS division, {conf_col} AS conference, "
        f"       {logo_col} AS logo_url "
        f"FROM {t_tbl}"
    ).fetchall()}

    # game_id is the basketball-framework PK (vs hockey's `id`).
    games = conn.execute(
        f"SELECT game_id, date, home_team_id, away_team_id, home_score, "
        f"       away_score "
        f"FROM {g_tbl} "
        f"WHERE status = 'final' AND season = ? "
        f"  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        f"ORDER BY date ASC, game_id ASC",
        (season,),
    ).fetchall()

    by_team: dict[int, list] = {}
    for g in games:
        for tid in (g["home_team_id"], g["away_team_id"]):
            by_team.setdefault(tid, []).append(g)

    out = []
    for tid, meta in teams_map.items():
        team_games = by_team.get(tid, [])
        wins = losses = 0
        pf = pa = 0
        results: list[str] = []
        for g in team_games:
            is_home = g["home_team_id"] == tid
            scored = g["home_score"] if is_home else g["away_score"]
            allowed = g["away_score"] if is_home else g["home_score"]
            pf += scored; pa += allowed
            if scored > allowed:
                wins += 1; results.append("W")
            elif scored < allowed:
                losses += 1; results.append("L")
        gp = wins + losses
        pct = round(wins / gp, 3) if gp else 0.0
        last10 = results[-10:]
        l10w = sum(1 for r in last10 if r == "W")
        l10l = sum(1 for r in last10 if r == "L")
        streak = ""
        if results:
            kind = results[-1]
            n = 0
            for r in reversed(results):
                if r != kind:
                    break
                n += 1
            streak = f"{kind}{n}"
        out.append({
            "team_id": tid,
            "abbreviation": meta.get("abbreviation") or str(tid),
            "name": meta.get("name") or "",
            "logo": meta.get("logo_url") or "",
            "division": meta.get("division") or "",
            "conference": meta.get("conference") or "",
            "wins": wins, "losses": losses,
            "games_played": gp,
            "win_pct": pct,
            "points_for": pf, "points_against": pa,
            "ppg": round(pf / gp, 1) if gp else 0.0,
            "papg": round(pa / gp, 1) if gp else 0.0,
            "diff": pf - pa,
            "l10": f"{l10w}-{l10l}",
            "streak": streak,
        })

    out.sort(key=lambda x: (-x["win_pct"], -x["wins"], x["abbreviation"]))
    # Compute games-back relative to the leader within each
    # conference/division group (NBA convention: GB = ((leader_wins -
    # team_wins) + (team_losses - leader_losses)) / 2). When there's no
    # grouping (international leagues), GB is computed against the
    # overall leader.
    def _set_gb(group: list[dict]):
        if not group: return
        leader = group[0]
        for t in group:
            gb = ((leader["wins"] - t["wins"]) + (t["losses"] - leader["losses"])) / 2
            t["gb"] = "-" if gb == 0 else f"{gb:.1f}"
    grouped: dict[str, list] = {}
    for t in out:
        k = t["division"] or t["conference"] or "_"
        grouped.setdefault(k, []).append(t)
    for g in grouped.values():
        _set_gb(g)
    return out


@router.get("/api/basketball/{league}/calibration")
def api_basketball_calibration(league: str) -> dict:
    """Surface the league's walk-forward calibration table (B4 output)."""
    from engine.basketball import LEAGUE_REGISTRY
    import json
    from pathlib import Path
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA calibration uses /api/calibration/nba"}
    path = Path("data/basketball") / f"{league}_calibration.json"
    if not path.exists():
        return {"league": league, "method": "none",
                "buckets": {}, "n_processed": 0,
                "deferred": ["No walk-forward seed run for this league yet"]}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        return {"error": f"calibration load failed: {e}"}


def _current_season(league: str) -> int:
    from datetime import datetime
    from engine.basketball import LEAGUE_REGISTRY
    cfg = LEAGUE_REGISTRY[league]
    months = cfg.get("season_months") or (10,)
    primary = months[0]
    now = et_now()
    return now.year if now.month >= primary else now.year - 1


@router.get("/api/basketball/{league}/potd")
def api_basketball_potd(league: str) -> dict:
    """Pick of the Day for a basketball framework league.

    Locks at first creation per (league, date). Reads the day's slate,
    selects the highest-conviction non-skip pick, and persists. NBA
    falls through to its existing /api/pick-of-day/nba endpoint.
    """
    from datetime import datetime
    from engine.basketball import LEAGUE_REGISTRY
    from engine.pick_of_day import (
        get_or_create_potd, get_today_potd,
    )
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA POTD lives at /api/pick-of-day/nba"}

    today = et_today_str()
    # Reuse the slate route to assemble (game, prediction, picks).
    slate = api_basketball_today(league)
    games = slate.get("games") or []

    # Read first; today + tomorrow-fallback for far-east leagues. The
    # lock-date and bet-shape helpers are shared with the slate route's
    # lazy-create so the two paths can't drift.
    potd = _read_active_potd(league, today)
    if potd:
        return potd

    bets, game_date_by_id = _build_potd_bets(games, today)
    if not bets:
        return {"message": "No slate available", "sport": league}
    lock_date = _resolve_potd_lock_date(league, today, bets, game_date_by_id)
    potd = get_or_create_potd(league, bets, date=lock_date, view="q1")
    return potd or {"message": "No qualifying picks today", "sport": league}


@router.get("/api/basketball/{league}/potd/summary")
def api_basketball_potd_summary(league: str) -> dict:
    """POTD running totals for the league."""
    from engine.basketball import LEAGUE_REGISTRY
    from engine.pick_of_day import get_potd_summary, settle_potd
    if league not in LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nba":
        return {"error": "NBA POTD lives at /api/pick-of-day/nba/summary"}
    try:
        settle_potd(league)
    except Exception as e:
        logger.debug("POTD auto-settle failed for %s: %s", league, e)
    return get_potd_summary(league)


@router.get("/api/basketball/leagues")
def api_basketball_leagues() -> dict:
    """Return every basketball league the framework knows about, with
    per-league metadata the frontend nav needs (display name, country,
    region grouping, season months, current in-season flag, status,
    and today's game count for the sidebar badges).

    Response shape::

        {
          "leagues": [
            {"key": "nba", "display_name": "NBA", "country": "USA",
             "region": "USA", "in_season": true, "status": "active",
             "data_source": "espn", "game_count_today": 4},
            ...
          ],
          "regions": ["USA", "International", "Europe",
                      "Americas", "Asia/Oceania"],
        }
    """
    from engine.basketball import LEAGUE_REGISTRY
    from engine.basketball._db import get_conn, games_table
    today = et_now()
    today_str = today.strftime("%Y-%m-%d")
    leagues = []
    for key, cfg in LEAGUE_REGISTRY.items():
        months = cfg.get("season_months") or ()
        # Per-league count for the sidebar badge — matches what the
        # /today route would actually render (today's games when
        # present; otherwise the next slate the panel falls forward
        # to). Avoids the "0 today" badge on a league that's about
        # to play later this week.
        try:
            conn = get_conn(key)
            tbl = games_table(key)
            n_today = conn.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE date = ?", (today_str,),
            ).fetchone()[0]
            if n_today == 0:
                # Tomorrow only — matches the slate route's +1-day
                # fallforward. Earlier this looked 14 days ahead and
                # the badge would advertise "3 games" for a league the
                # user clicked into and saw an empty slate.
                nxt = conn.execute(
                    f"SELECT date, COUNT(*) FROM {tbl} "
                    f"WHERE date > ? AND date <= date(?, '+1 days') "
                    f"GROUP BY date ORDER BY date LIMIT 1",
                    (today_str, today_str),
                ).fetchone()
                n_today = int(nxt[1]) if nxt else 0
            # Live fallback for ESPN-tracked leagues when the DB is
            # stale. NBA scoreboard occasionally drifts behind ESPN
            # because the worker poll runs on a cadence; without this
            # the sidebar shows '0' even though ESPN says there are
            # games today.
            if n_today == 0 and cfg.get("data_source") == "espn":
                live_n = _live_espn_count(cfg.get("espn_league_path"),
                                            today_str)
                if live_n is not None:
                    n_today = live_n
            # HR-event fallback for RealGM / SofaScore leagues. RealGM
            # only ships RESULTS — no future schedule — so the DB
            # never knows a game is coming up. HR's competition tree
            # carries `numEvents` (count of upcoming events) per
            # competition. Use it when DB shows 0 and we have an
            # hr_comp_id wired. Bulgaria/Czech NBL hit this path —
            # season finalized 5-15, playoffs scheduled on HR but not
            # in the RealGM-sourced DB.
            if n_today == 0 and cfg.get("hr_comp_id"):
                hr_n = _live_hr_basketball_count(cfg.get("hr_comp_id"))
                if hr_n is not None:
                    n_today = hr_n
        except Exception:
            n_today = 0
        leagues.append({
            "key": key,
            "display_name": cfg.get("display_name") or key,
            "country": cfg.get("country") or "Unknown",
            "region": _region_for(cfg.get("country") or ""),
            "in_season": today.month in months,
            "status": cfg.get("status"),
            "data_source": cfg.get("data_source"),
            "hr_comp_name": cfg.get("hr_comp_name"),
            "game_count_today": int(n_today),
        })
    return {
        "leagues": leagues,
        "regions": ["USA", "International", "Europe",
                    "Americas", "Asia/Oceania"],
    }


def _live_espn_count(espn_path: str | None, date_str: str) -> int | None:
    """Count games on ESPN's scoreboard for ``date_str``. Returns None
    on any failure (caller falls through to the DB count). Cached for
    60 seconds per (path, date) to keep the leagues endpoint snappy
    when the user clicks around quickly."""
    if not espn_path:
        return None
    import time as _t, urllib.request, json as _json
    cache_key = (espn_path, date_str)
    cached = _live_espn_cache.get(cache_key)
    now = _t.time()
    if cached and (now - cached[0]) < 60:
        return cached[1]
    try:
        url = (f"https://site.api.espn.com/apis/site/v2/sports/{espn_path}"
                f"/scoreboard?dates={date_str.replace('-','')}")
        req = urllib.request.Request(url, headers={
            "User-Agent": "SportsBettor/1.0 (leagues-count-fallback)",
        })
        with urllib.request.urlopen(req, timeout=4) as r:
            data = _json.loads(r.read())
        n = len(data.get("events") or [])
    except Exception:
        n = None
    _live_espn_cache[cache_key] = (now, n)
    return n


_live_espn_cache: dict[tuple[str, str], tuple[float, int | None]] = {}


def _live_hr_basketball_count(comp_id: str | None) -> int | None:
    """Sidebar-badge fallback for RealGM/SofaScore-sourced leagues whose
    DBs only hold results (no future schedule). HR's sports tree carries
    a ``numEvents`` counter per competition — use it when our DB shows
    0 upcoming and HR has events on its board. Cached 60s per comp_id."""
    if not comp_id:
        return None
    import time as _t
    now = _t.time()
    cached = _live_hr_bb_cache.get(comp_id)
    if cached and (now - cached[0]) < 60:
        return cached[1]
    try:
        from scrapers.hardrock_odds import _fetch_sports_tree
        tree, _ = _fetch_sports_tree()
        sports = (tree.get("data", {}).get("betSync", {}).get("sports") or [])
        n: int | None = None
        for sp in sports:
            if sp.get("code") != "BASKETBALL":
                continue
            for cat in sp.get("categories") or []:
                for comp in cat.get("competitions") or []:
                    if str(comp.get("id") or "") == str(comp_id):
                        n = int(comp.get("numEvents") or 0)
                        break
                if n is not None:
                    break
            if n is not None:
                break
    except Exception:
        n = None
    _live_hr_bb_cache[comp_id] = (now, n)
    return n


_live_hr_bb_cache: dict[str, tuple[float, int | None]] = {}


# Country → region grouping. Kept here (not in _config.py) because it's
# UI/UX taxonomy, not a model property — different frontends might
# regroup the same leagues differently.
_REGION_MAP = {
    # USA
    "USA": "USA",
    # International umbrella (multi-nation comps)
    "International": "International",
    # Europe
    "France": "Europe", "Lithuania": "Europe", "Israel": "Europe",
    "Greece": "Europe", "Czech Republic": "Europe", "Latvia": "Europe",
    "Bulgaria": "Europe", "Iceland": "Europe", "Germany": "Europe",
    "Hungary": "Europe", "Slovenia": "Europe", "Denmark": "Europe",
    "Finland": "Europe", "Slovakia": "Europe", "Sweden": "Europe",
    # Americas (non-USA)
    "Dominican Republic": "Americas", "Puerto Rico": "Americas",
    "Brazil": "Americas", "Argentina": "Americas",
    # Asia / Oceania
    "Japan": "Asia/Oceania", "South Korea": "Asia/Oceania",
    "Australia": "Asia/Oceania", "New Zealand": "Asia/Oceania",
    "China": "Asia/Oceania",
}


def _region_for(country: str) -> str:
    return _REGION_MAP.get(country, "Other")
