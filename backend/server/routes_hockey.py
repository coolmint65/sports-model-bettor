"""Hockey routes — exposes HOCKEY_LEAGUE_REGISTRY to the frontend so
the nested sidebar can render NHL/AHL/PWHL with per-league badges.

NHL keeps its existing `/api/nhl/*` routes (heavy single-flight cache,
intermission predictor, etc) — they're untouched here. This module is
for the framework-managed leagues (AHL, PWHL) and the cross-league
listing endpoint.
"""
from __future__ import annotations

import logging
from datetime import datetime

from fastapi import APIRouter

from ._tz import et_today_str, et_now, et_month

logger = logging.getLogger(__name__)

router = APIRouter()


# Game statuses that are NOT live. Anything outside this set triggers
# the lock-on-go-live behavior in _lock_hockey_live_pick.
_NOT_LIVE_STATUSES = frozenset({
    "scheduled", "pre_game", "pregame",
    "final", "complete", "completed",
    "cancelled", "postponed", "voided",
})


def _lock_hockey_live_pick(conn, d: dict, league: str) -> None:
    """Surface the pre-puck-drop pick once the game is live. The
    tracker grades whatever was recorded at scheduled time; the card
    must match. Mutates ``d`` in place.
    """
    status = (d.get("status") or "scheduled").lower()
    if status in _NOT_LIVE_STATUSES:
        return
    gid = d.get("game_id")
    if not gid:
        return
    # Hockey framework leagues all use the unified `picks` table (the
    # NHL-only `nhl_picks` table sits in a separate DB and isn't part
    # of this connection).
    try:
        rows = conn.execute(
            "SELECT bet_type, pick, model_prob, edge, odds "
            "FROM picks "
            "WHERE game_id = ? AND result IS NULL "
            "ORDER BY edge DESC",
            (gid,),
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
        "locked":     True,
    }
    d["best_pick"] = locked
    d["picks"] = [locked]
    d["pick_locked"] = True


@router.get("/api/hockey/leagues")
def api_hockey_leagues() -> dict:
    """Per-league metadata for the sidebar nav. Mirrors
    /api/basketball/leagues so the same Sidebar component pattern can
    drive the Hockey nested group."""
    from engine.hockey import HOCKEY_LEAGUE_REGISTRY
    today = et_now()
    today_str = today.strftime("%Y-%m-%d")
    leagues = []
    for key, cfg in HOCKEY_LEAGUE_REGISTRY.items():
        months = cfg.get("season_months") or ()
        n_today = _count_today_games(key, today_str)
        leagues.append({
            "key": key,
            "display_name": cfg.get("display_name") or key,
            "country": cfg.get("country") or "USA",
            "region": cfg.get("region") or "USA",
            "in_season": today.month in months,
            "status": cfg.get("status"),
            "data_source": cfg.get("data_source"),
            "hr_comp_name": cfg.get("hr_comp_name"),
            "game_count_today": int(n_today),
        })
    return {"leagues": leagues, "regions": ["USA", "International"]}


def _count_today_games(league: str, today_str: str) -> int:
    """Today's game count per league. Falls forward to the next slate
    within 7 days when today is empty so the sidebar badge stays useful
    on off-days. NHL goes through its existing DB; AHL/PWHL through
    their per-league theScore-backed DBs."""
    try:
        if league == "nhl":
            from engine.nhl_db import get_conn
            conn = get_conn()
            tbl = "nhl_games"
        else:
            mod = __import__(f"engine.sports.{league}.db",
                             fromlist=["get_conn"])
            conn = mod.get_conn()
            tbl = "games"
        row = conn.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE date = ?", (today_str,),
        ).fetchone()
        n = int(row[0] if row else 0)
        if n > 0:
            return n
        # Fall forward to the next date with games (cap 7 days). Avoids
        # the "0 today" badge when the league has games tomorrow night.
        nxt = conn.execute(
            f"SELECT date, COUNT(*) FROM {tbl} "
            f"WHERE date > ? AND date <= date(?, '+7 days') "
            f"GROUP BY date ORDER BY date LIMIT 1",
            (today_str, today_str),
        ).fetchone()
        return int(nxt[1]) if nxt else 0
    except Exception:
        return 0


@router.get("/api/hockey/{league}/today")
def api_hockey_today(league: str) -> dict:
    """Today's slate for ``league`` with per-game predictions + HR odds.

    NHL goes through its existing /api/nhl/best-bets endpoint (heavy
    pipeline) and is rejected here. AHL + PWHL run a lightweight
    pipeline: predictor.predict() per game, HR odds attached when
    available, no live worker dependency.
    """
    from engine.hockey import HOCKEY_LEAGUE_REGISTRY
    if league not in HOCKEY_LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nhl":
        return {"error": "NHL uses /api/nhl/best-bets — don't route here"}

    try:
        mod = __import__(f"engine.sports.{league}.db",
                         fromlist=["get_conn"])
        conn = mod.get_conn()
    except Exception as e:
        logger.warning("[%s] db open failed: %s", league, e)
        return {"error": f"db open failed for {league}"}

    today = et_today_str()
    real_today = today  # preserved across fall-forward for stub-gate window
    # ``abbreviation`` is the canonical short-code column across BOTH
    # ingest sources. theScore stores the same string in short_name +
    # abbreviation; SofaScore stores SKY/MELI/etc. only in
    # abbreviation. Reading short_name was the historical default but
    # silently broke the SofaScore leagues (Skycity Stampede stored
    # short_name='Skycity Stampede' which then printed as the matchup
    # token in the card AND caused a stub-duplicate via the HR-only
    # fallback). Always use abbreviation.
    select_cols = (
        "g.id, g.date, g.start_time, g.status, g.game_type, "
        "g.has_overtime, g.has_shootout, "
        "g.home_score, g.away_score, "
        "g.home_team_id, g.away_team_id, "
        "h.full_name AS home_name, h.abbreviation AS home_short, "
        "h.color_primary AS home_color, h.logo_url AS home_logo, "
        "a.full_name AS away_name, a.abbreviation AS away_short, "
        "a.color_primary AS away_color, a.logo_url AS away_logo "
    )
    join_clause = (
        "FROM games g "
        "LEFT JOIN teams h ON g.home_team_id = h.id "
        "LEFT JOIN teams a ON g.away_team_id = a.id "
    )
    # Far-east hockey (none today, but future-proof for any KHL/Asia
    # League onboardings) needs the same today + tomorrow window
    # basketball uses, since a 7 PM local tipoff stamps as tomorrow ET.
    from engine._geo import is_far_east_country
    from engine.hockey import HOCKEY_LEAGUE_REGISTRY
    from datetime import datetime as _dt, timedelta as _td
    cfg = HOCKEY_LEAGUE_REGISTRY.get(league) or {}
    is_far_east = is_far_east_country(cfg.get("country"))
    if is_far_east:
        tmrw = (_dt.strptime(today, "%Y-%m-%d") + _td(days=1)
                ).strftime("%Y-%m-%d")
        rows = conn.execute(
            f"SELECT {select_cols}{join_clause}WHERE g.date IN (?, ?) "
            f"ORDER BY g.date, g.id",
            (today, tmrw),
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT {select_cols}{join_clause}WHERE g.date = ? ORDER BY g.id",
            (today,),
        ).fetchall()
    fell_forward = False
    if not rows and not is_far_east:
        # Walk forward up to 7 days. Cap was +1 historically because
        # the card didn't surface the game-date pill prominently — a
        # 3-days-out PWHL game looked like "tonight" without it. The
        # card now always renders the date row (matches basketball /
        # football / soccer), so a wider walk is safe and gives the
        # user the next-slate preview off-days instead of empty.
        # `fell_forward` is returned so the frontend can render a
        # banner explaining the date jump.
        for delta in range(1, 8):
            cand = (_dt.strptime(today, "%Y-%m-%d") + _td(days=delta)
                    ).strftime("%Y-%m-%d")
            rows = conn.execute(
                f"SELECT {select_cols}{join_clause}WHERE g.date = ? "
                f"ORDER BY g.id",
                (cand,),
            ).fetchall()
            if rows:
                today = cand
                fell_forward = True
                break

    # HR odds — single fetch per request, cached upstream.
    try:
        from engine.hockey._odds import fetch_league_odds
        odds_by_key = fetch_league_odds(league)
    except Exception as e:
        logger.warning("[%s] HR odds fetch failed: %s", league, e)
        odds_by_key = {}

    from engine.predictor import GameContext, get as get_predictor
    from engine.hockey._picks import generate_picks
    predictor = get_predictor(league)

    games = []
    for r in rows:
        d = dict(r)
        pred = None
        odds = None
        picks: list = []
        if predictor and d["status"] == "pre_game":
            try:
                p = predictor.predict(GameContext(
                    sport=league, home=d["home_name"] or "",
                    away=d["away_name"] or "",
                ))
                if not p.error:
                    pred = {
                        "home_expected": p.home_expected,
                        "away_expected": p.away_expected,
                        "total": p.total, "margin": p.margin,
                        "ml_home": p.ml_home, "ml_away": p.ml_away,
                    }
            except Exception as e:
                logger.debug("predict %s/%s @ %s failed: %s",
                              league, d["away_short"], d["home_short"], e)
        if d["home_short"] and d["away_short"]:
            key = f"{d['away_short']}@{d['home_short']}"
            odds = odds_by_key.get(key)
        # Generate picks only when both sides have a pre-game window:
        # our DB status must be pre_game AND HR must not have flipped
        # the event to in-play. Odds still surface either way so the
        # card has live market context.
        if pred and odds and not (odds or {}).get("hr_inplay"):
            try:
                picks = generate_picks(pred, odds, sport=league)
            except Exception as e:
                logger.warning("[%s] picks gen failed for %s@%s: %s",
                                league, d.get("away_short"), d.get("home_short"), e)
        # Standard layout: ONE pick per game. The picker may emit
        # multiple positive-edge candidates; we keep the top one for
        # the card + tracker. Other markets stay reachable via the
        # game-detail pane if needed.
        best = picks[0] if picks else None
        d["prediction"] = pred
        d["odds"] = odds
        d["picks"] = [best] if best else []
        d["best_pick"] = best
        # Lock the pre-puck-drop pick once the game flips to live.
        _lock_hockey_live_pick(conn, d, league)
        games.append(d)

    # Surface HR-only events (theScore lags behind on AHL playoff
    # scheduling). For any HR matchup we don't already have, append a
    # stub game so the user sees odds + a prediction even when our
    # ingest hasn't caught up.
    seen_keys = {f"{g['away_short']}@{g['home_short']}"
                 for g in games if g.get('away_short') and g.get('home_short')}
    for hr_key, hr_odds in odds_by_key.items():
        if hr_key in seen_keys:
            continue
        away_abbr = hr_odds.get("away_abbr") or hr_key.split("@")[0]
        home_abbr = hr_odds.get("home_abbr") or hr_key.split("@")[1]
        # Resolve full team names from the per-league teams table so the
        # card has logos + display names. Falls back to abbreviation
        # when the team isn't in our DB (rare).
        h_team = _team_by_short(conn, home_abbr) or {}
        a_team = _team_by_short(conn, away_abbr) or {}
        # Stable stub-id derivation. Python's built-in ``hash()`` is
        # randomized per-process by PYTHONHASHSEED, so the SAME hr_key
        # produced a DIFFERENT id every time the server restarted —
        # every restart triggered a fresh batch of "phantom" picks for
        # the same matchup, none matchable to a real games row. AHL
        # tracker carried 3× duplicates per HR-only matchup at 2026-05-05.
        # MD5 is deterministic across runs and a 9-digit window is
        # comfortable for collision avoidance at slate scale.
        import hashlib as _hashlib
        _stub_id = int(_hashlib.md5(hr_key.encode("utf-8"))
                        .hexdigest()[:8], 16) % (10 ** 9)
        # Real game date — HR's eventTime is authoritative for puck
        # drop. Falling back to "today" stamps tomorrow's HR matchups
        # with today's date and silently feeds the tracker a pick for
        # a game that isn't even live yet (and re-stamps the same
        # matchup again tomorrow with a different date → duplicate).
        # AHL MANM @ GRG on 2026-05-05 was actually a 5-06 game; the
        # pick recorded as 5-05 settled against tomorrow's row only
        # by accident of the reconciler's ±1-day window.
        _start_iso = hr_odds.get("start_time") or ""
        stub_date = _start_iso[:10] if len(_start_iso) >= 10 else today
        # Cap stub-backfill window at real-today + 1. Gate against the
        # actual `now()` date — `today` may have been reassigned to
        # `tomorrow` by fall-forward, which would let a 2-days-out HR
        # matchup pass a |stub - today|<=1 check (PWHL BOS@OTT 2026-05-08
        # leaked through on a 2026-05-06 hit because fall-forward had
        # already moved `today` to 2026-05-07).
        from datetime import datetime as _dt, timedelta as _td
        try:
            _today_d = _dt.strptime(real_today, "%Y-%m-%d").date()
            _stub_d = _dt.strptime(stub_date, "%Y-%m-%d").date()
            if abs((_stub_d - _today_d).days) > 1:
                continue
        except (ValueError, TypeError):
            pass
        stub = {
            "id": -_stub_id,
            "date": stub_date,
            "start_time": _start_iso or None,
            "status": "pre_game",
            "home_score": None, "away_score": None,
            "home_team_id": h_team.get("id"),
            "away_team_id": a_team.get("id"),
            "home_name": h_team.get("full_name") or home_abbr,
            "home_short": home_abbr,
            "home_color": h_team.get("color_primary"),
            "home_logo": h_team.get("logo_url"),
            "away_name": a_team.get("full_name") or away_abbr,
            "away_short": away_abbr,
            "away_color": a_team.get("color_primary"),
            "away_logo": a_team.get("logo_url"),
        }
        # Run the predictor + picks against the stub so the card has
        # the same shape as theScore-sourced rows.
        pred = None
        picks_stub: list = []
        if predictor and stub["home_name"] and stub["away_name"]:
            try:
                p = predictor.predict(GameContext(
                    sport=league, home=stub["home_name"],
                    away=stub["away_name"],
                ))
                if not p.error:
                    pred = {
                        "home_expected": p.home_expected,
                        "away_expected": p.away_expected,
                        "total": p.total, "margin": p.margin,
                        "ml_home": p.ml_home, "ml_away": p.ml_away,
                    }
            except Exception:
                pass
        if pred and not hr_odds.get("hr_inplay"):
            try:
                picks_stub = generate_picks(pred, hr_odds, sport=league)
            except Exception:
                pass
        best = picks_stub[0] if picks_stub else None
        stub["prediction"] = pred
        stub["odds"] = hr_odds
        stub["picks"] = [best] if best else []
        stub["best_pick"] = best
        games.append(stub)

    cfg = HOCKEY_LEAGUE_REGISTRY[league]

    # Piggyback record + settle on every slate hit. Mirrors the NHL/NBA
    # convention: picks land in the tracker the moment they're emitted,
    # and games that just went final get settled without waiting for
    # the worker tick. Both calls are idempotent on (date, game_id,
    # bet_type, pick), so re-hitting the route is a no-op.
    try:
        from engine.hockey._tracker import (record_picks as _hk_record,
                                              settle_pending as _hk_settle)
        rec = _hk_record(league, games)
        if rec.get("inserted"):
            logger.info("hockey tracker (api hit) %s: %s", league, rec)
        sett = _hk_settle(league)
        if sett.get("settled"):
            logger.info("hockey settle (api hit) %s: %s", league, sett)
    except Exception as e:
        logger.warning("hockey tracker piggyback %s failed: %s", league, e)

    return {
        "league": league,
        "display_name": cfg.get("display_name") or league,
        "date": today,
        "fell_forward": fell_forward,
        "status": cfg.get("status"),
        "in_season": et_month() in (cfg.get("season_months") or ()),
        "games": games,
    }


@router.get("/api/hockey/{league}/standings")
def api_hockey_standings(league: str) -> list[dict]:
    """NHL-shape standings: W-L-OTL record, points (W=2, OT=1),
    GF/GA/Diff, L10 (last 10 games), streak, division grouping.
    Matches the structure /api/nhl/standings returns so the same
    NHLStandings frontend component can render hockey-framework leagues
    without per-league branching.

    NHL itself uses its dedicated /api/nhl/standings route."""
    from engine.hockey import HOCKEY_LEAGUE_REGISTRY
    if league not in HOCKEY_LEAGUE_REGISTRY or league == "nhl":
        return []
    try:
        mod = __import__(f"engine.sports.{league}.db",
                         fromlist=["get_conn"])
        conn = mod.get_conn()
    except Exception:
        return []
    season = _current_hockey_season()

    # Pull every team and walk per-team game history. Doing this in
    # Python is straightforward + cheap (small team counts) and lets us
    # compute streak + L10 without a window-function SQL gymnastic.
    teams = conn.execute(
        "SELECT id, full_name, short_name, abbreviation, "
        "       division, conference, logo_url FROM teams"
    ).fetchall()
    games = conn.execute(
        "SELECT id, date, home_team_id, away_team_id, home_score, "
        "       away_score, has_overtime, has_shootout "
        "FROM games WHERE season = ? AND status = 'final' "
        "  AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date ASC, id ASC",
        (season,),
    ).fetchall()

    by_team: dict[int, list[dict]] = {}
    for g in games:
        for tid in (g["home_team_id"], g["away_team_id"]):
            by_team.setdefault(tid, []).append(g)

    out = []
    for t in teams:
        tid = t["id"]
        team_games = by_team.get(tid, [])
        wins = losses = otl = 0
        gf = ga = 0
        results: list[str] = []  # 'W' / 'L' / 'OTL' per game in date order
        for g in team_games:
            is_home = g["home_team_id"] == tid
            scored = g["home_score"] if is_home else g["away_score"]
            allowed = g["away_score"] if is_home else g["home_score"]
            gf += scored; ga += allowed
            if scored > allowed:
                wins += 1; results.append("W")
            elif scored < allowed:
                # OT/SO loss = 1 standings point per NHL convention.
                if g["has_overtime"] or g["has_shootout"]:
                    otl += 1; results.append("OTL")
                else:
                    losses += 1; results.append("L")
            else:
                results.append("T")  # rare in hockey

        points = wins * 2 + otl
        played = wins + losses + otl
        pct = round((wins + 0.5 * otl) / played, 3) if played else 0.0
        # Last 10 games (oldest of those 10 first by date)
        last10 = results[-10:]
        l10w = sum(1 for r in last10 if r == "W")
        l10l = sum(1 for r in last10 if r == "L")
        l10o = sum(1 for r in last10 if r == "OTL")
        # Streak: walk backwards from latest result, count run of same kind.
        streak = ""
        if results:
            kind = results[-1]
            n = 0
            for r in reversed(results):
                if r != kind:
                    break
                n += 1
            # NHL convention: 'W3', 'L2', 'OT1'. Map OTL → OT for display.
            code = {"W": "W", "L": "L", "OTL": "OT", "T": "T"}.get(kind, kind)
            streak = f"{code}{n}"

        out.append({
            "team_id": tid,
            "name": t["full_name"],
            "abbreviation": t["short_name"] or t["abbreviation"] or "",
            "logo": t["logo_url"] or "",
            "division": t["division"] or "",
            "conference": t["conference"] or "",
            "record": f"{wins}-{losses}-{otl}",
            "wins": wins, "losses": losses, "otl": otl,
            "points": points,
            "gf": gf, "ga": ga, "diff": gf - ga,
            "l10": f"{l10w}-{l10l}-{l10o}",
            "streak": streak,
            "win_pct": pct,
            # Frontend basketball-style fields kept as aliases so the
            # generic table renderer still works when the consumer
            # doesn't switch on hockey-specific columns.
            "points_for": gf, "points_against": ga,
        })

    out.sort(key=lambda r: (-r["points"], -(r["diff"]), -r["wins"]))
    return out


@router.get("/api/hockey/{league}/calibration")
def api_hockey_calibration(league: str) -> dict:
    """Per-league calibration snapshot. Returns the fitted constants and
    a basic Brier breakdown placeholder (filled in once picks accumulate)."""
    from engine.hockey import HOCKEY_LEAGUE_REGISTRY
    if league not in HOCKEY_LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    cfg = HOCKEY_LEAGUE_REGISTRY[league]
    constants = {
        "league_avg_gpg": cfg.get("league_avg_gpg"),
        "league_avg_total": cfg.get("league_avg_total"),
        "home_boost": cfg.get("home_boost"),
        "status": cfg.get("status"),
    }
    return {
        "league": league,
        "constants": constants,
        # Picks-based metrics (Brier, hit rate, ROI) will fill in once
        # tracker tables exist for AHL/PWHL. Empty for now keeps the
        # frontend layout stable.
        "buckets": [],
        "n_settled": 0,
    }


@router.get("/api/hockey/{league}/tracker/history")
def api_hockey_tracker_history(league: str) -> dict:
    """Tracker history — picks recorded via /tracker/record, settled by
    /tracker/settle. Returns the standard PickHistory shape."""
    from engine.hockey import HOCKEY_LEAGUE_REGISTRY
    if league not in HOCKEY_LEAGUE_REGISTRY or league == "nhl":
        return {"summary": _empty_tracker_summary(), "history": []}
    try:
        from engine.hockey._tracker import list_history
        return list_history(league)
    except Exception as e:
        logger.warning("[%s] tracker history failed: %s", league, e)
        return {"summary": _empty_tracker_summary(), "history": []}


def _empty_tracker_summary() -> dict:
    return {"by_type": {}, "totals": {"hits": 0, "losses": 0,
                                        "pushes": 0, "voids": 0},
             "n_settled": 0, "roi": 0.0, "profit": 0.0}


@router.get("/api/hockey/{league}/potd/summary")
def api_hockey_potd_summary(league: str) -> dict:
    """POTD lifetime summary (W-L-P + ROI). Mirrors basketball/potd/summary."""
    from engine.hockey import HOCKEY_LEAGUE_REGISTRY
    if league not in HOCKEY_LEAGUE_REGISTRY or league == "nhl":
        return {"sport": league, "wins": 0, "losses": 0, "pushes": 0,
                 "pending": 0, "profit": 0.0, "roi": 0.0, "win_pct": 0.0}
    try:
        from engine.pick_of_day import potd_summary
        return potd_summary(league)
    except Exception as e:
        logger.debug("[%s] potd summary failed: %s", league, e)
        return {"sport": league, "wins": 0, "losses": 0, "pushes": 0,
                 "pending": 0, "profit": 0.0, "roi": 0.0, "win_pct": 0.0}


@router.get("/api/hockey/{league}/potd")
def api_hockey_potd(league: str) -> dict:
    """Pick of the Day for hockey-framework leagues (AHL, PWHL).

    Locks at first creation per (league, date). Reads today's slate,
    picks the highest-conviction non-skip best_pick, persists. NHL
    POTD lives at its own /api/pick-of-day/nhl path."""
    from engine.hockey import HOCKEY_LEAGUE_REGISTRY
    from engine.pick_of_day import get_or_create_potd, get_today_potd
    if league not in HOCKEY_LEAGUE_REGISTRY:
        return {"error": f"unknown league {league!r}"}
    if league == "nhl":
        return {"error": "NHL POTD lives at /api/pick-of-day/nhl"}

    slate = api_hockey_today(league)
    games = slate.get("games") or []
    bets: list[dict] = []
    for g in games:
        if g.get("status") != "pre_game":
            continue
        bp = g.get("best_pick")
        if not bp:
            continue
        bp_norm = dict(bp)
        if "prob" not in bp_norm and "raw_prob" in bp_norm:
            bp_norm["prob"] = bp_norm["raw_prob"]
        home_abbr = g.get("home_short") or "?"
        away_abbr = g.get("away_short") or "?"
        bets.append({
            "game_id": g.get("id"),
            "matchup": f"{away_abbr} @ {home_abbr}",
            "best_pick": bp_norm,
            "home": {"name": g.get("home_name") or home_abbr,
                      "abbreviation": home_abbr},
            "away": {"name": g.get("away_name") or away_abbr,
                      "abbreviation": away_abbr},
            "time": g.get("start_time") or "",
            "venue": g.get("venue") or "",
            "status": {"state": "pre"},
        })
    potd = get_today_potd(league, view="q1")
    if potd:
        return potd
    if bets:
        potd = get_or_create_potd(league, bets, view="q1")
        return potd or {"message": "No qualifying picks today",
                          "sport": league}
    return {"message": "No slate available", "sport": league}


@router.post("/api/hockey/{league}/tracker/settle")
def api_hockey_tracker_settle(league: str) -> dict:
    """Manual settle trigger — walk pending picks for ``league`` and
    settle anything whose game has gone final since last poll."""
    from engine.hockey import HOCKEY_LEAGUE_REGISTRY
    if league not in HOCKEY_LEAGUE_REGISTRY or league == "nhl":
        return {"settled": 0}
    try:
        from engine.hockey._tracker import settle_pending
        return settle_pending(league)
    except Exception as e:
        logger.warning("[%s] tracker settle failed: %s", league, e)
        return {"error": str(e)}


@router.post("/api/hockey/{league}/tracker/record")
def api_hockey_tracker_record(league: str) -> dict:
    """Persist today's best_pick from each game into the league's
    picks table. Idempotent — duplicates blocked by partial UNIQUE."""
    from engine.hockey import HOCKEY_LEAGUE_REGISTRY
    if league not in HOCKEY_LEAGUE_REGISTRY or league == "nhl":
        return {"recorded": 0, "skipped": 0}
    try:
        from engine.hockey._tracker import record_picks, settle_pending
        slate = api_hockey_today(league)
        rec = record_picks(league, slate.get("games", []))
        # Settle anything finalised since the last poll while we're here.
        sett = settle_pending(league)
        return {**rec, "settled": sett.get("settled", 0)}
    except Exception as e:
        logger.warning("[%s] tracker record failed: %s", league, e)
        return {"error": str(e)}


@router.get("/api/hockey/{league}/pick-events")
def api_hockey_pick_events(league: str, game_id: int,
                            hours: int = 24) -> dict:
    """Pick-transition events for one game over the last N hours. Maps
    to the same shape /api/{sport}/pick-events uses (NHL/NBA), so the
    PickEventsBadge component renders without per-league branching.

    Today AHL/PWHL just have a single 'appeared' event per pick — no
    swap / pull / line_shift telemetry until the live worker tracks
    line moves for these leagues. Empty array when no picks recorded.
    """
    from engine.hockey import HOCKEY_LEAGUE_REGISTRY
    if league not in HOCKEY_LEAGUE_REGISTRY or league == "nhl":
        return {"events": []}
    try:
        mod = __import__(f"engine.sports.{league}.db",
                         fromlist=["get_conn"])
        rows = mod.get_conn().execute(
            "SELECT bet_type, pick, edge, odds, confidence, "
            "       result, profit, created_at, settled_at "
            "FROM picks WHERE game_id = ? "
            "ORDER BY created_at DESC LIMIT 20",
            (int(game_id),),
        ).fetchall()
        events = []
        for r in rows:
            events.append({
                "kind": "appeared" if not r["result"] else (
                    "settled_w" if r["result"] == "W" else "settled_l"
                ),
                "bet_type": r["bet_type"],
                "pick": r["pick"],
                "edge": r["edge"],
                "odds": r["odds"],
                "confidence": r["confidence"],
                "ts": r["settled_at"] or r["created_at"],
                "result": r["result"],
                "profit": r["profit"],
            })
        return {"events": events}
    except Exception as e:
        logger.debug("[%s] pick-events failed for game %s: %s",
                      league, game_id, e)
        return {"events": []}


def _current_hockey_season() -> int:
    n = et_now()
    return n.year if n.month >= 9 else n.year - 1


def _team_by_short(conn, short: str) -> dict | None:
    """Resolve a team row from the per-league theScore-backed DB by
    short_name OR abbreviation (case-insensitive)."""
    if not short:
        return None
    n = short.lower()
    row = conn.execute(
        "SELECT id, full_name, short_name, abbreviation, "
        "       color_primary, logo_url FROM teams "
        "WHERE LOWER(short_name) = ? OR LOWER(abbreviation) = ?",
        (n, n),
    ).fetchone()
    return dict(row) if row else None
