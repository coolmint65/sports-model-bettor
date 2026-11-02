"""
NHL player-prop picker (Phase 2i-iii).

Mirrors ``engine.mlb_prop_picks`` for hockey. Skater + goalie props
share one code path; the bet_type → stat_key map decides which
sample array each prop reads from.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime

from ...player_props_db import insert_pick, _conn_for
from ...player_props_tracker import _NHL_STAT_KEY
from .player_mc import build_player_mc
from ..mlb.prop_picks import (
    _normalize_name, _confidence_for,
    PROP_MIN_EDGE_PCT, PROP_MAX_ODDS, PROP_JUICE_WALL,
    TOP_N_PER_GAME, TOP_N_PER_SPORT, _collapse_and_cap, _slate_rank_score,
)
from ..._tz import et_today_str, et_now
from ...mlb_player_mc import prob_over, prob_under

logger = logging.getLogger(__name__)


# ── Adaptive MC lookback (Austin 2026-10-01) ────────────────────────
#
# Replaces the old flat lookback_days=365 that the best-bets piggyback
# hardcoded. Verified live 2026-10-01 (season day 0): the picker's own
# 60-day default yields ZERO picks that early (1,403 props evaluated,
# 1,390 skipped_no_mc) because build_player_mc's min_games=5 floor
# can't be met from 60 days of a season that just started. 365 days
# reaches back into last season so there's an MC sample at all.
#
# Season-aware staircase (simpler + safer than a per-player retry loop
# inside build_player_mc, which would have to re-run per stat_key too):
#   days_in <  21  -> 365   (too early for a 60-120d window to fill
#                             min_games=5 for most of the roster)
#   days_in <  45  -> 120   (taper — current season has some data now)
#   days_in >= 45  ->  60   (plenty of current-season games; prefer
#                             recent-form over reaching back a year)
def _season_start(d: date) -> date:
    """NHL regular season opens ~Oct 1. Before July we're still in the
    back half of the season that started the PRIOR October."""
    year = d.year if d.month >= 7 else d.year - 1
    return date(year, 10, 1)


def _adaptive_lookback(target_date: str) -> int:
    try:
        target = datetime.strptime(target_date, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return 60
    days_in = (target - _season_start(target)).days
    if days_in < 21:
        return 365
    if days_in < 45:
        return 120
    return 60


# ── Headshot / team-logo URL builders (Austin 2026-10-01) ───────────
#
# Mirrors the already-shipped frontend helpers so the API payload and
# the DB-persisted picks carry the same URLs the UI would otherwise
# compute client-side (frontend/src/lib/playerPhoto.js, teamLogo.js).
# Both confirmed live via curl 2026-10-01 (200, real image bytes, not
# a placeholder) against a current roster player + all 32 team abbrs.
_NHLE_MUG = "https://assets.nhle.com/mugs/nhl/latest/{}.png"
# ESPN's NHL team-logo slugs match our DB abbreviations except these
# three (verified by curl against all 32 teams 2026-10-01) — same
# three exceptions frontend/src/lib/teamLogo.js already aliases.
_ESPN_NHL_LOGO_ALIAS = {"TBL": "TB", "LAK": "LA", "SJS": "SJ"}
_ESPN_LOGO = "https://a.espncdn.com/i/teamlogos/nhl/500/{}.png"


def _headshot_url(player_id: int | None) -> str | None:
    if not player_id:
        return None
    return _NHLE_MUG.format(int(player_id))


def _team_logo_url(team_abbr: str | None) -> str | None:
    if not team_abbr:
        return None
    slug = _ESPN_NHL_LOGO_ALIAS.get(team_abbr.upper(), team_abbr.upper())
    return _ESPN_LOGO.format(slug)


def _build_name_index() -> dict[str, list[dict]]:
    """Returns ``{normalized_lookup_key: [candidate_dict, ...]}``.

    Each candidate carries ``player_id, team_id, position, last_date``
    so the resolver can disambiguate same-name players using team
    context (NHL has Elias Pettersson the forward AND Elias Pettersson
    the defenseman on the same Vancouver roster) and bet-type's
    implied role (Goalie Saves only matches G; skater stats only
    match F/D).

    Each player gets multiple keys: full name AND ``initial lastname``
    so HR's "Mattias Ekholm" hits the boxscore's "M. Ekholm" entry.
    Per-key list preserves all candidates -- single-name lookups still
    work, but ambiguous ones surface every option for downstream
    filtering.
    """
    conn = _conn_for("nhl")
    rows = conn.execute(
        "SELECT player_id, player_name, team_id, "
        "       json_extract(stats_json, '$.position') AS position, "
        "       MAX(date) AS last_date "
        "FROM player_game_logs GROUP BY player_id ORDER BY last_date"
    ).fetchall()
    out: dict[str, list[dict]] = {}
    for r in rows:
        norm = _normalize_name(r["player_name"])
        if not norm:
            continue
        cand = {
            "player_id": int(r["player_id"]),
            "team_id":   int(r["team_id"]) if r["team_id"] is not None else 0,
            "position":  (r["position"] or "").upper() or "F",
            "last_date": r["last_date"] or "",
        }
        out.setdefault(norm, []).append(cand)
        parts = norm.split()
        if len(parts) >= 2:
            li_key = f"{parts[0][0]} {' '.join(parts[1:])}"
            if li_key != norm:
                out.setdefault(li_key, []).append(cand)
    return out


# bet_type → required position. Empty string means "anything but goalie".
_BET_TYPE_POSITION = {
    "Goalie Saves":          "G",
    "Goalie Goals Against":  "G",
    # All skater stats accept F or D, never G.
    "Skater Goals":   "skater",
    "Skater Assists": "skater",
    "Skater Points":  "skater",
    "Skater SOG":     "skater",
    "Skater Hits":    "skater",
    "Skater Blocks":  "skater",
}


def _resolve_player_id(name: str, idx: dict[str, list[dict]],
                        bet_type: str = "",
                        away_team_id: int = 0,
                        home_team_id: int = 0) -> int | None:
    """Resolve a player name to an id, using team + position context
    to disambiguate same-name candidates."""
    norm = _normalize_name(name)
    candidates: list[dict] = []
    if norm in idx:
        candidates.extend(idx[norm])
    parts = norm.split()
    if not candidates and len(parts) >= 2:
        li = f"{parts[0][0]} {' '.join(parts[1:])}"
        candidates.extend(idx.get(li, []))
    if not candidates:
        return None
    # Position filter: keep candidates whose role matches bet_type.
    required = _BET_TYPE_POSITION.get(bet_type, "")
    if required == "G":
        candidates = [c for c in candidates if c["position"] == "G"] or candidates
    elif required == "skater":
        candidates = [c for c in candidates if c["position"] != "G"] or candidates
    # Team filter: candidates on one of the two playing teams.
    if away_team_id or home_team_id:
        team_filtered = [c for c in candidates
                         if c["team_id"] in (away_team_id, home_team_id)]
        if team_filtered:
            candidates = team_filtered
    # Most recently active wins remaining ties.
    candidates.sort(key=lambda c: c["last_date"], reverse=True)
    return candidates[0]["player_id"] if candidates else None


def _resolve_game_id(games_conn: sqlite3.Connection,
                     away_abbr: str, home_abbr: str,
                     date: str) -> tuple[str | None, str | None]:
    """Resolve a (away, home) abbr pair to (game_id, actual_date).

    Hard Rock offers props for whichever games are next bookable —
    that's not always our ``target_date``. A series sitting on a 1-day
    rest day will surface tomorrow's props today; a postponed game
    crosses calendar boundaries. Searching strictly on ``date`` makes
    every such matchup fail at the resolver step.

    So search the upcoming-games window (yesterday→date+2) and return
    the earliest non-final match. Caller uses ``actual_date`` as the
    ``date`` field on the inserted pick so settling later finds the
    right game.
    """
    try:
        from ...abbr import aliases_for
        a_alts = aliases_for(away_abbr.strip(), sport="nhl") or [away_abbr.strip()]
        h_alts = aliases_for(home_abbr.strip(), sport="nhl") or [home_abbr.strip()]
    except Exception:
        a_alts = [away_abbr.strip()]
        h_alts = [home_abbr.strip()]
    abbrs = set(a_alts) | set(h_alts)
    if not abbrs:
        return None, None
    placeholders = ",".join("?" * len(abbrs))
    from datetime import datetime as _dt, timedelta as _td
    try:
        anchor = _dt.strptime(date, "%Y-%m-%d")
    except ValueError:
        anchor = _dt.now()
    lo = (anchor - _td(days=1)).strftime("%Y-%m-%d")
    hi = (anchor + _td(days=2)).strftime("%Y-%m-%d")
    row = games_conn.execute(
        f"SELECT g.game_id, g.date FROM nhl_games g "
        f"JOIN nhl_teams ht ON ht.id = g.home_team_id AND ht.abbreviation IN ({placeholders}) "
        f"JOIN nhl_teams at ON at.id = g.away_team_id AND at.abbreviation IN ({placeholders}) "
        f"WHERE g.date BETWEEN ? AND ? "
        f"  AND COALESCE(g.status, '') NOT IN ('final', 'postponed') "
        f"ORDER BY g.date ASC LIMIT 1",
        (*abbrs, *abbrs, lo, hi),
    ).fetchone()
    if not row:
        return None, None
    return str(row["game_id"]), str(row["date"])


_PROP_MIN_PROB = 0.40   # don't headline a play more likely to lose than win (Austin 2026-10-23)
# Card-lock timing (Austin 2026-10-26, "lock them as soon as"): NHL props lock the FIRST time
# HR's board prices the game -- usually the evening before -- not 09:00 game day like NBA / NFL.
# The opening prop lines are the soft ones (he took Shea / Fox U1.5 SOG at +125 / +115 the night
# before and held cash-out profit before puck drop), and the per-stat anchor weights are fit
# against the DK OPEN (prop_eval.DEFAULT_W), so the opener is what the model is tuned to beat.


def _main_line_rows(rows: list) -> list:
    """Per (player, bet_type), keep only the MAIN line -- the one whose
    market sits closest to a coin flip (implied prob nearest 0.5). Skips
    deep alt-lines (e.g. Under 0.5 on a 1.5-shot player) where the model
    is least calibrated and the close is thinnest. Austin 2026-10-23."""
    def _imp(o):
        if o is None:
            return None
        o = float(o)
        return 100.0 / (o + 100.0) if o > 0 else abs(o) / (abs(o) + 100.0)
    best: dict = {}
    for r in rows:
        key = (r.get("player_name"), r.get("bet_type"))
        imp = _imp(r.get("over_odds"))
        if imp is None:
            imp = _imp(r.get("under_odds"))
        bal = abs(imp - 0.5) if imp is not None else 1.0
        if r.get("over_odds") is None or r.get("under_odds") is None:
            bal += 1.0                     # prefer a two-sided rung (one-sided can't be anchored -> no pick)
        cur = best.get(key)
        if cur is None or bal < cur[0]:
            best[key] = (bal, r)
    return [v[1] for v in best.values()]


def _model_probs(samples: dict, prop: dict):
    """(p_over, p_under) from the player's MC samples after the per-stat shrink (and the
    derivative calibration when enabled) -- the model's own number, BEFORE the market
    anchor. None when the stat/line/samples are missing."""
    bet_type = prop.get("bet_type") or ""
    line = prop.get("line")
    if line is None:
        return None
    stat_key = _NHL_STAT_KEY.get(bet_type)
    if stat_key is None:
        return None
    samp = samples.get(stat_key)
    if samp is None or len(samp) == 0:
        return None
    p_over = prob_over(samp, line)
    p_under = prob_under(samp, line)
    from ...distribution_fit import get_prob_shrink
    shrink = get_prob_shrink("nhl", stat_key)
    if shrink < 1.0:
        p_over = 0.5 + (p_over - 0.5) * shrink
        p_under = 0.5 + (p_under - 0.5) * shrink
    # ---- Derivative/prop post-hoc calibration (2026-09-16) ----
    # De-bias per-stat overconfidence on the settled prop history
    # (data/derivative_prob_calibration.json), keyed "nhl:<bet_type>",
    # right before edge. Reversible via ENABLE_DERIV_CALIBRATION.
    try:
        from ...config import get_flag as _gf
        if _gf("ENABLE_DERIV_CALIBRATION", True, sport="nhl"):
            from ...derivative_prob_calibration import apply as _dcal
            _dk = "nhl:" + str(bet_type)
            p_over = _dcal(_dk, p_over)
            p_under = _dcal(_dk, p_under)
    except Exception:
        pass
    return p_over, p_under


_NO_PICK_TYPES = {"Skater Goals"}


def _score(samples: dict, prop: dict, probs=None) -> dict | None:
    if probs is None:
        probs = _model_probs(samples, prop)
    if probs is None:
        return None
    p_over, p_under = probs
    bet_type = prop.get("bet_type") or ""
    line = prop.get("line")
    over_odds = prop.get("over_odds")
    under_odds = prop.get("under_odds")
    # ---- Market-anchoring (Austin 2026-10-23) ----
    # Blend each side's prob toward the de-vigged market so an
    # overconfident thin-sample prob has to EARN its edge. The model's
    # share starts at 0.35 (~a third of its say) and is refit weekly per
    # stat from graded lines (prop_eval.weight_for, 2026-10-26).
    # Anytime goals (Skater Goals) are OUT as picks (Austin 2026-10-23 "not a prop I want"; it came back in
    # through Skater Goals Over 0.5). One-sided markets are out too (MintyBot 2026-11-01): with no Under price
    # the de-vig can't resolve, the anchor silently returned the RAW model, and raw anytime-goal probs of
    # .42-.50 on +185..+275 4th-liners became "+12-23% edges". Both still go to the eval log.
    if bet_type in _NO_PICK_TYPES or over_odds is None or under_odds is None:
        return None
    try:
        from ...market_blend import anchor_to_market as _anchor, devig_prob as _devig
        from .prop_eval import weight_for as _anchor_w
        if _devig(over_odds, under_odds) is None:
            return None
        _PROP_ANCHOR_W = _anchor_w(bet_type)
        p_over = _anchor(p_over, over_odds, under_odds, w=_PROP_ANCHOR_W)
        p_under = _anchor(p_under, under_odds, over_odds, w=_PROP_ANCHOR_W)
    except Exception:
        return None                        # no anchor = no pick (never price a prop off the raw model)
    cands: list[dict] = []
    for side, odds, p in [("Over", over_odds, p_over), ("Under", under_odds, p_under)]:
        if odds is None:
            continue
        if odds > PROP_MAX_ODDS:
            continue
        if odds < PROP_JUICE_WALL:
            continue
        if p < _PROP_MIN_PROB:
            continue
        # Hard Rock UI is Over-only for these bet types even though
        # the API ships under_odds. Recommending an Under pick the
        # user can't actually place on their sportsbook is worse than
        # recommending nothing. Add bet types here as the UX gap is
        # confirmed (Goalie Goals Against added 2026-05-03).
        if bet_type in {"Skater Goals", "Goalie Goals Against"} and side == "Under":
            continue
        n = float(odds)
        implied = 100.0/(n+100.0) if n > 0 else abs(n)/(abs(n)+100.0)
        edge = (p - implied) * 100.0
        cands.append({"side": side, "line": float(line), "odds": int(odds),
                      "model_prob": p, "implied_prob": implied, "edge": edge})
    if not cands:
        return None
    best = max(cands, key=lambda c: c["edge"])
    if best["edge"] < PROP_MIN_EDGE_PCT:
        return None
    return best


def generate_picks(date: str | None = None,
                   props: dict | None = None,
                   *,
                   n_sims: int = 10_000,
                   lookback_days: int | None = None) -> dict:
    target_date = date or et_today_str()
    # None -> season-aware adaptive default (see _adaptive_lookback above).
    # An explicit int still wins, so callers/tests that want a fixed
    # window keep working unchanged.
    if lookback_days is None:
        lookback_days = _adaptive_lookback(target_date)
    if props is None:
        from scrapers.hardrock_props import fetch_nhl_props
        props = fetch_nhl_props()

    name_idx = _build_name_index()
    if not name_idx:
        return {"evaluated": 0, "picked": 0,
                "skipped_no_player": 0, "skipped_no_mc": 0,
                "skipped_low_edge": 0,
                "warning": "name index empty"}

    # Flattened player_id -> team_id, derived from the same candidates
    # name_idx already fetched (no extra query). Feeds headshot/
    # team_logo URL construction at insert time below.
    pid_to_team_id: dict[int, int] = {}
    for _cands in name_idx.values():
        for _c in _cands:
            pid_to_team_id.setdefault(_c["player_id"], _c["team_id"])

    mc_cache: dict[int, dict] = {}
    eval_rows: list[dict] = []   # every priced main line -> prop_eval (weekly blend refit)
    counts = {"evaluated": 0, "picked": 0,
              "skipped_no_player": 0, "skipped_no_mc": 0,
              "skipped_low_edge": 0}

    from .db import get_conn as _nhl_conn
    games_conn = _nhl_conn()
    # HR posts the next slate's props the evening before, but nothing ingested tomorrow's games until the next
    # morning, so every matchup failed to resolve ("skipped_no_player": 560 on 10-30 at 00:05). Pull the
    # schedule for today .. +2 days first (3 NHL API calls; upserts, so it's idempotent). Austin 2026-10-30.
    try:
        from datetime import datetime as _dt2, timedelta as _td2
        from scrapers.nhl_api import fetch_schedule as _fetch_schedule
        _d0 = _dt2.strptime(target_date, "%Y-%m-%d")
        _fetch_schedule(target_date, (_d0 + _td2(days=2)).strftime("%Y-%m-%d"))
    except Exception as _e:
        logger.warning("nhl props: schedule refresh failed: %s", _e)

    # Two-tier cap: per-matchup dedup feeds the game-day card lock
    # (_record_game_day). Each entry carries its resolved game date
    # because HR's listed matchup may be 1 day off from target_date.
    slate_winners: list[tuple[dict, str, str, str]] = []

    for matchup_key, prop_rows in props.items():
        if "@" not in matchup_key:
            continue
        away_abbr, home_abbr = matchup_key.split("@", 1)
        game_id, game_date = _resolve_game_id(games_conn, away_abbr, home_abbr,
                                               target_date)
        if not game_id:
            counts["skipped_no_player"] += len(prop_rows)
            continue
        # Use the resolved game's actual date so settling can locate
        # the boxscore (HR's matchup may be 1 day off from target_date).
        pick_date = game_date or target_date

        # Resolve game's team_ids once for use in name disambiguation.
        gt_row = games_conn.execute(
            "SELECT home_team_id, away_team_id FROM nhl_games WHERE game_id = ?",
            (game_id,),
        ).fetchone()
        away_tid = gt_row["away_team_id"] if gt_row else 0
        home_tid = gt_row["home_team_id"] if gt_row else 0

        # Dedup at (player_id, bet_type) — keep highest-edge line per
        # pair. See mlb_prop_picks for full rationale (HR ships ~6 alt
        # lines per stat per player; without this filter the UI shows
        # one player with multiple picks for the same stat).
        best_per_pair: dict[tuple, dict] = {}
        for prop in _main_line_rows(prop_rows):
            counts["evaluated"] += 1
            player_name = prop.get("player_name") or ""
            bet_type = prop.get("bet_type", "")
            player_id = _resolve_player_id(player_name, name_idx,
                                            bet_type=bet_type,
                                            away_team_id=away_tid,
                                            home_team_id=home_tid)
            if not player_id:
                counts["skipped_no_player"] += 1
                continue
            cache_key = (player_id, str(game_id))
            if cache_key not in mc_cache:
                mc_cache[cache_key] = build_player_mc(
                    player_id, n_sims=n_sims,
                    lookback_days=lookback_days,
                    game_id=str(game_id),
                )
            samples = mc_cache[cache_key]
            probs = _model_probs(samples, prop) if samples else None
            if probs is not None:
                _ev = {
                    "game_id": str(game_id), "game_date": pick_date,
                    "player_id": player_id, "player_name": player_name,
                    "bet_type": bet_type, "line": prop.get("line"),
                    "over_odds": prop.get("over_odds"), "under_odds": prop.get("under_odds"),
                    "p_over": probs[0]}
                # SOG v2 (2026-11-01): tag the model and keep logging the v1 number beside it.
                if bet_type == "Skater SOG" and samples and "sog_v1" in samples:
                    _ev["model"] = "sog_v2"
                    _p1 = _model_probs({"sog": samples["sog_v1"]}, prop)
                    _ev["p_over_v1"] = _p1[0] if _p1 else None
                eval_rows.append(_ev)
            scored = _score(samples, prop, probs)
            if scored is None:
                if not samples:
                    counts["skipped_no_mc"] += 1
                else:
                    counts["skipped_low_edge"] += 1
                continue
            # Data-driven NOPLAY gate — same path as NBA props.
            from ...edge_floors import is_noplay as _is_noplay
            pick_text = f"{scored['side']} {scored['line']:g}"
            if _is_noplay("nhl", bet_type, pick_text=pick_text):
                counts["skipped_noplay"] = counts.get("skipped_noplay", 0) + 1
                continue
            pair_key = (player_id, bet_type)
            if pair_key in best_per_pair and \
                    scored["edge"] <= best_per_pair[pair_key]["scored"]["edge"]:
                continue
            # Which side of the matchup the player is on, for the
            # team_logo URL. Falls back to None (no logo) on the rare
            # case a resolved candidate's team_id matches neither side.
            pid_team = pid_to_team_id.get(player_id)
            if pid_team == away_tid:
                team_abbr = away_abbr
            elif pid_team == home_tid:
                team_abbr = home_abbr
            else:
                team_abbr = None
            best_per_pair[pair_key] = {
                "player_id": player_id, "player_name": player_name,
                "prop": prop, "scored": scored, "team_abbr": team_abbr,
            }

        for entry in _collapse_and_cap(best_per_pair):
            slate_winners.append(
                (entry, str(game_id), pick_date,
                 f"{away_abbr} @ {home_abbr}"))

    try:
        from .prop_eval import log_lines as _log_eval
        _log_eval(eval_rows)
    except Exception as e:
        logger.warning("nhl_prop_picks: eval log failed: %s", e)
    counts["picked"] = _record_game_day(slate_winners)

    logger.info("nhl_prop_picks: %s", counts)
    return counts


def _record_game_day(slate_winners: list) -> int:
    """CARD LOCK (MintyBot 2026-10-26, the NBA / NFL props standard; early lock the same day):
    for every game date from today on, fill up to TOP_N_PER_SPORT picks with at most
    TOP_N_PER_GAME per game, best edge x reliability first, the first time HR's board prices the
    game (no game-day hour gate -- see the note above _main_line_rows). Picks already recorded
    for a date count against its caps and are never voided or swapped -- the old slate re-rank
    deleted a pending pick whenever a later run (every ~3h) ranked it out, so the card chased
    the line and the CLV read lied."""
    today = et_now().strftime("%Y-%m-%d")
    by_date: dict[str, list] = {}
    for t in slate_winners:
        if t[2] and t[2] >= today:
            by_date.setdefault(t[2], []).append(t)
    if not by_date:
        return 0
    conn = _conn_for("nhl")
    # CARD GATE (MintyBot 2026-11-01, see card_gate.py): the rule's picks only reach the real card
    # from a PROVEN bet_type x side cell; the rest go to prop_card_shadow (same caps, graded) so the
    # evidence keeps building without Austin's money on it. Fail-closed: gate error -> shadow.
    from . import card_gate as _gate
    try:
        _gate.grade_shadow()
        gate_recs = _gate.cell_records()
    except Exception as e:
        logger.warning("nhl_prop_picks: card gate unavailable (fail-closed, shadow only): %s", e)
        gate_recs = {}
    rec = 0
    for day in sorted(by_date):
        per_game = {str(r["game_id"]): int(r["n"]) for r in conn.execute(
            "SELECT game_id, COUNT(*) AS n FROM player_props_picks WHERE date = ? GROUP BY game_id",
            (day,))}
        try:
            for _g, _n in _gate.shadow_counts(day).items():      # shadow picks hold their slot too
                per_game[_g] = per_game.get(_g, 0) + _n
        except Exception as e:
            logger.warning("nhl_prop_picks: shadow count failed: %s", e)
        slots = TOP_N_PER_SPORT - sum(per_game.values())
        if slots <= 0:
            continue
        entries = [t for t in by_date[day] if per_game.get(str(t[1]), 0) < TOP_N_PER_GAME]
        entries.sort(key=lambda t: _slate_rank_score("nhl", t[0]), reverse=True)
        for entry, game_id, pick_date, matchup in entries[:slots]:
            best = entry["scored"]
            _bt = entry["prop"].get("bet_type", "")
            if not _gate.is_proven(_bt, best["side"], gate_recs):
                try:
                    if _gate.record_shadow(game_id, pick_date, matchup, entry["player_id"],
                                           entry["player_name"], _bt, best):
                        logger.info("nhl_prop_picks: SHADOW (cell not proven) %s %s %s %s %s @%s (edge %+.1f)",
                                    matchup, entry["player_name"], _bt, best["side"], best["line"],
                                    best["odds"], best["edge"])
                except Exception as e:
                    logger.warning("nhl_prop_picks: shadow write failed: %s", e)
                continue
            pid = insert_pick(
                "nhl",
                game_id=game_id, date=pick_date,
                matchup=matchup,
                player_id=entry["player_id"],
                player_name=entry["player_name"],
                bet_type=entry["prop"].get("bet_type", ""),
                pick=f"{best['side']} {best['line']:g}",
                line=best["line"], side=best["side"],
                model_prob=best["model_prob"],
                edge=best["edge"], odds=best["odds"],
                confidence=_confidence_for(best["edge"]),
                headshot=_headshot_url(entry["player_id"]),
                team_logo=_team_logo_url(entry.get("team_abbr")),
            )
            if pid:
                rec += 1
                logger.info("nhl_prop_picks: LOCKED %s %s %s %s %s @%s (edge %+.1f)", matchup,
                            entry["player_name"], entry["prop"].get("bet_type", ""), best["side"],
                            best["line"], best["odds"], best["edge"])
    return rec


__all__ = ["generate_picks"]
