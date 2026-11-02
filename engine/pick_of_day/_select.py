"""POTD selection + creation.

Two entry points:
  - select_potd      — pure function: given today's bets, return the
                       single best candidate (no DB writes)
  - get_or_create_potd — locks the selection to a DB row; idempotent

POTD per-game candidate is `bet.best_pick` (or NBA's `best_pick_full`/
`best_pick_q1` when applicable). Using the GameCard's already-chosen
headline guarantees POTD always agrees with the card on which pick
from a given game qualifies. Cross-game ranking still happens via
`_score_pick`. See feedback memory `project_sports_model.md` for the
"POTD shows +1.5 RL but card shows ML" incident this design avoids.
"""

from __future__ import annotations
import logging
from datetime import datetime
from typing import Any

from ._storage import _get_conn, _ensure_potd_table, _potd_table, _implied_from_odds
from ._scoring import _score_pick, _kelly_fraction, MIN_EDGE, MIN_PROB
from .._tz import et_today_str

# Airtight POTD calibration (MintyBot 2026-09-16): sports whose best_pick carries the RAW
# model prob → run it through the same per-league calibrator the card uses (and recompute
# edge) INSIDE select_potd, so the POTD's edge filter + stored prob + edge all match the
# card's calibrated pick logic. Scoped to an allowlist so we never double-calibrate a sport
# whose bets are already calibrated. (wnba verified RAW: its stored prob ~0.84 exceeds the
# wnba calibrator's 0.67 ceiling, so it cannot already be calibrated.)
_POTD_CALIBRATE_SPORTS = {"wnba"}
# NHL/NBA/MLB: empirical (Bayesian) calibration instead of final_calibration, whose
# top bin maps high->1.0 (the fake 100%/+35% POTD edges). Empirical caps to the
# (sport,bet_type,bucket) posterior (~0.79). MintyBot 2026-10-06.
# nhl removed from empirical POTD recal 2026-10-18 (MintyBot, Austin "anchor it is"):
# NHL best_pick now carries the market-anchored/recorded edge (card fix in routes_nhl),
# not the raw model prob, so re-running cal_prob here DOUBLE-calibrates it. The empirical
# NHL period-total cell is mis-fit (inflates 0.63->0.76 vs the ~54% realized base rate),
# which turned the anchored P1 +6.6 back into +19.8 on the POTD hero. DEEPER TODO:
# retune/cap that empirical cell to the base rate so the scorer path is right too.
_POTD_EMPIRICAL_SPORTS = {"nba", "mlb",
    # Soccer leagues (Austin 2026-10-12): EPL + the other top leagues now
    # calibrate their POTD prob/edge instead of shipping raw. Keep in sync
    # with empirical_calibration._SOCCER_LEAGUES.
    "eng_premier", "esp_laliga", "ita_seriea", "ger_bundesliga",
    "fra_ligue1", "bra_seriea", "arg_lpf", "conmebol_libertadores"}
# Tennis: per-bet-type Platt logistic (empirical cells too thin for tennis favorites)
_POTD_TENNIS_SPORTS = {"tennis"}
# Union of every sport the POTD calibrates. A sport NOT in this set ships raw (unchanged).
_POTD_CALIBRATED_SPORTS = _POTD_CALIBRATE_SPORTS | _POTD_EMPIRICAL_SPORTS | _POTD_TENNIS_SPORTS

logger = logging.getLogger(__name__)


def _ensure_potd_mirror(sport, conn, picks_table,
                          target_date, game_id, matchup,
                          bet_type, pick_text,
                          model_prob, edge, odds) -> None:
    """Idempotent mirror INSERT — succeeds when the row already exists,
    inserts when it doesn't. Called both during POTD creation and lazily
    on settle/read paths so orphaned POTDs (committed but mirror-INSERT
    crashed) heal on the next access rather than accumulating."""
    try:
        # The game-id column name differs by target table: the {sport}_picks
        # tables (nhl/nba/tennis) and the mlb/wnba "picks" tables use game_id,
        # but the soccer-league "picks" tables use match_id. Detect from the live
        # schema so the mirror works for both (fixes the recurring "no such column:
        # game_id" POTD-mirror failure for soccer). The id VALUE is the same ESPN
        # event-id scheme in both tables (verified), so dedup still matches the
        # picks-engine's pending row. MintyBot 2026-10-15.
        _tcols = {r[1] for r in conn.execute(f"PRAGMA table_info({picks_table})")}
        gid_col = "game_id" if "game_id" in _tcols else (
            "match_id" if "match_id" in _tcols else None)
        if gid_col is None:
            logger.warning("POTD mirror to %s: no game_id/match_id column — skipping %s/%s",
                           picks_table, sport, bet_type)
            return
        # Dedup on (game_id, bet_type, result IS NULL) — the picks-engine
        # may have already inserted a pending row for this game under
        # the GAME's date (e.g. 2026-05-12 for a Sunday-night WNBA tipoff)
        # while POTD's target_date is today ET (2026-05-11). Matching on
        # (target_date, game_id, bet_type) would miss it and create a
        # second pending row — exactly the WNBA ATL@DAL bug that surfaced
        # 2026-05-12.
        existing = conn.execute(
            f"SELECT id FROM {picks_table} "
            f"WHERE {gid_col}=? AND bet_type=? "
            f"  AND (result IS NULL OR result = '') "
            f"LIMIT 1",
            (game_id, bet_type),
        ).fetchone()
        if existing:
            return
        cur = conn.execute(
            f"INSERT OR IGNORE INTO {picks_table} ("
            f"  date, {gid_col}, matchup, bet_type, pick,"
            "  model_prob, edge, odds"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (target_date, game_id, matchup, bet_type, pick_text,
             model_prob, edge, odds),
        )
        conn.commit()
        logger.info("POTD mirror %s: inserted pick id=%s for %s/%s/%s",
                    sport, cur.lastrowid, target_date, bet_type, pick_text)
    except Exception as e:
        logger.warning("POTD mirror to %s FAILED for %s/%s/%s: %s",
                       picks_table, sport, target_date, bet_type, e,
                       exc_info=True)


_NBA_Q1_TYPES = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
_NBA_FULL_PRIMARY = {"ML", "SPREAD", "TOTAL"}
_NBA_FULL_ALT = {"ALT SPREAD", "ALT TOTAL"}
_NBA_FULL_TYPES = _NBA_FULL_PRIMARY | _NBA_FULL_ALT


_BET_TYPE_LABELS = {
    "ML": "moneyline", "RL": "run line", "PL": "puck line",
    "O/U": "over/under", "1st INN": "first inning",
    "ALT RL": "alt run line", "ALT O/U": "alt over/under",
    "ALT PL": "alt puck line",
    "Q1_ML": "Q1 moneyline", "Q1_SPREAD": "Q1 spread",
    "Q1_TOTAL": "Q1 total",
    "F5 ML": "first 5 moneyline", "F5 OU": "first 5 over/under",
    # Soccer markets
    "1X2": "1X2 match result", "DC": "double chance", "DNB": "draw no bet",
    "OU": "goals over/under", "BTTS": "both teams to score",
    "AH": "asian handicap",
    "H1_1X2": "first-half 1X2", "H1_DC": "first-half double chance",
    "H1_DNB": "first-half draw no bet", "H1_TOTAL": "first-half total",
    "H1_BTTS": "first-half both teams to score",
}


def _filter_picks_for_view(picks: list, sport: str, view: str) -> list:
    """For NBA, restrict pick candidates to the right category set.
    ALT TOTAL/SPREAD picks at +60%+ edges are the calibration trap —
    POTD only surfaces primary-market picks. A game whose primary
    picks don't qualify gets EXCLUDED from POTD candidacy entirely
    rather than falling back to its ALT lines.
    """
    if sport != "nba" or view not in ("q1", "full"):
        return picks
    if view == "q1":
        return [p for p in picks if p.get("type") in _NBA_Q1_TYPES]
    return [p for p in picks if p.get("type") in _NBA_FULL_PRIMARY]


def _build_reasoning(pick: dict, sport: str) -> str:
    """Generate a human-readable explanation for the POTD selection."""
    edge = pick.get("edge", 0)
    prob = pick.get("prob", 0)
    bet_type = pick.get("type", "")
    bt_label = _BET_TYPE_LABELS.get(bet_type, bet_type.replace("_", " "))

    strength = "strong" if edge > 8 else "moderate" if edge > 5 else "lean"

    pick_display = pick.get("pick_full", pick.get("pick", ""))
    implied = _implied_from_odds(pick.get("odds", 0)) * 100

    parts = []
    # Resolve display name from registries when available so framework
    # leagues read "China CBA slate" not "CHINA_CBA slate". Falls back
    # to the upper-cased key for sports that don't have a registry.
    sport_label = sport.upper()
    try:
        from engine.basketball import LEAGUE_REGISTRY as _BB
        if sport in _BB:
            sport_label = _BB[sport].get("display_name") or sport_label
    except Exception:
        pass
    try:
        from engine.hockey import HOCKEY_LEAGUE_REGISTRY as _HK
        if sport in _HK:
            sport_label = _HK[sport].get("display_name") or sport_label
    except Exception:
        pass
    try:
        from engine.soccer import LEAGUE_REGISTRY as _SOC_R
        if sport in _SOC_R:
            sport_label = _SOC_R[sport].get("display_name") or sport_label
    except Exception:
        pass
    try:
        from engine.football import LEAGUE_REGISTRY as _FB_R
        if sport in _FB_R:
            sport_label = _FB_R[sport].get("display_name") or sport_label
    except Exception:
        pass
    parts.append(f"Best {bt_label} play on today's {sport_label} slate.")
    parts.append(f"Model gives {pick_display} a {prob * 100:.1f}% chance "
                 f"vs the market's {implied:.1f}%.")
    parts.append(f"That's a {edge:+.1f}% edge with {strength} conviction.")
    return " ".join(parts)


def _pick_side(bet_type: str, pick_text: str, home_abbr: str = "",
                away_abbr: str = "") -> str:
    """Direction key used to match a POTD against a live pick at a
    different line. ``Under 53.5`` and ``Under 54.5`` are the same
    side; ``MIN +10.5`` and ``MIN +9.5`` likewise.

    Returns 'over' / 'under' for total-style markets, the team abbr
    for side-style markets, and '' when the pick doesn't fit either.
    """
    if not pick_text:
        return ""
    pk = pick_text.strip()
    pk_lower = pk.lower()
    if pk_lower.startswith("over"):
        return "over"
    if pk_lower.startswith("under"):
        return "under"
    head = pk.split()[0] if pk.split() else ""
    return head


def _is_game_locked(game: dict) -> bool:
    """True when the game has tipped off / first-pitched / dropped the
    puck. POTDs shouldn't be CREATED on locked games — there's no
    "place a pre-game bet" window left, and any pick we'd select would
    immediately freeze under the live-lock rule. The card's own status
    detail is the source of truth (state in {'in', 'post'} per ESPN's
    convention)."""
    status = game.get("status") or {}
    state = (status.get("state") or "").lower()
    return state in ("in", "post")


# Per-sport caps on the model's expected score, set well ABOVE realistic maxima so only
# physically-impossible projections (early-season small-n blowups) trip them -- never a
# legitimately high-scoring game. A game that breaches is excluded from POTD candidacy
# entirely (its "edges" are artifacts). MintyBot 2026-10-08, after COL -1.5 +20.6% headlined
# on expected_score COL 8.87 / CGY 1.01 (total 9.88) -- see nhl_potd_pl_margin_ghost.md.
# Real maxima for reference: NHL team ~5 / total ~8; NBA team ~140 / total ~260;
# MLB team ~7 / total ~13; WNBA team ~100 / total ~185.
_PROJ_CAPS = {
    "nhl":  {"team": 7.0,   "total": 9.0},
    "nba":  {"team": 160.0, "total": 300.0},
    "mlb":  {"team": 15.0,  "total": 22.0},
    "wnba": {"team": 130.0, "total": 230.0},
}


def _projection_implausible(sport: str, game: dict) -> bool:
    """True when the model's expected_score for this game breaches the sport's
    plausibility cap = a broken projection whose "edge" is an artifact. Fail-OPEN:
    a missing / malformed / unknown-sport projection returns False (we only exclude
    on a projection we can SEE is impossible, never on absence of data)."""
    caps = _PROJ_CAPS.get(sport)
    if not caps:
        return False
    es = game.get("expected_score") or {}
    try:
        h = float(es.get("home"))
        a = float(es.get("away"))
    except (TypeError, ValueError):
        return False
    if h <= 0 and a <= 0:
        return False
    return h > caps["team"] or a > caps["team"] or (h + a) > caps["total"]


def _ep_eligible(sport: str, league: str, bet_type: str, pick_text: str = "", odds=None) -> bool:
    """Pick Authority gate: True only if edge_proven has PROVEN this
    (sport, league, market, condition). `condition` (fav/dog/over/under) is derived from
    pick_text/odds the same way select_pick does it. Fail-OPEN on an edge_proven error so a
    transient gate failure can't nuke every POTD; normal ineligibility is False.
    (Austin 2026-10-21 "Pick authority"; league-segmented 2026-10-22; condition-segmented
    2026-10-22.)"""
    try:
        from engine.selection import edge_proven
        cond = edge_proven._condition(pick_text, bet_type, odds)
        return bool(edge_proven.is_eligible(sport, league, bet_type, cond))
    except Exception:
        return True


def select_potd(sport: str, games_with_bets: list[dict],
                view: str = "q1", db_sport: str | None = None,
                league: str | None = None) -> dict | None:
    """
    Select the Pick of the Day from a list of games with bets.

    Args:
        sport: "mlb" / "nhl" / "nba" / "wnba" / a soccer league / etc -- this
               module's long-standing "page key" (NOT always the picks_unified
               sport column; see db_sport below).
        games_with_bets: list of bet dicts from /api/best-bets endpoints
        view: NBA-only — 'q1' (Q1 markets) or 'full' (full-game ML/SPREAD/TOTAL).
              Ignored for MLB/NHL.
        db_sport: the picks_unified `sport` column for the edge_proven gate, when it
               differs from `sport` above (e.g. "basketball" for wnba/minor-league
               callers, "hockey"/"soccer" for their frameworks). Defaults to `sport`
               (correct for nhl/mlb/nba/tennis, whose page key already IS the db sport).
        league: the picks_unified `league` column for the edge_proven gate. Defaults to
               `sport` (correct for every page-key that's already a real league --
               nhl/mlb/nba/wnba/a soccer league); tennis passes its own `tour` ("atp"/
               "wta") since `sport` stays "tennis" for calibration/storage purposes.
               (Pick Authority league-segmentation, Austin 2026-10-22.)

    Returns:
        POTD dict or None if no qualifying picks
    """
    _db_sport = db_sport or sport
    _league = league or sport
    candidates = []
    for game in games_with_bets:
        # Skip games that have already started — POTD only ever locks
        # against pre-game opportunities. Without this gate, an early
        # afternoon slate could sneak a "POTD" onto a game that's
        # already tipped off, which the lock-on-live rule would
        # then freeze instantly.
        if _is_game_locked(game):
            continue
        # Projection-sanity gate (MintyBot 2026-10-08): drop a whole game whose model
        # projection is physically impossible for its sport (e.g. NHL 8.87 expected goals,
        # a 3-game early-season blowup). Such a game's edges are garbage; it must never
        # headline as POTD. Fail-open on missing/unknown projections -- see _projection_implausible.
        if _projection_implausible(sport, game):
            logger.warning("POTD: %s %s projection implausible (expected_score=%s) -- excluded from POTD candidacy",
                           sport, game.get("matchup", ""), game.get("expected_score"))
            continue
        # POTD per-game candidate is the bet's already-chosen headline —
        # `best_pick` for the card. The picker (engine.picks /
        # engine.nba_picks / engine.nhl_picks) sorts intra-game by
        # adjusted_ev (edge × reliability × CLV × line_move), and
        # `best_pick` is the top non-skip pick from that order. Using
        # the same source of truth here guarantees the POTD headline
        # always matches what the user sees on that game's card.
        candidate_picks: list[dict] = []
        if sport == "nba" and view == "full":
            full_best = game.get("best_pick_full")
            if full_best:
                candidate_picks.append(full_best)
            else:
                for fp in game.get("full_picks") or []:
                    if fp.get("type") in _NBA_FULL_PRIMARY:
                        candidate_picks.append(fp)
                        break
        elif sport == "nba" and view == "q1":
            q1_best = game.get("best_pick_q1") or game.get("best_pick")
            if q1_best:
                candidate_picks.append(q1_best)
        else:
            best = game.get("best_pick")
            if best:
                candidate_picks.append(best)

        candidate_picks = _filter_picks_for_view(candidate_picks, sport, view)

        for pick in candidate_picks:
            if sport in _POTD_CALIBRATED_SPORTS:
                # Unified calibration via calibration_gate: dispatches WNBA->final_calibration,
                # tennis->tennis_platt, NHL/NBA/MLB/soccer->empirical -- the SAME path the cards
                # and the picks_unified record use. (MintyBot 2026-10-15: collapsed three identical
                # inline branches into the shared gate; verified identical prob+edge per sport, so
                # one calibration fix -- e.g. a tennis refit -- now lands everywhere at once.)
                # Copy the dict; never mutate the shared best_pick.
                try:
                    from engine.calibration_gate import cal_prob as _cal_prob
                    _raw = pick.get("prob")
                    if _raw is not None:
                        _cp = float(_cal_prob(sport, pick.get("type", ""), _raw,
                                              pick.get("odds", 0), pick_text=pick.get("pick"),
                                              raw_edge=pick.get("edge")))
                        _imp = _implied_from_odds(pick.get("odds", 0))
                        pick = dict(pick)
                        pick["prob"] = round(_cp, 4)
                        pick["edge"] = round((_cp - _imp) * 100.0, 2)
                except Exception:
                    pass
            pick_type = pick.get("type", "")
            pick_name = pick.get("pick", "")
            # Pick Authority gate (Austin 2026-10-21 "Pick authority"): POTD only
            # headlines a market edge_proven has PROVEN -- the same authority the
            # cards use. Else POTD surfaced raw-edge picks the cards NO-PLAY (e.g.
            # NBA SPREAD conf 0.46) so POTD/card disagreed + POTD ran underwater.
            # Condition-segmented 2026-10-22: gated on THIS pick's own fav/dog/
            # over/under side, not the whole blended market.
            if not _ep_eligible(_db_sport, _league, pick_type, pick_name, pick.get("odds")):
                continue
            if pick.get("edge", 0) < MIN_EDGE:
                continue
            # POTD targets ~80% WR; skip picks below the conviction
            # floor regardless of edge. Without this, a 0.55-prob 12%-
            # edge longshot could outscore a 0.78-prob 5%-edge favorite
            # — the opposite of what "lock pick of the day" means.
            if pick.get("prob", 0) < MIN_PROB:
                continue
            if not pick.get("odds"):
                continue

            matchup = game.get("matchup", "")
            is_total_pick = any(x in pick_name for x in ("Over", "Under", "over", "under"))
            if not is_total_pick and pick_name and matchup and not any(
                abbr in pick_name for abbr in matchup.replace(" @ ", "|").split("|")
            ):
                logger.warning("POTD: pick '%s' not in matchup '%s', skipping", pick_name, matchup)
                continue

            candidates.append({
                "game_id": str(game.get("game_id", "")),
                "matchup": game.get("matchup", ""),
                "type": pick_type,
                "pick": pick.get("pick", ""),
                "prob": pick.get("prob", 0),
                "edge": pick.get("edge", 0),
                "odds": pick.get("odds", 0),
                "home": game.get("home", {}),
                "away": game.get("away", {}),
                "time": game.get("time", ""),
                "venue": game.get("venue", ""),
            })

    if not candidates:
        return None

    candidates.sort(key=lambda c: _score_pick(c, sport), reverse=True)
    best = candidates[0]

    # Resolve full team names for the pick display
    home_info = best.get("home", {})
    away_info = best.get("away", {})
    home_name = home_info.get("name", home_info.get("abbreviation", "Home"))
    away_name = away_info.get("name", away_info.get("abbreviation", "Away"))

    pick_str = best.get("pick", "")
    home_abbr = home_info.get("abbreviation", "")
    away_abbr = away_info.get("abbreviation", "")
    if home_abbr and pick_str.startswith(home_abbr):
        best["pick_full"] = pick_str.replace(home_abbr, home_name, 1)
    elif away_abbr and pick_str.startswith(away_abbr):
        best["pick_full"] = pick_str.replace(away_abbr, away_name, 1)
    else:
        best["pick_full"] = pick_str

    best["kelly_pct"] = round(_kelly_fraction(best["prob"], best["odds"]) * 100, 1)
    best["reasoning"] = _build_reasoning(best, sport)

    return best


def get_or_create_potd(sport: str, games_with_bets: list[dict],
                      date: str | None = None,
                      view: str = "q1",
                      tour: str | None = None,
                      db_sport: str | None = None,
                      league: str | None = None) -> dict | None:
    """Get today's POTD, creating it if it doesn't exist.

    Once created, POTD is locked for the day - subsequent calls return
    the same pick regardless of updated predictions.

    ``view`` only matters for NBA. 'q1' uses pick_of_day; 'full' uses
    pick_of_day_full. Both can coexist for the same date.

    ``tour`` only matters for tennis. 'atp' or 'wta' scopes both the
    existing-row read and the INSERT so each tour gets its own POTD.

    ``db_sport``/``league`` thread straight through to select_potd's edge_proven
    gate (see its docstring) -- storage/calibration below still key on ``sport``
    and ``tour`` unchanged. (Pick Authority league-segmentation, Austin 2026-10-22.)
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    target_date = date or et_today_str()
    table = _potd_table(sport, view)

    if sport == "tennis" and tour:
        existing = conn.execute(
            f"SELECT * FROM {table} WHERE date = ? AND tour = ?",
            (target_date, tour),
        ).fetchone()
    else:
        existing = conn.execute(
            f"SELECT * FROM {table} WHERE date = ?", (target_date,)
        ).fetchone()

    # POTD locks at 7am ET (Austin 2026-10-08): a midnight/first-fetch lock
    # catches late games that roll past midnight and locks before the day's
    # markets + odds are up. Before 7am ET the pick stays PROVISIONAL -- it is
    # re-selected each call as odds settle overnight (started/final games are
    # already excluded by _is_game_locked). At/after 7am ET the first pick is
    # frozen for the day.
    # Time-based lock (Austin 2026-10-14): a POTD freezes at its game's lock
    # time per engine.lock_schedule -- 7am ET day games, 4:30pm ET night games,
    # prior-9pm ET overnight (tennis). Generalises the old blanket-7am lock so
    # night slates lock on mature odds and overnight slates don't lock after
    # first ball. Provisional (re-selected as odds settle) until its game's lock
    # passes; then frozen. A game off the slate (started/final) is treated locked.
    from .._tz import et_now
    from ..lock_schedule import is_locked as _is_locked
    _now_et = et_now()

    def _potd_game_start(_gid):
        for _g in games_with_bets:
            if str(_g.get("game_id", "")) == str(_gid):
                _t = _g.get("time")
                if not _t:
                    return None
                try:
                    return datetime.fromisoformat(str(_t).replace("Z", "+00:00"))
                except Exception:
                    return None
        return None

    if existing:
        _gs = _potd_game_start(existing["game_id"])
        if _gs is None or _is_locked(_gs, _now_et):
            return dict(existing)

    selected = select_potd(sport, games_with_bets, view=view,
                           db_sport=db_sport, league=league)
    if not selected:
        return dict(existing) if existing else None
    if existing:
        # pre-lock re-selection: replace the provisional with the current best pick
        if sport == "tennis" and tour:
            conn.execute(f"DELETE FROM {table} WHERE date = ? AND tour = ?", (target_date, tour))
        else:
            conn.execute(f"DELETE FROM {table} WHERE date = ?", (target_date,))

    if sport == "tennis":
        conn.execute(f"""
            INSERT OR IGNORE INTO {table} (
                date, tour, game_id, matchup, bet_type, pick,
                model_prob, edge, odds, kelly_pct, reasoning
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            target_date, tour,
            selected.get("game_id"),
            selected.get("matchup"),
            selected.get("type"),
            selected.get("pick_full", selected.get("pick")),
            selected.get("prob"),
            selected.get("edge"),
            selected.get("odds"),
            selected.get("kelly_pct"),
            selected.get("reasoning"),
        ))
    else:
        conn.execute(f"""
            INSERT OR IGNORE INTO {table} (
                date, game_id, matchup, bet_type, pick,
                model_prob, edge, odds, kelly_pct, reasoning
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            target_date,
            selected.get("game_id"),
            selected.get("matchup"),
            selected.get("type"),
            selected.get("pick_full", selected.get("pick")),
            selected.get("prob"),
            selected.get("edge"),
            selected.get("odds"),
            selected.get("kelly_pct"),
            selected.get("reasoning"),
        ))

    # Race-safety: another concurrent caller may have won the INSERT
    # OR IGNORE. Re-read the canonical row and replace ``selected`` so
    # the mirror below uses whatever's actually persisted, not what
    # this thread happened to pick.
    if sport == "tennis" and tour:
        winner_row = conn.execute(
            f"SELECT * FROM {table} WHERE date = ? AND tour = ?",
            (target_date, tour),
        ).fetchone()
    else:
        winner_row = conn.execute(
            f"SELECT * FROM {table} WHERE date = ?", (target_date,),
        ).fetchone()
    if winner_row:
        winner = dict(winner_row)
        # Map DB column names back to the picker shape the mirror
        # logic below expects (type / pick_full / prob).
        selected = {
            "game_id": winner.get("game_id"),
            "matchup": winner.get("matchup"),
            "type": winner.get("bet_type"),
            "pick": winner.get("pick"),
            "pick_full": winner.get("pick"),
            "prob": winner.get("model_prob"),
            "edge": winner.get("edge"),
            "odds": winner.get("odds"),
            "kelly_pct": winner.get("kelly_pct"),
            "reasoning": winner.get("reasoning"),
        }
        bet_type = selected["type"]
        pick_text = selected["pick"]
        raw_game_id = selected["game_id"]
    # Mirror into the per-sport picks table so the pick tracker shows
    # the POTD alongside the day's other picks — but ONLY when the
    # tracker doesn't already represent this bet.
    # Per-sport picks table:
    #   mlb / basketball-framework / hockey-framework leagues → "picks"
    #   nhl / nba / tennis / wnba (basketball-framework but old name)
    #     → f"{sport}_picks"
    # Hockey framework (AHL / PWHL) and basketball framework leagues
    # share the unified "picks" name in their per-league DB.
    from ..hockey import HOCKEY_LEAGUE_REGISTRY as _HOCKEY
    from ..basketball import LEAGUE_REGISTRY as _BB
    try:
        from ..soccer import LEAGUE_REGISTRY as _SOC
    except Exception:
        _SOC = {}
    try:
        from ..football import LEAGUE_REGISTRY as _FB
    except Exception:
        _FB = {}
    if sport == "mlb" or sport in _HOCKEY and sport != "nhl" or (
        sport in _BB and sport != "nba"
    ) or sport in _SOC or sport in _FB:
        picks_table = "picks"
    else:
        picks_table = f"{sport}_picks"
    # Picks-table mirror MUST use the abbreviated `pick` ("ATL -2.5"),
    # not the friendly `pick_full` ("Atlanta Dream -2.5"). The basketball
    # framework's record_pick writes abbreviation format, so if we mirror
    # the friendly format here the dedup-by-pick-text mismatch creates a
    # second pending row for the same game. Friendly display lives in
    # the pick_of_day table itself (pick column there can stay friendly
    # for human-readable POTD card text).
    pick_text = selected.get("pick", "")
    bet_type = selected.get("type")
    raw_game_id = selected.get("game_id")
    # MLB ID translation: best-bets / POTD store ESPN event IDs
    # (9-digit), but the picks table joins to the games table on
    # mlb_game_id (the 6-digit MLB Stats game_pk). Without this
    # translation the settler can never match the mirrored POTD row.
    if sport == "mlb":
        try:
            from ..derivative_tracker import _resolve_mlb_game_pk
            resolved_pk = _resolve_mlb_game_pk(
                conn, str(raw_game_id), target_date,
                selected.get("matchup", ""),
            )
            game_id = resolved_pk if resolved_pk else raw_game_id
        except Exception as e:
            logger.debug("MLB POTD game_pk resolve failed: %s — falling back to raw id", e)
            game_id = raw_game_id
    else:
        game_id = raw_game_id
    # Commit pick_of_day INSERT FIRST so a mirror failure doesn't roll
    # it back. Earlier this batched into one commit and a silent mirror
    # exception caused the POTD row itself to vanish.
    #
    # Tradeoff: if the mirror INSERT later fails, the POTD row is
    # committed but the per-sport picks table doesn't know about it.
    # ``_ensure_potd_mirror`` retries the mirror lazily on every POTD
    # read path (settle_potd, get_today_potd) so orphans heal on the
    # next access instead of accumulating.
    conn.commit()
    _ensure_potd_mirror(sport, conn, picks_table,
                         target_date, game_id,
                         selected.get("matchup"),
                         bet_type, pick_text,
                         selected.get("prob"),
                         selected.get("edge"),
                         selected.get("odds"))

    logger.info("Created %s POTD for %s: %s %s (%s edge %+.1f%%)",
                sport.upper(), target_date, selected.get("matchup"),
                selected.get("pick"), selected.get("type"), selected.get("edge", 0))

    # Return the canonical DB row shape (bet_type, model_prob, ...)
    # so the API response matches what get_today_potd returns.
    from ._read import get_today_potd
    canonical = get_today_potd(sport, date=target_date, view=view, tour=tour)
    if canonical:
        return canonical
    # Fallback if the round-trip read failed for some reason.
    result = dict(selected)
    result.setdefault("bet_type", selected.get("type"))
    result.setdefault("model_prob", selected.get("prob"))
    result["date"] = target_date
    return result
