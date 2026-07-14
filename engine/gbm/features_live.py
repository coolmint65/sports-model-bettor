"""
Live GBM feature extraction (Phase 5e).

Builds (state-at-time-T, final-outcome) training pairs from
``historical_pbp``. The model learns: given pre-match team profiles
PLUS the score / pace / shooting state at time T, what's the final
score / margin / win prob?

Why this is its own module
--------------------------
The team-game GBMs in ``features_nhl.py`` / ``features_nba.py``
expect a single feature row per game (prematch only). The live GBM
emits MANY rows per game (one per intermission moment + optionally
per quarter break). Same target, different feature set, different
training shape.

Sample points
-------------
For each completed game we emit feature rows at:
  - End of Q1 (NBA) / End of P1 (NHL)
  - End of Q2 / Halftime
  - End of Q3 / End of P2
  - (Q4 / P3 ends are the game itself — no future to predict)

Each row carries:
  - Prematch features (team season stats, surface for NHL)
  - Game-state features at time T (score, pace, shot stats so far)
  - The (sport, game_id, period_at_state) tuple as the natural key

Targets:
  - final_total_points (regression)
  - final_margin       (regression, home - away)
  - home_final_win     (classification)

Implementation notes
--------------------
PBP plays are processed once per game; we accumulate running totals
in a single pass and emit a row whenever we see a "Period End" play
(except the final one).
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)


_DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "live.db"


# League-average defaults so cold-start rows don't NaN out.
_LIVE_DEFAULTS_NBA = {
    # Game state at intermission
    "period_ended": 1,
    "elapsed_min": 12.0,
    "remaining_min": 36.0,
    "home_score_so_far": 0,
    "away_score_so_far": 0,
    "score_total_so_far": 0,
    "score_margin_so_far": 0,
    "made_fg_home_so_far": 0,
    "missed_fg_home_so_far": 0,
    "made_fg_away_so_far": 0,
    "missed_fg_away_so_far": 0,
    "fouls_home_so_far": 0,
    "fouls_away_so_far": 0,
    "turnovers_home_so_far": 0,
    "turnovers_away_so_far": 0,
    # Pace + efficiency derivations
    "pace_per_min": 4.0,
    "home_efg_so_far": 0.500,
    "away_efg_so_far": 0.500,
    "home_score_per_min": 1.0,
    "away_score_per_min": 1.0,
    # Prematch (filled from features_nba when available)
    "home_prematch_ppg": 113.0,
    "away_prematch_ppg": 113.0,
    "home_prematch_def": 113.0,
    "away_prematch_def": 113.0,
}

_LIVE_DEFAULTS_NHL = {
    "period_ended": 1,
    "elapsed_min": 20.0,
    "remaining_min": 40.0,
    "home_score_so_far": 0,
    "away_score_so_far": 0,
    "score_total_so_far": 0,
    "score_margin_so_far": 0,
    "shots_home_so_far": 0,
    "shots_away_so_far": 0,
    "shot_attempts_home_so_far": 0,
    "shot_attempts_away_so_far": 0,
    "penalties_home_so_far": 0,
    "penalties_away_so_far": 0,
    "home_save_pct_so_far": 0.905,
    "away_save_pct_so_far": 0.905,
    "home_score_per_min": 0.05,
    "away_score_per_min": 0.05,
    "home_prematch_gf_pg": 3.10,
    "away_prematch_gf_pg": 3.10,
    "home_prematch_ga_pg": 3.10,
    "away_prematch_ga_pg": 3.10,
}


def _live_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ── NBA per-game state walk ───────────────────────────────────

_NBA_FOUL_TYPES = {
    "shooting foul", "personal foul", "offensive foul",
    "loose ball foul", "technical foul",
}
_NBA_TURNOVER_TYPES = {
    "bad pass turnover", "lost ball turnover",
    "out of bounds - bad pass turnover", "offensive foul turnover",
    "traveling", "double dribble",
}
# NBA ESPN uses "period end"/"end period"; NHL Stats API uses
# "period-end" (hyphenated). Match all variants.
_PERIOD_END_TYPES = {"period end", "end period", "end of period",
                      "period-end"}


def _walk_nba_game(conn, game_id: str,
                    home_team_id: str | None,
                    sport: str = "nba") -> Iterable[tuple[int, dict]]:
    """Walk one basketball game's PBP, yielding (period_just_ended,
    state_dict) at every period-end play except the final-period buzzer.

    ``sport`` keys into ``historical_pbp.sport`` — WNBA reuses the same
    NBA-shaped feature extractor since ESPN ships an identical play
    schema (period 1-4, same type_text values, same scoring_play flag).
    state_dict carries running totals up to and including the
    period-end play. Caller layers on prematch features + targets.
    """
    plays = conn.execute(
        "SELECT period, sequence, team_id, type_text, "
        "       home_score, away_score, scoring_play, "
        "       shooting_play, score_value "
        "FROM historical_pbp "
        "WHERE sport = ? AND game_id = ? "
        "ORDER BY period, sequence",
        (sport, str(game_id),),
    ).fetchall()
    if not plays:
        return

    home_id = str(home_team_id or "")
    state = {
        "made_fg_home": 0, "missed_fg_home": 0,
        "made_fg_away": 0, "missed_fg_away": 0,
        "fouls_home": 0, "fouls_away": 0,
        "turnovers_home": 0, "turnovers_away": 0,
    }
    max_period = max(int(p["period"] or 0) for p in plays)

    for p in plays:
        type_text = (p["type_text"] or "").strip().lower()
        team_id = str(p["team_id"] or "")
        is_home = (team_id == home_id) if home_id and team_id else None

        if type_text in _NBA_FOUL_TYPES and is_home is not None:
            if is_home:
                state["fouls_home"] += 1
            else:
                state["fouls_away"] += 1
        elif type_text in _NBA_TURNOVER_TYPES and is_home is not None:
            if is_home:
                state["turnovers_home"] += 1
            else:
                state["turnovers_away"] += 1
        elif p["shooting_play"] and is_home is not None:
            if p["scoring_play"]:
                if is_home:
                    state["made_fg_home"] += 1
                else:
                    state["made_fg_away"] += 1
            else:
                if is_home:
                    state["missed_fg_home"] += 1
                else:
                    state["missed_fg_away"] += 1

        if type_text in _PERIOD_END_TYPES:
            period = int(p["period"] or 0)
            if 0 < period < max_period:
                snap = dict(state)
                snap["home_score_so_far"] = int(p["home_score"] or 0)
                snap["away_score_so_far"] = int(p["away_score"] or 0)
                snap["period_ended"] = period
                yield period, snap


def _nba_state_to_features(state: dict, prematch: dict | None) -> dict:
    """Convert one period-end state snapshot to a feature dict.

    Sport-aware timing — the state dict's ``sport`` key (when present)
    chooses the right quarter length: NBA 12-min, WNBA 10-min, NCAAM
    20-min halves, AFL 20-min quarters. State carries its own
    ``elapsed_min`` overrides this default when present, which the
    AFL training builder already does. Without the sport key the
    function falls back to NBA-shaped 12-min quarters (unchanged
    behaviour for callers that don't pass sport)."""
    period = state.get("period_ended", 0)
    sport = (state.get("sport") or "nba").lower()
    if sport in ("ncaam",):
        period_min = 20.0
        game_min = 40.0
    elif sport == "wnba":
        period_min = 10.0
        game_min = 40.0
    elif sport == "afl":
        period_min = 20.0
        game_min = 80.0
    else:
        period_min = 12.0
        game_min = 48.0
    # State-provided elapsed_min wins (training builders compute it
    # exactly); otherwise derive from period × period_min.
    elapsed = float(state.get("elapsed_min") or (period * period_min))
    remaining = max(0.0, game_min - elapsed)
    total = state["home_score_so_far"] + state["away_score_so_far"]
    margin = state["home_score_so_far"] - state["away_score_so_far"]

    out = dict(_LIVE_DEFAULTS_NBA)
    out["period_ended"] = period
    out["elapsed_min"] = elapsed
    out["remaining_min"] = remaining
    out["home_score_so_far"] = state["home_score_so_far"]
    out["away_score_so_far"] = state["away_score_so_far"]
    out["score_total_so_far"] = total
    out["score_margin_so_far"] = margin
    out["made_fg_home_so_far"]   = state["made_fg_home"]
    out["missed_fg_home_so_far"] = state["missed_fg_home"]
    out["made_fg_away_so_far"]   = state["made_fg_away"]
    out["missed_fg_away_so_far"] = state["missed_fg_away"]
    out["fouls_home_so_far"]     = state["fouls_home"]
    out["fouls_away_so_far"]     = state["fouls_away"]
    out["turnovers_home_so_far"]   = state["turnovers_home"]
    out["turnovers_away_so_far"]   = state["turnovers_away"]
    if elapsed > 0:
        out["pace_per_min"] = total / elapsed
        out["home_score_per_min"] = state["home_score_so_far"] / elapsed
        out["away_score_per_min"] = state["away_score_so_far"] / elapsed
    home_attempts = state["made_fg_home"] + state["missed_fg_home"]
    away_attempts = state["made_fg_away"] + state["missed_fg_away"]
    if home_attempts > 0:
        out["home_efg_so_far"] = state["made_fg_home"] / home_attempts
    if away_attempts > 0:
        out["away_efg_so_far"] = state["made_fg_away"] / away_attempts
    if prematch:
        out["home_prematch_ppg"] = float(prematch.get("home_ppg") or 113.0)
        out["away_prematch_ppg"] = float(prematch.get("away_ppg") or 113.0)
        out["home_prematch_def"] = float(prematch.get("home_opp_ppg") or 113.0)
        out["away_prematch_def"] = float(prematch.get("away_opp_ppg") or 113.0)
    return out


# ── NHL per-game state walk ───────────────────────────────────

_NHL_SHOT_TYPES = {"shot-on-goal", "goal", "shot"}
_NHL_MISSED_TYPES = {"missed-shot", "missed"}
_NHL_BLOCKED_TYPES = {"blocked-shot", "blocked"}
_NHL_PENALTY_KEYS = {
    "tripping", "interference", "holding", "boarding", "hooking",
    "slashing", "high-sticking", "roughing", "cross checking",
    "elbowing", "delaying game", "delay of game", "too many men",
    "unsportsmanlike", "fighting", "instigator", "misconduct",
    "penalty",
}


def _walk_nhl_game(conn, game_id: str,
                    home_team_id: str | None) -> Iterable[tuple[int, dict]]:
    """Same shape as _walk_nba_game but for NHL plays."""
    plays = conn.execute(
        "SELECT period, sequence, team_id, type_text, "
        "       home_score, away_score, scoring_play "
        "FROM historical_pbp "
        "WHERE sport = 'nhl' AND game_id = ? "
        "ORDER BY period, sequence",
        (str(game_id),),
    ).fetchall()
    if not plays:
        return
    home_id = str(home_team_id or "")
    state = {
        "shots_home": 0, "shots_away": 0,
        "shot_attempts_home": 0, "shot_attempts_away": 0,
        "penalties_home": 0, "penalties_away": 0,
    }
    max_period = max(int(p["period"] or 0) for p in plays)
    for p in plays:
        type_text = (p["type_text"] or "").strip().lower()
        team_id = str(p["team_id"] or "")
        is_home = (team_id == home_id) if home_id and team_id else None
        if type_text in _NHL_SHOT_TYPES and is_home is not None:
            if is_home:
                state["shots_home"] += 1
                state["shot_attempts_home"] += 1
            else:
                state["shots_away"] += 1
                state["shot_attempts_away"] += 1
        elif (type_text in _NHL_MISSED_TYPES
              or type_text in _NHL_BLOCKED_TYPES) and is_home is not None:
            if is_home:
                state["shot_attempts_home"] += 1
            else:
                state["shot_attempts_away"] += 1
        elif type_text in _NHL_PENALTY_KEYS and is_home is not None:
            if is_home:
                state["penalties_home"] += 1
            else:
                state["penalties_away"] += 1
        if type_text in _PERIOD_END_TYPES:
            period = int(p["period"] or 0)
            if 0 < period < max_period:
                snap = dict(state)
                snap["home_score_so_far"] = int(p["home_score"] or 0)
                snap["away_score_so_far"] = int(p["away_score"] or 0)
                snap["period_ended"] = period
                yield period, snap


def _nhl_state_to_features(state: dict, prematch: dict | None) -> dict:
    period = state.get("period_ended", 0)
    elapsed = period * 20.0
    remaining = 60.0 - elapsed
    out = dict(_LIVE_DEFAULTS_NHL)
    out["period_ended"] = period
    out["elapsed_min"] = elapsed
    out["remaining_min"] = remaining
    out["home_score_so_far"] = state["home_score_so_far"]
    out["away_score_so_far"] = state["away_score_so_far"]
    out["score_total_so_far"] = state["home_score_so_far"] + state["away_score_so_far"]
    out["score_margin_so_far"] = state["home_score_so_far"] - state["away_score_so_far"]
    out["shots_home_so_far"]    = state["shots_home"]
    out["shots_away_so_far"]    = state["shots_away"]
    out["shot_attempts_home_so_far"] = state["shot_attempts_home"]
    out["shot_attempts_away_so_far"] = state["shot_attempts_away"]
    out["penalties_home_so_far"]= state["penalties_home"]
    out["penalties_away_so_far"]= state["penalties_away"]
    if elapsed > 0:
        out["home_score_per_min"] = state["home_score_so_far"] / elapsed
        out["away_score_per_min"] = state["away_score_so_far"] / elapsed
    if state["shots_away"] > 0:
        out["home_save_pct_so_far"] = (
            1.0 - state["away_score_so_far"] / state["shots_away"])
    if state["shots_home"] > 0:
        out["away_save_pct_so_far"] = (
            1.0 - state["home_score_so_far"] / state["shots_home"])
    if prematch:
        out["home_prematch_gf_pg"] = float(prematch.get("home_gf_pg") or 3.10)
        out["away_prematch_gf_pg"] = float(prematch.get("away_gf_pg") or 3.10)
        out["home_prematch_ga_pg"] = float(prematch.get("home_ga_pg") or 3.10)
        out["away_prematch_ga_pg"] = float(prematch.get("away_ga_pg") or 3.10)
    return out


# ── Public API ────────────────────────────────────────────────

NBA_LIVE_FEATURE_NAMES = list(_LIVE_DEFAULTS_NBA.keys())
NHL_LIVE_FEATURE_NAMES = list(_LIVE_DEFAULTS_NHL.keys())


def build_live_dataset_nba(limit: int | None = None) -> tuple:
    """Walk NBA historical games, emit (features, targets) DataFrames
    suitable for engine.gbm.train training shape."""
    import pandas as pd
    from engine.nba_db import get_conn as nba_conn
    nbac = nba_conn()
    games = nbac.execute(
        "SELECT g.game_id, g.home_team_id, g.away_team_id, "
        "       g.home_score, g.away_score, "
        "       g.home_q1, g.away_q1, g.home_q2, g.away_q2, "
        "       g.home_q3, g.away_q3, g.home_q4, g.away_q4, g.season "
        "FROM nba_games g WHERE g.status='final' "
        "  AND g.home_score IS NOT NULL "
        "ORDER BY g.date ASC"
    ).fetchall()
    if limit:
        games = games[: int(limit)]

    pbp_conn = _live_conn()
    feats: list[dict] = []
    targets: list[dict] = []
    for g in games:
        d = dict(g)
        # Final-outcome targets
        ftotal = (d["home_score"] or 0) + (d["away_score"] or 0)
        fmargin = (d["home_score"] or 0) - (d["away_score"] or 0)
        home_win = 1 if fmargin > 0 else 0

        for period, state in _walk_nba_game(pbp_conn, d["game_id"],
                                              str(d["home_team_id"])):
            f = _nba_state_to_features(state, prematch=None)
            f["_date"] = ""  # placeholder for chronological sort
            feats.append(f)
            targets.append({
                "final_total_points": ftotal,
                "final_margin":       fmargin,
                "home_final_win":     home_win,
            })
    if not feats:
        return None, None
    return pd.DataFrame(feats), pd.DataFrame(targets)


def build_live_dataset_wnba(limit: int | None = None) -> tuple:
    """Same shape as build_live_dataset_nba — WNBA games + PBP land in
    the basketball framework's per-league DB rather than nba_db, and
    historical_pbp keys plays under sport='wnba'."""
    import pandas as pd
    from engine.basketball._db import get_conn as bb_conn
    bbc = bb_conn("wnba")
    games = bbc.execute(
        "SELECT g.game_id, g.home_team_id, g.away_team_id, "
        "       g.home_score, g.away_score, "
        "       g.home_q1, g.away_q1, g.home_q2, g.away_q2, "
        "       g.home_q3, g.away_q3, g.home_q4, g.away_q4, g.season "
        "FROM games g WHERE g.status='final' "
        "  AND g.home_score IS NOT NULL "
        "ORDER BY g.date ASC"
    ).fetchall()
    if limit:
        games = games[: int(limit)]

    pbp_conn = _live_conn()
    feats: list[dict] = []
    targets: list[dict] = []
    for g in games:
        d = dict(g)
        ftotal = (d["home_score"] or 0) + (d["away_score"] or 0)
        fmargin = (d["home_score"] or 0) - (d["away_score"] or 0)
        home_win = 1 if fmargin > 0 else 0

        for period, state in _walk_nba_game(pbp_conn, d["game_id"],
                                              str(d["home_team_id"]),
                                              sport="wnba"):
            f = _nba_state_to_features(state, prematch=None)
            f["_date"] = ""
            feats.append(f)
            targets.append({
                "final_total_points": ftotal,
                "final_margin":       fmargin,
                "home_final_win":     home_win,
            })
    if not feats:
        return None, None
    return pd.DataFrame(feats), pd.DataFrame(targets)


def build_live_dataset_ncaam(limit: int | None = None) -> tuple:
    """NCAAM has 2 × 20-min halves (one intermission per game).
    _walk_nba_game still produces the right state at end-of-period 1 —
    same 'period end' play type, same scoring schema."""
    import pandas as pd
    from engine.basketball._db import get_conn as bb_conn
    bbc = bb_conn("ncaam")
    games = bbc.execute(
        "SELECT g.game_id, g.home_team_id, g.away_team_id, "
        "       g.home_score, g.away_score, "
        "       g.home_q1, g.away_q1, g.home_q2, g.away_q2, "
        "       g.home_q3, g.away_q3, g.home_q4, g.away_q4, g.season "
        "FROM games g WHERE g.status='final' "
        "  AND g.home_score IS NOT NULL "
        "ORDER BY g.date ASC"
    ).fetchall()
    if limit:
        games = games[: int(limit)]

    pbp_conn = _live_conn()
    feats: list[dict] = []
    targets: list[dict] = []
    for g in games:
        d = dict(g)
        ftotal = (d["home_score"] or 0) + (d["away_score"] or 0)
        fmargin = (d["home_score"] or 0) - (d["away_score"] or 0)
        home_win = 1 if fmargin > 0 else 0
        for period, state in _walk_nba_game(pbp_conn, d["game_id"],
                                              str(d["home_team_id"]),
                                              sport="ncaam"):
            f = _nba_state_to_features(state, prematch=None)
            f["_date"] = ""
            feats.append(f)
            targets.append({
                "final_total_points": ftotal,
                "final_margin":       fmargin,
                "home_final_win":     home_win,
            })
    if not feats:
        return None, None
    return pd.DataFrame(feats), pd.DataFrame(targets)


def build_live_dataset_afl(limit: int | None = None) -> tuple:
    """AFL — 4 × 20-min quarters in the basketball framework DB.

    AFL PBP only has 3 play types (Goal/Behind/Rushed) and no
    "period end" marker, so the basketball walker (which keys on
    period-end plays) yields nothing. Instead we build state at each
    quarter boundary directly from the games table's q1-q4 columns.
    Shooting-play counts approximated as goals (made) + behinds
    (missed) per side, which preserves the same NBA feature shape so
    the predict_live path stays uniform across sports."""
    import pandas as pd
    from engine.basketball._db import get_conn as bb_conn
    bbc = bb_conn("afl")
    games = bbc.execute(
        "SELECT g.game_id, g.home_team_id, g.away_team_id, "
        "       g.home_score, g.away_score, "
        "       g.home_q1, g.away_q1, g.home_q2, g.away_q2, "
        "       g.home_q3, g.away_q3, g.home_q4, g.away_q4, g.season "
        "FROM games g WHERE g.status='final' "
        "  AND g.home_score IS NOT NULL AND g.home_q1 IS NOT NULL "
        "ORDER BY g.date ASC"
    ).fetchall()
    if limit:
        games = games[: int(limit)]

    pbp_conn = _live_conn()
    feats: list[dict] = []
    targets: list[dict] = []
    for g in games:
        d = dict(g)
        ftotal = (d["home_score"] or 0) + (d["away_score"] or 0)
        fmargin = (d["home_score"] or 0) - (d["away_score"] or 0)
        home_win = 1 if fmargin > 0 else 0
        gid = str(d["game_id"])
        # PBP-derived per-side goal + behind counts (proxies for
        # made/missed-FG in the shared feature shape). Bucketed by
        # period so feature at end-of-Q2 reflects the cumulative
        # tallies through Q2.
        plays = pbp_conn.execute(
            "SELECT period, type_text, team_id FROM historical_pbp "
            "WHERE sport='afl' AND game_id=? ORDER BY period, sequence",
            (gid,),
        ).fetchall()
        h_team = str(d["home_team_id"])
        cum_goals = {"home": 0, "away": 0}
        cum_behinds = {"home": 0, "away": 0}
        per_period_cum = {p: dict(cum_goals=dict(cum_goals),
                                    cum_behinds=dict(cum_behinds))
                          for p in range(1, 5)}
        cur_p = 1
        for pl in plays:
            p = int(pl["period"] or 0)
            if p < 1 or p > 4:
                continue
            t_type = (pl["type_text"] or "").lower()
            side = "home" if str(pl["team_id"] or "") == h_team else "away"
            if t_type == "goal":
                cum_goals[side] += 1
            elif t_type in ("behind", "rushed"):
                cum_behinds[side] += 1
            # Snapshot when we cross a period boundary
            if p != cur_p:
                per_period_cum[cur_p] = {
                    "cum_goals": dict(cum_goals),
                    "cum_behinds": dict(cum_behinds),
                }
                cur_p = p
        # Final period snapshot
        per_period_cum[cur_p] = {
            "cum_goals": dict(cum_goals),
            "cum_behinds": dict(cum_behinds),
        }
        # Build a state dict at each Q-end (Q1, Q2, Q3) — Q4 is the
        # final so we don't emit a sample for it.
        cum_h = 0
        cum_a = 0
        for q_idx in (1, 2, 3):
            cum_h += int(d[f"home_q{q_idx}"] or 0)
            cum_a += int(d[f"away_q{q_idx}"] or 0)
            snap = per_period_cum.get(q_idx, {})
            cg = snap.get("cum_goals", {"home": 0, "away": 0})
            cb = snap.get("cum_behinds", {"home": 0, "away": 0})
            state = {
                "made_fg_home":     cg["home"],
                "missed_fg_home":   cb["home"],
                "made_fg_away":     cg["away"],
                "missed_fg_away":   cb["away"],
                "fouls_home": 0, "fouls_away": 0,
                "turnovers_home": 0, "turnovers_away": 0,
                "home_score_so_far": cum_h,
                "away_score_so_far": cum_a,
                "period_ended": q_idx,
                "elapsed_min": q_idx * 20.0,
            }
            f = _nba_state_to_features(state, prematch=None)
            f["_date"] = ""
            feats.append(f)
            targets.append({
                "final_total_points": ftotal,
                "final_margin":       fmargin,
                "home_final_win":     home_win,
            })
    if not feats:
        return None, None
    return pd.DataFrame(feats), pd.DataFrame(targets)


def build_live_dataset_nhl(limit: int | None = None) -> tuple:
    import pandas as pd
    from engine.nhl_db import get_conn as nhl_conn
    nhlc = nhl_conn()
    games = nhlc.execute(
        "SELECT g.game_id, g.home_team_id, g.away_team_id, "
        "       g.home_score, g.away_score, "
        "       g.home_p1, g.away_p1, g.home_p2, g.away_p2, "
        "       g.home_p3, g.away_p3, g.season "
        "FROM nhl_games g WHERE g.status='final' "
        "  AND g.home_score IS NOT NULL "
        "ORDER BY g.date ASC"
    ).fetchall()
    if limit:
        games = games[: int(limit)]
    pbp_conn = _live_conn()
    feats: list[dict] = []
    targets: list[dict] = []
    for g in games:
        d = dict(g)
        ftotal = (d["home_score"] or 0) + (d["away_score"] or 0)
        fmargin = (d["home_score"] or 0) - (d["away_score"] or 0)
        home_win = 1 if fmargin > 0 else 0
        for period, state in _walk_nhl_game(pbp_conn, d["game_id"],
                                              str(d["home_team_id"])):
            f = _nhl_state_to_features(state, prematch=None)
            f["_date"] = ""
            feats.append(f)
            targets.append({
                "final_total_points": ftotal,
                "final_margin":       fmargin,
                "home_final_win":     home_win,
            })
    if not feats:
        return None, None
    return pd.DataFrame(feats), pd.DataFrame(targets)


__all__ = [
    "NBA_LIVE_FEATURE_NAMES", "NHL_LIVE_FEATURE_NAMES",
    "build_live_dataset_nba", "build_live_dataset_nhl",
    "build_live_dataset_wnba", "build_live_dataset_ncaam",
    "build_live_dataset_afl",
]
