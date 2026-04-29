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
from ._scoring import _score_pick, _kelly_fraction, MIN_EDGE

logger = logging.getLogger(__name__)


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
    parts.append(f"Best {bt_label} play on today's {sport.upper()} slate.")
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


def select_potd(sport: str, games_with_bets: list[dict],
                view: str = "q1") -> dict | None:
    """
    Select the Pick of the Day from a list of games with bets.

    Args:
        sport: "mlb" / "nhl" / "nba"
        games_with_bets: list of bet dicts from /api/best-bets endpoints
        view: NBA-only — 'q1' (Q1 markets) or 'full' (full-game ML/SPREAD/TOTAL).
              Ignored for MLB/NHL.

    Returns:
        POTD dict or None if no qualifying picks
    """
    candidates = []
    for game in games_with_bets:
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
            pick_type = pick.get("type", "")
            if pick.get("edge", 0) < MIN_EDGE:
                continue
            if not pick.get("odds"):
                continue

            pick_name = pick.get("pick", "")
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
                      view: str = "q1") -> dict | None:
    """Get today's POTD, creating it if it doesn't exist.

    Once created, POTD is locked for the day - subsequent calls return
    the same pick regardless of updated predictions.

    ``view`` only matters for NBA. 'q1' uses pick_of_day; 'full' uses
    pick_of_day_full. Both can coexist for the same date.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    table = _potd_table(sport, view)

    existing = conn.execute(
        f"SELECT * FROM {table} WHERE date = ?", (target_date,)
    ).fetchone()

    if existing:
        return dict(existing)

    selected = select_potd(sport, games_with_bets, view=view)
    if not selected:
        return None

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
    # Mirror into the per-sport picks table so the pick tracker shows
    # the POTD alongside the day's other picks — but ONLY when the
    # tracker doesn't already represent this bet.
    picks_table = "picks" if sport == "mlb" else f"{sport}_picks"
    pick_text = selected.get("pick_full", selected.get("pick"))
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
    conn.commit()
    try:
        existing = conn.execute(
            f"SELECT id FROM {picks_table} "
            f"WHERE date=? AND game_id=? AND bet_type=? LIMIT 1",
            (target_date, game_id, bet_type),
        ).fetchone()
        if not existing:
            cur = conn.execute(
                f"INSERT INTO {picks_table} ("
                "  date, game_id, matchup, bet_type, pick,"
                "  model_prob, edge, odds"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    target_date, game_id,
                    selected.get("matchup"),
                    bet_type, pick_text,
                    selected.get("prob"),
                    selected.get("edge"),
                    selected.get("odds"),
                ),
            )
            conn.commit()
            logger.info("POTD mirror %s: inserted pick id=%s for %s/%s/%s",
                        sport, cur.lastrowid, target_date, bet_type, pick_text)
        else:
            logger.info("POTD mirror %s: existing pick %s already covers "
                        "(%s, %s) — skipped", sport, existing["id"],
                        target_date, bet_type)
    except Exception as e:
        logger.warning("POTD mirror to %s FAILED for %s/%s/%s: %s",
                       picks_table, sport, target_date, bet_type, e,
                       exc_info=True)

    logger.info("Created %s POTD for %s: %s %s (%s edge %+.1f%%)",
                sport.upper(), target_date, selected.get("matchup"),
                selected.get("pick"), selected.get("type"), selected.get("edge", 0))

    # Return the canonical DB row shape (bet_type, model_prob, ...)
    # so the API response matches what get_today_potd returns.
    from ._read import get_today_potd
    canonical = get_today_potd(sport, date=target_date, view=view)
    if canonical:
        return canonical
    # Fallback if the round-trip read failed for some reason.
    result = dict(selected)
    result.setdefault("bet_type", selected.get("type"))
    result.setdefault("model_prob", selected.get("prob"))
    result["date"] = target_date
    return result
