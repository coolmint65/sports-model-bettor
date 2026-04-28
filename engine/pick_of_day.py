"""
Pick of the Day - single highest-conviction play per sport per day.

Unlike the regular pick tracker (which can record many picks), POTD is
the ONE pick the model is most confident in. It's locked the first time
it's generated for a given date and never changes - so we can measure
whether the model's top-conviction plays are actually its best bets.

Selection criteria:
1. Must be a historically profitable bet type:
   - MLB: RL or ML (1st INN and O/U excluded based on backtest)
   - NHL: O/U or PL (ML excluded based on backtest)
2. Must have real DK odds (not derived)
3. Must have meaningful edge (>= 5%)
4. Among qualifying picks: highest edge-adjusted expected value

Storage: dedicated table in each sport's DB so POTD history is separate
from regular picks and can be summarized independently.

Usage:
    from engine.pick_of_day import get_or_create_potd, settle_potd, get_potd_summary
    potd = get_or_create_potd("mlb", games_with_bets)  # Returns today's POTD
    summary = get_potd_summary("mlb")  # Returns running W/L/profit
"""

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# Deprecated 2026-04-24 — POTD now considers every pick from
# `all_picks`, ranked by `_score_pick`. The score formula already
# incorporates per-market reliability + tracker win rate, so derivative
# bet types fairly compete with proven ML/RL/PL/O/U markets without
# the previous hardcoded whitelist excluding them. Cold-start markets
# with no tracker history score at neutral 1.0x reliability — they win
# only when their raw edge × CI confidence beats the proven markets
# in adjusted terms. Kept here as documentation of the historical
# whitelist so the intent isn't lost.
MLB_ALLOWED_TYPES_LEGACY = {"RL", "ML"}
NHL_ALLOWED_TYPES_LEGACY = {"O/U", "PL"}
NBA_ALLOWED_TYPES_LEGACY = {"Q1_SPREAD", "Q1_TOTAL", "Q1_ML"}

MIN_EDGE = 5.0  # Minimum edge % to qualify as POTD


def _get_conn(sport: str):
    """Get DB connection for the given sport."""
    if sport == "mlb":
        from .db import get_conn
        return get_conn()
    elif sport == "nhl":
        from .nhl_db import get_conn
        return get_conn()
    elif sport == "nba":
        from .nba_db import get_conn
        return get_conn()
    else:
        raise ValueError(f"Unknown sport: {sport}")


def _ensure_potd_table(sport: str) -> None:
    """Create the POTD table if it doesn't exist."""
    conn = _get_conn(sport)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pick_of_day (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL UNIQUE,
            game_id     TEXT,
            matchup     TEXT NOT NULL,
            bet_type    TEXT NOT NULL,
            pick        TEXT NOT NULL,
            model_prob  REAL,
            edge        REAL,
            odds        INTEGER,
            kelly_pct   REAL,
            reasoning   TEXT,
            result      TEXT,
            profit      REAL,
            created_at  TEXT DEFAULT (datetime('now')),
            settled_at  TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_potd_date ON pick_of_day(date)")
    # Migration: add closing_odds + closing_odds_updated_at columns to
    # existing tables. Captured on each sync run while the POTD is still
    # pending so the latest pre-settle value is effectively the closing
    # line. CLV is computed lazily from (odds, closing_odds) -- no need
    # to persist it.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(pick_of_day)").fetchall()}
    if "closing_odds" not in cols:
        conn.execute("ALTER TABLE pick_of_day ADD COLUMN closing_odds INTEGER")
    if "closing_odds_updated_at" not in cols:
        conn.execute("ALTER TABLE pick_of_day ADD COLUMN closing_odds_updated_at TEXT")
    conn.commit()
    # Phase 2k (NBA only): full-game POTDs land in a sibling table so
    # they coexist with the existing Q1 POTD on the same date. MLB/NHL
    # don't use this — single view only.
    if sport == "nba":
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pick_of_day_full (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT NOT NULL UNIQUE,
                game_id     TEXT,
                matchup     TEXT NOT NULL,
                bet_type    TEXT NOT NULL,
                pick        TEXT NOT NULL,
                model_prob  REAL,
                edge        REAL,
                odds        INTEGER,
                kelly_pct   REAL,
                reasoning   TEXT,
                result      TEXT,
                profit      REAL,
                closing_odds INTEGER,
                closing_odds_updated_at TEXT,
                created_at  TEXT DEFAULT (datetime('now')),
                settled_at  TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_potd_full_date ON pick_of_day_full(date)")
        conn.commit()


def _kelly_fraction(prob: float, odds: int) -> float:
    """Quarter-Kelly fraction for bet sizing."""
    if not odds or prob is None or prob <= 0 or prob >= 1:
        return 0.0
    decimal = (odds / 100) + 1 if odds > 0 else (100 / abs(odds)) + 1
    b = decimal - 1
    if b <= 0:
        return 0.0
    q = 1 - prob
    kelly = (b * prob - q) / b
    if kelly <= 0:
        return 0.0
    return max(0.0, min(0.25, kelly * 0.25))


def _get_market_win_rate(sport: str, bet_type: str, days: int = 30) -> float:
    """Get recent win rate for a bet type from the tracker DB.

    Returns win rate (0.0-1.0) or 0.5 if insufficient data.
    """
    try:
        conn = _get_conn(sport)
        table = {"mlb": "picks", "nhl": "nhl_picks", "nba": "nba_picks"}.get(sport, "picks")
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        row = conn.execute(
            f"SELECT COUNT(*) as total, "
            f"SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins "
            f"FROM {table} WHERE bet_type = ? AND date >= ? AND result IS NOT NULL",
            (bet_type, cutoff),
        ).fetchone()
        total = row["total"] if row else 0
        wins = row["wins"] if row else 0
        if total >= 5:
            return wins / total
    except Exception:
        pass
    return 0.5  # neutral when no data


_PRIMARY_MARKET_TYPES = {
    "mlb": {"ML", "RL", "O/U", "F5 ML", "F5 O/U", "F5 RL"},
    "nhl": {"ML", "PL", "O/U"},
    "nba": {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"},
}
_ALT_MARKET_TYPES = {
    "mlb": {"ALT RL", "ALT O/U", "ALT ML"},
    "nhl": {"ALT PL", "ALT O/U"},
    "nba": set(),
}


def _market_class_factor(sport: str, bet_type: str) -> float:
    """Primary > alt > derivative weighting. The discount for alts and
    derivatives is intentionally aggressive — a POTD on an alt-line
    longshot doesn't read as 'THE bet of the day', regardless of edge.
    Tuned so a primary market clearing 8–10% edge with prob>=0.55
    reliably outranks a 15%+ alt edge."""
    primary = _PRIMARY_MARKET_TYPES.get(sport, set())
    alt = _ALT_MARKET_TYPES.get(sport, set())
    if bet_type in primary:
        return 1.00
    if bet_type in alt:
        return 0.65
    return 0.55  # derivatives (Period Total, Q1 Team Total, BTS, etc.)


# Break-even hit rate at -110 (the standard juice). Used as the
# baseline that a market's recent win rate is divided against to get
# a reliability multiplier.
_BREAK_EVEN_HIT_RATE = 0.5238


def _score_pick(pick: dict, sport: str = "mlb") -> float:
    """Conviction-weighted POTD score. Replaces raw-edge selection.

    The user's POTD philosophy: "THE bet, no matter what." Raw edge is
    too brittle (any noisy +12% alt-line longshot wins) and Kelly is
    explicitly rejected (user finds it counterintuitive and over-rewards
    longshots — see feedback_no_kelly.md). The right answer is a single
    score that combines:

      score = capped_edge
              × confidence_factor   (model prob clamped, used directly)
              × reliability_factor  (markets we historically hit better get a nudge)
              × market_class_factor (primary > alt > derivative)

    Why each piece:
      - capped_edge: edge is clipped at 12 because anything above is
        usually a model-calibration artifact (alt-line longshots
        attracting +250 mispricings). A 20.8% edge MIA ML and a 12%
        edge MIA ML count the same here — the cap stops the headline
        landing on a coinflip outcome with a calibration spike.
      - confidence_factor: the model's own probability, bounded [0.45,
        0.85]. A 0.55 prob bet scores ~12% better than a 0.50 prob bet
        at the same edge. Bigger swings would dominate edge entirely.
      - reliability_factor: tracker win rate / break-even, clamped to
        [0.85, 1.15]. Cold-start markets stay neutral.
      - market_class_factor: primary 1.00, alt 0.65, derivative 0.55.
        Stops the POTD landing on alt lines unless overwhelming.
    """
    edge = float(pick.get("edge", 0) or 0)
    if edge <= 0:
        return 0.0

    # Cap raw edge at 12% — see docstring. The picker's own per-market
    # edge floors keep noise out of the bottom; the cap keeps noise out
    # of the top (calibration spikes on alt longshots).
    capped_edge = min(edge, 12.0)

    # Confidence: model probability used directly, clamped so a coinflip
    # pick can't score zero (we'd lose every alt market) and so a 95%
    # super-chalk can't dominate (those are usually -800 -- bad EV).
    prob = float(pick.get("prob", 0) or 0)
    confidence_factor = max(0.45, min(0.85, prob))

    # Market reliability: tracker win rate ÷ break-even. Cold-start
    # markets stay neutral. Bounded so a single hot streak can't
    # dominate selection.
    bet_type = pick.get("type", "") or ""
    try:
        wr = _get_market_win_rate(sport, bet_type, days=30)
    except Exception:
        wr = 0.5
    reliability_factor = max(0.85, min(1.15, wr / _BREAK_EVEN_HIT_RATE))

    # Market class: primary markets headline, alts and derivatives
    # discount.
    class_factor = _market_class_factor(sport, bet_type)

    return capped_edge * confidence_factor * reliability_factor * class_factor


_NBA_Q1_TYPES = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
_NBA_FULL_PRIMARY = {"ML", "SPREAD", "TOTAL"}
_NBA_FULL_ALT = {"ALT SPREAD", "ALT TOTAL"}
_NBA_FULL_TYPES = _NBA_FULL_PRIMARY | _NBA_FULL_ALT


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
    # Full view: primary only. No ALT fallback — the headline POTD
    # should be a clean ML/SPREAD/TOTAL pick or no pick from this
    # game at all (some other game will headline instead).
    return [p for p in picks if p.get("type") in _NBA_FULL_PRIMARY]


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
        # always matches what the user sees on that game's card —
        # there is no longer a "card says ML, POTD says +1.5 RL"
        # divergence because the POTD's per-game candidate IS the card
        # pick. Cross-game ranking still happens via _score_pick below.
        candidate_picks: list[dict] = []
        if sport == "nba" and view == "full":
            # Full view uses `best_pick_full` (primary-only ML/SPREAD/TOTAL)
            # if the API attached one; otherwise pull from the full_picks
            # list (the picker's full-game ranking). Q1 best_pick is
            # ignored entirely in this view.
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
            # No bet-type whitelist — every pick that clears MIN_EDGE +
            # has valid odds + matches the matchup is a candidate. The
            # `_score_pick` formula already weights by per-market
            # tracker reliability so unproven derivatives compete on
            # adjusted-EV terms with proven markets.
            if pick.get("edge", 0) < MIN_EDGE:
                continue
            if not pick.get("odds"):
                continue

            # Safety: verify the pick team is actually in this matchup
            # Skip check for O/U and total picks (they don't contain team names)
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

    # Rank by conviction-weighted score — see _score_pick docstring
    # for the full formula. Headline play, not maximum-EV play.
    candidates.sort(key=lambda c: _score_pick(c, sport), reverse=True)
    best = candidates[0]

    # Resolve full team names for the pick display
    home_info = best.get("home", {})
    away_info = best.get("away", {})
    home_name = home_info.get("name", home_info.get("abbreviation", "Home"))
    away_name = away_info.get("name", away_info.get("abbreviation", "Away"))

    # Resolve pick to full team name
    pick_str = best.get("pick", "")
    home_abbr = home_info.get("abbreviation", "")
    away_abbr = away_info.get("abbreviation", "")
    if home_abbr and pick_str.startswith(home_abbr):
        best["pick_full"] = pick_str.replace(home_abbr, home_name, 1)
    elif away_abbr and pick_str.startswith(away_abbr):
        best["pick_full"] = pick_str.replace(away_abbr, away_name, 1)
    else:
        best["pick_full"] = pick_str

    # Add Kelly fraction and confidence
    best["kelly_pct"] = round(_kelly_fraction(best["prob"], best["odds"]) * 100, 1)
    best["reasoning"] = _build_reasoning(best, sport)

    return best


_BET_TYPE_LABELS = {
    "ML": "moneyline", "RL": "run line", "PL": "puck line",
    "O/U": "over/under", "1st INN": "first inning",
    "ALT RL": "alt run line", "ALT O/U": "alt over/under",
    "ALT PL": "alt puck line",
    "Q1_ML": "Q1 moneyline", "Q1_SPREAD": "Q1 spread",
    "Q1_TOTAL": "Q1 total",
    "F5 ML": "first 5 moneyline", "F5 OU": "first 5 over/under",
}


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


def _implied_from_odds(odds: int) -> float:
    if not odds:
        return 0.5
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100 / (odds + 100)


def _potd_table(sport: str, view: str = "q1") -> str:
    """NBA Full-game POTD lives in pick_of_day_full; everything else
    in pick_of_day."""
    if sport == "nba" and view == "full":
        return "pick_of_day_full"
    return "pick_of_day"


def get_or_create_potd(sport: str, games_with_bets: list[dict],
                      date: str | None = None,
                      view: str = "q1") -> dict | None:
    """
    Get today's POTD, creating it if it doesn't exist.

    Once created, POTD is locked for the day - subsequent calls return
    the same pick regardless of updated predictions.

    ``view`` only matters for NBA. 'q1' uses pick_of_day; 'full' uses
    pick_of_day_full. Both can coexist for the same date.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    table = _potd_table(sport, view)

    # Check for existing POTD for this date
    existing = conn.execute(
        f"SELECT * FROM {table} WHERE date = ?", (target_date,)
    ).fetchone()

    if existing:
        return dict(existing)

    # No POTD yet - select one
    selected = select_potd(sport, games_with_bets, view=view)
    if not selected:
        return None

    # Lock it in — store the canonical abbr matchup so settler/closing-odds
    # lookups never need to reverse-map full names back to team IDs.
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
    # tracker doesn't already represent this bet. Surfaced as
    # "POTD missing from tracker" by the user when the POTD comes from
    # an alt-line bet type that wasn't in the top-N per-game picks the
    # tracker stores (MLB ALT O/U today). NHL/NBA usually have their
    # main bet (PL / Q1_TOTAL) already in picks under the abbreviated
    # display label ("COL -1.5") while POTD stores the full team name
    # ("Colorado Avalanche -1.5") — these aren't dupes, they're the
    # same bet at the (game_id, bet_type) level. Match on that instead
    # of pick text to avoid the doubling-up the user reported.
    picks_table = "picks" if sport == "mlb" else f"{sport}_picks"
    pick_text = selected.get("pick_full", selected.get("pick"))
    bet_type = selected.get("type")
    raw_game_id = selected.get("game_id")
    # MLB ID translation: best-bets / POTD store ESPN event IDs
    # (9-digit, e.g. 401815109), but the picks table joins to the
    # games table on mlb_game_id (the 6-digit MLB Stats game_pk).
    # Without this translation the settler can never match the
    # mirrored POTD row and the pick stays PEND forever -- which
    # was the bug today on SD@ARI ALT O/U Under 12.5.
    if sport == "mlb":
        try:
            from .derivative_tracker import _resolve_mlb_game_pk
            resolved_pk = _resolve_mlb_game_pk(
                conn, str(raw_game_id), target_date,
                selected.get("matchup", ""),
            )
            game_id = resolved_pk if resolved_pk else raw_game_id
        except Exception:
            game_id = raw_game_id
    else:
        # NHL / NBA picks tables share their respective game_id (the
        # ESPN event id for NBA, the NHL gamePk for NHL) -- no
        # translation needed.
        game_id = raw_game_id
    # Commit the pick_of_day INSERT FIRST so the mirror's failure
    # (if any) doesn't drag it down with it. Earlier this rolled into
    # one commit at the end and a silent mirror exception caused the
    # mirrored row to vanish — the user's MIA@LAD POTD case from
    # 2026-04-27 04:01 surfaced as "POTD card different to its actual
    # card" because the mirror INSERT was never committed.
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
        # Loud warning so a future failure is visible in logs instead
        # of silently dropping the mirrored row.
        logger.warning("POTD mirror to %s FAILED for %s/%s/%s: %s",
                       picks_table, sport, target_date, bet_type, e,
                       exc_info=True)

    logger.info("Created %s POTD for %s: %s %s (%s edge %+.1f%%)",
                sport.upper(), target_date, selected.get("matchup"),
                selected.get("pick"), selected.get("type"), selected.get("edge", 0))

    # Return the canonical DB row shape (bet_type, model_prob, ...)
    # so the API response matches what get_today_potd returns. The
    # select_potd dict uses different field names (type, prob) which
    # broke the dashboard's PotdHero bar render — model_prob came back
    # None because the wire shape was the selected-dict not the row.
    canonical = get_today_potd(sport, date=target_date, view=view)
    if canonical:
        return canonical
    # Fallback if the round-trip read failed for some reason — shape
    # the selected dict to match the row schema as best we can.
    result = dict(selected)
    result.setdefault("bet_type", selected.get("type"))
    result.setdefault("model_prob", selected.get("prob"))
    result["date"] = target_date
    return result


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
    # Team-side bets ("MIN +10.5 Q1", "Timberwolves -1.5", "DEN ML",
    # "MIN Q1 ML"). First whitespace-separated token wins.
    head = pk.split()[0] if pk.split() else ""
    return head


def refresh_potd_for_line_movement(sport: str,
                                   games_with_bets: list[dict] | None = None,
                                   view: str = "q1") -> dict:
    """Re-stamp pending POTD rows with the current line/odds/edge if
    the line has moved since the lock. Selection itself stays frozen
    — same game, same bet_type, same direction (Over/Under or team)
    — but Q1 totals / spreads / RLs / PLs that drifted on Hard Rock
    will display the live line in the POTD card so the user can place
    the bet at the price they're actually offered.

    Sourced from the per-sport picks tracker (which ``record_picks``
    keeps in sync with HR), so this piggy-backs on the existing live-
    line refresh rather than re-fetching odds itself.

    When ``games_with_bets`` is supplied, this also invalidates POTD
    locks whose bet_type no longer matches the per-game ``best_pick``
    on the dashboard. Same-game + bet_type drift is the
    "POTD shows +1.5 RL but card shows ML" case the user reported on
    2026-04-27 — the picker re-ranked but the locked POTD didn't
    follow. Drop the lock so the next read uses select_potd's new
    candidate (bet.best_pick) and the headline realigns.
    """
    if sport not in ("mlb", "nhl", "nba"):
        return {"updated": 0, "reason": f"unsupported sport {sport!r}"}
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    # NBA splits POTDs across two tables (Q1 in pick_of_day, Full in
    # pick_of_day_full). Iterate both so neither view goes stale.
    pot_tables = ["pick_of_day"]
    if sport == "nba":
        pot_tables.append("pick_of_day_full")

    pending = []
    for tbl in pot_tables:
        rows = conn.execute(
            f"SELECT id, game_id, matchup, bet_type, pick, odds, edge, model_prob, "
            f"       '{tbl}' AS _table FROM {tbl} WHERE result IS NULL"
        ).fetchall()
        for r in rows:
            pending.append(dict(r))
    if not pending:
        return {"updated": 0, "skipped": 0, "invalidated": 0}

    # Build a (game_id -> headline_bet_type) map so we can invalidate
    # POTDs whose lock has drifted away from the GameCard's best_pick.
    # NBA Q1 reads bet.best_pick (or best_pick_q1); NBA Full reads
    # bet.best_pick_full. MLB/NHL just use best_pick.
    headline_for_game: dict[str, str] = {}
    if games_with_bets:
        for game in games_with_bets:
            gid = str(game.get("game_id") or "")
            if not gid:
                continue
            if sport == "nba" and view == "full":
                bp = game.get("best_pick_full")
            elif sport == "nba":
                bp = game.get("best_pick_q1") or game.get("best_pick")
            else:
                bp = game.get("best_pick")
            if bp and bp.get("type"):
                headline_for_game[gid] = bp["type"]

    picks_table = "picks" if sport == "mlb" else f"{sport}_picks"
    updated = 0
    skipped = 0
    invalidated = 0
    for row in pending:
        pot_table = row.pop("_table", "pick_of_day")

        # Drift check: if the GameCard's current best_pick for this
        # game uses a different bet_type than the locked POTD,
        # invalidate so the next select_potd re-aligns with the card.
        # Matches the table the lock lives in to the right view (the
        # NBA full-game table only invalidates against full headlines).
        gid_key = str(row.get("game_id") or "")
        if gid_key and gid_key in headline_for_game:
            row_view = "full" if pot_table == "pick_of_day_full" else "q1"
            if sport != "nba" or row_view == view:
                live_bt = headline_for_game[gid_key]
                if live_bt and live_bt != row["bet_type"]:
                    conn.execute(f"DELETE FROM {pot_table} WHERE id = ?", (row["id"],))
                    logger.info("POTD %s invalidated #%s (%s): bet_type %s "
                                "no longer matches GameCard headline %s",
                                sport, row["id"], pot_table,
                                row["bet_type"], live_bt)
                    invalidated += 1
                    continue

        # Match: same game, same bet type, same direction. We don't
        # care if line changed — that's the whole point of the
        # refresh. Pull every pending pick for the (game, bet_type)
        # pair, then filter to same-side.
        live_rows = conn.execute(
            f"SELECT pick, odds, edge, model_prob FROM {picks_table} "
            f"WHERE game_id = ? AND bet_type = ? AND result IS NULL "
            f"ORDER BY id DESC LIMIT 10",
            (row["game_id"], row["bet_type"]),
        ).fetchall()
        if not live_rows:
            # Locked pick no longer in the live picks table — model
            # state has fundamentally changed (edge dropped below floor,
            # bet was voided by line drift). Invalidate the lock so the
            # next POTD read re-selects from current candidates.
            conn.execute(f"DELETE FROM {pot_table} WHERE id = ?", (row["id"],))
            logger.info("POTD %s invalidated #%s (%s/%s/%s): no live pick "
                        "still matches", sport, row["id"], pot_table,
                        row["bet_type"], row["pick"])
            invalidated += 1
            continue
        target_side = _pick_side(row["bet_type"], row["pick"])
        match = None
        for lr in live_rows:
            lr_d = dict(lr)
            if _pick_side(row["bet_type"], lr_d["pick"]) == target_side:
                match = lr_d
                break
        if not match:
            skipped += 1
            continue
        # Skip the no-op case so we don't churn the DB on every sync.
        if (match["pick"] == row["pick"]
                and match["odds"] == row["odds"]
                and match["edge"] == row["edge"]):
            continue
        # Re-render reasoning so the displayed copy reflects the
        # current line / edge, not the locked-at-creation snapshot.
        refreshed = {
            "type": row["bet_type"],
            "pick": match["pick"],
            "pick_full": match["pick"],
            "prob": match["model_prob"],
            "edge": match["edge"],
            "odds": match["odds"],
        }
        new_reasoning = _build_reasoning(refreshed, sport)
        conn.execute(
            f"UPDATE {pot_table} SET pick = ?, odds = ?, edge = ?, "
            f"                       model_prob = ?, reasoning = ? "
            f"WHERE id = ?",
            (match["pick"], match["odds"], match["edge"],
             match["model_prob"], new_reasoning, row["id"]),
        )
        logger.info("POTD %s line-refresh #%s (%s): %r %+d edge=%.1f -> %r %+d edge=%.1f",
                    sport, row["id"], pot_table, row["pick"], int(row["odds"] or 0),
                    float(row["edge"] or 0), match["pick"],
                    int(match["odds"] or 0), float(match["edge"] or 0))
        updated += 1

    if updated or invalidated:
        conn.commit()
    return {"updated": updated, "skipped": skipped, "invalidated": invalidated}


def update_potd_closing_odds(sport: str) -> dict:
    """Refresh closing_odds on every un-settled POTD with the latest line.

    The pick selection itself stays locked from get_or_create_potd();
    this only updates the price we'll use for CLV measurement at settle
    time. Designed to be called on every sync run -- since each call
    overwrites with the freshest available price, the LAST update before
    settle_potd() runs is effectively the closing line we record.

    Supports mlb, nhl, nba. Dispatches to per-sport resolvers because
    each sport sources odds from a different module (MLB: engine.picks,
    NHL: inline odds-api fetch, NBA: scrapers.nba_odds).
    """
    if sport not in ("mlb", "nhl", "nba"):
        return {"updated": 0, "skipped": 0, "reason": f"unsupported sport {sport!r}"}

    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    pending = conn.execute(
        "SELECT id, game_id, matchup, bet_type, pick FROM pick_of_day "
        "WHERE result IS NULL"
    ).fetchall()
    if not pending:
        return {"updated": 0, "skipped": 0}

    resolver = {
        "mlb": _resolve_mlb_closing_for_pending,
        "nhl": _resolve_nhl_closing_for_pending,
        "nba": _resolve_nba_closing_for_pending,
    }[sport]

    try:
        pairs = resolver(pending)  # list of (pending_row_id, closing_price)
    except Exception as e:
        logger.warning("%s POTD closing-odds resolver crashed: %s", sport, e)
        return {"updated": 0, "skipped": len(pending), "reason": str(e)}

    updated = 0
    for pid, closing in pairs:
        if closing is None:
            continue
        conn.execute(
            "UPDATE pick_of_day SET closing_odds = ?, "
            "       closing_odds_updated_at = datetime('now') "
            "WHERE id = ?",
            (int(closing), pid),
        )
        updated += 1

    skipped = len(pending) - updated
    if updated:
        conn.commit()
        logger.info("POTD closing odds (%s): updated %d, skipped %d",
                    sport, updated, skipped)
    return {"updated": updated, "skipped": skipped}


def _resolve_mlb_closing_for_pending(pending: list) -> list[tuple]:
    """Per-POTD (id, closing_price) pairs for MLB."""
    from .picks import fetch_real_odds_for_games, match_odds
    from .tracker import _extract_closing_for_pick
    all_odds = fetch_real_odds_for_games() or {}
    if not all_odds:
        return [(r["id"], None) for r in pending]

    out = []
    for row in pending:
        row = dict(row)
        a_abbr, h_abbr = _matchup_to_abbrs(row["matchup"])
        if not (a_abbr and h_abbr):
            out.append((row["id"], None))
            continue
        game_odds = match_odds(h_abbr, a_abbr, all_odds) or {}
        closing = _extract_closing_for_pick(
            row["bet_type"], row["pick"], h_abbr, game_odds,
        ) if game_odds else None
        out.append((row["id"], closing))
    return out


def _resolve_nhl_closing_for_pending(pending: list) -> list[tuple]:
    """Per-POTD (id, closing_price) pairs for NHL.

    NHL odds are fetched inline via the-odds-api. Shape mirrors the MLB
    odds dict so the same bet-type -> field extractor can be reused.
    """
    all_odds = _fetch_nhl_odds_map()
    if not all_odds:
        return [(r["id"], None) for r in pending]

    out = []
    for row in pending:
        row = dict(row)
        a_abbr, h_abbr = _matchup_to_abbrs(row["matchup"], sport="nhl")
        if not (a_abbr and h_abbr):
            out.append((row["id"], None))
            continue
        game_odds = _lookup_by_abbr_with_aliases(
            h_abbr, a_abbr, all_odds, sport="nhl",
        ) or {}
        closing = _nhl_closing_for_bet_type(
            row["bet_type"], row["pick"], h_abbr, game_odds,
        ) if game_odds else None
        out.append((row["id"], closing))
    return out


def _resolve_nba_closing_for_pending(pending: list) -> list[tuple]:
    """Per-POTD (id, closing_price) pairs for NBA Q1 markets."""
    try:
        from scrapers.nba_odds import fetch_nba_odds
    except Exception as e:
        logger.warning("fetch_nba_odds unavailable: %s", e)
        return [(r["id"], None) for r in pending]

    all_odds = fetch_nba_odds() or {}
    if not all_odds:
        return [(r["id"], None) for r in pending]

    out = []
    for row in pending:
        row = dict(row)
        a_abbr, h_abbr = _matchup_to_abbrs(row["matchup"], sport="nba")
        if not (a_abbr and h_abbr):
            out.append((row["id"], None))
            continue
        game_odds = _lookup_by_abbr_with_aliases(
            h_abbr, a_abbr, all_odds, sport="nba",
        ) or {}
        closing = _nba_closing_for_bet_type(
            row["bet_type"], row["pick"], h_abbr, game_odds,
        ) if game_odds else None
        out.append((row["id"], closing))
    return out


def _fetch_nhl_odds_map() -> dict:
    """Pull current NHL h2h/spreads/totals from the-odds-api, keyed by AWAY@HOME.

    Duplicates the inline fetch in engine/nhl_tracker but returns the map
    in a shape compatible with the MLB odds-dict convention (so the
    bet-type extractor can work on it unmodified).
    """
    import os, urllib.request, json as _json
    from pathlib import Path as _Path
    key_file = _Path(__file__).resolve().parent.parent / "data" / "odds_api_key.txt"
    api_key = (os.environ.get("ODDS_API_KEY")
               or (key_file.read_text().strip() if key_file.exists() else None))
    if not api_key:
        return {}
    url = ("https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds/"
           f"?apiKey={api_key}&regions=us&markets=h2h,spreads,totals"
           "&oddsFormat=american&bookmakers=draftkings")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "POTDClosing/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = _json.loads(resp.read().decode())
    except Exception as e:
        logger.warning("NHL odds fetch failed: %s", e)
        return {}

    _NHL_TEAM_ABBR = {
        "Anaheim Ducks": "ANA", "Utah Hockey Club": "UTA",
        "Boston Bruins": "BOS", "Buffalo Sabres": "BUF",
        "Calgary Flames": "CGY", "Carolina Hurricanes": "CAR",
        "Chicago Blackhawks": "CHI", "Colorado Avalanche": "COL",
        "Columbus Blue Jackets": "CBJ", "Dallas Stars": "DAL",
        "Detroit Red Wings": "DET", "Edmonton Oilers": "EDM",
        "Florida Panthers": "FLA", "Los Angeles Kings": "LAK",
        "Minnesota Wild": "MIN", "Montreal Canadiens": "MTL",
        "Nashville Predators": "NSH", "New Jersey Devils": "NJD",
        "New York Islanders": "NYI", "New York Rangers": "NYR",
        "Ottawa Senators": "OTT", "Philadelphia Flyers": "PHI",
        "Pittsburgh Penguins": "PIT", "San Jose Sharks": "SJS",
        "Seattle Kraken": "SEA", "St. Louis Blues": "STL",
        "Tampa Bay Lightning": "TBL", "Toronto Maple Leafs": "TOR",
        "Vancouver Canucks": "VAN", "Vegas Golden Knights": "VGK",
        "Washington Capitals": "WSH", "Winnipeg Jets": "WPG",
    }
    out: dict = {}
    for g in data or []:
        home = g.get("home_team", "")
        away = g.get("away_team", "")
        h_ab = _NHL_TEAM_ABBR.get(home, home[:3].upper())
        a_ab = _NHL_TEAM_ABBR.get(away, away[:3].upper())
        parsed = {"provider": "DraftKings"}
        for book in g.get("bookmakers", []):
            for market in book.get("markets", []):
                mkey = market.get("key")
                for o in market.get("outcomes", []):
                    price = o.get("price")
                    point = o.get("point")
                    name = o.get("name", "")
                    if mkey == "h2h":
                        if name == home:
                            parsed["home_ml"] = price
                        elif name == away:
                            parsed["away_ml"] = price
                    elif mkey == "spreads":
                        if name == home:
                            parsed["home_spread_odds"] = price
                            parsed["home_spread_point"] = point
                        elif name == away:
                            parsed["away_spread_odds"] = price
                            parsed["away_spread_point"] = point
                    elif mkey == "totals":
                        if "over" in name.lower():
                            parsed["over_odds"] = price
                            parsed["over_under"] = point
                        elif "under" in name.lower():
                            parsed["under_odds"] = price
        if parsed.get("home_ml"):
            out[f"{a_ab}@{h_ab}"] = parsed
    return out


def _lookup_by_abbr_with_aliases(h_abbr: str, a_abbr: str,
                                  odds_map: dict, sport: str) -> dict:
    """Abbreviation-alias-aware lookup. Mirrors engine.picks.match_odds()
    but works for NHL/NBA via the canonical alias table in engine.abbr."""
    try:
        from .abbr import alt_abbr
    except Exception:
        return odds_map.get(f"{a_abbr}@{h_abbr}", {})
    h_alt = alt_abbr(h_abbr, sport)
    a_alt = alt_abbr(a_abbr, sport)
    for a, h in ((a_abbr, h_abbr), (a_alt, h_alt), (a_alt, h_abbr), (a_abbr, h_alt)):
        row = odds_map.get(f"{a}@{h}")
        if row:
            return row
    return {}


def _nhl_closing_for_bet_type(bet_type: str, pick: str,
                               home_abbr: str, game_odds: dict) -> int | None:
    """Pick out the right NHL price for the pick's bet_type (O/U, PL, ML)."""
    if not game_odds:
        return None
    bt = bet_type
    pk = pick or ""
    if bt in ("ml", "ML"):
        return (game_odds.get("home_ml") if pk == home_abbr
                else game_odds.get("away_ml"))
    if bt in ("ou", "O/U"):
        return (game_odds.get("over_odds") if "Over" in pk
                else game_odds.get("under_odds"))
    if bt in ("pl", "PL", "rl", "RL"):
        pick_team = pk.split()[0] if pk.split() else ""
        return (game_odds.get("home_spread_odds") if pick_team == home_abbr
                else game_odds.get("away_spread_odds"))
    return None


def _nba_closing_for_bet_type(bet_type: str, pick: str,
                               home_abbr: str, game_odds: dict) -> int | None:
    """Pick out the right NBA Q1 price for the pick's bet_type."""
    if not game_odds:
        return None
    bt = bet_type
    pk = pick or ""
    if bt == "Q1_ML":
        return (game_odds.get("home_ml") if pk.startswith(home_abbr)
                else game_odds.get("away_ml"))
    if bt == "Q1_SPREAD":
        # Pick format: "BOS +2.5 Q1" or "LAL -2.5 Q1". Home vs away by
        # leading team abbreviation.
        pick_home = pk.startswith(home_abbr)
        return (game_odds.get("q1_spread_home_odds") if pick_home
                else game_odds.get("q1_spread_away_odds"))
    if bt == "Q1_TOTAL":
        return (game_odds.get("q1_over_odds") if "Over" in pk
                else game_odds.get("q1_under_odds"))
    return None


def _matchup_to_abbrs(matchup: str, sport: str = "mlb") -> tuple[str | None, str | None]:
    """Best-effort split of the matchup string into (away_abbr, home_abbr).

    Handles both ``"BOS @ NYY"`` and ``"Boston Red Sox at New York Yankees"``
    by looking up team names in the sport-specific DB. Returns
    (None, None) when parsing fails so the caller can skip silently.

    New POTDs always store abbr format; this path remains for older rows
    written before the format was normalized. Uses case-insensitive exact
    then LIKE fallback so drifted casing/nickname rows still resolve.
    """
    if not matchup:
        return None, None
    if " @ " in matchup:
        parts = matchup.split(" @ ", 1)
        return parts[0].strip(), parts[1].strip()
    if " at " not in matchup:
        return None, None
    try:
        away_name, home_name = matchup.split(" at ", 1)
        away_name = away_name.strip()
        home_name = home_name.strip()
        if sport == "mlb":
            from .db import get_conn as _gc
            table = "teams"
        elif sport == "nhl":
            from .nhl_db import get_conn as _gc
            table = "nhl_teams"
        elif sport == "nba":
            from .nba_db import get_conn as _gc
            table = "nba_teams"
        else:
            return None, None
        c = _gc()

        def _lookup(name: str):
            row = c.execute(
                f"SELECT abbreviation FROM {table} WHERE LOWER(name) = LOWER(?)",
                (name,),
            ).fetchone()
            if row:
                return row["abbreviation"]
            # Last-resort fuzzy: try matching against the final nickname token
            # (handles "Toronto Maple Leafs" vs stored "Maple Leafs") in either
            # direction. Uses LIKE rather than fetching all teams to keep the
            # query cheap.
            row = c.execute(
                f"SELECT abbreviation FROM {table} "
                f"WHERE LOWER(name) LIKE LOWER(?) OR LOWER(?) LIKE '%' || LOWER(name) || '%' "
                f"LIMIT 1",
                (f"%{name}%", name),
            ).fetchone()
            return row["abbreviation"] if row else None

        a_abbr = _lookup(away_name)
        h_abbr = _lookup(home_name)
        if a_abbr and h_abbr:
            return a_abbr, h_abbr
    except Exception:
        pass
    return None, None


def settle_potd(sport: str) -> dict:
    """Settle any pending POTDs whose games have completed."""
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    pending = conn.execute(
        "SELECT * FROM pick_of_day WHERE result IS NULL"
    ).fetchall()

    if not pending:
        return {"settled": 0}

    settled = 0
    wins = 0
    losses = 0

    for potd in pending:
        potd = dict(potd)
        result, profit = _determine_outcome(sport, conn, potd)

        if result is None:
            continue  # Game not finished yet

        conn.execute("""
            UPDATE pick_of_day SET result = ?, profit = ?, settled_at = datetime('now')
            WHERE id = ?
        """, (result, profit, potd["id"]))
        settled += 1
        if result == "W":
            wins += 1
        elif result == "L":
            losses += 1

    conn.commit()
    return {"settled": settled, "wins": wins, "losses": losses}


def _determine_outcome(sport: str, conn, potd: dict) -> tuple[str | None, float]:
    """
    Figure out whether a POTD won, lost, or pushed. Returns (result, profit).
    Returns (None, 0) if the game isn't finished yet.
    """
    date = potd["date"]
    matchup = potd["matchup"]
    bet_type = potd["bet_type"]
    pick = potd["pick"]
    odds = potd.get("odds") or -110

    # Parse matchup - handles both "AWAY @ HOME" and "Away Team at Home Team"
    try:
        if " @ " in matchup:
            away_part, home_part = [s.strip() for s in matchup.split(" @ ")]
        elif " at " in matchup:
            away_part, home_part = [s.strip() for s in matchup.split(" at ")]
        else:
            return None, 0
    except ValueError:
        return None, 0

    # Find the game - try matching by game_id first, then by date + team names
    game_id = potd.get("game_id")

    if sport == "mlb":
        # Try game_id match first
        row = None
        if game_id:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM games g
                LEFT JOIN teams ht ON g.home_team_id = ht.mlb_id
                LEFT JOIN teams at ON g.away_team_id = at.mlb_id
                WHERE g.mlb_game_id = ? AND g.status = 'final'
                LIMIT 1
            """, (game_id,)).fetchone()
        # Fallback: search by date and team name substring
        if not row:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM games g
                LEFT JOIN teams ht ON g.home_team_id = ht.mlb_id
                LEFT JOIN teams at ON g.away_team_id = at.mlb_id
                WHERE g.date = ? AND g.status = 'final'
                  AND (ht.name LIKE ? OR ht.abbreviation = ?)
                  AND (at.name LIKE ? OR at.abbreviation = ?)
                LIMIT 1
            """, (date, f"%{home_part}%", home_part, f"%{away_part}%", away_part)).fetchone()
    elif sport == "nhl":
        row = None
        if game_id:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM nhl_games g
                LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nhl_teams at ON g.away_team_id = at.id
                WHERE g.game_id = ? AND g.status = 'final'
                LIMIT 1
            """, (game_id,)).fetchone()
        if not row:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation as home_abbr, at.abbreviation as away_abbr
                FROM nhl_games g
                LEFT JOIN nhl_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nhl_teams at ON g.away_team_id = at.id
                WHERE g.date = ? AND g.status = 'final'
                  AND (ht.name LIKE ? OR ht.abbreviation = ?)
                  AND (at.name LIKE ? OR at.abbreviation = ?)
                LIMIT 1
            """, (date, f"%{home_part}%", home_part, f"%{away_part}%", away_part)).fetchone()
    elif sport == "nba":
        row = None
        if game_id:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                FROM nba_games g
                LEFT JOIN nba_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nba_teams at ON g.away_team_id = at.id
                WHERE g.game_id = ? AND g.status = 'final'
                LIMIT 1
            """, (game_id,)).fetchone()
        # Fallback by date + team names -- the ESPN game_id stored on
        # the POTD doesn't match the nba_games primary key, so for NBA
        # this path is the usual one that resolves.
        if not row:
            row = conn.execute("""
                SELECT g.*, ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr
                FROM nba_games g
                LEFT JOIN nba_teams ht ON g.home_team_id = ht.id
                LEFT JOIN nba_teams at ON g.away_team_id = at.id
                WHERE g.date = ? AND g.status = 'final'
                  AND (ht.name LIKE ? OR ht.abbreviation = ?)
                  AND (at.name LIKE ? OR at.abbreviation = ?)
                LIMIT 1
            """, (date, f"%{home_part}%", home_part, f"%{away_part}%", away_part)).fetchone()
    else:
        return None, 0

    if not row:
        # Differentiate "game not finished yet" from "we can't find
        # this game at all" -- the latter usually means a date/team
        # -name mismatch between the POTD row and the games table,
        # which is silent without a log.
        logger.debug(
            "POTD settle: no final game row for %s %s / %s (game_id=%s, "
            "home_part=%r, away_part=%r)",
            sport, date, matchup, game_id, home_part, away_part,
        )
        return None, 0

    row = dict(row)
    hs = row.get("home_score", 0) or 0
    as_ = row.get("away_score", 0) or 0

    # Compute result based on bet type
    result = None

    home_abbr = row.get("home_abbr", "")
    away_abbr = row.get("away_abbr", "")

    if bet_type == "ML":
        home_won = hs > as_
        # Pick can be abbreviation ("STL") or full name ("St. Louis Cardinals")
        pick_home = (pick == home_abbr
                     or home_part in pick
                     or (home_abbr and home_abbr in pick))
        won = (pick_home and home_won) or (not pick_home and not home_won)
        result = "W" if won else "L"

    elif bet_type in ("O/U", "ALT O/U"):
        # ALT O/U is identical to O/U for settlement — same "Over N.N" /
        # "Under N.N" pick label, just at a different line value. Without
        # this, MLB POTD picks landing on alt lines (which they routinely
        # do when the model finds value at non-standard totals) sit
        # PENDING forever.
        total = hs + as_
        if "Over" in pick:
            line = float(pick.split()[-1])
            if total > line:
                result = "W"
            elif total < line:
                result = "L"
            else:
                result = "P"
        else:
            line = float(pick.split()[-1])
            if total < line:
                result = "W"
            elif total > line:
                result = "L"
            else:
                result = "P"

    elif bet_type in ("RL", "PL", "ALT RL", "ALT PL"):
        # Parse spread from pick string (e.g. "St. Louis Cardinals +1.5" or "STL +1.5")
        import re
        spread_match = re.search(r'([+-]?\d+\.?\d*)\s*$', pick)
        spread = float(spread_match.group(1)) if spread_match else 1.5

        # Determine which team was picked - check if home team name/abbr is in pick
        pick_is_home = (home_abbr and home_abbr in pick) or (home_part and home_part in pick)
        if pick_is_home:
            margin = hs - as_
        else:
            margin = as_ - hs

        covered = margin + spread > 0
        pushed = margin + spread == 0
        if pushed:
            result = "P"
        else:
            result = "W" if covered else "L"

    elif bet_type == "1st INN":
        # Use linescore if available (MLB) - inning 1 runs
        import json as _json
        if sport == "mlb":
            home_ls = row.get("home_linescore")
            away_ls = row.get("away_linescore")
            try:
                h_inn = _json.loads(home_ls) if home_ls else []
                a_inn = _json.loads(away_ls) if away_ls else []
                scoreless = (len(h_inn) > 0 and len(a_inn) > 0
                             and h_inn[0] == 0 and a_inn[0] == 0)
            except (_json.JSONDecodeError, TypeError):
                scoreless = False
            if pick == "NRFI":
                result = "W" if scoreless else "L"
            else:
                result = "W" if not scoreless else "L"

    elif bet_type == "Q1_ML":
        # Pick format: "BOS Q1 ML" or "Boston Celtics Q1 ML" (team name
        # resolved via _pick_full_name at create time).
        hq1 = row.get("home_q1", 0) or 0
        aq1 = row.get("away_q1", 0) or 0
        pick_home = ((home_abbr and home_abbr in pick)
                     or (home_part and home_part in pick))
        home_won_q1 = hq1 > aq1
        if hq1 == aq1:
            result = "P"
        else:
            won = (pick_home and home_won_q1) or (not pick_home and not home_won_q1)
            result = "W" if won else "L"

    elif bet_type == "Q1_SPREAD":
        # Pick format: "BOS -2.5 Q1" or "Boston Celtics -2.5 Q1".
        import re as _re
        hq1 = row.get("home_q1", 0) or 0
        aq1 = row.get("away_q1", 0) or 0
        # Spread is the first +/-N.N appearing anywhere in the pick.
        m = _re.search(r"([+-]\d+\.?\d*)", pick)
        spread = float(m.group(1)) if m else 0.0
        pick_home = ((home_abbr and home_abbr in pick)
                     or (home_part and home_part in pick))
        if pick_home:
            margin = hq1 - aq1
        else:
            margin = aq1 - hq1
        covered = margin + spread > 0
        pushed = margin + spread == 0
        if pushed:
            result = "P"
        else:
            result = "W" if covered else "L"

    elif bet_type == "Q1_TOTAL":
        # Pick format: "Over 55.5 Q1" or "Under 55.5 Q1".
        hq1 = row.get("home_q1", 0) or 0
        aq1 = row.get("away_q1", 0) or 0
        q1_total = hq1 + aq1
        import re as _re
        m = _re.search(r"(\d+\.?\d*)", pick)
        line = float(m.group(1)) if m else 0.0
        is_over = "Over" in pick or "over" in pick
        if q1_total == line:
            result = "P"
        elif is_over:
            result = "W" if q1_total > line else "L"
        else:
            result = "W" if q1_total < line else "L"

    if result is None:
        # Row was found but no outcome handler matched the bet_type.
        # Without logging, the POTD silently stays pending forever.
        logger.warning(
            "POTD settle: no outcome handler for bet_type=%r on %s %s/%s",
            bet_type, sport, date, matchup,
        )
        return None, 0

    return result, _profit_on_settled(result, odds)


def _profit_on_settled(result: str | None, odds: int | float | None) -> float:
    """$100-unit profit formula used by settle_potd. Exposed so
    recalc_potd_profit can rewrite stored profits to match current code
    when an older run left them on a different unit sizing."""
    if result == "W":
        o = odds if odds is not None else -110
        return round(o if o > 0 else 10000 / abs(o), 2)
    if result == "L":
        return -100.0
    return 0.0


def recalc_potd_profit(sport: str) -> dict:
    """Rewrite the ``profit`` column on every settled POTD row using
    the current $100-unit formula. No-op on pending rows (result NULL).

    Purpose: historical rows may have been saved under an older unit
    sizing (e.g. a Kelly-fraction dollarization that used a $1000 or
    $5000 bankroll instead of $100), so the POTD hero's running
    profit diverged from what the current code would compute. Running
    this once after a unit-sizing change realigns the ledger.

    Idempotent — running it twice produces the same values.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    rows = conn.execute(
        "SELECT id, result, odds, profit FROM pick_of_day "
        "WHERE result IN ('W', 'L', 'P')"
    ).fetchall()

    updated = 0
    for r in rows:
        new_profit = _profit_on_settled(r["result"], r["odds"])
        if r["profit"] is None or abs((r["profit"] or 0) - new_profit) > 0.01:
            conn.execute(
                "UPDATE pick_of_day SET profit = ? WHERE id = ?",
                (new_profit, r["id"]),
            )
            updated += 1

    conn.commit()
    return {"sport": sport, "settled_rows": len(rows), "updated": updated}


def get_potd_summary(sport: str, limit: int = 30) -> dict:
    """Return running POTD totals + recent history."""
    _ensure_potd_table(sport)
    conn = _get_conn(sport)

    overall = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
            SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
            COALESCE(SUM(profit), 0) as profit
        FROM pick_of_day
    """).fetchone()

    overall = dict(overall)
    w = overall.get("wins") or 0
    l = overall.get("losses") or 0
    settled_total = w + l

    recent = conn.execute(
        "SELECT * FROM pick_of_day ORDER BY date DESC LIMIT ?", (limit,)
    ).fetchall()

    return {
        "total": overall.get("total") or 0,
        "wins": w,
        "losses": l,
        "pushes": overall.get("pushes") or 0,
        "pending": overall.get("pending") or 0,
        "profit": round(overall.get("profit") or 0, 2),
        "win_pct": round(w / settled_total * 100, 1) if settled_total > 0 else 0,
        "roi": round((overall.get("profit") or 0) / settled_total, 1) if settled_total > 0 else 0,
        "recent": [dict(r) for r in recent],
    }


def get_today_potd(sport: str, date: str | None = None,
                   view: str = "q1") -> dict | None:
    """Fetch just today's POTD (doesn't create one).

    Annotates the response with a computed `clv` field when both odds
    and closing_odds are present, so the UI doesn't have to redo the
    arithmetic. Positive CLV = we got a better price than the close.

    ``view`` only matters for NBA. 'q1' reads pick_of_day, 'full' reads
    pick_of_day_full.
    """
    _ensure_potd_table(sport)
    conn = _get_conn(sport)
    target_date = date or datetime.now().strftime("%Y-%m-%d")
    table = _potd_table(sport, view)
    row = conn.execute(
        f"SELECT * FROM {table} WHERE date = ?", (target_date,)
    ).fetchone()
    if not row:
        return None
    out = dict(row)
    bet_odds = out.get("odds")
    close = out.get("closing_odds")
    if bet_odds and close:
        bet_imp = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 \
                  else 100 / (bet_odds + 100)
        close_imp = abs(close) / (abs(close) + 100) if close < 0 \
                    else 100 / (close + 100)
        out["clv"] = round((close_imp - bet_imp) * 100, 2)
    return out
