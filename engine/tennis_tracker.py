"""
Tennis tracker — recording + settling tennis picks.

Schema mirrors the other sports' picks tables (id, match_id, date,
matchup, bet_type, pick, model_prob, edge, odds, result, profit,
created_at, settled_at, closing_odds, conviction_score) plus
tennis-specific columns:

    tour          'atp' | 'wta'
    surface       'Hard' | 'Clay' | 'Grass' | 'Carpet'
    best_of       3 | 5
    p1_id, p2_id  Sackmann ids
    tourney_level Sackmann level code (G/M/F/A/...)

Settlement reads ``tennis_matches.score`` to determine winner +
games-total. Tennis matches don't have intermediate "live" state in
this MVP — we settle once the final score row lands in the matches
table from the next Sackmann sync.

Public API::

    record_tennis_pick(pick_payload) -> int
    settle_tennis_picks() -> dict
    list_history(tour=None, limit=200) -> list[dict]
    get_pick_summary() -> dict
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any
from ._tz import et_today_str

logger = logging.getLogger(__name__)


_DDL = """
CREATE TABLE IF NOT EXISTS tennis_picks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tour            TEXT NOT NULL,
    match_id        TEXT NOT NULL,
    date            TEXT NOT NULL,
    matchup         TEXT NOT NULL,
    surface         TEXT,
    best_of         INTEGER,
    tourney_level   TEXT,
    p1_id           INTEGER,
    p2_id           INTEGER,
    bet_type        TEXT NOT NULL,
    pick            TEXT NOT NULL,
    model_prob      REAL,
    edge            REAL,
    odds            INTEGER NOT NULL,
    stake_units     REAL,
    result          TEXT,
    profit          REAL,
    created_at      TEXT NOT NULL,
    settled_at      TEXT,
    closing_odds    INTEGER,
    conviction_score REAL,
    -- Probation flag. While 1 (default during ramp), the pick is
    -- recorded for calibration/auto-tune purposes but the frontend
    -- shows it with a PAPER badge and the bankroll module treats
    -- profit as informational only. Flips to 0 once we accumulate
    -- N+ settled picks AND the model passes a calibration check.
    is_paper        INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_tennis_picks_pending
    ON tennis_picks (result) WHERE result IS NULL;
CREATE INDEX IF NOT EXISTS idx_tennis_picks_match
    ON tennis_picks (tour, match_id);
CREATE INDEX IF NOT EXISTS idx_tennis_picks_date
    ON tennis_picks (date);
-- Full unique on the canonical pick family. 2026-06-10 audit caught
-- 85 duplicate tennis_picks rows because no unique guard existed at
-- all — recorder spammed dupes on every slate tick.
CREATE UNIQUE INDEX IF NOT EXISTS uq_tennis_picks_family
    ON tennis_picks (date, tour, match_id, bet_type, pick);
"""

# Probation policy — until this many decided picks (W or L) accumulate,
# every emitted pick is paper-only. After threshold, calibration check
# decides whether to flip is_paper to 0 (real money) or stay paper.
PROBATION_MIN_DECIDED = 50

# Calibration tolerance for graduating off paper. Brier score across
# STAKED picks (see is_on_probation for scope) must be at most this
# much worse than ideal (0 = perfect). Tennis with calibrated Elo +
# GBM ran Brier 0.24 in the early sample; 2026-07-03 audit found the
# 105-pick real-money window at Brier 0.256 with +17.7% ROI and 57% WR.
# The 0.024 gap over the old target isn't miscalibration in the
# ROI-relevant sense (edge capture is strong) — it comes from a couple
# high-conviction shortfalls in low-frequency markets. Ceiling raised
# to 0.28 so profitable-but-imperfect calibration doesn't trap us in
# paper mode. Tighten if we ever see Brier drift with ROI decay.
PROBATION_BRIER_CEILING = 0.28


def ensure_table() -> None:
    from .tennis_db import get_conn, ensure_tables as _ensure_base
    _ensure_base()
    conn = get_conn()
    conn.executescript(_DDL)
    # Idempotent migration: add is_paper column to existing tables.
    cols = {r[1] for r in conn.execute(
        "PRAGMA table_info(tennis_picks)").fetchall()}
    if "is_paper" not in cols:
        try:
            conn.execute(
                "ALTER TABLE tennis_picks "
                "ADD COLUMN is_paper INTEGER NOT NULL DEFAULT 1"
            )
            conn.commit()
        except Exception as e:
            logger.warning("is_paper migration failed: %s", e)


def is_on_probation() -> bool:
    """True iff Tennis is still in paper-bet probation.

    2026-07-03 audit: probation gate was blocking every real bet since
    2026-05-26 (n=0 staked bets in the 5-week window) while stake-
    weighted results on the 105 real bets pre-gate were +17.7% ROI /
    57% WR. Brier on the raw pool included shadow-pick miscalibration
    which is inherent to picks the picks_core has already refused —
    counting those in the calibration gate is double-punishment.

    Gate is now scoped to REAL bets (stake > 0). Shadow picks stay in
    the DB for the empirical calibrator to learn from, but they don't
    keep the sports panel stuck in paper mode.

    Operator can still force paper via TENNIS_PAPER_ONLY flag (rare —
    e.g. after a model change we want to re-probate).
    """
    try:
        from .config import get_flag
        if get_flag("TENNIS_PAPER_ONLY", False, sport="tennis"):
            return True
    except Exception:
        pass
    from .tennis_db import get_conn
    conn = get_conn()
    cutoff = None
    try:
        from .config import CALIBRATION_CUTOFF
        cutoff = CALIBRATION_CUTOFF.get("tennis")
    except Exception:
        pass
    # Scope: real bets only (stake > 0). Shadow picks (stake = 0) are
    # tracked separately for calibration data collection and shouldn't
    # count toward the "am I trustworthy enough" gate.
    where = ["result IN ('W', 'L', 'P')", "stake_units > 0"]
    params: list = []
    if cutoff:
        where.append("date >= ?")
        params.append(cutoff)
    rows = conn.execute(
        f"SELECT result, model_prob FROM tennis_picks WHERE "
        f"{' AND '.join(where)}", params,
    ).fetchall()
    decided = [r for r in rows if r["result"] in ("W", "L")]
    if len(decided) < PROBATION_MIN_DECIDED:
        return True
    brier_terms = []
    for r in rows:
        prob = float(r["model_prob"] or 0.5)
        if r["result"] == "W":
            actual = 1.0
        elif r["result"] == "L":
            actual = 0.0
        else:
            actual = 0.5
        brier_terms.append((prob - actual) ** 2)
    if not brier_terms:
        return True
    brier = sum(brier_terms) / len(brier_terms)
    return brier > PROBATION_BRIER_CEILING


# ── Record ─────────────────────────────────────────────────────

def record_tennis_pick(pick: dict) -> int:
    """Insert one pick row. Idempotent on (tour, match_id, bet_type,
    pick) while pending — re-recording the same opportunity returns
    the existing id rather than duplicating.

    Required keys: tour, match_id, matchup, bet_type, pick, odds,
    model_prob, edge, surface, best_of, p1_id, p2_id, tourney_level.
    """
    from .tennis_db import get_conn
    ensure_table()
    conn = get_conn()

    tour = (pick.get("tour") or "").lower()
    match_id = str(pick.get("match_id") or "")
    bet_type = str(pick.get("bet_type") or "")
    pick_text = str(pick.get("pick") or "")
    p1_id = _int_or_none(pick.get("p1_id"))
    p2_id = _int_or_none(pick.get("p2_id"))
    pick_date = str(pick.get("date") or "")

    # Dedup at the match level — one pending pick per match. Two-stage
    # lookup so cross-source duplicates collapse:
    #   1. Same (tour, match_id) — typical re-record case.
    #   2. Same (tour, p1_id, p2_id, date) but DIFFERENT match_id —
    #      catches the ESPN-id-vs-TE-id duplicate (e.g. Fucsovics vs
    #      Prizmic recorded once with match_id=175351 from ESPN and
    #      again with match_id=te-3196248 from Tennis Explorer for the
    #      identical real match).
    # Standard layout #2: ONE pick per game card. Tracker mirrors that.
    existing = conn.execute(
        "SELECT id, bet_type, pick FROM tennis_picks "
        "WHERE tour = ? AND match_id = ? AND result IS NULL "
        "ORDER BY id DESC LIMIT 1",
        (tour, match_id),
    ).fetchone()
    if not existing and p1_id and p2_id and pick_date:
        # Player-pair sig is order-independent; accept either (p1, p2)
        # or (p2, p1). Date window widened to ±2 days so cross-source
        # duplicates collapse even when ESPN and TE put the same fixture
        # on adjacent dates (the canonical Parry/Putintseva dup 2026-05-15
        # had ESPN at 5/15 and TE at 5/16 for the same scheduled match).
        # In tennis the same two players can't legitimately play twice
        # within 48h on the same tour — every other tournament has at
        # least one rest-day between rounds — so the false-positive
        # risk is essentially zero.
        existing = conn.execute(
            "SELECT id, bet_type, pick FROM tennis_picks "
            "WHERE tour = ? "
            "  AND date BETWEEN date(?, '-2 days') AND date(?, '+2 days') "
            "  AND result IS NULL "
            "  AND ( (p1_id = ? AND p2_id = ?) "
            "     OR (p1_id = ? AND p2_id = ?) ) "
            "ORDER BY id DESC LIMIT 1",
            (tour, pick_date, pick_date, p1_id, p2_id, p2_id, p1_id),
        ).fetchone()
    if not existing and p1_id and p2_id and pick_date:
        # Voided-pick recurrence guard. If the same player-pair was
        # voided within the last 3 days (likely a wrongly-voided pick
        # or a phantom-projection void) and now a fresh pick for the
        # same pair lands, skip the insert — they're the same logical
        # match, not two distinct opportunities. Without this, a void
        # on day N gets followed by a fresh duplicate insert on day
        # N+1 (Parry/Putintseva 2026-05-15→5-16 case).
        existing_void = conn.execute(
            "SELECT id FROM tennis_picks "
            "WHERE tour = ? "
            "  AND date BETWEEN date(?, '-3 days') AND date(?, '+3 days') "
            "  AND result = 'V' "
            "  AND ( (p1_id = ? AND p2_id = ?) "
            "     OR (p1_id = ? AND p2_id = ?) ) "
            "ORDER BY id DESC LIMIT 1",
            (tour, pick_date, pick_date, p1_id, p2_id, p2_id, p1_id),
        ).fetchone()
        if existing_void:
            logger.info("skip duplicate pick (same player-pair previously "
                        "voided, id=%s) for %s vs %s on %s",
                        existing_void["id"], p1_id, p2_id, pick_date)
            return int(existing_void["id"])
    if existing:
        # Same bet+pick → no-op. Different bet/pick → replace headline.
        if (existing["bet_type"] == bet_type
                and existing["pick"] == pick_text):
            return int(existing["id"])
        conn.execute(
            "UPDATE tennis_picks SET bet_type = ?, pick = ?, "
            "  model_prob = ?, edge = ?, odds = ? "
            "WHERE id = ?",
            (
                bet_type, pick_text,
                float(pick.get("model_prob") or 0.0),
                float(pick.get("edge") or 0.0),
                int(pick.get("odds") or 0),
                int(existing["id"]),
            ),
        )
        conn.commit()
        return int(existing["id"])

    now_utc = datetime.now(timezone.utc).isoformat()
    today_local = et_today_str()
    # Pick's stored date is the MATCH date, not the recording date —
    # otherwise picks for tomorrow's matches recorded today land under
    # today, settle queries can't find them on the actual match day,
    # and the tracker UI groups them under the wrong day. Caller
    # passes pick['date'] from tennis_scheduled_matches.date; fall
    # back to today only when a caller hasn't migrated yet.
    pick_date = str(pick.get("date") or today_local)

    paper_flag = 1 if is_on_probation() else 0
    cur = conn.execute(
        """
        INSERT INTO tennis_picks (
            tour, match_id, date, matchup, surface, best_of,
            tourney_level, p1_id, p2_id, bet_type, pick,
            model_prob, edge, odds, stake_units, result, profit,
            created_at, settled_at, closing_odds, conviction_score,
            is_paper
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL,
                  ?, NULL, NULL, ?, ?)
        """,
        (
            tour, match_id, pick_date,
            str(pick.get("matchup") or ""),
            str(pick.get("surface") or ""),
            int(pick.get("best_of") or 3),
            str(pick.get("tourney_level") or ""),
            _int_or_none(pick.get("p1_id")),
            _int_or_none(pick.get("p2_id")),
            bet_type, pick_text,
            float(pick.get("model_prob") or 0.0),
            float(pick.get("edge") or 0.0),
            int(pick.get("odds") or 0),
            pick.get("stake_units"),
            now_utc,
            float(pick.get("conviction_score") or 0.0) or None,
            paper_flag,
        ),
    )
    conn.commit()
    new_id = int(cur.lastrowid)

    # Write-through to unified picks store (#160 / #163). Tennis stores
    # extra context (tour/surface/best_of/etc.) in the unified row's
    # JSON context field; sync_for_date pulls them from tennis_picks.
    try:
        from .unified_tracker import sync_for_date
        sync_for_date("tennis", pick_date)
    except Exception as e:
        logger.debug("unified write-through (tennis) skipped: %s", e)

    # Phase-2 cutover: dual-write into picks_unified (canonical store).
    try:
        from .picks_unified._legacy_bridge import mirror_to_unified
        mirror_to_unified(
            sport="tennis", league=tour,
            native_game_id=match_id,
            pick_date=pick_date,
            matchup=str(pick.get("matchup") or ""),
            bet_type=bet_type,
            pick_text=pick_text,
            odds=int(pick.get("odds") or 0),
            prob=float(pick.get("model_prob") or 0.0),
            edge_pct=float(pick.get("edge") or 0.0),
            stake_units=float(pick.get("stake_units") or 0.0),
            is_shadow=bool(paper_flag),
        )
    except Exception as e:
        logger.debug("picks_unified mirror (tennis) skipped: %s", e)

    return new_id


# ── Settle ─────────────────────────────────────────────────────

_SET_SCORE_RE = re.compile(r"(\d+)-(\d+)(?:\([^)]*\))?")


def _parse_score(score: str | None) -> dict | None:
    """Parse a Sackmann-style score string like '6-4 7-6(5) 6-2' into
    ``{p1_sets, p2_sets, p1_games, p2_games}``.

    p1 here is the ROW WINNER (Sackmann lists winner's score first
    in each set token). Caller maps row-winner ↔ pick player using
    the matches.winner_id field.
    """
    if not score:
        return None
    score = score.strip()
    if score in ("W/O", "RET", "DEF", "ABD") or "ret." in score.lower():
        return None
    p1_sets = p2_sets = 0
    p1_games = p2_games = 0
    for m in _SET_SCORE_RE.finditer(score):
        a, b = int(m.group(1)), int(m.group(2))
        p1_games += a
        p2_games += b
        if a > b:
            p1_sets += 1
        elif b > a:
            p2_sets += 1
    if p1_sets == 0 and p2_sets == 0:
        return None
    return {"p1_sets": p1_sets, "p2_sets": p2_sets,
            "p1_games": p1_games, "p2_games": p2_games}


def _resolve_pick(row: dict, match: dict) -> str | None:
    """Compute W/L/P for one pending tennis pick. Returns None when
    the match isn't yet in the DB or the score is unresolvable
    (walkovers, retirements before sets complete).

    ``match`` carries either Sackmann-shape fields (winner_id /
    winner_name / loser_name / score) or ESPN-shape fields adapted
    via ``_match_from_scheduled``. Both end up with the same field
    set so this function doesn't care which source it came from.
    """
    score = match.get("score") or ""
    parsed = _parse_score(score)
    if parsed is None:
        return None
    # Normalize so p1_* always refers to the winner. Sackmann lists
    # winner-first by convention but ESPN scores "p1-p2 p1-p2 ..."
    # which can have either side losing — without this, a Fils win
    # 1-6 4-6 reports parsed.p1_sets=0 and the rest of the resolver
    # would think p1 (Fils) lost when he actually won.
    if parsed["p2_sets"] > parsed["p1_sets"]:
        parsed = {
            "p1_sets":  parsed["p2_sets"],
            "p2_sets":  parsed["p1_sets"],
            "p1_games": parsed["p2_games"],
            "p2_games": parsed["p1_games"],
        }
    bet_type = (row.get("bet_type") or "").upper()
    pick_text = (row.get("pick") or "").strip()
    winner_name = (match.get("winner_name") or "").lower()
    loser_name = (match.get("loser_name") or "").lower()

    p1_sets = parsed["p1_sets"]    # winner sets (post-normalization)
    p2_sets = parsed["p2_sets"]    # loser sets
    p1_games = parsed["p1_games"]
    p2_games = parsed["p2_games"]
    total_sets = p1_sets + p2_sets
    total_games = p1_games + p2_games

    def _picked_is_winner(name_token: str) -> bool | None:
        """Returns True if name_token names the winner, False if
        loser, None if neither matches.

        Accepts both full names ("Zhizhen Zhang") and TE's last-first-
        initial format ("Zhang Z."). For TE-shape names, compare last
        name only so "Zhizhen Zhang" matches "Zhang Z." → both share
        last name 'zhang'.
        """
        if not name_token:
            return None
        t = name_token.strip().lower()

        def _last(name: str) -> str:
            n = name.strip().lower()
            # TE renders "Last F." → take everything before the
            # initial+period. Otherwise the trailing token is the
            # last name.
            n = re.sub(r"\s+[a-z]\.?\s*$", "", n)
            tokens = n.split()
            return tokens[-1] if tokens else n

        # Direct substring match (fast path for full-name vs full-name).
        if t in winner_name or winner_name.startswith(t) \
                or winner_name.endswith(t):
            return True
        if t in loser_name or loser_name.startswith(t) \
                or loser_name.endswith(t):
            return False
        # Last-name fallback — TE shape vs full-name shape disagreement.
        last_pick = _last(t)
        if last_pick:
            if last_pick == _last(winner_name):
                return True
            if last_pick == _last(loser_name):
                return False
        return None

    if bet_type == "ML":
        side = _picked_is_winner(pick_text)
        if side is None:
            return None
        return "W" if side else "L"

    if bet_type == "SET_SPREAD":
        m = re.match(r"^(.*?)([+-]\d+\.\d+)\s*$", pick_text)
        if not m:
            return None
        picked_name = m.group(1).strip()
        try:
            point = float(m.group(2))
        except ValueError:
            return None
        side = _picked_is_winner(picked_name)
        if side is None:
            return None
        picked_sets = p1_sets if side else p2_sets
        other_sets = p2_sets if side else p1_sets
        margin = picked_sets - other_sets
        if margin + point > 0:
            return "W"
        if margin + point == 0:
            return "P"
        return "L"

    if bet_type == "TOTAL_GAMES":
        m = re.match(r"^(over|under)\s+(\d+\.?\d*)$", pick_text, re.I)
        if not m:
            return None
        side = m.group(1).lower()
        try:
            line = float(m.group(2))
        except ValueError:
            return None
        if total_games == line:
            return "P"
        is_over = total_games > line
        return "W" if (is_over and side == "over") or \
                       (not is_over and side == "under") else "L"

    if bet_type == "TOTAL_SETS":
        m = re.match(r"^(over|under)\s+(\d+\.?\d*)$", pick_text, re.I)
        if not m:
            return None
        side = m.group(1).lower()
        try:
            line = float(m.group(2))
        except ValueError:
            return None
        if total_sets == line:
            return "P"
        is_over = total_sets > line
        return "W" if (is_over and side == "over") or \
                       (not is_over and side == "under") else "L"

    if bet_type == "WIN_AT_LEAST_ONE_SET":
        # Pick text: "{player_name} Yes" or "{player_name} No"
        # — Yes wins if that player won >= 1 set.
        m = re.match(r"^(.*?)\s+(yes|no)\s*$", pick_text, re.I)
        if not m:
            return None
        picked_name = m.group(1).strip()
        side = m.group(2).lower()
        is_winner = _picked_is_winner(picked_name)
        if is_winner is None:
            return None
        # Picked player's sets = p1_sets if they won, p2_sets if they lost
        picked_sets = p1_sets if is_winner else p2_sets
        won_at_least_one = picked_sets >= 1
        if side == "yes":
            return "W" if won_at_least_one else "L"
        return "L" if won_at_least_one else "W"

    if bet_type == "SET_BETTING":
        # Pick text: "2-1", "3-2", etc. — the explicit set score.
        m = re.match(r"^(\d+)-(\d+)$", pick_text.strip())
        if not m:
            return None
        # Convention: pick is from p1's perspective ("2-1" means p1
        # wins 2 sets to 1). Compare against actual winner's sets.
        try:
            picked_p1 = int(m.group(1))
            picked_p2 = int(m.group(2))
        except ValueError:
            return None
        # We need to know which player p1 was. The picks row carries
        # p1_id / p2_id; the match row has winner_id (when from
        # tennis_matches) or winner='p1'/'p2' (from scheduled).
        winner_is_p1 = match.get("winner") == "p1" or \
                        match.get("winner_id") == row.get("p1_id")
        if winner_is_p1:
            actual_p1_sets = p1_sets
            actual_p2_sets = p2_sets
        else:
            actual_p1_sets = p2_sets
            actual_p2_sets = p1_sets
        if picked_p1 == actual_p1_sets and picked_p2 == actual_p2_sets:
            return "W"
        return "L"

    return None


def _match_from_scheduled(row: dict, p1_id: int, p2_id: int) -> dict:
    """Adapt a tennis_scheduled_matches row to the field shape
    `_resolve_pick` expects (winner_id, winner_name, loser_name).
    """
    winner = row.get("winner")  # 'p1' | 'p2' | None
    if winner == "p1":
        return {
            "score": row.get("score"),
            "winner_id": int(p1_id),
            "winner_name": row.get("p1_name"),
            "loser_name": row.get("p2_name"),
            "winner": "p1",
        }
    if winner == "p2":
        return {
            "score": row.get("score"),
            "winner_id": int(p2_id),
            "winner_name": row.get("p2_name"),
            "loser_name": row.get("p1_name"),
            "winner": "p2",
        }
    return {"score": row.get("score"), "winner": winner}


def _match_from_results_table(conn, pick: dict) -> dict | None:
    """Look up a result in tennis_match_results when neither the
    scheduled row nor tennis_matches has it (HR-supplement picks for
    Challenger / ITF / sub-tour events). Matches by Sackmann player
    ids first (most reliable), then by player-name pair (covers rows
    where TE name resolution failed at ingest).

    Returns the same shape as ``_match_from_scheduled``.
    """
    p1_id = int(pick.get("p1_id") or 0)
    p2_id = int(pick.get("p2_id") or 0)
    pick_date = str(pick.get("date") or "")
    if not pick_date:
        return None
    # ±2 day window so timezone slippage between HR (UTC-ish) and TE
    # (Europe/Prague) doesn't drop matches that played at midnight.
    if p1_id and p2_id:
        row = conn.execute(
            """SELECT p1_name, p2_name, p1_id, p2_id, winner, score
               FROM tennis_match_results
               WHERE date BETWEEN date(?, '-2 day') AND date(?, '+2 day')
                 AND tour = ?
                 AND ((p1_id = ? AND p2_id = ?)
                      OR (p1_id = ? AND p2_id = ?))
               ORDER BY ABS(julianday(date) - julianday(?))
               LIMIT 1""",
            (pick_date, pick_date, pick.get("tour"),
             p1_id, p2_id, p2_id, p1_id, pick_date),
        ).fetchone()
        if row:
            return _adapt_results_row(dict(row), p1_id, p2_id)
    # Name-pair fallback. Strip the tour filter — the TennisExplorer
    # scraper has known cases where WTA matches land in the table with
    # tour='atp' (the per-block women/men detection misses some pages,
    # falls back to default_tour which is 'atp'). Without dropping the
    # tour filter, every WTA Rome 5/7-5/8 pick voided despite results
    # being present. Name-pair on first+last is specific enough that
    # cross-tour collisions are negligible.
    matchup = pick.get("matchup") or ""
    parts = matchup.split(" vs ")
    if len(parts) != 2:
        return None
    p1_name, p2_name = parts[0].strip(), parts[1].strip()
    last1 = p1_name.split()[-1] if p1_name else ""
    last2 = p2_name.split()[-1] if p2_name else ""
    if not (last1 and last2):
        return None
    # First-name + last-name pair is more selective than last-name only.
    # Build LIKE patterns for "First L%" (TE truncates first name to
    # initial: "Gauff C." matches "%Gauff%" but a single last name like
    # "Smith" could collide across tours, so we also require the leading
    # first-name fragment when present.
    first1 = p1_name.split()[0] if p1_name else ""
    first2 = p2_name.split()[0] if p2_name else ""
    row = conn.execute(
        """SELECT p1_name, p2_name, p1_id, p2_id, winner, score
           FROM tennis_match_results
           WHERE date BETWEEN date(?, '-2 day') AND date(?, '+2 day')
             AND ((p1_name LIKE ? AND p2_name LIKE ?)
                  OR (p1_name LIKE ? AND p2_name LIKE ?))
           LIMIT 1""",
        (pick_date, pick_date,
         f"{last1}%{first1[:1] if first1 else ''}%",
         f"{last2}%{first2[:1] if first2 else ''}%",
         f"{last2}%{first2[:1] if first2 else ''}%",
         f"{last1}%{first1[:1] if first1 else ''}%"),
    ).fetchone()
    if row:
        return _adapt_results_row(dict(row), p1_id, p2_id)
    return None


def _adapt_results_row(row: dict, pick_p1_id: int, pick_p2_id: int) -> dict:
    """Map a tennis_match_results row to the (winner_id, winner_name,
    loser_name, score) shape ``_resolve_pick`` consumes.

    The pick stores its OWN p1/p2 ordering; the results row may have
    them flipped. Reconcile by checking which result-side matches the
    pick's p1.
    """
    winner_side = row.get("winner")  # 'p1' | 'p2' (TE-side)
    res_p1_id = int(row.get("p1_id") or 0)
    pick_p1_in_te_p1 = (res_p1_id == pick_p1_id) if pick_p1_id else None
    # If the TE row has p1_id == pick.p1_id, sides align. Otherwise
    # they're swapped. When ids are missing on TE we leave this
    # decision up to the name match (last-name was already filtered).
    if pick_p1_in_te_p1 is False:
        # Swap winner perspective
        winner_side = "p2" if winner_side == "p1" else "p1"
    if winner_side == "p1":
        return {
            "score": row.get("score"),
            "winner_id": pick_p1_id or res_p1_id,
            "winner_name": row.get("p1_name"),
            "loser_name": row.get("p2_name"),
            "winner": "p1",
        }
    return {
        "score": row.get("score"),
        "winner_id": pick_p2_id or int(row.get("p2_id") or 0),
        "winner_name": row.get("p2_name"),
        "loser_name": row.get("p1_name"),
        "winner": "p2",
    }


def _profit(odds: int | None, result: str) -> float:
    if not odds or result == "P":
        return 0.0
    if result == "L":
        return -100.0
    if result != "W":
        return 0.0
    n = float(odds)
    return 100.0 * (100.0 / abs(n)) if n < 0 else n


# Grace window before Path-2 (legacy) phantom voiding fires. Was 1 day,
# but TennisExplorer's results scraper lags 2-3 days behind ESPN's
# schedule update, so picks were getting voided BEFORE their results
# landed. Medjedovic-Royer 5/7 Rome lost 6-4 6-3 (clean L) but got
# voided on 5/8 because tennis_match_results didn't catch up until
# 5/10. 22 voids on 5/7 were almost all this pattern.
# 5 days gives the results scraper plenty of room. Path 1 (both-players-
# played-someone-else conclusive evidence) still voids same-day when
# applicable; this only delays the legacy fallback.
PHANTOM_VOID_DAYS = 5


def _void_phantom_picks(conn) -> int:
    """Force-void HR-supplement picks that never resolved.

    HR posts bracket-projection markets at the start of a tournament:
    "if both win their R32, they meet in R16 on date X." If either
    player loses earlier in the bracket, the projected match never
    happens, but the market sits in our scheduled table with
    status='pre' forever.

    Two detection paths:

    1. **Both-played-someone-else (fast)**: pick is dated yesterday or
       earlier AND BOTH players have a tennis_match_results row for
       the pick date showing they played a DIFFERENT opponent. This is
       same-day-or-next conclusive evidence the projected match never
       happened. Voids immediately without waiting for the grace window.
    2. **Stale-with-no-match (legacy)**: pick is older than
       PHANTOM_VOID_DAYS AND no row in tennis_match_results shows the
       player pair playing within ±2 days. Safety net for cases where
       only one player's result is available.

    We only fire on ``hr-`` prefixed match_ids — ESPN matches always
    settle via the scheduled table flipping to 'post', so phantoms
    there are someone else's bug.
    """
    from datetime import date, timedelta, datetime as _dt, timezone as _tz
    # Path 1 cutoff: yesterday. Path 2 cutoff: PHANTOM_VOID_DAYS ago.
    fast_cutoff = (date.today() - timedelta(days=1)).isoformat()
    cutoff = (date.today() - timedelta(days=PHANTOM_VOID_DAYS)).isoformat()
    # Both HR-supplemented matchups AND TE-sourced ones can ship
    # bracket-projection phantoms. Without this, TE picks for
    # qualifier matches that one player lost early sat as zombies
    # (Fucsovics vs Prizmic 5-06 — Fucsovics didn't play 5-06; the
    # Prizmic-vs-? match never resolved on TE either).
    candidates = conn.execute(
        "SELECT id, tour, match_id, p1_id, p2_id, date, matchup "
        "FROM tennis_picks "
        "WHERE result IS NULL AND date <= ? "
        "  AND (match_id LIKE 'hr-%' OR match_id LIKE 'te-%')",
        (fast_cutoff,),
    ).fetchall()
    if not candidates:
        return 0

    voided = 0
    now_iso = _dt.now(_tz.utc).isoformat()
    for c in candidates:
        c = dict(c)
        p1 = int(c.get("p1_id") or 0)
        p2 = int(c.get("p2_id") or 0)
        if not (p1 and p2):
            # No resolved player ids — skip the void check (we'd
            # false-positive on the name path). Settle path's name
            # fallback will still try.
            continue
        # Did this exact pair play within ±2 days of the pick date?
        played = conn.execute(
            """SELECT 1 FROM tennis_match_results
                WHERE tour = ?
                  AND date BETWEEN date(?, '-2 day') AND date(?, '+2 day')
                  AND ((p1_id = ? AND p2_id = ?)
                       OR (p1_id = ? AND p2_id = ?))
                LIMIT 1""",
            (c["tour"], c["date"], c["date"],
             p1, p2, p2, p1),
        ).fetchone()
        if played:
            continue
        # Suspended-match guard. If the scheduled_matches row for our
        # pick is still sitting at a non-final status (pre / live /
        # suspended / postponed), the actual match hasn't concluded
        # yet — voiding now would clobber a real outcome that's still
        # being played. Arthur Fery vs Choinski 2026-05-15 sat
        # suspended twice for two days; Path 1 voided it because both
        # players had nearby results from earlier matches in the
        # tournament. Keep the pick pending while the schedule still
        # carries the match.
        sched_status = conn.execute(
            "SELECT status FROM tennis_scheduled_matches "
            "WHERE match_id = ?",
            (c["match_id"],),
        ).fetchone()
        if sched_status and (sched_status["status"] or "").lower() != "post":
            continue
        # Path 1 (fast): if BOTH players have a result on the pick date
        # showing they played someone ELSE (not each other), void NOW
        # — same-day evidence the projected matchup never happened.
        # Earlier this checked only "did player X play any match" which
        # false-positived when player X had a recent tournament result
        # vs a third party. Now require the opponent in that result to
        # be a third party, not the other side of our pick.
        p1_played = conn.execute(
            """SELECT 1 FROM tennis_match_results
                WHERE tour = ?
                  AND date BETWEEN date(?, '-2 day') AND date(?, '+2 day')
                  AND ((p1_id = ? AND COALESCE(p2_id, 0) != ?)
                       OR (p2_id = ? AND COALESCE(p1_id, 0) != ?))
                LIMIT 1""",
            (c["tour"], c["date"], c["date"], p1, p2, p1, p2),
        ).fetchone()
        p2_played = conn.execute(
            """SELECT 1 FROM tennis_match_results
                WHERE tour = ?
                  AND date BETWEEN date(?, '-2 day') AND date(?, '+2 day')
                  AND ((p1_id = ? AND COALESCE(p2_id, 0) != ?)
                       OR (p2_id = ? AND COALESCE(p1_id, 0) != ?))
                LIMIT 1""",
            (c["tour"], c["date"], c["date"], p2, p1, p2, p1),
        ).fetchone()
        if not (p1_played and p2_played):
            # Path 2 (legacy): defer to the grace window. Only one
            # player has played-someone-else evidence yet — wait for
            # the other before concluding the matchup is truly phantom.
            if c["date"] > cutoff:
                continue
        # Phantom confirmed — void it.
        conn.execute(
            "UPDATE tennis_picks SET result = ?, profit = 0, "
            "  settled_at = ? WHERE id = ?",
            ("V", now_iso, c["id"]),
        )
        voided += 1
        logger.info("voided phantom pick id=%s %s on %s",
                    c["id"], c.get("matchup"), c.get("date"))
    if voided:
        conn.commit()
    return voided


def settle_tennis_picks() -> dict:
    """Walk pending tennis picks, look up the match in
    tennis_matches, settle whatever has resolved.

    Auto-fetches the last 5 days of TennisExplorer results before the
    settle pass — without this, picks sat pending forever because
    scrapers.tennis_results was a standalone CLI nobody invoked. The
    settler hit tennis_match_results expecting fresh data and got
    nothing. Fetch is idempotent (existing rows update in place) and
    cheap (~5s for 5 days of TE pages)."""
    from .tennis_db import get_conn
    # Refresh TE results so the settler has the latest data. Failures
    # are logged but don't abort the settle pass — stale results are
    # better than zero settle.
    try:
        from datetime import date, timedelta
        from scrapers.tennis_results import fetch_results_for_date, store_results
        today = date.today()
        for back in range(0, 5):
            d = (today - timedelta(days=back)).isoformat()
            try:
                rows = fetch_results_for_date(d)
                if rows:
                    store_results(rows)
            except Exception as e:
                logger.warning("TE results fetch %s failed: %s", d, e)
    except Exception as e:
        logger.warning("TE results fetch chain crashed: %s", e)

    ensure_table()
    conn = get_conn()
    pending = conn.execute(
        "SELECT * FROM tennis_picks WHERE result IS NULL"
    ).fetchall()
    summary = {"settled": 0, "wins": 0, "losses": 0, "pushes": 0,
               "pending_remaining": 0, "voided_phantom": 0}
    if not pending:
        summary["pending_remaining"] = 0
        return summary

    # Phantom-matchup void: HR's supplement publishes bracket-projection
    # markets for matchups that often never play (Bolt vs Walton 5-01
    # was the canonical case — neither faced the other that day, they
    # met on 5-02). Without a void rule, those picks accumulate as
    # zombie pendings forever and pollute the tracker.
    #
    # Rule: any HR-supplement pick whose date is >= PHANTOM_VOID_DAYS
    # ago AND has no matching player-pair in tennis_match_results
    # within ±2 days is force-voided. Result='V', profit=0.
    summary["voided_phantom"] = _void_phantom_picks(conn)
    if summary["voided_phantom"]:
        # Re-pull pending after the void pass so we don't waste cycles
        # trying to settle rows we just voided.
        pending = conn.execute(
            "SELECT * FROM tennis_picks WHERE result IS NULL"
        ).fetchall()

    now_iso = datetime.now(timezone.utc).isoformat()
    for row in pending:
        d = dict(row)
        # Live picks reference ESPN-style match_ids that live in
        # tennis_scheduled_matches, not Sackmann's tennis_matches
        # historical CSV. Look at scheduled first (the actual source
        # for our picks); fall back to tennis_matches for any legacy
        # Sackmann-id rows still around.
        sched_row = conn.execute(
            "SELECT * FROM tennis_scheduled_matches "
            "WHERE tour = ? AND match_id = ?",
            (d["tour"], d["match_id"]),
        ).fetchone()
        match: dict | None = None
        if sched_row:
            sched_dict = dict(sched_row)
            if (sched_dict.get("status") == "post"
                    and sched_dict.get("score")):
                match = _match_from_scheduled(
                    sched_dict,
                    int(d.get("p1_id") or 0),
                    int(d.get("p2_id") or 0),
                )
        if not match:
            match_row = conn.execute(
                "SELECT * FROM tennis_matches "
                "WHERE tour = ? AND match_id = ?",
                (d["tour"], d["match_id"]),
            ).fetchone()
            if match_row:
                match = dict(match_row)
        if not match:
            # HR-supplement fallback: ESPN doesn't carry sub-tour
            # events (Challenger / ITF / WTA125) so the scheduled row
            # never flips to 'post', and Sackmann ships annually so
            # the corpus doesn't have current-season minor-tour
            # results either. Tennis Explorer fills the gap — see
            # scrapers.tennis_results. Match by Sackmann player ids
            # within a ±2 day window, name-pair as a final fallback.
            match = _match_from_results_table(conn, d)
        if not match:
            summary["pending_remaining"] += 1
            continue
        result = _resolve_pick(d, match)
        if result is None:
            summary["pending_remaining"] += 1
            continue
        profit = _profit(d.get("odds"), result)
        try:
            conn.execute(
                "UPDATE tennis_picks SET result = ?, profit = ?, "
                "  settled_at = ? WHERE id = ?",
                (result, profit, now_iso, d["id"]),
            )
            summary["settled"] += 1
            if result == "W":
                summary["wins"] += 1
            elif result == "L":
                summary["losses"] += 1
            else:
                summary["pushes"] += 1
        except Exception as e:
            logger.warning("settle update failed for id=%s: %s", d["id"], e)
            summary["pending_remaining"] += 1
    conn.commit()
    return summary


# ── Reads ─────────────────────────────────────────────────────

def list_history(tour: str | None = None, limit: int = 200) -> list[dict]:
    from .tennis_db import get_conn
    ensure_table()
    conn = get_conn()
    if tour:
        rows = conn.execute(
            "SELECT * FROM tennis_picks WHERE tour = ? "
            "ORDER BY id DESC LIMIT ?",
            (tour, int(limit)),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM tennis_picks ORDER BY id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        out.append(d)
    return out


def get_pick_summary(tour: str | None = None) -> dict:
    """Tracker summary in the same {overall, by_type} shape team
    sports use, so the frontend's PickHistory component renders
    tennis identically to MLB/NHL/NBA.

    Per-bet-type tiles cover every market the tennis picker emits:
    ML, SET_BETTING, SET_SPREAD, TOTAL_GAMES, TOTAL_SETS,
    WIN_AT_LEAST_ONE_SET, P1_TOTAL_GAMES, P2_TOTAL_GAMES.

    `overall` includes CLV (avg implied-prob delta over settled
    picks with closing_odds populated) and pending count, mirroring
    the team-sport summary contract.

    Also keeps the legacy flat fields (total / wins / losses / etc.)
    so older readers don't break.
    """
    from .tennis_db import get_conn
    ensure_table()
    conn = get_conn()

    tour_clause = ""
    tour_params: list = []
    if tour:
        tour_clause = " AND tour = ?"
        tour_params = [tour]

    bet_types = (
        "ML", "SET_BETTING", "SET_SPREAD", "TOTAL_GAMES", "TOTAL_SETS",
        "WIN_AT_LEAST_ONE_SET", "P1_TOTAL_GAMES", "P2_TOTAL_GAMES",
    )
    # Profit + ROI stake-weighted: each row contributes
    #   profit * COALESCE(stake_units, 1.0) to the dollar numerator
    #   COALESCE(stake_units, 1.0) (for W/L only) to the unit denom
    # NULL stake_units defaults to 1.0 (legacy 1u-basis preservation);
    # 0.0 (shadow) correctly contributes nothing.
    by_type: dict[str, dict] = {}
    for bt in bet_types:
        row = conn.execute(
            f"""SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses,
                    SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) AS pushes,
                    SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) AS pending,
                    COALESCE(SUM(profit * COALESCE(stake_units, 1.0)), 0) AS profit,
                    COALESCE(SUM(CASE WHEN result IN ('W','L')
                                        THEN COALESCE(stake_units, 1.0)
                                        ELSE 0 END), 0) AS stake_units_settled
                FROM tennis_picks
                WHERE bet_type = ?{tour_clause}""",
            (bt, *tour_params),
        ).fetchone()
        total = row["total"] or 0
        w = row["wins"] or 0
        l = row["losses"] or 0
        settled = w + l
        staked_u = row["stake_units_settled"] or 0
        by_type[bt] = {
            "total": total,
            "wins": w,
            "losses": l,
            "pushes": row["pushes"] or 0,
            "pending": row["pending"] or 0,
            "profit": round(row["profit"] or 0, 2),
            "win_pct": round(w / settled * 100, 1) if settled else 0.0,
            "roi": round((row["profit"] or 0) / staked_u, 1) if staked_u else 0.0,
        }

    totals = conn.execute(
        f"""SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) AS pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) AS pending,
                COALESCE(SUM(profit * COALESCE(stake_units, 1.0)), 0) AS profit,
                COALESCE(SUM(CASE WHEN result IN ('W','L')
                                    THEN COALESCE(stake_units, 1.0)
                                    ELSE 0 END), 0) AS stake_units_settled
            FROM tennis_picks
            WHERE 1=1{tour_clause}""",
        tour_params,
    ).fetchone()
    tw = totals["wins"] or 0
    tl = totals["losses"] or 0
    settled = tw + tl
    tstaked = totals["stake_units_settled"] or 0

    # CLV across settled picks with closing_odds — same implied-prob
    # delta formula team sports use.
    clv_rows = conn.execute(
        f"""SELECT odds, closing_odds FROM tennis_picks
            WHERE result IS NOT NULL
              AND odds IS NOT NULL
              AND closing_odds IS NOT NULL{tour_clause}""",
        tour_params,
    ).fetchall()
    clv_values: list[float] = []
    for r in clv_rows:
        try:
            o, co = int(r["odds"]), int(r["closing_odds"])
        except (TypeError, ValueError):
            continue
        bet_imp = (abs(o) / (abs(o) + 100)) if o < 0 else (100 / (o + 100))
        cls_imp = (abs(co) / (abs(co) + 100)) if co < 0 else (100 / (co + 100))
        clv_values.append((cls_imp - bet_imp) * 100.0)
    avg_clv = round(sum(clv_values) / len(clv_values), 2) if clv_values else None

    overall = {
        "total": totals["total"] or 0,
        "wins": tw,
        "losses": tl,
        "pushes": totals["pushes"] or 0,
        "pending": totals["pending"] or 0,
        "profit": round(totals["profit"] or 0, 2),
        "win_pct": round(tw / settled * 100, 1) if settled else 0.0,
        "avg_clv": avg_clv,
        "clv_sample": len(clv_values),
    }

    overall["roi"] = round(overall["profit"] / tstaked, 1) if tstaked else 0.0
    return {
        "overall": overall,
        "by_type": by_type,
        # Legacy flat fields — kept so older readers (probation banner,
        # original tracker) don't break. Mirrors `overall` plus ROI.
        "total": overall["total"],
        "wins": tw,
        "losses": tl,
        "pushes": totals["pushes"] or 0,
        "profit": overall["profit"],
        "win_pct": overall["win_pct"],
        "roi": overall["roi"],
    }


# ── Helpers ────────────────────────────────────────────────────

def _int_or_none(v) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


__all__ = [
    "ensure_table", "record_tennis_pick", "settle_tennis_picks",
    "list_history", "get_pick_summary", "is_on_probation",
    "PROBATION_MIN_DECIDED", "PROBATION_BRIER_CEILING",
]
