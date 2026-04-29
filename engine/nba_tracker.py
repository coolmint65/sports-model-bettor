"""
NBA Q1 Pick Tracker -- records model picks and settles them against results.

Settles Q1 bets using the home_q1 and away_q1 columns from nba_games.
Bet types: Q1_SPREAD, Q1_TOTAL, Q1_ML

Usage:
    python -m engine.nba_tracker --record     # Record today's picks
    python -m engine.nba_tracker --settle     # Settle completed picks
    python -m engine.nba_tracker --summary    # Print running totals
"""

import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Phase 1 Q1 derivatives — excluded from main NBA tracker recording.
# Routed through engine.derivative_tracker for paper-bet evaluation.
_NBA_DERIVATIVE_TYPES: set[str] = {"Q1 Team Total", "Q1 Total O/E"}


def _core_picks(picks: list[dict]) -> list[dict]:
    return [p for p in picks if p.get("type") not in _NBA_DERIVATIVE_TYPES]


def _compute_clv(bet_odds, closing_odds):
    """Compute closing line value.
    Positive CLV = got better price than closing line = sharp.
    """
    if not bet_odds or not closing_odds:
        return None
    bet_implied = abs(bet_odds) / (abs(bet_odds) + 100) if bet_odds < 0 else 100 / (bet_odds + 100)
    close_implied = abs(closing_odds) / (abs(closing_odds) + 100) if closing_odds < 0 else 100 / (closing_odds + 100)
    return round((close_implied - bet_implied) * 100, 2)  # positive = we got a better price


def _extract_nba_closing_for_pick(bet_type: str, pick_text: str,
                                  home_abbr: str, game_odds: dict) -> int | None:
    """Pure helper: pick the right Q1 closing-odds field for an NBA pick.

    Mirrors engine.tracker._extract_closing_for_pick but for the Q1
    markets that nba_tracker records (Q1_ML / Q1_SPREAD / Q1_TOTAL).
    Hard Rock exposes q1_home_ml / q1_away_ml / q1_spread_*_odds /
    q1_over_odds / q1_under_odds on the per-matchup bucket.
    """
    if not game_odds:
        return None
    pk = pick_text or ""
    parts = pk.split()
    if bet_type == "Q1_ML":
        # "LAL Q1 ML" → first token is team abbr
        if not parts:
            return None
        pick_team = parts[0]
        is_home = pick_team == home_abbr or pick_team == _ALT_ABBRS.get(home_abbr, "")
        return game_odds.get("q1_home_ml") if is_home else game_odds.get("q1_away_ml")
    if bet_type == "Q1_SPREAD":
        # "LAL -2.5 Q1"
        if len(parts) < 2:
            return None
        pick_team = parts[0]
        is_home = pick_team == home_abbr or pick_team == _ALT_ABBRS.get(home_abbr, "")
        return (game_odds.get("q1_spread_home_odds") if is_home
                else game_odds.get("q1_spread_away_odds"))
    if bet_type == "Q1_TOTAL":
        # "Over 55.5 Q1" / "Under 55.5 Q1"
        if not parts:
            return None
        return (game_odds.get("q1_over_odds") if parts[0].lower() == "over"
                else game_odds.get("q1_under_odds"))
    return None


def capture_closing_odds() -> int:
    """Snapshot current Hard Rock NBA Q1 odds for all pending picks.

    Call before games tip off (sync script ~10 min pre-tip) so the
    CLV computed at settle time reflects the true closing line. The
    inline capture inside settle_picks() can't see Q1 lines because
    HR drops the Q1 markets the moment Q1 ends, leaving CLV null.

    Returns number of picks updated.
    """
    from .nba_db import get_conn
    conn = get_conn()
    pending = conn.execute(
        "SELECT id, matchup, bet_type, pick FROM nba_picks "
        "WHERE result IS NULL AND closing_odds IS NULL"
    ).fetchall()
    if not pending:
        return 0

    try:
        from scrapers.hardrock_odds import fetch_nba as _hr_nba
        all_odds = _hr_nba() or {}
    except Exception as e:
        logger.warning("NBA closing capture: HR fetch failed: %s", e)
        return 0
    if not all_odds:
        return 0

    from .picks import match_odds as _match_odds
    updated = 0
    for pick in pending:
        matchup = pick["matchup"]
        sep = " @ " if " @ " in matchup else "@"
        parts = matchup.split(sep)
        if len(parts) != 2:
            continue
        away, home = parts[0].strip(), parts[1].strip()
        game_odds = _match_odds(home, away, all_odds)
        if not game_odds:
            continue
        closing = _extract_nba_closing_for_pick(
            pick["bet_type"], pick["pick"], home, game_odds,
        )
        if closing is not None:
            conn.execute(
                "UPDATE nba_picks SET closing_odds = ? WHERE id = ?",
                (int(closing), pick["id"]),
            )
            updated += 1

    conn.commit()
    logger.info("NBA closing capture: %d/%d pending picks updated", updated, len(pending))
    return updated


# ESPN alternate abbreviation map (ESPN sometimes uses different abbrs)
_ALT_ABBRS = {
    "GS": "GSW", "GSW": "GS",
    "SA": "SAS", "SAS": "SA",
    "NO": "NOP", "NOP": "NO",
    "NY": "NYK", "NYK": "NY",
    "PHO": "PHX", "PHX": "PHO",
    "UTAH": "UTA", "UTA": "UTAH",
    "WSH": "WAS", "WAS": "WSH",
    "BKN": "BK", "BK": "BKN",
    "CHA": "CHO", "CHO": "CHA",
}


def _fetch_nba_scoreboard(date: str) -> list[dict]:
    """Fetch NBA scoreboard from ESPN for a given date (YYYY-MM-DD)."""
    espn_date = date.replace("-", "")
    url = (
        "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
        f"/scoreboard?dates={espn_date}"
    )
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("events", [])
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to fetch NBA scoreboard for %s: %s", date, e)
        return []


def _parse_q1_scores(event: dict) -> dict | None:
    """Parse Q1 scores and metadata from an ESPN event.

    Returns dict with home_abbr, away_abbr, home_q1, away_q1, etc.
    Returns None when Q1 isn't LOCKED yet (game in progress, still in
    Q1 with seconds left — score can still change). Q1 is locked when
    either:
      - state == "post" (game complete), OR
      - state == "in" AND current period > 1 (Q2+ has tipped off)

    Previously this only checked `state == "pre"` and returned partial
    mid-Q1 scores, which prematurely settled Q1 picks (e.g. Under 53.5
    settled W at 24-18 with 5:00 left in Q1, before any more points
    could be scored).
    """
    comp = event.get("competitions", [{}])[0]
    status = comp.get("status", {})
    status_type = status.get("type", {})
    state = status_type.get("state", "pre")
    cur_period = status.get("period") or 0

    if state == "pre":
        return None  # game hasn't started
    # Q1 locked = game over OR current period advanced past 1
    q1_locked = (state == "post") or (state == "in" and cur_period > 1)
    if not q1_locked:
        return None

    # Distinguish "Q1 locked but game still going" from "game final".
    # Full-game pickers settle only when state == "post"; Q1 settles
    # the moment Q1 ends.
    is_completed = (state == "post")
    result = {"game_id": event.get("id", ""), "is_completed": is_completed}

    for team_entry in comp.get("competitors", []):
        team = team_entry.get("team", {})
        abbr = team.get("abbreviation", "")
        is_home = team_entry.get("homeAway") == "home"
        score = 0
        raw_score = team_entry.get("score", "0")
        if isinstance(raw_score, (int, str)) and str(raw_score).isdigit():
            score = int(raw_score)

        # Parse linescores for Q1
        linescores = team_entry.get("linescores", [])
        q1 = None
        if linescores:
            val = linescores[0].get("value")
            if val is not None:
                q1 = int(val)

        if is_home:
            result["home_abbr"] = abbr
            result["home_score"] = score
            result["home_q1"] = q1
        else:
            result["away_abbr"] = abbr
            result["away_score"] = score
            result["away_q1"] = q1

    if result.get("home_q1") is None or result.get("away_q1") is None:
        return None

    result["q1_total"] = result["home_q1"] + result["away_q1"]
    result["q1_margin"] = result["home_q1"] - result["away_q1"]  # positive = home won Q1

    return result


# ESPN scoreboard abbreviations that don't match the Odds API / internal
# abbrs used elsewhere. Extend this when a new mismatch shows up.
_ESPN_TO_INTERNAL_ABBR = {
    "GS": "GSW",     # Golden State Warriors
    # ESPN sometimes uses these too:
    "NOP": "NO", "NYK": "NY", "SAS": "SA", "UTA": "UTAH", "WAS": "WSH",
}


def _normalize_espn_abbr(abbr: str) -> str:
    """Map an ESPN scoreboard team abbreviation to the internal form used
    by nba_odds / nba_db / nba_picks."""
    return _ESPN_TO_INTERNAL_ABBR.get(abbr, abbr)


def refresh_pending_for_today(bets: list[dict],
                               target_date: str | None = None) -> dict:
    """NBA twin of engine.tracker.refresh_pending_for_today. See that
    docstring for the design rationale."""
    from .nba_db import get_conn as _conn
    target_date = target_date or datetime.now().strftime("%Y-%m-%d")
    conn = _conn()
    # Track locked matchups separately so the void path leaves their
    # pending picks alone — see engine.tracker for the bug history.
    current_by_matchup: dict[str, dict] = {}
    locked_matchups: set[str] = set()
    # Defense: also derive lock from the bets entry's own start-time
    # field. ``is_locked`` is precomputed elsewhere and has been seen
    # to return False on games clearly underway (NBA BOS@PHI today
    # silently swapped Under 54.5 → Q1_SPREAD BOS -2.5 Q1 at 22:54
    # ET, hours after tip-off). Treating any past-tip game as locked
    # at this point is the right semantic regardless of the upstream
    # flag's reliability.
    from datetime import datetime as _dt, timezone as _tz
    now_utc = _dt.now(_tz.utc)
    def _bet_started(bet: dict) -> bool:
        if bet.get("is_locked"):
            return True
        t = bet.get("time") or bet.get("date") or ""
        if not isinstance(t, str) or not t:
            return False
        try:
            s = t.replace("Z", "+00:00") if t.endswith("Z") else t
            ts = _dt.fromisoformat(s)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=_tz.utc)
            return ts < now_utc
        except (ValueError, TypeError):
            return False

    # Phase 2k: track current pick PER (matchup, bet_type family). Q1
    # and Full are distinct bets; the legacy single-best_pick channel
    # was morphing Q1_TOTAL rows into TOTAL rows on every refresh.
    # current_by_key[(matchup, family)] = pick_dict
    current_by_key: dict[tuple, dict] = {}
    Q1_TYPES = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
    FULL_TYPES = {"ML", "SPREAD", "TOTAL", "ALT SPREAD", "ALT TOTAL"}
    def _family(bt: str) -> str:
        if bt in Q1_TYPES:   return "q1"
        if bt in FULL_TYPES: return "full"
        return "other"

    for b in bets:
        if _bet_started(b):
            locked_matchups.add(b["matchup"])
            continue  # locked: tracker entry stays frozen
        bq = b.get("best_pick_q1")
        bf = b.get("best_pick_full")
        if bq:
            current_by_key[(b["matchup"], "q1")] = bq
            current_by_matchup[b["matchup"]] = bq  # legacy fallback
        if bf:
            current_by_key[(b["matchup"], "full")] = bf
            if not bq:
                current_by_matchup[b["matchup"]] = bf
        # Pre-2k bets without per-view picks fall back to legacy best_pick.
        if not bq and not bf:
            bp = b.get("best_pick")
            if bp:
                fam = _family(bp.get("type") or "")
                current_by_key[(b["matchup"], fam)] = bp
                current_by_matchup[b["matchup"]] = bp

    pending = conn.execute(
        "SELECT id, matchup, bet_type, pick, game_id FROM nba_picks "
        "WHERE date = ? AND result IS NULL",
        (target_date,),
    ).fetchall()

    # Belt-and-suspenders DB-level lock check — see engine.tracker.
    def _pick_game_started(game_id) -> bool:
        if not game_id:
            return False
        row = conn.execute(
            "SELECT date, status FROM nba_games WHERE game_id = ? LIMIT 1",
            (str(game_id),),
        ).fetchone()
        if not row:
            return False
        if row["status"] in ("live", "final", "postponed"):
            return True
        try:
            return str(row["date"]) < target_date
        except Exception:
            return False

    updated = swapped = voided = 0
    for p in pending:
        p = dict(p)
        if p["matchup"] in locked_matchups:
            continue  # frozen at lock time
        if _pick_game_started(p.get("game_id")):
            continue  # game underway per DB; freeze regardless of bets dict
        # Match this pending row to the SAME family's current best pick.
        # Q1_TOTAL never gets morphed into TOTAL — they're distinct bets.
        fam = _family(p["bet_type"] or "")
        current = current_by_key.get((p["matchup"], fam))
        if not current:
            # Family no longer has a pick (e.g. Q1 fell below floor).
            # Void the row so the tracker doesn't carry a phantom bet.
            matchup_in_response = any(b["matchup"] == p["matchup"] for b in bets)
            if matchup_in_response:
                conn.execute("DELETE FROM nba_picks WHERE id = ?", (p["id"],))
                voided += 1
            continue

        if current.get("type") != p["bet_type"] or current.get("pick") != p["pick"]:
            conn.execute(
                "UPDATE nba_picks SET bet_type = ?, pick = ?, model_prob = ?, "
                "edge = ?, odds = ? WHERE id = ?",
                (current.get("type"), current.get("pick"),
                 current.get("prob"), current.get("edge"),
                 current.get("odds"), p["id"]),
            )
            swapped += 1
        else:
            conn.execute(
                "UPDATE nba_picks SET model_prob = ?, edge = ?, odds = ? "
                "WHERE id = ?",
                (current.get("prob"), current.get("edge"),
                 current.get("odds"), p["id"]),
            )
            updated += 1

    conn.commit()
    return {"updated": updated, "swapped": swapped, "voided": voided}


def record_picks(date: str | None = None, min_edge: float = 1.5,
                 force: bool = False) -> list[dict]:
    """Run NBA Q1 model on today's games and record the best pick per game.

    Args:
        date: Target date (YYYY-MM-DD). Defaults to today.
        min_edge: Minimum edge percentage to record a pick.
        force: If True, delete any existing pick for each game before
            recording so the latest model/odds take precedence. Use this
            when the model or odds have materially changed during the day
            (e.g. starter-rest news lands after the first sync).

    Returns:
        List of recorded pick dicts.
    """
    from .nba_db import get_conn

    conn = get_conn()
    target_date = date or datetime.now().strftime("%Y-%m-%d")

    from .nba_q1_predict import generate_q1_picks

    # Fetch today's games from ESPN
    events = _fetch_nba_scoreboard(target_date)
    if not events:
        logger.info("No NBA games found for %s", target_date)
        return []

    # Pull odds — Hard Rock first (has Q1 markets), then fallback chain.
    q1_odds_map = {}
    try:
        from scrapers.hardrock_odds import fetch_nba as _hr_nba
        q1_odds_map = _hr_nba()
        if q1_odds_map:
            logger.info("NBA tracker: %d games from Hard Rock", len(q1_odds_map))
    except Exception as e:
        logger.debug("NBA tracker Hard Rock failed: %s", e)
    if not q1_odds_map:
        try:
            from scrapers.nba_odds import fetch_all_nba_odds
            q1_odds_map = fetch_all_nba_odds()
        except Exception as e:
            logger.debug("NBA Q1 odds fallback failed: %s", e)

    recorded = []

    for event in events:
        game_id = event.get("id", "")
        comp = event.get("competitions", [{}])[0]

        # Skip completed games
        status = comp.get("status", {}).get("type", {})
        if status.get("completed", False):
            continue

        competitors = comp.get("competitors", [])
        h_abbr = ""
        a_abbr = ""
        for c in competitors:
            team = c.get("team", {})
            abbr = team.get("abbreviation", "")
            # Normalize ESPN scoreboard abbrs to internal form so lookups
            # in the odds map (keyed by internal abbrs like GSW/NO/NY) hit.
            abbr = _normalize_espn_abbr(abbr)
            if c.get("homeAway") == "home":
                h_abbr = abbr
            else:
                a_abbr = abbr

        if not h_abbr or not a_abbr:
            continue

        matchup = f"{a_abbr} @ {h_abbr}"

        # Duplicate handling:
        #   force=False: skip games that already have a pick recorded
        #   force=True:  delete existing picks for this game so the new
        #                model/odds take effect.
        if force:
            conn.execute("DELETE FROM nba_picks WHERE game_id = ? "
                         "AND result IS NULL", (game_id,))
        else:
            existing = conn.execute(
                "SELECT COUNT(*) as c FROM nba_picks WHERE game_id = ?",
                (game_id,)
            ).fetchone()["c"]
            if existing > 0:
                continue

        # Read from shared picks store (same picks the card shows).
        # Only fall back to generating if store is empty.
        picks = None
        try:
            from backend.server import _picks_store_get
            stored = _picks_store_get("nba", h_abbr, a_abbr)
            if stored and stored.get("picks"):
                picks = stored["picks"]
        except Exception:
            pass

        if not picks:
            from engine.picks import match_odds as _match_odds
            odds_dict = _match_odds(h_abbr, a_abbr, q1_odds_map)
            picks = generate_q1_picks(h_abbr, a_abbr, odds_dict)
        if not picks:
            continue

        # Filter out derivatives — they go to engine.derivative_tracker
        # via the /api/best-bets recorder, not the main tracker.
        core = _core_picks(picks)
        if not core:
            continue
        from .nba_picks import _valid_odds as _nba_valid

        # Split into Q1 markets (existing) and full-game markets (Phase 2k).
        # Record the highest-edge pick per market family per game so the
        # tracker captures both layers of betting opportunity. Full
        # uses primary-market preference: ML/SPREAD/TOTAL clearing 6%
        # wins over an ALT line, even if the ALT has a bigger edge —
        # ALT lines at +60% edge are the calibration trap player props
        # got burned by, and we shouldn't seed them into the tracker
        # without backtest validation.
        Q1_TYPES = {"Q1_ML", "Q1_SPREAD", "Q1_TOTAL"}
        FULL_PRIMARY = {"ML", "SPREAD", "TOTAL"}
        FULL_ALT = {"ALT SPREAD", "ALT TOTAL"}

        q1_picks = [p for p in core if p.get("type") in Q1_TYPES]
        full_primary = [p for p in core if p.get("type") in FULL_PRIMARY]
        full_alt = [p for p in core if p.get("type") in FULL_ALT]

        # Primary-only on the tracker. ALT lines stay generated for
        # the picks list (visible inside the bets payload) but don't
        # become recorded picks until backtest validates them — same
        # caution that pulled player-prop ALT bets after they bled
        # money live.
        full_picks = full_primary

        for family_picks, label in ((q1_picks, "Q1"), (full_picks, "Full")):
            if not family_picks:
                continue
            best = max(family_picks, key=lambda p: p.get("edge", 0))
            if best["edge"] < min_edge:
                continue
            if not _nba_valid(best.get("odds")):
                logger.warning("Skipping NBA %s pick with invalid odds=%s for %s",
                               label, best.get("odds"), matchup)
                continue
            # INSERT OR IGNORE against the unique index on
            # (date, game_id, bet_type) for pending rows, so re-runs
            # don't pile up duplicate rows. If the bet_type is already
            # tracked for this game today, refresh_pending_for_today
            # keeps it fresh.
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO nba_picks (
                        game_id, date, matchup, bet_type, pick,
                        model_prob, edge, odds
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (game_id, target_date, matchup, best["type"], best["pick"],
                      best["prob"], best["edge"], best["odds"]))
            except Exception as e:
                logger.warning("nba_picks insert failed for %s/%s: %s",
                               matchup, best["type"], e)
                continue
            recorded.append({
                "matchup": matchup, "type": best["type"],
                "pick": best["pick"], "prob": round(best["prob"], 3),
                "edge": round(best["edge"], 1), "odds": best["odds"],
            })

    conn.commit()
    return recorded


def settle_picks() -> dict:
    """Settle all pending NBA Q1 picks against final game results.

    Uses Q1 scores from ESPN scoreboard to determine outcomes.
    Handles Q1_SPREAD, Q1_TOTAL, and Q1_ML bet types.
    """
    from .nba_db import get_conn
    from datetime import datetime, timedelta

    conn = get_conn()

    # Self-heal: auto-push picks pending >7 days. Same rationale as
    # MLB / NHL settlers.
    stale_cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    stale = conn.execute(
        "SELECT id, date, matchup, bet_type, pick FROM nba_picks "
        "WHERE result IS NULL AND date < ?", (stale_cutoff,),
    ).fetchall()
    for row in stale:
        conn.execute(
            "UPDATE nba_picks SET result='P', profit=0, settled_at=datetime('now') "
            "WHERE id=?", (row["id"],),
        )
        logger.warning(
            "NBA settle: auto-pushed stale pending id=%s date=%s "
            "%s %s/%s — older than 7 days",
            row["id"], row["date"], row["matchup"], row["bet_type"], row["pick"],
        )
    if stale:
        conn.commit()

    pending = conn.execute(
        "SELECT * FROM nba_picks WHERE result IS NULL"
    ).fetchall()

    if not pending:
        return {"settled": 0, "message": "No pending NBA picks"}

    # Group by date to fetch scoreboards efficiently
    dates = set()
    for p in pending:
        dates.add(p["date"])

    # Fetch final Q1 scores for each date
    final_q1: dict[str, dict] = {}  # game_id -> q1 scores dict
    for d in dates:
        events = _fetch_nba_scoreboard(d)
        for event in events:
            q1_data = _parse_q1_scores(event)
            if q1_data:
                final_q1[q1_data["game_id"]] = q1_data

    # Best-effort capture for any picks that didn't get closing odds
    # stamped pre-game by capture_closing_odds(). HR Q1 markets often
    # vanish post-game, so this rarely fires — the pre-game capture is
    # the real source. Done as a no-op if HR returns nothing.
    try:
        from scrapers.hardrock_odds import fetch_nba as _hr_nba
        hr_nba_now = _hr_nba() or {}
    except Exception as e:
        logger.debug("NBA settle-time HR fetch failed: %s", e)
        hr_nba_now = {}

    settled = 0
    wins = 0
    losses = 0

    for pick in pending:
        pick = dict(pick)
        game_id = pick["game_id"]

        game = final_q1.get(game_id)
        if not game:
            continue

        h_q1 = game["home_q1"]
        a_q1 = game["away_q1"]
        q1_total = game["q1_total"]
        q1_margin = game["q1_margin"]  # positive = home won Q1
        h = game["home_abbr"]
        a = game["away_abbr"]

        # Settle-time fallback capture — only fires if pre-game
        # capture_closing_odds() didn't get a chance to run.
        if not pick.get("closing_odds") and h and a and hr_nba_now:
            from .picks import match_odds as _match_odds
            game_cl_odds = _match_odds(h, a, hr_nba_now)
            if game_cl_odds:
                closing = _extract_nba_closing_for_pick(
                    pick["bet_type"], pick["pick"], h, game_cl_odds,
                )
                if closing is not None:
                    conn.execute(
                        "UPDATE nba_picks SET closing_odds = ? WHERE id = ?",
                        (int(closing), pick["id"]),
                    )
                    pick["closing_odds"] = int(closing)

        bt = pick["bet_type"]
        pk = pick["pick"]
        odds = pick["odds"] or -110
        result = None

        if bt == "Q1_ML":
            # Pick format: "LAL Q1 ML" -- first token is team abbreviation
            pick_team = pk.split()[0]
            home_won_q1 = q1_margin > 0

            # Check both direct and alternate abbreviations
            is_home_pick = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))
            is_away_pick = (pick_team == a or pick_team == _ALT_ABBRS.get(a, ""))

            if is_home_pick:
                if home_won_q1:
                    result = "W"
                elif q1_margin == 0:
                    result = "P"
                else:
                    result = "L"
            elif is_away_pick:
                if not home_won_q1 and q1_margin != 0:
                    result = "W"
                elif q1_margin == 0:
                    result = "P"
                else:
                    result = "L"

        elif bt == "Q1_SPREAD":
            # Pick format: "LAL -2.5 Q1" or "BOS +2.5 Q1"
            parts = pk.split()
            if len(parts) >= 2:
                pick_team = parts[0]
                spread = float(parts[1])

                is_home_pick = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))

                if is_home_pick:
                    actual_margin = h_q1 - a_q1
                else:
                    actual_margin = a_q1 - h_q1

                covered = actual_margin + spread
                if covered > 0:
                    result = "W"
                elif covered == 0:
                    result = "P"
                else:
                    result = "L"

        elif bt == "Q1_TOTAL":
            # Pick format: "Over 55.5 Q1" or "Under 55.5 Q1"
            parts = pk.split()
            if len(parts) >= 2:
                direction = parts[0].lower()
                line = float(parts[1])

                if direction == "over":
                    if q1_total > line:
                        result = "W"
                    elif q1_total < line:
                        result = "L"
                    else:
                        result = "P"
                else:  # under
                    if q1_total < line:
                        result = "W"
                    elif q1_total > line:
                        result = "L"
                    else:
                        result = "P"

        # ── Phase 1 derivative markets ──
        elif bt == "Q1 Team Total":
            # "DEN Q1 Over 28.5" / "MIN Q1 Under 27.5"
            parts = pk.split()
            if len(parts) >= 4:
                pick_team = parts[0]
                direction = parts[2].lower()
                try:
                    line = float(parts[3])
                except ValueError:
                    continue
                is_home_pick = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))
                team_q1 = h_q1 if is_home_pick else a_q1
                if team_q1 > line:
                    result = "W" if direction == "over" else "L"
                elif team_q1 < line:
                    result = "L" if direction == "over" else "W"
                else:
                    result = "P"

        elif bt == "Q1 Total O/E":
            # "Q1 Total Odd" / "Q1 Total Even"
            parts = pk.split()
            if len(parts) >= 3:
                direction = parts[2].lower()
                is_odd = (q1_total % 2 == 1)
                if direction == "odd":
                    result = "W" if is_odd else "L"
                else:
                    result = "W" if not is_odd else "L"

        # ── Phase 2k: full-game markets ──
        # Only settle when the game is fully complete; partial scores
        # mid-Q2/Q3/Q4 would mis-settle Over/Under and ML.
        elif bt in ("ML", "SPREAD", "TOTAL", "ALT SPREAD", "ALT TOTAL"):
            if not game.get("is_completed"):
                continue  # game not final yet, leave PEND

            home_score = game["home_score"]
            away_score = game["away_score"]
            full_margin = home_score - away_score
            full_total = home_score + away_score

            if bt == "ML":
                # "LAL ML" — first token is team abbr
                pick_team = pk.split()[0]
                home_won = full_margin > 0
                is_home_pick = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))
                is_away_pick = (pick_team == a or pick_team == _ALT_ABBRS.get(a, ""))
                if is_home_pick:
                    result = "W" if home_won else ("P" if full_margin == 0 else "L")
                elif is_away_pick:
                    result = "W" if (not home_won and full_margin != 0) else ("P" if full_margin == 0 else "L")

            elif bt in ("SPREAD", "ALT SPREAD"):
                # "LAL -2.5" or "BOS +5.5"
                parts = pk.split()
                if len(parts) >= 2:
                    pick_team = parts[0]
                    try:
                        spread = float(parts[1])
                    except ValueError:
                        continue
                    is_home_pick = (pick_team == h or pick_team == _ALT_ABBRS.get(h, ""))
                    actual = full_margin if is_home_pick else -full_margin
                    covered = actual + spread
                    if covered > 0:
                        result = "W"
                    elif covered == 0:
                        result = "P"
                    else:
                        result = "L"

            elif bt in ("TOTAL", "ALT TOTAL"):
                # "Over 224.5" or "Under 224.5"
                parts = pk.split()
                if len(parts) >= 2:
                    direction = parts[0].lower()
                    try:
                        line = float(parts[1])
                    except ValueError:
                        continue
                    if direction == "over":
                        result = "W" if full_total > line else ("P" if full_total == line else "L")
                    else:
                        result = "W" if full_total < line else ("P" if full_total == line else "L")

        if result is None:
            continue

        # Calculate profit (based on $100 unit)
        if result == "W":
            if odds > 0:
                profit = float(odds)
            else:
                profit = 100 / abs(odds) * 100
            wins += 1
        elif result == "L":
            profit = -100.0
            losses += 1
        else:  # Push
            profit = 0.0

        conn.execute("""
            UPDATE nba_picks SET result = ?, profit = ?, settled_at = datetime('now')
            WHERE id = ?
        """, (result, round(profit, 2), pick["id"]))
        settled += 1

    conn.commit()

    return {
        "settled": settled,
        "wins": wins,
        "losses": losses,
        "pending_remaining": conn.execute(
            "SELECT COUNT(*) as c FROM nba_picks WHERE result IS NULL"
        ).fetchone()["c"],
    }


def get_pick_summary() -> dict:
    """Get running totals across all NBA Q1 picks."""
    from .nba_db import get_conn

    conn = get_conn()

    summary = {}
    for bt in ["Q1_SPREAD", "Q1_TOTAL", "Q1_ML",
               "ML", "SPREAD", "TOTAL", "ALT SPREAD", "ALT TOTAL"]:
        row = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'P' THEN 1 ELSE 0 END) as pushes,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
                COALESCE(SUM(profit), 0) as profit
            FROM nba_picks WHERE bet_type = ?
        """, (bt,)).fetchone()

        total = row["total"] or 0
        w = row["wins"] or 0
        l = row["losses"] or 0
        settled_count = w + l
        summary[bt] = {
            "total": total,
            "wins": w,
            "losses": l,
            "pushes": row["pushes"] or 0,
            "pending": row["pending"] or 0,
            "profit": round(row["profit"], 2),
            "win_pct": round(w / settled_count * 100, 1) if settled_count > 0 else 0,
            "roi": round(row["profit"] / settled_count, 1) if settled_count > 0 else 0,
        }

    recent = conn.execute("""
        SELECT * FROM nba_picks ORDER BY created_at DESC LIMIT 30
    """).fetchall()

    totals = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending,
            COALESCE(SUM(profit), 0) as profit
        FROM nba_picks
    """).fetchone()

    tw = totals["wins"] or 0
    tl = totals["losses"] or 0

    # Compute CLV across all settled picks that have closing odds
    clv_rows = conn.execute("""
        SELECT odds, closing_odds FROM nba_picks
        WHERE result IS NOT NULL AND odds IS NOT NULL AND closing_odds IS NOT NULL
    """).fetchall()
    clv_values = []
    for r in clv_rows:
        clv = _compute_clv(r["odds"], r["closing_odds"])
        if clv is not None:
            clv_values.append(clv)
    avg_clv = round(sum(clv_values) / len(clv_values), 2) if clv_values else None

    return {
        "by_type": summary,
        "overall": {
            "total": totals["total"] or 0,
            "wins": tw,
            "losses": tl,
            "pending": totals["pending"] or 0,
            "profit": round(totals["profit"] or 0, 2),
            "win_pct": round(tw / (tw + tl) * 100, 1) if (tw + tl) > 0 else 0,
            "avg_clv": avg_clv,
            "clv_sample": len(clv_values),
        },
        "recent": [dict(r) for r in recent],
    }


# ── Public API (nba_-prefixed aliases used by backend/server.py) ──
# The backend routes import sport-prefixed names by convention so the
# endpoints read clearly at the call site. Module-internal names stayed
# short; expose the prefixed aliases here so /api/nba/tracker/* actually
# resolves the functions instead of hitting ImportError and silently
# returning "module not loaded yet".

record_nba_picks = record_picks
settle_nba_picks = settle_picks
get_nba_pick_summary = get_pick_summary


def get_nba_pick_history(limit: int = 30) -> list[dict]:
    """Return the most recent NBA picks for the tracker history tab.

    Mirrors get_nba_pick_summary() but peels off just the `recent` list
    so the /api/nba/tracker/history endpoint can stream a flat array.
    """
    summary = get_pick_summary() or {}
    recent = summary.get("recent") or []
    return recent[:limit]


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = set(sys.argv[1:])

    if "--record" in args:
        force = "--force" in args
        print(f"Recording today's NBA Q1 picks{' (force refresh)' if force else ''}...",
              flush=True)
        picks = record_picks(force=force)
        print(f"Recorded {len(picks)} NBA Q1 picks:")
        for p in picks:
            print(f"  {p['matchup']} | {p['type']:12s} | {p['pick']:20s} | "
                  f"{p['prob']:.1%} | edge: {p['edge']:+.1f}%")

    elif "--capture-closing" in args:
        print("Capturing NBA Q1 closing odds...", flush=True)
        n = capture_closing_odds()
        print(f"Updated {n} pending picks with closing odds.")

    elif "--settle" in args:
        print("Settling completed NBA Q1 picks...", flush=True)
        result = settle_picks()
        print(f"Settled: {result.get('settled', 0)} "
              f"({result.get('wins', 0)}W-{result.get('losses', 0)}L)")
        print(f"Pending: {result.get('pending_remaining', 0)}")
        if result.get("message"):
            print(f"  {result['message']}")

    elif "--summary" in args:
        summary = get_pick_summary()
        overall = summary["overall"]
        print(f"\n{'='*55}")
        print(f"  NBA Q1 PICK TRACKER -- Running Totals")
        print(f"{'='*55}")
        print(f"  Total picks: {overall['total']}")
        print(f"  Record: {overall['wins']}-{overall['losses']} ({overall['win_pct']}%)")
        print(f"  Profit: ${overall['profit']:+.2f}")
        print(f"  Pending: {overall['pending']}")
        if overall.get("avg_clv") is not None:
            print(f"  Avg CLV: {overall['avg_clv']:+.2f}% ({overall['clv_sample']} picks)")
        print()
        for bt, label in [("Q1_SPREAD", "Q1 Spread"), ("Q1_TOTAL", "Q1 Total"),
                          ("Q1_ML", "Q1 Moneyline")]:
            s = summary["by_type"][bt]
            if s["total"] == 0:
                continue
            print(f"  {label:14s}: {s['wins']}-{s['losses']} "
                  f"({s['win_pct']}%) ${s['profit']:+.2f} "
                  f"[ROI: {s['roi']:+.1f}]")
        print(f"{'='*55}")

        # Show recent picks
        recent = summary.get("recent", [])
        if recent:
            print(f"\n  Recent picks (last {len(recent)}):")
            for p in recent[:10]:
                result_str = p.get("result") or "PEND"
                profit_str = f"${p['profit']:+.0f}" if p.get("profit") is not None else ""
                print(f"    {p['date']} | {p['matchup']:12s} | {p['bet_type']:12s} | "
                      f"{p['pick']:20s} | {result_str:4s} {profit_str}")

    else:
        print("Usage: python -m engine.nba_tracker --record | --settle | --summary")
