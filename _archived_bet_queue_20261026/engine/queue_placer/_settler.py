"""Grade placed queue-placer rows after the underlying event resolves.

Primary path: HR's own settlement via the relay's POST /history
(→ /mybets/closed on api.hardrocksportsbook.com). HR is authoritative
for won/lost/void/cashout — no ESPN score re-derivation, no missing
retirement-void edge cases.

Fallback: per-sport match-result graders (tennis wired). Used when a
placed ticket hasn't yet appeared in HR's /mybets/closed (HR's history
can lag placement by minutes to hours) OR when the relay is unreachable.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable

from ._schema import get_conn

logger = logging.getLogger(__name__)


def _american_profit(result: str, stake_dollars: float,
                      american_odds: int) -> float:
    if result == "W":
        if american_odds > 0:
            return stake_dollars * (american_odds / 100.0)
        return stake_dollars * (100.0 / abs(american_odds))
    if result == "L":
        return -stake_dollars
    return 0.0


# HR splits settle state across two fields:
#   betStatus       — coarse state: SETTLED (win or loss), VOID, PENDING.
#   displayStatus   — fine-grained outcome for settled bets: WIN, LOSE,
#                     PUSH, HALF_WIN, HALF_LOSE.
#
# Grading rule (from live payload inspection 2026-07-14):
#   betStatus=VOID     → V   (ignore displayStatus; refund is on the bet)
#   betStatus=SETTLED  → look at displayStatus → W/L/P
#   anything else      → None (pending / unknown → row stays open)
_HR_DISPLAY_MAP: dict[str, str] = {
    "WIN":         "W",
    "WON":         "W",
    "HALF_WIN":    "W",
    "LOSE":        "L",
    "LOST":        "L",
    "LOSS":        "L",
    "HALF_LOSE":   "L",
    "PUSH":        "P",
    "PUSHED":      "P",
    "CASHED_OUT":  "W",
    "CASHOUT":     "W",
}


def _read_status(bet: dict, *keys: str) -> str:
    """Case-insensitive fetch — returns UPPER + '_'-normalized string."""
    for k in keys:
        v = bet.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().upper().replace(" ", "_")
    return ""


def _read_float(bet: dict, *keys: str) -> float:
    """Fetch the first numeric-coercible field; supports both top-level
    numbers and {amount: N} nested shapes."""
    for k in keys:
        v = bet.get(k)
        if v is None:
            continue
        if isinstance(v, dict):
            v = v.get("amount")
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return 0.0


def _hr_extract_settle(bet: dict) -> tuple[str, float] | None:
    """From an HR bet object, extract (result, profit_dollars).

    Live payload shape observed 2026-07-14:
      WIN     — betStatus=SETTLED, displayStatus=WIN,   payout=<total>
      LOSS    — betStatus=SETTLED, displayStatus=LOSE
      VOID    — betStatus=VOID,    refund=<stake>
    """
    if not isinstance(bet, dict):
        return None

    bet_status = _read_status(bet, "betStatus", "status")
    if bet_status == "VOID":
        result = "V"
    elif bet_status == "SETTLED":
        display = _read_status(bet, "displayStatus", "outcome", "result")
        result = _HR_DISPLAY_MAP.get(display)
        if result is None:
            return None  # SETTLED with unrecognized displayStatus
    else:
        return None  # PENDING / unknown → leave row open

    stake = _read_float(bet, "stake", "stakeAmount", "risk", "wagerAmount")
    # HR ships payout as a top-level number (payout / totalPayout), not
    # a {amount} dict — _read_float handles both.
    payout = _read_float(bet, "payout", "totalPayout",
                          "winnings", "returnAmount", "netPayout",
                          "payoutAmount")
    refund = _read_float(bet, "refund", "refundAmount")

    if result == "W":
        # payout = stake + profit for winners. Fall through to 0 if HR
        # didn't ship a payout; the caller then uses the odds fallback.
        profit = (payout - stake) if payout > 0 else 0.0
    elif result == "L":
        profit = -stake
    elif result == "V":
        # HR refunds stake on voids — net profit is 0. If refund<stake
        # (partial void), signal by returning the actual delta.
        profit = (refund - stake) if refund > 0 else 0.0
    else:  # P (push)
        profit = 0.0
    return result, profit


def _hr_history_snapshot() -> dict[str, tuple[str, float]]:
    """Pull HR's settled-bets history and index by hr_ticket_id.

    Empty dict on any error — settler falls back to per-sport graders.
    """
    from . import _relay
    try:
        bets = _relay.history_closed(days_back=30)
    except Exception as e:
        logger.warning("HR history fetch crashed: %s", e)
        return {}
    snapshot: dict[str, tuple[str, float]] = {}
    for bet in bets:
        # Ticket id — HR field name observed variations: id, betId,
        # ticketId. Placer stores whatever HR returned in placeBets, so
        # we probe the same names on the readback side.
        ticket_id = None
        for k in ("id", "betId", "ticketId"):
            v = bet.get(k)
            if v is not None:
                ticket_id = str(v)
                break
        if not ticket_id:
            continue
        settled = _hr_extract_settle(bet)
        if settled is not None:
            snapshot[ticket_id] = settled
    return snapshot


def _refresh_tennis_te_window() -> None:
    """Pull the last 5 days of TennisExplorer results before fallback
    grading. Mirrors settle_tennis_picks()'s pre-flight so the fallback
    path works even if the tennis-picks cadence hasn't run recently.
    """
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


def _grade_tennis(row: dict) -> tuple[str, dict] | None:
    """Fallback grader: match a tennis placement to tennis_match_results
    and grade via the tennis tracker's existing resolver. Used only
    when HR's /mybets/closed doesn't yet carry the ticket.

    Returns (result, meta) on hit, None to leave the row pending.
    """
    from ..tennis_tracker import _match_from_results_table, _resolve_pick
    from ..tennis_db import get_conn as get_tennis_conn

    matchup = row.get("matchup") or ""
    if " vs " not in matchup:
        return None

    tennis_conn = get_tennis_conn()
    adapted_pick = {
        "matchup": matchup,
        "date": (row.get("queued_at") or "")[:10],
        "tour": None,
        "p1_id": 0,
        "p2_id": 0,
    }
    match = _match_from_results_table(tennis_conn, adapted_pick)
    if not match:
        return None

    pick_row = {
        "bet_type": row.get("bet_type"),
        "pick": row.get("pick"),
        "p1_id": 0,
        "p2_id": 0,
    }
    result = _resolve_pick(pick_row, match)
    if result is None:
        return None
    return result, {"score": match.get("score"),
                     "winner": match.get("winner_name")}


# ── WNBA fallback grader ────────────────────────────────────────────────
# DK WNBA legs carry no HR ticket (or a DK event id there, never an HR one),
# so they never hit the HR snapshot; and a Q1/full total's outcome is pure
# arithmetic on the box score. Grade both straight off ESPN's per-quarter
# linescores (book-agnostic → works for DK rows AND as an HR fallback if HR's
# history lags). Returns None until the relevant period is final, leaving the
# row pending. Validated 2026-08-04 vs LV@ATL (Q1 46), PHX@CHI (Q1 53).
_WNBA_ABBR_ALIASES = {
    "PHO": "PHX", "GSV": "GS", "LVA": "LV", "VEG": "LV",
    "NYL": "NY", "WSH": "WAS", "CONN": "CON", "GST": "GS",
}


def _wnba_norm(tok: Any) -> str:
    import re
    ab = re.sub(r"[^A-Za-z]", "", str(tok or "")).upper()
    return _WNBA_ABBR_ALIASES.get(ab, ab)


def _wnba_dates_to_try(qdate: str) -> list[str]:
    out: list[str] = []
    try:
        from datetime import timedelta
        base = datetime.strptime(str(qdate)[:10], "%Y-%m-%d")
        for off in (0, 1, -1):
            out.append((base + timedelta(days=off)).strftime("%Y%m%d"))
    except Exception:
        pass
    return out


def _espn_wnba_game(date_yyyymmdd: str, away_ab: str, home_ab: str) -> dict | None:
    import json
    import urllib.request
    url = ("https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/"
           "scoreboard?dates=" + date_yyyymmdd)
    req = urllib.request.Request(url, headers={"User-Agent": "curl/8.4.0"})
    try:
        d = json.load(urllib.request.urlopen(req, timeout=12))
    except Exception:
        return None
    for e in d.get("events", []):
        comp = (e.get("competitions") or [{}])[0]
        stype = (e.get("status") or {}).get("type", {})
        period = (e.get("status") or {}).get("period")
        teams: dict = {}
        for c in comp.get("competitors", []):
            teams[c.get("homeAway")] = {
                "ab": _wnba_norm((c.get("team") or {}).get("abbreviation")),
                "ls": [x.get("value") for x in (c.get("linescores") or [])],
                "score": c.get("score"),
            }
        a, h = teams.get("away", {}), teams.get("home", {})
        if a.get("ab") == away_ab and h.get("ab") == home_ab:
            q1 = None
            if a.get("ls") and h.get("ls"):
                try:
                    q1 = float(a["ls"][0]) + float(h["ls"][0])
                except Exception:
                    q1 = None
            ft = None
            try:
                ft = float(a["score"]) + float(h["score"])
            except Exception:
                ft = None
            return {"state": stype.get("state"), "period": period,
                    "q1_total": q1, "full_total": ft}
    return None


def _grade_wnba(row: dict) -> tuple[str, dict] | None:
    import re
    pick = str(row.get("pick") or "")
    bet_type = str(row.get("bet_type") or "").upper()
    matchup = str(row.get("matchup") or "")
    m = re.search(r"(over|under)\s+([0-9]+(?:\.[0-9]+)?)", pick, re.I)
    if not m:
        return None
    side, line = m.group(1).lower(), float(m.group(2))
    parts = re.split(r"\s*@\s*", matchup)
    if len(parts) != 2:
        return None
    away_ab, home_ab = _wnba_norm(parts[0]), _wnba_norm(parts[1])
    is_q1 = bet_type.startswith("Q1")
    game = None
    for dd in _wnba_dates_to_try(str(row.get("queued_at") or "")):
        game = _espn_wnba_game(dd, away_ab, home_ab)
        if game:
            break
    if not game:
        return None
    st, period = game["state"], (game["period"] or 0)
    if is_q1:
        if not (st == "post" or (st == "in" and period >= 2)):
            return None
        total = game["q1_total"]
    else:
        if st != "post":
            return None
        total = game["full_total"]
    if total is None:
        return None
    if abs(total - line) < 1e-9:
        result = "P"
    elif (total > line) == (side == "over"):
        result = "W"
    else:
        result = "L"
    return result, {"score": total, "line": line, "side": side,
                    "period": "Q1" if is_q1 else "FG"}


def _grade_tennis_from_tracker(row: dict) -> tuple[str, dict] | None:
    """Last-resort tennis grader: copy the settled result of the model
    tracker's OWN pick for the same match + bet_type + pick text
    (tennis_picks self-grades via the results pipeline). Rescues rows whose
    match aged out of the te-results window -- e.g. a DK placement stranded
    while the DK scrape was stuck (id 6584, 2026-07-21)."""
    from ..tennis_db import get_conn as get_tennis_conn
    mu = (row.get("matchup") or "").strip().lower()
    bt = (row.get("bet_type") or "").strip().upper()
    pk = (row.get("pick") or "").strip().lower()
    if not mu or not bt or not pk:
        return None
    tc = get_tennis_conn()
    for r in tc.execute(
            "SELECT result FROM tennis_picks WHERE lower(matchup) = ? "
            "AND upper(bet_type) = ? AND lower(pick) = ? "
            "AND result IN ('W','L','P','V') "
            "ORDER BY settled_at DESC LIMIT 1", (mu, bt, pk)):
        return (r[0], {"source": "tennis_tracker"})
    return None


_SPORT_GRADERS: dict[str, Callable[[dict], tuple[str, dict] | None]] = {
    "tennis": _grade_tennis,
    "wnba": _grade_wnba,
}


def _grade_kalshi(row: dict) -> tuple[str, float] | None:
    """Kalshi settlement: exact market result via the order id. Added
    2026-08-07: the first real Kalshi fill exposed that NO path graded
    kalshi rows (ML picks fall through _grade_wnba's totals-only regex,
    _kalshi.poll_result is a stub, HR snapshot never matches a UUID)."""
    from . import _kalshi
    oid = str(row.get("hr_ticket_id") or "")
    if not oid:
        return None
    st, resp = 0, None
    for path in ("/portfolio/orders/%s" % oid, "/portfolio/events/orders/%s" % oid):
        try:                                   # non-JSON bodies raise; try the next shape
            st, resp = _kalshi._authed("GET", path)
        except Exception:
            continue
        if st == 200 and ((resp or {}).get("order") or {}).get("ticker"):
            break
    ticker = (((resp or {}).get("order") or {}).get("ticker")) or ""
    if st != 200 or not ticker:
        return None
    try:
        mkt = (_kalshi._get("/markets/%s" % ticker) or {}).get("market") or {}
    except Exception:
        return None
    res = (mkt.get("result") or "").lower()
    if res not in ("yes", "no"):
        return None                      # market not settled yet
    bt = str(row.get("bet_type") or "").upper()
    pick = str(row.get("pick") or "").strip().lower()
    order = ((resp or {}).get("order")) or {}
    # Which contract we actually bought ("yes"/"no"). Prefer OUR recorded side (written to
    # hr_response_json at placement, authoritative for every bet placed after 2026-08-18), then
    # Kalshi's live order, and only THEN the pick heuristic. The old code went straight to the
    # heuristic, which DEFAULTS to "yes" for anything that is not an under-total -> a non-total
    # we bought as the NO contract graded backwards. (2026-08-18)
    import json as _json
    our = ""
    try:
        _stored = _json.loads(row.get("hr_response_json") or "{}")
        our = str((_stored or {}).get("outcome_side") or "").lower()
    except Exception:
        our = ""
    if our not in ("yes", "no"):
        our = (order.get("outcome_side") or order.get("side") or "").lower()
    if our not in ("yes", "no"):
        our = "no" if (bt in ("TOTAL", "Q1_TOTAL") and pick.startswith("under")) else "yes"
    won = (res == our)
    stake = float(row.get("placed_stake_d") or 0.0)
    dec = float(row.get("placed_odds") or 0.0)
    profit = round(stake * (dec - 1.0), 2) if (won and dec > 1) else (0.0 if won else round(-stake, 2))
    return ("W" if won else "L"), profit


def settle_placements(sport: str | None = None,
                       max_rows: int = 500) -> dict:
    """Grade pending placer placements.

    Tries HR's authoritative settlement first (via relay POST /history →
    /mybets/closed). Rows not yet in HR's history fall back to per-sport
    match-result graders where available.

    Returns per-sport summary dict.
    """
    conn = get_conn()
    where = ("status = 'placed' AND mode = 'live' AND result IS NULL "
             "AND (hr_ticket_id IS NOT NULL OR book = 'dk')")
    args: list[Any] = []
    if sport:
        where += " AND sport = ?"
        args.append(sport)
    pending = conn.execute(
        f"SELECT * FROM placements WHERE {where} "
        "ORDER BY queued_at ASC LIMIT ?",
        (*args, max_rows),
    ).fetchall()

    summary: dict[str, dict] = {}
    if not pending:
        return summary

    # 1) HR-native snapshot — one relay round trip covers every open
    #    ticket regardless of sport.
    hr_snapshot = _hr_history_snapshot()

    # 2) Fallback pre-flight (only if any tennis row is pending AND its
    #    ticket isn't in the HR snapshot).
    tennis_pending_fallback = any(
        (dict(r).get("sport") == "tennis"
         and str(dict(r).get("hr_ticket_id") or "") not in hr_snapshot)
        for r in pending
    )
    if tennis_pending_fallback:
        _refresh_tennis_te_window()

    now_iso = datetime.now(timezone.utc).isoformat()
    for row in pending:
        d = dict(row)
        s = d.get("sport") or ""
        bucket = summary.setdefault(s, {
            "settled_hr": 0, "settled_fallback": 0,
            "wins": 0, "losses": 0, "pushes": 0, "voids": 0,
            "pending_remaining": 0, "unsupported": 0,
        })
        stake = float(d.get("placed_stake_d")
                       or d.get("requested_stake_d") or 0.0)
        odds = int(d.get("placed_odds") or d.get("requested_odds") or 0)

        result: str | None = None
        profit: float | None = None
        source: str = ""

        # Prefer HR-native settlement (HR book only). A DK row's ticket
        # column holds a DK EVENT id, which must never be matched against
        # HR's ticket-indexed snapshot; DK grades via the book-agnostic
        # tennis fallback below (win-a-set outcome depends only on the match).
        ticket_id = str(d.get("hr_ticket_id") or "")
        if ticket_id and (d.get("book") or "hr") != "dk" and ticket_id in hr_snapshot:
            result, hr_profit = hr_snapshot[ticket_id]
            # If HR gave us a payout, use it; else compute from odds.
            profit = hr_profit if hr_profit else (
                _american_profit(result, stake, odds) if odds else 0.0
            )
            source = "hr"

        # Kalshi rows: grade off the market's own settlement (ticket is a
        # Kalshi order UUID - never in the HR snapshot; MLs have no per-sport
        # fallback). Unsettled markets just stay pending.
        if result is None and (d.get("book") or "") == "kalshi":
            gk = None
            try:
                gk = _grade_kalshi(d)
            except Exception as e:
                logger.warning("kalshi settle failed id=%s: %s", d.get("id"), e)
            if gk is None:
                bucket["pending_remaining"] += 1
                continue
            result, profit = gk
            source = "kalshi"

        # Fallback to per-sport grader.
        if result is None:
            grader = _SPORT_GRADERS.get(s)
            if grader is None:
                bucket["unsupported"] += 1
                continue
            try:
                graded = grader(d)
            except Exception as e:
                logger.warning(
                    "queue placer settle: grader crash id=%s sport=%s: %s",
                    d.get("id"), s, e,
                )
                bucket["pending_remaining"] += 1
                continue
            if graded is None and s == "tennis":
                # Aged-out-of-window rescue: the tracker's own settled pick.
                try:
                    graded = _grade_tennis_from_tracker(d)
                except Exception:
                    graded = None
            if graded is None:
                bucket["pending_remaining"] += 1
                continue
            result, _meta = graded
            profit = _american_profit(result, stake, odds) if odds else 0.0
            source = "fallback"

        conn.execute(
            "UPDATE placements SET result = ?, profit_dollars = ?, "
            "settled_at = ? WHERE id = ?",
            (result, profit, now_iso, d.get("id")),
        )
        if source == "hr":
            bucket["settled_hr"] += 1
        else:
            bucket["settled_fallback"] += 1
        if result == "W":
            bucket["wins"] += 1
        elif result == "L":
            bucket["losses"] += 1
        elif result == "P":
            bucket["pushes"] += 1
        elif result == "V":
            bucket["voids"] += 1
        logger.info(
            "queue placer settled id=%s %s %s %s @ %+d → %s "
            "(profit=$%.2f, source=%s)",
            d.get("id"), s, d.get("bet_type"), d.get("pick"),
            odds, result, profit or 0.0, source,
        )

    # Reconcile: HR's authoritative settlement OVERWRITES an earlier
    # fallback grade (a gap-window bet graded before HR settled, or a
    # retirement HR voided). Without this the `result IS NULL` filter
    # froze the first grade and the breakers/ROI trusted a stale
    # fallback (audit T1 6b). Bounded to the 500 most-recent HR rows.
    if hr_snapshot:
        recon_where = ("status='placed' AND mode='live' AND result IS NOT NULL "
                       "AND hr_ticket_id IS NOT NULL AND (book IS NULL OR book='hr')")
        recon_args: list = []
        if sport:
            recon_where += " AND sport = ?"
            recon_args.append(sport)
        recon_rows = conn.execute(
            f"SELECT * FROM placements WHERE {recon_where} "
            "ORDER BY settled_at DESC LIMIT 500", recon_args).fetchall()
        for row in recon_rows:
            d = dict(row)
            snap = hr_snapshot.get(str(d.get("hr_ticket_id") or ""))
            if not snap:
                continue
            hr_result, hr_profit = snap
            if hr_result and hr_result != d.get("result"):
                stake = float(d.get("placed_stake_d") or d.get("requested_stake_d") or 0.0)
                odds = int(d.get("placed_odds") or d.get("requested_odds") or 0)
                profit = hr_profit if hr_profit else (
                    _american_profit(hr_result, stake, odds) if odds else 0.0)
                conn.execute(
                    "UPDATE placements SET result=?, profit_dollars=?, settled_at=? WHERE id=?",
                    (hr_result, profit, now_iso, d.get("id")))
                logger.info("settle reconcile: HR overwrote %s->%s id=%s ticket=%s",
                            d.get("result"), hr_result, d.get("id"),
                            d.get("hr_ticket_id"))
    return summary
