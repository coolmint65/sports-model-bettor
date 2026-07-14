"""Soccer pick tracker + settler.

Records picks emitted by ``generate_picks_for_slate`` into the per-
league picks table. Idempotent on (date, match_id, bet_type, pick)
via the uq_picks_open unique index.

Settlement rules:

    ML       — pick text 'Home' → result by home_score > away_score
                home/away tags ship as team abbr; 'Draw' is its own pick.
    OU       — pick text 'Over 2.5' → result by (h + a) vs line.
    BTTS     — Yes wins iff h > 0 AND a > 0.
    DNB      — Refund (P) on draw; W/L otherwise.
    DC       — Two-cell coverage; W if either cell hits.
    AH       — Asian handicap with half-line; W if (h - a) + line > 0
                (home side) or (a - h) + line > 0 (away side).
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

from ._db import get_conn

logger = logging.getLogger(__name__)


def _payout(odds: int, won: bool) -> float:
    if not won:
        return -100.0
    if odds > 0:
        return float(odds)
    if odds < 0:
        return round(100.0 * 100.0 / abs(odds), 2)
    return 0.0


def record_picks(league: str, match_id: int, picks: Iterable[dict]) -> dict:
    """Persist one pick per scope-family for ``match_id``. Family logic
    lives in the shared ``engine._dedup_helpers`` module so soccer
    enforces dedup the same way every other sport does (full vs H1
    families, line-variant + name-format matching collapsed by family).
    """
    from .._dedup_helpers import (
        enforce_one_per_game_per_family, is_settled_dup,
    )
    conn = get_conn(league)
    # Pick.date must align with match.date so the slate query buckets
    # the pick under the right calendar day. Previously used utcnow()
    # which mis-buckets late-night ET picks (Copa Libertadores
    # 2026-05-26 8:30 PM ET kickoff stamped 2026-05-27 because the
    # recorder ran at 03:51 UTC = 5/27 UTC). Match.date is already
    # league-local; copy it so card slate alignment is preserved.
    match_row = conn.execute(
        "SELECT date FROM matches WHERE id = ?", (int(match_id),)
    ).fetchone()
    pick_date = (match_row["date"] if match_row and match_row["date"]
                  else datetime.utcnow().strftime("%Y-%m-%d"))
    picks_list = list(picks or [])
    # Lock-on-record: drop family members from the incoming batch whose
    # family already has a pending row for this match. The previous
    # behavior voided the existing row and inserted the new one — but
    # the model often changes its mind between slate-ticks (AH NED -0.5
    # → ML NED) and the dedup helper would void a higher-edge earlier
    # pick in favor of a lower-edge later pick. User flagged the void
    # pattern on WC 2026-06-22. Lock the first family head so the
    # tracker reflects what we'd actually have bet at lock time.
    from .._dedup_helpers import default_family_for
    existing_families = {
        default_family_for(r["bet_type"])
        for r in conn.execute(
            "SELECT bet_type FROM picks "
            "WHERE match_id=? AND result IS NULL",
            (int(match_id),),
        ).fetchall()
    }
    picks_list = [
        p for p in picks_list
        if default_family_for(p.get("type") or "") not in existing_families
    ]
    # New families still get the standard family-dedup against any
    # superseded picks WITHIN the same batch (rare — usually only
    # one head per family per call anyway).
    enforce_one_per_game_per_family(
        conn, table="picks", game_id=str(int(match_id)), date=pick_date,
        picks=picks_list, game_id_col="match_id",
    )
    out = {"recorded": 0, "duplicate": 0, "errors": 0, "blocked_settled": 0}
    for p in picks_list:
        if is_settled_dup(conn, table="picks", game_id=str(int(match_id)),
                            bet_type=p.get("type") or "",
                            game_id_col="match_id"):
            out["blocked_settled"] += 1
            continue
        try:
            conn.execute(
                "INSERT INTO picks "
                "(date, match_id, matchup, bet_type, pick, side, line, "
                " model_prob, edge, odds, stake_units) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (pick_date, int(match_id), p.get("matchup") or "",
                  p.get("type"), p.get("pick"),
                  p.get("side"), p.get("line"),
                  float(p.get("prob") or 0.0),
                  float(p.get("edge") or 0.0),
                  int(p.get("odds") or 0),
                  p.get("stake_units")),
            )
            out["recorded"] += 1
            # Mirror to unified picks layer (best-effort during cutover).
            try:
                from ..picks_unified._legacy_bridge import mirror_to_unified
                mirror_to_unified(
                    sport="soccer", league=league,
                    native_game_id=int(match_id),
                    pick_date=pick_date,
                    matchup=p.get("matchup") or "",
                    bet_type=p.get("type") or "",
                    pick_text=p.get("pick") or "",
                    odds=int(p.get("odds") or 0),
                    prob=float(p.get("prob") or 0.0),
                    edge_pct=float(p.get("edge") or 0.0),
                    stake_units=float(p.get("stake_units") or 0.0),
                    side=p.get("side"),
                    line=p.get("line"),
                    confidence=p.get("confidence"),
                    is_shadow=bool(p.get("shadow")),
                )
            except Exception:
                pass
        except Exception as e:
            if "UNIQUE" in str(e).upper():
                out["duplicate"] += 1
            else:
                logger.warning("[soccer:%s] record pick failed: %s",
                                league, e)
                out["errors"] += 1
    conn.commit()
    return out


def _resolve_result(row: dict, match: dict) -> tuple[str | None, float | None]:
    """Return (result, profit) for one pick row against a final match.

    H1_* bet types resolve against the HT (half-time) scores rather
    than the full-time scores. Strip the ``H1_`` prefix and swap in
    the HT line scores so the rest of the resolver logic is unchanged.
    """
    bt = (row.get("bet_type") or "").upper()
    is_h1 = bt.startswith("H1_")
    if is_h1:
        h = match.get("home_score_ht")
        a = match.get("away_score_ht")
        bt = bt[3:]  # drop the H1_ prefix
        # H1_TOTAL gets recorded with bet_type prefix "H1_TOTAL" but
        # the picker emits full-game over/unders as "OU". After the
        # prefix strip, "TOTAL" needs to map to "OU" so the resolver
        # branch matches. Same shape conversion for any other H1_*
        # bet types whose prefix-stripped name doesn't match the
        # full-game label.
        if bt == "TOTAL":
            bt = "OU"
    else:
        h = match.get("home_score")
        a = match.get("away_score")
    if h is None or a is None:
        return None, None
    h, a = int(h), int(a)
    side = (row.get("side") or "").lower()
    pick_text = (row.get("pick") or "").strip()
    odds = int(row.get("odds") or 0)
    line = row.get("line")

    def W(): return ("W", _payout(odds, True))
    def L(): return ("L", _payout(odds, False))
    def P(): return ("P", 0.0)

    if bt == "ML":
        if side == "home" or pick_text == match.get("home_abbr"):
            return W() if h > a else L()
        if side == "away" or pick_text == match.get("away_abbr"):
            return W() if a > h else L()
        if side == "draw" or pick_text.lower() == "draw":
            return W() if h == a else L()
        return None, None
    if bt == "OU" and line is not None:
        total = h + a
        line_f = float(line)
        if side == "over":
            if total > line_f: return W()
            if total < line_f: return L()
            return P()
        if side == "under":
            if total < line_f: return W()
            if total > line_f: return L()
            return P()
        return None, None
    if bt == "BTTS":
        both = (h > 0 and a > 0)
        if side == "yes":
            return W() if both else L()
        if side == "no":
            return L() if both else W()
        return None, None
    if bt == "ADVANCE":
        # Knockout progression. Precedence: shootout > ET > regulation.
        # Falls through to (None, None) when the match record hasn't
        # yet been enriched with ET/PEN scores for a KO game that
        # went past regulation — the ESPN summary backfill covers HT
        # today, ET/PEN population is a follow-up.
        hp, ap = match.get("home_pens"), match.get("away_pens")
        het, aet = match.get("home_score_et"), match.get("away_score_et")
        if hp is not None and ap is not None:
            home_advanced = int(hp) > int(ap)
        elif het is not None and aet is not None:
            if int(het) == int(aet):
                # ET tied but no shootout data yet — defer.
                return None, None
            home_advanced = int(het) > int(aet)
        else:
            # Regulation-only decision. A tied regulation game in KO
            # means ET/PEN happened but we lack the data; defer.
            if h == a:
                return None, None
            home_advanced = h > a
        if side == "home":
            return W() if home_advanced else L()
        if side == "away":
            return L() if home_advanced else W()
        return None, None
    if bt == "GIBH":
        # Goal in Both Halves requires HT scores. On live rows without
        # HT set, defer resolution.
        ht_h = match.get("home_score_ht")
        ht_a = match.get("away_score_ht")
        if ht_h is None or ht_a is None:
            return None, None
        h1_total = int(ht_h) + int(ht_a)
        h2_total = (h - int(ht_h)) + (a - int(ht_a))
        both_halves = (h1_total >= 1 and h2_total >= 1)
        if side == "yes":
            return W() if both_halves else L()
        if side == "no":
            return L() if both_halves else W()
        return None, None
    if bt == "DNB":
        if h == a:
            return P()
        if side == "home":
            return W() if h > a else L()
        if side == "away":
            return W() if a > h else L()
        return None, None
    if bt == "DC":
        # side = "home_draw" / "draw_away" / "home_away"
        if side == "home_draw":
            return W() if (h > a or h == a) else L()
        if side == "draw_away":
            return W() if (a > h or h == a) else L()
        if side == "home_away":
            return L() if h == a else W()
        return None, None
    if bt == "AH" and line is not None:
        line_f = float(line)
        # side carries 'home' / 'away' direction
        if side == "home":
            adj = (h - a) + line_f
        elif side == "away":
            adj = (a - h) + line_f
        else:
            return None, None
        if adj > 0:    return W()
        if adj < 0:    return L()
        return P()
    return None, None


def settle_picks(league: str) -> dict:
    """Walk pending picks; grade those whose match is final.

    Belt-and-suspenders for ESPN-stale match rows: if ``status='live'``
    but both scores are set AND the match date is >1 day past, treat
    it as final. ESPN occasionally drops the FT status update; without
    this clause, Libertadores 2-1 picks from two nights ago sat
    pending forever.
    """
    from datetime import date as _date, timedelta as _td
    conn = get_conn(league)
    # HT backfill before settling. ESPN's scoreboard endpoint sometimes
    # drops `home_score_ht` on finals — only the per-game summary
    # endpoint carries it. Without this preflight, H1_* picks against
    # those matches got auto-voided (MLS 2026-05-23/24 lost a full
    # weekend of H1 picks this way). The backfill is cheap (only finals
    # with NULL HT scores) and idempotent.
    try:
        from ._ht_backfill import backfill as _ht_backfill
        _ht_backfill(league, limit=50)
    except Exception as e:
        logger.debug("[soccer:%s] HT backfill skipped: %s", league, e)
    # Self-heal: auto-push picks pending >7 days. Same threshold MLB /
    # NBA / NHL use — if a match never gets graded after a week, the
    # match was likely cancelled, abandoned, or has a non-ESPN status
    # detail the parser doesn't recognize. Push to break the stuck loop
    # rather than have rows sit pending forever and pollute the tracker.
    from datetime import datetime as _dt, timedelta as _td2
    stale_cutoff = (_dt.now() - _td2(days=7)).strftime("%Y-%m-%d")
    auto_pushed = 0
    for row in conn.execute(
        "SELECT id, date, bet_type, pick FROM picks "
        "WHERE result IS NULL AND date < ?", (stale_cutoff,),
    ).fetchall():
        conn.execute(
            "UPDATE picks SET result='P', profit=0, "
            "  settled_at=datetime('now') WHERE id=?", (row["id"],),
        )
        logger.warning(
            "[soccer:%s] settle: auto-pushed stale pending id=%s "
            "date=%s %s/%s — older than 7 days",
            league, row["id"], row["date"], row["bet_type"], row["pick"],
        )
        auto_pushed += 1
    if auto_pushed:
        conn.commit()
    pending = conn.execute(
        "SELECT id, date, bet_type, pick, side, line, odds, match_id "
        "FROM picks WHERE result IS NULL"
    ).fetchall()
    out = {"checked": len(pending), "settled": 0,
           "wins": 0, "losses": 0, "pushes": 0}
    if not pending:
        return out
    today = _date.today()
    match_cache: dict[int, dict] = {}
    for r in pending:
        mid = int(r["match_id"])
        if mid not in match_cache:
            mrow = conn.execute(
                "SELECT m.id, m.status, m.date, m.home_score, m.away_score, "
                "       m.home_score_ht, m.away_score_ht, "
                "       m.home_score_et, m.away_score_et, "
                "       m.home_pens, m.away_pens, "
                "       m.has_extra_time, m.has_shootout, "
                "       ht.abbreviation AS home_abbr, at.abbreviation AS away_abbr "
                "FROM matches m "
                "JOIN teams ht ON ht.id = m.home_team_id "
                "JOIN teams at ON at.id = m.away_team_id "
                "WHERE m.id = ?", (mid,)
            ).fetchone()
            match_cache[mid] = dict(mrow) if mrow else {}
        match = match_cache[mid]
        status = (match.get("status") or "")
        # Cancelled matches grade as void (no action). Without this,
        # a pick whose match was cancelled by the league sits pending
        # forever — the MLS 2026-05-17 LAFC@NSH AH pick hit this case.
        if status == "cancelled":
            conn.execute(
                "UPDATE picks SET result='V', profit=0, settled_at=? "
                "WHERE id=?",
                (datetime.utcnow().isoformat(), r["id"]),
            )
            out["settled"] += 1
            out["pushes"] += 1
            continue
        if status != "final":
            # Stale-live promotion: live row, both scores set, date past
            # the 1-day buffer → grade with available scores.
            if status == "live" and match.get("home_score") is not None \
                    and match.get("away_score") is not None \
                    and match.get("date"):
                try:
                    md = _date.fromisoformat(match["date"])
                    if (today - md) >= _td(days=1):
                        match["status"] = "final"
                except ValueError:
                    continue
            if (match.get("status") or "") != "final":
                continue
        # H1 picks need halftime scores. If status='final' but HT scores
        # are NULL (ESPN sometimes drops HT detail), void instead of
        # letting the pick sit pending forever. Same logic basketball
        # uses for Q1 picks when home_q1 is missing on a final game.
        bt_upper = (r["bet_type"] or "").upper()
        if bt_upper.startswith("H1_") and (
                match.get("home_score_ht") is None
                or match.get("away_score_ht") is None):
            conn.execute(
                "UPDATE picks SET result='V', profit=0, settled_at=? "
                "WHERE id=?",
                (datetime.utcnow().isoformat(), r["id"]),
            )
            out["settled"] += 1
            out["pushes"] += 1
            continue
        verdict, profit = _resolve_result(dict(r), match)
        if verdict is None:
            continue
        conn.execute(
            "UPDATE picks SET result = ?, profit = ?, settled_at = ? "
            "WHERE id = ?",
            (verdict, profit, datetime.utcnow().isoformat(), r["id"]),
        )
        out["settled"] += 1
        if verdict == "W":   out["wins"] += 1
        elif verdict == "L": out["losses"] += 1
        else:                 out["pushes"] += 1
    conn.commit()
    if out["settled"]:
        logger.info("[soccer:%s] settled %d (W=%d L=%d P=%d)",
                    league, out["settled"], out["wins"],
                    out["losses"], out["pushes"])
    return out


def list_history(league: str, limit: int = 200) -> list[dict]:
    """Pending + settled picks for ``league`` joined with match context.
    Row shape matches the shared PicksTable primitive (matchup, pick,
    etc.) so the same UI primitive renders soccer alongside other
    sports."""
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT p.id, p.date, p.match_id, p.matchup, p.bet_type, p.pick, "
        "       p.side, p.line, p.model_prob, p.edge, p.odds, "
        "       p.closing_odds, p.result, p.profit, p.stake_units, "
        "       p.created_at, p.settled_at, "
        "       m.start_time, m.home_score, m.away_score, m.status "
        "FROM picks p "
        "JOIN matches m ON m.id = p.match_id "
        "ORDER BY p.created_at DESC LIMIT ?",
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]
