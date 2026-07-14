"""Generic basketball tracker.

Records picks into the per-league ``picks`` table and settles them
against the league's ``games`` table. Idempotent on
(date, game_id, bet_type, pick) so re-recording the same pick returns
the existing id rather than duplicating.

Mirrors engine.nba_tracker for shape but reads/writes the
framework-managed leagues only. NBA stays on its existing tracker.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Iterable

from ._config import get_league_config
from ._db import get_conn, picks_table, games_table

logger = logging.getLogger(__name__)


def record_pick(league: str, pick: dict) -> int | None:
    """Insert a pick into the per-league picks table. Returns the new id,
    or the existing id if a matching pending row already exists.

    Required pick fields:
        date, game_id, matchup, bet_type, pick, odds, model_prob, edge
    """
    if league == "nba":
        # Don't intercept NBA — it has its own tracker with extra
        # columns (Q1 splits, derivative refs, etc.). Cutover happens
        # in #209.
        from ..nba_tracker import record_pick as _nba_record  # type: ignore
        return _nba_record(pick)

    cfg = get_league_config(league)
    if cfg.get("status") in ("pending_data", "offseason"):
        # Recorder must not raise for inactive leagues — pickers may
        # speculatively call us during boot. Silent skip.
        return None
    conn = get_conn(league)
    tbl = picks_table(league)

    # One pending pick per game (standard layout #2). When the headline
    # swaps (model picked Over earlier, now prefers SPREAD), update the
    # existing row in place instead of inserting another pending row —
    # otherwise Euroleague's MAD@HTA accumulated 4 pending picks across
    # market types for one matchup.
    #
    # Dedup hierarchy (most-to-least precise):
    #   1. game_id match — covers same-canonical-id repeat hits
    #   2. (date, matchup) match — covers stub→canonical game_id flip
    #      between two slate hits (HR fuzzy match resolved differently)
    #   3. (date, partial-matchup) match — covers abbreviation drift
    #      ("ZHE @ SHE" vs "ZHEC @ SHE") when fuzzy match upgraded the
    #      team abbreviation between hits.
    # When we find an older stub-id row and this hit has a canonical
    # game_id, we ALSO migrate the row's game_id to the canonical so
    # settle can match against the games table.
    gid = str(pick.get("game_id") or "")
    date = pick.get("date")
    matchup = pick.get("matchup") or ""
    incoming_bt = (pick.get("bet_type") or "").upper()
    # Market family — Q1 picks are independent of full-game picks so they
    # must dedupe within their own family, not against each other. Without
    # this, recording the Q1 pick for a game overwrites the previously
    # recorded full-game pick (AFL/WNBA Q1 tracker bug, 2026-05-17).
    incoming_family = "q1" if incoming_bt.startswith("Q1_") else "full"
    def _row_family(bt: str) -> str:
        return "q1" if (bt or "").upper().startswith("Q1_") else "full"
    existing = None
    if gid:
        for row in conn.execute(
            f"SELECT id, game_id, bet_type, pick FROM {tbl} "
            f"WHERE game_id = ? AND result IS NULL",
            (gid,),
        ).fetchall():
            if _row_family(row["bet_type"]) == incoming_family:
                existing = row
                break
    if not existing and date and matchup:
        for row in conn.execute(
            f"SELECT id, game_id, bet_type, pick FROM {tbl} "
            f"WHERE date = ? AND matchup = ? AND result IS NULL",
            (date, matchup),
        ).fetchall():
            if _row_family(row["bet_type"]) == incoming_family:
                existing = row
                break
    if not existing and date and " @ " in matchup:
        # Partial-matchup fallback: pull every pending pick on this
        # date, split into (away_part, home_part), and match when each
        # side is a prefix of (or prefixed by) ours. Catches abbrev
        # upgrades like "ZHE @ SHE" -> "ZHEC @ SHE" between hits.
        away_part, home_part = [s.strip() for s in matchup.split(" @ ", 1)]
        if away_part and home_part:
            candidates = conn.execute(
                f"SELECT id, game_id, bet_type, pick, matchup FROM {tbl} "
                f"WHERE date = ? AND result IS NULL",
                (date,),
            ).fetchall()
            for cand in candidates:
                cand_matchup = (cand["matchup"] or "")
                if " @ " not in cand_matchup:
                    continue
                c_away, c_home = [s.strip() for s in cand_matchup.split(" @ ", 1)]
                if not (c_away and c_home):
                    continue

                def _prefix_match(a: str, b: str) -> bool:
                    a_l, b_l = a.lower(), b.lower()
                    return a_l.startswith(b_l) or b_l.startswith(a_l)

                if (_prefix_match(away_part, c_away)
                        and _prefix_match(home_part, c_home)
                        and _row_family(cand["bet_type"]) == incoming_family):
                    existing = cand
                    break
    # Defense: if a settled pick already exists in the same family for
    # this (date, game_id) — regardless of exact pick text — this hit
    # is a duplicate of already-graded content. Pick TEXT changes when
    # the line moves (POR +10.5 → +14.5) or when the picker swaps the
    # team format ("POR" → "Portland Fire") between runs; tying the
    # dedup to exact text leaves stale settled picks unprotected.
    # User flagged 2026-05-26: WNBA POR @ NY 5/25 had 3 separate
    # settled SPREAD rows because each line/format variant slipped
    # past the prior exact-text guard.
    settled_dup = None
    if date and gid and pick.get("bet_type"):
        # Map bet_type to family (Q1 vs full vs spread/total/ml).
        bt_upper_dup = (pick.get("bet_type") or "").upper()
        if bt_upper_dup.startswith("Q1_"):
            fam_types = ("Q1_ML", "Q1_SPREAD", "Q1_TOTAL")
        elif bt_upper_dup == "ML":
            fam_types = ("ML",)
        elif bt_upper_dup in ("SPREAD",):
            fam_types = ("SPREAD",)
        elif bt_upper_dup in ("TOTAL",):
            fam_types = ("TOTAL",)
        else:
            fam_types = (pick.get("bet_type"),)
        ph = ','.join('?' * len(fam_types))
        settled_dup = conn.execute(
            f"SELECT id FROM {tbl} "
            f"WHERE date = ? AND game_id = ? "
            f"  AND bet_type IN ({ph}) "
            f"  AND result IN ('W', 'L', 'P') LIMIT 1",
            (date, gid, *fam_types),
        ).fetchone()
        # Fallback to (matchup, exact pick text) when game_id not yet
        # canonical (legacy HR-stub case).
        if not settled_dup and matchup:
            settled_dup = conn.execute(
                f"SELECT id FROM {tbl} "
                f"WHERE date = ? AND matchup = ? "
                f"  AND bet_type = ? AND pick = ? "
                f"  AND result IN ('W', 'L', 'P') LIMIT 1",
                (date, matchup, pick.get("bet_type"), pick.get("pick")),
            ).fetchone()
    if settled_dup:
        return int(settled_dup["id"])

    if existing:
        # Prefer canonical id when this hit upgraded from stub.
        old_gid = str(existing["game_id"] or "")
        new_gid = gid if (gid and not gid.startswith("hr-")) else old_gid
        same_pick = (existing["bet_type"] == pick.get("bet_type")
                     and existing["pick"] == pick.get("pick")
                     and old_gid == new_gid)
        if same_pick:
            return int(existing["id"])
        # Sync the pick's date to the latest hit. WNBA ATL@DAL on
        # 2026-05-11 sat with date=5/11 because the original game-row
        # date was 5/11 (later corrected to 5/12 in the games table);
        # the picks row was never refreshed since the prior UPDATE
        # omitted `date`. Updating it here keeps pick.date aligned with
        # game.date so the slate query stops surfacing stale-date picks.
        conn.execute(
            f"UPDATE {tbl} SET date = ?, game_id = ?, matchup = ?, "
            f"  bet_type = ?, pick = ?, "
            f"  model_prob = ?, edge = ?, odds = ? "
            f"WHERE id = ?",
            (date or None,
             new_gid, matchup or None,
             pick.get("bet_type"), pick.get("pick"),
             pick.get("model_prob"), pick.get("edge"), pick.get("odds"),
             int(existing["id"])),
        )
        conn.commit()
        return int(existing["id"])

    # Defense: skip the insert when the supplied game_id doesn't
    # resolve to a row in games. Orphan picks (legacy hash references
    # that survive a games-table cleanup) accumulate as ghost-pending
    # entries that can never settle. Empty/HR-stub game_ids are
    # allowed through — HR-stub picks intentionally live without a
    # canonical games row until ingest backfills them.
    g_tbl = games_table(league)
    if gid and not gid.startswith("hr-"):
        game_row = conn.execute(
            f"SELECT 1 FROM {g_tbl} WHERE game_id = ? LIMIT 1", (gid,)
        ).fetchone()
        if not game_row:
            logger.warning(
                "[%s] record_pick: refusing orphan insert — game_id=%s "
                "has no row in games (date=%s, matchup=%s, pick=%s)",
                league, gid, date, matchup, pick.get("pick"),
            )
            return None

    try:
        cur = conn.execute(
            f"INSERT INTO {tbl} ("
            f"  date, game_id, matchup, bet_type, pick, "
            f"  model_prob, edge, odds, stake_units"
            f") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                pick.get("date"),
                str(pick.get("game_id") or ""),
                pick.get("matchup"),
                pick.get("bet_type"),
                pick.get("pick"),
                pick.get("model_prob"),
                pick.get("edge"),
                pick.get("odds"),
                pick.get("stake_units"),
            ),
        )
    except sqlite3.IntegrityError:
        # Unique-index hit (idx_picks_unique). A concurrent record_pick
        # call won the race. Fetch the winning row's id and return it
        # so the caller's flow continues exactly as if we'd inserted.
        winner = conn.execute(
            f"SELECT id FROM {tbl} "
            f"WHERE date = ? AND game_id = ? "
            f"  AND bet_type = ? AND pick = ? AND result IS NULL",
            (pick.get("date"), str(pick.get("game_id") or ""),
             pick.get("bet_type"), pick.get("pick")),
        ).fetchone()
        if winner:
            return int(winner["id"])
        return None
    conn.commit()
    new_id = int(cur.lastrowid) if cur.lastrowid else None

    # Phase-2 cutover: dual-write into picks_unified (canonical store).
    if new_id:
        try:
            from ..picks_unified._legacy_bridge import mirror_to_unified
            mirror_to_unified(
                sport="basketball", league=league,
                native_game_id=pick.get("game_id"),
                pick_date=pick.get("date") or "",
                matchup=pick.get("matchup") or "",
                bet_type=pick.get("bet_type") or "",
                pick_text=pick.get("pick") or "",
                odds=int(pick.get("odds") or 0),
                prob=float(pick.get("model_prob") or 0.0),
                edge_pct=float(pick.get("edge") or 0.0),
                stake_units=float(pick.get("stake_units") or 0.0),
            )
        except Exception as e:
            logger.debug("picks_unified mirror (basketball/%s) skipped: %s",
                         league, e)
    return new_id


def settle_picks(league: str) -> dict:
    """Walk pending picks for ``league`` and settle any whose game is
    now final. Returns ``{checked, settled, skipped}``.

    Settle logic:
        - ML / SPREAD / TOTAL run against home_score / away_score from
          the games table.
        - Push (refund) when the line lands exactly on the spread/total.
    """
    if league == "nba":
        from ..nba_tracker import settle_picks as _nba_settle  # type: ignore
        return _nba_settle()

    conn = get_conn(league)
    p_tbl = picks_table(league)
    g_tbl = games_table(league)

    # Self-heal: auto-push picks pending >7 days. Same threshold the
    # core team-sport settlers use. Without this, RealGM-stale games
    # whose status never flips from 'scheduled' would keep their picks
    # pending forever (we saw this with the 5/17 batch the user flagged).
    from datetime import datetime as _dt2, timedelta as _td2
    stale_cutoff = (_dt2.now() - _td2(days=7)).strftime("%Y-%m-%d")
    auto_pushed = 0
    for row in conn.execute(
        f"SELECT id, date, bet_type, pick FROM {p_tbl} "
        f"WHERE result IS NULL AND date < ?", (stale_cutoff,),
    ).fetchall():
        conn.execute(
            f"UPDATE {p_tbl} SET result='P', profit=0, "
            f"  settled_at=datetime('now') WHERE id=?", (row["id"],),
        )
        logger.warning(
            "[basketball:%s] settle: auto-pushed stale pending id=%s "
            "date=%s %s/%s — older than 7 days",
            league, row["id"], row["date"], row["bet_type"], row["pick"],
        )
        auto_pushed += 1
    if auto_pushed:
        conn.commit()

    # Orphan-stub reconciler. Pick was recorded with a game_id that
    # doesn't match any row in the games table — happens when the
    # picks recorder used an HR matchup id before the games table got
    # the real row, or after the games table was re-stamped with new
    # ids. Without this, every pre-reconcile pick sat as 'pending'
    # forever after its game date passed (user flagged 2026-05-17).
    _reconcile_orphan_picks(conn, league)

    # Stale auto-void. Any pending pick whose date passed >7 days ago
    # AND still has no matching game row (post-reconcile) is structurally
    # ungradable — HR shipped an event that never landed in our games
    # table, or the matchup string is malformed enough that the
    # reconciler can't bridge to the canonical row. Voiding keeps the
    # tracker honest instead of accumulating phantom pending forever.
    # Same 7-day rule the NHL/MLB settlers use.
    from datetime import datetime as _dt, timedelta as _td
    stale_cutoff = (_dt.now() - _td(days=7)).strftime("%Y-%m-%d")
    stale = conn.execute(
        f"SELECT p.id FROM {p_tbl} p "
        f"LEFT JOIN {g_tbl} g ON g.game_id = p.game_id "
        f"WHERE p.result IS NULL AND p.date < ? "
        f"  AND g.game_id IS NULL",
        (stale_cutoff,),
    ).fetchall()
    for s in stale:
        conn.execute(
            f"UPDATE {p_tbl} SET result='V', profit=0, "
            f"  settled_at=? WHERE id=?",
            (datetime.now().isoformat(), s["id"]),
        )
    if stale:
        conn.commit()
        logger.warning("[%s] auto-voided %d stale orphan picks (date <%s)",
                       league, len(stale), stale_cutoff)

    # Stuck-in-progress auto-void. ESPN sometimes leaves a game's
    # status='in' indefinitely (the FT update never lands). Score we
    # captured is a mid-game snapshot, not the final. Void picks
    # whose game is still status='in' but the date is >2 days past —
    # we can't grade them correctly. WNBA CON @ POR 2026-05-18
    # surfaced this 3 days later with score 64-64 + status='in'.
    stuck_cutoff = (_dt.now() - _td(days=2)).strftime("%Y-%m-%d")
    stuck_in = conn.execute(
        f"SELECT p.id FROM {p_tbl} p "
        f"JOIN {g_tbl} g ON g.game_id = p.game_id "
        f"WHERE p.result IS NULL AND p.date < ? "
        f"  AND g.status = 'in'",
        (stuck_cutoff,),
    ).fetchall()
    for s in stuck_in:
        conn.execute(
            f"UPDATE {p_tbl} SET result='V', profit=0, "
            f"  settled_at=? WHERE id=?",
            (datetime.now().isoformat(), s["id"]),
        )
    if stuck_in:
        conn.commit()
        logger.warning("[%s] auto-voided %d stuck-in-progress picks (date <%s)",
                       league, len(stuck_in), stuck_cutoff)

    pending = conn.execute(
        f"SELECT * FROM {p_tbl} WHERE result IS NULL"
    ).fetchall()
    out = {"checked": len(pending), "settled": 0, "skipped": 0}
    if not pending:
        return out

    from ._db import teams_table
    t_tbl = teams_table(league)
    for row in pending:
        game = conn.execute(
            f"SELECT g.home_score, g.away_score, g.status, "
            f"  g.home_q1, g.away_q1, "
            f"  ht.abbreviation AS h_abbr, ht.name AS h_name, "
            f"  at.abbreviation AS a_abbr, at.name AS a_name "
            f"FROM {g_tbl} g "
            f"LEFT JOIN {t_tbl} ht ON ht.id = g.home_team_id "
            f"LEFT JOIN {t_tbl} at ON at.id = g.away_team_id "
            f"WHERE g.game_id = ?",
            (row["game_id"],),
        ).fetchone()
        if not game:
            out["skipped"] += 1
            continue
        # Cancelled games — void the pick. Without this any pending
        # pick whose game was called off sits forever (Argentina
        # 2026-05-24 BOC@QSDE was the canonical case). Result='V'
        # with profit=0 is the standard refund convention.
        if (game["status"] or "").lower() in ("cancelled", "canceled",
                                                "postponed"):
            conn.execute(
                f"UPDATE {p_tbl} SET result='V', profit=0, "
                f"  settled_at=? WHERE id=?",
                (datetime.now().isoformat(), row["id"]),
            )
            out["settled"] += 1
            continue
        bt_upper = (row["bet_type"] or "").upper()
        is_q1_bet = bt_upper in ("Q1_ML", "Q1_SPREAD", "Q1_TOTAL")
        # Q1 picks settle as soon as Q1 ends — don't wait for the full
        # game to finish. Same shape as NBA's live tracker: scope-aware
        # readiness check. Full-game picks still require status=final.
        home_q1 = game["home_q1"] if "home_q1" in game.keys() else None
        away_q1 = game["away_q1"] if "away_q1" in game.keys() else None
        if is_q1_bet:
            if home_q1 is None or away_q1 is None:
                # Q1 splits absent. If the full game is FINAL and we
                # still don't have Q1 scores, the upstream source
                # (RealGM for several leagues) is never going to ship
                # them. Auto-void the pick rather than leave it stuck
                # forever — a 'V' (push) refund is the right resolve
                # when we made a pick we can't grade.
                if game["status"] == "final":
                    conn.execute(
                        f"UPDATE {p_tbl} SET result='V', profit=0, "
                        f"  settled_at=? WHERE id=?",
                        (datetime.now().isoformat(), row["id"]),
                    )
                    out["settled"] += 1
                    continue
                out["skipped"] += 1
                continue
        else:
            if (game["status"] != "final"
                    or game["home_score"] is None
                    or game["away_score"] is None):
                out["skipped"] += 1
                continue
        home_s = int(game["home_score"]) if game["home_score"] is not None else 0
        away_s = int(game["away_score"]) if game["away_score"] is not None else 0
        # Pass both abbreviations and full names — pick text may carry
        # either ("ATL -2.5" from the framework's record_pick, or
        # "Atlanta Dream -2.5" from legacy POTD mirror rows). Both
        # need to settle.
        verdict = _verdict(row["bet_type"], row["pick"], row["matchup"],
                            home_s, away_s,
                            home_abbr=game["h_abbr"], home_name=game["h_name"],
                            away_abbr=game["a_abbr"], away_name=game["a_name"],
                            home_q1=int(home_q1) if home_q1 is not None else None,
                            away_q1=int(away_q1) if away_q1 is not None else None)
        if verdict is None:
            out["skipped"] += 1
            continue
        result, profit = _settle_with_odds(verdict, int(row["odds"] or 0))
        conn.execute(
            f"UPDATE {p_tbl} SET result=?, profit=?, settled_at=? "
            f"WHERE id=?",
            (result, profit, datetime.now().isoformat(), row["id"]),
        )
        out["settled"] += 1
    conn.commit()
    return out


# ── Per-bet-type settlement ──────────────────────────────────

def _verdict(bet_type: str, pick: str, matchup: str | None,
              home_s: int, away_s: int,
              *,
              home_abbr: str | None = None, home_name: str | None = None,
              away_abbr: str | None = None, away_name: str | None = None,
              home_q1: int | None = None, away_q1: int | None = None,
              ) -> str | None:
    """Return 'W' / 'L' / 'P' (push) / None (unsettled / unknown bet
    shape). matchup is the canonical "<away> @ <home>" string used to
    identify which side ML picks land on; None disables the check.

    The optional ``*_abbr`` / ``*_name`` kwargs cover the case where the
    pick text uses the full team name ("Atlanta Dream -2.5") rather
    than the abbreviation. POTD mirror rows from before 2026-05-11
    have the full-name format, and ML/SPREAD parsing failed silently
    because "Atlanta Dream" doesn't contain "ATL". With name+abbr
    matching against both sides, those legacy rows settle correctly.
    """
    bt = (bet_type or "").upper()
    pk = pick or ""

    def _side_matches(pk_token: str, abbr: str | None, name: str | None) -> bool:
        """True iff ``pk_token`` references this side. Case-insensitive
        substring on both abbreviation and full name."""
        t = (pk_token or "").lower().strip()
        if not t:
            return False
        for tag in (abbr or "", name or ""):
            tag_l = (tag or "").lower().strip()
            if not tag_l:
                continue
            if t == tag_l or tag_l in t or t in tag_l:
                return True
        return False

    if bt == "ML":
        if not matchup or " @ " not in matchup:
            return None
        away, home = matchup.split(" @ ", 1)
        # Pick text matches by abbreviation+name on either side.
        if _side_matches(pk, home_abbr or home, home_name):
            return "W" if home_s > away_s else "L"
        if _side_matches(pk, away_abbr or away, away_name):
            return "W" if away_s > home_s else "L"
        return None
    if bt == "SPREAD":
        # Pick text "TEAM +/-X.Y"
        parts = pk.rsplit(" ", 1)
        if len(parts) != 2:
            return None
        team, line_s = parts
        try:
            line = float(line_s)
        except ValueError:
            return None
        if not matchup or " @ " not in matchup:
            return None
        away, home = matchup.split(" @ ", 1)
        if _side_matches(team, home_abbr or home, home_name):
            margin = home_s - away_s
        elif _side_matches(team, away_abbr or away, away_name):
            margin = away_s - home_s
        else:
            return None
        # Cover: margin + line > 0 (line is the team's spread, e.g. -3)
        adj = margin + line
        if adj > 0:
            return "W"
        if adj < 0:
            return "L"
        return "P"
    if bt == "TOTAL":
        parts = pk.split(" ", 1)
        if len(parts) != 2:
            return None
        side, line_s = parts
        try:
            line = float(line_s)
        except ValueError:
            return None
        total = home_s + away_s
        if side.lower() == "over":
            if total > line: return "W"
            if total < line: return "L"
            return "P"
        if side.lower() == "under":
            if total < line: return "W"
            if total > line: return "L"
            return "P"
    # ── Q1 markets ──
    if bt in ("Q1_ML", "Q1_SPREAD", "Q1_TOTAL"):
        if home_q1 is None or away_q1 is None:
            return None  # Q1 scores missing — retry on next settle pass
        if bt == "Q1_ML":
            # Pick text "<TEAM> Q1" (Draw-No-Bet refunds ties)
            tok = pk.replace(" Q1", "").strip()
            if not matchup or " @ " not in matchup:
                return None
            away, home = matchup.split(" @ ", 1)
            if _side_matches(tok, home_abbr or home, home_name):
                if home_q1 > away_q1: return "W"
                if home_q1 < away_q1: return "L"
                return "P"
            if _side_matches(tok, away_abbr or away, away_name):
                if away_q1 > home_q1: return "W"
                if away_q1 < home_q1: return "L"
                return "P"
            return None
        if bt == "Q1_SPREAD":
            # Pick text "<TEAM> +/-X.Y Q1"
            body = pk.replace(" Q1", "").strip()
            parts = body.rsplit(" ", 1)
            if len(parts) != 2:
                return None
            team, line_s = parts
            try:
                line = float(line_s)
            except ValueError:
                return None
            if not matchup or " @ " not in matchup:
                return None
            away, home = matchup.split(" @ ", 1)
            if _side_matches(team, home_abbr or home, home_name):
                margin = home_q1 - away_q1
            elif _side_matches(team, away_abbr or away, away_name):
                margin = away_q1 - home_q1
            else:
                return None
            adj = margin + line
            if adj > 0: return "W"
            if adj < 0: return "L"
            return "P"
        if bt == "Q1_TOTAL":
            # Pick text "Over X.Y Q1" / "Under X.Y Q1"
            body = pk.replace(" Q1", "").strip()
            parts = body.split(" ", 1)
            if len(parts) != 2:
                return None
            side, line_s = parts
            try:
                line = float(line_s)
            except ValueError:
                return None
            total = home_q1 + away_q1
            if side.lower() == "over":
                if total > line: return "W"
                if total < line: return "L"
                return "P"
            if side.lower() == "under":
                if total < line: return "W"
                if total > line: return "L"
                return "P"
    return None


def _settle_with_odds(result: str, odds: int) -> tuple[str, float]:
    """Convert (result, American odds) → (result_letter, profit_per_unit).
    Unit stake = $100 to mirror the existing per-sport tracker convention."""
    if result == "P":
        return ("P", 0.0)
    if result not in ("W", "L"):
        return (result, 0.0)
    if odds == 0:
        return (result, 0.0)
    if result == "L":
        return ("L", -100.0)
    # Win — payoff per $100 stake
    if odds > 0:
        return ("W", float(odds))
    return ("W", round(100.0 * 100 / abs(odds), 2))


# ── Read helpers ─────────────────────────────────────────────

def _norm_team_token(s: str) -> str:
    """Accent-strip + lowercase + drop separator junk. Used by the
    orphan-pick reconciler when team tables have dup entries with
    different accents/abbreviations (HR uses 'QUI' for Quimsa, RealGM
    ingests it as 'QSDE' aka 'Quimsa Santiago del Estero')."""
    import unicodedata
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def _team_first_word(s: str) -> str:
    """Normalised first whitespace-token of a team name. 'Quimsa
    Santiago del Estero' → 'quimsa'. Matches against the pick's
    abbreviation OR a shorter team-name variant."""
    norm = _norm_team_token(s)
    return norm.split()[0] if norm else ""


def _reconcile_orphan_picks(conn, league: str) -> int:
    """Rewrite orphan game_ids on pending picks to the real games row.

    Match strategy (in order, most precise first):
      1. (date, matchup) — exact abbr or full name match on either
         orientation (legacy path).
      2. (date, matchup) — accent-stripped first-token match. Catches
         the QUI/QSDE/Quimsa-Santiago-del-Estero family where HR's
         3-letter abbreviation and RealGM's long-form name disagree
         but BOTH start with 'Quimsa' once accent-stripped.

    Returns count of rewrites. Idempotent: safe to call every settle.
    """
    from ._db import teams_table
    from ..picks_core import score_pick  # noqa: F401 — ensure import works
    p_tbl = picks_table(league)
    g_tbl = games_table(league)
    t_tbl = teams_table(league)
    orphans = conn.execute(
        f"SELECT p.id, p.game_id, p.date, p.matchup, p.bet_type, p.pick "
        f"FROM {p_tbl} p "
        f"LEFT JOIN {g_tbl} g ON g.game_id = p.game_id "
        f"WHERE p.result IS NULL AND g.game_id IS NULL"
    ).fetchall()
    if not orphans:
        return 0
    rewritten = 0
    for orph in orphans:
        matchup = orph["matchup"] or ""
        date = orph["date"]
        if not (matchup and date):
            continue
        # Pick text shape is "AWAY @ HOME" with team abbreviations OR
        # full names. Look up games on the same date whose home/away
        # team matches either side, both orientations.
        parts = matchup.split(" @ ")
        if len(parts) != 2:
            continue
        away_s, home_s = parts[0].strip(), parts[1].strip()
        # Match either orientation, abbreviation OR name.
        candidates = conn.execute(
            f"SELECT g.game_id, "
            f"  ht.abbreviation AS h_abbr, ht.name AS h_name, "
            f"  at.abbreviation AS a_abbr, at.name AS a_name "
            f"FROM {g_tbl} g "
            f"LEFT JOIN {t_tbl} ht ON ht.id = g.home_team_id "
            f"LEFT JOIN {t_tbl} at ON at.id = g.away_team_id "
            f"WHERE g.date = ?", (date,),
        ).fetchall()
        real_gid = None
        # Pass 1 — exact abbr / name (legacy).
        for c in candidates:
            home_ids = {(c["h_abbr"] or "").lower(),
                        (c["h_name"] or "").lower()}
            away_ids = {(c["a_abbr"] or "").lower(),
                        (c["a_name"] or "").lower()}
            if (home_s.lower() in home_ids
                    and away_s.lower() in away_ids):
                real_gid = c["game_id"]
                break
            # Swapped orientation tolerance
            if (home_s.lower() in away_ids
                    and away_s.lower() in home_ids):
                real_gid = c["game_id"]
                break
        # Pass 2 — accent-stripped + prefix tolerance. Each side of the
        # matchup is checked against EITHER the team's abbreviation OR
        # the first token of the team name, allowing a 3-letter abbrev
        # like 'QUI' to match a long form like 'Quimsa Santiago del
        # Estero' (whose first word is 'quimsa', shares prefix 'qui').
        # Fixes the HR-vs-RealGM team-table duplication where the same
        # physical team has two team_ids with different abbreviations
        # because HR and the ingest source disagreed on the canonical
        # form.
        if not real_gid:
            home_norm = _norm_team_token(home_s)
            away_norm = _norm_team_token(away_s)

            def _matches(needle: str, candidates_set: set[str]) -> bool:
                if not needle:
                    return False
                if needle in candidates_set:
                    return True
                # Prefix tolerance: needle must be ≥3 chars and a
                # genuine prefix of some candidate token (not the other
                # way around — 'quimsa' isn't a prefix of 'qui').
                if len(needle) < 3:
                    return False
                for cand in candidates_set:
                    if cand and len(cand) >= len(needle) \
                            and cand.startswith(needle):
                        return True
                return False

            for c in candidates:
                h_keys = {
                    _norm_team_token(c["h_abbr"] or ""),
                    _team_first_word(c["h_name"] or ""),
                }
                a_keys = {
                    _norm_team_token(c["a_abbr"] or ""),
                    _team_first_word(c["a_name"] or ""),
                }
                h_keys.discard("")
                a_keys.discard("")
                if _matches(home_norm, h_keys) and _matches(away_norm, a_keys):
                    real_gid = c["game_id"]
                    break
                # Swapped orientation tolerance
                if _matches(home_norm, a_keys) and _matches(away_norm, h_keys):
                    real_gid = c["game_id"]
                    break
        if not real_gid:
            continue
        # UNIQUE(date, game_id, bet_type, pick) — if a real-game-id row
        # already covers the same family/pick the orphan would collapse
        # into, delete the orphan instead of trying to UPDATE into a
        # collision. Previously the UPDATE threw IntegrityError mid-
        # reconcile and aborted the whole settle pass (WNBA 5/29 had
        # both '401856946' and 'hr-684772568' for LA @ WSH SPREAD; the
        # update from the orphan to the real id collided and the
        # exception unwound the entire settler).
        existing = conn.execute(
            f"SELECT id FROM {p_tbl} "
            f"WHERE date = ? AND game_id = ? AND bet_type = ? AND pick = ? "
            f"  AND id != ? LIMIT 1",
            (orph["date"], real_gid, orph["bet_type"], orph["pick"],
              orph["id"]),
        ).fetchone()
        if existing:
            conn.execute(
                f"DELETE FROM {p_tbl} WHERE id = ?", (orph["id"],),
            )
            logger.info(
                "[%s] orphan pick id=%s collapsed into existing id=%s "
                "for %s/%s — deleted orphan",
                league, orph["id"], existing["id"],
                orph["matchup"], orph["bet_type"],
            )
        else:
            conn.execute(
                f"UPDATE {p_tbl} SET game_id = ? WHERE id = ?",
                (real_gid, orph["id"]),
            )
        rewritten += 1
    if rewritten:
        conn.commit()
        logger.info("[%s] reconciled %d orphan picks", league, rewritten)
    return rewritten


def list_pending(league: str, limit: int = 200) -> list[dict]:
    if league == "nba":
        from ..nba_tracker import list_pending as _nba_pending  # type: ignore
        return _nba_pending(limit=limit)
    conn = get_conn(league)
    tbl = picks_table(league)
    rows = conn.execute(
        f"SELECT * FROM {tbl} WHERE result IS NULL "
        f"ORDER BY date DESC, id DESC LIMIT ?",
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]


def list_history(league: str, limit: int = 200) -> list[dict]:
    if league == "nba":
        from ..nba_tracker import list_history as _nba_history  # type: ignore
        return _nba_history(limit=limit)
    conn = get_conn(league)
    tbl = picks_table(league)
    rows = conn.execute(
        f"SELECT * FROM {tbl} ORDER BY id DESC LIMIT ?",
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]
