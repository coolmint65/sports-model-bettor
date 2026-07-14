"""Generic hockey pick tracker — record / settle / list per league.

Per-league picks tables live inside the same per-league DB
(``data/hockey/{league}.db``) created by ``_thescore_ingest``. Schema
is identical across leagues so this module can take a league key and
do the right thing without any per-league branching.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable
from .._tz import et_today_str

logger = logging.getLogger(__name__)


def _conn(league: str):
    """Resolve the per-league sqlite handle. AHL/PWHL/etc each shadow
    the same db.get_conn() helper."""
    mod = __import__(f"engine.sports.{league}.db",
                     fromlist=["get_conn"])
    return mod.get_conn()


def record_picks(league: str, games: Iterable[dict]) -> dict:
    """Persist the best non-skip pick of each game to the league's
    picks table.

    Dedup is delegated to ``engine._dedup_helpers`` so hockey shares the
    same family + settled-dup defense every other sport has: stale
    pending picks in the same family get voided when the model changes
    its mind (e.g. OU Over 5.5 → ML SIDE between runs), and re-records
    after the game has graded are blocked. Without this, AHL OU dups
    accumulated when the picker swapped totals across runs.

    ``games`` is the slate dict shape from ``/api/hockey/{league}/today``.
    """
    from .._dedup_helpers import (
        enforce_one_per_game_per_family, is_settled_dup,
    )
    conn = _conn(league)
    today = et_today_str()
    n_recorded = 0
    n_skipped = 0
    n_blocked_settled = 0

    # Group incoming picks by game_id so we can run the family-dedup
    # helper once per game with the canonical (bet_type, pick) tuple
    # that the model wants to keep for that game.
    by_game: dict[int, list[dict]] = {}
    by_game_meta: dict[int, dict] = {}
    for g in games:
        best = g.get("best_pick")
        if not best:
            n_skipped += 1
            continue
        conf = (best.get("confidence") or "skip").lower()
        if conf == "skip":
            n_skipped += 1
            continue
        gid = int(g["id"])
        away = g.get("away_short") or g.get("away_name") or ""
        home = g.get("home_short") or g.get("home_name") or ""
        matchup = f"{away} @ {home}"
        date = g.get("date") or today
        by_game.setdefault(gid, []).append({
            "type": best.get("type") or "?",
            "pick": best.get("pick") or "?",
            "prob": best.get("raw_prob") or best.get("prob"),
            "edge": best.get("edge"),
            "odds": best.get("odds"),
            "confidence": conf,
            "stake_units": best.get("stake_units"),
            "matchup": matchup,
            "date": date,
        })
        by_game_meta[gid] = {"date": date, "matchup": matchup}

    for gid, picks_list in by_game.items():
        date = by_game_meta[gid]["date"]
        enforce_one_per_game_per_family(
            conn, table="picks", game_id=str(gid), date=date,
            picks=picks_list,
        )
        for p in picks_list:
            if is_settled_dup(conn, table="picks", game_id=str(gid),
                                bet_type=p.get("type") or ""):
                n_blocked_settled += 1
                continue
            try:
                conn.execute(
                    "INSERT INTO picks (game_id, date, matchup, bet_type, "
                    "pick, model_prob, edge, odds, confidence, "
                    " stake_units) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        gid, date, p["matchup"], p["type"], p["pick"],
                        p["prob"], p["edge"],
                        int(p["odds"]) if p.get("odds") is not None else None,
                        p["confidence"],
                        p.get("stake_units"),
                    ),
                )
                n_recorded += 1
            except Exception as e:
                if "UNIQUE" in str(e).upper():
                    n_skipped += 1
                else:
                    logger.debug("[%s] record pick failed %s/%s: %s",
                                  league, gid, p.get("pick"), e)
                    n_skipped += 1
    conn.commit()

    # Phase-2 cutover: dual-write into picks_unified (canonical store).
    try:
        from ..picks_unified._legacy_bridge import mirror_to_unified
        for gid, picks_list in by_game.items():
            for p in picks_list:
                mirror_to_unified(
                    sport="hockey", league=league,
                    native_game_id=gid,
                    pick_date=p["date"],
                    matchup=p["matchup"],
                    bet_type=p["type"] or "",
                    pick_text=p["pick"] or "",
                    odds=int(p["odds"] or 0) if p.get("odds") is not None else 0,
                    prob=float(p.get("prob") or 0.0),
                    edge_pct=float(p.get("edge") or 0.0),
                    stake_units=float(p.get("stake_units") or 0.0),
                    confidence=p.get("confidence"),
                )
    except Exception as e:
        logger.debug("picks_unified mirror (hockey/%s) skipped: %s", league, e)

    return {"recorded": n_recorded, "skipped": n_skipped,
            "blocked_settled": n_blocked_settled}


def _reconcile_stub_game_ids(conn) -> int:
    """Rewrite orphaned stub game_ids on pending picks to the real
    games row once theScore's ingest has caught up. Picks created from
    an HR-only matchup carry an MD5-derived negative stub id that
    doesn't exist in the games table. When theScore later posts the
    same matchup, this reconciler matches it.

    Match strategy is forgiving — HR's home/away orientation can
    disagree with theScore's (e.g. AHL HEN@COLEA on 2026-05-05 is
    COLEA@HEN per theScore, with the score reflecting the swap), and
    theScore can roll the date forward past midnight UTC. So we try:
      1. exact (date, away, home)
      2. swapped (date, home, away)  -- different orientation
      3. ±1 day on (away, home)      -- timezone roll
      4. ±1 day on (home, away)
    Returns count of rewrites."""
    from datetime import datetime as _dt, timedelta as _td

    rewritten = 0
    orphans = conn.execute(
        "SELECT p.id, p.game_id, p.date, p.matchup "
        "FROM picks p LEFT JOIN games g ON p.game_id = g.id "
        "WHERE p.result IS NULL AND g.id IS NULL"
    ).fetchall()
    for orph in orphans:
        parts = (orph["matchup"] or "").split(" @ ")
        if len(parts) != 2:
            continue
        away_abbr, home_abbr = parts[0].strip(), parts[1].strip()
        try:
            base = _dt.strptime(orph["date"], "%Y-%m-%d")
        except (TypeError, ValueError):
            continue
        date_window = [(base + _td(days=d)).strftime("%Y-%m-%d")
                        for d in (0, 1, -1)]
        # Each candidate is (date, expected_away, expected_home).
        candidates: list[tuple[str, str, str]] = []
        for d in date_window:
            candidates.append((d, away_abbr, home_abbr))
            candidates.append((d, home_abbr, away_abbr))

        real = None
        for d, exp_away, exp_home in candidates:
            # Same short_name-vs-abbreviation gotcha that broke
            # settle_pending — SofaScore-sourced leagues (AIHL/NZIHL)
            # leave short_name empty so the join must match against
            # abbreviation instead. COALESCE keeps both data sources
            # working without a per-source fork.
            real = conn.execute(
                "SELECT g.id FROM games g "
                "JOIN teams h ON g.home_team_id = h.id "
                "JOIN teams a ON g.away_team_id = a.id "
                "WHERE g.date = ? "
                "  AND COALESCE(NULLIF(h.abbreviation,''),"
                "               NULLIF(h.short_name,''),"
                "               h.full_name) = ? "
                "  AND COALESCE(NULLIF(a.abbreviation,''),"
                "               NULLIF(a.short_name,''),"
                "               a.full_name) = ? "
                "LIMIT 1",
                (d, exp_home, exp_away),
            ).fetchone()
            if real:
                break
        if not real:
            continue
        conn.execute(
            "UPDATE picks SET game_id = ? WHERE id = ?",
            (int(real["id"]), int(orph["id"])),
        )
        rewritten += 1
    if rewritten:
        conn.commit()
    return rewritten


def settle_pending(league: str) -> dict:
    """Walk pending picks and settle any whose game is now final.

    Result mapping per bet_type:
      - ML:  Win iff the picked side won.
      - PL:  Win iff the picked side covered the +/- 1.5 line.
      - OU:  Win iff the total falls on the picked side of the line.
    Pushes (PL exact +/-1.5? not possible since 1.5 is half line) and
    voids are unlikely on these markets but handled.
    """
    conn = _conn(league)
    # Self-heal: auto-push picks pending >7 days. Same threshold the
    # rest of the per-sport settlers use. Catches games whose theScore
    # status never flips (offseason gap, suspended games, etc.) so
    # pending rows can't accumulate forever.
    from datetime import datetime as _dt2, timedelta as _td2
    stale_cutoff = (_dt2.now() - _td2(days=7)).strftime("%Y-%m-%d")
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
            "[hockey:%s] settle: auto-pushed stale pending id=%s "
            "date=%s %s/%s — older than 7 days",
            league, row["id"], row["date"], row["bet_type"], row["pick"],
        )
        auto_pushed += 1
    if auto_pushed:
        conn.commit()
    # First reconcile orphan stub IDs so newly-final theScore games
    # can settle picks that were created on an HR-only matchup.
    reconciled = _reconcile_stub_game_ids(conn)
    if reconciled:
        logger.info("[%s] reconciled %d orphan-stub pick(s) to real games",
                    league, reconciled)
    # short_name is empty for most theScore-sourced teams (AIHL/NZIHL);
    # abbreviation is the column the picks engine writes (e.g. 'SYDB',
    # 'BOT'). Use abbreviation so _resolve_result's _is_*_side helpers
    # don't short-circuit on the empty string. Fall back to short_name
    # for the rare row that has one but no abbreviation, then to
    # full_name as a last-ditch literal match.
    pending = conn.execute(
        "SELECT p.id, p.game_id, p.bet_type, p.pick, p.odds, "
        "       g.home_score, g.away_score, g.status, "
        "       COALESCE(NULLIF(h.abbreviation, ''), "
        "                NULLIF(h.short_name, ''), "
        "                h.full_name) AS home_abbr, "
        "       COALESCE(NULLIF(a.abbreviation, ''), "
        "                NULLIF(a.short_name, ''), "
        "                a.full_name) AS away_abbr "
        "FROM picks p "
        "JOIN games g ON p.game_id = g.id "
        "LEFT JOIN teams h ON g.home_team_id = h.id "
        "LEFT JOIN teams a ON g.away_team_id = a.id "
        "WHERE p.result IS NULL AND g.status = 'final' "
        "  AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL"
    ).fetchall()
    n_settled = w = l = pushes = 0
    for r in pending:
        result = _resolve_result(
            bet_type=r["bet_type"], pick=r["pick"],
            home_score=r["home_score"], away_score=r["away_score"],
            home_abbr=r["home_abbr"], away_abbr=r["away_abbr"],
        )
        if result is None:
            continue
        profit = _profit_for(result, r["odds"])
        conn.execute(
            "UPDATE picks SET result = ?, profit = ?, "
            "settled_at = datetime('now') WHERE id = ?",
            (result, profit, r["id"]),
        )
        n_settled += 1
        if result == "W": w += 1
        elif result == "L": l += 1
        else: pushes += 1
    conn.commit()
    return {"settled": n_settled, "wins": w, "losses": l, "pushes": pushes,
             "checked": len(pending), "skipped": len(pending) - n_settled}


def _resolve_result(*, bet_type: str, pick: str,
                     home_score: int, away_score: int,
                     home_abbr: str | None = None,
                     away_abbr: str | None = None) -> str | None:
    """Return 'W'/'L'/'P' or None when the pick text isn't parseable.

    Accepts team abbreviations OR the literal words 'Home'/'Away' in
    the pick text — the picks generator now uses real abbreviations
    (WBSP / HERB / etc) but legacy rows from earlier this session
    have 'Home'/'Away' literals; both formats resolve cleanly.
    """
    bt = (bet_type or "").upper()
    pk = (pick or "").strip()
    pk_l = pk.lower()
    h_abbr_l = (home_abbr or "").lower()
    a_abbr_l = (away_abbr or "").lower()
    def _is_home_side(s: str) -> bool:
        return ("home" in s) or (h_abbr_l and s.startswith(h_abbr_l))
    def _is_away_side(s: str) -> bool:
        return ("away" in s) or (a_abbr_l and s.startswith(a_abbr_l))
    if bt == "ML":
        if _is_home_side(pk_l):
            return "W" if home_score > away_score else "L"
        if _is_away_side(pk_l):
            return "W" if away_score > home_score else "L"
        return None
    if bt == "PL":
        # "WBSP -1.5" / "HERB +1.5" / legacy "Home -1.5"
        side = ("home" if _is_home_side(pk_l)
                  else ("away" if _is_away_side(pk_l) else None))
        if side is None:
            return None
        try:
            line = float(pk.split()[-1])
        except ValueError:
            return None
        margin = (home_score - away_score) if side == "home" else (away_score - home_score)
        return "W" if margin > -line else "L"
    if bt == "OU":
        # "Over 5.5" / "Under 4.5"
        try:
            line = float(pk.split()[-1])
        except ValueError:
            return None
        total = home_score + away_score
        if pk.lower().startswith("over"):
            return "W" if total > line else ("L" if total < line else "P")
        return "W" if total < line else ("L" if total > line else "P")
    return None


def _profit_for(result: str, odds: int | None) -> float:
    """$100 base bet."""
    if odds is None:
        return 0.0
    if result == "W":
        return 100.0 * (100 / abs(odds)) if odds < 0 else float(odds)
    if result == "L":
        return -100.0
    return 0.0


def list_history(league: str, limit: int = 200) -> dict:
    """Return aggregate summary + recent picks for the Tracker tab.
    Summary shape matches what `PickHistory.jsx` expects (overall +
    by_type tiles), so the same component renders across NHL, hockey
    framework, basketball framework, etc.

    Summary scans the FULL picks table — every settled pick on file
    contributes to overall + per-market tiles. History rows stay
    capped at ``limit`` for the table display so the response payload
    doesn't bloat as the season piles up."""
    conn = _conn(league)
    rows = conn.execute(
        "SELECT id, game_id, date, matchup, bet_type, pick, "
        "       model_prob, edge, odds, confidence, result, profit, "
        "       stake_units, settled_at, created_at "
        "FROM picks ORDER BY COALESCE(settled_at, created_at) DESC "
        "LIMIT ?",
        (limit,),
    ).fetchall()
    history = []
    for r in rows:
        d = dict(r)
        stake_u = d["stake_units"] if d.get("stake_units") is not None else 1.0
        if d.get("profit") is not None:
            d["profit"] = round(d["profit"] * stake_u, 2)
        history.append(d)

    # Summary across every pick ever, not the limit-capped slice.
    # Stake-weighted: each row contributes profit*stake_units to the
    # numerator and stake_units to the ROI denom.
    all_picks = conn.execute(
        "SELECT bet_type, result, profit, stake_units FROM picks"
    ).fetchall()
    by_type: dict[str, dict] = {}
    wins = losses = pushes = pending = 0
    profit_sum = 0.0
    stake_settled = 0.0
    for r in all_picks:
        bt = r["bet_type"] or "?"
        b = by_type.setdefault(bt, {"total": 0, "wins": 0, "losses": 0,
                                     "pushes": 0, "pending": 0,
                                     "profit": 0.0, "win_pct": 0.0,
                                     "roi": 0.0, "stake_settled": 0.0})
        b["total"] += 1
        res = r["result"]
        stake_u = float(r["stake_units"] if r["stake_units"] is not None else 1.0)
        pr = float(r["profit"] or 0) * stake_u
        if res == "W":
            b["wins"] += 1; wins += 1
            b["profit"] += pr; profit_sum += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        elif res == "L":
            b["losses"] += 1; losses += 1
            b["profit"] += pr; profit_sum += pr
            b["stake_settled"] += stake_u; stake_settled += stake_u
        elif res == "P":
            b["pushes"] += 1; pushes += 1
        else:
            b["pending"] += 1; pending += 1
    for bt, b in by_type.items():
        d = b["wins"] + b["losses"]
        st = b.pop("stake_settled", 0)
        b["win_pct"] = round(b["wins"] / d * 100, 1) if d else 0.0
        b["roi"] = round(b["profit"] / st, 1) if st else 0.0
        b["profit"] = round(b["profit"], 2)
    decided = wins + losses
    return {
        "summary": {
            "overall": {
                "total": wins + losses + pushes,
                "wins": wins, "losses": losses,
                "pushes": pushes, "pending": pending,
                "profit": round(profit_sum, 2),
                "win_pct": round(wins / decided * 100, 1) if decided else 0.0,
                "roi": round(profit_sum / stake_settled, 1) if stake_settled else 0.0,
                "avg_clv": None, "clv_sample": 0,
            },
            "by_type": by_type,
        },
        "history": history,
    }
