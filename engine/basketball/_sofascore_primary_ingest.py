"""SofaScore-primary historical ingest for basketball-framework leagues.

The existing RealGM ingest covers 5 leagues (china_cba / australia_nbl /
nz_nbl / brazil_nbb / argentina_lnb). 17 other registered leagues
declare ``data_source: "realgm"`` but have no entry in the realgm map,
which silently leaves them empty. SofaScore exposes every one of them
via the tournament-id table the quarter-backfill already uses, so we
can drive their team registration + historical games end-to-end from
SofaScore directly.

This is intentionally **history-only**. Daily ingest still flows
through the registered RealGM/SofaScore fallback paths; this module's
job is to seed enough finals that the calibrator can fit constants and
the league can flip from pending_calibration → beta.

Public:
    ingest_history(league, max_seasons=2) -> dict
    ingest_all_pending(max_seasons=2) -> dict
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


def _derive_abbreviation(name: str, used: set[str]) -> str:
    """Tight reimplementation of the realgm helper so this module is
    self-contained. Generates a 3-4 char abbr from team name tokens."""
    toks = [t for t in (name or "").strip().split() if t]
    if not toks:
        return "UNK"
    # Multi-word: initials, max 4
    if len(toks) > 1:
        abbr = "".join(t[0] for t in toks[:4]).upper()
    else:
        abbr = toks[0][:3].upper()
    # Resolve collision
    base = abbr
    suffix = 1
    while abbr in used:
        suffix += 1
        abbr = (base + str(suffix))[:4]
    return abbr


def _upsert_team(conn, t_tbl: str, name: str, used: set[str]
                  ) -> int:
    """Return the team_id for ``name`` in ``t_tbl``, inserting if new.

    Resolves SofaScore-shipped team names to existing RealGM-sourced
    rows so we don't accumulate duplicate teams (Beijing Ducks vs
    Beijing Shougang — same club, different source name). Without
    this, SofaScore historical ingest creates stub teams that don't
    join to the RealGM-stored game rows and quarter splits never
    land in our DB (audit, 2026-05-20: 4 dup pairs in china_cba).
    """
    # 1. Exact name match — covers MLS / Big-5 leagues that ship same
    #    canonical names from both sources.
    row = conn.execute(
        f"SELECT id FROM {t_tbl} WHERE name = ?", (name,),
    ).fetchone()
    if row:
        return int(row["id"])
    # 2. First-word (city) match, but ONLY when unambiguous. Two
    #    Beijing teams in CBA (Shougang vs BeiKong) → first-word
    #    ambiguous → fall through. One Shanghai team → first-word
    #    safe. Skip very short city tokens (≤3 chars) to avoid
    #    matching "Real" / "Inter" across leagues by accident.
    toks = name.strip().split()
    if toks and len(toks[0]) >= 4:
        candidates = conn.execute(
            f"SELECT id, name FROM {t_tbl} WHERE name LIKE ? || ' %'",
            (toks[0],),
        ).fetchall()
        if len(candidates) == 1:
            return int(candidates[0]["id"])
    # 3. Fuzzy match against the existing rows. Single SQL pass plus
    #    a SequenceMatcher rank; only accept above 0.85 to avoid
    #    false positives. Cap at 50 rows for league size sanity.
    from difflib import SequenceMatcher
    all_rows = conn.execute(
        f"SELECT id, name FROM {t_tbl} WHERE name IS NOT NULL LIMIT 200"
    ).fetchall()
    best_id = None
    best_ratio = 0.0
    n_lower = name.lower()
    for r in all_rows:
        ratio = SequenceMatcher(None, n_lower,
                                 (r["name"] or "").lower()).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_id = int(r["id"])
    if best_id is not None and best_ratio >= 0.85:
        return best_id
    # 4. Truly new team — insert with a generated abbreviation.
    abbr = _derive_abbreviation(name, used)
    used.add(abbr)
    cur = conn.execute(
        f"INSERT INTO {t_tbl} "
        f"(name, abbreviation, city, venue, external_id, logo_url) "
        f"VALUES (?, ?, ?, ?, ?, ?)",
        (name, abbr, "", "", None, None),
    )
    return int(cur.lastrowid)


def _norm_event_row(row: dict, conn, t_tbl: str,
                     used_abbrs: set[str]) -> dict | None:
    """Turn a SofaScore normalized event into the upsert shape the
    framework games table expects. Resolves team names → team_ids,
    computes a stable game_id from (date, home_id, away_id)."""
    date = row.get("date")
    if not date:
        return None
    home_id = _upsert_team(conn, t_tbl, row["home"], used_abbrs)
    away_id = _upsert_team(conn, t_tbl, row["away"], used_abbrs)
    # SofaScore-provided event id is more stable than a hash;
    # fall back to (date, away, home) when absent.
    ext_id = row.get("event_id") or f"{date}-{away_id}-{home_id}"
    game_id = f"sofa-{ext_id}"
    # Status: only persist finals here — pre/scheduled rows have no
    # scores, so they don't help calibration and would pollute the
    # finals query.
    status = row.get("status")
    if status != "final":
        return None
    return {
        "game_id": game_id,
        "date": date,
        "home_team_id": home_id,
        "away_team_id": away_id,
        "home_score": row.get("home_score"),
        "away_score": row.get("away_score"),
        "home_q1": row.get("home_q1"),
        "away_q1": row.get("away_q1"),
        "home_q2": row.get("home_q2"),
        "away_q2": row.get("away_q2"),
        "home_q3": row.get("home_q3"),
        "away_q3": row.get("away_q3"),
        "home_q4": row.get("home_q4"),
        "away_q4": row.get("away_q4"),
        "external_id": str(ext_id),
        "start_time": f"{date}T00:00:00Z",
    }


def _insert_game(conn, g_tbl: str, row: dict, season: int) -> bool:
    """Insert one game. Idempotent — INSERT OR IGNORE on game_id."""
    try:
        cur = conn.execute(
            f"INSERT OR IGNORE INTO {g_tbl} "
            f"(game_id, date, start_time, home_team_id, away_team_id, "
            f" home_score, away_score, "
            f" home_q1, away_q1, home_q2, away_q2, "
            f" home_q3, away_q3, home_q4, away_q4, "
            f" status, season, external_id) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (row["game_id"], row["date"], row["start_time"],
             row["home_team_id"], row["away_team_id"],
             row["home_score"], row["away_score"],
             row.get("home_q1"), row.get("away_q1"),
             row.get("home_q2"), row.get("away_q2"),
             row.get("home_q3"), row.get("away_q3"),
             row.get("home_q4"), row.get("away_q4"),
             "final", season, row["external_id"]),
        )
        return cur.rowcount > 0
    except Exception as e:
        logger.debug("insert_game failed %s: %s", row["game_id"], e)
        return False


def ingest_history(league: str, *, max_seasons: int = 2,
                    throttle: float = 0.4) -> dict:
    """Walk recent SofaScore seasons for ``league`` and seed games.
    Returns {seasons, events_seen, inserted, teams_seeded}.

    Idempotent — INSERT OR IGNORE on game_id, name-keyed team lookup."""
    from scrapers.sofascore_basketball import (
        list_seasons, fetch_history_for_season,
    )
    from ._config import sofascore_tournament_id, LEAGUE_REGISTRY
    from ._db import get_conn, teams_table, games_table

    if league not in LEAGUE_REGISTRY:
        raise KeyError(f"unknown league {league!r}")
    tid = sofascore_tournament_id(league)
    if not tid:
        return {"seasons": 0, "events_seen": 0, "inserted": 0,
                "teams_seeded": 0,
                "reason": "no sofascore tournament_id"}
    seasons = list_seasons(tid)[:max_seasons]
    if not seasons:
        return {"seasons": 0, "events_seen": 0, "inserted": 0,
                "teams_seeded": 0,
                "reason": "no seasons available"}

    conn = get_conn(league)
    t_tbl = teams_table(league)
    g_tbl = games_table(league)
    teams_before = conn.execute(f"SELECT COUNT(*) FROM {t_tbl}").fetchone()[0]
    used_abbrs = {r[0] for r in conn.execute(
        f"SELECT abbreviation FROM {t_tbl} "
        f"WHERE abbreviation IS NOT NULL").fetchall() if r[0]}
    seasons_used = events_seen = inserted = 0
    for s in seasons:
        season_id = s["id"]
        year = s.get("year") or 0
        # SofaScore ships season "year" as "YY/YY" for cross-calendar
        # seasons (e.g. "25/26" = the 2025-26 season starting in fall
        # 2025) and "YYYY" for single-calendar leagues (e.g. WNBA
        # "2025"). The first 4 chars of "25/26" is "25/2" which the
        # old int() parse rejected, so every cross-calendar season
        # got stamped with `datetime.now().year` (2026) — collapsing
        # 3 separate seasons into one. Parse both formats correctly.
        try:
            ystr = str(year).strip()
            if "/" in ystr:
                # "25/26" → start-year 2025 (matches the same convention
                # _season_for_date uses: tag by the year the season
                # STARTED).
                lead = ystr.split("/", 1)[0]
                year_int = 2000 + int(lead) if len(lead) <= 2 else int(lead)
            elif len(ystr) == 4 and ystr.isdigit():
                year_int = int(ystr)
            elif len(ystr) == 2 and ystr.isdigit():
                year_int = 2000 + int(ystr)
            else:
                year_int = int(ystr[:4])
        except (TypeError, ValueError):
            year_int = datetime.now().year
        seasons_used += 1
        rows = fetch_history_for_season(tid, season_id)
        events_seen += len(rows)
        for r in rows:
            norm = _norm_event_row(r, conn, t_tbl, used_abbrs)
            if not norm:
                continue
            if _insert_game(conn, g_tbl, norm, year_int):
                inserted += 1
        conn.commit()
        time.sleep(throttle)
        logger.info(
            "[%s] season %s: walked %d events, %d inserted so far",
            league, s.get("name"), len(rows), inserted,
        )
    teams_after = conn.execute(f"SELECT COUNT(*) FROM {t_tbl}").fetchone()[0]
    return {
        "seasons": seasons_used, "events_seen": events_seen,
        "inserted": inserted,
        "teams_seeded": teams_after - teams_before,
    }


def ingest_today(league: str, *, days_back: int = 4) -> dict:
    """Cheap daily refresh for SofaScore-backed leagues.

    Pulls finished + upcoming events from the last ``days_back`` days
    AND today, upserts into the games table. Closes the loop the user
    flagged 2026-05-17: the slate route was failing silently for
    SofaScore-only leagues because it tried `_realgm_ingest.ingest_today`
    which raises on unmapped leagues. Without this, fresh game results
    never landed in the DB → picks stayed pending → tracker rotted.

    Idempotent — INSERT OR IGNORE on game_id. Picks up tomorrow's
    schedule additions too so the sidebar count starts populating
    without waiting on the next historical-pull job.
    """
    from scrapers.sofascore_basketball import fetch_results_window
    from ._config import sofascore_tournament_id, LEAGUE_REGISTRY
    from ._db import get_conn, teams_table, games_table

    if league not in LEAGUE_REGISTRY:
        return {"events": 0, "inserted": 0,
                "reason": "unknown league"}
    tid = sofascore_tournament_id(league)
    if not tid:
        return {"events": 0, "inserted": 0,
                "reason": "no sofascore tournament_id"}
    conn = get_conn(league)
    t_tbl = teams_table(league)
    g_tbl = games_table(league)
    used_abbrs = {r[0] for r in conn.execute(
        f"SELECT abbreviation FROM {t_tbl} "
        f"WHERE abbreviation IS NOT NULL").fetchall() if r[0]}
    rows = fetch_results_window(tid, days_back=days_back)
    inserted = 0
    for r in rows:
        norm = _norm_event_row(r, conn, t_tbl, used_abbrs)
        if not norm:
            continue
        # Re-derive season from the row's date so the per-league
        # season tag stays correct (matches the post-2026-05-17
        # season-tag convention).
        try:
            from datetime import datetime as _dt
            from ._realgm_ingest import _season_for_date as _sfd
            year = _sfd(league, _dt.strptime(norm["date"], "%Y-%m-%d"))
        except Exception:
            year = datetime.now().year
        if _insert_game(conn, g_tbl, norm, year):
            inserted += 1
    conn.commit()
    return {"events": len(rows), "inserted": inserted}


def ingest_all_pending(*, max_seasons: int = 2) -> dict:
    """Run ingest_history across every pending_calibration league with
    a sofascore_tournament_id. Returns {league: result_dict}."""
    from ._config import LEAGUE_REGISTRY, sofascore_tournament_id
    out: dict[str, dict] = {}
    for slug, cfg in LEAGUE_REGISTRY.items():
        if cfg.get("status") != "pending_calibration":
            continue
        if not sofascore_tournament_id(slug):
            continue
        try:
            out[slug] = ingest_history(slug, max_seasons=max_seasons)
        except Exception as e:
            logger.warning("[%s] history ingest crashed: %s", slug, e)
            out[slug] = {"error": str(e)}
    return out


def _cli() -> int:
    import argparse, logging as _logging
    ap = argparse.ArgumentParser(
        prog="engine.basketball._sofascore_primary_ingest")
    ap.add_argument("league", nargs="?", default=None,
                     help="Single league slug or 'all'")
    ap.add_argument("--max-seasons", type=int, default=2)
    args = ap.parse_args()
    _logging.basicConfig(level=_logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.league in (None, "all"):
        res = ingest_all_pending(max_seasons=args.max_seasons)
        for slug, r in res.items():
            print(f"  {slug}: {r}")
    else:
        res = ingest_history(args.league, max_seasons=args.max_seasons)
        print(res)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
