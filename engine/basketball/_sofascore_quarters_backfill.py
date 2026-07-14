"""One-shot historical quarter-split backfill for RealGM-sourced
basketball leagues whose ``home_q1..q4`` / ``away_q1..q4`` columns are
empty because their primary feed (RealGM scoreboards) doesn't ship
period-level scores.

Strategy: SofaScore's per-event payload includes ``homeScore.period1..4``
and ``awayScore.period1..4`` on every finished game. Walk every season
SofaScore exposes for the league, normalize each event, then UPDATE
games already in our DB by matching (date, away_team, home_team).

We don't INSERT new rows here — that's the RealGM ingest's job. Backfill
only fills quarter columns on games we already have, so dedup with the
main ingest is the same identity tuple it uses.

Public:
    backfill_league(league_slug, *, max_seasons=3) -> dict
    backfill_all() -> dict   # iterates the 5 quarter-blind leagues
"""
from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

# Quarter-blind RealGM leagues (audited 2026-05-15).
QUARTER_BLIND_LEAGUES = (
    "brazil_nbb", "argentina_lnb", "australia_nbl", "nz_nbl", "china_cba",
)


# Per-league SofaScore → DB team-name aliases. Keys are SofaScore-shipped
# names (lowercased); values are the DB-stored canonical name (also
# lowercase). When the SofaScore name differs from RealGM's sponsor /
# mascot variant the fuzzy ratio doesn't bridge the gap, and these
# explicit aliases keep the (date, away_id, home_id) join from missing.
_SS_TO_DB_ALIAS: dict[str, dict[str, str]] = {
    "argentina_lnb": {
        # Sponsor/city variants vs RealGM's canonical name.
        "gimnasia comodoro rivadavia": "gimnasia indalo",
        "atletico tucuman": "atl tucuman",
        "newell's old boys": "newells old boys",
        "huracan las heras": "huracan",
        # SofaScore appends province codes (la pampa / etc.) RealGM
        # drops. Map both directions where collisions are unlikely.
    },
    "china_cba": {
        "beijing ducks":           "beijing shougang",
        "beijing royal fighters":  "beijing beikong",
        "shanghai sharks":         "shanghai dongfang",
        "guangdong southern tigers": "guangdong dongguan",
        "shanxi loongs":           "shanxi zhongyu",
        "zhejiang guangsha lions": "zhejiang guangsha",
        "zhejiang golden bulls":   "zhejiang chouzhou",
        "liaoning flying leopards": "liaoning",
        "jilin northeast tigers":  "jilin northeast",
        "shandong heroes":         "shandong",
        "fujian sturgeons":        "fujian",
        "qingdao eagles":          "qingdao",
        "tianjin pioneers":        "tianjin",
        "sichuan blue whales":     "sichuan",
        "jiangsu dragons":         "jiangsu",
        "xinjiang flying tigers":  "xinjiang",
    },
    "brazil_nbb": {
        # Brazilian teams pivot sponsor names every couple of seasons.
        # Add aliases as audits surface them.
    },
    "australia_nbl": {},
    "nz_nbl": {},
}


def _norm_name(s: str | None) -> str:
    return (s or "").strip().lower()


def backfill_league(league: str, *, max_seasons: int = 3,
                    throttle: float = 0.2) -> dict:
    """Walk the last ``max_seasons`` SofaScore seasons for ``league``,
    UPDATE any matching games rows with quarter splits. Returns
    {seasons, events, updated, skipped}.

    Match key: (date_iso, away_name_lower, home_name_lower). Skips
    games whose home_q1 is already populated (idempotent re-runs)."""
    from scrapers.sofascore_basketball import (
        list_seasons, fetch_history_for_season,
    )
    from ._config import sofascore_tournament_id
    from ._db import get_conn, teams_table, games_table

    tid = sofascore_tournament_id(league)
    if not tid:
        return {"seasons": 0, "events": 0, "updated": 0, "skipped": 0,
                "reason": "no sofascore tournament_id"}

    seasons = list_seasons(tid)[:max_seasons]
    if not seasons:
        return {"seasons": 0, "events": 0, "updated": 0, "skipped": 0,
                "reason": "no seasons available"}

    conn = get_conn(league)
    t_tbl = teams_table(league)
    g_tbl = games_table(league)

    # Build name → id lookup with multi-variant keys so SofaScore's
    # canonical name ("Tauranga Whai") still resolves when RealGM ships
    # just the nickname ("Whai") or a sponsored variant ("ZeroFees
    # Southland Sharks"). Three keys per team: full name, first word,
    # last word.
    name_lookup: dict[str, int] = {}
    db_team_names: list[tuple[int, str]] = []
    for r in conn.execute(
        f"SELECT id, name FROM {t_tbl} WHERE name IS NOT NULL"
    ).fetchall():
        nm = (r["name"] or "").strip()
        if not nm:
            continue
        team_id = int(r["id"])
        db_team_names.append((team_id, nm.lower()))
        name_lookup[nm.lower()] = team_id
        toks = nm.split()
        if len(toks) > 1:
            name_lookup.setdefault(toks[0].lower(), team_id)
            name_lookup.setdefault(toks[-1].lower(), team_id)

    aliases = _SS_TO_DB_ALIAS.get(league, {})

    def _resolve(name: str) -> int | None:
        n = (name or "").strip().lower()
        if not n:
            return None
        # Per-league alias first — handles SofaScore mascot / sponsor
        # variants that the fuzzy matcher can't bridge (Argentine
        # Gimnasia Indalo vs Gimnasia Comodoro Rivadavia, CBA Beijing
        # Ducks vs Beijing Shougang, etc).
        if n in aliases:
            n = aliases[n]
        if n in name_lookup:
            return name_lookup[n]
        toks = n.split()
        for tok in toks:
            if tok in name_lookup and len(tok) >= 4:
                return name_lookup[tok]
        from difflib import SequenceMatcher
        best_id = None
        best_ratio = 0.0
        for t_id, dbn in db_team_names:
            r = SequenceMatcher(None, n, dbn).ratio()
            if r > best_ratio:
                best_ratio = r
                best_id = t_id
        return best_id if best_ratio >= 0.78 else None

    # Build (date, away_id, home_id) → game_id lookup
    game_lookup: dict[tuple[str, int, int], tuple] = {}
    for r in conn.execute(
        f"SELECT game_id, date, home_team_id, away_team_id, home_q1 "
        f"FROM {g_tbl} WHERE status='final'"
    ).fetchall():
        if not r["date"]:
            continue
        game_lookup[(r["date"],
                     int(r["away_team_id"]),
                     int(r["home_team_id"]))] = (r["game_id"],
                                                   r["home_q1"] is not None)

    seasons_used = events_seen = updated = skipped = 0
    for s in seasons:
        seasons_used += 1
        rows = fetch_history_for_season(tid, s["id"])
        events_seen += len(rows)
        for r in rows:
            aid = _resolve(r["away"])
            hid = _resolve(r["home"])
            if aid is None or hid is None:
                skipped += 1
                continue
            k = (r["date"], aid, hid)
            hit = game_lookup.get(k)
            if not hit:
                # Try ±1 day for timezone wrap (SofaScore UTC vs DB local)
                from datetime import datetime, timedelta
                try:
                    d0 = datetime.strptime(r["date"], "%Y-%m-%d")
                    for delta in (-1, 1):
                        alt = (d0 + timedelta(days=delta)).strftime("%Y-%m-%d")
                        hit = game_lookup.get((alt, aid, hid))
                        if hit:
                            break
                except ValueError:
                    pass
            if not hit:
                skipped += 1
                continue
            gid, already = hit
            if already:
                skipped += 1
                continue
            if r.get("home_q1") is None:
                skipped += 1
                continue
            conn.execute(
                f"UPDATE {g_tbl} SET "
                f"  home_q1=?, away_q1=?, home_q2=?, away_q2=?, "
                f"  home_q3=?, away_q3=?, home_q4=?, away_q4=? "
                f"WHERE game_id=?",
                (r.get("home_q1"), r.get("away_q1"),
                 r.get("home_q2"), r.get("away_q2"),
                 r.get("home_q3"), r.get("away_q3"),
                 r.get("home_q4"), r.get("away_q4"),
                 gid),
            )
            updated += 1
        conn.commit()
        time.sleep(throttle)
        logger.info("[%s] season %s: walked %d events, %d updated so far",
                    league, s.get("name"), len(rows), updated)
    return {"seasons": seasons_used, "events": events_seen,
            "updated": updated, "skipped": skipped}


def backfill_all(*, max_seasons: int = 3) -> dict:
    """Run backfill_league across all known quarter-blind leagues."""
    results: dict[str, dict] = {}
    for slug in QUARTER_BLIND_LEAGUES:
        try:
            results[slug] = backfill_league(slug, max_seasons=max_seasons)
        except Exception as e:
            logger.exception("[%s] backfill crashed: %s", slug, e)
            results[slug] = {"error": str(e)}
    return results


def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(
        prog="engine.basketball._sofascore_quarters_backfill")
    ap.add_argument("league", nargs="?", default=None,
                     choices=(*QUARTER_BLIND_LEAGUES, "all"))
    ap.add_argument("--max-seasons", type=int, default=3)
    ap.add_argument("--throttle", type=float, default=0.2)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    if args.league in (None, "all"):
        res = backfill_all(max_seasons=args.max_seasons)
    else:
        res = backfill_league(args.league, max_seasons=args.max_seasons,
                               throttle=args.throttle)
    print(res)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
