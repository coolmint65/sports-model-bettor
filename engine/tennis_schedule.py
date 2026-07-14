"""
Tennis schedule ingest + daily-pick orchestrator.

Walks ESPN's tennis scoreboard for the date, resolves player names
to Sackmann ids, persists to ``tennis_scheduled_matches``, and the
daily picker (:func:`generate_picks`) consumes that table to emit
edges using the existing ``engine.tennis_predict`` + ``tennis_picks``
modules.

Combined events handling
------------------------
Slams + Masters mix ATP and WTA matches under one tournament. ESPN
returns the same competitions in BOTH tour scoreboards. We dedupe
by ``match_id`` and route each match to the tour whose roster
actually resolved both players. This keeps the slate clean — no
ATP men predicting on a WTA match or vice versa.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from typing import Any
from ._tz import et_today_str

logger = logging.getLogger(__name__)


def _try_resolve(name: str, tour: str) -> int | None:
    from .tennis_db import resolve_player_id
    return resolve_player_id(tour, name)


# ESPN's tennis endpoint ships a start_time = "<date>T04:00Z" (midnight
# ET) or "<date>T00:00Z" for slates where the exact match time hasn't
# been announced yet. Treat those as placeholders so Tennis Explorer's
# real HH:MM overwrites them when both sources cover the same match.
_PLACEHOLDER_ISO_SUFFIXES = (
    "T00:00Z", "T00:00:00Z", "T00:00", "T00:00:00",
    "T04:00Z", "T04:00:00Z",
)


def _looks_like_default_time(iso: str | None) -> bool:
    if not iso:
        return True
    s = str(iso)
    return any(s.endswith(sfx) for sfx in _PLACEHOLDER_ISO_SUFFIXES)


def _resolved_tour(p1_name: str, p2_name: str) -> tuple[str | None, int | None, int | None]:
    """Determine which tour both players belong to. Tries ATP first,
    then WTA, then mixed (returns None tour for mixed-doubles or
    unresolvable matches)."""
    for try_tour in ("atp", "wta"):
        p1 = _try_resolve(p1_name, try_tour)
        p2 = _try_resolve(p2_name, try_tour)
        if p1 and p2:
            return try_tour, p1, p2
    return None, None, None


def ingest_schedule(date: str | None = None) -> dict:
    """Pull ESPN tennis scoreboards (ATP + WTA, deduped) and persist
    every resolvable match for ``date`` to tennis_scheduled_matches.

    Returns a summary dict.
    """
    from scrapers.tennis_espn import fetch_today_all_tours
    from .tennis_db import get_conn, ensure_tables
    ensure_tables()
    target = date or et_today_str()
    matches = fetch_today_all_tours(date=target)
    # Dedupe on match_id (combined events appear twice)
    seen_ids: set[str] = set()
    deduped: list[dict] = []
    for m in matches:
        mid = m.get("match_id")
        if not mid or mid in seen_ids:
            continue
        seen_ids.add(mid)
        deduped.append(m)

    summary = {"date": target, "fetched": len(matches),
               "deduped": len(deduped),
               "inserted": 0, "skipped_unresolved": 0}
    if not deduped:
        return summary

    conn = get_conn()
    now_iso = datetime.now().isoformat(timespec="seconds")
    for m in deduped:
        tour, p1_id, p2_id = _resolved_tour(m["p1_name"], m["p2_name"])
        if not tour:
            summary["skipped_unresolved"] += 1
            logger.debug("schedule: cannot resolve %r vs %r — skipped",
                         m["p1_name"], m["p2_name"])
            continue
        try:
            conn.execute(
                """
                INSERT INTO tennis_scheduled_matches (
                    tour, match_id, date, start_time,
                    tournament, tournament_id, surface, best_of, round,
                    status, p1_name, p1_country, p1_id, p1_image, p1_flag,
                    p2_name, p2_country, p2_id, p2_image, p2_flag,
                    score, winner, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tour, match_id) DO UPDATE SET
                    status = excluded.status,
                    score = excluded.score,
                    winner = excluded.winner,
                    p1_image = COALESCE(excluded.p1_image, p1_image),
                    p1_flag  = COALESCE(excluded.p1_flag,  p1_flag),
                    p2_image = COALESCE(excluded.p2_image, p2_image),
                    p2_flag  = COALESCE(excluded.p2_flag,  p2_flag),
                    fetched_at = excluded.fetched_at
                """,
                (
                    tour, m["match_id"], target,
                    m.get("date"),
                    m.get("tournament"), m.get("tournament_id"),
                    m.get("surface"),
                    _coerce_best_of(m.get("best_of"), tour, m.get("tournament")),
                    m.get("round"),
                    m.get("status") or "pre",
                    m["p1_name"], m.get("p1_country"), int(p1_id),
                    m.get("p1_image"), m.get("p1_flag"),
                    m["p2_name"], m.get("p2_country"), int(p2_id),
                    m.get("p2_image"), m.get("p2_flag"),
                    m.get("score"), m.get("winner"),
                    now_iso,
                ),
            )
            summary["inserted"] += 1
        except Exception as e:
            logger.warning("schedule insert failed for %s: %s",
                           m.get("match_id"), e)
    conn.commit()
    logger.info("tennis_schedule[%s]: %d match(es) ingested, %d skipped",
                target, summary["inserted"], summary["skipped_unresolved"])
    return summary


def ingest_schedule_from_te(date: str) -> dict:
    """Pull Tennis Explorer's daily schedule and upsert into
    tennis_scheduled_matches. ESPN covers only ATP / WTA main draws —
    Challenger / WTA125 / ITF Futures slip through. TE publishes the
    /matches/ page for every tour HR trades, which is exactly the
    coverage gap.

    Each TE row carries a stable per-match id (`source_match_id`) we
    rewrite to ``te-<id>`` so it can't collide with ESPN ids. Player
    names go through ``resolve_player_id`` — TE renders names as
    "Sinner J." (last + initial), and the resolver's last-name fallback
    handles that without a separate parser. Rows where either player
    fails to resolve are dropped (no Sackmann history → no prediction
    → no point creating a phantom row)."""
    from scrapers.tennis_results import fetch_schedule_for_date
    from .tennis_db import get_conn, ensure_tables
    from .tennis_surface import infer_surface as _surface_for

    rows = fetch_schedule_for_date(date)
    summary = {"date": date, "fetched": len(rows),
               "inserted": 0, "skipped_unresolved": 0,
               "skipped_existing": 0}
    if not rows:
        return summary
    ensure_tables()
    conn = get_conn()
    now_iso = datetime.now().isoformat(timespec="seconds")

    from scrapers.tennis_results import _resolve_te_name
    from .tennis_db import resolve_player_id, get_player_by_id
    for r in rows:
        # TE name format ("Sinner J.") goes through the existing TE
        # resolver helper from the results scraper, which token-rotates
        # to "J Sinner" then leans on the active-roster fuzzy match.
        tour = r.get("tour")
        if tour not in ("atp", "wta"):
            summary["skipped_unresolved"] += 1
            continue
        p1_id = _resolve_te_name(r["p1_name"], tour, resolve_player_id)
        p2_id = _resolve_te_name(r["p2_name"], tour, resolve_player_id)
        if not (p1_id and p2_id):
            summary["skipped_unresolved"] += 1
            continue
        # Rewrite TE's "Sinner J." short form to the canonical Sackmann
        # full name once we've resolved the id. The HR-odds matcher
        # frozenset-keys on normalized full names — without this swap,
        # TE rows would never link to HR's "Jannik Sinner" entry.
        p1_full = (get_player_by_id(tour, p1_id) or {}).get("name") or r["p1_name"]
        p2_full = (get_player_by_id(tour, p2_id) or {}).get("name") or r["p2_name"]
        r["p1_name"] = p1_full
        r["p2_name"] = p2_full

        match_id = f"te-{r['source_match_id']}"
        # Build a UTC-ish ISO start_time — TE ships local "HH:MM"
        # without a timezone. Best we can do without per-tournament
        # timezone tables is stamp the date + time and let the
        # frontend treat it as already-local. Leaving as None when
        # missing is also fine (the card just renders "TBD").
        st = r.get("start_time")
        start_iso = f"{date}T{st}:00" if st else None

        # Cross-source dedup: ESPN + TE ingest the same tour/date, so
        # ATP/WTA main-draw matches land in BOTH sources. TE existed to
        # cover Challenger/WTA125/ITF, not to duplicate ESPN main-draw
        # matches. Skip the TE row when an ESPN-sourced row already
        # exists for the same tour+date+player pair (order-independent).
        # If TE has a better start_time than ESPN, patch it onto the
        # existing row so we don't lose the more accurate wall-clock.
        pid_a, pid_b = sorted((int(p1_id), int(p2_id)))
        existing = conn.execute(
            """
            SELECT match_id, start_time
              FROM tennis_scheduled_matches
             WHERE tour = ? AND date = ?
               AND match_id NOT LIKE 'te-%'
               AND ((p1_id = ? AND p2_id = ?)
                    OR (p1_id = ? AND p2_id = ?))
             LIMIT 1
            """,
            (tour, date, pid_a, pid_b, pid_b, pid_a),
        ).fetchone()
        if existing:
            existing_start = (dict(existing).get("start_time") or "")
            # ESPN sometimes ships date-default placeholder times when
            # the actual start hasn't been announced (00:00Z midnight
            # UTC, 04:00Z midnight ET, or missing HH:MM). TE's local
            # "HH:MM" is usually more accurate. Prefer TE's start_time
            # when it exists AND the ESPN row is empty or looks like
            # one of these placeholder shapes.
            looks_placeholder = _looks_like_default_time(existing_start)
            if start_iso and (not existing_start or looks_placeholder):
                conn.execute(
                    "UPDATE tennis_scheduled_matches "
                    "SET start_time = ?, fetched_at = ? "
                    "WHERE tour = ? AND match_id = ?",
                    (start_iso, now_iso, tour,
                     dict(existing)["match_id"]),
                )
            summary["skipped_existing"] += 1
            continue

        try:
            conn.execute(
                """
                INSERT INTO tennis_scheduled_matches (
                    tour, match_id, date, start_time,
                    tournament, tournament_id, surface, best_of, round,
                    status, p1_name, p1_country, p1_id,
                    p2_name, p2_country, p2_id,
                    score, winner, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tour, match_id) DO UPDATE SET
                    start_time = COALESCE(excluded.start_time, start_time),
                    tournament = COALESCE(excluded.tournament, tournament),
                    fetched_at = excluded.fetched_at
                """,
                (
                    tour, match_id, date, start_iso,
                    r.get("tournament"), "",
                    # Surface inferred from tournament name (shared with
                    # ESPN ingest). Without this, every TE-sourced match
                    # used the predictor's combined-surface Elo fallback
                    # — losing the surface-specific signal for ~half the
                    # slate. Falls back to "Hard" (most common) when
                    # unknown rather than None.
                    _surface_for(r.get("tournament")),
                    _coerce_best_of(None, tour, r.get("tournament")),
                    None,
                    "pre",
                    r["p1_name"], None, int(p1_id),
                    r["p2_name"], None, int(p2_id),
                    None, None, now_iso,
                ),
            )
            if conn.total_changes:
                summary["inserted"] += 1
            else:
                summary["skipped_existing"] += 1
        except Exception as e:
            logger.warning("TE schedule insert failed for %s/%s: %s",
                           r["p1_name"], r["p2_name"], e)
    conn.commit()
    logger.info("tennis_schedule[TE %s]: fetched=%d inserted=%d "
                "unresolved=%d existing=%d",
                date, summary["fetched"], summary["inserted"],
                summary["skipped_unresolved"], summary["skipped_existing"])
    return summary


def ingest_schedule_window(start_date: str | None = None,
                            days: int = 2,
                            with_hr_supplement: bool = False) -> dict:
    """Ingest the schedule for ``days`` consecutive dates starting at
    ``start_date`` (default today). Default ``days=2`` covers today
    and tomorrow. ESPN covers ATP/WTA main only; Tennis Explorer fills
    Challenger / WTA125 / ITF Futures.

    ``with_hr_supplement`` is RETAINED for API compatibility but is
    now a no-op. HR's tennis market feed publishes bracket-projection
    matchups that frequently never play (Bolt vs Walton 2026-05-01
    case — neither faced the other that day). TE replaced HR as the
    Challenger / sub-tour schedule source 2026-05-05.
    """
    base = (datetime.strptime(start_date, "%Y-%m-%d")
            if start_date
            else datetime.now())
    summaries: list[dict] = []
    for i in range(max(1, int(days))):
        d = (base + timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            summaries.append(ingest_schedule(d))
        except Exception as e:
            logger.warning("ingest_schedule_window: %s ESPN failed: %s", d, e)
            summaries.append({"date": d, "error": str(e)})
        # TE supplements ESPN with Challenger / WTA125 / ITF Futures.
        # Errors are logged but never propagate — TE outage shouldn't
        # block the ESPN-driven main-tour slate.
        try:
            te_summary = ingest_schedule_from_te(d)
            summaries.append({"date": d, "te": te_summary})
        except Exception as e:
            logger.warning("ingest_schedule_window: %s TE failed: %s", d, e)
    if with_hr_supplement:
        logger.info("HR-supplement ingest is a no-op since 2026-05-02 "
                     "(phantom matchup source, dropped at the root).")
    return {"days": int(days), "summaries": summaries, "hr_supplement": None}


def ingest_schedule_from_hr() -> dict:
    """Insert synthetic schedule rows for any HR match-pair signature
    that doesn't already exist in tennis_scheduled_matches. Resolves
    Sackmann player ids against HR's player names so the predictor
    + picker can score the match. Date is 'today' since HR doesn't
    expose a structured match-date — the API layer's window query
    will still surface it under today's slate.

    Pulls events through ``tennis_odds.fetch_all`` so the shared HR
    cache + circuit breaker apply. Doesn't reach `_fetch_with_markets`
    directly any more — every HR call lives behind one rate-aware
    fetcher.
    """
    from .tennis_db import get_conn, ensure_tables
    from .tennis_odds import fetch_all as _fetch_hr
    ensure_tables()
    conn = get_conn()

    today = et_today_str()
    summary = {"hr_events_seen": 0, "inserted": 0,
               "skipped_unresolved": 0, "skipped_existing": 0}

    events = _fetch_hr()
    if not events:
        logger.warning("HR supplement: cache empty / circuit open — skipping")
        return summary

    now_iso = datetime.now().isoformat(timespec="seconds")

    # Map HR category names back to ATP/WTA tour. Doubles + mixed
    # are excluded by the scraper-level filter already, but keep a
    # defensive skip here for safety.
    def _tour_for_category(cat_name: str) -> str | None:
        n = (cat_name or "").lower()
        if "doubles" in n or "mixed" in n:
            return None
        if n.startswith("atp") or n.startswith("challenger") \
                or n.startswith("itf men"):
            return "atp"
        if n.startswith("wta") or n.startswith("itf women"):
            return "wta"
        return None

    for ev in events:
        comp_name = ev.get("comp") or ""
        summary["hr_events_seen"] += 1
        p1_name = ev.get("p1_name") or ""
        p2_name = ev.get("p2_name") or ""
        hr_match_id = ev.get("match_id")
        if not p1_name or not p2_name or not hr_match_id:
            continue

        # Cheap dedupe — if any scheduled match already has this
        # player-pair signature today/tomorrow, skip.
        row = conn.execute(
            "SELECT 1 FROM tennis_scheduled_matches "
            "WHERE date >= date('now', '-1 day') "
            "  AND ((p1_name = ? AND p2_name = ?) "
            "       OR (p1_name = ? AND p2_name = ?))",
            (p1_name, p2_name, p2_name, p1_name),
        ).fetchone()
        if row:
            summary["skipped_existing"] += 1
            continue

        tour = _tour_for_category(comp_name)
        if not tour:
            continue
        # Resolve Sackmann ids — picker needs them for Elo lookup.
        p1_id = _try_resolve(p1_name, tour)
        p2_id = _try_resolve(p2_name, tour)
        if not p1_id or not p2_id:
            summary["skipped_unresolved"] += 1
            continue

        match_id = f"hr-{hr_match_id}"
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO tennis_scheduled_matches (
                    tour, match_id, date, start_time,
                    tournament, tournament_id, surface, best_of, round,
                    status, p1_name, p1_country, p1_id,
                    p2_name, p2_country, p2_id,
                    score, winner, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tour, match_id, today, None,
                    comp_name, "",
                    None,  # surface unknown from HR
                    _coerce_best_of(None, tour, comp_name),
                    None,  # round unknown
                    "pre",
                    p1_name, None, int(p1_id),
                    p2_name, None, int(p2_id),
                    None, None, now_iso,
                ),
            )
            if conn.total_changes:
                summary["inserted"] += 1
        except Exception as e:
            logger.warning("HR supplement insert failed for %s/%s: %s",
                           p1_name, p2_name, e)
    conn.commit()
    logger.info("HR supplement: %d events seen, %d inserted, "
                "%d skipped existing, %d unresolved",
                summary["hr_events_seen"], summary["inserted"],
                summary["skipped_existing"], summary["skipped_unresolved"])
    return summary


def _coerce_best_of(best_of: int | None, tour: str,
                     tournament: str | None) -> int:
    """Authoritative best_of — IGNORES ESPN's value when it
    contradicts the tournament's known format. ESPN occasionally
    reports BO5 for ATP Masters (Madrid, Rome, etc.) which used to
    be BO5 historically but have been BO3 since 2007. Predictor
    fed BO5 when reality is BO3 produces nonsense expected_games
    (Sinner-Fils Madrid showed exp_total_games=34.9 → bogus +49%
    Over 26.5 edge). Tournament name is the source of truth.

    Rules:
      - Men's Slam: BO5
      - Anything else: BO3
    """
    name = (tournament or "").lower()
    is_mens_slam = (tour == "atp" and any(
        s in name for s in
        ("australian open", "french open", "roland",
         "wimbledon", "us open")
    ))
    if is_mens_slam:
        return 5
    return 3


# ── Daily picker ──────────────────────────────────────────────

def generate_picks(date: str | None = None, *,
                    min_edge_ml: float = 4.0,
                    min_edge_other: float = 6.0) -> dict:
    """Walk today's tennis_scheduled_matches and emit edge picks.

    Per-match flow:
      1. Pull scheduled match + resolved player ids
      2. Run engine.tennis_predict.predict_match
      3. (Future) Fetch HR live odds for the match — for now odds
         are passed via callable so a backtest / dry-run can supply
         test odds without HR auth
      4. engine.tennis_picks.generate_tennis_picks finds edges
      5. Persist via engine.tennis_tracker.record_tennis_pick

    Returns ``{date, scanned, picks_emitted, errors}``. The caller is
    responsible for actually placing or paper-betting; this just
    records candidate picks into the tracker.
    """
    from .tennis_db import get_conn, ensure_tables, get_player_by_id
    from .tennis_predict import predict_match
    from .tennis_picks import generate_tennis_picks
    from .tennis_tracker import record_tennis_pick
    from .tennis_odds import fetch_all as _fetch_hr_odds, build_lookup, find_match, align_to_caller
    ensure_tables()
    target = date or et_today_str()
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM tennis_scheduled_matches "
        "WHERE date = ? AND status IN ('pre', 'in') "
        "  AND p1_id IS NOT NULL AND p2_id IS NOT NULL "
        "ORDER BY tournament, start_time",
        (target,),
    ).fetchall()
    summary = {"date": target, "scanned": len(rows),
               "picks_emitted": 0, "errors": 0,
               "hr_matches_found": 0}

    # Pull HR's tennis tape once and index by player-pair signature
    try:
        hr_events = _fetch_hr_odds()
        hr_lookup = build_lookup(hr_events)
    except Exception as e:
        logger.warning("HR tennis odds fetch failed: %s", e)
        hr_lookup = {}

    for r in rows:
        d = dict(r)
        try:
            pred = predict_match(
                d["tour"], int(d["p1_id"]), int(d["p2_id"]),
                surface=d.get("surface") or "Hard",
                best_of=int(d.get("best_of") or 3),
            )
        except Exception as e:
            logger.warning("predict failed for %s: %s", d.get("match_id"), e)
            summary["errors"] += 1
            continue
        if not pred:
            continue
        # Decorate with names so picks emit human-readable text
        pred["p1_name"] = d["p1_name"]
        pred["p2_name"] = d["p2_name"]

        # Match HR odds by player-pair signature. align_to_caller
        # swaps p1/p2 so HR's A/B labels match our scheduled match
        # ordering.
        odds: dict = {}
        hr_match = find_match(hr_lookup, d["p1_name"], d["p2_name"])
        if hr_match:
            aligned = align_to_caller(hr_match, d["p1_name"], d["p2_name"])
            odds = aligned  # contains {markets: {...}} which the picker auto-detects
            summary["hr_matches_found"] += 1
        candidates = generate_tennis_picks(
            pred, odds,
            min_edge_ml=min_edge_ml,
            min_edge_other=min_edge_other,
            tournament_level=_level_code_for(d.get("tournament")),
            include_lower_tiers=False,
        )
        for c in candidates:
            try:
                record_tennis_pick({
                    "tour":            d["tour"],
                    "match_id":        d["match_id"],
                    "date":            d["date"],  # match date, not recording date
                    "matchup":         f"{d['p1_name']} vs {d['p2_name']}",
                    "surface":         d.get("surface"),
                    "best_of":         int(d.get("best_of") or 3),
                    "tourney_level":   _level_code_for(d.get("tournament")),
                    "p1_id":           int(d["p1_id"]),
                    "p2_id":           int(d["p2_id"]),
                    "bet_type":        c["type"],
                    "pick":            c["pick"],
                    "odds":            int(c["odds"]),
                    "model_prob":      float(c["model_prob"]),
                    "edge":            float(c["edge"]),
                    "conviction_score": float(c.get("conviction_score") or 0),
                })
                summary["picks_emitted"] += 1
            except Exception as e:
                logger.warning("record failed: %s", e)
                summary["errors"] += 1
    return summary


def _level_code_for(tournament: str | None,
                     hr_comp: str | None = None) -> str:
    """Heuristic mapping of tournament name → Sackmann-ish level code.
    Used for both pick-floor calibration (lower tiers need higher
    edge to clear) and slate ordering (Slams > Masters > 500 > 250 …).

    ``hr_comp`` is an optional Hard Rock category-prefixed comp name
    (e.g. "WTA125 Saint-Malo", "ITF Men Castelldefels", "Challenger
    Aix en Provence"). HR's prefix is the most reliable tier signal
    we have — ESPN names like "L'Open 35 de Saint Malo" don't carry
    the tier — so we consult it first when present.

    Codes:
      G   — Grand Slam
      F   — Year-end Finals
      M   — Masters 1000 / WTA 1000
      500 — ATP 500 / WTA 500
      250 — ATP 250 / WTA 250 / WTA Premier (legacy)
      125 — WTA 125 Series
      C   — Challenger
      ITF — ITF / Futures
      A   — fallback
    """
    # HR comp prefix wins when present.
    if hr_comp:
        h = hr_comp.lower()
        if h.startswith("wta125") or h.startswith("wta 125"):
            return "125"
        if h.startswith("challenger"):
            return "C"
        if h.startswith("itf men") or h.startswith("itf women") \
                or h.startswith("itf "):
            return "ITF"
        # ATP/WTA prefixes fall through to the tournament-name
        # classifier below since HR doesn't distinguish tour-level
        # tiers (Masters/500/250 all live under just "ATP" or "WTA").

    if not tournament:
        return "A"
    n = tournament.lower()
    if any(s in n for s in
            ("australian open", "roland", "french open",
             "wimbledon", "us open")):
        return "G"
    if any(s in n for s in ("atp finals", "wta finals", "year-end")):
        return "F"
    if any(s in n for s in
            ("indian wells", "miami open", "monte-carlo", "madrid",
             "rome", "cincinnati", "canadian", "rogers", "shanghai",
             "paris masters", "wta 1000")):
        return "M"
    if "atp 500" in n or "wta 500" in n or "500 series" in n:
        return "500"
    if "atp 250" in n or "wta 250" in n or "premier" in n:
        return "250"
    if "wta 125" in n or "125k" in n:
        return "125"
    if "challenger" in n:
        return "C"
    if "itf" in n or "futures" in n or "w15" in n or "w25" in n or "w35" in n or "w50" in n or "w60" in n or "w75" in n or "w100" in n:
        return "ITF"
    return "A"


# Tournament priority for slate sort (higher = leads). Ordered so
# the most-important matches surface first regardless of when they
# play. Used by the frontend to rank tournaments inside each
# Live/Upcoming/Final partition.
TOURNAMENT_PRIORITY: dict[str, int] = {
    "G":   100,
    "F":   90,
    "M":   80,
    "P":   80,   # legacy WTA Premier Mandatory
    "PM":  80,
    "P5":  75,
    "500": 60,
    "250": 40,
    "A":   30,   # generic tour-level
    "125": 20,
    "C":   10,
    "ITF": 5,
}


# Per-tier edge-floor multipliers. Lower tiers require higher edge
# because Sackmann's calibration window thins out and the field is
# more uneven. Multiplied against the caller's base floors, so a
# Grand Slam keeps the 4%/6% defaults but a Challenger needs 8%/10%.
TIER_EDGE_FLOOR_MULT: dict[str, float] = {
    "G":   1.0,
    "F":   1.0,
    "M":   1.0,
    "P":   1.0,
    "PM":  1.0,
    "P5":  1.0,
    "500": 1.10,
    "250": 1.25,
    "A":   1.25,
    "125": 1.50,
    "C":   1.75,
    "ITF": 2.00,
}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="engine.tennis_schedule")
    ap.add_argument("--date", default=None,
                    help="YYYY-MM-DD start date (default today)")
    ap.add_argument("--days", type=int, default=2,
                    help="Number of consecutive days to ingest "
                         "starting at --date (default 2 = today + "
                         "tomorrow). Set 1 for a single-day pull.")
    ap.add_argument("--no-picks", action="store_true",
                    help="Ingest schedule only; skip pick generation.")
    ap.add_argument("--hr-supplement", action="store_true",
                    help="Also pull HR-only matches into the schedule "
                         "(matches HR has odds for that ESPN hasn't ingested "
                         "yet). Heavy HR fetch — operator action only.")
    args = ap.parse_args(argv)
    if args.days <= 1:
        res = ingest_schedule(args.date)
        print(f"\n  schedule: {res}")
        if args.hr_supplement:
            sup = ingest_schedule_from_hr()
            print(f"  hr_supplement: {sup}")
    else:
        res = ingest_schedule_window(args.date, days=args.days,
                                       with_hr_supplement=args.hr_supplement)
        print(f"\n  schedule window ({args.days}d):")
        for s in res["summaries"]:
            print(f"    {s}")
        if res.get("hr_supplement"):
            print(f"  hr_supplement: {res['hr_supplement']}")
    if not args.no_picks:
        # Run picker for each date in the window so tomorrow's
        # matches get their candidate picks recorded ahead of time.
        if args.days <= 1:
            picks = generate_picks(args.date)
            print(f"  picks:    {picks}")
        else:
            base = (datetime.strptime(args.date, "%Y-%m-%d")
                    if args.date else datetime.now())
            for i in range(args.days):
                d = (base + timedelta(days=i)).strftime("%Y-%m-%d")
                p = generate_picks(d)
                print(f"  picks[{d}]: {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["ingest_schedule", "ingest_schedule_window", "generate_picks"]
