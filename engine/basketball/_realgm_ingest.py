"""RealGM ingest for basketball framework leagues.

Mirrors `engine.basketball._espn_ingest` but pulls from
`scrapers.realgm_basketball` instead of the ESPN scoreboard JSON.
RealGM is the data source for leagues whose ``data_source`` is
``"realgm"`` in ``LEAGUE_REGISTRY``: China CBA today, plus any
international league we onboard whose only public source is RealGM.

Three entry points (same shape as ESPN):

    ingest_teams(league)                        — populate teams table from standings
    ingest_today(league)                        — pull today's schedule, upsert games
    backfill(league, start_date, days)          — historical sweep for calibration
"""

from __future__ import annotations

import logging
import unicodedata
from datetime import datetime, timedelta, timezone

from ._config import get_league_config
from ._db import get_conn, teams_table, games_table, picks_table
from .._tz import et_today_str

logger = logging.getLogger(__name__)


# Generic words that shouldn't drive team-identity matches. Includes
# Spanish / Portuguese stopwords (the bulk of RealGM's non-NBA coverage
# is South America) plus club-type suffixes. Single-letter tokens and
# tokens < 4 chars are filtered separately.
_NAME_STOPWORDS = frozenset({
    "de", "do", "da", "dos", "das",
    "la", "el", "los", "las",
    "y", "und", "von", "of", "the",
    "fc", "sc", "sl", "ec", "ad", "ae", "sp", "mg", "rj", "br", "sk",
    "club", "clube", "clb",
    "basket", "basketball", "basquete", "basquetes",
    "tenis", "tennis", "tnis",
    "sky", "union", "uniao", "esporte", "sports",
    "centro", "associacion", "asociacion",
})


def _primary_tokens(name: str | None) -> set[str]:
    """Identifying tokens for team-name matching. Strips diacritics,
    lowercases, drops generic words + short tokens (<4 chars). Returns
    the bag of remaining 'primary' tokens. Empty set when nothing
    survives the filter (shouldn't happen for real team names)."""
    return set(_primary_tokens_ordered(name))


def _primary_tokens_ordered(name: str | None) -> list[str]:
    """Same filter as _primary_tokens but preserves token order — used
    to prefer earlier (= more identifying) tokens in match scoring."""
    if not name:
        return []
    n = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    n = n.lower()
    n = "".join(c if c.isalnum() or c == " " else " " for c in n)
    return [t for t in n.split()
            if len(t) >= 4 and t not in _NAME_STOPWORDS]


# ── League id / slug map ────────────────────────────────────────
# ``hr_comp_id`` lives in the registry already; RealGM uses its own
# integer id and a slug. Hardcoded here so the registry doesn't need
# two additional fields per RealGM-backed league.

_REALGM_LEAGUES: dict[str, tuple[int, str]] = {
    "china_cba":     (40, "Chinese-CBA"),
    "australia_nbl": (5,  "Australian-NBL"),
    "nz_nbl":        (75, "New-Zealand-NBL"),
    "brazil_nbb":    (59, "Brazilian-NBB"),
    "argentina_lnb": (58, "Argentinian-Liga-A"),
    # Dominican LNB / Puerto Rico BSN / Korea KBL / Mexico LNBP / Japan B2 —
    # add as we onboard. Slug + id from RealGM directory at
    # basketball.realgm.com/international/leagues.
}


def _league_meta(league: str) -> tuple[int, str]:
    if league not in _REALGM_LEAGUES:
        raise ValueError(f"League {league!r} has no RealGM mapping")
    return _REALGM_LEAGUES[league]


# ── Abbreviation derivation ────────────────────────────────────

def _derive_abbreviation(name: str, used: set[str]) -> str:
    """Generate a 3-4 char uppercase abbreviation that doesn't collide
    with anything in ``used``. Strategy:
      1. First word, first 3 chars uppercase (e.g. "Beijing Shougang"
         → "BEI", "Shanghai Dongfang" → "SHA"). Collisions extend.
      2. If 3-char first word collides, append the FIRST char of the
         second word (e.g. "Beijing Shougang" → "BES", "Beijing
         BeiKong" → "BEB").
      3. Falls back to first 4 chars if no second word.
    """
    parts = (name or "").strip().split()
    if not parts:
        return ""
    first = parts[0].upper()
    base = first[:3]
    if base not in used:
        return base
    # Extend with the second word's first letter.
    if len(parts) >= 2:
        ext = base + parts[1][0].upper()
        if ext not in used:
            return ext
    # Last resort: first 4 chars of first word.
    if len(first) >= 4 and first[:4] not in used:
        return first[:4]
    # Even longer if forced.
    for n in range(5, len(first) + 1):
        if first[:n] not in used:
            return first[:n]
    return first


# ── Team ingest ────────────────────────────────────────────────

def ingest_teams(league: str) -> dict:
    """Pull RealGM standings to seed the per-league teams table.

    Idempotent — re-running updates the W/L derived metadata but keeps
    the assigned ``id`` / ``abbreviation`` stable so foreign-key refs
    in the games table don't break."""
    from scrapers.realgm_basketball import fetch_standings
    league_id, slug = _league_meta(league)
    standings = fetch_standings(league_id, slug)
    if not standings:
        return {"fetched": 0, "inserted": 0, "updated": 0}

    conn = get_conn(league)
    t_tbl = teams_table(league)

    # Existing abbreviations — used to avoid clashes during derivation.
    used = {r["abbreviation"] for r in conn.execute(
        f"SELECT abbreviation FROM {t_tbl} WHERE abbreviation IS NOT NULL"
    ).fetchall() if r["abbreviation"]}

    inserted = updated = 0
    for s in standings:
        team_name = s["team"]
        if not team_name:
            continue
        existing = conn.execute(
            f"SELECT id, abbreviation FROM {t_tbl} WHERE name = ?",
            (team_name,),
        ).fetchone()
        if existing:
            updated += 1
            continue
        abbr = _derive_abbreviation(team_name, used)
        used.add(abbr)
        conn.execute(
            f"INSERT INTO {t_tbl} "
            f"(name, abbreviation, city, venue, external_id, logo_url) "
            f"VALUES (?, ?, ?, ?, ?, ?)",
            (team_name, abbr, "", "", None, None),
        )
        inserted += 1
    conn.commit()
    logger.info("[%s] realgm teams: fetched=%d inserted=%d existing=%d",
                league, len(standings), inserted, updated)
    return {"fetched": len(standings), "inserted": inserted,
             "updated": updated}


# ── Game ingest ────────────────────────────────────────────────

def _team_id_lookup(conn, t_tbl: str) -> dict[str, int]:
    """Build {name_lower: id} for the league. RealGM ships variation in
    team display ('Beijing' vs 'Beijing Shougang'), so we also index
    the first word so HR-shipped abbreviated names still resolve."""
    out: dict[str, int] = {}
    for r in conn.execute(
        f"SELECT id, name FROM {t_tbl} WHERE name IS NOT NULL"
    ).fetchall():
        nm = (r["name"] or "").strip()
        out[nm.lower()] = int(r["id"])
        if " " in nm:
            out[nm.split()[0].lower()] = int(r["id"])
    return out


def _upsert_game(league: str, row: dict, season: int) -> bool:
    """Upsert one RealGM schedule row into the games table. Returns
    True when a write happened (insert or update)."""
    conn = get_conn(league)
    t_tbl = teams_table(league)
    g_tbl = games_table(league)

    lookup = _team_id_lookup(conn, t_tbl)
    home_id = lookup.get((row.get("home") or "").lower())
    away_id = lookup.get((row.get("away") or "").lower())
    if not (home_id and away_id):
        # Auto-register an unknown team — RealGM standings sometimes
        # lags new franchises. Better than silently dropping the game.
        used_abbrs = {r["abbreviation"] for r in conn.execute(
            f"SELECT abbreviation FROM {t_tbl} WHERE abbreviation IS NOT NULL"
        ).fetchall() if r["abbreviation"]}
        # Existing names for fuzzy-merge — RealGM occasionally ships a
        # one-char typo of an established team ("Aukland Tuatara" for
        # NZ NBL on 2026-05-13). Without similarity matching, the typo
        # auto-registers as a fresh team and the next slate ingest
        # produces a duplicate game tied to the typo'd id.
        #
        # Two-layer match:
        #   (a) SequenceMatcher ratio ≥0.90 — catches single-char swaps.
        #   (b) Token-overlap on primary identifying tokens — catches
        #       sponsor variants like "Minas" ↔ "Minas Tênis Clube" or
        #       "Franca" ↔ "Sesi Franca Basquete" that don't pass (a)
        #       because the strings are too long to fuzzy-match.
        from difflib import SequenceMatcher
        existing_names = [(r["id"], r["name"]) for r in conn.execute(
            f"SELECT id, name FROM {t_tbl} WHERE name IS NOT NULL"
        ).fetchall() if r["name"]]
        for raw_name, side in (("home", "home"), ("away", "away")):
            tnm = row.get(side) or ""
            if not tnm or lookup.get(tnm.lower()):
                continue
            # Fuzzy match: ratio ≥0.90 against an existing name reuses
            # that team. 0.90 catches single-char swaps in 10-char
            # names without merging genuinely-different short names.
            match_id = None
            for eid, ename in existing_names:
                if SequenceMatcher(None, tnm.lower(),
                                    ename.lower()).ratio() >= 0.90:
                    match_id = eid
                    break
            # Token-overlap fallback — only fires when ratio match
            # failed. Cross-references the new name against existing
            # names on shared primary tokens (≥4 chars, not stopwords).
            # Scoring approach: iterate every candidate, score overlap,
            # pick the highest-scoring match (instead of first-hit).
            # Avoids false-positives like "Atenas de Córdoba" matching
            # "Instituto Atletico Central Cordoba" on shared "cordoba"
            # when the correct match is "Asociacion Deportiva Atenas"
            # on shared "atenas".
            if match_id is None:
                new_toks_ordered = _primary_tokens_ordered(tnm)
                new_toks = set(new_toks_ordered)
                # Position weight: earlier tokens are stronger identifiers
                # (clubs lead with their own name, geographic / sponsor
                # context trails). Bonus halves for each position.
                pos_weight = {t: 1.0 / (i + 1)
                              for i, t in enumerate(new_toks_ordered)}
                if new_toks:
                    token_freq: dict[str, int] = {}
                    parsed_existing = []
                    for eid, ename in existing_names:
                        ex_toks = _primary_tokens(ename)
                        parsed_existing.append((eid, ename, ex_toks))
                        for t in ex_toks:
                            token_freq[t] = token_freq.get(t, 0) + 1

                    best_score = 0.0
                    best_candidate = None
                    for eid, ename, ex_toks in parsed_existing:
                        if not ex_toks:
                            continue
                        shared = new_toks & ex_toks
                        score = 0.0
                        if shared and min(len(new_toks), len(ex_toks)) <= 2:
                            # Rarity × position weight per shared token.
                            score = sum(
                                pos_weight.get(t, 0.1)
                                / token_freq.get(t, 1)
                                for t in shared
                            )
                            # Specificity tiebreaker — small bias toward
                            # candidates with fewer primary tokens (more
                            # focused name). Keeps the bias < 0.05 so it
                            # only flips on near-ties.
                            score += 0.05 / len(ex_toks)
                        else:
                            short = (new_toks if len(new_toks) <= len(ex_toks)
                                     else ex_toks)
                            long_ = (ex_toks if short is new_toks
                                     else new_toks)
                            if (short and len(short) >= 2
                                and all(any(len(s) >= 5
                                              and (s in lt or lt in s)
                                              for lt in long_)
                                          for s in short)):
                                score = 0.5 * len(short)
                        if score > best_score:
                            best_score = score
                            best_candidate = (eid, ename, shared, score)
                    if best_candidate and best_score >= 0.5:
                        eid, ename, shared, score = best_candidate
                        match_id = eid
                        logger.info(
                            "[realgm] name-merge: %r <-> %r "
                            "(shared %r, score=%.2f -> team id=%d)",
                            tnm, ename, sorted(shared) if shared else "(substr)",
                            score, eid,
                        )
            if match_id is not None:
                lookup[tnm.lower()] = match_id
                if " " in tnm:
                    lookup[tnm.split()[0].lower()] = match_id
                continue
            abbr = _derive_abbreviation(tnm, used_abbrs)
            used_abbrs.add(abbr)
            cur = conn.execute(
                f"INSERT INTO {t_tbl} "
                f"(name, abbreviation, city, venue, external_id, logo_url) "
                f"VALUES (?, ?, ?, ?, ?, ?)",
                (tnm, abbr, "", "", None, None),
            )
            new_id = int(cur.lastrowid)
            lookup[tnm.lower()] = new_id
            if " " in tnm:
                lookup[tnm.split()[0].lower()] = new_id
            existing_names.append((new_id, tnm))
        conn.commit()
        home_id = lookup.get((row.get("home") or "").lower())
        away_id = lookup.get((row.get("away") or "").lower())
        if not (home_id and away_id):
            return False

    # Stable per-game id derived from (date, home_team_id, away_team_id).
    # RealGM doesn't expose a numeric game_id on the schedule rows, and
    # we can't link to box score URLs without a separate lookup. Earlier
    # this hashed the team NAMES, which broke on RealGM team renames
    # (e.g. "Shanghai Sharks" → "Shanghai Sharks East") — the new name
    # produced a fresh game_id and the next upsert created a duplicate
    # row instead of updating. Team IDs are stable across renames.
    import hashlib
    seed = f"{row.get('date')}|{home_id}|{away_id}"
    game_id = "rg-" + hashlib.md5(seed.encode("utf-8")).hexdigest()[:10]

    # Dedup against any legacy row with a different game_id but same
    # natural key (date, home_team_id, away_team_id). Earlier ingest
    # versions hashed team NAMES, so a name rename produced a fresh
    # hash and a duplicate row; the picks pointing at the old hash
    # stayed orphaned (stuck pending) while the new row got the final
    # score (see china_cba + argentina_lnb 5/7-5/9 bug, 2026-05-11).
    # Reuse the existing row's game_id so this upsert merges into it.
    existing = conn.execute(
        f"SELECT game_id FROM {g_tbl} "
        f"WHERE date = ? AND home_team_id = ? AND away_team_id = ? "
        f"  AND game_id != ? LIMIT 1",
        (row.get("date"), home_id, away_id, game_id),
    ).fetchone()
    if existing:
        game_id = existing["game_id"]

    status = row.get("status") or "pre"
    # Translate RealGM status to our framework values.
    if status == "final":
        st_db = "final"
    elif status == "in":
        st_db = "in"
    else:
        st_db = "scheduled"

    home_score = row.get("home_score")
    away_score = row.get("away_score")
    iso_start = None
    if row.get("time_et"):
        # RealGM ships HH:MM ET. Convert to UTC ISO with Z suffix so the
        # frontend's `new Date(...)` parses unambiguously regardless of
        # the viewer's local TZ. Without this, naive "2026-05-08T05:00"
        # gets read as the viewer's local 5:00 AM — works for ET viewers
        # but mis-renders elsewhere, and any TZ misconfig flips the time.
        try:
            from datetime import datetime as _dt2
            try:
                from zoneinfo import ZoneInfo as _ZI
                _ET = _ZI("America/New_York")
            except Exception:
                _ET = None
            t_clean = row["time_et"].replace(" ET", "").strip()
            naive = _dt2.strptime(f"{row['date']} {t_clean}",
                                   "%Y-%m-%d %H:%M")
            if _ET is not None:
                aware_et = naive.replace(tzinfo=_ET)
                iso_start = aware_et.astimezone(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ")
            else:
                # Fallback: month-keyed offset. EDT (UTC-4) approx
                # mid-March through early November per US DST; EST
                # (UTC-5) the rest of the year. Off by an hour during
                # DST transition weekends only.
                m = int(row["date"].split("-")[1])
                offset_hours = 4 if 3 <= m <= 10 else 5
                iso_start = (naive + timedelta(hours=offset_hours)
                              ).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            iso_start = f"{row['date']}T{t_clean}"

    conn.execute(
        f"INSERT OR REPLACE INTO {g_tbl} "
        f"(game_id, date, start_time, home_team_id, away_team_id, "
        f" home_score, away_score, "
        f" home_q1, away_q1, home_q2, away_q2, "
        f" home_q3, away_q3, home_q4, away_q4, "
        f" status, season, external_id) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            game_id, row.get("date"), iso_start,
            home_id, away_id,
            home_score, away_score,
            row.get("home_q1"), row.get("away_q1"),
            row.get("home_q2"), row.get("away_q2"),
            row.get("home_q3"), row.get("away_q3"),
            row.get("home_q4"), row.get("away_q4"),
            st_db, season, game_id,
        ),
    )
    conn.commit()
    return True


def _current_season(league: str) -> int:
    """Pivot on the league's first season_month from the registry.
    NZ NBL starts in March, China CBA in October, Brazil NBB in October,
    etc. — a fixed September pivot stamped NZ games as the wrong season,
    making the standings query (which filters by season) return 0-0 for
    every team. Reading the pivot from the registry keeps every league
    self-consistent."""
    return _season_for_date(league, datetime.now())


def _season_for_date(league: str, dt: datetime) -> int:
    """Season-year for ``dt`` based on the league's season_months pivot.

    Backfill walks dates spanning multiple seasons; each row's season
    tag must match its actual date, not today's pivot. Without this the
    backfill stamped 2024-October games as season=2025 (current season),
    making the standings + season-filter queries return wrong rows."""
    cfg = get_league_config(league)
    months = cfg.get("season_months") or (10,)
    primary = months[0]
    return dt.year if dt.month >= primary else dt.year - 1


def ingest_today(league: str) -> dict:
    """Pull RealGM's today schedule + the last 3 days, upsert into games.
    Returns ``{ingested, updated, errors}``.

    Why the 3-day backwards window: RealGM doesn't publish final scores
    in real time — they show up 12-36h after the game ends. Without
    refetching past dates, a game ingested at scheduled-time stays
    ``status='scheduled'`` indefinitely, the games table never gets the
    final score, and any pending picks for that game stay pending
    forever. CBA + Argentine LNB picks from 2026-05-07 were stuck this
    way for 4 days before this fix landed.

    Far-east leagues (China/Japan/Korea/Australia/NZ) also pull tomorrow
    ET — their local evening tipoffs (7 PM local) land at 3-7 AM ET the
    next day, so games their fans consider "tonight" only appear on
    tomorrow's RealGM page."""
    from scrapers.realgm_basketball import (
        fetch_schedule_today, fetch_schedule_for_date,
    )
    from ._config import is_far_east
    league_id, slug = _league_meta(league)
    rows = list(fetch_schedule_today(league_id, slug))
    # Refetch any date that has pending picks so their games get their
    # final scores (RealGM publishes results 12-36h after a game ends).
    # Combined with a fixed 3-day lookback floor — caches the common
    # case where pending picks are recent and we still catch them.
    conn = get_conn(league)
    p_tbl = picks_table(league)
    pending_dates = {
        r[0] for r in conn.execute(
            f"SELECT DISTINCT date FROM {p_tbl} WHERE result IS NULL"
        ).fetchall() if r[0]
    }
    today = datetime.now()
    refetch_dates: set[str] = set(pending_dates)
    for back in range(1, 4):
        refetch_dates.add((today - timedelta(days=back)).strftime("%Y-%m-%d"))
    today_str = today.strftime("%Y-%m-%d")
    refetch_dates.discard(today_str)  # already fetched
    for d in sorted(refetch_dates):
        # Skip future-dated pending (e.g., a pick for tomorrow that
        # hasn't played yet — refetching pre-game schedules adds noise
        # without progressing the settlement).
        if d > today_str:
            continue
        try:
            rows.extend(fetch_schedule_for_date(league_id, slug, d))
        except Exception as e:
            logger.warning("[%s] realgm refetch %s failed: %s", league, d, e)
    if is_far_east(league):
        tmrw = (today + timedelta(days=1)).strftime("%Y-%m-%d")
        rows.extend(fetch_schedule_for_date(league_id, slug, tmrw))

    # SofaScore fallback — RealGM lags 12-36h on finals for small leagues
    # (NZ NBL 2026-05-13: pick id=16 stuck on FRA -4.5 because RealGM still
    # showed status='pre' 12h after the game finished). SofaScore publishes
    # same-day so we pull the trailing 3-day window and merge in any
    # finished games. ``_upsert_game`` derives game_id from
    # (date, home_team_id, away_team_id), so SofaScore rows with the same
    # natural key merge directly into the existing scheduled row instead
    # of creating duplicates.
    from ._config import sofascore_tournament_id as _sofa_tid
    sofa_tid = _sofa_tid(league)
    sofa_count = 0
    if sofa_tid:
        try:
            from scrapers.sofascore_basketball import fetch_results_window
            sofa_rows = fetch_results_window(sofa_tid, days_back=3)
            # Only keep rows the RealGM batch hasn't already covered as
            # final — saves the upsert work when both sources agree.
            sofa_rows = [r for r in sofa_rows if r.get("status") == "final"]
            sofa_count = len(sofa_rows)
            rows.extend(sofa_rows)
        except Exception as e:
            logger.warning("[%s] sofascore fallback failed: %s", league, e)

    season = _current_season(league)
    n_ok = 0
    for r in rows:
        try:
            if _upsert_game(league, r, season):
                n_ok += 1
        except Exception as e:
            logger.warning("[%s] upsert failed for %s @ %s (source=%s): %s",
                           league, r.get("away"), r.get("home"),
                           r.get("source") or "realgm", e)
    logger.info("[%s] realgm ingest_today: rows=%d upserted=%d sofa=%d",
                league, len(rows), n_ok, sofa_count)
    return {"ingested": n_ok, "fetched": len(rows),
             "sofascore_rows": sofa_count}


def backfill(league: str, start_date: str, days: int = 90) -> dict:
    """Walk ``days`` consecutive dates BACKWARD from ``start_date``
    (inclusive) and upsert every schedule row found. ``start_date``
    is YYYY-MM-DD; the window covers ``[start_date - days, start_date]``.

    Note: this walks BACKWARD whereas the ESPN backfill walks
    FORWARD between an explicit (start, end) pair. Don't try to
    pass an end_date here — pass days instead.

    Use this once per onboarding to seed the games table for Elo /
    calibration. Rate-limited to RealGM's 1.2s/request etiquette in
    the underlying scraper, so a 90-day backfill is ~108s of network."""
    from scrapers.realgm_basketball import fetch_schedule_for_date
    league_id, slug = _league_meta(league)
    base = datetime.strptime(start_date, "%Y-%m-%d")
    n_total = n_ok = 0
    for i in range(int(days)):
        d_dt = base - timedelta(days=i)
        d = d_dt.strftime("%Y-%m-%d")
        # Per-date season tag — backfill spans multiple seasons; each
        # row's tag must match its date so season-filter queries
        # (standings, _calibrate.fit, GBM season filters) bucket
        # correctly. Earlier this used `_current_season(league)` for
        # every row which dumped multi-year backfill into a single
        # season tag.
        season = _season_for_date(league, d_dt)
        rows = fetch_schedule_for_date(league_id, slug, d)
        n_total += len(rows)
        for r in rows:
            try:
                if _upsert_game(league, r, season):
                    n_ok += 1
            except Exception as e:
                logger.warning("[%s] backfill upsert failed %s: %s",
                               league, d, e)
    logger.info("[%s] realgm backfill: dates=%d rows=%d upserted=%d",
                league, days, n_total, n_ok)
    return {"days": days, "fetched": n_total, "ingested": n_ok}


def refresh(league: str = "china_cba") -> dict:
    """Compatibility shim for the worker hook (mirrors the hockey
    framework `refresh()` signature)."""
    return ingest_today(league)


def _cli() -> int:
    import argparse
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("league", help="e.g. china_cba")
    p.add_argument("--teams", action="store_true", help="ingest teams")
    p.add_argument("--today", action="store_true", help="ingest today")
    p.add_argument("--backfill", action="store_true",
                    help="backfill past N days")
    p.add_argument("--start", default=et_today_str())
    p.add_argument("--days", type=int, default=90)
    args = p.parse_args()
    if args.teams:
        print(ingest_teams(args.league))
    if args.today:
        print(ingest_today(args.league))
    if args.backfill:
        print(backfill(args.league, args.start, args.days))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
