"""Logo backfill for basketball framework leagues.

Walks every league with a SofaScore tournament_id (see
``SOFASCORE_TOURNAMENT_IDS`` in ``_config``), pulls the team list from
SofaScore, matches names against our DB teams using the same token-
overlap heuristic as the team-dedup path, and persists missing logo
URLs.

SofaScore exposes team logos via a stable CDN endpoint:
    https://api.sofascore.com/api/v1/team/{id}/image

We store the URL (not the bytes) — the frontend's <img> tag fetches on
demand, with onError fallthrough to the initial-letter placeholder.

Idempotent: only writes when the existing logo_url is NULL or empty.
Re-runs are no-ops once every team has a logo.

Usage:
    python -m engine.basketball._logo_backfill                # all leagues
    python -m engine.basketball._logo_backfill nz_nbl china_cba  # subset
"""
from __future__ import annotations

import logging
import sys

from ._config import LEAGUE_REGISTRY, sofascore_tournament_id
from ._db import get_conn, teams_table
from ._realgm_ingest import _primary_tokens

logger = logging.getLogger(__name__)


def _best_db_match(sofa_name: str, db_teams: list[tuple[int, str]]
                    ) -> int | None:
    """Pick the DB team that best matches a SofaScore team name. Mirrors
    the scoring in ``_realgm_ingest._upsert_game``'s dedup pass:
      1. exact case-insensitive name match
      2. token-overlap with rarity + position weights

    Returns the DB team id or None."""
    sofa_l = sofa_name.lower().strip()
    # Exact
    for tid, name in db_teams:
        if (name or "").lower().strip() == sofa_l:
            return tid
    # Token overlap
    sofa_toks_ordered = list(_primary_tokens(sofa_name))
    sofa_toks = set(sofa_toks_ordered)
    if not sofa_toks:
        return None
    token_freq: dict[str, int] = {}
    parsed: list[tuple[int, set[str]]] = []
    for tid, name in db_teams:
        toks = _primary_tokens(name)
        parsed.append((tid, toks))
        for t in toks:
            token_freq[t] = token_freq.get(t, 0) + 1
    best_score, best_id = 0.0, None
    for tid, toks in parsed:
        if not toks:
            continue
        shared = sofa_toks & toks
        # Score the identical-token path when it has support, AND also
        # try the substring path. Take the higher score. This handles
        # the case where two names share a weak token ("oeste") AND have
        # strong substring relationships ("ferrocarril" vs "ferro" +
        # "carril") — without trying both, the weak shared token
        # short-circuits the stronger substring evidence.
        ident_score = 0.0
        if shared:
            if min(len(sofa_toks), len(toks)) <= 2 or len(shared) >= 2:
                ident_score = sum(1.0 / token_freq.get(t, 1) for t in shared)
        # Substring path — runs regardless of shared tokens.
        short = sofa_toks if len(sofa_toks) <= len(toks) else toks
        long_ = toks if short is sofa_toks else sofa_toks
        substr_score = 0.0
        if (len(short) >= 2
            and all(any(len(s) >= 5 and (s in lt or lt in s) for lt in long_)
                    for s in short)):
            substr_score = 0.5 * len(short)
        score = max(ident_score, substr_score)
        if score > best_score:
            best_score = score
            best_id = tid
    return best_id if best_score >= 0.5 else None


# Hosts known to be dead / broken. URLs from these are treated as
# missing — the backfill re-resolves via SofaScore. Argentina's
# ``lnb.com.ar`` 301-redirects to laliganacional.com.ar which doesn't
# serve the legacy ``/escudos/{id}/{n}`` paths, breaking every Argentine
# team logo in the UI (2026-05-13 user report).
_BROKEN_LOGO_HOSTS = (
    "lnb.com.ar/",
    "/escudos/",   # relative LNB paths (no host)
)


def _is_broken(url: str | None) -> bool:
    if not url:
        return True
    u = url.strip()
    if not u:
        return True
    return any(b in u for b in _BROKEN_LOGO_HOSTS)


def backfill_league(league: str) -> dict:
    """Backfill missing team logos for ``league`` via SofaScore.
    Returns ``{teams_total, missing_before, matched, updated, skipped}``.

    Teams whose existing logo_url points at a known-broken host (see
    ``_BROKEN_LOGO_HOSTS``) are treated as missing and re-resolved.
    """
    sofa_tid = sofascore_tournament_id(league)
    if not sofa_tid:
        return {"error": f"no sofascore_tournament_id for {league!r}"}
    try:
        from scrapers.sofascore_basketball import fetch_teams, team_logo_url
    except Exception as e:
        return {"error": f"sofascore scraper unavailable: {e}"}

    sofa_teams = fetch_teams(sofa_tid)
    if not sofa_teams:
        return {"error": "sofascore returned no teams (off-season?)"}

    conn = get_conn(league)
    t_tbl = teams_table(league)
    db_rows = conn.execute(
        f"SELECT id, name, logo_url FROM {t_tbl}"
    ).fetchall()
    teams_total = len(db_rows)
    missing = [(r["id"], r["name"]) for r in db_rows
                if _is_broken(r["logo_url"])]
    matched = updated = skipped = 0
    for db_id, db_name in missing:
        sofa_id = None
        # Pass 1: name-match against the league's standings+events.
        candidates = [(sid, snm) for sid, snm in sofa_teams.items()]
        best = _best_db_match(db_name, candidates)
        if best is not None:
            sofa_id = best
        else:
            # Pass 2: SofaScore team-search fallback. Picks up teams
            # that aren't on the current standings (relegated / inactive
            # franchises we still carry in our historical games table).
            try:
                from scrapers.sofascore_basketball import search_team
                country = LEAGUE_REGISTRY.get(league, {}).get("country")
                hit = search_team(db_name, sport="Basketball", country=country)
                if hit and hit.get("id"):
                    sofa_id = int(hit["id"])
                    logger.info(
                        "[%s] logo backfill via search: %r -> sofa_id=%d %r",
                        league, db_name, sofa_id, hit.get("name"),
                    )
            except Exception as e:
                logger.debug("[%s] search fallback for %r failed: %s",
                             league, db_name, e)
        if sofa_id is None:
            skipped += 1
            continue
        matched += 1
        url = team_logo_url(sofa_id)
        conn.execute(
            f"UPDATE {t_tbl} SET logo_url = ? WHERE id = ?",
            (url, db_id),
        )
        updated += 1
        logger.info("[%s] logo backfill: %r -> sofa_id=%d url=%s",
                    league, db_name, sofa_id, url)
    conn.commit()
    return {
        "teams_total": teams_total,
        "missing_before": len(missing),
        "matched": matched,
        "updated": updated,
        "skipped": skipped,
    }


def backfill_all() -> dict[str, dict]:
    """Run ``backfill_league`` on every league with a SofaScore id."""
    out: dict[str, dict] = {}
    for league in LEAGUE_REGISTRY:
        if not sofascore_tournament_id(league):
            continue
        try:
            out[league] = backfill_league(league)
        except Exception as e:
            out[league] = {"error": str(e)}
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(message)s")
    targets = sys.argv[1:] if len(sys.argv) > 1 else None
    if targets:
        for lg in targets:
            print(f"=== {lg} ===")
            print(backfill_league(lg))
    else:
        results = backfill_all()
        for lg, res in results.items():
            print(f"{lg}: {res}")
