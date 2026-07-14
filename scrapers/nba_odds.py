"""NBA odds orchestrator — merges Hard Rock + ESPN sources.

Source chain (each merges without overwriting populated keys):
  1. Hard Rock Bet (FL operator, free, full slate + Q1 alts)
  2. ESPN summary/core endpoints (pickcenter Q1 markets — fallback only
     when Hard Rock doesn't supply Q1 lines)

Returns dict keyed by "AWAY@HOME" with the schema described inline
in the per-source scrapers.
"""

import logging

logger = logging.getLogger(__name__)


def _has_q1_data(odds_map: dict) -> bool:
    """True iff at least one game in the map has a Q1 market populated."""
    for v in (odds_map or {}).values():
        if (v.get("q1_spread") is not None
                or v.get("q1_total") is not None
                or v.get("q1_home_ml") is not None):
            return True
    return False


def _merge_odds(base: dict, extra: dict) -> dict:
    """Merge extra-source odds into base without overwriting populated keys."""
    out = dict(base)
    for key, payload in (extra or {}).items():
        existing = out.get(key, {}) or {}
        for k, v in (payload or {}).items():
            if v is None:
                continue
            if existing.get(k) in (None, 0, ""):
                existing[k] = v
        out[key] = existing
    return out


def fetch_all_nba_odds() -> dict:
    """Fetch NBA odds with a multi-source fallback chain. Each source
    merges into the running map without overwriting populated keys."""
    odds: dict = {}

    # 1. Hard Rock (preferred — FL operator + Q1 alts)
    try:
        from .hardrock_odds import fetch_nba as fetch_hr_nba
        hr = fetch_hr_nba()
        if hr:
            odds = _merge_odds(odds, hr)
            logger.info("NBA odds: %d games from Hard Rock", len(hr))
    except Exception as e:
        logger.debug("Hard Rock NBA odds failed: %s", e)

    # ESPN pickcenter fallback (full-game only — no Q1)
    if not _has_q1_data(odds):
        try:
            from .nba_espn_odds import fetch_nba_espn_odds
            espn = fetch_nba_espn_odds()
            if espn:
                odds = _merge_odds(odds, espn)
                logger.info("NBA odds fallback: merged %d ESPN games", len(espn))
        except Exception as e:
            logger.debug("ESPN NBA odds fallback failed: %s", e)

    return odds


# Back-compat shim: the diagnose endpoint and a couple of legacy call
# sites still reference fetch_nba_odds (the original-style single-source
# entry point). Redirect to the orchestrator so they keep working.
def fetch_nba_odds() -> dict:
    return fetch_all_nba_odds()
