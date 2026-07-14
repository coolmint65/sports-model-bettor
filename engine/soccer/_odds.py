"""HR odds fetcher for soccer competitions.

Mirrors ``engine.basketball._odds`` in structure — goes straight to
``scrapers.hardrock_odds._fetch_events_for_comp(comp_id)`` and parses
soccer-shaped markets locally. The shared scraper's ``_parse_response``
hardcodes US team-name maps; soccer ships world-wide club + nation
names so we keep the parsing here.

Public:
    fetch_league_odds(league) -> dict[match_key, odds_dict]

Match key is ``f"{away_abbr}@{home_abbr}"`` to match the framework's
matches table on the standard HOME @ AWAY convention. HR's
``eventNames.name`` ships as ``"<Away> @ <Home>"`` so the away token
arrives first; we always store home/away by ``homeAway`` field rather
than by position.

Markets parsed (verified live 2026-05-14 via MLS comp probe):

    SOCCER:FT:AXB   — 1X2 winner. Selections A=home, X=draw, B=away.
                       (HR's A/B follow participants[0]/participants[1]
                       order; for the AXB market that's home/draw/away.)
    SOCCER:FT:OU    — total goals. Selections Over/Under x.5; line in
                       selection ``name`` (no separate handicap field).
                       Multiple alt-line copies ship — caller picks the
                       primary by juice-closest-to-100 logic.
    SOCCER:FT:WINBOTHTEAMSTOSCORE — BTTS. Selections Yes/No.
    SOCCER:FT:DNB   — Draw No Bet. A=home, B=away (no draw cell).
    SOCCER:FT:DBLC  — Double Chance. AX = home-or-draw, XB =
                       draw-or-away, AB = home-or-away (no draw).
    SOCCER:FT:AHCP  — Asian Handicap. AH=home-with-handicap,
                       BH=away-with-handicap. Line in the selection
                       ``name`` string ("Home Team -0.5").

Returned ``odds_dict`` shape::

    {
      "_key": "ARS@WHU",
      "comp": "England - Premier League",
      "hr_event_id": "...",
      "home_abbr": "WHU", "away_abbr": "ARS",
      "home_full_name": "West Ham United",
      "away_full_name": "Arsenal",
      "start_time": "2026-08-...Z",

      # 1X2
      "ml":   {"home": int, "draw": int, "away": int},
      # Total goals (primary main line — usually 2.5)
      "total": {"line": float, "over_odds": int, "under_odds": int},
      "alt_totals": [{"line": float, "over_odds": int, "under_odds": int}, ...],
      # Both teams to score
      "btts": {"yes": int, "no": int},
      # Draw-no-bet
      "dnb":  {"home": int, "away": int},
      # Double chance
      "dc":   {"home_draw": int, "draw_away": int, "home_away": int},
      # Asian handicap (primary line — usually -0.5 / +0.5)
      "ah":   {"line": float, "home_odds": int, "away_odds": int},
      "alt_ah": [{"line": float, "home_odds": int, "away_odds": int}, ...],
    }
"""
from __future__ import annotations

import logging
import time
import unicodedata
from typing import Any

from ._config import get_league_config
from ._db import get_conn

logger = logging.getLogger(__name__)


# Per-process odds cache. 2-min TTL — short enough that line moves catch
# during a slate refresh, long enough that a tab flip doesn't re-hit HR.
_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 120


# ── Public ────────────────────────────────────────────────

def fetch_league_odds(league: str, force: bool = False) -> dict[str, dict]:
    """Fetch HR odds for ``league``. Returns ``{match_key: odds_dict}``
    keyed by ``"<away_abbr>@<home_abbr>"``.

    Empty dict on any failure — the picks pipeline should fall through
    to "no odds = no picks" rather than crash."""
    cfg = get_league_config(league)
    comp_id = cfg.get("hr_comp_id")
    if not comp_id:
        logger.debug("[soccer:%s] no hr_comp_id — skipping", league)
        return {}
    now = time.time()
    cached = _CACHE.get(league)
    if cached and not force and (now - cached[0]) < _CACHE_TTL_S:
        return cached[1]

    try:
        from scrapers.hardrock_odds import _fetch_events_for_comp
    except Exception as e:
        logger.warning("[soccer:%s] HR scraper import failed: %s", league, e)
        return {}

    events, err = _fetch_events_for_comp(str(comp_id))
    if err or not events:
        if err:
            logger.warning("[soccer:%s] HR comp_id=%s fetch failed: %s",
                            league, comp_id, err)
        _CACHE[league] = (now, {})
        return {}

    name_to_abbr = _build_name_lookup(league)
    out: dict[str, dict] = {}
    for ev in events:
        parsed = _parse_event(
            ev, name_to_abbr,
            league_name=cfg.get("hr_comp_name") or league,
        )
        if parsed:
            out[parsed["_key"]] = {k: v for k, v in parsed.items()
                                     if not k.startswith("_")}
    _CACHE[league] = (now, out)
    logger.info("[soccer:%s] HR fetched %d matches (comp_id=%s)",
                league, len(out), comp_id)
    return out


# ── Name resolution ─────────────────────────────────────────

def _strip_accents(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s or "")
    out = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Normalize compound-name separators so "Bosnia-Herzegovina" (DB) and
    # "Bosnia & Herzegovina" (HR) resolve against the same key. Collapse
    # runs of whitespace afterward so the join stays clean.
    out = out.replace("&", " ").replace("-", " ")
    return " ".join(out.split())


# Per-league hard aliases for HR display names that don't fuzzy-resolve
# to our team table. HR's display names for MLS use the expanded
# city-form ("Los Angeles FC") while our teams table stores the
# popular acronym ("LAFC") — there's no shared token so the fuzzy
# resolver returns None or the wrong club (Chicago Fire FC matched on
# the trailing "FC"). Per-league overrides win before any fuzzy logic.
#
# Keys are lowercase ASCII of the HR-shipped name; values are the
# canonical team abbreviation in our teams table.
_HR_NAME_ALIASES: dict[str, dict[str, str]] = {
    "mls": {
        "los angeles fc": "LAFC",
        "los angeles galaxy": "LA",
        # football-data short name DC United doesn't share any token
        # with our "D.C. United" DB row (the periods break tokenization),
        # and "united" alone is ambiguous post-deconflict.
        "dc united": "DC",
        "d.c. united": "DC",
    },
    "eng_premier": {
        # football-data calls Wolverhampton Wanderers "Wolves" — not a
        # token of the canonical full name in our teams table.
        "wolves": "WOL",
        "nott'm forest": "NFO",
    },
    "esp_laliga": {
        "espanol":    "ESP",
        "espanyol":   "ESP",
        "valladolid": "VLL",
    },
    "ger_bundesliga": {
        "m'gladbach":          "BMG",
        "borussia m'gladbach": "BMG",
        "monchengladbach":     "BMG",
    },
    "fra_ligue1": {
        "lyon":      "LYON",
        "paris sg":  "PSG",
        "st etienne": "ASSE",
        "saint etienne": "ASSE",
    },
    "arg_lpf": {
        "ind. rivadavia":      "RIV",
        "independiente rivadavia": "RIV",
        "gimnasia l.p.":       "GLP",
        "gimnasia lp":         "GLP",
        "estudiantes l.p.":    "EST",
        "estudiantes lp":      "EST",
        "union de santa fe":   "USF",
    },
}


def _build_name_lookup(league: str) -> dict[str, str]:
    """Multi-spelling team-name → abbreviation index for one league.

    HR ships club names with sponsors (``Bayer 04 Leverkusen`` vs our
    ``Leverkusen``) and varying suffixes (``FC``, ``CF``, ``Club``).
    We index the abbreviation, the full name, each space-split token
    of length ≥ 4, and a few targeted fallbacks (first word, last word)
    so the resolver can find a hit regardless of how HR formats the
    name."""
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT id, name, abbreviation FROM teams"
    ).fetchall()
    out: dict[str, str] = {}
    # When a generic token like "city" or "united" gets added by two
    # different teams (Manchester City vs New York City), we must NOT
    # silently keep the first writer — it would silently misroute every
    # subsequent lookup. Track ambiguous keys + remove them from the
    # final map so the resolver falls through to a full-name match
    # instead of hitting the wrong abbreviation.
    _ambiguous: set[str] = set()

    def add(key: str, abbr: str) -> None:
        if not key or not abbr:
            return
        norm = _strip_accents(key).strip().lower()
        if not norm:
            return
        existing = out.get(norm)
        if existing is None:
            out[norm] = abbr
            return
        if existing != abbr:
            _ambiguous.add(norm)

    for r in rows:
        abbr = (r["abbreviation"] or "").strip()
        name = (r["name"] or "").strip()
        if not abbr:
            continue
        add(abbr, abbr)
        if not name:
            continue
        add(name, abbr)
        tokens = name.split()
        # Filter out noise tokens ("fc", "sc", "club" …) before any
        # token-level indexing — they appear in many clubs' names and
        # the substring fallback in _resolve_team would otherwise let
        # "Los Angeles FC" hit Chicago Fire's lookup entry "fc". Same
        # filter the resolver applies on the query side; pruning here
        # keeps the table self-consistent.
        meaningful = [t for t in tokens if t.lower() not in _NOISE_TOKENS]
        if meaningful:
            add(meaningful[0], abbr)
            add(meaningful[-1], abbr)
        for tok in meaningful:
            if len(tok) >= 4:
                add(tok, abbr)

    # Drop ambiguous keys — these belonged to multiple teams during the
    # build and any single-token lookup against them would silently
    # misroute. The resolver's full-name path still works because the
    # full normalized name was added once (and is therefore unambiguous).
    for k in _ambiguous:
        out.pop(k, None)
    # Layer per-league HR aliases on top — written last so they win
    # over any clashing fuzzy index entry (e.g. "los angeles" alone
    # would otherwise hit LA Galaxy via its "los" token).
    for hr_name, target_abbr in (_HR_NAME_ALIASES.get(league) or {}).items():
        out[hr_name] = target_abbr
    return out


# Stop-words HR likes to pad club names with. Stripping these before
# fuzzy match lifts resolution rate on European leagues that ship
# heavily-suffixed names. Order doesn't matter — set membership only.
_NOISE_TOKENS = {
    "fc", "afc", "cf", "sc", "ac", "as", "bk", "if", "tsv", "vfl",
    "vfb", "borussia", "club", "the",
}


def _resolve_team(name: str, name_to_abbr: dict[str, str]) -> str | None:
    if not name:
        return None
    n = _strip_accents(name).strip().lower()
    if n in name_to_abbr:
        return name_to_abbr[n]
    tokens = [t for t in n.split() if t not in _NOISE_TOKENS]
    if tokens:
        joined = " ".join(tokens)
        if joined in name_to_abbr:
            return name_to_abbr[joined]
        for tok in (tokens[-1], tokens[0]):
            # Require ≥3 chars on the token so a 2-letter prefix like
            # "RB" doesn't sweep every team whose name contains "rb" as
            # a substring (e.g. "Paderborn" → RBL because "rb" is inside
            # "paderborn").
            if len(tok) >= 3 and tok in name_to_abbr:
                return name_to_abbr[tok]
    # Last-resort: bidirectional substring (bounded by team count, ~20).
    # Require key length ≥4 so 2-3 letter keys don't false-match against
    # unrelated team names that incidentally contain those letters.
    for k, abbr in name_to_abbr.items():
        if k and len(k) >= 4 and (k in n or n in k):
            return abbr
    return None


def _synth_abbr(team_name: str) -> str | None:
    if not team_name:
        return None
    first = team_name.strip().split()[0]
    return first.upper()[:4] if first else None


# ── Odds helpers ────────────────────────────────────────────

def _decimal_to_american(dec: float | None) -> int | None:
    """HR ships decimal odds; the rest of our pipeline expects American.
    Same conversion as basketball._odds."""
    if dec is None:
        return None
    try:
        d = float(dec)
    except (TypeError, ValueError):
        return None
    if d <= 1.0:
        return None
    if d >= 2.0:
        return int(round((d - 1.0) * 100))
    return int(round(-100 / (d - 1.0)))


def _selection_odds_american(sel: dict) -> int | None:
    """HR puts the decimal price on different keys depending on the
    market generation. ``odds`` is the canonical key in the comp-events
    response; ``priceDecimal`` lingers on a few legacy markets."""
    val = sel.get("odds")
    if val is None:
        val = sel.get("priceDecimal")
    return _decimal_to_american(val)


def _line_from_name(name: str | None) -> float | None:
    """Extract a numeric line ('Over 2.5', '-1.5', '+0.5') from the
    selection's display name. Soccer OU + AH lines live in the name,
    not in a structured handicap field."""
    if not name:
        return None
    for tok in name.replace("+", " ").replace("-", " -").split():
        try:
            return float(tok)
        except ValueError:
            continue
    return None


def _start_time_iso(ev: dict) -> str | None:
    et = ev.get("eventTime")
    if not isinstance(et, (int, float)) or et <= 0:
        return None
    from datetime import datetime as _dt, timezone as _tz
    try:
        return _dt.fromtimestamp(et / 1000.0, tz=_tz.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, OSError):
        return None


def _split_canonical(s: str) -> tuple[str, str]:
    """Returns ``(away_name, home_name)`` parsed from HR's
    ``eventNames.name``. Three delimiters in the wild:

      * ``" at "`` — basketball-style ``"<Away> at <Home>"``
      * ``" @ "``  — same convention, short form
      * ``" vs "`` — soccer flips this: ``"<Home> vs <Away>"``
        (verified 2026-05-14 — HR ships MLS as
        ``"CF Montréal vs Chicago Fire"`` where CF Montréal is home)

    We always return (away, home) so callers don't have to remember
    which delimiter implies which order."""
    if " at " in s:
        a, b = s.split(" at ", 1)
        return a.strip(), b.strip()
    if " @ " in s:
        a, b = s.split(" @ ", 1)
        return a.strip(), b.strip()
    if " vs " in s:
        a, b = s.split(" vs ", 1)
        # HR's "vs" puts HOME first — flip to keep the (away, home) tuple.
        return b.strip(), a.strip()
    return ("", "")


# Distance from -100 (even). Lower = closer to true main line. Picks the
# primary OU / AH from a bundle of alt lines.
def _juice_distance(american: int | None) -> int:
    if american is None:
        return 9999
    return abs(int(american) - (-100))


# ── Per-market extractors ────────────────────────────────

def _extract_1x2(market: dict) -> dict | None:
    """SOCCER:FT:AXB. Selection type A = home, X = draw, B = away."""
    sels = market.get("selection") or market.get("selections") or []
    out = {}
    for s in sels:
        t = (s.get("type") or "").upper()
        am = _selection_odds_american(s)
        if am is None:
            continue
        if t == "A":
            out["home"] = am
        elif t == "X":
            out["draw"] = am
        elif t == "B":
            out["away"] = am
    if "home" in out and "away" in out:
        return out
    return None


def _extract_dnb(market: dict) -> dict | None:
    """SOCCER:FT:DNB. Two selections — A=home, B=away. Refund on draw."""
    sels = market.get("selection") or market.get("selections") or []
    out = {}
    for s in sels:
        t = (s.get("type") or "").upper()
        am = _selection_odds_american(s)
        if am is None:
            continue
        if t == "A":
            out["home"] = am
        elif t == "B":
            out["away"] = am
    if "home" in out and "away" in out:
        return out
    return None


def _extract_dc(market: dict) -> dict | None:
    """SOCCER:FT:DBLC. AX=home-or-draw, XB=draw-or-away, AB=home-or-away."""
    sels = market.get("selection") or market.get("selections") or []
    out = {}
    for s in sels:
        t = (s.get("type") or "").upper()
        am = _selection_odds_american(s)
        if am is None:
            continue
        if t == "AX":
            out["home_draw"] = am
        elif t == "XB":
            out["draw_away"] = am
        elif t == "AB":
            out["home_away"] = am
    if out:
        return out
    return None


def _extract_ou(market: dict) -> dict | None:
    """SOCCER:FT:OU — over/under total goals."""
    sels = market.get("selection") or market.get("selections") or []
    line = None
    over_odds = None
    under_odds = None
    for s in sels:
        t = (s.get("type") or "").lower()
        am = _selection_odds_american(s)
        ln = _line_from_name(s.get("name"))
        if ln is not None:
            line = ln
        if t == "over":
            over_odds = am
        elif t == "under":
            under_odds = am
    if line is None or over_odds is None or under_odds is None:
        return None
    return {"line": line, "over_odds": over_odds, "under_odds": under_odds}


def _extract_btts(market: dict) -> dict | None:
    """SOCCER:FT:WINBOTHTEAMSTOSCORE. Selections Yes / No."""
    sels = market.get("selection") or market.get("selections") or []
    yes_odds = no_odds = None
    for s in sels:
        t = (s.get("type") or "").lower()
        am = _selection_odds_american(s)
        if t == "yes":
            yes_odds = am
        elif t == "no":
            no_odds = am
    if yes_odds is None or no_odds is None:
        return None
    return {"yes": yes_odds, "no": no_odds}


def _extract_cs(market: dict) -> dict | None:
    """SOCCER:FT:CS — Correct Score. Selections labeled like ``1-0``,
    ``2-1`` etc. Returns a flat dict keyed by score string."""
    sels = market.get("selection") or market.get("selections") or []
    out: dict[str, int] = {}
    for s in sels:
        # HR's ``type`` field IS the "H-A" score string for CS. Skip
        # cells with malformed types or missing prices.
        key = (s.get("type") or "").strip()
        # Sanity: score format is "N-N" with 0-9 on each side.
        if not key or "-" not in key:
            continue
        try:
            h, a = key.split("-")
            int(h); int(a)
        except (ValueError, AttributeError):
            continue
        am = _selection_odds_american(s)
        if am is None:
            continue
        out[key] = am
    return out or None


def _extract_htft(market: dict) -> dict | None:
    """SOCCER:FT:HTFT — Half-time / Full-time. HR ships selections
    keyed as ``A/A``, ``A/X``, ``A/B``, ``X/A``, ``X/X``, ``X/B``,
    ``B/A``, ``B/X``, ``B/B`` where A=home, X=draw, B=away. Normalize
    to ``H/D/A`` used elsewhere in the codebase."""
    _MAP = {"A": "H", "X": "D", "B": "A"}
    sels = market.get("selection") or market.get("selections") or []
    out: dict[str, int] = {}
    for s in sels:
        t = (s.get("type") or "").upper().replace(" ", "")
        if "/" not in t:
            continue
        try:
            ht, ft = t.split("/", 1)
        except ValueError:
            continue
        ht_n = _MAP.get(ht.strip())
        ft_n = _MAP.get(ft.strip())
        if ht_n is None or ft_n is None:
            continue
        am = _selection_odds_american(s)
        if am is None:
            continue
        out[f"{ht_n}/{ft_n}"] = am
    return out or None


def _extract_gibh(market: dict) -> dict | None:
    """SOCCER:FT:GOALINBOTHHALVES — Yes/No both halves see a goal."""
    sels = market.get("selection") or market.get("selections") or []
    yes = no = None
    for s in sels:
        t = (s.get("type") or "").lower()
        am = _selection_odds_american(s)
        if t == "yes":
            yes = am
        elif t == "no":
            no = am
    if yes is None or no is None:
        return None
    return {"yes": yes, "no": no}


def _extract_wm(market: dict) -> dict | None:
    """SOCCER:FT:WM — Winning Margin. HR ships types like
    ``>=+4`` / ``+3`` / ``+2`` / ``Score Draw`` / ``-1`` / ``<=-4``.
    Also ``0-0`` for goalless draw sometimes. Normalize to
    ``home+N`` / ``draw`` / ``away+N`` keys the predictor uses."""
    sels = market.get("selection") or market.get("selections") or []
    out: dict[str, int] = {}
    for s in sels:
        raw = (s.get("type") or "").strip()
        name = (s.get("name") or "").lower()
        am = _selection_odds_american(s)
        if am is None:
            continue
        key: str | None = None
        # Goalless draw + score draw both map to the draw bucket. Our
        # prediction combines all draws — matching book granularity
        # would require an added split we don't model.
        if "draw" in name or raw.lower() == "score draw" or raw == "0-0":
            key = "draw"
        elif raw.startswith(">=+") or raw.startswith("+4") or "4+" in name:
            key = "home+4"
        elif raw.startswith("<=-") or raw.startswith("-4") or "-4+" in raw:
            key = "away+4"
        elif raw.startswith("+"):
            try:
                n = int(raw.lstrip("+"))
                if 1 <= n <= 3:
                    key = f"home+{n}"
                elif n >= 4:
                    key = "home+4"
            except ValueError:
                pass
        elif raw.startswith("-"):
            try:
                n = int(raw.lstrip("-"))
                if 1 <= n <= 3:
                    key = f"away+{n}"
                elif n >= 4:
                    key = "away+4"
            except ValueError:
                pass
        elif raw.isdigit():
            # bare "3" — direction inferred from name
            n = int(raw)
            if "by" in name or "home" in name:
                key = f"home+{n}" if n <= 3 else "home+4"
        if key:
            # Sum in case book splits a bucket (e.g., "Draw 1-1" +
            # "Draw 2-2" both mapping to "draw")
            existing = out.get(key)
            out[key] = am if existing is None else min(existing, am)
    return out or None


def _extract_advance(market: dict) -> dict | None:
    """SOCCER:FT:WTWWF — "To Advance". Selections: A=home, B=away.
    Knockout-stage only; HR doesn't ship this on group games."""
    sels = market.get("selection") or market.get("selections") or []
    home_odds = away_odds = None
    for s in sels:
        t = (s.get("type") or "").upper()
        am = _selection_odds_american(s)
        if t == "A":
            home_odds = am
        elif t == "B":
            away_odds = am
    if home_odds is None or away_odds is None:
        return None
    return {"home": home_odds, "away": away_odds}


def _extract_ah(market: dict) -> dict | None:
    """SOCCER:FT:AHCP — asian handicap. AH=home line, BH=away line."""
    sels = market.get("selection") or market.get("selections") or []
    home_odds = away_odds = home_line = away_line = None
    for s in sels:
        t = (s.get("type") or "").upper()
        am = _selection_odds_american(s)
        ln = _line_from_name(s.get("name"))
        if t == "AH":
            home_odds = am
            home_line = ln
        elif t == "BH":
            away_odds = am
            away_line = ln
    line = home_line if home_line is not None else (
        -away_line if away_line is not None else None)
    if line is None or home_odds is None or away_odds is None:
        return None
    return {"line": line, "home_odds": home_odds, "away_odds": away_odds}


def _pick_main_total(cands: list[dict]) -> dict | None:
    if not cands:
        return None
    return min(cands, key=lambda c: (_juice_distance(c.get("over_odds"))
                                       + _juice_distance(c.get("under_odds"))))


def _pick_main_ah(cands: list[dict]) -> dict | None:
    if not cands:
        return None
    return min(cands, key=lambda c: (_juice_distance(c.get("home_odds"))
                                       + _juice_distance(c.get("away_odds"))))


# ── Per-event parse ─────────────────────────────────────────

def _parse_event(ev: dict, name_to_abbr: dict[str, str],
                  league_name: str) -> dict | None:
    parts = ev.get("participants") or []
    if len(parts) < 2:
        return None
    en = ev.get("eventNames") or {}
    canonical = en.get("name") if isinstance(en, dict) else ""
    away_name, home_name = _split_canonical(canonical or "")
    if not (away_name and home_name):
        away_name = (parts[0].get("name") or "").strip()
        home_name = (parts[1].get("name") or "").strip()

    home_abbr = _resolve_team(home_name, name_to_abbr)
    away_abbr = _resolve_team(away_name, name_to_abbr)
    if not home_abbr:
        home_abbr = _synth_abbr(home_name)
    if not away_abbr:
        away_abbr = _synth_abbr(away_name)
    if not (home_abbr and away_abbr) or home_abbr == away_abbr:
        return None

    odds: dict[str, Any] = {
        "_key": f"{away_abbr}@{home_abbr}",
        "comp": league_name,
        "hr_event_id": ev.get("id") or ev.get("eventId"),
        "home_abbr": home_abbr,
        "away_abbr": away_abbr,
        "home_full_name": home_name,
        "away_full_name": away_name,
        "start_time": _start_time_iso(ev),
    }

    ou_cands: list[dict] = []
    ah_cands: list[dict] = []
    # H1 (first-half period) markets — SOCCER:P:* family.
    h1_ou_cands: list[dict] = []
    h1_odds: dict[str, Any] = {}
    for m in ev.get("markets") or []:
        mtype = (m.get("type") or "").upper()
        # Team-specific markets (A:WINBOTHTEAMSTOSCORE etc.) have ":A:"
        # or ":B:" in the type — exclude.
        if ":A:" in mtype or ":B:" in mtype:
            continue

        # H1 (period) markets. HR groups all per-period soccer markets
        # under SOCCER:P:*; H1 vs H2 is differentiated by the
        # ``period`` field carrying '1' / '2' (or '1H' / '2H' depending
        # on the upstream). v1 only consumes H1 since that's the bucket
        # the picker emits — H2 follows when we add second-half picks.
        if mtype.startswith("SOCCER:P:"):
            period = (m.get("period") or "").upper()
            if period not in ("", "M", "1", "1H", "H1"):
                continue
            if mtype == "SOCCER:P:AXB":
                ml = _extract_1x2(m)
                if ml and "ml" not in h1_odds:
                    h1_odds["ml"] = ml
            elif mtype == "SOCCER:P:DNB":
                dnb = _extract_dnb(m)
                if dnb and "dnb" not in h1_odds:
                    h1_odds["dnb"] = dnb
            elif mtype == "SOCCER:P:DBLC":
                dc = _extract_dc(m)
                if dc and "dc" not in h1_odds:
                    h1_odds["dc"] = dc
            elif mtype == "SOCCER:P:OU":
                ou = _extract_ou(m)
                if ou:
                    h1_ou_cands.append(ou)
            elif mtype == "SOCCER:P:BTS":
                btts = _extract_btts(m)
                if btts and "btts" not in h1_odds:
                    h1_odds["btts"] = btts
            continue

        # Period 'M' is the full match (90 min). Other periods are
        # H1/H2 half markets — handled above.
        period = (m.get("period") or "").upper()
        if period and period != "M":
            continue

        if mtype == "SOCCER:FT:AXB":
            ml = _extract_1x2(m)
            if ml and "ml" not in odds:
                odds["ml"] = ml
        elif mtype == "SOCCER:FT:DNB":
            dnb = _extract_dnb(m)
            if dnb and "dnb" not in odds:
                odds["dnb"] = dnb
        elif mtype == "SOCCER:FT:DBLC":
            dc = _extract_dc(m)
            if dc and "dc" not in odds:
                odds["dc"] = dc
        elif mtype == "SOCCER:FT:OU":
            ou = _extract_ou(m)
            if ou:
                ou_cands.append(ou)
        elif mtype in ("SOCCER:FT:BTS", "SOCCER:FT:WINBOTHTEAMSTOSCORE"):
            # HR ships two variants: SOCCER:FT:BTS is the plain
            # Yes/No selection (what we want); WINBOTHTEAMSTOSCORE
            # ships on some competitions. Kept both so cup + league
            # feeds resolve uniformly. Team-specific ":A:" / ":B:"
            # variants were already filtered upstream.
            btts = _extract_btts(m)
            if btts and "btts" not in odds:
                odds["btts"] = btts
        elif mtype == "SOCCER:FT:CS":
            cs = _extract_cs(m)
            if cs and "cs" not in odds:
                odds["cs"] = cs
        elif mtype == "SOCCER:FT:HTFT":
            htft = _extract_htft(m)
            if htft and "htft" not in odds:
                odds["htft"] = htft
        elif mtype == "SOCCER:FT:GOALINBOTHHALVES":
            gibh = _extract_gibh(m)
            if gibh and "gibh" not in odds:
                odds["gibh"] = gibh
        elif mtype == "SOCCER:FT:WM":
            wm = _extract_wm(m)
            if wm and "wm" not in odds:
                odds["wm"] = wm
        elif mtype == "SOCCER:FT:WTWWF":
            adv = _extract_advance(m)
            if adv and "advance" not in odds:
                odds["advance"] = adv
        elif mtype == "SOCCER:FT:AHCP":
            ah = _extract_ah(m)
            if ah:
                ah_cands.append(ah)

    # Promote primary main lines + keep the alts available for picks
    # that target a specific alt line.
    if ou_cands:
        odds["total"] = _pick_main_total(ou_cands)
        odds["alt_totals"] = sorted(
            ou_cands, key=lambda c: (c.get("line") or 0.0)
        )
    if ah_cands:
        odds["ah"] = _pick_main_ah(ah_cands)
        odds["alt_ah"] = sorted(
            ah_cands, key=lambda c: (c.get("line") or 0.0)
        )

    if h1_ou_cands:
        h1_odds["total"] = _pick_main_total(h1_ou_cands)
        h1_odds["alt_totals"] = sorted(
            h1_ou_cands, key=lambda c: (c.get("line") or 0.0)
        )
    if h1_odds:
        odds["h1"] = h1_odds

    # Drop matches with zero usable markets — keeps the picks engine
    # from being asked to evaluate a row with no prices.
    if not any(k in odds for k in
                ("ml", "total", "btts", "dnb", "dc", "ah", "h1")):
        return None
    return odds
