"""Tennis surface inference from tournament name.

Source-agnostic helper used by ESPN ingest (`scrapers.tennis_espn`),
Tennis Explorer ingest (`engine.tennis_schedule.ingest_schedule_from_te`),
and the one-time backfill that fills NULL `surface` cells on existing
rows. ESPN doesn't ship surface; TE doesn't ship it on the day's
schedule page either. Best signal both share is the tournament name.

The map covers all four Slams, the ATP/WTA Masters/1000 swing, the
ATP 500 / WTA 500 tier, and a long tail of Challenger / WTA125 / ITF
events on each surface tier. Everything else falls back to ``"Hard"``,
which is correct ~70% of the time for sub-tour events held in indoor
arenas, US college venues, and Asian Tour stops.
"""

from __future__ import annotations


# ── Tournament → surface map ──────────────────────────────────
# Each tuple is (substring_match, surface). The first match wins, so
# more-specific names should appear before more-general ones (e.g.
# "Tokyo" → Hard before "Tokyo Open" if we ever needed it). Keys are
# lower-cased; comparisons are substring matches.

_CLAY = "Clay"
_GRASS = "Grass"
_HARD = "Hard"

_SURFACE_MAP: tuple[tuple[str, str], ...] = (
    # ── Grand Slams ──
    ("australian open", _HARD),
    ("us open", _HARD),
    ("roland garros", _CLAY),
    ("french open", _CLAY),
    ("internazionali bnl", _CLAY),  # Rome under its full name
    ("wimbledon", _GRASS),

    # ── Masters / 1000 ──
    ("indian wells", _HARD),
    ("miami open", _HARD),
    ("rome", _CLAY),
    ("madrid", _CLAY),
    ("monte carlo", _CLAY),
    ("monte-carlo", _CLAY),
    ("toronto", _HARD),
    ("canadian open", _HARD),
    ("rogers cup", _HARD),
    ("cincinnati", _HARD),
    ("shanghai", _HARD),
    ("paris masters", _HARD),
    ("rolex paris", _HARD),
    ("bnp paribas open", _HARD),

    # ── Year-end finals ──
    ("atp finals", _HARD),
    ("wta finals", _HARD),
    ("next gen", _HARD),
    ("nitto atp", _HARD),

    # ── ATP 500 / WTA 500 + named clay swing stops ──
    ("barcelona", _CLAY),
    ("hamburg", _CLAY),
    ("bastad", _CLAY),
    ("kitzbuhel", _CLAY),
    ("kitzbühel", _CLAY),
    ("umag", _CLAY),
    ("gstaad", _CLAY),
    ("stuttgart", _GRASS),  # ATP only — clay until 2014, grass since
    ("estoril", _CLAY),
    ("houston", _CLAY),
    ("geneva", _CLAY),
    ("lyon", _CLAY),
    ("munich", _CLAY),
    ("rio open", _CLAY),
    ("santiago", _CLAY),
    ("buenos aires", _CLAY),
    ("cordoba", _CLAY),
    ("córdoba", _CLAY),
    ("marrakech", _CLAY),
    ("acapulco", _HARD),
    ("dubai", _HARD),
    ("doha", _HARD),
    ("rotterdam", _HARD),
    ("delray beach", _HARD),
    ("memphis", _HARD),
    ("brisbane", _HARD),
    ("auckland", _HARD),
    ("adelaide", _HARD),
    ("sydney", _HARD),
    ("hobart", _HARD),
    ("eastbourne", _GRASS),
    ("birmingham", _GRASS),
    ("nottingham", _GRASS),
    ("queen's club", _GRASS),
    ("queens club", _GRASS),
    ("halle", _GRASS),
    ("'s-hertogenbosch", _GRASS),
    ("hertogenbosch", _GRASS),
    ("mallorca", _GRASS),
    ("newport", _GRASS),
    ("washington", _HARD),
    ("citi open", _HARD),
    ("winston-salem", _HARD),
    ("zhuhai", _HARD),
    ("beijing", _HARD),
    ("china open", _HARD),
    ("japan open", _HARD),
    ("tokyo", _HARD),
    ("seoul", _HARD),
    ("hong kong", _HARD),
    ("vienna", _HARD),
    ("basel", _HARD),
    ("antwerp", _HARD),
    ("st petersburg", _HARD),
    ("st. petersburg", _HARD),
    ("st-petersburg", _HARD),
    ("moscow", _HARD),
    ("kremlin cup", _HARD),
    ("metz", _HARD),
    ("sofia", _HARD),
    ("marseille", _HARD),
    ("montpellier", _HARD),
    ("pune", _HARD),
    ("chennai", _HARD),
    ("singapore", _HARD),

    # ── Generic surface keywords in lower-tier event names ──
    # Some Challenger / ITF tournaments encode surface in their name.
    (" clay", _CLAY),
    ("clay court", _CLAY),
    (" grass", _GRASS),
    ("grass court", _GRASS),
    (" hard", _HARD),
    ("hard court", _HARD),
    ("indoor", _HARD),
    ("carpet", _HARD),  # historical; collapse to Hard for modeling

    # ── Notable Challenger / sub-tour clay strongholds ──
    ("santos", _CLAY),
    ("francavilla", _CLAY),
    ("aix-en-provence", _CLAY),
    ("perugia", _CLAY),
    ("biella", _CLAY),
    ("oeiras", _CLAY),
    ("split", _CLAY),
    ("zadar", _CLAY),
    ("trieste", _CLAY),
    ("santa margherita di pula", _CLAY),
    ("monastir", _HARD),  # ITF Tunisia hardcourt cluster
    ("antalya", _HARD),
    ("hurghada", _HARD),
    ("sharm el sheikh", _HARD),
    ("doha futures", _HARD),
    ("kazakhstan", _HARD),
    ("almaty", _HARD),
    ("nur-sultan", _HARD),
    ("astana", _HARD),
    ("changwon", _HARD),
    ("nakhon pathom", _HARD),
    ("bangkok", _HARD),
    ("wuxi", _HARD),
    ("jiangxi", _HARD),
    ("jiujiang", _HARD),
    ("kunming", _HARD),
    ("istanbul", _HARD),
    ("bonita springs", _HARD),
    ("indian harbour", _HARD),
    ("saint-gaudens", _CLAY),
    ("lopota", _CLAY),
    ("kalmar", _CLAY),
    ("reichstett", _CLAY),
    ("belgrade", _CLAY),
    ("fukuoka", _HARD),
    ("yokohama", _HARD),
    ("kashima", _HARD),
    ("luan", _HARD),
    ("tumkur", _HARD),
    ("brazzaville", _CLAY),
    ("mbombela", _HARD),
    ("islamabad", _HARD),
    ("nairobi", _HARD),
    ("addis ababa", _HARD),

    # ── UTR Pro Tennis Series (year-round indoor hardcourt) ──
    ("utr pro", _HARD),

    # ── Wuning + similar generic indoor stops ──
    ("wuning", _HARD),
)


def infer_surface(tournament: str | None) -> str | None:
    """Return ``"Hard"`` / ``"Clay"`` / ``"Grass"`` for ``tournament``,
    or ``None`` when the name is empty. Default when no rule matches
    is ``"Hard"`` — most indoor / unknown events.

    Match uses word-boundary semantics so short needles (``halle``,
    ``rome``) don't false-positive against longer words (``challenger``
    contains ``halle``; that miscoded a chunk of Challenger Cagliari /
    Aix-en-Provence as Grass before this fix).
    """
    if not tournament:
        return None
    def _norm(s: str) -> str:
        out = " " + s.lower().strip() + " "
        for sep in ("-", ".", "/", "(", ")", ",", "'", "’"):
            out = out.replace(sep, " ")
        # Collapse repeated whitespace.
        return " ".join(out.split()).join((" ", " "))
    name = _norm(tournament)
    for needle, surface in _SURFACE_MAP:
        n = needle.lower()
        # Sandwich short, non-spaced needles to enforce word-boundary
        # matching ("halle" must not match inside "challenger"). Needles
        # that already lead/trail with a space keep their own boundary.
        if n.startswith(" ") or n.endswith(" "):
            pat = n
        else:
            pat = f" {n.strip()} "
        # Normalize the needle's separators the same way we normalize
        # the tournament — so a map entry "monte-carlo" matches both
        # "Monte-Carlo Masters" AND "Monte Carlo Open".
        pat = _norm(pat).strip()
        pat = f" {pat} " if pat else ""
        if pat and pat in name:
            return surface
    return _HARD
