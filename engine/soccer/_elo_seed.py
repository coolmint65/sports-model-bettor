"""External Elo seeding for the soccer framework.

Pulls current national-team Elo ratings from eloratings.net and writes
them into the ``team_elo`` table of the source league DB
(``fifa_internationals`` for World Cup purposes — see
``engine/soccer/_predict.py`` where the WC predictor reads from there).

eloratings.net runs the canonical international-football Elo system
(Wolfgang Hochberg's model, used by the FIFA Coca-Cola Ranking
Working Group's reference implementation). The in-house Elo we
compute from friendlies + qualifiers is a degenerate subset: it
misses confederation-weighted match importance, has a cold-start
problem for teams we haven't seen in our match history, and converges
to ~1500 (INIT_ELO) for everyone over its limited sample.

This module replaces those internal ratings with the externally-
validated set at seed time. The in-house update loop continues to
nudge ratings during the tournament — so a real WC result still moves
the dial — but the starting point is reality, not 1500-anchored
guesses (MEX was at 1450 in the internal sample; eloratings.net has
MEX above 1700).

Run via ``python -m engine.soccer._elo_seed`` — idempotent; safe to
re-run before each WC matchday to refresh.
"""
from __future__ import annotations

import logging
import urllib.request
from datetime import datetime, timezone

from ._db import get_conn

logger = logging.getLogger(__name__)


_ELO_URL = "https://www.eloratings.net/World.tsv"


# eloratings.net uses ISO 3166-1 alpha-2 codes. Our teams table uses
# ESPN's 3-letter codes (or league-specific abbreviations). Hand-built
# crosswalk covering every nation likely to appear in WC 2026 or any
# international friendly. Add entries as new federations qualify.
_ISO2_TO_OUR_ABBR: dict[str, str] = {
    # UEFA — eloratings.net uses some non-standard 2-letter codes:
    # EN for England, WA for Wales, SQ for Slovakia, NM for North
    # Macedonia, etc. Crosswalk covers both ISO 3166-1 alpha-2 and
    # eloratings' internal codes so a refresh of either source flows.
    "ES": "ESP", "FR": "FRA", "PT": "POR", "EN": "ENG", "GB-ENG": "ENG",
    "DE": "GER", "IT": "ITA", "NL": "NED", "BE": "BEL", "HR": "CRO",
    "CH": "SUI", "DK": "DEN", "AT": "AUT", "PL": "POL", "RS": "SRB",
    "UA": "UKR", "TR": "TUR", "WA": "WAL", "WLS": "WAL", "GB-WLS": "WAL",
    "SCT": "SCO", "GB-SCT": "SCO",
    "HU": "HUN", "RO": "ROU", "GR": "GRE",
    "CZ": "CZE", "BA": "BIH", "NO": "NOR", "SE": "SWE",
    "SK": "SVK",
    "FI": "FIN", "IS": "ISL", "MK": "MKD", "NM": "MKD",
    "AL": "ALB", "BG": "BUL",
    "ME": "MNE", "IE": "IRL", "EI": "IRL",
    "NIR": "NIR", "GB-NIR": "NIR",
    "GE": "GEO", "BY": "BLR", "MD": "MDA", "AZ": "AZE", "AM": "ARM",
    "LU": "LUX", "LV": "LVA", "LT": "LTU", "EE": "EST", "KZ": "KAZ",
    "MT": "MLT", "CY": "CYP", "SI": "SVN", "FO": "FRO",
    "XK": "KOS", "KV": "KOS", "KO": "KOS",
    "AD": "AND", "SM": "SMR", "GI": "GIB", "LI": "LIE", "IL": "ISR",
    "RU": "RUS",
    # CONMEBOL
    "AR": "ARG", "BR": "BRA", "UY": "URU", "CO": "COL", "CL": "CHI",
    "PY": "PAR", "EC": "ECU", "PE": "PER", "VE": "VEN", "BO": "BOL",
    # CONCACAF
    "US": "USA", "MX": "MEX", "CA": "CAN", "CR": "CRC", "PA": "PAN",
    "HN": "HON", "JM": "JAM", "SV": "SLV", "GT": "GUA", "HT": "HAI",
    "TT": "TRI", "CW": "CUW", "SR": "SUR", "GP": "GLP", "MQ": "MTQ",
    "DO": "DOM", "AG": "ATG", "CU": "CUB", "BB": "BRB", "GD": "GRN",
    "LC": "LCA", "VC": "VIN", "KN": "SKN", "AW": "ARU",
    "BM": "BER", "VG": "VGB", "BS": "BAH", "BZ": "BIZ",
    "KY": "CAY", "TC": "TCA", "DM": "DMA", "AI": "AIA",
    "VI": "VIR", "MS": "MSR", "PR": "PUR",
    # AFC
    "JP": "JPN", "KR": "KOR", "IR": "IRN", "AU": "AUS", "SA": "KSA",
    "QA": "QAT", "AE": "UAE", "CN": "CHN", "TH": "THA", "VN": "VIE",
    "IQ": "IRQ", "OM": "OMA", "UZ": "UZB", "JO": "JOR", "BH": "BHR",
    "IN": "IND", "ID": "IDN", "MY": "MAS", "SG": "SIN", "MM": "MYA",
    "PH": "PHI", "HK": "HKG", "TW": "TPE", "PK": "PAK", "BD": "BAN",
    "NP": "NEP", "SY": "SYR", "PS": "PLE", "LB": "LBN", "KG": "KGZ",
    "TJ": "TJK", "TM": "TKM", "MN": "MGL", "MV": "MDV", "BT": "BHU",
    "LK": "SRI", "MO": "MAC", "KH": "CAM", "LA": "LAO", "BN": "BRU",
    "TL": "TLS", "GU": "GUM", "AF": "AFG", "YE": "YEM", "KP": "PRK",
    # CAF
    "MA": "MAR", "SN": "SEN", "EG": "EGY", "TN": "TUN", "DZ": "ALG",
    "NG": "NGA", "GH": "GHA", "CM": "CMR", "CI": "CIV", "ZA": "RSA",
    "ML": "MLI", "BF": "BFA", "ZM": "ZAM", "AO": "ANG", "MZ": "MOZ",
    "MG": "MAD", "UG": "UGA", "TZ": "TAN", "KE": "KEN", "ZW": "ZIM",
    "BJ": "BEN", "TG": "TOG", "CD": "COD", "CG": "CGO", "GA": "GAB",
    "MR": "MTN", "GW": "GNB", "GN": "GUI", "SL": "SLE", "LR": "LBR",
    "GM": "GAM", "NE": "NIG", "CF": "CTA", "SD": "SUD", "SS": "SSD",
    "ET": "ETH", "ER": "ERI", "DJ": "DJI", "SO": "SOM", "RW": "RWA",
    "BI": "BDI", "MW": "MWI", "BW": "BOT", "NA": "NAM", "SZ": "SWZ",
    "LS": "LES", "LY": "LBA", "KM": "COM", "MU": "MRI", "SC": "SEY",
    "ST": "STP", "CV": "CPV", "GQ": "EQG",
    # OFC
    "NZ": "NZL", "FJ": "FIJ", "PG": "PNG", "SB": "SOL", "NC": "NCL",
    "VU": "VAN", "PF": "TAH", "TO": "TGA", "WS": "SAM", "AS": "ASA",
    "CK": "COK",
}


def _fetch_ratings() -> dict[str, float]:
    """Pull the current eloratings.net World.tsv and return a dict of
    {our_3letter_abbr: rating}. Skips any 2-letter code we don't have
    a crosswalk entry for — logs them so the gap can be patched."""
    req = urllib.request.Request(
        _ELO_URL,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        text = r.read().decode("utf-8", errors="replace")
    out: dict[str, float] = {}
    unknown: list[str] = []
    for line in text.splitlines():
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        try:
            iso2 = parts[2].strip()
            rating = float(parts[3])
        except (ValueError, IndexError):
            continue
        our_abbr = _ISO2_TO_OUR_ABBR.get(iso2)
        if not our_abbr:
            unknown.append(iso2)
            continue
        out[our_abbr] = rating
    if unknown:
        logger.info("[elo_seed] no crosswalk for: %s",
                    ", ".join(sorted(set(unknown))))
    logger.info("[elo_seed] pulled %d ratings from eloratings.net",
                 len(out))
    return out


def seed(league: str = "fifa_internationals", *,
         dry_run: bool = False) -> dict:
    """Overwrite team_elo with externally-validated ratings.

    Returns ``{"updated": N, "missing_team": [abbrs], "missing_rating":
    [abbrs]}`` so callers can spot crosswalk holes.
    """
    ratings = _fetch_ratings()
    conn = get_conn(league)
    teams = {
        (r["abbreviation"] or "").upper(): int(r["id"])
        for r in conn.execute("SELECT id, abbreviation FROM teams").fetchall()
        if r["abbreviation"]
    }
    now = datetime.now(timezone.utc).isoformat()
    updated = 0
    missing_team: list[str] = []
    missing_rating: list[str] = []
    for abbr, elo in ratings.items():
        tid = teams.get(abbr)
        if tid is None:
            missing_team.append(abbr)
            continue
        if not dry_run:
            conn.execute(
                "INSERT INTO team_elo(team_id, elo, matches_played, "
                "                       last_match_id, updated_at) "
                "VALUES (?, ?, COALESCE("
                "  (SELECT matches_played FROM team_elo "
                "    WHERE team_id = ?), 0), "
                "  (SELECT last_match_id FROM team_elo WHERE team_id = ?), "
                "  ?) "
                "ON CONFLICT(team_id) DO UPDATE SET "
                "  elo = excluded.elo, updated_at = excluded.updated_at",
                (tid, elo, tid, tid, now),
            )
        updated += 1
    # Teams in DB with no external rating — typically minor / inactive.
    for abbr in teams:
        if abbr not in ratings:
            missing_rating.append(abbr)
    if not dry_run:
        conn.commit()
    logger.info(
        "[elo_seed] %s updated=%d missing_team=%d missing_rating=%d",
        league, updated, len(missing_team), len(missing_rating),
    )
    return {
        "updated": updated,
        "missing_team": sorted(missing_team),
        "missing_rating": sorted(missing_rating),
    }


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    result = seed("fifa_internationals")
    print(json.dumps(result, indent=2))
