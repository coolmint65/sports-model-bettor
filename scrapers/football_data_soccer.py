"""football-data.co.uk closing-odds scraper.

Pulls Pinnacle closing odds (PSCH/PSCD/PSCA + totals + Asian
handicap) from football-data.co.uk's public CSV exports. Two URL
patterns are supported:

  * EU per-league per-season:
        /mmz4281/{SEASON}/{LEAGUE}.csv
    where SEASON is YYYY (e.g. 2425 for 2024-25) and LEAGUE is
    E0/SP1/I1/D1/F1/P1/N1/... — 380-ish rows per league per season.

  * New-format multi-season (non-EU):
        /new/{COUNTRY}.csv
    where COUNTRY is USA/ARG/BRA/MEX/... — one CSV per country with
    every season inline. Different column shape (Country/League/
    Season/Date/Time/Home/Away/HG/AG/Res/PSCH/PSCD/PSCA/...).

The two schemas are reconciled into a single row shape and written
to ``data/soccer/{league_key}/historical_odds.db`` keyed by
``(season, date, home, away)``. Match rate against our DB is brittle
on team names, so a per-league alias map lives in this module —
extend it when a new league onboards.

Public API:

    fetch_league_season(league_key, season) -> list[dict]
    backfill(league_key, *, seasons=None) -> dict
    _LEAGUE_SPECS                         (table of supported leagues)

Pinnacle is the public closing-odds gold standard — they keep their
line on the board to the last minute and rarely move it after the
market settles. Useful as the V3.1 "market-as-feature" input even
though we don't actually bet there (geo-blocked in FL).
"""
from __future__ import annotations

import csv
import io
import logging
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


# Per-league specs. `format`:
#   "eu"   -> /mmz4281/{season}/{code}.csv, season-scoped
#   "new"  -> /new/{code}.csv, multi-season; the row carries Season +
#              League so we can filter post-fetch
_LEAGUE_SPECS: dict[str, dict] = {
    "eng_premier":    {"format": "eu",  "code": "E0",   "league_label": None},
    "esp_laliga":     {"format": "eu",  "code": "SP1",  "league_label": None},
    "ita_seriea":     {"format": "eu",  "code": "I1",   "league_label": None},
    "ger_bundesliga": {"format": "eu",  "code": "D1",   "league_label": None},
    "fra_ligue1":     {"format": "eu",  "code": "F1",   "league_label": None},
    "mls":            {"format": "new", "code": "USA",  "league_label": "MLS"},
    "arg_lpf":        {"format": "new", "code": "ARG",
                         "league_label": "Liga Profesional"},
    "bra_seriea":     {"format": "new", "code": "BRA",  "league_label": "Serie A"},
}


_BASE_URL = "https://www.football-data.co.uk"
_HEADERS = {"User-Agent": "Mozilla/5.0 (sports-bettor-v3.1-soccer)"}


# Per-league team-name normalization. football-data uses short names
# ("Man United", "Wolves") while our DB uses canonical full names
# ("Manchester United", "Wolverhampton Wanderers"). Map left-side as
# normalized lowercase key -> the abbreviation (3-letter ISO-style)
# the soccer DB stores. Extend per league as match-rate audits land.
_TEAM_ALIASES: dict[str, dict[str, str]] = {
    "eng_premier": {
        # Built ad-hoc from football-data EPL CSV. Casefold-compared.
        "man united": "MUN", "manchester united": "MUN",
        "man city": "MCI", "manchester city": "MCI",
        "newcastle": "NEW", "newcastle united": "NEW",
        "tottenham": "TOT", "spurs": "TOT",
        "wolves": "WOL", "wolverhampton wanderers": "WOL",
        "nott'm forest": "NFO", "nottingham forest": "NFO",
        "leicester": "LEI", "leicester city": "LEI",
        "ipswich": "IPS", "ipswich town": "IPS",
        "west ham": "WHU", "west ham united": "WHU",
        "brighton": "BHA", "brighton & hove albion": "BHA",
    },
    "esp_laliga": {
        "ath bilbao": "ATH", "athletic bilbao": "ATH", "athletic club": "ATH",
        "ath madrid": "ATM", "atletico madrid": "ATM",
        "real madrid": "RMA",
        "barcelona": "BAR",
        "betis": "BET", "real betis": "BET",
        "celta": "CEL", "celta vigo": "CEL",
        "sociedad": "RSO", "real sociedad": "RSO",
        "mallorca": "MLL",
        "valladolid": "VLD", "real valladolid": "VLD",
        "leganes": "LEG", "leganés": "LEG",
        "vallecano": "RAY", "rayo vallecano": "RAY",
    },
    "ita_seriea": {
        "inter": "INT", "internazionale": "INT",
        "juventus": "JUV",
        "milan": "MIL", "ac milan": "MIL",
        "napoli": "NAP",
        "roma": "ROM", "as roma": "ROM",
        "lazio": "LAZ",
        "fiorentina": "FIO",
        "atalanta": "ATA",
        "bologna": "BOL",
        "verona": "VER", "hellas verona": "VER",
    },
    "ger_bundesliga": {
        "bayern munich": "BAY", "bayern münchen": "BAY", "bayern": "BAY",
        "dortmund": "BVB", "borussia dortmund": "BVB",
        "leverkusen": "B04", "bayer leverkusen": "B04",
        "leipzig": "RBL", "rb leipzig": "RBL",
        "stuttgart": "VFB", "vfb stuttgart": "VFB",
        "frankfurt": "SGE", "eintracht frankfurt": "SGE",
        "wolfsburg": "WOB", "vfl wolfsburg": "WOB",
        "ein frankfurt": "SGE",
        "freiburg": "SCF", "sc freiburg": "SCF",
        "m'gladbach": "BMG", "borussia monchengladbach": "BMG",
        "borussia mönchengladbach": "BMG",
        "union berlin": "UNB", "fc union berlin": "UNB",
        "st pauli": "STP", "fc st. pauli": "STP",
    },
    "fra_ligue1": {
        "paris sg": "PSG", "paris saint-germain": "PSG", "psg": "PSG",
        "marseille": "OM", "olympique marseille": "OM",
        "lyon": "LYO", "olympique lyonnais": "LYO",
        "monaco": "MON", "as monaco": "MON",
        "lille": "LIL", "losc lille": "LIL",
        "nice": "NCE", "ogc nice": "NCE",
        "rennes": "REN", "stade rennais": "REN",
        "lens": "LEN", "rc lens": "LEN",
        "strasbourg": "STR",
        "st etienne": "ASS", "saint-etienne": "ASS",
        "le havre": "LEH",
    },
    "mls": {
        "la galaxy": "LA",
        "lafc": "LAFC", "los angeles fc": "LAFC",
        "new york red bulls": "RBNY", "ny red bulls": "RBNY",
        "new york city": "NYC", "new york city fc": "NYC",
        "cf montreal": "MTL",
        "minnesota united": "MIN",
        "real salt lake": "RSL",
        "sporting kansas city": "SKC", "sporting kc": "SKC",
        "fc dallas": "DAL",
        "fc cincinnati": "CIN",
        "dc united": "DC",
        "columbus crew": "CLB",
        "toronto fc": "TOR",
        "vancouver whitecaps": "VAN",
        "seattle sounders": "SEA",
        "portland timbers": "POR",
        "atlanta united": "ATL",
        "philadelphia union": "PHI",
        "orlando city": "ORL",
        "houston dynamo": "HOU",
        "colorado rapids": "COL",
        "chicago fire": "CHI",
        "nashville sc": "NSH",
        "inter miami": "MIA",
        "charlotte fc": "CLT",
        "austin fc": "ATX",
        "st louis city": "STL", "st. louis city": "STL",
        "san diego fc": "SD",
    },
    "arg_lpf": {
        "river plate": "RIV",
        "boca juniors": "BOC",
        "racing club": "RAC",
        "san lorenzo": "SLO",
        "estudiantes": "EST", "estudiantes lp": "EST",
        "velez sarsfield": "VEL",
        "newell's old boys": "NOB", "newells old boys": "NOB",
        "huracan": "HUR", "huracán": "HUR",
        "atl tucuman": "AT", "atletico tucuman": "AT",
        "gimnasia": "GIM", "gimnasia lp": "GIM",
        "talleres": "TAL", "talleres cordoba": "TAL",
        "rosario central": "RSC",
        "godoy cruz": "GOD",
        "instituto": "INS",
    },
    "bra_seriea": {
        "flamengo rj": "FLA", "flamengo": "FLA",
        "palmeiras": "PAL",
        "corinthians": "COR",
        "sao paulo": "SPO", "são paulo": "SPO",
        "santos": "SAN",
        "internacional": "INT",
        "gremio": "GRE", "grêmio": "GRE",
        "cruzeiro": "CRU",
        "atletico-mg": "CAM", "atletico mineiro": "CAM",
        "atlético-mg": "CAM",
        "fluminense": "FLU",
        "vasco": "VAS", "vasco da gama": "VAS",
        "athletico-pr": "CAP", "athletico paranaense": "CAP",
        "botafogo rj": "BOT", "botafogo": "BOT",
        "bahia": "BAH",
        "fortaleza": "FOR",
        "ceara": "CEA", "ceará": "CEA",
    },
}


def _data_dir(league_key: str) -> Path:
    p = (Path(__file__).resolve().parent.parent
         / "data" / "soccer" / league_key)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _db_path(league_key: str) -> Path:
    return _data_dir(league_key) / "historical_odds.db"


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS historical_odds (
            id           INTEGER PRIMARY KEY,
            league       TEXT NOT NULL,
            season       TEXT NOT NULL,
            match_date   TEXT NOT NULL,
            home_name    TEXT NOT NULL,
            away_name    TEXT NOT NULL,
            home_abbr    TEXT,
            away_abbr    TEXT,
            psch         REAL,
            pscd         REAL,
            psca         REAL,
            pc_over25    REAL,
            pc_under25   REAL,
            pcahh        REAL,
            pcaha        REAL,
            fthg         INTEGER,
            ftag         INTEGER,
            fetched_at   TEXT DEFAULT (datetime('now')),
            UNIQUE(league, season, match_date, home_name, away_name)
        );
        CREATE INDEX IF NOT EXISTS idx_ho_date
            ON historical_odds(match_date);
        CREATE INDEX IF NOT EXISTS idx_ho_abbr
            ON historical_odds(home_abbr, away_abbr);
    """)
    conn.commit()


def _normalize_team(name: str, league_key: str) -> str | None:
    if not name:
        return None
    key = name.strip().lower()
    return _TEAM_ALIASES.get(league_key, {}).get(key)


def _parse_date(s: str) -> str | None:
    """football-data uses DD/MM/YYYY (or DD/MM/YY pre-2002). Return
    ISO YYYY-MM-DD; None on parse failure."""
    if not s:
        return None
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _float_or_none(v) -> float | None:
    if v in (None, "", "NA"):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _int_or_none(v) -> int | None:
    if v in (None, "", "NA"):
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


def _fetch_csv(url: str, *, retries: int = 3, throttle: float = 0.4
                ) -> list[dict]:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=20) as r:
                raw = r.read()
            data = raw.decode("utf-8-sig", errors="replace")
            reader = csv.DictReader(io.StringIO(data))
            return list(reader)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.debug("[football-data] 404 %s", url)
                return []
            last_err = e
        except Exception as e:
            last_err = e
        time.sleep(throttle * (attempt + 1))
    if last_err:
        logger.warning("[football-data] fetch failed: %s (%s)", url, last_err)
    return []


def fetch_league_season(league_key: str, season: str | None = None
                        ) -> list[dict]:
    """Fetch one league × one season (or for new-format leagues, the
    full multi-season CSV filtered to ``season`` when supplied).

    ``season`` shape:
      * EU format: ``"2425"`` for 2024-25
      * New format: ``"2024/2025"`` or ``"2024"`` (Brazilian calendar
        years). Pass None to ingest every season the file ships.

    Returns parsed rows ready for ``store_rows`` — no DB write here.
    """
    spec = _LEAGUE_SPECS.get(league_key)
    if not spec:
        raise ValueError(f"no football-data spec for {league_key!r}")
    if spec["format"] == "eu":
        if not season:
            raise ValueError(f"EU format requires season (e.g. '2425')")
        url = f"{_BASE_URL}/mmz4281/{season}/{spec['code']}.csv"
    else:
        url = f"{_BASE_URL}/new/{spec['code']}.csv"
    rows_raw = _fetch_csv(url)
    out: list[dict] = []
    for r in rows_raw:
        if spec["format"] == "eu":
            row = _parse_eu_row(r, league_key, season)
        else:
            row = _parse_new_row(r, league_key, spec.get("league_label"),
                                  season)
        if row:
            out.append(row)
    logger.info("[football-data:%s] %s: %d rows", league_key,
                season or "all-seasons", len(out))
    return out


def _parse_eu_row(r: dict, league_key: str, season: str) -> dict | None:
    date = _parse_date(r.get("Date"))
    home = (r.get("HomeTeam") or "").strip()
    away = (r.get("AwayTeam") or "").strip()
    if not (date and home and away):
        return None
    return {
        "league":     league_key,
        "season":     season,
        "match_date": date,
        "home_name":  home,
        "away_name":  away,
        "home_abbr":  _normalize_team(home, league_key),
        "away_abbr":  _normalize_team(away, league_key),
        "psch":       _float_or_none(r.get("PSCH")),
        "pscd":       _float_or_none(r.get("PSCD")),
        "psca":       _float_or_none(r.get("PSCA")),
        "pc_over25":  _float_or_none(r.get("PC>2.5")),
        "pc_under25": _float_or_none(r.get("PC<2.5")),
        "pcahh":      _float_or_none(r.get("PCAHH")),
        "pcaha":      _float_or_none(r.get("PCAHA")),
        "fthg":       _int_or_none(r.get("FTHG")),
        "ftag":       _int_or_none(r.get("FTAG")),
    }


def _parse_new_row(r: dict, league_key: str, league_label: str | None,
                    season_filter: str | None) -> dict | None:
    # New-format files mix multiple competitions per country. ARG.csv
    # has Liga Profesional + Copa de la Liga; BRA.csv has Serie A +
    # Serie B; USA.csv has MLS + USL maybe — filter by league_label
    # when the registry specifies one.
    league_field = (r.get("League") or "").strip()
    if league_label and league_label not in league_field:
        return None
    season = (r.get("Season") or "").strip()
    if season_filter and season != season_filter:
        return None
    date = _parse_date(r.get("Date"))
    home = (r.get("Home") or "").strip()
    away = (r.get("Away") or "").strip()
    if not (date and home and away):
        return None
    return {
        "league":     league_key,
        "season":     season,
        "match_date": date,
        "home_name":  home,
        "away_name":  away,
        "home_abbr":  _normalize_team(home, league_key),
        "away_abbr":  _normalize_team(away, league_key),
        "psch":       _float_or_none(r.get("PSCH")),
        "pscd":       _float_or_none(r.get("PSCD")),
        "psca":       _float_or_none(r.get("PSCA")),
        "pc_over25":  None,    # New format doesn't carry totals/AH
        "pc_under25": None,
        "pcahh":      None,
        "pcaha":      None,
        "fthg":       _int_or_none(r.get("HG")),
        "ftag":       _int_or_none(r.get("AG")),
    }


def store_rows(league_key: str, rows: list[dict]) -> dict:
    """Idempotent persist via the (league, season, date, home, away)
    unique key. Returns counters."""
    if not rows:
        return {"inserted": 0, "skipped": 0}
    conn = sqlite3.connect(_db_path(league_key))
    conn.row_factory = sqlite3.Row
    _ensure_schema(conn)
    inserted = 0
    skipped = 0
    for r in rows:
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO historical_odds
                (league, season, match_date, home_name, away_name,
                 home_abbr, away_abbr, psch, pscd, psca,
                 pc_over25, pc_under25, pcahh, pcaha, fthg, ftag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r["league"], r["season"], r["match_date"],
                    r["home_name"], r["away_name"],
                    r["home_abbr"], r["away_abbr"],
                    r["psch"], r["pscd"], r["psca"],
                    r["pc_over25"], r["pc_under25"],
                    r["pcahh"], r["pcaha"],
                    r["fthg"], r["ftag"],
                ),
            )
            if conn.total_changes:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            logger.warning("[football-data:%s] insert failed: %s",
                            league_key, e)
            skipped += 1
    conn.commit()
    conn.close()
    return {"inserted": inserted, "skipped": skipped}


# Default seasons to walk for the EU per-season format. Two-year window
# matches what V3.1 prototypes used (2024-25 + 2025-26).
_EU_DEFAULT_SEASONS = ("2425", "2526")


def backfill(league_key: str, *, seasons: list[str] | None = None) -> dict:
    """One-shot backfill for a league. EU leagues walk ``seasons``
    (defaults to last two seasons); new-format leagues fetch their
    whole multi-season file (Pinnacle data starts ~2012).
    """
    spec = _LEAGUE_SPECS.get(league_key)
    if not spec:
        raise ValueError(f"no football-data spec for {league_key!r}")
    totals = {"league": league_key, "seasons_pulled": 0,
              "inserted": 0, "skipped": 0}
    if spec["format"] == "eu":
        seasons = seasons or list(_EU_DEFAULT_SEASONS)
        for s in seasons:
            rows = fetch_league_season(league_key, s)
            res = store_rows(league_key, rows)
            totals["seasons_pulled"] += 1
            totals["inserted"] += res["inserted"]
            totals["skipped"] += res["skipped"]
    else:
        rows = fetch_league_season(league_key, season=None)
        res = store_rows(league_key, rows)
        totals["seasons_pulled"] = 1
        totals["inserted"] += res["inserted"]
        totals["skipped"] += res["skipped"]
    logger.info("[football-data:%s] backfill done: %s", league_key, totals)
    return totals


# ── CLI ──

def _cli() -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="scrapers.football_data_soccer")
    sub = ap.add_subparsers(dest="cmd", required=True)
    pb = sub.add_parser("backfill")
    pb.add_argument("league", choices=sorted(_LEAGUE_SPECS.keys()))
    pb.add_argument("--seasons", nargs="*", default=None,
                     help="EU format only — e.g. 2324 2425 2526")
    pa = sub.add_parser("backfill-all")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")
    if args.cmd == "backfill":
        out = backfill(args.league, seasons=args.seasons)
        print(out)
    elif args.cmd == "backfill-all":
        for lk in _LEAGUE_SPECS:
            print(backfill(lk))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
