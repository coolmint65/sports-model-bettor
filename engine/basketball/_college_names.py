"""HR -> our-team resolution for college basketball (NCAAM / NCAAW) (MintyBot 2026-10-26).

The framework's generic resolver (first word, last word, then substring; first match wins) is
fine for a 12-team pro league and wrong for ~360 college programs among ~900 opponents: "NC
State Wolfpack" can land on Loyola New Orleans (also the Wolfpack), and abbreviations collide
(UNI = Northern Iowa AND Union (TN); VAL, WIN ...). Here a name resolves only by EXACT match on
a normalized school name -- ESPN's display / short / location names, the teams table, and the
HR spellings seen on the college-football board -- preferring Division I; no token guessing.
An unknown name stays unresolved and is logged (data/basketball/<league>_hr_unresolved.json)
so it gets an alias instead of a wrong team. Each HR event is then tied to OUR scheduled game
by both team ids (+/-1 day), flipping the prices when HR lists a neutral-site game the other way
round, so the card's odds always sit on the right side.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path

from ._db import get_conn, games_table, teams_table

logger = logging.getLogger(__name__)

_DATA = Path(__file__).resolve().parent.parent.parent / "data" / "basketball"
_TTL_S = 6 * 3600
_CACHE: dict = {}
_LOCK = threading.Lock()

# HR / sportsbook spelling (normalized) -> a normalized name ESPN knows. Football's board
# taught the first few (engine/football/_odds._HR_ALIASES / _HR_DIRECT); extend from the
# unresolved log.
_ALIASES = {
    "miami fl": "miami", "miami florida": "miami", "miami ohio": "miami oh",
    "appalachian state": "app state",
    "louisiana monroe": "ul monroe", "ulm": "ul monroe",
    "louisiana lafayette": "louisiana", "ul lafayette": "louisiana",
    "usf": "south florida", "connecticut": "uconn", "mississippi": "ole miss",
    "unc": "north carolina", "north carolina state": "nc state", "n carolina state": "nc state",
    "sam houston state": "sam houston", "albany": "ualbany", "albany ny": "ualbany",
    "loyola il": "loyola chicago", "loyola illinois": "loyola chicago",
    "loyola maryland": "loyola maryland", "st francis pa": "saint francis",
    "texas and m cc": "texas a and m corpus christi",
    "texas a and m cc": "texas a and m corpus christi",
    "utrgv": "ut rio grande valley", "texas rio grande valley": "ut rio grande valley",
    "tennessee martin": "ut martin", "texas arlington": "ut arlington",
    "nebraska omaha": "omaha", "umkc": "kansas city", "missouri kansas city": "kansas city",
    "iupui": "iu indianapolis", "detroit": "detroit mercy", "fort wayne": "purdue fort wayne",
    "ipfw": "purdue fort wayne", "southeastern louisiana": "se louisiana",
    "mcneese state": "mcneese", "nicholls state": "nicholls", "grambling state": "grambling",
    "cal baptist": "california baptist", "csu fullerton": "cal state fullerton",
    "cs fullerton": "cal state fullerton", "csu bakersfield": "cal state bakersfield",
    "csu northridge": "cal state northridge", "louisiana state": "lsu", "brigham young": "byu",
    "texas christian": "tcu", "southern methodist": "smu", "central florida": "ucf",
    "southern california": "usc", "nevada las vegas": "unlv", "virginia commonwealth": "vcu",
    "texas el paso": "utep", "texas san antonio": "utsa", "alabama birmingham": "uab",
    "illinois chicago": "uic", "college of charleston": "charleston", "citadel": "the citadel",
    "southern mississippi": "southern miss", "middle tennessee state": "middle tennessee",
    "seattle": "seattle u", "wisconsin green bay": "green bay", "wisconsin milwaukee": "milwaukee",
    "arkansas little rock": "little rock", "long island": "long island university",
    "liu brooklyn": "long island university", "southern u": "southern",
    "southern university": "southern", "texas a and m commerce": "east texas a and m",
    "maryland eastern shore": "maryland eastern shore", "md eastern shore": "maryland eastern shore",
    "central connecticut state": "central connecticut", "east tennessee state": "east tennessee state",
    "lsu new orleans": "lsu new orleans", "new orleans": "lsu new orleans",
    "uno": "lsu new orleans", "sc upstate": "south carolina upstate",
    "usc upstate": "south carolina upstate", "ut chattanooga": "chattanooga",
    "tennessee chattanooga": "chattanooga", "pennsylvania": "pennsylvania",
    "se missouri state": "southeast missouri state", "florida intl": "fiu",
    "florida international": "fiu", "queens": "queens university", "queens nc": "queens university",
}

# A trailing state code in parentheses that only disambiguates ("Saint Mary's (CA)",
# "Queens (NC)", "St. Thomas (MN)") -- tried AFTER the full name fails, so "Miami (OH)" (an
# ESPN location) still wins over "Miami".
_STATE_CODES = {"ca", "nc", "mn", "pa", "ny", "md", "il", "fl", "oh", "tn", "tx", "la", "va",
                "in", "ky", "sc", "ga", "mo", "co", "ut", "nj", "ct", "ma", "me", "nh", "vt",
                "ri", "de", "dc", "wv", "al", "ms", "ar", "ok", "ks", "ne", "ia", "wi", "mi",
                "id", "wa", "or", "nv", "az", "nm", "mt", "wy", "nd", "sd", "hi", "ak"}


def norm(s) -> str:
    """Lowercase, accent/punctuation-free school name; 'St.' at the end means State,
    anywhere else Saint; '#25 ' rank prefixes and NCAAW's ' (W)' suffix dropped."""
    s = "".join(c for c in unicodedata.normalize("NFKD", str(s or ""))
                if not unicodedata.combining(c)).lower().strip()
    s = re.sub(r"^#\s*\d+\s+", "", s)
    s = re.sub(r"\s*\((w|women|womens)\)\s*$", "", s)
    s = s.replace("&", " and ")
    s = re.sub(r"['’`.]", "", s)
    s = re.sub(r"[-/,()]", " ", s)
    toks = s.split()
    if toks and toks[-1] == "st":
        toks[-1] = "state"
    if len(toks) > 2 and toks[0] == "cal" and toks[1] == "st":     # Cal St. Fullerton
        toks[1] = "state"
    toks = ["saint" if (t == "st" and i < len(toks) - 1) else t for i, t in enumerate(toks)]
    return " ".join(toks)


def _tables(league):
    """{normalized name: team ids}, {team id: (abbr, mascot)}, D1 id set -- cached 6h."""
    with _LOCK:
        hit = _CACHE.get(league)
        if hit and time.time() - hit[0] < _TTL_S:
            return hit[1]
        from ._ratings import espn_d1_teams
        names: dict[str, set] = {}
        info: dict[int, tuple] = {}

        def add(key, tid):
            k = norm(key)
            if k:
                names.setdefault(k, set()).add(tid)

        conn = get_conn(league)
        for r in conn.execute(f"SELECT id, name, abbreviation, city FROM {teams_table(league)}"):
            tid = int(r["id"])
            info[tid] = ((r["abbreviation"] or "").strip(), "")
            for f in (r["name"], r["city"]):
                add(f, tid)
        d1 = set()
        for t in espn_d1_teams(league):
            try:
                tid = int(t["id"])
            except (TypeError, ValueError, KeyError):
                continue
            d1.add(tid)
            for f in ("displayName", "shortDisplayName", "location", "abbreviation", "nickname"):
                add(t.get(f), tid)
            mascot = norm(t.get("name"))
            if tid in info:
                info[tid] = (info[tid][0], mascot)
            else:
                info[tid] = ((t.get("abbreviation") or "").strip(), mascot)
        for a, target in _ALIASES.items():
            ids = names.get(norm(target))
            if ids:
                names.setdefault(norm(a), set()).update(ids)
        out = (names, info, d1)
        _CACHE[league] = (time.time(), out)
        return out


def _pick(ids, d1):
    if not ids:
        return None
    if len(ids) == 1:
        return next(iter(ids))
    dd = [t for t in ids if t in d1]
    return dd[0] if len(dd) == 1 else None


def resolve(league, hr_name):
    """Team id for an HR team name, or None (never a guess)."""
    names, info, d1 = _tables(league)
    n = norm(hr_name)
    if not n:
        return None
    tid = _pick(names.get(n), d1)
    if tid is not None:
        return tid
    toks = n.split()
    if len(toks) > 1 and toks[-1] in _STATE_CODES:      # "saint marys ca" -> "saint marys"
        tid = _pick(names.get(" ".join(toks[:-1])), d1)
        if tid is not None:
            return tid
    # "<school> <mascot>" where the school form isn't ESPN's: accept only when the rest of
    # the name IS that team's mascot ("miami fl hurricanes" -> Miami).
    for i in range(len(toks) - 1, 0, -1):
        tid = _pick(names.get(" ".join(toks[:i])), d1)
        if tid is not None and info.get(tid, ("", ""))[1] == " ".join(toks[i:]):
            return tid
    return None


def _et_date(iso):
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        from zoneinfo import ZoneInfo
        return dt.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        return str(iso)[:10]


def _flip(odds: dict) -> dict:
    """The same prices seen from the other side (HR's home = our away)."""
    o = dict(odds)
    for k1, k2 in (("home_abbr", "away_abbr"), ("home_full_name", "away_full_name")):
        o[k1], o[k2] = odds.get(k2), odds.get(k1)

    def ml(m):
        return {"home": m.get("away"), "away": m.get("home")} if m else m

    def sp(s):
        return ({"home_pt": s.get("away_pt"), "home_odds": s.get("away_odds"),
                 "away_pt": s.get("home_pt"), "away_odds": s.get("home_odds")} if s else s)

    if odds.get("ml"):
        o["ml"] = ml(odds["ml"])
    if odds.get("spread"):
        o["spread"] = sp(odds["spread"])
    if odds.get("alt_spreads"):
        o["alt_spreads"] = [sp(s) for s in odds["alt_spreads"]]
    if odds.get("q1"):
        q = dict(odds["q1"])
        if q.get("ml"):
            q["ml"] = ml(q["ml"])
        if q.get("spread"):
            q["spread"] = sp(q["spread"])
        if q.get("alt_spreads"):
            q["alt_spreads"] = [sp(s) for s in q["alt_spreads"]]
        o["q1"] = q
    return o


_UNRESOLVED: dict = {}


def _log_miss(league, kind, detail):
    key = (league, kind, detail)
    if key in _UNRESOLVED:
        return
    _UNRESOLVED[key] = time.time()
    logger.info("[%s] HR college %s: %s", league, kind, detail)
    try:
        path = _DATA / f"{league}_hr_unresolved.json"
        rows = [{"kind": k, "detail": d, "ts": round(t)} for (lg, k, d), t in _UNRESOLVED.items()
                if lg == league]
        path.write_text(json.dumps(rows[-500:], indent=1), encoding="utf-8")
    except Exception:
        pass


def attach(league, parsed: dict) -> dict | None:
    """Re-resolve a parsed HR event onto OUR scheduled game: correct team ids, our game_id and
    key, prices flipped if HR has the sides the other way. None when either team or the game
    can't be pinned down (logged)."""
    hn, an = parsed.get("home_full_name"), parsed.get("away_full_name")
    hid, aid = resolve(league, hn), resolve(league, an)
    if hid is None or aid is None or hid == aid:
        if hid is None:
            _log_miss(league, "unresolved team", hn)
        if aid is None:
            _log_miss(league, "unresolved team", an)
        return None
    d = _et_date(parsed.get("start_time"))
    try:
        base = datetime.strptime(d, "%Y-%m-%d") if d else datetime.now()
    except ValueError:
        base = datetime.now()
    days = [(base + timedelta(days=k)).strftime("%Y-%m-%d") for k in (0, -1, 1)]
    conn = get_conn(league)
    rows = conn.execute(
        f"SELECT game_id, date, home_team_id, away_team_id FROM {games_table(league)} "
        f"WHERE date IN (?, ?, ?) AND COALESCE(status, '') NOT IN "
        f"  ('cancelled', 'postponed', 'voided', 'final') "
        f"  AND ((home_team_id = ? AND away_team_id = ?) OR (home_team_id = ? AND away_team_id = ?))",
        (*days, hid, aid, aid, hid)).fetchall()
    if not rows:
        _log_miss(league, "no scheduled game", f"{an} @ {hn} ({d})")
        return None
    rows = sorted(rows, key=lambda r: (days.index(r["date"]) if r["date"] in days else 9))
    g = rows[0]
    out = parsed if int(g["home_team_id"]) == hid else _flip(parsed)
    names, info, _d1 = _tables(league)
    h_id, a_id = int(g["home_team_id"]), int(g["away_team_id"])
    out = dict(out)
    out["home_abbr"] = info.get(h_id, (out.get("home_abbr"), ""))[0] or out.get("home_abbr")
    out["away_abbr"] = info.get(a_id, (out.get("away_abbr"), ""))[0] or out.get("away_abbr")
    out["home_team_id"], out["away_team_id"] = h_id, a_id
    out["game_id"] = str(g["game_id"])
    out["_key"] = f"{out['away_abbr']}@{out['home_abbr']}"
    out["sides_flipped"] = h_id != hid
    return out
