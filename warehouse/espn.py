"""ESPN client + parsers (scoreboard, core odds, propBets, summary box scores).

Polite by construction: ONE process-wide pacer (default 0.4 s between requests = 2.5 req/s,
override WH_ESPN_MIN_INTERVAL), UA ``curl/8.4.0`` (ESPN 403s browser UAs, see
espn_ua_403_blocks_browser_uas), exponential backoff on 429/403/5xx/timeouts, 404 = definitive.

Odds semantics (verified 2026-11-01 on live responses):
  * core ``/events/{id}/competitions/{id}/odds`` -> one item per provider, each with
    homeTeamOdds/awayTeamOdds.{open,close,current}.{moneyLine,spread,pointSpread} and
    item.{open,close,current}.{over,under,total[,draw]}; prices carry an ``american`` string.
    No timestamps: the warehouse stamps open = start - OPEN_LEAD, close = start - 60 s.
  * Some providers lack the open/close blocks (older seasons); then the top-level
    moneyLine / spreadOdds / overUnder fields are the last value -> stored as close.
  * "... Live Odds" providers are in-play feeds -> skipped.
  * 3-way trap (hockey): several providers' "moneyLine" is the 3-way REGULATION price (both sides
    plus-money, implied sum < 1). Detected per item: implied(home)+implied(away) < 0.97 -> family ML3
    (no draw price), never ML. Soccer moneylines are always ML3 (draw from ``draw`` / drawOdds).
  * propBets: shape A (per-item odds.american/total {value, open}; Over = first item of each
    two-item pair, verified by existing PickMint code on 10k+ graded lines) and shape B (explicit
    current/open.{over,under,target}; main line = the items carrying prices, as nflcal/fetch_hist.py).
"""
from __future__ import annotations

import gzip
import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

UA = "curl/8.4.0"
MIN_INTERVAL = float(os.environ.get("WH_ESPN_MIN_INTERVAL", "0.4"))

LEAGUES = {
    # league: (sport path, league path, scoreboard extra params)
    "nhl": ("hockey", "nhl", ""),
    "nba": ("basketball", "nba", ""),
    "wnba": ("basketball", "wnba", ""),
    "ncaab": ("basketball", "mens-college-basketball", "&groups=50"),
    "nfl": ("football", "nfl", ""),
    "cfb": ("football", "college-football", "&groups=80"),
    "mlb": ("baseball", "mlb", ""),
    "epl": ("soccer", "eng.1", ""),
}

# Conservative "result visible after" durations (seconds after start) for backfilled results.
FINAL_AFTER = {"nfl": 4.5 * 3600, "cfb": 4.5 * 3600, "nba": 3.0 * 3600, "wnba": 3.0 * 3600,
               "ncaab": 3.0 * 3600, "nhl": 3.25 * 3600, "mlb": 4.5 * 3600, "epl": 2.25 * 3600}
# Assumed lead of an ESPN "open" price before start (open quotes carry ts = start - lead).
OPEN_LEAD = {"nfl": 6 * 86400, "cfb": 6 * 86400, "epl": 5 * 86400, "nba": 18 * 3600, "wnba": 18 * 3600,
             "ncaab": 18 * 3600, "nhl": 18 * 3600, "mlb": 18 * 3600}
PROP_OPEN_LEAD = {"nfl": 3 * 86400, "nba": 8 * 3600, "nhl": 10 * 3600, "wnba": 8 * 3600}

_pace_lock = threading.Lock()
_last = [0.0]
STATS = {"requests": 0, "errors": 0, "retries": 0, "404": 0}


class EspnError(Exception):
    pass


def _pace():
    with _pace_lock:
        dt = time.time() - _last[0]
        if dt < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - dt)
        _last[0] = time.time()


def get_json(url: str, tries: int = 6, timeout: float = 25.0):
    """GET JSON. Returns None on 404/400 (definitive). Raises EspnError after retries."""
    delay = 15.0
    last = None
    for k in range(tries):
        _pace()
        STATS["requests"] += 1
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json",
                                                       "Accept-Encoding": "gzip"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read()
            if body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
            return json.loads(body)
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                STATS["404"] += 1
                return None
            last = f"HTTP {e.code}"
        except Exception as e:  # timeouts, resets, bad json
            last = repr(e)[:120]
        STATS["retries"] += 1
        time.sleep(delay + random.uniform(0, 3))
        delay = min(delay * 2, 600)
    STATS["errors"] += 1
    raise EspnError(f"{url} -> {last}")


def _base(league):
    sp, lg, extra = LEAGUES[league]
    return sp, lg, extra


def scoreboard_url(league, yyyymmdd):
    sp, lg, extra = _base(league)
    return f"https://site.api.espn.com/apis/site/v2/sports/{sp}/{lg}/scoreboard?dates={yyyymmdd}&limit=1000{extra}"


def scoreboard_week_url(league, season, stype, week):
    sp, lg, extra = _base(league)
    return (f"https://site.api.espn.com/apis/site/v2/sports/{sp}/{lg}/scoreboard?seasontype={stype}&week={week}"
            f"&dates={season}&limit=1000{extra}")


def odds_url(league, eid):
    sp, lg, _ = _base(league)
    return (f"https://sports.core.api.espn.com/v2/sports/{sp}/leagues/{lg}/events/{eid}/competitions/{eid}"
            f"/odds?lang=en&region=us&limit=100")


def propbets_url(league, eid, provider, page=1):
    sp, lg, _ = _base(league)
    return (f"https://sports.core.api.espn.com/v2/sports/{sp}/leagues/{lg}/events/{eid}/competitions/{eid}"
            f"/odds/{provider}/propBets?lang=en&region=us&limit=1000&page={page}")


def summary_url(league, eid):
    sp, lg, _ = _base(league)
    return f"https://site.api.espn.com/apis/site/v2/sports/{sp}/{lg}/summary?event={eid}"


# --------------------------------------------------------------------------- scoreboard

_STATUS = {"STATUS_FINAL": "final", "STATUS_FULL_TIME": "final", "STATUS_FINAL_OT": "final",
           "STATUS_FINAL_SO": "final", "STATUS_FINAL_PEN": "final", "STATUS_FINAL_AET": "final",
           "STATUS_POSTPONED": "postponed", "STATUS_CANCELED": "canceled", "STATUS_SUSPENDED": "suspended",
           "STATUS_DELAYED": "scheduled", "STATUS_SCHEDULED": "scheduled", "STATUS_FORFEIT": "void",
           "STATUS_ABANDONED": "void", "STATUS_IN_PROGRESS": "in", "STATUS_HALFTIME": "in",
           "STATUS_END_PERIOD": "in", "STATUS_RAIN_DELAY": "in", "STATUS_FIRST_HALF": "in",
           "STATUS_SECOND_HALF": "in", "STATUS_OVERTIME": "in", "STATUS_SHOOTOUT": "in"}


def iso_ts(s):
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        if len(s) == 22 and s[16] == "+":   # 2025-09-05T00:20+00:00 (no seconds)
            s = s[:16] + ":00" + s[16:]
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return None


def parse_event(e: dict, league: str) -> dict | None:
    c = (e.get("competitions") or [{}])[0]
    comps = c.get("competitors") or []
    side = {}
    for t in comps:
        ha = t.get("homeAway")
        tm = t.get("team") or {}
        if ha not in ("home", "away") or not tm.get("id"):
            continue
        ls = []
        for l in t.get("linescores") or []:
            v = l.get("value")
            ls.append(None if v is None else int(round(float(v))))
        sc = t.get("score")
        try:
            sc = None if sc in (None, "") else int(round(float(sc if not isinstance(sc, dict) else sc.get("value"))))
        except (TypeError, ValueError):
            sc = None
        side[ha] = {"espn_id": str(tm["id"]), "abbr": tm.get("abbreviation"), "name": tm.get("displayName"),
                    "short": tm.get("shortDisplayName"), "nick": tm.get("name"), "loc": tm.get("location"),
                    "score": sc, "lines": ls, "winner": t.get("winner")}
    if "home" not in side or "away" not in side:
        return None
    st = ((c.get("status") or e.get("status") or {}).get("type") or {})
    status = _STATUS.get(st.get("name"), "final" if st.get("completed") else
                         ("in" if st.get("state") == "in" else "scheduled"))
    if status == "final" and not st.get("completed", True):
        status = "in"
    season = (e.get("season") or {}).get("year")
    stype = (e.get("season") or {}).get("type")
    return {"espn_id": str(e["id"]), "start_ts": iso_ts(e.get("date") or c.get("date")), "season": season,
            "stype": stype, "neutral": 1 if c.get("neutralSite") else 0, "name": e.get("shortName") or e.get("name"),
            "status": status, "status_name": st.get("name"), "period": (c.get("status") or {}).get("period"),
            "home": side["home"], "away": side["away"]}


def calendar_dates(sb: dict) -> list[str]:
    """Season date list from a scoreboard response (leagues[0].calendar), as YYYYMMDD."""
    out = []
    for lg in sb.get("leagues") or []:
        cal = lg.get("calendar") or []
        for x in cal:
            if isinstance(x, str):
                out.append(x[:10].replace("-", ""))
            elif isinstance(x, dict) and x.get("entries"):
                pass  # week-type calendars (football) handled by week iteration
    return sorted(set(out))


# --------------------------------------------------------------------------- game odds

def _price(blk) -> float | None:
    """decimal odds from an ESPN price block {american: '+195', decimal: 2.95, ...} or a number."""
    from .db import am_to_dec
    if blk is None:
        return None
    if isinstance(blk, (int, float)):
        return am_to_dec(blk)
    a = blk.get("american") if isinstance(blk, dict) else None
    if a is None and isinstance(blk, dict):
        a = blk.get("alternateDisplayValue")
    if a is None:
        return None
    s = str(a).strip().upper()
    if s in ("EVEN", "EV", "PK"):
        return 2.0
    return am_to_dec(s)


def _num(x) -> float | None:
    if x is None:
        return None
    if isinstance(x, dict):
        x = x.get("american", x.get("alternateDisplayValue", x.get("value")))
    try:
        s = str(x).strip().upper()
        if s in ("PK", "EVEN", "EV"):
            return 0.0
        return float(s.replace("+", ""))
    except (TypeError, ValueError):
        return None


def book_code(provider_name: str) -> str:
    from .db import norm
    n = norm(provider_name)
    alias = {"draftkings": "dk", "espnbet": "espnbet", "mgm": "mgm", "betmgm": "mgm", "fanduel": "fanduel",
             "bet365": "bet365", "unibet": "unibet", "sugarhouse": "betrivers", "betrivers": "betrivers",
             "pointsbet": "pointsbet", "titanbets": "titanbets", "betfairsportsbook": "betfair",
             "caesarssportsbookcolorado": "caesars_co", "caesarssportsbooknewjersey": "caesars_nj",
             "caesarssportsbooktennessee": "caesars_tn", "williamhill": "caesars", "westgate": "westgate",
             "consensus": "consensus", "accuscore": "accuscore", "numberfire": "numberfire",
             "teamrankings": "teamrankings"}
    return alias.get(n, n[:24] or "unknown")


def _imp(d):
    return None if not d or d <= 1 else 1.0 / d


def parse_game_odds(items: list, league: str) -> list[dict]:
    """-> rows {book, book_name, provider, family, side, line, price, which} (which: open|close|current)."""
    out = []
    soccer = league == "epl"
    for it in items or []:
        prov = it.get("provider") or {}
        pname = prov.get("name") or ""
        if "live odds" in pname.lower():
            continue
        code = book_code(pname)
        if code in ("accuscore", "numberfire", "teamrankings"):
            continue
        ho, ao = it.get("homeTeamOdds") or {}, it.get("awayTeamOdds") or {}
        has_blocks = bool(ho.get("open") or ho.get("close") or it.get("open") or it.get("close"))
        whichs = ("open", "close", "current") if has_blocks else ("top",)
        for w in whichs:
            rows = []
            if w == "top":
                hml, aml = _price(ho.get("moneyLine")), _price(ao.get("moneyLine"))
                dml = _price((it.get("drawOdds") or {}).get("moneyLine")) if soccer else None
                sp = _num(it.get("spread"))
                hsp_o, asp_o = _price(ho.get("spreadOdds")), _price(ao.get("spreadOdds"))
                tot = _num(it.get("overUnder"))
                oo, uo = _price(it.get("overOdds")), _price(it.get("underOdds"))
                # top-level `spread` is the HOME line in ESPN's convention
                hline = sp
                aline = -sp if sp is not None else None
            else:
                hb, ab, gb = ho.get(w) or {}, ao.get(w) or {}, it.get(w) or {}
                hml, aml = _price(hb.get("moneyLine")), _price(ab.get("moneyLine"))
                dml = _price(gb.get("draw")) if soccer else None
                if soccer and dml is None and w in ("close", "current"):
                    dml = _price((it.get("drawOdds") or {}).get("moneyLine")) if w == "current" else None
                hline, aline = _num(hb.get("pointSpread")), _num(ab.get("pointSpread"))
                hsp_o, asp_o = _price(hb.get("spread")), _price(ab.get("spread"))
                tot = _num((gb.get("total") or {}))
                oo, uo = _price(gb.get("over")), _price(gb.get("under"))
            # moneyline: 2-way vs 3-way
            if hml and aml:
                ssum = _imp(hml) + _imp(aml)
                if soccer or ssum < 0.97:
                    fam = "ML3"
                    rows += [("ML3", "home", None, hml), ("ML3", "away", None, aml)]
                    if dml:
                        rows.append(("ML3", "draw", None, dml))
                else:
                    rows += [("ML", "home", None, hml), ("ML", "away", None, aml)]
            if hline is not None and hsp_o and asp_o:
                if aline is None:
                    aline = -hline
                rows += [("SPREAD", "home", hline, hsp_o), ("SPREAD", "away", aline, asp_o)]
            if tot is not None and oo and uo:
                rows += [("TOTAL", "over", tot, oo), ("TOTAL", "under", tot, uo)]
            for fam, sd, line, pr in rows:
                out.append({"book": code, "book_name": pname, "provider": str(prov.get("id")), "family": fam,
                            "side": sd, "line": line, "price": pr, "which": "close" if w == "top" else w})
    return out


# --------------------------------------------------------------------------- props

PROP_TYPES = {
    "basketball": {
        "Total Points": "pts", "Total Rebounds": "reb", "Total Assists": "ast", "Total 3-Point Field Goals": "fg3m",
        "Total Points and Rebounds": "pr", "Total Points and Assists": "pa", "Total Assists and Rebounds": "ar",
        "Total Points, Rebounds, and Assists": "pra", "Total Steals": "stl", "Total Blocks": "blk",
        "Total Steals and Blocks": "stocks", "Total Turnovers": "tov",
    },
    "hockey": {
        "Total Points": "pts", "Total Assists": "ast", "Total Shots on Goal": "sog", "Total Saves": "saves",
        "Total Goals": "g", "Total Blocked Shots": "blk", "Total Power Play Points": "ppp",
        "Total Goals Against": "ga",
    },
    "football": {
        "Total Receiving Yards (incl. overtime)": "rec_yds", "Total Receptions (incl. overtime)": "receptions",
        "Total Rushing Yards (incl. overtime)": "rush_yds", "Total Passing Yards (incl. overtime)": "pass_yds",
        "Total Carries (incl. overtime)": "carries", "Total Pass Completions (incl. overtime)": "completions",
        "Total Passing Attempts (incl. overtime)": "pass_att",
        "Total Rushing Plus Receiving Yards (incl. overtime)": "rr_yds",
        "Total Passing Touchdowns (incl. overtime)": "pass_td",
        "Total Passing Interceptions (incl. overtime)": "pass_int",
    },
}
PROPY_TYPES = {"hockey": {"Anytime Goalscorer": "goal"}, "football": {"Anytime Touchdown Scorer": "td"},
               "basketball": {"To Record a Double Double": "dd"}}
# team-level period markets carried in ESPN BET propBets (NBA 2024-25 etc.)
_TEAM_PERIOD = {"1st Half": "1H", "2nd Half": "2H", "1st Quarter": "Q1", "2nd Quarter": "Q2", "3rd Quarter": "Q3",
                "4th Quarter": "Q4", "1st Period": "P1", "2nd Period": "P2", "3rd Period": "P3"}


def _ath(it):
    a = ((it.get("athlete") or {}).get("$ref") or "").split("/athletes/")[-1].split("?")[0]
    return a if a.isdigit() else None


def _team_ref(it):
    t = ((it.get("team") or {}).get("$ref") or "").split("/teams/")[-1].split("?")[0]
    return t if t.isdigit() else None


def parse_propbets(items: list, sport: str) -> dict:
    """-> {"props": [...], "team": [...]}.

    props rows: {ath, stat, fam ('PROP'|'PROPY'), which ('open'|'current'), line, over, under, main}
    team rows : {team_espn_id, period, family ('ML'|'SPREAD'|'TOTAL'|'TT'), which, line, price, pos}
    """
    pt = PROP_TYPES.get(sport, {})
    py = PROPY_TYPES.get(sport, {})
    props, team = [], []
    grp = defaultdict(list)       # shape A: (ath, stat) -> [(k, item)]
    grpb = defaultdict(list)      # shape B
    for k, it in enumerate(items or []):
        tname = (it.get("type") or {}).get("name") or ""
        a = _ath(it)
        if a and (tname in pt or tname in py):
            stat = pt.get(tname) or py.get(tname)
            fam = "PROP" if tname in pt else "PROPY"
            o = it.get("odds")
            if isinstance(o, dict) and (o.get("american") or {}).get("value") is not None:
                grp[(a, stat, fam)].append((k, it))
            else:
                grpb[(a, stat, fam)].append((k, it))
            continue
        if not a and _team_ref(it) and isinstance(it.get("odds"), dict):
            # team-level period ML / spread / total (shape A)
            per = None
            for pfx, code in _TEAM_PERIOD.items():
                if tname.startswith(pfx):
                    per = code
                    break
            fam = None
            low = tname.lower()
            if tname.startswith("Team ") and "Total Points" in tname:
                fam, per = "TT", ("1H" if "1st Half" in tname else "FG")
            elif per and low.endswith("moneyline"):
                fam = "ML"
            elif per and low.endswith("spread"):
                fam = "SPREAD"
            elif per and ("total points" in low or low.endswith("total")):
                fam = "TOTAL"
            if fam:
                o = it["odds"]
                for which, vk in (("open", "open"), ("current", "value")):
                    pr = _price({"american": (o.get("american") or {}).get(vk)})
                    ln = _num((o.get("total") or {}).get(vk))
                    if pr:
                        team.append({"team_espn_id": _team_ref(it), "period": per, "family": fam, "which": which,
                                     "line": ln, "price": pr, "pos": k, "type": tname})
    # shape A: pair items with the same current line (first = over), main = most balanced pair
    for (a, stat, fam), lst in grp.items():
        if fam == "PROPY":
            for k, it in lst:
                o = it["odds"]
                for which, vk in (("open", "open"), ("current", "value")):
                    pr = _price({"american": (o.get("american") or {}).get(vk)})
                    ln = _num((o.get("total") or {}).get(vk))
                    if pr:
                        props.append({"ath": a, "stat": stat, "fam": fam, "which": which,
                                      "line": ln if ln is not None else 0.5, "over": pr, "under": None, "main": 1})
            continue
        byline = defaultdict(list)
        for k, it in lst:
            ln = _num((it["odds"].get("total") or {}).get("value"))
            if ln is not None:
                byline[ln].append((k, it))
        pairs = []
        for ln, v in byline.items():
            if len(v) != 2:
                continue
            v.sort(key=lambda x: x[0])
            (k1, io), (k2, iu) = v
            po = _price({"american": (io["odds"].get("american") or {}).get("value")})
            pu = _price({"american": (iu["odds"].get("american") or {}).get("value")})
            if not po or not pu:
                continue
            bal = abs(_imp(po) / (_imp(po) + _imp(pu)) - 0.5)
            oo = _price({"american": (io["odds"].get("american") or {}).get("open")})
            ou = _price({"american": (iu["odds"].get("american") or {}).get("open")})
            lo = _num((io["odds"].get("total") or {}).get("open"))
            lo2 = _num((iu["odds"].get("total") or {}).get("open"))
            pairs.append((bal, ln, po, pu, lo if lo == lo2 else None, oo, ou))
        if not pairs:
            continue
        pairs.sort()
        for i, (bal, ln, po, pu, lo, oo, ou) in enumerate(pairs):
            props.append({"ath": a, "stat": stat, "fam": fam, "which": "current", "line": ln, "over": po,
                          "under": pu, "main": 1 if i == 0 else 0})
            if lo is not None and oo and ou:
                props.append({"ath": a, "stat": stat, "fam": fam, "which": "open", "line": lo, "over": oo,
                              "under": ou, "main": 1 if i == 0 else 0})
    # shape B: explicit over/under blocks (main line = items carrying prices); line-only items too
    for (a, stat, fam), lst in grpb.items():
        its = [it for _, it in lst]
        for which in ("open", "current"):
            ov = [i for i in its if ((i.get(which) or {}).get("over"))]
            un = [i for i in its if ((i.get(which) or {}).get("under"))]
            if ov or un:
                o = ov[0] if ov else None
                u = un[0] if un else None
                lo = ((o or u).get(which) or {}).get("target", {}).get("value")
                lu = ((u or o).get(which) or {}).get("target", {}).get("value")
                po = _price((o.get(which) or {}).get("over")) if o else None
                pu = _price((u.get(which) or {}).get("under")) if u else None
                if lo is not None and lu is not None and float(lo) != float(lu):
                    pu = None if po else pu
                props.append({"ath": a, "stat": stat, "fam": fam, "which": which, "line": _num(lo if o else lu),
                              "over": po, "under": pu, "main": 1})
            else:
                # lines only (e.g. DK 2026 NFL): keep the line with no price
                i0 = its[0]
                ln = ((i0.get(which) or {}).get("target") or {}).get("value")
                if ln is not None:
                    props.append({"ath": a, "stat": stat, "fam": fam, "which": which, "line": _num(ln),
                                  "over": None, "under": None, "main": 1})
    return {"props": props, "team": team}


# --------------------------------------------------------------------------- box scores
# Parsed by ESPN stat KEYS (stable), never labels: in the NHL box the label "SOG" is SHOOTOUT goals
# (key shootoutGoals); shots on goal are label "S" / key "shotsTotal".
BOX_KEYS = {
    "basketball": {"points": "pts", "rebounds": "reb", "assists": "ast", "steals": "stl", "blocks": "blk",
                   "turnovers": "tov", "minutes": "min",
                   "threePointFieldGoalsMade-threePointFieldGoalsAttempted": "fg3m"},
    "hockey": {"goals": "g", "assists": "ast", "shotsTotal": "sog", "blockedShots": "blk", "hits": "hits",
               "timeOnIce": "toi", "plusMinus": "pm", "penaltyMinutes": "pim", "saves": "saves",
               "goalsAgainst": "ga", "shotsAgainst": "sa"},
    "football": {("passing", "completions/passingAttempts"): "c/att", ("passing", "passingYards"): "pass_yds",
                 ("passing", "passingTouchdowns"): "pass_td", ("passing", "interceptions"): "pass_int",
                 ("rushing", "rushingAttempts"): "carries", ("rushing", "rushingYards"): "rush_yds",
                 ("rushing", "rushingTouchdowns"): "rush_td", ("receiving", "receptions"): "receptions",
                 ("receiving", "receivingYards"): "rec_yds", ("receiving", "receivingTargets"): "targets",
                 ("receiving", "receivingTouchdowns"): "rec_td"},
}


def _fnum(v):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _mins(v):
    v = str(v)
    try:
        return round(float(v.split(":")[0]) + float(v.split(":")[1]) / 60.0, 2) if ":" in v else float(v)
    except ValueError:
        return None


def parse_box(summary: dict, sport: str) -> list[dict]:
    """-> [{ath, name, team, stat, value}]; stat 'dnp' = 1 for players listed as did-not-play."""
    km = BOX_KEYS.get(sport, {})
    out = []
    for team in ((summary or {}).get("boxscore") or {}).get("players") or []:
        tid = str((team.get("team") or {}).get("id") or "")
        for cat in team.get("statistics") or []:
            keys = list(cat.get("keys") or [])
            cname = str(cat.get("name") or "").lower()
            for ath in cat.get("athletes") or []:
                a = ath.get("athlete") or {}
                aid = str(a.get("id") or "")
                if not aid:
                    continue
                name = a.get("displayName")
                stats = ath.get("stats") or []
                if ath.get("didNotPlay") or not stats:
                    out.append({"ath": aid, "name": name, "team": tid, "stat": "dnp", "value": 1.0})
                    continue
                for key, val in zip(keys, stats):
                    st = km.get((cname, key)) if sport == "football" else km.get(key)
                    if not st:
                        continue
                    if st == "c/att":
                        try:
                            cpl, att = str(val).split("/")
                            out.append({"ath": aid, "name": name, "team": tid, "stat": "completions", "value": float(cpl)})
                            out.append({"ath": aid, "name": name, "team": tid, "stat": "pass_att", "value": float(att)})
                        except ValueError:
                            pass
                        continue
                    if st == "fg3m":
                        fv = _fnum(str(val).split("-")[0])
                    elif st in ("toi", "min"):
                        fv = _mins(val)
                    else:
                        fv = _fnum(val)
                    if fv is not None:
                        out.append({"ath": aid, "name": name, "team": tid, "stat": st, "value": fv})
    by = defaultdict(dict)
    for r in out:
        by[(r["ath"], r["name"], r["team"])][r["stat"]] = r["value"]
    for (aid, name, tid), d in by.items():
        if "dnp" in d:
            continue
        if sport == "basketball" and "pts" in d:
            p, rb, a = d.get("pts", 0), d.get("reb", 0), d.get("ast", 0)
            for st, v in (("pr", p + rb), ("pa", p + a), ("ar", a + rb), ("pra", p + rb + a),
                          ("stocks", d.get("stl", 0) + d.get("blk", 0))):
                out.append({"ath": aid, "name": name, "team": tid, "stat": st, "value": v})
        if sport == "hockey" and "g" in d:
            out.append({"ath": aid, "name": name, "team": tid, "stat": "pts", "value": d.get("g", 0) + d.get("ast", 0)})
        if sport == "football" and ("rush_yds" in d or "rec_yds" in d):
            out.append({"ath": aid, "name": name, "team": tid, "stat": "rr_yds",
                        "value": (d.get("rush_yds") or 0) + (d.get("rec_yds") or 0)})
    return out
