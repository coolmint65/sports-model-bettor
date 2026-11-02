"""ESPN box-score layer for football (NFL / CFB).

``games`` stores points + per-quarter linescores only. The stat / game-script markets
HR prices beyond points (FGs made, XPs, kicking points, pass/rush/DEF-ST TDs, INTs,
punts, team yardage, red-zone trips, 2-pt conversions, safeties, pick-sixes, first-drive
results, method of first TD, ...) need the box score. ESPN ships it per game on
``/summary?event=`` (team statistics, player stat-group totals, every scoring play, every
drive), so it is pulled lazily for FINAL games and cached in ``game_stats`` — one row per
game, with a normalized ``facts`` JSON the settlers grade from (MintyBot 2026-10-25,
Austin: "work on 3 to 5" = odd/even + exact points + OT, scoring-type props, yardage/drives).

  - ``ingest_boxscores(league, limit=12)`` — fetch + store up to ``limit`` finals with no
    settlement-grade row yet (slate compute calls it on the 90s ingest debounce).
  - ``backfill(league)`` — every final in the DB (CLI: ``python -m engine.football._boxscore backfill nfl``).
  - ``load_facts(league, game_id)`` — facts dict or None.
  - ``resolve_stat_market(bet_type, pick_text, facts, home_abbr, away_abbr)`` — shared
    W/L/P/None for the facts-based bet types (``_tracker`` and the unified adapter both call
    it, so a market grades the same way on both sides).

Facts shape (all counts ints, None when ESPN didn't ship the stat — CFB has no red-zone /
sack / drive team stats):
  facts["home"|"away"] = {pts, td, pass_td, rush_td, dst_td, fgm, fga, fg_dists, xpm, xpa,
      kick_pts, two_att, two_made, safety, int_thrown, int_made, pick6_thrown, pick6_made,
      punts, punt_yds, punt_net_yds, punt_long, punt_tb, punt_in20, total_yds, net_pass_yds,
      gross_pass_yds, pass_rush_yds, rush_yds, rush_att, rec_yds, rush_rec_yds, pass_att,
      pass_cmp, first_downs, turnovers, fumbles_lost, sacks_taken, rz_trips, drives,
      first_drive{result, cat, prog, score}, q_pts, scored_every_q, dst_td_h1,
      unique_td_scorers}
  facts["game"] = {ot, periods, total, odd, first_td{side, method, q, secs, yds, scorer},
      last_td{...}, first_fg_dist, last_fg_dist, fg_yds, fgm, xpm, kick_pts, two_att,
      two_made, ints, punts, punt_tb, punt_net_yds, rz_trips, total_yds, net_pass_yds,
      gross_pass_yds, rush_yds, rush_att, pass_td, unique_td_scorers, turnovers, any_safety,
      any_pick6, any_punt_ret_td, any_blocked_punt}
"""
from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

from . import get_league_config
from ._db import get_conn

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "curl/8.4.0"}      # ESPN 403s browser UAs from servers; curl UA passes

_FG_RE = re.compile(r"(\d+)\s*Yd Field Goal", re.I)
_TD_RE = re.compile(r"^(.*?)\s(\d+)\s*Yd\b", re.I)
_PAREN_RE = re.compile(r"\(([^()]*)\)\s*$")

# Drive result -> HR's 4-way first-drive buckets (Touchdown / Field Goal Attempt / Punt /
# Turnover or Safety). END = half/game ran out (HR voids -> we push).
_DRIVE_CAT = {
    "TD": "TD",
    "FG": "FGA", "MISSED FG": "FGA", "BLOCKED FG": "FGA",
    "PUNT": "PUNT",
    "INT": "TO", "INT TD": "TO", "FUMBLE": "TO", "FUMBLE TD": "TO", "FUMBLE RETURN TD": "TO",
    "DOWNS": "TO", "SF": "TO", "SAFETY": "TO", "BLOCKED PUNT": "TO", "BLOCKED PUNT TD": "TO",
    "END OF HALF": "END", "END OF GAME": "END",
}


def _fetch(url: str, *, retries: int = 3, timeout: int = 40) -> dict | None:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            logger.debug("[football] boxscore HTTPError %s: %s", e.code, url)
        except Exception as e:
            logger.debug("[football] boxscore fetch failed (%d/%d): %s", attempt + 1, retries, e)
        time.sleep(0.6 * (attempt + 1))
    return None


def _int(v):
    if v in (None, "", "--"):
        return None
    try:
        return int(float(str(v).replace(",", "")))
    except (ValueError, TypeError):
        return None


def _pair(v):
    """'2/2' -> (2, 2); '3-17' -> (3, 17); None when not a pair."""
    if not v or not isinstance(v, str):
        return None
    for sep in ("/", "-"):
        if sep in v:
            a, _, b = v.partition(sep)
            ia, ib = _int(a), _int(b)
            if ia is not None and ib is not None:
                return ia, ib
    return None


def _clock_secs(s) -> int | None:
    if not s or not isinstance(s, str) or ":" not in s:
        return None
    m, _, sec = s.partition(":")
    try:
        return int(m) * 60 + int(float(sec))
    except ValueError:
        return None


def _side_of(team: dict | None, h_id: str, a_id: str, h_abbr: str, a_abbr: str) -> str | None:
    team = team or {}
    tid = str(team.get("id") or "")
    if tid and tid == h_id:
        return "home"
    if tid and tid == a_id:
        return "away"
    ab = (team.get("abbreviation") or "").upper()
    if ab and ab == h_abbr:
        return "home"
    if ab and ab == a_abbr:
        return "away"
    return None


def _td_method(type_text: str) -> str:
    t = (type_text or "").lower()
    if "pass" in t:                         # Passing Touchdown / Pass Reception
        return "pass"
    if "rush" in t or "run" in t or "recovery (own)" in t:
        return "rush"
    if "return" in t or "blocked" in t or "interception" in t or "fumble" in t or "kickoff" in t or "punt" in t:
        return "dst"
    return "other"


def parse_summary(d: dict) -> dict | None:
    """ESPN /summary payload -> {"home_abbr","away_abbr","final","team_stats","groups",
    "plays","drives","facts"} or None when the header is unusable."""
    comps = (d.get("header") or {}).get("competitions") or []
    comp = comps[0] if comps else {}
    competitors = comp.get("competitors") or []
    home = next((c for c in competitors if c.get("homeAway") == "home"), None)
    away = next((c for c in competitors if c.get("homeAway") == "away"), None)
    if not (home and away):
        return None
    h_abbr = ((home.get("team") or {}).get("abbreviation") or "").upper()
    a_abbr = ((away.get("team") or {}).get("abbreviation") or "").upper()
    h_id = str((home.get("team") or {}).get("id") or "")
    a_id = str((away.get("team") or {}).get("id") or "")
    stype = (comp.get("status") or {}).get("type") or {}
    final = bool(stype.get("completed")) or stype.get("state") == "post"
    h_score, a_score = _int(home.get("score")), _int(away.get("score"))

    def _ls(side):
        out = []
        for x in side.get("linescores") or []:
            v = _int((x or {}).get("value", (x or {}).get("displayValue")))
            out.append(v or 0)
        return out
    h_ls, a_ls = _ls(home), _ls(away)

    bs = d.get("boxscore") or {}
    team_stats = {"home": {}, "away": {}}
    for t in bs.get("teams") or []:
        side = _side_of(t.get("team"), h_id, a_id, h_abbr, a_abbr)
        if side:
            team_stats[side] = {s.get("name"): s.get("displayValue")
                                for s in (t.get("statistics") or []) if s.get("name")}
    groups = {"home": {}, "away": {}}
    for pl in bs.get("players") or []:
        side = _side_of(pl.get("team"), h_id, a_id, h_abbr, a_abbr)
        if not side:
            continue
        for g in pl.get("statistics") or []:
            name = g.get("name")
            if not name:
                continue
            labels, totals = g.get("labels") or [], g.get("totals") or []
            groups[side][name] = dict(zip(labels, totals)) if totals else {}

    # Scoring plays. The scoring side + points come from the running score (robust to
    # ESPN's team attribution on safeties); XP / 2-pt from the trailing parenthetical.
    plays = []
    prev_h = prev_a = 0
    for p in d.get("scoringPlays") or []:
        hs, as_ = _int(p.get("homeScore")), _int(p.get("awayScore"))
        if hs is None or as_ is None:
            continue
        dh, da = hs - prev_h, as_ - prev_a
        prev_h, prev_a = hs, as_
        if dh <= 0 and da <= 0:
            continue
        side = "home" if dh >= da else "away"
        delta = max(dh, da)
        kind = ((p.get("scoringType") or {}).get("name") or "").lower()
        type_text = (p.get("type") or {}).get("text") or ""
        text = p.get("text") or ""
        if not kind:
            kind = "field-goal" if "field goal" in text.lower() else ("safety" if delta == 2 else "touchdown")
        q = _int((p.get("period") or {}).get("number"))
        clock = (p.get("clock") or {}).get("displayValue")
        m = _PAREN_RE.search(text)
        par = (m.group(1) if m else "").lower()
        xpa = xpm = two_att = two_made = 0
        fg_yds = td_yds = None
        scorer = None
        method = None
        if kind == "touchdown":
            method = _td_method(type_text)
            if "two-point" in par or "two point" in par or "2-pt" in par:
                two_att, two_made = 1, (0 if "fail" in par else 1)
            elif "kick" in par or "pat" in par:
                xpa, xpm = 1, (0 if ("fail" in par or "block" in par or "miss" in par or "no good" in par) else 1)
            if delta == 7:
                xpa, xpm, two_att, two_made = max(xpa, 1), 1, 0, 0
            elif delta == 8:
                xpa, xpm, two_att, two_made = 0, 0, 1, 1
            elif delta == 6:
                xpm, two_made = 0, 0
            tm = _TD_RE.match(text)
            if tm:
                scorer, td_yds = tm.group(1).strip(), _int(tm.group(2))
        elif kind == "field-goal":
            fm = _FG_RE.search(text)
            fg_yds = _int(fm.group(1)) if fm else None
        plays.append({"q": q, "clock": clock, "side": side, "delta": delta, "kind": kind,
                      "type": type_text, "method": method, "text": text[:160], "home": hs,
                      "away": as_, "fg_yds": fg_yds, "td_yds": td_yds, "scorer": scorer,
                      "xpa": xpa, "xpm": xpm, "two_att": two_att, "two_made": two_made})

    # Drives. ESPN's yardLine is an absolute field coordinate: 0 = home goal line, 100 =
    # away goal line (verified JAX@HOU 2026-10-18: home 'JAX 30' -> 30, away 'HOU 6' -> 94).
    # "prog" = how far the offense got (own goal line = 0 ... opp goal line = 100).
    drives = []
    for x in (d.get("drives") or {}).get("previous") or []:
        side = _side_of(x.get("team"), h_id, a_id, h_abbr, a_abbr)
        start, end = x.get("start") or {}, x.get("end") or {}
        eyl = _int(end.get("yardLine"))
        res = (x.get("result") or "").upper()
        prog = None
        if side and eyl is not None:
            prog = eyl if side == "home" else 100 - eyl
        if res == "TD":
            prog = 100
        drives.append({"side": side, "team": (x.get("team") or {}).get("abbreviation"),
                       "result": res, "cat": _DRIVE_CAT.get(res, "OTHER"),
                       "yards": _int(x.get("yards")), "plays": _int(x.get("offensivePlays")),
                       "score": bool(x.get("isScore")), "start": start.get("text"),
                       "end": end.get("text"), "eyl": eyl, "q": _int((start.get("period") or {}).get("number")),
                       "clock": (start.get("clock") or {}).get("displayValue"), "prog": prog})

    facts = _facts(h_score, a_score, h_ls, a_ls, team_stats, groups, plays, drives)
    facts["abbr"] = {"home": h_abbr, "away": a_abbr}
    facts["final"] = final
    facts["ok"] = bool(team_stats["home"] and team_stats["away"])
    return {"home_abbr": h_abbr, "away_abbr": a_abbr, "final": final, "team_stats": team_stats,
            "groups": groups, "plays": plays, "drives": drives, "facts": facts}


def _side_facts(side, score, ts, grp, opp_grp, plays, drives, ls) -> dict:
    f: dict = {"pts": score}
    tds = [p for p in plays if p["side"] == side and p["kind"] == "touchdown"]
    f["td"] = len(tds)
    f["pass_td"] = sum(1 for p in tds if p["method"] == "pass")
    f["rush_td"] = sum(1 for p in tds if p["method"] == "rush")
    f["dst_td"] = sum(1 for p in tds if p["method"] == "dst")
    f["dst_td_h1"] = sum(1 for p in tds if p["method"] == "dst" and (p["q"] or 9) <= 2)
    f["unique_td_scorers"] = len({p["scorer"] for p in tds if p["scorer"]})
    f["first_td_method"] = (tds[0]["method"] or "other") if tds else "none"
    fgs = [p for p in plays if p["side"] == side and p["kind"] == "field-goal"]
    f["fg_dists"] = [p["fg_yds"] for p in fgs if p["fg_yds"] is not None]
    kicking = grp.get("kicking") or {}
    fg = _pair(kicking.get("FG"))
    xp = _pair(kicking.get("XP"))
    f["fgm"], f["fga"] = (fg if fg else (len(fgs), None))
    f["xpm"], f["xpa"] = (xp if xp else (sum(p["xpm"] for p in tds), sum(p["xpa"] for p in tds)))
    f["kick_pts"] = 3 * f["fgm"] + f["xpm"]
    f["two_att"] = sum(p["two_att"] for p in tds)
    f["two_made"] = sum(p["two_made"] for p in tds)
    f["safety"] = sum(1 for p in plays if p["side"] == side and p["kind"] == "safety")
    passing = grp.get("passing") or {}
    rushing = grp.get("rushing") or {}
    receiving = grp.get("receiving") or {}
    punting = grp.get("punting") or {}
    ints = grp.get("interceptions") or {}
    opp_ints = opp_grp.get("interceptions") or {}
    opp_pr = opp_grp.get("puntReturns") or {}
    defensive = grp.get("defensive") or {}
    ca = _pair(passing.get("C/ATT"))
    f["pass_cmp"], f["pass_att"] = (ca if ca else (None, None))
    f["gross_pass_yds"] = _int(passing.get("YDS"))
    f["int_thrown"] = _int(passing.get("INT")) if passing.get("INT") not in (None, "") else _int(ts.get("interceptions"))
    f["rush_yds"] = _int(rushing.get("YDS")) if rushing.get("YDS") not in (None, "") else _int(ts.get("rushingYards"))
    f["rush_att"] = _int(rushing.get("CAR")) if rushing.get("CAR") not in (None, "") else _int(ts.get("rushingAttempts"))
    f["rec_yds"] = _int(receiving.get("YDS"))
    f["total_yds"] = _int(ts.get("totalYards"))
    f["net_pass_yds"] = _int(ts.get("netPassingYards"))
    f["first_downs"] = _int(ts.get("firstDowns"))
    f["turnovers"] = _int(ts.get("turnovers"))
    f["fumbles_lost"] = _int(ts.get("fumblesLost"))
    sk = _pair(ts.get("sacksYardsLost"))
    f["sacks_taken"] = sk[0] if sk else None
    rz = _pair(ts.get("redZoneAttempts"))
    f["rz_trips"] = rz[1] if rz else None
    my_drives = [x for x in drives if x["side"] == side]
    f["drives"] = len(my_drives) if drives else None
    punt_drives = sum(1 for x in my_drives if x["result"] == "PUNT")
    f["punts"] = _int(punting.get("NO")) if punting.get("NO") not in (None, "") else (punt_drives if (drives or has_groups) else None)
    has_groups = bool(grp)                      # box score shipped player groups at all
    def _g(group, key):
        # group present -> its value (0 when blank); whole group missing but others present -> 0
        if group:
            return _int(group.get(key)) or 0
        return 0 if has_groups else None
    f["punt_yds"] = _g(punting, "YDS")
    f["punt_long"] = _g(punting, "LONG")
    f["punt_tb"] = _g(punting, "TB")
    f["punt_in20"] = _g(punting, "In 20")
    if f["punt_yds"] is not None:
        f["punt_net_yds"] = f["punt_yds"] - (_g(opp_pr, "YDS") or 0) - 20 * (f["punt_tb"] or 0)
    else:
        f["punt_net_yds"] = None
    f["int_made"] = _g(ints, "INT")
    f["pick6_made"] = _g(ints, "TD")
    f["pick6_thrown"] = _g(opp_ints, "TD")
    f["def_td"] = _g(defensive, "TD")
    f["pass_rush_yds"] = (f["gross_pass_yds"] + f["rush_yds"]) if (f["gross_pass_yds"] is not None and f["rush_yds"] is not None) else None
    f["rush_rec_yds"] = (f["rush_yds"] + f["rec_yds"]) if (f["rush_yds"] is not None and f["rec_yds"] is not None) else None
    first = next((x for x in my_drives if x["cat"] != "END"), None)
    f["first_drive"] = ({"result": first["result"], "cat": first["cat"], "prog": first["prog"],
                         "score": first["result"] in ("TD", "FG")} if first else None)
    f["q_pts"] = ls
    f["scored_every_q"] = all(x > 0 for x in ls[:4]) if len(ls) >= 4 else None
    return f


def _facts(h_score, a_score, h_ls, a_ls, team_stats, groups, plays, drives) -> dict:
    home = _side_facts("home", h_score, team_stats["home"], groups["home"], groups["away"], plays, drives, h_ls)
    away = _side_facts("away", a_score, team_stats["away"], groups["away"], groups["home"], plays, drives, a_ls)
    tds = [p for p in plays if p["kind"] == "touchdown"]
    fgs = [p for p in plays if p["kind"] == "field-goal" and p["fg_yds"] is not None]

    def _td_info(p):
        q = p["q"] or 0
        cs = _clock_secs(p["clock"])
        secs = ((q - 1) * 900 + (900 - cs)) if (q and cs is not None and q <= 4) else None
        return {"side": p["side"], "method": p["method"], "q": q, "secs": secs,
                "yds": p["td_yds"], "scorer": p["scorer"]}

    def _sum(k):
        a, b = home.get(k), away.get(k)
        return (a + b) if (a is not None and b is not None) else None
    total = (h_score or 0) + (a_score or 0)
    g = {
        "ot": len(h_ls) > 4 or any((p["q"] or 0) > 4 for p in plays),
        "periods": max(len(h_ls), 4),
        "total": total, "odd": total % 2 == 1,
        "first_td": _td_info(tds[0]) if tds else None,
        "last_td": _td_info(tds[-1]) if tds else None,
        "first_fg_dist": fgs[0]["fg_yds"] if fgs else None,
        "last_fg_dist": fgs[-1]["fg_yds"] if fgs else None,
        "fg_yds": sum(p["fg_yds"] for p in fgs) if fgs else 0,
        "unique_td_scorers": len({p["scorer"] for p in tds if p["scorer"]}),
        "any_safety": any(p["kind"] == "safety" for p in plays),
        "any_pick6": any(p["method"] == "dst" and "interception" in (p["type"] or "").lower() for p in tds),
        "any_punt_ret_td": any(p["method"] == "dst" and "punt return" in ((p["type"] or "") + " " + p["text"]).lower() for p in tds),
        "any_blocked_punt": (any("blocked punt" in p["text"].lower() for p in plays)
                             or any(x["result"].startswith("BLOCKED PUNT") for x in drives)),
    }
    for k in ("fgm", "xpm", "kick_pts", "two_att", "two_made", "punts", "punt_tb", "punt_net_yds",
              "rz_trips", "total_yds", "net_pass_yds", "gross_pass_yds", "rush_yds", "rush_att",
              "pass_td", "turnovers", "dst_td_h1"):
        g[k] = _sum(k)
    g["ints"] = _sum("int_thrown")
    return {"home": home, "away": away, "game": g}


# ──────────────────────────── storage ────────────────────────────

def _store(conn, game_id: str, parsed: dict, settle_grade: bool) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO game_stats "
        "(game_id, fetched_at, final, home_abbr, away_abbr, team_stats, group_totals, "
        " scoring_plays, drives, facts) VALUES (?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?)",
        (game_id, 1 if settle_grade else 0, parsed["home_abbr"], parsed["away_abbr"],
         json.dumps(parsed["team_stats"]), json.dumps(parsed["groups"]),
         json.dumps(parsed["plays"]), json.dumps(parsed["drives"]), json.dumps(parsed["facts"])),
    )


def ingest_game(league: str, game_id: str, *, espn_path: str | None = None, conn=None) -> str:
    """Fetch one game's summary and store it. Returns 'ok' / 'nostats' / 'notfinal' /
    'fetchfail'. A final whose box score ESPN hasn't posted yet is stored with final=0 (so
    it is retried) until ~8h after kickoff, then kept as-is (ESPN never posts stats for
    some FCS games) so the ingest doesn't loop on it forever."""
    cfg = get_league_config(league)
    espn_path = espn_path or cfg.get("espn_league_path") or f"football/{league}"
    conn = conn or get_conn(league)
    d = _fetch(f"https://site.api.espn.com/apis/site/v2/sports/{espn_path}/summary?event={game_id}")
    if not d:
        return "fetchfail"
    parsed = parse_summary(d)
    if parsed is None:
        return "fetchfail"
    if not parsed["final"]:
        return "notfinal"
    ok = parsed["facts"].get("ok")
    if ok:
        _store(conn, game_id, parsed, True)
        return "ok"
    age_h = None
    row = conn.execute("SELECT start_time FROM games WHERE game_id = ?", (game_id,)).fetchone()
    if row and row[0]:
        try:
            st = datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
            age_h = (datetime.now(timezone.utc) - st).total_seconds() / 3600.0
        except Exception:
            age_h = None
    _store(conn, game_id, parsed, settle_grade=(age_h is None or age_h > 8))
    return "nostats"


def ingest_boxscores(league: str, *, limit: int = 12, since_days: int = 14,
                     throttle: float = 0.25) -> dict:
    """Store box scores for up to ``limit`` recent finals lacking a settlement-grade row."""
    cfg = get_league_config(league)
    espn_path = cfg.get("espn_league_path") or f"football/{league}"
    conn = get_conn(league)
    rows = conn.execute(
        "SELECT g.game_id FROM games g LEFT JOIN game_stats s ON s.game_id = g.game_id "
        "WHERE g.status = 'final' AND g.date >= date('now', ?) "
        "  AND (s.game_id IS NULL OR COALESCE(s.final, 0) = 0) "
        "ORDER BY g.date DESC LIMIT ?", (f"-{int(since_days)} days", int(limit)),
    ).fetchall()
    out = {"ok": 0, "nostats": 0, "notfinal": 0, "fetchfail": 0}
    for r in rows:
        try:
            res = ingest_game(league, r[0], espn_path=espn_path, conn=conn)
        except Exception as e:
            logger.debug("[football:%s] boxscore %s failed: %s", league, r[0], e)
            res = "fetchfail"
        out[res] = out.get(res, 0) + 1
        time.sleep(throttle)
    if rows:
        logger.info("[football:%s] boxscores: %s", league, out)
    return out


def backfill(league: str, *, throttle: float = 0.25, batch: int = 50, max_games: int | None = None,
             since_days: int = 4000, progress=print) -> dict:
    totals = {"ok": 0, "nostats": 0, "notfinal": 0, "fetchfail": 0}
    done = 0
    while True:
        lim = batch if max_games is None else min(batch, max_games - done)
        if lim <= 0:
            break
        res = ingest_boxscores(league, limit=lim, since_days=since_days, throttle=throttle)
        n = sum(res.values())
        for k, v in res.items():
            totals[k] = totals.get(k, 0) + v
        done += n
        if progress:
            progress(f"[{league}] +{n} -> {totals}")
        # stop when nothing is left, or when a batch made no settlement-grade progress
        # (every remaining row is a repeat 'fetchfail'/'notfinal' we'd loop on)
        if n < lim or (res.get("ok", 0) + res.get("nostats", 0)) == 0:
            break
    return totals


def load_facts(league: str, game_id: str) -> dict | None:
    row = get_conn(league).execute(
        "SELECT facts FROM game_stats WHERE game_id = ? AND final = 1", (game_id,)).fetchone()
    if not row or not row[0]:
        return None
    try:
        f = json.loads(row[0])
    except Exception:
        return None
    return f if f.get("ok") else None


# ──────────────────────── settlement (shared) ────────────────────────
#
# Pick-text conventions (what _derivatives emits; both settlers parse):
#   game O/U:    "Over 4.5" / "Under 4.5"            team O/U:  "PIT Over 4.5"
#   game Y/N:    "Yes" / "No"                        team Y/N:  "PIT Yes"
#   BTTS_TD:     "Yes 3+" / "No 3+"                  first-TD method: "PIT Pass" | "PIT Rush" | "PIT DST" | "None"
#   first drive: FIRST_DRIVE_RESULT "PIT TD" | "PIT FGA" | "PIT Punt" | "PIT TO"

_GAME_OU = {
    "FGM": lambda g, h, a: g["fgm"], "XPM": lambda g, h, a: g["xpm"],
    "KICK_PTS": lambda g, h, a: g["kick_pts"], "FG_YDS": lambda g, h, a: g["fg_yds"],
    "LAST_FG_DIST": lambda g, h, a: g["last_fg_dist"], "FIRST_FG_DIST": lambda g, h, a: g["first_fg_dist"],
    "PASS_TD": lambda g, h, a: g["pass_td"], "INTS": lambda g, h, a: g["ints"],
    "TWO_PT_ATT": lambda g, h, a: g["two_att"], "PUNTS": lambda g, h, a: g["punts"],
    "PUNT_YDS": lambda g, h, a: g["punt_net_yds"], "PUNT_TB": lambda g, h, a: g["punt_tb"],
    "RZ_TRIPS": lambda g, h, a: g["rz_trips"], "TOTAL_YDS": lambda g, h, a: g["total_yds"],
    "PASS_YDS": lambda g, h, a: g["net_pass_yds"], "RUSH_YDS": lambda g, h, a: g["rush_yds"],
    "RUSH_ATT": lambda g, h, a: g["rush_att"], "UNIQUE_TD_SCORERS": lambda g, h, a: g["unique_td_scorers"],
    "FIRST_TD_YDS": lambda g, h, a: (g["first_td"] or {}).get("yds"),
    "DST_TD_H1": lambda g, h, a: g["dst_td_h1"],
}
_TEAM_OU = {
    "TEAM_RUSH_TD": "rush_td", "TEAM_DST_TD": "dst_td", "TEAM_TWO_PT_ATT": "two_att",
    "TEAM_PUNTS": "punts", "TEAM_PUNT_YDS": "punt_net_yds", "TEAM_PUNT_TB": "punt_tb",
    "TEAM_RZ_TRIPS": "rz_trips", "TEAM_RUSH_YDS": "rush_yds", "TEAM_PASS_RUSH_YDS": "pass_rush_yds",
    "TEAM_RUSH_REC_YDS": "rush_rec_yds", "TEAM_FGM": "fgm", "TEAM_PASS_TD": "pass_td", "TEAM_INTS": "int_thrown",
}
_GAME_YN = {
    "TWO_PT_CONV": lambda g, h, a: (g["two_made"] or 0) > 0 if g["two_made"] is not None else None,
    "SAFETY": lambda g, h, a: g["any_safety"],
    "PICK6": lambda g, h, a: g["any_pick6"],
    "PUNT_RET_TD": lambda g, h, a: g["any_punt_ret_td"],
    "BLOCKED_PUNT": lambda g, h, a: g["any_blocked_punt"],
    "FG_GT_TO": lambda g, h, a: (g["fgm"] > g["turnovers"]) if (g["fgm"] is not None and g["turnovers"] is not None) else None,
    "BTTS_FG_TD": lambda g, h, a: (h["fgm"] >= 1 and h["td"] >= 1 and a["fgm"] >= 1 and a["td"] >= 1),
}
_TEAM_YN = {
    "TEAM_TWO_PT_CONV": lambda t: t["two_made"] > 0,
    "TEAM_SAFETY": lambda t: t["safety"] > 0,
    "TEAM_PICK6": lambda t: (t["pick6_thrown"] > 0) if t["pick6_thrown"] is not None else None,
    "TEAM_FG_GT_TO": lambda t: (t["fgm"] > t["turnovers"]) if t["turnovers"] is not None else None,
    "TEAM_SCORE_EVERY_Q": lambda t: t["scored_every_q"],
    "FIRST_DRIVE_SCORE": lambda t: (t["first_drive"] or {}).get("score") if t["first_drive"] else None,
    "FIRST_DRIVE_CROSS_50": lambda t: (t["first_drive"]["prog"] > 50) if (t["first_drive"] and t["first_drive"]["prog"] is not None) else None,
    "FIRST_DRIVE_CROSS_35": lambda t: (t["first_drive"]["prog"] >= 65) if (t["first_drive"] and t["first_drive"]["prog"] is not None) else None,
    "FIRST_DRIVE_CROSS_20": lambda t: (t["first_drive"]["prog"] >= 80) if (t["first_drive"] and t["first_drive"]["prog"] is not None) else None,
}
STAT_BET_TYPES = frozenset(_GAME_OU) | frozenset(_TEAM_OU) | frozenset(_GAME_YN) | frozenset(_TEAM_YN) | frozenset(
    {"BTTS_TD", "FIRST_TD_METHOD", "LAST_TD_METHOD", "TEAM_FIRST_TD_METHOD", "FIRST_DRIVE_RESULT"})


def _ou(val, pick_rest: str) -> str | None:
    toks = pick_rest.split()
    if val is None or len(toks) < 2 or toks[0].lower() not in ("over", "under"):
        return None
    try:
        line = float(toks[1])
    except ValueError:
        return None
    if val == line:
        return "P"
    hit = val > line
    return "W" if (hit if toks[0].lower() == "over" else not hit) else "L"


def _yn(flag, pick_rest: str) -> str | None:
    tok = pick_rest.strip().split()
    if flag is None or not tok or tok[0].lower() not in ("yes", "no"):
        return None
    return "W" if (bool(flag) == (tok[0].lower() == "yes")) else "L"


def resolve_stat_market(bet_type: str, pick_text: str, facts: dict | None,
                        home_abbr: str, away_abbr: str) -> str | None:
    """'W' / 'L' / 'P', or None when the market can't be graded yet (no facts, stat not
    shipped for this league, unparseable text)."""
    bt = (bet_type or "").upper()
    if not facts or not facts.get("ok"):
        return None
    g, h, a = facts["game"], facts["home"], facts["away"]
    text = (pick_text or "").strip()
    toks = text.split()
    if not toks:
        return None

    def _team(tok):
        t = tok.upper()
        if t == (home_abbr or "").upper():
            return h
        if t == (away_abbr or "").upper():
            return a
        return None

    if bt in _GAME_OU:
        return _ou(_GAME_OU[bt](g, h, a), text)
    if bt in _TEAM_OU:
        t = _team(toks[0])
        return _ou(t.get(_TEAM_OU[bt]), " ".join(toks[1:])) if t else None
    if bt in _GAME_YN:
        return _yn(_GAME_YN[bt](g, h, a), text)
    if bt in _TEAM_YN:
        t = _team(toks[0])
        return _yn(_TEAM_YN[bt](t), " ".join(toks[1:])) if t else None
    if bt == "BTTS_TD":                                   # "Yes 3+"
        if toks[0].lower() not in ("yes", "no"):
            return None
        thr = int(toks[1].rstrip("+")) if len(toks) > 1 and toks[1].rstrip("+").isdigit() else 1
        both = h["td"] >= thr and a["td"] >= thr
        return "W" if (both == (toks[0].lower() == "yes")) else "L"
    if bt in ("FIRST_TD_METHOD", "LAST_TD_METHOD"):       # "PIT Pass" | "None"
        info = g["first_td"] if bt == "FIRST_TD_METHOD" else g["last_td"]
        if toks[0].lower() == "none":
            return "W" if info is None else "L"
        if len(toks) < 2:
            return None
        side = "home" if toks[0].upper() == (home_abbr or "").upper() else ("away" if toks[0].upper() == (away_abbr or "").upper() else None)
        if side is None:
            return None
        if info is None:
            return "L"
        return "W" if (info["side"] == side and (info["method"] or "") == toks[1].lower()) else "L"
    if bt == "TEAM_FIRST_TD_METHOD":                      # "PIT Pass" | "PIT None"
        t = _team(toks[0])
        if not t or len(toks) < 2:
            return None
        return "W" if (t.get("first_td_method") or "") == toks[1].lower() else "L"
    if bt == "FIRST_DRIVE_RESULT":                        # "PIT TD" | "PIT FGA" | "PIT Punt" | "PIT TO"
        t = _team(toks[0])
        if not t or len(toks) < 2 or not t.get("first_drive"):
            return None
        cat = t["first_drive"]["cat"]
        if cat in ("END", "OTHER"):
            return "P"
        return "W" if cat == toks[1].upper() else "L"
    return None


# ──────────────────────────── CLI ────────────────────────────

def _cli() -> int:
    import argparse
    import sys
    ap = argparse.ArgumentParser(prog="engine.football._boxscore")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_b = sub.add_parser("backfill"); p_b.add_argument("league"); p_b.add_argument("--throttle", type=float, default=0.25)
    p_b.add_argument("--max", type=int, default=None)
    p_b.add_argument("--since-days", type=int, default=4000)
    p_s = sub.add_parser("show"); p_s.add_argument("league"); p_s.add_argument("game_id")
    p_g = sub.add_parser("game"); p_g.add_argument("league"); p_g.add_argument("game_id")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.cmd == "backfill":
        print(backfill(args.league, throttle=args.throttle, max_games=args.max, since_days=args.since_days))
    elif args.cmd == "game":
        print(ingest_game(args.league, args.game_id))
        print(json.dumps(load_facts(args.league, args.game_id), indent=1)[:4000])
    elif args.cmd == "show":
        print(json.dumps(load_facts(args.league, args.game_id), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
