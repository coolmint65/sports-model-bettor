"""NBA missing-regulars adjustment, sized from the market (MintyBot 2026-10-26, Austin "does nba and NFL
have injuries also" -- the NBA had a hand-built per-player Q1 impact x 3.5 in the factor leg only).

A REGULAR played (minutes > 0) in >= half of his team's previous 20 games (player_game_logs); his value is
measured on those games: val = max(0, (points + rebounds + assists) per game - 12). For an upcoming game,
regulars on ESPN's injury report count with their chance of missing it (Out 1, Doubtful .75, Questionable /
Day-To-Day .25 -- most questionable players play), and

    home margin  += 0.12 * (val_away - val_home)        total += -0.049 * (val_home + val_away)

Fit (Pi scratchpad nbainj/nba_market.py) on 4,094 games 2023-10..2026-10 with the ACTUAL lineups: the
DraftKings / ESPN BET closing spread moves 0.109 points per unit of missing val with team-season fixed
effects (residual sd 3.25), the closing total -0.049. Outcomes vs the close: +0.018 per unit (t 1.8 -- the
close is about right, a hair light), vs the open +0.069 (t 6.8 -- injury news lands after the open). 0.12
splits the close and the outcome-implied 0.127. Example: Jalen Williams (27 PRA) out = ~1.8 points.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
import unicodedata

logger = logging.getLogger(__name__)

LOOKBACK, REG_SHARE, MIN_GAMES = 20, 0.5, 10
REPLACEMENT_PRA = 12.0
MARGIN_PER_VAL = 0.12
TOTAL_PER_VAL = -0.049
_STATUS_W = {"out": 1.0, "doubtful": 0.75, "questionable": 0.25, "day-to-day": 0.25, "suspension": 1.0}
_CACHE: dict = {}
_TTL_S = 600
_LOCK = threading.Lock()


def _norm(name: str) -> str:
    t = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode().lower()
    t = re.sub(r"\b(jr|sr|ii|iii|iv)\b\.?", "", t)
    return re.sub(r"[^a-z]", "", t)


def _short(name: str):
    t = [x for x in re.split(r"[ .]+", unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode().lower())
         if x and x not in ("jr", "sr", "ii", "iii", "iv")]
    return (re.sub(r"[^a-z]", "", t[-1]), t[0][0]) if len(t) >= 2 else None


def regulars(conn, team_id: int, before: str) -> dict | None:
    """{player_id: info} for regulars of ``team_id`` over its last LOOKBACK games before ``before``."""
    gids = [r[0] for r in conn.execute(
        "SELECT game_id FROM player_game_logs WHERE team_id = ? AND date < ? GROUP BY game_id "
        "ORDER BY MAX(date) DESC LIMIT ?", (team_id, before, LOOKBACK))]
    if len(gids) < MIN_GAMES:
        return None
    marks = ",".join("?" * len(gids))
    acc: dict = {}
    for pid, name, sj in conn.execute(
            f"SELECT player_id, player_name, stats_json FROM player_game_logs WHERE team_id = ? AND game_id IN ({marks})",
            (team_id, *gids)):
        try:
            s = json.loads(sj)
        except Exception:
            continue
        if not (s.get("min") or 0) > 0:
            continue
        a = acc.setdefault(pid, {"name": name, "n": 0, "pra": 0.0})
        a["n"] += 1
        a["pra"] += float(s.get("pts") or 0) + float(s.get("reb") or 0) + float(s.get("ast") or 0)
    out = {}
    for pid, a in acc.items():
        if a["n"] >= REG_SHARE * len(gids):
            pra = a["pra"] / a["n"]
            out[pid] = {"name": a["name"], "val": round(max(0.0, pra - REPLACEMENT_PRA), 2),
                        "pra": round(pra, 1), "gp": a["n"], "of": len(gids)}
    return out


def team_missing(conn, team_id: int, date: str, injuries: list) -> dict:
    regs = regulars(conn, team_id, date)
    res = {"val": 0.0, "players": []}
    if not regs:
        return res
    full = {}
    short: dict = {}
    for pid, r in regs.items():
        full[_norm(r["name"])] = pid
        k = _short(r["name"])
        if k:
            short.setdefault(k, []).append(pid)
    for inj in injuries or []:
        w = _STATUS_W.get(str(inj.get("status") or "").strip().lower(), 0.0)
        if not w:
            continue
        pid = full.get(_norm(inj.get("name") or ""))
        if pid is None:
            cand = short.get(_short(inj.get("name") or "")) or []
            pid = cand[0] if len(cand) == 1 else None
        if pid is None:
            continue
        r = regs[pid]
        res["val"] += w * r["val"]
        res["players"].append({"name": r["name"], "status": inj.get("status"), "w": w, "val": r["val"],
                               "pra": r["pra"], "gp": f"{r['gp']}/{r['of']}"})
    res["val"] = round(res["val"], 2)
    return res


def adjustment(conn, home_id: int, away_id: int, date: str) -> dict:
    """{'margin': home-margin points, 'total': points, 'home': ..., 'away': ...} for an upcoming game."""
    ck = (home_id, away_id, date)
    now = time.time()
    with _LOCK:
        hit = _CACHE.get(ck)
        if hit and now - hit[0] < _TTL_S:
            return hit[1]
    out = {"margin": 0.0, "total": 0.0, "home": {"val": 0.0, "players": []}, "away": {"val": 0.0, "players": []}}
    try:
        from ...injuries import fetch_nba_injuries
        rep = fetch_nba_injuries() or {}
        ab = {r[0]: (r[1] or "").upper() for r in conn.execute(
            "SELECT id, abbreviation FROM nba_teams WHERE id IN (?, ?)", (home_id, away_id))}
        hm = team_missing(conn, home_id, date, rep.get(ab.get(home_id, "")) or [])
        am = team_missing(conn, away_id, date, rep.get(ab.get(away_id, "")) or [])
        out = {"margin": round(MARGIN_PER_VAL * (am["val"] - hm["val"]), 2),
               "total": round(TOTAL_PER_VAL * (hm["val"] + am["val"]), 2), "home": hm, "away": am}
    except Exception as e:
        logger.warning("nba availability failed for %s @ %s: %s", away_id, home_id, e)
    with _LOCK:
        _CACHE[ck] = (now, out)
    return out


__all__ = ["adjustment", "regulars", "team_missing", "MARGIN_PER_VAL", "TOTAL_PER_VAL"]
