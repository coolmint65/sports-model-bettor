"""NHL lineups + the missing-regulars injury layer (MintyBot 2026-10-26, Austin "NHL has injuries and
stuff right?" -- it didn't: the ratings model priced every game at full strength).

Who dressed in each final comes from the NHL API boxscore (``/v1/gamecenter/{id}/boxscore``) into
``nhl_lineups``. A REGULAR is a player who dressed in >= half of his team's previous 20 games; his value is
measured on those games only:
    forward  F_val = max(0, points/game - 0.30)                     (0.30 ~ replacement level)
    defense  D_val = max(0, TOI min - 17) / 5 + max(0, points/game - 0.15)
    goalie   G_share = the share of those games he dressed in (a regular goalie NOT dressing = hurt / gone;
             a backup starting while the usual starter sits on the bench is NOT 'missing', as in the fit)
For an upcoming game, regulars on ESPN's injury report count as missing (Injured Reserve / Out /
Suspension 1.0, Day-To-Day / Questionable 0.5). Regulation scoring rates then move by

    lam_own *= exp(-0.018 F_own - 0.0022 D_own + 0.0263 F_opp + 0.0099 D_opp + 0.0231 G_opp)

Fit (Pi scratchpad nhlpa/bt/inj_fit_market.py) on 4,207 games 2023-26 with the ACTUAL lineups, against the
CLOSING line: the close prices missing regulars correctly (outcome residual vs the close t 0.5; vs the open
t 1.4 -- injury news lands after the open), so the market is the precise target and outcomes are too noisy
(GLM t ~1.5, same signs). Holdout Jul 2025 - Oct 2026: moneyline log-loss .6778 -> .6775, mean squared
logit gap to the close -10%, correlation with the open->close move .32 -> .42. Not an edge source -- it
removes the fake 'value' an injury-blind model sees on a team missing its stars. Goalie-quality and
backup-in-net features had already tested flat on outcomes (2026-10-26 rebuild).
"""
from __future__ import annotations

import json
import logging
import math
import re
import threading
import time
import unicodedata
import urllib.request
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

_BOX_URL = "https://api-web.nhle.com/v1/gamecenter/{gid}/boxscore"
LOOKBACK, REG_SHARE, MIN_GAMES = 20, 0.5, 10          # as in the fit (inj_feats.py)
F_REPL, D_REPL = 0.30, 0.15
INJ_COEF = (-0.018, -0.0022, 0.0263, 0.0099, 0.0231)      # own F, own D, opp F, opp D, opp G
_STATUS_W = {"injured reserve": 1.0, "out": 1.0, "suspension": 1.0, "suspended": 1.0,
             "day-to-day": 0.5, "questionable": 0.5}
_CACHE: dict = {}
_CACHE_TTL_S = 600
_LOCK = threading.Lock()


def _ensure(conn) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS nhl_lineups(game_id INTEGER, team TEXT, player_id INTEGER,
        name TEXT, pos TEXT, toi_s INTEGER, pts INTEGER, starter INTEGER, PRIMARY KEY(game_id, player_id))""")
    conn.execute("CREATE INDEX IF NOT EXISTS nhl_lineups_team ON nhl_lineups(team, game_id)")


def _get(url: str, retries: int = 3):
    for k in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "curl/8.4.0", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            logger.debug("nhl boxscore %s attempt %d: %s", url, k + 1, e)
            time.sleep(1.5 * (k + 1))
    return None


def _toi(s) -> int | None:
    try:
        m, sec = str(s).split(":")
        return int(m) * 60 + int(sec)
    except Exception:
        return None


def sync_game(gid: int, conn) -> int:
    """Fetch first, then write + commit at once: nhl.db is shared with the backend's odds / goalie / pick
    writers, and a write transaction left open across HTTP fetches made them fail 'database is locked'
    (the first backfill committed every 50 games, 2026-10-26)."""
    d = _get(_BOX_URL.format(gid=gid))
    if not d or d.get("gameState") not in ("FINAL", "OFF"):
        return 0
    n = 0
    for side in ("homeTeam", "awayTeam"):
        team = (d.get(side) or {}).get("abbrev")
        blk = (d.get("playerByGameStats") or {}).get(side) or {}
        for grp, pos in (("forwards", "F"), ("defense", "D"), ("goalies", "G")):
            for p in blk.get(grp) or []:
                conn.execute("INSERT OR REPLACE INTO nhl_lineups VALUES (?,?,?,?,?,?,?,?)",
                             (gid, team, p.get("playerId"), (p.get("name") or {}).get("default"), pos,
                              _toi(p.get("toi")), p.get("points") or 0, 1 if p.get("starter") else 0))
                n += 1
    conn.commit()
    return n


def _missing_games(conn, since: str) -> list[int]:
    _ensure(conn)
    return [r[0] for r in conn.execute(
        "SELECT g.game_id FROM nhl_games g WHERE g.status = 'final' AND g.game_type IN (2, 3) AND g.date >= ? "
        "AND NOT EXISTS (SELECT 1 FROM nhl_lineups l WHERE l.game_id = g.game_id) ORDER BY g.date", (since,))]


def sync_recent(days: int = 10, conn=None) -> int:
    """Worker entry: lineups for every final in the last ``days`` that doesn't have one yet."""
    if conn is None:
        from .db import get_conn
        conn = get_conn()
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    n = 0
    for gid in _missing_games(conn, since):
        if sync_game(gid, conn):
            n += 1
    conn.commit()
    return n


def backfill(since: str = "2026-02-01", pause: float = 0.2) -> int:
    from .db import get_conn
    conn = get_conn()
    n = 0
    for gid in _missing_games(conn, since):
        if sync_game(gid, conn):
            n += 1
        time.sleep(pause)
    return n


def _key(name: str):
    """('mcdavid', 'c') from 'Connor McDavid' (ESPN) or 'C. McDavid' (NHL boxscore)."""
    t = re.sub(r"[^a-z .'-]", "", unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode().lower())
    toks = [x for x in re.split(r"[ .]+", t) if x]
    if len(toks) < 2:
        return None
    return toks[-1].replace("'", "").replace("-", ""), toks[0][0]


def regulars(conn, team: str, before: str) -> dict | None:
    """{player_id: info} for players who dressed in >= REG_SHARE of the team's last LOOKBACK games before
    ``before`` (YYYY-MM-DD); None when the team has fewer than MIN_GAMES lineups. Read-only (no _ensure:
    prediction paths must never take the write lock)."""
    gids = [r[0] for r in conn.execute(
        "SELECT DISTINCT l.game_id FROM nhl_lineups l JOIN nhl_games g ON g.game_id = l.game_id "
        "WHERE l.team = ? AND g.date < ? ORDER BY g.date DESC LIMIT ?", (team, before, LOOKBACK))]
    if len(gids) < MIN_GAMES:
        return None
    acc: dict = {}
    marks = ",".join("?" * len(gids))
    for pid, name, pos, toi, pts, starter in conn.execute(
            f"SELECT player_id, name, pos, toi_s, pts, starter FROM nhl_lineups WHERE team = ? AND game_id IN ({marks})",
            (team, *gids)):
        a = acc.setdefault(pid, {"name": name, "pos": pos, "n": 0, "toi": 0.0, "pts": 0.0, "starts": 0})
        a["n"] += 1; a["toi"] += toi or 0; a["pts"] += pts or 0; a["starts"] += starter or 0
    out = {}
    for pid, a in acc.items():
        if a["n"] < REG_SHARE * len(gids):
            continue
        ppg, toi_m = a["pts"] / a["n"], a["toi"] / a["n"] / 60.0
        if a["pos"] == "F":
            val = max(0.0, ppg - F_REPL)
        elif a["pos"] == "D":
            val = max(0.0, toi_m - 17.0) / 5.0 + max(0.0, ppg - D_REPL)
        else:
            val = a["n"] / len(gids)
        out[pid] = {"name": a["name"], "pos": a["pos"], "val": round(val, 3), "gp": a["n"], "of": len(gids),
                    "ppg": round(ppg, 2), "toi": round(toi_m, 1)}
    return out


def team_missing(conn, team: str, game_date: str, injuries: list) -> dict:
    """Missing value for one team in one upcoming game (regulars on ESPN's injury report)."""
    regs = regulars(conn, team, game_date)
    res = {"F_val": 0.0, "D_val": 0.0, "G_share": 0.0, "players": []}
    if not regs:
        return res
    by_key = {}
    for pid, r in regs.items():
        k = _key(r["name"])
        if k:
            by_key.setdefault(k, []).append(pid)
    for inj in injuries or []:
        w = _STATUS_W.get(str(inj.get("status") or "").strip().lower(), 0.0)
        pids = by_key.get(_key(inj.get("name") or "")) or []
        if not w or len(pids) != 1:
            continue
        r = regs[pids[0]]
        if r["pos"] == "F":
            res["F_val"] += w * r["val"]
        elif r["pos"] == "D":
            res["D_val"] += w * r["val"]
        else:
            res["G_share"] = max(res["G_share"], w * r["val"])
        res["players"].append({"name": inj.get("name"), "pos": r["pos"], "status": inj.get("status"),
                               "w": w, "val": r["val"], "gp": f"{r['gp']}/{r['of']}"})
    for k in ("F_val", "D_val", "G_share"):
        res[k] = round(res[k], 3)
    return res


def injury_multipliers(home: str, away: str, game_date: str, home_inj: list, away_inj: list) -> tuple:
    """(mult_home, mult_away, detail) for an upcoming game; (1, 1, None) when lineups are thin."""
    ck = (home, away, game_date)
    now = time.time()
    with _LOCK:
        hit = _CACHE.get(ck)
        if hit and now - hit[0] < _CACHE_TTL_S:
            return hit[1]
    try:
        from .db import get_conn
        conn = get_conn()
        mh = team_missing(conn, home, game_date, home_inj)
        ma = team_missing(conn, away, game_date, away_inj)
        bF, bD, cF, cD, cG = INJ_COEF
        mult_h = math.exp(bF * mh["F_val"] + bD * mh["D_val"] + cF * ma["F_val"] + cD * ma["D_val"] + cG * ma["G_share"])
        mult_a = math.exp(bF * ma["F_val"] + bD * ma["D_val"] + cF * mh["F_val"] + cD * mh["D_val"] + cG * mh["G_share"])
        out = (mult_h, mult_a, {"home": mh, "away": ma, "mult": [round(mult_h, 4), round(mult_a, 4)]})
    except Exception as e:
        logger.warning("nhl injury layer failed for %s @ %s: %s", away, home, e)
        out = (1.0, 1.0, None)
    with _LOCK:
        _CACHE[ck] = (now, out)
    return out


__all__ = ["sync_recent", "backfill", "regulars", "team_missing", "injury_multipliers", "INJ_COEF"]
