"""Model Health -- one page that tells the truth about every PickMint model (MintyBot 2026-11-01,
Austin's PickMint V3 choice: "real W-L / $ / ROI vs break-even, not CLV").

READ-ONLY. Nothing here writes to a pick table: every connection is opened ``mode=ro`` and the gate
functions it calls are the live ones (engine.selection.edge_proven, engine.sports.nhl.card_gate,
engine.sports.nfl.prop_picks.card_gate/shadow_cells, engine.player_props_potd.cell_records).

One row per sport x market (bet_type) x side, counted the way its CARD GATE counts it, since the
current model / evidence window:

  game markets  data/picks_unified.db ``picks`` (real + shadow lanes). The Pick Authority gate's own
                population: edge_proven._segment_and_window (evidence windows _EVIDENCE_SINCE + the
                FCS re-key), then ONE TRIAL PER GAME / MARKET / SIDE (median rung, as _compute does).
                Status = edge_proven.is_eligible(). Every row is cross-checked against
                edge_proven._map() (n / win% / ROI) and flagged if they ever disagree.
  card          the sport's recorded-authority table = what its tracker grades (first pick wins):
                nba_paper_picks / nhl_picks(is_shadow=1) / football picks(is_shadow=0) /
                basketball picks(is_shadow=1) / EPL picks -- shown per row as a sub-record.
  NHL props     player_props_picks (real card) + prop_card_shadow since card_gate.SINCE.
                Status = card_gate.is_proven() (n>=30, ROI > 1 SE); unproven -> PAUSED.
  NFL props     prop_eval_log replay of the live rule (prop_picks.shadow_cells, re-run here row by
                row with the module's own _score so 7d/30d/calibration can be shown). Status =
                card_gate()["proven"] (n>=100, margin > 1.5 SE); card = player_props_picks.
  NBA props     player_props_picks since player_props_potd.MODEL_SINCE. There is NO card gate --
                every rule pick goes on the real card -- so status is ON CARD / UNPROVEN unless the
                props-POTD proven-cell bar (n>=20, margin > 0.5 SE) is met.

Built by a background thread (start_background, every INTERVAL_S) into data/model_health.json; the
endpoint only ever serves that cache.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

VERSION = 1
INTERVAL_S = 600
INITIAL_DELAY_S = 240
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_DATA = os.path.join(_ROOT, "data")
CACHE_PATH = os.path.join(_DATA, "model_health.json")
PU_DB = os.path.join(_DATA, "picks_unified.db")
NBA_DB = os.path.join(_DATA, "nba.db")
NHL_DB = os.path.join(_DATA, "nhl.db")
MLB_DB = os.path.join(_DATA, "mlb.db")
GAME_ODDS_DB = os.path.join(_DATA, "game_odds.db")
PROV_DB = os.path.join(_DATA, "pick_provenance.db")
LEAN_LOCK = os.path.join(_DATA, "card_lean_lock.json")


def _fb_db(lg):
    return os.path.join(_DATA, "football", f"{lg}.db")


def _bb_db(lg):
    return os.path.join(_DATA, "basketball", f"{lg}.db")


NFL_PROPS_DB = os.path.join(_DATA, "football", "nfl_player.db")
EPL_DB = os.path.join(_DATA, "soccer", "eng_premier.db")

# (picks_unified sport, league) -> (filter chip, label). Only the leagues PickMint still runs
# (smaller leagues removed 10-27, golf/motorsports 10-22, tennis 10-17).
GAME_LEAGUES = {
    ("nba", "nba"): ("NBA", "NBA"),
    ("nhl", "nhl"): ("NHL", "NHL"),
    ("football", "nfl"): ("NFL", "NFL"),
    ("football", "cfb"): ("CFB", "CFB"),
    ("football", "cfb_fcs"): ("CFB", "CFB (FCS)"),
    ("basketball", "ncaam"): ("NCAAM", "NCAAM"),
    ("basketball", "ncaaw"): ("NCAAW", "NCAAW"),
    ("basketball", "china_cba"): ("CBA", "China CBA"),
    ("basketball", "australia_nbl"): ("NBL", "Australia NBL"),
    ("basketball", "wnba"): ("WNBA", "WNBA"),
    ("soccer", "eng_premier"): ("EPL", "EPL"),
    ("mlb", "mlb"): ("MLB", "MLB"),
}
SPORT_ORDER = ["NBA", "NHL", "NFL", "CFB", "NCAAM", "NCAAW", "CBA", "NBL", "EPL", "WNBA", "MLB"]
# Sports switched off in the app (App.jsx / backend prewarm: "MLB OFF 2026-10-29").
OFF_SPORTS = {"MLB": "MLB is off for the season (turned off 10-29: no slate, no card, no props)"}
# Model changes that landed AFTER the gate's evidence window opened (memory notes, verified in code).
MODEL_NOTES = {
    "NFL": "NFL game model switched to the EPA ridge 11-01 00:50Z (engine/football/_predict.py, NFL_EPA "
           "on by default) but edge_proven has no NFL evidence window, so the gate still counts all-time "
           "NFL picks (mostly the old Elo model).",
    "EPL": "EPL ratings changed again 11-01 00:50Z (half-life 450 d + football-data history); the gate's "
           "EPL window still starts 10-29.",
}
LIVE_ONLY = re.compile(r"^LIVE_", re.I)   # NHL live model: display + shadow only (engine/live graduated=False)
STALE_AFTER_H = 24.0
GAME_LEN_H = 4.0
IMMINENT_H = 6
# Card holds the tracker's recorded pick from here on (NBA card-hold 11-01 11:39 ET; basketball/EPL/NBA/football
# first-pick-wins the same day) -- card/tracker mismatches before it are re-graded history, listed but not counted.
CARD_FIX_DATE = "2026-11-01"


# --------------------------------------------------------------------------- small helpers
def _ro(path):
    c = sqlite3.connect("file:" + os.path.abspath(path) + "?mode=ro", uri=True, timeout=15)
    c.row_factory = sqlite3.Row
    return c


def _exists(path):
    try:
        return os.path.getsize(path) > 0
    except OSError:
        return False


def _dec(odds):
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0 or abs(o) < 100:
        return None
    return 1.0 + (o / 100.0 if o > 0 else 100.0 / abs(o))


def _imp(odds):
    d = _dec(odds)
    return (1.0 / d) if d else None


def _now_utc():
    return datetime.now(timezone.utc)


def _today_et():
    try:
        from engine._tz import et_today_str
        return et_today_str()
    except Exception:
        return (_now_utc() - timedelta(hours=5)).strftime("%Y-%m-%d")


def _et_str(dt):
    try:
        from engine._tz import _US_EASTERN
        if _US_EASTERN is not None:
            return dt.astimezone(_US_EASTERN).strftime("%m-%d %H:%M ET")
    except Exception:
        pass
    return dt.strftime("%m-%d %H:%MZ")


def _parse_ts(s):
    """ISO string ('2026-11-02T02:00Z', '2026-11-02 02:59:25', with/without tz) or epoch ms -> aware UTC."""
    if s is None or s == "":
        return None
    try:
        if isinstance(s, (int, float)) or re.fullmatch(r"\d{11,14}(\.\d+)?", str(s)):
            return datetime.fromtimestamp(float(s) / 1000.0, tz=timezone.utc)
        t = str(s).strip().replace(" ", "T")
        dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


_DAYS_MEMO: dict = {}


def _days_ago(today, n):
    k = (today, n)
    v = _DAYS_MEMO.get(k)
    if v is None:
        v = _DAYS_MEMO[k] = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=n)).strftime("%Y-%m-%d")
    return v


def _market_label(bt):
    keep = {"ML", "Q1", "Q2", "Q3", "Q4", "H1", "H2", "P1", "P2", "P3", "O/U", "DNB", "DC",
            "BTTS", "GIBH", "SOG", "PRA", "1ST", "INN", "F5", "TD", "NRFI"}
    out = []
    for w in str(bt or "").replace("_", " ").split():
        u = w.upper()
        out.append("3-Way ML" if u == "ML3" else "O/U" if u == "OU" else u if u in keep else w.capitalize())
    return " ".join(out)


_SIDE_LABEL = {"over": "Over", "under": "Under", "fav": "Fav", "dog": "Dog", "": "—"}


# --------------------------------------------------------------------------- record maths
def _trial(res, odds, date, prob=None, close=None, stake=None, extra=None):
    t = {"res": res, "odds": odds, "date": date or "", "prob": prob, "close": close, "stake": stake}
    if extra:
        t.update(extra)
    return t


def _agg(trials, pushes=0, pending=0):
    """Record over W/L trials (one flat unit each at the logged odds)."""
    tr = [t for t in trials if t["res"] in ("W", "L") and _dec(t["odds"])]
    n = len(tr)
    out = {"n": n, "w": 0, "l": 0, "p": int(pushes), "pending": int(pending), "win_pct": None,
           "be_pct": None, "margin_pp": None, "roi": None, "roi_se": None, "units": None,
           "claimed": None, "cal_n": 0, "cal_gap": None, "clv": None, "clv_n": 0,
           "staked_units": None, "staked_risk": None, "staked_roi": None, "staked_n": 0,
           "first": None, "last": None}
    if not n:
        return out
    pl, be = [], 0.0
    for t in tr:
        d = _dec(t["odds"])
        be += 1.0 / d
        pl.append((d - 1.0) if t["res"] == "W" else -1.0)
    w = sum(1 for t in tr if t["res"] == "W")
    m = sum(pl) / n
    se = math.sqrt(sum((x - m) ** 2 for x in pl) / (n - 1) / n) if n > 1 else None
    out.update(n=n, w=w, l=n - w, win_pct=round(100.0 * w / n, 1), be_pct=round(100.0 * be / n, 1),
               roi=round(100.0 * m, 1), roi_se=(round(100.0 * se, 1) if se is not None else None),
               units=round(sum(pl), 2))
    out["margin_pp"] = round(out["win_pct"] - out["be_pct"], 1)
    probs = [float(t["prob"]) for t in tr if t.get("prob") is not None and 0.0 < float(t["prob"]) < 1.0]
    if probs:
        out["claimed"] = round(100.0 * sum(probs) / len(probs), 1)
        out["cal_n"] = len(probs)
        hit = sum(1 for t in tr if t.get("prob") is not None and 0.0 < float(t["prob"]) < 1.0
                  and t["res"] == "W") / len(probs)
        out["cal_gap"] = round(out["claimed"] - 100.0 * hit, 1)
    clv = []
    for t in tr:
        ci, ei = _imp(t.get("close")), _imp(t["odds"])
        if ci is not None and ei is not None:
            clv.append(100.0 * (ci - ei))
    if clv:
        out["clv"] = round(sum(clv) / len(clv), 2)
        out["clv_n"] = len(clv)
    st = [(float(t["stake"]), x) for t, x in zip(tr, pl) if t.get("stake") is not None and float(t["stake"]) > 0]
    if st:
        su = sum(s * x for s, x in st)
        sr = sum(s for s, _ in st)
        out.update(staked_units=round(su, 2), staked_risk=round(sr, 2),
                   staked_roi=round(100.0 * su / sr, 1) if sr else None, staked_n=len(st))
    ds = sorted(t["date"] for t in tr if t["date"])
    if ds:
        out["first"], out["last"] = ds[0], ds[-1]
    return out


def _short(rec):
    """Compact window record for 7d / 30d / legacy / card cells."""
    if not rec:
        return None
    return {k: rec.get(k) for k in ("n", "w", "l", "p", "pending", "win_pct", "be_pct", "roi", "roi_se",
                                    "units", "staked_units", "staked_n", "first", "last")}


def _windows(trials, today):
    d7, d30 = _days_ago(today, 6), _days_ago(today, 29)
    return (_short(_agg([t for t in trials if t["date"] >= d7])),
            _short(_agg([t for t in trials if t["date"] >= d30])))


# --------------------------------------------------------------------------- NBA box-score regrade
_SCOPE_TAIL = re.compile(r"\s+(Q[1-4]|[12]H|H[12])\s*$", re.I)
_SCOPE_MID = re.compile(r"\s+(Q1|1H|2H|H1|H2)\s+", re.I)


def _nba_scope(bt):
    b = str(bt or "").upper().replace(" ", "_")
    for s in ("H1", "H2", "Q1"):
        if b.startswith(s):
            return s
    return "FULL"


def _scope_pts(g, sc):
    def _i(x):
        return None if x is None else int(x)
    hq = [_i(g.get("home_q%d" % k)) for k in (1, 2)]
    aq = [_i(g.get("away_q%d" % k)) for k in (1, 2)]
    hs, as_ = _i(g.get("home_score")), _i(g.get("away_score"))
    if sc == "Q1":
        return None if None in (hq[0], aq[0]) else (hq[0], aq[0])
    if sc == "H1":
        return None if None in hq + aq else (hq[0] + hq[1], aq[0] + aq[1])
    if sc == "H2":
        return None if None in hq + aq + [hs, as_] else (hs - hq[0] - hq[1], as_ - aq[0] - aq[1])
    return None if None in (hs, as_) else (hs, as_)


def nba_box_grade(bet_type, pick_text, game, home_abbrs, away_abbrs):
    """Grade an NBA pick from nba_games quarter/final scores (2H = final - 1H, OT included, as the
    tracker grades it). Returns ('W'|'L'|'P', scope, (home_pts, away_pts)) or None if unparseable."""
    sc = _nba_scope(bet_type)
    pts = _scope_pts(game, sc)
    if pts is None:
        return None
    hp, ap = pts
    b = str(bet_type or "").upper().replace(" ", "_")
    txt = _SCOPE_TAIL.sub("", str(pick_text or "").strip())
    txt = _SCOPE_MID.sub(" ", txt).strip()

    def _side(team):
        t = (team or "").upper()
        if t in home_abbrs:
            return hp, ap
        if t in away_abbrs:
            return ap, hp
        return None

    if "TOTAL" in b:
        m = re.match(r"^(?:([A-Za-z]{2,5})\s+)?(Over|Under)\s+(\d+(?:\.\d+)?)$", txt, re.I)
        if not m:
            return None
        if "TEAM_TOTAL" in b:
            s = _side(m.group(1))
            if s is None:
                return None
            val = s[0]
        else:
            val = hp + ap
        line, over = float(m.group(3)), m.group(2).lower() == "over"
        res = "P" if val == line else ("W" if (val > line) == over else "L")
        return res, sc, pts
    if "SPREAD" in b:
        m = re.match(r"^([A-Za-z]{2,5})\s+([+-]?\d+(?:\.\d+)?)$", txt)
        if not m:
            return None
        s = _side(m.group(1))
        if s is None:
            return None
        d = s[0] - s[1] + float(m.group(2))
        return ("P" if d == 0 else ("W" if d > 0 else "L")), sc, pts
    if b.endswith("ML"):
        m = re.match(r"^([A-Za-z]{2,5})(?:\s+ML)?$", txt)
        if not m:
            return None
        s = _side(m.group(1))
        if s is None:
            return None
        d = s[0] - s[1]
        return ("P" if d == 0 else ("W" if d > 0 else "L")), sc, pts
    return None


class _NbaBox:
    """nba_games (final) + team abbreviations, loaded once per build."""

    def __init__(self):
        self.games, self.abbr = {}, {}
        if not _exists(NBA_DB):
            return
        c = _ro(NBA_DB)
        try:
            self.abbr = {r["id"]: (r["abbreviation"] or "").upper() for r in c.execute("SELECT id, abbreviation FROM nba_teams")}
            for r in c.execute("SELECT * FROM nba_games WHERE status='final' AND date >= '2026-09-01'"):
                self.games[str(r["game_id"])] = dict(r)
        finally:
            c.close()

    def grade(self, game_id, matchup, bet_type, pick_text):
        g = self.games.get(str(game_id))
        if not g:
            return None
        home, away = {self.abbr.get(g["home_team_id"], "")}, {self.abbr.get(g["away_team_id"], "")}
        if matchup and "@" in matchup:
            a, h = [x.strip().upper() for x in matchup.split("@", 1)]
            home.add(h)
            away.add(a)
        home.discard("")
        away.discard("")
        return nba_box_grade(bet_type, pick_text, g, home, away)


# --------------------------------------------------------------------------- game markets
def _line_of(r):
    """edge_proven._compute's median-rung key (copied verbatim -- it is a nested function there)."""
    try:
        if r["line"] is not None:
            return float(r["line"])
    except (TypeError, ValueError):
        pass
    m = re.findall(r"-?\d+\.?\d*", str(r["pick_text"] or ""))
    return float(m[-1]) if m else 0.0


def _gate_reason(ep, cell, key):
    if not cell:
        return "not graduated: no graded picks in the current window yet"
    n, roi, conf = cell["n"], cell["roi"], cell["conf"]
    floor = ep._CONF_FLOORS.get(key, ep.MIN_CONF)
    if cell.get("eligible"):
        path = (f"n {n} >= {ep.STD_N}" if n >= ep.STD_N
                else f"fast-track n {n} >= {ep.FAST_N} at conf {conf:.2f} >= {ep.FAST_CONF}")
        return f"graduated: ROI {roi:+.1f}% > +{ep.MIN_ROI:g}%, conf {conf:.2f} >= {floor:.2f}, {path}"
    fails = []
    if not roi > ep.MIN_ROI:
        fails.append(f"ROI {roi:+.1f}% <= +{ep.MIN_ROI:g}%")
    if not conf >= floor:
        fails.append(f"conf {conf:.2f} < {floor:.2f}")
    if not (n >= ep.STD_N or (n >= ep.FAST_N and conf >= ep.FAST_CONF)):
        fails.append(f"n {n} < {ep.STD_N}" + (f" (fast-track at n >= {ep.FAST_N} needs conf >= {ep.FAST_CONF})"
                                              if n >= ep.FAST_N else ""))
    return "not graduated: " + ("; ".join(fails) if fails else "gate says no")


def _fcs_ids():
    try:
        from engine.football._segments import fcs_game_ids, FCS_LEAGUE
        return fcs_game_ids(), FCS_LEAGUE
    except Exception:
        return frozenset(), "cfb_fcs"


def _segment(ep, sp, lg, gk, bt, fcs, fcs_lg):
    lg = ep._norm_lg(lg)
    if sp == "football" and lg == "cfb" and fcs and str(gk).rsplit(":", 1)[-1] in fcs:
        lg = fcs_lg
    nbt = ep._norm_bt(bt)
    since = ep._EVIDENCE_SINCE.get((str(sp), lg, nbt)) or ep._EVIDENCE_SINCE.get((str(sp), lg, ep._family(nbt)))
    return lg, nbt, since


_CARD_MEMO: dict = {"key": None, "rows": None}


def _card_rows(today):
    """Per-build memo of _card_rows_uncached (three consumers, one read)."""
    if _CARD_MEMO["key"] != _BUILD_ID["v"] or _CARD_MEMO["rows"] is None:
        _CARD_MEMO.update(key=_BUILD_ID["v"], rows=_card_rows_uncached(today))
    return [dict(r) for r in _CARD_MEMO["rows"]]


_BUILD_ID = {"v": 0}


def _card_rows_uncached(today):
    """Recorded-authority picks per sport = what each sport's tracker grades (first pick wins).
    -> list of dicts {sport, league, game_id, game_key, date, matchup, bet_type, pick_text, odds, prob,
    result, stake, close}."""
    out = []

    def add(sp, lg, r, pick_text, gid_col="game_id", stake_col="stake_units"):
        gid = str(r[gid_col])
        out.append({"sport": sp, "league": lg, "game_id": gid, "game_key": f"{sp}:{lg}:{gid}",
                    "date": r["date"], "matchup": r["matchup"], "bet_type": r["bet_type"],
                    "pick_text": pick_text, "odds": r["odds"], "prob": r["model_prob"],
                    "result": r["result"], "stake": r[stake_col] if stake_col else None,
                    "close": r["closing_odds"]})
    # NBA: earliest non-void nba_paper_picks row per game (_nba_recorded_authority_best)
    if _exists(NBA_DB):
        c = _ro(NBA_DB)
        try:
            for r in c.execute(
                    "SELECT p.* FROM nba_paper_picks p JOIN (SELECT game_id, MIN(id) mid FROM nba_paper_picks "
                    "WHERE result IS NULL OR result != 'V' GROUP BY game_id) e ON p.id = e.mid"):
                add("nba", "nba", r, r["pick"])
        finally:
            c.close()
    # NHL: the paper card lean, one is_shadow=1 row per game (test rows excluded)
    if _exists(NHL_DB):
        c = _ro(NHL_DB)
        try:
            for r in c.execute(
                    "SELECT p.* FROM nhl_picks p JOIN (SELECT game_id, MIN(id) mid FROM nhl_picks WHERE "
                    "COALESCE(is_shadow,0)=1 AND (result IS NULL OR result != 'V') AND game_id NOT LIKE 'zz%' "
                    "GROUP BY game_id) e ON p.id = e.mid"):
                add("nhl", "nhl", r, r["pick"])
        finally:
            c.close()
    # Football: earliest non-void is_shadow=0 row per game (_football_recorded_authority_pick)
    for lg in ("nfl", "cfb"):
        if not _exists(_fb_db(lg)):
            continue
        c = _ro(_fb_db(lg))
        try:
            for r in c.execute(
                    "SELECT p.* FROM picks p JOIN (SELECT game_id, MIN(id) mid FROM picks WHERE COALESCE(is_shadow,0)=0 "
                    "AND (result IS NULL OR result != 'V') GROUP BY game_id) e ON p.id = e.mid"):
                add("football", lg, r, _disp_pick(r["bet_type"], r["pick"], r["line"]))
        finally:
            c.close()
    # Basketball framework: is_shadow=1 rows (_basketball_recorded_authority_best): PA leagues -> first
    # row; other leagues (WNBA) -> best Full / best Q1 by edge x prob, then the better of the two.
    try:
        from engine.basketball._config import get_league_config as _glc
    except Exception:
        _glc = None
    for lg in ("ncaam", "ncaaw", "china_cba", "australia_nbl", "wnba"):
        if not _exists(_bb_db(lg)):
            continue
        try:
            pa = bool(_glc(lg).get("pick_authority")) if _glc else (lg != "wnba")
        except Exception:
            pa = lg != "wnba"
        c = _ro(_bb_db(lg))
        try:
            rows = c.execute("SELECT * FROM picks WHERE COALESCE(is_shadow,0)=1 AND (result IS NULL OR result != 'V') "
                             "ORDER BY id").fetchall()
        finally:
            c.close()
        by = {}
        for r in rows:
            by.setdefault(str(r["game_id"]), []).append(r)
        for gid, rs in by.items():
            if pa:
                b = rs[0]
            else:
                def _epr(p):
                    return float(p["edge"] or 0) * float(p["model_prob"] or 0)
                q1 = [r for r in rs if str(r["bet_type"] or "").upper().startswith(("Q1_", "Q1 "))]
                full = [r for r in rs if r not in q1]
                mains = [x for x in ((max(full, key=_epr) if full else None), (max(q1, key=_epr) if q1 else None)) if x]
                b = max(mains, key=_epr)
            add("basketball", lg, b, b["pick"])
    # EPL: earliest non-void picks row per match (_soccer_recorded_authority_best, PA league)
    if _exists(EPL_DB):
        c = _ro(EPL_DB)
        try:
            for r in c.execute(
                    "SELECT p.* FROM picks p JOIN (SELECT match_id, MIN(id) mid FROM picks WHERE result IS NULL "
                    "OR result != 'V' GROUP BY match_id) e ON p.id = e.mid"):
                add("soccer", "eng_premier", r, r["pick"], gid_col="match_id")
        finally:
            c.close()
    return out


def _disp_pick(bet_type, pick, line):
    """routes_football._disp_pick (football stores side + line separately)."""
    p = (pick or "").strip()
    if line is None or line == "":
        return p
    try:
        lf = float(line)
    except (TypeError, ValueError):
        return p
    toks = p.split()
    if toks and any(ch.isdigit() for ch in toks[-1]):
        return p
    ls = "%g" % lf
    bt = (bet_type or "").upper()
    if "TOTAL" in bt or bt in ("O/U", "OU"):
        return "%s %s" % (p, ls)
    if "SPREAD" in bt or bt in ("RL", "RUN_LINE", "PL", "PUCK_LINE", "SPRD"):
        return "%s %s%s" % (p, "+" if lf > 0 else "", ls)
    return p


def _game_market_rows(today, box, diag):
    from engine.selection import edge_proven as ep
    _t = time.time()
    gmap = ep._map()          # the cached map the cards use right now (TTL ep.TTL)
    fresh = ep._compute()     # the same code on the current data, no cache side effects (self-check)
    diag.setdefault("timing_ms", {})["g_gate_compute"] = int((time.time() - _t) * 1000)
    fcs, fcs_lg = _fcs_ids()
    c = _ro(PU_DB)
    try:
        raw = [dict(r) for r in c.execute(
            "SELECT id, sport, league, game_key, pick_date, matchup, bet_type, pick_text, line, odds, closing_odds, "
            "prob, stake_units, is_shadow, result, created_at FROM picks")]
    finally:
        c.close()
    # self-check: our window filter must keep exactly the rows the gate keeps
    proj = [(r["sport"], r["league"], r["bet_type"], r["pick_text"], r["odds"], r["result"], r["game_key"],
             r["id"], r["line"], r["pick_date"]) for r in raw if r["result"] in ("W", "L") and r["odds"] is not None]
    gate_ids = {t[7] for t in ep._segment_and_window(proj)}
    rows = []
    for r in raw:
        lg, nbt, since = _segment(ep, r["sport"], r["league"], r["game_key"], r["bet_type"], fcs, fcs_lg)
        if (r["sport"], lg) not in GAME_LEAGUES:
            continue
        r["lg"], r["nbt"], r["since"] = lg, nbt, since
        r["in_win"] = (not since) or str(r["pick_date"] or "") >= since
        r["cond"] = ep._norm_cond(ep._condition(r["pick_text"], r["bet_type"], r["odds"]))
        rows.append(r)
    mine_ids = {r["id"] for r in rows if r["in_win"] and r["result"] in ("W", "L") and r["odds"] is not None}
    scope_ids = {r["id"] for r in rows}
    gate_scope_ids = {i for i in gate_ids if i in scope_ids}
    diag["window_selfcheck"] = {"ours": len(mine_ids), "gate": len(gate_scope_ids),
                                "ok": mine_ids == gate_scope_ids}

    def trials_of(rs):
        groups = {}
        for r in rs:
            if r["result"] in ("W", "L") and r["odds"] is not None:
                groups.setdefault((r["sport"], r["lg"], str(r["game_key"]), r["nbt"], r["cond"]), []).append(r)
        picked = [sorted(g, key=lambda x: (_line_of(x), x["id"]))[len(g) // 2] for g in groups.values()]
        return [p for p in picked if ep._imp(p["odds"]) is not None], set(groups)

    cells = {}
    for r in rows:
        cells.setdefault((r["sport"], r["lg"], r["nbt"], r["cond"]), []).append(r)
    card = {}
    for cr in _card_rows(today):
        lg, nbt, since = _segment(ep, cr["sport"], cr["league"], cr["game_key"], cr["bet_type"], fcs, fcs_lg)
        if (cr["sport"], lg) not in GAME_LEAGUES:
            continue
        cond = ep._norm_cond(ep._condition(cr["pick_text"], cr["bet_type"], cr["odds"]))
        cr["in_win"] = (not since) or str(cr["date"] or "") >= since
        card.setdefault((cr["sport"], lg, nbt, cond), []).append(cr)
        cells.setdefault((cr["sport"], lg, nbt, cond), [])
    out = []
    for key, rs in cells.items():
        sp, lg, nbt, cond = key
        chip, label = GAME_LEAGUES[(sp, lg)]
        win = [r for r in rs if r["in_win"]]
        leg = [r for r in rs if not r["in_win"]]
        tw, wl_keys = trials_of(win)
        tl, _ = trials_of(leg)
        p_keys = {(r["sport"], r["lg"], str(r["game_key"]), r["nbt"], r["cond"]) for r in win if r["result"] == "P"}
        pend_keys = {(r["sport"], r["lg"], str(r["game_key"]), r["nbt"], r["cond"]) for r in win
                     if r["result"] in (None, "")}
        pushes = len(p_keys - wl_keys)
        pending = len(pend_keys - wl_keys - p_keys)
        T = [_trial(t["result"], t["odds"], t["pick_date"], t["prob"], t["closing_odds"], t["stake_units"],
                    {"game_key": t["game_key"], "matchup": t["matchup"], "pick_text": t["pick_text"],
                     "bet_type": t["bet_type"]}) for t in tw]
        rec = _agg(T, pushes, pending)
        d7, d30 = _windows(T, today)
        legacy = _short(_agg([_trial(t["result"], t["odds"], t["pick_date"]) for t in tl]))
        since = next((r["since"] for r in rs if r.get("since")), None)
        if since is None:
            since = ep._EVIDENCE_SINCE.get((sp, lg, nbt)) or ep._EVIDENCE_SINCE.get((sp, lg, ep._family(nbt)))
        gcell = gmap.get(key)
        fcell = fresh.get(key)
        elig = bool(ep.is_eligible(sp, lg, nbt, cond))
        flags = []
        # self-check against the gate's own computation on the same data
        if fcell or rec["n"]:
            gn = (fcell or {}).get("n", 0)
            groi = (fcell or {}).get("roi")
            if gn != rec["n"] or (groi is not None and rec["roi"] is not None and abs(groi - rec["roi"]) > 0.11):
                flags.append({"kind": "selfcheck", "text": f"record differs from edge_proven._compute (gate n={gn}, "
                              f"ROI {groi if groi is None else round(groi, 1)}%)"})
        if bool((fcell or {}).get("eligible")) != elig:
            flags.append({"kind": "cache_lag", "text": (
                f"the gate's cached map (refreshes every {ep.TTL // 60} min) says "
                f"{'eligible' if elig else 'not eligible'}; on the current data it is "
                f"{'eligible' if (fcell or {}).get('eligible') else 'not eligible'} -- flips at its next refresh")})
        reason = _gate_reason(ep, gcell, key)
        lane = "proven" if elig else "shadow"
        if LIVE_ONLY.match(nbt):
            if elig:
                reason = ("gate cell is eligible, but NHL live picks never reach a card: engine/live returns "
                          "them graduated=False (display + shadow evidence only). " + reason)
            lane = "shadow"
        elif not elig:
            reason += " -- picks show on cards only as an amber 'lean, not graduated'"
        if chip in OFF_SPORTS:
            lane = "paused"
            reason = OFF_SPORTS[chip] + ". Gate: " + _gate_reason(ep, gcell, key)
        # NBA: re-grade the gate's own trials from box scores
        box_rec = None
        if sp == "nba" and box is not None:
            bt_trials, dis = [], 0
            for t in tw:
                gid = str(t["game_key"]).rsplit(":", 1)[-1]
                g = box.grade(gid, t["matchup"], t["bet_type"], t["pick_text"])
                if g is None:
                    continue
                bres = g[0]
                if bres != t["result"]:
                    dis += 1
                if bres in ("W", "L"):
                    bt_trials.append(_trial(bres, t["odds"], t["pick_date"]))
            if dis:
                box_rec = _short(_agg(bt_trials))
                box_rec["disagree"] = dis
                flags.append({"kind": "misgraded", "text": f"{dis} of {rec['n']} graded trials disagree with the "
                              f"box score -- box re-grade {box_rec['w']}-{box_rec['l']}"
                              f" ({box_rec['roi']:+.1f}% ROI)" if box_rec.get("roi") is not None else
                              f"{dis} graded trials disagree with the box score"})
        cr = card.get(key) or []
        crw = [x for x in cr if x["in_win"]]
        card_rec = None
        if crw:
            card_rec = _short(_agg([_trial(x["result"], x["odds"], x["date"], x["prob"], x["close"], x["stake"])
                                    for x in crw if x["result"] in ("W", "L")],
                                   pushes=sum(1 for x in crw if x["result"] == "P"),
                                   pending=sum(1 for x in crw if x["result"] in (None, ""))))
        last_pick = max([str(r["pick_date"] or "") for r in rs] + [str(x["date"] or "") for x in cr] or [""])
        window_flag = None if since else "no evidence window in edge_proven for this cell -- the gate counts all-time"
        out.append({
            "id": f"game|{sp}|{lg}|{nbt}|{cond}", "group": "game", "sport": chip, "sport_label": label,
            "src_sport": sp, "league": lg, "market": nbt, "market_label": _market_label(nbt),
            "side": cond, "side_label": _SIDE_LABEL.get(cond, cond.title()),
            "lane": lane, "reason": reason, "since": since, "window_flag": window_flag,
            "model_note": MODEL_NOTES.get(chip), "rec": rec, "d7": d7, "d30": d30, "legacy": legacy,
            "card": card_rec, "box": box_rec, "flags": flags, "last_pick": last_pick or None,
            "gate": ({"kind": "edge_proven", "n": gcell["n"], "win": round(gcell["win"], 1),
                      "roi": round(gcell["roi"], 1), "conf": round(gcell["conf"], 3), "eligible": bool(gcell["eligible"]),
                      "floor": ep._CONF_FLOORS.get(key, ep.MIN_CONF)} if gcell else
                     {"kind": "edge_proven", "n": 0, "eligible": False}),
        })
    return out


# --------------------------------------------------------------------------- props
def _prop_rows_db(path):
    if not _exists(path):
        return []
    c = _ro(path)
    try:
        return [dict(r) for r in c.execute("SELECT * FROM player_props_picks ORDER BY id")]
    finally:
        c.close()


def _side_of(p):
    return (p.get("side") or str(p.get("pick") or "").split(" ")[0]).title()


def _dedupe_props(rows):
    seen, out = set(), []
    for r in rows:
        k = (str(r.get("game_id")), r.get("player_id"), r.get("bet_type"), _side_of(r))
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def _prop_trials(rows):
    return [_trial(r["result"], r["odds"], r["date"], r.get("model_prob"), r.get("closing_odds"), r.get("stake_units"))
            for r in rows if r.get("result") in ("W", "L")]


def _prop_row(chip, label, bt, side, lane, reason, since, rows_win, rows_leg, today, gate, card_rows=None,
              flags=None, window_flag=None, trials=None, potd=None):
    T = trials if trials is not None else _prop_trials(rows_win)
    pushes = sum(1 for r in rows_win if r.get("result") == "P") if trials is None else 0
    pending = sum(1 for r in rows_win if r.get("result") in (None, "")) if trials is None else 0
    rec = _agg(T, pushes, pending)
    d7, d30 = _windows(T, today)
    card_rec = None
    if card_rows is not None:
        card_rec = _short(_agg(_prop_trials(card_rows), pushes=sum(1 for r in card_rows if r.get("result") == "P"),
                               pending=sum(1 for r in card_rows if r.get("result") in (None, ""))))
    dates = [str(r.get("date") or "") for r in (rows_win + rows_leg + (card_rows or []))] + [t["date"] for t in T]
    return {"id": f"props|{chip}|{bt}|{side}", "group": "props", "sport": chip, "sport_label": label + " props",
            "src_sport": chip.lower(), "league": chip.lower(), "market": bt, "market_label": bt,
            "side": side.lower(), "side_label": side, "lane": lane, "reason": reason, "since": since,
            "window_flag": window_flag, "model_note": None, "rec": rec, "d7": d7, "d30": d30,
            "legacy": _short(_agg(_prop_trials(_dedupe_props(rows_leg)))), "card": card_rec, "box": None,
            "flags": flags or [], "last_pick": max(dates) if dates else None, "gate": gate, "potd": potd}


def _potd_cells(sport):
    try:
        from engine.player_props_potd import cell_records
        return cell_records(sport)
    except Exception as e:
        logger.warning("model_health: props POTD cells (%s) failed: %s", sport, e)
        return {}


def _potd_badge(recs, key):
    r = recs.get(key)
    if not r:
        return None
    return {"proven": bool(r.get("proven")), "n": r.get("n"), "win_pct": r.get("win_pct"),
            "be_pct": r.get("be_pct"), "margin_pp": r.get("margin_pp"), "se_pp": r.get("se_pp")}


def _nhl_props(today):
    from engine.sports.nhl import card_gate as cg
    recs = cg.cell_records()
    potd = _potd_cells("nhl")
    real = _prop_rows_db(NHL_DB)
    c = _ro(NHL_DB)
    try:
        has = c.execute("SELECT 1 FROM sqlite_master WHERE name='prop_card_shadow'").fetchone()
        shadow = [dict(r) for r in c.execute("SELECT * FROM prop_card_shadow ORDER BY id")] if has else []
    finally:
        c.close()
    since = cg.SINCE
    keys = set()
    for r in real + shadow:
        if r.get("side"):
            keys.add((r["bet_type"], str(r["side"]).title()))
    out = []
    for bt, side in sorted(keys):
        rw = [r for r in real if r["bet_type"] == bt and str(r.get("side") or "").title() == side]
        sw = [r for r in shadow if r["bet_type"] == bt and str(r.get("side") or "").title() == side]
        since = cg.since_for(bt)                           # SOG counts v2 picks only (card_gate.SINCE_BY_BT)
        win = [r for r in rw if str(r["date"]) >= since] + [r for r in sw if str(r["date"]) >= since]
        leg = [r for r in rw if str(r["date"]) < since]
        g = recs.get((bt, side))
        proven = bool(cg.is_proven(bt, side, recs))
        trial = (bt, side) in cg.TRIAL
        flags = []
        trials = [t for t in _prop_trials(win) if t["odds"] is not None]
        rec_n = len([t for t in trials if _dec(t["odds"])])
        if g and (g["n"] != rec_n):
            flags.append({"kind": "selfcheck", "text": f"record differs from card_gate.cell_records (n={g['n']})"})
        if proven:
            reason = f"card gate open: n {g['n']} >= {cg.MIN_N} and ROI {g['roi_pct']:+.1f}% > {cg.SE_MULT:g} SE ({g['se_pct']})"
            lane = "proven"
        elif trial:
            reason = (f"on the card ON TRIAL, not proven (Austin 11-01: 'I want to bet them ... trial by fire'); "
                      f"proven needs n >= {cg.MIN_N} graded picks since {since} and ROI > {cg.SE_MULT:g} SE; now "
                      + (f"n {g['n']}, ROI {g['roi_pct']:+.1f}%" + (f" +/- {g['se_pct']}" if g.get("se_pct") is not None else "")
                         if g else "no graded picks yet"))
            lane = "live_unproven"
        else:
            if g:
                reason = (f"card gate closed (NHL props, 11-01): needs n >= {cg.MIN_N} graded rule picks since {since} "
                          f"(real + shadow) and ROI > {cg.SE_MULT:g} SE; now n {g['n']}, ROI {g['roi_pct']:+.1f}%"
                          + (f" +/- {g['se_pct']}" if g.get("se_pct") is not None else "")
                          + " -- new picks go to prop_card_shadow, not the card")
            else:
                reason = (f"card gate closed: no graded picks since {since} (needs n >= {cg.MIN_N} and ROI > "
                          f"{cg.SE_MULT:g} SE)")
            lane = "paused"
        out.append(_prop_row("NHL", "NHL", bt, side, lane, reason, since, win, leg, today,
                             gate={"kind": "nhl_card_gate", "n": (g or {}).get("n", 0), "real": (g or {}).get("real", 0),
                                   "shadow": (g or {}).get("shadow", 0), "roi": (g or {}).get("roi_pct"),
                                   "se": (g or {}).get("se_pct"), "proven": proven, "trial": trial, "min_n": cg.MIN_N},
                             card_rows=[r for r in rw if str(r["date"]) >= since], flags=flags,
                             potd=_potd_badge(potd, (bt, side))))
    return out


def _nfl_props(today):
    from engine.sports.nfl import prop_picks as npp
    from engine.player_props_potd import MODEL_SINCE
    gate = npp.card_gate()
    gcells = gate.get("cells") or {}
    proven = set(gate.get("proven") or [])
    since = MODEL_SINCE.get("nfl", "")
    # Re-run shadow_cells row by row (same SELECT, band, _score and side rule) to get dated trials.
    trials, eval_min, n_graded = {}, None, 0
    c = _ro(NFL_PROPS_DB)
    try:
        eval_min = c.execute("SELECT MIN(game_date) FROM prop_eval_log").fetchone()[0]
        rows = c.execute(
            "SELECT game_date, bet_type, actual, last_line, last_over, last_under, "
            "CASE WHEN pick_seen IS NOT NULL THEN pick_line ELSE first_line END AS l, "
            "CASE WHEN pick_seen IS NOT NULL THEN pick_over ELSE first_over END AS oo, "
            "CASE WHEN pick_seen IS NOT NULL THEN pick_under ELSE first_under END AS uo, "
            "CASE WHEN pick_seen IS NOT NULL THEN pick_p_over ELSE first_p_over END AS q "
            "FROM prop_eval_log WHERE actual IS NOT NULL").fetchall()
    finally:
        c.close()
    n_graded = len(rows)
    for r in rows:
        if None in (r["l"], r["oo"], r["uo"], r["q"]) or r["actual"] == r["l"]:
            continue
        q = float(r["q"])
        if not (npp._RAW_BAND[0] <= q <= npp._RAW_BAND[1]):
            continue
        sc = npp._score({"over_odds": r["oo"], "under_odds": r["uo"], "line": r["l"], "bet_type": r["bet_type"]}, q, 1.0 - q)
        if not sc:
            continue
        won = (r["actual"] > r["l"]) if sc["side"] == "Over" else (r["actual"] < r["l"])
        close = None
        if r["last_line"] is not None and float(r["last_line"]) == float(r["l"]):
            close = r["last_over"] if sc["side"] == "Over" else r["last_under"]
        trials.setdefault((r["bet_type"], sc["side"]), []).append(
            _trial("W" if won else "L", sc["odds"], r["game_date"], sc["model_prob"], close))
    real = _prop_rows_db(NFL_PROPS_DB)
    potd = _potd_cells("nfl")
    keys = set(trials) | {(r["bet_type"], _side_of(r)) for r in real if str(r["date"]) >= since}
    out = []
    for bt, side in sorted(keys):
        name = f"{bt} {side}"
        g = gcells.get(name)
        T = trials.get((bt, side), [])
        flags = []
        if (g or T) and ((g or {}).get("n", 0) != len(T) or (g or {}).get("wins", 0) != sum(1 for t in T if t["res"] == "W")):
            age = time.time() - float((getattr(npp, "_CELL_CACHE", {}) or {}).get("ts") or 0)
            if age > 60:   # shadow_cells is cached 30 min; newer gradings land in the replay first
                flags.append({"kind": "cache_lag", "text": f"shadow_cells cache ({int(age // 60)} min old) has n "
                              f"{(g or {}).get('n', 0)} / {(g or {}).get('wins', 0)} W; the current eval log has "
                              f"{len(T)} / {sum(1 for t in T if t['res'] == 'W')} W"})
            else:
                flags.append({"kind": "selfcheck", "text": f"replay differs from shadow_cells ({(g or {}).get('n', 0)} / "
                              f"{(g or {}).get('wins', 0)} W)"})
        rr = [r for r in real if r["bet_type"] == bt and _side_of(r) == side]
        cw = [r for r in rr if str(r["date"]) >= since]
        leg = [r for r in rr if str(r["date"]) < since]
        if name in proven:
            lane, reason = "proven", f"card gate open: shadow record n {g['n']}, margin {g['margin_pp']:+.1f}pp > {npp._CELL_MIN_SE} SE"
        else:
            lane = "paused"
            reason = (f"card gate closed (NFL props, 11-01): the rule's shadow record on graded HR main lines needs n >= "
                      f"{npp._CELL_MIN_N} and win% - break-even > {npp._CELL_MIN_SE} SE"
                      + (f"; now n {g['n']}, {g['win_pct']}% vs {g['be_pct']}% (margin {g['margin_pp']:+.1f}pp, SE {g['se_pp']})"
                         if g else (f"; the rule picks NONE of the {n_graded} graded eval-log lines so far "
                                    f"({sum(len(v) for v in trials.values())} qualify in any cell: the anchored edge rarely "
                                    "clears +4% at pick-time prices)"))
                      + " -- no card picks until a cell proves out")
        out.append(_prop_row("NFL", "NFL", bt, side, lane, reason, eval_min or since, [], leg, today,
                             gate={"kind": "nfl_card_gate", "n": (g or {}).get("n", 0), "wins": (g or {}).get("wins"),
                                   "margin": (g or {}).get("margin_pp"), "se": (g or {}).get("se_pp"),
                                   "proven": name in proven, "min_n": npp._CELL_MIN_N},
                             card_rows=cw, flags=flags, trials=T, potd=_potd_badge(potd, (bt, side)),
                             window_flag="shadow record = every graded line in prop_eval_log (v2 eval log, starts "
                                         f"{eval_min}); card record since {since}"))
    return out


def _simple_props(chip, path, today, since, lane_fn):
    rows = _prop_rows_db(path)
    potd = _potd_cells(chip.lower())
    keys = {(r["bet_type"], _side_of(r)) for r in rows}
    out = []
    for bt, side in sorted(keys):
        rr = [r for r in rows if r["bet_type"] == bt and _side_of(r) == side]
        win = [r for r in rr if (not since) or str(r["date"]) >= since]
        leg = [r for r in rr if since and str(r["date"]) < since]
        pb = _potd_badge(potd, (bt, side))
        lane, reason = lane_fn(pb, win)
        flags = []
        pr = potd.get((bt, side))
        if pr and since:
            mine = _agg(_prop_trials(win))
            if pr.get("n") != mine["n"] or (pr.get("roi") is not None and mine["roi"] is not None
                                            and abs(pr["roi"] - mine["roi"]) > 0.11):
                flags.append({"kind": "selfcheck", "text": f"record differs from player_props_potd.cell_records "
                              f"(n={pr.get('n')}, ROI {pr.get('roi')}%)"})
        out.append(_prop_row(chip, chip, bt, side, lane, reason, since or None, win, leg, today,
                             gate={"kind": "props_potd", **(pb or {"proven": False, "n": 0})},
                             card_rows=win, flags=flags, potd=pb,
                             window_flag=None if since else "no model window in code (player_props_potd.MODEL_SINCE "
                                                            "has none) -- all-time shown"))
    return out


def _prop_rows(today, diag):
    from engine.player_props_potd import MODEL_SINCE, PROVEN_MIN_N, PROVEN_SE_MULT
    out = []
    for fn, name in ((_nhl_props, "nhl"), (_nfl_props, "nfl")):
        try:
            out += fn(today)
        except Exception as e:
            logger.exception("model_health: %s props failed", name)
            diag.setdefault("errors", []).append(f"{name} props: {e}")

    def _nba_lane(pb, win):
        if pb and pb.get("proven"):
            return "proven", (f"props POTD proven-cell bar met: {pb['n']} graded, {pb['win_pct']}% vs "
                              f"{pb['be_pct']}% break-even (margin {pb['margin_pp']:+.1f}pp > {PROVEN_SE_MULT:g} SE)")
        base = ("no card gate for NBA props: every rule pick goes on the real card. Props-POTD bar "
                f"(n >= {PROVEN_MIN_N}, win% - break-even > {PROVEN_SE_MULT:g} SE) ")
        if pb and pb.get("n"):
            return "live_unproven", base + f"not met: {pb['n']} graded, {pb['win_pct']}% vs {pb['be_pct']}%"
        return "live_unproven", base + "not met: no graded picks yet"

    def _off_lane(chip):
        def f(pb, win):
            return "paused", OFF_SPORTS.get(chip, "off-season")
        return f

    def _wnba_lane(pb, win):
        last = max((str(r["date"]) for r in win), default="")
        return "live_unproven", ("no card gate for WNBA props (every rule pick goes on the card); "
                                 f"last pick {last or 'none'}")
    for chip, path, since, lane_fn in (("NBA", NBA_DB, MODEL_SINCE.get("nba", ""), _nba_lane),
                                       ("WNBA", _bb_db("wnba"), "", _wnba_lane),
                                       ("MLB", MLB_DB, "", _off_lane("MLB"))):
        try:
            out += _simple_props(chip, path, today, since, lane_fn)
        except Exception as e:
            logger.exception("model_health: %s props failed", chip)
            diag.setdefault("errors", []).append(f"{chip} props: {e}")
    return out


# --------------------------------------------------------------------------- integrity checks
def _start_index(today):
    """{(source_key, native_id): (start_utc|None, status, date)} from each sport's games table (last 150 days;
    older pending rows fall back to the date rule, which flags them anyway)."""
    idx = {}
    d0 = _days_ago(today, 150)

    def load(key, path, sql):
        if not _exists(path):
            return
        c = _ro(path)
        try:
            for r in c.execute(sql):
                idx[(key, str(r[0]))] = (_parse_ts(r[1]), r[2], r[3])
        except Exception as e:
            logger.debug("model_health: start index %s failed: %s", key, e)
        finally:
            c.close()
    q = " WHERE date >= '%s'" % d0
    load("nba", NBA_DB, "SELECT game_id, start_time, status, date FROM nba_games" + q)
    load("nhl_api", NHL_DB, "SELECT game_id, start_time, status, date FROM nhl_games" + q)
    load("mlb", MLB_DB, "SELECT mlb_game_id, start_time, status, date FROM games" + q)
    for lg in ("nfl", "cfb"):
        load("football:" + lg, _fb_db(lg), "SELECT game_id, start_time, status, date FROM games" + q)
    for lg in ("ncaam", "ncaaw", "china_cba", "australia_nbl", "wnba"):
        load("basketball:" + lg, _bb_db(lg), "SELECT game_id, start_time, status, date FROM games" + q)
    load("soccer:eng_premier", EPL_DB, "SELECT id, start_time, status, date FROM matches" + q)
    return idx


def _stale_check(now, today, idx, row, src_key, gid, date):
    """(is_stale, hours_since_end|None, start, status). Rule: game start + 4h is 'ended'; stale once 24h past
    that. No start time on record -> the game DATE (ET) must be 2+ days ago."""
    st = idx.get((src_key, str(gid)))
    if st and st[0]:
        ended = st[0] + timedelta(hours=GAME_LEN_H)
        h = (now - ended).total_seconds() / 3600.0
        return h > STALE_AFTER_H, round(h, 1), st[0], st[1]
    d = str((st[2] if st and st[2] else date) or "")
    if d and d <= _days_ago(today, 2):
        return True, None, None, (st[1] if st else None)
    return False, None, None, (st[1] if st else None)


def _integrity(today, now, game_rows, box, diag):
    checks = []
    tm = diag.setdefault("timing_ms", {})
    _t = time.time()
    idx = _start_index(today)
    tm["i_start_index"] = int((time.time() - _t) * 1000)
    _t = time.time()
    # ---------------------------------------------------------------- 1. ungraded > 24h after the game
    stale, test_rows = [], []

    def consider(source, sport, src_key, gid, date, matchup, pick, extra=""):
        if str(gid).startswith("zz") or str(matchup or "").startswith("AAA @"):
            test_rows.append({"source": source, "sport": sport, "date": date, "matchup": matchup, "pick": pick,
                              "game_id": gid, "note": "test/stub row (pending, never settles)"})
            return
        s, h, st, status = _stale_check(now, today, idx, None, src_key, gid, date)
        if s:
            stale.append({"source": source, "sport": sport, "date": date, "matchup": matchup, "pick": pick,
                          "game_id": str(gid), "start": _et_str(st) if st else None, "status": status,
                          "hours_past_end": h, "note": extra})
    c = _ro(PU_DB)
    try:
        for r in c.execute("SELECT sport, league, game_key, pick_date, matchup, bet_type, pick_text, is_shadow FROM picks "
                           "WHERE result IS NULL OR result = ''"):
            sp, lg = r["sport"], str(r["league"] or "").lower()
            if (sp, lg) not in GAME_LEAGUES:
                continue
            gid = str(r["game_key"]).rsplit(":", 1)[-1]
            key = {"nba": "nba", "nhl": "nhl_espn", "mlb": "mlb_espn"}.get(sp, f"{sp}:{lg}")
            consider("picks_unified" + (" (shadow)" if r["is_shadow"] else " (real)"), GAME_LEAGUES[(sp, lg)][0],
                     key, gid, r["pick_date"], r["matchup"], f"{_market_label(r['bet_type'])}: {r['pick_text']}")
    finally:
        c.close()
    tables = [(NBA_DB, "nba_paper_picks", "NBA", "nba", "game_id", "pick"),
              (NBA_DB, "nba_derivative_picks", "NBA", "nba", "game_id", "pick"),
              (NHL_DB, "nhl_picks", "NHL", "nhl_espn", "game_id", "pick"),
              (NHL_DB, "nhl_derivative_picks", "NHL", "nhl_espn", "game_id", "pick"),
              (NHL_DB, "nhl_p1_paper_picks", "NHL", "nhl_espn", "game_id", "pick"),
              (_fb_db("nfl"), "picks", "NFL", "football:nfl", "game_id", "pick"),
              (_fb_db("cfb"), "picks", "CFB", "football:cfb", "game_id", "pick"),
              (EPL_DB, "picks", "EPL", "soccer:eng_premier", "match_id", "pick")]
    for lg in ("ncaam", "ncaaw", "china_cba", "australia_nbl", "wnba"):
        tables.append((_bb_db(lg), "picks", GAME_LEAGUES[("basketball", lg)][0], "basketball:" + lg, "game_id", "pick"))
    for path, tbl, chip, key, gcol, pcol in tables:
        if not _exists(path):
            continue
        c = _ro(path)
        try:
            for r in c.execute(f"SELECT {gcol} AS gid, date, matchup, bet_type, {pcol} AS pick FROM {tbl} "
                               f"WHERE result IS NULL OR result = ''"):
                consider(f"{os.path.basename(path)}:{tbl}", chip, key, r["gid"], r["date"], r["matchup"],
                         f"{_market_label(r['bet_type'])}: {r['pick']}")
        except Exception as e:
            logger.debug("model_health: pending scan %s.%s failed: %s", path, tbl, e)
        finally:
            c.close()
    for chip, path, key in (("NHL", NHL_DB, "nhl_api"), ("NBA", NBA_DB, "nba"), ("NFL", NFL_PROPS_DB, "football:nfl"),
                            ("MLB", MLB_DB, "mlb"), ("WNBA", _bb_db("wnba"), "basketball:wnba")):
        for r in _prop_rows_db(path):
            if r.get("result") in (None, ""):
                consider(f"{os.path.basename(path)}:player_props_picks", chip + " props", key, r["game_id"], r["date"],
                         r["matchup"], f"{r['player_name']} {r['bet_type']} {r['pick']}")
    if _exists(NHL_DB):
        c = _ro(NHL_DB)
        try:
            if c.execute("SELECT 1 FROM sqlite_master WHERE name='prop_card_shadow'").fetchone():
                for r in c.execute("SELECT * FROM prop_card_shadow WHERE result IS NULL"):
                    consider("nhl.db:prop_card_shadow", "NHL props", "nhl_api", r["game_id"], r["date"], r["matchup"],
                             f"{r['player_name']} {r['bet_type']} {r['side']} {r['line']}")
        finally:
            c.close()
    stale.sort(key=lambda x: (x["date"] or ""), reverse=True)
    recent = [x for x in stale if (x["date"] or "") >= _days_ago(today, 30)]
    by_src = {}
    for x in stale:
        by_src[f"{x['source']} · {x['sport']}"] = by_src.get(f"{x['source']} · {x['sport']}", 0) + 1
    tm["i_stale"] = int((time.time() - _t) * 1000)
    _t = time.time()
    checks.append({"key": "stale_pending", "label": "Ungraded > 24h after the game", "ok": not recent,
                   "count": len(recent), "count_all": len(stale),
                   "rule": "still pending although the game started more than 28h ago (start + 4h = ended, then 24h). "
                           "No start time on record -> the game date is 2+ days ago. Counted: last 30 days; "
                           "older ones listed too.",
                   "by_source": dict(sorted(by_src.items(), key=lambda kv: -kv[1])), "items": stale[:400]})
    # ---------------------------------------------------------------- 2. stake missing where a stake is expected
    from engine.player_props_potd import MODEL_SINCE
    miss = []
    for chip, path, sp in (("NHL", NHL_DB, "nhl"), ("NBA", NBA_DB, "nba"), ("NFL", NFL_PROPS_DB, "nfl")):
        since = MODEL_SINCE.get(sp, "")
        for r in _prop_rows_db(path):
            if str(r["date"]) >= since and r.get("result") in ("W", "L", "P") and not r.get("stake_units"):
                miss.append({"source": f"{os.path.basename(path)}:player_props_picks", "sport": chip + " props",
                             "date": r["date"], "matchup": r["matchup"],
                             "pick": f"{r['player_name']} {r['bet_type']} {r['pick']} @ {r['odds']}",
                             "result": r["result"], "stake_units": r.get("stake_units"),
                             "note": ("tracker multiplies profit by stake_units -> counted as $0" if r.get("stake_units") == 0
                                      else "stake_units NULL -> tracker assumes 1u")})
        if _exists(path):
            c = _ro(path)
            try:
                for r in c.execute("SELECT d.date, d.stake_units, p.player_name, p.bet_type, p.pick, p.result, p.matchup "
                                   "FROM player_props_picks_pot_day d JOIN player_props_picks p ON p.id = d.pick_id "
                                   "WHERE d.date >= '2026-11-01'"):
                    if not r["stake_units"]:
                        miss.append({"source": f"{os.path.basename(path)}:player_props_picks_pot_day", "sport": chip + " props POTD",
                                     "date": r["date"], "matchup": r["matchup"], "pick": f"{r['player_name']} {r['bet_type']} {r['pick']}",
                                     "result": r["result"], "stake_units": r["stake_units"],
                                     "note": "POTD lock rows carry the card's stake chip since 11-01"})
            except Exception as e:
                logger.debug("model_health: potd stake scan %s failed: %s", path, e)
            finally:
                c.close()
    if _exists(NHL_DB):
        c = _ro(NHL_DB)
        try:
            for r in c.execute("SELECT date, matchup, bet_type, pick, result, stake_units FROM nhl_picks WHERE "
                               "COALESCE(is_shadow,0)=1 AND date >= '2026-10-26' AND result IN ('W','L','P') "
                               "AND (stake_units IS NULL OR stake_units = 0)"):
                miss.append({"source": "nhl.db:nhl_picks", "sport": "NHL", "date": r["date"], "matchup": r["matchup"],
                             "pick": f"{r['bet_type']}: {r['pick']}", "result": r["result"], "stake_units": r["stake_units"],
                             "note": "NHL tracker multiplies profit by stake_units"})
        finally:
            c.close()
    miss.sort(key=lambda x: x["date"] or "", reverse=True)
    checks.append({"key": "stake_missing", "label": "Settled with stake 0 / NULL", "ok": not miss, "count": len(miss),
                   "rule": "settled real-card prop picks since each sport's model start (the props tracker multiplies $ "
                           "by stake_units), props POTD locks since 11-01, and NHL card leans since 10-26 (the NHL "
                           "tracker multiplies by stake). Game-market trackers grade flat $100, so they are not checked.",
                   "items": miss})
    # ---------------------------------------------------------------- 3. card (lean lock) vs tracker
    mism, checked = [], 0
    try:
        lock = json.load(open(LEAN_LOCK, encoding="utf-8"))
    except Exception:
        lock = {}
    tracker = {}
    for cr in _card_rows(today):
        tracker[(cr["sport"] if cr["sport"] in ("nba", "nhl") else f"{cr['sport']}:{cr['league']}", str(cr["game_id"]))] = cr
    cutoff = time.time() - 4 * 86400

    def _norm(p):
        p = re.sub(r"\s+", " ", str(p or "").strip().lower())
        return re.sub(r"\s+ml$", "", p)
    for k, v in lock.items():
        if "|" not in k or not isinstance(v, dict) or (v.get("at") or 0) < cutoff:
            continue
        sp, gid = k.split("|", 1)
        lean = v.get("lean") or {}
        t = tracker.get((sp, str(gid)))
        if not t or not lean:
            continue
        checked += 1
        if _norm(lean.get("pick")) != _norm(t["pick_text"]):
            mism.append({"sport": sp, "game_id": gid, "matchup": t["matchup"] or lean.get("matchup"), "date": t["date"],
                         "card": f"{_market_label(lean.get('type'))}: {lean.get('pick')} @ {lean.get('odds')}"
                                 + (" (graduated)" if lean.get("graduated") else " (lean)"),
                         "tracker": f"{_market_label(t['bet_type'])}: {t['pick_text']} @ {t['odds']}",
                         "tracker_result": t["result"]})
    for x in mism:
        x["after_fix"] = str(x.get("date") or "") >= CARD_FIX_DATE
    mism.sort(key=lambda x: (not x["after_fix"], str(x.get("date") or "")), reverse=False)
    post = [x for x in mism if x["after_fix"]]
    checks.append({"key": "card_vs_tracker", "label": "Card ≠ tracker pick", "ok": not post, "count": len(post),
                   "count_all": len(mism), "checked": checked,
                   "rule": "games of the last 4 days where the card's held pick (data/card_lean_lock.json, what live/final "
                           "cards show) differs from the tracker's recorded authority pick (first pick wins). Counted from "
                           f"{CARD_FIX_DATE} (the card-holds-tracker / first-pick-wins fix); older games are listed too -- "
                           "their history was re-graded by the earliest-row rule on 11-01. Covers NBA, NHL, NFL, CFB, "
                           "college/CBA/NBL/WNBA basketball and EPL.",
                   "items": mism})
    tm["i_stake_card"] = int((time.time() - _t) * 1000)
    # ---------------------------------------------------------------- 4. odds staleness
    _t = time.time()
    checks.append(_odds_check(now, idx))
    tm["i_odds"] = int((time.time() - _t) * 1000)
    # ---------------------------------------------------------------- 5. no market anchor
    _t = time.time()
    checks.append(_anchor_check(today))
    tm["i_anchor"] = int((time.time() - _t) * 1000)
    # ---------------------------------------------------------------- 6. NBA box-score regrade
    _t = time.time()
    checks.append(_regrade_check(box, today))
    tm["i_regrade"] = int((time.time() - _t) * 1000)
    # ---------------------------------------------------------------- 7. test rows
    checks.append({"key": "test_rows", "label": "Test / stub rows", "ok": not test_rows, "count": len(test_rows),
                   "rule": "pending rows that look like fixtures (game id 'zz...', matchup 'AAA @ BBB'): they never settle.",
                   "items": test_rows})
    # ---------------------------------------------------------------- 8. our numbers vs the gates
    bad = [r for r in game_rows if any(f["kind"] == "selfcheck" for f in r["flags"])]
    wsc = diag.get("window_selfcheck") or {}
    checks.append({"key": "selfcheck", "label": "Page numbers = gate numbers", "ok": not bad and wsc.get("ok", True),
                   "count": len(bad) + (0 if wsc.get("ok", True) else 1),
                   "rule": "every row's n / win% / ROI recomputed here must equal the gate's own cell "
                           "(edge_proven._map, card_gate.cell_records, prop_picks.shadow_cells, "
                           "player_props_potd.cell_records), and the evidence-window filter must keep exactly the rows "
                           f"edge_proven keeps (ours {wsc.get('ours')} / gate {wsc.get('gate')}).",
                   "items": [{"sport": r["sport"], "pick": f"{r['market_label']} {r['side_label']}",
                              "note": "; ".join(f["text"] for f in r["flags"] if f["kind"] == "selfcheck")} for r in bad]})
    return checks


def _odds_check(now, idx):
    in24, in6 = now + timedelta(hours=24), now + timedelta(hours=IMMINENT_H)
    upcoming = {}
    for (src, gid), (st, status, _d) in idx.items():
        if st and now - timedelta(hours=3) <= st <= in24 and status not in ("final", "postponed", "canceled", "cancelled"):
            upcoming.setdefault(src, []).append(st)
    srcmap = {"nba": ("NBA", ("basketball", "nba"), "nba"), "nhl_api": ("NHL", ("hockey", "nhl"), "nhl"),
              "football:nfl": ("NFL", ("football", "nfl"), "football_nfl"),
              "football:cfb": ("CFB", ("football", "cfb"), "football_cfb"),
              "basketball:ncaam": ("NCAAM", ("basketball", "ncaam"), "ncaam"),
              "basketball:ncaaw": ("NCAAW", ("basketball", "ncaaw"), "ncaaw"),
              "basketball:china_cba": ("CBA", ("basketball", "china_cba"), "china_cba"),
              "basketball:australia_nbl": ("NBL", ("basketball", "australia_nbl"), "australia_nbl"),
              "basketball:wnba": ("WNBA", ("basketball", "wnba"), "wnba"),
              "soccer:eng_premier": ("EPL", ("soccer", "eng_premier"), "eng_premier")}
    cap, prov, nhl_odds = {}, {}, None
    if _exists(GAME_ODDS_DB):
        c = _ro(GAME_ODDS_DB)
        try:
            for r in c.execute("SELECT sport, league, MAX(last_seen) FROM game_odds GROUP BY 1,2"):
                cap[(r[0], r[1])] = _parse_ts(r[2])
        finally:
            c.close()
    if _exists(PROV_DB):
        c = _ro(PROV_DB)
        try:
            d0 = (now - timedelta(days=3)).strftime("%Y-%m-%d")
            for sp in [v[2] for v in srcmap.values()]:
                # index-only (sport, date) scan for the newest rowid, then one row read (rows carry big trace blobs)
                r = c.execute("SELECT MAX(rowid) FROM pick_provenance WHERE sport = ? AND date >= ?", (sp, d0)).fetchone()
                if r and r[0]:
                    t = c.execute("SELECT created_at FROM pick_provenance WHERE rowid = ?", (r[0],)).fetchone()
                    prov[sp] = _parse_ts(t[0]) if t and t[0] else None
        finally:
            c.close()
    if _exists(NHL_DB):
        c = _ro(NHL_DB)
        try:
            nhl_odds = _parse_ts(c.execute("SELECT MAX(captured_at) FROM nhl_odds").fetchone()[0])
        finally:
            c.close()
    inproc, breaker = {}, None
    try:
        import sys as _sys
        hr = _sys.modules.get("scrapers.hardrock_odds")
        if hr is not None:
            for sp in ("nba", "nhl"):
                v = getattr(hr, "_cache", {}).get(sp)
                if v:
                    inproc[sp] = datetime.fromtimestamp(v[0], tz=timezone.utc)
            b = float(getattr(hr, "_cf_blocked_until", 0.0) or 0.0)
            breaker = int(b - time.time()) if b > time.time() else 0
    except Exception:
        pass
    items, bad = [], 0
    for src, (chip, capkey, provkey) in srcmap.items():
        ups = upcoming.get(src) or []
        cands = [("CaptureAllOdds (game_odds.db)", cap.get(capkey)), ("last priced pick (pick_provenance)", prov.get(provkey))]
        if chip == "NHL":
            cands.append(("nhl_odds snapshot", nhl_odds))
        if chip.lower() in inproc:
            cands.append(("backend HR odds cache", inproc[chip.lower()]))
        cands = [(n, t) for n, t in cands if t]
        best = max(cands, key=lambda x: x[1]) if cands else (None, None)
        age = round((now - best[1]).total_seconds() / 60.0) if best[1] else None
        soon = [st for st in ups if st <= in6]
        stale = bool(soon) and (age is None or age > 120)
        bad += 1 if stale else 0
        items.append({"sport": chip, "games_next_24h": len(ups), "games_live_or_6h": len(soon),
                      "next_start": _et_str(min(ups)) if ups else None, "freshest": _et_str(best[1]) if best[1] else None,
                      "age_min": age, "source": best[0], "stale": stale,
                      "sources": {n: _et_str(t) for n, t in cands}})
    items.sort(key=lambda x: (not x["stale"], -(x["games_next_24h"] or 0)))
    note = "" if not breaker else f" HR circuit-breaker OPEN ({breaker}s left) -- odds fetches are being skipped."
    return {"key": "odds_stale", "label": "Odds staleness", "ok": bad == 0 and not breaker, "count": bad + (1 if breaker else 0),
            "rule": f"for every sport with a game live or starting within {IMMINENT_H}h, the freshest odds observation "
                    "(hourly CaptureAllOdds, the last priced pick in pick_provenance, NHL odds snapshots, the backend's "
                    "in-memory HR cache) must be under 2h old. Games later in the next 24h are listed for context." + note,
            "breaker_s": breaker, "items": items}


def _anchor_check(today):
    """Props: the card pick's line must have BOTH sides priced in prop_eval_log at pick time (else no de-vig
    -> the raw model prob leaks through as edge, the 11-01 anytime-goal bug). Game markets: a card pick whose
    probability equals the shadow lane's raw (W=1) number for the same bet was never anchored."""
    from engine.player_props_potd import MODEL_SINCE
    items, checked, unverified = [], 0, 0
    d14 = _days_ago(today, 14)
    for chip, path, sp, pid in (("NHL", NHL_DB, "nhl", "player_id"), ("NBA", NBA_DB, "nba", "player_id"),
                                ("NFL", NFL_PROPS_DB, "nfl", "espn_id")):
        if not _exists(path):
            continue
        since = max(MODEL_SINCE.get(sp, ""), d14)
        c = _ro(path)
        try:
            cols = {r[1] for r in c.execute("PRAGMA table_info(prop_eval_log)")}
            snaps = [s for s in ("pick", "first", "last") if f"{s}_line" in cols]
            for r in c.execute("SELECT * FROM player_props_picks WHERE date >= ?", (since,)).fetchall():
                ev = c.execute(f"SELECT * FROM prop_eval_log WHERE game_id = ? AND {pid} = ? AND bet_type = ?",
                               (str(r["game_id"]), r["player_id"], r["bet_type"])).fetchone()
                if ev is None:
                    unverified += 1
                    continue
                checked += 1
                ok = False
                for s in snaps:
                    ln, o, u = ev[f"{s}_line"], ev[f"{s}_over"], ev[f"{s}_under"]
                    if ln is not None and r["line"] is not None and float(ln) == float(r["line"]) and o is not None and u is not None:
                        ok = True
                        break
                if not ok:
                    items.append({"sport": chip + " props", "date": r["date"], "matchup": r["matchup"],
                                  "pick": f"{r['player_name']} {r['bet_type']} {r['pick']} @ {r['odds']}", "result": r["result"],
                                  "note": "no two-sided price at this line in prop_eval_log -> no de-vig anchor"})
        finally:
            c.close()
    # game markets: card pick prob == raw shadow prob of the same bet
    twin = {}
    c = _ro(PU_DB)
    try:
        for r in c.execute("SELECT game_key, bet_type, pick_text, odds, prob FROM picks WHERE is_shadow = 1 AND pick_date >= ?",
                           (d14,)):
            twin.setdefault((r["game_key"], str(r["bet_type"]).upper().replace(" ", "_"),
                             re.sub(r"\s+", " ", str(r["pick_text"]).strip().lower()), r["odds"]), []).append(r["prob"])
    finally:
        c.close()
    g_checked = 0
    for cr in _card_rows(today):
        if str(cr["date"] or "") < d14 or cr["sport"] == "nhl":
            continue
        k = (cr["game_key"], str(cr["bet_type"]).upper().replace(" ", "_"),
             re.sub(r"\s+", " ", str(cr["pick_text"]).strip().lower()), cr["odds"])
        probs = [p for p in twin.get(k, []) if p]
        if not probs or not cr["prob"]:
            continue
        g_checked += 1
        if any(abs(float(p) - float(cr["prob"])) < 0.0005 for p in probs):
            items.append({"sport": cr["sport"] + (":" + cr["league"] if cr["sport"] not in ("nba",) else ""),
                          "date": cr["date"], "matchup": cr["matchup"],
                          "pick": f"{_market_label(cr['bet_type'])}: {cr['pick_text']} @ {cr['odds']}", "result": cr["result"],
                          "note": f"card prob {float(cr['prob']):.4f} = the shadow lane's raw model prob -> the market "
                                  "anchor did not move it (missing de-vig?)"})
    return {"key": "no_anchor", "label": "No market anchor / de-vig", "ok": not items, "count": len(items),
            "checked": checked + g_checked, "unverified": unverified,
            "rule": "props card picks (last 14 days): the picked line must have both Over and Under priced in "
                    "prop_eval_log at pick time, else no de-vig is possible. Game-market card picks: a probability equal "
                    "to the shadow lane's raw W=1 number for the same bet means the anchor never applied.",
            "items": items}


def _regrade_check(box, today):
    items, n_ok, n_bad = [], 0, 0
    if box is None:
        return {"key": "misgraded", "label": "NBA grades vs box score", "ok": True, "count": 0, "items": [],
                "rule": "nba.db unavailable"}
    from engine.selection import edge_proven as ep
    c = _ro(PU_DB)
    try:
        rows = c.execute("SELECT id, game_key, pick_date, matchup, bet_type, pick_text, odds, result, is_shadow FROM picks "
                         "WHERE sport='nba' AND result IN ('W','L','P') AND pick_date >= '2026-10-19'").fetchall()
    finally:
        c.close()
    for r in rows:
        g = box.grade(str(r["game_key"]).rsplit(":", 1)[-1], r["matchup"], r["bet_type"], r["pick_text"])
        if g is None:
            continue
        nbt = ep._norm_bt(r["bet_type"])
        since = ep._EVIDENCE_SINCE.get(("nba", "nba", nbt)) or ep._EVIDENCE_SINCE.get(("nba", "nba", ep._family(nbt)))
        if g[0] == r["result"]:
            n_ok += 1
            continue
        n_bad += 1
        items.append({"source": "picks_unified" + (" (shadow)" if r["is_shadow"] else " (real)"), "date": r["pick_date"],
                      "matchup": r["matchup"], "pick": f"{_market_label(r['bet_type'])}: {r['pick_text']} @ {r['odds']}",
                      "stored": r["result"], "box": g[0], "scope": g[1], "score": f"{g[2][1]}-{g[2][0]} (away-home)",
                      "in_gate_window": bool(not since or str(r["pick_date"]) >= since)})
    c = _ro(NBA_DB)
    try:
        crows = c.execute("SELECT id, game_id, date, matchup, bet_type, pick, odds, result FROM nba_paper_picks "
                          "WHERE result IN ('W','L','P') AND date >= '2026-10-19'").fetchall()
    finally:
        c.close()
    c_ok = c_bad = 0
    for r in crows:
        g = box.grade(r["game_id"], r["matchup"], r["bet_type"], r["pick"])
        if g is None:
            continue
        if g[0] == r["result"]:
            c_ok += 1
            continue
        c_bad += 1
        items.append({"source": "nba.db:nba_paper_picks (card)", "date": r["date"], "matchup": r["matchup"],
                      "pick": f"{_market_label(r['bet_type'])}: {r['pick']} @ {r['odds']}", "stored": r["result"],
                      "box": g[0], "scope": g[1], "score": f"{g[2][1]}-{g[2][0]} (away-home)", "in_gate_window": None})
    items.sort(key=lambda x: (x["date"] or ""), reverse=True)
    in_win = sum(1 for x in items if x.get("in_gate_window"))
    return {"key": "misgraded", "label": "NBA grades vs box score", "ok": n_bad == 0 and c_bad == 0,
            "count": n_bad + c_bad, "in_gate_window": in_win,
            "checked": n_ok + n_bad + c_ok + c_bad, "card_checked": c_ok + c_bad, "card_bad": c_bad,
            "rule": "every settled NBA pick since 10-19 re-graded from nba_games quarter/final scores (2H = final - 1H, "
                    "OT included). picks_unified is the Pick Authority's evidence; nba_paper_picks is the card/tracker. "
                    "A stored 'P' with a gradable box score is an auto-push (the settler gave up).",
            "items": items}


# --------------------------------------------------------------------------- build / cache / loop
def build_payload():
    t0 = time.time()
    _BUILD_ID["v"] += 1
    now = _now_utc()
    today = _today_et()
    diag = {}
    try:
        box = _NbaBox()
    except Exception as e:
        logger.warning("model_health: NBA box load failed: %s", e)
        box = None
    tm = diag.setdefault("timing_ms", {})
    tm["nba_box"] = int((time.time() - t0) * 1000)
    t1 = time.time()
    game_rows = _game_market_rows(today, box, diag)
    tm["game_markets"] = int((time.time() - t1) * 1000)
    time.sleep(0.05)
    t1 = time.time()
    prop_rows = _prop_rows(today, diag)
    tm["props"] = int((time.time() - t1) * 1000)
    rows = game_rows + prop_rows
    rank = {"proven": 0, "live_unproven": 1, "shadow": 2, "paused": 2}
    for r in rows:
        recent = r.get("last_pick") and r["last_pick"] >= _days_ago(today, 14)
        r["active"] = bool((r["rec"]["n"] or r["rec"]["pending"] or (r.get("card") or {}).get("n")
                            or (r.get("card") or {}).get("pending")) and recent and r["sport"] not in OFF_SPORTS)
    rows.sort(key=lambda r: (not r["active"], rank.get(r["lane"], 3), -(r["rec"]["n"] or 0), -(r["rec"]["pending"] or 0),
                             r["sport"], r["market"], r["side"]))
    time.sleep(0.05)
    t1 = time.time()
    integrity = _integrity(today, now, game_rows, box, diag)
    tm["integrity"] = int((time.time() - t1) * 1000)
    from engine.selection import edge_proven as ep
    try:
        from engine.sports.nhl import card_gate as cg
        nhl_rule = cg.RULE
    except Exception:
        nhl_rule = None
    try:
        from engine.sports.nfl import prop_picks as npp
        nfl_rule = npp.card_gate()["rule"]
    except Exception:
        nfl_rule = None
    from engine.player_props_potd import MODEL_SINCE, PROVEN_MIN_N, PROVEN_SE_MULT
    sports = [s for s in SPORT_ORDER if any(r["sport"] == s for r in rows)]
    payload = {
        "version": VERSION, "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "generated_et": _et_str(now),
        "today_et": today, "compute_ms": None, "sports": sports, "rows": rows, "integrity": integrity,
        "counts": {k: sum(1 for r in rows if r["lane"] == k and r["active"]) for k in ("proven", "live_unproven", "shadow", "paused")},
        "gates": {
            "edge_proven": {"rule": (f"graduated = ROI > +{ep.MIN_ROI:g}% AND Bayesian conf >= {ep.MIN_CONF} (per-cell floors "
                                     f"{len(ep._CONF_FLOORS)}) AND (n >= {ep.STD_N} OR (n >= {ep.FAST_N} AND conf >= "
                                     f"{ep.FAST_CONF})); n = one trial per game/market/side (median rung), since the "
                                     "cell's evidence window"),
                            "windows": {"/".join(k): v for k, v in sorted(ep._EVIDENCE_SINCE.items())}},
            "nhl_props": nhl_rule, "nfl_props": nfl_rule,
            "props_potd": f"proven = n >= {PROVEN_MIN_N} and win% - break-even > {PROVEN_SE_MULT:g} SE (since MODEL_SINCE {MODEL_SINCE})",
        },
        "sources": SOURCES, "notes": diag.get("errors") or [],
        "window_selfcheck": diag.get("window_selfcheck"), "timing_ms": diag.get("timing_ms"),
    }
    payload["compute_ms"] = int((time.time() - t0) * 1000)
    return payload


SOURCES = [
    {"sport": "Game markets (all sports)", "text": "data/picks_unified.db picks, real + shadow lanes = the Pick Authority "
     "gate's evidence: windowed by edge_proven._EVIDENCE_SINCE, CFB games with no FBS team re-keyed to cfb_fcs, one trial "
     "per game/market/side (the median rung of an alt ladder). Status = edge_proven.is_eligible()."},
    {"sport": "NBA", "text": "evidence window 10-26 (all markets; Q1 too). Card/tracker = nba.db nba_paper_picks, earliest "
     "non-void row per game (first pick wins). Props = nba.db player_props_picks since 10-27 (no card gate)."},
    {"sport": "NHL", "text": "evidence window 10-26. Card = nhl.db nhl_picks is_shadow=1 (none since 10-26). Live P1-P3 "
     "picks are shadow-only. Props = nhl.db player_props_picks + prop_card_shadow since 10-27 (card gate)."},
    {"sport": "NFL", "text": "no evidence window in code (all-time). Card = football/nfl.db picks is_shadow=0 (earliest per "
     "game). Props: shadow = football/nfl_player.db prop_eval_log replay; card = player_props_picks since 10-26."},
    {"sport": "CFB", "text": "windows: totals 10-20, sides 10-26 (FCS split out). Card = football/cfb.db picks is_shadow=0."},
    {"sport": "CBA / NBL", "text": "windows 10-29 / 10-30. Card = basketball/<league>.db picks is_shadow=1, first row per game."},
    {"sport": "NCAAM / NCAAW / WNBA", "text": "no evidence window in code (all-time). Card = basketball/<league>.db picks "
     "is_shadow=1 (WNBA: best Full / Q1 row by edge x prob)."},
    {"sport": "EPL", "text": "window 10-29. Card = soccer/eng_premier.db picks, earliest per match."},
    {"sport": "MLB", "text": "off since 10-29; team-total windows 10-27, everything else all-time."},
]

_lock = threading.Lock()
_state = {"payload": None, "bytes": None, "built_at": 0.0, "building": False, "last_error": None, "kick": False}


def _store(payload):
    raw = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    tmp = CACHE_PATH + ".tmp"
    try:
        with open(tmp, "wb") as fh:
            fh.write(raw)
        os.replace(tmp, CACHE_PATH)
    except Exception as e:
        logger.warning("model_health: cache write failed: %s", e)
    with _lock:
        _state.update(payload=payload, bytes=raw, built_at=time.time(), last_error=None)


def refresh():
    with _lock:
        if _state["building"]:
            return False
        _state["building"] = True
    try:
        _store(build_payload())
        return True
    except Exception as e:
        logger.exception("model_health: build failed")
        with _lock:
            _state["last_error"] = f"{type(e).__name__}: {e}"
        return False
    finally:
        with _lock:
            _state["building"] = False


def cached_bytes():
    """(json bytes | None, built_at epoch, building, last_error). Loads the on-disk cache once if memory is empty."""
    with _lock:
        if _state["bytes"] is None and os.path.exists(CACHE_PATH):
            try:
                with open(CACHE_PATH, "rb") as fh:
                    _state["bytes"] = fh.read()
                _state["built_at"] = os.path.getmtime(CACHE_PATH)
            except Exception as e:
                logger.warning("model_health: cache read failed: %s", e)
        return _state["bytes"], _state["built_at"], _state["building"], _state["last_error"]


def kick():
    """Ask the loop for an early rebuild (never builds on the caller's thread)."""
    with _lock:
        _state["kick"] = True


def _loop():
    time.sleep(INITIAL_DELAY_S)
    while True:
        refresh()
        slept = 0
        while slept < INTERVAL_S:
            time.sleep(5)
            slept += 5
            with _lock:
                if _state["kick"] and time.time() - _state["built_at"] > 60:
                    _state["kick"] = False
                    break


_started = {"v": False}


def start_background():
    if _started["v"]:
        return
    _started["v"] = True
    threading.Thread(target=_loop, name="model-health", daemon=True).start()
    logger.warning("[model-health] background builder started (first build in %ds, every %ds)", INITIAL_DELAY_S, INTERVAL_S)


if __name__ == "__main__":   # standalone: python -m engine.model_health
    import sys
    p = build_payload()
    print(json.dumps({"compute_ms": p["compute_ms"], "rows": len(p["rows"]), "counts": p["counts"],
                      "integrity": [(c["key"], c["ok"], c["count"]) for c in p["integrity"]],
                      "window_selfcheck": p["window_selfcheck"], "notes": p["notes"]}, indent=1))
    if "--write" in sys.argv:
        _store(p)
        print("wrote", CACHE_PATH)
