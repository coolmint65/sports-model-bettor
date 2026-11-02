"""HR spool (written by warehouse.hr_tee) -> wh.db quotes.

    .venv\\Scripts\\python.exe -m warehouse.hr_ingest            # one pass (the live loop calls run())

Per board file (one HR comp fetch): league from compName, each HR event matched to the ESPN event
(same league, start within 18 h, team names via aliases + fuzzy fallback; neutral-site swaps handled),
each market classified, quotes written ON CHANGE, a NULL-price row when a previously offered selection
disappears / is suspended / is hidden, and board_obs intervals updated.

HR encoding (verified on live boards 2026-11-01): market.type = SPORT:SCOPE:CODE; market.subtype =
"<period>[:A|B:<hrPlayerId>][#<line>]" (period M = full game, H1/H2/Q1-4/P1-3/G1...); selection.type A/AH =
HOME (participant 0), B/BH = away, X/Draw = tie, Over/Under, Yes/No; spread line in the subtype is the
HOME handicap; selection odds are decimal (rehydrated from rootIdx by scrapers.hardrock_odds).
Canonical families: ML, ML3, DNB, SPREAD, TOTAL, TT (team total), PROP (player O/U), PROPY (player
yes/no). Everything else is kept as family "X:<code>" (sides via the labels table) so idea-8 style
internal-consistency tests can still read it. In-play boards: only ML/ML3/DNB/SPREAD/TOTAL/TT.
Files are moved to spool/hr_done/<day>/ after ingest and pruned after KEEP_DAYS.
"""
from __future__ import annotations

import difflib
import glob
import gzip
import json
import logging
import os
import re
import shutil
import time

from . import db as D

log = logging.getLogger("wh.hr_ingest")

SPOOL = os.path.join(D.WH_DIR, "spool", "hr")
DONE = os.path.join(D.WH_DIR, "spool", "hr_done")
KEEP_DAYS = 3
LADDER = os.path.join(D.ROOT, "scrapers", "hardrock_odds", "hardrock-ladder.json")

LEAGUE_BY_COMP = {"nhl": "nhl", "nba": "nba", "nfl": "nfl", "ncaaf": "cfb", "ncaam": "ncaab", "wnba": "wnba",
                  "mlb": "mlb", "englandpremierleague": "epl"}
SPORT_KEY = {"ICE_HOCKEY": "hockey", "BASKETBALL": "basketball", "AMERICAN_FOOTBALL": "football",
             "BASEBALL": "baseball", "SOCCER": "soccer"}
PERIOD = {"M": "FG", "H1": "1H", "H2": "2H", "Q1": "Q1", "Q2": "Q2", "Q3": "Q3", "Q4": "Q4", "P1": "P1", "P2": "P2",
          "P3": "P3", "OT": "OT"}
SIDE = {"A": "home", "AH": "home", "B": "away", "BH": "away", "X": "draw", "Draw": "draw", "Tie": "draw",
        "Over": "over", "Under": "under", "Yes": "yes", "No": "no"}
MAIN_CODES = {
    "FTOT:ML": "ML", "FTEI:ML": "ML", "FT:ML": "ML", "P:ML": "ML", "P:DNB": "ML",
    "FT:AXB": "ML3", "P:AXB": "ML3", "FT:DNB": "DNB",
    "FTOT:SPRD": "SPREAD", "FTEI:SPRD": "SPREAD", "FT:SPRD": "SPREAD", "P:SPRD": "SPREAD", "FT:AHCP": "SPREAD",
    "FTOT:OU": "TOTAL", "FTEI:OU": "TOTAL", "FT:OU": "TOTAL", "P:OU": "TOTAL",
    "FTOT:A:OU": "TT:home", "FTOT:B:OU": "TT:away", "FT:A:OU": "TT:home", "FT:B:OU": "TT:away",
    "P:A:OU": "TT:home", "P:B:OU": "TT:away", "FTOT:A:OUEXSO": "TT:home", "FTOT:B:OUEXSO": "TT:away",
    # baseball first-5
    "FT:I1T5W": "ML@F5", "P:OU5": "TOTAL@F5", "P:A:OU5": "TT:home@F5", "P:B:OU5": "TT:away@F5",
}
INPLAY_FAMS = {"ML", "ML3", "DNB", "SPREAD", "TOTAL", "TT"}
PROP_STAT = {
    "basketball": {"PTS": "pts", "REB": "reb", "AST": "ast", "3PM": "fg3m", "3MADE": "fg3m", "BLK": "blk",
                   "STL": "stl", "PPA": "pa", "PPR": "pr", "PPAP": "pra", "APR": "ar", "TO": "tov",
                   "1QP": ("pts", "Q1"), "4QP": ("pts", "Q4"), "4QA": ("ast", "Q4"), "4QR": ("reb", "Q4")},
    "hockey": {"S": "sog", "SHOTS": "sog", "G": "g", "GOALS": "g", "A": "ast", "PTS": "pts", "PPP": "ppp",
               "SV": "saves", "SAVES": "saves", "BS": "blk", "BLK": "blk"},
    "football": {"PSYDS": "pass_yds", "RCYDS": "rec_yds", "RECYDS": "rec_yds", "REC": "receptions",
                 "RECEPTIONS": "receptions", "RSYDS": "rush_yds", "RUYDS": "rush_yds", "PSATT": "pass_att",
                 "PSCMP": "completions", "PSTD": "pass_td", "PSTDS": "pass_td", "INT": "pass_int", "RSATT": "carries",
                 "RSPRCYDS": "rr_yds", "PSPRSYDS": "pr_yds", "LNGREC": "long_rec", "LNGRS": "long_rush",
                 "LNGPSCMP": "long_pass", "TTD": "tds", "TACKLES": "tackles", "SACKSPLAYER": "sacks",
                 "TKPAST": "tack_ast", "KPT": "kick_pts", "FGM": "fgm", "EXTPTM": "xpm", "PASSRTG": "pass_rtg"},
    "baseball": {"HITS": "hits", "K": "k", "KS": "k", "TB": "tb", "RBI": "rbi", "HR": "hr", "R": "runs"},
    "soccer": {},
}
PROPY_STAT = {"ATD": "td", "AGSC": "goal", "DD": "dd", "TD": "td3", "1STTD": "first_td", "LSTTD": "last_td",
              "1STGS": "first_goal"}
_PER_SUFFIX = re.compile(r"(Q[1-4]|H[12])$")
_OU_RE = re.compile(r"^(Over|Under)\s+(-?\d+(?:\.\d+)?)$")


def sel_side(s):
    """(side, line_or_None) from a selection: type A/B/X/Over/Under/Yes/No, or 'Over 11.5' (MULTILINE)."""
    st = str(s.get("type") or "")
    sd = SIDE.get(st)
    if sd:
        return sd, None
    m = _OU_RE.match(st) or _OU_RE.match(str(s.get("name") or ""))
    if m:
        return m.group(1).lower(), float(m.group(2))
    return None, None

_ladder = None


def _ladder_dec(idx):
    global _ladder
    if _ladder is None:
        try:
            _ladder = json.load(open(LADDER, encoding="utf-8"))
        except Exception:
            _ladder = {}
    e = _ladder.get(str(idx))
    try:
        return float(e.get("decimal")) if e else None
    except (TypeError, ValueError):
        return None


def _price(s):
    if s.get("displayed") is False:
        return None
    o = s.get("odds")
    if o is None and s.get("rootIdx") is not None:
        o = _ladder_dec(s.get("rootIdx"))
    try:
        o = float(o)
    except (TypeError, ValueError):
        return None
    return o if o > 1.0 else None


def _subtype(st):
    """'M:A:3393#2.5' -> ('M', 'A', '3393', 2.5); 'Q1#-9.5' -> ('Q1', None, None, -9.5)."""
    st = str(st or "")
    head, _, ln = st.partition("#")
    try:
        line = float(ln) if ln not in ("", None) else None
    except ValueError:
        line = None
    toks = head.split(":")
    per = toks[0] if toks else "M"
    ab = toks[1] if len(toks) > 1 and toks[1] in ("A", "B") else None
    pid = toks[2] if len(toks) > 2 and toks[2].isdigit() else None
    return per, ab, pid, line


def _period(per, sport):
    if per in PERIOD:
        return PERIOD[per]
    if sport == "baseball" and re.fullmatch(r"I\d+", per or ""):
        return per
    if re.fullmatch(r"G\d+", per or ""):
        return per            # "goal N" markets
    return None               # timed windows / odd periods -> generic X with raw period


def classify(sport, code, mname, per, ab, pid, sels):
    """-> (family, period, subject_kind, stat, is_alt) or None."""
    rest = code.split(":", 1)[1] if ":" in code else code
    fam = MAIN_CODES.get(rest)
    period = _period(per, sport)
    if fam:
        subj = "game"
        if "@" in fam:
            fam, period = fam.split("@")
        if fam.startswith("TT:"):
            fam, subj = "TT", fam.split(":")[1]
        if period is None:
            return None
        return fam, period, subj, "", False
    scope, _, pc = rest.partition(":")
    if pid and (pc.startswith("PROP") or pc.startswith("ALTPROP")):
        alt = pc.startswith("PROPALT") or pc.startswith("ALTPROP")
        x = pc[7:] if alt else pc[4:]
        x = x.replace("MULTILINE", "")
        x = re.sub(r"\d+$", "", x) if alt else x
        pm = _PER_SUFFIX.search(x)
        if pm and scope == "P" and len(x) > 2:
            x = x[: pm.start()]
        st = PROP_STAT.get(sport, {}).get(x)
        sides = {sel_side(s)[0] for s in sels}
        if isinstance(st, tuple):
            st, period = st
        if st and sides & {"over", "under"}:
            return "PROP", period or "FG", "player", st, alt
        if x in PROPY_STAT or (sides and sides <= {"yes", "no"}):
            return "PROPY", period or "FG", "player", PROPY_STAT.get(x, x.lower()), alt
        return "X:" + rest, period or (per or "FG"), "player", "", alt
    if rest.endswith(":AGSC") or rest == "FT:AGSC":
        return "PROPY_SELS", period or "FG", "player", "goal", False
    subj = "game"
    m = re.match(r"(FT|FTOT|FTEI|P):(A|B):", rest)
    if m:
        subj = "home" if m.group(2) == "A" else "away"
    return "X:" + rest, period or (per or "FG"), subj, "", False


# ------------------------------------------------------------------------------------ matching

class Matcher:
    def __init__(self, c, ids):
        self.c = c
        self.ids = ids
        self.cache = {}
        self.names = {}
        self.unmatched = {}

    def team_names(self, team_id):
        if team_id in self.names:
            return self.names[team_id]
        r = self.c.execute("SELECT abbr, name, short FROM teams WHERE id=?", (team_id,)).fetchone()
        al = [a for (a,) in self.c.execute("SELECT alias FROM team_alias WHERE team_id=?", (team_id,))]
        out = {D.norm(x) for x in (r or ()) if x} | set(al)
        self.names[team_id] = out
        return out

    def _sim(self, hr_name, team_id):
        n = D.norm(hr_name)
        best = 0.0
        for a in self.team_names(team_id):
            if not a:
                continue
            if a == n:
                return 1.0
            if len(n) >= 4 and (a.startswith(n) or n.startswith(a)):
                best = max(best, 0.9)
            best = max(best, difflib.SequenceMatcher(None, n, a).ratio())
        return best

    def event(self, league, hr_eid, start_ms, home_name, away_name):
        k = str(hr_eid)
        if k in self.cache:
            return self.cache[k]
        r = self.c.execute("SELECT event_id FROM event_xref WHERE src='hr' AND ext_id=?", (k,)).fetchone()
        if r:
            sw = self.c.execute("SELECT 1 FROM event_xref WHERE src='hr_swap' AND ext_id=?", (k,)).fetchone()
            self.cache[k] = (r[0], bool(sw))
            return self.cache[k]
        try:
            ts = int(int(start_ms) / 1000)
        except (TypeError, ValueError):
            return None
        cands = self.c.execute("SELECT id, home_id, away_id, start_ts FROM events WHERE league=? AND start_ts BETWEEN ? AND ?",
                               (league, ts - 18 * 3600, ts + 18 * 3600)).fetchall()
        best = None
        for eid, hid, aid, st in cands:
            s1 = min(self._sim(home_name, hid), self._sim(away_name, aid))
            s2 = min(self._sim(home_name, aid), self._sim(away_name, hid))
            for s, swap in ((s1, False), (s2, True)):
                score = s - abs(st - ts) / 86400.0 * 0.05
                if s >= 0.72 and (best is None or score > best[0]):
                    best = (score, eid, swap)
        if not best:
            self.unmatched[k] = (league, home_name, away_name, ts)
            return None
        _, eid, swap = best
        D.add_xref(self.c, "hr", k, eid)
        if swap:
            D.add_xref(self.c, "hr_swap", k, eid)
        # learn the HR spellings as aliases (exact names only)
        e = self.c.execute("SELECT home_id, away_id FROM events WHERE id=?", (eid,)).fetchone()
        hid, aid = (e[1], e[0]) if swap else (e[0], e[1])
        self.ids.alias(league, home_name, hid)
        self.ids.alias(league, away_name, aid)
        self.cache[k] = (eid, swap)
        return self.cache[k]


def _player(c, sport, hr_pid, name, team_id, cache):
    key = (sport, hr_pid or "", name)
    if key in cache:
        return cache[key]
    pid = None
    if hr_pid:
        r = c.execute("SELECT player_id FROM player_xref WHERE src=? AND ext_id=?", ("hr:" + sport, hr_pid)).fetchone()
        pid = r[0] if r else None
    if pid is None and name:
        nm = name
        if "," in nm:                       # "Matinpalo, Nikolas" -> "Nikolas Matinpalo"
            a, b = nm.split(",", 1)
            nm = b.strip() + " " + a.strip()
        n = D.norm_person(nm)
        rows = c.execute("SELECT id, team_id FROM players WHERE sport=? AND norm=?", (sport, n)).fetchall()
        if len(rows) == 1:
            pid = rows[0][0]
        elif len(rows) > 1 and team_id:
            same = [r[0] for r in rows if r[1] == team_id]
            pid = same[0] if len(same) == 1 else None
        if pid is None:
            cur = c.execute("INSERT INTO players(sport, name, norm, team_id) VALUES (?,?,?,?)", (sport, nm, n, team_id))
            pid = cur.lastrowid
        if hr_pid:
            c.execute("INSERT OR IGNORE INTO player_xref(src, ext_id, player_id) VALUES (?,?,?)",
                      ("hr:" + sport, hr_pid, pid))
    cache[key] = pid
    return pid


# ------------------------------------------------------------------------------------ board parse

def _event_teams(ev):
    nm = ev.get("name") or ""
    for sep in (" vs. ", " vs ", " v "):
        if sep in nm:
            h, a = nm.split(sep, 1)
            return h.strip(), a.strip()
    if " @ " in nm:
        a, h = nm.split(" @ ", 1)
        return h.strip(), a.strip()
    return None, None


def parse_event(c, ids, matcher, league, sport, ev, pcache):
    """-> (event_id, {key: (price, main)}, inplay) for one HR event, or None if unmatched."""
    home, away = _event_teams(ev)
    mk = ev.get("markets") or []
    for m in mk:  # prefer ML selection names
        if (m.get("type") or "").endswith("FTOT:ML") or (m.get("type") or "").endswith("FT:AXB"):
            for s in m.get("sel") or []:
                if s.get("type") == "A":
                    home = s.get("name") or home
                elif s.get("type") == "B":
                    away = s.get("name") or away
            break
    if not home or not away:
        return None
    hit = matcher.event(league, ev.get("id"), ev.get("eventTime"), home, away)
    if not hit:
        return None
    eid, swap = hit
    inplay = bool(ev.get("inplay"))
    e = c.execute("SELECT home_id, away_id FROM events WHERE id=?", (eid,)).fetchone()
    team_of = {"A": e[1] if swap else e[0], "B": e[0] if swap else e[1]}
    raw = {}           # (family, period, subj, stat) -> list of (side, l100, price, alt, label)
    for m in mk:
        if m.get("state") not in (None, "OPEN") or m.get("displayed") is False:
            continue
        code = m.get("type") or ""
        per, ab, hpid, line = _subtype(m.get("subtype"))
        sels = m.get("sel") or []
        cl = classify(sport, code, m.get("name"), per, ab, hpid, sels)
        if not cl:
            continue
        fam, period, subj_kind, stat, alt = cl
        base = fam.split(":")[0] if not fam.startswith("X:") else "X"
        if inplay and (base not in INPLAY_FAMS):
            continue
        if fam == "PROPY_SELS":         # one market, one selection per player (anytime goalscorer)
            for s in sels:
                p = _price(s)
                st = str(s.get("type") or "")
                abp = st.split(":")[0] if ":" in st else None
                hp = st.split(":")[-1] if st else None
                pid = _player(c, sport, hp, s.get("name"), team_of.get(abp), pcache)
                raw.setdefault(("PROPY", period, pid, stat), []).append(("yes", 50, p, False, None))
            continue
        if subj_kind == "player":
            nm = (m.get("name") or "").split(" - ")[0].strip()
            subj = _player(c, sport, hpid, nm, team_of.get(ab), pcache)
        elif subj_kind == "home":
            subj = -2 if swap else -1
        elif subj_kind == "away":
            subj = -1 if swap else -2
        else:
            subj = 0
        for s in sels:
            p = _price(s)
            stype = str(s.get("type") or "")
            sd, sline = sel_side(s)
            if swap and sd in ("home", "away"):
                sd = "away" if sd == "home" else "home"
            lab = None
            if fam in ("ML", "ML3", "DNB", "SPREAD"):
                if sd not in ("home", "away", "draw"):
                    continue
            elif fam in ("TOTAL", "TT", "PROP"):
                if sd not in ("over", "under"):
                    continue
            elif fam == "PROPY":
                if sd not in ("yes", "no", "over", "under"):
                    continue
                sd = "yes" if sd in ("yes", "over") else "no"
            else:  # generic
                if sd is None:
                    lab = (s.get("name") or stype)[:60] if not stype.isdigit() else (s.get("name") or stype)[:60]
            lk = 0
            ln = sline if sline is not None else line
            if fam == "SPREAD" and line is not None:
                home_line = -line if swap else line
                lk = D.l100(home_line if sd == "home" else -home_line)
            elif ln is not None and fam not in ("ML", "ML3", "DNB"):
                lk = D.l100(ln)
            raw.setdefault((fam, period, subj, stat), []).append((sd, lk, p, alt, lab))
    out = {}
    for (fam, period, subj, stat), sl in raw.items():
        mid = ids.market(eid, fam, period, subj, stat)
        # main line: most balanced two-way pair among non-alt lines
        mains = set()
        two_way = fam in ("SPREAD", "TOTAL", "TT", "PROP")

        def pkey(sd, lk):            # spreads pair on the signed HOME line
            return (lk if sd == "home" else -lk) if fam == "SPREAD" else lk
        if two_way:
            pairs, nonalt = {}, set()
            for sd, lk, p, alt, lab in sl:
                if alt or p is None:
                    continue
                nonalt.add(pkey(sd, lk))
                pairs.setdefault(pkey(sd, lk), {})[sd] = p
            best = None
            for key, d in pairs.items():
                if len(d) == 2:
                    a, b = list(d.values())
                    bal = abs((1 / a) / (1 / a + 1 / b) - 0.5)
                    if best is None or bal < best[0]:
                        best = (bal, key)
            if best:
                mains.add(best[1])
            elif len(nonalt) == 1:   # one-sided market with a single non-alt line (e.g. NHL goals o0.5)
                mains |= nonalt
        rank = {}
        for sd, lk, p, alt, lab in sl:
            side = D.SIDES[sd] if sd in D.SIDES else ids.label(lab or "?")
            if two_way:
                main = 1 if (pkey(sd, lk) in mains and not alt) else 0
            else:
                main = 0 if alt else 1
            # The main market and an alt ladder rung often share a key (SOG O/U 1.5 vs "To Record 2+ Shots",
            # whose Under is listed UNPRICED). Keep the non-alt, priced quote whatever the board order;
            # last-one-wins wiped the main Under and flagged the Over as alt (MintyBot 2026-11-02).
            r = (1 if alt else 0, 1 if p is None else 0)
            k = (mid, side, lk)
            if k in rank and rank[k] <= r:
                continue
            rank[k] = r
            out[k] = (p, main)
    return eid, out, inplay


def _latest_state(c, eid, book):
    rows = c.execute(
        "SELECT q.market_id, q.side, q.l100, q.price, q.main FROM quotes q JOIN markets m ON m.id=q.market_id "
        "WHERE m.event_id=? AND q.book=? AND q.src=1 AND q.ts = (SELECT MAX(q2.ts) FROM quotes q2 WHERE "
        "q2.market_id=q.market_id AND q2.book=q.book AND q2.side=q.side AND q2.l100=q.l100 AND q2.src=1)",
        (eid, book)).fetchall()
    return {(r[0], r[1], r[2]): (r[3], r[4]) for r in rows}


def ingest_file(c, ids, matcher, path, state, pcache, stats):
    d = json.loads(gzip.decompress(open(path, "rb").read()))
    comp_name = D.norm((d.get("events") or [{}])[0].get("compName") if d.get("events") else "")
    league = LEAGUE_BY_COMP.get(comp_name)
    if not league:
        stats["skipped_comp"] += 1
        return 0
    ts = int(d.get("ts") or os.path.getmtime(path))
    book = ids.book("hr", "Hard Rock")
    n = 0
    rows = []
    for ev in d.get("events") or []:
        sport = SPORT_KEY.get(ev.get("sport"))
        if not sport:
            continue
        res = parse_event(c, ids, matcher, league, sport, ev, pcache)
        if not res:
            stats["unmatched_events"] += 1
            continue
        eid, cur, inplay = res
        last_obs = c.execute("SELECT MAX(last_ts) FROM board_obs WHERE event_id=? AND book=?", (eid, book)).fetchone()[0]
        if last_obs and ts < last_obs:
            stats["stale_boards"] += 1
            continue
        if eid not in state:
            state[eid] = _latest_state(c, eid, book)
        prev = state[eid]
        for key, (p, main) in cur.items():
            old = prev.get(key)
            if p is None:
                if old is not None and old[0] is not None:
                    rows.append((key[0], book, key[1], key[2], ts, 1, None, main))
                    prev[key] = (None, main)
                continue
            if old is None or old[0] != round(p, 6) or old[1] != main:
                rows.append((key[0], book, key[1], key[2], ts, 1, round(p, 6), main))
                prev[key] = (round(p, 6), main)
        # removals: offered before, absent now (in-play boards only judge in-play families)
        for key, old in list(prev.items()):
            if old[0] is None or key in cur:
                continue
            rows.append((key[0], book, key[1], key[2], ts, 1, None, old[1]))
            prev[key] = (None, old[1])
        D.obs(c, eid, book, ts)
        n += 1
    if rows:
        c.executemany("INSERT OR REPLACE INTO quotes(market_id, book, side, l100, ts, src, price, main) "
                      "VALUES (?,?,?,?,?,?,?,?)", rows)
    stats["quotes"] += len(rows)
    stats["events"] += n
    return len(rows)


def run(max_files=None, min_age_s=30):
    c = D.open_db()
    ids = D.Ids(c)
    matcher = Matcher(c, ids)
    state, pcache = {}, {}
    stats = {"files": 0, "quotes": 0, "events": 0, "unmatched_events": 0, "skipped_comp": 0, "stale_boards": 0,
             "errors": 0}
    now = time.time()
    files = sorted(glob.glob(os.path.join(SPOOL, "*", "*.json.gz")),
                   key=lambda p: (os.path.basename(os.path.dirname(p)), os.path.basename(p)))
    for p in files:
        if max_files and stats["files"] >= max_files:
            break
        try:
            if now - os.path.getmtime(p) < min_age_s:
                continue
            ingest_file(c, ids, matcher, p, state, pcache, stats)
            c.commit()
            day = os.path.basename(os.path.dirname(p))
            os.makedirs(os.path.join(DONE, day), exist_ok=True)
            shutil.move(p, os.path.join(DONE, day, os.path.basename(p)))
            stats["files"] += 1
        except Exception as e:
            stats["errors"] += 1
            log.warning("hr_ingest %s: %r", os.path.basename(p), e)
            c.rollback()
            try:
                bad = os.path.join(D.WH_DIR, "spool", "hr_bad")
                os.makedirs(bad, exist_ok=True)
                shutil.move(p, os.path.join(bad, os.path.basename(p)))
            except Exception:
                pass
    # stale tmp files + old done dirs
    for t in glob.glob(os.path.join(SPOOL, "*", "*.tmp")):
        if now - os.path.getmtime(t) > 3600:
            try:
                os.remove(t)
            except OSError:
                pass
    cutoff = time.strftime("%Y%m%d", time.gmtime(now - KEEP_DAYS * 86400))
    for dd in glob.glob(os.path.join(DONE, "*")):
        if os.path.basename(dd) < cutoff:
            shutil.rmtree(dd, ignore_errors=True)
    for dd in glob.glob(os.path.join(SPOOL, "*")):
        try:
            if os.path.isdir(dd) and not os.listdir(dd) and os.path.basename(dd) < time.strftime("%Y%m%d", time.gmtime(now)):
                os.rmdir(dd)
        except OSError:
            pass
    c.commit()
    if matcher.unmatched:
        sample = list(matcher.unmatched.values())[:8]
        log.info("hr_ingest unmatched HR events: %d (sample %s)", len(matcher.unmatched), sample)
    log.info("hr_ingest %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    print(run())
