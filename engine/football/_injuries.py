"""ESPN injury signal for football (Austin 2026-09-19).

The team-Elo model is blind to a starting QB being out — the single biggest
"team looks great on paper but their QB is hurt" blind spot. This fetches ESPN's
league-wide injury report (one call, all teams), stores it per team, and exposes
a per-team signal (QB status + how many are out) the predictor applies as an Elo
adjustment. ESPN team ids == our teams.id (verified), so no name-matching.

Foundation increment: data in + signal out. The predict-time adjustment + a
walk-forward validation of the QB-out penalty are the next increments.
"""
from __future__ import annotations

import json
import logging
import urllib.request
from datetime import datetime, timedelta as _td, timezone as _tz

from ._db import get_conn

logger = logging.getLogger(__name__)

_ESPN = "https://site.api.espn.com/apis/site/v2/sports/football/{sport}/injuries"
_SPORT = {"nfl": "nfl", "cfb": "college-football"}  # cfb data is sparser/less official

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS team_injuries ("
    " id INTEGER PRIMARY KEY AUTOINCREMENT,"
    " team_id INTEGER, player TEXT, position TEXT, status TEXT,"
    " body_part TEXT, captured_at TEXT,"
    " UNIQUE(team_id, player)"
    ")"
)

# Statuses that meaningfully remove a player from the game.
_OUT = {"Out", "Injured Reserve", "Doubtful"}


def _ensure(conn):
    conn.execute(_SCHEMA)
    # 2026-10-25: ESPN's projected return date + when the status was set. The per-game miss
    # probabilities need both (a status set before the team's last game is LAST week's).
    have = {r[1] for r in conn.execute("PRAGMA table_info(team_injuries)")}
    for col in ("return_date", "status_date"):
        if col not in have:
            conn.execute(f"ALTER TABLE team_injuries ADD COLUMN {col} TEXT")
    conn.commit()


def fetch_injuries(league: str) -> dict:
    """ESPN league injury report -> {team_id: [{player, position, status, body_part}, ...]}."""
    sport = _SPORT.get(league)
    if not sport:
        return {}
    try:
        req = urllib.request.Request(
            _ESPN.format(sport=sport), headers={"User-Agent": "curl/8.4.0"})
        data = json.loads(urllib.request.urlopen(req, timeout=15).read())
    except Exception as e:
        logger.warning("[football:%s] injury fetch failed: %s", league, e)
        return {}
    out: dict = {}
    for team in (data.get("injuries") or []):
        try:
            tid = int(team.get("id"))
        except (TypeError, ValueError):
            continue
        lst = []
        for it in (team.get("injuries") or []):
            ath = it.get("athlete") or {}
            pos = ((ath.get("position") or {}).get("abbreviation")) or ""
            det = it.get("details") if isinstance(it.get("details"), dict) else {}
            lst.append({
                "player": ath.get("displayName") or "",
                "position": pos,
                "status": it.get("status") or "",
                "body_part": det.get("type") or "",
                "return_date": det.get("returnDate") or None,
                "status_date": it.get("date") or None,
            })
        out[tid] = lst
    return out


def refresh_injuries(league: str) -> dict:
    """Fetch + persist the current report (replace-in-place per team)."""
    inj = fetch_injuries(league)
    if not inj:
        return {"teams": 0, "players": 0}
    conn = get_conn(league)
    _ensure(conn)
    now = datetime.utcnow().isoformat()
    n = 0
    for tid, lst in inj.items():
        conn.execute("DELETE FROM team_injuries WHERE team_id=?", (tid,))
        for p in lst:
            if not p["player"]:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO team_injuries "
                "(team_id, player, position, status, body_part, captured_at, return_date, status_date) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (tid, p["player"], p["position"], p["status"], p["body_part"], now,
                 p.get("return_date"), p.get("status_date")))
            n += 1
    conn.commit()
    logger.info("[football:%s] injuries refreshed: %d teams, %d players", league, len(inj), n)
    return {"teams": len(inj), "players": n}


def team_injury_signal(league: str, team_id: int) -> dict:
    """Per-team signal: is the QB out (name), and how many players are Out/IR/
    Doubtful. Benign/empty when we have no data (so callers no-op safely)."""
    conn = get_conn(league)
    _ensure(conn)
    rows = conn.execute(
        "SELECT player, position, status FROM team_injuries WHERE team_id = ?",
        (int(team_id),)).fetchall()
    qb_out = None
    out_count = 0
    for r in rows:
        st = r["status"]
        if st in _OUT:
            out_count += 1
            if r["position"] == "QB" and qb_out is None:
                qb_out = r["player"]
    return {"qb_out": qb_out, "starters_out": out_count, "n_injuries": len(rows)}


# ── Calibration logging (Austin 2026-09-19) ──────────────────────────
# Snapshot each game's pre-kickoff injury state so that, as games settle, we
# accumulate (injury state -> outcome) pairs and can fit the QB-out penalty
# instead of guessing it. ESPN gives only CURRENT injuries, so we must capture
# the state at game time and store it against the game_id.

_LOG_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS injury_game_log ("
    " game_id TEXT PRIMARY KEY, home_team_id INTEGER, away_team_id INTEGER,"
    " home_qb_out INTEGER, away_qb_out INTEGER,"
    " home_out_count INTEGER, away_out_count INTEGER,"
    " home_qb_name TEXT, away_qb_name TEXT, snapshot_at TEXT"
    ")"
)


def snapshot_injury(league: str, game_id, home_team_id, away_team_id) -> None:
    """Upsert the CURRENT injury signal for a (still-scheduled) game. Called each
    compute while the game is scheduled, so the last write before kickoff is the
    game-day state we calibrate against. Once the game goes live/final the caller
    stops snapshotting, locking the pre-game record."""
    try:
        conn = get_conn(league)
        conn.execute(_LOG_SCHEMA)
        sh = team_injury_signal(league, home_team_id)
        sa = team_injury_signal(league, away_team_id)
        conn.execute(
            "INSERT INTO injury_game_log (game_id, home_team_id, away_team_id,"
            " home_qb_out, away_qb_out, home_out_count, away_out_count,"
            " home_qb_name, away_qb_name, snapshot_at) VALUES (?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(game_id) DO UPDATE SET"
            "  home_qb_out=excluded.home_qb_out, away_qb_out=excluded.away_qb_out,"
            "  home_out_count=excluded.home_out_count, away_out_count=excluded.away_out_count,"
            "  home_qb_name=excluded.home_qb_name, away_qb_name=excluded.away_qb_name,"
            "  snapshot_at=excluded.snapshot_at",
            (str(game_id), int(home_team_id), int(away_team_id),
             1 if sh.get("qb_out") else 0, 1 if sa.get("qb_out") else 0,
             int(sh.get("starters_out") or 0), int(sa.get("starters_out") or 0),
             sh.get("qb_out") or "", sa.get("qb_out") or "",
             datetime.utcnow().isoformat()))
        conn.commit()
    except Exception as e:
        logger.debug("[football:%s] injury snapshot failed for %s: %s", league, game_id, e)


def calibrate_qb_penalty(league: str) -> dict:
    """Empirical QB-out effect over SETTLED games that have an injury snapshot.
    Walk Elo forward to each game (pre-game state, leak-safe) to get the model's
    expected home win-prob, then compare the actual result for games where
    exactly one side's QB was out. Returns the observed under-performance and its
    Elo-equivalent. Needs a season of data to be meaningful."""
    from ._elo import update as _eu, expected_score, INIT_ELO, DEFAULT_HFA, DEFAULT_K
    from . import get_league_config
    conn = get_conn(league)
    conn.execute(_LOG_SCHEMA)
    cfg = get_league_config(league)
    hfa = cfg.get("home_advantage") or DEFAULT_HFA
    k = cfg.get("elo_k") or DEFAULT_K
    snaps = {r["game_id"]: r for r in conn.execute(
        "SELECT game_id, home_qb_out, away_qb_out FROM injury_game_log").fetchall()}
    if not snaps:
        return {"n_settled_with_snapshot": 0, "note": "no snapshots yet — logging just started"}
    rows = conn.execute(
        "SELECT game_id, home_team_id h, away_team_id a, home_score hs, away_score as_ "
        "FROM games WHERE status='final' AND home_score IS NOT NULL AND away_score IS NOT NULL "
        "ORDER BY date ASC, game_id ASC").fetchall()
    ratings = {}
    # tally: for games where exactly one QB is out, actual home-win minus expected
    diffs = []
    n = 0
    for r in rows:
        gid = str(r["game_id"]); h = int(r["h"]); a = int(r["a"])
        rh = ratings.get(h, INIT_ELO); ra = ratings.get(a, INIT_ELO)
        snap = snaps.get(gid)
        if snap is not None and (snap["home_qb_out"] ^ snap["away_qb_out"]):
            exp_home = expected_score(rh, ra, hfa)
            actual_home = 1.0 if r["hs"] > r["as_"] else 0.0 if r["hs"] < r["as_"] else 0.5
            # sign so positive = the QB-OUT team under-performed
            signed = (exp_home - actual_home) if snap["home_qb_out"] else (actual_home - exp_home)
            diffs.append(signed); n += 1
        nh, na = _eu(rh, ra, home_score=int(r["hs"]), away_score=int(r["as_"]), hfa=hfa, k=k)
        ratings[h] = nh; ratings[a] = na
    if not diffs:
        return {"n_settled_with_snapshot": 0, "note": "no settled one-QB-out games yet"}
    avg = sum(diffs) / len(diffs)
    return {"n_one_qb_out_games": n,
            "avg_qb_out_underperformance_pp": round(avg * 100, 1),
            "note": "positive = QB-out teams lost more than Elo expected; multiply by ~elo_per_point to size the penalty. Needs many games."}


# ---------------------------------------------------------------------------
# NON-QB injury penalty (snap-share weighted) -- Austin 2026-10-11. The factor
# model only docked for QB-out; every other injury (star WR/CB/edge out) was a
# blind spot. Weight each Out/IR/Doubtful non-QB player by how much he actually
# plays (nflverse snap_counts offense_pct/defense_pct, season max) so a
# full-time starter moves the number and a backup barely does. NFL ONLY (no
# nflverse snaps for CFB). Snap-share caps at 1.0 -> superstars slightly
# under-weighted vs role starters (no player-value data); still far better than
# the blind spot. GBM is dropped in _ensemble when this is material.
# ---------------------------------------------------------------------------
import os as _os, time as _time, unicodedata as _ud, re as _re2, threading as _threading
from pathlib import Path as _Path

_SNAP_PARQUET = _Path(__file__).resolve().parents[2] / "data" / "football" / "_nflverse_snaps_2026.parquet"
_SNAP_URL = ("https://github.com/nflverse/nflverse-data/releases/download/"
             "snap_counts/snap_counts_2026.parquet")
_SNAP_TTL = 86400.0
_SNAP_CACHE = {"ts": 0.0, "lk": {}}
_NON_QB_SCALE = 30.0   # Elo per full-time-starter-equivalent (snap-share 1.0 ~ 1 pt)
_NON_QB_CAP = 120.0    # cap non-QB injury penalty below the QB's 175

_SUFFIX_RE = _re2.compile(r"\b(jr|sr|ii|iii|iv|v)\b")

def _snap_norm(name):
    nk = _ud.normalize("NFKD", name or "")
    a = "".join(c for c in nk if not _ud.combining(c))
    a = _re2.sub(r"[^a-zA-Z\s]", " ", a).lower()
    a = _SUFFIX_RE.sub(" ", a)        # ESPN "Nolan Smith Jr." / "Michael Carter II" == nflverse's plain name
    return _re2.sub(r"\s+", " ", a).strip()


def _snap_lookup():
    """{(norm_name, team): season-PEAK snap share} (= the player's role when healthy), plus
    _SNAP_CACHE["last"] (last week he actually played) and _SNAP_CACHE["mw"] (latest week in
    the file). 2026-10-25 (MintyBot, Austin "is the model accounting for PHI's injuries?"):
    the old "appeared in the last 2 weeks" cliff dropped exactly the players most likely to
    be out -- a starter who already missed last week vanished from the table and his absence
    priced at 0 (Hurts, PHI week 7). Replaced by _snap_weight's per-missed-game decay."""
    now = _time.time()
    if _SNAP_CACHE["lk"] and (now - _SNAP_CACHE["ts"] < _SNAP_TTL):
        return _SNAP_CACHE["lk"]
    try:
        import pandas as _pd, urllib.request as _url
        if (not _SNAP_PARQUET.exists()) or (now - _SNAP_PARQUET.stat().st_mtime > _SNAP_TTL):
            _url.urlretrieve(_SNAP_URL, str(_SNAP_PARQUET))
        df = _pd.read_parquet(_SNAP_PARQUET)
        df["_snap"] = df[["offense_pct", "defense_pct"]].max(axis=1)
        df["_pn"] = df["player"].map(_snap_norm)
        played = df[df["_snap"] > 0]
        _smax = played.groupby(["_pn", "team"])["_snap"].max()
        _last = played.groupby(["_pn", "team"])["week"].max()
        _SNAP_CACHE["lk"] = {k: float(v) for k, v in _smax.items()}
        _SNAP_CACHE["last"] = {k: int(v) for k, v in _last.items()}
        _SNAP_CACHE["mw"] = int(df["week"].max())
        _SNAP_CACHE["ts"] = now
    except Exception as e:
        logger.warning("[football:nfl] snap-share load failed (injury penalties run blind): %s", e)
    return _SNAP_CACHE["lk"]


def _snap_weight(name, abbr):
    """(decayed_weight, peak) for an injured player. Decay = the games Elo has already
    absorbed without him: ~15% of the effect per missed game (Elo moves ~25 pts/game on a
    175-pt QB hit). Just went down -> full; out since week 1 -> a quarter; no snaps this
    season -> not a loss vs current Elo -> 0."""
    lk = _snap_lookup()
    key = (_snap_norm(name), abbr)
    peak = lk.get(key)
    if not peak:
        return 0.0, 0.0
    mw = _SNAP_CACHE.get("mw") or 0
    last = (_SNAP_CACHE.get("last") or {}).get(key, mw)
    missed = max(0, mw - last - 1)
    return float(peak) * max(0.0, 1.0 - 0.15 * missed), float(peak)


# ESPN team codes -> nflverse (snap counts + official report). Before 2026-10-25 the Rams (LAR)
# and Commanders (WSH) never matched, so their injuries were never priced at all.
_NFLV_TEAM = {"LAR": "LA", "WSH": "WAS"}

# ── Per-game miss probabilities (MintyBot 2026-10-25, Austin "have it fade it and then ...
# confirm if the player is actually out or not before the game"). Today's injury list used to
# count in full against EVERY upcoming game: a player ruled Out today was priced 100% out next
# week too (BAL@BUF: Lamar Jackson inactive today -> model BUF by 13.5 vs HR -3.5). Each player
# now carries the probability he misses THIS game, measured on nflverse official reports x snap
# counts 2022-25 (regular season, players with season-peak snap share >= .5):
#   the game's own report:   Out 1.00 | Doubtful .99 | Questionable .32
#   carried a week ahead:    Out .64 (limited practice .41, full .54, DNP .665)
#                            Doubtful .40 (limited .32, DNP .47)
#                            Questionable .19 (inactive that game .37, played it .11)
# Order: IR/reserve (or ESPN projecting him out a week past the game) -> out; game day (<=6h)
# ESPN's statuses are the inactives list -> confirmed; the game's official report filed ->
# confirmed; an ESPN status set for this game but not official yet -> faded from the carried
# rate (6+ days out) to the game rate (<=2 days out); otherwise last game's status, carried.
_P_GAME = {"Out": 1.0, "Doubtful": 0.99, "Questionable": 0.32}
_P_CARRY = {"Out": 0.64, "Doubtful": 0.40, "Questionable": 0.19}
_P_CARRY_FINE = {("Out", "LP"): 0.41, ("Out", "FP"): 0.54, ("Out", "DNP"): 0.665,
                 ("Doubtful", "LP"): 0.32, ("Doubtful", "DNP"): 0.47,
                 ("Questionable", "missed"): 0.37, ("Questionable", "played"): 0.11}
_LONG_TERM = {"Injured Reserve", "Physically Unable to Perform", "Suspension",
              "Non-Football Injury", "Commissioner Exempt"}
_GAME_DAY_H = 6.0


def _ts(s):
    """'2026-10-27T00:15Z' / '2026-10-25T18:48Z' / '2026-11-01' -> aware UTC datetime (or None)."""
    if not s:
        return None
    try:
        d = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=_tz.utc)
    except Exception:
        return None


def _practice(ps):
    ps = ps or ""
    return "DNP" if "Did Not" in ps else ("LP" if "Limited" in ps else ("FP" if "Full" in ps else ""))


def game_for(league, home_team_id, away_team_id):
    """(kickoff_utc, week) of the next not-yet-final game between the two teams, or None."""
    try:
        r = get_conn(league).execute(
            "SELECT start_time, week FROM games WHERE home_team_id=? AND away_team_id=? "
            "AND COALESCE(status,'') != 'final' ORDER BY start_time LIMIT 1",
            (int(home_team_id), int(away_team_id))).fetchone()
    except Exception:
        return None
    k = _ts(r[0]) if r else None
    return (k, r[1]) if k else None


def _miss_prob(espn, off_game, filed, off_prev, kick, prev_kick, now):
    """(p_miss, basis) for one player. ``espn`` = (status, status_dt, return_dt) or None;
    ``off_game`` / ``off_prev`` = (report_status, practice) on this game's / last game's report."""
    st, st_dt, ret = espn if espn else (None, None, None)
    if st in _LONG_TERM:
        if ret is None or ret.date() > kick.date():
            return 1.0, "IR/reserve"
        st = "Out"                                            # designated to return by this game
    elif st in _P_GAME and ret is not None and (ret - kick).days >= 7:
        return 0.9, "ESPN: out until %s" % ret.date()
    fresh = bool(st_dt and prev_kick and st_dt > prev_kick + _td(hours=6))   # status set for THIS game
    hrs = (kick - now).total_seconds() / 3600.0
    if hrs <= _GAME_DAY_H and fresh and (st == "Active" or st in _P_GAME):
        return (0.0, "active (game day)") if st == "Active" else (_P_GAME[st], "game day: " + st)
    if filed:
        rs = off_game[0] if off_game else None
        return (_P_GAME[rs], "official: " + rs) if rs in _P_GAME else (0.0, "not on official report")
    if fresh and st in _P_GAME:
        days = hrs / 24.0
        f = min(1.0, max(0.0, (days - 2.0) / 4.0))            # 0 at <=2 days out, 1 at >=6
        return _P_GAME[st] + f * (_P_CARRY[st] - _P_GAME[st]), "ESPN %s, faded %.1fd out" % (st, days)
    if fresh and st == "Active":
        return 0.0, "ESPN active"
    if off_prev and off_prev[0] in _P_CARRY:
        rs, ps = off_prev
        if rs == "Questionable":
            missed = st in ("Out", "Doubtful") or st in _LONG_TERM
            key = (rs, "missed" if missed else ("played" if st in (None, "Active") else ""))
        else:
            key = (rs, ps)
        return _P_CARRY_FINE.get(key, _P_CARRY[rs]), "carried: last week %s" % rs
    if st in _P_CARRY:
        return _P_CARRY[st], "carried: ESPN " + st
    return 0.0, ""


def _injury_probs(conn, abbr, nv, team_id, game):
    """{norm_name: (name, position, p_miss, basis)} for the team's ``game`` = (kickoff, week).
    ``game`` None (no scheduled game found) -> the old face-value read (Out/Doubtful/IR = out)."""
    _ensure(conn)
    espn = {}
    for r in conn.execute(
            "SELECT player, position, status, status_date, return_date FROM team_injuries "
            "WHERE team_id=?", (int(team_id),)):
        espn[_snap_norm(r[0])] = (r[0], r[1], r[2], _ts(r[3]), _ts(r[4]))
    rep = _official_report()
    if game is None:
        out = {}
        wks = [w for (t, w) in rep if t == nv]
        for n, v in (rep.get((nv, max(wks)), {}) if wks else {}).items():
            if v[2] in ("Out", "Doubtful"):
                out[n] = (v[0], v[1], 1.0, "official: " + v[2])
        for n, e in espn.items():
            if e[2] in ("Out", "Injured Reserve", "Doubtful"):
                out.setdefault(n, (e[0], e[1], 1.0, "ESPN: " + e[2]))
        return out
    kick, wk = game
    prev = conn.execute(
        "SELECT start_time, week FROM games WHERE (home_team_id=? OR away_team_id=?) "
        "AND start_time < ? ORDER BY start_time DESC LIMIT 1",
        (int(team_id), int(team_id), (kick - _td(days=1)).strftime("%Y-%m-%dT%H:%MZ"))).fetchone()
    prev_kick = _ts(prev[0]) if prev else None
    off_g = rep.get((nv, wk), {})
    filed = any(v[2] in _P_GAME for v in off_g.values())
    off_p = rep.get((nv, prev[1]), {}) if prev and prev[1] is not None else {}
    now = datetime.now(_tz.utc)
    out = {}
    for n in set(espn) | set(off_g) | set(off_p):
        e = espn.get(n); og = off_g.get(n); op = off_p.get(n)
        src = e or og or op
        p, basis = _miss_prob((e[2], e[3], e[4]) if e else None,
                              (og[2], _practice(og[3])) if og else None, filed,
                              (op[2], _practice(op[3])) if op else None, kick, prev_kick, now)
        if p > 0:
            out[n] = (src[0], src[1], p, basis)
    return out


def team_non_qb_injury_penalty(league, team_id, game=None):
    """(penalty_elo, snap_weight) for a team's injured NON-QB players in ``game``. NFL only."""
    if league != "nfl":
        return 0.0, 0.0
    try:
        _, nq, _, total, _ = _penalties(league, team_id, game)
        return nq, round(total, 3)
    except Exception:
        return 0.0, 0.0


# ── Combined snap-weighted injury penalty (QB + non-QB), Austin 2026-10-11 v2: the QB
# penalty scales with the OUT quarterback's own snap-share (starter ~1.0 -> 175, a backup
# ~0.05 -> ~0; ATL's Cooper Rush/Jack Strand once wrongly tanked ATL). Non-QB snap x 30,
# cap 120. NFL only. 2026-10-25 v3: sources unioned (official report + ESPN incl. IR),
# weights decayed by games already absorbed (see _snap_weight), suffix-safe names.
_QB_SNAP_SCALE = 175.0
_QB_PEN_CAP = 175.0
_STARTER_SNAP = 0.5


_PEN_MEMO: dict = {}   # (league, team_id, game) -> (ts, result): one slate pass calls predict ~5x/game
_PEN_MEMO_TTL = 120.0


def _penalties(league, team_id, game=None):
    """(qb_pen, non_qb_pen, starter_qb_out, non_qb_weight, detail) for ``game`` = (kickoff, week).
    Each injured player counts p_miss x snap weight (see _miss_prob / _snap_weight); a starting
    QB "out" = season-peak snap >= .5 AND p_miss >= .5. ``detail`` = [(name, pos, p_miss,
    snap_weight, elo, basis)] largest first."""
    mk = (league, int(team_id), game)
    hit = _PEN_MEMO.get(mk)
    if hit and _time.time() - hit[0] < _PEN_MEMO_TTL:
        return hit[1]
    conn = get_conn(league)
    row = conn.execute("SELECT abbreviation FROM teams WHERE id=?", (int(team_id),)).fetchone()
    abbr = row[0] if row else None
    if not abbr or not _snap_lookup():
        return 0.0, 0.0, None, 0.0, []
    nv = _NFLV_TEAM.get(abbr, abbr)
    qb_pen = 0.0; nq_pen = 0.0; nq_total = 0.0; starter = None; detail = []
    for player, pos, p, basis in _injury_probs(conn, abbr, nv, team_id, game).values():
        w, peak = _snap_weight(player, nv)
        if w <= 0:
            continue
        if (pos or "") == "QB":
            e = p * w * _QB_SNAP_SCALE
            qb_pen += e
            if peak >= _STARTER_SNAP and p >= 0.5 and starter is None:
                starter = player
        else:
            e = p * w * _NON_QB_SCALE
            nq_pen += e
            nq_total += p * w
        detail.append((player, pos, round(p, 2), round(w, 2), round(e, 1), basis))
    detail.sort(key=lambda d: -d[4])
    res = (min(_QB_PEN_CAP, round(qb_pen, 1)), min(_NON_QB_CAP, round(nq_pen, 1)), starter, nq_total, detail)
    _PEN_MEMO[mk] = (_time.time(), res)
    return res


def team_injury_penalty(league, team_id, game=None):
    """(qb_penalty, non_qb_penalty, starter_qb_out_name) for ``game`` = (kickoff, week) from
    game_for() -- each player weighted by the chance he misses THAT game (faded a week ahead,
    confirmed by the official report / game-day inactives). Snap-share weighted, decayed by
    games already absorbed into Elo. NFL only."""
    if league != "nfl":
        return 0.0, 0.0, None
    try:
        qb, nq, starter, _, _ = _penalties(league, team_id, game)
        return qb, nq, starter
    except Exception as e:
        logger.debug("[football:nfl] injury penalty failed for team %s: %s", team_id, e)
        return 0.0, 0.0, None


def team_injury_detail(league, team_id, game=None):
    """[(name, pos, p_miss, snap_weight, elo, basis)] behind team_injury_penalty (display/audit)."""
    if league != "nfl":
        return []
    try:
        return _penalties(league, team_id, game)[4]
    except Exception:
        return []


# ── Official NFL injury report (nflverse `injuries` = the weekly game-status report every
# team files) -- Austin 2026-10-11. 2026-10-25: nflverse ships a week's rows with PRACTICE
# statuses days before the GAME statuses are filed, so a team-week only counts as "filed" once
# it carries Out/Doubtful/Questionable (see _injury_probs). Every week is kept (last week's
# report drives the carried probabilities); refreshed every 3h so Friday/Saturday filings land
# before kickoff (ESPN's feed, refreshed every 30 min, carries the game-day inactives).
_INJ_PARQUET = _Path(__file__).resolve().parents[2] / "data" / "football" / "_nflverse_injuries_2026.parquet"
_INJ_URL = ("https://github.com/nflverse/nflverse-data/releases/download/"
            "injuries/injuries_2026.parquet")
_INJ_TTL = 3 * 3600.0
_INJ_CACHE = {"ts": 0.0, "rep": None}
_INJ_LOCK = _threading.Lock()


def _official_report():
    """{(nflverse_team, week): {norm_name: (full_name, position, report_status|None, practice|None)}}"""
    now = _time.time()
    if _INJ_CACHE["rep"] is not None and now - _INJ_CACHE["ts"] < _INJ_TTL:
        return _INJ_CACHE["rep"]
    with _INJ_LOCK:
        if _INJ_CACHE["rep"] is not None and now - _INJ_CACHE["ts"] < _INJ_TTL:
            return _INJ_CACHE["rep"]
        rep = _INJ_CACHE["rep"] or {}
        try:
            import pandas as _pd, urllib.request as _url
            if (not _INJ_PARQUET.exists()) or (now - _INJ_PARQUET.stat().st_mtime > _INJ_TTL):
                tmp = str(_INJ_PARQUET) + ".tmp"
                _url.urlretrieve(_INJ_URL, tmp)
                _os.replace(tmp, str(_INJ_PARQUET))
            df = _pd.read_parquet(_INJ_PARQUET)
            new = {}
            for r in df[["team", "week", "full_name", "position", "report_status", "practice_status"]].itertuples(index=False):
                rs = r.report_status if isinstance(r.report_status, str) else None
                ps = r.practice_status if isinstance(r.practice_status, str) else None
                new.setdefault((r.team, int(r.week)), {})[_snap_norm(r.full_name)] = (r.full_name, r.position, rs, ps)
            rep = new
        except Exception as e:
            logger.warning("[football:nfl] official injury report load failed: %s", e)
        _INJ_CACHE["rep"] = rep
        _INJ_CACHE["ts"] = now
        return rep
