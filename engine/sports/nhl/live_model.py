"""NHL live (in-game) model (MintyBot 2026-10-28, Austin "the live model ... first pass NHL"; the July
engine priced only period markets off a flat 2.0 goals/period with no team strength, score state or
empty net, and was switched off 2026-09-16).

Each team scores at its PRE-GAME regulation rate from the ratings model (engine.sports.nhl._ratings,
injuries included), spread over the periods by the measured shares and scaled by a multiplier for the
game state -- lead bucket (-3+..+3+, from the team's own view) x late window (the last 10 / 5 / 2
minutes of regulation when the game is within two: pulled goalies and empty-net goals). The remaining
regulation is stepped in 20 s increments over the joint (home, away) added-goals grid, the rates
re-read from the running score each step. A regulation tie goes to OT/SO (home .533, one goal).

Multipliers: goals / expected per state over 3,942 regular-season + playoff games 2023-11..2026-04
(Beelink data/live.db historical_pbp x walk-forward ratings lambdas), shrunk with 25 expected goals,
level-normalised (overall goals/expected = .9614, the same scale as the ratings grid's LATE s=.9611)
so the pre-game model stays the anchor. Walk-forward check (fit before 2025-07, test 2025-26, n=1,348;
Pi scratchpad live/nhl_live_fit.py), live vs the same model with no state effects, log-loss:
  moneyline   P2 start .6054 / .6055   10 left .3745 / .3745   2 left .2663 / .2691
  over 5.5    P2 start .5725 / .5731   10 left .2892 / .2972   2 left .1374 / .1825
  home -1.5   P2 start .5188 / .5192   10 left .2729 / .2820   2 left .1530 / .2065
v2 (same night, Austin "needs to be better, more confident, smarter"): the CURRENT special-teams
situation (NHL API live play-by-play situationCode + active penalties) multiplies each team's rate
for its remaining time -- power play x2.09, short-handed x.54, 4-on-4 x1.16, own goalie pulled x1.57,
shooting at an empty net x1.65 (goals / expected per state, train seasons; +445 nats on the 2025-26
test). In-game shot / xG form was tested (shot-quality xG off 221k attempts) and adds nothing beyond
the pre-game rate, so it is NOT used -- the model deliberately holds steady where the live line chases
shot dominance.
No historical live prices exist, so the edge vs Hard Rock's live board is proven going forward: every
period's first live read of each market is recorded to picks_unified as a shadow row (bet types
"LIVE P<n> ML / PL / O/U", one cell per period), graded by engine.grading.nhl_adapter.
"""
from __future__ import annotations

import logging
import math
import threading
import time

import numpy as np

logger = logging.getLogger(__name__)

PSHARE = (0.3068, 0.3608, 0.3729)
P_OT_HOME = 0.533
STEP_S = 20
LATE_EDGES = (600, 300, 120, 0)
# (lead from the scoring team's view, late window) -> rate multiplier
MULT = {(-3, 0): 0.9174, (-2, 0): 1.0267, (-1, 0): 0.9974, (0, 0): 0.9627, (1, 0): 0.9617, (2, 0): 0.9257,
        (3, 0): 0.9496, (-2, 1): 0.9587, (-1, 1): 0.8944, (0, 1): 0.8148, (1, 1): 0.7332, (2, 1): 0.6761,
        (-2, 2): 1.2315, (-1, 2): 0.8829, (0, 2): 0.724, (1, 2): 0.9203, (2, 2): 2.1466, (-2, 3): 1.465,
        (-1, 3): 1.7359, (0, 3): 0.6833, (1, 3): 4.6081, (2, 3): 4.6432}
# strength-state multipliers (relative to the average state mix the base rate already carries)
STRENGTH = {"PP": 2.0870, "SH": 0.5428, "EV4": 1.1566, "EN": 1.6460, "XA": 1.5709}
_K = 12
_I = np.arange(_K + 1)[:, None]
_J = np.arange(_K + 1)[None, :]


def _late(tl: float) -> int:
    if tl > LATE_EDGES[0]:
        return 0
    for i in range(1, len(LATE_EDGES)):
        if tl > LATE_EDGES[i]:
            return i
    return len(LATE_EDGES) - 1


def _mult_grid(d: np.ndarray, tl: float) -> np.ndarray:
    lb = np.clip(d, -3, 3)
    late = np.where(np.abs(d) <= 2, _late(tl), 0) if tl <= LATE_EDGES[0] else np.zeros_like(d)
    out = np.ones(d.shape)
    for (lv, lt), m in MULT.items():
        out[(lb == lv) & (late == lt)] = m
    return out


def strength_key(side: str, sc) -> str:
    """situationCode 'abcd' = away goalie, away skaters, home skaters, home goalie -> the state from
    ``side``'s view: EN (opponent net empty), XA (own goalie pulled), PP, SH, EV4 (4v4/3v3), EV."""
    try:
        s = str(sc)
        ag, ask, hsk, hg = int(s[0]), int(s[1]), int(s[2]), int(s[3])
    except Exception:
        return "EV"
    own, opp = (hsk, ask) if side == "home" else (ask, hsk)
    if (ag if side == "home" else hg) == 0:
        return "EN"
    if (hg if side == "home" else ag) == 0:
        return "XA"
    if own > opp:
        return "PP"
    if own < opp:
        return "SH"
    return "EV" if own == 5 else "EV4"


def price(lam_home: float, lam_away: float, elapsed_s: float, home: int, away: int,
          situation: dict | None = None) -> dict:
    """Distribution of final regulation goals from the current state. ``elapsed_s`` = regulation
    seconds played (0..3600). ``situation`` = {"code": situationCode, "remaining_s": seconds the
    current special-teams state still runs} -> that state's multipliers apply for that long."""
    P = np.zeros((_K + 1, _K + 1)); P[0, 0] = 1.0
    d = (home + _I) - (away + _J)
    t0 = t = max(0.0, min(3600.0, float(elapsed_s)))
    sh_h = sh_a = 1.0; s_until = t0
    if situation and situation.get("code"):
        sh_h = STRENGTH.get(strength_key("home", situation["code"]), 1.0)
        sh_a = STRENGTH.get(strength_key("away", situation["code"]), 1.0)
        s_until = t0 + max(0.0, float(situation.get("remaining_s") or 0))
    while t < 3600:
        dt = min(STEP_S, 3600 - t)
        base = 3 * PSHARE[min(2, int(t // 1200))] / 3600.0 * dt
        tl = 3600 - t
        on = t < s_until
        ph = 1 - np.exp(-lam_home * base * _mult_grid(d, tl) * (sh_h if on else 1.0))
        pa = 1 - np.exp(-lam_away * base * _mult_grid(-d, tl) * (sh_a if on else 1.0))
        Q = P * (1 - ph) * (1 - pa)
        Q[1:, :] += (P * ph * (1 - pa))[:-1, :]
        Q[:, 1:] += (P * pa * (1 - ph))[:, :-1]
        Q[1:, 1:] += (P * ph * pa)[:-1, :-1]
        P = Q
        t += dt
    fh, fa = home + _I, away + _J
    return {"grid": P, "fh": fh, "fa": fa}


def markets(res: dict) -> dict:
    P, fh, fa = res["grid"], res["fh"], res["fa"]
    tie = float(P[fh == fa].sum())
    reg_h, reg_a = float(P[fh > fa].sum()), float(P[fh < fa].sum())
    tot = fh + fa + (fh == fa)          # OT / shootout adds one goal

    def over(line):
        return float(P[tot > line].sum())

    def cover(team, point):             # puck line incl OT/SO (an OT win is by one)
        m = (fh - fa) if team == "home" else (fa - fh)
        return float(P[(m + point) > 0].sum())
    return {"home_win": reg_h + P_OT_HOME * tie, "away_win": reg_a + (1 - P_OT_HOME) * tie,
            "reg": (reg_h, tie, reg_a), "over": over, "cover": cover}


# -- pre-game rates (cached per game for the day) -------------------------------------------------
_LAM: dict = {}
_LAM_LOCK = threading.Lock()


def pregame_lambdas(home: str, away: str, game_date: str):
    key = (home, away, game_date)
    with _LAM_LOCK:
        hit = _LAM.get(key)
    if hit and time.time() - hit[0] < 6 * 3600:
        return hit[1]
    try:
        from ._ratings import lambdas
        lam = lambdas(home, away, game_date)
    except Exception as e:
        logger.debug("nhl live: pregame lambdas failed %s@%s: %s", away, home, e)
        lam = None
    val = (lam["home"], lam["away"]) if lam else None
    with _LAM_LOCK:
        _LAM[key] = (time.time(), val)
    return val


# -- live special-teams situation (NHL API) -------------------------------------------------------
_SIT_CACHE: dict = {}


def _nhl_get(path: str, ttl: float = 20.0):
    hit = _SIT_CACHE.get(path)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    import json as _json
    import urllib.request
    try:
        req = urllib.request.Request("https://api-web.nhle.com" + path, headers={"User-Agent": "curl/8.4.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            val = _json.loads(r.read())
    except Exception as e:
        logger.debug("nhl live: %s failed: %s", path, e)
        val = None
    _SIT_CACHE[path] = (time.time(), val)
    return val


def live_situation(home: str, away: str) -> dict | None:
    """{"code", "remaining_s", "nhl_id", "elapsed"} for a live game from the NHL API: the last play's
    situationCode and, for a special-teams state, how long it still runs (active penalties; a minor
    ends at the first power-play goal). Goalie-pulled states are assumed to last 60 s. None if the
    game can't be matched (the model then prices plain even strength)."""
    from ._ratings import canon
    sc = _nhl_get("/v1/score/now")
    gid = None
    for g in (sc or {}).get("games") or []:
        if canon((g.get("homeTeam") or {}).get("abbrev")) == canon(home) and \
                canon((g.get("awayTeam") or {}).get("abbrev")) == canon(away):
            gid = g.get("id")
            break
    if not gid:
        return None
    pbp = _nhl_get(f"/v1/gamecenter/{gid}/play-by-play")
    plays = (pbp or {}).get("plays") or []
    if not plays:
        return None
    home_id = (pbp.get("homeTeam") or {}).get("id")

    def el(p):
        per = int((p.get("periodDescriptor") or {}).get("number") or 1)
        mm, ss = str(p.get("timeInPeriod") or "0:00").split(":")
        return (per - 1) * 1200 + int(mm) * 60 + int(ss)
    last = plays[-1]
    code = last.get("situationCode") or "1551"
    now = el(last)
    clk = pbp.get("clock") or {}
    try:
        per = int((pbp.get("periodDescriptor") or {}).get("number") or 1)
        now = max(now, (per - 1) * 1200 + 1200 - int(clk.get("secondsRemaining")))
    except (TypeError, ValueError):
        pass
    kh, ka = strength_key("home", code), strength_key("away", code)
    rem = 0.0
    if kh in ("XA", "EN") or ka in ("XA", "EN"):
        rem = 60.0
    elif kh in ("PP", "SH") or ka in ("PP", "SH") or kh == "EV4":
        ends = []
        for p in plays:
            if p.get("typeDescKey") != "penalty":
                continue
            dd = p.get("details") or {}
            dur = float(dd.get("duration") or 0) * 60
            if dur <= 0 or dur > 600:
                continue
            st0 = el(p); end = st0 + dur
            if end <= now:
                continue
            if dur <= 120:    # a minor ends at the opponent's first goal after it starts
                pen_team = dd.get("eventOwnerTeamId")
                if any(q.get("typeDescKey") == "goal" and st0 < el(q) <= now
                       and (q.get("details") or {}).get("eventOwnerTeamId") not in (None, pen_team) for q in plays):
                    continue
            ends.append(end)
        rem = (min(ends) - now) if ends else 0.0
    return {"code": code, "remaining_s": max(0.0, min(rem, 300.0)), "nhl_id": gid, "elapsed": now,
            "home_state": kh, "away_state": ka, "home_id": home_id}


def _implied(am):
    try:
        o = float(am)
    except (TypeError, ValueError):
        return None
    if o == 0 or abs(o) < 100:
        return None
    return abs(o) / (abs(o) + 100) if o < 0 else 100 / (o + 100)


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def candidates(state: dict, *, shadow: bool = False) -> list[dict]:
    """Live candidates for one in-progress game vs Hard Rock's live board (state = engine.live._state
    shape + "odds"). Regulation only (no OT pricing). Probabilities are PA-anchored to the de-vigged
    live market unless ``shadow`` (W=1: the model's own number)."""
    st = state.get("status") or {}
    per = int(st.get("period") or 0)
    if per < 1 or per > 3 or st.get("completed"):
        return []
    elapsed = (per - 1) * 1200 + (1200 - int(st.get("clock_secs") or 0))
    if elapsed >= 3590:
        return []
    odds = state.get("odds") or {}
    if not odds:
        return []
    h_ab, a_ab = (state.get("home") or {}).get("abbr"), (state.get("away") or {}).get("abbr")
    from engine._tz import et_today_str
    lam = pregame_lambdas(h_ab, a_ab, et_today_str())
    if not lam:
        return []
    hs, as_ = int((state.get("home") or {}).get("score") or 0), int((state.get("away") or {}).get("score") or 0)
    try:
        sit = live_situation(h_ab, a_ab)
    except Exception as e:
        logger.debug("nhl live situation failed %s@%s: %s", a_ab, h_ab, e)
        sit = None
    mk = markets(price(lam[0], lam[1], elapsed, hs, as_, sit))
    from ._market_anchor import anchor, shadow_mode
    out = []
    snap = {"pick_at_period": per, "pick_at_clock_secs": int(st.get("clock_secs") or 0),
            "home_score": hs, "away_score": as_,
            "situation": (sit or {}).get("home_state") and {"home": sit["home_state"], "away": sit["away_state"],
                                                              "remaining_s": round(sit["remaining_s"])},
            "model_home_win": round(mk["home_win"], 4)}
    _ih, _ia = _implied(odds.get("home_ml")), _implied(odds.get("away_ml"))
    snap["market_home_win"] = round(_ih / (_ih + _ia), 4) if _ih and _ia else None

    def add(bt, pick, p, side_odds, opp_odds, line=None, main=True):
        imp = _implied(side_odds)
        if imp is None or p is None:
            return
        if shadow:
            with shadow_mode():
                pp = anchor(p, side_odds, opp_odds, bet_type=bt, pick=pick)
        else:
            pp = anchor(p, side_odds, opp_odds, bet_type=bt, pick=pick)
        out.append({"type": bt, "bet_type": bt, "pick": pick, "odds": int(side_odds), "line": line, "main": main,
                    "model_prob": round(p, 4), "prob": round(pp, 4), "edge": round((pp - imp) * 100, 2),
                    "game_id": state.get("game_id"), "matchup": state.get("matchup"), **snap})

    tag = f"LIVE P{per}"
    hm, am_ = odds.get("home_ml"), odds.get("away_ml")
    add(f"{tag} ML", h_ab, mk["home_win"], hm, am_)
    add(f"{tag} ML", a_ab, mk["away_win"], am_, hm)
    # puck line: main + alt rungs (alt "point" is the HOME point)
    pls = []
    if _f(odds.get("home_spread_point")) is not None:
        pls.append((_f(odds.get("home_spread_point")), odds.get("home_spread_odds"), odds.get("away_spread_odds"), True))
    for a in odds.get("alt_spreads") or []:
        if _f(a.get("point")) is not None:
            pls.append((_f(a.get("point")), a.get("home_odds"), a.get("away_odds"), False))
    seen = set()
    for hp, ho, ao, main in pls:
        if hp is None or hp in seen or abs(abs(hp % 1) - 0.5) > 1e-9:
            continue
        seen.add(hp)
        add(f"{tag} PL", f"{h_ab} {hp:+g}", mk["cover"]("home", hp), ho, ao, hp, main)
        add(f"{tag} PL", f"{a_ab} {-hp:+g}", mk["cover"]("away", -hp), ao, ho, -hp, main)
    tots = []
    if _f(odds.get("over_under")) is not None:
        tots.append((_f(odds.get("over_under")), odds.get("over_odds"), odds.get("under_odds"), True))
    for a in odds.get("alt_totals") or []:
        if _f(a.get("line")) is not None:
            tots.append((_f(a.get("line")), a.get("over_odds"), a.get("under_odds"), False))
    seen = set()
    for L, oo, uo, main in tots:
        if L is None or L in seen or abs(abs(L % 1) - 0.5) > 1e-9:
            continue
        seen.add(L)
        po = mk["over"](L)
        add(f"{tag} O/U", f"Over {L:g}", po, oo, uo, L, main)
        add(f"{tag} O/U", f"Under {L:g}", 1 - po, uo, oo, L, main)
    return out


RECORD_WINDOW_S = 150
LIVE_JUICE_WALL = -200          # no live pick priced shorter than -200
# Live CLV (2026-10-28): a live pick has no single closing line, so its "close" is Hard Rock's price
# for the SAME selection CLV_AFTER_S later, taken only while the score is unchanged (a goal changes
# the bet, not the market's opinion of it). Pending reads live in memory; a worker restart drops them.
CLV_AFTER_S = 180
CLV_GIVE_UP_S = 900
_CLV_PENDING: dict = {}   # (game_key, bet_type) -> {"pick", "score", "t"}


def best_pick(cands: list[dict]) -> dict | None:
    """ONE pick per period, like the full-game cards (Austin 2026-10-28): the best positive-edge
    candidate across every live market (ML, puck lines, totals, alts) ranked edge x prob -- the same
    ranking select_pick / select_lean use. None = no-play this period."""
    # Juice wall (Austin 2026-11-01 "it realistically shouldn't be picking anything lower than -200"): live ML
    # picks averaged -690 and went 5-7 for -4.74u -- one loss erased ~7 wins.
    pos = [c for c in cands or [] if c.get("edge", 0) > 0 and int(c.get("odds") or 0) >= LIVE_JUICE_WALL]
    if not pos:
        return None
    return max(pos, key=lambda c: c["edge"] * c["prob"])


def record_shadow(state: dict) -> int:
    """Mirror this game's START-OF-PERIOD pick to picks_unified (is_shadow=1): within the first
    RECORD_WINDOW_S of each period, the single best candidate (best_pick, model's own number) is
    recorded -- one fixed-time pick per game per period, never re-picked later in the period. v1
    logged each market the first moment it showed any positive edge (cherry-picks moments)."""
    st = state.get("status") or {}
    per = int(st.get("period") or 0)
    if int(st.get("clock_secs") or 0) < 1200 - RECORD_WINDOW_S:
        return 0
    try:
        from engine.picks_unified._schema import get_conn
        from engine.picks_unified._legacy_bridge import _game_key_for
        gk = str(_game_key_for("nhl", "nhl", state.get("game_id")))
        if get_conn().execute("SELECT 1 FROM picks WHERE game_key = ? AND bet_type LIKE ? LIMIT 1",
                              (gk, f"LIVE P{per} %")).fetchone():
            return 0
    except Exception as e:
        logger.debug("nhl live shadow dedupe failed %s: %s", state.get("game_id"), e)
        return 0
    try:
        pick = best_pick(candidates(state, shadow=True))
    except Exception as e:
        logger.debug("nhl live shadow pricing failed %s: %s", state.get("game_id"), e)
        return 0
    if not pick:
        return 0
    try:
        from engine.picks_unified import record_shadow_slate
        from engine._tz import et_today_str
        r = record_shadow_slate("nhl", "nhl", state.get("game_id"), state.get("matchup"), [pick],
                                pick_date=et_today_str())
        if r.get("mirrored"):
            try:   # the game state the pick was made at (period / clock / score) -- the tracker shows it
                c = get_conn()
                c.execute("UPDATE picks SET pick_at_period = ?, pick_at_clock_secs = ?, pick_at_home_score = ?, "
                          "pick_at_away_score = ? WHERE game_key = ? AND bet_type = ? AND pick_at_period IS NULL",
                          (pick.get("pick_at_period"), pick.get("pick_at_clock_secs"), pick.get("home_score"),
                           pick.get("away_score"), gk, pick["type"]))
                c.commit()
            except Exception as e:
                logger.debug("nhl live shadow state stamp failed %s: %s", gk, e)
            _CLV_PENDING[(gk, pick["type"])] = {"pick": pick["pick"], "t": time.time(),
                                                 "score": (pick.get("home_score"), pick.get("away_score"))}
        return int(r.get("mirrored") or 0)
    except Exception as e:
        logger.debug("nhl live shadow record failed %s: %s", state.get("game_id"), e)
        return 0


def capture_live_clv(state: dict) -> int:
    """Stamp closing_odds on this game's pending live picks once CLV_AFTER_S has passed, if the score
    hasn't changed since the pick (see CLV_AFTER_S)."""
    if not _CLV_PENDING:
        return 0
    try:
        from engine.picks_unified._schema import get_conn
        from engine.picks_unified._legacy_bridge import _game_key_for
        gk = str(_game_key_for("nhl", "nhl", state.get("game_id")))
    except Exception:
        return 0
    mine = [k for k in _CLV_PENDING if k[0] == gk]
    if not mine:
        return 0
    now = time.time()
    score = (int((state.get("home") or {}).get("score") or 0), int((state.get("away") or {}).get("score") or 0))
    cands = None
    n = 0
    for k in mine:
        rec = _CLV_PENDING[k]
        age = now - rec["t"]
        if age > CLV_GIVE_UP_S or score != rec["score"]:
            _CLV_PENDING.pop(k, None)
            continue
        if age < CLV_AFTER_S:
            continue
        if cands is None:
            try:
                cands = candidates(state, shadow=True)
            except Exception:
                cands = []
        # the pick's bet type carries its period; once the period has rolled, look up the same
        # market under the current period's tag
        base_mk = k[1].split(" ", 2)[-1]
        hit = next((c for c in cands if c["type"].split(" ", 2)[-1] == base_mk and c["pick"] == rec["pick"]), None)
        if not hit:
            continue
        try:
            from engine.tracker_core import _cc_now_iso
            c = get_conn()
            c.execute("UPDATE picks SET closing_odds = ?, closing_captured_at = ? "
                      "WHERE game_key = ? AND bet_type = ? AND closing_odds IS NULL", (hit["odds"], _cc_now_iso(), gk, k[1]))
            c.commit()
            n += 1
        except Exception as e:
            logger.debug("nhl live clv write failed %s: %s", k, e)
        _CLV_PENDING.pop(k, None)
    return n


def card(state: dict) -> dict:
    """Everything the live NHL card shows for one game: model vs Hard Rock win%, the current
    special-teams situation, this period's single lean (model's own number, not graduated) and the
    picks already recorded for earlier periods with their results."""
    cands = []
    try:
        cands = candidates(state, shadow=True)
    except Exception as e:
        logger.debug("nhl live card pricing failed %s: %s", state.get("game_id"), e)
    pick = best_pick(cands)
    snap = cands[0] if cands else {}
    periods = []
    try:
        from engine.picks_unified._schema import get_conn
        from engine.picks_unified._legacy_bridge import _game_key_for
        gk = str(_game_key_for("nhl", "nhl", state.get("game_id")))
        for bt, pk, od, pr, ed, res in get_conn().execute(
                "SELECT bet_type, pick_text, odds, prob, edge_pct, result FROM picks "
                "WHERE game_key = ? AND bet_type LIKE 'LIVE P% %' ORDER BY bet_type", (gk,)):
            periods.append({"period": int(bt.split()[1][1:]), "bet_type": bt, "pick": pk, "odds": od,
                            "prob": pr, "edge": ed, "result": res})
    except Exception as e:
        logger.debug("nhl live card periods failed %s: %s", state.get("game_id"), e)
    return {"model_home_win": snap.get("model_home_win"), "market_home_win": snap.get("market_home_win"),
            "situation": snap.get("situation"),
            "pick": dict(pick, graduated=False, lean=True) if pick else None, "periods": periods}


def _imp(am):
    try:
        am = float(am)
    except (TypeError, ValueError):
        return None
    return 100.0 / (am + 100.0) if am > 0 else -am / (-am + 100.0)


def tracker(limit: int = 300) -> dict:
    """Live NHL tracker (Austin 2026-10-29): every recorded start-of-period pick (LIVE P<n> shadow
    rows), graded W/L/P with flat 1u P/L at the recorded price, CLV (implied prob of the price
    CLV_AFTER_S later minus the pick price's, + = beat it), split by period and by market."""
    from engine.picks_unified._schema import get_conn
    rows = []
    for (gk, date, mu, bt, pk, od, co, pr, ed, res, prof, per, clk, hs, as_, ts) in get_conn().execute(
            "SELECT game_key, pick_date, matchup, bet_type, pick_text, odds, closing_odds, prob, edge_pct, result, "
            "profit, pick_at_period, pick_at_clock_secs, pick_at_home_score, pick_at_away_score, created_at "
            "FROM picks WHERE sport = 'nhl' AND bet_type LIKE 'LIVE P% %' ORDER BY created_at DESC LIMIT ?", (limit,)):
        p_in, p_cl = _imp(od), _imp(co)
        units = None
        if res in ("W", "L", "P") and od is not None:
            units = 0.0 if res == "P" else (-1.0 if res == "L" else (od / 100.0 if od > 0 else 100.0 / -od))
        rows.append({"game_key": gk, "date": date, "matchup": mu, "bet_type": bt,
                     "period": int(bt.split()[1][1:]), "market": bt.split(" ", 2)[-1], "pick": pk,
                     "odds": od, "closing_odds": co, "model_prob": pr, "edge": ed, "result": res,
                     "units": units, "clv_pp": (round((p_cl - p_in) * 100, 1) if p_in is not None and p_cl is not None else None),
                     "at_period": per, "at_clock_secs": clk, "at_home_score": hs, "at_away_score": as_, "created_at": ts})

    def summ(rs):
        g = [r for r in rs if r["units"] is not None]
        w = sum(1 for r in g if r["result"] == "W"); l = sum(1 for r in g if r["result"] == "L")
        u = sum(r["units"] for r in g)
        cl = [r["clv_pp"] for r in rs if r["clv_pp"] is not None]
        return {"n": len(rs), "graded": len(g), "w": w, "l": l, "p": len(g) - w - l,
                "pending": sum(1 for r in rs if r["result"] not in ("W", "L", "P", "V")),
                "units": round(u, 2), "roi": (round(100 * u / len(g), 1) if g else None),
                "clv_n": len(cl), "clv_pp": (round(sum(cl) / len(cl), 2) if cl else None),
                "beat_clv": sum(1 for x in cl if x > 0),
                "exp_edge": (round(sum((r["edge"] or 0) for r in rs) / len(rs), 2) if rs else None)}
    return {"overall": summ(rows),
            "by_period": {f"P{n}": summ([r for r in rows if r["period"] == n]) for n in (1, 2, 3)},
            "by_market": {m: summ([r for r in rows if r["market"] == m]) for m in ("ML", "PL", "O/U")},
            "rows": rows}


__all__ = ["price", "markets", "candidates", "best_pick", "record_shadow", "capture_live_clv", "pregame_lambdas", "card",
           "tracker"]
