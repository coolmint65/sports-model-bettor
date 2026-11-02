"""Dynamic proven-edge gate for the Pick Authority (MintyBot 2026-10-18, Austin "one system
for every pick, period"). Computes each (sport, league, bet_type) market's realized edge from
the settled record in picks_unified and decides HEADLINE-ELIGIBILITY: a market earns a real/
headline slot only once it has PROVEN +ROI over a real sample. Defer-to-market by default;
everything is paper-tracked until it graduates, so nothing is ever "untried" (the chicken-and-egg
Austin raised).

Hybrid gate: ROI > MIN_ROI AND ( n >= STD_N  OR  (n >= FAST_N AND Bayesian_conf >= FAST_CONF) ).
The Bayesian confidence uses a skeptical prior centered at the market's own break-even (strength
PRIOR_N), so a market must EARN the right to disagree with the close. Cached (TTL) and recomputed
from the live settled record, so graduation is automatic (no hardcoded list).

LEAGUE SEGMENTATION (MintyBot 2026-10-22, Austin "segment ALL of them"): the gate used to key
solely on (sport, bet_type), which blends every league inside a multi-league sport into one cell
-- "basketball" lumps wnba with ~20 minor leagues, "soccer" lumps 16 leagues, "football" lumps
nfl/cfb/ufl, "tennis" lumps atp/wta, "hockey" lumps its own framework's leagues too. A
league with its own real edge could be diluted below the floor by its sport-mates (or vice versa,
masked as "proven" by a league that isn't). Keying on (sport, league, bet_type) lets each league
earn its own eligibility. Single-league sports (nhl, nba, mlb, "baseball"==NCAA) are unaffected --
their one league's cell is identical to the old blended cell.

CONDITION SEGMENTATION (MintyBot 2026-10-22, Austin "add fav/dog + over/under as a gate dimension
so only the PROVEN side headlines"): keying on (sport, league, bet_type) still blends a market's
TWO SIDES into one cell -- a spread/moneyline/3-way market blends its favorite and underdog picks,
a totals market blends its overs and unders. Within a market the edge is routinely ONE-SIDED (mlb
RL profits on the dog, loses on the fav; cfb ALT_SPREAD same shape; wnba TOTAL profits on the
under, loses on the over) and WHICH side is proven VARIES by market (wnba Q1_SPREAD is the mirror
image -- there it's the FAV side that's proven). Keying on (sport, league, bet_type, condition)
lets each side earn its own eligibility, so a losing side can no longer headline on its market-
mate's coattails. `_condition()` derives 'over'/'under' straight from the pick text for totals,
'fav'/'dog' from the spread sign printed in the pick text ("DET +1.5" -> dog, "TB -1.5" -> fav),
or -- for a moneyline/3-way/prop pick with no line printed, just a team/player name -- from the
sign of its own priced odds (positive American odds = dog, negative = fav; still a real favorite/
underdog signal, just carried by the price instead of a printed line). A market with neither
signal (condition='') aggregates as the whole market, unchanged.
"""
from __future__ import annotations
import sqlite3, math, re, time, os

_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "picks_unified.db")
PRIOR_N = 10.0     # skeptical prior strength (market pseudo-counts)
MIN_ROI = 2.0      # realized ROI%% floor
STD_N   = 15       # standard sample needed to headline (GAMES, not rows, since the 2026-10-25 one-trial-per-game dedupe; 20->15 Austin "also lower it 15")
FAST_N  = 8        # fast-track: a strong small sample can graduate early ...
FAST_CONF = 0.90   # ... if the Bayesian confidence it beats break-even is this high
MIN_CONF  = 0.70   # global floor: a market must clear this Bayesian conf to headline (Austin 2026-10-20)
# Per-market conf-floor overrides (non-universal; default = MIN_CONF). Austin 2026-10-21: the floor
# shouldn't be one universal number. Key = (sport, league, NORMALIZED bet_type, CONDITION). Lower
# only where the cell is +EV, standing on n>=STD_N (so the floor is the ONLY thing blocking it --
# below STD_N a cell needs FAST_CONF=0.90 to qualify regardless of this floor, so a floor override
# never changes a thin cell's outcome), and the floor is set just under the cell's own proven conf
# ("prove-it", not a free pass).
#
# CONDITION-SEGMENTED 2026-10-22 -- every floor below was re-verified against its condition-split
# cell, not the old blended (sport, league, bet_type) cell. Segmentation routinely shows the floor
# was propping up a blend of one proven side + one unproven (or thin) side:
#   nhl/PL: BOTH sides are -EV post-split (dog n=51 roi=-12.8%/conf .19; fav n=36 roi=-10.9%/conf
#     .31) -- stays DORMANT (inherited dormant from the 2026-10-22 league-split, blended conf .156);
#     kept on "fav" (the less-negative side) as the best-available bookkeeping target -- zero live
#     effect either way until/unless a side turns +EV again.
#   nba/Q1_TOTAL: "over" is the proven direction (n=16, roi +12.6%, conf .676) but still BLOCKED by
#     sample size (n=16<STD_N=20 and conf<FAST_CONF=0.90, so this floor is currently a no-op);
#     "under" is genuinely NOT proven (n=40, roi only +0.57% -- fails MIN_ROI outright, floor can't
#     rescue it). Floor kept on "over" for when the sample grows.
#   basketball/wnba/TOTAL: "under" is the clean proven side (n=22, roi +20.8%, conf .813 -- already
#     clears the DEFAULT 0.70 without this override); "over" is -EV (-2.7%). Floor kept on "under"
#     as a standing lower bound in case its conf ever softens below 0.70.
#   football/cfb/ALT_SPREAD: "dog" is the clean proven side (n=27, roi +32.4%, conf .954 -- also
#     already clears DEFAULT 0.70 without this override); "fav" is -EV (-10.2%). Floor kept on
#     "dog", same reasoning as wnba/TOTAL above.
# NEW floor: nhl/O/U/under (n=45, roi +8.8%, conf .669) -- clears every other bar, misses only the
# 0.70 default by 0.031. Full post-segmentation sweep (every cell, n>=20, roi>MIN_ROI, conf in
# [0.55,0.70)) found this to be the ONLY cell matching the unlock pattern -- nothing else qualifies.
# See memory pick_authority_conf_floor_2026-10-20 (2026-10-22-dated section) for the per-cell
# (n, win, roi, conf) numbers this builds on.
_CONF_FLOORS_RAW = {
    # nhl/PL/fav 0.60 and nhl/O/U/under 0.66 REMOVED 2026-10-26: NHL's record restarts on the
    # ratings model (_EVIDENCE_SINCE below); fresh cells earn the default floor.
    # nba/Q1_TOTAL/over 0.65 REMOVED 2026-10-26: its record was reset (_EVIDENCE_SINCE below);
    # the fresh cell earns the default floor.
    ("basketball", "wnba", "TOTAL", "under"): 0.64,
    # football/cfb/ALT_SPREAD/dog 0.61 REMOVED 2026-10-25: it stood on the pre-dedupe cell (n=27
    # rows, +32.4%) -- one trial per game it is 46 games at -5.3%, and CFB sides restart from the
    # 10-25 rating model anyway (_EVIDENCE_SINCE). A fresh cell earns the default floor.
    # mlb/TOTAL/over (Austin 2026-10-25 "3, yes"): canonical labels merged the proven "O/U over"
    # cell (n=29, +16.9%, conf .81) with the two post-relabel "TOTAL over" rows (0-2) -> n=31,
    # 58.1%, +9.4% ROI, conf 0.693 -- misses the 0.70 default by 0.007 on noise. Standing above
    # STD_N, +EV; floor set just under its own conf ("prove-it", same pattern as nhl/O/U/under).
    ("mlb", "mlb", "TOTAL", "over"): 0.68,
}
# Keys canonicalized through the same normalizer _compute() uses (PL -> PUCK_LINE,
# O/U -> TOTAL) so a floor keeps matching its cell after label aliasing (2026-10-25).
from .._bet_type_alias import canonical_bet_type as _canon_bt  # noqa: E402
_CONF_FLOORS = {(s, l, _canon_bt(b), c): v for (s, l, b, c), v in _CONF_FLOORS_RAW.items()}
# EVIDENCE WINDOWS (MintyBot 2026-10-25, Austin PA CFB walkthrough "fix it all up"): a cell's
# record must come from the model that is live NOW. CFB's only "proven" cell (TOTAL/under, 17
# games, +22.9%) was entirely Sept 3-12 under the pinned-~53 total that the 10-20 scoring model
# replaced -- and the paper lane's unders from those same weeks went -31% on 72. Nothing scoped
# evidence to a model version, so a retired model vouched for the current one. Key = (sport,
# league, family) -> first pick_date that counts; family "TOTALS" = any bet type containing
# TOTAL (game / alt / team / period), "SIDES" = everything else. Add a row whenever a model
# change means the old record stops describing the live model. (The FCS segment shares CFB's.)
_EVIDENCE_SINCE = {
    ("football", "cfb", "TOTALS"): "2026-10-20",      # opponent-adjusted scoring model replaced the pinned total
    ("football", "cfb", "SIDES"): "2026-10-26",       # points-based CFB ratings went live the evening of 10-25
    ("football", "cfb_fcs", "TOTALS"): "2026-10-20",
    ("football", "cfb_fcs", "SIDES"): "2026-10-26",
    # A key may also name ONE canonical bet type (checked before its family). NBA first-quarter
    # markets (Austin 2026-10-26 "Reset q1 yeah"): every Q1 pick before the 10-26 08:25 ship came
    # off the stacked chain whose derivative curve pinned Q1 probs at .5263 -- the Q1_TOTAL/over
    # cell (19 games, +32%) graduated on that record. Q1 restarts on the calibrate-once chain.
    ("nba", "nba", "Q1_TOTAL"): "2026-10-26",
    ("nba", "nba", "Q1_SPREAD"): "2026-10-26",
    ("nba", "nba", "Q1_ML"): "2026-10-26",
    # ...and every other NBA market (Austin 2026-10-26 "let's make sure the full game picks
    # are under this model too"): full game, alts, halves and team totals all graded off
    # the stacked chain before the 10-26 08:25 ship (TOTAL/under, 20 games +18.5%, was
    # eligible on it). The 17 shadow rows recorded 10-26 before 08:25 were purged
    # (data/nba/purged_oldchain_shadow_20261026.json) so today's games re-record new-chain.
    ("nba", "nba", "TOTALS"): "2026-10-26",
    ("nba", "nba", "SIDES"): "2026-10-26",
    # NHL (Austin 2026-10-26 "make sure NHL is up to snuff"): every market now prices off the
    # ratings model (engine.sports.nhl._ratings) through the calibrate-once PA anchor. The old
    # record came from the factor/MC/GBM blend under stacked calibration (October: shadow
    # 89-121, PL claimed .68 / won 25%). Shadow rows recorded 10-26 before the ship were purged
    # (data/nhl/purged_oldchain_shadow_20261026.json) so tonight's games re-record.
    ("nhl", "nhl", "TOTALS"): "2026-10-26",
    ("nhl", "nhl", "SIDES"): "2026-10-26",
    # MLB team totals (MintyBot 2026-10-26): the HR scraper mapped FTOT:A:OUEXSO / P:A:OU5 to
    # the AWAY team, but A is the HOME team (HR's market names), so every team-total row was
    # priced against the other team's line -- the 4-0 over / 5-0 under cells stood on prices
    # that never existed. Fixed in scrapers/hardrock_odds; evidence starts the next day.
    ("mlb", "mlb", "TEAM_TOTAL"): "2026-10-27",
    # EPL (Austin 2026-10-28 "EPL up to standard"): opponent-adjusted ridge xG ratings + the
    # calibrate-once PA anchor replace the xG-ratio model under stacked calibration (12-14,
    # -$521; walk-forward 1X2 log-loss 1.0237 vs the open's 1.0009). Every market restarts.
    ("soccer", "eng_premier", "TOTALS"): "2026-10-29",
    ("soccer", "eng_premier", "SIDES"): "2026-10-29",
    # China CBA (Austin 2026-10-28 backlog #5): the ratings model + calibrate-once PA anchor replace
    # the framework factor model (55-52, -$44). Every market restarts.
    ("basketball", "china_cba", "TOTALS"): "2026-10-29",
    ("basketball", "china_cba", "SIDES"): "2026-10-29",
    # Australia NBL (Austin 2026-10-29 "up to standard fully"): back from the 10-27 removal on the ratings
    # model + PA anchor; the old framework rows were rated on a DB that held every team twice. Restarts.
    ("basketball", "australia_nbl", "TOTALS"): "2026-10-30",
    ("basketball", "australia_nbl", "SIDES"): "2026-10-30",
    ("mlb", "mlb", "F5_TEAM_TOTAL"): "2026-10-27",
}


def _family(bt):
    return "TOTALS" if "TOTAL" in bt else "SIDES"


def _segment_and_window(rows):
    """Re-key CFB games with no FBS team to league 'cfb_fcs' (engine.football._segments) and
    drop rows older than their cell's evidence window. Rows keep _compute()'s tuple shape."""
    try:
        from engine.football._segments import fcs_game_ids, FCS_LEAGUE
        fcs = fcs_game_ids()
    except Exception:
        fcs, FCS_LEAGUE = frozenset(), "cfb_fcs"
    out = []
    for sp, lg, bt, pt, od, res, gk, rid, ln, pd in rows:
        lg = _norm_lg(lg)
        if sp == "football" and lg == "cfb" and fcs and str(gk).rsplit(":", 1)[-1] in fcs:
            lg = FCS_LEAGUE
        nbt = _norm_bt(bt)
        since = (_EVIDENCE_SINCE.get((str(sp), lg, nbt))
                 or _EVIDENCE_SINCE.get((str(sp), lg, _family(nbt))))
        if since and str(pd or "") < since:
            continue
        out.append((sp, lg, bt, pt, od, res, gk, rid, ln))
    return out


_W_PROVEN = 0.50    # eligible bucket: model keeps its current (earned) say
_W_UNPROVEN = 0.15  # unproven/-EV bucket: defer hard to the de-vigged market
TTL = 600          # recompute the map at most every 10 min

_cache = {"t": 0.0, "map": {}}

_UNDER_RE = re.compile(r"\bunder\b", re.I)
_OVER_RE = re.compile(r"\bover\b", re.I)
_SIGN_RE = re.compile(r"([+-])\s?\d")

def _imp(odds):
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0 or abs(o) < 100:
        return None
    return abs(o) / (abs(o) + 100.0) if o < 0 else 100.0 / (o + 100.0)

def _ncdf(z):
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))

def _norm_bt(bt):
    # Canonical label (engine/_bet_type_alias.py, MintyBot 2026-10-25): the trackers carry
    # two label families for the same market ("O/U"/"RL"/"ALT O/U"/"F5 O/U"/"Team Total" vs
    # "TOTAL"/"RUN_LINE"/"ALT_TOTAL"/"F5_TOTAL"/"TEAM_TOTAL"). Keying cells on the raw label
    # split MLB's proven "O/U over" (n=29, +16.9%) and "RL dog" (n=162, +8.5%) away from the
    # new candidates ("TOTAL"/"RUN_LINE" -> n=2 cells -> unproven -> W=0.15) — the proven
    # markets were silently gated OFF from the 10-14/23 relabel until this fix.
    return _canon_bt(bt)

def _norm_lg(lg):
    return str(lg or "").strip().lower()

def _norm_cond(c):
    return str(c or "").strip().lower()

def _condition(pick_text, bet_type, odds):
    """This pick's CONDITION -- the market's other segmentation axis, alongside bet_type:
    'over'/'under' for totals, 'fav'/'dog' for everything with a favorite/underdog side.
    'over'/'under' comes straight from the pick text ("Under 8.5", "DUKE Over 24.5", "P1 Over
    1.5"). 'fav'/'dog' comes from the spread sign IN the pick text when one is printed ("DET
    +1.5" -> dog, "TB -1.5" -> fav, "P1 CAR -0.5" -> fav); when the pick is a moneyline/3-way/
    prop with no line printed (just a team/player name, e.g. "MIL" priced +129, or a 3-way
    "REG CAR" priced -135), it falls back to the PRICED odds' own sign (positive American odds
    = dog, negative = fav) -- still a real fav/dog signal, just carried by the price instead of
    a printed line. `bet_type` is accepted (part of the gate's signature) but unused by this
    heuristic -- every bet_type observed in picks_unified resolves unambiguously off pick_text/
    odds alone; kept for a future bet_type that might need type-specific handling. Returns ''
    only if NEITHER signal is available (e.g. missing/zero odds and no printed line) -- markets
    that genuinely have no two-sided condition aggregate as the whole market, unchanged."""
    pt = str(pick_text or "")
    if _UNDER_RE.search(pt):
        return "under"
    if _OVER_RE.search(pt):
        return "over"
    m = _SIGN_RE.search(pt)
    if m:
        return "dog" if m.group(1) == "+" else "fav"
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return ""
    if o > 0:
        return "dog"
    if o < 0:
        return "fav"
    return ""

def _compute():
    try:
        con = sqlite3.connect("file:" + os.path.abspath(_DB) + "?mode=ro", uri=True)
        rows = con.execute(
            "SELECT sport, league, bet_type, pick_text, odds, result, game_key, id, line, pick_date FROM picks "
            "WHERE result IN ('W','L') AND odds IS NOT NULL ORDER BY id").fetchall()
        con.close()
    except Exception:
        return {}
    rows = _segment_and_window(rows)
    # ONE TRIAL PER GAME / MARKET / SIDE (MintyBot 2026-10-25, Austin PA walkthrough). Every alt
    # rung (PHI +3.5, +4, +4.5 ...) and every re-recorded line move of the same pick used to count
    # as an independent settled bet, so a handful of games could carry a cell to n>=STD_N and a
    # ladder where every rung won looked like a proven market: nba ALT_TOTAL/under read +31% on
    # 23 rows but -12% on its 13 actual games (same verdict under max-edge / median / first-
    # recorded rung selection). Keep the rung nearest the family's main line (median by line,
    # tie -> first recorded) so n counts GAMES and the ROI is the model's central lean, not the
    # ladder's shape. Cell keys are unchanged -> is_eligible/market_weight callers unaffected.
    _groups = {}
    for r in rows:
        sp, lg, bt, pt, od, res, gk, rid, ln = r
        cond = _norm_cond(_condition(pt, bt, od))
        _groups.setdefault((str(sp), _norm_lg(lg), str(gk), _norm_bt(bt), cond), []).append(r)
    def _line_of(r):
        try:
            if r[8] is not None:
                return float(r[8])
        except (TypeError, ValueError):
            pass
        m = re.findall(r"-?\d+\.?\d*", str(r[3] or ""))
        return float(m[-1]) if m else 0.0
    rows = [sorted(g, key=lambda r: (_line_of(r), r[7]))[len(g) // 2] for g in _groups.values()]
    agg = {}
    for sp, lg, bt, pt, od, res, _gk, _rid, _ln in rows:
        imp = _imp(od)
        if imp is None:
            continue
        win = (res == "W")
        cond = _norm_cond(_condition(pt, bt, od))
        k = (str(sp), _norm_lg(lg), _norm_bt(bt), cond)
        a = agg.setdefault(k, [0, 0, 0.0, 0.0])
        a[0] += 1
        a[1] += 1 if win else 0
        a[2] += imp
        o = float(od)
        a[3] += (o / 100.0 if o > 0 else 100.0 / abs(o)) if win else -1.0
    out = {}
    for (sp, lg, bt, cond), (n, w, si, pnl) in agg.items():
        be = si / n
        roi = 100.0 * pnl / n
        A = be * PRIOR_N + w
        B = (1.0 - be) * PRIOR_N + (n - w)
        mean = A / (A + B)
        var = A * B / ((A + B) ** 2 * (A + B + 1.0))
        se = var ** 0.5
        conf = _ncdf((mean - be) / se) if se > 0 else 0.0
        _floor = _CONF_FLOORS.get((sp, lg, bt, cond), MIN_CONF)
        elig = roi > MIN_ROI and conf >= _floor and (n >= STD_N or (n >= FAST_N and conf >= FAST_CONF))
        out[(sp, lg, bt, cond)] = {"n": n, "win": 100.0 * w / n, "roi": roi,
                                   "conf": conf, "eligible": bool(elig)}
    return out

def _map():
    if (time.time() - _cache["t"]) > TTL or not _cache["map"]:
        mm = _compute()
        if mm:
            _cache["map"] = mm
            _cache["t"] = time.time()
    return _cache["map"]

def is_eligible(sport, league, bet_type, condition):
    """True if this (sport, league, bet_type, condition) market SIDE has PROVEN edge ->
    headline-eligible. `condition` is the pick's fav/dog/over/under side -- see _condition()."""
    return bool((_map().get((str(sport), _norm_lg(league), _norm_bt(bet_type), _norm_cond(condition))) or {}).get("eligible"))

def market_weight(sport, league, bet_type, pick_text="", odds=None):
    """Per-bucket market-anchor weight. A bucket with PROVEN edge (is_eligible)
    keeps the model's full say (_W_PROVEN); everything unproven defers hard to the
    market (_W_UNPROVEN). condition is derived from pick_text/odds exactly like the gate."""
    cond = _norm_cond(_condition(pick_text, bet_type, odds))
    cell = _map().get((str(sport), _norm_lg(league), _norm_bt(bet_type), cond))
    return _W_PROVEN if (cell and cell.get("eligible")) else _W_UNPROVEN

def market_stats(sport, league, bet_type, condition):
    return _map().get((str(sport), _norm_lg(league), _norm_bt(bet_type), _norm_cond(condition)))

def eligible_markets():
    return {k: v for k, v in _map().items() if v.get("eligible")}

def refresh():
    _cache["t"] = 0.0
    return _map()
