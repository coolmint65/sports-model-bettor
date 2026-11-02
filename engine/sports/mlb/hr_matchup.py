# -*- coding: utf-8 -*-
"""Matchup-adjusted batter home-run probability model (MLB).

The existing prop pipeline (engine.sports.mlb.player_mc) prices Batter HR from
the batter's OWN game-log HR distribution -- batter-only, blind to the matchup.
The market prices batters roughly right in aggregate; the soft spots are STACKED
matchups it doesn't fully compound. This layers those onto the batter baseline:

    p_pa   = batter_hr_rate(regressed) * pitcher_factor * park_factor * platoon
    P(>=1 HR in the game) = 1 - (1 - p_pa) ** expected_PA

Every factor is relative-to-league so it multiplies cleanly. Read-only, paper:
computes edge vs the DE-VIGGED market price. Never places.
"""
from __future__ import annotations

import os
import sqlite3
import unicodedata

_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))), "data", "mlb.db")

# Bayesian shrinkage: how many league-average PAs / IP to blend into a thin
# sample before we trust the player's own rate.
_PRIOR_PA = 100        # shrinkage strength (lower = trust the hitter's own rate
                       # more; 250 over-inflated light hitters toward league)
_PRIOR_IP = 40         # ~ a starter's month
# Per-PA HR probability is capped -- no hitter homers in >~12% of PAs even in
# the best matchup; anything higher is a data/park artifact, not signal.
_MAX_P_PA = 0.12
# Platoon: hitters gain vs opposite-handed pitching, lose vs same-hand.
_PLATOON_OPP = 1.08
_PLATOON_SAME = 0.93
# Expected PAs by batting-order slot (1-9); leadoff sees the most.
_PA_BY_SLOT = {1: 4.6, 2: 4.5, 3: 4.4, 4: 4.3, 5: 4.1,
               6: 4.0, 7: 3.9, 8: 3.8, 9: 3.7}
_PA_DEFAULT = 4.1


def _norm(s: str) -> str:
    s = "".join(c for c in unicodedata.normalize("NFKD", s or "")
                if not unicodedata.combining(c))
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(f"file:{_DB}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c


def league_baselines(conn) -> dict:
    """League HR/PA and HR/9 for the most recent COMPLETE season.

    NOTE: the in-season (current-year) batter_stats sync is stale -- it freezes
    a couple weeks in (max PA ~60 in August). A HR-rate baseline needs full
    samples, so we anchor on the last season whose data is complete (max PA >
    300). Fixing the current-season sync would let this blend recent form.
    """
    row = conn.execute(
        "SELECT season FROM batter_stats GROUP BY season "
        "HAVING MAX(plate_appearances) > 300 ORDER BY season DESC LIMIT 1"
    ).fetchone()
    season = (row[0] if row
              else conn.execute("SELECT MAX(season) FROM batter_stats").fetchone()[0])
    b = conn.execute(
        "SELECT SUM(home_runs) hr, SUM(plate_appearances) pa "
        "FROM batter_stats WHERE season=? AND plate_appearances>0",
        (season,)).fetchone()
    hr_per_pa = (b["hr"] / b["pa"]) if b and b["pa"] else 0.032
    p = conn.execute(
        "SELECT SUM(home_runs) hr, SUM(innings) ip "
        "FROM pitcher_stats WHERE season=? AND innings>0",
        (season,)).fetchone()
    hr_per_9 = (p["hr"] * 9.0 / p["ip"]) if p and p["ip"] else 1.25
    # iso -> HR/PA linear fit. iso (isolated power) is the only populated power
    # metric -- Statcast barrel/xSLG aren't synced. Shrinking a batter's own
    # HR/PA toward this iso-implied rate (instead of the league mean) is what
    # stops light hitters inflating toward average. Fit over the two most
    # recent complete seasons; R^2 ~0.9.
    fr = conn.execute(
        "SELECT home_runs hr, plate_appearances pa, iso FROM batter_stats "
        "WHERE season IN (?, ?) AND plate_appearances>=200 AND iso IS NOT NULL",
        (season, season - 1)).fetchall()
    iso_a, iso_b = -0.0118, 0.2658
    if len(fr) >= 50:
        xs = [r["iso"] for r in fr]
        ys = [r["hr"] / r["pa"] for r in fr]
        nx = len(xs)
        mx = sum(xs) / nx
        my = sum(ys) / nx
        denom = sum((x - mx) ** 2 for x in xs)
        if denom > 0:
            iso_b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom
            iso_a = my - iso_b * mx
    return {"season": season, "hr_per_pa": hr_per_pa, "hr_per_9": hr_per_9,
            "iso_a": iso_a, "iso_b": iso_b}


def batter_hr_rate(conn, player_id: int, base: dict) -> float | None:
    """Regressed HR-per-PA for a batter, shrunk toward the batter's OWN
    iso-implied rate (power-aware, PA-weighted). Aggregated over the last TWO
    complete seasons -- one partial season's small sample cuts both ways (a
    92-PA hot streak inflates a light hitter; an injury-shortened year deflates
    a slugger), and pooling smooths it."""
    rows = conn.execute(
        "SELECT home_runs hr, plate_appearances pa, iso FROM batter_stats "
        "WHERE player_id=? AND season IN (?, ?) AND plate_appearances>0",
        (player_id, base["season"], base["season"] - 1)).fetchall()
    if not rows:
        return None
    hr = sum(r["hr"] or 0 for r in rows)
    pa = sum(r["pa"] or 0 for r in rows)
    if pa <= 0:
        return None
    iso_rows = [(r["iso"], r["pa"]) for r in rows if r["iso"] is not None and r["pa"]]
    if iso_rows:
        iso = sum(v * w for v, w in iso_rows) / sum(w for _, w in iso_rows)
        prior = max(0.003, base["iso_a"] + base["iso_b"] * iso)
    else:
        prior = base["hr_per_pa"]      # no iso -> fall back to league mean
    return (hr + prior * _PRIOR_PA) / (pa + _PRIOR_PA)


def pitcher_hr_factor(conn, pitcher_id: int, base: dict) -> float:
    """Pitcher HR-allowed relative to league (regressed). 1.0 = average."""
    r = conn.execute(
        "SELECT hr_per_9, innings, home_runs FROM pitcher_stats "
        "WHERE player_id=? AND season=?", (pitcher_id, base["season"])).fetchone()
    if not r:
        return 1.0
    ip = r["innings"] or 0
    hr = r["home_runs"] or 0
    lg9 = base["hr_per_9"]
    # regress the pitcher's own HR/9 toward league by IP
    reg_hr9 = (hr * 9.0 + lg9 * _PRIOR_IP) / (ip + _PRIOR_IP) if (ip + _PRIOR_IP) else lg9
    return max(0.5, min(1.8, reg_hr9 / lg9)) if lg9 else 1.0


def park_hr_factor(conn, team_id: int, base: dict) -> float:
    r = conn.execute(
        "SELECT hr_factor FROM park_factors WHERE team_id=? "
        "ORDER BY season DESC LIMIT 1", (team_id,)).fetchone()
    return float(r["hr_factor"]) if r and r["hr_factor"] else 1.0


def platoon_factor(bats: str | None, throws: str | None) -> float:
    if not bats or not throws:
        return 1.0
    if bats == "S":                       # switch hitter always bats opposite
        return _PLATOON_OPP
    return _PLATOON_OPP if bats != throws else _PLATOON_SAME


def weather_factor(wind: str | None, temp) -> float:
    """Rough HR weather multiplier. Warm air + wind blowing OUT carries balls;
    cold + wind IN kills them. ~+0.4%/degF over 70; wind out ~+1.2%/mph (capped)."""
    f = 1.0
    try:
        f *= 1.0 + max(-0.15, min(0.15, (float(temp) - 70.0) * 0.004))
    except (TypeError, ValueError):
        pass
    if wind:
        import re
        w = wind.lower()
        m = re.search(r"(\d+)\s*mph", w)
        mph = int(m.group(1)) if m else 0
        if "out" in w:
            f *= 1.0 + min(0.20, mph * 0.012)
        elif "in" in w:
            f *= 1.0 - min(0.18, mph * 0.011)
    return max(0.75, min(1.30, f))


def p_hr(conn, batter_id: int, pitcher_id: int | None, park_team_id: int | None,
         *, bats=None, throws=None, slot=None, base=None,
         wind=None, temp=None) -> dict | None:
    """Matchup-adjusted P(>=1 HR) for a batter this game."""
    base = base or league_baselines(conn)
    br = batter_hr_rate(conn, batter_id, base)
    if br is None:
        return None
    pf = pitcher_hr_factor(conn, pitcher_id, base) if pitcher_id else 1.0
    kf = park_hr_factor(conn, park_team_id, base) if park_team_id else 1.0
    tf = platoon_factor(bats, throws)
    wf = weather_factor(wind, temp)
    p_pa = min(_MAX_P_PA, br * pf * kf * tf * wf)
    pa = _PA_BY_SLOT.get(slot, _PA_DEFAULT)
    p = 1.0 - (1.0 - p_pa) ** pa
    return {"p_hr": p, "p_pa": p_pa, "batter_rate": br,
            "pitcher_factor": pf, "park_factor": kf, "platoon": tf,
            "weather": wf, "expected_pa": pa}


def price_todays_props(conn, date: str, *, min_edge: float | None = None) -> list:
    """Resolve + price today's 1+HR 'Batter HR' props (read-only, paper).
    Returns priced rows sorted by edge desc; caller filters by edge/lineup."""
    import json
    from scrapers.hardrock_props import fetch_mlb_props
    base = league_baselines(conn)
    pidx, throws = {}, {}
    for r in conn.execute("SELECT mlb_id,name,team_id,bats,throws FROM players "
                          "WHERE mlb_id IS NOT NULL"):
        pidx[_norm(r["name"])] = (r["mlb_id"], r["team_id"], r["bats"])
        throws[r["mlb_id"]] = r["throws"]
    teamctx = {}
    for g in conn.execute(
            "SELECT home_team_id ht,away_team_id at,home_pitcher_id hp,"
            "away_pitcher_id ap,weather_wind ww,weather_temp wt FROM games "
            "WHERE date>=? AND (lower(status)='scheduled' "
            "OR lower(status) LIKE 'pre%')", (date,)):    # PRE-GAME only
        teamctx[g["ht"]] = {"opp": g["ap"], "park": g["ht"], "wind": g["ww"], "temp": g["wt"]}
        teamctx[g["at"]] = {"opp": g["hp"], "park": g["ht"], "wind": g["ww"], "temp": g["wt"]}
    slot = {}
    for ls in conn.execute("SELECT home_lineup,away_lineup FROM lineup_snapshots "
                           "WHERE date>=?", (date,)):
        for lu in (ls["home_lineup"], ls["away_lineup"]):
            try:
                ids = json.loads(lu) if lu else []
            except Exception:
                ids = []
            for i, pid in enumerate(ids):
                slot[int(pid)] = i + 1
    out = []
    for lst in fetch_mlb_props().values():
        for p in lst:
            if p.get("bet_type") != "Batter HR" or (p.get("line") or 0.5) != 0.5:
                continue
            who = pidx.get(_norm(p.get("player_name", "")))
            if not who:
                continue
            bmlb, team, bats = who
            if bmlb not in slot:        # GUARDRAIL: only confirmed-lineup bats
                continue
            ctx = teamctx.get(team)
            if not ctx:
                continue
            res = p_hr(conn, bmlb, ctx["opp"], ctx["park"], bats=bats,
                       throws=throws.get(ctx["opp"]), slot=slot.get(bmlb),
                       base=base, wind=ctx.get("wind"), temp=ctx.get("temp"))
            if not res:
                continue
            fair = devig_over(p.get("over_odds"), p.get("under_odds"))
            if fair is None:
                continue
            edge = (res["p_hr"] - fair) * 100.0
            if min_edge is not None and edge < min_edge:
                continue
            out.append({"player_name": p.get("player_name"), "mlb_id": bmlb,
                        "odds": p.get("over_odds"), "our_p": round(res["p_hr"], 4),
                        "market_p": round(fair, 4), "edge": round(edge, 2),
                        "factors": res})
    out.sort(key=lambda r: -r["edge"])
    return out


# ── market / edge ───────────────────────────────────────────────

def _implied(odds) -> float | None:
    try:
        o = int(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    return (100.0 / (o + 100.0)) if o > 0 else (abs(o) / (abs(o) + 100.0))


def devig_over(over_odds, under_odds) -> float | None:
    """Two-way no-vig fair probability of the OVER (to hit >=1 HR)."""
    io, iu = _implied(over_odds), _implied(under_odds)
    if io is None:
        return None
    if iu is None:
        return io
    tot = io + iu
    return io / tot if tot > 0 else io
