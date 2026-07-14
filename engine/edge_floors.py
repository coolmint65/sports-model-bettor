"""
Data-driven per-cell edge floors.

Replaces static MAIN_EDGE_FLOOR (4% ML / 12% TOTAL) with floors
derived from realized tracker ROI per (sport, bet_type, direction,
edge_tier). The textbook 4%/12% applies a single threshold across
all markets — but the QA snapshot 2026-05-01 showed MLB Unders at
"clearing the 4% floor" still bled -40% ROI, while Overs at the
same nominal edge profit +36%. The floor for Unders should be
higher than for Overs, period.

How it works
------------

For each (sport, bet_type, direction) cell, walk settled picks and
compute ROI per edge tier:
  - low:   0-3%
  - mid:   3-7%
  - high:  7-15%
  - xhigh: 15%+

The floor for the cell = the LOWEST edge tier where realized ROI
crossed ``MIN_PROFITABLE_ROI`` (default +3%). If no tier crosses,
the cell is marked "noplay" and picks are rejected outright.

Refresh nightly via the sync scripts. Picks engines call
``required_edge(sport, bet_type, direction)`` at filter time as an
override on top of the static MAIN_EDGE_FLOOR floor — whichever is
higher gates the pick.

Persistence
-----------
``data/edge_floors.json`` shape::

    {
      "mlb": {
        "ML|home":   {"floor": 4.0, "tier": "mid",   "n": 20, "roi": +6.97},
        "O/U|under": {"floor": 99.0, "tier": "noplay", "n": 13, "roi": -40.22}
      },
      ...
    }

Floor 99.0 means "do not play this cell" — the gate effectively
bans picks rather than accepting any edge. Better than chasing a
losing market with a tighter floor that won't actually save it.

Relationship with empirical_calibration + blend_calibration
-----------------------------------------------------------

empirical_calibration shrinks the model_prob within the bucket
(direction-aware shrinkage). blend_calibration applies a sport-wide
isotonic on top. By the time we compute edge, the prob is already
calibrated. Edge floors here are the FINAL gate — even after both
calibration layers, some markets just consistently lose money for
us. We refuse them.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)


_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "edge_floors.json",
)
_CACHE: dict[str, dict[str, dict]] = {}
_CACHE_LOADED = False

# Edge tier definitions — same boundaries as the existing autopsy
# bucketer so the dashboards line up.
_EDGE_TIERS = [
    ("low",   0.0,  3.0),
    ("mid",   3.0,  7.0),
    ("high",  7.0, 15.0),
    ("xhigh", 15.0, 999.0),
]

# Minimum tier sample size before we trust the ROI to derive a floor.
# Below this, fall back to static floor — small samples are noisy.
# Raised 10 → 15 on 2026-06-20 after auditing the NOPLAY system:
# 24 of 39 active cells (62%) were banned, several on n=5-8 samples
# that turned out to be coin-flip noise. Pushing to 15 means cells
# need a more honest sample before getting the permanent-ban tier
# treatment; n=5-14 cells get the marginal floor instead.
MIN_TIER_N = 15

# Floor "passes" when realized ROI is above this. +3% gives a margin
# above break-even (roughly +1pp accounting for variance + juice).
MIN_PROFITABLE_ROI = 3.0

# Floor for marginal cells (ROI between the profitability threshold
# and 0). Discourage but don't ban — picks fire only on fat edges,
# model keeps learning.
MARGINAL_CELL_FLOOR = 15.0

# Bleed-cell floor (ROI below profitability threshold). Requires a
# meaningful edge before a pick fires, but does NOT hard-ban the cell
# — stake_multiplier below scales the size down. 2026-07-03: user
# directive to remove NOPLAY entirely. Reason: hard bans stop new
# data from proving the model recovered when we do fix it. Every cell
# stays alive; catastrophic bleeders just bet small until they earn
# their stake back.
BLEED_CELL_FLOOR = 20.0


def stake_multiplier_for_roi(roi_pct: float) -> float:
    """Continuous stake-size scale factor from realized ROI.

    Replaces the old NOPLAY on/off gate. A cell bleeding at -30% ROI
    used to emit shadow picks (stake=0); it now emits real picks at
    ~10% normal size. Data continues to flow, so if the model gets
    fixed the multiplier auto-recovers on the next floor refit.

    Piecewise linear anchors:

        ROI >= +5%  → 1.00 (full stake)
        ROI =   0%  → 0.60
        ROI = -10%  → 0.30
        ROI = -30%  → 0.10
        ROI <= -30% → 0.10 (floor — never zero)
    """
    if roi_pct >= 5.0:
        return 1.0
    if roi_pct >= 0.0:
        return 0.60 + (roi_pct / 5.0) * 0.40
    if roi_pct >= -10.0:
        return 0.30 + ((roi_pct + 10.0) / 10.0) * 0.30
    if roi_pct >= -30.0:
        return 0.10 + ((roi_pct + 30.0) / 20.0) * 0.20
    return 0.10

# Per-sport cutoff dates live in engine.model_cutoffs (shared with
# tracker_autopsy / clv_diagnostics so a single edit moves all the
# diagnostics together). Kept as a local alias for any external
# importers of `_MODEL_CUTOFFS`.
from .model_cutoffs import MODEL_CUTOFFS as _MODEL_CUTOFFS


# ── Tier mapping ──────────────────────────────────────────────

def _tier_for(edge: float) -> str:
    for label, lo, hi in _EDGE_TIERS:
        if lo <= edge < hi:
            return label
    return _EDGE_TIERS[-1][0]


def _tier_floor(label: str) -> float:
    for tlabel, lo, _hi in _EDGE_TIERS:
        if tlabel == label:
            return lo
    return 0.0


# ── Pick directionality (mirrors empirical_calibration) ──────

def _direction_label(bet_type: str, pick_text: str | None) -> str:
    """Same direction logic empirical_calibration uses. Over/Under for
    totals, NRFI/YRFI for first-inning, fav/dog for spread-style
    markets (sign on the spread number), '' otherwise."""
    if not pick_text:
        return ""
    pk = pick_text.strip().lower()
    bt = (bet_type or "").strip().lower()
    if pk.startswith("over"):
        return "over"
    if pk.startswith("under"):
        return "under"
    if bt in ("1st inn", "1stinn", "nrfi"):
        if "nrfi" in pk:
            return "nrfi"
        if "yrfi" in pk:
            return "yrfi"
    # Spread-style markets (RL / PL / SPREAD / ALT *): direction is
    # the sign of the spread number in the pick text. "BOS -1.5" =
    # fav (laying), "BOS +1.5" = dog (taking points). Realized ROI
    # diverges meaningfully between these two — treating them as the
    # same bucket masks where the model is actually profitable.
    if bt in ("rl", "pl", "spread", "alt rl", "alt pl", "alt spread"):
        # Find the spread number's sign (first +/- followed by digits).
        import re as _re
        m = _re.search(r"([+-])\d", pk)
        if m:
            return "fav" if m.group(1) == "-" else "dog"
    return ""


# ── Per-sport DB ──────────────────────────────────────────────

_SOURCES = {
    # (db_module, picks_table) — top-3 sports use a sport-wide DB.
    "mlb": ("engine.db",     "picks"),
    "nhl": ("engine.nhl_db", "nhl_picks"),
    "nba": ("engine.nba_db", "nba_picks"),
    # Tennis added 2026-05-25 (V3 Phase B). The tracker showed Tennis
    # TOTAL_GAMES bleeding at -14.3% ROI over 94 picks with no edge-
    # floor coverage. Same compute_floors logic applies — Bayesian
    # bucket realized-ROI per (bet_type, direction) cell.
    "tennis": ("engine.tennis_db", "tennis_picks"),
    # Basketball framework leagues use a per-league DB via
    # ``engine.basketball._db.get_conn(league)``. Add the league key
    # as a third tuple element so ``_load_settled`` knows to pass it
    # through. AFL added 2026-05-28 (task #463) — historical ROI on
    # 29 settled picks finally cleared the small-sample bypass
    # threshold built for tennis. Adding more basketball leagues here
    # is a one-line append once they accumulate ≥10 settled picks.
    "afl":      ("engine.basketball._db", "picks", "afl"),
    "wnba":     ("engine.basketball._db", "picks", "wnba"),
    "ncaam":    ("engine.basketball._db", "picks", "ncaam"),
    "ncaaw":    ("engine.basketball._db", "picks", "ncaaw"),
    "euroleague": ("engine.basketball._db", "picks", "euroleague"),
    "nba_summer_league": ("engine.basketball._db", "picks", "nba_summer_league"),
    # Soccer framework leagues — same per-league DB pattern as
    # basketball. 2026-06-30 audit found soccer picks at 30%+ edge
    # bleed at 16% WR (-54% ROI) with no floor coverage. The floor
    # refit picks off the bleed cells per (bet_type, direction) once
    # each league accumulates enough settled picks. Every soccer
    # league gets an entry so the floor system can operate uniformly;
    # cold leagues just return an empty settled set until picks land.
    "fifa_world_cup":    ("engine.soccer._db", "picks", "fifa_world_cup"),
    "fifa_internationals": ("engine.soccer._db", "picks", "fifa_internationals"),
    "eng_premier":       ("engine.soccer._db", "picks", "eng_premier"),
    "esp_laliga":        ("engine.soccer._db", "picks", "esp_laliga"),
    "ita_seriea":        ("engine.soccer._db", "picks", "ita_seriea"),
    "ger_bundesliga":    ("engine.soccer._db", "picks", "ger_bundesliga"),
    "fra_ligue1":        ("engine.soccer._db", "picks", "fra_ligue1"),
    "usa_mls":           ("engine.soccer._db", "picks", "usa_mls"),
    "bra_seriea":        ("engine.soccer._db", "picks", "bra_seriea"),
    "arg_lpf":           ("engine.soccer._db", "picks", "arg_lpf"),
    "conmebol_libertadores": ("engine.soccer._db", "picks", "conmebol_libertadores"),
    "uefa_champions":    ("engine.soccer._db", "picks", "uefa_champions"),
    "uefa_europa":       ("engine.soccer._db", "picks", "uefa_europa"),
    "uefa_conference":   ("engine.soccer._db", "picks", "uefa_conference"),
    "nwsl":              ("engine.soccer._db", "picks", "nwsl"),
    "usl_championship":  ("engine.soccer._db", "picks", "usl_championship"),
    "usa_open_cup":      ("engine.soccer._db", "picks", "usa_open_cup"),
    # Motorsports series — same shape as basketball framework leagues:
    # per-series `picks` table with (bet_type, edge, result, profit).
    # F1 already had implicit coverage via the picks_core path but
    # never registered here for automated floor refits. NASCAR + IndyCar
    # onboard fresh with beta status — floors self-populate as picks
    # settle. NASCAR's variance (restrictor plate lottery + fuel
    # strategy noise) is exactly the pattern edge_floors was built for.
    "f1":                ("engine.motorsports._db", "picks", "f1"),
    "nascar":            ("engine.motorsports._db", "picks", "nascar"),
    "indycar":           ("engine.motorsports._db", "picks", "indycar"),
}


def _load_settled(sport: str, since: str | None = None) -> list[dict]:
    src = _SOURCES.get(sport)
    if not src:
        return []
    db_module_name = src[0]
    table = src[1]
    league_arg = src[2] if len(src) >= 3 else None
    try:
        import importlib
        mod = importlib.import_module(db_module_name)
        conn = (mod.get_conn(league_arg) if league_arg is not None
                else mod.get_conn())
    except Exception as e:
        logger.warning("edge_floors: cannot open %s: %s", db_module_name, e)
        return []
    where = ["result IN ('W', 'L', 'P')"]
    params: list = []
    if since:
        where.append("date >= ?")
        params.append(since)
    sql = (f"SELECT bet_type, pick, edge, result, profit, odds "
           f"FROM {table} WHERE {' AND '.join(where)}")
    try:
        rows = conn.execute(sql, params).fetchall()
    except Exception as e:
        logger.warning("edge_floors: query on %s failed: %s", table, e)
        rows = []
    out = [dict(r) for r in rows]

    # Also fold in settled player-prop picks. Same schema (bet_type =
    # "Player PRA"/"Player Steals"/..., pick = "Over X.X"/"Under X.X"),
    # different table. Without this, NBA Player PRA could bleed -49%
    # ROI forever without the auto-derived floor catching it because
    # compute_floors only saw the game-line picks table.
    try:
        pp_where = list(where)
        pp_sql = (f"SELECT bet_type, pick, edge, result, profit, odds "
                   f"FROM player_props_picks "
                   f"WHERE {' AND '.join(pp_where)}")
        prop_rows = conn.execute(pp_sql, params).fetchall()
        out.extend(dict(r) for r in prop_rows)
    except Exception:
        # player_props_picks may not exist for all sports — quietly
        # skip when the table isn't there.
        pass
    return out


# ── Floor derivation ──────────────────────────────────────────

def compute_floors(sport: str, since: str | None = None) -> dict:
    """Walk settled picks, group by (bet_type, direction, edge_tier),
    compute ROI per tier, derive the minimum-clearing tier as the
    cell's floor.

    ``since`` defaults to the per-sport model-stack cutoff so pre-MC
    picks don't contaminate. Pass an explicit date to override (e.g.
    backfill scenarios).

    Returns the per-(bet_type, direction) floor dict for ``sport``.
    """
    if since is None:
        since = _MODEL_CUTOFFS.get(sport)
    rows = _load_settled(sport, since=since)
    by_cell: dict[tuple, dict] = defaultdict(
        lambda: {tier[0]: {"n": 0, "profit": 0.0, "w": 0, "l": 0}
                  for tier in _EDGE_TIERS}
    )
    for r in rows:
        bt = (r.get("bet_type") or "").strip()
        if not bt:
            continue
        direction = _direction_label(bt, r.get("pick"))
        edge = float(r.get("edge") or 0.0)
        if edge <= 0:
            continue
        tier = _tier_for(edge)
        cell = by_cell[(bt, direction)][tier]
        cell["n"] += 1
        cell["profit"] += float(r.get("profit") or 0)
        if r.get("result") == "W":
            cell["w"] += 1
        elif r.get("result") == "L":
            cell["l"] += 1

    floors: dict[str, dict] = {}
    for (bt, direction), tiers in by_cell.items():
        # Find the LOWEST tier where ROI clears the threshold AND has
        # min sample. Tiers walked low-to-high.
        chosen_tier = None
        chosen_roi = None
        chosen_n = 0
        for tier_label, _lo, _hi in _EDGE_TIERS:
            t = tiers[tier_label]
            n = t["n"]
            if n < MIN_TIER_N:
                continue
            roi = (t["profit"] / n) if n else 0.0
            if roi >= MIN_PROFITABLE_ROI:
                chosen_tier = tier_label
                chosen_roi = roi
                chosen_n = n
                break
        cell_key = f"{bt}|{direction}" if direction else bt
        if chosen_tier is None:
            # No profitable tier found. Under the pre-2026-07-03 logic
            # this cell would have been NOPLAY-banned; the user asked
            # for that removed because hard bans stop the model from
            # ever proving itself recovered. Instead: keep the cell
            # live at a high edge floor + a stake multiplier scaled by
            # realized ROI. Bleeders bet at ~10% of Kelly size; if the
            # model gets fixed, ROI improves on the next refit and the
            # multiplier auto-scales back up.
            total_n = sum(t["n"] for t in tiers.values())
            total_profit = sum(t["profit"] for t in tiers.values())
            overall_roi = (total_profit / total_n if total_n else 0.0)
            any_tier_with_data = any(t["n"] >= MIN_TIER_N
                                       for t in tiers.values())
            cell_has_evidence = (any_tier_with_data
                                  or total_n >= MIN_TIER_N)
            if not cell_has_evidence:
                # Thin sample → skip; use static fallback via caller.
                continue
            if overall_roi < 0.0:
                # Bleeder: require fat edge + reduce stake.
                floor_val = BLEED_CELL_FLOOR
                tier_label = "bleeder"
            else:
                floor_val = MARGINAL_CELL_FLOOR
                tier_label = "marginal"
            floors[cell_key] = {
                "floor": floor_val,
                "tier": tier_label,
                "n": total_n,
                "roi": round(overall_roi, 2),
                "stake_mult": round(
                    stake_multiplier_for_roi(overall_roi), 3),
            }
        else:
            floors[cell_key] = {
                "floor": _tier_floor(chosen_tier),
                "tier": chosen_tier,
                "n": chosen_n,
                "roi": round(chosen_roi, 2),
                "stake_mult": round(
                    stake_multiplier_for_roi(chosen_roi), 3),
            }

    _persist(sport, floors)
    return floors


def compute_all_sports(since: str | None = None) -> dict:
    return {s: compute_floors(s, since=since) for s in _SOURCES}


# ── Inference-time API ────────────────────────────────────────

def required_edge(sport: str, bet_type: str,
                   pick_text: str | None = None,
                   default: float = 4.0) -> float:
    """Return the minimum edge a pick must clear for ``sport`` /
    ``bet_type`` / direction. Falls back to ``default`` (the static
    floor the caller would otherwise use) when no derived floor is
    on file.

    When data IS on file we trust it — even when the data-derived
    floor is below the static default. Earlier policy raised the
    floor to max(default, data) which contradicted the whole point
    of running the floor backtest: if the data shows MLB ML at 3%
    runs +47% ROI on n≥10 settled picks, requiring 4% just because
    "the textbook says so" suppresses real picks for no benefit.

    Picks engines call this AFTER calibration but BEFORE the
    final filter. ``NOPLAY_FLOOR`` (99.0) means the cell is banned —
    that path still wins over default.
    """
    _ensure_loaded()
    sport_table = _CACHE.get(sport) or {}
    direction = _direction_label(bet_type, pick_text)
    cell_key = f"{bet_type}|{direction}" if direction else bet_type
    entry = sport_table.get(cell_key)
    if entry is None:
        # Try without direction (some bet types don't carry one)
        entry = sport_table.get(bet_type)
    if entry is None:
        return default
    return float(entry.get("floor", default))


def is_noplay(sport: str, bet_type: str,
               pick_text: str | None = None) -> bool:
    """Kept for API compat with older imports. NOPLAY was retired
    2026-07-03; every cell is now live at a scaled stake. Always
    returns False so any remaining shadow-mode gate short-circuits
    cleanly."""
    return False


def stake_multiplier(sport: str, bet_type: str,
                      pick_text: str | None = None,
                      default: float = 1.0) -> float:
    """Multiplicative stake scale from the cell's realized ROI.

    Multiplied against the Kelly-recommended stake in picks_core so
    catastrophic bleeders bet ~10% of Kelly instead of getting hard-
    banned. Cells with no cell record use ``default`` (1.0 — full
    Kelly)."""
    _ensure_loaded()
    sport_table = _CACHE.get(sport) or {}
    direction = _direction_label(bet_type, pick_text)
    cell_key = f"{bet_type}|{direction}" if direction else bet_type
    entry = sport_table.get(cell_key) or sport_table.get(bet_type)
    if entry is None:
        return default
    mult = entry.get("stake_mult")
    if mult is None:
        # Legacy row without stake_mult — derive it now so old JSONs
        # still enforce the scaled sizing on load.
        roi = float(entry.get("roi", 0.0))
        mult = stake_multiplier_for_roi(roi)
    return float(mult)


# ── Persistence ───────────────────────────────────────────────

def _persist(sport: str, floors: dict) -> None:
    _ensure_loaded()
    _CACHE[sport] = floors
    os.makedirs(os.path.dirname(_PATH), exist_ok=True)
    with open(_PATH, "w", encoding="utf-8") as f:
        json.dump(_CACHE, f, indent=2, sort_keys=True)


def _ensure_loaded() -> None:
    global _CACHE_LOADED
    if _CACHE_LOADED:
        return
    _CACHE_LOADED = True
    if not os.path.exists(_PATH):
        return
    try:
        with open(_PATH, "r", encoding="utf-8") as f:
            blob = json.load(f) or {}
    except Exception as e:
        logger.warning("edge_floors load failed: %s", e)
        return
    if isinstance(blob, dict):
        _CACHE.update(blob)


def reload() -> None:
    global _CACHE_LOADED
    _CACHE.clear()
    _CACHE_LOADED = False
    _ensure_loaded()


# ── CLI ───────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="engine.edge_floors")
    ap.add_argument("--sport", choices=tuple(_SOURCES.keys()) + ("all",),
                    default="all")
    ap.add_argument("--since", type=str, default=None,
                    help="YYYY-MM-DD lower bound on settled date.")
    args = ap.parse_args(argv)

    if args.sport == "all":
        floors = compute_all_sports(since=args.since)
    else:
        floors = {args.sport: compute_floors(args.sport, since=args.since)}

    print()
    for sport, cells in floors.items():
        print(f"  == {sport.upper()} edge floors ==")
        if not cells:
            print(f"    (no cells passed sample threshold)")
            continue
        # Sort by floor descending (harshest gate first)
        sorted_cells = sorted(
            cells.items(),
            key=lambda kv: (-kv[1]["floor"], kv[0]),
        )
        for cell_key, info in sorted_cells:
            mult = info.get("stake_mult")
            mult_txt = f"×{mult:.2f}" if mult is not None else "  —"
            print(f"    {cell_key:<28s}  floor={info['floor']:>5.1f}%  "
                  f"stake={mult_txt:>6s}  "
                  f"tier={info['tier']:<8s}  n={info['n']:>3}  "
                  f"roi={info['roi']:>+6.2f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())


__all__ = [
    "compute_floors", "compute_all_sports", "required_edge",
    "is_noplay", "stake_multiplier", "stake_multiplier_for_roi",
    "reload", "MIN_TIER_N", "MIN_PROFITABLE_ROI",
]
