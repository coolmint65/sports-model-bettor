"""
Factor ablation backtest.

For each toggleable factor, re-runs predict_matchup on historical settled
games with the factor OFF, compares to the baseline (all factors ON), and
reports the WR/ROI delta.

Use this BEFORE enabling any new factor to prove it actually lifts
performance on held-out data. No more intuition-driven factor stacking.

Usage:
    python -m engine.factor_backtest mlb
    python -m engine.factor_backtest mlb --factor MLB_ENABLE_COORS_BOOST
    python -m engine.factor_backtest mlb --limit 100

Caveats:
  - Requires settled picks in the target DB (result IS NOT NULL).
  - Uses the game metadata stored on each pick row to re-predict.
    If the underlying stats have shifted since the pick was made,
    the re-predict may not exactly reproduce the original prob
    (expected for a living model). What matters is the DELTA.
"""

import argparse
import logging
import sys
from dataclasses import dataclass, field

from . import config as cfg
from .db import get_conn as get_mlb_conn

logger = logging.getLogger(__name__)


# ── Factor registry ────────────────────────────────────────

# Map of sport -> list of toggle-able factor names (config attribute names)
FACTORS = {
    "mlb": [
        "MLB_ENABLE_BULLPEN_FATIGUE",
        "MLB_ENABLE_SITUATIONAL_AGG",
        "MLB_ENABLE_UMPIRE_FACTOR",
        "MLB_ENABLE_WEATHER_ADJ",
        "MLB_ENABLE_TRAVEL_FATIGUE",
        "MLB_ENABLE_MATCHUP_INTERACTION",
        "MLB_ENABLE_COORS_BOOST",
        "MLB_ENABLE_PLATOON_DUP",
        "MLB_ENABLE_H2H_VS_PITCHER",
        "MLB_ENABLE_LINEUP_STRENGTH",
        "MLB_ENABLE_SITUATIONAL_FACTORS",
    ],
    "nhl": [
        "NHL_ENABLE_GRANULAR_FACTORS",
        "NHL_ENABLE_GOALIE_SV",
        "NHL_ENABLE_GOALIE_STARTER",
        "NHL_ENABLE_GOALIE_BACKUP_PEN",
        "NHL_ENABLE_PP_PK",
        "NHL_ENABLE_SHOT_DIFF",
        "NHL_ENABLE_FACEOFF",
        "NHL_ENABLE_FORM",
        "NHL_ENABLE_HOME_AWAY_SPLIT",
        "NHL_ENABLE_STANDINGS_FORM",
        "NHL_ENABLE_VENUE_SPLIT",
        "NHL_ENABLE_MOTIVATION",
        "NHL_ENABLE_QUALITY_DIFF",
        "NHL_ENABLE_H2H",
    ],
    "nba": [
        "NBA_ENABLE_ROSTER_ADJUSTMENT",
    ],
}


# ── Result accumulator ─────────────────────────────────────

@dataclass
class BacktestStats:
    picks_total: int = 0
    picks_flipped: int = 0  # pick direction changed when factor disabled
    baseline_wins: int = 0
    baseline_losses: int = 0
    baseline_profit: float = 0.0
    ablated_wins: int = 0
    ablated_losses: int = 0
    ablated_profit: float = 0.0
    by_type: dict = field(default_factory=dict)

    def baseline_wr(self) -> float:
        d = self.baseline_wins + self.baseline_losses
        return self.baseline_wins / d if d else 0.0

    def ablated_wr(self) -> float:
        d = self.ablated_wins + self.ablated_losses
        return self.ablated_wins / d if d else 0.0


# ── MLB backtest driver ────────────────────────────────────

def _fetch_settled_mlb_picks(limit: int | None = None) -> list[dict]:
    """Pull settled MLB picks with enough metadata to re-predict."""
    conn = get_mlb_conn()
    q = """
        SELECT p.id, p.game_id, p.bet_type, p.pick, p.model_prob,
               p.edge, p.odds, p.result, p.profit, p.matchup,
               g.home_team_id, g.away_team_id,
               g.home_starter_id, g.away_starter_id, g.venue, g.date
        FROM picks p
        LEFT JOIN games g ON p.game_id = g.mlb_game_id
        WHERE p.result IS NOT NULL
        ORDER BY p.settled_at DESC
    """
    if limit:
        q += f" LIMIT {limit}"
    try:
        rows = conn.execute(q).fetchall()
    except Exception as e:
        logger.warning("Failed to fetch settled MLB picks with metadata: %s", e)
        # Fall back to picks-only query
        q2 = "SELECT * FROM picks WHERE result IS NOT NULL"
        if limit:
            q2 += f" LIMIT {limit}"
        rows = conn.execute(q2).fetchall()
    return [dict(r) for r in rows]


def _ablate_mlb(factor: str, picks: list[dict]) -> BacktestStats:
    """Re-predict each pick with `factor` set to False, tally deltas."""
    from .mlb_predict import predict_matchup
    from .picks import generate_picks, get_best_pick

    if not hasattr(cfg, factor):
        logger.warning("Unknown factor %s - skipping", factor)
        return BacktestStats()

    stats = BacktestStats()

    # Save original state + flip to False for the ablation pass
    original = getattr(cfg, factor)
    setattr(cfg, factor, False)

    try:
        for p in picks:
            stats.picks_total += 1

            home_tid = p.get("home_team_id")
            away_tid = p.get("away_team_id")
            if not home_tid or not away_tid:
                continue

            # Baseline result is what's stored on the pick (case-insensitive)
            res = _canon_result(p["result"])
            baseline_won = (res == "win")
            baseline_profit = p.get("profit") or 0.0
            if baseline_won:
                stats.baseline_wins += 1
            elif res == "loss":
                stats.baseline_losses += 1
            stats.baseline_profit += baseline_profit

            # Re-predict with factor OFF
            try:
                new_picks = generate_picks(
                    home_team_id=home_tid,
                    away_team_id=away_tid,
                    home_pitcher_id=p.get("home_starter_id"),
                    away_pitcher_id=p.get("away_starter_id"),
                    venue=p.get("venue"),
                    odds={"home_ml": None, "away_ml": None},  # minimal
                )
                new_best = get_best_pick(new_picks) if new_picks else None
            except Exception as e:
                logger.debug("Re-predict failed for pick %s: %s", p.get("id"), e)
                # Treat as unchanged (baseline still wins/loses)
                if baseline_won:
                    stats.ablated_wins += 1
                elif res == "loss":
                    stats.ablated_losses += 1
                stats.ablated_profit += baseline_profit
                continue

            # Compare direction/type
            if new_best and (new_best.get("pick") != p["pick"]
                             or new_best.get("type") != p["bet_type"]):
                stats.picks_flipped += 1
                # Different pick - we can't know if it would've won without
                # re-settling against the actual game result. For now treat
                # flipped picks as "skipped" (no W/L contribution).
                # Proper fix: reconstruct the game outcome for the new pick
                # type+direction from games table. TODO in a follow-up.
                continue

            # Same pick - outcome would have been same
            if baseline_won:
                stats.ablated_wins += 1
            elif res == "loss":
                stats.ablated_losses += 1
            stats.ablated_profit += baseline_profit

            # Per-type tally
            bt = p["bet_type"]
            t = stats.by_type.setdefault(
                bt, {"total": 0, "flipped": 0}
            )
            t["total"] += 1

    finally:
        setattr(cfg, factor, original)

    return stats


# ── Report rendering ───────────────────────────────────────

def _render_report(sport: str, results: dict[str, BacktestStats]) -> str:
    lines = []
    lines.append(f"\n{'=' * 72}")
    lines.append(f"  Factor Ablation Backtest - {sport.upper()}")
    lines.append(f"{'=' * 72}")
    lines.append(
        f"\n  {'Factor':<34} {'Picks':>6} {'Flipped':>8} "
        f"{'Base WR':>8} {'Base $':>10} {'Ablated WR*':>11}"
    )
    lines.append("  " + "-" * 70)

    for factor, stats in results.items():
        short = factor.replace("MLB_ENABLE_", "").replace("NHL_ENABLE_", "") \
                      .replace("NBA_ENABLE_", "")
        lines.append(
            f"  {short:<34} {stats.picks_total:>6d} "
            f"{stats.picks_flipped:>8d} "
            f"{stats.baseline_wr():>7.1%} "
            f"${stats.baseline_profit:>+9.2f} "
            f"{stats.ablated_wr():>10.1%}"
        )

    lines.append(
        "\n  * Ablated WR only counts picks that did NOT change direction"
        " when the"
    )
    lines.append(
        "    factor was disabled (flipped picks are excluded - need game-"
        "level"
    )
    lines.append(
        "    outcome re-settling to score them, which is a TODO)."
    )
    lines.append("")
    lines.append(
        "  Interpretation:"
    )
    lines.append(
        "    - High 'Flipped' count = factor is load-bearing for pick"
        " selection."
    )
    lines.append(
        "    - Low 'Flipped' + same WR = factor isn't doing much; safe to"
        " disable."
    )
    lines.append(
        "    - Low 'Flipped' + worse ablated WR = factor is subtly helpful;"
    )
    lines.append(
        "      keep on. (Rare - usually these factors are just noise.)"
    )
    return "\n".join(lines)


# ── CLI ────────────────────────────────────────────────────

def _fetch_settled_picks(sport: str, limit: int | None = None) -> list[dict]:
    """Generic settled-pick loader for NHL / NBA. MLB has its own with
    extra columns (pitcher IDs + venue) via _fetch_settled_mlb_picks."""
    import sqlite3
    from pathlib import Path
    paths = {
        "nhl": (Path(__file__).resolve().parent.parent / "data" / "nhl.db",
                "nhl_picks"),
        "nba": (Path(__file__).resolve().parent.parent / "data" / "nba.db",
                "nba_picks"),
    }
    if sport not in paths:
        return []
    db_path, table = paths[sport]
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Defensive: sport DB may exist but table hasn't been
        # initialized yet (first-time install, broken migration, etc.)
        tbl_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (table,)
        ).fetchone()
        if not tbl_exists:
            return []
        q = (f"SELECT id, game_id, bet_type, pick, model_prob, edge, odds, "
             f"result, matchup FROM {table} WHERE result IS NOT NULL "
             f"ORDER BY id DESC")
        if limit:
            q += f" LIMIT {limit}"
        return [dict(r) for r in conn.execute(q).fetchall()]
    except sqlite3.Error as e:
        logger.warning("Failed to load settled %s picks: %s", sport, e)
        return []
    finally:
        conn.close()


def _split_matchup(matchup: str) -> tuple[str, str] | None:
    """Parse 'AWAY @ HOME' string. Returns (home, away) or None."""
    if not matchup:
        return None
    parts = matchup.split(" @ ")
    if len(parts) != 2:
        return None
    return parts[1].strip(), parts[0].strip()


def _canon_result(v) -> str:
    """Canonicalize WIN/LOSS/PUSH across case + variants."""
    if v is None:
        return ""
    s = str(v).strip().lower()
    if s in ("win", "w", "hit", "1", "true"):
        return "win"
    if s in ("loss", "lose", "l", "miss", "0", "false"):
        return "loss"
    if s in ("push", "p", "tie", "draw"):
        return "push"
    return s


def _nhl_abbr_to_key() -> dict[str, str]:
    """Build abbreviation -> team_key map matching the NHL tracker's
    resolution logic. Mirrors the _ALT_ABBRS treatment so ESPN-style
    abbrs (TBL/NJD/SJS/LAK/WSH) resolve as well as internal (TB/NJ/
    SJ/LA/WAS)."""
    from .data import list_teams, load_team
    alt = {
        "TBL": "TB", "TB": "TBL", "NJD": "NJ", "NJ": "NJD",
        "SJS": "SJ", "SJ": "SJS", "LAK": "LA", "LA": "LAK",
        "WSH": "WAS", "WAS": "WSH", "CBJ": "CLB", "CLB": "CBJ",
        "MTL": "MON", "MON": "MTL", "NSH": "NAS", "NAS": "NSH",
        "UTA": "UTAH", "UTAH": "UTA",
    }
    m: dict[str, str] = {}
    try:
        for t in list_teams("NHL"):
            team = load_team("NHL", t["key"])
            if not team:
                continue
            abbr = (team.get("abbreviation") or "").strip()
            if abbr:
                m[abbr] = t["key"]
                if abbr in alt:
                    m[alt[abbr]] = t["key"]
    except Exception as e:
        logger.warning("Failed to build NHL abbr map: %s", e)
    return m


def _nhl_prob_for_pick(pred: dict, bet_type: str, pick: str,
                        home_abbr: str, away_abbr: str) -> float | None:
    """Extract the model's probability that this specific pick WINS.

    Works off predict_matchup's return dict directly so we don't need
    to pass odds through generate_nhl_picks (which would return nothing
    without market lines).
    """
    if not pred:
        return None
    wp = pred.get("win_prob") or {}
    pl = pred.get("puck_line") or {}
    ou = pred.get("over_under") or {}
    bt = (bet_type or "").upper()
    pk = (pick or "").strip()

    if bt == "ML":
        if pk == home_abbr:
            return wp.get("home")
        if pk == away_abbr:
            return wp.get("away")
    elif bt == "PL":
        # Pick strings look like "NYR -1.5" / "BOS +1.5"
        if " -1.5" in pk or pk.endswith("-1.5"):
            if pk.startswith(home_abbr):
                return pl.get("home_minus_1_5")
            if pk.startswith(away_abbr):
                return pl.get("away_minus_1_5")
        if " +1.5" in pk or pk.endswith("+1.5"):
            if pk.startswith(home_abbr):
                return pl.get("home_plus_1_5")
            if pk.startswith(away_abbr):
                return pl.get("away_plus_1_5")
    elif bt == "O/U":
        low = pk.lower()
        # Pick strings look like "Over 6.5" / "Under 5.5"
        tokens = pk.split()
        if len(tokens) >= 2:
            try:
                line = float(tokens[-1])
            except ValueError:
                line = None
            if line is not None:
                # Find closest key in over_under dict
                best_k = None
                best_d = 999
                for k in ou:
                    try:
                        d = abs(float(k) - line)
                        if d < best_d:
                            best_d = d
                            best_k = k
                    except (ValueError, TypeError):
                        continue
                if best_k:
                    row = ou[best_k]
                    if "over" in low:
                        return row.get("over")
                    if "under" in low:
                        return row.get("under")
    return None


def _ablate_nhl(factor: str, picks: list[dict]) -> BacktestStats:
    """Re-predict each NHL pick with `factor` set to False.

    Method: call predict_matchup directly (not generate_nhl_picks, which
    needs odds we don't always have stored). Compare the model's
    probability for the picked side before vs after the ablation. If
    the factor's removal pushes that probability below 0.5, the pick
    would have flipped direction - we count that separately.
    """
    from .nhl_predict import predict_matchup

    if not hasattr(cfg, factor):
        logger.warning("Unknown factor %s - skipping", factor)
        return BacktestStats()

    stats = BacktestStats()
    original = getattr(cfg, factor)
    abbr_key = _nhl_abbr_to_key()
    try:
        for p in picks:
            stats.picks_total += 1
            res = _canon_result(p["result"])
            baseline_won = (res == "win")
            if baseline_won:
                stats.baseline_wins += 1
            elif res == "loss":
                stats.baseline_losses += 1

            split = _split_matchup(p.get("matchup"))
            if not split:
                continue
            home_abbr, away_abbr = split
            home_key = abbr_key.get(home_abbr)
            away_key = abbr_key.get(away_abbr)
            if not home_key or not away_key:
                # Unresolvable teams - carry baseline outcome, don't flip
                if baseline_won:
                    stats.ablated_wins += 1
                elif res == "loss":
                    stats.ablated_losses += 1
                continue

            # First: baseline probability (factor ON)
            setattr(cfg, factor, original)
            try:
                base_pred = predict_matchup(home_key, away_key)
            except Exception:
                base_pred = None
            base_prob = _nhl_prob_for_pick(
                base_pred, p["bet_type"], p["pick"], home_abbr, away_abbr)

            # Then: ablated probability (factor OFF)
            setattr(cfg, factor, False)
            try:
                abl_pred = predict_matchup(home_key, away_key)
            except Exception as e:
                logger.debug("NHL re-predict failed for %s: %s", p.get("id"), e)
                abl_pred = None
            abl_prob = _nhl_prob_for_pick(
                abl_pred, p["bet_type"], p["pick"], home_abbr, away_abbr)

            if base_prob is None or abl_prob is None:
                # Can't compare - carry baseline outcome
                if baseline_won:
                    stats.ablated_wins += 1
                elif res == "loss":
                    stats.ablated_losses += 1
                continue

            # Flip definition: factor ON said >=50% on this pick, factor
            # OFF says <50%. Means this factor was propping up the pick
            # above the decision boundary.
            if base_prob >= 0.5 and abl_prob < 0.5:
                stats.picks_flipped += 1
                continue

            if baseline_won:
                stats.ablated_wins += 1
            elif res == "loss":
                stats.ablated_losses += 1

            bt = p["bet_type"]
            t = stats.by_type.setdefault(bt, {"total": 0, "flipped": 0})
            t["total"] += 1
    finally:
        setattr(cfg, factor, original)
    return stats


def _ablate_nba(factor: str, picks: list[dict]) -> BacktestStats:
    """Re-predict each NBA Q1 pick with `factor` set to False."""
    from .nba_picks import generate_q1_picks

    if not hasattr(cfg, factor):
        logger.warning("Unknown factor %s - skipping", factor)
        return BacktestStats()

    stats = BacktestStats()
    original = getattr(cfg, factor)
    setattr(cfg, factor, False)
    try:
        for p in picks:
            stats.picks_total += 1
            res = _canon_result(p["result"])
            baseline_won = (res == "win")
            if baseline_won:
                stats.baseline_wins += 1
            elif res == "loss":
                stats.baseline_losses += 1

            split = _split_matchup(p.get("matchup"))
            if not split:
                continue
            home, away = split

            try:
                new_picks = generate_q1_picks(home, away, odds={})
                new_best = new_picks[0] if new_picks else None
            except Exception as e:
                logger.debug("NBA re-predict failed for %s: %s", p.get("id"), e)
                if baseline_won:
                    stats.ablated_wins += 1
                elif res == "loss":
                    stats.ablated_losses += 1
                continue

            if new_best and (new_best.get("pick") != p["pick"]
                             or new_best.get("type") != p["bet_type"]):
                stats.picks_flipped += 1
                continue

            if baseline_won:
                stats.ablated_wins += 1
            elif res == "loss":
                stats.ablated_losses += 1
    finally:
        setattr(cfg, factor, original)
    return stats


def run(sport: str, specific_factor: str | None = None,
        limit: int | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)-7s %(message)s",
    )

    # Select loader + ablator per sport. MLB has its own loader with
    # pitcher-metadata columns; NHL/NBA use the generic one.
    if sport == "mlb":
        picks = _fetch_settled_mlb_picks(limit=limit)
        ablator = _ablate_mlb
    elif sport == "nhl":
        picks = _fetch_settled_picks("nhl", limit=limit)
        ablator = _ablate_nhl
    elif sport == "nba":
        picks = _fetch_settled_picks("nba", limit=limit)
        ablator = _ablate_nba
    else:
        print(f"Unknown sport: {sport}")
        return

    if not picks:
        print(f"No settled {sport.upper()} picks found. Run the tracker and "
              f"let games settle first.")
        return
    print(f"Loaded {len(picks)} settled {sport.upper()} picks.")

    factors = [specific_factor] if specific_factor else FACTORS.get(sport, [])
    if not factors:
        print(f"No factors registered for {sport.upper()}. "
              f"Add entries to FACTORS['{sport}'] in engine/factor_backtest.py.")
        return

    results = {}
    for f in factors:
        if f and hasattr(cfg, f):
            print(f"  Ablating {f}...")
            results[f] = ablator(f, picks)

    print(_render_report(sport, results))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sport", choices=["mlb", "nhl", "nba"])
    parser.add_argument("--factor", default=None,
                        help="Run ablation for a single factor by name")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit to N most-recent settled picks")
    args = parser.parse_args()
    run(args.sport, args.factor, args.limit)
