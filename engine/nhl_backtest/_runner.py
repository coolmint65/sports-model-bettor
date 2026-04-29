"""Backtest runner + threshold sweep + pretty-printer.

run_nhl_backtest is the single public entry point. analyze_edge_thresholds
sweeps it across [1, 3, 5, 8, 10, 15] min-edge values and reports which
threshold maximises ROI. print_backtest formats the result for the CLI.
"""

from __future__ import annotations
import logging

from ._helpers import (
    SEASON, OU_ODDS, MAX_GOALS,
    _implied, _empty_cat, _record_bet, _summarize, _prob_to_american,
    _score_matrix,
)
from ._io import _load_odds_map, _lookup_game_odds, _load_games_from_db, _load_games_from_api
from ._pit import (
    _compute_market_prob, _compute_pit_stats, _pit_predict,
    _predict_game, _check_b2b_from_games,
)

logger = logging.getLogger(__name__)


def run_nhl_backtest(days: int = 30, min_edge: float = 3.0,
                     season: int | None = None,
                     pit_mode: bool = True) -> dict:
    """Run backtest on historical NHL games.

    Args:
        days: Number of recent days to include (0 = full season).
        min_edge: Minimum edge percentage to place a bet.
        season: NHL season year (e.g. 2025 for 2025-26).
        pit_mode: If True, use point-in-time rolling stats to avoid
            lookahead bias. If False, use the live prediction model
            (current stats applied to historical games — for comparison).
    """
    games = _load_games_from_db(days=days, season=season)
    source = "db"

    if not games:
        effective_days = days or 30
        logger.info("No DB games found, fetching last %d days from ESPN API...",
                     effective_days)
        games = _load_games_from_api(days=effective_days)
        source = "api"

    if not games:
        return {"error": "No completed games found", "games_tested": 0}

    yr = season or SEASON

    odds_map = _load_odds_map(games)
    odds_real_count = 0
    odds_synthetic_count = 0

    pit_conn = None
    if pit_mode:
        try:
            from ..nhl_db import get_conn
            pit_conn = get_conn()
        except Exception:
            logger.warning("PIT mode requested but nhl_db unavailable, "
                           "falling back to live model")
            pit_mode = False

    game_dates = [g.get("date", "?") for g in games[:3]]
    game_seasons = [g.get("season", "?") for g in games[:3]]

    results = {
        "season": yr,
        "source": source,
        "pit_mode": pit_mode,
        "debug_game_count": len(games),
        "debug_sample_dates": game_dates,
        "debug_sample_seasons": game_seasons,
        "games_tested": 0,
        "games_skipped": 0,
        "moneyline": _empty_cat(),
        "over_under": _empty_cat(),
        "puck_line": _empty_cat(),
        "best_bet": _empty_cat(),
        "nrfi": _empty_cat(),
        "run_line": _empty_cat(),
    }

    recent_picks = []
    calibration_buckets = {
        "0-10": [0, 0], "10-20": [0, 0], "20-30": [0, 0],
        "30-40": [0, 0], "40-50": [0, 0], "50-60": [0, 0],
        "60-70": [0, 0], "70-80": [0, 0], "80-90": [0, 0],
        "90-100": [0, 0],
    }

    for game_idx, game in enumerate(games):
        home_abbr = game.get("home_abbr", "")
        away_abbr = game.get("away_abbr", "")
        home_score = game.get("home_score")
        away_score = game.get("away_score")

        if not home_abbr or not away_abbr:
            results["games_skipped"] += 1
            continue
        if home_score is None or away_score is None:
            results["games_skipped"] += 1
            continue

        # ── Get prediction (PIT or live model) ──
        pred = None
        home_pit = None
        away_pit = None
        if pit_mode and pit_conn:
            home_tid = game.get("home_team_id")
            away_tid = game.get("away_team_id")
            game_date = game.get("date", "")
            if home_tid and away_tid and game_date:
                home_pit = _compute_pit_stats(pit_conn, home_tid, game_date)
                away_pit = _compute_pit_stats(pit_conn, away_tid, game_date)
                if home_pit and away_pit:
                    pred = _pit_predict(home_pit, away_pit)

        if pred is None and not pit_mode:
            pred = _predict_game(home_abbr, away_abbr)

        if not pred:
            results["games_skipped"] += 1
            continue

        results["games_tested"] += 1
        home_won = home_score > away_score
        actual_total = home_score + away_score
        margin = home_score - away_score
        game_bets = []

        p_home = pred["p_home"]
        p_away = pred["p_away"]
        home_xg = pred["home_xg"]
        away_xg = pred["away_xg"]

        # ── Calibration tracking ──
        bucket_idx = min(int(max(p_home, p_away) * 100), 99)
        bucket_key = f"{(bucket_idx // 10) * 10}-{(bucket_idx // 10) * 10 + 10}"
        if bucket_key in calibration_buckets:
            calibration_buckets[bucket_key][0] += 1
            fav_won = (p_home > p_away and home_won) or \
                      (p_away > p_home and not home_won)
            if fav_won:
                calibration_buckets[bucket_key][1] += 1

        # ── Look up real historical odds ──
        game_date = game.get("date", "")
        real_odds = _lookup_game_odds(odds_map, game_date, home_abbr, away_abbr)
        game_used_real_odds = real_odds is not None

        # ── Compute market baseline ──
        if pit_mode and pit_conn and home_pit and away_pit:
            market_home = _compute_market_prob(home_pit, away_pit)
        else:
            market_home = 0.5
        market_away = 1 - market_home

        # ── Moneyline ──
        fav_home = p_home > p_away
        if fav_home:
            ml_prob = p_home
            ml_pick = home_abbr
            if real_odds and real_odds.get("home_ml") is not None:
                ml_odds = real_odds["home_ml"]
                ml_market = _implied(ml_odds)
            else:
                ml_market = market_home
                ml_odds = _prob_to_american(ml_market)
        else:
            ml_prob = p_away
            ml_pick = away_abbr
            if real_odds and real_odds.get("away_ml") is not None:
                ml_odds = real_odds["away_ml"]
                ml_market = _implied(ml_odds)
            else:
                ml_market = market_away
                ml_odds = _prob_to_american(ml_market)

        ml_edge = (ml_prob - ml_market) * 100
        if ml_edge >= min_edge:
            ml_correct = (fav_home and home_won) or \
                         (not fav_home and not home_won)
            _record_bet(results["moneyline"], ml_correct, ml_odds)
            game_bets.append((ml_edge, ml_correct, ml_odds, "ML", ml_pick))
            if len(recent_picks) < 50:
                recent_picks.append({
                    "date": game.get("date", ""),
                    "matchup": f"{away_abbr} @ {home_abbr}",
                    "type": "ML",
                    "pick": ml_pick,
                    "prob": round(ml_prob, 3),
                    "edge": round(ml_edge, 1),
                    "result": "W" if ml_correct else "L",
                    "score": f"{home_score}-{away_score}",
                })

        # ── Back-to-back adjustment for O/U ──
        ou_home_xg = pred.get("ou_home_xg", home_xg)
        ou_away_xg = pred.get("ou_away_xg", away_xg)
        b2b_adjusted = False

        if pit_mode and pit_conn:
            home_tid = game.get("home_team_id")
            away_tid = game.get("away_team_id")
            if home_tid and away_tid:
                home_b2b = _check_b2b_from_games(games, game_idx, home_tid)
                away_b2b = _check_b2b_from_games(games, game_idx, away_tid)
                if home_b2b:
                    ou_away_xg *= 1.04
                    b2b_adjusted = True
                if away_b2b:
                    ou_home_xg *= 1.04
                    b2b_adjusted = True

        # ── Over/Under ──
        ou_line = pred.get("ou_line")
        p_over = pred.get("p_over", 0.5)

        if b2b_adjusted:
            ou_home_xg = max(ou_home_xg, 1.0)
            ou_away_xg = max(ou_away_xg, 1.0)
            ou_line = 5.5
            ou_matrix = _score_matrix(ou_home_xg, ou_away_xg)
            p_over = 0.0
            for h in range(MAX_GOALS + 1):
                for a in range(MAX_GOALS + 1):
                    eff_total = (h + a + 1) if h == a else (h + a)
                    if eff_total > ou_line:
                        p_over += ou_matrix[h][a]
        elif ou_line is None:
            ou_line = 5.5
            matrix = _score_matrix(home_xg, away_xg)
            p_over = 0.0
            for h in range(MAX_GOALS + 1):
                for a in range(MAX_GOALS + 1):
                    eff_total = (h + a + 1) if h == a else (h + a)
                    if eff_total > ou_line:
                        p_over += matrix[h][a]

        if real_odds and real_odds.get("over_under") is not None:
            ou_line = real_odds["over_under"]
            ou_xg_h = pred.get("ou_home_xg", home_xg)
            ou_xg_a = pred.get("ou_away_xg", away_xg)
            if b2b_adjusted:
                ou_xg_h = max(ou_home_xg, 1.0)
                ou_xg_a = max(ou_away_xg, 1.0)
            ou_recomp_matrix = _score_matrix(ou_xg_h, ou_xg_a)
            p_over = 0.0
            for h in range(MAX_GOALS + 1):
                for a in range(MAX_GOALS + 1):
                    eff_total = (h + a + 1) if h == a else (h + a)
                    if eff_total > ou_line:
                        p_over += ou_recomp_matrix[h][a]

        ou_pick_over = p_over > 0.50
        ou_prob = p_over if ou_pick_over else (1 - p_over)

        if real_odds and ou_pick_over and real_odds.get("over_odds") is not None:
            ou_odds = real_odds["over_odds"]
            ou_market = _implied(ou_odds)
        elif real_odds and not ou_pick_over and real_odds.get("under_odds") is not None:
            ou_odds = real_odds["under_odds"]
            ou_market = _implied(ou_odds)
        else:
            ou_odds = OU_ODDS
            ou_market = _implied(OU_ODDS)

        ou_edge = (ou_prob - ou_market) * 100
        if ou_edge >= min_edge:
            if actual_total == ou_line:
                results["over_under"]["pushes"] += 1
            elif ou_pick_over:
                ou_correct = actual_total > ou_line
                _record_bet(results["over_under"], ou_correct, ou_odds)
                game_bets.append((ou_edge, ou_correct, ou_odds, "O/U",
                                  f"Over {ou_line}"))
            else:
                ou_correct = actual_total < ou_line
                _record_bet(results["over_under"], ou_correct, ou_odds)
                game_bets.append((ou_edge, ou_correct, ou_odds, "O/U",
                                  f"Under {ou_line}"))

        # ── Puck Line (-1.5) ──
        p_home_cover = pred.get("p_home_cover", 0.0)
        if p_home_cover == 0.0:
            matrix = _score_matrix(home_xg, away_xg)
            p_home_cover = sum(matrix[h][a] for h in range(MAX_GOALS + 1)
                               for a in range(MAX_GOALS + 1) if (h - a) >= 2)
        p_away_cover = 1 - p_home_cover

        pl_pick_home = p_home_cover > 0.50
        pl_prob = p_home_cover if pl_pick_home else p_away_cover

        if pl_pick_home and real_odds and real_odds.get("home_spread_odds") is not None:
            pl_odds = real_odds["home_spread_odds"]
            pl_market = _implied(pl_odds)
        elif not pl_pick_home and real_odds and real_odds.get("away_spread_odds") is not None:
            pl_odds = real_odds["away_spread_odds"]
            pl_market = _implied(pl_odds)
        else:
            if pl_pick_home:
                pl_market = 0.37
                pl_odds = 170
            else:
                pl_market = 0.65
                pl_odds = -180

        pl_edge = (pl_prob - pl_market) * 100
        if pl_edge >= min_edge:
            if pl_pick_home:
                pl_correct = margin >= 2
                pl_pick = f"{home_abbr} -1.5"
            else:
                pl_correct = margin <= 1
                pl_pick = f"{away_abbr} +1.5"
            _record_bet(results["puck_line"], pl_correct, pl_odds)
            game_bets.append((pl_edge, pl_correct, pl_odds, "PL", pl_pick))

        # ── Best bet per game ──
        if game_bets:
            game_bets.sort(key=lambda x: x[0], reverse=True)
            best_edge, best_correct, best_odds, _, _ = game_bets[0]
            _record_bet(results["best_bet"], best_correct, best_odds)

        if game_bets:
            if game_used_real_odds:
                odds_real_count += 1
            else:
                odds_synthetic_count += 1

    # ── Summaries ──
    for cat in ["moneyline", "over_under", "puck_line", "best_bet"]:
        _summarize(results[cat])

    results["run_line"] = results["puck_line"]
    results["recent_picks"] = recent_picks[-20:]

    cal = {}
    for bucket, (total, correct) in calibration_buckets.items():
        if total > 0:
            cal[bucket] = {
                "total": total,
                "correct": correct,
                "actual_pct": round(correct / total * 100, 1),
            }
    results["calibration"] = cal
    results["source"] = source

    total_odds_games = odds_real_count + odds_synthetic_count
    results["odds_real_count"] = odds_real_count
    results["odds_synthetic_count"] = odds_synthetic_count
    if total_odds_games > 0:
        results["odds_real_pct"] = round(odds_real_count / total_odds_games * 100, 1)
        results["odds_synthetic_pct"] = round(odds_synthetic_count / total_odds_games * 100, 1)
    else:
        results["odds_real_pct"] = 0.0
        results["odds_synthetic_pct"] = 0.0

    return results


def analyze_edge_thresholds(days: int = 0, season: int | None = None,
                            pit_mode: bool = True) -> list[dict]:
    """Sweep min_edge over [1, 3, 5, 8, 10, 15] and report each one's
    bets / win_pct / roi / profit. Used by /api/nhl/backtest/thresholds."""
    thresholds = [1, 3, 5, 8, 10, 15]
    results = []
    for threshold in thresholds:
        bt = run_nhl_backtest(days=days, min_edge=threshold, season=season,
                              pit_mode=pit_mode)
        entry = {
            "threshold": threshold,
            "games_tested": bt.get("games_tested", 0),
        }
        for cat in ["moneyline", "over_under", "puck_line", "best_bet"]:
            cat_data = bt.get(cat, {})
            entry[cat] = {
                "bets": cat_data.get("total_bets", 0),
                "wins": cat_data.get("wins", 0),
                "losses": cat_data.get("losses", 0),
                "win_pct": cat_data.get("win_pct", 0),
                "roi": cat_data.get("roi", 0),
                "profit": cat_data.get("profit", 0),
            }
        results.append(entry)
    return results


def print_backtest(results: dict) -> None:
    """Pretty-print backtest results to stdout (CLI)."""
    if "error" in results:
        print(f"Error: {results['error']}")
        return

    print(f"\n{'='*60}")
    print(f"  NHL MODEL BACKTEST -- {results.get('season', '?')} Season")
    print(f"{'='*60}")
    print(f"  Games tested:  {results['games_tested']}")
    print(f"  Games skipped: {results['games_skipped']}")
    print(f"  Data source:   {results.get('source', 'unknown')}")
    real_pct = results.get('odds_real_pct', 0)
    synth_pct = results.get('odds_synthetic_pct', 0)
    real_n = results.get('odds_real_count', 0)
    synth_n = results.get('odds_synthetic_count', 0)
    print(f"  Odds source:   {real_pct}% real historical ({real_n} games), "
          f"{synth_pct}% synthetic fallback ({synth_n} games)")
    print()

    for name, label in [("moneyline", "Moneyline"), ("over_under", "Over/Under"),
                         ("puck_line", "Puck Line")]:
        bt = results[name]
        if bt["total_bets"] == 0:
            print(f"  {label}: No qualifying bets")
            continue
        status = "PROFITABLE" if bt["profit"] > 0 else "LOSING"
        print(f"  {label}:")
        print(f"    Record: {bt['wins']}-{bt['losses']} ({bt['win_pct']}%)")
        print(f"    Profit: ${bt['profit']:+.2f} per $100 flat bets")
        print(f"    ROI: {bt['roi']:+.1f}% [{status}]")
        print()

    bb = results.get("best_bet", {})
    if bb.get("total_bets", 0) > 0:
        print(f"  Best Bet (1 per game):")
        print(f"    Record: {bb['wins']}-{bb['losses']} ({bb['win_pct']}%)")
        print(f"    Profit: ${bb['profit']:+.2f}")
        print(f"    ROI: {bb['roi']:+.1f}%")
        print()

    cal = results.get("calibration", {})
    if cal:
        print(f"  Calibration:")
        for bucket in sorted(cal.keys()):
            c = cal[bucket]
            print(f"    {bucket}%: {c['correct']}/{c['total']} "
                  f"({c['actual_pct']}% actual)")
        print()

    print(f"{'='*60}")
