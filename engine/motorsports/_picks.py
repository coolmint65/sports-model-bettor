"""F1 picks engine: race-winner + podium markets.

Picks emit when model probability beats juice-removed market probability
by more than the bet-type floor. Two market-specific guards:

  - Race-winner: only emit when our model also ranks the driver as the
    *most-likely* winner of the field. This honours the no-underdog-ML
    directive — we don't bet a driver we don't think will win, even if
    edge looks positive on a +800 outright (positive edge on a 12%
    market prob isn't a "win prediction").
  - Podium: no rank guard. Podium is a 3-of-22 market with ~13.6% per-
    driver baseline, so positive edge is a fair signal regardless of
    where the driver lands in the model's win ranking.

Juice removal: motorsports outrights have a *huge* overround (often
30-40%) because the long tail of priced-but-unlikely entries inflates
the book's vig. We normalize implied probs by dividing by the overround
sum so they re-add to 1.0 (winner) or 3.0 (podium). Same approach used
by every public outright-handicap reference.
"""
from __future__ import annotations

import logging
from typing import Any

from ._db import get_conn

logger = logging.getLogger(__name__)


# Bet-type floors: edge >= floor required for a pick to fire. Picked
# conservatively for v1 because we have no calibration history yet.
# Once we accumulate ~30 settled picks, the dynamic_reliability layer
# can re-tune these from realized ROI.
WINNER_EDGE_FLOOR = 0.10        # 10% edge minimum for race-winner
PODIUM_EDGE_FLOOR = 0.12        # slightly higher — podium has more
                                # variance per single race outcome

# Belief gates: refuse picks the model doesn't actually believe in,
# regardless of nominal "edge". Honours the user's directive: only back
# a pick when the system thinks it's a likely outcome. For motorsports
# the analog of the moneyline-underdog rule is: model_prob must clear
# a multiple of the no-info baseline (1/N for win, 3/N for podium).
# Without these gates the engine fired 15 podium picks on +50000 dogs
# whose model prob (3%) only "beat" market (0.2%) because the predictor
# never zeroes out long-tail drivers — that's noise, not edge.
WINNER_BELIEF_MULT = 2.0        # require p_win >= 2 * (1/field_size)
PODIUM_BELIEF_MULT = 1.25       # require p_podium >= 1.25 * (3/field_size)
                                # — looser since podium is a 3-slot
                                # market and the baseline is already
                                # higher

# Sanity cap on edge — defense-in-depth backstop above the calibration
# layer. Pre-calibration this was tight at 50% because the raw model
# spat out phantom 100-200% ROI edges on cold-start drivers. With the
# per-bucket walk-forward calibration now applied
# (engine.motorsports._calibration) those phantom probs get shrunk to
# realistic values before edge calc runs, so the ceiling can be
# permissive. Raised to 100% ROI on 2026-05-17 so legitimate calibrated
# longshots (rare but real — e.g. a backmarker the model thinks should
# be quicker than the book has priced) can still ship. Truly absurd
# edges beyond 100% still get blocked.
MAX_EDGE = 1.00


def _american_to_decimal(american: int) -> float:
    if american >= 0:
        return 1.0 + american / 100.0
    return 1.0 + 100.0 / abs(american)


def _implied_prob(american: int) -> float:
    if american >= 0:
        return 100.0 / (american + 100.0)
    return abs(american) / (abs(american) + 100.0)


def _devig_field(odds_map: dict[int, dict],
                  market_norm: float = 1.0) -> dict[int, float]:
    """Strip overround from a multi-selection outright market. Sum the
    raw implied probabilities, divide each by sum/market_norm so they
    re-add to ``market_norm`` (1.0 for winner, 3.0 for podium)."""
    raw = {did: _implied_prob(info["odds"]) for did, info in odds_map.items()}
    total = sum(raw.values())
    if total <= 0:
        return raw
    scale = market_norm / total
    return {did: p * scale for did, p in raw.items()}


def _stake_ladder(edge: float) -> float:
    """F1 outright-market stake ladder. Returns 0u (shadow) when edge
    falls outside the [floor, MAX_EDGE] band. F1 outright probabilities
    sit below the universal Kelly lean gate (12-30% win, 30-60% podium)
    so this ladder substitutes for ``_pick_helpers.stake_units_for``."""
    if edge < PODIUM_EDGE_FLOOR or edge > MAX_EDGE:
        return 0.0
    if edge >= 0.30:
        return 1.0
    if edge >= 0.18:
        return 0.5
    return 0.25


def generate_picks(series: str, race_id: str,
                   prediction: dict, odds: dict,
                   write_to_db: bool = True) -> list[dict]:
    """Generate the model's race podium prediction: 1 WINNER pick + 2
    PODIUM picks. The PODIUM picks are the model's 2 next-most-likely
    podium finishers behind the predicted winner. Output is always
    "winner + 2 podium spots" so the card surfaces a complete podium
    forecast rather than 0-N picks depending on edge-gate variance.

    Stake sizing still respects belief + edge: when the model doesn't
    believe (p_win below the field-size baseline multiple) or has no
    edge over market, the pick ships as a *shadow* (stake_units=0.0).
    Shadow picks still record so the calibration loop sees them, but
    the display layer should filter them out of "active bets".

    Args:
        prediction: output of ``_predict.predict_race`` — per-driver
                    p_win + p_podium, drivers sorted by p_win desc.
        odds: output of ``_odds.fetch_series_odds`` — 'winner' and/or
              'podium' subkeys keyed by driver_id.

    Returns:
        list of pick dicts. Also persisted when ``write_to_db`` is True.
    """
    out: list[dict] = []
    drivers = prediction.get("drivers") or []
    if not drivers:
        return out

    field_size = max(len(drivers), 1)
    win_baseline = 1.0 / field_size
    pod_baseline = 3.0 / field_size

    # The model's predicted race winner — drives both the WINNER pick
    # and the podium-exclusion below (we never pick the same driver for
    # both WINNER and PODIUM since winning implies podium).
    predicted_winner_id = drivers[0]["driver_id"]

    # WINNER pick — model's top driver, emitted whenever priced.
    win_odds = odds.get("winner") or {}
    if win_odds and predicted_winner_id in win_odds:
        top = drivers[0]
        win_implied = _devig_field(win_odds, market_norm=1.0)
        implied = win_implied.get(predicted_winner_id, 0.0)
        believe = top["p_win"] >= WINNER_BELIEF_MULT * win_baseline
        edge = ((top["p_win"] - implied) / implied) if implied > 0 else 0.0
        stake = _stake_ladder(edge) if believe else 0.0
        out.append({
            "race_id": race_id,
            "driver_id": predicted_winner_id,
            "bet_type": "WINNER",
            "pick": top["abbrev"],
            "model_prob": top["p_win"],
            "edge": edge,
            "odds": win_odds[predicted_winner_id]["odds"],
            "stake_units": stake,
        })

    # PODIUM picks — top 2 by p_podium, excluding the predicted winner.
    # Always 2 spots filled (when priced) so the card reflects a full
    # podium forecast. Belief + edge gates downgrade to shadow stake,
    # but the picks still emit so the calibration loop learns from them.
    pod_odds = odds.get("podium") or {}
    if pod_odds:
        pod_implied = _devig_field(pod_odds, market_norm=3.0)
        podium_pool = sorted(
            [d for d in drivers
              if d["driver_id"] in pod_odds
              and d["driver_id"] != predicted_winner_id],
            key=lambda d: d["p_podium"],
            reverse=True,
        )[:2]
        for d in podium_pool:
            did = d["driver_id"]
            implied = pod_implied.get(did, 0.0)
            believe = d["p_podium"] >= PODIUM_BELIEF_MULT * pod_baseline
            edge = ((d["p_podium"] - implied) / implied) if implied > 0 else 0.0
            stake = _stake_ladder(edge) if believe else 0.0
            out.append({
                "race_id": race_id,
                "driver_id": did,
                "bet_type": "PODIUM",
                "pick": d["abbrev"],
                "model_prob": d["p_podium"],
                "edge": edge,
                "odds": pod_odds[did]["odds"],
                "stake_units": stake,
            })

    if write_to_db and out:
        _persist(series, race_id, out)
    return out


def _persist(series: str, race_id: str, picks: list[dict]) -> None:
    """Upsert picks to the picks table. Idempotent on
    (race_id, driver_id, bet_type).

    Silent-demotion rule: when the model re-ranks between runs — e.g.
    yesterday's #3 PODIUM drops to #4 and falls out of the top-2 — the
    demoted pick is DELETED from the pending set, not voided. Voiding
    used to be the behavior; the tracker was showing 9 F1 PODIUM V's
    for completed races where the driver actually podiumed but their
    row had been graded V days earlier by a subsequent picker run. See
    engine.motorsports._picks._persist history + tracker audit
    2026-07-09. Settled picks (result != NULL) are never touched — if a
    demoted pick was already graded W/L/P from a prior settle pass, it
    stays in the record."""
    conn = get_conn(series)
    race = conn.execute(
        "SELECT race_date FROM races WHERE race_id = ?", (race_id,)
    ).fetchone()
    if not race:
        return
    date = race["race_date"]
    # Silently drop demoted pending picks — they never really "were"
    # a pick, they were a candidate that dropped out of the top slate.
    keep = {(p["driver_id"], p["bet_type"]) for p in picks}
    pending = conn.execute("""
        SELECT id, driver_id, bet_type FROM picks
        WHERE race_id = ? AND result IS NULL
    """, (race_id,)).fetchall()
    for r in pending:
        if (r["driver_id"], r["bet_type"]) not in keep:
            conn.execute("DELETE FROM picks WHERE id = ?", (r["id"],))
    for p in picks:
        existing = conn.execute("""
            SELECT id FROM picks
            WHERE race_id = ? AND driver_id = ? AND bet_type = ?
        """, (race_id, p["driver_id"], p["bet_type"])).fetchone()
        if existing:
            conn.execute("""
                UPDATE picks
                SET model_prob = ?, edge = ?, odds = ?, pick = ?
                WHERE id = ?
            """, (p["model_prob"], p["edge"], p["odds"],
                  p["pick"], existing["id"]))
        else:
            conn.execute("""
                INSERT INTO picks (date, race_id, driver_id, bet_type,
                                    pick, model_prob, edge, odds, stake_units)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (date, race_id, p["driver_id"], p["bet_type"],
                  p["pick"], p["model_prob"], p["edge"], p["odds"],
                  p.get("stake_units")))
    conn.commit()

    # Phase-2 cutover: dual-write into picks_unified (canonical store).
    try:
        from ..picks_unified._legacy_bridge import mirror_to_unified
        for p in picks:
            mirror_to_unified(
                sport="motorsports", league=series,
                native_game_id=race_id,
                pick_date=date or "",
                matchup="",
                bet_type=p["bet_type"],
                pick_text=p["pick"],
                odds=int(p.get("odds") or 0),
                prob=float(p.get("model_prob") or 0.0),
                edge_pct=float(p.get("edge") or 0.0),
                stake_units=float(p.get("stake_units") or 0.0),
            )
    except Exception as e:
        import logging
        logging.getLogger(__name__).debug(
            "picks_unified mirror (motorsports/%s) skipped: %s", series, e)


def settle_race(series: str, race_id: str) -> dict:
    """Mark this race's picks W/L/P now that race_results has the
    classification. Returns counts."""
    conn = get_conn(series)
    out = {"settled": 0, "still_pending": 0}
    pending = conn.execute("""
        SELECT id, driver_id, bet_type, odds
        FROM picks WHERE race_id = ? AND result IS NULL
    """, (race_id,)).fetchall()
    for p in pending:
        result_row = conn.execute("""
            SELECT finish_pos, status FROM race_results
            WHERE race_id = ? AND driver_id = ?
        """, (race_id, p["driver_id"])).fetchone()
        if not result_row or result_row["finish_pos"] is None:
            out["still_pending"] += 1
            continue
        finish = result_row["finish_pos"]
        win = (p["bet_type"] == "WINNER" and finish == 1) or \
              (p["bet_type"] == "PODIUM" and finish <= 3)
        # American odds → profit per 1u stake
        odds = p["odds"]
        if odds >= 0:
            profit = odds / 100.0 if win else -1.0
        else:
            profit = 100.0 / abs(odds) if win else -1.0
        conn.execute("""
            UPDATE picks SET result = ?, profit = ?,
                              settled_at = datetime('now')
            WHERE id = ?
        """, ("W" if win else "L", profit, p["id"]))
        out["settled"] += 1
    conn.commit()
    return out
