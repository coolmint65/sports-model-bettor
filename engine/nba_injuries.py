"""
NBA Q1 roster-availability adjustment.

Reads the `nba_injuries` and `nba_players` tables (populated by the ESPN
scraper) and computes how many Q1 points a team is expected to lose given
its current injury report.  Also runs a load-management detector that
catches classic end-of-regular-season "resting starters" spots even when
ESPN's injury feed hasn't been updated yet (e.g. late-breaking DNPs).

Integration: called once per matchup from `engine/nba_q1_predict.py`.
"""

import logging

logger = logging.getLogger(__name__)

# Statuses that mean the player is definitely not playing
_OUT_STATUSES = {
    "out", "injured", "suspended", "g league", "two-way",
    "not with team", "dnp", "did not play",
}

# Statuses where the player might play — applied at 50% impact
_DOUBTFUL_STATUSES = {"doubtful"}
_QUESTIONABLE_STATUSES = {"questionable", "day-to-day", "dtd", "probable"}


def _classify_status(status: str) -> float:
    """Return an availability-loss weight in [0, 1].

    1.0 = fully out (apply full impact)
    0.5 = doubtful (apply half impact)
    0.25 = questionable (apply quarter impact)
    0.0 = probable/active (no impact)
    """
    s = (status or "").strip().lower()
    if not s:
        return 0.0
    if any(k in s for k in _OUT_STATUSES):
        return 1.0
    if any(k in s for k in _DOUBTFUL_STATUSES):
        return 0.5
    if any(k in s for k in _QUESTIONABLE_STATUSES):
        # "Probable" is very likely to play, treat as zero impact
        if "probable" in s:
            return 0.0
        return 0.25
    # Unknown — conservative zero
    return 0.0


def compute_q1_adjustment(team_id: int, season: int) -> dict:
    """Compute Q1 points adjustment for a team given current injuries.

    Returns:
        {
            "q1_delta": -3.4,                  # pts to subtract from team's Q1
            "out_players": [{name, status, q1_impact}, ...],
            "starters_out": 2,                 # count of starters affected
            "load_management": True,           # auto-detected resting risk
        }
    """
    from .nba_db import get_team_injuries, get_team_players

    injuries = get_team_injuries(team_id)
    players = get_team_players(team_id, season)

    # Build name/player_id lookup into player table for impact values
    by_name = {p["name"]: p for p in players}
    by_pid = {p["player_id"]: p for p in players if p.get("player_id")}

    q1_delta = 0.0
    out_list = []
    starters_out = 0

    for inj in injuries:
        weight = _classify_status(inj.get("status", ""))
        if weight == 0.0:
            continue

        # Try to locate the player row by ID first, then name
        p = None
        pid = inj.get("player_id")
        if pid and pid in by_pid:
            p = by_pid[pid]
        elif inj["name"] in by_name:
            p = by_name[inj["name"]]

        if p is None:
            # Player not in roster table yet (new call-up, roster lag).
            # Still log them with zero impact so callers can see them.
            out_list.append({
                "name": inj["name"],
                "status": inj.get("status", ""),
                "q1_impact": 0.0,
                "starter": False,
            })
            continue

        impact = (p.get("q1_impact") or 0.0) * weight
        q1_delta -= impact
        is_starter = bool(p.get("starter"))
        if is_starter and weight >= 1.0:
            starters_out += 1

        out_list.append({
            "name": inj["name"],
            "status": inj.get("status", ""),
            "q1_impact": round(impact, 3),
            "starter": is_starter,
        })

    # Tier 4 — load-management auto-detection.
    # Trigger when 3+ starters are flagged out at the same time (classic
    # tanking or playoff-rest signature). Additional -2.0 Q1 penalty on
    # top of individual impact, capped at -8.0 total.
    load_management = starters_out >= 3
    if load_management:
        q1_delta -= 2.0
        q1_delta = max(q1_delta, -8.0)

    return {
        "q1_delta": round(q1_delta, 2),
        "out_players": out_list,
        "starters_out": starters_out,
        "load_management": load_management,
    }


def is_likely_resting_spot(team_id: int, game_date: str,
                           season: int) -> bool:
    """Heuristic: detect a likely starter-resting spot from schedule context.

    Fires when both:
      - Team has <= 2 games remaining in the regular season after `game_date`
      - Team has played their full regular-season slate (last-game scenario)

    Called as a secondary signal alongside compute_q1_adjustment — if it
    fires without injuries already predicting a resting spot, apply a
    modest -3.0 Q1 penalty as a safety net.
    """
    from .nba_db import get_conn
    conn = get_conn()

    # Count team's future scheduled games this season after the date
    row = conn.execute("""
        SELECT COUNT(*) AS n
        FROM nba_games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND season = ?
          AND date > ?
          AND status != 'final'
    """, (team_id, team_id, season, game_date)).fetchone()
    remaining = (row[0] if row else 0)

    # Count completed games for context — regular season is 82 games
    row2 = conn.execute("""
        SELECT COUNT(*) AS n
        FROM nba_games
        WHERE (home_team_id = ? OR away_team_id = ?)
          AND season = ?
          AND status = 'final'
    """, (team_id, team_id, season)).fetchone()
    played = (row2[0] if row2 else 0)

    # End-of-regular-season: played 79+ games and 0-2 remaining
    return played >= 79 and remaining <= 1


# ── CLI (debug / inspection) ───────────────────────────────

if __name__ == "__main__":
    import logging
    import sys
    from datetime import datetime

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s")

    from .nba_db import get_all_nba_teams

    season = int(sys.argv[1]) if len(sys.argv) > 1 else (
        datetime.now().year if datetime.now().month >= 9
        else datetime.now().year - 1)

    teams = get_all_nba_teams()
    print(f"\n{'=' * 72}")
    print(f"  NBA Roster/Injury Adjustments (season {season})")
    print(f"{'=' * 72}")
    if not teams:
        print("  No teams in DB — run 'python -m scrapers.nba_espn --full' first.")
        sys.exit(0)

    any_adjustments = False
    for t in sorted(teams, key=lambda x: x["abbreviation"]):
        adj = compute_q1_adjustment(t["id"], season)
        if adj["q1_delta"] == 0 and not adj["out_players"]:
            continue
        any_adjustments = True
        flag = " [LOAD MGMT]" if adj["load_management"] else ""
        print(f"\n  {t['abbreviation']:4s} {t['name']:25s}  "
              f"Q1 delta: {adj['q1_delta']:+.1f}  "
              f"starters out: {adj['starters_out']}{flag}")
        for p in adj["out_players"]:
            star = "*" if p.get("starter") else " "
            print(f"    {star} {p['name']:28s} {p['status']:15s}  "
                  f"impact: {p['q1_impact']:+.2f}")

    if not any_adjustments:
        print("\n  No injuries affecting Q1 across any team.\n")
    else:
        print()
