"""
Seed park_factors rows for 2026 venues that hit the live slate but
weren't in the existing 29-row table — found via a 30-day backtest
audit (engine.train mlb).

Run from repo root:
    python -m scripts.seed_park_factors_2026

Idempotent. Re-running just refreshes the run_factor for any row that
was already there.

Why each entry:
    Estadio Alfredo Harp Helu — Mexico City, ~7300 ft elevation,
        higher than Coors (5200 ft). 2024 SD/SF series saw ~14
        runs/game. Set 1.20 (Coors is 1.15).

    Oriole Park at Camden Yards — somehow missing entirely (14
        recent games, no row). Camden has historically been
        moderately hitter-friendly. Set 1.04.

    Daikin Park — name change from Minute Maid Park (Houston)
        for the 2025+ season. Same building. Match the existing
        Minute Maid factor (1.01).

    UNIQLO Field at Dodger Stadium — sponsor naming on Dodger
        Stadium for 2026. Same factor as Dodger Stadium (0.98).

    Rate Field — name change from Guaranteed Rate Field (CHW).
        Same factor (1.02).

    Sutter Health Park — Athletics' temporary home in Sacramento
        after leaving Oakland Coliseum. Minor-league dimensions
        (340 LF / 405 CF / 325 RF) but at lower elevation than
        Oakland. Set 0.95 (slightly above Coliseum's 0.92).
"""

from __future__ import annotations

from engine.db import get_conn

SEEDS = [
    ('Estadio Alfredo Harp Helu',     2026, 1.20),
    ('Oriole Park at Camden Yards',   2026, 1.04),
    ('Daikin Park',                   2026, 1.01),
    ('UNIQLO Field at Dodger Stadium', 2026, 0.98),
    ('Rate Field',                    2026, 1.02),
    ('Sutter Health Park',            2026, 0.95),
]


def seed():
    conn = get_conn()
    for venue, season, rf in SEEDS:
        conn.execute(
            "INSERT OR REPLACE INTO park_factors (venue, season, run_factor) "
            "VALUES (?, ?, ?)",
            (venue, season, rf),
        )
    conn.commit()
    print(f"Seeded {len(SEEDS)} park factor rows for 2026.")
    for venue, season, rf in SEEDS:
        print(f"  {venue:<40} {rf:.3f}")


if __name__ == "__main__":
    seed()
