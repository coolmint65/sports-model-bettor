"""DataGolf live strokes-gained scraper.

DataGolf's `/live-strokes-gained` page embeds the current tournament's
per-player SG breakdown (TOT / OTT / APP / ARG / PUTT / T2G / BS) in
``window.reload_data`` at page load. We use Playwright to execute the
page's JS just enough to populate that global, then read it out — no
DOM scraping, no brittle HTML parsing. Each refresh pulls the most
recent (live or last-finished) PGA Tour event.

Output shape:

    {
      "event_name":    "Truist Championship",
      "course_name":   "Quail Hollow Club",
      "current_round": 4,
      "fetched_iso":   "2026-05-10T22:08:40Z",
      "players": [
        {
          "dg_id":         21407,
          "player_num":    "49855",
          "name":          "Reitan, Kristoffer",
          "flag":          "NOR",
          "position":      "1",
          "total":         -15,
          "sg_total":      {"event": 3.12, "event_cum": 12.48, ...},
          "sg_app":        {...},  # per stat
          ...
        }, ...
      ],
    }

We persist one row per (player, event, stat) into the golf DB's
``player_sg`` table (created lazily). The predictor then has a real
strokes-gained signal to layer on top of the score-to-par baseline.

Per-call cost: ~3 sec for Playwright cold start + 3 sec for page
load. Cheap enough to hit hourly via the worker, but the data only
updates inter-round (typically 4x per tournament week) so we skip
when ``current_round`` hasn't advanced.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def fetch_live_sg(tour: str = "pga") -> dict | None:
    """Pull DataGolf's most-recent SG snapshot for ``tour``. Returns
    None on any failure; the predictor gracefully falls back to the
    score-to-par-only model."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("DataGolf: playwright not installed")
        return None
    # DataGolf paths per tour. LPGA + Korn Ferry have their own slugs.
    path = {
        "pga":       "live-strokes-gained",
        "lpga":      "live-strokes-gained?tour=lpga",
        "kornferry": "live-strokes-gained?tour=ko",   # guess; verify if needed
    }.get(tour, "live-strokes-gained")
    url = f"https://datagolf.com/{path}"
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)
            blob = page.evaluate("() => window.reload_data")
            browser.close()
    except Exception as e:
        logger.warning("DataGolf playwright nav failed: %s", e)
        return None
    if not blob:
        logger.info("DataGolf: window.reload_data was empty (off-season?)")
        return None
    # Pivot stats from `{stat: [{player_num, ...}]}` to per-player rows.
    stats: dict[str, list[dict]] = blob.get("stats") or {}
    lb: list[dict] = blob.get("lb") or []
    # Index by player_num for cross-join.
    stats_by_player: dict[str, dict[str, dict]] = {}
    for stat_key, rows in stats.items():
        for r in rows or []:
            pn = str(r.get("player_num") or "")
            if not pn:
                continue
            stats_by_player.setdefault(pn, {})[stat_key] = r
    players: list[dict] = []
    for row in lb:
        pn = str(row.get("player_num") or "")
        pstats = stats_by_player.get(pn) or {}
        players.append({
            "dg_id":       row.get("dg_id"),
            "player_num":  pn,
            "name":        row.get("player_name") or "",
            "flag":        row.get("flag") or "",
            "position":    row.get("position") or "",
            "total":       row.get("total"),
            "rounds":      {f"r{i}": row.get(f"r{i}")
                            for i in range(1, 5) if row.get(f"r{i}") is not None},
            "stats":       pstats,
        })
    return {
        "event_name":    blob.get("event_name") or "",
        "course_name":   blob.get("course_name") or "",
        "current_round": blob.get("current_round"),
        "fetched_iso":   blob.get("iso") or "",
        "players":       players,
    }
