"""
Hard Rock derivatives discovery — dumps every market HR offers per
sport with NO whitelist applied, so we can see the full menu of
markets we could model.

The production scraper at scrapers/hardrock_odds.py intentionally
filters markets through `_market_kind()` to keep only game-level
ML/spread/total + NBA Q1. This module bypasses that filter to enumerate
team totals, period totals, derivatives, and props that are present in
the same payload but discarded by the production code.

Usage:
    python -m scrapers.hardrock_discover [sport]
        sport = mlb | nhl | nba | all (default: all)

Output:
    data/hr_discover/<sport>_markets.json   per-sport raw market dump
    data/hr_discover/<sport>_summary.txt    human-readable category summary

Findings drive the Phase 1 derivatives plan: which markets to whitelist
into the production scraper, which our existing models can already
price (free win), which need new math.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from collections import Counter, defaultdict
from typing import Any

logger = logging.getLogger(__name__)

OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                       "data", "hr_discover")


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    return str(v)


def _walk_events(data: Any) -> list[dict]:
    """Reuse the production scraper's event-flattening so we hit the
    same matchups it does. Falls back to a recursive scan when the
    helper isn't available."""
    try:
        from scrapers.hardrock_odds import _walk_events_flat
        flat = _walk_events_flat(data)
        if flat:
            return flat
    except Exception:
        pass

    # Fallback: walk the tree looking for dicts that have markets
    found: list[dict] = []

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            if "markets" in node or "offers" in node or "bets" in node:
                found.append(node)
            for v in node.values():
                visit(v)
        elif isinstance(node, list):
            for v in node:
                visit(v)

    visit(data)
    return found


def _extract_event_meta(event: dict) -> dict:
    from scrapers.hardrock_odds import _extract_teams, _pick
    away, home = _extract_teams(event)
    return {
        "matchup": f"{away} @ {home}" if (away and home) else "?",
        "id": _pick(event, "id", "eventId", "uid"),
        "start_time": _safe_str(_pick(event, "startTime", "startsAt",
                                      "scheduledStart", "start")),
    }


def _extract_market_meta(mkt: dict) -> dict:
    from scrapers.hardrock_odds import _pick
    label = _safe_str(_pick(mkt, "name", "label", "marketName"))
    mtype = _safe_str(_pick(mkt, "type", "marketType"))
    period = _safe_str(mkt.get("period") or "")
    line = _pick(mkt, "line", "spread")
    outcomes = (_pick(mkt, "selection", "outcomes",
                      "selections", "runners") or [])
    sides = []
    if isinstance(outcomes, list):
        for o in outcomes[:6]:  # cap to keep dump readable
            if isinstance(o, dict):
                sides.append({
                    "name": _safe_str(_pick(o, "name", "displayName")),
                    "price": _pick(o, "price", "americanOdds", "odds"),
                    "line": _pick(o, "line", "handicap", "spread"),
                })
    return {
        "label": label,
        "type_code": mtype,
        "period": period,
        "market_line": line,
        "outcome_count": len(outcomes) if isinstance(outcomes, list) else 0,
        "sample_sides": sides,
    }


def discover(sport: str) -> dict:
    """Fetch the HR event tree and dump every market for the given sport.
    Returns a dict {events_seen, markets_seen, by_type, by_label, raw}.
    """
    from scrapers.hardrock_odds import (
        _fetch_event_tree, _matches_sport, _collect_events_from_sport,
        _walk_sports, _walk_events_flat, _pick,
    )

    data, err = _fetch_event_tree()
    if err or not isinstance(data, dict):
        return {"error": err or "no data"}

    # Strip outer data wrapper (HR's betSync GraphQL shape).
    if "data" in data and isinstance(data["data"], dict):
        data = data["data"]

    # Same dispatch the production scraper uses — _walk_sports digs
    # through betSync/sports/sport variants and returns the sport
    # nodes regardless of the layout.
    events_pairs: list[tuple[str, list]] = []
    sports_nodes = _walk_sports(data)
    if sports_nodes:
        for s in sports_nodes:
            sport_name = _safe_str(_pick(s, "name", "displayName",
                                         "code", "id"))
            events = _collect_events_from_sport(s, sport)
            if events:
                events_pairs.append((sport_name, events))
    else:
        flat = _walk_events_flat(data)
        if flat:
            events_pairs.append(("", flat))

    raw_dump: list[dict] = []
    type_counter: Counter = Counter()
    label_counter: Counter = Counter()
    label_by_type: dict[str, set] = defaultdict(set)
    period_counter: Counter = Counter()

    events_seen = 0
    markets_seen = 0
    matchups_seen: list[str] = []

    for sport_name, events in events_pairs:
        if sport_name and not _matches_sport(sport_name, sport):
            continue
        for event in events:
            if not isinstance(event, dict):
                continue
            evt_meta = _extract_event_meta(event)
            markets = (_pick(event, "markets", "offers",
                             "bets", "marketGroups") or [])
            if not isinstance(markets, list):
                continue
            events_seen += 1
            matchups_seen.append(evt_meta["matchup"])
            evt_markets: list[dict] = []
            for m in markets:
                if not isinstance(m, dict):
                    continue
                inner = _pick(m, "markets")
                iterable = inner if isinstance(inner, list) else [m]
                for mkt in iterable:
                    if not isinstance(mkt, dict):
                        continue
                    meta = _extract_market_meta(mkt)
                    evt_markets.append(meta)
                    markets_seen += 1
                    if meta["type_code"]:
                        type_counter[meta["type_code"]] += 1
                        label_by_type[meta["type_code"]].add(meta["label"])
                    if meta["label"]:
                        label_counter[meta["label"]] += 1
                    if meta["period"]:
                        period_counter[meta["period"]] += 1
            raw_dump.append({**evt_meta, "markets": evt_markets})

    return {
        "sport": sport,
        "events_seen": events_seen,
        "markets_seen": markets_seen,
        "matchups": matchups_seen,
        "type_codes": dict(type_counter.most_common()),
        "labels_top": dict(label_counter.most_common(50)),
        "periods": dict(period_counter.most_common()),
        "label_by_type": {k: sorted(v) for k, v in label_by_type.items()},
        "raw": raw_dump,
    }


def write_outputs(sport: str, result: dict) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    json_path = os.path.join(OUT_DIR, f"{sport}_markets.json")
    summary_path = os.path.join(OUT_DIR, f"{sport}_summary.txt")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)

    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"Hard Rock market discovery — {sport.upper()}\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Events: {result.get('events_seen', 0)}\n")
        f.write(f"Markets: {result.get('markets_seen', 0)}\n\n")
        f.write("Type codes (count):\n")
        for code, count in (result.get("type_codes") or {}).items():
            f.write(f"  {count:5d}  {code}\n")
        f.write("\nUnique labels per type code:\n")
        for code, labels in (result.get("label_by_type") or {}).items():
            f.write(f"\n  [{code}]\n")
            for lbl in labels:
                f.write(f"    {lbl}\n")
        f.write("\nPeriod values:\n")
        for p, count in (result.get("periods") or {}).items():
            f.write(f"  {count:5d}  {p}\n")
    print(f"  -> wrote{json_path} ({os.path.getsize(json_path):,} bytes)")
    print(f"  -> wrote{summary_path}")


def main(argv: list[str]) -> int:
    sport = (argv[1] if len(argv) > 1 else "all").lower()
    sports = ["mlb", "nhl", "nba"] if sport == "all" else [sport]
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    for s in sports:
        print(f"\n=== Discovering {s.upper()} markets ===")
        result = discover(s)
        if result.get("error"):
            print(f"  !! error: {result['error']}")
            continue
        print(f"  events: {result['events_seen']}  "
              f"markets: {result['markets_seen']}  "
              f"unique types: {len(result['type_codes'])}  "
              f"unique labels: {len(result['labels_top'])}")
        write_outputs(s, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
