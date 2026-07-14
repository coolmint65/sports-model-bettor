"""OWGR + DataGolf SG ingest with name-resolution to our ESPN player_id
namespace.

Neither OWGR nor DataGolf shares ESPN's player id space. We bridge via
name matching: normalize (strip accents, drop punctuation, lowercase),
support "Last, First" ↔ "First Last" reordering, and accept short-form
("M. Fitzpatrick") via last-name fallback. Misses log + skip rather
than mis-attribute — false positives poison the skill prior.

Public:
    refresh_owgr(tour)       — pull current OWGR top-300 and upsert
    refresh_datagolf_sg(tour)— pull DataGolf live SG and upsert
"""
from __future__ import annotations

import logging
import unicodedata

from ._db import get_conn

logger = logging.getLogger(__name__)


def _norm(s: str | None) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return "".join(c.lower() for c in s if c.isalnum() or c == " ").strip()


def _build_player_index(tour: str) -> tuple[dict[str, int], dict[str, int]]:
    """Returns ``(full_name_index, last_name_index)``. Full-name keys
    include both "First Last" and "Last, First" variants. Last-name
    keys map ambiguously — only use when full match misses AND the
    last name appears in a single player row."""
    conn = get_conn(tour)
    rows = conn.execute(
        "SELECT id, name, short_name FROM players"
    ).fetchall()
    full: dict[str, int] = {}
    last_count: dict[str, list[int]] = {}
    for r in rows:
        pid = int(r["id"])
        nm = (r["name"] or "").strip()
        if not nm:
            continue
        key = _norm(nm)
        full.setdefault(key, pid)
        # Reversed: "Last, First" → "First Last"
        if "," in nm:
            parts = [p.strip() for p in nm.split(",", 1)]
            if len(parts) == 2:
                full.setdefault(_norm(f"{parts[1]} {parts[0]}"), pid)
        # Comma-rendered for DataGolf compat: "Reitan, Kristoffer"
        tokens = nm.split()
        if len(tokens) >= 2:
            full.setdefault(
                _norm(f"{tokens[-1]}, {' '.join(tokens[:-1])}"),
                pid,
            )
            last_count.setdefault(_norm(tokens[-1]), []).append(pid)
        short = (r["short_name"] or "").strip()
        if short:
            full.setdefault(_norm(short), pid)
    # Only keep last-name keys that resolve unambiguously.
    last: dict[str, int] = {k: v[0] for k, v in last_count.items()
                            if len(set(v)) == 1}
    return full, last


def _resolve_player(name: str, full_index: dict[str, int],
                    last_index: dict[str, int]) -> int | None:
    if not name:
        return None
    key = _norm(name)
    if key in full_index:
        return full_index[key]
    # Try without middle initials: "Scottie A Scheffler" → "Scottie Scheffler"
    tokens = key.split()
    if len(tokens) >= 3:
        # Drop middle tokens that are single chars (initials)
        trimmed = [tokens[0]] + [t for t in tokens[1:-1] if len(t) > 1] + [tokens[-1]]
        ktrim = " ".join(trimmed)
        if ktrim in full_index:
            return full_index[ktrim]
    # Last-name fallback (only when unambiguous in our DB)
    if tokens:
        last = tokens[-1]
        if last in last_index:
            return last_index[last]
    return None


# ── OWGR ────────────────────────────────────────────────────

def refresh_owgr(tour: str = "pga", limit: int = 300) -> dict:
    """Pull OWGR top ``limit`` and upsert into ``owgr_rankings`` keyed
    on our player_id. Returns ``{fetched, matched, skipped}``."""
    try:
        from scrapers.owgr import fetch_rankings
    except Exception as e:
        return {"error": f"owgr scraper unavailable: {e}"}
    rows = fetch_rankings(limit=limit)
    if not rows:
        return {"fetched": 0, "matched": 0, "skipped": 0}
    full_index, last_index = _build_player_index(tour)
    conn = get_conn(tour)
    matched = skipped = 0
    for r in rows:
        pid = _resolve_player(r.get("name") or "", full_index, last_index)
        if pid is None:
            skipped += 1
            continue
        conn.execute(
            "INSERT INTO owgr_rankings "
            "(player_id, rank, last_week_rank, points_average, points_total, "
            " country_code, name, week_end_date, fetched_at) "
            "VALUES (?,?,?,?,?,?,?,?,datetime('now')) "
            "ON CONFLICT(player_id) DO UPDATE SET "
            "  rank = excluded.rank, "
            "  last_week_rank = excluded.last_week_rank, "
            "  points_average = excluded.points_average, "
            "  points_total = excluded.points_total, "
            "  country_code = excluded.country_code, "
            "  name = excluded.name, "
            "  week_end_date = excluded.week_end_date, "
            "  fetched_at = datetime('now')",
            (pid, r.get("rank"), r.get("last_week_rank"),
             r.get("points_average"), r.get("points_total"),
             r.get("country_code"), r.get("name"), r.get("week_end_date")),
        )
        matched += 1
    conn.commit()
    logger.info("[golf:%s] OWGR refresh: fetched=%d matched=%d skipped=%d",
                tour, len(rows), matched, skipped)
    return {"fetched": len(rows), "matched": matched, "skipped": skipped}


# ── DataGolf SG ─────────────────────────────────────────────

def refresh_datagolf_sg(tour: str = "pga") -> dict:
    """Pull DataGolf live SG snapshot and upsert per-player rows
    keyed on (player_id, event_name)."""
    try:
        from scrapers.datagolf_sg import fetch_live_sg
    except Exception as e:
        return {"error": f"datagolf scraper unavailable: {e}"}
    snap = fetch_live_sg(tour)
    if not snap:
        return {"fetched": 0, "matched": 0, "skipped": 0}
    full_index, last_index = _build_player_index(tour)
    conn = get_conn(tour)
    event_name = snap.get("event_name") or ""
    course_name = snap.get("course_name") or ""
    rounds_done = snap.get("current_round")
    matched = skipped = 0
    for p in snap.get("players") or []:
        pid = _resolve_player(p.get("name") or "", full_index, last_index)
        if pid is None:
            skipped += 1
            continue
        stats = p.get("stats") or {}
        # Each stat row has event_cum (the running SG total through the
        # current round). That's what we store as the headline value.
        def _ec(key):
            s = stats.get(key) or {}
            return s.get("event_cum") if isinstance(s, dict) else None
        conn.execute(
            "INSERT INTO player_sg "
            "(player_id, event_name, course_name, rounds_completed, "
            " sg_total, sg_ott, sg_app, sg_arg, sg_putt, sg_t2g, sg_bs, "
            " finish_to_par, finish_position, fetched_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now')) "
            "ON CONFLICT(player_id, event_name) DO UPDATE SET "
            "  course_name = excluded.course_name, "
            "  rounds_completed = excluded.rounds_completed, "
            "  sg_total = excluded.sg_total, "
            "  sg_ott = excluded.sg_ott, "
            "  sg_app = excluded.sg_app, "
            "  sg_arg = excluded.sg_arg, "
            "  sg_putt = excluded.sg_putt, "
            "  sg_t2g = excluded.sg_t2g, "
            "  sg_bs = excluded.sg_bs, "
            "  finish_to_par = excluded.finish_to_par, "
            "  finish_position = excluded.finish_position, "
            "  fetched_at = datetime('now')",
            (pid, event_name, course_name, rounds_done,
             _ec("sg_total"), _ec("sg_ott"), _ec("sg_app"),
             _ec("sg_arg"), _ec("sg_putt"), _ec("sg_t2g"), _ec("sg_bs"),
             p.get("total"), p.get("position")),
        )
        matched += 1
    conn.commit()
    logger.info("[golf:%s] DataGolf SG: event=%r matched=%d skipped=%d",
                tour, event_name, matched, skipped)
    return {"fetched": len(snap.get("players") or []),
             "matched": matched, "skipped": skipped,
             "event": event_name}
