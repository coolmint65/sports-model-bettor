"""Queue placer — the orchestrator.

Reads the current bet queue, decides which picks to place, gates every
one through the circuit breakers, and either dry-runs (log only) or
POSTs to the relay. Every attempt lands in the placements table.

Public entry:
  sweep_and_place(dry_run: bool | None = None) -> dict
      Runs one full sweep. Returns a summary dict — {attempted, placed,
      rejected: {reason: count}, mode}. Called by the worker on a
      cadence (see _config.PLACER_INTERVAL_S).

Design:
  - IDEMPOTENT — dedup key + unique index prevent double-fires
  - AUDITABLE — every decision writes a placements row, even the ones
    we bail on. Rejected rows use status='rejected_*' so the log shows
    "we saw this pick but declined it because <reason>."
  - MODE-SAFE — dry-run is the default; live mode requires the two-key
    kill switch (env + on-disk flag).
"""
from __future__ import annotations
import json
import logging
from datetime import datetime, timezone

from . import _config, _relay, _dk_relay, _betconstruct_relay, _kalshi, _sizing
from ._circuit import check_can_place
from ._dedup import dedup_key
from ._schema import STATUS_VALUES, get_conn


def _utc_now_iso() -> str:
    """UTC ISO-8601 timestamp with trailing Z. Matches the frontend's
    _parseUTC parser and stays consistent with queued_at, which the
    schema defaults to SQLite's `datetime('now')` (also UTC)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

logger = logging.getLogger(__name__)


_RETRYABLE_STATUSES = frozenset({
    "rejected_offline", "rejected_halt", "rejected_other",
    # Market-side failures (HR INTERNAL_ERROR, market suspended, etc.)
    # clear on their own once HR settles — must be retryable so the
    # next sweep gets a fresh attempt.
    "rejected_market",
})


def _record(pick: dict, *, mode: str, status: str,
             reject_reason: str | None = None,
             placed_odds: int | None = None,
             placed_stake_d: float | None = None,
             line_drift_pp: float | None = None,
             hr_ticket_id: str | None = None,
             hr_response: dict | None = None,
             placed_line: float | None = None) -> int | None:
    """Insert one placements row. Returns the row id. Silently drops
    on the dedup unique-index collision (idempotent by design).

    Before inserting, sweeps out any same-day rows with retryable
    rejections for this dedup_key so the new attempt isn't blocked
    by the unique index. Terminal statuses (placed, rejected_line,
    rejected_stake, rejected_market, rejected_dedup) are NEVER
    swept — they stay in the log and block re-fires.
    """
    assert status in STATUS_VALUES, f"unknown status: {status}"
    conn = get_conn()
    key = dedup_key(pick)
    # Clear the runway if a previous same-day attempt was rejected
    # for a retryable reason. Placeholder for the retry — we'll insert
    # a fresh row below with the current status.
    placeholders = ",".join(["?"] * len(_RETRYABLE_STATUSES))
    conn.execute(
        f"DELETE FROM placements "
        f"WHERE dedup_key = ? AND DATE(queued_at) = DATE('now') "
        f"  AND status IN ({placeholders})",
        (key, *_RETRYABLE_STATUSES),
    )
    # When inserting a LIVE attempt, also clear same-key dry_run rows
    # from today. Dry-run is a preview; the real live outcome should
    # replace it, not fight it on the unique index. Never the reverse
    # (a fresh dry-run must not wipe a live row — production history
    # is authoritative over preview).
    if mode == "live":
        conn.execute(
            "DELETE FROM placements "
            "WHERE dedup_key = ? AND DATE(queued_at) = DATE('now') "
            "  AND mode = 'dry_run'",
            (key,),
        )
    # Record the ACTUAL placed line, not the model line, when the book snapped to
    # a different rung (e.g. Kalshi ladder: model Over 41.5 -> strike 39.5). This
    # is display-layer only — settlement grades off the book's ticker/line, not
    # this text — so the records match what we truly hold. (2026-08-12)
    _pick_text = pick.get("pick")
    if placed_line is not None and _pick_text:
        import re as _re
        _pick_text = _re.sub(r"\d+(?:\.\d+)?",
                             lambda _mm: format(float(placed_line), "g"),
                             _pick_text, count=1)
    try:
        cur = conn.execute(
            "INSERT INTO placements ("
            "  dedup_key, sport, event_id, bet_type, pick, matchup, "
            "  requested_odds, requested_stake_u, requested_stake_d, "
            "  placed_odds, placed_stake_d, line_drift_pp, "
            "  mode, status, reject_reason, "
            "  hr_ticket_id, hr_response_json, "
            "  submitted_at, completed_at, book, tour"
            ") VALUES (?, ?, ?, ?, ?, ?,  ?, ?, ?,  ?, ?, ?, "
            "          ?, ?, ?,  ?, ?,  ?, ?, ?, ?)",
            (
                key,
                pick.get("sport"),
                pick.get("event_id"),
                pick.get("bet_type"),
                _pick_text,
                pick.get("matchup"),
                int(pick.get("odds") or 0),
                float(pick.get("stake_units") or 0.0),
                float(pick.get("stake_dollars") or 0.0),
                placed_odds,
                placed_stake_d,
                line_drift_pp,
                mode,
                status,
                reject_reason,
                hr_ticket_id,
                json.dumps(hr_response) if hr_response else None,
                _utc_now_iso()
                    if status in ("submitted", "placed") else None,
                _utc_now_iso()
                    if status not in ("queued", "submitted") else None,
                (pick.get("book") or "hr"),
                pick.get("tour"),
            ),
        )
        return cur.lastrowid
    except Exception as e:
        # Unique-index collision on (dedup_key, DATE(queued_at)) is
        # expected when the sweep hits the same pick twice — idempotent
        # by design. Bump to WARNING because a swallowed collision
        # hides real placement failures (e.g. dry-run row shadowing a
        # live attempt), which is what we hit 2026-07-14.
        logger.warning("placement insert skipped for %s status=%s: %s",
                        key, status, e)
        return None


def _place_one(pick: dict, *, mode: str) -> str:
    """Handle a single queue pick. Returns the final status label
    written to the log. Never raises to the caller — errors are
    classified into a status."""
    pick = _sizing.apply_unit(pick)   # balance-aware per-book unit ($2/$3/$5 ladder)
    ok, reason = check_can_place(pick, mode=mode)
    if not ok:
        # Halt / cap / dedup reject — always logged so the placement
        # ledger shows the queue saw the pick and declined it.
        status_map = {
            "kill_switch": "rejected_halt",
            "offline": "rejected_offline",
            "per_bet_cap": "rejected_cap",
            "daily_cap": "rejected_cap",
            "consecutive_loss": "rejected_halt",
            "daily_drawdown": "rejected_halt",
            "dedup": "rejected_dedup",
        }
        status = status_map.get(reason, "rejected_other")
        _record(pick, mode=mode, status=status, reject_reason=reason)
        return status

    if mode == "dry_run":
        _record(pick, mode="dry_run", status="dry_run")
        return "dry_run"

    # Live mode — talk to the relay.
    key = dedup_key(pick)
    _book = (pick.get("book") or "").lower()
    if _book == "dk":
        relay = _dk_relay
    elif _book in ("zotabet", "betandplay"):
        relay = _betconstruct_relay
    elif _book == "kalshi":
        relay = _kalshi
    else:
        relay = _relay
    try:
        resp = relay.place_bet(pick, dedup_key=key)
    except _relay.RelayOffline as e:
        _record(pick, mode="live", status="rejected_offline",
                reject_reason=str(e))
        return "rejected_offline"
    except _relay.RelayTransient as e:
        # Transient errors leave the pick eligible on the next sweep.
        # We still log so the operator can see churn.
        logger.info("place transient (will retry): %s", e)
        return "queued"
    except _relay.RelayFatal as e:
        _record(pick, mode="live", status="rejected_other",
                reject_reason=str(e))
        return "rejected_other"

    # Parse the relay response.
    status_from_relay = (resp.get("status") or "").lower()
    reject_reason = resp.get("reject_reason")
    placed_odds = resp.get("placed_odds")
    placed_stake_d = resp.get("placed_stake_d")
    line_drift_pp = resp.get("line_drift_pp")
    hr_ticket_id = resp.get("hr_ticket_id")
    placed_line = resp.get("placed_line")   # actual snapped line (Kalshi), else None

    # Map relay status to our vocabulary.
    if status_from_relay == "placed":
        status = "placed"
    elif status_from_relay == "queued":
        status = "queued"
    elif status_from_relay == "submitted":
        status = "submitted"
    elif status_from_relay in ("rejected_line", "line_moved"):
        status = "rejected_line"
    elif status_from_relay in ("rejected_market", "market_unavailable",
                                "market_suspended"):
        status = "rejected_market"
    elif status_from_relay in ("rejected_stake", "insufficient_funds",
                                "bad_stake"):
        status = "rejected_stake"
    else:
        status = "rejected_other"

    _record(pick, mode="live", status=status,
            reject_reason=reject_reason,
            placed_odds=placed_odds,
            placed_stake_d=placed_stake_d,
            line_drift_pp=line_drift_pp,
            hr_ticket_id=hr_ticket_id,
            hr_response=resp,
            placed_line=placed_line)
    return status


def _dk_eligible(pick: dict) -> bool:
    """DK handles win-a-set tennis + WNBA full-game (ML/spread/total)
    AND WNBA Q1 totals - DK posts them prematch + live; placement +
    the ESPN-score grader proven 2026-08-04."""
    sport = (pick.get("sport") or "").lower()
    bt = (pick.get("bet_type") or "").upper()
    # DK win-a-set tennis retired 2026-08-12 (Austin) - DK is WNBA-only now.
    if sport == "wnba":
        return bt in ("ML", "SPREAD", "TOTAL", "Q1_TOTAL")
    return False


def _betconstruct_eligible(pick: dict) -> bool:
    """Zotabet + Betandplay (BetConstruct) place WNBA - all 6 markets
    (full-game + 1st-quarter; BetConstruct posts Q1 prematch)."""
    return ((pick.get("sport") or "").lower() == "wnba"
            and (pick.get("bet_type") or "").upper() in
            ("ML", "SPREAD", "TOTAL", "Q1_ML", "Q1_SPREAD", "Q1_TOTAL"))


def sweep_and_place(dry_run: bool | None = None) -> dict:
    """Run one placer sweep. Reads the current bet queue via
    engine.bet_queue.build_queue, then processes each pick.

    `dry_run=None` picks the mode from the kill switch: env+flag both
    on → live, otherwise dry_run.
    """
    from ..bet_queue import build_queue

    if dry_run is None:
        dry_run = not _config.is_live_fire_enabled()
    mode = "dry_run" if dry_run else "live"

    queue = build_queue(unit_dollars=_config.UNIT_DOLLARS)
    all_picks = queue.get("picks") or []
    # Filter to sports the HR resolver knows. Everything else is
    # silently skipped so unsupported-sport picks don't churn the
    # placement log every sweep. Adding a new sport to the resolver
    # flips it on here automatically.
    from . import _hr_resolver
    supported = [p for p in all_picks
                 if (p.get("sport") or "").lower() in _hr_resolver.SUPPORTED_SPORTS]
    # Per-sport arming gate: wnba is always live; any OTHER resolver-supported
    # sport must be explicitly armed (arm_<sport>.flag / env ARM_<SPORT>=1) or
    # its picks are held back. Decouples "resolver can price it" from "real
    # money authorized on it" so widening the resolver can't auto-fire a new
    # sport without a deliberate opt-in. All book passes
    # below iterate this filtered list, so the gate covers every book.
    picks = [p for p in supported
             if _config.sport_armed(p.get("sport"))
             and _config.market_armed(p.get("sport"), p.get("bet_type"))]
    skipped = len(all_picks) - len(supported)
    skipped_unarmed = len(supported) - len(picks)
    summary = {
        "mode": mode,
        "attempted": len(picks),
        "skipped_unsupported": skipped,
        "skipped_unarmed": skipped_unarmed,
        "placed": 0,
        "queued": 0,
        "dry_run": 0,
        "rejected": {},
        "dk": {},
    }
    for p in picks:
        status = _place_one(p, mode=mode)
        if status == "placed":
            summary["placed"] += 1
        elif status == "queued":
            summary["queued"] += 1
        elif status == "dry_run":
            summary["dry_run"] += 1
        elif status.startswith("rejected_") or status == "expired":
            summary["rejected"][status] = summary["rejected"].get(status, 0) + 1
    # DK second book alongside HR - win-a-set tennis only, when enabled.
    # Separate pass AFTER the HR loop so DK's slower keyboard placement never
    # delays an HR bet; capped per sweep so a post-enable backlog can't drain
    # in one blocking burst.
    if _config.dk_enabled():
        dk_fired = 0
        for p in picks:
            if not _dk_eligible(p):
                continue
            if dk_fired >= _config.MAX_DK_PER_SWEEP:
                summary["dk"]["deferred_cap"] = summary["dk"].get("deferred_cap", 0) + 1
                continue
            status = _place_one({**p, "book": "dk"}, mode=mode)
            summary["dk"][status] = summary["dk"].get(status, 0) + 1
            if status not in ("rejected_dedup", "rejected_cap", "rejected_halt"):
                dk_fired += 1
    # BetConstruct books (Zotabet + Betandplay) alongside HR/DK - WNBA all 6
    # markets, when enabled. Separate pass per book (each is its own account).
    if _config.betconstruct_enabled():
        for _bcbook in ("zotabet", "betandplay"):
            bc_fired = 0
            for p in picks:
                if not _betconstruct_eligible(p):
                    continue
                if bc_fired >= _config.MAX_BC_PER_SWEEP:
                    summary.setdefault(_bcbook, {})["deferred_cap"] = summary.setdefault(_bcbook, {}).get("deferred_cap", 0) + 1
                    continue
                status = _place_one({**p, "book": _bcbook}, mode=mode)
                summary.setdefault(_bcbook, {})[status] = summary.setdefault(_bcbook, {}).get(status, 0) + 1
                if status not in ("rejected_dedup", "rejected_cap", "rejected_halt"):
                    bc_fired += 1
    # Kalshi (US-legal CFTC) alongside HR/DK - WNBA Q1 totals only, when
    # enabled. Own account/caps; the relay self-gates on key presence.
    if _config.kalshi_enabled() and _kalshi.enabled():
        ka_fired = 0
        for p in picks:
            sp = (p.get("sport") or "").lower()
            bt = (p.get("bet_type") or "").upper()
            if sp != "wnba" or bt not in ("ML", "SPREAD", "TOTAL", "Q1_TOTAL"):
                continue
            if ka_fired >= _config.MAX_KALSHI_PER_SWEEP:
                summary.setdefault("kalshi", {})["deferred_cap"] = summary.setdefault("kalshi", {}).get("deferred_cap", 0) + 1
                continue
            status = _place_one({**p, "book": "kalshi"}, mode=mode)
            summary.setdefault("kalshi", {})[status] = summary.setdefault("kalshi", {}).get(status, 0) + 1
            if status not in ("rejected_dedup", "rejected_cap", "rejected_halt"):
                ka_fired += 1
    # POTD pass (Austin 2026-08-06): auto-bet the daily WNBA Pick of the Day
    # on every armed book that carries its market. Sourced from the POTD
    # subsystem, NOT the queue — POTD's verified record (52-33, +14.5u flat
    # over 85 picks) is its qualification, so it skips cell-stat gating.
    # Every money safety still applies via _place_one: two-key live-fire,
    # dedup (line-stripped — collapses a POTD duplicating a queue pick on
    # the same game+market+side), per-book caps, halts, HR line-drift
    # guard. Skips once the pick is settled; a mid-game attempt self-limits
    # via MarketUnavailable / line-drift rejections.
    if _config.potd_enabled():
        try:
            from ..pick_of_day._read import get_today_potd
            _potd = get_today_potd("wnba")
        except Exception as e:
            logger.info("potd read failed: %s", e)
            _potd = None
    else:
        _potd = None
    if _potd and not _potd.get("result"):
        _su = float(_potd.get("stake_units") or 1.0)
        _pp = {
            "sport": "wnba",
            "bet_type": (_potd.get("bet_type") or "").upper(),
            "pick": _potd.get("pick") or "",
            "matchup": _potd.get("matchup") or "",
            "odds": _potd.get("odds"),
            "model_prob": _potd.get("model_prob"),
            "edge": _potd.get("edge"),
            "event_id": _potd.get("game_id"),
            "stake_units": _su,
            "stake_dollars": round(_su * (_config.UNIT_DOLLARS or 5.0), 2),
            "date": _potd.get("date"),
            "potd": True,
        }
        _potd_books = [("hr", True),
                       ("dk", _config.dk_enabled() and _dk_eligible(_pp)),
                       ("kalshi", _config.kalshi_enabled() and _kalshi.enabled()
                        and _pp["bet_type"] in ("ML", "SPREAD", "TOTAL", "Q1_TOTAL"))]
        for _bk, _armed in _potd_books:
            if not _armed:
                continue
            status = _place_one({**_pp, "book": _bk}, mode=mode)
            _k = "%s:%s" % (_bk, status)
            summary.setdefault("potd", {})[_k] = summary.setdefault("potd", {}).get(_k, 0) + 1
    logger.info("queue placer sweep: %s", summary)
    return summary


def poll_pending_placements() -> dict:
    """Sweep queued/submitted placements and refresh their status by
    polling the relay's /place-result. Called by the worker on the same
    cadence so terminal state (placed / rejected) lands on our side
    without waiting for the next sweep."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, dedup_key FROM placements "
        "WHERE mode = 'live' AND status IN ('queued', 'submitted')"
    ).fetchall()
    refreshed = 0
    for r in rows:
        try:
            resp = _relay.poll_result(r["dedup_key"])
        except _relay.RelayError as e:
            logger.debug("poll_result %s failed: %s", r["dedup_key"], e)
            continue
        status_from_relay = (resp.get("status") or "").lower()
        if status_from_relay not in ("placed", "rejected_line",
                                      "rejected_market", "rejected_stake",
                                      "rejected_other", "expired"):
            continue   # still queued / submitted
        conn.execute(
            "UPDATE placements SET status = ?, reject_reason = ?, "
            "  placed_odds = COALESCE(?, placed_odds), "
            "  placed_stake_d = COALESCE(?, placed_stake_d), "
            "  line_drift_pp = COALESCE(?, line_drift_pp), "
            "  hr_ticket_id = COALESCE(?, hr_ticket_id), "
            "  hr_response_json = ?, "
            "  completed_at = datetime('now') "
            "WHERE id = ?",
            (
                status_from_relay,
                resp.get("reject_reason"),
                resp.get("placed_odds"),
                resp.get("placed_stake_d"),
                resp.get("line_drift_pp"),
                resp.get("hr_ticket_id"),
                json.dumps(resp),
                r["id"],
            ),
        )
        refreshed += 1
    return {"refreshed": refreshed, "pending": len(rows) - refreshed}
