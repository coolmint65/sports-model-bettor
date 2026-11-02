"""HTTP surface for the queue auto-placer.

Endpoints:
  GET  /api/bet-queue/live-fire/status   Current mode + config snapshot
  POST /api/bet-queue/live-fire/on       Set the on-disk flag
  POST /api/bet-queue/live-fire/off      Clear the on-disk flag (halt)
  GET  /api/bet-queue/placements         Placement log (paginated)
  GET  /api/bet-queue/placements/summary Summary stats
  POST /api/bet-queue/place/sweep        Trigger a sweep manually
                                          (dry-run unless kill switch on)
  POST /api/bet-queue/place/poll         Poll pending relay responses
  GET  /api/bet-queue/relay/health       Probe the Beelink relay

Note: the on-disk flag is only HALF the kill switch — the AUTO_BET_LIVE
env var must also be set. That's intentional: even a compromised HTTP
endpoint can't flip us into live-fire without shell access.
"""
from __future__ import annotations
import logging
from fastapi import APIRouter, Query, HTTPException

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/bet-queue/live-fire/status")
def api_live_fire_status() -> dict:
    from engine.queue_placer import (
        is_live_fire_enabled, kill_flag_path, relay_url, relay_token,
        UNIT_DOLLARS, MAX_STAKE_PER_BET_DOLLARS,
        MAX_STAKED_PER_DAY_DOLLARS,
        CONSECUTIVE_LOSS_HALT, DAILY_DRAWDOWN_HALT_DOLLARS,
        LINE_DRIFT_TOLERANCE_PP,
    )
    import os
    tok = relay_token()
    return {
        "live_fire_enabled": is_live_fire_enabled(),
        "env_flag": os.environ.get("AUTO_BET_LIVE") == "1",
        "on_disk_flag": kill_flag_path().exists(),
        "flag_path": str(kill_flag_path()),
        "relay_url": relay_url(),
        "relay_secret_set": bool(tok),   # legacy field name for older UI
        "relay_token_set": bool(tok),
        "relay_token_preview": (tok[:8] + "…") if tok else None,
        "caps": {
            "unit_dollars": UNIT_DOLLARS,
            "max_stake_per_bet_dollars": MAX_STAKE_PER_BET_DOLLARS,
            "max_staked_per_day_dollars": MAX_STAKED_PER_DAY_DOLLARS,
            "consecutive_loss_halt": CONSECUTIVE_LOSS_HALT,
            "daily_drawdown_halt_dollars": DAILY_DRAWDOWN_HALT_DOLLARS,
            "line_drift_tolerance_pp": LINE_DRIFT_TOLERANCE_PP,
        },
    }


@router.post("/api/bet-queue/live-fire/on")
def api_live_fire_on() -> dict:
    """Set the on-disk flag. Live-fire still requires AUTO_BET_LIVE=1
    in the environment — this endpoint is one half of a two-key
    launch."""
    from engine.queue_placer import set_live_fire, is_live_fire_enabled
    set_live_fire(True)
    logger.warning("QUEUE PLACER: on-disk live-fire flag SET via API")
    return {
        "on_disk_flag": True,
        "live_fire_enabled": is_live_fire_enabled(),
        "note": ("Env var AUTO_BET_LIVE=1 must ALSO be set for real "
                  "money to fire. Check /live-fire/status."),
    }


@router.post("/api/bet-queue/live-fire/off")
def api_live_fire_off() -> dict:
    """Clear the on-disk flag. Halts placement immediately regardless
    of the env var."""
    from engine.queue_placer import set_live_fire, is_live_fire_enabled
    set_live_fire(False)
    logger.warning("QUEUE PLACER: on-disk live-fire flag CLEARED via API")
    return {
        "on_disk_flag": False,
        "live_fire_enabled": is_live_fire_enabled(),
    }


# ── Per-book arm/disarm ─────────────────────────────────────
# Independent of the master live-fire switch: a book only places when
# live-fire is on AND its own enable flag is set. These wrap the on-disk
# per-book flags (dk_enabled.flag / betconstruct_enabled.flag) so the UI
# can arm/disarm DK and BetConstruct (zotabet + betandplay together)
# without touching the master switch.
@router.get("/api/bet-queue/book-enable/status")
def api_book_enable_status() -> dict:
    from engine.queue_placer import _config
    return {"dk": _config.dk_enabled(), "bc": _config.betconstruct_enabled(), "kalshi": _config.kalshi_enabled()}


@router.post("/api/bet-queue/book-enable/dk/on")
def api_book_enable_dk_on() -> dict:
    from engine.queue_placer import _config
    _config.set_dk_enabled(True)
    logger.warning("QUEUE PLACER: DK book-enable flag SET via API")
    return {"dk": _config.dk_enabled(), "bc": _config.betconstruct_enabled(), "kalshi": _config.kalshi_enabled()}


@router.post("/api/bet-queue/book-enable/dk/off")
def api_book_enable_dk_off() -> dict:
    from engine.queue_placer import _config
    _config.set_dk_enabled(False)
    logger.warning("QUEUE PLACER: DK book-enable flag CLEARED via API")
    return {"dk": _config.dk_enabled(), "bc": _config.betconstruct_enabled(), "kalshi": _config.kalshi_enabled()}


@router.post("/api/bet-queue/book-enable/bc/on")
def api_book_enable_bc_on() -> dict:
    from engine.queue_placer import _config
    _config.set_betconstruct_enabled(True)
    logger.warning("QUEUE PLACER: BetConstruct book-enable flag SET via API")
    return {"dk": _config.dk_enabled(), "bc": _config.betconstruct_enabled(), "kalshi": _config.kalshi_enabled()}


@router.post("/api/bet-queue/book-enable/bc/off")
def api_book_enable_bc_off() -> dict:
    from engine.queue_placer import _config
    _config.set_betconstruct_enabled(False)
    logger.warning("QUEUE PLACER: BetConstruct book-enable flag CLEARED via API")
    return {"dk": _config.dk_enabled(), "bc": _config.betconstruct_enabled(), "kalshi": _config.kalshi_enabled()}


@router.post("/api/bet-queue/book-enable/kalshi/on")
def api_book_enable_kalshi_on() -> dict:
    from engine.queue_placer import _config
    _config.set_kalshi_enabled(True)
    logger.warning("QUEUE PLACER: Kalshi book-enable flag SET via API")
    return {"dk": _config.dk_enabled(), "bc": _config.betconstruct_enabled(), "kalshi": _config.kalshi_enabled()}


@router.post("/api/bet-queue/book-enable/kalshi/off")
def api_book_enable_kalshi_off() -> dict:
    from engine.queue_placer import _config
    _config.set_kalshi_enabled(False)
    logger.warning("QUEUE PLACER: Kalshi book-enable flag CLEARED via API")
    return {"dk": _config.dk_enabled(), "bc": _config.betconstruct_enabled(), "kalshi": _config.kalshi_enabled()}


@router.get("/api/bet-queue/market-arming")
def api_market_arming() -> dict:
    """Per-market real-money arming: which (sport, bet_type) cells are armed to
    auto-bet. wnba defaults ON (legacy), every other sport/market opt-in. The UI
    lists `markets` + toggles each via POST. Added 2026-09-02."""
    from engine.queue_placer import _config
    markets = []
    seen = set()
    # "Markets the queue allows in" = qualifying main cells (_cell_qualifies) + qualifying
    # derivatives (own gate: decided>=MIN_SETTLED, roi>=MIN_ROI). Mirrors build_queue. 2026-09-02.
    try:
        from engine import bet_queue as _bq
        cs = _bq._load_all_stats() or {}
        for sport, cells in cs.items():
            for key, cell in cells.items():
                bt = key[0] if isinstance(key, tuple) else key
                if (sport, bt) in seen:
                    continue
                try:
                    ok = _bq._cell_qualifies(cell, _bq.MIN_SETTLED, _bq.MIN_ROI)
                except Exception:
                    ok = False
                if ok:
                    seen.add((sport, bt))
                    markets.append({"sport": sport, "bet_type": bt, "armed": _config.market_armed(sport, bt)})
        from engine.derivative_tracker import get_summary as _ds
        for bt, v in (_ds("mlb") or {}).items():
            if bt == "_grand" or not isinstance(v, dict):
                continue
            dec = int(v.get("wins", 0)) + int(v.get("losses", 0))
            if dec >= _bq.MIN_SETTLED and float(v.get("roi", 0.0)) >= _bq.MIN_ROI and ("mlb", bt) not in seen:
                seen.add(("mlb", bt))
                markets.append({"sport": "mlb", "bet_type": bt, "armed": _config.market_armed("mlb", bt)})
    except Exception as e:
        logger.warning("market-arming allowed-in list failed: %s", e)
    markets.sort(key=lambda m: (m["sport"], m["bet_type"]))
    return {
        "arming": _config.list_market_arming(),
        "sports": {s: _config.sport_armed(s) for s in ("wnba", "mlb", "nba", "nhl")},
        "markets": markets,
    }


@router.post("/api/bet-queue/market-arming")
def api_set_market_arming(sport: str = Query(...),
                          bet_type: str = Query(...),
                          on: bool = Query(...)) -> dict:
    """Arm/disarm one (sport, market) cell. Arming a cell auto-arms its sport."""
    from engine.queue_placer import _config
    _config.set_market_armed(sport, bet_type, on)
    return {
        "arming": _config.list_market_arming(),
        "sports": {s: _config.sport_armed(s) for s in ("wnba", "mlb", "nba", "nhl")},
        "set": {"sport": sport, "bet_type": bet_type, "armed": _config.market_armed(sport, bet_type)},
    }


@router.get("/api/bet-queue/placements")
def api_placements(limit: int = Query(50, ge=1, le=500),
                    offset: int = Query(0, ge=0),
                    mode: str | None = None,
                    status: str | None = None,
                    include_rejected: bool = False) -> dict:
    """Placement log, most recent first. Filter by mode ('live' /
    'dry_run') or specific status ('placed', 'rejected_cap', etc.)."""
    from engine.queue_placer._schema import get_conn
    conn = get_conn()
    where = []
    args: list = []
    if mode:
        where.append("mode = ?")
        args.append(mode)
    if status:
        where.append("status = ?")
        args.append(status)
    elif not include_rejected:
        # Default view hides terminal-rejected + expired attempts (no bet
        # placed, nothing to settle) so the log shows live/settled activity
        # instead of a wall of blocked rows. Pass include_rejected=true (or
        # an explicit status=) to see them.
        where.append("status NOT LIKE 'rejected%' AND status != 'expired'")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    rows = conn.execute(
        f"SELECT id, dedup_key, sport, tour, event_id, matchup, bet_type, pick, book, "
        f"       requested_odds, requested_stake_d, "
        f"       placed_odds, placed_stake_d, line_drift_pp, "
        f"       mode, status, reject_reason, hr_ticket_id, "
        f"       result, profit_dollars, "
        f"       queued_at, submitted_at, completed_at, settled_at "
        f"FROM placements {where_sql} "
        f"ORDER BY queued_at DESC LIMIT ? OFFSET ?",
        (*args, limit, offset),
    ).fetchall()
    return {
        "rows": [dict(r) for r in rows],
        "limit": limit,
        "offset": offset,
    }


@router.get("/api/bet-queue/placements/summary")
def api_placements_summary() -> dict:
    """Aggregate stats over the placements log — total attempted,
    placed count, dry-run count, per-status breakdown, realized P/L
    on live placements."""
    from engine.queue_placer._schema import get_conn
    conn = get_conn()
    # Per-status counts (all-time)
    by_status_rows = conn.execute(
        "SELECT mode, status, COUNT(*) n FROM placements "
        "GROUP BY mode, status ORDER BY mode, status"
    ).fetchall()
    by_status = [dict(r) for r in by_status_rows]
    # Live totals
    live_row = conn.execute(
        "SELECT COUNT(*) attempted, "
        "  COUNT(CASE WHEN status = 'placed' THEN 1 END) placed, "
        "  COALESCE(SUM(CASE WHEN status IN ('placed','submitted') "
        "                     THEN placed_stake_d ELSE 0 END), 0) staked, "
        "  COALESCE(SUM(profit_dollars), 0) profit "
        "FROM placements WHERE mode = 'live'"
    ).fetchone()
    # Today's totals
    today_row = conn.execute(
        "SELECT COALESCE(SUM(CASE WHEN status IN ('placed','submitted') "
        "                          THEN placed_stake_d ELSE 0 END), 0) staked, "
        "  COALESCE(SUM(profit_dollars), 0) profit, "
        "  COUNT(CASE WHEN status = 'placed' THEN 1 END) placed "
        "FROM placements WHERE mode = 'live' "
        "  AND DATE(queued_at) = DATE('now')"
    ).fetchone()
    return {
        "by_status": by_status,
        "live": dict(live_row),
        "today": dict(today_row),
    }


@router.post("/api/bet-queue/place/sweep")
def api_place_sweep(dry_run: bool | None = None) -> dict:
    """Run one placer sweep. `dry_run=None` picks mode from the kill
    switch; `dry_run=true` forces dry-run regardless."""
    from engine.queue_placer import sweep_and_place
    return sweep_and_place(dry_run=dry_run)


@router.post("/api/bet-queue/place/poll")
def api_place_poll() -> dict:
    """Poll the relay for terminal state on queued/submitted
    placements. Called by the worker; also available for manual
    debugging."""
    from engine.queue_placer import poll_pending_placements
    return poll_pending_placements()


@router.post("/api/bet-queue/placements/settle")
def api_place_settle(sport: str | None = None) -> dict:
    """Grade placed placements — walks rows with status='placed' AND
    result IS NULL AND mode='live' and writes W/L/P/V + profit +
    settled_at. Currently only tennis has a grader wired. Called by the
    worker on a cadence; exposed here for manual triggers when
    debugging."""
    from engine.queue_placer import settle_placements
    return settle_placements(sport=sport)


@router.get("/api/bet-queue/hr-balance")
def api_hr_balance() -> dict:
    """HR account balance (cached on the relay, fed by the Pi)."""
    from engine.queue_placer import _relay
    try:
        return _relay.hr_balance()
    except Exception as e:  # pragma: no cover - defensive
        return {"balance": None, "error": str(e)}


@router.get("/api/bet-queue/kalshi-balance")
def api_kalshi_balance() -> dict:
    """Live Kalshi exchange balance (direct signed API) + the book's current
    unit-ladder tier. {balance: float|None, unit: float}."""
    from engine.queue_placer import _kalshi, _sizing
    bal = _kalshi.get_balance_dollars()
    return {"balance": bal, "unit": _sizing.unit_for_book("kalshi")}


@router.get("/api/bet-queue/dk-balance")
def api_dk_balance() -> dict:
    """DK (KY-box DraftKings) balance for the frontend. dkkbd posts the live balance to the
    Pi's dkserve; read it back over Tailscale (Pi 100.118.96.46:9098)."""
    import json
    from urllib import request as _rq
    try:
        with _rq.urlopen("http://100.118.96.46:9098/dk-balance", timeout=6) as r:
            data = json.loads((r.read().decode() or "{}"))
        amt = data.get("amount"); at = data.get("at"); age = None
        if at:
            try:
                from datetime import datetime, timezone
                t = datetime.fromisoformat(str(at).replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - t).total_seconds()
            except Exception:
                pass
        return {"balance": amt, "at": at, "ageSec": age}
    except Exception as e:  # pragma: no cover
        return {"balance": None, "error": str(e)[:120]}


@router.get("/api/bet-queue/bc-balance")
def api_bc_balance(book: str = Query(...)) -> dict:
    """BetConstruct (Zotabet / Betandplay) balance for the frontend. The
    Pi's BetConstruct service caches each book's balance; read it back
    over Tailscale (Pi 100.118.96.46:9100)."""
    if book not in ("zotabet", "betandplay"):
        raise HTTPException(status_code=400,
                            detail="book must be zotabet or betandplay")
    import json
    from urllib import request as _rq
    try:
        with _rq.urlopen(f"http://100.118.96.46:9100/balance?book={book}",
                          timeout=6) as r:
            data = json.loads((r.read().decode() or "{}"))
        amt = data.get("amount"); at = data.get("at"); age = None
        if at:
            try:
                import time
                age = time.time() - float(at) / 1000.0
            except Exception:
                pass
        return {"balance": amt, "at": at, "ageSec": age}
    except Exception as e:  # pragma: no cover
        return {"balance": None, "error": str(e)[:120]}


@router.get("/api/bet-queue/relay/health")
def api_relay_health() -> dict:
    """Probe the Beelink relay's /health endpoint. Returns whatever
    the relay ships plus a `reachable` flag."""
    from engine.queue_placer import _relay
    from engine.queue_placer import relay_url
    if not relay_url():
        return {"reachable": False, "reason": "HR_RELAY_URL not set"}
    try:
        payload = _relay.health()
        return {"reachable": True, **(payload or {})}
    except _relay.RelayError as e:
        return {"reachable": False, "reason": str(e)}


@router.post("/api/bet-queue/relay/verify")
def api_relay_verify() -> dict:
    """Send an authenticated probe to the relay and report whether
    the token is accepted. Never actually places a bet — the probe
    payload is invalid on purpose so the relay bounces it before it
    touches HR. Used from the UI's "Verify Connection" button."""
    from engine.queue_placer import _relay
    return _relay.verify_token()


@router.get("/api/bet-queue/place/resolve-preview")
def api_place_resolve_preview() -> dict:
    """Dry-run resolver preview. For every current queue pick in a
    supported sport, run `_hr_resolver.resolve` and show what HR
    selection we'd hit. Never touches the relay or places anything.

    Two ways to use it:
      - Before flipping live-fire on, sanity-check that the resolver
        finds the picks on HR's board.
      - After flipping on, if a placement rejected as
        `rejected_market` or `rejected_line`, this shows the exact
        market-type / decimal HR was quoting."""
    from engine.queue_placer import _hr_resolver, _config
    from engine.bet_queue import build_queue
    q = build_queue(unit_dollars=_config.UNIT_DOLLARS)
    picks = q.get("picks") or []
    out = []
    for p in picks:
        sport = (p.get("sport") or "").lower()
        row = {
            "sport": sport,
            "matchup": p.get("matchup"),
            "bet_type": p.get("bet_type"),
            "pick": p.get("pick"),
            "captured_odds": p.get("odds"),
            "stake_units": p.get("stake_units"),
        }
        if sport not in _hr_resolver.SUPPORTED_SPORTS:
            row["status"] = "skip_unsupported"
            out.append(row)
            continue
        try:
            resolved = _hr_resolver.resolve(p)
            row["status"] = "resolved"
            row["resolved"] = {
                "market_type":  resolved["market_type"],
                "market_id":    resolved["market_id"],
                "selection_id": resolved["selection_id"],
                "decimal":      resolved["decimal"],
                "period":       resolved["period"],
                "line":         resolved.get("line"),
                "side":         resolved.get("side"),
            }
        except _hr_resolver.LineDrift as e:
            row["status"] = "line_drift"
            row["reason"] = str(e)
        except _hr_resolver.MarketUnavailable as e:
            row["status"] = "market_unavailable"
            row["reason"] = str(e)
        except _hr_resolver.ResolveError as e:
            row["status"] = "resolve_error"
            row["reason"] = str(e)
        out.append(row)
    supported = sum(1 for r in out if r["status"] == "resolved")
    return {
        "supported_sports": sorted(_hr_resolver.SUPPORTED_SPORTS),
        "counts": {
            "total": len(out),
            "resolved": supported,
            "line_drift": sum(1 for r in out if r["status"] == "line_drift"),
            "market_unavailable": sum(1 for r in out
                                        if r["status"] == "market_unavailable"),
            "resolve_error": sum(1 for r in out
                                   if r["status"] == "resolve_error"),
            "skip_unsupported": sum(1 for r in out
                                       if r["status"] == "skip_unsupported"),
        },
        "picks": out,
    }


@router.get("/api/bet-queue/place/breakers")
def api_place_breakers(book: str = "hr") -> dict:
    """Snapshot each circuit breaker's current state so the UI can
    show 'would fire? yes/no' without having to read the placer's
    internal query logic. Each entry ships {ok, current, threshold,
    hint} — `ok=false` means this breaker would block a fresh
    placement right now."""
    from engine.queue_placer import _config
    from engine.queue_placer._circuit import (
        _sum_staked_since, _daily_realized_pnl,
        _consecutive_recent_losses,
        _today_iso,
    )
    caps = _config.caps_for(book)
    today_staked = _sum_staked_since(_today_iso(), book)
    pnl = _daily_realized_pnl(book)
    losses = _consecutive_recent_losses(book, caps['consecutive_loss_halt'] + 5)

    # TT's daily stake — for display only (contention info). Never
    # subtracted from our own daily cap.
    tt_today = 0.0
    try:
        from engine.queue_placer import _relay as _r
        tt_today = float((_r.daily_stake() or {}).get("tt") or 0.0)
    except Exception:
        pass

    out = {
        "book": book,
        "daily_cap": {
            "ok": today_staked < caps['max_staked_per_day'] - 1e-9,
            "current": round(today_staked, 2),
            "threshold": caps['max_staked_per_day'],
            "hint": f"Today we've staked ${today_staked:.2f} of a ${caps['max_staked_per_day']:.2f} daily budget.",
        },
        "daily_drawdown": {
            "ok": pnl > caps['daily_drawdown_halt'] + 1e-9,
            "current": round(pnl, 2),
            "threshold": caps['daily_drawdown_halt'],
            "hint": (f"Today's P/L is ${pnl:.2f}. Auto-betting pauses "
                      f"if it drops to ${caps['daily_drawdown_halt']:.2f}."),
        },
        "consecutive_loss": {
            "ok": losses < caps['consecutive_loss_halt'],
            "current": losses,
            "threshold": caps['consecutive_loss_halt'],
            "hint": (f"{losses} losing bet(s) in a row. Auto-betting "
                      f"pauses after {caps['consecutive_loss_halt']} straight losses."),
        },
        # Sibling app info — display-only. Not a breaker on its own.
        "tt_today_staked": tt_today,
    }

    # Optional shared-account ceiling — only reported when configured.
    ceiling = _config.TOTAL_ACCOUNT_DAILY_CEILING_DOLLARS
    if ceiling and ceiling > 0:
        combined = today_staked + tt_today
        out["shared_account_ceiling"] = {
            "ok": combined < ceiling - 1e-9,
            "current": round(combined, 2),
            "threshold": ceiling,
            "hint": (f"Sports + TT combined: ${combined:.2f} of a "
                      f"${ceiling:.2f} shared account ceiling."),
        }
    return out
