"""Kalshi relay — 3rd WNBA book (US-legal CFTC exchange, direct REST API).

Phase 2 (2026-08-06): ALL four WNBA markets, mapped onto Kalshi series:
  Q1_TOTAL -> KXWNBA1QTOTAL-<YYMONDD><AWAY><HOME>-<ceil(line)>   Over=YES
  TOTAL    -> KXWNBATOTAL-...-<ceil(line)>                        Over=YES
  ML       -> KXWNBAGAME-...-<TEAM>                               team=YES
  SPREAD   -> KXWNBASPREAD-...-<FAV><ceil(|line|)>  ("FAV wins by over X")
              our favorite pick (-X)  = YES on the favorite's ladder rung
              our dog pick     (+X)  = NO  on the favorite's ladder rung
Marketable IOC at the touch, sized to stake_dollars. Same place_bet shape +
exception classes as _dk_relay so _place_one treats every book uniformly.

Auth: RSA-PSS(SHA256) over ts_ms + METHOD + /trade-api/v2/path.
Config: key id from KALSHI_KEY_ID env or data/queue_placer/kalshi_key_id;
private key PEM at KALSHI_KEY_PATH env or <root>/secrets/kalshi_key.pem.
"""
from __future__ import annotations
import os
import json
import uuid
import time
import math
import base64
import datetime
import logging
import re
from pathlib import Path
from urllib import request, error

from ._relay import RelayError, RelayTransient, RelayFatal, RelayOffline  # noqa: F401

logger = logging.getLogger(__name__)

_BASE = os.environ.get("KALSHI_API_BASE", "https://external-api.kalshi.com/trade-api/v2")
_ROOT = Path(__file__).resolve().parent.parent.parent          # sports-model-bettor/
_KEY_ID_FILE = _ROOT / "data" / "queue_placer" / "kalshi_key_id"
_DEFAULT_KEY = _ROOT / "secrets" / "kalshi_key.pem"
_TIMEOUT_S = 20.0

# Max points a book line may be snapped to Kalshi's coarser strike ladder.
# Their WNBA totals step by 3, so 1.5 is the minimum that makes the
# ladder usable at all; the rung carries its own price, so edge is preserved.
_LADDER_SLOP = 1.5
# A safer-side rung may sit a full ladder step away and still be worth
# taking; the nearest-rung cap above only applies when no safe rung fits.
_SAFE_SLOP = 3.5
# A quoted price is only a price when both sides exist and are close.
_FILL_CHASE = 0.03     # cents we will pay up to complete a partial fill
_MAX_SPREAD = 0.12      # dollars between bid and ask
_MIN_VOL = 25.0         # contracts traded on that strike

_SERIES = {
    "Q1_TOTAL": "KXWNBA1QTOTAL",
    "TOTAL":    "KXWNBATOTAL",
    "SPREAD":   "KXWNBASPREAD",
    "ML":       "KXWNBAGAME",
}

# Our WNBA abbreviations -> Kalshi (which concatenates AWAY+HOME in tickers).
_TEAM = {
    "ATL": "ATL", "CHI": "CHI", "CONN": "CONN", "CON": "CONN", "DAL": "DAL",
    "IND": "IND", "LV": "LV", "LA": "LA", "MIN": "MIN", "NY": "NY", "PHX": "PHX",
    "SEA": "SEA", "WSH": "WSH", "WAS": "WSH", "GS": "GS", "GSV": "GS",
    "TOR": "TOR", "PDX": "PDX", "POR": "PDX",
}

# Pick-text tokens (cities + nicknames) -> Kalshi abbr, for ML/SPREAD picks
# like "Seattle Storm +9.5". Resolved against the game's two teams, so a
# stray token can't map outside the matchup.
_NAME_TOKENS = {
    "atlanta": "ATL", "dream": "ATL", "chicago": "CHI", "sky": "CHI",
    "connecticut": "CONN", "sun": "CONN", "dallas": "DAL", "wings": "DAL",
    "indiana": "IND", "fever": "IND", "las vegas": "LV", "vegas": "LV",
    "aces": "LV", "los angeles": "LA", "sparks": "LA", "minnesota": "MIN",
    "lynx": "MIN", "new york": "NY", "liberty": "NY", "phoenix": "PHX",
    "mercury": "PHX", "seattle": "SEA", "storm": "SEA", "washington": "WSH",
    "mystics": "WSH", "golden state": "GS", "valkyries": "GS",
    "toronto": "TOR", "tempo": "TOR", "portland": "PDX", "fire": "PDX",
}

_REJ = {"status": "rejected_market", "placed_stake_d": None, "placed_odds": None,
        "line_drift_pp": None, "hr_ticket_id": None}


_MON_I = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
          "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}


def _minutes_to_tip(pick, ticker):
    """Minutes until tip. The app's own schedule is authoritative - Kalshi's
    ticker carries no clock for WNBA (date only) and its occurrence_datetime
    disagrees with the real tip. Returns None when unknown, and callers then
    skip the window check rather than block the bet."""
    import datetime as _dt
    import sqlite3
    gid = str(pick.get("event_id") or "")
    if not gid:
        return None
    try:
        con = sqlite3.connect("data/basketball/wnba.db")
        row = con.execute("select start_time from games where game_id=?", (gid,)).fetchone()
        con.close()
    except Exception:
        return None
    if not row or not row[0]:
        return None
    s = str(row[0]).strip().replace("Z", "+00:00")
    try:
        tip = _dt.datetime.fromisoformat(s)
    except ValueError:
        return None
    if tip.tzinfo is None:
        tip = tip.replace(tzinfo=_dt.timezone.utc)
    return (tip - _dt.datetime.now(_dt.timezone.utc)).total_seconds() / 60.0


# Placement windows, minutes before tip (Austin 2026-08-08: "check maybe an hour
# and then 30m before tip off if the hour one fails"). Sweeping every minute
# would take the first price that merely clears the gate; these books thicken as
# tip approaches, so we look twice at chosen moments instead of grabbing early.
_WINDOWS = ((55.0, 65.0), (25.0, 35.0))


def _reject(reason, status="rejected_market", oid=None):
    return {**_REJ, "status": status, "reject_reason": reason, "hr_ticket_id": oid}


def _key_id():
    v = os.environ.get("KALSHI_KEY_ID")
    if v:
        return v.strip()
    try:
        return _KEY_ID_FILE.read_text().strip() or None
    except Exception:
        return None


def _key_path():
    return os.environ.get("KALSHI_KEY_PATH") or str(_DEFAULT_KEY)


def enabled() -> bool:
    """Configured only when both the key id and the private key file exist.
    (The placer also gates on a separate kalshi_enabled.flag — see _config.)"""
    return bool(_key_id()) and os.path.exists(_key_path())


def _get(path):
    req = request.Request(_BASE + path, headers={"Accept": "application/json",
                                                 "User-Agent": "pibot-kalshi/1.0"})
    with request.urlopen(req, timeout=_TIMEOUT_S) as r:
        return json.loads(r.read())


def _sign(method, full_path):
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    kid, kp = _key_id(), _key_path()
    if not kid or not os.path.exists(kp):
        raise RelayOffline("kalshi key not configured")
    with open(kp, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)
    ts = str(int(time.time() * 1000))
    sig = key.sign((ts + method + full_path).encode(),
                   padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                               salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    return kid, ts, base64.b64encode(sig).decode()


def _authed(method, path, body=None):
    # Kalshi signs METHOD + PATH ONLY - including a query string produces
    # INCORRECT_API_KEY_SIGNATURE. Every authed GET with params was silently
    # 401ing (and callers read the error body as "no positions"), which is how
    # a portfolio audit came back empty while real positions were open.
    _sign_path = path.split("?", 1)[0]
    kid, ts, sig = _sign(method, "/trade-api/v2" + _sign_path)
    headers = {"KALSHI-ACCESS-KEY": kid, "KALSHI-ACCESS-SIGNATURE": sig,
               "KALSHI-ACCESS-TIMESTAMP": ts, "Content-Type": "application/json",
               "Accept": "application/json"}
    data = json.dumps(body).encode() if body is not None else None
    req = request.Request(_BASE + path, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=_TIMEOUT_S) as r:
            return r.status, json.loads(r.read())
    except error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def _abbr(x):
    x = (x or "").strip().upper()
    return _TEAM.get(x, x)


def _team_from_pick(text, away_ab, home_ab):
    """Which of the game's two teams does the pick name? Returns abbr or None."""
    up = (text or "").upper()
    low = " " + re.sub(r"[^a-z ]+", " ", (text or "").lower()) + " "
    cands = set()
    for tok, ab in _NAME_TOKENS.items():
        if (" " + tok + " ") in low:
            cands.add(ab)
    for ab in (away_ab, home_ab):
        if re.search(r"\b%s\b" % re.escape(ab), up):
            cands.add(ab)
    hits = [a for a in cands if a in (away_ab, home_ab)]
    return hits[0] if len(hits) == 1 else None


def _event_markets(series, matchup, game_date):
    """(event_ticker, markets[], away_ab, home_ab) for a game's series event."""
    away, _, home = [p.strip() for p in (matchup or "").partition("@")]
    aab, hab = _abbr(away), _abbr(home)
    ds = game_date.strftime("%y%b%d").upper()
    want = "%s-%s%s%s" % (series, ds, aab, hab)
    q = "?series_ticker=%s&with_nested_markets=true&limit=200" % series
    for ev in (_get("/events" + q).get("events") or []):
        if ev.get("event_ticker") == want:
            return want, (ev.get("markets") or []), aab, hab
    return want, [], aab, hab


def _dollars(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _touch_cents(ticker, contract):
    m = _get("/markets/%s" % ticker).get("market") or {}
    v = m.get("yes_ask_dollars") if contract == "yes" else m.get("no_ask_dollars")
    return None if v is None else round(float(v) * 100)


def _resolve(pick):
    """Map our pick -> (market_ticker, contract 'yes'/'no') or a reject dict."""
    bt = (pick.get("bet_type") or "").upper()
    series = _SERIES.get(bt)
    if (pick.get("sport") or "").lower() != "wnba" or not series:
        return _reject("kalshi: unsupported market %r" % bt)
    text = pick.get("pick") or ""
    try:
        evt, mkts, aab, hab = _event_markets(series, pick.get("matchup") or "",
                                             datetime.date.today())
    except (error.URLError, TimeoutError, OSError) as e:
        raise RelayTransient("kalshi market lookup: %s" % e)
    if not mkts:
        return _reject("kalshi: event %s not listed" % evt)

    if bt in ("Q1_TOTAL", "TOTAL"):
        mt = re.search(r"(over|under)\s+([0-9.]+)", text, re.I)
        if not mt:
            return _reject("kalshi: cannot parse total pick %r" % text)
        side, line = mt.group(1).lower(), float(mt.group(2))
        rungs = [(abs(float(m["floor_strike"]) - line), float(m["floor_strike"]), m)
                 for m in mkts if m.get("floor_strike") is not None]
        if not rungs:
            return _reject("kalshi: no strikes listed for %s" % bt)
        rungs.sort(key=lambda x: x[0])
        # Prefer the SAFER rung, not merely the nearest (Austin 2026-08-07: "I'd
        # rather take the lower number for less profit than the higher one for
        # more"). For an OVER the safer strike is BELOW our line (easier to
        # clear); for an UNDER it is ABOVE. We took Over 162.5 when Over 159.5
        # was on the board - nearer, better priced, but a harder bet, and it
        # also loses alongside the HR/DK legs instead of covering them.
        safe = [r for r in rungs
                if (r[1] <= line if side == "over" else r[1] >= line)]
        if safe and safe[0][0] <= _SAFE_SLOP:
            dist, strike, m = safe[0]
            slop = _SAFE_SLOP          # a safer strike may sit a full step away
        else:
            dist, strike, m = rungs[0]
            slop = _LADDER_SLOP
        # Kalshi's WNBA total ladder steps by 3 points, so an exact match for a
        # book line like 161.5 usually does not exist (Austin 2026-08-07: "can we
        # put that over on Kalshi also?"). Snap to the nearest rung within
        # _LADDER_SLOP. This does NOT worsen the bet: the rung is priced off the
        # same ladder, so the edge carries across (the market's own 159.5/162.5
        # prices imply the same edge for a side our model likes at 161.5).
        # Mirrors the standing nearest-line convention on HR.
        if dist > slop:
            return _reject("kalshi: line %s not on the %s ladder (nearest %s)"
                           % (line, bt, strike))
        if dist > 0.01:
            logger.info("kalshi ladder snap: %s %s -> %s (%.1f pt)",
                        bt, line, strike, dist)
        return (m.get("ticker"), "yes" if side == "over" else "no")

    if bt == "ML":
        team = _team_from_pick(text, aab, hab)
        if not team:
            return _reject("kalshi: cannot resolve ML team from %r" % text)
        want = "%s-%s" % (evt, team)
        for m in mkts:
            if m.get("ticker") == want:
                return (want, "yes")
        return _reject("kalshi: ML market %s not listed" % want)

    if bt == "SPREAD":
        ln = re.search(r"([+-])\s*(\d+(?:\.\d+)?)", text)
        team = _team_from_pick(text, aab, hab)
        if not ln or not team:
            return _reject("kalshi: cannot parse spread pick %r" % text)
        sign, mag = ln.group(1), float(ln.group(2))
        other = hab if team == aab else aab
        # Kalshi lists the FAVORITE's "wins by over X" ladder. Our favorite
        # pick (-X) = YES on the picked team's rung; our dog pick (+X) = NO
        # on the opponent's rung (exact complement at .5 lines).
        lad_team, contract = (team, "yes") if sign == "-" else (other, "no")
        want = "%s-%s%d" % (evt, lad_team, math.ceil(mag))
        for m in mkts:
            if m.get("ticker") == want:
                fs = m.get("floor_strike")
                if fs is not None and abs(float(fs) - mag) > 0.01:
                    return _reject("kalshi: rung %s floor %s != line %s" % (want, fs, mag))
                return (want, contract)
        return _reject("kalshi: spread rung %s not listed" % want)

    return _reject("kalshi: unhandled market %r" % bt)


def place_bet(pick: dict, *, dedup_key: str) -> dict:
    """WNBA pick -> Kalshi contract, marketable IOC at the touch. Returns the
    placer's response shape; raises RelayTransient/Fatal/Offline on failure."""
    r = _resolve(pick)
    if isinstance(r, dict):          # reject dict from _resolve
        return r
    ticker, contract = r
    _mins = _minutes_to_tip(pick, ticker)
    if _mins is not None:
        if _mins < min(w[0] for w in _WINDOWS):
            return _reject("kalshi: past the last window (T-%.0fm)" % _mins)
        if not any(lo <= _mins <= hi for lo, hi in _WINDOWS):
            return _reject("kalshi: waiting for placement window (T-%.0fm)" % _mins)
    try:
        ask = _touch_cents(ticker, contract)
    except (error.URLError, TimeoutError, OSError) as e:
        raise RelayTransient("kalshi price: %s" % e)
    if not ask or ask <= 0 or ask >= 100:
        return _reject("kalshi: no ask/liquidity on %s" % ticker)
    # LIQUIDITY GATE (2026-08-08). Kalshi lists WNBA quarter-total ladders that
    # nobody trades: single-digit volume, a bid at 8c and an ask at 92c. An IOC
    # at the touch then "fills" against garbage - we paid 92c for a contract
    # worth ~66c on a market with 1 contract of volume. A price only means
    # something when there are two sides to it, so require a real bid and a
    # sane spread, and treat anything wider as no market at all.
    try:
        pick = dict(pick)
        pick["_contracts_needed"] = max(1, int(float(pick.get("stake_dollars") or 0) / (ask / 100.0)))
        _m = (_get("/markets/%s" % ticker) or {}).get("market") or {}
        # Actual placed line = this market's floor_strike (e.g. 39.5) — may differ
        # from the model's line after the ladder-snap. Record what we truly hold,
        # not the model number. None for ML (no strike). (2026-08-12)
        try:
            placed_line = float(_m["floor_strike"]) if _m.get("floor_strike") is not None else None
        except (TypeError, ValueError):
            placed_line = None
        _yb, _ya = _dollars(_m.get("yes_bid_dollars")), _dollars(_m.get("yes_ask_dollars"))
        _vol = _dollars(_m.get("volume_fp")) or 0.0
        if contract == "yes":
            _bid, _asked = _yb, _ya
        else:                                  # our NO price mirrors the YES book
            _bid, _asked = (1.0 - _ya) if _ya is not None else None, (1.0 - _yb) if _yb is not None else None
        if _bid is None or _bid <= 0.0:
            return _reject("kalshi: one-sided book on %s (no bid)" % ticker)
        _spread = (_asked - _bid) if (_asked is not None and _bid is not None) else 1.0
        if _spread > _MAX_SPREAD:
            return _reject("kalshi: spread %.0fc too wide on %s (vol %.0f)"
                           % (_spread * 100, ticker, _vol))
        # Volume is yesterday's news; SIZE AT THE TOUCH is what we can actually
        # hit right now. These ladders fill up as tip approaches (Austin
        # 2026-08-08: "there might be people placing contracts closer to game
        # time"), so a market that is empty at 4am can be perfectly tradeable at
        # 7pm. Accept it the moment there is real size resting on our side, and
        # fall back to traded volume only when sizes are not published.
        _bs = _dollars(_m.get("yes_bid_size_fp"))
        _as = _dollars(_m.get("yes_ask_size_fp"))
        _our_size = _as if contract == "yes" else _bs   # we lift the other side
        _need = float(pick.get("_contracts_needed") or 1)
        if _our_size is not None:
            if _our_size < _need:
                return _reject("kalshi: only %.0f contracts at the touch on %s (need %.0f)"
                               % (_our_size, ticker, _need))
        elif _vol < _MIN_VOL:
            return _reject("kalshi: untraded market %s (vol %.0f)" % (ticker, _vol))
    except (error.URLError, TimeoutError, OSError) as e:
        raise RelayTransient("kalshi book check: %s" % e)
    stake = float(pick.get("stake_dollars") or 0)
    contracts = int(stake / (ask / 100.0))
    _ = contracts
    if contracts < 1:
        return _reject("kalshi: stake $%.2f < 1 contract @ %dc" % (stake, ask))
    # Marketable IOC at the touch. yes -> bid at yes_ask; no -> sell YES at
    # (1 - no_ask) == yes_bid. Crossing the touch fills against resting size;
    # IOC cancels any unfilled remainder.
    pd = ask / 100.0
    kside, price = ("bid", pd) if contract == "yes" else ("ask", 1.0 - pd)
    body = {"ticker": ticker, "side": kside, "count": "%.2f" % contracts,
            "price": "%.4f" % price, "time_in_force": "immediate_or_cancel",
            "self_trade_prevention_type": "taker_at_cross",
            # DETERMINISTIC idempotency key (2026-08-12). Was uuid4() per call, so
            # every 60s queue-sweep that re-entered place_bet fired a NEW order on
            # the SAME market — 6 fires stacked to 30 contracts / ~$20 on one WNBA
            # Q1 (intended ~$5). The DB dedup only blocked the duplicate ROWS, not
            # the live orders. A stable key per (dedup_key,ticker) makes Kalshi
            # idempotent-return the existing order on a re-fire instead of placing
            # again — so a re-swept pick can never over-stake.
            "client_order_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, dedup_key + "|" + ticker))}
    st, resp = _authed("POST", "/portfolio/events/orders", body)
    if st in (401, 403):
        raise RelayFatal("kalshi auth %s" % st)
    if st >= 500 or st == 429:
        raise RelayTransient("kalshi place %s" % st)
    if st not in (200, 201):
        return _reject("kalshi %s: %s" % (st, json.dumps(resp)[:120]), status="rejected_other")
    filled = float(resp.get("fill_count") or resp.get("fill_count_fp") or 0)
    oid = resp.get("order_id")
    if filled < 1:
        return _reject("kalshi: IOC no fill (thin book)", oid=oid)
    # PARTIAL FILL (2026-08-08). An IOC takes whatever size is resting and
    # cancels the rest - today that meant 1 contract of an intended 7, and
    # because the row records as "placed" the dedup index then blocks the
    # remainder forever. Sweep up the rest immediately at the touch; if the book
    # still cannot fill us, report what we actually hold rather than the ask.
    if filled < contracts:
        want = contracts - int(filled)
        cost = filled * pd
        for _ri in range(2):
            if want < 1:
                break
            try:
                ask2 = _touch_cents(ticker, contract)
            except (error.URLError, TimeoutError, OSError):
                break
            if not ask2 or ask2 <= 0 or ask2 >= 100:
                break
            pd2 = ask2 / 100.0
            if pd2 - pd > _FILL_CHASE:          # price ran away; keep what we got
                break
            k2, px2 = ("bid", pd2) if contract == "yes" else ("ask", 1.0 - pd2)
            body2 = {"ticker": ticker, "side": k2, "count": "%.2f" % want,
                     "price": "%.4f" % px2, "time_in_force": "immediate_or_cancel",
                     "self_trade_prevention_type": "taker_at_cross",
                     # Deterministic per (dedup_key,ticker,retry-index) — a re-swept
                     # call reuses the same keys → Kalshi idempotent-returns the
                     # prior fills instead of buying more (see primary order above).
                     "client_order_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, "%s|%s|r%d" % (dedup_key, ticker, _ri)))}
            st2, resp2 = _authed("POST", "/portfolio/events/orders", body2)
            if st2 not in (200, 201):
                break
            got = float(resp2.get("fill_count") or resp2.get("fill_count_fp") or 0)
            if got <= 0:
                break
            cost += got * pd2
            filled += got
            want -= int(got)
            oid = resp2.get("order_id") or oid
        if filled > 0:
            pd = cost / filled              # blended price actually paid
    return {"status": "placed", "reject_reason": None,
            "placed_stake_d": round(filled * pd, 2),
            "placed_odds": round(1.0 / pd, 4),   # decimal odds for a $pd contract
            "line_drift_pp": None, "hr_ticket_id": oid,
            "outcome_side": contract,            # yes/no we actually bought -> authoritative at settle (2026-08-18)
            "placed_line": placed_line}


def get_balance_dollars():
    """Live Kalshi cash balance in dollars, or None on any failure (callers
    fail SAFE — the unit ladder sizes DOWN on a bad read, never up)."""
    try:
        st, resp = _authed("GET", "/portfolio/balance")
        if st != 200:
            return None
        v = resp.get("balance_dollars") or resp.get("balance")
        return float(v) if v is not None else None
    except Exception:
        return None


def poll_result(dedup_key):
    raise RelayError("kalshi placement is synchronous; no poll")


def session_pull():
    raise RelayError("kalshi manages its own auth")
