"""CLV report -- the edge instrument (audit T2; broadened 2026-09-28).

Scores every settled pick on Closing Line Value: imp(closing) - imp(entry).
POSITIVE = we got a better price than the market's close = the sharp money moved
to our side = the single most reliable edge signal (holds up in hundreds of bets,
not thousands, unlike ROI).

Broadened from 8 main-market sports to EVERY sport x pick-type PickMint tracks:
majors (mlb/nhl/nba/nfl/cfb/wnba/tennis) sliced into main / derivative / props /
pick-of-day, plus the framework families (soccer, intl basketball, minor hockey)
aggregated per family. One source of truth for the /clv
dashboard and the React CLV Scorecard.

The `clean` subset counts ONLY picks whose close carries a capture timestamp
(`closing_odds_captured_at`, or `closing_odds_updated_at` on pick-of-day) -- the
honest, verified-pre-game number. Tables without a timestamp (derivatives, props,
framework mains) still contribute to the headline `n`, but not to `clean` -- their
close can't be verified pre-game, so they're directional only.
"""
from __future__ import annotations

import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone

_ROOT = os.path.dirname(os.path.dirname(__file__))

# Ordered pick-type buckets each sport is sliced into.
_PTYPES = ("main", "derivative", "props", "potd")


def _majors():
    """Explicit (sport, ptype, db_rel, table) for the named majors."""
    return [
        ("mlb", "main", "data/mlb.db", "picks"),
        ("mlb", "derivative", "data/mlb.db", "derivative_picks"),
        ("mlb", "props", "data/mlb.db", "player_props_picks"),
        ("mlb", "potd", "data/mlb.db", "pick_of_day"),
        ("nhl", "main", "data/nhl.db", "nhl_picks"),
        ("nhl", "derivative", "data/nhl.db", "nhl_derivative_picks"),
        ("nhl", "props", "data/nhl.db", "player_props_picks"),
        ("nhl", "potd", "data/nhl.db", "pick_of_day"),
        ("nba", "main", "data/nba.db", "nba_picks"),
        ("nba", "derivative", "data/nba.db", "nba_derivative_picks"),
        ("nba", "props", "data/nba.db", "player_props_picks"),
        ("nba", "potd", "data/nba.db", "pick_of_day"),
        ("wnba", "main", "data/basketball/wnba.db", "picks"),
        ("wnba", "props", "data/basketball/wnba.db", "player_props_picks"),
        ("wnba", "potd", "data/basketball/wnba.db", "pick_of_day"),
        ("nfl", "main", "data/football/nfl.db", "picks"),
        ("nfl", "props", "data/football/nfl_player.db", "player_props_picks"),
        ("nfl", "potd", "data/football/nfl.db", "pick_of_day"),
        ("cfb", "main", "data/football/cfb.db", "picks"),
        ("cfb", "potd", "data/football/cfb.db", "pick_of_day"),
        ("tennis", "main", "data/tennis.db", "tennis_picks"),
        ("tennis", "potd", "data/tennis.db", "pick_of_day"),
    ]


def _framework(dir_rel, sport, exclude=()):
    """Every top-level .db in a framework dir -> (sport, main|potd, rel, tbl),
    aggregated under one sport bucket (soccer, intl basketball, ...)."""
    out = []
    d = os.path.join(_ROOT, dir_rel.replace("/", os.sep))
    if not os.path.isdir(d):
        return out
    for fn in sorted(os.listdir(d)):
        if not fn.endswith(".db") or fn in exclude:
            continue
        rel = f"{dir_rel}/{fn}"
        out.append((sport, "main", rel, "picks"))
        out.append((sport, "potd", rel, "pick_of_day"))
    return out


def _build_sources():
    s = _majors()
    s += _framework("data/soccer", "soccer")
    s += _framework("data/basketball", "basketball_intl", exclude=("wnba.db",))
    s += _framework("data/hockey", "hockey_minor")
    return s


def _imp(american):
    try:
        a = float(american)
    except (TypeError, ValueError):
        return None
    if a == 0:
        return None
    return (100.0 / (a + 100.0)) if a > 0 else ((-a) / ((-a) + 100.0))


def _ret(odds, res):
    try:
        a = float(odds)
    except (TypeError, ValueError):
        return None
    r = str(res).upper()
    if r in ("P", "PUSH", "V", "VOID"):
        return 0.0
    if r in ("W", "WON", "WIN"):
        return (a / 100.0) if a > 0 else (100.0 / (-a))
    return -1.0


def _blank():
    return {"n": 0, "beat": 0, "clv": 0.0, "ret": 0.0,
            "cn": 0, "cbeat": 0, "cclv": 0.0, "cret": 0.0}


def _add(c, clv, ret, cap):
    c["n"] += 1
    c["clv"] += clv
    if clv > 0:
        c["beat"] += 1
    if ret is not None:
        c["ret"] += ret
    if cap:  # trustworthy pre-game capture
        c["cn"] += 1
        c["cclv"] += clv
        if clv > 0:
            c["cbeat"] += 1
        if ret is not None:
            c["cret"] += ret


def _fmt(c):
    n, cn = c["n"], c["cn"]
    return {
        "n": n,
        "avg_clv_pct": round(c["clv"] / n * 100, 2) if n else None,
        "beat_pct": round(c["beat"] / n * 100, 1) if n else None,
        "roi_pct": round(c["ret"] / n * 100, 1) if n else None,
        "clean_n": cn,
        "clean_avg_clv_pct": round(c["cclv"] / cn * 100, 2) if cn else None,
        "clean_beat_pct": round(c["cbeat"] / cn * 100, 1) if cn else None,
        "clean_roi_pct": round(c["cret"] / cn * 100, 1) if cn else None,
    }


def _cap_col(cols):
    for cand in ("closing_odds_captured_at", "closing_odds_updated_at"):
        if cand in cols:
            return cand
    return None


def _load(rel, tbl):
    """Yield (bet_type, odds, closing_odds, result, cap) for one settled,
    closing-captured table. Returns [] (never raises) on any problem so one
    bad table can't sink the whole report; second return value = skip reason
    (None when loaded)."""
    path = os.path.join(_ROOT, rel.replace("/", os.sep))
    if not os.path.exists(path):
        return [], "no db file"
    try:
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        if not db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (tbl,)).fetchone():
            db.close()
            return [], "no table"
        cols = [d[1] for d in db.execute(f"PRAGMA table_info({tbl})")]
        need = ("bet_type", "odds", "closing_odds", "result")
        missing = [c for c in need if c not in cols]
        if missing:
            db.close()
            return [], f"missing cols {missing}"
        capcol = _cap_col(cols)
        sel = "bet_type, odds, closing_odds, result" + (
            f", {capcol} AS cap" if capcol else "")
        rows = db.execute(
            f"SELECT {sel} FROM {tbl} WHERE result IN ('W','L','P') "
            f"AND odds IS NOT NULL AND closing_odds IS NOT NULL").fetchall()
        db.close()
    except Exception as e:
        return [], f"error: {e}"
    out = [(r["bet_type"], r["odds"], r["closing_odds"], r["result"],
            (r["cap"] if capcol else None)) for r in rows]
    return out, None


def clv_report() -> dict:
    """Segmented Closing Line Value across every sport x pick-type."""
    st = defaultdict(_blank)      # (sport, ptype)         -> counter
    stm = defaultdict(_blank)     # (sport, ptype, market) -> counter
    sall = defaultdict(_blank)    # sport                  -> counter
    grand = _blank()
    loaded, skipped = [], []

    for sport, ptype, rel, tbl in _build_sources():
        rows, reason = _load(rel, tbl)
        if reason:
            skipped.append(f"{rel}::{tbl} ({reason})")
            continue
        loaded.append(f"{rel}::{tbl} [{len(rows)}]")
        for bt, odds, close, res, cap in rows:
            ie, ic = _imp(odds), _imp(close)
            if ie is None or ic is None:
                continue
            clv = ic - ie
            ret = _ret(odds, res)
            for c in (st[(sport, ptype)], stm[(sport, ptype, bt or "?")],
                      sall[sport], grand):
                _add(c, clv, ret, cap)

    sports = sorted(sall.keys(), key=lambda s: -sall[s]["n"])
    by_sport = {}
    for sport in sports:
        types = {}
        for ptype in _PTYPES:
            key = (sport, ptype)
            if key not in st or st[key]["n"] == 0:
                continue
            markets = [
                {"market": m, **_fmt(c)}
                for (s, t, m), c in stm.items()
                if s == sport and t == ptype and c["n"] >= 3
            ]
            markets.sort(key=lambda x: -x["n"])
            types[ptype] = {"overall": _fmt(st[key]), "by_market": markets}
        by_sport[sport] = {"overall": _fmt(sall[sport]), "by_type": types}

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall": _fmt(grand),
        "by_sport": by_sport,
        "meta": {"sources_loaded": len(loaded), "sources_skipped": len(skipped),
                 "skipped": skipped},
    }


if __name__ == "__main__":
    import json
    print(json.dumps(clv_report(), indent=2))
