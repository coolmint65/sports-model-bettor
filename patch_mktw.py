import sys, os, difflib

# usage: patch_mktw.py <mode:dry|apply> <edge_proven.py> <_picks.py> <_derivatives.py>
mode = sys.argv[1]
EDGE_P = sys.argv[2]
PICKS_P = sys.argv[3]
DERIV_P = sys.argv[4]

I16 = " " * 16

def conv(s):
    # typed strings use \n; files are CRLF. Convert so matches/writes stay CRLF.
    return s.replace("\n", "\r\n")

# ---- edge_proven.py ----
EDGE = [
    # (1) two module constants right after the _CONF_FLOORS dict
    (
        '    ("nhl", "nhl", "O/U", "under"): 0.66,\n}\nTTL = 600          # recompute the map at most every 10 min',
        '    ("nhl", "nhl", "O/U", "under"): 0.66,\n}\n'
        '_W_PROVEN = 0.50    # eligible bucket: model keeps its current (earned) say\n'
        '_W_UNPROVEN = 0.15  # unproven/-EV bucket: defer hard to the de-vigged market\n'
        'TTL = 600          # recompute the map at most every 10 min',
        1,
    ),
    # (2) market_weight function, inserted after is_eligible, before market_stats
    (
        '    return bool((_map().get((str(sport), _norm_lg(league), _norm_bt(bet_type), _norm_cond(condition))) or {}).get("eligible"))\n'
        '\n'
        'def market_stats(sport, league, bet_type, condition):',
        '    return bool((_map().get((str(sport), _norm_lg(league), _norm_bt(bet_type), _norm_cond(condition))) or {}).get("eligible"))\n'
        '\n'
        'def market_weight(sport, league, bet_type, pick_text="", odds=None):\n'
        '    """Per-bucket market-anchor weight. A bucket with PROVEN edge (is_eligible)\n'
        '    keeps the model\'s full say (_W_PROVEN); everything unproven defers hard to the\n'
        '    market (_W_UNPROVEN). condition is derived from pick_text/odds exactly like the gate."""\n'
        '    cond = _norm_cond(_condition(pick_text, bet_type, odds))\n'
        '    cell = _map().get((str(sport), _norm_lg(league), _norm_bt(bet_type), cond))\n'
        '    return _W_PROVEN if (cell and cell.get("eligible")) else _W_UNPROVEN\n'
        '\n'
        'def market_stats(sport, league, bet_type, condition):',
        1,
    ),
]

# ---- _picks.py ----
PICKS = [
    # import
    (
        'from ..market_blend import anchor_to_market as _anchor',
        'from ..market_blend import anchor_to_market as _anchor\n'
        'from ..selection.edge_proven import market_weight',
        1,
    ),
    # ML block
    (
        I16 + '"type": "ML",\n' + I16 + '"pick": label,\n' + I16 + '"raw_prob": _anchor(prob, american, opp),',
        I16 + '"type": "ML",\n' + I16 + '"pick": label,\n' + I16 + '"raw_prob": _anchor(prob, american, opp, w=market_weight("football", league, "ML", label, american)),',
        1,
    ),
    # SPREAD block
    (
        I16 + '"type": "SPREAD",\n' + I16 + '"pick": f"{label_team} {sign}{line}",\n' + I16 + '"raw_prob": _anchor(prob, american, opp),',
        I16 + '"type": "SPREAD",\n' + I16 + '"pick": f"{label_team} {sign}{line}",\n' + I16 + '"raw_prob": _anchor(prob, american, opp, w=market_weight("football", league, "SPREAD", f"{label_team} {sign}{line}", american)),',
        1,
    ),
    # TOTAL block
    (
        I16 + '"type": "TOTAL",\n' + I16 + '"pick": label,\n' + I16 + '"raw_prob": _anchor(prob, american, opp),',
        I16 + '"type": "TOTAL",\n' + I16 + '"pick": label,\n' + I16 + '"raw_prob": _anchor(prob, american, opp, w=market_weight("football", league, "TOTAL", label, american)),',
        1,
    ),
    # both derivative callers (price_samples + price_period) end identically -> thread league to BOTH
    (
        'away_abbr=odds.get("away_abbr")):',
        'away_abbr=odds.get("away_abbr"), league=league):',
        2,
    ),
]

# ---- _derivatives.py ----  (order matters: add-closure anchor before _winner anchor)
DERIV = [
    # import
    (
        'anchor_prob as _anchor_p)',
        'anchor_prob as _anchor_p)\nfrom ..selection.edge_proven import market_weight',
        1,
    ),
    # price_samples signature: + league=None
    (
        'home_abbr="HOME", away_abbr="AWAY"):',
        'home_abbr="HOME", away_abbr="AWAY", league=None):',
        1,
    ),
    # add() closure anchor (unique via its comment)
    (
        '_anchor(float(prob), odds, opp_odds)  # defer to de-vigged market (MintyBot 2026-10-16)',
        '_anchor(float(prob), odds, opp_odds, w=market_weight("football", league, t, pick, odds))  # per-bucket CLV weight (MintyBot 2026-10-22)',
        1,
    ),
    # price_period signature: + league=None
    (
        'away_abbr="AWAY", seed=1337):',
        'away_abbr="AWAY", seed=1337, league=None):',
        1,
    ),
    # price_samples(...) call inside price_period: + league=league
    (
        'home_abbr=home_abbr, away_abbr=away_abbr)',
        'home_abbr=home_abbr, away_abbr=away_abbr, league=league)',
        1,
    ),
    # _winner home call
    (
        'ph / dec, ho, ao)',
        'ph / dec, ho, ao, league=league)',
        1,
    ),
    # _winner away call
    (
        'pa / dec, ao, ho)',
        'pa / dec, ao, ho, league=league)',
        1,
    ),
    # _winner3 call
    (
        'dv[i] if dv else None)',
        'dv[i] if dv else None, league=league)',
        1,
    ),
    # _winner def
    (
        'def _winner(out, t, pick, prob, odds, opp_odds):',
        'def _winner(out, t, pick, prob, odds, opp_odds, league=None):',
        1,
    ),
    # _winner anchor (unique AFTER the add-closure anchor edit above)
    (
        'prob = _anchor(float(prob), odds, opp_odds)',
        'prob = _anchor(float(prob), odds, opp_odds, w=market_weight("football", league, t, pick, odds))',
        1,
    ),
    # _winner3 def
    (
        'def _winner3(out, t, pick, prob, odds, devig):',
        'def _winner3(out, t, pick, prob, odds, devig, league=None):',
        1,
    ),
    # _winner3 anchor
    (
        '_anchor_p(float(prob), devig) if devig is not None else float(prob)',
        '_anchor_p(float(prob), devig, w=market_weight("football", league, t, pick, odds)) if devig is not None else float(prob)',
        1,
    ),
]


def apply_file(path, repls):
    with open(path, "r", newline="") as f:
        orig = f.read()
    cur = orig
    for i, (old, new, expect) in enumerate(repls):
        o = conv(old); n = conv(new)
        c = cur.count(o)
        if c != expect:
            raise SystemExit("[FAIL] %s repl#%d: expected %d occurrence(s), got %d of:\n%r" % (path, i, expect, c, o))
        cur = cur.replace(o, n)
    return orig, cur


def main():
    for path, repls in ((EDGE_P, EDGE), (PICKS_P, PICKS), (DERIV_P, DERIV)):
        orig, new = apply_file(path, repls)
        if mode == "dry":
            diff = difflib.unified_diff(
                orig.replace("\r\n", "\n").splitlines(True),
                new.replace("\r\n", "\n").splitlines(True),
                os.path.basename(path) + " (orig)", os.path.basename(path) + " (new)")
            sys.stdout.write("".join(diff))
            sys.stdout.write("\n")
            with open(path + ".patched", "w", newline="") as f:
                f.write(new)
            # assert still CRLF
            with open(path + ".patched", "rb") as f:
                raw = f.read()
            assert b"\r\n" in raw and b"\n" not in raw.replace(b"\r\n", b""), "CRLF integrity broken in " + path
        elif mode == "apply":
            with open(path + ".bak-mktw", "w", newline="") as f:
                f.write(orig)
            with open(path, "w", newline="") as f:
                f.write(new)
            with open(path, "rb") as f:
                raw = f.read()
            assert b"\r\n" in raw and b"\n" not in raw.replace(b"\r\n", b""), "CRLF integrity broken in " + path
            print("[OK] patched %s  (+backup %s.bak-mktw)" % (path, path))
    print("[DONE] mode=%s" % mode)


if __name__ == "__main__":
    main()
