"""The ONE pick selector -- the Pick Authority (MintyBot 2026-10-18, Austin "one system for
every pick... goes over all the options and chooses the best one. Period.").

select_pick gathers a game's candidate markets, keeps only PROVEN-edge markets (the edge_proven
gate), ranks the survivors by edge x prob ("both" -- Austin's 70%/+13pp beats 90%/+5pp rule),
and returns the single best HEADLINE pick, or None for NO-PLAY. Every surface (card, POTD,
scoreboard, tracker) reads this one output -- no per-surface re-selection, no stacked re-cal.
The paper/shadow layer still records ALL candidates so unproven markets keep building toward
graduation. This is the engine that ends the N-selectors / N-calibrations drift.
"""
from __future__ import annotations
from . import edge_proven

def _bt(c):
    return c.get("bet_type") or c.get("type") or ""

def _pick_text(c):
    return c.get("pick") or c.get("pick_text") or ""

def _odds(c):
    return c.get("odds")

def _prob(c):
    for k in ("prob", "model_prob", "raw_prob"):
        v = c.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return 0.0

def _edge(c):
    try:
        return float(c.get("edge") or 0.0)
    except (TypeError, ValueError):
        return 0.0

def select_pick(sport, league, candidates):
    """Return the single best PROVEN-market headline pick (edge x prob), or None = NO-PLAY.

    league: the game's own league (e.g. "wnba", "cfb", "nhl") -- passed straight through to
    edge_proven so a multi-league sport's markets are gated per-league, not blended (Pick
    Authority league-segmentation, Austin 2026-10-22).
    candidates: list of pick dicts carrying bet_type/type, pick, prob/model_prob, edge, odds.
    Matches get_best_pick's contract (drops confidence=='skip', ranks edge x prob, prefers a
    real stake over a 0u shadow) PLUS the proven-edge gate: only markets edge_proven has
    graduated are eligible, and only with positive edge right now -- graduated now means on
    THIS candidate's own fav/dog/over/under side (Pick Authority condition-segmentation,
    Austin 2026-10-22): a market proven on dogs no longer waves through its favorite side too.
    Everything else is a NO-PLAY (still paper-tracked upstream so unproven markets keep
    building toward graduation).
    """
    elig = [c for c in (candidates or [])
            if c.get("confidence") != "skip"
            and _edge(c) > 0
            and edge_proven.is_eligible(sport, league, _bt(c),
                                        edge_proven._condition(_pick_text(c), _bt(c), _odds(c)))]
    if not elig:
        return None
    elig.sort(key=lambda c: -(_edge(c) * _prob(c)))
    staked = [c for c in elig if (c.get("stake_units") or 0) > 0]
    return staked[0] if staked else elig[0]


def _with_cell(sport, league, c):
    """Attach the candidate's market-cell record (edge_proven.market_stats: n games, roi, conf =
    the posterior confidence the cell is +EV) so the "Picks · best first" rail can rank leans by
    track record (Austin 2026-10-27). No record yet -> cell_conf None (the rail reads 0.5)."""
    try:
        st = edge_proven.market_stats(sport, league, _bt(c),
                                      edge_proven._condition(_pick_text(c), _bt(c), _odds(c))) or {}
    except Exception:
        st = {}
    c["cell_n"] = st.get("n")
    c["cell_roi"] = round(float(st["roi"]), 1) if st.get("roi") is not None else None
    c["cell_conf"] = round(float(st["conf"]), 3) if st.get("conf") is not None else None
    return c


def card_lean(sport, league, candidates, best=None):
    """What a card's pick slot shows: the graduated headline (``best`` from select_pick, flagged
    graduated) when there is one, else select_lean's ungraduated lean, else None. Copies -- never
    mutates the candidates the trackers record."""
    if best:
        return _with_cell(sport, league, dict(best, graduated=True, lean=False))
    return select_lean(sport, league, candidates)


# Lean-only market pull (Austin 2026-10-31 "Yeah"): markets whose model-only leans proved overconfident show a
# lean priced at W*model + (1-W)*market. CFB alt spreads, October: model said 64%, hit 49% (n=166); log-loss improved
# at every step toward the market (W 1 .728 -> 0 .696). W=.25 keeps the strongest disagreements visible with an honest
# edge. Display only: the shadow lane still records the raw model number so the record keeps measuring the model.
_LEAN_W = {("football", "cfb", "ALT_SPREAD"): 0.25}
_ALT_VIG = 1.045   # two-sided overround on HR alt ladders; only the picked side's price is on the candidate


def _lean_pull(sport, league, c):
    w = _LEAN_W.get((sport, league, str(c.get("type") or "").upper()))
    o = c.get("odds")
    if w is None or not o:
        return c
    try:
        o = float(o)
        implied = 100.0 / (o + 100.0) if o > 0 else -o / (-o + 100.0)
        p = float(c.get("prob"))
    except (TypeError, ValueError):
        return c
    pulled = w * p + (1.0 - w) * (implied / _ALT_VIG)
    return dict(c, prob=round(pulled, 4), prob_model=p, edge=round((pulled - implied) * 100.0, 1), lean_pull_w=w)


def select_lean(sport, league, candidates):
    """The card's LEAN (Austin 2026-10-26: "all sport cards show their best lean (but make it VERY CLEAR
    that these are not graduated) until we have cells that graduate"): the same ranking as select_pick
    (edge x prob over positive-edge candidates) WITHOUT the proven-edge gate. Returns a COPY flagged
    ``lean=True`` and ``graduated`` (its own fav/dog/over/under cell proven?), or None when nothing has
    positive edge. Display only -- never a headline, POTD or tracker pick (those stay on select_pick)."""
    pos = [c for c in (_lean_pull(sport, league, c) for c in (candidates or []) if c) if _edge(c) > 0]
    if not pos:
        return None
    pos.sort(key=lambda c: -(_edge(c) * _prob(c)))
    c = dict(pos[0])
    try:
        c["graduated"] = bool(edge_proven.is_eligible(
            sport, league, _bt(c), edge_proven._condition(_pick_text(c), _bt(c), _odds(c))))
    except Exception:
        c["graduated"] = False
    c["lean"] = True
    return _with_cell(sport, league, c)
