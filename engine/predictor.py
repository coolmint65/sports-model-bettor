"""Sport-agnostic Predictor protocol (A5).

The architecture goal: factor / MC / GBM / ensemble all become
implementations of a single ``Predictor`` interface. Adding a new
sport (or a new variant of an existing predictor) = new
implementation, not a new orchestration code path.

Today every sport has its own ``predict_full`` function with subtly
different signatures and return shapes:

    engine.nba_predict.predict_full(home_abbr, away_abbr, spread=, total=, ...)
    engine.mlb_predict.predict_matchup(home_id, away_id, ...)
    engine.basketball._predict.predict_full(league, home_abbr, away_abbr, ...)
    engine.tennis_predict.predict_match(tour, p1_id, p2_id, ...)

The first wave of consolidation: define the protocol + adapter wrappers
around the existing modules. Caller switches from
``engine.nba_predict.predict_full(...)`` to
``predictor.predict(GameContext(...))``. Underlying logic unchanged.

A6 lands the actual file consolidation (sports/{sport}/...). A7 unifies
the train/infer code paths. A5's job is the contract.

Protocol:
    Predictor.predict(game) -> Prediction

Game shape:
    GameContext(sport, league=None, home, away, spread=None, total=None,
                start_time=None, season=None, extras=None)

Prediction shape:
    Prediction(home_expected, away_expected, margin, total,
               ml_home, ml_away, spread_cover_prob=None, over_prob=None,
               signals=None, reasoning=None, model_version=None,
               source='factor'|'gbm'|'mc'|'ensemble', extras=None)

Concrete implementations live alongside the protocol:
    - FactorPredictor       (sport-aware dispatch wrapper)
    - BasketballFactorPredictor (basketball framework via predict_full)
    - GBMPredictor          (XGBoost wrapper)
    - MCPredictor           (Monte Carlo sampler)
    - EnsemblePredictor     (blend via weights)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


# ── Game context ─────────────────────────────────────────────

@dataclass
class GameContext:
    """Input to every Predictor. Sport-agnostic enough that the same
    GameContext shape feeds NBA / NHL / MLB / Tennis / Basketball-
    framework / future sports without per-sport branching at the
    call site.

    ``home`` and ``away`` are abbreviation strings for team sports;
    tennis adapters interpret them as p1/p2 player abbreviations
    (the protocol itself is sport-blind)."""
    sport: str
    home: str
    away: str
    league: str | None = None        # basketball framework sub-key
    spread: float | None = None      # posted home spread (negative=home fav)
    total: float | None = None       # posted O/U total
    start_time: str | None = None    # ISO-8601 with TZ
    season: int | None = None
    extras: dict = field(default_factory=dict)


@dataclass
class Prediction:
    """Standardized output every Predictor returns.

    Some predictors won't fill every field — e.g. a pure-classifier GBM
    only produces ml_home/ml_away. Callers handle ``None`` values
    gracefully; the EnsemblePredictor weights only present fields.

    ``signals`` is freeform per-implementation context (component-model
    breakdowns, confidence intervals, etc). The framework's existing
    ``ensemble`` block lives here.

    ``model_version`` stamps which A2 version produced this prediction
    so decision events tie back to the version that generated them.
    """
    sport: str
    home: str
    away: str
    home_expected: float | None = None
    away_expected: float | None = None
    margin: float | None = None
    total: float | None = None
    ml_home: float | None = None
    ml_away: float | None = None
    spread_cover_prob: float | None = None
    over_prob: float | None = None
    signals: dict | None = None
    reasoning: list[str] = field(default_factory=list)
    model_version: str | None = None
    source: str = "factor"           # factor / gbm / mc / ensemble
    error: str | None = None
    extras: dict = field(default_factory=dict)


# ── Protocol ────────────────────────────────────────────────

@runtime_checkable
class Predictor(Protocol):
    """Anything with .predict(GameContext) -> Prediction is a Predictor.

    Structural typing via Protocol means implementations don't have to
    inherit. NBA's existing ``predict_full`` becomes a Predictor by
    wrapping in a thin adapter — no rewrites required."""
    name: str

    def predict(self, game: GameContext) -> Prediction:
        ...


# ── Concrete implementations ──────────────────────────────────

class BasketballFactorPredictor:
    """Wraps ``engine.basketball._predict.predict_full`` so the
    framework's factor model implements the protocol. WNBA / Euroleague
    already route through this entry point; the only change is the
    return-type adapter."""
    name = "basketball_factor"

    def predict(self, game: GameContext) -> Prediction:
        from .basketball import predict_full as _predict_full
        league = game.league or game.sport
        try:
            raw = _predict_full(
                league=league,
                home_abbr=game.home,
                away_abbr=game.away,
                spread=game.spread,
                total=game.total,
                season=game.season,
            )
        except Exception as e:
            logger.warning("BasketballFactorPredictor failed for %s: %s",
                           league, e)
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, error=str(e),
                                source="factor")
        if not isinstance(raw, dict) or raw.get("error"):
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away,
                                error=(raw or {}).get("error"),
                                source="factor")
        ensemble = raw.get("ensemble")
        return Prediction(
            sport=game.sport,
            home=game.home,
            away=game.away,
            home_expected=raw.get("home_expected"),
            away_expected=raw.get("away_expected"),
            margin=raw.get("predicted_margin"),
            total=raw.get("predicted_total"),
            ml_home=raw.get("ml_home"),
            ml_away=raw.get("ml_away"),
            spread_cover_prob=raw.get("spread_cover_prob"),
            over_prob=raw.get("over_prob"),
            signals={
                "ensemble": ensemble,
                "factors": raw.get("factors"),
                "constants_source": raw.get("constants_source"),
            },
            reasoning=raw.get("reasoning") or [],
            source="factor",
        )


class NBAFactorPredictor:
    """Wraps engine.nba_predict.predict_full. NBA stays on its native
    module — the protocol just unifies the call shape."""
    name = "nba_factor"

    def predict(self, game: GameContext) -> Prediction:
        from .nba_predict import predict_full
        try:
            raw = predict_full(
                game.home, game.away,
                spread=game.spread, total=game.total,
                season=game.season,
                **(game.extras.get("nba_kwargs") or {}),
            )
        except Exception as e:
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, error=str(e),
                                source="factor")
        if not isinstance(raw, dict) or raw.get("error"):
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away,
                                error=(raw or {}).get("error"),
                                source="factor")
        return Prediction(
            sport=game.sport,
            home=game.home,
            away=game.away,
            home_expected=raw.get("home_expected"),
            away_expected=raw.get("away_expected"),
            margin=raw.get("predicted_margin"),
            total=raw.get("predicted_total"),
            ml_home=raw.get("ml_home"),
            ml_away=raw.get("ml_away"),
            spread_cover_prob=raw.get("spread_cover_prob"),
            over_prob=raw.get("over_prob"),
            signals={"factors": raw.get("factors")},
            reasoning=raw.get("reasoning") or [],
            source="factor",
        )


class MLBFactorPredictor:
    """Wraps engine.mlb_predict.predict_matchup. The MLB legacy module
    keys on integer team_id, so the adapter resolves the abbreviation
    via engine.mlb_db. Pitchers + venue are pulled from extras when
    the caller supplies them; otherwise the predictor falls back to
    its own default (probable starter for today)."""
    name = "mlb_factor"

    def predict(self, game: GameContext) -> Prediction:
        from .mlb_predict import predict_matchup
        from .db import get_team_by_abbr as get_team_by_abbreviation
        try:
            home_t = get_team_by_abbreviation(game.home)
            away_t = get_team_by_abbreviation(game.away)
        except Exception as e:
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, error=str(e),
                                source="factor")
        if not home_t or not away_t:
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away,
                                error=f"team lookup failed ({game.home} / {game.away})",
                                source="factor")
        try:
            raw = predict_matchup(
                home_team_id=int(home_t["mlb_id"]),
                away_team_id=int(away_t["mlb_id"]),
                home_pitcher_id=(game.extras or {}).get("home_pitcher_id"),
                away_pitcher_id=(game.extras or {}).get("away_pitcher_id"),
                venue=(game.extras or {}).get("venue"),
                backtest=bool((game.extras or {}).get("backtest", False)),
            )
        except Exception as e:
            logger.warning("MLBFactorPredictor failed for %s@%s: %s",
                           game.away, game.home, e)
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, error=str(e),
                                source="factor")
        if not isinstance(raw, dict) or raw.get("error"):
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away,
                                error=(raw or {}).get("error"),
                                source="factor")
        es = raw.get("expected_score") or {}
        wp = raw.get("win_prob") or {}
        return Prediction(
            sport=game.sport,
            home=game.home,
            away=game.away,
            home_expected=es.get("home"),
            away_expected=es.get("away"),
            margin=raw.get("spread"),
            total=raw.get("total"),
            ml_home=wp.get("home"),
            ml_away=wp.get("away"),
            signals={
                "f5": raw.get("f5"),
                "first_inning": raw.get("first_inning"),
                "innings": raw.get("innings"),
                "park_factor": raw.get("park_factor"),
                "umpire": raw.get("umpire"),
                "weather_adj": raw.get("weather_adj"),
                "confidence": raw.get("confidence"),
                "h2h": raw.get("h2h"),
                "lineups": raw.get("lineups"),
            },
            reasoning=raw.get("reasoning") or [],
            source="factor",
        )


class NHLFactorPredictor:
    """Wraps engine.nhl_predict.predict_matchup. NHL keys on the
    JSON-file stem ("maple_leafs"), so the adapter resolves the
    abbreviation via engine.data.list_teams. Goalies optional via
    extras['home_goalie_id'] / extras['away_goalie_id']."""
    name = "nhl_factor"

    _abbr_to_key: dict[str, str] | None = None

    @classmethod
    def _resolve_key(cls, abbr: str) -> str | None:
        if cls._abbr_to_key is None:
            from .data import list_teams, load_team
            mapping: dict[str, str] = {}
            for t in list_teams("NHL"):
                full = load_team("NHL", t["key"]) or {}
                ab = (full.get("abbreviation") or "").upper()
                if ab:
                    mapping[ab] = t["key"]
            cls._abbr_to_key = mapping
        return cls._abbr_to_key.get((abbr or "").upper())

    def predict(self, game: GameContext) -> Prediction:
        from .nhl_predict import predict_matchup
        home_key = self._resolve_key(game.home)
        away_key = self._resolve_key(game.away)
        if not (home_key and away_key):
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away,
                                error=f"team key resolution failed "
                                       f"({game.home} / {game.away})",
                                source="factor")
        try:
            raw = predict_matchup(
                home_key=home_key,
                away_key=away_key,
                home_goalie_id=(game.extras or {}).get("home_goalie_id"),
                away_goalie_id=(game.extras or {}).get("away_goalie_id"),
                backtest=bool((game.extras or {}).get("backtest", False)),
            )
        except Exception as e:
            logger.warning("NHLFactorPredictor failed for %s@%s: %s",
                           game.away, game.home, e)
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, error=str(e),
                                source="factor")
        if not isinstance(raw, dict):
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away,
                                error="predictor returned no result",
                                source="factor")
        es = raw.get("expected_score") or {}
        wp = raw.get("win_prob") or {}
        return Prediction(
            sport=game.sport,
            home=game.home,
            away=game.away,
            home_expected=es.get("home"),
            away_expected=es.get("away"),
            margin=raw.get("spread"),
            total=raw.get("total"),
            ml_home=wp.get("home"),
            ml_away=wp.get("away"),
            signals={
                "puck_line": raw.get("puck_line"),
                "margin_probs": raw.get("margin_probs"),
                "regulation_draw_prob": raw.get("regulation_draw_prob"),
                "goalie_matchup": raw.get("goalie_matchup"),
                "phase": raw.get("phase"),
            },
            reasoning=raw.get("reasoning") or [],
            source="factor",
        )


class GBMPredictor:
    """Adapter around the per-sport GBM trainer's ``predict()``.
    Currently NBA / MLB / NHL each have separate train.py entries; the
    adapter takes ``sport`` + ``targets`` and returns a Prediction with
    the GBM heads filled in. Other fields stay None — caller should
    blend with a factor predictor for the full shape."""
    name = "gbm"

    def __init__(self, targets: list[str] | None = None):
        self.targets = targets or ["home_win", "margin", "total_points"]

    def predict(self, game: GameContext) -> Prediction:
        sport = (game.sport or "").lower()
        # Basketball framework leagues + NBA all flow through engine.gbm.predict
        # via predict_nba (NBA DB) or basketball._gbm (per-league shim).
        if sport in ("wnba", "ncaam", "ncaaw", "euroleague") or game.league:
            return self._predict_basketball_framework(game)
        if sport == "nba":
            return self._predict_nba(game)
        if sport == "mlb":
            return self._predict_mlb(game)
        if sport == "nhl":
            return self._predict_nhl(game)
        return Prediction(sport=game.sport, home=game.home,
                            away=game.away, source="gbm",
                            error=f"GBMPredictor: no GBM path for sport {sport!r}")

    def _predict_basketball_framework(self, game: GameContext) -> Prediction:
        from datetime import datetime
        from .basketball._features import extract_features
        from .basketball._gbm import predict as gbm_predict
        from .basketball._db import get_conn, teams_table
        league = game.league or game.sport
        conn = get_conn(league)
        tbl = teams_table(league)
        h = conn.execute(
            f"SELECT id FROM {tbl} WHERE abbreviation = ?", (game.home,),
        ).fetchone()
        a = conn.execute(
            f"SELECT id FROM {tbl} WHERE abbreviation = ?", (game.away,),
        ).fetchone()
        if not (h and a):
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, source="gbm",
                                error="team lookup failed")
        feats = extract_features(league, {
            "home_team_id": h["id"], "away_team_id": a["id"],
            "date": (game.start_time or datetime.now().isoformat())[:10],
            "season": game.season,
        })
        if not feats:
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, source="gbm",
                                error="feature extraction failed")
        ml = gbm_predict(league, "home_win", feats)
        margin = gbm_predict(league, "margin", feats)
        total = gbm_predict(league, "total_points", feats)
        return Prediction(
            sport=game.sport, home=game.home, away=game.away,
            ml_home=ml,
            ml_away=(1 - ml) if ml is not None else None,
            margin=margin,
            total=total,
            source="gbm",
            signals={"features_used": list(feats.keys())[:8]},
        )

    def _predict_nba(self, game: GameContext) -> Prediction:
        from datetime import datetime
        from .nba_db import get_conn, get_nba_team_by_abbr
        from .gbm.predict import predict_nba
        h = get_nba_team_by_abbr(game.home)
        a = get_nba_team_by_abbr(game.away)
        if not (h and a):
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, source="gbm",
                                error=f"team lookup failed ({game.home}/{game.away})")
        out = predict_nba(get_conn(), {
            "home_team_id": h["id"], "away_team_id": a["id"],
            "date": (game.start_time or datetime.now().isoformat())[:10],
            "season": game.season,
        })
        if "error" in out:
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, source="gbm",
                                error=out["error"])
        return Prediction(
            sport=game.sport, home=game.home, away=game.away,
            ml_home=out.get("home_win"),
            ml_away=(1 - out["home_win"]) if "home_win" in out else None,
            margin=out.get("margin"),
            total=out.get("total_points"),
            source="gbm",
            signals={k: v for k, v in out.items()
                       if k.startswith("q1_") or k == "model_trained_at"},
        )

    def _predict_mlb(self, game: GameContext) -> Prediction:
        from datetime import datetime
        from .db import get_team_by_abbr as get_team_by_abbreviation
        from .db import get_conn
        from .gbm.predict import predict_mlb
        h = get_team_by_abbreviation(game.home)
        a = get_team_by_abbreviation(game.away)
        if not (h and a):
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, source="gbm",
                                error=f"team lookup failed ({game.home}/{game.away})")
        out = predict_mlb(get_conn(), {
            "home_team_id": h["id"], "away_team_id": a["id"],
            "home_pitcher_id": (game.extras or {}).get("home_pitcher_id"),
            "away_pitcher_id": (game.extras or {}).get("away_pitcher_id"),
            "date": (game.start_time or datetime.now().isoformat())[:10],
            "venue": (game.extras or {}).get("venue"),
        })
        if "error" in out:
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, source="gbm",
                                error=out["error"])
        return Prediction(
            sport=game.sport, home=game.home, away=game.away,
            ml_home=out.get("home_win"),
            ml_away=(1 - out["home_win"]) if "home_win" in out else None,
            total=out.get("total_runs"),
            source="gbm",
            signals={k: out[k] for k in ("nrfi_hit", "f5_home_win", "f5_total",
                                            "model_trained_at") if k in out},
        )

    def _predict_nhl(self, game: GameContext) -> Prediction:
        from datetime import datetime
        from .nhl_db import get_conn, get_nhl_team_by_abbr
        from .gbm.predict import predict_nhl
        h = get_nhl_team_by_abbr(game.home)
        a = get_nhl_team_by_abbr(game.away)
        if not (h and a):
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, source="gbm",
                                error=f"team lookup failed ({game.home}/{game.away})")
        out = predict_nhl(get_conn(), {
            "home_team_id": h["id"], "away_team_id": a["id"],
            "date": (game.start_time or datetime.now().isoformat())[:10],
            "season": game.season,
            "game_type": (game.extras or {}).get("game_type", 2),
        })
        if "error" in out:
            return Prediction(sport=game.sport, home=game.home,
                                away=game.away, source="gbm",
                                error=out["error"])
        return Prediction(
            sport=game.sport, home=game.home, away=game.away,
            ml_home=out.get("home_win"),
            ml_away=(1 - out["home_win"]) if "home_win" in out else None,
            total=out.get("total_goals"),
            source="gbm",
            signals={k: out[k] for k in ("p1_home_win", "p1_total_goals")
                       if k in out},
        )


class EnsemblePredictor:
    """Blends N predictor outputs by configured weights. Drop-N-replace
    for the basketball framework's existing ``_ensemble.blend()`` once
    A6 lands; until then it's a clean reference for the protocol pattern.

    weights example::
        {"ml_home": {"factor": 0.34, "gbm": 0.33, "mc": 0.33},
         "margin":  {"factor": 0.34, "gbm": 0.33, "mc": 0.33},
         "total":   {"factor": 0.34, "gbm": 0.33, "mc": 0.33}}
    """
    name = "ensemble"

    def __init__(self, members: dict[str, Predictor],
                  weights: dict | None = None):
        self.members = members
        self.weights = weights or {}

    def predict(self, game: GameContext) -> Prediction:
        outputs = {name: p.predict(game) for name, p in self.members.items()}
        # Blend each headline field
        def _blend(field_name: str, target_key: str | None = None):
            target_key = target_key or field_name
            w_for_target = self.weights.get(target_key) or {
                k: 1.0 / max(len(self.members), 1) for k in self.members
            }
            collected: list[tuple[float, float]] = []
            for name, out in outputs.items():
                v = getattr(out, field_name, None)
                if v is None:
                    continue
                w = w_for_target.get(name, 0.0)
                if w > 0:
                    collected.append((v, w))
            if not collected:
                return None
            total_w = sum(w for _, w in collected)
            if total_w <= 0:
                return None
            return sum(v * (w / total_w) for v, w in collected)

        return Prediction(
            sport=game.sport,
            home=game.home,
            away=game.away,
            ml_home=_blend("ml_home"),
            ml_away=(1 - _blend("ml_home")) if _blend("ml_home") is not None else None,
            margin=_blend("margin"),
            total=_blend("total"),
            home_expected=_blend("home_expected"),
            away_expected=_blend("away_expected"),
            signals={
                "members": {n: out.__dict__ for n, out in outputs.items()},
                "weights": self.weights,
            },
            source="ensemble",
        )


# ── Registry ─────────────────────────────────────────────────

# Per-sport default predictor — the orchestrator looks up the live one
# here. As sports get ported (A6), entries get added; A2 versioning
# eventually swaps these by version_id at runtime.
_REGISTRY: dict[str, Predictor] = {}


def register(sport: str, predictor: Predictor) -> None:
    _REGISTRY[sport] = predictor


def get(sport: str) -> Predictor | None:
    return _REGISTRY.get(sport)


def predict(game: GameContext) -> Prediction:
    """Convenience entry point — looks up the registered Predictor for
    the game's sport and dispatches. Falls back to the basketball
    factor predictor for any league key it knows; raises for unknown
    sports.

    The first-wave port: WNBA + Euroleague go through the basketball
    factor predictor. NBA can also flow through this once
    NBAFactorPredictor is registered."""
    p = _REGISTRY.get(game.sport)
    if p is not None:
        return p.predict(game)
    # Auto-register basketball framework leagues on first use.
    from .basketball import LEAGUE_REGISTRY
    if game.sport in LEAGUE_REGISTRY:
        bb = BasketballFactorPredictor()
        register(game.sport, bb)
        return bb.predict(game)
    # NBA legacy auto-register
    if game.sport == "nba":
        nba = NBAFactorPredictor()
        register("nba", nba)
        return nba.predict(game)
    if game.sport == "mlb":
        mlb = MLBFactorPredictor()
        register("mlb", mlb)
        return mlb.predict(game)
    if game.sport == "nhl":
        nhl = NHLFactorPredictor()
        register("nhl", nhl)
        return nhl.predict(game)
    return Prediction(sport=game.sport, home=game.home, away=game.away,
                        error=f"no predictor registered for {game.sport!r}")


# Auto-register all sports at import time so callers can dispatch
# without explicit setup. Each block is wrapped so a single sport's
# import failure (e.g. missing DB) doesn't prevent the others from
# registering.
def _bootstrap() -> None:
    try:
        from .basketball import LEAGUE_REGISTRY
        bb = BasketballFactorPredictor()
        for league_key in LEAGUE_REGISTRY:
            register(league_key, bb)
    except Exception as e:
        logger.debug("predictor bootstrap (basketball) skipped: %s", e)
    try:
        register("nba", NBAFactorPredictor())
    except Exception as e:
        logger.debug("predictor bootstrap (nba) skipped: %s", e)
    try:
        register("mlb", MLBFactorPredictor())
    except Exception as e:
        logger.debug("predictor bootstrap (mlb) skipped: %s", e)
    try:
        register("nhl", NHLFactorPredictor())
    except Exception as e:
        logger.debug("predictor bootstrap (nhl) skipped: %s", e)
    # Tennis lives under engine.sports.tennis (A6) — load it so the
    # 'tennis' key is available without explicit import at the call site.
    try:
        from .sports import tennis as _tennis  # noqa: F401
    except Exception as e:
        logger.debug("predictor bootstrap (tennis) skipped: %s", e)
    # AHL + PWHL — theScore-sourced minor/women's hockey leagues.
    try:
        from .sports import ahl as _ahl  # noqa: F401
        _ahl.predict  # touch the module so AHLPredictor registers
    except Exception as e:
        logger.debug("predictor bootstrap (ahl) skipped: %s", e)
    try:
        from .sports import pwhl as _pwhl  # noqa: F401
        _pwhl.predict
    except Exception as e:
        logger.debug("predictor bootstrap (pwhl) skipped: %s", e)
    # AIHL + NZIHL — Oceania hockey via SofaScore. Same Poisson shape
    # as AHL/PWHL; predictors register themselves on import.
    try:
        from .sports import aihl as _aihl  # noqa: F401
        _aihl.predict
    except Exception as e:
        logger.debug("predictor bootstrap (aihl) skipped: %s", e)
    try:
        from .sports import nzihl as _nzihl  # noqa: F401
        _nzihl.predict
    except Exception as e:
        logger.debug("predictor bootstrap (nzihl) skipped: %s", e)


_bootstrap()
