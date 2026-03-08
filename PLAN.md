# PropEngine Implementation Plan

## Architecture

New isolated module `backend/app/props/` — completely separate from `analytics/models.py` ML/totals/spreads pipeline.

```
backend/app/props/
├── __init__.py          # Public API: run_props(features, odds_data, matrix, home_xg, away_xg) → list[dict]
├── engine.py            # PropEngine: loops PROP_REGISTRY, runs predict → filter → map_odds
├── grading.py           # Dispatches grading to each prop type's grade() method
└── types/
    ├── __init__.py      # PROP_REGISTRY list
    ├── base.py          # Abstract BaseProp interface
    ├── btts.py          # Both Teams To Score — NO only
    ├── period_total.py  # Period Over/Under (P1, P2, P3) — both sides
    ├── period_winner.py # Period Winner (P1, P2, P3) — both sides incl draw
    ├── first_goal.py    # Team To Score First — both sides
    ├── overtime.py      # Game Goes To OT — YES only
    └── regulation.py    # Regulation Winner (3-Way ML) — home/away/draw
```

## Base Interface (`types/base.py`)

```python
class BaseProp(ABC):
    bet_type: str          # e.g. "both_score", "period_total"
    display_name: str      # e.g. "BTTS", "P1 Over/Under"

    @abstractmethod
    def predict(self, features, matrix, home_xg, away_xg) -> list[dict]:
        """Return candidates: [{side, confidence, reasoning}, ...]"""

    @abstractmethod
    def filter(self, candidates) -> list[dict]:
        """Directional filter. BTTS keeps only 'no', OT keeps only 'yes', etc."""

    @abstractmethod
    def map_odds(self, candidates, odds_data) -> list[dict]:
        """Attach implied_probability, odds, edge from scraped data."""

    @abstractmethod
    def grade(self, prediction_value, game) -> bool | None:
        """True=win, False=loss, None=push/ungradeable."""

    @abstractmethod
    def determine_outcome(self, game) -> str | None:
        """Actual outcome string for a settled game."""
```

## Prop Type Details

### 1. BTTS (`btts.py`)
- **bet_type**: `"both_score"`
- **predict**: From score matrix, `P(both_score) = sum(matrix[i][j] for i>0, j>0)`. Emit `P(no) = 1 - P(both_score)`.
- **filter**: Only emit `"no"` side. "Yes" is always -800+ juice in hockey.
- **prediction_value**: `"both_score_no"`
- **map_odds**: Look for `btts_no_price` in odds_data.
- **grade**: `not (game.home_score > 0 and game.away_score > 0)` → True means BTTS-No won.

### 2. Period Totals (`period_total.py`)
- **bet_type**: `"period_total"`
- **predict**: For each period (P1, P2, P3), build mini-Poisson from period stats features (`home_period_stats.avg_p{n}_for` + `away_period_stats.avg_p{n}_against` → period home_xg, and vice versa). Evaluate over/under on lines [0.5, 1.5, 2.5].
- **filter**: Both sides eligible. Pick best edge per period.
- **prediction_value**: `"p1_over_1.5"`, `"p2_under_0.5"`, etc.
- **map_odds**: Look for `p{n}_over_price`, `p{n}_under_price`, `p{n}_total_line` in odds_data.
- **grade**: `(game.home_score_p{n} + game.away_score_p{n})` vs line. These columns already exist.

### 3. Period Winners (`period_winner.py`)
- **bet_type**: `"period_winner"`
- **predict**: From same per-period mini-Poisson, compute P(home_wins_period), P(away_wins_period), P(draw). Sum cells where team_goals > opp_goals, etc.
- **filter**: All three outcomes (home/away/draw) eligible.
- **prediction_value**: `"p1_home"`, `"p2_away"`, `"p1_draw"`
- **map_odds**: `p{n}_home_price`, `p{n}_away_price`, `p{n}_draw_price`.
- **grade**: Compare `home_score_p{n}` vs `away_score_p{n}`.

### 4. First Goal (`first_goal.py`)
- **bet_type**: `"first_goal"`
- **predict**: Approximate from P1 data: `P(home_scores_first) ≈ home_p1_xg / (home_p1_xg + away_p1_xg)`, weighted by `first_period_scoring_rate`. Both teams' P1 xG derived from period stats.
- **filter**: Both sides eligible.
- **prediction_value**: `"first_goal_HOM"`, `"first_goal_AWY"`
- **map_odds**: `first_goal_home_price`, `first_goal_away_price`.
- **grade**: Needs "which team scored first" data. If Game model lacks this field, grade returns None (ungradeable) until we add it. Don't block the prop on grading.

### 5. Overtime (`overtime.py`)
- **bet_type**: `"overtime"`
- **predict**: `P(OT) = sum(matrix[i][i] for all i)` (regulation tie = diagonal). Blend with OT tendency features: `(matrix_p * 0.7 + avg_team_ot_pct * 0.3)`.
- **filter**: Only emit `"yes"`. "No" is heavy juice.
- **prediction_value**: `"overtime_yes"`
- **map_odds**: `ot_yes_price`, `ot_no_price`.
- **grade**: `game.went_to_overtime` — field already exists on Game model.

### 6. Regulation Winner (`regulation.py`)
- **bet_type**: `"regulation_winner"`
- **predict**: Direct decomposition from score matrix: `P(home) = sum(matrix[i][j] for i>j)`, `P(away) = sum(matrix[i][j] for j>i)`, `P(draw) = sum(matrix[i][i])`. No OT resolution.
- **filter**: All three outcomes eligible.
- **prediction_value**: `"reg_home"`, `"reg_away"`, `"reg_draw"`
- **map_odds**: `reg_home_price`, `reg_away_price`, `reg_draw_price`.
- **grade**: If `game.went_to_overtime` → outcome is "reg_draw". Otherwise compare `home_score` vs `away_score`. Can use `home_score_p1+p2+p3` vs `away_score_p1+p2+p3` for regulation-only score.

## PropEngine (`engine.py`)

```python
class PropEngine:
    def run(self, features, odds_data, matrix, home_xg, away_xg) -> list[dict]:
        results = []
        for prop_cls in PROP_REGISTRY:
            prop = prop_cls()
            candidates = prop.predict(features, matrix, home_xg, away_xg)
            filtered = prop.filter(candidates)
            with_odds = prop.map_odds(filtered, odds_data)
            for c in with_odds:
                results.append({
                    "bet_type": prop.bet_type,
                    "prediction": c["side"],
                    "confidence": c["confidence"],
                    "probability": c["confidence"],
                    "implied_probability": c.get("implied_probability"),
                    "odds": c.get("odds"),
                    "edge": c.get("edge"),
                    "reasoning": c["reasoning"],
                })
        return results
```

## Integration Points

### 1. Call from `analytics/models.py:predict_all()`
After the ML/total/spread predictions are built (line ~1716), call:
```python
from app.props import PropEngine
prop_engine = PropEngine()
prop_preds = prop_engine.run(features, odds_data, matrix, home_xg, away_xg)
predictions.extend(prop_preds)
```
The `matrix`, `home_xg`, `away_xg` are already computed as `_pre` on line 1192-1194. `odds_data` is already extracted on line 1197. Zero new queries needed.

### 2. Grading via `services/grading.py:check_outcome()`
Add prop dispatch at bottom of `check_outcome()`:
```python
from app.props.grading import check_prop_outcome
if bet_type not in ("ml", "total", "spread"):
    return check_prop_outcome(bet_type, prediction_value, game, home_abbr)
```
`check_prop_outcome` looks up the prop class by bet_type from registry and calls its `grade()`.

Same pattern for `determine_actual_outcome()` — add prop dispatch.

### 3. Best-bet eligibility
Props are NOT in `MARKET_BET_TYPES` so they're automatically excluded from best-bet ranking (line 720 in predictions.py). This is correct for now — props need odds data before they can have meaningful edge. No changes needed to `predictions.py`.

### 4. Odds scraping (future, not this PR)
Props work without odds — predictions only, no edge. Odds mapping returns empty when no prop odds exist. Prop odds scraping is a separate task once we identify which sportsbook endpoints serve these markets.

## Implementation Order

1. `props/types/base.py` — abstract base
2. `props/types/regulation.py` — simplest, straight from matrix
3. `props/types/btts.py` — also trivial from matrix
4. `props/types/overtime.py` — matrix diagonal + OT tendency blend
5. `props/types/period_total.py` — mini-Poisson per period
6. `props/types/period_winner.py` — same mini-Poisson
7. `props/types/first_goal.py` — P1 approximation
8. `props/types/__init__.py` — registry
9. `props/engine.py` — orchestrator
10. `props/__init__.py` — public API
11. `props/grading.py` — grading dispatch
12. Wire into `analytics/models.py:predict_all()` (3 lines)
13. Wire into `services/grading.py:check_outcome()` and `determine_actual_outcome()` (~6 lines each)

## What This Does NOT Touch

- `analytics/models.py` — No changes to BettingModel internals (only appending to predict_all output)
- `analytics/features.py` — No changes (reuses existing features as-is)
- `analytics/predictions.py` — No changes
- `services/odds.py` / `scrapers/odds_multi.py` — No changes
- All existing ML/total/spread bets continue working identically
