# Prediction System Improvement Plan

## Problems Identified

### P1: Calibration too weak (5% shrinkage)
- Model says 70% → outputs 69%. Real NHL accuracy at that confidence is ~62-64%.
- Hockey has too much randomness for a Poisson model to be 70% confident.
- The calibration analysis tool exists but isn't feeding back into the model.

### P2: 40+ adjustments compound unpredictably
- Each factor is small (0.06-0.18 weight), but they stack multiplicatively.
- A team with 8 positive signals gets xG inflated by all of them independently.
- Mean regression (18%) at the end doesn't fully counteract the compounding.

### P3: Form weighting too reactive (50% on last 5 games)
- 5 games = 1.5 weeks. Model chases recent results.
- A 4-1 stretch against weak teams inflates offensive rating disproportionately.
- Creates swings in confidence from day to day.

### P4: Massive goalie double-counting
- Goalie quality (0.20) + tier mismatch (0.08) + hot/cold (0.15) + vs opponent
  (0.12) + venue (0.08) + workload (0.10) = 0.73 total goalie influence.
- Goalie matters but not 73% of the prediction.

### P5: Correlated feature stacking
- Corsi (0.08) + 5v5 Corsi (0.10) + Close-game Corsi (0.06) = 0.24 on highly
  correlated possession metrics.
- Form + Momentum + Scoring-first + Close-game record all capture "recent
  performance" from different angles but compound.

### P6: No market prior
- Model ignores sportsbook lines when building xG.
- Market lines encode sharp information from millions in handle.
- Model should use market as a prior and adjust from there.

---

## Proposed Fixes

### Fix 1: Stronger calibration (HIGH IMPACT)
**Current**: shrinkage = 0.05 (hardcoded)
**Proposed**: shrinkage = 0.15-0.20 (configurable via ModelConfig)

New mapping:
- 50% → 50%
- 55% → 54.3%
- 60% → 58.5%
- 65% → 62.8%
- 70% → 67.0%
- 75% → 71.3%

This alone will reduce variance significantly. Predictions cluster closer to
reality, reducing the swings between "high confidence win" and "upset loss."

### Fix 2: Reduce form reactivity (MEDIUM IMPACT)
**Current**: L5=50%, L10=30%, Season=20%
**Proposed**: L5=35%, L10=35%, Season=30%

More weight on season stabilizes predictions day-to-day. L10 is the sweet spot
for capturing form without chasing noise. Season baseline prevents wild swings.

### Fix 3: Cap total goalie influence (HIGH IMPACT)
**Current**: 6 goalie factors sum to 0.73 max influence
**Proposed**: Apply all goalie adjustments, then cap the total goalie delta
to ±0.40 xG from the pre-goalie baseline. Prevents goalie-related factors
from dominating the entire prediction.

### Fix 4: Deduplicate correlated possession metrics (MEDIUM IMPACT)
**Current**: 3 separate Corsi adjustments (0.08 + 0.10 + 0.06 = 0.24)
**Proposed**: Use the BEST available possession metric (prefer 5v5 EV Corsi
from MoneyPuck > close-game Corsi > all-situations Corsi proxy). Apply once
with weight 0.12. No stacking.

### Fix 5: Stronger mean regression (MEDIUM IMPACT)
**Current**: mean_regression = 0.18
**Proposed**: mean_regression = 0.25

NHL outcomes regress heavily to the mean. A 25% pull toward league average
dampens compound factor stacking without eliminating genuine edges.

### Fix 6: Market-informed xG prior (HIGH IMPACT, COMPLEX)
After computing Poisson xG, blend with market-implied xG:
- Convert sportsbook moneyline to implied win probability
- Convert implied win probability back to expected goals differential
- Blend: final_xg = model_xg * 0.60 + market_xg * 0.40

This means the model can still disagree with the market, but it starts from
a much more informed baseline. The 60/40 split means the model needs strong
evidence to deviate significantly from the market.

### Fix 7: Tighten xG bounds (LOW IMPACT)
**Current**: floor=1.6, ceiling=4.0
**Proposed**: floor=2.0, ceiling=3.8

NHL teams almost never average below 2.0 or above 3.8 xG. Tighter bounds
prevent extreme predictions.

### Fix 8: Reduce edge cap and thresholds (MEDIUM IMPACT)
**Current**: edge cap = 25%, min_edge = 5%, best_bet_edge = 8%
**Proposed**: edge cap = 15%, min_edge = 3%, best_bet_edge = 5%

After better calibration, edges will be smaller but more accurate. Adjust
thresholds to match. A 5% edge against sharp lines is genuinely significant.

---

## Implementation Order

1. Fix 1 (calibration) + Fix 2 (form weights) — quick config changes
2. Fix 5 (mean regression) + Fix 7 (xG bounds) — quick config changes
3. Fix 3 (goalie cap) — moderate code change
4. Fix 4 (possession dedup) — moderate code change
5. Fix 8 (edge thresholds) — config changes
6. Fix 6 (market prior) — significant code change

Fixes 1-5 and 7-8 are straightforward and should be done together.
Fix 6 is the biggest lift but has the highest long-term impact.
