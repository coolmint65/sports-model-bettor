/**
 * Shared betting math helpers used by both MLB and NHL game detail views.
 * Keep these in sync with engine/accuracy.py :: compute_kelly_fraction.
 */

/**
 * Quarter-Kelly bet sizing. Caps at 25% of bankroll for safety.
 *
 * Returns the fraction of bankroll to wager. Returns 0 if the bet has
 * non-positive expected value or if inputs are missing.
 */
export function kellyFraction(probWin, odds) {
  if (!odds || probWin == null) return 0
  const decimal = odds > 0 ? (odds / 100) + 1 : (100 / Math.abs(odds)) + 1
  const b = decimal - 1
  if (b <= 0) return 0
  const p = probWin
  const q = 1 - p
  const kelly = (b * p - q) / b
  if (kelly <= 0) return 0
  // Quarter-Kelly, clamped to [0, 0.25]
  return Math.max(0, Math.min(0.25, kelly / 4))
}

/**
 * Convert American moneyline odds to an implied probability.
 */
export function mlToProb(ml) {
  if (ml < 0) return (-ml) / (-ml + 100)
  return 100 / (ml + 100)
}

/**
 * Implied probability helper used by PickRow — mirrors the inline math
 * that both MLB and NHL had duplicated.
 */
export function impliedFromOdds(odds) {
  if (odds == null) return null
  return odds < 0 ? Math.abs(odds) / (Math.abs(odds) + 100) : 100 / (odds + 100)
}
