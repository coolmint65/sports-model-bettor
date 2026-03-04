/**
 * Shared formatting utilities.
 *
 * Single source of truth for odds formatting, confidence colors,
 * and game ID normalization. Eliminates duplication across
 * BestBets, GameDetail, GameCard, History, and PredictionCard.
 */

/**
 * Format American odds with +/- prefix.
 * @param {number|null} odds - American odds value
 * @returns {string|null} Formatted string like "+130" or "-150"
 */
export function formatAmericanOdds(odds) {
  if (odds == null) return null;
  const v = Math.round(odds);
  return v > 0 ? `+${v}` : `${v}`;
}

/**
 * Format American odds, returning '-' for null instead of null.
 * Use in places where a display fallback is always needed.
 */
export function formatAmericanOddsOrDash(odds) {
  return formatAmericanOdds(odds) ?? '-';
}

/**
 * Convert implied probability to American odds display string.
 */
export function formatOddsFromProb(impliedProb) {
  if (!impliedProb || impliedProb <= 0 || impliedProb >= 1) return null;
  if (impliedProb > 0.5) {
    const odds = Math.round(-(impliedProb / (1 - impliedProb)) * 100);
    return odds.toString();
  } else {
    const odds = Math.round(((1 - impliedProb) / impliedProb) * 100);
    return `+${odds}`;
  }
}

/**
 * Get the themed color for a confidence percentage.
 */
export function getConfidenceColor(confidence) {
  if (confidence >= 75) return '#00ff88';
  if (confidence >= 60) return '#4fc3f7';
  if (confidence >= 45) return '#ffd700';
  return '#ff5252';
}

/**
 * Normalize game ID from objects that use either `id` or `game_id`.
 */
export function getGameId(game) {
  return game?.game_id ?? game?.id ?? null;
}
