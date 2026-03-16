/**
 * Shared formatting utilities.
 *
 * Single source of truth for odds formatting, confidence colors,
 * game date/time formatting, and confidence labels.
 * Used by GameDetail, GameCard, History, BestBets, and PredictionCard.
 */

import { format } from 'date-fns';
import { parseAsUTC } from './teams';

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
 * Convert an implied probability (0-1) to American odds string.
 * Returns null for out-of-range values.
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
 * Get a human-readable label for a confidence percentage.
 * Used by both PredictionCard and GameDetail.
 */
export function getConfidenceLabel(confidence) {
  if (confidence >= 75) return 'Very High';
  if (confidence >= 60) return 'High';
  if (confidence >= 45) return 'Medium';
  return 'Low';
}

/**
 * Format a game's date for display.
 * @param {object} game - Game object with start_time or datetime
 * @param {string} dateFormat - date-fns format string (default: short date)
 * @returns {string|null}
 */
export function formatGameDate(game, dateFormat = 'EEE, MMM d') {
  try {
    const dateStr = game.start_time || game.datetime;
    if (!dateStr) return null;
    const date = parseAsUTC(dateStr);
    if (!date || isNaN(date.getTime())) return null;
    return format(date, dateFormat);
  } catch {
    return null;
  }
}

/**
 * Format a game's start time for display.
 * @param {object} game - Game object with start_time or datetime
 * @returns {string}
 */
export function formatGameTime(game) {
  try {
    const dateStr = game.start_time || game.datetime;
    if (!dateStr) return 'TBD';
    const date = parseAsUTC(dateStr);
    if (!date || isNaN(date.getTime())) return 'TBD';
    return format(date, 'h:mm a');
  } catch {
    return game.time || 'TBD';
  }
}

