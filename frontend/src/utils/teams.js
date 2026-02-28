/**
 * Safely extract a team name from either a string or a team object.
 * The API may return team data as:
 *   - A plain string ("Toronto Maple Leafs")
 *   - A TeamBrief/TeamSnapshot object ({id, name, abbreviation, ...})
 */
export function teamName(team, fallback = 'TBD') {
  if (!team) return fallback;
  if (typeof team === 'string') return team;
  return team.name || team.full_name || team.abbreviation || fallback;
}

export function teamAbbrev(team, fallback = '???') {
  if (!team) return fallback;
  if (typeof team === 'string') return team.substring(0, 3).toUpperCase();
  return team.abbreviation || team.abbrev || (team.name ? team.name.substring(0, 3).toUpperCase() : fallback);
}

/**
 * Extract a team logo URL from a team object.
 * Returns null if none is available.
 */
export function teamLogo(team) {
  if (!team || typeof team === 'string') return null;
  return team.logo_url || team.logo || team.team_logo || null;
}

/**
 * Normalise a confidence value to a 0-100 percentage.
 * The model stores confidence as 0-1 floats; the UI expects 0-100.
 */
export function confidencePct(value) {
  if (value == null) return 0;
  // If already in 0-100 range, return as-is
  if (value > 1) return value;
  return value * 100;
}

/**
 * Parse a datetime string from the API as UTC.
 *
 * SQLite drops timezone info, so the backend may return datetimes
 * like "2026-02-28T00:00:00" without a timezone suffix.
 * The NHL API times are always UTC, so we append 'Z' if missing
 * to ensure correct local-time conversion in the browser.
 */
/**
 * Format a raw bet_type string into a human-readable label.
 * e.g., "period_total" → "Period Total", "ml" → "Moneyline"
 */
const BET_TYPE_LABELS = {
  ml: 'Moneyline',
  total: 'Total Goals',
  spread: 'Puck Line',
  team_total: 'Team Total',
  period_total: 'Period Total',
  period_winner: 'Period Winner',
  first_goal: 'First Goal',
  both_score: 'Both Teams Score',
  overtime: 'Overtime',
  odd_even: 'Odd/Even',
};

export function formatBetType(betType) {
  if (!betType) return 'Prediction';
  const lower = betType.toLowerCase();
  if (BET_TYPE_LABELS[lower]) return BET_TYPE_LABELS[lower];
  // Fallback: replace underscores with spaces and title-case
  return lower
    .split('_')
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}

/**
 * Format a raw prediction_value string into a human-readable label.
 * e.g., "WPG_-1.5" → "WPG -1.5", "over_5.5" → "Over 5.5", "ANA" → "ANA"
 *
 * Team abbreviations (2-3 uppercase letters) are kept as-is.
 * Spread signs (+/-) are preserved.
 */
export function formatPredictionValue(value) {
  if (!value) return 'N/A';
  // Replace underscores with spaces
  let formatted = value.replace(/_/g, ' ');
  // Title-case each word, keeping abbreviations and special chars as-is
  formatted = formatted
    .split(' ')
    .map((w) => {
      // Keep team abbreviations uppercase (2-3 letter all-caps like WPG, ANA, TOR)
      if (/^[A-Z]{2,3}$/.test(w)) return w;
      // Keep period prefixes uppercase (p1, p2, p3)
      if (/^p\d$/.test(w)) return w.toUpperCase();
      // Preserve +/- signs in spread values like "+1.5" or "-1.5"
      if (/^[+-]/.test(w)) return w;
      return w.charAt(0).toUpperCase() + w.slice(1);
    })
    .join(' ');
  return formatted;
}

export function parseAsUTC(dateStr) {
  if (!dateStr) return null;
  let s = String(dateStr);
  // Python's str(datetime) uses space separator; normalize to ISO 'T'
  // e.g., "2026-02-28 00:00:00+00:00" → "2026-02-28T00:00:00+00:00"
  if (/^\d{4}-\d{2}-\d{2} \d{2}:/.test(s)) {
    s = s.replace(' ', 'T');
  }
  // If it has a 'T' (ISO datetime) but no timezone indicator, treat as UTC
  if (s.includes('T') && !s.includes('+') && !s.includes('Z') && !s.includes('-', s.indexOf('T'))) {
    s += 'Z';
  }
  return new Date(s);
}
