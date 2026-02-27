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
export function parseAsUTC(dateStr) {
  if (!dateStr) return null;
  let s = String(dateStr);
  // If it has a 'T' (ISO datetime) but no timezone indicator, treat as UTC
  if (s.includes('T') && !s.includes('+') && !s.includes('Z') && !s.includes('-', s.indexOf('T'))) {
    s += 'Z';
  }
  return new Date(s);
}
