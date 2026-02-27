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
