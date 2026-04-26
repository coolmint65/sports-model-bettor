/**
 * Season-relevance sort for the sidebar nav.
 *
 * Sports are pinned to the top while in-season and sorted by today's
 * game count (most active first). Out-of-season sports drop to the
 * bottom at reduced opacity so the user's eye lands on the live ones
 * without losing access to the dormant ones (offseason research,
 * historical tracker browsing, etc.).
 *
 * Season windows are inclusive month ranges (1=Jan, 12=Dec). When
 * `start > end` the window wraps the calendar year — used for NHL/NBA
 * which run Oct -> Jun. Adding a new sport later only requires adding
 * an entry to SPORT_SEASON_WINDOWS; the sort logic generalizes.
 */

/** [startMonth, endMonth] inclusive, 1-indexed. */
export const SPORT_SEASON_WINDOWS = {
  mlb: [3, 10],   // Spring training spillover -> end of regular season
  nhl: [10, 6],   // Wraps year-end (Oct -> Jun)
  nba: [10, 6],   // Wraps year-end (Oct -> Jun)
}

/**
 * Returns true when `month` (1-12) falls inside the inclusive
 * [start, end] window. Wraps the year when start > end.
 */
export function inWindow(month, start, end) {
  if (start <= end) return month >= start && month <= end
  // Wrapped window: Oct..Dec or Jan..Jun for NHL/NBA
  return month >= start || month <= end
}

export function isInSeason(sport, date = new Date()) {
  const window = SPORT_SEASON_WINDOWS[sport]
  if (!window) return true   // Unknown sport defaults to in-season
  return inWindow(date.getMonth() + 1, window[0], window[1])
}

/**
 * Sort + tag sports for sidebar render.
 *
 * @param {string[]} sports          - sport keys to render
 * @param {Object}   gameCounts      - { sport: numberOfTodayGames }
 * @param {Date}     [date]          - injected for testability
 * @returns {{ sport: string, gameCount: number, inSeason: boolean }[]}
 *          In-season sports first, sorted by game count desc; then
 *          out-of-season sports in original order.
 */
export function sortSports(sports, gameCounts = {}, date = new Date()) {
  const decorated = sports.map(s => ({
    sport: s,
    gameCount: gameCounts[s] ?? 0,
    inSeason: isInSeason(s, date),
  }))

  const inSeason = decorated
    .filter(d => d.inSeason)
    .sort((a, b) => b.gameCount - a.gameCount)

  const offSeason = decorated.filter(d => !d.inSeason)

  return [...inSeason, ...offSeason]
}
