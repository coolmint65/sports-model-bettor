/**
 * Basketball league nav helpers — region grouping + season-relevance
 * sort, mirroring sportSort.js but one level deeper (per-league inside
 * the Basketball entry).
 *
 * Source of truth for league metadata is the backend's
 * /api/basketball/leagues endpoint. The frontend caches the response
 * and uses these helpers to render the nested nav.
 */

/** Region display order — matches the backend's "regions" list. */
export const REGION_ORDER = [
  'USA',
  'International',
  'Europe',
  'Americas',
  'Asia/Oceania',
  'Other',
]

/**
 * Group leagues by region with in-season-first sort inside each region.
 *
 * @param {Array<{key, display_name, region, in_season, status}>} leagues
 * @returns {Array<{ region: string, leagues: Array<league> }>}
 *          Regions in REGION_ORDER; inside each region, in-season
 *          leagues come first (by display_name asc) then off-season
 *          leagues (also by display_name asc) so the user's eye lands
 *          on what's playing tonight without losing access to the rest.
 */
export function groupByRegion(leagues) {
  const buckets = new Map()
  for (const L of leagues || []) {
    const r = L.region || 'Other'
    if (!buckets.has(r)) buckets.set(r, [])
    buckets.get(r).push(L)
  }
  const ordered = []
  for (const r of REGION_ORDER) {
    const list = buckets.get(r)
    if (!list || list.length === 0) continue
    list.sort((a, b) => {
      // In-season first, then alpha by display name
      if (a.in_season !== b.in_season) return a.in_season ? -1 : 1
      return (a.display_name || a.key).localeCompare(b.display_name || b.key)
    })
    ordered.push({ region: r, leagues: list })
  }
  // Append any region not in REGION_ORDER (defensive — shouldn't happen)
  for (const [r, list] of buckets) {
    if (!REGION_ORDER.includes(r)) {
      ordered.push({ region: r, leagues: list })
    }
  }
  return ordered
}

/**
 * Pick the default league to surface when the user clicks Basketball
 * with nothing previously selected. Prefers an in-season USA league
 * (NBA/WNBA/NCAAM) so the most-relevant content lands first.
 */
export function defaultLeague(leagues) {
  if (!leagues || leagues.length === 0) return null
  const inSeasonUsa = leagues.find(L => L.in_season && L.region === 'USA')
  if (inSeasonUsa) return inSeasonUsa.key
  const anyInSeason = leagues.find(L => L.in_season)
  if (anyInSeason) return anyInSeason.key
  return leagues[0].key
}
