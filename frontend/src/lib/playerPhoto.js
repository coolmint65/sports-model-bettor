/**
 * Player headshot URL helpers.
 *
 * All three sports expose stable per-player CDN URLs keyed by player_id
 * — no team or season needed for the path shapes used here.
 *
 *   NBA: https://a.espncdn.com/i/headshots/nba/players/full/{id}.png
 *   MLB: https://content.mlb.com/images/headshots/current/60x60/{id}.png
 *   NHL: https://cms.nhl.bamgrid.com/images/headshots/current/168x168/{id}.jpg
 *
 * onError fall-through to a neutral silhouette is the caller's
 * responsibility — these just return URLs.
 */

export function playerPhotoUrl(sport, playerId) {
  if (!playerId) return null
  const id = String(playerId).trim()
  if (!id) return null
  if (sport === 'nba') return `https://a.espncdn.com/i/headshots/nba/players/full/${id}.png`
  if (sport === 'mlb') return `https://content.mlb.com/images/headshots/current/60x60/${id}.png`
  if (sport === 'nhl') return `https://cms.nhl.bamgrid.com/images/headshots/current/168x168/${id}.jpg`
  return null
}
