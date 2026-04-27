/**
 * Player headshot URL helpers.
 *
 * Each league exposes stable per-player CDN URLs keyed by player_id.
 * Picking the highest-res versions available — earlier 60x60 (MLB)
 * and 168x168 (NHL) sources looked pixelated when scaled up to the
 * 80px POTD photo / 56px card photo (any upscale shows artifacts).
 *
 *   NBA: cdn.nba.com 1040x760 (official, highest available)
 *   MLB: img.mlbstatic.com Cloudinary transform → 480x480 silo PNG
 *   NHL: bamgrid 336x336 (2x the previous URL)
 *
 * No team or season needed for any of these path shapes. onError
 * fall-through to a neutral silhouette is the caller's job.
 */

export function playerPhotoUrl(sport, playerId) {
  if (!playerId) return null
  const id = String(playerId).trim()
  if (!id) return null
  if (sport === 'nba') {
    return `https://cdn.nba.com/headshots/nba/latest/1040x760/${id}.png`
  }
  if (sport === 'mlb') {
    // mlbstatic Cloudinary endpoint serves the silo cutout at any
    // requested size; w_480 looks crisp at the 80px POTD slot.
    return `https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:silo:current.png/w_480,q_auto:best/v1/people/${id}/headshot/silo/current`
  }
  if (sport === 'nhl') {
    return `https://cms.nhl.bamgrid.com/images/headshots/current/336x336/${id}.jpg`
  }
  return null
}
