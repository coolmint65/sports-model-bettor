/**
 * Player headshot URL helpers.
 *
 *   NBA: a.espncdn.com NBA headshot PNG. We use ESPN player IDs end-to-
 *        end (boxscore ingest, prop picks); cdn.nba.com uses a different
 *        ID namespace and silently returns a silhouette stub for unknown
 *        IDs, so it looked "broken" on every NBA prop card.
 *   MLB: img.mlbstatic.com Cloudinary silo PNG at w_480.
 *   NHL: assets.nhle.com mugs latest PNG (cms.nhl.bamgrid.com is dead).
 *
 * No team or season needed for any of these path shapes. onError
 * fall-through to a neutral silhouette is the caller's job.
 */

export function playerPhotoUrl(sport, playerId) {
  if (!playerId) return null
  const id = String(playerId).trim()
  if (!id) return null
  if (sport === 'nba') {
    return `https://a.espncdn.com/i/headshots/nba/players/full/${id}.png`
  }
  if (sport === 'mlb') {
    return `https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:silo:current.png/w_480,q_auto:best/v1/people/${id}/headshot/silo/current`
  }
  if (sport === 'nhl') {
    return `https://assets.nhle.com/mugs/nhl/latest/${id}.png`
  }
  return null
}
