/**
 * NBA detail-view helpers.
 *
 * Extracted from NBAGameDetail 2026-07-09 refactor pass. Pure logic —
 * no React state — that shapes the model output into a "best edge"
 * card for Q1 and full-game markets.
 */
import { impliedFromOdds } from '../gameDetail/kelly'

export function edgeFromBackendPick(pick) {
  if (!pick) return null
  return {
    label: pick.pick,
    odds: pick.odds,
    edge: pick.edge,
    rating: pick.confidence || 'lean',
  }
}


export function findBestQ1Edge(data, odds, home, away) {
  const candidates = []
  if (data.spread_cover_prob != null && odds) {
    const spreadOdds = odds.q1_spread_home_odds || -110
    const implied = impliedFromOdds(spreadOdds)
    const e = (data.spread_cover_prob - implied) * 100
    if (e > 1.5) {
      const m = data.predicted_margin || 0
      const fav = m > 0 ? home.abbreviation : away.abbreviation
      candidates.push({ label: `${fav} Q1 Spread`, odds: spreadOdds, edge: e })
    }
  }
  if (data.over_prob != null && odds) {
    const total = data.predicted_total || 0
    const pickOver = data.over_prob > 0.5
    const prob = pickOver ? data.over_prob : 1 - data.over_prob
    const ouOdds = pickOver ? (odds.q1_over_odds || -110) : (odds.q1_under_odds || -110)
    const implied = impliedFromOdds(ouOdds)
    const e = (prob - implied) * 100
    if (e > 1.5) candidates.push({ label: `${pickOver ? 'Over' : 'Under'} ${total.toFixed(1)} Q1`, odds: ouOdds, edge: e })
  }
  if (!candidates.length) return null
  const best = candidates.sort((a, b) => b.edge - a.edge)[0]
  best.rating = best.edge > 8 ? 'strong' : best.edge > 4 ? 'moderate' : 'lean'
  return best
}
