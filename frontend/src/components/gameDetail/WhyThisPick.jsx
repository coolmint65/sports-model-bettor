/**
 * WhyThisPick
 * ──────────────────────────────────────────────────────────────
 * The "Why this pick?" card shown above the factors table on
 * every GameDetail view. Replaces three near-identical inline
 * blocks in PredictionResults (MLB), NHLGameDetail, NBAGameDetail.
 *
 * Key change vs the old inline block: reasoning strings are run
 * through curateReasoning() first so the list only surfaces
 * signals that support (or are neutral toward) the recommended
 * pick, instead of dumping every stat comparison — which reads as
 * self-contradictory on +EV underdog plays.
 *
 * `matchup_insights` are pick-specific interaction signals from
 * the engine and always render first when present.
 */

import { curateReasoning } from './curateReasoning'

export default function WhyThisPick({ pred, pick, home, away, title = 'Why this pick?' }) {
  const insights = pred?.matchup_insights || []
  const curated = curateReasoning(pred?.reasoning || [], pick, home, away)

  // Dedupe between insights and curated list, preserving order.
  const seen = new Set()
  const lines = []
  for (const s of [...insights, ...curated]) {
    if (!s || seen.has(s)) continue
    seen.add(s)
    lines.push(s)
  }

  if (lines.length === 0) return null

  return (
    <div className="result-card why-pick-card">
      <h2>{title}</h2>
      <ul className="why-pick-list">
        {lines.map((line, i) => (
          <li key={i} className="why-pick-item">
            <span className="why-pick-num">{i + 1}.</span>
            <span className="why-pick-text">{line}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}
