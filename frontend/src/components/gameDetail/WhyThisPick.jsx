/**
 * WhyThisPick — curated reasoning bullets above the factor table.
 *
 * Phase 2-cleanup restyle: Tailwind chrome around the existing data
 * (insights + curated reasoning lines, deduped, numbered).
 *
 * Reasoning strings run through curateReasoning() so the list only
 * surfaces signals that support (or are neutral toward) the
 * recommended pick — avoids self-contradictory dumps on +EV
 * underdog plays.
 */

import { curateReasoning } from './curateReasoning'

export default function WhyThisPick({ pred, pick, home, away, title = 'Why this pick?' }) {
  const insights = pred?.matchup_insights || []
  const curated = curateReasoning(pred?.reasoning || [], pick, home, away)

  const seen = new Set()
  const lines = []
  for (const s of [...insights, ...curated]) {
    if (!s || seen.has(s)) continue
    seen.add(s)
    lines.push(s)
  }

  if (lines.length === 0) return null

  return (
    <section className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="flex items-center gap-2 px-5 py-3 border-b border-border">
        <h3 className="text-sm font-semibold text-foreground">{title}</h3>
      </div>
      <ol className="divide-y divide-border">
        {lines.map((line, i) => (
          <li key={i} className="flex gap-3 px-5 py-2.5 text-sm">
            <span className="flex-shrink-0 text-xs font-semibold tabular-nums text-muted-foreground">
              {i + 1}.
            </span>
            <span className="text-foreground/90 leading-relaxed">{line}</span>
          </li>
        ))}
      </ol>
    </section>
  )
}
