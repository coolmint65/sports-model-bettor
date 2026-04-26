import { cn } from '../../lib/utils'

/**
 * Edge callout — STRONG / MODERATE / LEAN confidence rating shown
 * above the per-pick details on game-detail pages.
 *
 * Phase 2-cleanup restyle: Tailwind tokens; no more inline styles.
 *
 * Accepts an `edge` object of shape:
 *   { label, odds, edge, rating: 'strong' | 'moderate' | 'lean' }
 */
const RATING_STYLE = {
  strong:   'bg-positive/15 text-positive border-positive/30',
  moderate: 'bg-primary/15 text-primary border-primary/30',
  lean:     'bg-warning/15 text-warning border-warning/30',
}

const RATING_LABEL = {
  strong:   'STRONG',
  moderate: 'MODERATE',
  lean:     'LEAN',
}

export default function EdgeCallout({ edge }) {
  if (!edge) return null
  const tone = RATING_STYLE[edge.rating] || RATING_STYLE.lean
  const label = RATING_LABEL[edge.rating] || 'LEAN'

  return (
    <div className="flex items-center gap-2 rounded-md border border-border bg-card px-3 py-2">
      <span className={cn(
        'rounded px-2 py-0.5 text-[10px] font-bold tracking-wider uppercase border',
        tone,
      )}>
        {label}
      </span>
      <span className="text-sm font-semibold text-foreground">
        {edge.label}
      </span>
      <span className="text-xs text-muted-foreground tabular-nums">
        ({edge.odds > 0 ? '+' : ''}{edge.odds})
      </span>
      <span className="ml-auto text-sm font-bold tabular-nums text-positive">
        +{edge.edge.toFixed(1)}%
      </span>
    </div>
  )
}
