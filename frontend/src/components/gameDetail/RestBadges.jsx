import { cn } from '../../lib/utils'

/**
 * Rest / back-to-back context pills shown under the matchup header.
 *
 * Phase 2-cleanup restyle: Tailwind tokens; no more inline styles.
 * Renders nothing when the prediction exposes no meaningful rest
 * flag.
 */
export default function RestBadges({ rest, home, away }) {
  if (!rest) return null
  const { home_b2b, away_b2b, home_rest_advantage, away_rest_advantage } = rest

  if (!home_b2b && !away_b2b && !home_rest_advantage && !away_rest_advantage) {
    return null
  }

  return (
    <div className="flex flex-wrap items-center justify-center gap-2">
      {home_b2b && <Pill tone="warn">{home.abbreviation} on back-to-back</Pill>}
      {away_b2b && <Pill tone="warn">{away.abbreviation} on back-to-back</Pill>}
      {home_rest_advantage && !away_rest_advantage && (
        <Pill tone="info">{home.abbreviation} extra rest</Pill>
      )}
      {away_rest_advantage && !home_rest_advantage && (
        <Pill tone="info">{away.abbreviation} extra rest</Pill>
      )}
    </div>
  )
}

function Pill({ tone, children }) {
  const cls = tone === 'warn'
    ? 'bg-negative/10 text-negative border-negative/25'
    : 'bg-primary/10 text-primary border-primary/20'
  return (
    <span className={cn(
      'inline-flex items-center rounded-md border px-2.5 py-0.5 text-[11px] font-semibold',
      cls,
    )}>
      {children}
    </span>
  )
}
