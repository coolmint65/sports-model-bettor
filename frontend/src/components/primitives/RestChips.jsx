/**
 * RestChips — "tired"/"B2B"/"rested" chip row above each game card.
 *
 * Phase 2-cleanup restyle: Tailwind tokens. Sport label variance:
 * MLB uses "tired", NHL+NBA use "B2B". Renders nothing when no rest
 * signal is present so callers can mount unconditionally.
 *
 * Signals:
 *   home_b2b / home_short_rest        -> negative "tired" / "B2B" chip
 *   home_rest_advantage (one-sided)   -> primary "rested" chip
 */

import { cn } from '../../lib/utils'

export default function RestChips({ rest, home, away, tiredLabel = 'tired' }) {
  if (!rest) return null

  const homeTired = rest.home_b2b || rest.home_short_rest
  const awayTired = rest.away_b2b || rest.away_short_rest
  const homeRested = rest.home_rest_advantage && !rest.away_rest_advantage
  const awayRested = rest.away_rest_advantage && !rest.home_rest_advantage

  if (!homeTired && !awayTired && !homeRested && !awayRested) return null

  return (
    <div className="flex flex-wrap gap-1">
      {awayTired && <Chip tone="warn">{away.abbreviation} {tiredLabel}</Chip>}
      {homeTired && <Chip tone="warn">{home.abbreviation} {tiredLabel}</Chip>}
      {awayRested && <Chip tone="info">{away.abbreviation} rested</Chip>}
      {homeRested && <Chip tone="info">{home.abbreviation} rested</Chip>}
    </div>
  )
}

function Chip({ tone, children }) {
  const cls = tone === 'warn'
    ? 'bg-negative/15 text-negative border-negative/25'
    : 'bg-primary/10 text-primary border-primary/25'
  return (
    <span className={cn(
      'inline-flex items-center rounded border px-1.5 py-0.5 text-[10px] font-bold',
      cls,
    )}>
      {children}
    </span>
  )
}
