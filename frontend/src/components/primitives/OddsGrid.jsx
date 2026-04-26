/**
 * OddsGrid + OddsRow + CardInsight primitives.
 *
 * Three small siblings that render the bottom section of every game
 * card across MLB / NHL / NBA. Extracted so the card chrome stays
 * Tailwind-tokened and the sport-specific Scoreboards can drop the
 * inline `.game-odds-grid` / `.odds-line` / `.card-insight` legacy
 * classes.
 *
 * - OddsGrid     wraps a stack of OddsRows in a divided container
 * - OddsRow      label + away cell + home cell (3-col grid)
 * - CardInsight  one-liner why-the-pick strip with left accent
 */

import { cn } from '../../lib/utils'

export function OddsGrid({ children }) {
  return (
    <div className="rounded-md border border-border/60 bg-background/30 divide-y divide-border/60 text-[11px]">
      {children}
    </div>
  )
}

export function OddsRow({ label, away, home }) {
  return (
    <div className="grid grid-cols-[2.5rem_1fr_1fr] items-center gap-2 px-2.5 py-1.5">
      <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
        {label}
      </span>
      <span className="tabular-nums text-foreground/90 truncate">{away}</span>
      <span className="tabular-nums text-foreground/90 truncate">{home}</span>
    </div>
  )
}

export function CardInsight({ children, className }) {
  return (
    <div className={cn(
      'rounded-md border-l-2 border-muted-foreground/40 bg-muted/30 px-2.5 py-1.5 text-[11px] leading-snug text-muted-foreground',
      className,
    )}>
      {children}
    </div>
  )
}
