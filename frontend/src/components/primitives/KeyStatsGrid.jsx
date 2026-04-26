import { cn } from '../../lib/utils'

/**
 * KeyStatsGrid + KeyStat — replaces legacy `.key-stats` + `.key-stat`.
 *
 * Used inside SectionCards to show 2-4 inline stat tiles
 * (e.g. Total / Spread / Park, or H2H wins/losses).
 */

export function KeyStatsGrid({ children, cols = 'auto' }) {
  // cols='auto' = auto-fit to content min-w-[8rem]; explicit values
  // (2/3/4) lock the column count.
  const colClass =
    cols === 2 ? 'grid-cols-2' :
    cols === 3 ? 'grid-cols-3' :
    cols === 4 ? 'grid-cols-2 sm:grid-cols-4' :
                 'grid-cols-2 sm:grid-cols-3 lg:grid-cols-4'
  return (
    <div className={cn('grid gap-3', colClass)}>
      {children}
    </div>
  )
}

export function KeyStat({ label, value, valueClassName }) {
  const tone =
    valueClassName === 'positive' ? 'text-positive' :
    valueClassName === 'negative' ? 'text-negative' :
    valueClassName === 'warning'  ? 'text-warning'  :
    valueClassName || 'text-foreground'
  return (
    <div className="rounded-md border border-border bg-background/40 px-3 py-2.5">
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      <div className={cn('mt-0.5 text-base font-bold tabular-nums', tone)}>
        {value}
      </div>
    </div>
  )
}
