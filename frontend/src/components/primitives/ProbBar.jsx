import { cn } from '../../lib/utils'

/**
 * Two small primitives for the "Betting Lines" section across all 3
 * sport detail pages:
 *
 *   ProbRow  - Line label + 2 probability cells (over/under, or
 *              home_cover/away_cover for run-line / puck-line).
 *   ProbBox  - Larger bordered cell used by NRFI / YRFI / first-period
 *              Over/Under highlight pairs.
 *
 * Replaces .ou-row / .ou-line / .ou-prob and .nrfi-display / .nrfi-box
 * legacy CSS.
 */

export function ProbRow({ line, leftLabel, rightLabel, leftProb, rightProb }) {
  const fav = (p) => p > 0.55
  return (
    <div className="grid grid-cols-[5rem_1fr_1fr] items-center gap-2 border-b border-border/50 py-1.5 last:border-0 text-sm">
      <span className="text-xs font-semibold text-muted-foreground tabular-nums">{line}</span>
      <span className={cn(
        'text-right tabular-nums',
        fav(leftProb) ? 'font-bold text-positive' : 'text-foreground/85',
      )}>
        {leftLabel ? <span className="text-muted-foreground mr-2">{leftLabel}</span> : null}
        {pct(leftProb)}
      </span>
      <span className={cn(
        'text-right tabular-nums',
        fav(rightProb) ? 'font-bold text-positive' : 'text-foreground/85',
      )}>
        {rightLabel ? <span className="text-muted-foreground mr-2">{rightLabel}</span> : null}
        {pct(rightProb)}
      </span>
    </div>
  )
}

export function ProbRowHeader({ leftLabel, rightLabel }) {
  return (
    <div className="grid grid-cols-[5rem_1fr_1fr] items-center gap-2 border-b border-border pb-1 mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
      <span>Line</span>
      <span className="text-right">{leftLabel}</span>
      <span className="text-right">{rightLabel}</span>
    </div>
  )
}

export function ProbBox({ label, value, sub, favored }) {
  return (
    <div className={cn(
      'flex-1 rounded-md border px-4 py-3 text-center transition-colors',
      favored
        ? 'border-positive/40 bg-positive/10'
        : 'border-border bg-background/40',
    )}>
      <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
        {label}
      </div>
      <div className={cn(
        'mt-1 text-2xl font-bold tabular-nums',
        favored ? 'text-positive' : 'text-foreground',
      )}>
        {value}
      </div>
      {sub && (
        <div className="mt-0.5 text-[10px] text-muted-foreground">{sub}</div>
      )}
    </div>
  )
}

function pct(n) {
  return `${(n * 100).toFixed(1)}%`
}
