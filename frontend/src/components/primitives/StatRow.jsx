/**
 * StatRow — label / value row used inside KeyStatsGrid + ad-hoc rows
 * across all three game-detail panes.
 *
 * Phase 2-cleanup restyle: Tailwind tokens. valueClassName accepts
 * 'positive' / 'negative' / 'warning' shorthand and maps to the
 * correct token, keeping legacy callers working.
 */

import { cn } from '../../lib/utils'

const TONE_MAP = {
  positive: 'text-positive',
  negative: 'text-negative',
  warning:  'text-warning',
}

export default function StatRow({ label, value, children, valueClassName }) {
  const tone = TONE_MAP[valueClassName] || valueClassName || 'text-foreground'
  return (
    <div className="flex items-center justify-between border-b border-border/60 py-2 last:border-0">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className={cn('text-sm font-semibold tabular-nums', tone)}>
        {children ?? value}
      </span>
    </div>
  )
}
