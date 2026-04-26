/**
 * WinProbBar — two-color bar showing home vs away win probability.
 *
 * Phase 2d-iv restyle: Tailwind tokens. The card variant is now
 * thinner (4px) and pairs with smaller labels for better card density.
 * Detail variant keeps the larger size used inside game-detail panes.
 *
 * Color: home gets the warning hue (amber, "home cooking"), away
 * gets primary blue. Favored side renders bold in the labels.
 */

import { cn } from '../../lib/utils'

function pct(v) {
  return `${Math.round((v || 0) * 100)}%`
}

export default function WinProbBar({ wp, home, away, variant = 'card' }) {
  const h = wp?.home || 0
  const a = wp?.away || 0
  const homeFavored = h > a

  if (variant === 'detail') {
    // Larger variant for the game-detail page. Same color story as the
    // card variant (home = warning amber, away = primary blue) but at
    // ~3x the height + bigger labels so it can headline a section.
    return (
      <div className="space-y-2">
        <div className="flex justify-between text-sm tabular-nums">
          <span className={cn(homeFavored && 'font-bold text-foreground', !homeFavored && 'text-muted-foreground')}>
            {home.abbreviation} {pct(h)}
          </span>
          <span className={cn(!homeFavored && 'font-bold text-foreground', homeFavored && 'text-muted-foreground')}>
            {away.abbreviation} {pct(a)}
          </span>
        </div>
        <div className="flex h-3 overflow-hidden rounded-full bg-secondary">
          <div className="bg-warning" style={{ width: pct(h) }} />
          <div className="bg-primary" style={{ width: pct(a) }} />
        </div>
      </div>
    )
  }

  // Card variant — away first to match the existing scorecard layout.
  return (
    <div className="mt-1">
      <div className="flex justify-between text-[10px] tabular-nums text-muted-foreground mb-1">
        <span className={cn(!homeFavored && 'font-semibold text-foreground')}>
          {away.abbreviation} {pct(a)}
        </span>
        <span className={cn(homeFavored && 'font-semibold text-foreground')}>
          {home.abbreviation} {pct(h)}
        </span>
      </div>
      <div className="flex h-1 overflow-hidden rounded-full bg-secondary">
        <div className="bg-primary" style={{ width: pct(a) }} />
        <div className="bg-warning" style={{ width: pct(h) }} />
      </div>
    </div>
  )
}
