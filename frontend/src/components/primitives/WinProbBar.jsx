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
    // Larger variant for the game-detail page. Kept on legacy CSS for
    // now (.prob-bar-container) since the detail page hasn't been
    // restyled yet — Phase 2 frontend work focused on the scoreboard.
    return (
      <div className="prob-bar-container">
        <div className="prob-bar-labels">
          <span className={homeFavored ? 'favored' : ''}>{home.abbreviation} {pct(h)}</span>
          <span className={!homeFavored ? 'favored' : ''}>{away.abbreviation} {pct(a)}</span>
        </div>
        <div className="prob-bar">
          <div className="home" style={{ width: pct(h) }} />
          <div className="away" style={{ width: pct(a) }} />
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
