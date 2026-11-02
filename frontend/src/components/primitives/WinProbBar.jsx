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

// Card % = the Pick Authority's blend of our model with the de-vigged line (Austin 2026-10-26: "Any
// percentages for any card in any sport, should be that way"). The model's own number rides along as
// wp.model_home / model_away and shows underneath when it differs.
function ModelLine({ wp, first, second, firstKey, secondKey }) {
  const m1 = wp?.[`model_${firstKey}`]
  const m2 = wp?.[`model_${secondKey}`]
  if (m1 == null || m2 == null || wp?.source !== 'blend') return null
  if (Math.abs(m1 - (wp?.[firstKey] || 0)) < 0.005) return null
  return (
    <div className="mt-0.5 text-[9px] tabular-nums text-muted-foreground/70"
         title={`Bar = ${Math.round((wp.w ?? 0.15) * 100)}% our model + ${100 - Math.round((wp.w ?? 0.15) * 100)}% the de-vigged line`}>
      model: {first} {pct(m1)} · {second} {pct(m2)}
    </div>
  )
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
        <ModelLine wp={wp} first={home.abbreviation} second={away.abbreviation} firstKey="home" secondKey="away" />
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
      <ModelLine wp={wp} first={away.abbreviation} second={home.abbreviation} firstKey="away" secondKey="home" />
    </div>
  )
}
