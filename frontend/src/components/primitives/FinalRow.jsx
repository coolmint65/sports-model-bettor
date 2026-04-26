/**
 * FinalRow — compact "final score" row for completed games.
 *
 * Phase 2d-v restyle: Tailwind tokens. Fits in the same scoreboard
 * grid as the active GameCard but with a flatter, more scannable
 * layout because there's no pick or live signal to draw the eye.
 *
 * NBA appends a Q1 recap line via the optional `extra` slot.
 */

import { cn } from '../../lib/utils'

export default function FinalRow({ game, onClick, extra }) {
  const { home, away } = game
  const hs = parseInt(home.score) || 0
  const as = parseInt(away.score) || 0
  const homeWon = hs > as

  return (
    <div
      onClick={onClick}
      role="button"
      tabIndex={0}
      className={cn(
        'flex items-center gap-3 rounded-lg border border-border bg-card/60 px-3 py-2.5 cursor-pointer',
        'transition-colors hover:bg-accent/40',
        'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
      )}
    >
      <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
        Final
      </span>
      <div className="flex flex-1 items-center justify-center gap-2 min-w-0">
        <div className="flex items-center gap-1.5 min-w-0">
          {away.logo && <img src={away.logo} alt="" className="h-5 w-5 object-contain" />}
          <span className={cn(
            'text-sm tabular-nums',
            !homeWon ? 'font-bold text-foreground' : 'font-semibold text-muted-foreground',
          )}>
            {away.abbreviation}
          </span>
        </div>
        <span className={cn(
          'text-base tabular-nums px-1',
          !homeWon ? 'font-bold text-foreground' : 'font-semibold text-muted-foreground',
        )}>
          {as}
        </span>
        <span className="text-muted-foreground/60">-</span>
        <span className={cn(
          'text-base tabular-nums px-1',
          homeWon ? 'font-bold text-foreground' : 'font-semibold text-muted-foreground',
        )}>
          {hs}
        </span>
        <div className="flex items-center gap-1.5 min-w-0">
          {home.logo && <img src={home.logo} alt="" className="h-5 w-5 object-contain" />}
          <span className={cn(
            'text-sm tabular-nums',
            homeWon ? 'font-bold text-foreground' : 'font-semibold text-muted-foreground',
          )}>
            {home.abbreviation}
          </span>
        </div>
      </div>
      {extra && (
        <div className="text-[10px] text-muted-foreground tabular-nums whitespace-nowrap">
          {extra}
        </div>
      )}
    </div>
  )
}
