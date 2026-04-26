/**
 * FinalRow — compact "final score" row for completed games.
 *
 * Phase 2-cleanup take 3: every cell locked to a fixed width via a
 * 7-column grid so a row of FinalRows lines up like a printed
 * scoreboard. Score columns get tabular-nums + min-width so 1-digit
 * vs 3-digit games occupy the same horizontal slot. Abbr column is
 * 3.5ch — wide enough for ATH/CHW (3-char) and PHI/NYY/HOU/etc.
 *
 * Layout: [FINAL] [as] [logo] [abbr] [-] [logo] [abbr] [hs]   (extra below)
 */

import { cn } from '../../lib/utils'

export default function FinalRow({ game, onClick, extra }) {
  const { home, away } = game
  const hs = parseInt(home.score) || 0
  const as = parseInt(away.score) || 0
  const homeWon = hs > as
  const awayTone = !homeWon ? 'font-bold text-foreground' : 'font-semibold text-muted-foreground'
  const homeTone = homeWon ? 'font-bold text-foreground' : 'font-semibold text-muted-foreground'

  return (
    <div
      onClick={onClick}
      role="button"
      tabIndex={0}
      className={cn(
        'flex flex-col gap-1 rounded-lg border border-border bg-card/60 px-3 py-2.5 cursor-pointer',
        'transition-colors hover:bg-accent/40',
        'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
      )}
    >
      <div className="grid grid-cols-[2.75rem_1.75rem_1.25rem_2.5rem_0.75rem_1.25rem_2.5rem_1.75rem] items-center gap-1.5">
        <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
          Final
        </span>
        <span className={cn('text-base tabular-nums text-right', awayTone)}>{as}</span>
        {away.logo
          ? <img src={away.logo} alt="" className="h-5 w-5 object-contain" />
          : <span aria-hidden="true" />}
        <span className={cn('text-sm tabular-nums', awayTone)}>{away.abbreviation}</span>
        <span className="text-muted-foreground/60 text-center">-</span>
        {home.logo
          ? <img src={home.logo} alt="" className="h-5 w-5 object-contain" />
          : <span aria-hidden="true" />}
        <span className={cn('text-sm tabular-nums', homeTone)}>{home.abbreviation}</span>
        <span className={cn('text-base tabular-nums text-left', homeTone)}>{hs}</span>
      </div>
      {extra && (
        <div className="text-[10px] text-muted-foreground tabular-nums text-center">
          {extra}
        </div>
      )}
    </div>
  )
}
