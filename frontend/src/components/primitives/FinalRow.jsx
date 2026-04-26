/**
 * FinalRow — compact "final score" row for completed games.
 *
 * Phase 2d-vi: redo the inner layout. Previous flex-row crashed
 * 3-digit scores into the team abbreviations on narrow cards
 * (DET105, OKC121 visibly clipped) and pushed the NBA Q1 recap
 * into the team labels. New shape:
 *
 *   Row 1: [FINAL]  [away abbr]  AS  -  HS  [home abbr]
 *   Row 2: [extra: Q1 recap]   (only when present)
 *
 * Score cells get a min-width so 1- vs 3-digit scores stay
 * visually anchored. Whole block stays clickable.
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
        'flex flex-col gap-1.5 rounded-lg border border-border bg-card/60 px-3 py-2.5 cursor-pointer',
        'transition-colors hover:bg-accent/40',
        'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
      )}
    >
      <div className="flex items-center gap-3">
        <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
          Final
        </span>
        <div className="flex flex-1 items-center justify-center gap-3 min-w-0">
          <TeamCell
            team={away}
            score={as}
            winner={!homeWon}
            align="right"
          />
          <span className="text-muted-foreground/60 px-0.5">-</span>
          <TeamCell
            team={home}
            score={hs}
            winner={homeWon}
            align="left"
          />
        </div>
      </div>
      {extra && (
        <div className="text-[10px] text-muted-foreground tabular-nums text-center">
          {extra}
        </div>
      )}
    </div>
  )
}

function TeamCell({ team, score, winner, align }) {
  const tone = winner
    ? 'font-bold text-foreground'
    : 'font-semibold text-muted-foreground'
  return (
    <div
      className={cn(
        'flex items-center gap-1.5 min-w-0',
        align === 'right' && 'justify-end',
      )}
    >
      {align === 'right' && (
        <span className={cn('text-base tabular-nums min-w-[2ch] text-right', tone)}>
          {score}
        </span>
      )}
      {team.logo && (
        <img
          src={team.logo}
          alt=""
          className="h-5 w-5 flex-shrink-0 object-contain"
        />
      )}
      <span className={cn('text-sm tabular-nums', tone)}>
        {team.abbreviation}
      </span>
      {align === 'left' && (
        <span className={cn('text-base tabular-nums min-w-[2ch]', tone)}>
          {score}
        </span>
      )}
    </div>
  )
}
