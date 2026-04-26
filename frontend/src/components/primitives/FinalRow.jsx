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
      {/* Fixed grid: each cell has a min-width so 1- vs 3-digit scores
          and 2- vs 4-char abbrs (LAA / ATH / NYM) don't shift the dash
          column. Across a row of FinalRows the dash and scores all line
          up vertically. */}
      <div className="grid grid-cols-[3rem_1fr_auto_1fr] items-center gap-2">
        <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
          Final
        </span>
        <TeamCell team={away} score={as} winner={!homeWon} align="right" />
        <span className="text-muted-foreground/60 text-center w-3">-</span>
        <TeamCell team={home} score={hs} winner={homeWon} align="left" />
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
  // Internal sub-grid locks score / logo / abbr to fixed slots so
  // names of varying lengths never shift the score column.
  // Away: [score | logo | abbr]   right-aligned
  // Home: [logo | abbr | score]   left-aligned
  return (
    <div className={cn(
      'grid items-center gap-1.5 min-w-0',
      align === 'right'
        ? 'grid-cols-[1fr_auto_3rem] justify-end'
        : 'grid-cols-[auto_3rem_1fr] justify-start',
    )}>
      {align === 'right' && (
        <span className={cn('text-base tabular-nums text-right', tone)}>
          {score}
        </span>
      )}
      {team.logo
        ? <img src={team.logo} alt="" className="h-5 w-5 flex-shrink-0 object-contain" />
        : <span className="h-5 w-5" aria-hidden="true" />}
      <span className={cn('text-sm tabular-nums', tone, align === 'right' ? 'text-right' : 'text-left')}>
        {team.abbreviation}
      </span>
      {align === 'left' && (
        <span className={cn('text-base tabular-nums text-left', tone)}>
          {score}
        </span>
      )}
    </div>
  )
}
