import { memo, useMemo } from 'react'
import { cn } from '../lib/utils'
import TeamRow from './primitives/TeamRow'

/**
 * FirstInningPicks — NRFI/YRFI surface section above the MLB scoreboard.
 *
 * Phase 2-cleanup restyle: Tailwind tokens. Backend already filters by
 * MLB_*_MIN_EDGE so anything reaching this component is playable.
 */
function FirstInningPicksImpl({ bestBets, games }) {
  const firstInningPicks = useMemo(() => {
    const out = []
    const covered = new Set()
    for (const bet of (bestBets || [])) {
      const fi = (bet.all_picks || []).find(p => p.type === '1st INN'
                                  && (p.confidence || 'lean') !== 'skip')
      if (!fi) continue
      out.push({ game_id: bet.game_id, matchup: bet.matchup, home: bet.home,
                 away: bet.away, time: bet.time, pick: fi, paper: false })
      covered.add(String(bet.game_id))
    }
    // PAPER fill (Austin 2026-09-19): the model's NRFI/YRFI lean for every game with no gated
    // 1st-inning rec — from the scoreboard-merged first_inning_paper (MC nrfi vs HR's I1 market).
    for (const g of (games || [])) {
      const pp = g.first_inning_paper
      if (pp && !covered.has(String(g.id))) {
        out.push({ game_id: g.id, matchup: `${g.away?.abbreviation ?? ''} @ ${g.home?.abbreviation ?? ''}`,
                   home: g.home, away: g.away, time: g.date, pick: pp, paper: true })
      }
    }
    out.sort((a, b) => (b.pick.edge || 0) - (a.pick.edge || 0))
    return out
  }, [bestBets, games])

  if (firstInningPicks.length === 0) return null

  const pct = n => `${(n * 100).toFixed(1)}%`

  return (
    <section className="mb-5">
      <header className="mb-2 flex items-center gap-2">
        <h3 className="text-lg font-semibold tracking-tight text-foreground">
          First Inning Picks
        </h3>
        <span className="rounded-full bg-muted px-2 py-0.5 text-[11px] font-semibold tabular-nums text-muted-foreground">
          {firstInningPicks.length}
        </span>
      </header>
      <p className="mb-3 text-xs text-muted-foreground">
        NRFI / YRFI plays the model likes for today's slate
      </p>
      <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-3">
        {firstInningPicks.map(entry => (
          <FirstInningCard key={entry.game_id} entry={entry} pct={pct} />
        ))}
      </div>
    </section>
  )
}


function FirstInningCard({ entry, pct }) {
  const { pick, home, away } = entry
  const isNrfi = pick.pick === 'NRFI'
  const confTint =
    pick.edge >= 8 ? 'border-l-positive' :
    pick.edge >= 4 ? 'border-l-primary'  :
                     'border-l-border'
  const gameTime = useMemo(
    () => entry.time
      ? new Date(entry.time).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })
      : '',
    [entry.time],
  )
  return (
    <div className={cn(
      'rounded-lg border border-border border-l-4 bg-card px-3 py-2.5',
      confTint,
    )}>
      <div className="mb-1.5 flex items-center justify-between text-[11px]">
        <span className="flex items-center gap-1.5 font-semibold uppercase tracking-wider text-muted-foreground">
          1st Inn
          {entry.paper && <span className="rounded-sm bg-sky-500/25 px-1 text-[8px] font-bold uppercase tracking-wider text-sky-300">Paper</span>}
        </span>
        {gameTime && <span className="tabular-nums text-muted-foreground">{gameTime}</span>}
      </div>
      {away && <TeamRow team={away} sport="mlb" />}
      {home && <TeamRow team={home} sport="mlb" />}
      <div className="mt-2 flex items-baseline justify-between border-t border-border/60 pt-2">
        <span className={cn('text-lg font-bold', isNrfi ? 'text-primary' : 'text-warning')}>
          {pick.pick}
        </span>
        <span className="text-sm font-semibold tabular-nums text-muted-foreground">
          {pick.odds > 0 ? '+' : ''}{pick.odds}
        </span>
      </div>
      <div className="mt-1 flex items-center justify-between text-xs">
        <span className="tabular-nums text-foreground/85">{pct(pick.prob)}</span>
        <span className={cn('font-semibold tabular-nums', pick.edge >= 0 ? 'text-positive' : 'text-muted-foreground')}>
          {pick.edge >= 0 ? '+' : ''}{pick.edge}% edge
        </span>
      </div>
    </div>
  )
}


const FirstInningPicks = memo(FirstInningPicksImpl)
export default FirstInningPicks
