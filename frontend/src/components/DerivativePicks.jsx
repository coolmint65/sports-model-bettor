import { memo, useMemo } from 'react'
import { cn } from '../lib/utils'
import TeamRow from './primitives/TeamRow'
import { derivMarketLabel } from '../lib/betType'

/**
 * DerivativePicks - per-game derivative (F5 / team-total) PAPER leans, rendered in
 * the same rich card style as the full-game cards (Austin 2026-09-19). Sourced from
 * the scoreboard-merged game.derivative_paper (MLB MC over/under vs HR line).
 */
function DerivativePicksImpl({ games }) {
  const picks = useMemo(() => {
    const out = []
    for (const g of (games || [])) {
      const dp = g.derivative_paper
      if (dp) out.push({ game_id: g.id, home: g.home, away: g.away, time: g.date, pick: dp })
    }
    out.sort((a, b) => (b.pick.edge || 0) - (a.pick.edge || 0))
    return out
  }, [games])

  if (picks.length === 0) return null
  const pct = n => `${(n * 100).toFixed(1)}%`

  return (
    <section className="mb-5">
      <header className="mb-2 flex items-center gap-2">
        <h3 className="text-lg font-semibold tracking-tight text-foreground">Derivative Picks</h3>
        <span className="rounded-full bg-muted px-2 py-0.5 text-[11px] font-semibold tabular-nums text-muted-foreground">{picks.length}</span>
      </header>
      <p className="mb-3 text-xs text-muted-foreground">F5 / team-total leans the model likes for today's slate</p>
      <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-3">
        {picks.map(entry => <DerivativeCard key={entry.game_id} entry={entry} pct={pct} />)}
      </div>
    </section>
  )
}

function DerivativeCard({ entry, pct }) {
  const { pick, home, away } = entry
  const confTint =
    pick.edge >= 8 ? 'border-l-positive' :
    pick.edge >= 4 ? 'border-l-primary'  :
                     'border-l-border'
  const gameTime = useMemo(
    () => entry.time ? new Date(entry.time).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }) : '',
    [entry.time],
  )
  const label = `${pick.pick}${pick.line != null ? ` ${pick.line}` : ''}`
  return (
    <div className={cn('rounded-lg border border-border border-l-4 bg-card px-3 py-2.5', confTint)}>
      <div className="mb-1.5 flex items-center justify-between text-[11px]">
        <span className="flex items-center gap-1.5 font-semibold uppercase tracking-wider text-muted-foreground">
          {derivMarketLabel(pick, { homeAbbr: home?.abbr, awayAbbr: away?.abbr, homeName: home?.name, awayName: away?.name })}
          <span className="rounded-sm bg-sky-500/25 px-1 text-[8px] font-bold uppercase tracking-wider text-sky-300">Paper</span>
        </span>
        {gameTime && <span className="tabular-nums text-muted-foreground">{gameTime}</span>}
      </div>
      {away && <TeamRow team={away} sport="mlb" />}
      {home && <TeamRow team={home} sport="mlb" />}
      <div className="mt-2 flex items-baseline justify-between border-t border-border/60 pt-2">
        <span className="text-lg font-bold text-primary">{label}</span>
        <span className="text-sm font-semibold tabular-nums text-muted-foreground">{pick.odds > 0 ? '+' : ''}{pick.odds}</span>
      </div>
      <div className="mt-1 flex items-center justify-between text-xs">
        <span className="tabular-nums text-foreground/85">{pct(pick.prob)}</span>
        <span className={cn('font-semibold tabular-nums', pick.edge >= 0 ? 'text-positive' : 'text-muted-foreground')}>{pick.edge >= 0 ? '+' : ''}{pick.edge}% edge</span>
      </div>
    </div>
  )
}

const DerivativePicks = memo(DerivativePicksImpl)
export default DerivativePicks
