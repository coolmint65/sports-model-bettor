/**
 * PlacementsView — auto-placer control center.
 *
 * Composes the launcher, stake envelope, circuit breakers, relay
 * health, and placement log into one full-page surface. Every card
 * fetches its own data on a 30s cadence so the panel stays live
 * without a global refresh dance.
 */
import { useCallback, useEffect, useState } from 'react'
import { cn } from '../../lib/utils'
import LauncherCard from './placer/LauncherCard'
import CapsCard from './placer/CapsCard'
import BreakersCard from './placer/BreakersCard'
import BalanceCard from './placer/BalanceCard'
import RelayCard from './placer/RelayCard'
import PlacementsTable from './placer/PlacementsTable'


export default function PlacementsView({ api }) {
  const [summary, setSummary] = useState(null)
  const [rows, setRows] = useState([])
  const [mode, setMode] = useState('all')
  const [loading, setLoading] = useState(false)

  const refreshLog = useCallback(() => {
    setLoading(true)
    const params = {}
    if (mode !== 'all') params.mode = mode
    Promise.all([
      api.get('/bet-queue/placements/summary'),
      api.get('/bet-queue/placements', { params }),
    ])
      .then(([s, r]) => { setSummary(s.data); setRows(r.data?.rows || []) })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [api, mode])

  useEffect(() => { refreshLog() }, [refreshLog])

  return (
    <div className="space-y-4">
      <LauncherCard api={api} />

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2">
          <SummaryHero summary={summary} />
        </div>
        <div className="space-y-4">
          <RelayCard api={api} />
          <BalanceCard api={api} />
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <CapsCard api={api} />
        <BreakersCard api={api} />
      </div>

      <section>
        <div className="flex items-baseline justify-between mb-2">
          <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
            Placement Log
          </div>
          <div className="flex items-center gap-2">
            <ModeFilter value={mode} onChange={setMode} />
            <button
              onClick={refreshLog}
              disabled={loading}
              className="rounded-md border border-border bg-card px-2.5 py-1 text-[10px] uppercase tracking-wider text-muted-foreground hover:text-foreground disabled:opacity-50"
            >
              {loading ? 'Refreshing…' : 'Refresh'}
            </button>
          </div>
        </div>
        <PlacementsTable rows={rows} />
      </section>
    </div>
  )
}


function SummaryHero({ summary }) {
  if (!summary) {
    return (
      <section className="rounded-xl border border-border bg-card p-5 text-sm text-muted-foreground">
        Loading summary…
      </section>
    )
  }
  const live = summary.live || {}
  const today = summary.today || {}
  const profit = Number(live.profit || 0)
  const profitTone = profit > 0.001 ? 'text-positive'
    : profit < -0.001 ? 'text-negative' : 'text-foreground'
  const todayProfit = Number(today.profit || 0)
  const todayTone = todayProfit > 0.001 ? 'text-positive'
    : todayProfit < -0.001 ? 'text-negative' : 'text-foreground'
  return (
    <section className="rounded-xl border border-border bg-card p-5 h-full">
      <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-3">
        Live-fire Performance
      </div>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Stat
          label="All-time P/L"
          value={`${profit >= 0 ? '+' : ''}$${profit.toFixed(2)}`}
          tone={profitTone}
        />
        <Stat
          label="Bets placed"
          value={live.placed ?? 0}
          hint={`${live.attempted ?? 0} attempted`}
        />
        <Stat
          label="Total staked"
          value={`$${Number(live.staked || 0).toFixed(2)}`}
        />
        <Stat
          label="Today"
          value={`${todayProfit >= 0 ? '+' : ''}$${todayProfit.toFixed(2)}`}
          hint={`${today.placed ?? 0} placed · $${Number(today.staked || 0).toFixed(2)} staked`}
          tone={todayTone}
        />
      </div>
    </section>
  )
}


function Stat({ label, value, hint, tone }) {
  return (
    <div>
      <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
        {label}
      </div>
      <div className={cn('mt-0.5 text-2xl font-bold tabular-nums',
                          tone || 'text-foreground')}>
        {value}
      </div>
      {hint && (
        <div className="text-[10px] text-muted-foreground tabular-nums">
          {hint}
        </div>
      )}
    </div>
  )
}


function ModeFilter({ value, onChange }) {
  const opts = [
    { id: 'all',     label: 'All' },
    { id: 'live',    label: 'Live' },
    { id: 'dry_run', label: 'Dry-run' },
  ]
  return (
    <div className="flex rounded-md border border-border overflow-hidden">
      {opts.map(o => (
        <button
          key={o.id}
          onClick={() => onChange(o.id)}
          className={cn(
            'px-3 py-1 text-[10px] font-semibold uppercase tracking-wider',
            value === o.id
              ? 'bg-primary text-primary-foreground'
              : 'bg-card text-muted-foreground hover:text-foreground',
          )}
        >
          {o.label}
        </button>
      ))}
    </div>
  )
}
