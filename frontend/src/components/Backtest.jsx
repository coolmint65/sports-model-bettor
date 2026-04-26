import { useState } from 'react'
import { cn } from '../lib/utils'

/**
 * Backtest — Phase 2d restyle. Controls + summary + per-category
 * breakdown grid, all on Tailwind. Inputs use the design tokens so
 * they sit consistently next to the rest of the new shell. Numerics
 * stay tabular-nums for clean column alignment.
 */
export default function Backtest({ data, loading, onRun }) {
  const currentYear = new Date().getFullYear()
  const [season, setSeason] = useState(String(currentYear))
  const [days, setDays] = useState('')
  const [minEdge, setMinEdge] = useState('3')

  const controls = (
    <div className="flex flex-wrap items-end gap-3">
      <Field label="Season">
        <select
          value={season}
          onChange={e => setSeason(e.target.value)}
          className="rounded-md border border-border bg-background px-2.5 py-1.5 text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-ring"
        >
          {[currentYear, currentYear - 1, currentYear - 2, currentYear - 3].map(y => (
            <option key={y} value={y}>{y}</option>
          ))}
        </select>
      </Field>
      <Field label="Last N days">
        <input
          type="number"
          placeholder="All"
          value={days}
          onChange={e => setDays(e.target.value)}
          className="w-24 rounded-md border border-border bg-background px-2.5 py-1.5 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
        />
      </Field>
      <Field label="Min edge %">
        <input
          type="number"
          placeholder="0"
          step="0.5"
          value={minEdge}
          onChange={e => setMinEdge(e.target.value)}
          className="w-24 rounded-md border border-border bg-background px-2.5 py-1.5 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
        />
      </Field>
      <button
        onClick={() => onRun(days || 0, minEdge || 0, season)}
        disabled={loading}
        className={cn(
          'rounded-md bg-primary px-4 py-1.5 text-sm font-semibold text-primary-foreground transition-colors',
          loading ? 'opacity-60 cursor-not-allowed' : 'hover:opacity-90',
        )}
      >
        {loading ? 'Running…' : 'Run Backtest'}
      </button>
    </div>
  )

  const Header = ({ subtitle }) => (
    <div>
      <h2 className="text-xl font-semibold tracking-tight text-foreground">
        Model Performance
      </h2>
      <p className="mt-0.5 text-xs text-muted-foreground">
        {subtitle}
      </p>
    </div>
  )

  if (loading) {
    return (
      <div className="space-y-4 py-4">
        <Header subtitle={`Backtesting ${season}…`} />
        {controls}
        <div className="flex items-center justify-center gap-3 rounded-lg border border-border bg-card py-12 text-sm text-muted-foreground">
          <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
          {parseInt(season) < currentYear
            ? `Loading ${season} season data and running backtest — this may take a minute.`
            : 'Running backtest against historical games…'}
        </div>
      </div>
    )
  }

  if (!data || data.error) {
    return (
      <div className="space-y-4 py-4">
        <Header subtitle="Replay the model against past games to validate edge claims." />
        {controls}
        <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center text-sm text-muted-foreground">
          {data?.error || 'Select a season and click Run Backtest.'}
        </div>
      </div>
    )
  }

  const cats = [
    { key: 'moneyline',  label: 'Moneyline' },
    { key: 'over_under', label: 'Over/Under' },
    { key: 'nrfi',       label: 'NRFI / YRFI' },
    { key: 'run_line',   label: 'Run Line' },
  ]

  const bb = data.best_bet || {}
  const totalProfit = bb.profit || 0
  const totalBets = bb.total_bets || 0
  const totalWins = bb.wins || 0
  const totalLosses = bb.losses || 0
  const roi = totalBets > 0 ? totalProfit / totalBets : 0
  const profitTone = totalProfit > 0 ? 'text-positive' : totalProfit < 0 ? 'text-negative' : 'text-foreground'

  return (
    <div className="space-y-5 py-4">
      <Header subtitle={`${season} season replay · best-bet-per-game basis`} />
      {controls}

      {/* Summary */}
      <section className="rounded-lg border border-border bg-card p-5">
        <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground mb-3">
          Summary
        </div>
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-5">
          <Stat big={data.games_tested}        label="Games" />
          <Stat big={totalBets}                 label="Bets" />
          <Stat big={`${totalWins}-${totalLosses}`} label="Record" />
          <Stat big={`$${totalProfit.toFixed(0)}`}  label="Profit" tone={profitTone} />
          <Stat big={`${roi >= 0 ? '' : ''}${roi.toFixed(1)}%`} label="ROI" tone={profitTone} />
        </div>
      </section>

      {/* Per-category breakdown */}
      <section className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {cats.map(({ key, label }) => {
          const bt = data[key]
          if (!bt || bt.total_bets === 0) return null
          const profitable = bt.profit > 0
          const wrTone = bt.win_pct > 55 ? 'text-positive' : bt.win_pct < 45 ? 'text-negative' : 'text-foreground'
          return (
            <div
              key={key}
              className={cn(
                'rounded-lg border bg-card p-4',
                profitable ? 'border-positive/30' : 'border-border',
              )}
            >
              <div className="flex items-center justify-between">
                <div className="text-sm font-semibold text-foreground">{label}</div>
                <span className={cn(
                  'rounded-full px-2 py-0.5 text-[9px] font-semibold uppercase tracking-wider',
                  profitable
                    ? 'bg-positive/15 text-positive'
                    : 'bg-negative/15 text-negative',
                )}>
                  {profitable ? 'Profitable' : 'Losing'}
                </span>
              </div>
              <dl className="mt-3 space-y-1.5 text-xs">
                <Row k="Record" v={`${bt.wins}-${bt.losses}`} />
                <Row k="Win rate" v={`${bt.win_pct}%`} tone={wrTone} />
                <Row k="Profit" v={`$${bt.profit.toFixed(0)}`} tone={profitable ? 'text-positive' : 'text-negative'} />
                <Row k="ROI" v={`${bt.roi > 0 ? '+' : ''}${bt.roi}%`} tone={bt.roi > 0 ? 'text-positive' : 'text-negative'} />
              </dl>
            </div>
          )
        })}
      </section>

      <p className="text-[11px] text-muted-foreground italic">
        Based on $100 flat bets. ML uses -150/+130 standard lines. O/U and
        Run Line use -110. NRFI uses -120. Past performance does not
        guarantee future results.
      </p>
    </div>
  )
}

function Field({ label, children }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </span>
      {children}
    </label>
  )
}

function Stat({ big, label, tone }) {
  return (
    <div>
      <div className={cn('text-2xl font-bold tabular-nums', tone || 'text-foreground')}>
        {big}
      </div>
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
    </div>
  )
}

function Row({ k, v, tone }) {
  return (
    <div className="flex items-center justify-between">
      <dt className="text-muted-foreground">{k}</dt>
      <dd className={cn('font-semibold tabular-nums', tone || 'text-foreground')}>{v}</dd>
    </div>
  )
}
