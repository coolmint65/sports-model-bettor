import { useEffect, useState, useCallback } from 'react'
import axios from 'axios'
import { cachedGet, invalidate } from '../lib/apiCache'
import SectionCard from './primitives/SectionCard'
import { cn } from '../lib/utils'

/**
 * LiveTrackerPanel — Phase 3d.
 *
 * Reads /api/{sport}/live-picks/history. Shows pending + settled
 * live picks ordered most-recent first. Each row includes the
 * snapshot fields captured at lock time (period/clock/score) so the
 * user can see the lock context alongside the result.
 *
 * Manual settle button next to the header — backend's settler runs
 * automatically on the worker cadence, but a manual sweep is useful
 * after a period closes if the user wants immediate feedback.
 */

const POLL_MS = 60000  // 60s — history changes slowly


export default function LiveTrackerPanel({ sport }) {
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(true)
  const [settling, setSettling] = useState(false)
  const [error, setError] = useState(null)

  const fetchHistory = useCallback(async () => {
    try {
      const res = await cachedGet(`/${sport}/live-picks/history`,
                                   { ttlMs: POLL_MS })
      setRows(res?.rows || [])
      setError(null)
    } catch (e) {
      setError(e.message || 'history fetch failed')
    } finally {
      setLoading(false)
    }
  }, [sport])

  useEffect(() => {
    setLoading(true)
    fetchHistory()
    const interval = setInterval(fetchHistory, POLL_MS)
    return () => clearInterval(interval)
  }, [sport, fetchHistory])

  const handleSettle = useCallback(async () => {
    setSettling(true)
    try {
      await axios.post(`/api/${sport}/live-picks/settle`)
      invalidate(`/${sport}/live-picks/history`)
      await fetchHistory()
    } catch (e) {
      alert(`Settle failed: ${e.response?.data?.detail || e.message}`)
    } finally {
      setSettling(false)
    }
  }, [sport, fetchHistory])

  const pending = rows.filter(r => !r.result)
  const settled = rows.filter(r => r.result)
  const wins = settled.filter(r => r.result === 'W').length
  const losses = settled.filter(r => r.result === 'L').length
  const pushes = settled.filter(r => r.result === 'P').length
  const decided = wins + losses
  const winPct = decided > 0 ? Math.round((wins / decided) * 1000) / 10 : 0
  const totalProfit = settled.reduce((s, r) => s + (r.profit || 0), 0)
  const profitTone = totalProfit > 0 ? 'text-positive'
                   : totalProfit < 0 ? 'text-negative' : 'text-foreground'

  // Per-scope breakdown for the tile grid (mirrors PropsPanel's
  // per-bet-type tiles). Sort by sample size so most-active scopes
  // lead. 'full' is renamed to "Full Game" for display clarity.
  const byScope = (() => {
    const out = {}
    for (const r of settled) {
      const k = r.market_scope || 'full'
      const s = out[k] || (out[k] = { wins: 0, losses: 0, pushes: 0, profit: 0, total: 0 })
      s.total += 1
      if (r.result === 'W') s.wins += 1
      else if (r.result === 'L') s.losses += 1
      else if (r.result === 'P') s.pushes += 1
      s.profit += Number(r.profit || 0)
    }
    return Object.entries(out).sort(([, a], [, b]) => b.total - a.total)
  })()

  if (loading && rows.length === 0) {
    return (
      <div className="flex items-center justify-center gap-3 rounded-lg border border-border bg-card py-12 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        Loading live tracker…
      </div>
    )
  }

  if (error) {
    return (
      <div className="rounded-lg border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
        {error}
      </div>
    )
  }

  if (rows.length === 0) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center text-sm text-muted-foreground">
        No live picks locked yet. Lock picks from the Live tab to track them here.
      </div>
    )
  }

  return (
    <div className="space-y-5 py-4">
      {/* Page header — matches PropsPanel rhythm */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold tracking-tight text-foreground">
            {sport.toUpperCase()} Live Tracker
          </h2>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Locked snapshots from the live engine ·{' '}
            {pending.length} pending · {settled.length} settled
          </p>
        </div>
        <button
          type="button"
          onClick={handleSettle}
          disabled={settling}
          className={cn(
            'rounded-md px-3 py-1.5 text-xs font-semibold transition-opacity',
            settling
              ? 'bg-muted text-muted-foreground cursor-wait'
              : 'bg-positive-strong text-background hover:opacity-90',
          )}
        >
          {settling ? 'Settling…' : 'Settle completed'}
        </button>
      </div>

      {/* Hero stats block — mirrors PropsPanel hero card */}
      <div className="rounded-lg border border-border bg-card p-4">
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
          <HeroStat label="Record" value={`${wins}-${losses}${pushes ? `-${pushes}` : ''}`}
                    sub={decided > 0 ? `${winPct.toFixed(1)}% WR` : '—'} />
          <HeroStat label="Profit"
                    value={`${totalProfit >= 0 ? '+' : ''}$${totalProfit.toFixed(2)}`}
                    valueClassName={profitTone}
                    sub={decided > 0 ? `${(totalProfit / decided).toFixed(2)} avg` : '—'} />
          <HeroStat label="Pending" value={pending.length}
                    sub={pending.length === 0 ? 'all caught up' : 'awaiting close'} />
          <HeroStat label="Total locked" value={rows.length}
                    sub={`${settled.length} resolved`} />
        </div>
        {byScope.length > 0 && (
          <div className="mt-4 grid grid-cols-2 gap-2 border-t border-border pt-4 sm:grid-cols-4">
            {byScope.map(([scope, s]) => (
              <ScopeTile key={scope} scope={scope} stats={s} />
            ))}
          </div>
        )}
      </div>

      {pending.length > 0 && (
        <SectionCard title={`Pending (${pending.length})`}>
          <div className="space-y-1.5">
            {pending.map(r => <Row key={r.id} row={r} sport={sport} />)}
          </div>
        </SectionCard>
      )}
      {settled.length > 0 && (
        <SectionCard title={`Settled (${settled.length})`}>
          <div className="space-y-1.5">
            {settled.map(r => <Row key={r.id} row={r} sport={sport} />)}
          </div>
        </SectionCard>
      )}
    </div>
  )
}


// Sport-aware period label. NBA periods are quarters (Q1-Q4) plus OT
// (period 5+); NHL periods are P1-P3 plus OT (period 4) / SO (5).
function periodLabel(sport, periodNum) {
  if (!periodNum) return ''
  const n = Number(periodNum)
  if (!Number.isFinite(n) || n <= 0) return ''
  if (sport === 'nba' || sport === 'wnba' || sport === 'afl') {
    if (n <= 4) return `Q${n}`
    return `OT${n - 4 > 1 ? n - 4 : ''}`  // OT1, OT2, OT3...
  }
  if (sport === 'ncaam') {
    if (n <= 2) return `H${n}`
    return `OT${n - 2 > 1 ? n - 2 : ''}`
  }
  if (sport === 'nhl') {
    if (n <= 3) return `P${n}`
    if (n === 4) return 'OT'
    return 'SO'
  }
  return `P${n}`
}


// True when the clock string represents 0:00 (period boundary). The
// live state stamps these with various forms — "0.0" / "0:00" /
// "0" — so accept any zero-only token.
function isPeriodEnd(clockStr) {
  if (!clockStr) return false
  const trimmed = String(clockStr).trim()
  if (!trimmed) return false
  // Matches "0", "0:00", "0.0", "00:00" etc.
  return /^0+([.:]0+)*$/.test(trimmed.replace(/\s+/g, ''))
}


// Format the lock-time context string. "End of Q1" when locked at the
// period boundary, otherwise "Q1 8:42".
function lockedAtLabel(sport, periodNum, clockStr) {
  const period = periodLabel(sport, periodNum)
  if (!period) return ''
  if (isPeriodEnd(clockStr)) return `End of ${period}`
  if (!clockStr) return period
  return `${period} ${clockStr}`
}


function HeroStat({ label, value, sub, valueClassName }) {
  return (
    <div>
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      <div className={cn('mt-0.5 text-xl font-bold tabular-nums', valueClassName)}>
        {value}
      </div>
      {sub && (
        <div className="mt-0.5 text-[11px] text-muted-foreground tabular-nums">{sub}</div>
      )}
    </div>
  )
}


function ScopeTile({ scope, stats }) {
  const decided = stats.wins + stats.losses
  const wr = decided > 0 ? Math.round((stats.wins / decided) * 1000) / 10 : 0
  const profitTone = stats.profit > 0 ? 'text-positive'
                   : stats.profit < 0 ? 'text-negative' : 'text-foreground'
  const scopeLabel = scope === 'full' ? 'Full Game' : scope
  return (
    <div className="rounded-md border border-border bg-background/40 px-3 py-2">
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {scopeLabel}
      </div>
      <div className="mt-0.5 flex items-baseline justify-between gap-2">
        <span className="text-sm font-semibold tabular-nums text-foreground">
          {stats.wins}-{stats.losses}{stats.pushes ? `-${stats.pushes}` : ''}
        </span>
        <span className="text-[11px] tabular-nums text-muted-foreground">
          {decided > 0 ? `${wr.toFixed(1)}%` : '—'}
        </span>
      </div>
      <div className={cn('mt-0.5 text-xs tabular-nums font-medium', profitTone)}>
        {stats.profit >= 0 ? '+' : ''}${stats.profit.toFixed(2)}
      </div>
    </div>
  )
}


function Row({ row, sport }) {
  const result = row.result
  const resultClass = result === 'W' ? 'bg-positive/15 text-positive border-positive/40'
                    : result === 'L' ? 'bg-negative/15 text-negative border-negative/40'
                    : result === 'P' ? 'bg-muted/30 text-muted-foreground border-border'
                    : 'bg-secondary text-muted-foreground border-border'

  const periodPart = lockedAtLabel(sport, row.pick_at_period, row.pick_at_clock)
  const scorePart = row.pick_at_period
    ? `(${row.pick_at_away_score ?? '?'}-${row.pick_at_home_score ?? '?'})`
    : ''
  const lockedAt = periodPart ? `${periodPart} ${scorePart}`.trim() : '—'

  return (
    <div className="grid grid-cols-[auto_1fr_auto_auto] items-center gap-3 rounded-md border border-border bg-background/40 px-3 py-2 text-sm">
      <span className="rounded bg-secondary px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground flex-shrink-0">
        {row.market_scope || 'full'}
      </span>
      <div className="min-w-0">
        <div className="flex items-baseline gap-2 truncate">
          <span className="font-medium text-foreground">{row.bet_type}</span>
          <span className="truncate text-foreground/90">{row.pick}</span>
        </div>
        <div className="mt-0.5 text-[11px] text-muted-foreground tabular-nums">
          {row.matchup} · locked at {lockedAt}
        </div>
      </div>
      <div className="text-sm tabular-nums font-medium text-foreground flex-shrink-0">
        {row.odds > 0 ? '+' : ''}{row.odds}
      </div>
      <div className={cn(
        'rounded-md border px-2 py-1 text-xs font-bold uppercase tabular-nums flex-shrink-0',
        resultClass,
      )}>
        {result || 'PEND'}
        {row.profit != null && (
          <span className="ml-2 text-[10px] font-semibold">
            {row.profit > 0 ? '+' : ''}${row.profit.toFixed(2)}
          </span>
        )}
      </div>
    </div>
  )
}
