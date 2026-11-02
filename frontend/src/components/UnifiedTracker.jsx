import { useEffect, useMemo, useState } from 'react'
import { cn } from '../lib/utils'
import PicksTable from './primitives/PicksTable'

/**
 * UnifiedTracker — ONE per-sport tracker across EVERY market (MintyBot
 * 2026-10-15, the "tracker merge"). Consumes GET /api/{sport}/tracker/unified,
 * which unions the sport's market-scoped pick stores (e.g. NHL: full game +
 * period/team-total + P1 + player props) into one payload. Renders a combined
 * P/L hero + a per-market tile row, and a single picks list
 * with a market filter. Additive: the legacy per-market tabs/endpoints are
 * untouched — this is the merged surface.
 */

// Preferred display order; unknown markets fall to the end, stable.
const MARKET_ORDER = ['Full Game', 'First Inning', 'Derivative', 'First Half', 'Period', 'Alt Line', 'Team Total', 'Period / Team Total', 'P1', 'Q1', 'Q1 Team Total']

function fmtMoney(n) {
  if (n == null || isNaN(n)) return '$0'
  const v = Math.round(n * 100) / 100
  return `${v > 0 ? '+' : ''}$${v}`
}

function Th({ children, align = 'left' }) {
  return (
    <th className={cn(
      'px-3 py-2 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground',
      align === 'right' ? 'text-right' : align === 'center' ? 'text-center' : 'text-left',
    )}>{children}</th>
  )
}

function Td({ children, className }) {
  return <td className={cn('px-3 py-2 align-top', className)}>{children}</td>
}

function FilterPill({ active, onClick, children }) {
  return (
    <button
      onClick={onClick}
      className={cn(
        'rounded-full px-3 py-1 text-xs font-medium transition-colors whitespace-nowrap',
        active
          ? 'bg-foreground text-background'
          : 'border border-border bg-card text-muted-foreground hover:bg-accent hover:text-foreground',
      )}
    >
      {children}
    </button>
  )
}

export default function UnifiedTracker({ sport, api }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [market, setMarket] = useState('all')

  useEffect(() => {
    let alive = true
    setLoading(true)
    setError(null)
    api.get('/' + sport + '/tracker/unified')
      .then(r => { if (alive) { setData(r.data); setLoading(false) } })
      .catch(e => { if (alive) { setError(e?.message || 'Failed to load'); setLoading(false) } })
    return () => { alive = false }
  }, [sport, api])

  const markets = useMemo(() => {
    const keys = Object.keys(data?.by_market || {})
    keys.sort((a, b) => {
      const ia = MARKET_ORDER.indexOf(a), ib = MARKET_ORDER.indexOf(b)
      return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib)
    })
    return keys
  }, [data])

  const rows = useMemo(() => {
    const all = data?.rows || []
    const filtered = market === 'all' ? all : all.filter(r => r.market === market)
    return filtered.slice(0, 150)
  }, [data, market])

  if (loading) {
    return (
      <div className="flex items-center justify-center gap-3 py-16 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        Loading tracker…
      </div>
    )
  }

  if (error) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
        <div className="text-sm font-semibold text-foreground">Couldn&apos;t load the tracker.</div>
        <div className="mt-1 text-xs text-muted-foreground">{error}</div>
      </div>
    )
  }

  const all = data?.summary?.all || {}
  const bm = data?.by_market || {}
  const profitTone = all.profit > 0 ? 'text-positive' : all.profit < 0 ? 'text-negative' : 'text-foreground'

  if (!(all.n > 0)) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
        <div className="text-sm font-semibold text-foreground">No picks recorded yet.</div>
      </div>
    )
  }

  return (
    <div className="space-y-5 py-4">
      {/* Combined hero + per-market tiles */}
      <section className="rounded-lg border border-border bg-card p-5">
        <div className="flex flex-col gap-1">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
            All Markets · Combined P/L
          </div>
          <div className={cn('text-3xl font-bold tabular-nums', profitTone)}>
            {fmtMoney(all.profit)}
          </div>
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <span className="tabular-nums">{all.wins}-{all.losses}</span>
            <span className="text-border">·</span>
            <span className="tabular-nums">{all.win_pct}% WR</span>
            <span className="text-border">·</span>
            <span className="tabular-nums">{all.n} picks</span>
            {all.pending > 0 && (
              <>
                <span className="text-border">·</span>
                <span className="rounded-full bg-warning/15 px-2 py-0.5 text-warning font-semibold tabular-nums">
                  {all.pending} pending
                </span>
              </>
            )}
          </div>
        </div>

        {markets.length > 0 && (
          <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4">
            {markets.map(m => {
              const s = bm[m] || {}
              const tone = s.profit > 0 ? 'text-positive' : s.profit < 0 ? 'text-negative' : 'text-foreground'
              return (
                <div key={m} className="rounded-md border border-border bg-background/50 px-3 py-2.5">
                  <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground truncate">{m}</div>
                  <div className={cn('mt-0.5 text-base font-semibold tabular-nums', tone)}>{fmtMoney(s.profit)}</div>
                  <div className="text-[10px] text-muted-foreground tabular-nums">
                    {s.wins}-{s.losses} ({s.win_pct}%)
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </section>

      {/* Market filter */}
      <div className="flex flex-wrap items-center gap-2">
        <FilterPill active={market === 'all'} onClick={() => setMarket('all')}>All</FilterPill>
        {markets.map(m => (
          <FilterPill key={m} active={market === m} onClick={() => setMarket(m)}>{m}</FilterPill>
        ))}
      </div>

      {/* Single picks list */}
      <section className="rounded-lg border border-border bg-card overflow-hidden">
        <div className="flex items-center justify-between px-5 py-3 border-b border-border">
          <h3 className="text-sm font-semibold text-foreground">
            {market === 'all' ? 'All Picks' : market}
          </h3>
          <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            {rows.length} shown
          </span>
        </div>
        <PicksTable picks={rows} sport={sport.includes('/') ? sport.split('/').pop() : sport} />
      </section>
    </div>
  )
}
