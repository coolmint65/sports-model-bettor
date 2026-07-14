import { useEffect, useMemo, useState } from 'react'
import axios from 'axios'
import SectionCard from './primitives/SectionCard'
import { cn } from '../lib/utils'

/**
 * PortfolioTracker — unified cross-sport tracker that consumes the new
 * /api/picks family (engine.picks_unified). Replaces the 13+ per-sport
 * tracker tabs with a single surface: overall ROI, by-sport tiles,
 * by-bet-type tiles, and a filtered picks list.
 *
 * Filters: sport · result · date range. Each filter narrows BOTH the
 * summary tiles AND the picks list so the user can drill into a sport
 * and see only that slice.
 *
 * The legacy per-sport TrackerView panels remain — this is the first
 * unified surface, not a replacement. Once parity is validated, the
 * per-sport tabs route through here.
 */

const api = axios.create({ baseURL: '/api' })

const SPORT_LABEL = {
  mlb: 'MLB', nhl: 'NHL', nba: 'NBA',
  basketball: 'Basketball', hockey: 'Hockey',
  soccer: 'Soccer', football: 'Football',
  baseball: 'College Baseball',
  tennis: 'Tennis', golf: 'Golf', motorsports: 'Motorsports',
}

const SPORT_ICON = {
  mlb: '⚾', nhl: '🏒', nba: '🏀',
  basketball: '🏀', hockey: '🏒',
  soccer: '⚽', football: '🏈', baseball: '⚾',
  tennis: '🎾', golf: '⛳', motorsports: '🏎',
}

const RESULT_OPTS = [
  { id: 'all',     label: 'All' },
  { id: 'pending', label: 'Pending' },
  { id: 'W',       label: 'Wins' },
  { id: 'L',       label: 'Losses' },
]

const RANGE_OPTS = [
  { id: '7',   label: '7d' },
  { id: '30',  label: '30d' },
  { id: '90',  label: '90d' },
  { id: 'ytd', label: 'YTD' },
  { id: 'all', label: 'All' },
]

function dateFromRange(rangeId) {
  if (rangeId === 'all') return null
  const now = new Date()
  if (rangeId === 'ytd') {
    return `${now.getFullYear()}-01-01`
  }
  const days = parseInt(rangeId, 10)
  const d = new Date(now.getTime() - days * 86400000)
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

export default function PortfolioTracker() {
  const [sportFilter, setSportFilter] = useState(null)
  const [resultFilter, setResultFilter] = useState('all')
  const [range, setRange] = useState('30')
  const [summary, setSummary] = useState(null)
  const [picks, setPicks] = useState([])
  const [loading, setLoading] = useState(true)

  // Load summary + picks whenever filters change.
  useEffect(() => {
    let live = true
    setLoading(true)
    const date_from = dateFromRange(range)
    const sParams = {}
    if (sportFilter) sParams.sport = sportFilter
    if (date_from) sParams.date_from = date_from

    const pParams = { ...sParams, limit: 200 }
    if (resultFilter !== 'all') pParams.result = resultFilter

    Promise.all([
      api.get('/picks/summary', { params: sParams })
         .then(r => r.data).catch(() => null),
      api.get('/picks', { params: pParams })
         .then(r => r.data?.picks || []).catch(() => []),
    ]).then(([s, p]) => {
      if (!live) return
      setSummary(s)
      setPicks(p)
    }).finally(() => { if (live) setLoading(false) })

    return () => { live = false }
  }, [sportFilter, resultFilter, range])

  const overall = summary?.overall || {}
  const bySport = summary?.by_sport || {}
  const byType = summary?.by_type || {}

  const sportTiles = useMemo(() => {
    return Object.entries(bySport)
      .map(([k, v]) => ({ key: k, ...v }))
      .sort((a, b) => (b.profit ?? 0) - (a.profit ?? 0))
  }, [bySport])

  const typeTiles = useMemo(() => {
    return Object.entries(byType)
      .map(([k, v]) => ({ key: k, ...v }))
      .filter(t => t.total >= 5)
      .sort((a, b) => (b.profit ?? 0) - (a.profit ?? 0))
      .slice(0, 12)
  }, [byType])

  return (
    <div className="space-y-6 py-4">
      <header>
        <h1 className="text-2xl font-semibold tracking-tight text-foreground">
          Portfolio
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Stake-weighted profit + ROI across every sport, league, and bet type.
        </p>
      </header>

      {/* Filter row */}
      <div className="flex flex-wrap items-center gap-3">
        <FilterChips
          label="Range"
          options={RANGE_OPTS}
          active={range}
          onChange={setRange}
        />
        <FilterChips
          label="Result"
          options={RESULT_OPTS}
          active={resultFilter}
          onChange={setResultFilter}
        />
        {sportFilter && (
          <button
            onClick={() => setSportFilter(null)}
            className="rounded-md border border-border bg-card px-2.5 py-1 text-xs text-muted-foreground hover:text-foreground"
          >
            Sport: <span className="font-medium text-foreground">{SPORT_LABEL[sportFilter] || sportFilter}</span> ✕
          </button>
        )}
      </div>

      {/* Overall hero */}
      <HeroStats overall={overall} loading={loading} />

      {/* By-sport tiles */}
      <SectionCard
        title="By sport"
        subtitle={sportFilter ? `Filtered to ${SPORT_LABEL[sportFilter] || sportFilter}` : 'Click a sport to drill in'}
      >
        {sportTiles.length === 0 ? (
          <p className="text-sm text-muted-foreground">No picks in this range.</p>
        ) : (
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4">
            {sportTiles.map(t => (
              <SportTile
                key={t.key}
                tile={t}
                active={sportFilter === t.key}
                onClick={() => setSportFilter(sportFilter === t.key ? null : t.key)}
              />
            ))}
          </div>
        )}
      </SectionCard>

      {/* By-bet-type tiles */}
      {typeTiles.length > 0 && (
        <SectionCard title="By bet type" subtitle="Stake-weighted, ≥5 settled picks">
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4">
            {typeTiles.map(t => (
              <TypeTile key={t.key} tile={t} />
            ))}
          </div>
        </SectionCard>
      )}

      {/* Picks list */}
      <SectionCard
        title="Picks"
        subtitle={`${picks.length} row${picks.length === 1 ? '' : 's'}`}
      >
        {loading ? (
          <div className="py-8 text-center text-sm text-muted-foreground">
            Loading…
          </div>
        ) : picks.length === 0 ? (
          <p className="text-sm text-muted-foreground">No picks match these filters.</p>
        ) : (
          <PicksList picks={picks} />
        )}
      </SectionCard>
    </div>
  )
}

function FilterChips({ label, options, active, onChange }) {
  return (
    <div className="flex items-center gap-1.5">
      <span className="text-[11px] uppercase tracking-wider text-muted-foreground">
        {label}
      </span>
      <div className="flex rounded-md border border-border bg-card p-0.5">
        {options.map(o => (
          <button
            key={o.id}
            onClick={() => onChange(o.id)}
            className={cn(
              'rounded px-2 py-1 text-xs transition-colors',
              active === o.id
                ? 'bg-primary text-primary-foreground'
                : 'text-muted-foreground hover:text-foreground',
            )}
          >
            {o.label}
          </button>
        ))}
      </div>
    </div>
  )
}

function HeroStats({ overall, loading }) {
  const profit = overall.profit ?? 0
  const roi = overall.roi ?? 0
  const wp = overall.win_pct ?? 0
  const profitTone =
    profit > 0 ? 'text-emerald-500' :
    profit < 0 ? 'text-rose-500' :
    'text-foreground'
  return (
    <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
      <Stat label="Profit (units)" value={loading ? '—' : profit.toFixed(2)} tone={profitTone} />
      <Stat label="ROI" value={loading ? '—' : `${roi.toFixed(1)}%`} tone={profitTone} />
      <Stat label="Win rate" value={loading ? '—' : `${wp.toFixed(1)}%`} />
      <Stat label="W / L / P / V" value={loading ? '—' :
        `${overall.wins ?? 0} / ${overall.losses ?? 0} / ${overall.pushes ?? 0} / ${overall.voids ?? 0}`} />
    </div>
  )
}

function Stat({ label, value, tone }) {
  return (
    <div className="rounded-lg border border-border bg-card px-4 py-3">
      <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      <div className={cn('mt-1 text-xl font-semibold tabular-nums', tone)}>
        {value}
      </div>
    </div>
  )
}

function SportTile({ tile, active, onClick }) {
  const profit = tile.profit ?? 0
  const tone =
    profit > 0 ? 'text-emerald-500' :
    profit < 0 ? 'text-rose-500' :
    'text-muted-foreground'
  return (
    <button
      onClick={onClick}
      className={cn(
        'rounded-lg border bg-card px-3 py-2 text-left transition-colors',
        active
          ? 'border-primary ring-1 ring-primary'
          : 'border-border hover:border-muted-foreground',
      )}
    >
      <div className="flex items-center gap-1.5">
        <span aria-hidden>{SPORT_ICON[tile.key] || '•'}</span>
        <span className="text-sm font-semibold text-foreground">
          {SPORT_LABEL[tile.key] || tile.key}
        </span>
      </div>
      <div className="mt-1 flex items-baseline justify-between gap-2">
        <span className={cn('text-base font-semibold tabular-nums', tone)}>
          {profit >= 0 ? '+' : ''}{profit.toFixed(2)}u
        </span>
        <span className="text-[11px] text-muted-foreground tabular-nums">
          {tile.total} picks · {(tile.roi ?? 0).toFixed(1)}%
        </span>
      </div>
      <div className="mt-0.5 text-[10px] text-muted-foreground tabular-nums">
        {tile.wins ?? 0}-{tile.losses ?? 0}
        {tile.pending ? ` · ${tile.pending} pending` : ''}
      </div>
    </button>
  )
}

function TypeTile({ tile }) {
  const profit = tile.profit ?? 0
  const tone =
    profit > 0 ? 'text-emerald-500' :
    profit < 0 ? 'text-rose-500' :
    'text-muted-foreground'
  return (
    <div className="rounded-lg border border-border bg-card px-3 py-2">
      <div className="text-xs font-semibold text-foreground truncate">{tile.key}</div>
      <div className="mt-1 flex items-baseline justify-between gap-2">
        <span className={cn('text-sm font-semibold tabular-nums', tone)}>
          {profit >= 0 ? '+' : ''}{profit.toFixed(2)}u
        </span>
        <span className="text-[10px] text-muted-foreground tabular-nums">
          {tile.total} · {(tile.roi ?? 0).toFixed(1)}%
        </span>
      </div>
    </div>
  )
}

function PicksList({ picks }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead className="border-b border-border text-muted-foreground">
          <tr>
            <Th>Date</Th>
            <Th>Sport</Th>
            <Th>Matchup</Th>
            <Th>Market</Th>
            <Th>Pick</Th>
            <Th numeric>Odds</Th>
            <Th numeric>Edge</Th>
            <Th numeric>Stake</Th>
            <Th>Result</Th>
            <Th numeric>P/L</Th>
          </tr>
        </thead>
        <tbody>
          {picks.map(p => (
            <PickRow key={p.id} p={p} />
          ))}
        </tbody>
      </table>
    </div>
  )
}

function Th({ children, numeric }) {
  return (
    <th className={cn(
      'px-2 py-1.5 text-[10px] font-medium uppercase tracking-wider',
      numeric ? 'text-right' : 'text-left',
    )}>
      {children}
    </th>
  )
}

function PickRow({ p }) {
  const resultTone =
    p.result === 'W' ? 'bg-emerald-500/5' :
    p.result === 'L' ? 'bg-rose-500/5' : ''
  const profit = p.profit
  const profitTone =
    profit == null ? 'text-muted-foreground' :
    profit > 0 ? 'text-emerald-500' :
    profit < 0 ? 'text-rose-500' :
    'text-foreground'
  const resultLabel = p.result || 'pending'
  const stake = p.stake_units == null ? '—' : `${p.stake_units}u`
  return (
    <tr className={cn('border-b border-border/50', resultTone)}>
      <Td>{p.pick_date || '—'}</Td>
      <Td>
        <span className="inline-flex items-center gap-1">
          <span aria-hidden>{SPORT_ICON[p.sport] || '•'}</span>
          <span className="text-foreground">{SPORT_LABEL[p.sport] || p.sport}</span>
        </span>
      </Td>
      <Td className="max-w-[12rem] truncate">{p.matchup}</Td>
      <Td>{p.bet_type}</Td>
      <Td className="max-w-[10rem] truncate">{p.pick_text}</Td>
      <Td numeric className="tabular-nums">{p.odds > 0 ? `+${p.odds}` : p.odds}</Td>
      <Td numeric className="tabular-nums">{p.edge_pct != null ? `${p.edge_pct.toFixed(1)}%` : '—'}</Td>
      <Td numeric className="tabular-nums">{stake}</Td>
      <Td>
        <span className={cn(
          'rounded px-1.5 py-0.5 text-[10px] font-medium',
          p.result === 'W' && 'bg-emerald-500/15 text-emerald-500',
          p.result === 'L' && 'bg-rose-500/15 text-rose-500',
          p.result === 'P' && 'bg-muted text-muted-foreground',
          p.result === 'V' && 'bg-muted text-muted-foreground',
          !p.result && 'bg-muted/40 text-muted-foreground',
        )}>
          {resultLabel}
        </span>
      </Td>
      <Td numeric className={cn('tabular-nums', profitTone)}>
        {profit == null ? '—' : (profit > 0 ? `+${profit.toFixed(2)}` : profit.toFixed(2))}
      </Td>
    </tr>
  )
}

function Td({ children, numeric, className }) {
  return (
    <td className={cn(
      'px-2 py-1.5',
      numeric ? 'text-right' : 'text-left',
      className,
    )}>
      {children}
    </td>
  )
}
