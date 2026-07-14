/**
 * TrackerHero + market tiles for the motorsports tracker view.
 *
 * Mirrors PickHistory's "Overall P/L" hero + per-bet-type tile grid so
 * the motorsports tracker reads as the same product as every other
 * sport's tracker. Two blocks:
 *   1. TrackerHero  — big profit + W-L + WR + ROI + pending badge
 *   2. MarketTiles  — WINNER + PODIUM per-market tiles with profit tone
 *
 * P/L is displayed in dollars (matches the bet queue convention:
 * 1u = $100). The tracker API returns profit as unit-weighted floats
 * so the multiplier lives here at the render layer.
 */
import { cn } from '../../lib/utils'

const UNIT_DOLLARS = 100

function fmtMoney(units) {
  const dollars = Number(units || 0) * UNIT_DOLLARS
  const sign = dollars > 0 ? '+' : dollars < 0 ? '-' : ''
  return `${sign}$${Math.abs(dollars).toFixed(0)}`
}


export function TrackerHero({ summary }) {
  const profit = Number(summary?.profit ?? 0)
  const wins = summary?.wins ?? 0
  const losses = summary?.losses ?? 0
  const wr = summary?.win_rate
  const roi = summary?.roi
  const pending = summary?.pending ?? 0
  const settled = summary?.settled ?? 0
  const profitTone = profit > 0.001
    ? 'text-positive'
    : profit < -0.001 ? 'text-negative' : 'text-foreground'

  // No picks yet — render a compact zero-state so we don't ship a
  // giant "0-0 · $0" hero.
  const hasAny = (settled > 0) || (pending > 0)
  if (!hasAny) {
    return (
      <section className="rounded-lg border border-dashed border-border bg-card/40 px-6 py-10 text-center">
        <div className="text-sm font-semibold text-foreground">
          No picks yet.
        </div>
        <div className="mt-1 text-xs text-muted-foreground">
          Picks land here after the first race with model output.
        </div>
      </section>
    )
  }

  return (
    <section className="rounded-2xl border border-border bg-card p-5">
      <div className="flex flex-col gap-1">
        <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">
          Overall P/L
        </div>
        <div className={cn('text-3xl font-bold tabular-nums', profitTone)}>
          {fmtMoney(profit)}
        </div>
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
          <span className="tabular-nums">{wins}-{losses}</span>
          {wr != null && (
            <>
              <span className="text-border">·</span>
              <span className="tabular-nums">
                {(wr * 100).toFixed(1)}% WR
              </span>
            </>
          )}
          {roi != null && (
            <>
              <span className="text-border">·</span>
              <span className={cn(
                'tabular-nums font-semibold',
                roi > 0 ? 'text-positive' : roi < 0 ? 'text-negative' : '',
              )}>
                ROI {roi > 0 ? '+' : ''}{(roi * 100).toFixed(1)}%
              </span>
            </>
          )}
          <span className="text-border">·</span>
          <span className="tabular-nums">
            {settled} settled
          </span>
          {pending > 0 && (
            <>
              <span className="text-border">·</span>
              <span className="rounded-full bg-warning/15 px-2 py-0.5 text-warning font-semibold tabular-nums">
                {pending} pending
              </span>
            </>
          )}
        </div>
      </div>
    </section>
  )
}


// Motorsports has two markets — Winner + Podium. Keep the tile grid
// simple and always in market order (not by volume) so the WINNER card
// is always on the left. Add new markets to this list as ingest expands.
const _MARKET_LABELS = {
  WINNER: 'Race Winner',
  PODIUM: 'Podium (Top 3)',
  H2H:    'Head-to-Head',
  FASTEST_LAP: 'Fastest Lap',
}


export function MarketTiles({ byType }) {
  const entries = Object.entries(byType || {})
    .filter(([, v]) => (v?.n ?? 0) > 0)
    .sort(([a], [b]) => {
      const order = ['WINNER', 'PODIUM', 'H2H', 'FASTEST_LAP']
      const ai = order.indexOf(a)
      const bi = order.indexOf(b)
      if (ai !== -1 && bi !== -1) return ai - bi
      if (ai !== -1) return -1
      if (bi !== -1) return 1
      return a.localeCompare(b)
    })
  if (!entries.length) return null
  return (
    <section>
      <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">
        By Market
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {entries.map(([bt, b]) => (
          <MarketCard key={bt} bt={bt} b={b} />
        ))}
      </div>
    </section>
  )
}


function MarketCard({ bt, b }) {
  const label = _MARKET_LABELS[bt] || bt.replace(/_/g, ' ')
  const profit = Number(b.profit || 0)
  const wr = b.win_rate
  const roi = b.roi
  const wins = b.w || 0
  const total = b.n || 0
  const losses = total - wins
  const profitTone = profit > 0.001
    ? 'text-positive'
    : profit < -0.001 ? 'text-negative' : 'text-foreground'
  const accent = profit > 0.001
    ? 'border-l-positive'
    : profit < -0.001 ? 'border-l-negative' : 'border-l-border'
  return (
    <div className={cn(
      'rounded-lg border border-border border-l-4 bg-card/60 px-4 py-3',
      accent,
    )}>
      <div className="flex items-baseline justify-between gap-2">
        <div className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground truncate">
          {label}
        </div>
        <span className={cn(
          'text-[10px] font-semibold tabular-nums',
          profitTone,
        )}>
          {roi != null ? `${roi > 0 ? '+' : ''}${(roi * 100).toFixed(0)}% ROI` : ''}
        </span>
      </div>

      <div className={cn('mt-1 text-2xl font-bold tabular-nums', profitTone)}>
        {fmtMoney(profit)}
      </div>

      <div className="mt-1 flex items-baseline justify-between text-[11px] text-muted-foreground tabular-nums">
        <span>
          <span className="text-foreground font-semibold">{wins}-{losses}</span>
          {wr != null && ` · ${(wr * 100).toFixed(0)}% WR`}
        </span>
        <span>{total} pick{total === 1 ? '' : 's'}</span>
      </div>
    </div>
  )
}
