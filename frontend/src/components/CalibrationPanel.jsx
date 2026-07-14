import { useEffect, useState, useMemo } from 'react'
import { cn } from '../lib/utils'

/**
 * CalibrationPanel — operational-dashboard view of model calibration.
 *
 * Renders reliability diagrams (predicted prob vs realized win rate)
 * per sport, per market. Headline ECE/Brier shown at top so drift
 * surfaces at a glance. Data comes from
 * `/api/calibration/reliability/{sport}` (see
 * engine/calibration_diagnostics.py).
 *
 * Why this exists: under "be right, not edge-first" the most important
 * question is "when the model says 60%, are 60% of those bets winning?"
 * Surfacing this in the UI makes calibration drift obvious instead of
 * hiding in CSV reports run on demand.
 */
export default function CalibrationPanel({ sport, api }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState(null)
  const [days, setDays] = useState(0)         // 0 = full history
  const [selectedBetType, setSelectedBetType] = useState('overall')

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setErr(null)
    // ``api`` is an axios instance with baseURL='/api' (see App.jsx).
    // Use it directly instead of fetch() — earlier version interpolated
    // the axios object into a fetch URL string and got the SPA's HTML
    // back ("JSON.parse: unexpected character" was that HTML).
    api.get(`/calibration/reliability/${sport}?days=${days}`)
      .then(r => {
        if (cancelled) return
        setData(r.data)
        setSelectedBetType('overall')
      })
      .catch(e => !cancelled && setErr(e.response?.statusText || e.message))
      .finally(() => !cancelled && setLoading(false))
    return () => { cancelled = true }
  }, [api, sport, days])

  const buckets = useMemo(() => {
    if (!data) return []
    if (selectedBetType === 'overall') return data.overall || []
    return (data.by_bet_type || {})[selectedBetType] || []
  }, [data, selectedBetType])

  const betTypes = useMemo(() => {
    if (!data?.by_bet_type) return []
    // Sort by sample size (largest first)
    return Object.entries(data.by_bet_type)
      .map(([bt, b]) => ({ bt, n: b.reduce((s, x) => s + (x.n || 0), 0) }))
      .sort((a, b) => b.n - a.n)
      .map(x => x.bt)
  }, [data])

  if (loading && !data) {
    return <div className="p-6 text-sm text-muted-foreground">Loading calibration data…</div>
  }
  if (err) {
    return <div className="p-6 text-sm text-destructive">Calibration fetch failed: {err}</div>
  }
  if (!data || data.n_total === 0) {
    return (
      <div className="p-6 text-sm text-muted-foreground">
        No settled picks for {sport.toUpperCase()} yet — calibration data appears once picks settle.
      </div>
    )
  }

  return (
    <div className="space-y-5 p-1">
      <Header
        sport={sport}
        nTotal={data.n_total}
        headline={data.headline}
        days={days}
        onDaysChange={setDays}
      />

      {/* Bet-type selector */}
      <div className="flex flex-wrap gap-2 text-sm">
        <BtChip
          label="Overall"
          n={data.n_total}
          active={selectedBetType === 'overall'}
          onClick={() => setSelectedBetType('overall')}
        />
        {betTypes.map(bt => (
          <BtChip
            key={bt}
            label={bt}
            n={data.by_bet_type[bt].reduce((s, x) => s + (x.n || 0), 0)}
            active={selectedBetType === bt}
            onClick={() => setSelectedBetType(bt)}
          />
        ))}
      </div>

      <ReliabilityChart buckets={buckets} />
      <BucketTable buckets={buckets} />
    </div>
  )
}

function Header({ sport, nTotal, headline, days, onDaysChange }) {
  const ecePct = headline?.ece != null ? (headline.ece * 100).toFixed(1) : '—'
  const eceColor = (() => {
    if (headline?.ece == null) return 'text-muted-foreground'
    if (headline.ece < 0.05) return 'text-emerald-500'
    if (headline.ece < 0.10) return 'text-yellow-500'
    return 'text-red-500'
  })()
  return (
    <div className="flex flex-wrap items-end justify-between gap-3 border-b border-border pb-3">
      <div>
        <div className="text-xs uppercase tracking-wider text-muted-foreground">
          {sport.toUpperCase()} Calibration
        </div>
        <div className="mt-0.5 text-sm text-muted-foreground">
          Settled picks: <span className="text-foreground font-mono tabular-nums">{nTotal}</span>
        </div>
      </div>
      <div className="flex items-center gap-4 text-sm">
        <div>
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground">Brier</div>
          <div className="font-mono tabular-nums text-foreground">
            {headline?.brier != null ? headline.brier.toFixed(4) : '—'}
          </div>
        </div>
        <div>
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground">ECE</div>
          <div className={cn('font-mono tabular-nums', eceColor)}>
            {ecePct}%
          </div>
        </div>
        <div className="ml-3 flex items-center gap-1.5">
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Window</span>
          <select
            value={days}
            onChange={e => onDaysChange(Number(e.target.value))}
            className="rounded-md border border-border bg-background px-2 py-1 text-xs text-foreground focus:outline-none focus:ring-1 focus:ring-ring"
          >
            <option value={0}>All time</option>
            <option value={7}>Last 7d</option>
            <option value={30}>Last 30d</option>
            <option value={90}>Last 90d</option>
          </select>
        </div>
      </div>
    </div>
  )
}

function BtChip({ label, n, active, onClick }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        'flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium transition-colors',
        active
          ? 'bg-primary text-primary-foreground'
          : 'bg-muted text-muted-foreground hover:bg-muted/70',
      )}
    >
      <span>{label}</span>
      <span className="font-mono tabular-nums opacity-70">{n}</span>
    </button>
  )
}

/**
 * ReliabilityChart — SVG line chart with the perfect-calibration
 * diagonal as reference and the actual mean_pred → mean_actual line
 * laid over. Buckets with n=0 are skipped from the plot.
 */
function ReliabilityChart({ buckets }) {
  const W = 480, H = 320
  const padL = 44, padR = 14, padT = 14, padB = 36

  const points = buckets
    .filter(b => b.n > 0 && b.mean_pred != null && b.mean_actual != null)
    .map(b => ({
      x: b.mean_pred,
      y: b.mean_actual,
      n: b.n,
      delta: b.delta,
    }))

  if (!points.length) {
    return (
      <div className="rounded-lg border border-border bg-muted/30 p-6 text-sm text-muted-foreground">
        No buckets with samples — try a wider window.
      </div>
    )
  }

  const xToPx = x => padL + x * (W - padL - padR)
  const yToPx = y => H - padB - y * (H - padT - padB)
  const r = n => Math.max(3, Math.min(14, Math.sqrt(n) * 1.2))

  // Path string for the actual curve
  const pathD = points
    .sort((a, b) => a.x - b.x)
    .map((p, i) => `${i === 0 ? 'M' : 'L'} ${xToPx(p.x).toFixed(1)} ${yToPx(p.y).toFixed(1)}`)
    .join(' ')

  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <div className="mb-2 flex items-center justify-between text-xs text-muted-foreground">
        <span>Reliability diagram</span>
        <span className="space-x-3">
          <Legend dot="bg-muted-foreground/60" label="perfect calibration" />
          <Legend dot="bg-primary" label="actual" />
        </span>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-auto">
        {/* Axes */}
        <line x1={padL} y1={H - padB} x2={W - padR} y2={H - padB} className="stroke-border" />
        <line x1={padL} y1={padT} x2={padL} y2={H - padB} className="stroke-border" />
        {/* Y-axis ticks */}
        {[0, 0.25, 0.5, 0.75, 1].map(v => (
          <g key={`y${v}`}>
            <line
              x1={padL - 4} y1={yToPx(v)} x2={padL} y2={yToPx(v)}
              className="stroke-border"
            />
            <text
              x={padL - 8} y={yToPx(v) + 3}
              textAnchor="end"
              className="fill-muted-foreground"
              style={{ fontSize: '10px' }}
            >
              {v.toFixed(2)}
            </text>
          </g>
        ))}
        {/* X-axis ticks */}
        {[0, 0.25, 0.5, 0.75, 1].map(v => (
          <g key={`x${v}`}>
            <line
              x1={xToPx(v)} y1={H - padB} x2={xToPx(v)} y2={H - padB + 4}
              className="stroke-border"
            />
            <text
              x={xToPx(v)} y={H - padB + 16}
              textAnchor="middle"
              className="fill-muted-foreground"
              style={{ fontSize: '10px' }}
            >
              {v.toFixed(2)}
            </text>
          </g>
        ))}
        <text x={W / 2} y={H - 6} textAnchor="middle" className="fill-muted-foreground" style={{ fontSize: '11px' }}>
          predicted probability
        </text>
        <text
          x={-(H / 2)} y={14} transform="rotate(-90)" textAnchor="middle"
          className="fill-muted-foreground" style={{ fontSize: '11px' }}
        >
          realized rate
        </text>

        {/* Diagonal (perfect calibration) */}
        <line
          x1={xToPx(0)} y1={yToPx(0)}
          x2={xToPx(1)} y2={yToPx(1)}
          className="stroke-muted-foreground/60"
          strokeDasharray="4 4"
          strokeWidth={1}
        />

        {/* Actual curve */}
        <path d={pathD} className="stroke-primary fill-none" strokeWidth={2} />

        {/* Bucket dots, sized by n */}
        {points.map((p, i) => (
          <g key={i}>
            <circle
              cx={xToPx(p.x)} cy={yToPx(p.y)} r={r(p.n)}
              className="fill-primary opacity-80"
            />
            <title>
              n={p.n} pred={p.x.toFixed(3)} actual={p.y.toFixed(3)} delta={p.delta?.toFixed(3) ?? '—'}
            </title>
          </g>
        ))}
      </svg>
      <p className="mt-2 text-[11px] text-muted-foreground">
        Dots above the diagonal: model is <em>under</em>confident. Below: <em>over</em>confident.
        Dot size = sample count.
      </p>
    </div>
  )
}

function Legend({ dot, label }) {
  return (
    <span className="inline-flex items-center gap-1">
      <span className={cn('inline-block h-2 w-2 rounded-full', dot)} />
      <span>{label}</span>
    </span>
  )
}

function BucketTable({ buckets }) {
  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <div className="mb-2 text-xs uppercase tracking-wider text-muted-foreground">Buckets</div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs tabular-nums">
          <thead>
            <tr className="text-left text-muted-foreground">
              <th className="px-2 py-1.5">range</th>
              <th className="px-2 py-1.5 text-right">n</th>
              <th className="px-2 py-1.5 text-right">predicted</th>
              <th className="px-2 py-1.5 text-right">actual</th>
              <th className="px-2 py-1.5 text-right">delta</th>
              <th className="px-2 py-1.5 text-right">brier</th>
            </tr>
          </thead>
          <tbody>
            {buckets.map(b => {
              const empty = !b.n
              const deltaPct = b.delta != null ? (b.delta * 100).toFixed(1) : '—'
              const deltaCls = (() => {
                if (b.delta == null) return 'text-muted-foreground'
                if (Math.abs(b.delta) < 0.03) return 'text-emerald-500'
                if (Math.abs(b.delta) < 0.10) return 'text-yellow-500'
                return 'text-red-500'
              })()
              return (
                <tr
                  key={b.bucket}
                  className={cn(
                    'border-t border-border',
                    empty && 'text-muted-foreground/50',
                  )}
                >
                  <td className="px-2 py-1.5 font-mono">
                    {b.lo.toFixed(2)}–{b.hi.toFixed(2)}
                  </td>
                  <td className="px-2 py-1.5 text-right">{b.n}</td>
                  <td className="px-2 py-1.5 text-right">{b.mean_pred?.toFixed(4) ?? '—'}</td>
                  <td className="px-2 py-1.5 text-right">{b.mean_actual?.toFixed(4) ?? '—'}</td>
                  <td className={cn('px-2 py-1.5 text-right font-medium', deltaCls)}>
                    {b.delta != null ? `${b.delta > 0 ? '+' : ''}${deltaPct}pp` : '—'}
                  </td>
                  <td className="px-2 py-1.5 text-right">{b.brier?.toFixed(4) ?? '—'}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
