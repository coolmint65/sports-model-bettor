/**
 * CLVBadge — compact per-pick "beat the close" chip.
 * Once the closing line is captured it shows the running/final CLV (green when
 * we beat the close, red when worse, muted on a push). Before the close is
 * snapshotted (upcoming pick) it shows a muted "close pending" chip. CLV = the
 * one reliable edge signal, so it rides alongside the pick's other info.
 */
import { clvPct } from '../lib/clv'
import { cn } from '../lib/utils'

function fmtOdds(o) {
  if (o == null || o === '') return '—'
  const n = Number(o)
  if (!Number.isFinite(n)) return '—'
  return n > 0 ? `+${n}` : `${n}`
}

export default function CLVBadge({ entryOdds, closingOdds, className }) {
  if (closingOdds == null || closingOdds === '') {
    return (
      <span
        title="Closing line not captured yet"
        className={cn(
          'inline-flex items-center rounded px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wider bg-muted text-muted-foreground',
          className,
        )}
      >
        close pending
      </span>
    )
  }
  const clv = clvPct(entryOdds, closingOdds)
  if (clv == null) return null
  const tone = clv > 0
    ? 'bg-positive/15 text-positive'
    : clv < 0
      ? 'bg-negative/15 text-negative'
      : 'bg-muted text-muted-foreground'
  const title = clv === 0
    ? `picked ${fmtOdds(entryOdds)}, closed ${fmtOdds(closingOdds)} → matched the close`
    : `picked ${fmtOdds(entryOdds)}, closed ${fmtOdds(closingOdds)} → ${clv > 0 ? 'beat the close by' : 'worse than close by'} ${clv > 0 ? '+' : ''}${clv.toFixed(1)}%`
  return (
    <span
      title={title}
      className={cn(
        'inline-flex items-center rounded px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider tabular-nums',
        tone,
        className,
      )}
    >
      CLV {clv > 0 ? '+' : ''}{clv.toFixed(1)}%
    </span>
  )
}
