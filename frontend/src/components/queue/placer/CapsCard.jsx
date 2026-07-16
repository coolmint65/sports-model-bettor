/**
 * CapsCard — visual usage bars for the authorized stake caps.
 *
 * Renders the current day + week staked totals against their caps with
 * horizontal fill bars. Bar turns amber at 80% of cap, negative at
 * 100%. Sourced from /api/bet-queue/place/breakers so the numbers
 * match the placer's own view.
 */
import { useEffect, useState } from 'react'
import { cn } from '../../../lib/utils'


export default function CapsCard({ api, book = 'hr' }) {
  const [breakers, setBreakers] = useState(null)

  useEffect(() => {
    let cancelled = false
    const fetch = () => api.get('/bet-queue/place/breakers', { params: { book } })
      .then(r => { if (!cancelled) setBreakers(r.data) })
      .catch(() => {})
    fetch()
    const id = setInterval(fetch, 30 * 1000)
    return () => { cancelled = true; clearInterval(id) }
  }, [api, book])

  if (!breakers) {
    return (
      <div className="rounded-xl border border-border bg-card p-4 text-sm text-muted-foreground">
        Loading caps…
      </div>
    )
  }

  const dc = breakers.daily_cap || {}
  const wc = breakers.weekly_cap || {}
  const dd = breakers.daily_drawdown || {}

  return (
    <section className="rounded-xl border border-border bg-card p-5">
      <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-3">
        {book.toUpperCase()} Today's Spending
      </div>
      <div className="space-y-4">
        <UsageBar
          label="Placed today"
          current={dc.current || 0}
          threshold={dc.threshold || 1}
          suffix={`$${(dc.current || 0).toFixed(2)} of $${(dc.threshold || 0).toFixed(2)}`}
        />
        <UsageBar
          label="Placed this week"
          current={wc.current || 0}
          threshold={wc.threshold || 1}
          suffix={`$${(wc.current || 0).toFixed(2)} of $${(wc.threshold || 0).toFixed(2)}`}
        />
        <DrawdownBar
          label="Today's profit/loss"
          current={dd.current ?? 0}
          floor={dd.threshold || -3}
        />
      </div>
      <p className="mt-4 pt-3 border-t border-border/60 text-[11px] text-muted-foreground leading-relaxed">
        Auto-betting stops if any bar hits its cap. Bars go amber at 80%
        so you get some warning.
      </p>
    </section>
  )
}


function UsageBar({ label, current, threshold, suffix }) {
  const pct = threshold > 0 ? Math.min(100, (current / threshold) * 100) : 0
  const tone = pct >= 100 ? 'bg-negative'
    : pct >= 80 ? 'bg-warning'
    : 'bg-primary'
  return (
    <div>
      <div className="flex items-baseline justify-between text-xs mb-1">
        <span className="text-muted-foreground uppercase tracking-wider font-semibold text-[10px]">
          {label}
        </span>
        <span className="tabular-nums text-foreground/85">{suffix}</span>
      </div>
      <div className="relative h-2 rounded-full bg-secondary overflow-hidden">
        <div
          className={cn('absolute inset-y-0 left-0 rounded-full transition-all', tone)}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  )
}


function DrawdownBar({ label, current, floor }) {
  // Drawdown floor is negative. Bar fills from LEFT (green, safe) to
  // RIGHT (red, halt) as realized P/L drops.
  const abs = Math.max(0, -current)
  const pct = Math.min(100, (abs / Math.abs(floor)) * 100)
  const tone = pct >= 100 ? 'bg-negative'
    : pct >= 66 ? 'bg-warning'
    : current > 0 ? 'bg-positive'
    : 'bg-primary'
  return (
    <div>
      <div className="flex items-baseline justify-between text-xs mb-1">
        <span className="text-muted-foreground uppercase tracking-wider font-semibold text-[10px]">
          {label}
        </span>
        <span className={cn(
          'tabular-nums font-semibold',
          current > 0 ? 'text-positive'
            : current < 0 ? 'text-negative' : 'text-foreground',
        )}>
          {current >= 0 ? '+' : ''}${current.toFixed(2)}
          <span className="ml-1 text-muted-foreground text-[10px] font-normal">
            (stops at ${floor.toFixed(2)})
          </span>
        </span>
      </div>
      <div className="relative h-2 rounded-full bg-secondary overflow-hidden">
        <div
          className={cn('absolute inset-y-0 left-0 rounded-full transition-all', tone)}
          style={{ width: `${current >= 0 ? 0 : pct}%` }}
        />
      </div>
    </div>
  )
}
