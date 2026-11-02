/**
 * KalshiBalanceCard - live Kalshi exchange balance (direct signed API, no
 * relay). Also shows the current unit-ladder tier for the book (kalshi's
 * unit scales with its own balance - see engine/queue_placer/_sizing.py).
 */
import { useEffect, useState } from 'react'

export default function KalshiBalanceCard({ api }) {
  const [bal, setBal] = useState(null)

  useEffect(() => {
    let cancelled = false
    const load = () => api.get('/bet-queue/kalshi-balance')
      .then(r => { if (!cancelled) setBal(r.data) })
      .catch(() => {})
    load()
    const id = setInterval(load, 60 * 1000)
    return () => { cancelled = true; clearInterval(id) }
  }, [api])

  const amount = bal?.balance
  const unit = bal?.unit

  return (
    <section className="rounded-xl border border-border bg-card p-5">
      <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">
        Kalshi Balance
      </div>
      <div className="flex items-baseline gap-2">
        <span className="text-2xl font-semibold tabular-nums text-foreground">
          {amount == null ? '--' : `$${Number(amount).toFixed(2)}`}
        </span>
        {unit != null && (
          <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            ${Number(unit).toFixed(0)} unit
          </span>
        )}
      </div>
      <p className="mt-3 pt-3 border-t border-border/60 text-[11px] text-muted-foreground leading-relaxed">
        {amount == null
          ? 'Waiting for the Kalshi balance (direct API)...'
          : 'Live Kalshi exchange balance. Unit steps with balance: $5 < $100, $8 >= $100, $10 >= $250.'}
      </p>
    </section>
  )
}
