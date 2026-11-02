/**
 * BalanceCard - live Hard Rock account balance. getBalance CF-blocks direct
 * from the Beelink, so the Pi fetches it and pushes it to the relay; this reads
 * the relay's cached value via /api/bet-queue/hr-balance (refreshed ~5 min).
 */
import { useEffect, useState } from 'react'

export default function BalanceCard({ api }) {
  const [bal, setBal] = useState(null)

  useEffect(() => {
    let cancelled = false
    const fetch = () => api.get('/bet-queue/hr-balance')
      .then(r => { if (!cancelled) setBal(r.data) })
      .catch(() => {})
    fetch()
    const id = setInterval(fetch, 60 * 1000)
    return () => { cancelled = true; clearInterval(id) }
  }, [api])

  const amount = bal?.balance
  const ageSec = bal?.ageSec
  const stale = ageSec != null && ageSec > 900

  return (
    <section className="rounded-xl border border-border bg-card p-5">
      <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">
        HR Balance
      </div>
      <div className="flex items-baseline gap-2">
        <span className="text-2xl font-semibold tabular-nums text-foreground">
          {amount == null ? '--' : `$${Number(amount).toFixed(2)}`}
        </span>
        {stale && (
          <span className="text-[10px] font-semibold uppercase tracking-wider text-warning">
            stale
          </span>
        )}
      </div>
      <p className="mt-3 pt-3 border-t border-border/60 text-[11px] text-muted-foreground leading-relaxed">
        {amount == null
          ? 'Waiting for the first balance push from the Pi...'
          : `Live Hard Rock account balance${ageSec != null ? ` (updated ${ageSec < 90 ? 'just now' : Math.round(ageSec / 60) + 'm ago'})` : ''}.`}
      </p>
    </section>
  )
}
