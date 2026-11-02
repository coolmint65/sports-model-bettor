/**
 * DkBalanceCard - live DraftKings (KY box) account balance. dkkbd posts the balance to the
 * Pi's dkserve; the backend reads it over Tailscale via /api/bet-queue/dk-balance.
 */
import { useEffect, useState } from 'react'

export default function DkBalanceCard({ api }) {
  const [bal, setBal] = useState(null)

  useEffect(() => {
    let cancelled = false
    const load = () => api.get('/bet-queue/dk-balance')
      .then(r => { if (!cancelled) setBal(r.data) })
      .catch(() => {})
    load()
    const id = setInterval(load, 60 * 1000)
    return () => { cancelled = true; clearInterval(id) }
  }, [api])

  const amount = bal?.balance
  const ageSec = bal?.ageSec
  const stale = ageSec != null && ageSec > 900

  return (
    <section className="rounded-xl border border-border bg-card p-5">
      <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">
        DK Balance
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
          ? 'Waiting for the DK balance from the KY box...'
          : `Live DraftKings balance, KY box${ageSec != null ? ` (updated ${ageSec < 90 ? 'just now' : Math.round(ageSec / 60) + 'm ago'})` : ''}.`}
      </p>
    </section>
  )
}
