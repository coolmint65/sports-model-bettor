/**
 * BreakersCard — safety cutoffs the placer respects on every attempt.
 *
 * Only surfaces the auto-halt rules that are UNIQUE to this card
 * (drawdown / losing-streak / shared-account ceiling). The "live-fire
 * enabled" and "relay configured" flags are already covered by the
 * launcher + relay cards, so we don't duplicate them here.
 */
import { useEffect, useState } from 'react'
import { cn } from '../../../lib/utils'


export default function BreakersCard({ api }) {
  const [breakers, setBreakers] = useState(null)

  useEffect(() => {
    let cancelled = false
    const fetch = () => api.get('/bet-queue/place/breakers')
      .then(r => { if (!cancelled) setBreakers(r.data) })
      .catch(() => {})
    fetch()
    const id = setInterval(fetch, 30 * 1000)
    return () => { cancelled = true; clearInterval(id) }
  }, [api])

  if (!breakers) {
    return (
      <div className="rounded-xl border border-border bg-card p-4 text-sm text-muted-foreground">
        Loading safety checks…
      </div>
    )
  }

  const items = [
    { key: 'daily_drawdown',    label: 'Daily loss limit' },
    { key: 'consecutive_loss',  label: 'Losing streak' },
    { key: 'shared_account_ceiling', label: 'Shared account ceiling' },
  ]

  return (
    <section className="rounded-xl border border-border bg-card p-5">
      <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-3">
        Safety Cutoffs
      </div>
      <ul className="space-y-3">
        {items.map(({ key, label }) => {
          const b = breakers[key]
          if (!b) return null
          return <BreakerRow key={key} label={label} state={b} />
        })}
      </ul>
      {breakers.tt_today_staked > 0 && (
        <div className="mt-4 pt-3 border-t border-border/60 text-[11px] text-muted-foreground">
          Table Tennis project has staked
          <span className="mx-1 font-semibold text-foreground/85 tabular-nums">
            ${Number(breakers.tt_today_staked).toFixed(2)}
          </span>
          today on the same Hard Rock account.
        </div>
      )}
    </section>
  )
}


function BreakerRow({ label, state }) {
  const ok = !!state.ok
  return (
    <li className="flex items-start gap-2.5">
      <span
        className={cn(
          'inline-flex items-center justify-center w-5 h-5 rounded-full flex-shrink-0 mt-0.5 text-[11px] font-bold',
          ok ? 'bg-positive/25 text-positive'
             : 'bg-warning/25 text-warning',
        )}
        aria-hidden="true"
      >
        {ok ? '✓' : '!'}
      </span>
      <div className="min-w-0 flex-1">
        <div className="text-sm font-semibold text-foreground">
          {label}
        </div>
        <div className="text-[11px] text-muted-foreground mt-0.5 leading-relaxed">
          {state.hint}
        </div>
      </div>
    </li>
  )
}
