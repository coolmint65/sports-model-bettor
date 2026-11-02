/**
 * RelayCard — Beelink relay health probe.
 *
 * Polls /api/bet-queue/relay/health every 30s. Renders:
 *   - Reachability (green / red)
 *   - Session age (from PiBot's session-push)
 *   - Session validity flag
 * When unreachable, shows the reason so the operator can debug
 * (env var missing, Tailscale down, relay crashed).
 */
import { useCallback, useEffect, useState } from 'react'
import { cn } from '../../../lib/utils'


export default function RelayCard({ api }) {
  const [health, setHealth] = useState(null)
  const [verify, setVerify] = useState(null)
  const [busy, setBusy] = useState(false)
  const [verifying, setVerifying] = useState(false)

  const refresh = useCallback(() => {
    setBusy(true)
    api.get('/bet-queue/relay/health')
      .then(r => setHealth(r.data))
      .catch(e => setHealth({ reachable: false, reason: e?.message }))
      .finally(() => setBusy(false))
  }, [api])

  const runVerify = useCallback(() => {
    setVerifying(true); setVerify(null)
    api.post('/bet-queue/relay/verify')
      .then(r => setVerify(r.data))
      .catch(e => setVerify({ reachable: false, token_ok: false,
                                note: e?.message || 'verify failed' }))
      .finally(() => setVerifying(false))
  }, [api])

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, 30 * 1000)
    return () => clearInterval(id)
  }, [refresh])

  if (!health) {
    return (
      <div className="rounded-xl border border-border bg-card p-4 text-sm text-muted-foreground">
        Probing relay…
      </div>
    )
  }

  const reachable = !!health.reachable
  const sessionAge = health.session_age_s
  const sessionValid = health.session_valid
  const tone = reachable ? 'border-positive/30' : 'border-negative/30'

  return (
    <section className={cn('rounded-xl border bg-card p-5', tone)}>
      <div className="flex items-baseline justify-between mb-3">
        <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
          Beelink Relay
        </div>
        <button
          onClick={refresh}
          disabled={busy}
          className="text-[10px] uppercase tracking-wider text-muted-foreground hover:text-foreground disabled:opacity-50"
        >
          {busy ? 'checking…' : 'refresh'}
        </button>
      </div>

      <div className="flex items-center gap-3">
        <span
          className={cn(
            'inline-flex items-center justify-center w-8 h-8 rounded-full flex-shrink-0',
            reachable ? 'bg-positive/20 text-positive'
                      : 'bg-negative/20 text-negative',
          )}
          aria-hidden="true"
        >
          {reachable ? '●' : '○'}
        </span>
        <div className="min-w-0 flex-1">
          <div className={cn(
            'text-sm font-bold',
            reachable ? 'text-positive' : 'text-negative',
          )}>
            {reachable ? 'Online' : 'Offline'}
          </div>
          <div className="text-[11px] text-muted-foreground truncate">
            {reachable
              ? (sessionValid === false
                  ? 'Relay reachable but HR session invalid — PiBot re-login in progress.'
                  : 'HR session held by PiBot (Table Tennis project).')
              : (health.reason || 'unreachable')}
          </div>
        </div>
      </div>

      {reachable && sessionAge != null && (
        <div className="mt-3 pt-3 border-t border-border/60 flex items-baseline justify-between text-xs">
          <span className="uppercase tracking-wider font-semibold text-[10px] text-muted-foreground">
            Session age
          </span>
          <span className="tabular-nums text-foreground/85">
            {formatAge(sessionAge)}
          </span>
        </div>
      )}

      <div className="mt-3 pt-3 border-t border-border/60 space-y-2">
        <button
          onClick={runVerify}
          disabled={verifying || !reachable}
          className={cn(
            'w-full rounded-md border px-3 py-1.5 text-[11px] font-semibold uppercase tracking-wider',
            'transition-colors hover:bg-accent/40 disabled:opacity-40',
            verify?.token_ok
              ? 'border-positive/40 text-positive bg-positive/5'
              : verify && verify.token_ok === false
                ? 'border-negative/40 text-negative bg-negative/5'
                : 'border-border text-foreground bg-card',
          )}
        >
          {verifying ? 'Verifying…'
            : verify?.token_ok ? '✓ Token verified'
            : verify?.token_ok === false ? '✕ Token rejected'
            : 'Verify Connection'}
        </button>
        {verify && (
          <div className={cn(
            'text-[11px] leading-relaxed',
            verify.token_ok ? 'text-muted-foreground'
              : verify.token_ok === false ? 'text-negative'
              : 'text-warning',
          )}>
            {verify.note}
          </div>
        )}
      </div>

      <div className="mt-3 pt-3 border-t border-border/60 text-[10px] text-muted-foreground leading-relaxed">
        Shared with the Table Tennis project. That project owns the
        Hard Rock login — this page just tells it what to bet.
      </div>
    </section>
  )
}


function formatAge(seconds) {
  if (seconds == null) return '—'
  const s = Math.max(0, Math.floor(seconds))
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  const rs = s % 60
  if (m < 60) return `${m}m ${rs}s`
  const h = Math.floor(m / 60)
  const rm = m % 60
  return `${h}h ${rm}m`
}
