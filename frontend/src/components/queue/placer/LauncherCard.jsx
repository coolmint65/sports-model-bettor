/**
 * LauncherCard — the human-facing "Auto-betting" state + toggle.
 *
 * Plain-English replacement for what used to be a config-file audit
 * page. Shows one big status line ("Auto-betting is off" / "Ready to
 * turn on" / "Auto-betting is on"), a single primary action button,
 * and a compact setup checklist that's only visible when something's
 * incomplete.
 */
import { useCallback, useEffect, useState } from 'react'
import { cn } from '../../../lib/utils'


export default function LauncherCard({ api }) {
  const [status, setStatus] = useState(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState(null)
  const [showHow, setShowHow] = useState(false)

  const refresh = useCallback(() => {
    api.get('/bet-queue/live-fire/status')
      .then(r => setStatus(r.data))
      .catch(e => setErr(e?.message || 'load failed'))
  }, [api])

  useEffect(() => { refresh() }, [refresh])

  const toggle = () => {
    if (!status) return
    setBusy(true); setErr(null)
    const path = status.on_disk_flag
      ? '/bet-queue/live-fire/off'
      : '/bet-queue/live-fire/on'
    api.post(path)
      .then(() => refresh())
      .catch(e => setErr(e?.message || 'toggle failed'))
      .finally(() => setBusy(false))
  }

  if (!status) {
    return (
      <div className="rounded-2xl border border-border bg-card p-6 text-sm text-muted-foreground">
        Loading…
      </div>
    )
  }

  // Three states: on / ready-but-off / needs-setup.
  const setupReady = !!status.env_flag && !!status.relay_url
  const isOn = status.live_fire_enabled
  const state = isOn ? 'on'
    : status.on_disk_flag ? 'flag_only'
    : setupReady ? 'ready'
    : 'setup'

  const meta = {
    on: {
      title: 'Auto-betting is on',
      sub: 'The queue is placing real bets on your Hard Rock account.',
      accent: 'from-positive/[0.06] border-positive/40',
      glow: 'bg-positive/10',
      titleTone: 'text-positive',
      btn: { label: 'Turn Off', tone: 'destructive' },
      btnHint: 'Halts placement instantly — any bet in flight is untouched.',
    },
    flag_only: {
      title: 'Almost on — one step missing',
      sub: 'You\'ve armed the button, but the backend isn\'t running in live mode. Restart the backend with AUTO_BET_LIVE=1 in the shell.',
      accent: 'from-warning/[0.06] border-warning/40',
      glow: 'bg-warning/10',
      titleTone: 'text-warning',
      btn: { label: 'Un-arm', tone: 'neutral' },
      btnHint: 'Clears the arm flag if you want to hold off until later.',
    },
    ready: {
      title: 'Ready to turn on',
      sub: 'Setup is complete. Click the button to start placing real bets from the queue.',
      accent: 'from-primary/[0.05] border-primary/40',
      glow: 'bg-primary/10',
      titleTone: 'text-primary',
      btn: { label: 'Turn On', tone: 'positive' },
      btnHint: 'Fires bets from the queue on the next 60-second sweep.',
    },
    setup: {
      title: 'Auto-betting is off',
      sub: 'Nothing will be placed. Finish the setup steps below to get ready, then flip the switch.',
      accent: 'from-muted/10 border-border',
      glow: 'bg-muted/10',
      titleTone: 'text-foreground',
      btn: { label: 'Turn On', tone: 'positive', disabled: true },
      btnHint: 'Finish setup before you can turn it on.',
    },
  }[state]

  const btnClasses = meta.btn.tone === 'destructive'
    ? 'border-negative text-negative bg-negative/10 hover:bg-negative/20'
    : meta.btn.tone === 'positive'
      ? 'border-positive text-positive bg-positive/10 hover:bg-positive/20'
      : 'border-border text-foreground bg-card hover:bg-accent/40'

  return (
    <section className={cn(
      'relative overflow-hidden rounded-2xl border p-6 bg-gradient-to-br from-card via-card',
      meta.accent,
    )}>
      <div
        aria-hidden="true"
        className={cn('absolute -right-24 -top-24 h-64 w-64 rounded-full blur-3xl',
                       meta.glow)}
      />

      <div className="relative flex flex-col lg:flex-row gap-6 items-start">
        <div className="flex-1 min-w-0">
          <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
            Auto-betting
          </div>
          <h2 className={cn('mt-1 text-3xl font-bold', meta.titleTone)}>
            {isOn && <span className="mr-2">●</span>}
            {meta.title}
          </h2>
          <p className="mt-2 text-sm text-muted-foreground max-w-xl leading-relaxed">
            {meta.sub}
          </p>

          {(state === 'setup' || state === 'flag_only') && (
            <div className="mt-4 space-y-2">
              <SetupStep
                done={status.env_flag}
                title="1. Start the backend with live mode enabled"
                hint={status.env_flag
                  ? 'Detected — backend is running in live mode.'
                  : 'Set AUTO_BET_LIVE=1 in the shell before starting the backend. This is a one-time terminal setup.'}
              />
              <SetupStep
                done={!!status.relay_url}
                title="2. Point at your Hard Rock relay"
                hint={status.relay_url
                  ? `Connected — ${status.relay_url}`
                  : 'Set HR_RELAY_URL in the shell. The Table Tennis project\'s Beelink relay is at http://100.80.245.68:7478.'}
              />
              <SetupStep
                done={status.on_disk_flag}
                title="3. Turn on auto-betting from this page"
                hint={status.on_disk_flag
                  ? 'Armed.'
                  : 'Once the two steps above are done, click the button and you\'re live.'}
              />
            </div>
          )}
        </div>

        <div className="flex flex-col items-center gap-2 lg:w-[220px] flex-shrink-0">
          <button
            onClick={toggle}
            disabled={busy || meta.btn.disabled}
            className={cn(
              'w-full rounded-lg border-2 px-6 py-3 text-sm font-bold uppercase tracking-wider',
              'transition-all hover:opacity-90 disabled:opacity-40 disabled:cursor-not-allowed',
              btnClasses,
            )}
          >
            {busy ? 'Working…' : meta.btn.label}
          </button>
          <div className="text-[10px] text-muted-foreground text-center leading-relaxed px-2">
            {meta.btnHint}
          </div>
          <button
            type="button"
            onClick={() => setShowHow(v => !v)}
            className="mt-2 text-[10px] uppercase tracking-wider text-muted-foreground/70 hover:text-foreground"
          >
            {showHow ? 'Hide details' : 'How this works'}
          </button>
        </div>
      </div>

      {showHow && (
        <div className="relative mt-5 pt-5 border-t border-border/60 text-xs text-muted-foreground leading-relaxed space-y-2">
          <p>
            <strong className="text-foreground">Two safety keys.</strong>{' '}
            Auto-betting needs both an on-disk flag (this page) and a
            shell env var (<code className="text-foreground/85">AUTO_BET_LIVE=1</code>)
            to fire real money. That way a stray click can't turn it on
            by itself, and a compromised web endpoint can't unlock the
            env-var side.
          </p>
          <p>
            <strong className="text-foreground">Shared connection.</strong>{' '}
            Bets place through a small helper program running on your
            Beelink (the same one the Table Tennis project uses). It
            owns the Hard Rock login and session — this app just tells
            it what to bet.
          </p>
          <p>
            <strong className="text-foreground">On-disk flag path:</strong>{' '}
            <code className="text-foreground/85">{status.flag_path}</code>
          </p>
        </div>
      )}

      {err && (
        <div className="relative mt-3 text-xs text-destructive">{err}</div>
      )}
    </section>
  )
}


function SetupStep({ done, title, hint }) {
  return (
    <div className="flex items-start gap-2.5">
      <span
        className={cn(
          'inline-flex items-center justify-center w-5 h-5 rounded-full flex-shrink-0 mt-0.5 text-[11px] font-bold',
          done
            ? 'bg-positive/25 text-positive'
            : 'bg-muted text-muted-foreground border border-border',
        )}
        aria-hidden="true"
      >
        {done ? '✓' : ''}
      </span>
      <div className="min-w-0 flex-1">
        <div className={cn(
          'text-sm font-semibold',
          done ? 'text-foreground/85' : 'text-foreground',
        )}>
          {title}
        </div>
        <div className="text-[11px] text-muted-foreground mt-0.5 leading-relaxed">
          {hint}
        </div>
      </div>
    </div>
  )
}
