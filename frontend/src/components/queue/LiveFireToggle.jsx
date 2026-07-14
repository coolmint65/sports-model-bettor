/**
 * LiveFireToggle — the panel-header kill switch UI.
 *
 * Renders a status pill (Live-fire ON / Dry-run) with a one-click
 * toggle. Only flips the on-disk flag — the AUTO_BET_LIVE env var is
 * a separate key that the operator must set from a shell. The
 * response payload spells out which key is missing so the user knows
 * exactly what to do next.
 */
import { useCallback, useEffect, useState } from 'react'
import { cn } from '../../lib/utils'


export default function LiveFireToggle({ api }) {
  const [status, setStatus] = useState(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState(null)

  const refresh = useCallback(() => {
    api.get('/bet-queue/live-fire/status')
      .then(r => setStatus(r.data))
      .catch(e => setErr(e?.message || 'load failed'))
  }, [api])

  useEffect(() => { refresh() }, [refresh])

  const toggle = () => {
    setBusy(true); setErr(null)
    const path = status?.on_disk_flag
      ? '/bet-queue/live-fire/off'
      : '/bet-queue/live-fire/on'
    api.post(path)
      .then(() => refresh())
      .catch(e => setErr(e?.message || 'toggle failed'))
      .finally(() => setBusy(false))
  }

  if (!status) {
    return (
      <span className="text-[11px] text-muted-foreground">
        loading…
      </span>
    )
  }

  const live = status.live_fire_enabled
  const pillTone = live
    ? 'bg-negative/15 text-negative border-negative/40'
    : status.on_disk_flag
      ? 'bg-warning/15 text-warning border-warning/40'
      : 'bg-muted text-muted-foreground border-border'
  const label = live
    ? '● Live-fire'
    : status.on_disk_flag
      ? 'Flag set · env off'
      : 'Dry-run'

  return (
    <div className="flex items-center gap-2">
      <button
        onClick={toggle}
        disabled={busy}
        className={cn(
          'inline-flex items-center rounded-full border px-2.5 py-0.5',
          'text-[10px] font-bold uppercase tracking-wider',
          'transition-colors hover:opacity-80 disabled:opacity-50',
          pillTone,
        )}
        title={live
          ? 'Real money is firing. Click to halt.'
          : status.on_disk_flag
            ? 'Flag is set but AUTO_BET_LIVE env is off. Click to unset the flag.'
            : 'Dry-run only. Click to set the on-disk flag (env AUTO_BET_LIVE=1 also required).'}
      >
        {label}
      </button>
      {err && (
        <span className="text-[10px] text-destructive">{err}</span>
      )}
    </div>
  )
}
