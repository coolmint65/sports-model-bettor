/**
 * BookEnableToggle — per-book arm/disarm switch.
 *
 * Mirrors LiveFireToggle but scoped to a single book's on-disk enable
 * flag (book = 'dk' | 'bc'). Independent of the master live-fire
 * switch: a book only places when live-fire is ON *and* its own flag
 * is set. This pill just arms/disarms the book. The 'bc' switch gates
 * Zotabet + Betandplay together.
 */
import { useCallback, useEffect, useState } from 'react'
import { cn } from '../../../lib/utils'


export default function BookEnableToggle({ api, book, label }) {
  const [status, setStatus] = useState(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState(null)

  const refresh = useCallback(() => {
    api.get('/bet-queue/book-enable/status')
      .then(r => setStatus(r.data))
      .catch(e => setErr(e?.message || 'load failed'))
  }, [api])

  useEffect(() => { refresh() }, [refresh])

  const toggle = () => {
    setBusy(true); setErr(null)
    const on = !status?.[book]           // desired new state
    api.post(`/bet-queue/book-enable/${book}/${on ? 'on' : 'off'}`)
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

  const enabled = !!status[book]
  const pillTone = enabled
    ? 'bg-positive/15 text-positive border-positive/40'
    : 'bg-muted text-muted-foreground border-border'

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
        title={enabled
          ? `${label} is armed — will place when auto-betting is on. Click to disarm.`
          : `${label} is disarmed — no bets. Click to arm (auto-betting must also be on to place).`}
      >
        <span className="mr-1">{enabled ? '●' : '○'}</span>
        {label}
      </button>
      {err && (
        <span className="text-[10px] text-destructive">{err}</span>
      )}
    </div>
  )
}
