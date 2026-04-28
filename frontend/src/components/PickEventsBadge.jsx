import { memo, useEffect, useRef, useState } from 'react'
import { ScrollText } from 'lucide-react'
import axios from 'axios'
import { cn } from '../lib/utils'
import { humanizeBetType } from '../lib/betType'

/**
 * PickEventsBadge — 📜 popover showing the breadcrumb of model pick
 * transitions for a single game today (appeared / swapped / pulled /
 * line_shift). Backed by /api/{sport}/pick-events?game_id=X.
 *
 * Lazy-fetches on first hover/click so a 30-card slate doesn't fan out
 * 30 requests on mount. Once loaded, the badge stays warm until the
 * card unmounts. The popover dismisses on outside-click and on Esc.
 *
 * Renders nothing visible when the API returns no events (silent
 * "first appearance" cases) so cards stay clean.
 *
 * Props:
 *   sport    - 'mlb' | 'nhl' | 'nba'
 *   gameId   - the bet.game_id for this card
 */
const api = axios.create({ baseURL: '/api' })

const EVENT_LABELS = {
  appeared:   'Picked',
  swapped:    'Swapped',
  pulled:     'Pulled',
  line_shift: 'Line moved',
}

const EVENT_TONES = {
  appeared:   'text-positive',
  swapped:    'text-warning',
  pulled:     'text-negative',
  line_shift: 'text-foreground',
}

function PickEventsBadgeImpl({ sport, gameId }) {
  const [events, setEvents] = useState(null) // null = unfetched
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const popRef = useRef(null)

  // Fetch lazily — first time the user hovers OR opens the popover.
  const ensureLoaded = () => {
    if (events !== null || loading) return
    setLoading(true)
    api.get(`/${sport}/pick-events`, { params: { game_id: gameId, hours: 24 } })
      .then(r => setEvents(Array.isArray(r.data) ? r.data : []))
      .catch(() => setEvents([]))
      .finally(() => setLoading(false))
  }

  // Close on outside click + Esc key.
  useEffect(() => {
    if (!open) return
    const onDown = (e) => {
      if (popRef.current && !popRef.current.contains(e.target)) setOpen(false)
    }
    const onKey = (e) => { if (e.key === 'Escape') setOpen(false) }
    window.addEventListener('mousedown', onDown)
    window.addEventListener('keydown', onKey)
    return () => {
      window.removeEventListener('mousedown', onDown)
      window.removeEventListener('keydown', onKey)
    }
  }, [open])

  // Hide entirely when we know there are no events. Until first fetch
  // we keep the icon visible so users can probe — once loaded with
  // zero results we collapse so empty cards don't show a dead icon.
  if (events !== null && events.length === 0) return null

  const count = events?.length ?? 0
  const stop = (e) => { e.stopPropagation() }

  return (
    <div className="relative inline-flex" ref={popRef} onClick={stop}>
      <button
        type="button"
        onMouseEnter={ensureLoaded}
        onClick={(e) => { e.stopPropagation(); ensureLoaded(); setOpen(o => !o) }}
        className={cn(
          'inline-flex items-center gap-1 rounded-md px-1.5 py-0.5 text-xs font-medium',
          'text-muted-foreground hover:text-foreground hover:bg-accent transition-colors',
          'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
          open && 'bg-accent text-foreground',
        )}
        title="Pick history"
      >
        <ScrollText className="h-3.5 w-3.5" aria-hidden="true" />
        {count > 0 && (
          <span className="tabular-nums text-[10px]">{count}</span>
        )}
      </button>

      {open && (
        <div className="absolute right-0 top-full z-20 mt-1 w-72 rounded-lg border border-border bg-popover shadow-lg animate-in fade-in zoom-in-95 duration-150 origin-top-right">
          <div className="px-3 py-2 border-b border-border text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            Pick history (24h)
          </div>
          {loading && (
            <div className="px-3 py-3 text-xs text-muted-foreground">Loading…</div>
          )}
          {!loading && (events?.length ?? 0) === 0 && (
            <div className="px-3 py-3 text-xs text-muted-foreground">
              No transitions logged yet today.
            </div>
          )}
          {!loading && (events?.length ?? 0) > 0 && (
            <ul className="divide-y divide-border max-h-64 overflow-y-auto">
              {events.map(ev => (
                <li key={ev.id} className="px-3 py-2 text-xs">
                  <div className="flex items-center justify-between">
                    <span className={cn('font-semibold', EVENT_TONES[ev.event_type])}>
                      {EVENT_LABELS[ev.event_type] ?? ev.event_type}
                    </span>
                    <span className="tabular-nums text-muted-foreground">
                      {formatTs(ev.ts)}
                    </span>
                  </div>
                  {renderEventBody(ev)}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  )
}

function formatTs(ts) {
  if (!ts) return ''
  // SQLite emits "YYYY-MM-DD HH:MM:SS" in UTC; treat as UTC for parsing.
  const d = new Date(ts.includes('T') ? ts : ts.replace(' ', 'T') + 'Z')
  if (Number.isNaN(d.getTime())) return ts
  return d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })
}


// Render the event body. Build the reason text from structured prev_*
// / curr_* fields so the popover never leaks engine enum keys
// (Q1_SPREAD, ALT TOTAL, etc) — humanizeBetType maps them to the
// presentable form used elsewhere on the dashboard.
function renderEventBody(ev) {
  const fmtPick = (bt, pick) => {
    if (!bt && !pick) return ''
    const label = bt ? humanizeBetType(bt) : ''
    return [label, pick].filter(Boolean).join(' · ')
  }
  if (ev.event_type === 'appeared' && ev.curr_pick) {
    return (
      <div className="mt-0.5 text-foreground">
        {fmtPick(ev.curr_bet_type, ev.curr_pick)}
        {ev.curr_odds != null && (
          <span className="ml-1 tabular-nums text-muted-foreground">
            {ev.curr_odds > 0 ? '+' : ''}{ev.curr_odds}
          </span>
        )}
      </div>
    )
  }
  if (ev.event_type === 'swapped') {
    return (
      <div className="mt-0.5 text-muted-foreground">
        {fmtPick(ev.prev_bet_type, ev.prev_pick)} → {fmtPick(ev.curr_bet_type, ev.curr_pick)}
      </div>
    )
  }
  if (ev.event_type === 'pulled' && ev.prev_pick) {
    return (
      <div className="mt-0.5 text-muted-foreground">
        Was {fmtPick(ev.prev_bet_type, ev.prev_pick)}
        {ev.reason && <> · {ev.reason}</>}
      </div>
    )
  }
  if (ev.event_type === 'line_shift') {
    return (
      <div className="mt-0.5 text-muted-foreground">
        {fmtPick(ev.curr_bet_type, ev.curr_pick)}
        {ev.reason && <> · {ev.reason}</>}
      </div>
    )
  }
  // Fallback: show the raw reason if present.
  return ev.reason
    ? <div className="mt-0.5 text-muted-foreground">{ev.reason}</div>
    : null
}

const PickEventsBadge = memo(PickEventsBadgeImpl)
export default PickEventsBadge
