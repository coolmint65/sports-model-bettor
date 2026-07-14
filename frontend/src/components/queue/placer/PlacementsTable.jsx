/**
 * PlacementsTable — auto-placement log.
 *
 * Grouped by local date so a night of sweeps doesn't repeat the same
 * `07-13` timestamp on every row. Times render in the viewer's local
 * timezone (backend stores UTC via `datetime('now')`, we convert here
 * so the display matches the wall-clock the user actually sees).
 *
 * Mode/status collapse into a single badge when they carry the same
 * information (dry-run rows are always mode=dry_run + status=dry_run,
 * so we show one pill instead of two).
 */
import { useMemo } from 'react'
import { cn } from '../../../lib/utils'
import { humanizeBetType } from '../../../lib/betType'


export default function PlacementsTable({ rows }) {
  const grouped = useMemo(() => groupByLocalDate(rows), [rows])

  if (!rows.length) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/40 px-6 py-10 text-center text-sm text-muted-foreground">
        No placements yet.
      </div>
    )
  }

  return (
    <div className="rounded-lg border border-border bg-card overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-background/40 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            <th className="px-3 py-2 text-left w-20">Time</th>
            <th className="px-3 py-2 text-left w-14">Sport</th>
            <th className="px-3 py-2 text-left">Matchup</th>
            <th className="px-3 py-2 text-left">Pick</th>
            <th className="px-3 py-2 text-right w-16">Odds</th>
            <th className="px-3 py-2 text-right w-16">Stake</th>
            <th className="px-3 py-2 text-center w-24">Status</th>
            <th className="px-3 py-2 text-right w-20">P/L</th>
          </tr>
        </thead>
        <tbody>
          {grouped.map(({ date, rows: dayRows }) => (
            <FragmentDay key={date} date={date} rows={dayRows} />
          ))}
        </tbody>
      </table>
    </div>
  )
}


function FragmentDay({ date, rows }) {
  return (
    <>
      <tr className="border-b border-border/60 bg-background/30">
        <td colSpan={8}
             className="px-3 py-1.5 text-[10px] font-bold uppercase tracking-[0.15em] text-muted-foreground">
          {formatDateHeader(date)}
        </td>
      </tr>
      {rows.map(r => <PlacementRow key={r.id} row={r} />)}
    </>
  )
}


function PlacementRow({ row }) {
  const stake = row.placed_stake_d ?? row.requested_stake_d
  const odds = row.placed_odds ?? row.requested_odds
  const profit = row.profit_dollars
  const pTone = profit == null ? 'text-muted-foreground'
    : profit > 0.001 ? 'text-positive'
    : profit < -0.001 ? 'text-negative' : 'text-foreground'
  const timestamp = row.completed_at || row.submitted_at || row.queued_at
  return (
    <tr className="border-b border-border/60 hover:bg-accent/30">
      <td className="px-3 py-2 text-xs text-muted-foreground whitespace-nowrap tabular-nums">
        {formatLocalTime(timestamp)}
      </td>
      <td className="px-3 py-2 text-xs">
        <span className="uppercase tracking-wider text-[10px] text-muted-foreground">
          {row.sport}
        </span>
      </td>
      <td className="px-3 py-2 text-xs text-foreground truncate max-w-[18rem]" title={row.matchup}>
        {row.matchup || '—'}
      </td>
      <td className="px-3 py-2">
        <span className="text-xs font-semibold text-foreground">{row.pick}</span>
        <span className="ml-1.5 text-[10px] uppercase tracking-wider text-muted-foreground">
          {humanizeBetType(row.bet_type)}
        </span>
      </td>
      <td className="px-3 py-2 text-right text-xs tabular-nums text-muted-foreground">
        {odds != null ? `${odds > 0 ? '+' : ''}${odds}` : '—'}
      </td>
      <td className="px-3 py-2 text-right text-xs tabular-nums">
        ${Number(stake || 0).toFixed(2)}
      </td>
      <td className="px-3 py-2 text-center">
        <StatusPill mode={row.mode} status={row.status}
                     reason={row.reject_reason} />
      </td>
      <td className={cn('px-3 py-2 text-right text-xs tabular-nums font-semibold', pTone)}>
        {profit != null
          ? `${profit >= 0 ? '+' : ''}$${profit.toFixed(2)}`
          : row.result === 'W' ? 'W'
          : row.result === 'L' ? 'L'
          : row.result === 'P' ? 'P'
          : '—'}
      </td>
    </tr>
  )
}


/**
 * StatusPill — collapses mode+status into a single readable label.
 * Dry-run rows always carry mode=dry_run + status=dry_run — show
 * one "Test" pill instead of two identical badges. Live rows show
 * only the terminal status.
 */
function StatusPill({ mode, status, reason }) {
  if (mode === 'dry_run') {
    return (
      <span
        className="inline-flex rounded px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider bg-muted text-muted-foreground border border-border"
        title="Dry-run — logged only, no real bet placed."
      >
        Test
      </span>
    )
  }
  const meta = _STATUS_META[status] || _STATUS_META.rejected_other
  return (
    <span
      className={cn(
        'inline-flex rounded px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider',
        meta.tone,
      )}
      title={reason || meta.title || status}
    >
      {meta.label}
    </span>
  )
}


const _STATUS_META = {
  placed:            { label: 'Placed',   tone: 'bg-positive/15 text-positive',
                       title: 'HR accepted the bet.' },
  submitted:         { label: 'Sending',  tone: 'bg-primary/15 text-primary',
                       title: 'Relay is forwarding to HR.' },
  queued:            { label: 'Queued',   tone: 'bg-primary/15 text-primary',
                       title: 'Waiting in the relay queue.' },
  rejected_line:     { label: 'Line moved', tone: 'bg-warning/15 text-warning',
                       title: 'HR line drifted past tolerance.' },
  rejected_market:   { label: 'Suspended', tone: 'bg-warning/15 text-warning',
                       title: 'Market was unavailable at HR.' },
  rejected_stake:    { label: 'Bad stake', tone: 'bg-warning/15 text-warning',
                       title: 'HR rejected on stake / balance.' },
  rejected_dedup:    { label: 'Duplicate', tone: 'bg-muted text-muted-foreground',
                       title: 'Already placed today.' },
  rejected_cap:      { label: 'Cap',       tone: 'bg-warning/15 text-warning',
                       title: 'Would breach a stake cap.' },
  rejected_halt:     { label: 'Halted',    tone: 'bg-negative/15 text-negative',
                       title: 'Circuit breaker halted this placement.' },
  rejected_offline:  { label: 'Offline',   tone: 'bg-negative/15 text-negative',
                       title: 'Relay unreachable.' },
  rejected_other:    { label: 'Error',     tone: 'bg-negative/15 text-negative',
                       title: 'Relay rejected — see reason.' },
  expired:           { label: 'Expired',   tone: 'bg-negative/15 text-negative',
                       title: 'Timed out before placement.' },
}


// ── Time helpers ─────────────────────────────────────────────

function _parseUTC(ts) {
  if (!ts) return null
  // Backend writes `2026-07-14 04:10:05` (SQLite datetime('now') style,
  // implicitly UTC) or ISO strings. Handle both by normalizing to
  // ISO+Z so Date parses in UTC.
  const s = String(ts).replace(' ', 'T')
  const withZ = /[zZ]|[+-]\d\d:?\d\d$/.test(s) ? s : s + 'Z'
  const d = new Date(withZ)
  return isNaN(d.getTime()) ? null : d
}


function formatLocalTime(ts) {
  const d = _parseUTC(ts)
  if (!d) return '—'
  return d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })
}


function groupByLocalDate(rows) {
  // Group rows by the viewer's local day. Backend timestamps are UTC
  // so a bet placed at 22:00 ET (02:00 UTC next day) needs to render
  // under the ET calendar day, not the UTC one.
  const groups = new Map()
  for (const r of rows) {
    const ts = r.completed_at || r.submitted_at || r.queued_at
    const d = _parseUTC(ts)
    const key = d
      ? `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
      : 'unknown'
    if (!groups.has(key)) groups.set(key, [])
    groups.get(key).push(r)
  }
  // Rows come in most-recent-first from the API; preserve that inside
  // each day, and sort day keys DESC.
  return Array.from(groups.entries())
    .sort(([a], [b]) => (a < b ? 1 : a > b ? -1 : 0))
    .map(([date, rows]) => ({ date, rows }))
}


function formatDateHeader(dateKey) {
  if (dateKey === 'unknown') return 'Undated'
  const [y, m, d] = dateKey.split('-').map(Number)
  const target = new Date(y, m - 1, d)
  const today = new Date()
  today.setHours(0, 0, 0, 0)
  const diffDays = Math.round((target - today) / 86400000)
  if (diffDays === 0) return 'Today'
  if (diffDays === -1) return 'Yesterday'
  return target.toLocaleDateString([], {
    weekday: 'short', month: 'short', day: 'numeric',
  })
}
