/**
 * Shared table cell + tiny presentational helpers for the motorsports
 * panel + its extracted subviews. Kept in one file so StandingsView /
 * CalibrationView / RaceView / TrackerView don't each duplicate the
 * `<th className="px-3 py-2.5..."` boilerplate.
 */


export function Th({ children, align = 'left' }) {
  const a = align === 'right' ? 'text-right'
    : align === 'center' ? 'text-center' : 'text-left'
  return (
    <th
      scope="col"
      className={`px-3 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground ${a}`}
    >
      {children}
    </th>
  )
}


export function Td({ children, align = 'left', className = '' }) {
  const a = align === 'right' ? 'text-right'
    : align === 'center' ? 'text-center' : 'text-left'
  return <td className={`px-3 py-2 ${a} ${className}`}>{children}</td>
}


export function Stat({ label, value, valueClass = '' }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      <div className={`text-lg font-semibold tabular-nums ${valueClass || 'text-foreground'}`}>
        {value}
      </div>
    </div>
  )
}


export function pct(p) {
  if (p == null) return '—'
  return `${(p * 100).toFixed(1)}%`
}


export function fmtOdds(o) {
  if (o == null) return '—'
  return o > 0 ? `+${o}` : `${o}`
}


export function fmtRaceTime(iso, fallbackDate) {
  if (!iso) return fallbackDate || ''
  try {
    const d = new Date(iso)
    return d.toLocaleString(undefined, {
      weekday: 'short', month: 'short', day: 'numeric',
      hour: 'numeric', minute: '2-digit', timeZoneName: 'short',
    })
  } catch {
    return iso
  }
}
