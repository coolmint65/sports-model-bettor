/**
 * PanelShell — shared layout for every sport landing.
 *
 * Captures the structure every per-sport panel already had piecemeal
 * (Header → SubNav → body) into one primitive so adding a new sport
 * is a content edit, not a layout copy. The header takes a small set
 * of slots (title, subtitle, status pill, context chips, right-aligned
 * extras) that cover the variations across MLB/NHL/NBA/Basketball
 * framework/Hockey framework/Motorsports/Golf/Tennis.
 *
 * Layout contract:
 *   - Header pinned at the top, wraps on narrow screens.
 *   - SubNav directly below the header (sticky-on-scroll via SubNav).
 *   - Body below — caller renders content per active tab.
 *
 * Standard tab vocabulary (use these ids unless your sport has a
 * good reason to deviate):
 *   slate       — "what's live / on the slate today"
 *   tracker     — pending + settled picks
 *   calibration — per-market reliability + bucket maps
 *   standings   — league / season standings
 *
 * Outright sports (motorsports, golf) re-label `slate` to whatever
 * makes sense (Race / Tournament) by setting tab.label — the id stays
 * 'slate' so cross-cutting code (POTD hero, deep-links) can rely on it.
 *
 * Props:
 *   title          - big display name (e.g. "PGA Tour", "WNBA")
 *   subtitle       - optional secondary line (tournament name, league
 *                    subtitle, "Week 12", etc)
 *   statusBadge    - optional { label, tone? } — 'beta' | 'active' | …
 *   contextChips   - optional array of {icon?, text, tone?, key?} items
 *                    rendered inline next to the title. Use for course
 *                    info, dates, season-state, major flags, etc.
 *   headerExtras   - optional right-aligned slot (tour selector,
 *                    summary tiles, action button, …)
 *   tabs           - array of {id, label, badge?, locked?}
 *   active         - currently active tab id
 *   onTabChange    - (id) => void
 *   children       - body content (caller switches on active tab)
 */
import SubNav from '../SubNav'
import { cn } from '../../lib/utils'

const STATUS_TONES = {
  beta:     'bg-warning/20 text-warning',
  active:   'bg-positive/20 text-positive',
  pending_calibration: 'bg-muted text-muted-foreground',
  experimental: 'bg-blue-500/20 text-blue-300',
}


export default function PanelShell({
  title,
  subtitle,
  statusBadge,
  contextChips,
  headerExtras,
  tabs,
  active,
  onTabChange,
  children,
}) {
  return (
    <section className="space-y-4">
      <PanelHeader
        title={title}
        subtitle={subtitle}
        statusBadge={statusBadge}
        contextChips={contextChips}
        headerExtras={headerExtras}
      />
      {tabs && tabs.length > 0 && (
        <SubNav tabs={tabs} active={active} onChange={onTabChange} />
      )}
      {children}
    </section>
  )
}


function PanelHeader({ title, subtitle, statusBadge, contextChips, headerExtras }) {
  return (
    <header className="flex items-start justify-between gap-3 flex-wrap">
      <div className="flex items-baseline gap-3 flex-wrap min-w-0">
        <h1 className="text-2xl font-bold tracking-tight text-foreground truncate">
          {title}
        </h1>
        {subtitle && (
          <span className="text-sm text-muted-foreground truncate">
            · {subtitle}
          </span>
        )}
        {statusBadge && <StatusPill {...statusBadge} />}
        {Array.isArray(contextChips) && contextChips.map((c, i) => (
          <ContextChip key={c.key ?? i} {...c} />
        ))}
      </div>
      {headerExtras && (
        <div className="flex items-center gap-2 ml-auto">
          {headerExtras}
        </div>
      )}
    </header>
  )
}


export function StatusPill({ label, tone }) {
  if (!label) return null
  const cls = STATUS_TONES[tone] ?? STATUS_TONES[label?.toLowerCase()]
    ?? 'bg-muted text-muted-foreground'
  return (
    <span className={cn(
      'text-[10px] uppercase tracking-wider rounded-full px-2 py-0.5',
      cls,
    )}>
      {label}
    </span>
  )
}


export function ContextChip({ icon, text, tone }) {
  if (!text) return null
  // Default = inline muted text; tone='amber' for special flags
  // (Major, in-progress, breakthrough); 'positive' for go-state.
  const styled = tone === 'amber'
    ? 'bg-amber-500/15 text-amber-200 rounded px-1.5 py-0.5 text-[10px] uppercase tracking-wider'
    : tone === 'positive'
      ? 'text-positive text-xs'
      : tone === 'warning'
        ? 'text-warning text-xs'
        : 'text-xs text-muted-foreground'
  return (
    <span className={cn('flex items-center gap-1', styled)}>
      {icon && <span aria-hidden="true">{icon}</span>}
      <span>{text}</span>
    </span>
  )
}


/**
 * StatTile — small KPI card used for tracker summaries + dashboards.
 * Same visual language across every panel so users can read W/L/ROI
 * at a glance without learning a per-sport layout.
 *
 *   label     short uppercase string (e.g. "Win Rate")
 *   value     primary number / string
 *   tone      'positive' | 'negative' | undefined → tint the value
 *   hint      optional caption below the value (e.g. "last 7 days")
 */
export function StatTile({ label, value, tone, hint }) {
  const valueClass = tone === 'positive' ? 'text-positive'
                    : tone === 'negative' ? 'text-negative'
                    : 'text-foreground'
  return (
    <div className="rounded-md border border-border bg-card px-3 py-2 min-w-0">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground truncate">
        {label}
      </div>
      <div className={cn('text-base font-semibold tabular-nums truncate', valueClass)}>
        {value}
      </div>
      {hint && (
        <div className="text-[10px] text-muted-foreground/80 truncate">
          {hint}
        </div>
      )}
    </div>
  )
}


/**
 * StatGrid — row of StatTiles wrapped in a consistent gap grid. Used
 * by trackers, calibration summary headers, etc.
 *
 *   tiles  array of StatTile props (each one rendered as its own tile)
 */
export function StatGrid({ tiles }) {
  if (!Array.isArray(tiles) || !tiles.length) return null
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-2">
      {tiles.map((t, i) => (
        <StatTile key={t.key ?? i} {...t} />
      ))}
    </div>
  )
}


/**
 * EmptyState — used by panels for "no slate", "no picks", "off-season".
 *
 *   title      headline ("No upcoming tournament")
 *   message    optional secondary line
 *   icon       optional emoji / icon
 */
export function EmptyState({ title, message, icon }) {
  return (
    <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-12 text-center">
      {icon && <div className="text-3xl mb-2" aria-hidden="true">{icon}</div>}
      <div className="text-sm font-semibold text-foreground">{title}</div>
      {message && (
        <div className="text-xs text-muted-foreground mt-1">{message}</div>
      )}
    </div>
  )
}


/**
 * ErrorBox — consistent error rendering across panels. Use when a
 * panel hits a real failure (vs an off-season / cold-start empty
 * state, which uses EmptyState).
 */
export function ErrorBox({ message }) {
  return (
    <div className="rounded-lg border border-destructive/40 bg-destructive/10 px-5 py-4 text-sm text-destructive">
      {message || 'Something went wrong loading this panel.'}
    </div>
  )
}


/**
 * Standard tab list for team / framework sports. Outright sports
 * (motorsports, golf) can override `slate` label but should keep
 * the id.
 */
export const STANDARD_TABS = Object.freeze([
  { id: 'slate',       label: 'Slate' },
  { id: 'tracker',     label: 'Tracker' },
  { id: 'calibration', label: 'Calibration' },
  { id: 'standings',   label: 'Standings' },
])
