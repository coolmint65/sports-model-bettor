import { cn } from '../../lib/utils'

/**
 * MarketToggle — segmented control for picking a market type within
 * the Bets / Tracker view. Replaces the separate top-level Props /
 * Derivatives / 1st Inn tabs that previously cluttered the SubNav.
 *
 * Caller passes the option set (sport-aware: Q1 only for NBA, 1st
 * Inn only for MLB) and an active id. The component is purely
 * presentational; routing + state lives in BetsView.
 *
 * Mobile behaviour: scrollable horizontally inside its container so
 * 4-5 options never wrap. The flex-shrink-0 on each button keeps the
 * pill from collapsing when overflowing.
 *
 * Props:
 *   options  — [{ id, label, badge? }] in display order
 *   active   — currently selected id
 *   onChange — (id) => void
 */
export default function MarketToggle({ options, active, onChange }) {
  if (!options || options.length === 0) return null
  return (
    <div className="inline-flex max-w-full overflow-x-auto rounded-md border border-border bg-card shadow-sm scrollbar-thin">
      {options.map(opt => (
        <button
          key={opt.id}
          type="button"
          onClick={() => onChange(opt.id)}
          className={cn(
            'flex-shrink-0 px-3 py-1.5 text-xs font-semibold tracking-wider uppercase transition-colors',
            active === opt.id
              ? 'bg-primary text-primary-foreground'
              : 'text-muted-foreground hover:bg-accent hover:text-foreground',
          )}
        >
          {opt.label}
          {opt.badge != null && opt.badge !== 0 && (
            <span className={cn(
              'ml-1.5 rounded-full px-1.5 py-px text-[10px] font-bold tabular-nums',
              active === opt.id
                ? 'bg-primary-foreground/20 text-primary-foreground'
                : 'bg-muted text-muted-foreground',
            )}>
              {opt.badge}
            </span>
          )}
        </button>
      ))}
    </div>
  )
}
