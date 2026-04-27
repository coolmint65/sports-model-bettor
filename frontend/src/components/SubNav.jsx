import { memo } from 'react'
import { cn } from '../lib/utils'

/**
 * SubNav — horizontal section nav inside a per-sport landing.
 *
 * Replaces the legacy `.nav-tabs` row with a Tailwind-driven version
 * sized for the new shell (sticky-on-scroll, sits below the page hero).
 * Tabs are statically defined in the parent so adding/renaming here
 * is just a config change rather than a layout edit. Locked tabs
 * (e.g. "Props" until Phase 2g+) render disabled with a "soon" badge
 * so users see the upcoming surface without trying to use it.
 *
 * Props:
 *   tabs       - [{ id, label, locked?: bool, badge?: string }]
 *   active     - the selected tab id
 *   onChange   - (id: string) => void; not called when locked
 */
function SubNavImpl({ tabs, active, onChange }) {
  return (
    <nav className="sticky top-0 z-10 flex items-center gap-1 border-b border-border bg-background/95 backdrop-blur px-1 -mx-1 overflow-x-auto overflow-y-hidden no-scrollbar">
      {tabs.map(t => {
        const isActive = active === t.id
        const interactive = !t.locked
        return (
          <button
            key={t.id}
            type="button"
            disabled={!interactive}
            onClick={() => interactive && onChange(t.id)}
            className={cn(
              'relative flex items-center gap-1.5 whitespace-nowrap px-4 py-3 text-sm font-medium transition-colors',
              'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
              isActive
                ? 'text-foreground'
                : interactive
                  ? 'text-muted-foreground hover:text-foreground'
                  : 'text-muted-foreground/50 cursor-not-allowed',
            )}
          >
            <span>{t.label}</span>
            {t.badge && (
              <span
                className={cn(
                  'rounded-full px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wider',
                  t.locked
                    ? 'bg-muted text-muted-foreground'
                    : 'bg-primary/15 text-primary',
                )}
              >
                {t.badge}
              </span>
            )}
            {isActive && (
              <span className="absolute inset-x-2 -bottom-px h-0.5 bg-primary" />
            )}
          </button>
        )
      })}
    </nav>
  )
}

const SubNav = memo(SubNavImpl)
export default SubNav
