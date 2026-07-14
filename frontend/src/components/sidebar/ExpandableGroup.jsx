/**
 * ExpandableGroup — shared collapsible sidebar group.
 *
 * Every sport that expands into leagues (Basketball / Hockey / Soccer /
 * Motorsports / Baseball) uses this shell:
 *   - Parent row with icon + label + count badge + chevron
 *   - Highlighted when one of its children is active
 *   - `children` renders the expanded sub-list (per-sport shape:
 *     confederation groups, region groups, flat sorts, etc.)
 *
 * Extracted from Sidebar 2026-07-08 — the five near-identical group
 * components were 100 lines each and all diverged only in the
 * expanded-list rendering. Now the shell lives here, each sport
 * contributes a ~15-line wrapper.
 */
import { ChevronRight } from 'lucide-react'
import { cn } from '../../lib/utils'


export default function ExpandableGroup({
  icon, label, expanded, onToggleExpanded, isAnyChildActive,
  gameCount, spacing = 'space-y-1', children,
}) {
  return (
    <div>
      <button
        type="button"
        onClick={onToggleExpanded}
        aria-expanded={expanded}
        className={cn(
          'flex w-full items-center justify-between rounded-md px-3 py-2.5 text-sm font-medium transition-colors',
          'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
          isAnyChildActive && !expanded
            ? 'bg-primary/10 text-primary'
            : 'text-foreground hover:bg-accent hover:text-accent-foreground',
        )}
      >
        <span className="flex items-center gap-2.5">
          <span className="text-base leading-none" aria-hidden="true">{icon}</span>
          <span>{label}</span>
        </span>
        <span className="flex items-center gap-1.5">
          {gameCount > 0 && (
            <span className="rounded-full px-1.5 py-0.5 text-[10px] font-semibold tabular-nums bg-muted text-muted-foreground">
              {gameCount}
            </span>
          )}
          <ChevronRight
            className={cn(
              'h-3.5 w-3.5 transition-transform',
              expanded ? 'rotate-90' : '',
            )}
            aria-hidden="true"
          />
        </span>
      </button>

      {expanded && (
        <div className={cn(
          'mt-1 ml-2 border-l border-border pl-1.5',
          spacing,
        )}>
          {children}
        </div>
      )}
    </div>
  )
}
