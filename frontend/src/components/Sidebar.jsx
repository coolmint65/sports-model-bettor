import { memo, useEffect, useState } from 'react'
import { LayoutDashboard, Menu, X } from 'lucide-react'
import { sortSports } from '../lib/sportSort'
import { cn } from '../lib/utils'

/**
 * Sidebar — vertical sport selector with season-relevance sort.
 *
 * Desktop (>= md): always visible as a sticky 14rem column on the left.
 * Mobile (< md):    hidden by default; a hamburger button at top-left
 *                   slides the sidebar in as a drawer with a backdrop.
 *                   Selecting an entry auto-closes the drawer so the
 *                   user lands on the chosen view without an extra tap.
 *
 * In-season sports pin to the top sorted by today's game count
 * (most active first); out-of-season sports sit at the bottom at
 * 60% opacity. The badge next to each sport shows today's slate
 * size so the user can scan "where's the action tonight" without
 * clicking.
 *
 * Phase 2e additions: mobile drawer + backdrop, drawer slide animation,
 * Esc-to-close, body scroll lock while open, focus rings on every
 * interactive surface.
 *
 * Props:
 *   sports        - array of sport keys (e.g. ['mlb','nhl','nba'])
 *   selected      - currently active sport key (lowercase) OR
 *                   'dashboard' when the cross-sport root is showing
 *   onSelect      - callback (sport: string) => void; called with
 *                   either a sport key or the literal 'dashboard'
 *   gameCounts    - { sport: number } for today's slate sizes
 */
const SPORT_LABELS = {
  mlb: 'MLB',
  nhl: 'NHL',
  nba: 'NBA',
}

// Sport glyphs as Unicode emoji rather than lucide icons — lucide
// doesn't ship per-sport icons (no Baseball/Basketball/Hockey-stick),
// and the closest matches (Beer for MLB, Dribbble for NBA) read as
// noise. Emoji renders consistently on Win Segoe UI Emoji + Apple
// Color Emoji and lets future sports (Tennis 🎾, Soccer ⚽, Golf 🏌️)
// plug in without hunting for substitutes.
const SPORT_ICONS = {
  mlb: '⚾',
  nhl: '🏒',
  nba: '🏀',
}

function SidebarImpl({ sports, selected, onSelect, gameCounts = {} }) {
  const ranked = sortSports(sports, gameCounts)
  const [open, setOpen] = useState(false)

  // Close on Esc and when viewport widens to md+ so reopening on
  // mobile doesn't surprise a desktop user.
  useEffect(() => {
    if (!open) return
    const onKey = (e) => { if (e.key === 'Escape') setOpen(false) }
    const onResize = () => {
      // 768 = Tailwind md breakpoint
      if (window.innerWidth >= 768) setOpen(false)
    }
    window.addEventListener('keydown', onKey)
    window.addEventListener('resize', onResize)
    // Body scroll lock while drawer is open so the underlying scroll
    // can't sneak past the backdrop on iOS.
    const prev = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => {
      window.removeEventListener('keydown', onKey)
      window.removeEventListener('resize', onResize)
      document.body.style.overflow = prev
    }
  }, [open])

  const handleSelect = (id) => {
    onSelect(id)
    setOpen(false) // auto-close drawer after a selection on mobile
  }

  return (
    <>
      {/* Mobile hamburger — visible only when sidebar is hidden.
          Sits inside a top-left fixed slot so it overlays page chrome. */}
      <button
        type="button"
        onClick={() => setOpen(true)}
        className={cn(
          'md:hidden fixed top-3 left-3 z-30 inline-flex h-10 w-10 items-center justify-center',
          'rounded-md border border-border bg-card text-foreground shadow-md',
          'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
        )}
        aria-label="Open navigation"
      >
        <Menu className="h-5 w-5" aria-hidden="true" />
      </button>

      {/* Backdrop — only shows on mobile while drawer is open. */}
      {open && (
        <div
          onClick={() => setOpen(false)}
          className="md:hidden fixed inset-0 z-40 bg-background/80 backdrop-blur-sm animate-in fade-in"
          aria-hidden="true"
        />
      )}

      <aside
        className={cn(
          'flex h-screen w-56 flex-col border-r border-border bg-card',
          // Desktop: sticky in flow.
          'md:sticky md:top-0 md:translate-x-0',
          // Mobile: fixed off-canvas; slide in when open.
          'fixed inset-y-0 left-0 z-50 transform transition-transform duration-200 ease-out',
          open ? 'translate-x-0 shadow-2xl' : '-translate-x-full md:translate-x-0',
        )}
        role="navigation"
        aria-label="Sport selector"
      >
        <div className="flex items-center justify-between px-5 pt-6 pb-4 border-b border-border">
          <div>
            <div className="flex items-center gap-1.5 text-base font-semibold tracking-tight">
              <span aria-hidden="true">🌿</span>
              <span className="text-primary">PickMint</span>
            </div>
            <div className="text-xs text-muted-foreground mt-0.5">
              Data-driven picks
            </div>
          </div>
          {/* Close button on mobile only */}
          <button
            type="button"
            onClick={() => setOpen(false)}
            className={cn(
              'md:hidden inline-flex h-8 w-8 items-center justify-center rounded-md',
              'text-muted-foreground hover:bg-accent hover:text-foreground transition-colors',
              'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
            )}
            aria-label="Close navigation"
          >
            <X className="h-4 w-4" aria-hidden="true" />
          </button>
        </div>

        <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
          {/* Dashboard pinned above the sport list — cross-sport root view */}
          <button
            onClick={() => handleSelect('dashboard')}
            className={cn(
              'flex w-full items-center gap-2.5 rounded-md px-3 py-2.5 text-sm font-medium transition-colors',
              'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
              selected === 'dashboard'
                ? 'bg-primary text-primary-foreground'
                : 'text-foreground hover:bg-accent hover:text-accent-foreground',
            )}
          >
            <LayoutDashboard className="h-4 w-4" aria-hidden="true" />
            <span>Dashboard</span>
          </button>

          <div className="my-2 px-3">
            <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              Sports
            </div>
          </div>

          {ranked.map(({ sport, gameCount, inSeason }) => {
            const isActive = selected === sport
            const glyph = SPORT_ICONS[sport]
            return (
              <button
                key={sport}
                onClick={() => handleSelect(sport)}
                className={cn(
                  'flex w-full items-center justify-between rounded-md px-3 py-2.5 text-sm font-medium transition-colors',
                  'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
                  isActive
                    ? 'bg-primary text-primary-foreground'
                    : 'text-foreground hover:bg-accent hover:text-accent-foreground',
                  !inSeason && 'opacity-60',
                )}
              >
                <span className="flex items-center gap-2.5">
                  {glyph && (
                    <span className="text-base leading-none" aria-hidden="true">
                      {glyph}
                    </span>
                  )}
                  <span>{SPORT_LABELS[sport] ?? sport.toUpperCase()}</span>
                </span>
                {inSeason && gameCount > 0 && (
                  <span
                    className={cn(
                      'rounded-full px-1.5 py-0.5 text-[10px] font-semibold tabular-nums',
                      isActive
                        ? 'bg-primary-foreground/20 text-primary-foreground'
                        : 'bg-muted text-muted-foreground',
                    )}
                  >
                    {gameCount}
                  </span>
                )}
                {!inSeason && (
                  <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
                    off
                  </span>
                )}
              </button>
            )
          })}
        </nav>

        <div className="px-5 py-3 border-t border-border text-[10px] text-muted-foreground">
          v2.0 · phase 2e
        </div>
      </aside>
    </>
  )
}

const Sidebar = memo(SidebarImpl)
export default Sidebar
