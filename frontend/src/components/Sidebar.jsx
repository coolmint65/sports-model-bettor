import { memo } from 'react'
import { Beer, Snowflake, Dribbble, LayoutDashboard } from 'lucide-react'
import { sortSports } from '../lib/sportSort'
import { cn } from '../lib/utils'

/**
 * Sidebar — vertical sport selector with season-relevance sort.
 *
 * In-season sports pin to the top sorted by today's game count
 * (most active first); out-of-season sports sit at the bottom at
 * 60% opacity. The badge next to each sport shows today's slate
 * size so the user can scan "where's the action tonight" without
 * clicking.
 *
 * Game counts come from the parent — App.jsx passes the lengths of
 * each sport's scoreboard array. Pre-load (count = 0) the badge is
 * suppressed so empty-vs-loading is unambiguous.
 *
 * Phase 2b scope: visual shell only. Mobile collapse + animations
 * land in Phase 2e. Currently desktop-only (>= 768px) — narrow
 * viewports still see the sidebar but it'll be cramped until 2e.
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

const SPORT_ICONS = {
  mlb: Beer,         // placeholder — swap for a baseball-specific icon
  nhl: Snowflake,    // ice / cold = hockey
  nba: Dribbble,     // basketball-ish
}

function SidebarImpl({ sports, selected, onSelect, gameCounts = {} }) {
  const ranked = sortSports(sports, gameCounts)

  return (
    <aside className="flex h-screen w-56 flex-col border-r border-border bg-card sticky top-0">
      <div className="px-5 pt-6 pb-4 border-b border-border">
        <div className="text-base font-semibold text-foreground tracking-tight">
          Sportsbook Edge
        </div>
        <div className="text-xs text-muted-foreground mt-0.5">
          Data-driven picks
        </div>
      </div>

      <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
        {/* Dashboard pinned above the sport list — cross-sport root view */}
        <button
          onClick={() => onSelect('dashboard')}
          className={cn(
            'flex w-full items-center gap-2.5 rounded-md px-3 py-2 text-sm font-medium transition-colors',
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
          const Icon = SPORT_ICONS[sport]
          return (
            <button
              key={sport}
              onClick={() => onSelect(sport)}
              className={cn(
                'flex w-full items-center justify-between rounded-md px-3 py-2 text-sm font-medium transition-colors',
                'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
                isActive
                  ? 'bg-primary text-primary-foreground'
                  : 'text-foreground hover:bg-accent hover:text-accent-foreground',
                !inSeason && 'opacity-60',
              )}
            >
              <span className="flex items-center gap-2.5">
                {Icon && <Icon className="h-4 w-4" aria-hidden="true" />}
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
        v2.0 · phase 2b
      </div>
    </aside>
  )
}

const Sidebar = memo(SidebarImpl)
export default Sidebar
