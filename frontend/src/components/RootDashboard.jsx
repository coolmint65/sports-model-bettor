import { memo, useMemo } from 'react'
import { ArrowRight } from 'lucide-react'
import { cn } from '../lib/utils'
import { sortSports, isInSeason } from '../lib/sportSort'

/**
 * RootDashboard — cross-sport landing for the "Dashboard" entry.
 *
 * Aggregates the per-sport bestBets payloads the parent already loads
 * so this view doesn't issue any new network calls. Three sections:
 *
 *   1. Slate strip       — game count + top edge per sport, sorted by
 *                          today's activity (in-season first).
 *   2. Top edges board   — flat list of the highest-edge picks across
 *                          all sports tonight, capped at 8.
 *   3. Quick actions     — direct jumps into each sport's Bets view.
 *
 * Visual restyle (full-bleed hero, dense card chrome, etc.) lands in
 * Phase 2d. This commit ships the structure so subsequent restyle
 * work has a place to attach.
 *
 * Props:
 *   sports         - ['mlb','nhl','nba']
 *   bestBetsBySport - { mlb: [...], nhl: [...], nba: [...] }
 *   onSelectSport  - (sport: string) => void; sets the active sport
 *                    AND switches to the Bets sub-view
 */
const SPORT_LABELS = { mlb: 'MLB', nhl: 'NHL', nba: 'NBA' }

function RootDashboardImpl({ sports, bestBetsBySport = {}, onSelectSport }) {
  const ranked = useMemo(() => {
    const counts = {}
    for (const s of sports) counts[s] = (bestBetsBySport[s] || []).length
    return sortSports(sports, counts)
  }, [sports, bestBetsBySport])

  // Top edges across all sports — flatten each sport's bets, take the
  // best_pick from each game with edge > 0, sort desc, cap at 8.
  const topEdges = useMemo(() => {
    const rows = []
    for (const s of sports) {
      for (const b of (bestBetsBySport[s] || [])) {
        if (!b.best_pick || (b.best_pick.edge ?? 0) <= 0) continue
        rows.push({
          sport: s,
          matchup: b.matchup,
          pick: b.best_pick.pick,
          type: b.best_pick.type,
          edge: b.best_pick.edge,
          odds: b.best_pick.odds,
        })
      }
    }
    rows.sort((a, b) => b.edge - a.edge)
    return rows.slice(0, 8)
  }, [sports, bestBetsBySport])

  return (
    <div className="space-y-6 py-4">
      <header>
        <h1 className="text-2xl font-semibold tracking-tight text-foreground">
          Dashboard
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Today's slate across every sport.
        </p>
      </header>

      {/* Slate strip — one card per sport */}
      <section>
        <h2 className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          Today's slate
        </h2>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {ranked.map(({ sport, gameCount, inSeason }) => {
            const bets = bestBetsBySport[sport] || []
            const topEdge = bets.reduce(
              (m, b) => Math.max(m, b.best_pick?.edge ?? 0), 0,
            )
            return (
              <button
                key={sport}
                onClick={() => onSelectSport(sport)}
                className={cn(
                  'group flex items-center justify-between rounded-lg border border-border bg-card px-4 py-4 text-left transition-colors',
                  'hover:border-primary/40 hover:bg-accent',
                  'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
                  !inSeason && 'opacity-60',
                )}
              >
                <div>
                  <div className="text-sm font-semibold text-foreground">
                    {SPORT_LABELS[sport] ?? sport.toUpperCase()}
                  </div>
                  <div className="mt-0.5 text-xs text-muted-foreground">
                    {inSeason
                      ? `${gameCount} game${gameCount === 1 ? '' : 's'}`
                      : 'Off-season'}
                    {inSeason && topEdge > 0 && ` · top +${topEdge.toFixed(1)}%`}
                  </div>
                </div>
                <ArrowRight className="h-4 w-4 text-muted-foreground transition-transform group-hover:translate-x-0.5" />
              </button>
            )
          })}
        </div>
      </section>

      {/* Top edges across all sports */}
      <section>
        <h2 className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          Top edges tonight
        </h2>
        {topEdges.length === 0 ? (
          <div className="rounded-lg border border-dashed border-border bg-card/50 p-6 text-center text-sm text-muted-foreground">
            No qualifying picks across the slate yet — best-bets is still
            computing or every game has crossed into live.
          </div>
        ) : (
          <ul className="divide-y divide-border rounded-lg border border-border bg-card">
            {topEdges.map((row, i) => (
              <li
                key={`${row.sport}-${row.matchup}-${i}`}
                className="flex items-center justify-between px-4 py-3"
              >
                <div className="min-w-0">
                  <div className="flex items-center gap-2 text-sm">
                    <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] font-semibold uppercase text-muted-foreground">
                      {SPORT_LABELS[row.sport]}
                    </span>
                    <span className="font-medium text-foreground truncate">
                      {row.matchup}
                    </span>
                  </div>
                  <div className="mt-0.5 text-xs text-muted-foreground truncate">
                    {row.type} · {row.pick}
                    {row.odds != null && (
                      <span className="ml-1 tabular-nums">
                        {row.odds > 0 ? '+' : ''}{row.odds}
                      </span>
                    )}
                  </div>
                </div>
                <div className="ml-3 text-right">
                  <div className="text-sm font-semibold tabular-nums text-positive">
                    +{row.edge.toFixed(1)}%
                  </div>
                </div>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  )
}

const RootDashboard = memo(RootDashboardImpl)
export default RootDashboard
