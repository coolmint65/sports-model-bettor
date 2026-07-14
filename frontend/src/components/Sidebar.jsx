import { memo, useEffect, useState } from 'react'
import { LayoutDashboard, Menu, X } from 'lucide-react'
import { sortSports } from '../lib/sportSort'
import { cn } from '../lib/utils'
import {
  BasketballGroup, HockeyGroup, SoccerGroup,
  MotorsportsGroup, BaseballGroup,
} from './sidebar/SportGroups'

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
 * Basketball expansion (2026-05-04 / task #214): when ``basketballLeagues``
 * is provided, the 'nba' entry collapses into a "Basketball" group that
 * expands to show every league we model — NBA + WNBA + NCAAM + ~28
 * international leagues, region-grouped (USA / International / Europe /
 * Americas / Asia/Oceania), in-season first inside each region. Selected
 * state highlights the active league. ``selected`` may be any league key
 * ('nba', 'wnba', 'euroleague', ...) — the parent owns the routing.
 *
 * Props:
 *   sports             - array of sport keys (e.g. ['mlb','nhl','nba'])
 *   selected           - currently active sport key (lowercase) OR
 *                        'dashboard' when the cross-sport root is showing
 *   onSelect           - callback (id: string) => void
 *   gameCounts         - { sport: number } for today's slate sizes
 *   basketballLeagues  - optional array of league dicts from
 *                        /api/basketball/leagues. When present, replaces
 *                        the flat 'nba' entry with the expandable group.
 */
const SPORT_LABELS = {
  mlb: 'MLB',
  nhl: 'NHL',
  nba: 'NBA',
  basketball: 'Basketball',
  hockey: 'Hockey',
  tennis: 'Tennis',
  afl: 'AFL',
  f1: 'Formula 1',
  golf: 'Golf',
  soccer: 'Soccer',
  ufl: 'UFL',
  college_baseball: 'College Baseball',
}

const SPORT_ICONS = {
  mlb: '⚾',
  nhl: '🏒',
  nba: '🏀',
  basketball: '🏀',
  hockey: '🏒',
  tennis: '🎾',
  afl: '🏉',
  f1: '🏎',
  golf: '⛳',
  soccer: '⚽',
  ufl: '🏈',
  college_baseball: '⚾',
}

// Set of league keys that count as "basketball" — used to keep the
// Basketball entry highlighted when any league inside is selected.
// AFL is excluded explicitly: it lives in the basketball backend
// framework but renders as its own top-level sidebar entry, and
// would otherwise auto-expand the Basketball group when clicked.
function isBasketballLeague(key, leagues) {
  if (key === 'afl') return false
  if (!leagues) return key === 'nba'
  return leagues.some(L => L.key === key)
}

function isHockeyLeague(key, leagues) {
  if (!leagues) return key === 'nhl'
  return leagues.some(L => L.key === key)
}

function isSoccerLeague(key, leagues) {
  if (!leagues) return key === 'soccer'
  return leagues.some(L => L.key === key)
}

function isMotorsportsSeries(key, series) {
  // 'f1' acts as the legacy standalone key; treat it as "in motorsports"
  // when the registry is loaded so the group auto-expands.
  if (!series) return key === 'f1'
  return series.some(S => S.key === key)
}

function isBaseballLeague(key, leagues) {
  // 'mlb' acts as the legacy standalone key — treat it as part of
  // baseball whenever the group is loaded so the group auto-expands.
  if (!leagues) return key === 'mlb'
  return leagues.some(L => L.key === key)
}

function SidebarImpl({
  sports, selected, onSelect, gameCounts = {}, queueCount = 0,
  basketballLeagues = null, hockeyLeagues = null, soccerLeagues = null,
  motorsportsSeries = null, baseballLeagues = null,
}) {
  const ranked = sortSports(sports, gameCounts)
  const [open, setOpen] = useState(false)

  // Auto-expand Basketball when any basketball league is selected so the
  // user can see where their selection lives in the hierarchy.
  const initiallyExpanded = isBasketballLeague(selected, basketballLeagues)
  const [bbExpanded, setBbExpanded] = useState(initiallyExpanded)
  useEffect(() => {
    if (isBasketballLeague(selected, basketballLeagues)) setBbExpanded(true)
  }, [selected, basketballLeagues])

  // Same pattern for Hockey — auto-expand when AHL/PWHL/NHL is selected.
  const hockeyInitiallyExpanded = isHockeyLeague(selected, hockeyLeagues)
  const [hkExpanded, setHkExpanded] = useState(hockeyInitiallyExpanded)
  useEffect(() => {
    if (isHockeyLeague(selected, hockeyLeagues)) setHkExpanded(true)
  }, [selected, hockeyLeagues])

  // Soccer — auto-expand when any soccer league is selected.
  const soccerInitiallyExpanded = isSoccerLeague(selected, soccerLeagues)
  const [soccerExpanded, setSoccerExpanded] = useState(soccerInitiallyExpanded)
  useEffect(() => {
    if (isSoccerLeague(selected, soccerLeagues)) setSoccerExpanded(true)
  }, [selected, soccerLeagues])

  // Motorsports — auto-expand when F1 / IndyCar / NASCAR is selected.
  const msInitiallyExpanded = isMotorsportsSeries(selected, motorsportsSeries)
  const [msExpanded, setMsExpanded] = useState(msInitiallyExpanded)
  useEffect(() => {
    if (isMotorsportsSeries(selected, motorsportsSeries)) setMsExpanded(true)
  }, [selected, motorsportsSeries])

  // Baseball — auto-expand when MLB or any baseball-framework league
  // is selected. Same pattern as the other groups.
  const baseballInitiallyExpanded = isBaseballLeague(selected, baseballLeagues)
  const [baseballExpanded, setBaseballExpanded] = useState(baseballInitiallyExpanded)
  useEffect(() => {
    if (isBaseballLeague(selected, baseballLeagues)) setBaseballExpanded(true)
  }, [selected, baseballLeagues])

  // Close on Esc and when viewport widens to md+
  useEffect(() => {
    if (!open) return
    const onKey = (e) => { if (e.key === 'Escape') setOpen(false) }
    const onResize = () => { if (window.innerWidth >= 768) setOpen(false) }
    window.addEventListener('keydown', onKey)
    window.addEventListener('resize', onResize)
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
    setOpen(false)
  }

  // Today's basketball game count — used as the badge on the
  // collapsed Basketball entry. Sums every league's today count when
  // we have the registry; falls back to the NBA-only count otherwise.
  const basketballGameCount = basketballLeagues
    ? basketballLeagues.reduce((s, L) => s + (L.game_count_today || 0), 0)
    : (gameCounts.basketball ?? gameCounts.nba ?? 0)
  const hockeyGameCount = hockeyLeagues
    ? hockeyLeagues.reduce((s, L) => s + (L.game_count_today || 0), 0)
    : (gameCounts.hockey ?? gameCounts.nhl ?? 0)
  const baseballGameCount = baseballLeagues
    ? baseballLeagues.reduce((s, L) => s + (L.game_count_today || 0), 0)
    : (gameCounts.baseball ?? gameCounts.mlb ?? 0)
  // Soccer rolls up every competition's today-count into the parent
  // entry badge — same pattern as Basketball + Hockey groups.
  const soccerGameCount = soccerLeagues
    ? soccerLeagues.reduce((s, L) => s + (L.game_count_today || 0), 0)
    : (gameCounts.soccer ?? 0)

  return (
    <>
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
          'md:sticky md:top-0 md:translate-x-0',
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
          <div className="mb-2 px-3">
            <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              Overview
            </div>
          </div>

          <button
            onClick={() => handleSelect('dashboard')}
            className={cn(
              'flex w-full items-center justify-between rounded-md px-3 py-2.5 text-sm font-medium transition-colors',
              'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
              selected === 'dashboard'
                ? 'bg-primary text-primary-foreground'
                : 'text-foreground hover:bg-accent hover:text-accent-foreground',
            )}
          >
            <span className="flex items-center gap-2.5">
              <LayoutDashboard className="h-4 w-4" aria-hidden="true" />
              <span>Dashboard</span>
            </span>
          </button>

          <button
            onClick={() => handleSelect('portfolio')}
            className={cn(
              'flex w-full items-center justify-between rounded-md px-3 py-2.5 text-sm font-medium transition-colors',
              'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
              selected === 'portfolio'
                ? 'bg-primary text-primary-foreground'
                : 'text-foreground hover:bg-accent hover:text-accent-foreground',
            )}
          >
            <span className="flex items-center gap-2.5">
              <span className="text-base leading-none" aria-hidden="true">📊</span>
              <span>Portfolio</span>
            </span>
          </button>

          <button
            onClick={() => handleSelect('queue')}
            className={cn(
              'flex w-full items-center justify-between rounded-md px-3 py-2.5 text-sm font-medium transition-colors',
              'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
              selected === 'queue'
                ? 'bg-primary text-primary-foreground'
                : 'text-foreground hover:bg-accent hover:text-accent-foreground',
            )}
          >
            <span className="flex items-center gap-2.5">
              <span className="text-base leading-none" aria-hidden="true">🎯</span>
              <span>Bet Queue</span>
            </span>
            {queueCount > 0 && (
              <span
                className={cn(
                  'rounded-full px-1.5 py-0.5 text-[10px] font-bold tabular-nums',
                  selected === 'queue'
                    ? 'bg-primary-foreground/20 text-primary-foreground'
                    : 'bg-positive/20 text-positive',
                )}
                title={`${queueCount} pick${queueCount === 1 ? '' : 's'} ready`}
              >
                {queueCount}
              </span>
            )}
          </button>

          <div className="mb-2 mt-4 px-3">
            <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              Sports
            </div>
          </div>

          {ranked.map(({ sport, gameCount, inSeason }) => {
            // Basketball expansion: replace the 'nba' entry with a
            // collapsible Basketball group when leagues are loaded.
            if (sport === 'nba' && basketballLeagues) {
              return (
                <BasketballGroup
                  key="basketball"
                  expanded={bbExpanded}
                  onToggleExpanded={() => setBbExpanded(v => !v)}
                  selected={selected}
                  onSelect={handleSelect}
                  leagues={basketballLeagues}
                  gameCount={basketballGameCount}
                />
              )
            }
            // Hockey expansion: same pattern as basketball — replace the
            // standalone NHL entry with a Hockey group when AHL/PWHL
            // come online via /api/hockey/leagues.
            if (sport === 'nhl' && hockeyLeagues && hockeyLeagues.length > 1) {
              return (
                <HockeyGroup
                  key="hockey"
                  expanded={hkExpanded}
                  onToggleExpanded={() => setHkExpanded(v => !v)}
                  selected={selected}
                  onSelect={handleSelect}
                  leagues={hockeyLeagues}
                  gameCount={hockeyGameCount}
                />
              )
            }
            // Baseball expansion: replace the standalone MLB entry
            // with a Baseball group containing MLB + College Baseball
            // (and future KBO/NPB). MLB stays on its dedicated heavy
            // path; baseball-framework leagues route through
            // /api/baseball/<league>/today.
            if (sport === 'mlb' && baseballLeagues && baseballLeagues.length > 1) {
              return (
                <BaseballGroup
                  key="baseball"
                  expanded={baseballExpanded}
                  onToggleExpanded={() => setBaseballExpanded(v => !v)}
                  selected={selected}
                  onSelect={handleSelect}
                  leagues={baseballLeagues}
                  gameCount={baseballGameCount}
                />
              )
            }
            // Suppress the standalone college_baseball entry — it's
            // rendered inside the Baseball group above.
            if (sport === 'college_baseball' && baseballLeagues
                && baseballLeagues.length > 1) {
              return null
            }
            // Soccer expansion: replace the flat 'soccer' entry with a
            // collapsible group that lists every competition (MLS,
            // Premier League, La Liga, Champions League, WC, …),
            // grouped by confederation.
            if (sport === 'soccer' && soccerLeagues && soccerLeagues.length > 0) {
              return (
                <SoccerGroup
                  key="soccer"
                  expanded={soccerExpanded}
                  onToggleExpanded={() => setSoccerExpanded(v => !v)}
                  selected={selected}
                  onSelect={handleSelect}
                  leagues={soccerLeagues}
                  gameCount={soccerGameCount}
                />
              )
            }
            // Motorsports expansion: replace the standalone 'f1' entry
            // with a Motorsports group covering F1 / IndyCar / NASCAR.
            // IndyCar + NASCAR ship as pending_data until their ingest
            // pipelines land — they render dimmed with an "off" tag.
            if (sport === 'f1' && motorsportsSeries && motorsportsSeries.length > 0) {
              return (
                <MotorsportsGroup
                  key="motorsports"
                  expanded={msExpanded}
                  onToggleExpanded={() => setMsExpanded(v => !v)}
                  selected={selected}
                  onSelect={handleSelect}
                  series={motorsportsSeries}
                  gameCount={gameCounts.f1 || 0}
                />
              )
            }
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
                    {/* F1 races weekly, so showing "13" without a unit
                        looks like a games-today count and misleads. The
                        '13d' suffix makes the count read as days-until-
                        race, matching what the F1 panel shows. */}
                    {sport === 'f1' ? `${gameCount}d` : gameCount}
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
