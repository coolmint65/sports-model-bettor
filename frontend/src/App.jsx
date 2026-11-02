import { useState, useEffect, useCallback, useMemo } from 'react'
import axios from 'axios'
import { useOnce, usePoll } from './lib/useAppFetch'
import { useSportSlate } from './lib/useSportSlate'
import Scoreboard from './components/Scoreboard'
import Standings from './components/Standings'
import Backtest from './components/Backtest'
import PickHistory from './components/PickHistory'
import NHLStandings from './components/NHLStandings'
import NBAStandings from './components/NBAStandings'
import BetsView from './components/BetsView'
import TrackerView from './components/TrackerView'
import Sidebar from './components/Sidebar'
import SubNav from './components/SubNav'
import RootDashboard from './components/RootDashboard'
import PortfolioTracker from './components/PortfolioTracker'
import UnifiedTracker from './components/UnifiedTracker'
import PotdPanel from './components/PotdPanel'
import TennisPanel from './components/TennisPanel'
import CalibrationPanel from './components/CalibrationPanel'
import BasketballPanel from './components/BasketballPanel'
import HockeyPanel from './components/HockeyPanel'
import SoccerPanel from './components/SoccerPanel'
import FootballPanel from './components/FootballPanel'
import PanelErrorBoundary from './components/PanelErrorBoundary'
import ArbPanel from './components/ArbPanel'
import PublicBettingPanel from './components/PublicBettingPanel'
import HorsesPanel from './components/HorsesPanel'

const api = axios.create({ baseURL: '/api' })

// Per-sport sub-nav. Phase-2 polish 2026-04-28: Props / Derivatives /
// 1st Inn collapsed under Bets via the in-tab MarketToggle. Bets is
// the single "what should I bet today" surface; Tracker is the single
// "how have my bets done" surface; Standings is league context. Live
// reserves a slot for Phase 3.
const SPORT_TABS = [
  { id: 'bets',         label: 'Bets' },
  { id: 'history',      label: 'Tracker' },
  { id: 'calibration',  label: 'Calibration' },
  { id: 'standings',    label: 'Standings' },
]

export default function App() {
  const [league, setLeague] = useState('NHL')   // was 'MLB'; MLB off for the off-season 2026-10-29
  // 'dashboard' is the cross-sport root view; otherwise this is one
  // of SPORT_TABS ids inside the active sport's landing.
  const [view, setView] = useState('bets')
  // Sidebar surface: 'dashboard' or a sport key. Tracks separately from
  // `league` so the dashboard view doesn't lose the user's last sport.
  // Defaults to 'potd'. (Was 'queue' -- the Bet Queue / auto-placer was
  // removed 2026-10-26, Austin "get rid of the bet queue".)
  const [surface, setSurface] = useState('potd')

  // Per-sport slate + best-bets loaders. Each hook fetches on mount,
  // polls /best-bets/progress for the "Computing 8/15 games..." spinner
  // label, and auto-refreshes every 2 min. See engine/live/... for what
  // the progress endpoint reports.
  // MLB OFF 2026-10-29 (Austin: "MLB is officially over so lets turn it off"): no slate/best-bets
  // fetches and no sidebar tab. Revive = enabled true + 'mlb' back in the Sidebar/RootDashboard lists.
  const mlbSlate = useSportSlate(api, '', false)
  const nhlSlate = useSportSlate(api, '/nhl')
  const nbaSlate = useSportSlate(api, '/nba')
  const {
    games, bestBets, bbProgress, loading: gamesLoading,
    setGames, setBestBets,
  } = mlbSlate
  const {
    games: nhlGames, bestBets: nhlBestBets, bbProgress: nhlBbProgress,
    loading: nhlLoading, setGames: setNhlGames, setBestBets: setNhlBestBets,
  } = nhlSlate
  const {
    games: nbaGames, bestBets: nbaBestBets, bbProgress: nbaBbProgress,
    loading: nbaLoading, setGames: setNbaGames, setBestBets: setNbaBestBets,
  } = nbaSlate

  // Sub-view state that isn't part of the shared slate/best-bets hook —
  // tracker slices — stays local per sport.
  const [standings, setStandings] = useState([])
  const [backtest, setBacktest] = useState(null)
  const [btLoading, setBtLoading] = useState(false)
  const [bbLoading, setBbLoading] = useState(false)
  const [pickSummary, setPickSummary] = useState(null)
  const [pickHistory, setPickHistory] = useState(null)
  const [phLoading, setPhLoading] = useState(false)
  // MLB PAPER (is_shadow) for the Tracker tab's "Paper Picks - shadow"
  // section -- reads the EXISTING /api/tracker/paper `full` scope so MLB
  // reaches parity with NHL/WNBA. Recording is untouched. (Austin 2026-09-22)
  const [mlbPaper, setMlbPaper] = useState(null)

  const [nhlBbLoading, setNhlBbLoading] = useState(false)
  const [nhlStandings, setNhlStandings] = useState([])
  const [nhlStandingsLoading, setNhlStandingsLoading] = useState(false)
  const [nhlPickSummary, setNhlPickSummary] = useState(null)
  const [nhlPickHistory, setNhlPickHistory] = useState(null)
  const [nhlPhLoading, setNhlPhLoading] = useState(false)

  const [nbaBbLoading, setNbaBbLoading] = useState(false)
  const [nbaStandings, setNbaStandings] = useState([])
  const [nbaStandingsLoading, setNbaStandingsLoading] = useState(false)
  const [nbaPickSummary, setNbaPickSummary] = useState(null)
  const [nbaPickHistory, setNbaPickHistory] = useState(null)
  const [nbaPaperTracker, setNbaPaperTracker] = useState(null)
  const [nbaPhLoading, setNbaPhLoading] = useState(false)

  // Per-sport league / series registries drive the Sidebar's
  // expandable groups. Each is a light "leagues[]" fetch that runs
  // once on mount; per-league in-season + game-count fields are
  // populated by the individual league panels as they hydrate.
  const [basketballLeagues, refreshBasketballLeagues] =
    useOnce(api, '/basketball/leagues', r => r.data?.leagues || [], [])
  const [hockeyLeagues, refreshHockeyLeagues] =
    useOnce(api, '/hockey/leagues', r => r.data?.leagues || [], [])
  const [soccerLeagues, refreshSoccerLeagues] =
    useOnce(api, '/soccer/leagues', r => r.data?.leagues || [], [])
  const [footballLeagues] =
    useOnce(api, '/football/leagues', r => r.data?.leagues || [], [])


  // Lightweight sidebar-badge counts — cheap endpoints that count
  // open picks / days-until-race etc. so the group icons carry a
  // live number without kicking off a heavy slate build.
  const tennisCount = usePoll(
    api, '/tennis/scheduled/count', r => r.data?.count ?? 0,
    5 * 60 * 1000, 0,
  )



  const showStandings = useCallback(() => {
    setView('standings')
    if (league === 'MLB' && standings.length === 0) {
      api.get('/standings').then(r => setStandings(r.data)).catch(() => {})
    }
    if (league === 'NHL' && nhlStandings.length === 0) {
      setNhlStandingsLoading(true)
      api.get('/nhl/standings')
        .then(r => setNhlStandings(r.data))
        .catch(() => {})
        .finally(() => setNhlStandingsLoading(false))
    }
    if (league === 'NBA' && nbaStandings.length === 0) {
      setNbaStandingsLoading(true)
      api.get('/nba/standings')
        .then(r => setNbaStandings(Array.isArray(r.data) ? r.data : []))
        .catch(() => {})
        .finally(() => setNbaStandingsLoading(false))
    }
  }, [league, standings.length, nhlStandings.length, nbaStandings.length])

  const showBacktest = useCallback(() => {
    setView('backtest')
  }, [])

  // Internal: load tracker history into state for the active league.
  // Keeps the `showHistory` legacy entry point working and lets the
  // new Tracker/History sub-nav tabs share one data fetch. The `view`
  // string is set by the caller (selectView) so this loader doesn't
  // care which tab actually renders the result.
  const loadTrackerHistory = useCallback(() => {
    if (league === 'MLB') {
      setPhLoading(true)
      Promise.all([
        api.get('/tracker/summary'),
        api.get('/tracker/history'),
        api.get('/tracker/paper'),
      ]).then(([s, h, p]) => {
        setPickSummary(s.data)
        setPickHistory(h.data)
        setMlbPaper(p.data)
      }).catch(() => {})
        .finally(() => setPhLoading(false))
    } else if (league === 'NHL') {
      setNhlPhLoading(true)
      Promise.all([
        api.get('/nhl/tracker/summary'),
        api.get('/nhl/tracker/history'),
      ]).then(([s, h]) => {
        setNhlPickSummary(s.data)
        setNhlPickHistory(h.data)
      }).catch(() => {})
        .finally(() => setNhlPhLoading(false))
    } else if (league === 'NBA') {
      setNbaPhLoading(true)
      Promise.all([
        api.get('/nba/tracker/summary'),
        api.get('/nba/tracker/history'),
      ]).then(([s, h]) => {
        setNbaPickSummary(s.data)
        setNbaPickHistory(Array.isArray(h.data) ? h.data : [])
      }).catch(() => {})
        .finally(() => setNbaPhLoading(false))
    }
  }, [league])

  // NBA paper-only Tracker (Austin 2026-10-04 "gone entirely"): load the PAPER
  // tracker (/nba/tracker/paper {summary:{overall,by_type},rows}) + render it in
  // the rich style. The active tracker is gone; active recording is gated off
  // server-side (ENABLE_NBA_ACTIVE_RECORDING=False).
  useEffect(() => {
    if (league !== 'NBA') return
    let alive = true
    api.get('/nba/tracker/paper').then((p) => {
      if (!alive) return
      const d = p.data || {}
      setNbaPaperTracker({
        summary: d.summary || null,
        rows: (d.rows || []).map(r => {
          const bt = String(r.bet_type || '').toUpperCase()
          const pk = bt.includes('TOTAL') ? `${r.pick} ${r.line ?? ''}`.trim()
            : (bt === 'SPREAD' || bt === 'RUN_LINE' || bt === 'RL' || bt === 'PL') && r.line != null
              ? `${r.pick} ${r.line > 0 ? '+' : ''}${r.line}` : r.pick
          return { ...r, type: r.bet_type, prob: r.model_prob, pick: pk }
        }),
      })
    }).catch(() => {})
    return () => { alive = false }
  }, [league])

  const runBacktest = useCallback((days, minEdge, season) => {
    setBtLoading(true); setBacktest(null)
    const params = new URLSearchParams()
    if (days) params.set('days', days)
    if (minEdge) params.set('min_edge', minEdge)
    if (season) params.set('season', season)
    const endpoint = league === 'NHL' ? '/nhl/backtest' : league === 'NBA' ? '/nba/backtest' : '/backtest'
    api.get(`${endpoint}?${params}`)
      .then(r => setBacktest(r.data))
      .catch(() => setBacktest({ error: "Backtest failed. Try again." }))
      .finally(() => setBtLoading(false))
  }, [league])


  const showHistory = useCallback(() => {
    setView('history')
    if (league === 'MLB') {
      setPhLoading(true)
      Promise.all([
        api.get('/tracker/summary'),
        api.get('/tracker/history'),
        api.get('/tracker/paper'),
      ]).then(([s, h, p]) => {
        setPickSummary(s.data)
        setPickHistory(h.data)
        setMlbPaper(p.data)
      }).catch(() => {})
        .finally(() => setPhLoading(false))
    } else if (league === 'NHL') {
      setNhlPhLoading(true)
      Promise.all([
        api.get('/nhl/tracker/summary'),
        api.get('/nhl/tracker/history'),
      ]).then(([s, h]) => {
        setNhlPickSummary(s.data)
        setNhlPickHistory(h.data)
      }).catch(() => {})
        .finally(() => setNhlPhLoading(false))
    } else if (league === 'NBA') {
      setNbaPhLoading(true)
      Promise.all([
        api.get('/nba/tracker/summary'),
        api.get('/nba/tracker/history'),
      ]).then(([s, h]) => {
        setNbaPickSummary(s.data)
        setNbaPickHistory(Array.isArray(h.data) ? h.data : [])
      }).catch(() => {})
        .finally(() => setNbaPhLoading(false))
    }
  }, [league])

  const recordPicks = useCallback(() => {
    if (league === 'MLB') {
      api.post('/tracker/record').then(() => {
        api.get('/tracker/summary').then(r => setPickSummary(r.data))
        api.get('/tracker/history').then(r => setPickHistory(r.data))
        api.get('/tracker/paper').then(r => setMlbPaper(r.data))
      })
    } else if (league === 'NHL') {
      api.post('/nhl/tracker/record').then(() => {
        api.get('/nhl/tracker/summary').then(r => setNhlPickSummary(r.data))
        api.get('/nhl/tracker/history').then(r => setNhlPickHistory(r.data))
      })
    } else if (league === 'NBA') {
      api.post('/nba/tracker/record').then(() => {
        api.get('/nba/tracker/summary').then(r => setNbaPickSummary(r.data))
        api.get('/nba/tracker/history').then(r => setNbaPickHistory(r.data))
      })
    }
  }, [league])

  const settlePicks = useCallback(() => {
    if (league === 'MLB') {
      api.post('/tracker/settle').then(() => {
        api.get('/tracker/summary').then(r => setPickSummary(r.data))
        api.get('/tracker/history').then(r => setPickHistory(r.data))
        api.get('/tracker/paper').then(r => setMlbPaper(r.data))
      })
    } else if (league === 'NHL') {
      api.post('/nhl/tracker/settle').then(() => {
        api.get('/nhl/tracker/summary').then(r => setNhlPickSummary(r.data))
        api.get('/nhl/tracker/history').then(r => setNhlPickHistory(r.data))
      })
    } else if (league === 'NBA') {
      api.post('/nba/tracker/settle').then(() => {
        api.get('/nba/tracker/summary').then(r => setNbaPickSummary(r.data))
        api.get('/nba/tracker/history').then(r => setNbaPickHistory(r.data))
      })
    }
  }, [league])

  const switchLeague = useCallback((l) => {
    setLeague(l)
    setSurface(l.toLowerCase())
    setView('bets')
  }, [])

  // Sidebar entry point — dispatches to dashboard surface OR a sport.
  // Kept separate from switchLeague so the dashboard view doesn't have
  // to fake a sport key just to satisfy switchLeague's signature.
  //
  // Basketball league keys (wnba / ncaam / euroleague / china_cba / ...)
  // route to a virtual league 'BASKETBALL' with the actual league key
  // stored separately so BasketballPanel can render the right league.
  // 'nba' keeps its legacy path (league='NBA') so existing NBA UI is
  // untouched.
  const [basketballLeague, setBasketballLeague] = useState(null)
  const [hockeyLeague, setHockeyLeague] = useState(null)
  const [soccerLeague, setSoccerLeague] = useState(null)
  const handleSidebarSelect = useCallback((id) => {
    if (id === 'dashboard') {
      setSurface('dashboard')
      return
    }
    if (id === 'portfolio') {
      setSurface('portfolio')
      return
    }
    if (id === 'potd') {
      setSurface('potd')
      return
    }
    if (id === 'arb') {
      setSurface('arb')
      return
    }
    if (id === 'public') {
      setSurface('public')
      return
    }
    if (id === 'horses') {
      setSurface('horses')
      return
    }
    // AFL — checked BEFORE the generic basketball-league branch since
    // AFL lives in the basketball registry (same ESPN ingest + math)
    // but rides as its own top-level sidebar entry. Without this
    // ordering, the basketball check would swallow it under
    // league='BASKETBALL' and the panel header would say "Basketball".
    if (id === 'afl') {
      setBasketballLeague('afl')
      setHockeyLeague(null)
      setSoccerLeague(null)
      switchLeague('AUSSIE_RULES')
      refreshBasketballLeagues()
      return
    }
    // Basketball-league key (anything in basketballLeagues that isn't 'nba' / 'afl')
    const isBasketballLeague = basketballLeagues
      && basketballLeagues.some(L => L.key === id)
      && id !== 'nba' && id !== 'afl'
    if (isBasketballLeague) {
      setBasketballLeague(id)
      setHockeyLeague(null)
      setSoccerLeague(null)
      switchLeague('BASKETBALL')
      refreshBasketballLeagues()
      return
    }
    // Hockey-league key (AHL, PWHL — anything in hockeyLeagues that isn't 'nhl').
    const isHockeyFrameworkLeague = hockeyLeagues
      && hockeyLeagues.some(L => L.key === id)
      && id !== 'nhl'
    if (isHockeyFrameworkLeague) {
      setHockeyLeague(id)
      setBasketballLeague(null)
      setSoccerLeague(null)
      switchLeague('HOCKEY')
      refreshHockeyLeagues()
      return
    }
    // Soccer-league key (MLS, Premier League, Champions League, WC, …)
    // — anything in soccerLeagues. There's no parent "soccer-parent"
    // sport; the user navigates straight to a league.
    const isSoccerLeague = soccerLeagues
      && soccerLeagues.some(L => L.key === id)
    if (isSoccerLeague) {
      setSoccerLeague(id)
      setBasketballLeague(null)
      setHockeyLeague(null)
      switchLeague('SOCCER')
      refreshSoccerLeagues()
      return
    }
    setBasketballLeague(null)
    setHockeyLeague(null)
    setSoccerLeague(null)
    switchLeague(id.toUpperCase())
  }, [switchLeague, basketballLeagues, hockeyLeagues, soccerLeagues,
       refreshBasketballLeagues, refreshHockeyLeagues, refreshSoccerLeagues])

  // Sub-nav tab dispatch — sets the view AND fires lazy data loaders.
  // Bets/Props don't need a loader (Bets is default; Props is a
  // placeholder until 2g+). Standings/Tracker/History/Backtest each
  // hit their respective endpoints on first visit.
  const selectView = useCallback((tabId) => {
    setView(tabId)
    if (tabId === 'standings') {
      // Reuse showStandings's data fetch but skip its setView (which
      // would override the new tab id back to 'standings'; that's
      // harmless because both names are now the same, but keep the
      // dispatcher authoritative).
      if (league === 'MLB' && standings.length === 0) {
        api.get('/standings').then(r => setStandings(r.data)).catch(() => {})
      } else if (league === 'NHL' && nhlStandings.length === 0) {
        setNhlStandingsLoading(true)
        api.get('/nhl/standings').then(r => setNhlStandings(r.data)).catch(() => {}).finally(() => setNhlStandingsLoading(false))
      } else if (league === 'NBA' && nbaStandings.length === 0) {
        setNbaStandingsLoading(true)
        api.get('/nba/standings').then(r => setNbaStandings(Array.isArray(r.data) ? r.data : [])).catch(() => {}).finally(() => setNbaStandingsLoading(false))
      }
    } else if (tabId === 'tracker' || tabId === 'history') {
      loadTrackerHistory()
    }
    // 'backtest' has no auto-load — Backtest component fires its own
    // request when the user clicks Run.
  }, [league, standings.length, nhlStandings.length, nbaStandings.length, loadTrackerHistory])

  const isMLB = league === 'MLB'
  const isNHL = league === 'NHL'
  const isNBA = league === 'NBA'
  const isTennis = league === 'TENNIS'
  const isBasketball = league === 'BASKETBALL'
  const isHockey = league === 'HOCKEY'
  const isAussieRules = league === 'AUSSIE_RULES'
  const isSoccer = league === 'SOCCER'    // multi-league soccer framework
                                          // when v2 series ship
  const isUFL = league === 'UFL'
  const isNFL = league === 'NFL'
  const isCFB = league === 'CFB'

  // Game counts for sidebar badge — pass length of each scoreboard so
  // the "where's the action tonight" hint is current. Passing 0 hides
  // the badge instead of showing "0".
  // AFL count comes from basketballLeagues.game_count_today (the
  // /api/basketball/leagues registry already reports it per-league).
  const aflCount = (basketballLeagues || [])
    .find(L => L.key === 'afl')?.game_count_today ?? 0

  // Per-sport top-level sidebar badges. Reads game_count_today from
  // the per-sport leagues endpoint where present. UFL
  // were missing from the dict so their sidebar entries silently never
  // showed the today's-game badge (user flagged 2026-05-29).
  const uflCount = (footballLeagues || [])
    .find(L => L.key === 'ufl')?.game_count_today ?? 0
  const nflCount = (footballLeagues || [])
    .find(L => L.key === 'nfl')?.game_count_today ?? 0
  const cfbCount = (footballLeagues || [])
    .find(L => L.key === 'cfb')?.game_count_today ?? 0
  const gameCounts = {
    mlb: games.length,
    nhl: nhlGames.length,
    nba: nbaGames.length,
    tennis: tennisCount,
    afl: aflCount,
    ufl: uflCount,
    nfl: nflCount,
    cfb: cfbCount,
  }


  return (
    <div className="flex min-h-screen bg-background">
      <Sidebar
        sports={['nhl', 'nba', 'nfl', 'cfb', 'soccer']}  // mlb off 2026-10-29 (off-season)  // soccer re-added 2026-10-11 (EPL un-killed); renders as its own top-level league tab via Sidebar when in-season
        selected={
          surface === 'dashboard' ? 'dashboard'
          : surface === 'portfolio' ? 'portfolio'
          : surface === 'potd' ? 'potd'
          : surface === 'arb' ? 'arb'
          : surface === 'public' ? 'public'
          : surface === 'horses' ? 'horses'
          : league === 'AUSSIE_RULES' ? 'afl'
          : league === 'SOCCER' ? (soccerLeague || 'soccer')
          : league === 'BASKETBALL' ? (basketballLeague || 'nba')
          : league === 'HOCKEY' ? (hockeyLeague || 'nhl')
          : league.toLowerCase()
        }
        onSelect={handleSidebarSelect}
        gameCounts={gameCounts}
        basketballLeagues={basketballLeagues}   // the registry IS the list now (smaller leagues removed 2026-10-27; China CBA kept + shown)
        hockeyLeagues={hockeyLeagues.filter(L => L.key === 'nhl')}
        soccerLeagues={soccerLeagues || []}
      />

      <main className="flex-1 min-w-0 overflow-x-hidden pt-14 md:pt-0">
       {surface === 'dashboard' ? (
         <div className="app">
           <RootDashboard
             sports={['nhl', 'nba']}
             bestBetsBySport={{ mlb: bestBets || [], nhl: nhlBestBets || [], nba: nbaBestBets || [] }}
             onSelectSport={s => { switchLeague(s.toUpperCase()) }}
           />
         </div>
       ) : surface === 'portfolio' ? (
         <div className="app">
           <PortfolioTracker />
         </div>
       ) : surface === 'potd' ? (
         <PanelErrorBoundary>
           <PotdPanel api={api} />
         </PanelErrorBoundary>
              ) : surface === 'arb' ? (
         <div className="app">
           <PanelErrorBoundary>
             <ArbPanel api={api} />
           </PanelErrorBoundary>
         </div>
              ) : surface === 'public' ? (
         <div className="app">
           <PanelErrorBoundary>
             <PublicBettingPanel api={api} />
           </PanelErrorBoundary>
         </div>
              ) : surface === 'horses' ? (
         <div className="app">
           <PanelErrorBoundary>
             <HorsesPanel api={api} />
           </PanelErrorBoundary>
         </div>
) : (
       <div className="app">
        <div className="header">
          <h1>{league.replace(/_/g, ' ')}</h1>
          <p className="subtitle">Data-driven {league.replace(/_/g, ' ')} predictions</p>
        </div>

      {/* Tennis is a single-tab tab — bypasses SubNav since the
          team-sport view system (bets / standings / history) doesn't
          map cleanly onto match-by-match tennis. TennisPanel handles
          its own internal sub-views. */}
      {isTennis && <TennisPanel api={api} />}

      {/* Basketball framework leagues (WNBA / NCAAM / Euroleague /
          international). NBA stays on its existing isNBA path; this
          renders only when the user picks a non-NBA basketball league
          from the nested sidebar. */}
      {isBasketball && (
        <BasketballPanel
          api={api}
          leagueKey={basketballLeague}
          leagues={basketballLeagues}
        />
      )}

      {/* Aussie Rules — hosted on the basketball framework backend
          (same ESPN ingest + 4-quarter linescore shape) but rendered
          via BasketballPanel with leagueKey='afl'. Top-level sidebar
          entry; same card chrome the user already knows. */}
      {isAussieRules && (
        <BasketballPanel
          api={api}
          leagueKey="afl"
          leagues={basketballLeagues}
        />
      )}

      {/* Hockey framework leagues (AHL, PWHL). NHL stays on its existing
          isNHL path; HockeyPanel renders only for non-NHL hockey leagues. */}
      {isHockey && (
        <HockeyPanel
          api={api}
          leagueKey={hockeyLeague}
          leagues={hockeyLeagues}
        />
      )}



      {/* Soccer — multi-league framework (MLS / Big-5 / CONMEBOL / WC).
          Mirror of BasketballPanel/HockeyPanel: sidebar holds every
          league as a nested entry, panel renders one league at a time
          via leagueKey + leagues props. */}
      {isSoccer && (
        <SoccerPanel
          api={api}
          leagueKey={soccerLeague}
          leagues={soccerLeagues}
        />
      )}

      {/* UFL — spring football. Single-league for now; framework is
          ready to register NFL/NCAAF later without code forks. Polish
          parity rides on the queued frontend overhaul (#484). */}
      {isUFL && <FootballPanel api={api} leagueKey="ufl" />}
      {isNFL && <FootballPanel api={api} leagueKey="nfl" />}
      {isCFB && <FootballPanel api={api} leagueKey="cfb" />}


      {!isTennis && !isBasketball && !isHockey && !isAussieRules && !isSoccer && !isUFL && !isNFL && !isCFB && (
        <SubNav
          tabs={SPORT_TABS.filter(t => !t.nhlOnly || isNHL)}
          active={view}
          onChange={selectView}
        />
      )}

      {/* ── MLB Views ── */}
      {isMLB && view === 'bets' && (
        <BetsView
          sport="mlb"
          api={api}
          data={{
            games,
            loading: gamesLoading,
            progress: bbProgress,
            bestBets,
          }}
        />
      )}

      {isMLB && view === 'standings' && <Standings divisions={standings} />}

      {isMLB && view === 'calibration' && <CalibrationPanel sport="mlb" api={api} />}

      {isMLB && view === 'history' && (
        <UnifiedTracker sport="mlb" api={api} />
      )}


      {/* ── NHL Views ── */}
      {isNHL && view === 'bets' && (
        <BetsView
          sport="nhl"
          api={api}
          data={{
            games: nhlGames,
            loading: nhlLoading,
            progress: nhlBbProgress,
            bestBets: nhlBestBets,
          }}
        />
      )}

      {isNHL && view === 'standings' && <NHLStandings divisions={nhlStandings} loading={nhlStandingsLoading} />}


      {isNHL && view === 'calibration' && <CalibrationPanel sport="nhl" api={api} />}

      {isNHL && view === 'history' && (
        <UnifiedTracker sport="nhl" api={api} />
      )}


      {/* ── NBA Views ── */}
      {isNBA && view === 'bets' && (
        <BetsView
          sport="nba"
          api={api}
          data={{
            games: nbaGames,
            loading: nbaLoading,
            progress: nbaBbProgress,
            bestBets: nbaBestBets,
          }}
        />
      )}

      {isNBA && view === 'standings' && <NBAStandings divisions={nbaStandings} loading={nbaStandingsLoading} />}

      {isNBA && view === 'calibration' && <CalibrationPanel sport="nba" api={api} />}

      {isNBA && view === 'history' && (
        <UnifiedTracker sport="nba" api={api} />
      )}

       </div>
       )}
      </main>
    </div>
  )
}

