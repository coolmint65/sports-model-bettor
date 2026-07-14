import { useState, useEffect, useCallback, useMemo } from 'react'
import axios from 'axios'
import { useOnce, usePoll } from './lib/useAppFetch'
import { useSportSlate } from './lib/useSportSlate'
import Scoreboard from './components/Scoreboard'
import GameDetail from './components/GameDetail'
import Standings from './components/Standings'
import Backtest from './components/Backtest'
import PickHistory from './components/PickHistory'
import NHLStandings from './components/NHLStandings'
import NHLGameDetail from './components/NHLGameDetail'
import NBAStandings from './components/NBAStandings'
import NBAGameDetail from './components/NBAGameDetail'
import BetsView from './components/BetsView'
import TrackerView from './components/TrackerView'
import Sidebar from './components/Sidebar'
import SubNav from './components/SubNav'
import RootDashboard from './components/RootDashboard'
import PortfolioTracker from './components/PortfolioTracker'
import QueuePanel from './components/QueuePanel'
import TennisPanel from './components/TennisPanel'
import CalibrationPanel from './components/CalibrationPanel'
import BasketballPanel from './components/BasketballPanel'
import HockeyPanel from './components/HockeyPanel'
import MotorsportsPanel from './components/MotorsportsPanel'
import GolfPanel from './components/GolfPanel'
import SoccerPanel from './components/SoccerPanel'
import FootballPanel from './components/FootballPanel'
import BaseballPanel from './components/BaseballPanel'

const api = axios.create({ baseURL: '/api' })

// Per-sport sub-nav. Phase-2 polish 2026-04-28: Props / Derivatives /
// 1st Inn collapsed under Bets via the in-tab MarketToggle. Bets is
// the single "what should I bet today" surface; Tracker is the single
// "how have my bets done" surface; Standings is league context. Live
// reserves a slot for Phase 3.
const SPORT_TABS = [
  { id: 'bets',        label: 'Bets' },
  { id: 'history',     label: 'Tracker' },
  { id: 'calibration', label: 'Calibration' },
  { id: 'standings',   label: 'Standings' },
]

export default function App() {
  const [league, setLeague] = useState('MLB')
  // 'dashboard' is the cross-sport root view; otherwise this is one
  // of SPORT_TABS ids inside the active sport's landing.
  const [view, setView] = useState('bets')
  // Sidebar surface: 'dashboard' or a sport key. Tracks separately from
  // `league` so the dashboard view doesn't lose the user's last sport.
  const [surface, setSurface] = useState('mlb')

  // Per-sport slate + best-bets loaders. Each hook fetches on mount,
  // polls /best-bets/progress for the "Computing 8/15 games..." spinner
  // label, and auto-refreshes every 2 min. See engine/live/... for what
  // the progress endpoint reports.
  const mlbSlate = useSportSlate(api, '')
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
  // detail panes, prediction popovers, tracker slices — stays local so
  // each sport's selectGame handler can mutate them freely.
  const [selectedGame, setSelectedGame] = useState(null)
  const [prediction, setPrediction] = useState(null)
  const [predLoading, setPredLoading] = useState(false)
  const [standings, setStandings] = useState([])
  const [backtest, setBacktest] = useState(null)
  const [btLoading, setBtLoading] = useState(false)
  const [bbLoading, setBbLoading] = useState(false)
  const [pickSummary, setPickSummary] = useState(null)
  const [pickHistory, setPickHistory] = useState(null)
  const [phLoading, setPhLoading] = useState(false)

  const [nhlBbLoading, setNhlBbLoading] = useState(false)
  const [nhlStandings, setNhlStandings] = useState([])
  const [nhlStandingsLoading, setNhlStandingsLoading] = useState(false)
  const [nhlPrediction, setNhlPrediction] = useState(null)
  const [nhlPredLoading, setNhlPredLoading] = useState(false)
  const [nhlSelectedGame, setNhlSelectedGame] = useState(null)
  const [nhlPickSummary, setNhlPickSummary] = useState(null)
  const [nhlPickHistory, setNhlPickHistory] = useState(null)
  const [nhlPhLoading, setNhlPhLoading] = useState(false)

  const [nbaBbLoading, setNbaBbLoading] = useState(false)
  const [nbaStandings, setNbaStandings] = useState([])
  const [nbaStandingsLoading, setNbaStandingsLoading] = useState(false)
  const [nbaPrediction, setNbaPrediction] = useState(null)
  const [nbaPredLoading, setNbaPredLoading] = useState(false)
  const [nbaSelectedGame, setNbaSelectedGame] = useState(null)
  const [nbaPickSummary, setNbaPickSummary] = useState(null)
  const [nbaPickHistory, setNbaPickHistory] = useState(null)
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
  const [motorsportsSeries] =
    useOnce(api, '/motorsports/series', r => r.data?.series || [], [])

  // Baseball is the odd one — synthesize an MLB entry alongside the
  // baseball-framework leagues so the sidebar renders them under one
  // expandable Baseball group. MLB stays on its dedicated /api/mlb/*
  // surface; framework leagues route through /api/baseball/<league>/*.
  const _MLB_STUB = {
    key: 'mlb', display_name: 'MLB', country: 'USA', region: 'USA',
    status: 'active', in_season: true, game_count_today: 0,
  }
  const [baseballLeagues] = useOnce(
    api, '/baseball/leagues',
    r => [_MLB_STUB, ...(r.data?.leagues || [])],
    [_MLB_STUB],
  )

  // Lightweight sidebar-badge counts — cheap endpoints that count
  // open picks / days-until-race etc. so the group icons carry a
  // live number without kicking off a heavy slate build.
  const [f1DaysUntil] = useOnce(
    api, '/motorsports/f1/today', r => r.data?.days_until_race ?? 0, 0,
  )
  const [golfPickCount] = useOnce(
    api, '/golf/counts', r => r.data?.open_picks ?? 0, 0,
  )
  const tennisCount = usePoll(
    api, '/tennis/scheduled/count', r => r.data?.count ?? 0,
    5 * 60 * 1000, 0,
  )

  // Bet queue data — hoisted to App so the picks + tracker persist
  // across tab switches instead of remounting empty every time the
  // user comes back to the queue page. The sidebar badge count reads
  // from the same fetched payload (single 2-min poll for both).
  const [queueData, setQueueData] = useState(null)
  const [trackerData, setTrackerData] = useState(null)
  const [queueLoading, setQueueLoading] = useState(false)
  const refreshQueue = useCallback(() => {
    setQueueLoading(true)
    return Promise.all([
      api.get('/bet-queue').then(r => setQueueData(r.data)).catch(() => {}),
      api.get('/bet-queue/tracker')
        .then(r => setTrackerData(r.data)).catch(() => {}),
    ]).finally(() => setQueueLoading(false))
  }, [])
  useEffect(() => {
    refreshQueue()
    const id = setInterval(refreshQueue, 2 * 60 * 1000)
    return () => clearInterval(id)
  }, [refreshQueue])
  const queueCount = (queueData?.picks || []).length


  // MLB handlers
  const selectGame = useCallback((game) => {
    setSelectedGame(game)
    setView('bets')
    setPrediction(null)
    setPredLoading(true)

    const homeId = game.home.team_id
    const awayId = game.away.team_id
    if (!homeId || !awayId) { setPredLoading(false); return }

    const homePid = game.home_pitcher?.id ? parseInt(game.home_pitcher.id) : null
    const awayPid = game.away_pitcher?.id ? parseInt(game.away_pitcher.id) : null

    api.post('/predict', {
      home_team_id: homeId, away_team_id: awayId,
      home_pitcher_id: homePid, away_pitcher_id: awayPid,
      venue: game.venue || null,
    })
      .then(r => setPrediction(r.data))
      .catch(() => setPrediction(null))
      .finally(() => setPredLoading(false))
  }, [])

  // NHL handlers
  const selectNhlGame = useCallback((game) => {
    setNhlSelectedGame(game)
    setView('bets')
    setNhlPrediction(null)
    setNhlPredLoading(true)

    // Map ESPN abbreviation to team key for prediction
    const h = game.home.abbreviation
    const a = game.away.abbreviation

    api.get(`/nhl/predict?home=${encodeURIComponent(h)}&away=${encodeURIComponent(a)}`)
      .then(r => setNhlPrediction(r.data))
      .catch(() => setNhlPrediction(null))
      .finally(() => setNhlPredLoading(false))
  }, [])

  // NBA handlers
  const selectNbaGame = useCallback((game) => {
    setNbaSelectedGame(game)
    setView('bets')
    setNbaPrediction(null)
    setNbaPredLoading(true)

    const h = game.home.abbreviation
    const a = game.away.abbreviation

    api.get(`/nba/predict?home=${encodeURIComponent(h)}&away=${encodeURIComponent(a)}`)
      .then(r => setNbaPrediction(r.data))
      .catch(() => setNbaPrediction(null))
      .finally(() => setNbaPredLoading(false))
  }, [])

  const showStandings = useCallback(() => {
    setView('standings'); setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)
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
    setView('backtest'); setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)
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
      ]).then(([s, h]) => {
        setPickSummary(s.data)
        setPickHistory(h.data)
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
    setView('history'); setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)
    if (league === 'MLB') {
      setPhLoading(true)
      Promise.all([
        api.get('/tracker/summary'),
        api.get('/tracker/history'),
      ]).then(([s, h]) => {
        setPickSummary(s.data)
        setPickHistory(h.data)
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

  const goBack = useCallback(() => {
    setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)
    setPrediction(null); setNhlPrediction(null); setNbaPrediction(null)
    setView('bets')
  }, [])

  const switchLeague = useCallback((l) => {
    setLeague(l)
    setSurface(l.toLowerCase())
    setView('bets')
    setSelectedGame(null)
    setNhlSelectedGame(null)
    setNbaSelectedGame(null)
    setPrediction(null)
    setNhlPrediction(null)
    setNbaPrediction(null)
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
  // Active motorsports series — null defaults to 'f1' so legacy direct
  // routes to league='F1' keep working without the sidebar.
  const [motorsportsSeriesKey, setMotorsportsSeriesKey] = useState(null)
  const handleSidebarSelect = useCallback((id) => {
    if (id === 'dashboard') {
      setSurface('dashboard')
      setSelectedGame(null)
      setNhlSelectedGame(null)
      setNbaSelectedGame(null)
      return
    }
    if (id === 'portfolio') {
      setSurface('portfolio')
      setSelectedGame(null)
      setNhlSelectedGame(null)
      setNbaSelectedGame(null)
      return
    }
    if (id === 'queue') {
      setSurface('queue')
      setSelectedGame(null)
      setNhlSelectedGame(null)
      setNbaSelectedGame(null)
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
      setMotorsportsSeriesKey(null)
      switchLeague('SOCCER')
      refreshSoccerLeagues()
      return
    }
    // Motorsports series key — F1 / IndyCar / NASCAR. Route under the
    // existing legacy league='F1' surface so the MotorsportsPanel
    // continues to mount; series key drives which slate it pulls.
    const isMotorsportsSeries = motorsportsSeries
      && motorsportsSeries.some(S => S.key === id)
    if (isMotorsportsSeries) {
      setMotorsportsSeriesKey(id)
      setBasketballLeague(null)
      setHockeyLeague(null)
      setSoccerLeague(null)
      switchLeague('F1')
      return
    }
    // Baseball-framework leagues — anything in baseballLeagues that
    // ISN'T 'mlb' (MLB stays on its dedicated heavy path). The sidebar
    // group passes the framework's registry key (e.g. 'college') so
    // we need to remap to the App's league surface key
    // ('COLLEGE_BASEBALL') here rather than blindly upcasing.
    if (baseballLeagues
            && baseballLeagues.some(L => L.key === id)
            && id !== 'mlb') {
      setBasketballLeague(null)
      setHockeyLeague(null)
      setSoccerLeague(null)
      setMotorsportsSeriesKey(null)
      // For now there's just one framework league (college); future
      // KBO / NPB / etc. will need an explicit mapping table here.
      const surfaceKey = id === 'college' ? 'COLLEGE_BASEBALL'
                          : id.toUpperCase()
      switchLeague(surfaceKey)
      return
    }
    setBasketballLeague(null)
    setHockeyLeague(null)
    setSoccerLeague(null)
    setMotorsportsSeriesKey(null)
    switchLeague(id.toUpperCase())
  }, [switchLeague, basketballLeagues, hockeyLeagues, soccerLeagues,
       motorsportsSeries, baseballLeagues,
       refreshBasketballLeagues, refreshHockeyLeagues, refreshSoccerLeagues])

  // Sub-nav tab dispatch — sets the view AND fires lazy data loaders.
  // Bets/Props don't need a loader (Bets is default; Props is a
  // placeholder until 2g+). Standings/Tracker/History/Backtest each
  // hit their respective endpoints on first visit.
  const selectView = useCallback((tabId) => {
    setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)
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
  const isMotorsports = league === 'F1'  // expand to FORMULA1/INDYCAR
  const isGolf = league === 'GOLF'
  const isSoccer = league === 'SOCCER'    // multi-league soccer framework
                                          // when v2 series ship
  const isUFL = league === 'UFL'
  const isCollegeBaseball = league === 'COLLEGE_BASEBALL'

  // Game counts for sidebar badge — pass length of each scoreboard so
  // the "where's the action tonight" hint is current. Passing 0 hides
  // the badge instead of showing "0".
  // AFL count comes from basketballLeagues.game_count_today (the
  // /api/basketball/leagues registry already reports it per-league).
  const aflCount = (basketballLeagues || [])
    .find(L => L.key === 'afl')?.game_count_today ?? 0

  // Per-sport top-level sidebar badges. Reads game_count_today from
  // the per-sport leagues endpoint where present. UFL + college_baseball
  // were missing from the dict so their sidebar entries silently never
  // showed the today's-game badge (user flagged 2026-05-29).
  const uflCount = (footballLeagues || [])
    .find(L => L.key === 'ufl')?.game_count_today ?? 0
  const collegeBaseballCount = (baseballLeagues || [])
    .find(L => L.key === 'college')?.game_count_today ?? 0
  const gameCounts = {
    mlb: games.length,
    nhl: nhlGames.length,
    nba: nbaGames.length,
    tennis: tennisCount,
    afl: aflCount,
    f1: f1DaysUntil,
    golf: golfPickCount,
    ufl: uflCount,
    college_baseball: collegeBaseballCount,
  }

  // Inject the live MLB game count into the synthesized MLB entry so
  // the Baseball group's badge reflects MLB + college combined.
  const baseballLeaguesWithCounts = useMemo(() => {
    if (!baseballLeagues) return null
    return baseballLeagues.map(L => (
      L.key === 'mlb' ? { ...L, game_count_today: games.length } : L
    ))
  }, [baseballLeagues, games.length])

  return (
    <div className="flex min-h-screen bg-background">
      <Sidebar
        sports={['mlb', 'nhl', 'nba', 'tennis', 'afl', 'f1', 'golf', 'soccer', 'ufl', 'college_baseball']}
        selected={
          surface === 'dashboard' ? 'dashboard'
          : surface === 'portfolio' ? 'portfolio'
          : surface === 'queue' ? 'queue'
          : league === 'AUSSIE_RULES' ? 'afl'
          : league === 'F1' ? (motorsportsSeriesKey || 'f1')
          : league === 'GOLF' ? 'golf'
          : league === 'SOCCER' ? (soccerLeague || 'soccer')
          : league === 'BASKETBALL' ? (basketballLeague || 'nba')
          : league === 'HOCKEY' ? (hockeyLeague || 'nhl')
          : league === 'COLLEGE_BASEBALL' ? 'college'
          : league.toLowerCase()
        }
        onSelect={handleSidebarSelect}
        gameCounts={gameCounts}
        queueCount={queueCount}
        basketballLeagues={basketballLeagues}
        hockeyLeagues={hockeyLeagues}
        soccerLeagues={soccerLeagues}
        motorsportsSeries={motorsportsSeries}
        baseballLeagues={baseballLeaguesWithCounts}
      />

      <main className="flex-1 min-w-0 overflow-x-hidden pt-14 md:pt-0">
       {surface === 'dashboard' ? (
         <div className="app">
           <RootDashboard
             sports={['mlb', 'nhl', 'nba', 'tennis']}
             bestBetsBySport={{ mlb: bestBets || [], nhl: nhlBestBets || [], nba: nbaBestBets || [] }}
             onSelectSport={s => { switchLeague(s.toUpperCase()) }}
           />
         </div>
       ) : surface === 'portfolio' ? (
         <div className="app">
           <PortfolioTracker />
         </div>
       ) : surface === 'queue' ? (
         <div className="app">
           <QueuePanel
             api={api}
             queue={queueData}
             tracker={trackerData}
             loading={queueLoading}
             onRefresh={refreshQueue}
           />
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

      {/* Motorsports — F1 race-winner + podium markets, plus IndyCar
          and NASCAR via the sidebar Motorsports group. Outright
          structure (1-of-N), so MotorsportsPanel uses a driver-table
          layout rather than the team-sport game cards. seriesKey
          falls back to 'f1' so legacy direct links to the F1 surface
          (no sidebar selection) keep working. */}
      {isMotorsports && (
        <MotorsportsPanel
          api={api}
          seriesKey={motorsportsSeriesKey || 'f1'}
          series={motorsportsSeries}
        />
      )}

      {/* Golf — outright structure (one tournament, 100+ player
          field). Tour selector inside the panel handles PGA/LPGA/
          Korn Ferry switching since they share the same panel UX. */}
      {isGolf && (
        <GolfPanel api={api} />
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

      {/* College Baseball — first league in the baseball framework
          (MLB stays on its dedicated heavy-feature path). NCAA D1
          season Feb–June, College World Series finishes mid-late
          June. */}
      {isCollegeBaseball && <BaseballPanel api={api} leagueKey="college" />}

      {!isTennis && !isBasketball && !isHockey && !isAussieRules && !isMotorsports && !isGolf && !isSoccer && !isUFL && !isCollegeBaseball && (
        <SubNav
          tabs={SPORT_TABS}
          active={view}
          onChange={selectView}
        />
      )}

      {/* ── MLB Views ── */}
      {isMLB && view === 'bets' && !selectedGame && (
        <BetsView
          sport="mlb"
          api={api}
          data={{
            games,
            loading: gamesLoading,
            progress: bbProgress,
            onSelectGame: selectGame,
            bestBets,
          }}
        />
      )}

      {isMLB && selectedGame && (
        <GameDetail game={selectedGame} prediction={prediction} loading={predLoading} onBack={goBack} />
      )}

      {isMLB && view === 'standings' && <Standings divisions={standings} />}

      {isMLB && view === 'calibration' && <CalibrationPanel sport="mlb" api={api} />}

      {isMLB && view === 'history' && (
        <TrackerView
          sport="mlb"
          api={api}
          trackerProps={{
            summary: pickSummary,
            history: pickHistory,
            loading: phLoading,
            onRecord: recordPicks,
            onSettle: settlePicks,
          }}
        />
      )}


      {/* ── NHL Views ── */}
      {isNHL && view === 'bets' && !nhlSelectedGame && (
        <BetsView
          sport="nhl"
          api={api}
          data={{
            games: nhlGames,
            loading: nhlLoading,
            progress: nhlBbProgress,
            onSelectGame: selectNhlGame,
            bestBets: nhlBestBets,
          }}
        />
      )}

      {isNHL && nhlSelectedGame && (
        <NHLGameDetail game={nhlSelectedGame} prediction={nhlPrediction} loading={nhlPredLoading} onBack={goBack} />
      )}

      {isNHL && view === 'standings' && <NHLStandings divisions={nhlStandings} loading={nhlStandingsLoading} />}

      {isNHL && view === 'calibration' && <CalibrationPanel sport="nhl" api={api} />}

      {isNHL && view === 'history' && (
        <TrackerView
          sport="nhl"
          api={api}
          trackerProps={{
            summary: nhlPickSummary,
            history: nhlPickHistory,
            loading: nhlPhLoading,
            onRecord: recordPicks,
            onSettle: settlePicks,
          }}
        />
      )}


      {/* ── NBA Views ── */}
      {isNBA && view === 'bets' && !nbaSelectedGame && (
        <BetsView
          sport="nba"
          api={api}
          data={{
            games: nbaGames,
            loading: nbaLoading,
            progress: nbaBbProgress,
            onSelectGame: selectNbaGame,
            bestBets: nbaBestBets,
          }}
        />
      )}

      {isNBA && nbaSelectedGame && (
        <NBAGameDetail game={nbaSelectedGame} prediction={nbaPrediction} loading={nbaPredLoading} onBack={goBack} />
      )}

      {isNBA && view === 'standings' && <NBAStandings divisions={nbaStandings} loading={nbaStandingsLoading} />}

      {isNBA && view === 'calibration' && <CalibrationPanel sport="nba" api={api} />}

      {isNBA && view === 'history' && (
        <TrackerView
          sport="nba"
          api={api}
          trackerProps={{
            summary: nbaPickSummary,
            history: nbaPickHistory,
            loading: nbaPhLoading,
            onRecord: recordPicks,
            onSettle: settlePicks,
          }}
        />
      )}

       </div>
       )}
      </main>
    </div>
  )
}

