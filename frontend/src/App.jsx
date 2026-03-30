import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import Scoreboard from './components/Scoreboard'
import GameDetail from './components/GameDetail'
import Standings from './components/Standings'
import Backtest from './components/Backtest'
import BestBets from './components/BestBets'
import PickHistory from './components/PickHistory'

const api = axios.create({ baseURL: '/api' })

export default function App() {
  const [view, setView] = useState('games')
  const [games, setGames] = useState([])
  const [gamesLoading, setGamesLoading] = useState(true)
  const [selectedGame, setSelectedGame] = useState(null)
  const [prediction, setPrediction] = useState(null)
  const [predLoading, setPredLoading] = useState(false)
  const [standings, setStandings] = useState([])
  const [backtest, setBacktest] = useState(null)
  const [btLoading, setBtLoading] = useState(false)
  const [bestBets, setBestBets] = useState(null)
  const [bbLoading, setBbLoading] = useState(false)
  const [pickSummary, setPickSummary] = useState(null)
  const [pickHistory, setPickHistory] = useState(null)
  const [phLoading, setPhLoading] = useState(false)

  // Load today's games on mount + auto-refresh every 5 min
  useEffect(() => {
    const fetchGames = () => {
      api.get('/scoreboard').then(r => setGames(r.data)).catch(() => {})
    }
    setGamesLoading(true)
    api.get('/scoreboard')
      .then(r => setGames(r.data))
      .catch(() => setGames([]))
      .finally(() => setGamesLoading(false))
    const interval = setInterval(fetchGames, 5 * 60 * 1000)
    return () => clearInterval(interval)
  }, [])

  const selectGame = useCallback((game) => {
    setSelectedGame(game)
    setView('games')
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

  const showStandings = useCallback(() => {
    setView('standings'); setSelectedGame(null)
    if (standings.length === 0) {
      api.get('/standings').then(r => setStandings(r.data)).catch(() => {})
    }
  }, [standings.length])

  const showBacktest = useCallback(() => {
    setView('backtest'); setSelectedGame(null)
  }, [])

  const runBacktest = useCallback((days, minEdge, season) => {
    setBtLoading(true); setBacktest(null)
    const params = new URLSearchParams()
    if (days) params.set('days', days)
    if (minEdge) params.set('min_edge', minEdge)
    if (season) params.set('season', season)
    api.get(`/backtest?${params}`)
      .then(r => setBacktest(r.data))
      .catch(() => setBacktest({ error: "Backtest failed. Try again." }))
      .finally(() => setBtLoading(false))
  }, [])

  const showBestBets = useCallback(() => {
    setView('best-bets'); setSelectedGame(null)
    setBbLoading(true)
    api.get('/best-bets')
      .then(r => setBestBets(r.data))
      .catch(() => setBestBets([]))
      .finally(() => setBbLoading(false))
  }, [])

  const showHistory = useCallback(() => {
    setView('history'); setSelectedGame(null)
    setPhLoading(true)
    Promise.all([
      api.get('/tracker/summary'),
      api.get('/tracker/history'),
    ]).then(([s, h]) => {
      setPickSummary(s.data)
      setPickHistory(h.data)
    }).catch(() => {})
      .finally(() => setPhLoading(false))
  }, [])

  const recordPicks = useCallback(() => {
    api.post('/tracker/record').then(() => {
      // Refresh
      api.get('/tracker/summary').then(r => setPickSummary(r.data))
      api.get('/tracker/history').then(r => setPickHistory(r.data))
    })
  }, [])

  const settlePicks = useCallback(() => {
    api.post('/tracker/settle').then(() => {
      api.get('/tracker/summary').then(r => setPickSummary(r.data))
      api.get('/tracker/history').then(r => setPickHistory(r.data))
    })
  }, [])

  const goBack = useCallback(() => {
    setSelectedGame(null); setPrediction(null); setView('games')
  }, [])

  return (
    <div className="app">
      <div className="header">
        <h1>MLB Prediction Engine</h1>
        <p className="subtitle">Data-driven MLB game predictions</p>
      </div>

      <nav className="nav-tabs">
        <button className={`nav-tab ${view === 'games' && !selectedGame ? 'active' : ''}`} onClick={goBack}>
          Games
        </button>
        <button className={`nav-tab ${view === 'best-bets' ? 'active' : ''}`} onClick={showBestBets}>
          Best Bets
        </button>
        <button className={`nav-tab ${view === 'standings' ? 'active' : ''}`} onClick={showStandings}>
          Standings
        </button>
        <button className={`nav-tab ${view === 'history' ? 'active' : ''}`} onClick={showHistory}>
          Pick Tracker
        </button>
        <button className={`nav-tab ${view === 'backtest' ? 'active' : ''}`} onClick={showBacktest}>
          Backtest
        </button>
      </nav>

      {view === 'games' && !selectedGame && (
        <Scoreboard games={games} loading={gamesLoading} onSelectGame={selectGame} />
      )}

      {selectedGame && (
        <GameDetail game={selectedGame} prediction={prediction} loading={predLoading} onBack={goBack} />
      )}

      {view === 'best-bets' && (
        <BestBets bets={bestBets} loading={bbLoading} />
      )}

      {view === 'standings' && (
        <Standings divisions={standings} />
      )}

      {view === 'history' && (
        <PickHistory
          summary={pickSummary}
          history={pickHistory}
          loading={phLoading}
          onRecord={recordPicks}
          onSettle={settlePicks}
        />
      )}

      {view === 'backtest' && (
        <Backtest data={backtest} loading={btLoading} onRun={runBacktest} />
      )}
    </div>
  )
}
