import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import Scoreboard from './components/Scoreboard'
import GameDetail from './components/GameDetail'
import Standings from './components/Standings'

const api = axios.create({ baseURL: '/api' })

export default function App() {
  const [view, setView] = useState('games')  // 'games' | 'standings'
  const [games, setGames] = useState([])
  const [gamesLoading, setGamesLoading] = useState(true)
  const [selectedGame, setSelectedGame] = useState(null)
  const [prediction, setPrediction] = useState(null)
  const [predLoading, setPredLoading] = useState(false)
  const [standings, setStandings] = useState([])

  // Load today's games on mount + auto-refresh every 5 min
  useEffect(() => {
    const fetchGames = () => {
      api.get('/scoreboard')
        .then(r => setGames(r.data))
        .catch(() => {})
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
    setPrediction(null)
    setPredLoading(true)

    const homeId = game.home.team_id
    const awayId = game.away.team_id
    if (!homeId || !awayId) {
      setPredLoading(false)
      return
    }

    // Get pitcher IDs from ESPN data if available
    const homePitcherId = game.home_pitcher?.id ? parseInt(game.home_pitcher.id) : null
    const awayPitcherId = game.away_pitcher?.id ? parseInt(game.away_pitcher.id) : null

    api.post('/predict', {
      home_team_id: homeId,
      away_team_id: awayId,
      home_pitcher_id: homePitcherId,
      away_pitcher_id: awayPitcherId,
      venue: game.venue || null,
    })
      .then(r => setPrediction(r.data))
      .catch(() => setPrediction(null))
      .finally(() => setPredLoading(false))
  }, [])

  const showStandings = useCallback(() => {
    setView('standings')
    setSelectedGame(null)
    if (standings.length === 0) {
      api.get('/standings')
        .then(r => setStandings(r.data))
        .catch(() => {})
    }
  }, [standings.length])

  const goBack = useCallback(() => {
    setSelectedGame(null)
    setPrediction(null)
    setView('games')
  }, [])

  return (
    <div className="app">
      <div className="header">
        <h1>MLB Prediction Engine</h1>
        <p className="subtitle">Data-driven MLB game predictions</p>
      </div>

      <nav className="nav-tabs">
        <button
          className={`nav-tab ${view === 'games' && !selectedGame ? 'active' : ''}`}
          onClick={goBack}
        >
          Today's Games
        </button>
        <button
          className={`nav-tab ${view === 'standings' ? 'active' : ''}`}
          onClick={showStandings}
        >
          Standings
        </button>
      </nav>

      {view === 'games' && !selectedGame && (
        <Scoreboard
          games={games}
          loading={gamesLoading}
          onSelectGame={selectGame}
        />
      )}

      {selectedGame && (
        <GameDetail
          game={selectedGame}
          prediction={prediction}
          loading={predLoading}
          onBack={goBack}
        />
      )}

      {view === 'standings' && (
        <Standings divisions={standings} />
      )}
    </div>
  )
}
