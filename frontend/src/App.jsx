import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import LeagueTabs from './components/LeagueTabs'
import GamesList from './components/GamesList'
import GameDetail from './components/GameDetail'
import TeamPicker from './components/TeamPicker'
import PredictionResults from './components/PredictionResults'

const api = axios.create({ baseURL: '/api' })

export default function App() {
  const [leagues, setLeagues] = useState([])
  const [selectedLeague, setSelectedLeague] = useState(null)
  const [games, setGames] = useState([])
  const [gamesLoading, setGamesLoading] = useState(false)
  const [selectedGame, setSelectedGame] = useState(null)

  // Custom matchup state
  const [showCustom, setShowCustom] = useState(false)
  const [teams, setTeams] = useState([])
  const [homeTeam, setHomeTeam] = useState(null)
  const [awayTeam, setAwayTeam] = useState(null)

  // Prediction state
  const [prediction, setPrediction] = useState(null)
  const [predLoading, setPredLoading] = useState(false)

  useEffect(() => {
    api.get('/leagues').then(r => setLeagues(r.data))
  }, [])

  const selectLeague = useCallback((key) => {
    setSelectedLeague(key)
    setSelectedGame(null)
    setPrediction(null)
    setShowCustom(false)
    setHomeTeam(null)
    setAwayTeam(null)
    setTeams([])

    // Fetch scoreboard
    setGamesLoading(true)
    setGames([])
    api.get(`/leagues/${key}/scoreboard`)
      .then(r => setGames(r.data))
      .catch(() => setGames([]))
      .finally(() => setGamesLoading(false))
  }, [])

  const openCustom = useCallback(() => {
    setShowCustom(true)
    setSelectedGame(null)
    setPrediction(null)
    if (teams.length === 0 && selectedLeague) {
      api.get(`/leagues/${selectedLeague}/teams`).then(r => setTeams(r.data))
    }
  }, [selectedLeague, teams.length])

  const selectGame = useCallback((game) => {
    setSelectedGame(game)
    setShowCustom(false)
    setPrediction(null)
    setPredLoading(true)
    api.post('/predict', {
      league: selectedLeague,
      home: game.home.key,
      away: game.away.key,
    })
      .then(r => setPrediction(r.data))
      .catch(() => setPrediction(null))
      .finally(() => setPredLoading(false))
  }, [selectedLeague])

  const runCustomPrediction = useCallback(async () => {
    if (!homeTeam || !awayTeam) return
    setPredLoading(true)
    setPrediction(null)
    setSelectedGame(null)
    try {
      const res = await api.post('/predict', {
        league: selectedLeague,
        home: homeTeam.key,
        away: awayTeam.key,
      })
      setPrediction(res.data)
    } catch (err) {
      console.error(err)
    } finally {
      setPredLoading(false)
    }
  }, [selectedLeague, homeTeam, awayTeam])

  const goBack = useCallback(() => {
    setSelectedGame(null)
    setPrediction(null)
    setShowCustom(false)
  }, [])

  const leagueInfo = leagues.find(l => l.key === selectedLeague)
  const canPredict = homeTeam && awayTeam && homeTeam.key !== awayTeam.key

  return (
    <div className="app">
      <div className="header">
        <h1>Sports Matchup Engine</h1>
      </div>

      <LeagueTabs
        leagues={leagues}
        selected={selectedLeague}
        onSelect={selectLeague}
      />

      {selectedLeague && !selectedGame && !showCustom && (
        <GamesList
          games={games}
          loading={gamesLoading}
          leagueInfo={leagueInfo}
          onSelectGame={selectGame}
          onCustomMatchup={openCustom}
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

      {showCustom && (
        <div className="custom-matchup">
          <button className="back-btn" onClick={goBack}>
            <span className="back-arrow">&larr;</span> Back to games
          </button>
          <h2 className="section-title">Custom Matchup</h2>
          <div className="matchup-setup">
            <TeamPicker
              label="Home"
              teams={teams}
              selected={homeTeam}
              onSelect={setHomeTeam}
              excludeKey={awayTeam?.key}
            />
            <div className="vs-divider">VS</div>
            <TeamPicker
              label="Away"
              teams={teams}
              selected={awayTeam}
              onSelect={setAwayTeam}
              excludeKey={homeTeam?.key}
            />
          </div>

          <button
            className="predict-btn"
            disabled={!canPredict || predLoading}
            onClick={runCustomPrediction}
          >
            {predLoading ? 'Running Model...' : 'Run Prediction'}
          </button>

          {predLoading && !prediction && (
            <div className="loading">
              <div className="spinner" />
              <p>Crunching numbers...</p>
            </div>
          )}

          {prediction && <PredictionResults data={prediction} />}
        </div>
      )}

      {!selectedLeague && (
        <div className="welcome">
          <p>Select a league above to see today's games and matchup predictions.</p>
        </div>
      )}
    </div>
  )
}
