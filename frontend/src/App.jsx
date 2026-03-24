import { useState, useEffect } from 'react'
import axios from 'axios'
import LeaguePicker from './components/LeaguePicker'
import TeamPicker from './components/TeamPicker'
import PredictionResults from './components/PredictionResults'

const api = axios.create({ baseURL: '/api' })

export default function App() {
  const [leagues, setLeagues] = useState([])
  const [selectedLeague, setSelectedLeague] = useState(null)
  const [teams, setTeams] = useState([])
  const [homeTeam, setHomeTeam] = useState(null)
  const [awayTeam, setAwayTeam] = useState(null)
  const [prediction, setPrediction] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    api.get('/leagues').then(r => setLeagues(r.data))
  }, [])

  useEffect(() => {
    if (!selectedLeague) return
    setTeams([])
    setHomeTeam(null)
    setAwayTeam(null)
    setPrediction(null)
    api.get(`/leagues/${selectedLeague}/teams`).then(r => setTeams(r.data))
  }, [selectedLeague])

  const runPrediction = async () => {
    if (!homeTeam || !awayTeam) return
    setLoading(true)
    setPrediction(null)
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
      setLoading(false)
    }
  }

  const canPredict = homeTeam && awayTeam && homeTeam.key !== awayTeam.key

  return (
    <div className="app">
      <div className="header">
        <h1>Sports Matchup Engine</h1>
        <p>Pick a league, choose two teams, get the full breakdown</p>
      </div>

      <LeaguePicker
        leagues={leagues}
        selected={selectedLeague}
        onSelect={setSelectedLeague}
      />

      {selectedLeague && (
        <>
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
            disabled={!canPredict || loading}
            onClick={runPrediction}
          >
            {loading ? 'Running Model...' : 'Run Prediction'}
          </button>
        </>
      )}

      {loading && (
        <div className="loading">
          <div className="spinner" />
          <p>Crunching numbers...</p>
        </div>
      )}

      {prediction && <PredictionResults data={prediction} />}
    </div>
  )
}
